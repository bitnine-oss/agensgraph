/*
 * nodeModifyGraph.c
 *	  routines to handle ModifyGraph nodes.
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeModifyGraph.c
 */

#include "postgres.h"

#include "ag_const.h"
#include "access/htup_details.h"
#include "catalog/ag_graph_fn.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/nodeModifyGraph.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "nodes/graphnodes.h"
#include "nodes/nodeFuncs.h"
#include "utils/arrayaccess.h"
#include "utils/builtins.h"
#include "utils/graph.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"

#define SQLCMD_BUFLEN				(NAMEDATALEN + 192)

/*
 * [!] If add SQLCMD, should add SqlcmdType and use MGPlanCache.
 */
#define SQLCMD_CREAT_VERTEX \
	"INSERT INTO \"%s\".\"%s\" VALUES (DEFAULT, $1) RETURNING " \
	AG_ELEM_LOCAL_ID " AS " AG_ELEM_ID ", " AG_ELEM_PROP_MAP
#define SQLCMD_VERTEX_NPARAMS		1

#define SQLCMD_CREAT_EDGE \
	"INSERT INTO \"%s\".\"%s\" VALUES (DEFAULT, $1, $2, $3) RETURNING " \
	AG_ELEM_LOCAL_ID " AS " AG_ELEM_ID ", " \
	AG_START_ID ", \"" AG_END_ID "\", " AG_ELEM_PROP_MAP
#define SQLCMD_EDGE_NPARAMS			3

#define SQLCMD_DEL_ELEM \
	"DELETE FROM ONLY \"%s\".\"%s\" WHERE " AG_ELEM_LOCAL_ID " = $1"
#define SQLCMD_DEL_ELEM_NPARAMS		1

#define SQLCMD_DETACH \
	"SELECT " AG_ELEM_LOCAL_ID " FROM \"%s\"." AG_EDGE \
	" WHERE " AG_START_ID " = $1 OR \"" AG_END_ID "\" = $1"
#define SQLCMD_DETACH_NPARAMS		1

#define SQLCMD_DEL_EDGES \
	"DELETE FROM \"%s\"." AG_EDGE \
	" WHERE " AG_START_ID " = $1 OR \"" AG_END_ID "\" = $1"
#define SQLCMD_DEL_EDGES_NPARAMS	1

#define SQLCMD_SET_PROP \
	"UPDATE \"%s\".\"%s\" SET properties = jsonb_set(properties, $1, $2)" \
	" WHERE " AG_ELEM_LOCAL_ID " = $3"
#define SQLCMD_SET_PROP_NPARAMS		3

#define SQLCMD_OVERWR_PROP \
	"UPDATE \"%s\".\"%s\" SET properties = $1 WHERE " AG_ELEM_LOCAL_ID " = $2"
#define SQLCMD_OVERWR_PROP_NPARAMS	2

#define SQLCMD_RM_PROP \
	"UPDATE \"%s\".\"%s\" SET properties = jsonb_delete_path(properties, $1)" \
	" WHERE " AG_ELEM_LOCAL_ID " = $2"
#define SQLCMD_RM_PROP_NPARAMS		2

typedef enum SqlcmdType
{
	SQLCMD_TYPE_CREATE_VERTEX,
	SQLCMD_TYPE_CREATE_EDGE,
	SQLCMD_TYPE_DEL_ELEM,
	SQLCMD_TYPE_DETACH,
	SQLCMD_TYPE_DEL_EDGES,
	SQLCMD_TYPE_SET_PROP,
	SQLCMD_TYPE_OVERWR_PROP,
	SQLCMD_TYPE_RM_PROP
} SqlcmdType;

typedef struct ArrayAccessTypeInfo {
	int16		typlen;
	bool		typbyval;
	char		typalign;
} ArrayAccessTypeInfo;

typedef enum DelElemKind {
	DEL_ELEM_VERTEX,
	DEL_ELEM_EDGE
} DelElemKind;

typedef struct {
	Oid			labelid;
	SqlcmdType	cmdtype;
} MGPlanKey;

typedef struct {
	MGPlanKey	key;
	SPIPlanPtr	plan;
} MGPlan;

static HTAB *MGPlanCache = NULL;

static List *ExecInitGraphPattern(List *pattern, ModifyGraphState *mgstate);
static List *ExecInitGraphSets(List *sets, ModifyGraphState *mgstate);
static TupleTableSlot *ExecCreateGraph(ModifyGraphState *mgstate,
									   TupleTableSlot *slot);
static Datum createVertex(ModifyGraphState *mgstate, GraphVertex *gvertex,
						  Graphid *vid, TupleTableSlot *slot, bool inPath);
static Datum createEdge(ModifyGraphState *mgstate, GraphEdge *gedge,
						Graphid *start, Graphid *end, TupleTableSlot *slot,
						bool inPath);
static TupleTableSlot *createPath(ModifyGraphState *mgstate, GraphPath *path,
								  TupleTableSlot *slot);
static Datum findVertex(TupleTableSlot *slot, GraphVertex *node, Graphid *vid);
static AttrNumber findAttrInSlotByName(TupleTableSlot *slot, char *name);
static Datum evalPropMap(ExprState *prop_map, ExprContext *econtext,
						 TupleTableSlot *slot);
static Datum copyTupleAsDatum(ExprContext *econtext, HeapTuple tuple,
							  Oid tupType);
static void setSlotValueByName(TupleTableSlot *slot, Datum value, char *name);
static Datum *makeDatumArray(ExprContext *econtext, int len);
static TupleTableSlot *ExecDeleteGraph(ModifyGraphState *mgstate,
									   TupleTableSlot *slot);
static void deleteVertex(ModifyGraphState *mgstate, Datum vertex, bool detach);
static bool vertexHasEdge(Datum vid);
static void deleteVertexEdges(ModifyGraphState *mgstate, Datum vid);
static void deleteElem(ModifyGraphState *mgstate, Datum id, DelElemKind kind);
static void deletePath(ModifyGraphState *mgstate, Datum graphpath, bool detach);
static TupleTableSlot *ExecSetGraph(ModifyGraphState *mgstate,
									TupleTableSlot *slot);
static void setElemProp(ModifyGraphState *mgstate, Datum id, Datum path,
						Datum expr);
static void overwiteElemProp(ModifyGraphState *mgstate, Datum id,
							 Datum prop_map);
static void removeElemProp(ModifyGraphState *mgstate, Datum id, Datum path);
static SPIPlanPtr getPreparedplan(Oid labelid, SqlcmdType cmdtype,
								  char *sqlcmd, int nargs, Oid *argtypes);
static void InitMGPlanHashTables(void);
static MGPlan *findPreparedPlan(MGPlanKey *key);
static MGPlan *savePreparedPlan(MGPlanKey *key, SPIPlanPtr plan);

ModifyGraphState *
ExecInitModifyGraph(ModifyGraph *mgplan, EState *estate, int eflags)
{
	ModifyGraphState *mgstate;

	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	mgstate = makeNode(ModifyGraphState);
	mgstate->ps.plan = (Plan *) mgplan;
	mgstate->ps.state = estate;

	ExecInitResultTupleSlot(estate, &mgstate->ps);
	ExecAssignResultType(&mgstate->ps, ExecTypeFromTL(NIL, false));

	ExecAssignExprContext(estate, &mgstate->ps);

	mgstate->canSetTag = mgplan->canSetTag;
	mgstate->done = false;
	mgstate->subplan = ExecInitNode(mgplan->subplan, estate, eflags);

	mgstate->pattern = ExecInitGraphPattern(mgplan->pattern, mgstate);
	mgstate->exprs = (List *) ExecInitExpr((Expr *) mgplan->exprs,
										   (PlanState *) mgstate);
	mgstate->sets = ExecInitGraphSets(mgplan->sets, mgstate);

	return mgstate;
}

TupleTableSlot *
ExecModifyGraph(ModifyGraphState *mgstate)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;

	if (mgstate->done)
		return NULL;

	for (;;)
	{
		TupleTableSlot *slot;

		slot = ExecProcNode(mgstate->subplan);
		if (TupIsNull(slot))
			break;

		switch (plan->operation)
		{
			case GWROP_CREATE:
				slot = ExecCreateGraph(mgstate, slot);
				break;
			case GWROP_DELETE:
				slot = ExecDeleteGraph(mgstate, slot);
				break;
			case GWROP_SET:
				slot = ExecSetGraph(mgstate, slot);
				break;
			default:
				elog(ERROR, "unknown operation");
				break;
		}

		if (slot != NULL)
			return slot;
	}

	mgstate->done = true;

	return NULL;
}

void
ExecEndModifyGraph(ModifyGraphState *mgstate)
{
	ExecFreeExprContext(&mgstate->ps);
	ExecEndNode(mgstate->subplan);
}

static List *
ExecInitGraphPattern(List *pattern, ModifyGraphState *mgstate)
{
	ListCell *lp;

	foreach(lp, pattern)
	{
		GraphPath  *p = lfirst(lp);
		ListCell   *le;

		foreach(le, p->chain)
		{
			Node *elem = lfirst(le);

			if (nodeTag(elem) == T_GraphVertex)
			{
				GraphVertex *gvertex = (GraphVertex *) elem;

				if (gvertex->create)
				{
					gvertex->es_prop_map
							= ExecInitExpr((Expr *) gvertex->prop_map,
										   (PlanState *) mgstate);
				}
			}
			else
			{
				GraphEdge *gedge = (GraphEdge *) elem;

				Assert(nodeTag(elem) == T_GraphEdge);

				gedge->es_prop_map = ExecInitExpr((Expr *) gedge->prop_map,
												  (PlanState *) mgstate);
			}
		}
	}

	return pattern;
}

static List *
ExecInitGraphSets(List *sets, ModifyGraphState *mgstate)
{
	ListCell *ls;

	foreach(ls, sets)
	{
		GraphSetProp *gsp = lfirst(ls);

		gsp->es_elem = ExecInitExpr((Expr *) gsp->elem, (PlanState *) mgstate);
		gsp->es_path = ExecInitExpr((Expr *) gsp->path, (PlanState *) mgstate);
		gsp->es_expr = ExecInitExpr((Expr *) gsp->expr, (PlanState *) mgstate);
	}

	return sets;
}

static TupleTableSlot *
ExecCreateGraph(ModifyGraphState *mgstate, TupleTableSlot *slot)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	ExprContext *econtext = mgstate->ps.ps_ExprContext;
	ListCell   *lp;

	ResetExprContext(econtext);

	/* create a pattern, accumulated paths `slot` has */
	foreach(lp, plan->pattern)
	{
		GraphPath *path = (GraphPath *) lfirst(lp);

		slot = createPath(mgstate, path, slot);
	}

	return (plan->last ? NULL : slot);
}

/* create a path and accumulate it to the given slot */
static TupleTableSlot *
createPath(ModifyGraphState *mgstate, GraphPath *path, TupleTableSlot *slot)
{
	ExprContext *econtext = mgstate->ps.ps_ExprContext;
	bool		out = (path->variable != NULL);
	int			pathlen;
	Datum	   *vertices = NULL;
	Datum	   *edges = NULL;
	int			nvertices;
	int			nedges;
	ListCell   *le;
	Graphid		prevvid;
	GraphEdge  *gedge = NULL;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	if (out)
	{
		pathlen = list_length(path->chain);
		Assert(pathlen % 2 == 1);

		vertices = makeDatumArray(econtext, (pathlen / 2) + 1);
		edges = makeDatumArray(econtext, pathlen / 2);

		nvertices = 0;
		nedges = 0;
	}

	foreach(le, path->chain)
	{
		Node *elem = (Node *) lfirst(le);

		if (nodeTag(elem) == T_GraphVertex)
		{
			GraphVertex *gvertex = (GraphVertex *) elem;
			Graphid		vid;
			Datum		vertex;

			if (gvertex->create)
				vertex = createVertex(mgstate, gvertex, &vid, slot, out);
			else
				vertex = findVertex(slot, gvertex, &vid);

			if (out)
				vertices[nvertices++] = vertex;

			if (gedge != NULL)
			{
				Datum edge;

				if (gedge->direction == GRAPH_EDGE_DIR_LEFT)
				{
					edge = createEdge(mgstate, gedge, &vid, &prevvid, slot,
									  out);
				}
				else
				{
					Assert(gedge->direction == GRAPH_EDGE_DIR_RIGHT);

					edge = createEdge(mgstate, gedge, &prevvid, &vid, slot,
									  out);
				}

				if (out)
					edges[nedges++] = edge;
			}

			prevvid = vid;
		}
		else
		{
			Assert(nodeTag(elem) == T_GraphEdge);

			gedge = (GraphEdge *) elem;
		}
	}

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	/* make a graphpath and set it to the slot */
	if (out)
	{
		MemoryContext oldmctx;
		Datum graphpath;

		Assert(nvertices == nedges + 1);
		Assert(pathlen == nvertices + nedges);

		oldmctx = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

		graphpath = makeGraphpathDatum(vertices, nvertices, edges, nedges);

		MemoryContextSwitchTo(oldmctx);

		setSlotValueByName(slot, graphpath, path->variable);
	}

	return slot;
}

/*
 * createVertex - creates a vertex of a given node
 *
 * NOTE: This function returns a vertex if it must be in the result(`slot`).
 */
static Datum
createVertex(ModifyGraphState *mgstate, GraphVertex *gvertex, Graphid *vid,
			 TupleTableSlot *slot, bool inPath)
{
	EState	   *estate = mgstate->ps.state;
	ExprContext *econtext = mgstate->ps.ps_ExprContext;
	char		sqlcmd[SQLCMD_BUFLEN];
	char	   *label;
	Datum		values[SQLCMD_VERTEX_NPARAMS];
	Oid			argTypes[SQLCMD_VERTEX_NPARAMS] = {JSONBOID};
	Oid			labid;
	int			ret;
	SPIPlanPtr	plan;
	TupleDesc	tupDesc;
	HeapTuple	tuple;
	Datum		vertex = (Datum) NULL;


	label = gvertex->label;
	if (label == NULL)
		label = AG_VERTEX;

	labid = get_labname_labid(label, get_graphname_oid(get_graph_path()));

	snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_CREAT_VERTEX,
			 get_graph_path(), label);

	values[0] = evalPropMap(gvertex->es_prop_map, mgstate->ps.ps_ExprContext,
							slot);

	/* Prepare and execute plan */
	plan = getPreparedplan(labid, SQLCMD_TYPE_CREATE_VERTEX, sqlcmd,
						   SQLCMD_VERTEX_NPARAMS, argTypes);
	ret = SPI_execp(plan, values, NULL, 0);

	if (ret != SPI_OK_INSERT_RETURNING)
		elog(ERROR, "SPI_execute failed: %s", sqlcmd);
	if (SPI_processed != 1)
		elog(ERROR, "SPI_execute: only one vertex per execution must be created");

	tupDesc = SPI_tuptable->tupdesc;
	tuple = SPI_tuptable->vals[0];

	if (vid != NULL)
	{
		bool		isnull;
		Graphid	   *id;

		Assert(SPI_fnumber(tupDesc, AG_ELEM_ID) == 1);
		id = DatumGetGraphidP(SPI_getbinval(tuple, tupDesc, 1, &isnull));
		Assert(!isnull);

		GraphidSet(vid, id->oid, id->lid);
	}

	/* if this vertex is in the result solely or in some paths, */
	if (gvertex->variable != NULL || inPath)
		vertex = copyTupleAsDatum(econtext, tuple, VERTEXOID);

	if (gvertex->variable != NULL)
		setSlotValueByName(slot, vertex, gvertex->variable);

	if (mgstate->canSetTag)
		estate->es_graphwrstats.insertVertex += SPI_processed;

	return vertex;
}

static Datum
createEdge(ModifyGraphState *mgstate, GraphEdge *gedge, Graphid *start,
		   Graphid *end, TupleTableSlot *slot, bool inPath)
{
	EState	   *estate = mgstate->ps.state;
	ExprContext *econtext = mgstate->ps.ps_ExprContext;
	char		sqlcmd[SQLCMD_BUFLEN];
	Datum		values[SQLCMD_EDGE_NPARAMS];
	Oid			argTypes[SQLCMD_EDGE_NPARAMS] = {GRAPHIDOID, GRAPHIDOID,
												 JSONBOID};
	Oid			labid;
	SPIPlanPtr	plan;
	int			ret;
	Datum		edge = (Datum) NULL;


	labid = get_labname_labid(gedge->label,
							  get_graphname_oid(get_graph_path()));

	snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_CREAT_EDGE,
			 get_graph_path(), gedge->label);

	values[0] = GraphidPGetDatum(start);
	values[1] = GraphidPGetDatum(end);
	values[2] = evalPropMap(gedge->es_prop_map, mgstate->ps.ps_ExprContext,
							slot);

	/* Prepare and execute plan */
	plan = getPreparedplan(labid, SQLCMD_TYPE_CREATE_EDGE, sqlcmd,
						   SQLCMD_EDGE_NPARAMS, argTypes);
	ret = SPI_execp(plan, values, NULL, 0);

	if (ret != SPI_OK_INSERT_RETURNING)
		elog(ERROR, "SPI_execute failed: %s", sqlcmd);
	if (SPI_processed != 1)
		elog(ERROR, "SPI_execute: only one edge per execution must be created");

	if (gedge->variable != NULL || inPath)
	{
		HeapTuple tuple = SPI_tuptable->vals[0];

		edge = copyTupleAsDatum(econtext, tuple, EDGEOID);
	}

	if (gedge->variable != NULL)
		setSlotValueByName(slot, edge, gedge->variable);

	if (mgstate->canSetTag)
		estate->es_graphwrstats.insertEdge += SPI_processed;

	return edge;
}

static Datum
findVertex(TupleTableSlot *slot, GraphVertex *gvertex, Graphid *vid)
{
	AttrNumber	attno;
	Datum		vertex;

	attno = findAttrInSlotByName(slot, gvertex->variable);

	vertex = slot->tts_values[attno - 1];

	if (vid != NULL)
	{
		Graphid *id;

		id = DatumGetGraphidP(getVertexIdDatum(vertex));

		GraphidSet(vid, id->oid, id->lid);
	}

	return vertex;
}

static AttrNumber
findAttrInSlotByName(TupleTableSlot *slot, char *name)
{
	TupleDesc	tupDesc = slot->tts_tupleDescriptor;
	int			i;

	for (i = 0; i < tupDesc->natts; i++)
	{
		if (namestrcmp(&(tupDesc->attrs[i]->attname), name) == 0 &&
			!tupDesc->attrs[i]->attisdropped)
			return tupDesc->attrs[i]->attnum;
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_NAME),
			 errmsg("variable \"%s\" does not exist", name)));
	return InvalidAttrNumber;
}

static Datum
evalPropMap(ExprState *prop_map, ExprContext *econtext, TupleTableSlot *slot)
{
	Datum		datum;
	bool		isNull;
	ExprDoneCond isDone;
	Jsonb	   *jsonb;

	if (prop_map == NULL)
		return jsonb_build_object_noargs(NULL);

	if (!(exprType((Node *) prop_map->expr) == JSONBOID))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("property map must be jsonb type")));

	econtext->ecxt_scantuple = slot;
	datum = ExecEvalExpr(prop_map, econtext, &isNull, &isDone);
	if (isNull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("property map cannot be NULL")));
	if (isDone != ExprSingleResult)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("single result is expected for property map")));

	jsonb = DatumGetJsonb(datum);
	if (!JB_ROOT_IS_OBJECT(jsonb))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("jsonb object is expected for property map")));

	return datum;
}

static Datum
copyTupleAsDatum(ExprContext *econtext, HeapTuple tuple, Oid tupType)
{
	TupleDesc		tupDesc;
	MemoryContext	oldmctx;
	Datum			value;

	tupDesc = lookup_rowtype_tupdesc(tupType, -1);

	oldmctx = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	value = heap_copy_tuple_as_datum(tuple, tupDesc);

	MemoryContextSwitchTo(oldmctx);

	ReleaseTupleDesc(tupDesc);

	return value;
}

static void
setSlotValueByName(TupleTableSlot *slot, Datum value, char *name)
{
	AttrNumber attno;

	attno = findAttrInSlotByName(slot, name);

	slot->tts_values[attno - 1] = value;
	slot->tts_isnull[attno - 1] = false;
}

static Datum *
makeDatumArray(ExprContext *econtext, int len)
{
	MemoryContext oldmctx;
	Datum *result;

	if (len == 0)
		return NULL;

	oldmctx = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	result = palloc(len * sizeof(Datum));

	MemoryContextSwitchTo(oldmctx);

	return result;
}

static TupleTableSlot *
ExecDeleteGraph(ModifyGraphState *mgstate, TupleTableSlot *slot)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	ExprContext *econtext = mgstate->ps.ps_ExprContext;
	ListCell   *le;

	ResetExprContext(econtext);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	foreach(le, mgstate->exprs)
	{
		ExprState  *e = (ExprState *) lfirst(le);
		Oid			type;
		Datum		datum;
		bool		isNull;
		ExprDoneCond isDone;

		type = exprType((Node *) e->expr);
		if (!(type == VERTEXOID || type == EDGEOID || type == GRAPHPATHOID))
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("expected node, relationship, or path")));

		econtext->ecxt_scantuple = slot;
		datum = ExecEvalExpr(e, econtext, &isNull, &isDone);
		if (isNull)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("deleting NULL is not allowed")));
		if (isDone != ExprSingleResult)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("expected single result")));

		switch (type)
		{
			case VERTEXOID:
				deleteVertex(mgstate, datum, plan->detach);
				break;
			case EDGEOID:
				deleteElem(mgstate, getEdgeIdDatum(datum), DEL_ELEM_EDGE);
				break;
			case GRAPHPATHOID:
				deletePath(mgstate, datum, plan->detach);
				break;
			default:
				elog(ERROR, "expected node, relationship, or path");
		}
	}

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	return (plan->last ? NULL : slot);
}

static void
deleteVertex(ModifyGraphState *mgstate, Datum vertex, bool detach)
{
	Datum		id_datum;
	Graphid	   *id;

	id_datum = getVertexIdDatum(vertex);
	id = DatumGetGraphidP(id_datum);

	if (vertexHasEdge(id_datum))
	{
		if (detach)
		{
			deleteVertexEdges(mgstate, id_datum);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
					 errmsg("vertex " INT64_FORMAT " in \"%s\" has edge(s)",
							id->lid, get_rel_name(id->oid))));
		}
	}

	deleteElem(mgstate, id_datum, DEL_ELEM_VERTEX);
}

static bool
vertexHasEdge(Datum vid)
{
	char		sqlcmd[SQLCMD_BUFLEN];
	Datum		values[SQLCMD_DETACH_NPARAMS];
	Oid			argTypes[SQLCMD_DETACH_NPARAMS];
	Oid			labid;
	SPIPlanPtr	plan;
	int			ret;

	snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_DETACH, get_graph_path());

	values[0] = vid;
	argTypes[0] = GRAPHIDOID;

	labid = get_labname_labid(AG_EDGE, get_graphname_oid(get_graph_path()));

	/* Prepare and execute plan */
	plan = getPreparedplan(labid, SQLCMD_TYPE_DETACH, sqlcmd,
						   SQLCMD_DETACH_NPARAMS, argTypes);
	ret = SPI_execp(plan, values, NULL, 0);

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute failed: %s", sqlcmd);

	return (SPI_processed > 0);
}

static void
deleteVertexEdges(ModifyGraphState *mgstate, Datum vid)
{
	EState	   *estate = mgstate->ps.state;
	char		sqlcmd[SQLCMD_BUFLEN];
	Datum		values[SQLCMD_DEL_EDGES_NPARAMS];
	Oid			argTypes[SQLCMD_DEL_EDGES_NPARAMS];
	Oid			labid;
	SPIPlanPtr	plan;
	int			ret;


	snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_DEL_EDGES, get_graph_path());

	values[0] = vid;
	argTypes[0] = GRAPHIDOID;

	labid = get_labname_labid(AG_EDGE, get_graphname_oid(get_graph_path()));

	/* Prepare and execute plan */
	plan = getPreparedplan(labid, SQLCMD_TYPE_DEL_EDGES, sqlcmd,
						   SQLCMD_DEL_EDGES_NPARAMS, argTypes);
	ret = SPI_execp(plan, values, NULL, 0);

	if (ret != SPI_OK_DELETE)
		elog(ERROR, "SPI_execute failed: %s", sqlcmd);

	if (mgstate->canSetTag)
		estate->es_graphwrstats.deleteEdge += SPI_processed;
}

static void
deleteElem(ModifyGraphState *mgstate, Datum id, DelElemKind kind)
{
	EState	   *estate = mgstate->ps.state;
	char	   *relname;
	char		sqlcmd[SQLCMD_BUFLEN];
	Datum		values[SQLCMD_DEL_ELEM_NPARAMS];
	Oid			argTypes[SQLCMD_DEL_ELEM_NPARAMS];
	int			ret;
	SPIPlanPtr	plan;

	relname = get_rel_name(DatumGetGraphidP(id)->oid);

	snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_DEL_ELEM, get_graph_path(), relname);

	values[0] = id;
	argTypes[0] = GRAPHIDOID;

	/* Prepare and execute plan */
	plan = getPreparedplan(DatumGetGraphidP(id)->oid, SQLCMD_TYPE_DEL_ELEM,
						   sqlcmd, SQLCMD_DEL_ELEM_NPARAMS, argTypes);
	ret = SPI_execp(plan, values, NULL, 0);

	if (ret != SPI_OK_DELETE)
		elog(ERROR, "SPI_execute failed: %s", sqlcmd);
	if (SPI_processed > 1)
		elog(ERROR, "SPI_execute: only one or no element per execution must be deleted");

	if (mgstate->canSetTag)
	{
		if (kind == DEL_ELEM_VERTEX)
			estate->es_graphwrstats.deleteVertex += SPI_processed;
		else
			estate->es_graphwrstats.deleteEdge += SPI_processed;
	}
}

static void
deletePath(ModifyGraphState *mgstate, Datum graphpath, bool detach)
{
	Datum		vertices_datum;
	Datum		edges_datum;
	AnyArrayType *vertices;
	AnyArrayType *edges;
	int			nvertices;
	int			nedges;
	ArrayAccessTypeInfo vertexInfo;
	ArrayAccessTypeInfo edgeInfo;
	array_iter	it;
	Datum		value;
	bool		null;
	int			i;

	getGraphpathArrays(graphpath, &vertices_datum, &edges_datum);

	vertices = DatumGetAnyArray(vertices_datum);
	edges = DatumGetAnyArray(edges_datum);

	nvertices = ArrayGetNItems(AARR_NDIM(vertices), AARR_DIMS(vertices));
	nedges = ArrayGetNItems(AARR_NDIM(edges), AARR_DIMS(edges));
	Assert(nvertices == nedges + 1);

	get_typlenbyvalalign(AARR_ELEMTYPE(vertices), &vertexInfo.typlen,
						 &vertexInfo.typbyval, &vertexInfo.typalign);
	get_typlenbyvalalign(AARR_ELEMTYPE(edges), &edgeInfo.typlen,
						 &edgeInfo.typbyval, &edgeInfo.typalign);

	/* delete edges first to avoid vertexHasEdge() */
	array_iter_setup(&it, edges);
	for (i = 0; i < nedges; i++)
	{
		value = array_iter_next(&it, &null, i, edgeInfo.typlen,
								edgeInfo.typbyval, edgeInfo.typalign);
		Assert(!null);

		deleteElem(mgstate, getEdgeIdDatum(value), DEL_ELEM_EDGE);
	}

	array_iter_setup(&it, vertices);
	for (i = 0; i < nvertices; i++)
	{
		value = array_iter_next(&it, &null, i, vertexInfo.typlen,
								vertexInfo.typbyval, vertexInfo.typalign);
		Assert(!null);

		deleteVertex(mgstate, value, detach);
	}
}

static TupleTableSlot *
ExecSetGraph(ModifyGraphState *mgstate, TupleTableSlot *slot)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	ExprContext *econtext = mgstate->ps.ps_ExprContext;
	ListCell   *ls;

	ResetExprContext(econtext);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	econtext->ecxt_scantuple = slot;

	foreach(ls, mgstate->sets)
	{
		GraphSetProp *gsp = lfirst(ls);
		Oid			elemtype;
		Datum		elem_datum;
		Datum		id_datum;
		Datum		path_datum = (Datum) 0;
		Datum		expr_datum;
		bool		isNull;
		ExprDoneCond isDone;

		elemtype = exprType((Node *) gsp->es_elem->expr);
		if (elemtype != VERTEXOID && elemtype != EDGEOID)
			elog(ERROR, "expected node or relationship");

		elem_datum = ExecEvalExpr(gsp->es_elem, econtext, &isNull, &isDone);
		if (isNull)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("updating NULL is not allowed")));
		if (isDone != ExprSingleResult)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("expected single result")));

		if (elemtype == VERTEXOID)
			id_datum = getVertexIdDatum(elem_datum);
		else
			id_datum = getEdgeIdDatum(elem_datum);

		if (gsp->es_path != NULL)
		{
			path_datum = ExecEvalExpr(gsp->es_path, econtext, &isNull,
									  &isDone);
			if (isNull || isDone != ExprSingleResult)
				elog(ERROR, "invalid path");
		}

		expr_datum = ExecEvalExpr(gsp->es_expr, econtext, &isNull, &isDone);
		if (isDone != ExprSingleResult)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("expected single result")));

		if (isNull)
		{
			Assert(gsp->es_path != NULL);

			removeElemProp(mgstate, id_datum, path_datum);
		}
		else
		{
			if (gsp->es_path == NULL)
			{
				if (exprType((Node *) gsp->es_expr->expr) != JSONBOID)
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg("expected jsonb")));

				overwiteElemProp(mgstate, id_datum, expr_datum);
			}
			else
			{
				setElemProp(mgstate, id_datum, path_datum, expr_datum);
			}
		}
	}

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	return (plan->last ? NULL : slot);
}

static void
setElemProp(ModifyGraphState *mgstate, Datum id, Datum path, Datum expr)
{
	EState	   *estate = mgstate->ps.state;
	char	   *relname;
	char		sqlcmd[SQLCMD_BUFLEN];
	Datum		values[SQLCMD_SET_PROP_NPARAMS];
	Oid			argTypes[SQLCMD_SET_PROP_NPARAMS] = {TEXTARRAYOID, JSONBOID,
													 GRAPHIDOID};
	SPIPlanPtr	plan;
	int			ret;

	relname = get_rel_name(DatumGetGraphidP(id)->oid);

	snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_SET_PROP, get_graph_path(), relname);

	values[0] = path;
	values[1] = expr;
	values[2] = id;

	/* Prepare and execute plan */
	plan = getPreparedplan(DatumGetGraphidP(id)->oid, SQLCMD_TYPE_SET_PROP,
						   sqlcmd, SQLCMD_SET_PROP_NPARAMS, argTypes);
	ret = SPI_execp(plan, values, NULL, 0);

	if (ret != SPI_OK_UPDATE)
		elog(ERROR, "SPI_execute failed: %s", sqlcmd);
	if (SPI_processed > 1)
		elog(ERROR, "SPI_execute: only one element per execution must be updated");

	if (mgstate->canSetTag)
		estate->es_graphwrstats.updateProperty += SPI_processed;
}

static void
overwiteElemProp(ModifyGraphState *mgstate, Datum id, Datum prop_map)
{
	EState	   *estate = mgstate->ps.state;
	char	   *relname;
	char		sqlcmd[SQLCMD_BUFLEN];
	Datum		values[SQLCMD_OVERWR_PROP_NPARAMS];
	Oid			argTypes[SQLCMD_OVERWR_PROP_NPARAMS] = {JSONBOID, GRAPHIDOID};
	SPIPlanPtr	plan;
	int			ret;

	relname = get_rel_name(DatumGetGraphidP(id)->oid);

	snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_OVERWR_PROP, get_graph_path(),
			 relname);

	values[0] = prop_map;
	values[1] = id;

	/* Prepare and execute plan */
	plan = getPreparedplan(DatumGetGraphidP(id)->oid, SQLCMD_TYPE_OVERWR_PROP,
						   sqlcmd, SQLCMD_OVERWR_PROP_NPARAMS, argTypes);
	ret = SPI_execp(plan, values, NULL, 0);

	if (ret != SPI_OK_UPDATE)
		elog(ERROR, "SPI_execute failed: %s", sqlcmd);
	if (SPI_processed > 1)
		elog(ERROR, "SPI_execute: only one element per execution must be updated");

	if (mgstate->canSetTag)
		estate->es_graphwrstats.updateProperty += SPI_processed;
}

static void
removeElemProp(ModifyGraphState *mgstate, Datum id, Datum path)
{
	EState	   *estate = mgstate->ps.state;
	char	   *relname;
	char		sqlcmd[SQLCMD_BUFLEN];
	Datum		values[SQLCMD_RM_PROP_NPARAMS];
	Oid			argTypes[SQLCMD_RM_PROP_NPARAMS] = {TEXTARRAYOID, GRAPHIDOID};
	SPIPlanPtr	plan;
	int			ret;

	relname = get_rel_name(DatumGetGraphidP(id)->oid);

	snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_RM_PROP, get_graph_path(), relname);

	values[0] = path;
	values[1] = id;

	/* Prepare and execute plan */
	plan = getPreparedplan(DatumGetGraphidP(id)->oid, SQLCMD_TYPE_RM_PROP,
						   sqlcmd, SQLCMD_RM_PROP_NPARAMS, argTypes);
	ret = SPI_execp(plan, values, NULL, 0);

	if (ret != SPI_OK_UPDATE)
		elog(ERROR, "SPI_execute failed: %s", sqlcmd);
	if (SPI_processed > 1)
		elog(ERROR, "SPI_execute: only one element per execution must be updated");

	if (mgstate->canSetTag)
		estate->es_graphwrstats.updateProperty += SPI_processed;
}

static SPIPlanPtr
getPreparedplan(Oid labelid, SqlcmdType cmdtype, char *sqlcmd,
				int nargs, Oid *argtypes)
{
	MGPlanKey key;
	MGPlan  *plan;

	/*
	 * Construct key and try to find prepared execution plan.
	 */
	key.labelid = labelid;
	key.cmdtype = cmdtype;

	plan = findPreparedPlan(&key);

	/* if there is no plan ... */
	if (plan == NULL)
	{
		SPIPlanPtr	pplan;

		/* Prepare plan for query */
		pplan = SPI_prepare(sqlcmd, nargs, argtypes);
		if (pplan == NULL)
			elog(ERROR, "SPI_prepare failed. %d", SPI_result);

		plan = savePreparedPlan(&key, pplan);
	}

	return plan->plan;
}

/* ----------
 * InitMGPlanHashTables -
 *
 *	Initialize plan hash table for modifyGraphPlan
 * ----------
 */
static void
InitMGPlanHashTables(void)
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(MGPlanKey);
	ctl.entrysize = sizeof(MGPlan);
	MGPlanCache = hash_create("ModifyGraph plan cache",
							  128, &ctl, HASH_ELEM | HASH_BLOBS);
}

/* ----------
 * findPreparedPlan -
 *
 *	Lookup for a query key in our private hash table of prepared
 *	and saved SPI execution plans. Return the plan if found or NULL.
 * ----------
 */
static MGPlan *
findPreparedPlan(MGPlanKey *key)
{
	MGPlan *entry;
	SPIPlanPtr	plan;

	/*
	 * On the first call initialize the hashtable
	 */
	if (!MGPlanCache)
		InitMGPlanHashTables();

	/*
	 * Lookup for the key
	 */
	entry = (MGPlan *) hash_search(MGPlanCache, (void *) key, HASH_FIND, NULL);
	if (entry == NULL)
		return NULL;

	/*
	 * Check whether the plan is still valid.
	 */
	plan = entry->plan;
	if (plan && SPI_plan_is_valid(plan))
		return entry;

	/*
	 * Otherwise we might as well flush the cached plan now, to free a little
	 * memory space before we make a new one.
	 */
	entry->plan = NULL;
	if (plan)
		SPI_freeplan(plan);

	return NULL;
}

/* ----------
 * enterPreparedPlan -
 *
 *	save prepared plan and register in plan hash table.
 * ----------
 */
static MGPlan *
savePreparedPlan(MGPlanKey *key, SPIPlanPtr plan)
{
	MGPlan *entry;
	bool	found;

	Assert(MGPlanCache);

	SPI_keepplan(plan);

	/*
	 * Add the new plan.
	 */
	entry = (MGPlan *) hash_search(MGPlanCache,(void *) key,
								   HASH_ENTER, &found);
	entry->plan = plan;

	return entry;
}
