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

typedef struct ArrayAccessTypeInfo {
	int16		typlen;
	bool		typbyval;
	char		typalign;
} ArrayAccessTypeInfo;

static List *ExecInitGraphPattern(List *pattern, ModifyGraphState *mgstate);
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
static Datum copyTupleAsDatum(EState *estate, HeapTuple tuple, Oid tupType);
static void setSlotValueByName(TupleTableSlot *slot, Datum value, char *name);
static Datum *makeDatumArray(EState *estate, int len);
static TupleTableSlot *ExecDeleteGraph(ModifyGraphState *mgstate,
									   TupleTableSlot *slot);
static void deleteVertex(Datum vertex, bool detach);
static bool vertexHasEdge(Datum vid);
static void deleteVertexEdges(Datum vid);
static void deleteElem(Datum id);
static void deletePath(Datum graphpath, bool detach);

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

	mgstate->done = false;
	mgstate->subplan = ExecInitNode(mgplan->subplan, estate, eflags);

	mgstate->pattern = ExecInitGraphPattern(mgplan->pattern, mgstate);
	mgstate->exprs = (List *) ExecInitExpr((Expr *) mgplan->exprs,
										   (PlanState *) mgstate);

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
		GraphPath  *p = (GraphPath *) lfirst(lp);
		ListCell   *le;

		foreach(le, p->chain)
		{
			Node *elem = (Node *) lfirst(le);

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

static TupleTableSlot *
ExecCreateGraph(ModifyGraphState *mgstate, TupleTableSlot *slot)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	ExprContext *econtext = mgstate->ps.ps_ExprContext;
	ListCell   *lp;

	/* create a pattern, accumulated paths `slot` has */
	foreach(lp, plan->pattern)
	{
		GraphPath *path = (GraphPath *) lfirst(lp);

		ResetExprContext(econtext);

		slot = createPath(mgstate, path, slot);
	}

	return (plan->last ? NULL : slot);
}

/* create a path and accumulate it to the given slot */
static TupleTableSlot *
createPath(ModifyGraphState *mgstate, GraphPath *path, TupleTableSlot *slot)
{
	EState	   *estate = mgstate->ps.state;
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

		vertices = makeDatumArray(estate, (pathlen / 2) + 1);
		edges = makeDatumArray(estate, pathlen / 2);

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
		MemoryContext oldMemoryContext;
		Datum graphpath;

		Assert(nvertices == nedges + 1);
		Assert(pathlen == nvertices + nedges);

		oldMemoryContext = MemoryContextSwitchTo(estate->es_query_cxt);

		graphpath = makeGraphpathDatum(vertices, nvertices, edges, nedges);

		MemoryContextSwitchTo(oldMemoryContext);

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
	char		sqlcmd[SQLCMD_BUFLEN];
	char	   *label;
	Datum		values[SQLCMD_VERTEX_NPARAMS];
	Oid			argTypes[SQLCMD_VERTEX_NPARAMS] = {JSONBOID};
	int			ret;
	TupleDesc	tupDesc;
	HeapTuple	tuple;
	Datum		vertex = (Datum) NULL;

	label = gvertex->label;
	if (label == NULL)
		label = AG_VERTEX;

	snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_CREAT_VERTEX,
			 get_graph_path(), label);

	values[0] = evalPropMap(gvertex->es_prop_map, mgstate->ps.ps_ExprContext,
							slot);

	ret = SPI_execute_with_args(sqlcmd, SQLCMD_VERTEX_NPARAMS, argTypes,
								values, NULL, false, 0);
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
		vertex = copyTupleAsDatum(estate, tuple, VERTEXOID);

	if (gvertex->variable != NULL)
		setSlotValueByName(slot, vertex, gvertex->variable);

	return vertex;
}

static Datum
createEdge(ModifyGraphState *mgstate, GraphEdge *gedge, Graphid *start,
		   Graphid *end, TupleTableSlot *slot, bool inPath)
{
	EState	   *estate = mgstate->ps.state;
	char		sqlcmd[SQLCMD_BUFLEN];
	Datum		values[SQLCMD_EDGE_NPARAMS];
	Oid			argTypes[SQLCMD_EDGE_NPARAMS] = {GRAPHIDOID, GRAPHIDOID,
												 JSONBOID};
	int			ret;
	Datum		edge = (Datum) NULL;

	snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_CREAT_EDGE,
			 get_graph_path(), gedge->label);

	values[0] = GraphidPGetDatum(start);
	values[1] = GraphidPGetDatum(end);
	values[2] = evalPropMap(gedge->es_prop_map, mgstate->ps.ps_ExprContext,
							slot);

	ret = SPI_execute_with_args(sqlcmd, SQLCMD_EDGE_NPARAMS, argTypes, values,
								NULL, false, 0);
	if (ret != SPI_OK_INSERT_RETURNING)
		elog(ERROR, "SPI_execute failed: %s", sqlcmd);
	if (SPI_processed != 1)
		elog(ERROR, "SPI_execute: only one edge per execution must be created");

	if (gedge->variable != NULL || inPath)
	{
		HeapTuple tuple = SPI_tuptable->vals[0];

		edge = copyTupleAsDatum(estate, tuple, EDGEOID);
	}

	if (gedge->variable != NULL)
		setSlotValueByName(slot, edge, gedge->variable);

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
copyTupleAsDatum(EState *estate, HeapTuple tuple, Oid tupType)
{
	TupleDesc		tupDesc;
	MemoryContext	oldMemoryContext;
	Datum			value;

	tupDesc = lookup_rowtype_tupdesc(tupType, -1);

	oldMemoryContext = MemoryContextSwitchTo(estate->es_query_cxt);

	value = heap_copy_tuple_as_datum(tuple, tupDesc);

	MemoryContextSwitchTo(oldMemoryContext);

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
makeDatumArray(EState *estate, int len)
{
	MemoryContext oldMemoryContext;
	Datum *result;

	if (len == 0)
		return NULL;

	oldMemoryContext = MemoryContextSwitchTo(estate->es_query_cxt);

	result = palloc(len * sizeof(Datum));

	MemoryContextSwitchTo(oldMemoryContext);

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
				deleteVertex(datum, plan->detach);
				break;
			case EDGEOID:
				deleteElem(getEdgeIdDatum(datum));
				break;
			case GRAPHPATHOID:
				deletePath(datum, plan->detach);
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
deleteVertex(Datum vertex, bool detach)
{
	Datum		id_datum;
	Graphid	   *id;

	id_datum = getVertexIdDatum(vertex);
	id = DatumGetGraphidP(id_datum);

	if (vertexHasEdge(id_datum))
	{
		if (detach)
		{
			deleteVertexEdges(id_datum);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
					 errmsg("vertex " INT64_FORMAT " in \"%s\" has edge(s)",
							id->lid, get_rel_name(id->oid))));
		}
	}

	deleteElem(id_datum);
}

static bool
vertexHasEdge(Datum vid)
{
	char		sqlcmd[SQLCMD_BUFLEN];
	Datum		values[SQLCMD_DETACH_NPARAMS];
	Oid			argTypes[SQLCMD_DETACH_NPARAMS];
	int			ret;

	snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_DETACH, get_graph_path());

	values[0] = vid;
	argTypes[0] = GRAPHIDOID;

	ret = SPI_execute_with_args(sqlcmd, SQLCMD_DETACH_NPARAMS,
								argTypes, values, NULL, false, 1);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute failed: %s", sqlcmd);

	return (SPI_processed > 0);
}

static void
deleteVertexEdges(Datum vid)
{
	char		sqlcmd[SQLCMD_BUFLEN];
	Datum		values[SQLCMD_DEL_EDGES_NPARAMS];
	Oid			argTypes[SQLCMD_DEL_EDGES_NPARAMS];
	int			ret;

	snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_DEL_EDGES, get_graph_path());

	values[0] = vid;
	argTypes[0] = GRAPHIDOID;

	ret = SPI_execute_with_args(sqlcmd, SQLCMD_DEL_EDGES_NPARAMS,
								argTypes, values, NULL, false, 0);
	if (ret != SPI_OK_DELETE)
		elog(ERROR, "SPI_execute failed: %s", sqlcmd);
}

static void
deleteElem(Datum id)
{
	char	   *relname;
	char		sqlcmd[SQLCMD_BUFLEN];
	Datum		values[SQLCMD_DEL_ELEM_NPARAMS];
	Oid			argTypes[SQLCMD_DEL_ELEM_NPARAMS];
	int			ret;

	relname = get_rel_name(DatumGetGraphidP(id)->oid);

	snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_DEL_ELEM, get_graph_path(), relname);

	values[0] = id;
	argTypes[0] = GRAPHIDOID;

	ret = SPI_execute_with_args(sqlcmd, SQLCMD_DEL_ELEM_NPARAMS, argTypes,
								values, NULL, false, 0);
	if (ret != SPI_OK_DELETE)
		elog(ERROR, "SPI_execute failed: %s", sqlcmd);
	if (SPI_processed > 1)
		elog(ERROR, "SPI_execute: only one or no element per execution must be deleted");
}

static void
deletePath(Datum graphpath, bool detach)
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

		deleteElem(getEdgeIdDatum(value));
	}

	array_iter_setup(&it, vertices);
	for (i = 0; i < nvertices; i++)
	{
		value = array_iter_next(&it, &null, i, vertexInfo.typlen,
								vertexInfo.typbyval, vertexInfo.typalign);
		Assert(!null);

		deleteVertex(value, detach);
	}
}
