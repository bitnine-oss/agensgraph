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
	"INSERT INTO " AG_GRAPH ".%s VALUES (DEFAULT, $1) RETURNING " \
	"(tableoid, " AG_ELEM_LOCAL_ID ")::graphid AS " AG_ELEM_ID ", " \
	AG_ELEM_PROP_MAP
#define SQLCMD_VERTEX_NPARAMS		1

#define SQLCMD_CREAT_EDGE \
	"INSERT INTO " AG_GRAPH ".%s VALUES (DEFAULT, $1, $2, $3) RETURNING " \
	"(tableoid, " AG_ELEM_LOCAL_ID ")::graphid AS " AG_ELEM_ID ", " \
	AG_START_ID ", \"" AG_END_ID "\", " AG_ELEM_PROP_MAP
#define SQLCMD_EDGE_NPARAMS			3

#define SQLCMD_DEL_ELEM \
	"DELETE FROM ONLY " AG_GRAPH ".%s WHERE " AG_ELEM_LOCAL_ID " = $1"
#define SQLCMD_DEL_ELEM_NPARAMS		1

#define SQLCMD_DETACH \
	"SELECT " AG_ELEM_LOCAL_ID " FROM " AG_GRAPH "." AG_EDGE \
	" WHERE " AG_START_ID " = $1 OR \"" AG_END_ID "\" = $1"
#define SQLCMD_DETACH_NPARAMS		1

#define SQLCMD_DEL_EDGES \
	"DELETE FROM " AG_GRAPH "." AG_EDGE \
	" WHERE " AG_START_ID " = $1 OR \"" AG_END_ID "\" = $1"
#define SQLCMD_DEL_EDGES_NPARAMS	1

typedef struct ArrayAccessTypeInfo {
	int16		typlen;
	bool		typbyval;
	char		typalign;
} ArrayAccessTypeInfo;

static TupleTableSlot *ExecCreateGraph(ModifyGraphState *mgstate,
									   TupleTableSlot *slot);
static Datum createVertex(EState *estate, GraphVertex *gvertex, Graphid *vid,
						  TupleTableSlot *slot, bool inPath);
static Datum createEdge(EState *estate, GraphEdge *gedge, Graphid start,
						Graphid end, TupleTableSlot *slot, bool inPath);
static TupleTableSlot *createPath(EState *estate, GraphPath *path,
								  TupleTableSlot *slot);
static Datum findVertex(TupleTableSlot *slot, GraphVertex *node, Graphid *vid);
static AttrNumber findAttrInSlotByName(TupleTableSlot *slot, char *name);
static Datum copyTupleAsDatum(EState *estate, HeapTuple tuple, Oid tupType);
static void setSlotValueByName(TupleTableSlot *slot, Datum value, char *name);
static Datum *makeDatumArray(EState *estate, int len);
static Datum CStringGetJsonbDatum(char *str);
static TupleTableSlot *ExecDeleteGraph(ModifyGraphState *mgstate,
									   TupleTableSlot *slot);
static void deleteVertex(Datum vertex, bool detach);
static bool vertexHasEdge(Datum vid);
static void deleteVertexEdges(Datum vid);
static void deleteElem(char *relname, int64 id);
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

static TupleTableSlot *
ExecCreateGraph(ModifyGraphState *mgstate, TupleTableSlot *slot)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	EState	   *estate = mgstate->ps.state;
	ListCell   *lp;

	/* create a pattern, accumulated paths `slot` has */
	foreach(lp, plan->pattern)
	{
		GraphPath *path = (GraphPath *) lfirst(lp);

		slot = createPath(estate, path, slot);
	}

	return (plan->last ? NULL : slot);
}

/* create a path and accumulate it to the given slot */
static TupleTableSlot *
createPath(EState *estate, GraphPath *path, TupleTableSlot *slot)
{
	bool		out = (path->variable != NULL);
	int			pathlen;
	Datum	   *vertices = NULL;
	Datum	   *edges = NULL;
	int			nvertices;
	int			nedges;
	ListCell   *le;
	Graphid		vid;
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
			Datum		vertex;

			if (gvertex->create)
				vertex = createVertex(estate, gvertex, &vid, slot, out);
			else
				vertex = findVertex(slot, gvertex, &vid);

			if (out)
				vertices[nvertices++] = vertex;

			if (gedge != NULL)
			{
				Datum edge;

				if (gedge->direction == GRAPH_EDGE_DIR_LEFT)
				{
					edge = createEdge(estate, gedge, vid, prevvid, slot, out);
				}
				else
				{
					Assert(gedge->direction == GRAPH_EDGE_DIR_RIGHT);

					edge = createEdge(estate, gedge, prevvid, vid, slot, out);
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
createVertex(EState *estate, GraphVertex *gvertex, Graphid *vid,
			 TupleTableSlot *slot, bool inPath)
{
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

	snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_CREAT_VERTEX, label);

	values[0] = CStringGetJsonbDatum(gvertex->prop_map);

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
		bool isnull;

		Assert(SPI_fnumber(tupDesc, AG_ELEM_ID) == 1);
		*vid = getGraphidStruct(SPI_getbinval(tuple, tupDesc, 1, &isnull));
		Assert(!isnull);
	}

	/* if this vertex is in the result solely or in some paths, */
	if (gvertex->variable != NULL || inPath)
		vertex = copyTupleAsDatum(estate, tuple, VERTEXOID);

	if (gvertex->variable != NULL)
		setSlotValueByName(slot, vertex, gvertex->variable);

	return vertex;
}

static Datum
createEdge(EState *estate, GraphEdge *gedge, Graphid start, Graphid end,
		   TupleTableSlot *slot, bool inPath)
{
	char		sqlcmd[SQLCMD_BUFLEN];
	Datum		values[SQLCMD_EDGE_NPARAMS];
	Oid			argTypes[SQLCMD_EDGE_NPARAMS] = {GRAPHIDOID, GRAPHIDOID,
												 JSONBOID};
	int			ret;
	Datum		edge = (Datum) NULL;

	snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_CREAT_EDGE, gedge->label);

	values[0] = getGraphidDatum(start);
	values[1] = getGraphidDatum(end);
	values[2] = CStringGetJsonbDatum(gedge->prop_map);

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
		*vid = getGraphidStruct(getVertexIdDatum(vertex));

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

static Datum
CStringGetJsonbDatum(char *str)
{
	if (str == NULL)
		return jsonb_build_object_noargs(NULL);
	else
		return DirectFunctionCall1(jsonb_in, CStringGetDatum(str));
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
				{
					Graphid eid;

					eid = getGraphidStruct(getEdgeIdDatum(datum));
					deleteElem(get_rel_name(eid.oid), eid.lid);
				}
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
	Graphid		id;
	char	   *labname;

	id_datum = getVertexIdDatum(vertex);
	id = getGraphidStruct(id_datum);

	if (vertexHasEdge(id_datum))
	{
		if (detach)
		{
			deleteVertexEdges(id_datum);
		}
		else
		{
			labname = get_rel_name(id.oid);

			ereport(ERROR,
					(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
					 errmsg("vertex :%s[%d:" INT64_FORMAT "] has edge(s)",
							labname, id.oid, id.lid)));
		}
	}

	labname = get_rel_name(id.oid);
	deleteElem(labname, id.lid);
}

static bool
vertexHasEdge(Datum vid)
{
	Datum		values[SQLCMD_DETACH_NPARAMS];
	Oid			argTypes[SQLCMD_DETACH_NPARAMS];
	int			ret;

	values[0] = vid;
	argTypes[0] = GRAPHIDOID;

	ret = SPI_execute_with_args(SQLCMD_DETACH, SQLCMD_DETACH_NPARAMS, argTypes,
								values, NULL, false, 1);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute failed: %s", SQLCMD_DETACH);

	return (SPI_processed > 0);
}

static void
deleteVertexEdges(Datum vid)
{
	Datum		values[SQLCMD_DEL_EDGES_NPARAMS];
	Oid			argTypes[SQLCMD_DEL_EDGES_NPARAMS];
	int			ret;

	values[0] = vid;
	argTypes[0] = GRAPHIDOID;

	ret = SPI_execute_with_args(SQLCMD_DEL_EDGES, SQLCMD_DEL_EDGES_NPARAMS,
								argTypes, values, NULL, false, 0);
	if (ret != SPI_OK_DELETE)
		elog(ERROR, "SPI_execute failed: %s", SQLCMD_DEL_EDGES);
}

static void
deleteElem(char *relname, int64 id)
{
	char		sqlcmd[SQLCMD_BUFLEN];
	Datum		values[SQLCMD_DEL_ELEM_NPARAMS];
	Oid			argTypes[SQLCMD_DEL_ELEM_NPARAMS];
	int			ret;

	snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_DEL_ELEM, relname);
	values[0] = Int64GetDatum(id);
	argTypes[0] = INT8OID;

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
		Graphid eid;

		value = array_iter_next(&it, &null, i, edgeInfo.typlen,
								edgeInfo.typbyval, edgeInfo.typalign);
		Assert(!null);

		eid = getGraphidStruct(getEdgeIdDatum(value));
		deleteElem(get_rel_name(eid.oid), eid.lid);
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
