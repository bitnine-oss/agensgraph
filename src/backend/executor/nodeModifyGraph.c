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
#include "parser/parse_relation.h"
#include "utils/arrayaccess.h"
#include "utils/builtins.h"
#include "utils/graph.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/typcache.h"

#define SQLCMD_BUFLEN				(NAMEDATALEN + 192)

/*
 * NOTE: If you add SQLCMD, you should add SqlcmdType for it and use
 *       sqlcmd_cache to run the SQLCMD.
 */
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
	SQLCMD_TYPE_CREAT_VERTEX,
	SQLCMD_TYPE_CREAT_EDGE,
	SQLCMD_TYPE_DEL_ELEM,
	SQLCMD_TYPE_DETACH,
	SQLCMD_TYPE_DEL_EDGES,
	SQLCMD_TYPE_SET_PROP,
	SQLCMD_TYPE_OVERWR_PROP,
	SQLCMD_TYPE_RM_PROP
} SqlcmdType;

typedef struct ArrayAccessTypeInfo
{
	int16		typlen;
	bool		typbyval;
	char		typalign;
} ArrayAccessTypeInfo;

typedef enum DelElemKind
{
	DEL_ELEM_VERTEX,
	DEL_ELEM_EDGE
} DelElemKind;

/* hash key */
typedef struct SqlcmdKey
{
	SqlcmdType	cmdtype;
	uint16		labid;
} SqlcmdKey;

/* hash entry */
typedef struct SqlcmdEntry
{
	SqlcmdKey	key;
	SPIPlanPtr	plan;
} SqlcmdEntry;

static HTAB *sqlcmd_cache = NULL;

static List *ExecInitGraphSets(List *sets, ModifyGraphState *mgstate);
static TupleTableSlot *ExecCreateGraph(ModifyGraphState *mgstate,
									   TupleTableSlot *slot);
static Datum createVertex(ModifyGraphState *mgstate, GraphVertex *gvertex,
						  Graphid *vid, TupleTableSlot *slot, bool inPath);
static Datum createEdge(ModifyGraphState *mgstate, GraphEdge *gedge,
						Graphid start, Graphid end, TupleTableSlot *slot,
						bool inPath);
static TupleTableSlot *createPath(ModifyGraphState *mgstate, GraphPath *path,
								  TupleTableSlot *slot);
static Datum findVertex(TupleTableSlot *slot, GraphVertex *node, Graphid *vid);
static Datum findEdge(TupleTableSlot *slot, GraphEdge *node, Graphid *eid);
static AttrNumber findAttrInSlotByName(TupleTableSlot *slot, char *name);
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

/* caching SPIPlan's (See ri_triggers.c) */
static void InitSqlcmdHashTable(MemoryContext mcxt);
static void EndSqlcmdHashTable(void);
static SPIPlanPtr findPreparedPlan(SqlcmdKey *key);
static SPIPlanPtr prepareSqlcmd(SqlcmdKey *key, char *sqlcmd,
								int nargs, Oid *argtypes);
static void savePreparedPlan(SqlcmdKey *key, SPIPlanPtr plan);

ModifyGraphState *
ExecInitModifyGraph(ModifyGraph *mgplan, EState *estate, int eflags)
{
	ModifyGraphState *mgstate;
	ListCell		*lc;

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

	mgstate->exprs = (List *) ExecInitExpr((Expr *) mgplan->exprs,
										   (PlanState *) mgstate);
	mgstate->sets = ExecInitGraphSets(mgplan->sets, mgstate);

	InitSqlcmdHashTable(estate->es_query_cxt);

	if (mgplan->resultRel)
	{
		int		numResultRelations = list_length(mgplan->resultRel);
		ResultRelInfo *resultRelInfos;
		ResultRelInfo *resultRelInfo;
		ParseState	  *temp_pstate = make_parsestate(NULL);

		resultRelInfos = (ResultRelInfo *)
			palloc(numResultRelations * sizeof(ResultRelInfo));
		resultRelInfo = resultRelInfos;
		foreach(lc, mgplan->resultRel)
		{
			Oid			resultrel = lfirst_oid(lc);
			Relation	relation;
			RangeTblEntry *rte;

			relation = heap_open(resultrel, RowExclusiveLock);

			/* Add this rel to the parsestate's rangetable, for dependencies */
			rte = addRangeTableEntryForRelation(temp_pstate, relation,
												NULL, false, false);
			rte->requiredPerms = ACL_INSERT;
			estate->es_range_table = lappend(estate->es_range_table, rte);

			InitResultRelInfo(resultRelInfo,
							  relation,
							  list_length(estate->es_range_table),
							  estate->es_instrument);

			ExecOpenIndices(resultRelInfo, false);

			resultRelInfo++;
		}

		estate->es_result_relations = resultRelInfos;
		estate->es_num_result_relations = numResultRelations;
		/* es_result_relation_info is NULL except when within ModifyTable */
		estate->es_result_relation_info = NULL;

		free_parsestate(temp_pstate);
	}

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

		DisableGraphDML = false;
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
		DisableGraphDML = true;

		if (slot != NULL)
			return slot;
	}

	mgstate->done = true;

	return NULL;
}

void
ExecEndModifyGraph(ModifyGraphState *mgstate)
{
	if (sqlcmd_cache != NULL)
		EndSqlcmdHashTable();

	ExecFreeExprContext(&mgstate->ps);
	ExecEndNode(mgstate->subplan);
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
	Graphid		prevvid = 0;
	GraphEdge  *gedge = NULL;

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
					edge = createEdge(mgstate, gedge, vid, prevvid, slot, out);
				}
				else
				{
					Assert(gedge->direction == GRAPH_EDGE_DIR_RIGHT);

					edge = createEdge(mgstate, gedge, prevvid, vid, slot, out);
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

static ResultRelInfo *
getResultRelationInfo(ModifyGraphState *mgstate, char *labelname)
{
	EState *estate = mgstate->ps.state;
	Oid		relOid;
	Oid		graphOid;
	int		i;
	ResultRelInfo *resultRel = estate->es_result_relations;

	graphOid = get_graphname_oid(get_graph_path());
	relOid   = get_laboid_relid(get_labname_laboid(labelname, graphOid));

	for (i = 0; i < estate->es_num_result_relations; i++)
	{
		if (RelationGetRelid(resultRel->ri_RelationDesc) == relOid)
			break;

		resultRel++;
	}

	if (i == estate->es_num_result_relations)
	{
		elog(ERROR, "could not find target label: %s", labelname);
	}

	return resultRel;
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
	Datum		values[2];
	bool		isnull[2];
	HeapTuple	tuple;
	Datum		vertex;
	ResultRelInfo *resultRelInfo;
	ResultRelInfo *savedResultRel;
	TupleTableSlot *insertSlot;

	Assert(gvertex->label != NULL && gvertex->variable != NULL);

	/*
	 * Get resultRelationInfo for current label and set to estate.
	 */
	resultRelInfo = getResultRelationInfo(mgstate, gvertex->label);
	savedResultRel = estate->es_result_relation_info;
	estate->es_result_relation_info = resultRelInfo;

	/* Find my vertex slot that be made from result plan. */
	vertex = findVertex(slot, gvertex, vid);

	/* Make Heap Tuple for inserting to the heap. */
	values[0] = GraphidGetDatum(*vid);
	values[1] = getVertexPropDatum(vertex);

	memset(isnull, 0, sizeof(bool)*2);

	insertSlot = MakeTupleTableSlot();
	insertSlot->tts_tupleDescriptor = RelationGetDescr(resultRelInfo->ri_RelationDesc);
	insertSlot->tts_values = values;
	insertSlot->tts_isnull = isnull;
	insertSlot->tts_isempty = false;

	tuple = ExecMaterializeSlot(insertSlot);

	/*
	 * Constraints might reference the tableoid column, so initialize
	 * t_tableOid before evaluating them.
	 */
	tuple->t_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

	/*
	 * Check the constraints of the tuple
	 */
	if (resultRelInfo->ri_RelationDesc->rd_att->constr)
		ExecConstraints(resultRelInfo, insertSlot, estate);

	/*
	 * insert the tuple normally.
	 *
	 * Note: heap_insert returns the tid (location) of the new tuple
	 * in the t_self field.
	 */
	(void)heap_insert(resultRelInfo->ri_RelationDesc, tuple,
					  estate->es_output_cid, 0, NULL);

	/* insert index entries for tuple */
	if (resultRelInfo->ri_NumIndices > 0)
		(void)ExecInsertIndexTuples(insertSlot, &(tuple->t_self),
									estate, false, NULL, NIL);

	if (mgstate->canSetTag)
		estate->es_graphwrstats.insertVertex++;

	estate->es_result_relation_info = savedResultRel;

	return vertex;
}

static Datum
createEdge(ModifyGraphState *mgstate, GraphEdge *gedge, Graphid start,
		   Graphid end, TupleTableSlot *slot, bool inPath)
{
	EState	   *estate = mgstate->ps.state;
	Datum		values[4];
	bool		isnull[4];
	Datum		edge;
	Graphid		id;
	ResultRelInfo *resultRelInfo;
	ResultRelInfo *savedResultRel;
	TupleTableSlot *insertSlot;
	HeapTuple	tuple;

	/*
	 * Get resultRelationInfo for current label and set to estate.
	 */
	resultRelInfo = getResultRelationInfo(mgstate, gedge->label);

	savedResultRel = estate->es_result_relation_info;
	estate->es_result_relation_info = resultRelInfo;

	edge = findEdge(slot, gedge, &id);

	values[0] = UInt64GetDatum(id);
	values[1] = GraphidGetDatum(start);
	values[2] = GraphidGetDatum(end);
	values[3] = getEdgePropDatum(edge);

	memset(isnull, 0, sizeof(bool)*4);

	insertSlot = MakeTupleTableSlot();
	insertSlot->tts_tupleDescriptor = RelationGetDescr(resultRelInfo->ri_RelationDesc);
	insertSlot->tts_values = values;
	insertSlot->tts_isnull = isnull;
	insertSlot->tts_isempty = false;

	tuple = ExecMaterializeSlot(insertSlot);

	/*
	 * Constraints might reference the tableoid column, so initialize
	 * t_tableOid before evaluating them.
	 */
	tuple->t_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
	/*
	 * Check the constraints of the tuple
	 */
	if (resultRelInfo->ri_RelationDesc->rd_att->constr)
		ExecConstraints(resultRelInfo, insertSlot, estate);

	/*
	 * insert the tuple normally.
	 *
	 * Note: heap_insert returns the tid (location) of the new tuple
	 * in the t_self field.
	 */
	(void)heap_insert(resultRelInfo->ri_RelationDesc, tuple,
					  estate->es_output_cid, 0, NULL);

	/* insert index entries for tuple */
	if (resultRelInfo->ri_NumIndices > 0)
		(void)ExecInsertIndexTuples(insertSlot, &(tuple->t_self),
									estate, false, NULL, NIL);

	if (mgstate->canSetTag)
		estate->es_graphwrstats.insertEdge++;

	estate->es_result_relation_info = savedResultRel;

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
		*vid = DatumGetGraphid(getVertexIdDatum(vertex));

	return vertex;
}

static Datum
findEdge(TupleTableSlot *slot, GraphEdge *gedge, Graphid *eid)
{
	AttrNumber	attno;
	Datum		edge;

	attno = findAttrInSlotByName(slot, gedge->variable);

	edge = slot->tts_values[attno - 1];

	if (eid != NULL)
		*eid = DatumGetGraphid(getEdgeIdDatum(edge));

	return edge;
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
	Graphid		id;

	id_datum = getVertexIdDatum(vertex);
	id = DatumGetGraphid(id_datum);

	if (vertexHasEdge(id_datum))
	{
		if (detach)
		{
			deleteVertexEdges(mgstate, id_datum);
		}
		else
		{
			Oid			graphid = get_graphname_oid(get_graph_path());
			Oid			relid = get_labid_relid(graphid, GraphidGetLabid(id));

			ereport(ERROR,
					(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
					 errmsg("vertex " INT64_FORMAT " in \"%s\" has edge(s)",
							GraphidGetLocid(id), get_rel_name(relid))));
		}
	}

	deleteElem(mgstate, id_datum, DEL_ELEM_VERTEX);
}

static bool
vertexHasEdge(Datum vid)
{
	uint16		labid;
	Datum		values[SQLCMD_DETACH_NPARAMS];
	Oid			argTypes[SQLCMD_DETACH_NPARAMS] = {GRAPHIDOID};
	SqlcmdKey	key;
	SPIPlanPtr	plan;
	int			ret;

	labid = get_labname_labid(AG_EDGE, get_graphname_oid(get_graph_path()));

	key.cmdtype = SQLCMD_TYPE_DETACH;
	key.labid = labid;
	plan = findPreparedPlan(&key);
	if (plan == NULL)
	{
		char sqlcmd[SQLCMD_BUFLEN];

		snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_DETACH, get_graph_path());

		plan = prepareSqlcmd(&key, sqlcmd, SQLCMD_DETACH_NPARAMS, argTypes);
	}

	values[0] = vid;

	ret = SPI_execp(plan, values, NULL, 0);
	if (ret != SPI_OK_SELECT)
	{
		Graphid id = DatumGetGraphid(vid);

		elog(ERROR, "DETACH (%hu." INT64_FORMAT "): SPI_execp returned %d",
			 GraphidGetLabid(id), GraphidGetLocid(id), ret);
	}

	return (SPI_processed > 0);
}

static void
deleteVertexEdges(ModifyGraphState *mgstate, Datum vid)
{
	EState	   *estate = mgstate->ps.state;
	uint16		labid;
	Datum		values[SQLCMD_DEL_EDGES_NPARAMS];
	Oid			argTypes[SQLCMD_DEL_EDGES_NPARAMS] = {GRAPHIDOID};
	SqlcmdKey	key;
	SPIPlanPtr	plan;
	int			ret;

	labid = get_labname_labid(AG_EDGE, get_graphname_oid(get_graph_path()));

	key.cmdtype = SQLCMD_TYPE_DEL_EDGES;
	key.labid = labid;
	plan = findPreparedPlan(&key);
	if (plan == NULL)
	{
		char sqlcmd[SQLCMD_BUFLEN];

		snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_DEL_EDGES, get_graph_path());

		plan = prepareSqlcmd(&key, sqlcmd, SQLCMD_DEL_EDGES_NPARAMS, argTypes);
	}

	values[0] = vid;

	ret = SPI_execp(plan, values, NULL, 0);
	if (ret != SPI_OK_DELETE)
	{
		Graphid id = DatumGetGraphid(vid);

		elog(ERROR, "DEL_EDGES (%hu." INT64_FORMAT "): SPI_execp returned %d",
			 GraphidGetLabid(id), GraphidGetLocid(id), ret);
	}

	if (mgstate->canSetTag)
		estate->es_graphwrstats.deleteEdge += SPI_processed;
}

static void
deleteElem(ModifyGraphState *mgstate, Datum id, DelElemKind kind)
{
	EState	   *estate = mgstate->ps.state;
	Graphid		id_val;
	uint16		labid;
	Datum		values[SQLCMD_DEL_ELEM_NPARAMS];
	Oid			argTypes[SQLCMD_DEL_ELEM_NPARAMS] = {GRAPHIDOID};
	SqlcmdKey	key;
	SPIPlanPtr	plan;
	int			ret;

	id_val = DatumGetGraphid(id);
	labid = GraphidGetLabid(id_val);

	key.cmdtype = SQLCMD_TYPE_DEL_ELEM;
	key.labid = labid;
	plan = findPreparedPlan(&key);
	if (plan == NULL)
	{
		char		sqlcmd[SQLCMD_BUFLEN];
		Oid			graphid = get_graphname_oid(get_graph_path());
		char	   *relname = get_rel_name(get_labid_relid(graphid, labid));

		snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_DEL_ELEM,
				 get_graph_path(), relname);

		plan = prepareSqlcmd(&key, sqlcmd, SQLCMD_DEL_ELEM_NPARAMS, argTypes);
	}

	values[0] = id;

	ret = SPI_execp(plan, values, NULL, 0);
	if (ret != SPI_OK_DELETE)
		elog(ERROR, "DEL_EDGES (%hu." INT64_FORMAT "): SPI_execp returned %d",
			 labid, GraphidGetLocid(id_val), ret);
	if (SPI_processed > 1)
		elog(ERROR, "DEL_EDGES (%hu." INT64_FORMAT "): only one or no element per execution must be deleted", labid, GraphidGetLocid(id_val));

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
	Graphid		id_val;
	uint16		labid;
	Datum		values[SQLCMD_SET_PROP_NPARAMS];
	Oid			argTypes[SQLCMD_SET_PROP_NPARAMS] = {TEXTARRAYOID, JSONBOID,
													 GRAPHIDOID};
	SqlcmdKey	key;
	SPIPlanPtr	plan;
	int			ret;

	id_val = DatumGetGraphid(id);
	labid = GraphidGetLabid(id_val);

	key.cmdtype = SQLCMD_TYPE_SET_PROP;
	key.labid = labid;
	plan = findPreparedPlan(&key);
	if (plan == NULL)
	{
		char		sqlcmd[SQLCMD_BUFLEN];
		Oid			graphid = get_graphname_oid(get_graph_path());
		char	   *relname = get_rel_name(get_labid_relid(graphid, labid));

		snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_SET_PROP,
				 get_graph_path(), relname);

		plan = prepareSqlcmd(&key, sqlcmd, SQLCMD_SET_PROP_NPARAMS, argTypes);
	}

	values[0] = path;
	values[1] = expr;
	values[2] = id;

	ret = SPI_execp(plan, values, NULL, 0);
	if (ret != SPI_OK_UPDATE)
		elog(ERROR, "SET_PROP (%hu." INT64_FORMAT "): SPI_execp returned %d",
			 labid, GraphidGetLocid(id_val), ret);
	if (SPI_processed > 1)
		elog(ERROR, "SET_PROP (%hu." INT64_FORMAT "): only one element per execution must be updated", labid, GraphidGetLocid(id_val));

	if (mgstate->canSetTag)
		estate->es_graphwrstats.updateProperty += SPI_processed;
}

static void
overwiteElemProp(ModifyGraphState *mgstate, Datum id, Datum prop_map)
{
	EState	   *estate = mgstate->ps.state;
	Graphid		id_val;
	uint16		labid;
	Datum		values[SQLCMD_OVERWR_PROP_NPARAMS];
	Oid			argTypes[SQLCMD_OVERWR_PROP_NPARAMS] = {JSONBOID, GRAPHIDOID};
	SqlcmdKey	key;
	SPIPlanPtr	plan;
	int			ret;

	id_val = DatumGetGraphid(id);
	labid = GraphidGetLabid(id_val);

	key.cmdtype = SQLCMD_TYPE_OVERWR_PROP;
	key.labid = labid;
	plan = findPreparedPlan(&key);
	if (plan == NULL)
	{
		char		sqlcmd[SQLCMD_BUFLEN];
		Oid			graphid = get_graphname_oid(get_graph_path());
		char	   *relname = get_rel_name(get_labid_relid(graphid, labid));

		snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_OVERWR_PROP,
				 get_graph_path(), relname);

		plan = prepareSqlcmd(&key, sqlcmd,
							 SQLCMD_OVERWR_PROP_NPARAMS, argTypes);
	}

	values[0] = prop_map;
	values[1] = id;

	ret = SPI_execp(plan, values, NULL, 0);
	if (ret != SPI_OK_UPDATE)
		elog(ERROR, "OVERWR_PROP (%hu." INT64_FORMAT "): SPI_execp returned %d",
			 labid, GraphidGetLocid(id_val), ret);
	if (SPI_processed > 1)
		elog(ERROR, "OVERWR_PROP (%hu." INT64_FORMAT "): only one element per execution must be updated", labid, GraphidGetLocid(id_val));

	if (mgstate->canSetTag)
		estate->es_graphwrstats.updateProperty += SPI_processed;
}

static void
removeElemProp(ModifyGraphState *mgstate, Datum id, Datum path)
{
	EState	   *estate = mgstate->ps.state;
	Graphid		id_val;
	uint16		labid;
	Datum		values[SQLCMD_RM_PROP_NPARAMS];
	Oid			argTypes[SQLCMD_RM_PROP_NPARAMS] = {TEXTARRAYOID, GRAPHIDOID};
	SqlcmdKey	key;
	SPIPlanPtr	plan;
	int			ret;

	id_val = DatumGetGraphid(id);
	labid = GraphidGetLabid(id_val);

	key.cmdtype = SQLCMD_TYPE_RM_PROP;
	key.labid = labid;
	plan = findPreparedPlan(&key);
	if (plan == NULL)
	{
		char		sqlcmd[SQLCMD_BUFLEN];
		Oid			graphid = get_graphname_oid(get_graph_path());
		char	   *relname = get_rel_name(get_labid_relid(graphid, labid));

		snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_RM_PROP,
				 get_graph_path(), relname);

		plan = prepareSqlcmd(&key, sqlcmd, SQLCMD_RM_PROP_NPARAMS, argTypes);
	}

	values[0] = path;
	values[1] = id;

	ret = SPI_execp(plan, values, NULL, 0);
	if (ret != SPI_OK_UPDATE)
		elog(ERROR, "RM_PROP (%hu." INT64_FORMAT "): SPI_execp returned %d",
			 labid, GraphidGetLocid(id_val), ret);
	if (SPI_processed > 1)
		elog(ERROR, "RM_PROP (%hu." INT64_FORMAT "): only one element per execution must be updated", labid, GraphidGetLocid(id_val));

	if (mgstate->canSetTag)
		estate->es_graphwrstats.updateProperty += SPI_processed;
}

/* 
 * NOTE: What happens if there is a multiple execution of ModifyGraph?
 */
static void
InitSqlcmdHashTable(MemoryContext mcxt)
{
	HASHCTL ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(SqlcmdKey);
	ctl.entrysize = sizeof(SqlcmdEntry);
	ctl.hcxt = mcxt;

	sqlcmd_cache = hash_create("ModifyGraph SPIPlan cache", 128, &ctl,
							   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * NOTE: If an error occurs during the execution of ModifyGraph,
 *       there is no way to remove saved plans.
 */
static void
EndSqlcmdHashTable(void)
{
	HASH_SEQ_STATUS seqStatus;
	SqlcmdEntry *entry;

	hash_seq_init(&seqStatus, sqlcmd_cache);
	while ((entry = hash_seq_search(&seqStatus)) != NULL)
		SPI_freeplan(entry->plan);

	hash_destroy(sqlcmd_cache);

	sqlcmd_cache = NULL;
}

static SPIPlanPtr
findPreparedPlan(SqlcmdKey *key)
{
	SqlcmdEntry *entry;
	SPIPlanPtr plan;

	Assert(sqlcmd_cache != NULL);

	entry = hash_search(sqlcmd_cache, (void *) key, HASH_FIND, NULL);
	if (entry == NULL)
		return NULL;

	plan = entry->plan;
	if (plan && SPI_plan_is_valid(plan))
		return plan;

	entry->plan = NULL;
	if (plan != NULL)
		SPI_freeplan(plan);

	return NULL;
}

static SPIPlanPtr
prepareSqlcmd(SqlcmdKey *key, char *sqlcmd, int nargs, Oid *argtypes)
{
	SPIPlanPtr plan;

	plan = SPI_prepare(sqlcmd, nargs, argtypes);
	if (plan == NULL)
		elog(ERROR, "failed to SPI_prepare(): %d", SPI_result);

	savePreparedPlan(key, plan);

	return plan;
}

static void
savePreparedPlan(SqlcmdKey *key, SPIPlanPtr plan)
{
	SqlcmdEntry *entry;
	bool		found;

	Assert(sqlcmd_cache != NULL);

	if (SPI_keepplan(plan))
		elog(ERROR, "savePreparedPlan: SPI_keepplan failed");

	entry = hash_search(sqlcmd_cache, (void *) key, HASH_ENTER, &found);
	Assert(!found || entry->plan == NULL);
	entry->plan = plan;
}
