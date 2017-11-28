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
#include "access/xact.h"
#include "catalog/ag_graph_fn.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/nodeModifyGraph.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/graphnodes.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_relation.h"
#include "utils/arrayaccess.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/graph.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/tuplestore.h"
#include "utils/typcache.h"

#define DATUM_NULL	PointerGetDatum(NULL)

/* hash entry */
typedef struct ModifiedElemEntry
{
	Graphid	key;
	Datum	elem_datum;
	Oid		type;
} ModifiedElemEntry;

static TupleTableSlot *ExecModifyGraph(PlanState *pstate);
static void initGraphWRStats(ModifyGraphState *mgstate, GraphWriteOp op);
static List *ExecInitGraphPattern(List *pattern, ModifyGraphState *mgstate);
static List *ExecInitGraphSets(List *sets, ModifyGraphState *mgstate);

/* CREATE */
static TupleTableSlot *ExecCreateGraph(ModifyGraphState *mgstate,
									   TupleTableSlot *slot);
static TupleTableSlot *createPath(ModifyGraphState *mgstate, GraphPath *path,
								  TupleTableSlot *slot);
static Datum createVertex(ModifyGraphState *mgstate, GraphVertex *gvertex,
						  Graphid *vid, TupleTableSlot *slot, bool inPath);
static Datum createEdge(ModifyGraphState *mgstate, GraphEdge *gedge,
						Graphid start, Graphid end, TupleTableSlot *slot,
						bool inPath);


/* DELETE */
static TupleTableSlot *ExecDeleteGraph(ModifyGraphState *mgstate,
									   TupleTableSlot *slot);
static void deleteElem(ModifyGraphState *mgstate, Datum elem,
					   Datum id, Oid type);

/* SET */
static TupleTableSlot *ExecSetGraph(ModifyGraphState *mgstate, GSPKind kind,
									TupleTableSlot *slot);
static void findAndReflectNewestValue(ModifyGraphState *mgstate,
									  ExprContext *econtext,
									  GraphSetProp *gsp, Datum gid);
static ItemPointer updateElemProp(ModifyGraphState *mgstate, Oid elemtype,
								  Datum gid, Datum elem_datum);
static Datum makeModifiedElem(ExprContext *econtext, Datum elem, Oid elemtype,
							  Datum id, Datum prop_map, Datum tid);

/* MERGE */
static TupleTableSlot *ExecMergeGraph(ModifyGraphState *mgstate,
									  TupleTableSlot *slot);
static bool isMatchedMergePattern(PlanState *planstate);
static TupleTableSlot *createMergePath(ModifyGraphState *mgstate,
									   GraphPath *path, TupleTableSlot *slot);
static Datum createMergeVertex(ModifyGraphState *mgstate,
							   GraphVertex *gvertex,
							   Graphid *vid, TupleTableSlot *slot);
static Datum createMergeEdge(ModifyGraphState *mgstate, GraphEdge *gedge,
							 Graphid start, Graphid end, TupleTableSlot *slot);
static TupleTableSlot *copyVirtualTupleTableSlot(TupleTableSlot *dstslot,
												 TupleTableSlot *srcslot);

/* eager */
static void enterSetPropTable(ModifyGraphState *mgstate, Oid type, Datum gid,
							  Datum newelem);
static void enterDelPropTable(ModifyGraphState *mgstate, Datum elem, Oid type);
static void getElemListInPath(Datum graphpath, List **vtxlist, List **edgelist);
static Datum getVertexFinalPropMap(ModifyGraphState *mgstate,
								   Datum origin, Graphid gid);
static Datum getEdgeFinalPropMap(ModifyGraphState *mgstate,
								 Datum origin, Graphid gid);
static Datum getPathFinalPropMap(ModifyGraphState *node, Datum origin);
static void reflectModifiedProp(ModifyGraphState *mgstate);

/* COMMON */
static ResultRelInfo *getResultRelInfo(ModifyGraphState *mgstate, Oid relid);
static Datum findVertex(TupleTableSlot *slot, GraphVertex *node, Graphid *vid);
static Datum findEdge(TupleTableSlot *slot, GraphEdge *node, Graphid *eid);
static AttrNumber findAttrInSlotByName(TupleTableSlot *slot, char *name);
static void setSlotValueByName(TupleTableSlot *slot, Datum value, char *name);
static void setSlotValueByAttnum(TupleTableSlot *slot, Datum value, int attnum);
static Datum *makeDatumArray(ExprContext *econtext, int len);

ModifyGraphState *
ExecInitModifyGraph(ModifyGraph *mgplan, EState *estate, int eflags)
{
	ModifyGraphState *mgstate;
	CommandId		  svCid;

	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	mgstate = makeNode(ModifyGraphState);
	mgstate->ps.plan = (Plan *) mgplan;
	mgstate->ps.state = estate;
	mgstate->ps.ExecProcNode = ExecModifyGraph;

	/* Tuple desc for result is the same as the subplan. */
	ExecInitResultTupleSlot(estate, &mgstate->ps);
	ExecAssignResultType(&mgstate->ps,
						 ExecTypeFromTL(mgplan->subplan->targetlist, false));

	ExecAssignExprContext(estate, &mgstate->ps);

	mgstate->canSetTag = mgplan->canSetTag;
	mgstate->done = false;
	mgstate->child_done = false;
	mgstate->eagerness = mgplan->eagerness;
	mgstate->modify_cid = GetCurrentCommandId(false) +
									(mgplan->modifyno * MODIFY_CID_MAX);

	/*
	 * Pass the lower limit of the cid of the current clause
	 * to the default cid of the previous clause.
	 */
	svCid = estate->es_snapshot->curcid;
	estate->es_snapshot->curcid = mgstate->modify_cid;

	mgstate->subplan = ExecInitNode(mgplan->subplan, estate, eflags);
	AssertArg(mgplan->operation != GWROP_MERGE ||
			  IsA(mgstate->subplan, NestLoopState));

	estate->es_snapshot->curcid = svCid;

	mgstate->elemTupleSlot = ExecInitExtraTupleSlot(estate);

	mgstate->graphid = get_graph_path_oid();
	mgstate->graphname = get_graph_path(false);
	mgstate->edgeid = get_labname_labid(AG_EDGE, mgstate->graphid);
	mgstate->numOldRtable = list_length(estate->es_range_table);

	mgstate->pattern = ExecInitGraphPattern(mgplan->pattern, mgstate);

	if (mgplan->targets != NIL)
	{
		int			numResultRelInfo = list_length(mgplan->targets);
		ResultRelInfo *resultRelInfos;
		ParseState *pstate;
		ResultRelInfo *resultRelInfo;
		ListCell   *lt;

		resultRelInfos = palloc(numResultRelInfo * sizeof(*resultRelInfos));

		pstate = make_parsestate(NULL);
		resultRelInfo = resultRelInfos;
		foreach(lt, mgplan->targets)
		{
			Oid			relid = lfirst_oid(lt);
			Relation	relation;
			RangeTblEntry *rte;

			relation = heap_open(relid, RowExclusiveLock);

			rte = addRangeTableEntryForRelation(pstate, relation,
												NULL, false, false);
			rte->requiredPerms = ACL_INSERT;
			estate->es_range_table = lappend(estate->es_range_table, rte);

			InitResultRelInfo(resultRelInfo,
							  relation,
							  list_length(estate->es_range_table),
							  NULL,
							  estate->es_instrument);

			ExecOpenIndices(resultRelInfo, false);

			resultRelInfo++;
		}

		mgstate->resultRelations = resultRelInfos;
		mgstate->numResultRelations = numResultRelInfo;

		/* es_result_relation_info is NULL except ModifyTable case */
		estate->es_result_relation_info = NULL;

		free_parsestate(pstate);
	}


	mgstate->exprs = ExecInitExprList(mgplan->exprs, (PlanState *) mgstate);
	mgstate->sets = ExecInitGraphSets(mgplan->sets, mgstate);

	initGraphWRStats(mgstate, mgplan->operation);

	if (mgstate->sets != NIL || mgstate->exprs != NIL)
	{
		HASHCTL ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Graphid);
		ctl.entrysize = sizeof(ModifiedElemEntry);
		ctl.hcxt = CurrentMemoryContext;

		mgstate->propTable =
					hash_create("modified object table", 128, &ctl,
								HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	else
	{
		mgstate->propTable = NULL;
	}

	mgstate->tuplestorestate = tuplestore_begin_heap(false, false, eager_mem);

	return mgstate;
}

static TupleTableSlot *
ExecModifyGraph(PlanState *pstate)
{
	ModifyGraphState *mgstate = castNode(ModifyGraphState, pstate);
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	EState	   *estate = mgstate->ps.state;

	if (mgstate->done)
		return NULL;

	if (!mgstate->child_done)
	{
		for (;;)
		{
			TupleTableSlot *slot;
			CommandId		svCid;

			/* ExecInsertIndexTuples() uses per-tuple context. Reset it here. */
			ResetPerTupleExprContext(estate);

			/* pass LOWER_BOUND_CID to subplan */
			svCid = estate->es_snapshot->curcid;
			estate->es_snapshot->curcid =
							mgstate->modify_cid + MODIFY_CID_LOWER_BOUND;

			slot = ExecProcNode(mgstate->subplan);
			estate->es_snapshot->curcid = svCid;

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
					{
						ExprContext *econtext = mgstate->ps.ps_ExprContext;

						ResetExprContext(econtext);
						econtext->ecxt_scantuple = slot;

						slot = ExecSetGraph(mgstate, GSP_NORMAL, slot);
					}
					break;
				case GWROP_MERGE:
					slot = ExecMergeGraph(mgstate, slot);
					break;
				default:
					elog(ERROR, "unknown operation");
					break;
			}

			if (mgstate->eagerness)
			{
				Assert(slot != NULL);

				tuplestore_puttupleslot(mgstate->tuplestorestate, slot);
			}
			else if (slot != NULL)
			{
				return slot;
			}
			else
			{
				Assert(plan->last == true);
			}
		}

		mgstate->child_done = true;

		if (mgstate->propTable != NULL &&
			hash_get_num_entries(mgstate->propTable) > 0)
			reflectModifiedProp(mgstate);
	}

	if (mgstate->eagerness)
	{
		TupleTableSlot *result;
		int			natts;
		int			i;

		/* don't care about scan direction */
		result = mgstate->ps.ps_ResultTupleSlot;
		tuplestore_gettupleslot(mgstate->tuplestorestate, true, false, result);

		if (TupIsNull(result))
			return result;

		slot_getallattrs(result);

		if (mgstate->propTable == NULL ||
			hash_get_num_entries(mgstate->propTable) < 1)
			return result;

		natts = result->tts_tupleDescriptor->natts;
		for (i = 0; i < natts; i++)
		{
			Oid			type;
			Graphid		gid;
			Datum		elem;

			if (result->tts_isnull[i])
				continue;

			type = result->tts_tupleDescriptor->attrs[i]->atttypid;
			if (type == VERTEXOID)
			{
				gid = getVertexIdDatum(result->tts_values[i]);
				elem = getVertexFinalPropMap(mgstate,
											 result->tts_values[i],
											 gid);
			}
			else if (type == EDGEOID)
			{
				gid = getEdgeIdDatum(result->tts_values[i]);
				elem = getEdgeFinalPropMap(mgstate,
										   result->tts_values[i],
										   gid);
			}
			else if (type == GRAPHPATHOID)
			{
				elem = getPathFinalPropMap(mgstate, result->tts_values[i]);
			}
			else if (type == EDGEARRAYOID && plan->operation == GWROP_DELETE)
			{
				/*
				 * The edges of the vertices are used only for removal,
				 * not for result output.
				 */
				continue;
			}
			else
			{
				elog(ERROR, "Invalid graph element type %d.", type);
			}

			setSlotValueByAttnum(result, elem, i + 1);
		}

		return result;
	}

	mgstate->done = true;

	return NULL;
}

void
ExecEndModifyGraph(ModifyGraphState *mgstate)
{
	EState		   *estate = mgstate->ps.state;
	ResultRelInfo  *resultRelInfo;
	int			i;

	if (mgstate->tuplestorestate != NULL)
		tuplestore_end(mgstate->tuplestorestate);
	mgstate->tuplestorestate = NULL;

	if (mgstate->propTable != NULL)
		hash_destroy(mgstate->propTable);

	resultRelInfo = mgstate->resultRelations;
	for (i = mgstate->numResultRelations; i > 0; i--)
	{
		ExecCloseIndices(resultRelInfo);
		heap_close(resultRelInfo->ri_RelationDesc, NoLock);

		resultRelInfo++;
	}

	/*
	 * PlannedStmt can be used as a cached plan,
	 * so remove the rtables added to this run.
	 */
	list_truncate(estate->es_range_table, mgstate->numOldRtable);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(mgstate->ps.ps_ResultTupleSlot);

	ExecEndNode(mgstate->subplan);
	ExecFreeExprContext(&mgstate->ps);
}

static void
initGraphWRStats(ModifyGraphState *mgstate, GraphWriteOp op)
{
	EState *estate = mgstate->ps.state;

	if (mgstate->pattern != NIL)
	{
		Assert(op == GWROP_CREATE || op == GWROP_MERGE);

		estate->es_graphwrstats.insertVertex = 0;
		estate->es_graphwrstats.insertEdge = 0;
	}
	if (mgstate->exprs != NIL)
	{
		Assert(op == GWROP_DELETE);

		estate->es_graphwrstats.deleteVertex = 0;
		estate->es_graphwrstats.deleteEdge = 0;
	}
	if (mgstate->sets != NIL)
	{
		Assert(op == GWROP_SET || op == GWROP_MERGE);

		estate->es_graphwrstats.updateProperty = 0;
	}
}

static List *
ExecInitGraphPattern(List *pattern, ModifyGraphState *mgstate)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	GraphPath  *gpath;
	ListCell   *le;

	if (plan->operation != GWROP_MERGE)
		return pattern;

	AssertArg(list_length(pattern) == 1);

	gpath = linitial(pattern);

	foreach(le, gpath->chain)
	{
		Node *elem = lfirst(le);

		if (IsA(elem, GraphVertex))
		{
			GraphVertex *gvertex = (GraphVertex *) elem;

			gvertex->es_expr = ExecInitExpr((Expr *) gvertex->expr,
											(PlanState *) mgstate);
		}
		else
		{
			GraphEdge *gedge = (GraphEdge *) elem;

			Assert(IsA(elem, GraphEdge));

			gedge->es_expr = ExecInitExpr((Expr *) gedge->expr,
										  (PlanState *) mgstate);
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

	ResetExprContext(econtext);

	/* create a pattern, accumulated paths `slot` has */
	foreach(lp, plan->pattern)
	{
		GraphPath *path = (GraphPath *) lfirst(lp);
		MemoryContext oldmctx;

		oldmctx = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

		slot = createPath(mgstate, path, slot);

		MemoryContextSwitchTo(oldmctx);
	}

	return (plan->last ? NULL : slot);
}

static List *
ExecInitGraphSets(List *sets, ModifyGraphState *mgstate)
{
	ListCell *ls;

	foreach(ls, sets)
	{
		GraphSetProp *gsp = lfirst(ls);

		gsp->es_elem = ExecInitExpr((Expr *) gsp->elem, (PlanState *) mgstate);
		gsp->es_expr = ExecInitExpr((Expr *) gsp->expr, (PlanState *) mgstate);
	}

	return sets;
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

		if (IsA(elem, GraphVertex))
		{
			GraphVertex *gvertex = (GraphVertex *) elem;
			Graphid		vid;
			Datum		vertex;

			if (gvertex->create)
				vertex = createVertex(mgstate, gvertex, &vid, slot, out);
			else
				vertex = findVertex(slot, gvertex, &vid);

			Assert(vertex != DATUM_NULL);

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
			Assert(IsA(elem, GraphEdge));

			gedge = (GraphEdge *) elem;
		}
	}

	/* make a graphpath and set it to the slot */
	if (out)
	{
		Datum graphpath;

		Assert(nvertices == nedges + 1);
		Assert(pathlen == nvertices + nedges);

		graphpath = makeGraphpathDatum(vertices, nvertices, edges, nedges);

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
	TupleTableSlot *elemTupleSlot = mgstate->elemTupleSlot;
	ResultRelInfo *resultRelInfo;
	ResultRelInfo *savedResultRelInfo;
	Datum		vertex;
	Datum		vertexProp;
	HeapTuple	tuple;

	resultRelInfo = getResultRelInfo(mgstate, gvertex->relid);
	savedResultRelInfo = estate->es_result_relation_info;
	estate->es_result_relation_info = resultRelInfo;

	vertex = findVertex(slot, gvertex, vid);

	vertexProp = getVertexPropDatum(vertex);
	if (!JB_ROOT_IS_OBJECT(DatumGetJsonb(vertexProp)))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("jsonb object is expected for property map")));

	ExecClearTuple(elemTupleSlot);

	ExecSetSlotDescriptor(elemTupleSlot,
						  RelationGetDescr(resultRelInfo->ri_RelationDesc));
	elemTupleSlot->tts_values[0] = GraphidGetDatum(*vid);
	elemTupleSlot->tts_values[1] = vertexProp;
	MemSet(elemTupleSlot->tts_isnull, false,
		   elemTupleSlot->tts_tupleDescriptor->natts * sizeof(bool));
	ExecStoreVirtualTuple(elemTupleSlot);

	tuple = ExecMaterializeSlot(elemTupleSlot);

	/*
	 * Constraints might reference the tableoid column, so initialize
	 * t_tableOid before evaluating them.
	 */
	tuple->t_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

	/*
	 * Check the constraints of the tuple
	 */
	if (resultRelInfo->ri_RelationDesc->rd_att->constr != NULL)
		ExecConstraints(resultRelInfo, elemTupleSlot, estate);

	/*
	 * insert the tuple normally
	 *
	 * NOTE: heap_insert() returns the cid of the new tuple in the t_self.
	 */
	heap_insert(resultRelInfo->ri_RelationDesc, tuple,
				mgstate->modify_cid + MODIFY_CID_OUTPUT,
				0, NULL);

	/* insert index entries for the tuple */
	if (resultRelInfo->ri_NumIndices > 0)
		ExecInsertIndexTuples(elemTupleSlot, &(tuple->t_self), estate, false,
							  NULL, NIL);

	vertex = makeGraphVertexDatum(elemTupleSlot->tts_values[0],
								  elemTupleSlot->tts_values[1],
								  PointerGetDatum(&tuple->t_self));

	if (gvertex->resno > 0)
		setSlotValueByAttnum(slot, vertex, gvertex->resno);

	if (mgstate->canSetTag)
	{
		Assert(estate->es_graphwrstats.insertVertex != UINT_MAX);

		estate->es_graphwrstats.insertVertex++;
	}

	estate->es_result_relation_info = savedResultRelInfo;

	return vertex;
}

static Datum
createEdge(ModifyGraphState *mgstate, GraphEdge *gedge, Graphid start,
		   Graphid end, TupleTableSlot *slot, bool inPath)
{
	EState	   *estate = mgstate->ps.state;
	TupleTableSlot *elemTupleSlot = mgstate->elemTupleSlot;
	ResultRelInfo *resultRelInfo;
	ResultRelInfo *savedResultRelInfo;
	Graphid		id = 0;
	Datum		edge;
	Datum		edgeProp;
	HeapTuple	tuple;

	resultRelInfo = getResultRelInfo(mgstate, gedge->relid);
	savedResultRelInfo = estate->es_result_relation_info;
	estate->es_result_relation_info = resultRelInfo;

	edge = findEdge(slot, gedge, &id);
	Assert(edge != DATUM_NULL);

	edgeProp = getEdgePropDatum(edge);
	if (!JB_ROOT_IS_OBJECT(DatumGetJsonb(edgeProp)))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("jsonb object is expected for property map")));

	ExecClearTuple(elemTupleSlot);

	ExecSetSlotDescriptor(elemTupleSlot,
						  RelationGetDescr(resultRelInfo->ri_RelationDesc));
	elemTupleSlot->tts_values[0] = GraphidGetDatum(id);
	elemTupleSlot->tts_values[1] = GraphidGetDatum(start);
	elemTupleSlot->tts_values[2] = GraphidGetDatum(end);
	elemTupleSlot->tts_values[3] = edgeProp;
	MemSet(elemTupleSlot->tts_isnull, false,
		   elemTupleSlot->tts_tupleDescriptor->natts * sizeof(bool));
	ExecStoreVirtualTuple(elemTupleSlot);

	tuple = ExecMaterializeSlot(elemTupleSlot);

	tuple->t_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

	if (resultRelInfo->ri_RelationDesc->rd_att->constr != NULL)
		ExecConstraints(resultRelInfo, elemTupleSlot, estate);

	heap_insert(resultRelInfo->ri_RelationDesc, tuple,
				mgstate->modify_cid + MODIFY_CID_OUTPUT,
				0, NULL);

	if (resultRelInfo->ri_NumIndices > 0)
		ExecInsertIndexTuples(elemTupleSlot, &(tuple->t_self), estate, false,
							  NULL, NIL);

	edge = makeGraphEdgeDatum(elemTupleSlot->tts_values[0],
							  elemTupleSlot->tts_values[1],
							  elemTupleSlot->tts_values[2],
							  elemTupleSlot->tts_values[3],
							  PointerGetDatum(&tuple->t_self));

	if (gedge->resno > 0)
		setSlotValueByAttnum(slot, edge, gedge->resno);

	if (mgstate->canSetTag)
	{
		Assert(estate->es_graphwrstats.insertEdge != UINT_MAX);

		estate->es_graphwrstats.insertEdge++;
	}

	estate->es_result_relation_info = savedResultRelInfo;

	return edge;
}

static ResultRelInfo *
getResultRelInfo(ModifyGraphState *mgstate, Oid relid)
{
	ResultRelInfo *resultRelInfo;
	int			i;

	resultRelInfo = mgstate->resultRelations;
	for (i = 0; i < mgstate->numResultRelations; i++)
	{
		if (RelationGetRelid(resultRelInfo->ri_RelationDesc) == relid)
			break;

		resultRelInfo++;
	}

	if (i >= mgstate->numResultRelations)
		elog(ERROR, "invalid object ID %u for the target label", relid);

	return resultRelInfo;
}

static Datum
findVertex(TupleTableSlot *slot, GraphVertex *gvertex, Graphid *vid)
{
	bool		isnull;
	Datum		vertex;

	if (gvertex->resno == InvalidAttrNumber)
		return DATUM_NULL;

	vertex = slot_getattr(slot, gvertex->resno, &isnull);
	if (isnull)
		return DATUM_NULL;

	if (vid != NULL)
		*vid = DatumGetGraphid(getVertexIdDatum(vertex));

	return vertex;
}

static Datum
findEdge(TupleTableSlot *slot, GraphEdge *gedge, Graphid *eid)
{
	bool		isnull;
	Datum		edge;

	if (gedge->resno == InvalidAttrNumber)
		return DATUM_NULL;

	edge = slot_getattr(slot, gedge->resno, &isnull);
	if (isnull)
		return DATUM_NULL;

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

	if (slot == NULL)
		return;

	attno = findAttrInSlotByName(slot, name);

	slot->tts_values[attno - 1] = value;
	slot->tts_isnull[attno - 1] = false;
}

static void
setSlotValueByAttnum(TupleTableSlot *slot, Datum value, int attnum)
{
	if (slot == NULL)
		return;

	AssertArg(attnum > 0 && attnum <= slot->tts_tupleDescriptor->natts);

	slot->tts_values[attnum - 1] = value;
	slot->tts_isnull[attnum - 1] = (value == (Datum) NULL) ? true : false;
}

static Datum *
makeDatumArray(ExprContext *econtext, int len)
{
	if (len == 0)
		return NULL;

	return palloc(len * sizeof(Datum));
}

static TupleTableSlot *
ExecDeleteGraph(ModifyGraphState *mgstate, TupleTableSlot *slot)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	ExprContext *econtext = mgstate->ps.ps_ExprContext;
	ListCell	*le;

	ResetExprContext(econtext);

	foreach(le, mgstate->exprs)
	{
		ExprState	*e = (ExprState *) lfirst(le);
		Oid			 type;
		Datum		 elem;
		bool		 isNull;

		type = exprType((Node *) e->expr);
		if (!(type == VERTEXOID || type == EDGEOID ||
			  type == GRAPHPATHOID || type == EDGEARRAYOID))
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("expected node, relationship, or path")));

		econtext->ecxt_scantuple = slot;
		elem = ExecEvalExpr(e, econtext, &isNull);
		if (isNull)
		{
			if (type == EDGEARRAYOID)
				continue;
			else
				ereport(NOTICE,
						(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						 errmsg("skipping deletion of NULL graph element")));

			continue;
		}

		/*
		 * Note: After all the graph elements to be removed are collected,
		 * the elements will be removed.
		 */
		enterDelPropTable(mgstate, elem, type);
	}

	return (plan->last ? NULL : slot);
}

static void
deleteElem(ModifyGraphState *mgstate, Datum elem, Datum gid, Oid type)
{
	EState		   *estate = mgstate->ps.state;
	Oid				relid;
	ItemPointer		ctid;
	ResultRelInfo  *resultRelInfo;
	ResultRelInfo  *savedResultRelInfo;
	Relation		resultRelationDesc;
	HTSU_Result 	result;
	HeapUpdateFailureData hufd;

	relid = get_labid_relid(mgstate->graphid,
							GraphidGetLabid(DatumGetGraphid(gid)));
	resultRelInfo = getResultRelInfo(mgstate, relid);

	savedResultRelInfo = estate->es_result_relation_info;
	estate->es_result_relation_info = resultRelInfo;
	resultRelationDesc = resultRelInfo->ri_RelationDesc;

	if (type == VERTEXOID)
		ctid = (ItemPointer) DatumGetPointer(getVertexTidDatum(elem));
	else if (type == EDGEOID)
		ctid = (ItemPointer) DatumGetPointer(getEdgeTidDatum(elem));
	else
		elog(ERROR, "Invalid graph element type %d.", type);

	/*
	 * see ExecDelete
	 */
	result = heap_delete(resultRelationDesc, ctid,
						 mgstate->modify_cid + MODIFY_CID_OUTPUT,
						 estate->es_crosscheck_snapshot,
						 true /* wait for commit */ ,
						 &hufd);
	switch (result)
	{
		case HeapTupleSelfUpdated:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Cypher query does not modify the record more than once.")));
			return ;

		case HeapTupleMayBeUpdated:
			break;

		case HeapTupleUpdated:
			ereport(ERROR,
					(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("could not serialize access due to concurrent update")));

			/* TODO : A solution to concurrent update is needed. */
			return ;

		default:
			elog(ERROR, "unrecognized heap_update status: %u", result);
			return ;
	}

	/*
	 * Note: Normally one would think that we have to delete index tuples
	 * associated with the heap tuple now...
	 *
	 * ... but in POSTGRES, we have no need to do this because VACUUM will
	 * take care of it later.  We can't delete index tuples immediately
	 * anyway, since the tuple is still visible to other transactions.
	 */

	if (mgstate->canSetTag)
	{
		if (type == VERTEXOID)
		{
			Assert(estate->es_graphwrstats.deleteVertex != UINT_MAX);

			estate->es_graphwrstats.deleteVertex += 1;
		}
		else
		{
			Assert(estate->es_graphwrstats.deleteEdge != UINT_MAX);

			estate->es_graphwrstats.deleteEdge += 1;
		}
	}

	estate->es_result_relation_info = savedResultRelInfo;
}

static TupleTableSlot *
ExecSetGraph(ModifyGraphState *mgstate, GSPKind kind, TupleTableSlot *slot)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	ExprContext *econtext = mgstate->ps.ps_ExprContext;
	ListCell	*ls;
	TupleTableSlot *result = mgstate->ps.ps_ResultTupleSlot;

	/*
	 * The results of previous clauses should be preserved.
	 * So, shallow copying is used.
	 */
	copyVirtualTupleTableSlot(result, slot);

	foreach(ls, mgstate->sets)
	{
		GraphSetProp *gsp = lfirst(ls);
		Oid		elemtype;
		Datum	elem_datum;
		Datum	expr_datum;
		bool	isNull;
		Datum	gid;
		Datum	tid;
		Datum	newelem;
		MemoryContext oldmctx;

		if (gsp->kind != kind)
		{
			Assert(kind != GSP_NORMAL);
			continue;
		}

		elemtype = exprType((Node *) gsp->es_elem->expr);
		if (elemtype != VERTEXOID && elemtype != EDGEOID)
			elog(ERROR, "expected node or relationship");

		/* Do not store intermediate results in query memory. */
		oldmctx = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

		/* Get original graph element */
		elem_datum = ExecEvalExpr(gsp->es_elem, econtext, &isNull);
		if (isNull)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("updating NULL is not allowed")));

		/* Evaluate SET expression */
		if (elemtype == VERTEXOID)
		{
			gid = getVertexIdDatum(elem_datum);
			tid = getVertexTidDatum(elem_datum);
		}
		else
		{
			Assert(elemtype == EDGEOID);

			gid = getEdgeIdDatum(elem_datum);
			tid = getEdgeTidDatum(elem_datum);
		}

		findAndReflectNewestValue(mgstate, econtext, gsp, gid);
		expr_datum = ExecEvalExpr(gsp->es_expr, econtext, &isNull);
		if (isNull)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("property map cannot be NULL")));

		newelem = makeModifiedElem(econtext, elem_datum, elemtype,
								   gid, expr_datum, tid);
		MemoryContextSwitchTo(oldmctx);

		enterSetPropTable(mgstate, elemtype, gid, newelem);

		setSlotValueByName(result, newelem, gsp->variable);
	}

	return (plan->last ? NULL : result);
}

static void
findAndReflectNewestValue(ModifyGraphState *mgstate, ExprContext *econtext,
						  GraphSetProp *gsp, Datum gid)
{
	ModifiedElemEntry *entry;
	bool	found;

	if (mgstate->propTable)
	{
		entry = hash_search(mgstate->propTable, (void *) &gid,
							HASH_FIND, &found);
		if (found)
		{
			/* reflect results to exprContext */
			setSlotValueByName(econtext->ecxt_scantuple,
							   entry->elem_datum, gsp->variable);
			setSlotValueByName(econtext->ecxt_innertuple,
							   entry->elem_datum, gsp->variable);
			setSlotValueByName(econtext->ecxt_outertuple,
							   entry->elem_datum, gsp->variable);
		}
	}
}

static ItemPointer
updateElemProp(ModifyGraphState *mgstate, Oid elemtype, Datum gid,
			   Datum elem_datum)
{
	EState		   *estate = mgstate->ps.state;
	TupleTableSlot *elemTupleSlot = mgstate->elemTupleSlot;
	Oid				relid;
	ItemPointer		ctid;
	HeapTuple		tuple;
	ResultRelInfo  *resultRelInfo;
	ResultRelInfo  *savedResultRelInfo;
	Relation		resultRelationDesc;
	LockTupleMode	lockmode;
	HTSU_Result		result;
	HeapUpdateFailureData hufd;

	relid = get_labid_relid(mgstate->graphid,
							GraphidGetLabid(DatumGetGraphid(gid)));
	resultRelInfo = getResultRelInfo(mgstate, relid);

	savedResultRelInfo = estate->es_result_relation_info;
	estate->es_result_relation_info = resultRelInfo;
	resultRelationDesc = resultRelInfo->ri_RelationDesc;

	/*
	 * Create a tuple to store.
	 * Attributes of a vertex/edge label is not the same as a vertex/edge.
	 */
	ExecClearTuple(elemTupleSlot);
	ExecSetSlotDescriptor(elemTupleSlot,
						  RelationGetDescr(resultRelInfo->ri_RelationDesc));
	if (elemtype == VERTEXOID)
	{
		elemTupleSlot->tts_values[0] = gid;
		elemTupleSlot->tts_values[1] = getVertexPropDatum(elem_datum);

		ctid = (ItemPointer) DatumGetPointer(getVertexTidDatum(elem_datum));
	}
	else
	{
		Assert(elemtype == EDGEOID);

		elemTupleSlot->tts_values[0] = gid;
		elemTupleSlot->tts_values[1] = getEdgeStartDatum(elem_datum);
		elemTupleSlot->tts_values[2] = getEdgeEndDatum(elem_datum);
		elemTupleSlot->tts_values[3] = getEdgePropDatum(elem_datum);

		ctid = (ItemPointer) DatumGetPointer(getEdgeTidDatum(elem_datum));
	}
	MemSet(elemTupleSlot->tts_isnull, false,
		   elemTupleSlot->tts_tupleDescriptor->natts * sizeof(bool));
	ExecStoreVirtualTuple(elemTupleSlot);

	tuple = ExecMaterializeSlot(elemTupleSlot);

	/*
	 * Constraints might reference the tableoid column, so initialize
	 * t_tableOid before evaluating them.
	 */
	tuple->t_tableOid = RelationGetRelid(resultRelationDesc);

	/*
	 * Check the constraints of the tuple
	 */
	if (resultRelationDesc->rd_att->constr)
		ExecConstraints(resultRelInfo, elemTupleSlot, estate);

	/*
	 * replace the heap tuple
	 *
	 * Note: if es_crosscheck_snapshot isn't InvalidSnapshot, we check
	 * that the row to be updated is visible to that snapshot, and throw a
	 * can't-serialize error if not. This is a special-case behavior
	 * needed for referential integrity updates in transaction-snapshot
	 * mode transactions.
	 */
	result = heap_update(resultRelationDesc, ctid, tuple,
						 mgstate->modify_cid + MODIFY_CID_SET,
						 estate->es_crosscheck_snapshot,
						 true /* wait for commit */ ,
						 &hufd, &lockmode);
	switch (result)
	{
		case HeapTupleSelfUpdated:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Cypher query does not modify the record more than once.")));
			break;
		case HeapTupleMayBeUpdated:
			break;

		case HeapTupleUpdated:
			ereport(ERROR,
					(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("could not serialize access due to concurrent update")));

			/* TODO : A solution to concurrent updates is needed. */
			break;
		default:
			elog(ERROR, "unrecognized heap_update status: %u", result);
	}

	/*
	 * insert index entries for tuple
	 *
	 * Note: heap_update returns the tid (location) of the new tuple in
	 * the t_self field.
	 *
	 * If it's a HOT update, we mustn't insert new index entries.
	 */
	if (resultRelInfo->ri_NumIndices > 0 && !HeapTupleIsHeapOnly(tuple))
		ExecInsertIndexTuples(elemTupleSlot, &(tuple->t_self),
							  estate, false, NULL, NIL);

	if (mgstate->canSetTag)
		(estate->es_graphwrstats.updateProperty)++;

	estate->es_result_relation_info = savedResultRelInfo;

	return &tuple->t_self;
}

static Datum
makeModifiedElem(ExprContext *econtext, Datum elem, Oid elemtype,
				 Datum id, Datum prop_map, Datum tid)
{
	Datum result;

	if (elemtype == VERTEXOID)
	{
		result = makeGraphVertexDatum(id, prop_map, tid);
	}
	else
	{
		Datum		start;
		Datum		end;

		start = getEdgeStartDatum(elem);
		end = getEdgeEndDatum(elem);

		result = makeGraphEdgeDatum(id, start, end, prop_map, tid);
	}

	return result;
}

static TupleTableSlot *
ExecMergeGraph(ModifyGraphState *mgstate, TupleTableSlot *slot)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	ExprContext *econtext = mgstate->ps.ps_ExprContext;
	GraphPath *path = (GraphPath *) linitial(mgstate->pattern);

	ResetExprContext(econtext);
	econtext->ecxt_scantuple = slot;

	if (isMatchedMergePattern(mgstate->subplan))
	{
		if (mgstate->sets != NIL)
			slot = ExecSetGraph(mgstate, GSP_ON_MATCH, slot);
	}
	else
	{
		MemoryContext oldmctx;

		oldmctx = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

		slot = createMergePath(mgstate, path, slot);

		MemoryContextSwitchTo(oldmctx);

		if (mgstate->sets != NIL)
			slot = ExecSetGraph(mgstate, GSP_ON_CREATE, slot);
	}

	return (plan->last ? NULL : slot);
}

/* tricky but efficient */
static bool
isMatchedMergePattern(PlanState *planstate)
{
	Assert(IsA(planstate, NestLoopState));

	return ((NestLoopState *) planstate)->nl_MatchedOuter;
}

static TupleTableSlot *
createMergePath(ModifyGraphState *mgstate, GraphPath *path,
				TupleTableSlot *slot)
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

		if (IsA(elem, GraphVertex))
		{
			GraphVertex *gvertex = (GraphVertex *) elem;
			Graphid		vid;
			Datum		vertex;

			vertex = findVertex(slot, gvertex, &vid);
			if (vertex == DATUM_NULL)
				vertex = createMergeVertex(mgstate, gvertex, &vid, slot);

			if (out)
				vertices[nvertices++] = vertex;

			if (gedge != NULL)
			{
				Datum edge;

				edge = findEdge(slot, gedge, NULL);
				Assert(edge == DATUM_NULL);

				if (gedge->direction == GRAPH_EDGE_DIR_LEFT)
				{
					edge = createMergeEdge(mgstate, gedge, vid, prevvid, slot);
				}
				else
				{
					Assert(gedge->direction == GRAPH_EDGE_DIR_RIGHT);

					edge = createMergeEdge(mgstate, gedge, prevvid, vid, slot);
				}

				if (out)
					edges[nedges++] = edge;
			}

			prevvid = vid;
		}
		else
		{
			Assert(IsA(elem, GraphEdge));

			gedge = (GraphEdge *) elem;
		}
	}

	/* make a graphpath and set it to the slot */
	if (out)
	{
		Datum graphpath;

		Assert(nvertices == nedges + 1);
		Assert(pathlen == nvertices + nedges);

		graphpath = makeGraphpathDatum(vertices, nvertices, edges, nedges);

		setSlotValueByName(slot, graphpath, path->variable);
	}

	return slot;
}

static Datum
createMergeVertex(ModifyGraphState *mgstate, GraphVertex *gvertex,
				  Graphid *vid, TupleTableSlot *slot)
{
	EState		   *estate = mgstate->ps.state;
	ExprContext	   *econtext = mgstate->ps.ps_ExprContext;
	ResultRelInfo  *resultRelInfo;
	ResultRelInfo  *savedResultRelInfo;
	bool			isNull;
	Datum			vertex;
	Datum			vertexId;
	Datum			vertexProp;
	TupleTableSlot *insertSlot = mgstate->elemTupleSlot;
	HeapTuple		tuple;

	resultRelInfo = getResultRelInfo(mgstate, gvertex->relid);
	savedResultRelInfo = estate->es_result_relation_info;
	estate->es_result_relation_info = resultRelInfo;

	vertex = ExecEvalExpr(gvertex->es_expr, econtext, &isNull);
	if (isNull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("NULL is not allowed in MERGE")));

	vertexId = getVertexIdDatum(vertex);
	*vid = DatumGetGraphid(vertexId);

	vertexProp = getVertexPropDatum(vertex);
	if (!JB_ROOT_IS_OBJECT(DatumGetJsonb(vertexProp)))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("jsonb object is expected for property map")));

	ExecClearTuple(insertSlot);

	ExecSetSlotDescriptor(insertSlot,
						  RelationGetDescr(resultRelInfo->ri_RelationDesc));
	insertSlot->tts_values[0] = vertexId;
	insertSlot->tts_values[1] = vertexProp;
	MemSet(insertSlot->tts_isnull, false,
		   insertSlot->tts_tupleDescriptor->natts * sizeof(bool));
	ExecStoreVirtualTuple(insertSlot);

	tuple = ExecMaterializeSlot(insertSlot);

	/*
	 * Constraints might reference the tableoid column, so initialize
	 * t_tableOid before evaluating them.
	 */
	tuple->t_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

	/*
	 * Check the constraints of the tuple
	 */
	if (resultRelInfo->ri_RelationDesc->rd_att->constr != NULL)
		ExecConstraints(resultRelInfo, insertSlot, estate);

	/*
	 * insert the tuple normally
	 */
	heap_insert(resultRelInfo->ri_RelationDesc, tuple,
				mgstate->modify_cid + MODIFY_CID_OUTPUT,
				0, NULL);

	/* insert index entries for the tuple */
	if (resultRelInfo->ri_NumIndices > 0)
		ExecInsertIndexTuples(insertSlot, &(tuple->t_self), estate, false,
							  NULL, NIL);

	vertex = makeGraphVertexDatum(insertSlot->tts_values[0],
								  insertSlot->tts_values[1],
								  PointerGetDatum(&tuple->t_self));

	if (gvertex->resno > 0)
		setSlotValueByAttnum(slot, vertex, gvertex->resno);

	if (mgstate->canSetTag)
	{
		Assert(estate->es_graphwrstats.insertVertex != UINT_MAX);

		estate->es_graphwrstats.insertVertex++;
	}

	estate->es_result_relation_info = savedResultRelInfo;

	return vertex;
}

static Datum
createMergeEdge(ModifyGraphState *mgstate, GraphEdge *gedge, Graphid start,
				Graphid end, TupleTableSlot *slot)
{
	EState		   *estate = mgstate->ps.state;
	ExprContext	   *econtext = mgstate->ps.ps_ExprContext;
	ResultRelInfo  *resultRelInfo;
	ResultRelInfo  *savedResultRelInfo;
	bool			isNull;
	Datum			edge;
	Datum			edgeProp;
	TupleTableSlot *insertSlot = mgstate->elemTupleSlot;
	HeapTuple		tuple;

	resultRelInfo = getResultRelInfo(mgstate, gedge->relid);
	savedResultRelInfo = estate->es_result_relation_info;
	estate->es_result_relation_info = resultRelInfo;

	edge = ExecEvalExpr(gedge->es_expr, econtext, &isNull);
	if (isNull)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("NULL is not allowed in MERGE")));

	edgeProp = getEdgePropDatum(edge);
	if (!JB_ROOT_IS_OBJECT(DatumGetJsonb(edgeProp)))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("jsonb object is expected for property map")));

	ExecClearTuple(insertSlot);

	ExecSetSlotDescriptor(insertSlot,
						  RelationGetDescr(resultRelInfo->ri_RelationDesc));
	insertSlot->tts_values[0] = getEdgeIdDatum(edge);
	insertSlot->tts_values[1] = GraphidGetDatum(start);
	insertSlot->tts_values[2] = GraphidGetDatum(end);
	insertSlot->tts_values[3] = edgeProp;
	MemSet(insertSlot->tts_isnull, false,
		   insertSlot->tts_tupleDescriptor->natts * sizeof(bool));
	ExecStoreVirtualTuple(insertSlot);

	tuple = ExecMaterializeSlot(insertSlot);

	tuple->t_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

	if (resultRelInfo->ri_RelationDesc->rd_att->constr != NULL)
		ExecConstraints(resultRelInfo, insertSlot, estate);

	heap_insert(resultRelInfo->ri_RelationDesc, tuple,
				mgstate->modify_cid + MODIFY_CID_OUTPUT,
				0, NULL);

	if (resultRelInfo->ri_NumIndices > 0)
		ExecInsertIndexTuples(insertSlot, &(tuple->t_self), estate, false,
							  NULL, NIL);

	edge = makeGraphEdgeDatum(insertSlot->tts_values[0],
							  insertSlot->tts_values[1],
							  insertSlot->tts_values[2],
							  insertSlot->tts_values[3],
							  PointerGetDatum(&tuple->t_self));

	if (gedge->resno > 0)
		setSlotValueByAttnum(slot, edge, gedge->resno);

	if (mgstate->canSetTag)
	{
		Assert(estate->es_graphwrstats.insertEdge != UINT_MAX);

		estate->es_graphwrstats.insertEdge++;
	}

	estate->es_result_relation_info = savedResultRelInfo;

	return edge;
}

static TupleTableSlot *
copyVirtualTupleTableSlot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	int natts = srcslot->tts_tupleDescriptor->natts;

	ExecSetSlotDescriptor(dstslot, srcslot->tts_tupleDescriptor);

	/* shallow copy */
	memcpy(dstslot->tts_values, srcslot->tts_values, natts * sizeof(Datum));
	memcpy(dstslot->tts_isnull, srcslot->tts_isnull, natts * sizeof(bool));

	ExecStoreVirtualTuple(dstslot);

	return dstslot;
}

static void
enterSetPropTable(ModifyGraphState *mgstate, Oid type, Datum gid,
				  Datum elem_datum)
{
	ModifiedElemEntry *entry;
	bool	found;

	entry = hash_search(mgstate->propTable, (void *) &gid, HASH_ENTER, &found);
	if (found)
		pfree(DatumGetPointer(entry->elem_datum));

	entry->elem_datum = datumCopy(elem_datum, false, -1);
	entry->type = type;
}

static void
enterDelPropTable(ModifyGraphState *mgstate, Datum elem, Oid type)
{
	Datum	gid;
	bool	found;
	ModifiedElemEntry *entry;

	if (type == VERTEXOID)
	{
		gid = getVertexIdDatum(elem);

		entry = hash_search(mgstate->propTable, (void *) &gid,
							HASH_ENTER, &found);
		if (found)
			return;

		entry->elem_datum = datumCopy(elem, false, -1);
		entry->type = type;
	}
	else if (type == EDGEOID)
	{
		gid = getEdgeIdDatum(elem);

		entry = hash_search(mgstate->propTable, (void *) &gid,
							HASH_ENTER, &found);
		if (found)
			return;

		entry->elem_datum = datumCopy(elem, false, -1);
		entry->type = type;
	}
	else if (type == GRAPHPATHOID)
	{
		List	   *vtxList = NIL;
		List	   *edgeList = NIL;
		ListCell   *lc;

		Assert(type == GRAPHPATHOID);

		getElemListInPath(elem, &vtxList, &edgeList);

		foreach(lc, vtxList)
		{
			Datum vtx = (Datum) lfirst(lc);

			gid = getVertexIdDatum(vtx);
			entry = hash_search(mgstate->propTable, (void *) &gid,
								HASH_ENTER, &found);
			if (found)
				continue;

			entry->elem_datum = datumCopy(vtx, false, -1);
			entry->type = VERTEXOID;
		}

		foreach(lc, edgeList)
		{
			Datum edge = (Datum) lfirst(lc);

			gid = getEdgeIdDatum(edge);
			entry = hash_search(mgstate->propTable, (void *) &gid,
								HASH_ENTER, &found);
			if (found)
				continue;

			entry->elem_datum = datumCopy(edge, false, -1);
			entry->type = EDGEOID;
		}
	}
	else if (type == EDGEARRAYOID)
	{
		AnyArrayType *edges;
		int			nedges;
		int16		typlen;
		bool		typbyval;
		char		typalign;
		array_iter	it;
		int			i;
		Datum		edge;
		bool		isnull;

		edges = DatumGetAnyArray(elem);
		nedges = ArrayGetNItems(AARR_NDIM(edges), AARR_DIMS(edges));

		get_typlenbyvalalign(AARR_ELEMTYPE(edges), &typlen,
							 &typbyval, &typalign);

		array_iter_setup(&it, edges);
		for (i = 0; i < nedges; i++)
		{
			edge = array_iter_next(&it, &isnull, i,typlen,
									typbyval, typalign);

			gid = getEdgeIdDatum(edge);
			entry = hash_search(mgstate->propTable,
								(void *) &gid, HASH_ENTER, &found);

			if (found)
				continue;

			entry->elem_datum = datumCopy(edge, false, -1);
			entry->type = EDGEOID;
		}
	}
	else
		elog(ERROR, "unexpected graph type %d", type);
}

static void
getElemListInPath(Datum graphpath, List **vtxlist, List **edgelist)
{
	Datum		vertices_datum;
	Datum		edges_datum;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	array_iter	it;
	Datum		value;
	bool		isnull;
	int			i;

	getGraphpathArrays(graphpath, &vertices_datum, &edges_datum);

	if (vtxlist != NULL)
	{
		AnyArrayType *vertices;
		int			nvertices;

		vertices = DatumGetAnyArray(vertices_datum);
		nvertices = ArrayGetNItems(AARR_NDIM(vertices), AARR_DIMS(vertices));

		get_typlenbyvalalign(AARR_ELEMTYPE(vertices), &typlen,
							 &typbyval, &typalign);
		array_iter_setup(&it, vertices);
		for (i = 0; i < nvertices; i++)
		{
			value = array_iter_next(&it, &isnull, i, typlen,
									typbyval, typalign);
			Assert(!isnull);

			*vtxlist = lappend(*vtxlist, (void *) value);
		}
	}

	if (edgelist != NULL)
	{
		AnyArrayType *edges;
		int			nedges;

		edges = DatumGetAnyArray(edges_datum);
		nedges = ArrayGetNItems(AARR_NDIM(edges), AARR_DIMS(edges));

		get_typlenbyvalalign(AARR_ELEMTYPE(edges), &typlen,
							 &typbyval, &typalign);
		array_iter_setup(&it, edges);
		for (i = 0; i < nedges; i++)
		{
			value = array_iter_next(&it, &isnull, i,typlen,
									typbyval, typalign);
			Assert(!isnull);

			*edgelist = lappend(*edgelist, (void *) value);
		}
	}
}

static Datum
getVertexFinalPropMap(ModifyGraphState *mgstate, Datum origin, Graphid gid)
{
	ModifyGraph   *plan = (ModifyGraph *) mgstate->ps.plan;
	ModifiedElemEntry *entry;

	entry = hash_search(mgstate->propTable, (void *) &gid, HASH_FIND, NULL);

	/* un-modified vertex */
	if (entry == NULL)
		return origin;

	if (plan->operation == GWROP_DELETE)
		return DATUM_NULL;
	else
		return entry->elem_datum;
}

static Datum
getEdgeFinalPropMap(ModifyGraphState *mgstate, Datum origin, Graphid gid)
{
	ModifyGraph   *plan = (ModifyGraph *) mgstate->ps.plan;
	ModifiedElemEntry *entry;

	entry = hash_search(mgstate->propTable, (void *) &gid, HASH_FIND, NULL);

	/* un-modified edge */
	if (entry == NULL)
		return origin;

	if (plan->operation == GWROP_DELETE)
		return (Datum) NULL;
	else
		return entry->elem_datum;
}

static Datum
getPathFinalPropMap(ModifyGraphState *mgstate, Datum origin)
{
	Datum		vertices_datum;
	Datum		edges_datum;
	AnyArrayType *arrVertices;
	AnyArrayType *arrEdges;
	int			nvertices;
	int			nedges;
	Datum	   *vertices;
	Datum	   *edges;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	array_iter	it;
	int			i;
	Datum		value;
	Graphid		gid;
	bool		isnull;
	bool		modified = false;
	Datum		result;

	getGraphpathArrays(origin, &vertices_datum, &edges_datum);

	arrVertices = DatumGetAnyArray(vertices_datum);
	arrEdges = DatumGetAnyArray(edges_datum);

	nvertices = ArrayGetNItems(AARR_NDIM(arrVertices), AARR_DIMS(arrVertices));
	nedges = ArrayGetNItems(AARR_NDIM(arrEdges), AARR_DIMS(arrEdges));
	Assert(nvertices == nedges + 1);

	vertices = palloc(nvertices * sizeof(Datum));
	edges = palloc(nedges * sizeof(Datum));

	get_typlenbyvalalign(AARR_ELEMTYPE(arrVertices), &typlen,
						 &typbyval, &typalign);
	array_iter_setup(&it, arrVertices);
	for (i = 0; i < nvertices; i++)
	{
		Datum		vertex;

		value = array_iter_next(&it, &isnull, i, typlen, typbyval, typalign);
		Assert(!isnull);

		gid = getVertexIdDatum(value);
		vertex = getVertexFinalPropMap(mgstate, value, gid);

		if (vertex == (Datum) NULL)
			elog(ERROR, "cannot delete a vertex in a graphpath");

		if (vertex != value)
			modified = true;

		vertices[i] = vertex;
	}

	get_typlenbyvalalign(AARR_ELEMTYPE(arrEdges), &typlen,
						 &typbyval, &typalign);
	array_iter_setup(&it, arrEdges);
	for (i = 0; i < nedges; i++)
	{
		Datum		edge;

		value = array_iter_next(&it, &isnull, i, typlen, typbyval, typalign);
		Assert(!isnull);

		gid = getEdgeIdDatum(value);
		edge = getEdgeFinalPropMap(mgstate, value, gid);

		if (edge == (Datum) NULL)
			elog(ERROR, "cannot modify the element of graphpath.");

		if (edge != value)
			modified = true;

		edges[i] = edge;
	}

	if (modified)
		result = makeGraphpathDatum(vertices, nvertices, edges, nedges);
	else
		result = origin;

	pfree(vertices);
	pfree(edges);

	return result;
}

static void
reflectModifiedProp(ModifyGraphState *mgstate)
{
	ModifyGraph	   *plan = (ModifyGraph *) mgstate->ps.plan;
	ExprContext	   *econtext = mgstate->ps.ps_ExprContext;
	HASH_SEQ_STATUS	seq;
	ModifiedElemEntry *entry;

	Assert(mgstate->propTable != NULL);

	hash_seq_init(&seq, mgstate->propTable);
	while ((entry = hash_seq_search(&seq)) != NULL)
	{
		Datum gid = PointerGetDatum(entry->key);

		/* write the object to heap */
		if (plan->operation == GWROP_DELETE)
			deleteElem(mgstate, entry->elem_datum, gid, entry->type);
		else
		{
			ItemPointer ctid;

			ctid = updateElemProp(mgstate, entry->type, gid, entry->elem_datum);

			if (mgstate->eagerness)
			{
				Datum property;
				Datum newelem;

				if (entry->type == VERTEXOID)
					property = getVertexPropDatum(entry->elem_datum);
				else if (entry->type == EDGEOID)
					property = getEdgePropDatum(entry->elem_datum);
				else
					elog(ERROR, "unexpected graph type %d", entry->type);

				newelem = makeModifiedElem(econtext, entry->elem_datum,
										   entry->type, gid, property,
										   PointerGetDatum(ctid));

				pfree(DatumGetPointer(entry->elem_datum));
				entry->elem_datum = newelem;
			}
		}
	}
}
