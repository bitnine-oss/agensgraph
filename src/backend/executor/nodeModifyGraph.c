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
#include "pgstat.h"
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

bool		enable_multiple_update = true;
bool		auto_gather_graphmeta = false;

/* hash entry */
typedef struct ModifiedElemEntry
{
	Graphid		key;
	union
	{
		Datum			elem;	/* modified graph element */
		ItemPointerData	tid;	/* use to find tuple in delete plan */
	} data;
} ModifiedElemEntry;

static TupleTableSlot *ExecModifyGraph(PlanState *pstate);
static void initGraphWRStats(ModifyGraphState *mgstate, GraphWriteOp op);
static List *ExecInitGraphPattern(List *pattern, ModifyGraphState *mgstate);
static List *ExecInitGraphSets(List *sets, ModifyGraphState *mgstate);
static List *ExecInitGraphDelExprs(List *exprs, ModifyGraphState *mgstate);

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
static bool isDetachRequired(ModifyGraphState *mgstate);
static bool isEdgeArrayOfPath(List *exprs, char *variable);
static void deleteElem(ModifyGraphState *mgstate, Datum gid, ItemPointer tid,
					   Oid type);

/* SET */
static TupleTableSlot *ExecSetGraph(ModifyGraphState *mgstate, GSPKind kind,
									TupleTableSlot *slot);
static TupleTableSlot *copyVirtualTupleTableSlot(TupleTableSlot *dstslot,
												 TupleTableSlot *srcslot);
static void findAndReflectNewestValue(ModifyGraphState *mgstate,
									  TupleTableSlot *slot);
static ItemPointer updateElemProp(ModifyGraphState *mgstate, Oid elemtype,
								  Datum gid, Datum elem_datum);
static Datum makeModifiedElem(Datum elem, Oid elemtype,
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

/* eager */
static void enterSetPropTable(ModifyGraphState *mgstate, Datum gid,
							  Datum newelem);
static void enterDelPropTable(ModifyGraphState *mgstate, Datum elem, Oid type);
static Datum getVertexFinal(ModifyGraphState *mgstate, Datum origin);
static Datum getEdgeFinal(ModifyGraphState *mgstate, Datum origin);
static Datum getPathFinal(ModifyGraphState *node, Datum origin);
static void reflectModifiedProp(ModifyGraphState *mgstate);

/* common */
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
	CommandId	svCid;

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

	mgstate->done = false;
	mgstate->child_done = false;
	mgstate->eagerness = mgplan->eagerness;
	mgstate->modify_cid = GetCurrentCommandId(false) +
						  (mgplan->nr_modify * MODIFY_CID_MAX);

	/*
	 * Pass the lower limit of the CID of the current clause to the previous
	 * clause as a default CID of it.
	 */
	svCid = estate->es_snapshot->curcid;
	estate->es_snapshot->curcid = mgstate->modify_cid;

	mgstate->subplan = ExecInitNode(mgplan->subplan, estate, eflags);
	AssertArg(mgplan->operation != GWROP_MERGE ||
			  IsA(mgstate->subplan, NestLoopState));

	estate->es_snapshot->curcid = svCid;

	mgstate->elemTupleSlot = ExecInitExtraTupleSlot(estate);

	mgstate->graphid = get_graph_path_oid();
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

	mgstate->exprs = ExecInitGraphDelExprs(mgplan->exprs, mgstate);
	mgstate->sets = ExecInitGraphSets(mgplan->sets, mgstate);

	initGraphWRStats(mgstate, mgplan->operation);

	if (mgstate->eagerness ||
		(mgstate->sets != NIL && enable_multiple_update) ||
		mgstate->exprs != NIL)
	{
		HASHCTL ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Graphid);
		ctl.entrysize = sizeof(ModifiedElemEntry);
		ctl.hcxt = CurrentMemoryContext;

		mgstate->elemTable =
				hash_create("modified object table", 128, &ctl,
							HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	else
	{
		mgstate->elemTable = NULL;
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
			CommandId	svCid;

			/* ExecInsertIndexTuples() uses per-tuple context. Reset it here. */
			ResetPerTupleExprContext(estate);

			/* pass lower bound CID to subplan */
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

		if (mgstate->elemTable != NULL)
			reflectModifiedProp(mgstate);
	}

	if (mgstate->eagerness)
	{
		TupleTableSlot *result;
		TupleDesc	tupDesc;
		int			natts;
		int			i;

		/* don't care about scan direction */
		result = mgstate->ps.ps_ResultTupleSlot;
		tuplestore_gettupleslot(mgstate->tuplestorestate, true, false, result);

		if (TupIsNull(result))
			return result;

		slot_getallattrs(result);

		if (mgstate->elemTable == NULL ||
			hash_get_num_entries(mgstate->elemTable) < 1)
			return result;

		tupDesc = result->tts_tupleDescriptor;
		natts = tupDesc->natts;
		for (i = 0; i < natts; i++)
		{
			Oid			type;
			Datum		elem;

			if (result->tts_isnull[i])
				continue;

			type = tupDesc->attrs[i]->atttypid;
			if (type == VERTEXOID)
			{
				elem = getVertexFinal(mgstate, result->tts_values[i]);
			}
			else if (type == EDGEOID)
			{
				elem = getEdgeFinal(mgstate, result->tts_values[i]);
			}
			else if (type == GRAPHPATHOID)
			{
				/*
				 * When deleting the graphpath, edge array of graphpath is
				 * deleted first and vertex array is deleted in the next plan.
				 * So, the graphpath must be passed to the next plan for
				 * deleting vertex array of the graphpath.
				 */
				if (isEdgeArrayOfPath(mgstate->exprs,
									  NameStr(tupDesc->attrs[i]->attname)))
					continue;

				elem = getPathFinal(mgstate, result->tts_values[i]);
			}
			else if (type == EDGEARRAYOID && plan->operation == GWROP_DELETE)
			{
				/*
				 * The edges are used only for removal,
				 * not for result output.
				 *
				 * This assumes that there are only variable references in the
				 * target list.
				 */
				continue;
			}
			else
			{
				continue;
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
	EState	   *estate = mgstate->ps.state;
	ResultRelInfo *resultRelInfo;
	int			i;
	CommandId	used_cid;

	if (mgstate->tuplestorestate != NULL)
		tuplestore_end(mgstate->tuplestorestate);
	mgstate->tuplestorestate = NULL;

	if (mgstate->elemTable != NULL)
		hash_destroy(mgstate->elemTable);

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

	/*
	 * ModifyGraph plan uses multi-level CommandId for supporting visibitliy
	 * between cypher Clauses. Need to raise the cid to see the modifications
	 * made by this ModifyGraph plan in the next command.
	 */
	used_cid = mgstate->modify_cid + MODIFY_CID_MAX;
	while (used_cid > GetCurrentCommandId(true))
	{
		CommandCounterIncrement();
	}
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

static List *
ExecInitGraphDelExprs(List *exprs, ModifyGraphState *mgstate)
{
	ListCell *lc;

	foreach(lc, exprs)
	{
		GraphDelElem *gde = lfirst(lc);

		gde->es_elem = ExecInitExpr((Expr *) gde->elem, (PlanState *) mgstate);
	}

	return exprs;
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

			Assert(vertex != (Datum) 0);

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

	estate->es_graphwrstats.insertVertex++;

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
	Assert(edge != (Datum) 0);

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

	estate->es_graphwrstats.insertEdge++;

	estate->es_result_relation_info = savedResultRelInfo;

	if (auto_gather_graphmeta)
		agstat_count_edge_create(id, start, end);

	return edge;
}

static TupleTableSlot *
ExecDeleteGraph(ModifyGraphState *mgstate, TupleTableSlot *slot)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	ExprContext *econtext = mgstate->ps.ps_ExprContext;
	TupleDesc	tupDesc = slot->tts_tupleDescriptor;
	ListCell   *le;

	ResetExprContext(econtext);

	if (isDetachRequired(mgstate))
		elog(ERROR, "vertices with edges can not be removed");

	foreach(le, mgstate->exprs)
	{
		GraphDelElem *gde = castNode(GraphDelElem, lfirst(le));
		Oid			type;
		Datum		elem;
		bool		isNull;
		AttrNumber	attno = findAttrInSlotByName(slot, gde->variable);

		type = exprType((Node *) gde->elem);
		if (!(type == VERTEXOID || type == EDGEOID ||
			  type == VERTEXARRAYOID || type == EDGEARRAYOID))
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("expected node, relationship, or path")));

		econtext->ecxt_scantuple = slot;
		elem = ExecEvalExpr(gde->es_elem, econtext, &isNull);
		if (isNull)
		{
			/*
			 * This assumes that there are only variable references in the
			 * target list.
			 */
			if (type == EDGEARRAYOID)
				continue;
			else
				ereport(NOTICE,
						(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						 errmsg("skipping deletion of NULL graph element")));

			continue;
		}

		/*
		 * NOTE: After all the graph elements to be removed are collected,
		 *       they will be removed.
		 */
		enterDelPropTable(mgstate, elem, type);

		/*
		 * The graphpath must be passed to the next plan for deleting
		 * vertex array of the graphpath.
		 */
		if (type == EDGEARRAYOID &&
			tupDesc->attrs[attno - 1]->atttypid == GRAPHPATHOID)
			continue;

		setSlotValueByAttnum(slot, (Datum) 0, attno);
	}

	return (plan->last ? NULL : slot);
}

/* tricky but efficient */
static bool
isDetachRequired(ModifyGraphState *mgstate)
{
	NestLoopState *nlstate;
	ModifyGraph *plan;

	/* no vertex in the target list of DELETE */
	if (!IsA(mgstate->subplan, NestLoopState))
		return false;

	/*
	 * The join may not be the join which retrieves edges connected to the
	 * target vertices.
	 */
	nlstate = (NestLoopState *) mgstate->subplan;
	if (nlstate->js.jointype != JOIN_CYPHER_DELETE)
		return false;

	/*
	 * All the target edges will be deleted. There may be a chance that no
	 * edge exists for the vertices in the current slot, but it doesn't
	 * matter.
	 */
	plan = (ModifyGraph *) mgstate->ps.plan;
	if (plan->detach)
		return false;

	/*
	 * true: At least one edge exists for the target vertices in the current
	 *       slot. (nl_MatchedOuter && !nl_NeedNewOuter)
	 * false: No edge exists for the target vertices in the current slot.
	 *        (!nl_MatchedOuter && nl_NeedNewOuter)
	 */
	return nlstate->nl_MatchedOuter;
}

static void
deleteElem(ModifyGraphState *mgstate, Datum gid, ItemPointer tid, Oid type)
{
	EState	   *estate = mgstate->ps.state;
	Oid			relid;
	ResultRelInfo *resultRelInfo;
	ResultRelInfo *savedResultRelInfo;
	Relation	resultRelationDesc;
	HTSU_Result	result;
	HeapUpdateFailureData hufd;

	relid = get_labid_relid(mgstate->graphid,
							GraphidGetLabid(DatumGetGraphid(gid)));
	resultRelInfo = getResultRelInfo(mgstate, relid);

	savedResultRelInfo = estate->es_result_relation_info;
	estate->es_result_relation_info = resultRelInfo;
	resultRelationDesc = resultRelInfo->ri_RelationDesc;

	/* see ExecDelete() */
	result = heap_delete(resultRelationDesc, tid,
						 mgstate->modify_cid + MODIFY_CID_OUTPUT,
						 estate->es_crosscheck_snapshot, true, &hufd);
	switch (result)
	{
		case HeapTupleSelfUpdated:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("modifying the same element more than once cannot happen")));
			return;

		case HeapTupleMayBeUpdated:
			break;

		case HeapTupleUpdated:
			/* TODO: A solution to concurrent update is needed. */
			ereport(ERROR,
					(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("could not serialize access due to concurrent update")));
			return;

		default:
			elog(ERROR, "unrecognized heap_update status: %u", result);
			return;
	}

	/*
	 * NOTE: VACUUM will delete index tuples associated with the heap tuple
	 *       later.
	 */

	if (type == VERTEXOID)
		estate->es_graphwrstats.deleteVertex++;
	else
	{
		Assert(type == EDGEOID);

		estate->es_graphwrstats.deleteEdge++;
	}

	estate->es_result_relation_info = savedResultRelInfo;
}

static bool
isEdgeArrayOfPath(List *exprs, char *variable)
{
	ListCell   *lc;

	foreach(lc, exprs)
	{
		GraphDelElem *gde = castNode(GraphDelElem, lfirst(lc));

		if (exprType(gde->elem) == EDGEARRAYOID &&
			strcmp(gde->variable, variable) == 0)
			return true;
	}

	return false;
}

static TupleTableSlot *
ExecSetGraph(ModifyGraphState *mgstate, GSPKind kind, TupleTableSlot *slot)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	ExprContext *econtext = mgstate->ps.ps_ExprContext;
	ListCell   *ls;
	TupleTableSlot *result = mgstate->ps.ps_ResultTupleSlot;

	/*
	 * The results of previous clauses should be preserved.
	 * So, shallow copying is used.
	 */
	copyVirtualTupleTableSlot(result, slot);

	foreach(ls, mgstate->sets)
	{
		GraphSetProp *gsp = lfirst(ls);
		Oid			elemtype;
		Datum		elem_datum;
		Datum		expr_datum;
		bool		isNull;
		Datum		gid;
		Datum		tid;
		Datum		newelem;
		MemoryContext oldmctx;

		if (gsp->kind != kind)
		{
			Assert(kind != GSP_NORMAL);
			continue;
		}

		elemtype = exprType((Node *) gsp->es_elem->expr);
		if (elemtype != VERTEXOID && elemtype != EDGEOID)
			elog(ERROR, "expected node or relationship");

		/* store intermediate results in tuple memory context */
		oldmctx = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

		/*
		 * Reflect newest value all types of scantuple
		 * before evaluating expression.
		 */
		findAndReflectNewestValue(mgstate, econtext->ecxt_scantuple);
		findAndReflectNewestValue(mgstate, econtext->ecxt_innertuple);
		findAndReflectNewestValue(mgstate, econtext->ecxt_outertuple);

		/* get original graph element */
		elem_datum = ExecEvalExpr(gsp->es_elem, econtext, &isNull);
		if (isNull)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("updating NULL is not allowed")));

		/* evaluate SET expression */
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

		expr_datum = ExecEvalExpr(gsp->es_expr, econtext, &isNull);
		if (isNull)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("property map cannot be NULL")));

		newelem = makeModifiedElem(elem_datum, elemtype, gid, expr_datum, tid);

		MemoryContextSwitchTo(oldmctx);

		if (mgstate->elemTable)
			enterSetPropTable(mgstate, gid, newelem);
		else
			updateElemProp(mgstate, elemtype, gid, newelem);

		setSlotValueByName(result, newelem, gsp->variable);
	}

	return (plan->last ? NULL : result);
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
findAndReflectNewestValue(ModifyGraphState *mgstate, TupleTableSlot *slot)
{
	int			i;

	if (slot == NULL)
		return;

	for (i = 0; i < slot->tts_tupleDescriptor->natts; i++)
	{
		Datum	finalValue;;

		if (slot->tts_isnull[i] ||
			slot->tts_tupleDescriptor->attrs[i]->attisdropped)
			continue;

		switch(slot->tts_tupleDescriptor->attrs[i]->atttypid)
		{
			case VERTEXOID:
				finalValue = getVertexFinal(mgstate, slot->tts_values[i]);
				break;
			case EDGEOID:
				finalValue = getEdgeFinal(mgstate, slot->tts_values[i]);
				break;
			case GRAPHPATHOID:
				finalValue = getPathFinal(mgstate, slot->tts_values[i]);
				break;
			default:
				continue;
		}

		setSlotValueByAttnum(slot, finalValue, i + 1);
	}
}

/* See ExecUpdate() */
static ItemPointer
updateElemProp(ModifyGraphState *mgstate, Oid elemtype, Datum gid,
			   Datum elem_datum)
{
	EState	   *estate = mgstate->ps.state;
	TupleTableSlot *elemTupleSlot = mgstate->elemTupleSlot;
	Oid			relid;
	ItemPointer	ctid;
	HeapTuple	tuple;
	ResultRelInfo *resultRelInfo;
	ResultRelInfo *savedResultRelInfo;
	Relation	resultRelationDesc;
	LockTupleMode lockmode;
	HTSU_Result	result;
	HeapUpdateFailureData hufd;

	relid = get_labid_relid(mgstate->graphid,
							GraphidGetLabid(DatumGetGraphid(gid)));
	resultRelInfo = getResultRelInfo(mgstate, relid);

	savedResultRelInfo = estate->es_result_relation_info;
	estate->es_result_relation_info = resultRelInfo;
	resultRelationDesc = resultRelInfo->ri_RelationDesc;

	/*
	 * Create a tuple to store. Attributes of vertex/edge label are not the
	 * same with those of vertex/edge.
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
	tuple->t_tableOid = RelationGetRelid(resultRelationDesc);

	if (resultRelationDesc->rd_att->constr)
		ExecConstraints(resultRelInfo, elemTupleSlot, estate);

	result = heap_update(resultRelationDesc, ctid, tuple,
						 mgstate->modify_cid + MODIFY_CID_SET,
						 estate->es_crosscheck_snapshot,
						 true, &hufd, &lockmode);
	switch (result)
	{
		case HeapTupleSelfUpdated:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("graph element(%hu," UINT64_FORMAT ") has been SET multiple times",
							GraphidGetLabid(DatumGetGraphid(gid)),
							GraphidGetLocid(DatumGetGraphid(gid)))));
			break;
		case HeapTupleMayBeUpdated:
			break;
		case HeapTupleUpdated:
			/* TODO: A solution to concurrent update is needed. */
			ereport(ERROR,
					(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("could not serialize access due to concurrent update")));
			break;
		default:
			elog(ERROR, "unrecognized heap_update status: %u", result);
	}

	if (resultRelInfo->ri_NumIndices > 0 && !HeapTupleIsHeapOnly(tuple))
		ExecInsertIndexTuples(elemTupleSlot, &(tuple->t_self),
							  estate, false, NULL, NIL);

	estate->es_graphwrstats.updateProperty++;

	estate->es_result_relation_info = savedResultRelInfo;

	return &tuple->t_self;
}

static Datum
makeModifiedElem(Datum elem, Oid elemtype,
				 Datum id, Datum prop_map, Datum tid)
{
	Datum		result;

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
			if (vertex == (Datum) 0)
				vertex = createMergeVertex(mgstate, gvertex, &vid, slot);

			if (out)
				vertices[nvertices++] = vertex;

			if (gedge != NULL)
			{
				Datum edge;

				edge = findEdge(slot, gedge, NULL);
				Assert(edge == (Datum) 0);

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

/* See ExecInsert() */
static Datum
createMergeVertex(ModifyGraphState *mgstate, GraphVertex *gvertex,
				  Graphid *vid, TupleTableSlot *slot)
{
	EState	   *estate = mgstate->ps.state;
	ExprContext *econtext = mgstate->ps.ps_ExprContext;
	ResultRelInfo *resultRelInfo;
	ResultRelInfo *savedResultRelInfo;
	bool		isNull;
	Datum		vertex;
	Datum		vertexId;
	Datum		vertexProp;
	TupleTableSlot *insertSlot = mgstate->elemTupleSlot;
	HeapTuple	tuple;

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
	tuple->t_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

	if (resultRelInfo->ri_RelationDesc->rd_att->constr != NULL)
		ExecConstraints(resultRelInfo, insertSlot, estate);

	heap_insert(resultRelInfo->ri_RelationDesc, tuple,
				mgstate->modify_cid + MODIFY_CID_OUTPUT,
				0, NULL);

	if (resultRelInfo->ri_NumIndices > 0)
		ExecInsertIndexTuples(insertSlot, &(tuple->t_self), estate, false,
							  NULL, NIL);

	vertex = makeGraphVertexDatum(insertSlot->tts_values[0],
								  insertSlot->tts_values[1],
								  PointerGetDatum(&tuple->t_self));

	if (gvertex->resno > 0)
		setSlotValueByAttnum(slot, vertex, gvertex->resno);

	estate->es_graphwrstats.insertVertex++;

	estate->es_result_relation_info = savedResultRelInfo;

	return vertex;
}

static Datum
createMergeEdge(ModifyGraphState *mgstate, GraphEdge *gedge, Graphid start,
				Graphid end, TupleTableSlot *slot)
{
	EState	   *estate = mgstate->ps.state;
	ExprContext *econtext = mgstate->ps.ps_ExprContext;
	ResultRelInfo *resultRelInfo;
	ResultRelInfo *savedResultRelInfo;
	bool		isNull;
	Datum		edge;
	Datum		edgeProp;
	TupleTableSlot *insertSlot = mgstate->elemTupleSlot;
	HeapTuple	tuple;

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

	estate->es_graphwrstats.insertEdge++;

	estate->es_result_relation_info = savedResultRelInfo;

	if (auto_gather_graphmeta)
		agstat_count_edge_create(GraphidGetDatum(getEdgeIdDatum(edge)), start, end);

	return edge;
}

static void
enterSetPropTable(ModifyGraphState *mgstate, Datum gid, Datum newelem)
{
	ModifiedElemEntry *entry;
	bool		found;

	entry = hash_search(mgstate->elemTable, &gid, HASH_ENTER, &found);
	if (found)
	{
		if (enable_multiple_update)
			pfree(DatumGetPointer(entry->data.elem));
		else
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("graph element(%hu," UINT64_FORMAT ") has been SET multiple times",
							GraphidGetLabid(entry->key),
							GraphidGetLocid(entry->key))));
	}

	entry->data.elem = datumCopy(newelem, false, -1);
}

static void
enterDelPropTable(ModifyGraphState *mgstate, Datum elem, Oid type)
{
	Datum		gid;
	bool		found;
	ModifiedElemEntry *entry;

	if (type == VERTEXOID)
	{
		gid = getVertexIdDatum(elem);

		entry = hash_search(mgstate->elemTable, &gid, HASH_ENTER, &found);
		if (found)
			return;

		entry->data.tid =
				*((ItemPointer) DatumGetPointer(getVertexTidDatum(elem)));
	}
	else if (type == EDGEOID)
	{
		gid = getEdgeIdDatum(elem);

		entry = hash_search(mgstate->elemTable, &gid, HASH_ENTER, &found);
		if (found)
			return;
		else
		{
			Graphid eid;
			Graphid start;
			Graphid end;

			eid = GraphidGetLabid(gid);
			start = GraphidGetLabid(getEdgeStartDatum(elem));
			end = GraphidGetLabid(getEdgeEndDatum(elem));

			if (auto_gather_graphmeta)
				agstat_count_edge_delete(eid, start, end);
		}

		entry->data.tid =
				*((ItemPointer) DatumGetPointer(getEdgeTidDatum(elem)));
	}
	else if (type == VERTEXARRAYOID)
	{
		AnyArrayType *vertices;
		int			nvertices;
		int16		typlen;
		bool		typbyval;
		char		typalign;
		array_iter	it;
		int			i;
		Datum		vtx;
		bool		isnull;

		vertices = DatumGetAnyArray(elem);
		nvertices = ArrayGetNItems(AARR_NDIM(vertices), AARR_DIMS(vertices));

		get_typlenbyvalalign(AARR_ELEMTYPE(vertices), &typlen,
							 &typbyval, &typalign);

		array_iter_setup(&it, vertices);
		for (i = 0; i < nvertices; i++)
		{
			vtx = array_iter_next(&it, &isnull, i, typlen, typbyval, typalign);

			gid = getVertexIdDatum(vtx);
			entry = hash_search(mgstate->elemTable, &gid, HASH_ENTER, &found);
			if (found)
				continue;

			entry->data.tid =
					*((ItemPointer) DatumGetPointer(getVertexTidDatum(vtx)));
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
			edge = array_iter_next(&it, &isnull, i, typlen, typbyval, typalign);

			gid = getEdgeIdDatum(edge);
			entry = hash_search(mgstate->elemTable, &gid, HASH_ENTER, &found);
			if (found)
				continue;
			else
			{
				Graphid eid;
				Graphid start;
				Graphid end;

				eid = GraphidGetLabid(gid);
				start = GraphidGetLabid(getEdgeStartDatum(edge));
				end = GraphidGetLabid(getEdgeEndDatum(edge));

				if (auto_gather_graphmeta)
					agstat_count_edge_delete(eid, start, end);
			}

			entry->data.tid =
					*((ItemPointer) DatumGetPointer(getEdgeTidDatum(edge)));
		}
	}
	else
	{
		elog(ERROR, "unexpected graph type %d", type);
	}
}

static Datum
getVertexFinal(ModifyGraphState *mgstate, Datum origin)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	ModifiedElemEntry *entry;
	Datum		gid = getVertexIdDatum(origin);
	bool		found;

	entry = hash_search(mgstate->elemTable, &gid, HASH_FIND, &found);

	/* unmodified vertex */
	if (!found)
		return origin;

	if (plan->operation == GWROP_DELETE)
		return (Datum) 0;
	else
		return entry->data.elem;
}

static Datum
getEdgeFinal(ModifyGraphState *mgstate, Datum origin)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	Datum		gid = getEdgeIdDatum(origin);
	bool		found;
	ModifiedElemEntry *entry;

	entry = hash_search(mgstate->elemTable, &gid, HASH_FIND, &found);

	/* unmodified edge */
	if (!found)
		return origin;

	if (plan->operation == GWROP_DELETE)
		return (Datum) 0;
	else
		return entry->data.elem;
}

static Datum
getPathFinal(ModifyGraphState *mgstate, Datum origin)
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
	bool		isnull;
	bool		modified = false;
	bool		isdeleted = false;
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

		vertex = getVertexFinal(mgstate, value);

		if (vertex == (Datum) 0)
		{
			if (i == 0)
				isdeleted = true;

			if (isdeleted)
				continue;
			else
				elog(ERROR, "cannot delete a vertex in graphpath");
		}

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

		edge = getEdgeFinal(mgstate, value);

		if (edge == (Datum) 0)
		{
			if (isdeleted)
				continue;
			else
				elog(ERROR, "cannot delete a edge in graphpath.");
		}

		if (edge != value)
			modified = true;

		edges[i] = edge;
	}

	if (isdeleted)
		result = (Datum) 0;
	else if (modified)
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
	ModifyGraph	*plan = (ModifyGraph *) mgstate->ps.plan;
	HASH_SEQ_STATUS	seq;
	ModifiedElemEntry *entry;

	Assert(mgstate->elemTable != NULL);

	hash_seq_init(&seq, mgstate->elemTable);
	while ((entry = hash_seq_search(&seq)) != NULL)
	{
		Datum	gid = PointerGetDatum(entry->key);
		Oid		type;

		type = get_labid_typeoid(mgstate->graphid,
								 GraphidGetLabid(DatumGetGraphid(gid)));

		/* write the object to heap */
		if (plan->operation == GWROP_DELETE)
			deleteElem(mgstate, gid, &entry->data.tid, type);
		else
		{
			ItemPointer	ctid;

			ctid = updateElemProp(mgstate, type, gid, entry->data.elem);

			if (mgstate->eagerness)
			{
				Datum		property;
				Datum		newelem;

				if (type == VERTEXOID)
					property = getVertexPropDatum(entry->data.elem);
				else if (type == EDGEOID)
					property = getEdgePropDatum(entry->data.elem);
				else
					elog(ERROR, "unexpected graph type %d", type);

				newelem = makeModifiedElem(entry->data.elem, type, gid,property,
										   PointerGetDatum(ctid));

				pfree(DatumGetPointer(entry->data.elem));
				entry->data.elem = newelem;
			}
		}
	}
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
		return (Datum) 0;

	vertex = slot_getattr(slot, gvertex->resno, &isnull);
	if (isnull)
		return (Datum) 0;

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
		return (Datum) 0;

	edge = slot_getattr(slot, gedge->resno, &isnull);
	if (isnull)
		return (Datum) 0;

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

	setSlotValueByAttnum(slot, value, attno);
}

static void
setSlotValueByAttnum(TupleTableSlot *slot, Datum value, int attnum)
{
	if (slot == NULL)
		return;

	AssertArg(attnum > 0 && attnum <= slot->tts_tupleDescriptor->natts);

	slot->tts_values[attnum - 1] = value;
	slot->tts_isnull[attnum - 1] = (value == (Datum) 0) ? true : false;
}

static Datum *
makeDatumArray(ExprContext *econtext, int len)
{
	if (len == 0)
		return NULL;

	return palloc(len * sizeof(Datum));
}
