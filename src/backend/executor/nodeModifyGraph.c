/*
 * nodeModifyGraph.c
 *	  routines to handle ModifyGraph nodes.
 *
 * Copyright (c) 2022 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeModifyGraph.c
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/ag_graph_fn.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/nodeModifyTable.h"
#include "executor/nodeModifyGraph.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_relation.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "utils/arrayaccess.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "access/heapam.h"
#include "executor/execCypherCreate.h"
#include "executor/execCypherSet.h"
#include "executor/execCypherDelete.h"
#include "executor/execCypherMerge.h"
#include "catalog/ag_label_fn.h"
#include "catalog/ag_label.h"
#include "catalog/ag_edge_d.h"
#include "executor/execGraphMeta.h"
#include "tcop/tcopprot.h"
#include "utils/graph.h"
#include "utils/syscache.h"

bool		enable_multiple_update = true;
bool		auto_gather_graphmeta = false;

#define DatumGetItemPointer(X)	 ((ItemPointer) DatumGetPointer(X))

static TupleTableSlot *ExecModifyGraph(PlanState *pstate);
static void initGraphWRStats(ModifyGraphState *mgstate, GraphWriteOp op);
static List *ExecInitGraphPattern(List *pattern, ModifyGraphState *mgstate);
static List *ExecInitGraphSets(List *sets, ModifyGraphState *mgstate);
static List *ExecInitGraphDelExprs(List *exprs, ModifyGraphState *mgstate);

/* eager */
static void reflectModifiedProp(ModifyGraphState *mgstate);
static TupleTableSlot *execModifyGraphReadSubplan(ModifyGraphState *mgstate);
static void publishModifiedCid(ModifyGraphState *mgstate);
static void execModifyGraphChild(ModifyGraphState *mgstate);
static bool predrainEagerWriterWalker(PlanState *node, void *context);
static void predrainEagerWriters(PlanState *node);

/* common */
static bool isEdgeArrayOfPath(List *exprs, char *variable);

static void openResultRelInfosIndices(ModifyGraphState *mgstate);

ModifyGraphState *
ExecInitModifyGraph(ModifyGraph *mgplan, EState *estate, int eflags)
{
	TupleTableSlot *slot;
	ModifyGraphState *mgstate;
	ResultRelInfo *resultRelInfo;
	ListCell   *l;

	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	mgstate = makeNode(ModifyGraphState);
	mgstate->ps.plan = (Plan *) mgplan;
	mgstate->ps.state = estate;
	mgstate->ps.ExecProcNode = ExecModifyGraph;

	/* Tuple desc for result is the same as the subplan. */
	slot = ExecAllocTableSlot(&estate->es_tupleTable, NULL, &TTSOpsMinimalTuple);
	mgstate->ps.ps_ResultTupleSlot = slot;

	/*
	 * We don't use ExecInitResultTypeTL because we need to get the
	 * information of the subplan, not the current plan.
	 */
	mgstate->ps.ps_ResultTupleDesc = ExecTypeFromTL(mgplan->subplan->targetlist);
	ExecSetSlotDescriptor(slot, mgstate->ps.ps_ResultTupleDesc);
	ExecAssignExprContext(estate, &mgstate->ps);

	mgstate->elemTupleSlot = ExecInitExtraTupleSlot(estate, NULL,
													&TTSOpsMinimalTuple);

	mgstate->done = false;
	mgstate->child_done = false;
	mgstate->predrained = false;
	mgstate->eagerness = mgplan->eagerness;
	mgstate->modify_cid = GetCurrentCommandId(false) +
		(mgplan->nr_modify * MODIFY_CID_MAX);

	mgstate->subplan = ExecInitNode(mgplan->subplan, estate, eflags);
	Assert(mgplan->operation != GWROP_MERGE ||
		   IsA(mgstate->subplan, NestLoopState) ||

	/*
	 * The subplan may be a Result node instead of a NestLoop if a one-time
	 * filter is applied (e.g. in case of MATCH with non-existent labels in
	 * previous clause).
	 */
		   IsA(mgstate->subplan, ResultState));

	mgstate->graphid = get_graph_path_oid();
	mgstate->pattern = ExecInitGraphPattern(mgplan->pattern, mgstate);
	mgstate->exprs = ExecInitGraphDelExprs(mgplan->exprs, mgstate);

	mgstate->numResultRelInfo = list_length(mgplan->resultRelations);
	mgstate->resultRelInfo = (ResultRelInfo *)
		palloc(mgstate->numResultRelInfo * sizeof(ResultRelInfo));

	resultRelInfo = mgstate->resultRelInfo;
	foreach(l, mgplan->resultRelations)
	{
		Index		resultRelation = lfirst_int(l);

		ExecInitResultRelation(estate, resultRelInfo, resultRelation);
		resultRelInfo++;
	}

	/*
	 * Initialize any WITH CHECK OPTION constraints if needed.
	 */
	resultRelInfo = mgstate->resultRelInfo;
	foreach(l, mgplan->withCheckOptionLists)
	{
		List	   *wcoList = (List *) lfirst(l);
		List	   *wcoExprs = NIL;
		ListCell   *ll;

		foreach(ll, wcoList)
		{
			WithCheckOption *wco = (WithCheckOption *) lfirst(ll);
			ExprState  *wcoExpr = ExecInitQual((List *) wco->qual,
											   &mgstate->ps);

			wcoExprs = lappend(wcoExprs, wcoExpr);
		}

		resultRelInfo->ri_WithCheckOptions = wcoList;
		resultRelInfo->ri_WithCheckOptionExprs = wcoExprs;
		resultRelInfo++;
	}

	openResultRelInfosIndices(mgstate);

	/* For Set Operation. */
	mgstate->sets = ExecInitGraphSets(mgplan->sets, mgstate);
	mgstate->update_cols = NULL;

	/* Initialize for EPQ. */
	EvalPlanQualInit(&mgstate->mt_epqstate, estate, NULL, NIL,
					 mgplan->epqParam, mgplan->resultRelations);
	mgstate->mt_arowmarks = (List **) palloc0(sizeof(List *) * 1);
	EvalPlanQualSetPlan(&mgstate->mt_epqstate, mgplan->subplan,
						mgstate->mt_arowmarks[0]);

	/* Fill eager action information */
	if (mgstate->eagerness ||
		(mgstate->sets != NIL && enable_multiple_update) ||
		mgstate->exprs != NIL)
	{
		HASHCTL		ctl;

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
		/* We will not use eager action */
		mgstate->elemTable = NULL;
	}
	mgstate->tuplestorestate = tuplestore_begin_heap(false, false, eager_mem);

	switch (mgplan->operation)
	{
		case GWROP_CREATE:
			mgstate->execProc = ExecCreateGraph;
			break;
		case GWROP_DELETE:
			mgstate->execProc = ExecDeleteGraph;
			break;
		case GWROP_SET:
			mgstate->execProc = ExecSetGraph;
			break;
		case GWROP_MERGE:
			mgstate->execProc = ExecMergeGraph;
			break;
		default:
			elog(ERROR, "unknown operation");
	}

	initGraphWRStats(mgstate, mgplan->operation);
	return mgstate;
}

static void
reflectTupleChanges(PlanState *pstate, TupleTableSlot *result)
{
	ModifyGraphState *mgstate = castNode(ModifyGraphState, pstate);
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	TupleDesc	tupDesc = result->tts_tupleDescriptor;
	int			natts = tupDesc->natts;
	int			i;

	for (i = 0; i < natts; i++)
	{
		Oid			type;
		Datum		orig_elem;
		Datum		elem;

		if (result->tts_isnull[i])
			continue;

		orig_elem = result->tts_values[i];
		type = TupleDescAttr(tupDesc, i)->atttypid;

		/*
		 * A node or relationship whose id is NULL was bound by an OPTIONAL
		 * MATCH that did not match; it carries no modification to reflect.
		 */
		if ((type == VERTEXOID || type == EDGEOID) &&
			graphElementIdIsNull(orig_elem, type))
			continue;

		if (type == VERTEXOID)
		{
			Datum		graphid;
			bool		found;

			graphid = getVertexIdDatum(orig_elem);
			elem = getElementFromEleTable(mgstate, type, orig_elem, graphid,
										  &found);
			if (!found)
			{
				continue;
			}
		}
		else if (type == EDGEOID)
		{
			Datum		graphid;
			bool		found;

			graphid = getEdgeIdDatum(orig_elem);
			elem = getElementFromEleTable(mgstate, type, orig_elem, graphid,
										  &found);
			if (!found)
			{
				continue;
			}
		}
		else if (type == GRAPHPATHOID)
		{
			/*
			 * When deleting the graphpath, edge array of graphpath is deleted
			 * first and vertex array is deleted in the next plan. So, the
			 * graphpath must be passed to the next plan for deleting vertex
			 * array of the graphpath.
			 */
			if (isEdgeArrayOfPath(mgstate->exprs,
								  NameStr(TupleDescAttr(tupDesc, i)->attname)))
				continue;

			elem = getPathFinal(mgstate, orig_elem);
		}
		else if (type == EDGEARRAYOID && plan->operation == GWROP_DELETE)
		{
			/*
			 * The edges are used only for removal, not for result output.
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
}

/*
 * execModifyGraphReadSubplan
 *
 * Pull one tuple from this clause's input, reading at the clause's command-id
 * window so the scan observes earlier clauses' writes but not its own.  Returns
 * the input slot, or NULL when the input is exhausted.  Shared by the eager
 * (execModifyGraphChild) and streaming (ExecModifyGraph) read paths so the
 * command-id window arithmetic lives in exactly one place.
 */
static TupleTableSlot *
execModifyGraphReadSubplan(ModifyGraphState *mgstate)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	EState	   *estate = mgstate->ps.state;
	CommandId	svCid;
	TupleTableSlot *slot;

	/* ExecInsertIndexTuples() uses per-tuple context. Reset it here. */
	ResetPerTupleExprContext(estate);

	svCid = estate->es_snapshot->curcid;

	switch (plan->operation)
	{
		case GWROP_MERGE:
		case GWROP_DELETE:
			estate->es_snapshot->curcid =
				mgstate->modify_cid + MODIFY_CID_NLJOIN_MATCH;
			break;
		default:
			estate->es_snapshot->curcid =
				mgstate->modify_cid + MODIFY_CID_LOWER_BOUND;
			break;
	}

	slot = ExecProcNode(mgstate->subplan);

	estate->es_snapshot->curcid = svCid;

	return slot;
}

/*
 * publishModifiedCid
 *
 * Make this clause's writes visible to a later *reading* clause in the same
 * statement.
 *
 * Cross-clause visibility is approximated with per-clause command-id windows
 * (modify_cid = base_cid + nr_modify * MODIFY_CID_MAX).  A write clause stamps
 * the tuple versions it produces with a command id inside its own window
 * (modify_cid + MODIFY_CID_OUTPUT for CREATE/MERGE/DELETE, modify_cid +
 * MODIFY_CID_SET for SET).  Another *write* clause that follows reads through
 * execModifyGraphReadSubplan(), which raises curcid to its own (higher) window
 * for the duration of that read, so it observes those versions.
 *
 * A trailing read clause -- MATCH ... RETURN, or any non-write clause after a
 * write, e.g. "... SET p.x = 1 WITH p MATCH (p) RETURN p.x" -- is not a
 * ModifyGraph node and never sets a window: it scans the heap at the ambient
 * snapshot command id, which was captured before this statement's writes ran
 * and is therefore below the command id those writes were stamped with.  The
 * read then misses them (stale property, or a freshly CREATEd element matching
 * nothing).
 *
 * Once this clause's modifications are physically applied, advance the ambient
 * snapshot command id past the top of this clause's window so that any later
 * heap read in the statement observes them.  This mirrors the command-counter
 * bump ExecEndModifyGraph() performs for the *next* statement, brought forward
 * to the moment the writes land so it benefits a reader in *this* statement.
 *
 * The bump is monotonic (never lowers curcid) and is invisible to other write
 * clauses, which always override curcid for their own reads regardless of the
 * ambient value.  It changes no plan, adds no scan or join, and runs once per
 * clause (eager) or once per produced row (streaming), so it preserves the
 * set-based plans and is EXPLAIN-identical.
 */
static void
publishModifiedCid(ModifyGraphState *mgstate)
{
	EState	   *estate = mgstate->ps.state;
	CommandId	visible_cid = mgstate->modify_cid + MODIFY_CID_MAX;

	if (estate->es_snapshot->curcid < visible_cid)
		estate->es_snapshot->curcid = visible_cid;
}

/*
 * execModifyGraphChild
 *
 * Pull every tuple from the subplan and apply this clause's graph
 * modifications, leaving the results buffered in the tuplestore for an eager
 * node.  This is the "child" (read+write) phase of ExecModifyGraph, factored
 * out so that a later clause can force an earlier eager clause to run before
 * the later clause reads the heap (see predrainEagerWriters).
 *
 * It is safe to call more than once: the second call is a no-op because
 * child_done is set.  A non-eager node streams its rows out one at a time and
 * therefore must not be drained ahead of time; this routine is only ever
 * applied to eager nodes.
 */
static void
execModifyGraphChild(ModifyGraphState *mgstate)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;

	if (mgstate->child_done)
		return;

	for (;;)
	{
		TupleTableSlot *slot = execModifyGraphReadSubplan(mgstate);

		if (TupIsNull(slot))
			break;

		slot = mgstate->execProc(mgstate, slot);

		Assert(mgstate->eagerness);
		Assert(slot != NULL);

		tuplestore_puttupleslot(mgstate->tuplestorestate, slot);
	}

	mgstate->child_done = true;

	publishModifiedCid(mgstate);

	if (mgstate->elemTable != NULL
		&& plan->operation != GWROP_DELETE
		&& plan->operation != GWROP_SET)
		reflectModifiedProp(mgstate);
}

/*
 * predrainEagerWriters
 *
 * Before a ModifyGraph clause reads the graph, run any eager write clause that
 * precedes it in the same statement so that the earlier clause's heap changes
 * physically exist by the time this clause scans the target labels.
 *
 * AgensGraph chains write clauses by nesting the earlier clause as a subquery
 * on one side of a join in the later clause's plan.  Cross-clause visibility
 * relies on per-clause command-id windows (modify_cid): the later clause reads
 * at a command id that already sees the earlier clause's writes.  That works
 * only if the earlier write has actually happened when the later scan runs.
 * Under a streaming join (e.g. nested loop) it has, because the inner side is
 * re-read after the outer (earlier) clause produces a row.  But a hash or merge
 * join materializes one input -- which may be the later clause's scan of a
 * label the earlier clause writes -- before pulling the side that drives the
 * earlier clause.  The materialized side then captures pre-write tuple versions
 * (stale ctids and stale properties), and the subsequent update/delete targets
 * a superseded tuple, which the heap reports as "attempted to update/delete
 * invisible tuple".
 *
 * Draining the earlier eager clause up front is the same barrier openCypher
 * implementations insert between a write and a following read (Neo4j's "Eager"
 * operator).  It changes nothing about the chosen join methods, so set-based
 * hash/merge plans are preserved; it only fixes the order in which the earlier
 * writes and the later reads happen.
 */
static bool
predrainEagerWriterWalker(PlanState *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, ModifyGraphState))
	{
		ModifyGraphState *child = (ModifyGraphState *) node;

		/*
		 * A ModifyGraphState keeps its input in its own "subplan" field rather
		 * than as an ordinary left child, so planstate_tree_walker() does not
		 * descend into it.  Recurse explicitly, and do so before draining this
		 * node, so that in a chain of three or more write clauses the innermost
		 * (earliest) eager clause is drained first.
		 */
		predrainEagerWriters(child->subplan);

		if (child->eagerness && !child->child_done)
		{
			execModifyGraphChild(child);

			/*
			 * Mark the child drained so that when the parent later pulls it via
			 * ExecProcNode() its own ExecModifyGraph() does not repeat the
			 * now-redundant subplan walk.
			 */
			child->predrained = true;
		}

		return false;
	}

	if (IsA(node, GraphVLEState))
	{
		/*
		 * A GraphVLE (variable-length path expansion) likewise holds its input
		 * in a private "subplan" field that planstate_tree_walker() does not
		 * descend, so recurse into it explicitly; otherwise a write clause
		 * nested under a [*] expansion would escape the barrier.
		 */
		predrainEagerWriters(((GraphVLEState *) node)->subplan);
		return false;
	}

	/*
	 * Ordinary node: descend through its execution inputs.  The context
	 * argument is unused -- the walk threads no state -- but is required by the
	 * planstate_tree_walker() callback signature.  planstate_tree_walker() also
	 * visits initPlan/subPlan expression subqueries; those can never contain a
	 * graph write (the parser rejects writes inside expression subqueries), so
	 * the walk drains exactly the writing clauses on this statement's input
	 * spine and nothing extraneous.
	 */
	return planstate_tree_walker(node, predrainEagerWriterWalker, NULL);
}

static void
predrainEagerWriters(PlanState *node)
{
	if (node == NULL)
		return;

	/*
	 * The ModifyGraph/GraphVLE arms of the walker recurse back into this
	 * function directly rather than through planstate_tree_walker(), so guard
	 * the recursion depth here the way planstate_tree_walker() does internally.
	 */
	check_stack_depth();

	/*
	 * planstate_tree_walker() invokes the walker on the children of "node",
	 * not on "node" itself, so handle the case where "node" is directly a
	 * write clause before descending.
	 */
	(void) predrainEagerWriterWalker(node, NULL);
}

static TupleTableSlot *
ExecModifyGraph(PlanState *pstate)
{
	ModifyGraphState *mgstate = castNode(ModifyGraphState, pstate);
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;

	if (mgstate->done)
		return NULL;

	/*
	 * Force any earlier eager write clause nested in our subplan to run before
	 * we read the heap, so its modifications are already in place.  This is done
	 * once, before the first read; for a streaming node we re-enter this
	 * function per output row, so guard against repeating the tree walk.
	 */
	if (!mgstate->predrained)
	{
		mgstate->predrained = true;
		predrainEagerWriters(mgstate->subplan);
	}

	if (!mgstate->child_done)
	{
		if (mgstate->eagerness)
		{
			execModifyGraphChild(mgstate);
		}
		else
		{
			for (;;)
			{
				TupleTableSlot *slot = execModifyGraphReadSubplan(mgstate);

				if (TupIsNull(slot))
					break;

				slot = mgstate->execProc(mgstate, slot);

				/*
				 * The write for this row is now applied; make it visible to a
				 * later reading clause before we hand the row upward (the
				 * consumer may read the heap as soon as it receives it).
				 */
				publishModifiedCid(mgstate);

				if (slot != NULL)
				{
					return slot;
				}
				else
				{
					Assert(plan->last == true);
				}
			}

			mgstate->child_done = true;

			if (mgstate->elemTable != NULL
				&& plan->operation != GWROP_DELETE
				&& plan->operation != GWROP_SET)
				reflectModifiedProp(mgstate);
		}
	}

	if (mgstate->eagerness)
	{
		TupleTableSlot *result;

		/* don't care about scan direction */
		result = mgstate->ps.ps_ResultTupleSlot;
		tuplestore_gettupleslot(mgstate->tuplestorestate, true, false, result);

		if (TupIsNull(result))
			return result;

		slot_getallattrs(result);

		if (mgstate->elemTable == NULL ||
			hash_get_num_entries(mgstate->elemTable) < 1)
			return result;

		reflectTupleChanges(pstate, result);

		return result;
	}

	mgstate->done = true;

	return NULL;
}

void
ExecEndModifyGraph(ModifyGraphState *mgstate)
{
	CommandId	used_cid;

	if (mgstate->update_cols != NULL)
	{
		pfree(mgstate->update_cols);
	}

	tuplestore_end(mgstate->tuplestorestate);

	if (mgstate->elemTable != NULL)
		hash_destroy(mgstate->elemTable);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(mgstate->ps.ps_ResultTupleSlot);

	/*
	 * Terminate EPQ execution if active
	 */
	EvalPlanQualEnd(&mgstate->mt_epqstate);

	ExecEndNode(mgstate->subplan);

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
	if (mgstate->pattern != NIL)
	{
		Assert(op == GWROP_CREATE || op == GWROP_MERGE);

		graphWriteStats.insertVertex = 0;
		graphWriteStats.insertEdge = 0;
	}
	if (mgstate->exprs != NIL)
	{
		Assert(op == GWROP_DELETE);

		graphWriteStats.deleteVertex = 0;
		graphWriteStats.deleteEdge = 0;
	}
	if (mgstate->sets != NIL)
	{
		Assert(op == GWROP_SET || op == GWROP_MERGE);

		graphWriteStats.updateProperty = 0;
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

	Assert(list_length(pattern) == 1);

	gpath = linitial(pattern);

	foreach(le, gpath->chain)
	{
		Node	   *elem = lfirst(le);

		if (IsA(elem, GraphVertex))
		{
			GraphVertex *gvertex = (GraphVertex *) elem;

			gvertex->es_expr = ExecInitExpr((Expr *) gvertex->expr,
											(PlanState *) mgstate);
		}
		else
		{
			GraphEdge  *gedge = (GraphEdge *) elem;

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
	ListCell   *ls;

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
	ListCell   *lc;

	foreach(lc, exprs)
	{
		GraphDelElem *gde = lfirst(lc);

		gde->es_elem = ExecInitExpr((Expr *) gde->elem, (PlanState *) mgstate);
	}

	return exprs;
}

Datum
getElementFromEleTable(ModifyGraphState *mgstate, Oid type_oid, Datum orig_elem,
					   Datum gid, bool *found)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	ModifiedElemEntry *entry;

	entry = hash_search(mgstate->elemTable, &gid, HASH_FIND, found);

	/* Unmodified or deleted */
	if (!(*found) || plan->operation == GWROP_DELETE)
		return (Datum) 0;
	else
		return entry->elem;
}

Datum
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

	arrVertices = DatumGetAnyArrayP(vertices_datum);
	arrEdges = DatumGetAnyArrayP(edges_datum);

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
		Datum		graphid;
		bool		found;

		value = array_iter_next(&it, &isnull, i, typlen, typbyval, typalign);
		Assert(!isnull);

		graphid = getVertexIdDatum(value);
		vertex = getElementFromEleTable(mgstate, VERTEXOID, value, graphid,
										&found);

		if (!found)
		{
			vertex = value;
		}

		if (vertex == (Datum) 0)
		{
			if (i == 0)
				isdeleted = true;

			if (isdeleted)
				continue;
			else
				elog(ERROR, "cannot delete a vertex in graphpath");
		}

		if (found)
		{
			modified = true;
		}

		vertices[i] = vertex;
	}

	get_typlenbyvalalign(AARR_ELEMTYPE(arrEdges), &typlen,
						 &typbyval, &typalign);
	array_iter_setup(&it, arrEdges);
	for (i = 0; i < nedges; i++)
	{
		Datum		edge;
		Datum		graphid;
		bool		found;

		value = array_iter_next(&it, &isnull, i, typlen, typbyval, typalign);
		Assert(!isnull);

		graphid = getEdgeIdDatum(value);
		edge = getElementFromEleTable(mgstate, EDGEOID, value, graphid, &found);

		if (!found)
		{
			edge = value;
		}

		if (edge == (Datum) 0)
		{
			if (isdeleted)
				continue;
			else
				elog(ERROR, "cannot delete a edge in graphpath.");
		}

		if (found)
		{
			modified = true;
		}

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
	HASH_SEQ_STATUS seq;
	ModifiedElemEntry *entry;

	Assert(mgstate->elemTable != NULL);

	hash_seq_init(&seq, mgstate->elemTable);
	while ((entry = hash_seq_search(&seq)) != NULL)
	{
		ItemPointer ctid;
		Datum		gid = PointerGetDatum((void *) entry->key);
		Oid			type;

		type = get_labid_typeoid(mgstate->graphid,
								 GraphidGetLabid(DatumGetGraphid(gid)));

		ctid = LegacyUpdateElemProp(mgstate, type, gid, entry->elem);

		if (mgstate->eagerness)
		{
			Datum		property;
			Datum		newelem;

			if (type == VERTEXOID)
				property = getVertexPropDatum(entry->elem);
			else if (type == EDGEOID)
				property = getEdgePropDatum(entry->elem);
			else
				elog(ERROR, "unexpected graph type %d", type);

			newelem = makeModifiedElem(entry->elem, type, gid, property,
									   PointerGetDatum(ctid));

			pfree(DatumGetPointer(entry->elem));
			entry->elem = newelem;
		}
	}
}

ResultRelInfo *
getResultRelInfo(ModifyGraphState *mgstate, Oid relid)
{
	ResultRelInfo *resultRelInfo = mgstate->resultRelInfo;
	int			i;

	for (i = 0; i < mgstate->numResultRelInfo; i++)
	{
		if (RelationGetRelid(resultRelInfo->ri_RelationDesc) == relid)
			return resultRelInfo;
		resultRelInfo++;
	}

	elog(ERROR, "invalid object ID %u for the target label", relid);
}

/*
 * GraphmetaRecordEdgeInsertFromSlot
 *		Record, in the transaction's ag_graphmeta deltas, that an edge described
 *		by `slot` was created in `rel`.  No-op unless connectivity gathering is
 *		on and `rel` is an edge label.  This is how non-Cypher write paths
 *		(direct SQL DML, COPY, logical-replication apply) keep ag_graphmeta
 *		complete, so graphmeta-based scan pruning stays sound.
 *
 * Only the connectivity-adding direction (inserts, and the new endpoint of a
 * rewiring update) is recorded here, which is all that soundness requires:
 * missing a new triple could make pruning drop rows, whereas a stale leftover
 * triple only causes a harmless extra empty scan.  Deletions on these
 * non-Cypher paths are therefore not decremented and may leave ag_graphmeta
 * overstating connectivity until a regather() compacts it (never wrong, only an
 * extra scan).  The graph and edge label are taken from `rel`, not the session
 * graph_path, so this is correct regardless of the caller's graph_path.
 */
void
GraphmetaRecordEdgeInsertFromSlot(Relation rel, TupleTableSlot *slot)
{
	HeapTuple	tup;
	Form_ag_label lab;
	Oid			graph;
	Labid		edge_labid;
	Datum		startd;
	Datum		endd;
	bool		startnull;
	bool		endnull;

	if (!auto_gather_graphmeta)
		return;

	/*
	 * Resolving (is-edge-label, graph, edge labid) from the relation is
	 * invariant for a given target, so a bulk load repeats it per row.  This is
	 * left per-row deliberately: LABELRELID is syscache-backed, so after the
	 * first row it is a hash probe, and it is dominated anyway by the per-row
	 * dynahash insert in agstat_count_edge_create_ext().  A relation-keyed memo
	 * is not worth the hazard -- a stale entry surviving a DROP/CREATE relid
	 * reuse could mis-resolve an edge label as a non-edge and under-record
	 * connectivity, dropping rows (the one failure the soundness rule forbids).
	 */
	tup = SearchSysCache1(LABELRELID, ObjectIdGetDatum(RelationGetRelid(rel)));
	if (!HeapTupleIsValid(tup))
		return;					/* not a graph label */
	lab = (Form_ag_label) GETSTRUCT(tup);
	if (lab->labkind != LABEL_KIND_EDGE)
	{
		ReleaseSysCache(tup);
		return;
	}
	graph = lab->graphid;
	edge_labid = (Labid) lab->labid;
	ReleaseSysCache(tup);

	startd = slot_getattr(slot, Anum_ag_edge_start, &startnull);
	endd = slot_getattr(slot, Anum_ag_edge_end, &endnull);
	if (startnull || endnull)
		return;

	agstat_count_edge_create_ext(graph, edge_labid,
								 GraphidGetLabid(DatumGetGraphid(startd)),
								 GraphidGetLabid(DatumGetGraphid(endd)));
}

Datum
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

Datum
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

AttrNumber
findAttrInSlotByName(TupleTableSlot *slot, char *name)
{
	TupleDesc	tupDesc = slot->tts_tupleDescriptor;
	int			i;

	for (i = 0; i < tupDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupDesc, i);

		if (namestrcmp(&(attr->attname), name) == 0 && !attr->attisdropped)
			return attr->attnum;
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_NAME),
			 errmsg("variable \"%s\" does not exist", name)));
}

void
setSlotValueByName(TupleTableSlot *slot, Datum value, char *name)
{
	AttrNumber	attno;

	if (slot == NULL)
		return;

	attno = findAttrInSlotByName(slot, name);

	setSlotValueByAttnum(slot, value, attno);
}

void
setSlotValueByAttnum(TupleTableSlot *slot, Datum value, int attnum)
{
	if (slot == NULL)
		return;

	Assert(attnum > 0 && attnum <= slot->tts_tupleDescriptor->natts);

	slot->tts_values[attnum - 1] = value;
	slot->tts_isnull[attnum - 1] = (value == (Datum) 0) ? true : false;
}

Datum *
makeDatumArray(int len)
{
	if (len == 0)
		return NULL;

	return palloc(len * sizeof(Datum));
}

/*
 * markStoredGeneratedColsNull
 *
 * The Cypher write path fills a label tuple's base columns (id/start/end and
 * the jsonb property bag) directly and blanket-marks every attribute non-null.
 * Two kinds of column are not filled by that direct assignment and would leave
 * an uninitialized tts_values[] entry wrongly flagged non-null; mark both NULL
 * here so a tuple materialization (an intervening ExecMaterializeSlot, or a
 * BEFORE-ROW trigger) never reads a garbage Datum:
 *
 *   - A dropped column keeps its physical slot in the tuple descriptor but is
 *     never assigned, so its tts_values[] entry is uninitialized.  A label
 *     accumulates such dropped-column gaps from ADD/DROP-COLUMN churn; leaving
 *     a dropped varlena slot non-null makes heap_form_*_tuple dereference the
 *     garbage value and crash.  This must run even for a label with no
 *     generated columns, so it is not gated on has_generated_stored.
 *
 *   - A promoted typed property is a STORED generated column that is not filled
 *     until ExecComputeStoredGenerated runs later.
 *
 * A no-op for a plain label (no dropped columns, no promoted columns).
 */
void
markStoredGeneratedColsNull(TupleTableSlot *slot)
{
	TupleDesc	tupdesc = slot->tts_tupleDescriptor;
	bool		has_generated_stored = tupdesc->constr != NULL &&
									   tupdesc->constr->has_generated_stored;
	int			i;

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		if (att->attisdropped)
			slot->tts_isnull[i] = true;
		else if (has_generated_stored &&
				 att->attgenerated == ATTRIBUTE_GENERATED_STORED)
			slot->tts_isnull[i] = true;
	}
}

/*
 * promotedGeneratedErrorCallback
 *
 * Add a graph-aware CONTEXT line to any error the generated-column computation
 * raises.  A promoted typed property is materialized by casting the jsonb bag
 * value to the column's type, so a value that does not fit the type (e.g. SET
 * n.age = 5.5 on an integer property) fails with a bare cast error; this tells
 * the user the failure came from a promoted property of a graph label.  The
 * relation name is the label name, and reading it from the already-open
 * relation avoids any catalog access while an error is being unwound.
 */
static void
promotedGeneratedErrorCallback(void *arg)
{
	Relation	rel = (Relation) arg;

	errcontext("while computing the promoted typed columns of graph label \"%s\"",
			   RelationGetRelationName(rel));
}

/*
 * computeLabelStoredGenerated
 *
 * Materialize the STORED generated columns (promoted typed properties) of a
 * label element tuple that the Cypher write path filled directly, rather than
 * through nodeModifyTable.  A no-op for a label without promoted columns.
 *
 * This pairs with markStoredGeneratedColsNull(): a write path marks the
 * generated columns null before storing the slot (so any intervening
 * materialization is safe), then calls this just before the physical
 * insert/update to compute them from the property bag.  Routing every write
 * path through this one function keeps a new path from silently omitting the
 * computation and persisting a stale or uninitialized generated column.
 */
void
computeLabelStoredGenerated(ResultRelInfo *resultRelInfo, EState *estate,
							TupleTableSlot *slot, CmdType cmdtype)
{
	TupleConstr *constr = resultRelInfo->ri_RelationDesc->rd_att->constr;

	if (constr != NULL && constr->has_generated_stored)
	{
		ErrorContextCallback errcallback;

		/*
		 * Only consulted if the computation raises, so a successful write is
		 * unaffected; the underlying cast error stays the primary message and
		 * gains a CONTEXT line naming the graph label.
		 */
		errcallback.callback = promotedGeneratedErrorCallback;
		errcallback.arg = (void *) resultRelInfo->ri_RelationDesc;
		errcallback.previous = error_context_stack;
		error_context_stack = &errcallback;

		ExecComputeStoredGenerated(resultRelInfo, estate, slot, cmdtype);

		error_context_stack = errcallback.previous;
	}
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

/*
 * openResultRelInfosIndices
 */
static void
openResultRelInfosIndices(ModifyGraphState *mgstate)
{
	int			index;
	ResultRelInfo *resultRelInfo = mgstate->resultRelInfo;

	for (index = 0; index < mgstate->numResultRelInfo; index++)
	{
		ExecOpenIndices(resultRelInfo, false);
		resultRelInfo++;
	}
}
