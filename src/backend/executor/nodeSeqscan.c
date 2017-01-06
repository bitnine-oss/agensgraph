/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.c
 *	  Support routines for sequential scans of relations.
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeSeqscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecSeqScan				sequentially scans a relation.
 *		ExecSeqNext				retrieve next tuple in sequential order.
 *		ExecInitSeqScan			creates and initializes a seqscan node.
 *		ExecEndSeqScan			releases any storage allocated.
 *		ExecReScanSeqScan		rescans the relation
 *
 *		ExecSeqScanEstimate		estimates DSM space needed for parallel scan
 *		ExecSeqScanInitializeDSM initialize DSM for parallel scan
 *		ExecSeqScanInitializeWorker attach to DSM info in parallel worker
 */
#include "postgres.h"

#include "access/relscan.h"
#include "catalog/pg_operator.h"
#include "executor/execdebug.h"
#include "executor/nodeSeqscan.h"
#include "optimizer/clauses.h"
#include "utils/graph.h"
#include "utils/memutils.h"
#include "utils/rel.h"

static void InitScanRelation(SeqScanState *node, EState *estate, int eflags);
static TupleTableSlot *SeqNext(SeqScanState *node);

static void InitScanLabelSkipExpr(SeqScanState *node);
static ExprState *GetScanLabelSkipExpr(SeqScanState *node, Expr *opexpr);
static bool IsGraphidColumn(SeqScanState *node, Node *expr);

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		SeqNext
 *
 *		This is a workhorse for ExecSeqScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
SeqNext(SeqScanState *node)
{
	HeapTuple	tuple;
	HeapScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;

	/*
	 * get information from the estate and scan state
	 */
	scandesc = node->ss.ss_currentScanDesc;
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	slot = node->ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		/*
		 * We reach here if the scan is not parallel, or if we're executing a
		 * scan that was intended to be parallel serially.
		 */
		scandesc = heap_beginscan(node->ss.ss_currentRelation,
								  estate->es_snapshot,
								  0, NULL);
		node->ss.ss_currentScanDesc = scandesc;
	}

	/*
	 * get the next tuple from the table
	 */
	tuple = heap_getnext(scandesc, direction);

	/*
	 * save the tuple and the buffer returned to us by the access methods in
	 * our scan tuple slot and return the slot.  Note: we pass 'false' because
	 * tuples returned by heap_getnext() are pointers onto disk pages and were
	 * not created with palloc() and so should not be pfree()'d.  Note also
	 * that ExecStoreTuple will increment the refcount of the buffer; the
	 * refcount will not be dropped until the tuple table slot is cleared.
	 */
	if (tuple)
		ExecStoreTuple(tuple,	/* tuple to store */
					   slot,	/* slot to store in */
					   scandesc->rs_cbuf,		/* buffer associated with this
												 * tuple */
					   false);	/* don't pfree this pointer */
	else
		ExecClearTuple(slot);

	return slot;
}

/*
 * SeqRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
SeqRecheck(SeqScanState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, SeqScan never use keys in heap_beginscan
	 * (and this is very bad) - so, here we do not check are keys ok or not.
	 */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecSeqScan(node)
 *
 *		Scans the relation sequentially and returns the next qualifying
 *		tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecSeqScan(SeqScanState *node)
{
	if (node->ss.ss_skipLabelScan)
	{
		node->ss.ss_skipLabelScan = false;
		return NULL;
	}

	return ExecScan((ScanState *) node,
					(ExecScanAccessMtd) SeqNext,
					(ExecScanRecheckMtd) SeqRecheck);
}

/* ----------------------------------------------------------------
 *		InitScanRelation
 *
 *		Set up to access the scan relation.
 * ----------------------------------------------------------------
 */
static void
InitScanRelation(SeqScanState *node, EState *estate, int eflags)
{
	Relation	currentRelation;

	/*
	 * get the relation object id from the relid'th entry in the range table,
	 * open that relation and acquire appropriate lock on it.
	 */
	currentRelation = ExecOpenScanRelation(estate,
								   ((SeqScan *) node->ss.ps.plan)->scanrelid,
										   eflags);

	node->ss.ss_currentRelation = currentRelation;

	/* and report the scan tuple slot's rowtype */
	ExecAssignScanType(&node->ss, RelationGetDescr(currentRelation));
}

static void
InitScanLabelSkipExpr(SeqScanState *node)
{
	List	   *qual = node->ss.ps.plan->qual;
	ListCell   *la;

	AssertArg(node->ss.ss_isLabel);

	if (qual == NIL)
		return;

	/* qual was implicitly-ANDed, so; */
	foreach(la, qual)
	{
		Expr	   *expr = lfirst(la);
		ExprState  *xstate;

		if (!is_opclause(expr))
			continue;

		if (((OpExpr *) expr)->opno != OID_GRAPHID_EQ_OP)
			continue;

		/* expr is of the form `graphid = graphid` */

		xstate = GetScanLabelSkipExpr(node, expr);
		if (xstate == NULL)
			continue;

		node->ss.ss_labelSkipExpr = xstate;
		break;
	}
}

static ExprState *
GetScanLabelSkipExpr(SeqScanState *node, Expr *opexpr)
{
	Node	   *left = get_leftop(opexpr);
	Node	   *right = get_rightop(opexpr);
	Node	   *expr;

	if (IsGraphidColumn(node, left))
		expr = right;
	else if (IsGraphidColumn(node, right))
		expr = left;
	else
		return NULL;

	/* Const or Param expected */
	if (IsA(expr, Const) || IsA(expr, Param))
		return ExecInitExpr((Expr *) expr, (PlanState *) node);

	return NULL;
}

static bool
IsGraphidColumn(SeqScanState *node, Node *expr)
{
	Var *var = (Var *) expr;

	/* TODO: use Anum_vertex_id */
	return (IsA(expr, Var) &&
			var->varno == ((SeqScan *) node->ss.ps.plan)->scanrelid &&
			var->varattno == 1);
}


/* ----------------------------------------------------------------
 *		ExecInitSeqScan
 * ----------------------------------------------------------------
 */
SeqScanState *
ExecInitSeqScan(SeqScan *node, EState *estate, int eflags)
{
	SeqScanState *scanstate;

	/*
	 * Once upon a time it was possible to have an outerPlan of a SeqScan, but
	 * not any more.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	scanstate = makeNode(SeqScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->plan.targetlist,
					 (PlanState *) scanstate);
	scanstate->ss.ps.qual = (List *)
		ExecInitExpr((Expr *) node->plan.qual,
					 (PlanState *) scanstate);

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &scanstate->ss.ps);
	ExecInitScanTupleSlot(estate, &scanstate->ss);

	/*
	 * initialize scan relation
	 */
	InitScanRelation(scanstate, estate, eflags);

	InitScanLabelInfo((ScanState *) scanstate);
	if (scanstate->ss.ss_isLabel)
		InitScanLabelSkipExpr(scanstate);

	scanstate->ss.ps.ps_TupFromTlist = false;

	/*
	 * Initialize result tuple type and projection info.
	 */
	ExecAssignResultTypeFromTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

	return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndSeqScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndSeqScan(SeqScanState *node)
{
	Relation	relation;
	HeapScanDesc scanDesc;

	/*
	 * get information from node
	 */
	relation = node->ss.ss_currentRelation;
	scanDesc = node->ss.ss_currentScanDesc;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * close heap scan
	 */
	if (scanDesc != NULL)
		heap_endscan(scanDesc);

	/*
	 * close the heap relation.
	 */
	ExecCloseScanRelation(relation);
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecReScanSeqScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanSeqScan(SeqScanState *node)
{
	HeapScanDesc scan;

	/* determine whether we can skip this label scan or not */
	if (node->ss.ss_isLabel && node->ss.ss_labelSkipExpr != NULL)
	{
		ExprContext *econtext = node->ss.ps.ps_ExprContext;
		MemoryContext oldmctx;
		bool		isnull;
		ExprDoneCond isdone;
		Datum		graphid;

		oldmctx = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

		graphid = ExecEvalExpr(node->ss.ss_labelSkipExpr, econtext,
							   &isnull, &isdone);
		if (isnull)
		{
			node->ss.ss_skipLabelScan = true;
		}
		else if (isdone == ExprSingleResult)
		{
			uint16 labid;

			labid = DatumGetUInt16(DirectFunctionCall1(graphid_labid, graphid));
			if (node->ss.ss_labid != labid)
				node->ss.ss_skipLabelScan = true;
		}

		ResetExprContext(econtext);

		MemoryContextSwitchTo(oldmctx);
	}

	scan = node->ss.ss_currentScanDesc;

	if (scan != NULL)
		heap_rescan(scan,		/* scan desc */
					NULL);		/* new scan keys */

	ExecScanReScan((ScanState *) node);
}

/* ----------------------------------------------------------------
 *						Parallel Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecSeqScanEstimate
 *
 *		estimates the space required to serialize seqscan node.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanEstimate(SeqScanState *node,
					ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;

	node->pscan_len = heap_parallelscan_estimate(estate->es_snapshot);
	shm_toc_estimate_chunk(&pcxt->estimator, node->pscan_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeDSM
 *
 *		Set up a parallel heap scan descriptor.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanInitializeDSM(SeqScanState *node,
						 ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;
	ParallelHeapScanDesc pscan;

	pscan = shm_toc_allocate(pcxt->toc, node->pscan_len);
	heap_parallelscan_initialize(pscan,
								 node->ss.ss_currentRelation,
								 estate->es_snapshot);
	shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, pscan);
	node->ss.ss_currentScanDesc =
		heap_beginscan_parallel(node->ss.ss_currentRelation, pscan);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanInitializeWorker(SeqScanState *node, shm_toc *toc)
{
	ParallelHeapScanDesc pscan;

	pscan = shm_toc_lookup(toc, node->ss.ps.plan->plan_node_id);
	node->ss.ss_currentScanDesc =
		heap_beginscan_parallel(node->ss.ss_currentRelation, pscan);
}
