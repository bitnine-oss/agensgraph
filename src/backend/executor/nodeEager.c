/*
 * nodeEager.c
 *
 * Copyright (c) 2017 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeEager.c
 */

#include "postgres.h"

#include "executor/execdebug.h"
#include "executor/nodeEager.h"
#include "miscadmin.h"
#include "utils/tuplestore.h"


/* ----------------------------------------------------------------
 *		ExecEager
 *
 *		Eagers tuples from the outer subtree of the node using tupleEager,
 *		which saves the results in a temporary file or memory. After the
 *		initial call, returns a tuple from the file with each call.
 *
 *		Conditions:
 *		  -- none.
 *
 *		Initial States:
 *		  -- the outer child is prepared to return the first tuple.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecEager(EagerState *node)
{
	EState	   *estate;
	ScanDirection dir;
	Tuplestorestate *tuplestorestate;
	TupleTableSlot *slot;
	TupleTableSlot *result;

	estate = node->ss.ps.state;
	dir = estate->es_direction;
	tuplestorestate = (Tuplestorestate *) node->tuplestorestate;

	/*
	 * If first time through, read all tuples from outer plan and pass them to
	 * tuplestore.c. Subsequent calls just fetch tuples from tuplestore.
	 */
	if (!node->child_done)
	{
		PlanState  *outerNode;
		TupleTableSlot *slot;

		/*
		 * Want to scan subplan in the forward direction.
		 */
		estate->es_direction = ForwardScanDirection;

		/*
		 * Initialize tuplestore module.
		 */
		outerNode = outerPlanState(node);

		tuplestorestate = tuplestore_begin_heap(false, false, work_mem);
		node->tuplestorestate = (void *) tuplestorestate;

		/*
		 * Scan the subplan and feed all the tuples to tuplestore.
		 */
		for (;;)
		{
			slot = ExecProcNode(outerNode);

			if (TupIsNull(slot))
				break;

			tuplestore_puttupleslot(tuplestorestate, slot);
		}

		/*
		 * restore to user specified direction
		 */
		estate->es_direction = dir;

		node->child_done = true;
	}

	/*
	 * Get the first or next tuple from tuplestore. Returns NULL if no more
	 * tuples.
	 */
	slot = node->ss.ss_ScanTupleSlot;
	(void) tuplestore_gettupleslot(tuplestorestate,
								   ScanDirectionIsForward(dir), false, slot);

	result = node->ss.ps.ps_ResultTupleSlot;
	ExecClearTuple(result);

	/* mark slot as containing a virtual tuple */
	if (!TupIsNull(slot))
	{
		int	natts = slot->tts_tupleDescriptor->natts;

		slot_getallattrs(slot);

		memcpy(result->tts_values, slot->tts_values, natts * sizeof(Datum));
		memcpy(result->tts_isnull, slot->tts_isnull, natts * sizeof(bool));

		ExecStoreVirtualTuple(result);
	}

	return result;
}

/* ----------------------------------------------------------------
 *		ExecInitEager
 *
 *		Creates the run-time state information for the Eager node
 *		produced by the planner and initializes its outer subtree.
 * ----------------------------------------------------------------
 */
EagerState *
ExecInitEager(Eager *node, EState *estate, int eflags)
{
	EagerState  *Eagerstate;

	/*
	 * create state structure
	 */
	Eagerstate = makeNode(EagerState);
	Eagerstate->ss.ps.plan = (Plan *) node;
	Eagerstate->ss.ps.state = estate;
	Eagerstate->child_done = false;

	/*
	 * tuple table initialization
	 *
	 * Eager nodes only return scan tuples from their child plan.
	 */
	ExecInitResultTupleSlot(estate, &Eagerstate->ss.ps);
	ExecInitScanTupleSlot(estate, &Eagerstate->ss);

	/*
	 * initialize child nodes
	 *
	 * We shield the child node from the need to support REWIND, BACKWARD, or
	 * MARK/RESTORE.
	 */
	eflags &= ~(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

	outerPlanState(Eagerstate) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * initialize tuple type.  no need to initialize projection info because
	 * this node doesn't do projections.
	 */
	ExecAssignResultTypeFromTL(&Eagerstate->ss.ps);
	ExecAssignScanTypeFromOuterPlan(&Eagerstate->ss);
	Eagerstate->ss.ps.ps_ProjInfo = NULL;

	return Eagerstate;
}

/* ----------------------------------------------------------------
 *		ExecEndEager(node)
 * ----------------------------------------------------------------
 */
void
ExecEndEager(EagerState *node)
{
	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ss.ss_ScanTupleSlot);
	/* must drop pointer to Eager result tuple */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	/*
	 * Release tupleEager resources
	 */
	if (node->tuplestorestate != NULL)
		tuplestore_end(node->tuplestorestate);
	node->tuplestorestate = NULL;

	/*
	 * shut down the subplan
	 */
	ExecEndNode(outerPlanState(node));
}

void
ExecReScanEager(EagerState *node)
{
	if (!node->child_done)
		return;

	/* must drop pointer to Eager result tuple */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	/*
	 * If subnode is to be rescanned then we forget previous Eager results; we
	 * have to re-read the subplan.
	 */
	node->child_done = false;
	tuplestore_end(node->tuplestorestate);
	node->tuplestorestate = NULL;
}
