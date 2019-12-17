/*-------------------------------------------------------------------------
 *
 * nodeAppend.c
 *	  routines to handle append nodes.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeAppend.c
 *
 *-------------------------------------------------------------------------
 */
/* INTERFACE ROUTINES
 *		ExecInitAppend	- initialize the append node
 *		ExecAppend		- retrieve the next tuple from the node
 *		ExecEndAppend	- shut down the append node
 *		ExecReScanAppend - rescan the append node
 *
 *	 NOTES
 *		Each append node contains a list of one or more subplans which
 *		must be iteratively processed (forwards or backwards).
 *		Tuples are retrieved by executing the 'whichplan'th subplan
 *		until the subplan stops returning tuples, at which point that
 *		plan is shut down and the next started up.
 *
 *		Append nodes don't make use of their left and right
 *		subtrees, rather they maintain a list of subplans so
 *		a typical append node looks like this in the plan tree:
 *
 *				   ...
 *				   /
 *				Append -------+------+------+--- nil
 *				/	\		  |		 |		|
 *			  nil	nil		 ...    ...    ...
 *								 subplans
 *
 *		Append nodes are currently used for unions, and to support
 *		inheritance queries, where several relations need to be scanned.
 *		For example, in our standard person/student/employee/student-emp
 *		example, where student and employee inherit from person
 *		and student-emp inherits from student and employee, the
 *		query:
 *
 *				select name from person
 *
 *		generates the plan:
 *
 *				  |
 *				Append -------+-------+--------+--------+
 *				/	\		  |		  |		   |		|
 *			  nil	nil		 Scan	 Scan	  Scan	   Scan
 *							  |		  |		   |		|
 *							person employee student student-emp
 */

#include "postgres.h"

#include "executor/execdebug.h"
#include "executor/nodeAppend.h"
#include "lib/ilist.h"
#include "miscadmin.h"

typedef struct AppendContext
{
	dlist_node	list;
	int			whichplan;
} AppendContext;


static TupleTableSlot *ExecAppend(PlanState *pstate);
static bool exec_append_initialize_next(AppendState *appendstate);


/* ----------------------------------------------------------------
 *		exec_append_initialize_next
 *
 *		Sets up the append state node for the "next" scan.
 *
 *		Returns t iff there is a "next" scan to process.
 * ----------------------------------------------------------------
 */
static bool
exec_append_initialize_next(AppendState *appendstate)
{
	int			whichplan;

	/*
	 * get information from the append node
	 */
	whichplan = appendstate->as_whichplan;

	if (whichplan < 0)
	{
		/*
		 * if scanning in reverse, we start at the last scan in the list and
		 * then proceed back to the first.. in any case we inform ExecAppend
		 * that we are at the end of the line by returning FALSE
		 */
		appendstate->as_whichplan = 0;
		return FALSE;
	}
	else if (whichplan >= appendstate->as_nplans)
	{
		/*
		 * as above, end the scan if we go beyond the last scan in our list..
		 */
		appendstate->as_whichplan = appendstate->as_nplans - 1;
		return FALSE;
	}
	else
	{
		return TRUE;
	}
}

/* ----------------------------------------------------------------
 *		ExecInitAppend
 *
 *		Begin all of the subscans of the append node.
 *
 *	   (This is potentially wasteful, since the entire result of the
 *		append node may not be scanned, but this way all of the
 *		structures get allocated in the executor's top level memory
 *		block instead of that of the call to ExecAppend.)
 * ----------------------------------------------------------------
 */
AppendState *
ExecInitAppend(Append *node, EState *estate, int eflags)
{
	AppendState *appendstate = makeNode(AppendState);
	PlanState **appendplanstates;
	int			nplans;
	int			i;
	ListCell   *lc;

	/* check for unsupported flags */
	Assert(!(eflags & EXEC_FLAG_MARK));

	/*
	 * Lock the non-leaf tables in the partition tree controlled by this node.
	 * It's a no-op for non-partitioned parent tables.
	 */
	ExecLockNonLeafAppendTables(node->partitioned_rels, estate);

	/*
	 * Set up empty vector of subplan states
	 */
	nplans = list_length(node->appendplans);

	appendplanstates = (PlanState **) palloc0(nplans * sizeof(PlanState *));

	/*
	 * create new AppendState for our append node
	 */
	appendstate->ps.plan = (Plan *) node;
	appendstate->ps.state = estate;
	appendstate->ps.ExecProcNode = ExecAppend;
	appendstate->appendplans = appendplanstates;
	appendstate->as_nplans = nplans;

	/*
	 * Miscellaneous initialization
	 *
	 * Append plans don't have expression contexts because they never call
	 * ExecQual or ExecProject.
	 */

	/*
	 * append nodes still have Result slots, which hold pointers to tuples, so
	 * we have to initialize them.
	 */
	ExecInitResultTupleSlot(estate, &appendstate->ps);

	/*
	 * call ExecInitNode on each of the plans to be executed and save the
	 * results into the array "appendplans".
	 */
	i = 0;
	foreach(lc, node->appendplans)
	{
		Plan	   *initNode = (Plan *) lfirst(lc);

		appendplanstates[i] = ExecInitNode(initNode, estate, eflags);
		i++;
	}

	/*
	 * initialize output tuple type
	 */
	ExecAssignResultTypeFromTL(&appendstate->ps);
	appendstate->ps.ps_ProjInfo = NULL;

	/*
	 * initialize to scan first subplan
	 */
	appendstate->as_whichplan = 0;
	exec_append_initialize_next(appendstate);

	dlist_init(&appendstate->ctxs_head);
	appendstate->prev_ctx_node = &appendstate->ctxs_head.head;

	return appendstate;
}

/* ----------------------------------------------------------------
 *	   ExecAppend
 *
 *		Handles iteration over multiple subplans.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecAppend(PlanState *pstate)
{
	AppendState *node = castNode(AppendState, pstate);

	for (;;)
	{
		PlanState  *subnode;
		TupleTableSlot *result;

		CHECK_FOR_INTERRUPTS();

		/*
		 * figure out which subplan we are currently processing
		 */
		subnode = node->appendplans[node->as_whichplan];

		/*
		 * get a tuple from the subplan
		 */
		result = ExecProcNode(subnode);

		if (!TupIsNull(result))
		{
			/*
			 * If the subplan gave us something then return it as-is. We do
			 * NOT make use of the result slot that was set up in
			 * ExecInitAppend; there's no need for it.
			 */
			return result;
		}

		/*
		 * Go on to the "next" subplan in the appropriate direction. If no
		 * more subplans, return the empty slot set up for us by
		 * ExecInitAppend.
		 */
		if (ScanDirectionIsForward(node->ps.state->es_direction))
			node->as_whichplan++;
		else
			node->as_whichplan--;
		if (!exec_append_initialize_next(node))
			return ExecClearTuple(node->ps.ps_ResultTupleSlot);

		/* Else loop back and try to get a tuple from the new subplan */
	}
}

/* ----------------------------------------------------------------
 *		ExecEndAppend
 *
 *		Shuts down the subscans of the append node.
 *
 *		Returns nothing of interest.
 * ----------------------------------------------------------------
 */
void
ExecEndAppend(AppendState *node)
{
	PlanState **appendplans;
	int			nplans;
	int			i;
	dlist_mutable_iter iter;

	/*
	 * get information from the node
	 */
	appendplans = node->appendplans;
	nplans = node->as_nplans;

	/*
	 * shut down each of the subscans
	 */
	for (i = 0; i < nplans; i++)
		ExecEndNode(appendplans[i]);

	dlist_foreach_modify(iter, &node->ctxs_head)
	{
		AppendContext *ctx;

		dlist_delete(iter.cur);

		ctx = dlist_container(AppendContext, list, iter.cur);
		pfree(ctx);
	}
	node->prev_ctx_node = &node->ctxs_head.head;
}

void
ExecReScanAppend(AppendState *node)
{
	int			i;

	for (i = 0; i < node->as_nplans; i++)
	{
		PlanState  *subnode = node->appendplans[i];

		/*
		 * ExecReScan doesn't know about my subplans, so I have to do
		 * changed-parameter signaling myself.
		 */
		if (node->ps.chgParam != NULL)
			UpdateChangedParamSet(subnode, node->ps.chgParam);

		/*
		 * If chgParam of subnode is not null then plan will be re-scanned by
		 * first ExecProcNode.
		 */
		if (subnode->chgParam == NULL)
			ExecReScan(subnode);
	}
	node->as_whichplan = 0;
	exec_append_initialize_next(node);
}

void
ExecNextAppendContext(AppendState *node)
{
	dlist_node *ctx_node;
	AppendContext *ctx;
	int			i;

	/* get the current context */
	if (dlist_has_next(&node->ctxs_head, node->prev_ctx_node))
	{
		ctx_node = dlist_next_node(&node->ctxs_head, node->prev_ctx_node);
		ctx = dlist_container(AppendContext, list, ctx_node);
	}
	else
	{
		ctx = palloc(sizeof(*ctx));
		ctx_node = &ctx->list;

		dlist_push_tail(&node->ctxs_head, ctx_node);
	}

	ctx->whichplan = node->as_whichplan;

	/* make the current context previous context */
	node->prev_ctx_node = ctx_node;

	/*
	 * We don't have to restore the current as_whichplan because it is an
	 * integer value and will be initialized when the current Append is
	 * re-scanned next time.
	 */

	for (i = 0; i < node->as_nplans; i++)
		ExecNextContext(node->appendplans[i]);
}

void
ExecPrevAppendContext(AppendState *node)
{
	dlist_node *ctx_node;
	AppendContext *ctx;
	int			i;

	/*
	 * We don't have to store the current as_whichplan because of the same
	 * reason above.
	 */

	/* if chgParam is not NULL, free it now */
	if (node->ps.chgParam != NULL)
	{
		bms_free(node->ps.chgParam);
		node->ps.chgParam = NULL;
	}

	/* make the previous context current context */
	ctx_node = node->prev_ctx_node;
	Assert(ctx_node != &node->ctxs_head.head);

	if (dlist_has_prev(&node->ctxs_head, ctx_node))
		node->prev_ctx_node = dlist_prev_node(&node->ctxs_head, ctx_node);
	else
		node->prev_ctx_node = &node->ctxs_head.head;

	ctx = dlist_container(AppendContext, list, ctx_node);
	node->as_whichplan = ctx->whichplan;

	for (i = 0; i < node->as_nplans; i++)
		ExecPrevContext(node->appendplans[i]);
}
