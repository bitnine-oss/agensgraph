/*
 *	 INTERFACE ROUTINES
 *		ExecNestLoopVLE	 	- process a nestloop join of two plans
 *		ExecInitNestLoopVLE - initialize the join
 *		ExecEndNestLoopVLE 	- shut down the join
 */

#include "postgres.h"

#include "executor/execdebug.h"
#include "executor/nodeNestloopVle.h"
#include "utils/datum.h"
#include "utils/memutils.h"
#include "catalog/pg_type.h"
#include "nodes/pg_list.h"


static bool incrDepth(NestLoopVLEState *node);
static bool decrDepth(NestLoopVLEState *node);
static bool isMaxDepth(NestLoopVLEState *node);
static void bindNestParam(NestLoopVLE *nlv,
						  ExprContext *econtext,
						  TupleTableSlot *outerTupleSlot,
						  PlanState *innerPlan);
static TupleTableSlot *upContext(NestLoopVLEState *node);
static void downContext(NestLoopVLEState *node,
						TupleTableSlot *outerTupleSlot);
static void clearVleCtxs(dlist_head *vleCtxs);
static void copySlot(TupleTableSlot *dst, TupleTableSlot *src);


TupleTableSlot *
ExecNestLoopVLE(NestLoopVLEState *node)
{
	NestLoopVLE *nlv;
	PlanState  *innerPlan;
	PlanState  *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	TupleTableSlot *selfTupleSlot;
	List	   *joinqual;
	List	   *otherqual;
	ExprContext *econtext;
	TupleTableSlot *result;
	ExprDoneCond isDone;

	/*
	 * get information from the node
	 */
	ENL1_printf("getting info from node");

	nlv = (NestLoopVLE *) node->nls.js.ps.plan;
	joinqual = node->nls.js.joinqual;
	otherqual = node->nls.js.ps.qual;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	econtext = node->nls.js.ps.ps_ExprContext;
	selfTupleSlot = node->selfTupleSlot;

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.  Note this can't happen
	 * until we're done projecting out tuples from a join tuple.
	 */
	ResetExprContext(econtext);

	/*
	 * Ok, everything is setup for the join so now loop until we return a
	 * qualifying join tuple.
	 */
	ENL1_printf("entering main loop");

	for (;;)
	{
		/*
		 * If we don't have an outer tuple, get the next one and reset the
		 * inner scan.
		 */
		if (node->nls.nl_NeedNewOuter)
		{
			ENL1_printf("getting new outer tuple");

			if (node->selfLoop)
				outerTupleSlot = selfTupleSlot;
			else
			{
				outerTupleSlot = ExecProcNode(outerPlan);
				/*
				 * if there are no more outer tuples, then the join is complete..
				 */
				if (TupIsNull(outerTupleSlot))
				{
					ENL1_printf("no outer tuple, ending join");
					return NULL;
				}
			}

			ENL1_printf("saving new outer tuple information");
			econtext->ecxt_outertuple = outerTupleSlot;

			result = NULL;
			if (! node->selfLoop && (node->curhops >= nlv->minHops))
			{
				econtext->ecxt_innertuple = node->nls.nl_NullInnerTupleSlot;
				result = ExecProject(node->nls.js.ps.ps_ProjInfo, &isDone);
			}

			if (incrDepth(node))
			{
				node->nls.nl_NeedNewOuter = false;

				bindNestParam(nlv, econtext, outerTupleSlot, innerPlan);

				/*
				 * now rescan the inner plan
				 */
				ENL1_printf("rescanning inner plan");
				if (node->selfLoop)
					ExecDownScan(innerPlan);
				ExecReScan(innerPlan);
			}

			if (result)
				return result;
		}

		/*
		 * we have an outerTuple, try to get the next inner tuple.
		 */
		ENL1_printf("getting new inner tuple");

		innerTupleSlot = ExecProcNode(innerPlan);
		econtext->ecxt_innertuple = innerTupleSlot;
		econtext->ecxt_outertuple->tts_isnull[1] = true;

		if (TupIsNull(innerTupleSlot))
		{
			ENL1_printf("no inner tuple, need new outer tuple");

			decrDepth(node);
			if (node->curCtx == NULL)
			{
				node->nls.nl_NeedNewOuter = true;
				node->selfLoop = false;
			}
			else
			{
				ExecUpScan(innerPlan);
				econtext->ecxt_outertuple = upContext(node);
				bindNestParam(nlv, econtext, econtext->ecxt_outertuple, NULL);
			}

			/*
			 * Otherwise just return to top of loop for a new outer tuple.
			 */
			continue;
		}

		/*
		 * at this point we have a new pair of inner and outer tuples so we
		 * test the inner and outer tuples to see if they satisfy the node's
		 * qualification.
		 *
		 * Only the joinquals determine MatchedOuter status, but all quals
		 * must pass to actually return the tuple.
		 */
		ENL1_printf("testing qualification");

		if (ExecQual(joinqual, econtext, false))
		{
			if (otherqual == NIL || ExecQual(otherqual, econtext, false))
			{
				/*
				 * qualification was satisfied so we project and return the
				 * slot containing the result tuple using ExecProject().
				 */
				ENL1_printf("qualification succeeded, projecting tuple");

				result = ExecProject(node->nls.js.ps.ps_ProjInfo, &isDone);

				if (! isMaxDepth(node))
				{
					copySlot(selfTupleSlot, result);
					downContext(node, econtext->ecxt_outertuple);
					node->nls.nl_NeedNewOuter = true;
					node->selfLoop = true;
				}

				return result;
			}
			else
				InstrCountFiltered2(node, 1);
		}
		else
			InstrCountFiltered1(node, 1);

		/*
		 * Tuple fails qual, so free per-tuple memory and try again.
		 */
		ResetExprContext(econtext);

		ENL1_printf("qualification failed, looping");
	}
}

/* ----------------------------------------------------------------
 *		ExecInitNestLoopVLE
 * ----------------------------------------------------------------
 */
NestLoopVLEState *
ExecInitNestLoopVLE(NestLoopVLE *node, EState *estate, int eflags)
{
	NestLoopVLEState *nlvstate;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	NL1_printf("ExecInitNestLoopVLE: %s\n",
			   "initializing node");

	/*
	 * create state structure
	 */
	nlvstate = makeNode(NestLoopVLEState);
	nlvstate->nls.js.ps.plan = (Plan *) node;
	nlvstate->nls.js.ps.state = estate;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &nlvstate->nls.js.ps);

	/*
	 * initialize child expressions
	 */
	nlvstate->nls.js.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->nl.join.plan.targetlist,
					 (PlanState *) nlvstate);
	nlvstate->nls.js.ps.qual = (List *)
		ExecInitExpr((Expr *) node->nl.join.plan.qual,
					 (PlanState *) nlvstate);
	nlvstate->nls.js.jointype = node->nl.join.jointype;
	nlvstate->nls.js.joinqual = (List *)
		ExecInitExpr((Expr *) node->nl.join.joinqual,
					 (PlanState *) nlvstate);

	/*
	 * initialize child nodes
	 *
	 * If we have no parameters to pass into the inner rel from the outer,
	 * tell the inner child that cheap rescans would be good.  If we do have
	 * such parameters, then there is no point in REWIND support at all in the
	 * inner child, because it will always be rescanned with fresh parameter
	 * values.
	 */
	outerPlanState(nlvstate) = ExecInitNode(outerPlan(node), estate, eflags);
	if (node->nl.nestParams == NIL)
		eflags |= EXEC_FLAG_REWIND;
	else
		eflags &= ~EXEC_FLAG_REWIND;
	innerPlanState(nlvstate) = ExecInitNode(innerPlan(node), estate, eflags);

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &nlvstate->nls.js.ps);
	nlvstate->selfTupleSlot = ExecInitExtraTupleSlot(estate);
	nlvstate->nls.nl_NullInnerTupleSlot = ExecInitNullTupleSlot(
			estate, ExecGetResultType(innerPlanState(nlvstate)));

	if (node->nl.join.jointype != JOIN_VLE)
		elog(ERROR, "unrecognized join type: %d", (int) node->nl.join.jointype);

	/*
	 * initialize tuple type and projection info
	 */
	ExecAssignResultTypeFromTL(&nlvstate->nls.js.ps);
	ExecAssignProjectionInfo(&nlvstate->nls.js.ps, NULL);

	ExecSetSlotDescriptor(
			nlvstate->selfTupleSlot,
			nlvstate->nls.js.ps.ps_ResultTupleSlot->tts_tupleDescriptor);

	nlvstate->selfLoop = false;
	nlvstate->curhops = (node->minHops == 0) ? 0 : 1;

	/*
	 * finally, wipe the current outer tuple clean.
	 */
	nlvstate->nls.js.ps.ps_TupFromTlist = false;
	nlvstate->nls.nl_NeedNewOuter = true;

	NL1_printf("ExecInitNestLoopVLE: %s\n",
			   "node initialized");

	return nlvstate;
}

/* ----------------------------------------------------------------
 *		ExecEndNestLoopVLE
 *
 *		closes down scans and frees allocated storage
 * ----------------------------------------------------------------
 */
void
ExecEndNestLoopVLE(NestLoopVLEState *node)
{
	NL1_printf("ExecEndNestLoopVLE: %s\n",
			   "ending node processing");

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->nls.js.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->nls.js.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->selfTupleSlot);
	clearVleCtxs(&node->vleCtxs);

	/*
	 * close down subplans
	 */
	ExecEndNode(outerPlanState(node));
	ExecEndNode(innerPlanState(node));

	NL1_printf("ExecEndNestLoopVLE: %s\n",
			   "node processing ended");
}

/* ----------------------------------------------------------------
 *		ExecReScanNestLoopVLE
 * ----------------------------------------------------------------
 */
void
ExecReScanNestLoopVLE(NestLoopVLEState *node)
{
	PlanState  *outerPlan = outerPlanState(node);

	/*
	 * If outerPlan->chgParam is not null then plan will be automatically
	 * re-scanned by first ExecProcNode.
	 */
	if (outerPlan->chgParam == NULL)
		ExecReScan(outerPlan);

	/*
	 * innerPlan is re-scanned for each new outer tuple and MUST NOT be
	 * re-scanned from here or you'll get troubles from inner index scans when
	 * outer Vars are used as run-time keys...
	 */

	node->nls.js.ps.ps_TupFromTlist = false;
	node->nls.nl_NeedNewOuter = true;
	node->selfLoop = false;
	node->curhops = ((NestLoopVLE *) node->nls.js.ps.plan)->minHops == 0
		? 0 : 1;

	ExecClearTuple(node->selfTupleSlot);
	clearVleCtxs(&node->vleCtxs);
	node->curCtx = NULL;
}

static bool
incrDepth(NestLoopVLEState *node)
{
	NestLoopVLE *nlv = (NestLoopVLE *) node->nls.js.ps.plan;

	if (nlv->maxHops != -1 && node->curhops >= nlv->maxHops)
		return false;
	node->curhops++;
	return true;
}

static bool
decrDepth(NestLoopVLEState *node)
{
	NestLoopVLE *nlv = (NestLoopVLE *) node->nls.js.ps.plan;
	int base = (nlv->minHops == 0) ? 0 : 1;

	if (node->curhops <= base)
		return false;
	node->curhops--;
	return true;
}

static bool
isMaxDepth(NestLoopVLEState *node)
{
	NestLoopVLE *nlv = (NestLoopVLE *) node->nls.js.ps.plan;
	return (node->curhops == nlv->maxHops);
}

/*
 * fetch the values of any outer Vars that must be passed to the
 * inner scan, and store them in the appropriate PARAM_EXEC slots.
 */
static void
bindNestParam(NestLoopVLE *nlv,
			  ExprContext *econtext,
			  TupleTableSlot *outerTupleSlot,
			  PlanState *innerPlan)
{
	ListCell *lc;

	foreach(lc, nlv->nl.nestParams)
	{
		NestLoopParam *nlp = (NestLoopParam *) lfirst(lc);
		int			paramno = nlp->paramno;
		ParamExecData *prm;

		prm = &(econtext->ecxt_param_exec_vals[paramno]);
		/* Param value should be an OUTER_VAR var */
		Assert(IsA(nlp->paramval, Var));
		Assert(nlp->paramval->varno == OUTER_VAR);
		Assert(nlp->paramval->varattno > 0);
		prm->value = slot_getattr(outerTupleSlot,
								  nlp->paramval->varattno,
								  &(prm->isnull));
		/* Flag parameter value as changed */
		if (innerPlan)
			innerPlan->chgParam = bms_add_member(innerPlan->chgParam, paramno);
	}
}
static TupleTableSlot *
upContext(NestLoopVLEState *node)
{
	NestLoopVLECtx *ctx;

	ctx = dlist_container(NestLoopVLECtx, list, node->curCtx);
	if (dlist_has_prev(&node->vleCtxs, node->curCtx))
		node->curCtx = dlist_prev_node(&node->vleCtxs, node->curCtx);
	else
		node->curCtx = NULL;
	return ctx->slot;
}


static void
downContext(NestLoopVLEState *node, TupleTableSlot *outerTupleSlot)
{
	NestLoopVLECtx *ctx;

	if (node->curCtx && dlist_has_next(&node->vleCtxs, node->curCtx))
	{
		node->curCtx = dlist_next_node(&node->vleCtxs, node->curCtx);
		ctx = dlist_container(NestLoopVLECtx, list, node->curCtx);
		copySlot(ctx->slot, outerTupleSlot);
	}
	else if (! node->curCtx && ! dlist_is_empty(&node->vleCtxs))
	{
		node->curCtx = dlist_head_node(&node->vleCtxs);
		ctx = dlist_container(NestLoopVLECtx, list, node->curCtx);
		copySlot(ctx->slot, outerTupleSlot);
	}
	else
	{
		ctx = (NestLoopVLECtx *) palloc(sizeof(NestLoopVLECtx));
		ctx->slot = MakeSingleTupleTableSlot(
				outerTupleSlot->tts_tupleDescriptor);
		copySlot(ctx->slot, outerTupleSlot);
		dlist_push_tail(&node->vleCtxs, &ctx->list);
		node->curCtx = dlist_tail_node(&node->vleCtxs);
	}
}

static void
clearVleCtxs(dlist_head *vleCtxs)
{
	dlist_mutable_iter miter;

	dlist_foreach_modify(miter, vleCtxs)
	{
		NestLoopVLECtx *ctx = dlist_container(NestLoopVLECtx, list, miter.cur);
		dlist_delete(miter.cur);
		ExecDropSingleTupleTableSlot(ctx->slot);
		pfree(ctx);
	}
	dlist_init(vleCtxs);
}

static void
copySlot(TupleTableSlot *dst, TupleTableSlot *src)
{
	int i;
	Form_pg_attribute *attrs = src->tts_tupleDescriptor->attrs;
	int natts = src->tts_tupleDescriptor->natts;

	ExecClearTuple(dst);
	for (i = 0; i < natts; ++i)
	{
		dst->tts_values[i] = datumCopy(
				src->tts_values[i], attrs[i]->attbyval, attrs[i]->attlen);
		dst->tts_isnull[i] = src->tts_isnull[i];
	}
	ExecStoreVirtualTuple(dst);
}
