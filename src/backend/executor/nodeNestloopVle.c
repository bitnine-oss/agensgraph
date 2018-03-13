/*
 * nodeNestloopVle.c
 *	  routines to support nest-loop joins for VLE
 *
 * Portions Copyright (c) 2017, Bitnine Inc.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeNestloopVle.c
 */

/*
 *	 INTERFACE ROUTINES
 *		ExecNestLoopVLE	 	- process a nestloop join of two plans
 *		ExecInitNestLoopVLE - initialize the join
 *		ExecEndNestLoopVLE 	- shut down the join
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "executor/execdebug.h"
#include "executor/nodeNestloopVle.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/graph.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"


#define OUTER_START_VARNO 	0
#define OUTER_BIND_VARNO	1
#define OUTER_ROWIDS_VARNO	2
#define OUTER_PATH_VARNO	3
#define INNER_BIND_VARNO	0
#define INNER_ROWID_VARNO	1
#define INNER_EREF_VARNO	2


static bool incrDepth(NestLoopVLEState *node);
static bool decrDepth(NestLoopVLEState *node);
static bool isMaxDepth(NestLoopVLEState *node);
static void bindNestParam(NestLoopVLE *nlv,
						  ExprContext *econtext,
						  TupleTableSlot *outerTupleSlot,
						  PlanState *innerPlan);
static TupleTableSlot *restoreStartAndBindVar(NestLoopVLEState *node);
static void storeStartAndBindVar(NestLoopVLEState *node,
								 TupleTableSlot *outerTupleSlot);
static void clearVleCtxs(dlist_head *vleCtxs);
static void copyStartAndBindVar(TupleTableSlot *dst, TupleTableSlot *src);
static void replaceResult(NestLoopVLEState *node, TupleTableSlot *slot);
static void initArray(VLEArrayExpr *array, Oid typid, ExprContext *econtext);
static Datum evalArray(VLEArrayExpr *array);
static void clearArray(VLEArrayExpr *array);
static void addElem(VLEArrayExpr *array, Datum elem);
static void popElem(VLEArrayExpr *array);
static bool hasElem(VLEArrayExpr *array, Datum elem);
static void addOuterRowidAndGid(NestLoopVLEState *node, TupleTableSlot *slot);
static void addInnerRowidAndGid(NestLoopVLEState *node, TupleTableSlot *slot);
static void addRowidAndGid(NestLoopVLEState *node, Datum rowid, Datum gid);
static void popRowidAndGid(NestLoopVLEState *node);


static TupleTableSlot *
ExecNestLoopVLE(PlanState *pstate)
{
	NestLoopVLEState *node = castNode(NestLoopVLEState, pstate);
	NestLoopVLE *nlv;
	PlanState  *innerPlan;
	PlanState  *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	TupleTableSlot *selfTupleSlot;
	ExprState  *otherqual;
	ExprContext *econtext;
	TupleTableSlot *result;

	CHECK_FOR_INTERRUPTS();

	/*
	 * get information from the node
	 */
	ENLV1_printf("getting info from node");

	nlv = (NestLoopVLE *) node->nls.js.ps.plan;
	otherqual = node->nls.js.ps.qual;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	econtext = node->nls.js.ps.ps_ExprContext;
	selfTupleSlot = node->selfTupleSlot;

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * Ok, everything is setup for the join so now loop until we return a
	 * qualifying join tuple.
	 */
	ENLV1_printf("entering main loop");

	for (;;)
	{
		/*
		 * If we don't have an outer tuple, get the next one and reset the
		 * inner scan.
		 */
		if (node->nls.nl_NeedNewOuter)
		{
			if (node->selfLoop)
			{
				ENLV1_printf("getting new self outer tuple");
				outerTupleSlot = selfTupleSlot;
			}
			else
			{
				ENLV1_printf("getting new outer tuple");
				outerTupleSlot = ExecProcNode(outerPlan);
				/*
				 * if there are no more outer tuples,
				 * then the join is complete..
				 */
				if (TupIsNull(outerTupleSlot))
				{
					ENLV1_printf("no outer tuple, ending join");
					return NULL;
				}

				addOuterRowidAndGid(node, outerTupleSlot);
			}

			ENLV1_printf("saving new outer tuple information");
			econtext->ecxt_outertuple = outerTupleSlot;

			result = NULL;

			/* in the case that minHops is 0 or 1 (starting point) */
			if (!node->selfLoop && (node->curhops >= nlv->minHops))
				result = outerTupleSlot;

			if (incrDepth(node))
			{
				node->nls.nl_NeedNewOuter = false;

				bindNestParam(nlv, econtext, outerTupleSlot, innerPlan);

				/*
				 * now rescan the inner plan
				 */
				if (node->selfLoop)
				{
					ENLV1_printf("downscanning inner plan");
					ExecDownScan(innerPlan);
				}
				ENLV1_printf("rescanning inner plan");
				node->nls.js.ps.state->es_forceReScan = true;
				ExecReScan(innerPlan);
				node->nls.js.ps.state->es_forceReScan = false;
			}

			if (result != NULL)
				return result;
		}

		/*
		 * we have an outerTuple, try to get the next inner tuple.
		 */
		ENLV1_printf("getting new inner tuple");

		innerTupleSlot = ExecProcNode(innerPlan);

		if (TupIsNull(innerTupleSlot))
		{
			decrDepth(node);
			popRowidAndGid(node);
			if (node->curCtx == NULL)
			{
				ENLV1_printf("no inner tuple, need new outer tuple");
				node->nls.nl_NeedNewOuter = true;
				node->selfLoop = false;
			}
			else
			{
				ENLV1_printf("no inner tuple, upscanning inner plan, looping");
				ExecUpScan(innerPlan);
				econtext->ecxt_outertuple = restoreStartAndBindVar(node);
				bindNestParam(nlv, econtext, econtext->ecxt_outertuple, NULL);
			}

			/*
			 * Otherwise just return to top of loop for a new outer tuple.
			 */
			continue;
		}

		econtext->ecxt_innertuple = innerTupleSlot;

		/*
		 * at this point we have a new pair of inner and outer tuples so we
		 * test the inner and outer tuples to see if they satisfy the node's
		 * qualification.
		 *
		 * Only the joinquals determine MatchedOuter status, but all quals
		 * must pass to actually return the tuple.
		 */
		ENLV1_printf("testing qualification");

		if (!hasElem(&node->rowids,
					 econtext->ecxt_innertuple->tts_values[INNER_ROWID_VARNO]))
		{
			if (otherqual == NULL || ExecQual(otherqual, econtext))
			{
				/*
				 * qualification was satisfied so we project and return the
				 * slot containing the result tuple using ExecProject().
				 */
				ENLV1_printf("qualification succeeded, projecting tuple");

				/* store current context before modifying outertuple */
				if (!isMaxDepth(node))
					storeStartAndBindVar(node, econtext->ecxt_outertuple);

				/* set outertuple of ExprContext for projection */
				econtext->ecxt_outertuple->tts_values[OUTER_BIND_VARNO]
					= econtext->ecxt_innertuple->tts_values[INNER_BIND_VARNO];
				econtext->ecxt_outertuple->tts_isnull[OUTER_BIND_VARNO]
					= econtext->ecxt_innertuple->tts_isnull[INNER_BIND_VARNO];

				result = ExecProject(node->nls.js.ps.ps_ProjInfo);

				addInnerRowidAndGid(node, econtext->ecxt_innertuple);

				/* in both [x..y] and [x..] cases */
				if (node->curhops >= nlv->minHops)
					replaceResult(node, result);

				if (isMaxDepth(node))
				{
					popRowidAndGid(node);
				}
				else
				{
					copyStartAndBindVar(selfTupleSlot, result);
					node->nls.nl_NeedNewOuter = true;
					node->selfLoop = true;
				}

				if (node->curhops >= nlv->minHops)
					return result;
			}
			else
			{
				InstrCountFiltered2(node, 1);
			}
		}
		else
		{
			InstrCountFiltered1(node, 1);
		}

		/*
		 * Tuple fails qual, so free per-tuple memory and try again.
		 */
		ResetExprContext(econtext);

		ENLV1_printf("qualification failed, looping");
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
	TupleDesc innerTupleDesc;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	NLV1_printf("ExecInitNestLoopVLE: %s\n", "initializing node");

	/*
	 * create state structure
	 */
	nlvstate = makeNode(NestLoopVLEState);
	nlvstate->nls.js.ps.plan = (Plan *) node;
	nlvstate->nls.js.ps.state = estate;
	nlvstate->nls.js.ps.ExecProcNode = ExecNestLoopVLE;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &nlvstate->nls.js.ps);

	/*
	 * initialize child expressions
	 */
	nlvstate->nls.js.ps.qual =
		ExecInitQual(node->nl.join.plan.qual, (PlanState *) nlvstate);
	nlvstate->nls.js.jointype = node->nl.join.jointype;

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

	innerTupleDesc =
			innerPlanState(nlvstate)->ps_ResultTupleSlot->tts_tupleDescriptor;
	initArray(&nlvstate->rowids,
			  innerTupleDesc->attrs[INNER_ROWID_VARNO]->atttypid,
			  nlvstate->nls.js.ps.ps_ExprContext);
	if (list_length(nlvstate->nls.js.ps.plan->targetlist) == 7)
	{
		initArray(&nlvstate->path,
				  innerTupleDesc->attrs[INNER_EREF_VARNO]->atttypid,
				  nlvstate->nls.js.ps.ps_ExprContext);
		nlvstate->hasPath = true;
	}
	else
	{
		nlvstate->hasPath = false;
	}

	/*
	 * finally, wipe the current outer tuple clean.
	 */
	nlvstate->nls.nl_NeedNewOuter = true;

	NLV1_printf("ExecInitNestLoopVLE: %s\n", "node initialized");

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
	NLV1_printf("ExecEndNestLoopVLE: %s\n", "ending node processing");

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
	clearArray(&node->rowids);
	if (node->hasPath)
		clearArray(&node->path);

	/*
	 * close down subplans
	 */
	ExecEndNode(outerPlanState(node));
	ExecEndNode(innerPlanState(node));

	NLV1_printf("ExecEndNestLoopVLE: %s\n", "node processing ended");
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

	node->nls.nl_NeedNewOuter = true;
	node->selfLoop = false;
	if (((NestLoopVLE *) node->nls.js.ps.plan)->minHops == 0)
		node->curhops = 0;
	else
		node->curhops = 1;

	ExecClearTuple(node->selfTupleSlot);
	clearVleCtxs(&node->vleCtxs);
	node->curCtx = NULL;

	clearArray(&node->rowids);
	if (node->hasPath)
		clearArray(&node->path);
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
		if (innerPlan != NULL)
			innerPlan->chgParam = bms_add_member(innerPlan->chgParam, paramno);
	}
}

static TupleTableSlot *
restoreStartAndBindVar(NestLoopVLEState *node)
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
storeStartAndBindVar(NestLoopVLEState *node, TupleTableSlot *outerTupleSlot)
{
	NestLoopVLECtx *ctx;

	if (node->curCtx != NULL && dlist_has_next(&node->vleCtxs, node->curCtx))
	{
		node->curCtx = dlist_next_node(&node->vleCtxs, node->curCtx);
		ctx = dlist_container(NestLoopVLECtx, list, node->curCtx);
		copyStartAndBindVar(ctx->slot, outerTupleSlot);
	}
	else if (node->curCtx == NULL && !dlist_is_empty(&node->vleCtxs))
	{
		node->curCtx = dlist_head_node(&node->vleCtxs);
		ctx = dlist_container(NestLoopVLECtx, list, node->curCtx);
		copyStartAndBindVar(ctx->slot, outerTupleSlot);
	}
	else
	{
		ctx = palloc(sizeof(*ctx));
		ctx->slot = MakeSingleTupleTableSlot(
				outerTupleSlot->tts_tupleDescriptor);
		copyStartAndBindVar(ctx->slot, outerTupleSlot);
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
copyStartAndBindVar(TupleTableSlot *dst, TupleTableSlot *src)
{
	Form_pg_attribute *attrs = src->tts_tupleDescriptor->attrs;
	int i;

	Assert(src->tts_tupleDescriptor->natts > 2);

	ExecClearTuple(dst);
	for (i = 0; i < 2; ++i)
	{
		if (src->tts_isnull[i])
		{
			dst->tts_values[i] = (Datum) 0;
			dst->tts_isnull[i] = true;
		}
		else
		{
			dst->tts_values[i] = datumCopy(src->tts_values[i],
										   attrs[i]->attbyval,
										   attrs[i]->attlen);
			dst->tts_isnull[i] = false;
		}
	}
	ExecStoreVirtualTuple(dst);
}

static void
replaceResult(NestLoopVLEState *node, TupleTableSlot *slot)
{
	slot->tts_values[OUTER_ROWIDS_VARNO] = evalArray(&node->rowids);
	slot->tts_isnull[OUTER_ROWIDS_VARNO] = false;
	if (node->hasPath)
	{
		slot->tts_values[OUTER_PATH_VARNO] = evalArray(&node->path);
		slot->tts_isnull[OUTER_PATH_VARNO] = false;
	}
}

#define VLEARRAY_INIT_SIZE 10
#define VLEARRAY_INCR_SIZE 10

static void
initArray(VLEArrayExpr *array, Oid typid, ExprContext *econtext)
{
	array->element_typeid = typid;
	get_typlenbyvalalign(array->element_typeid,
						 &array->elemlength,
						 &array->elembyval,
						 &array->elemalign);
	array->telems = VLEARRAY_INIT_SIZE;
	array->elements = palloc(sizeof(Datum) * array->telems);
	array->nelems = 0;
	array->econtext = econtext;
}

static Datum
evalArray(VLEArrayExpr *array)
{
	MemoryContext oldContext;
	Datum	   *values;
	bool	   *nulls;
	int			i;
	int			dims[1];
	int			lbs[1];
	ArrayType  *result;

	if (array->nelems == 0)
		return PointerGetDatum(construct_empty_array(array->element_typeid));

	oldContext = MemoryContextSwitchTo(array->econtext->ecxt_per_tuple_memory);

	values = palloc(array->nelems * sizeof(*values));
	nulls = palloc(array->nelems * sizeof(*nulls));
	for (i = 0; i < array->nelems; i++)
	{
		values[i] = array->elements[i];
		nulls[i] = false;
	}

	dims[0] = array->nelems;
	lbs[0] = 1;

	result = construct_md_array(values, nulls, 1, dims, lbs,
								array->element_typeid,
								array->elemlength,
								array->elembyval,
								array->elemalign);

	MemoryContextSwitchTo(oldContext);

	return PointerGetDatum(result);
}

static void
clearArray(VLEArrayExpr *array)
{
	if (!array->elembyval)
	{
		int i;

		for (i = 0; i < array->nelems; i++)
			pfree(DatumGetPointer(array->elements[i]));
	}

	array->nelems = 0;
}

static void
addElem(VLEArrayExpr *array, Datum elem)
{
	if (array->nelems >= array->telems)
	{
		array->telems += VLEARRAY_INCR_SIZE;
		array->elements = repalloc(array->elements,
								   sizeof(Datum) * array->telems);
	}

	array->elements[array->nelems++] = elem;
}

static void
popElem(VLEArrayExpr *array)
{
	if (array->nelems > 0)
	{
		array->nelems--;
		if (!array->elembyval)
			pfree(DatumGetPointer(array->elements[array->nelems]));
	}
}

static bool
hasElem(VLEArrayExpr *array, Datum elem)
{
	int i;

	for (i = 0; i < array->nelems; i++)
	{
		if (DatumGetBool(DirectFunctionCall2(rowid_eq,
											 array->elements[i], elem)))
			return true;
	}

	return false;
}

static void
addOuterRowidAndGid(NestLoopVLEState *node, TupleTableSlot *slot)
{
	Form_pg_attribute *attrs = slot->tts_tupleDescriptor->attrs;
	Datum		rowid = (Datum) 0;
	Datum		gid = (Datum) 0;
	IntArray	upper;
	bool		isNull;

	upper.indx[0] = 1;
	rowid = array_get_element(slot->tts_values[OUTER_ROWIDS_VARNO],
							  1, upper.indx, attrs[OUTER_ROWIDS_VARNO]->attlen,
							  node->rowids.elemlength, node->rowids.elembyval,
							  node->rowids.elemalign, &isNull);
	if (isNull)
		return;

	if (node->hasPath)
	{
		gid = array_get_element(slot->tts_values[OUTER_PATH_VARNO],
								1, upper.indx, attrs[OUTER_PATH_VARNO]->attlen,
								node->path.elemlength, node->path.elembyval,
								node->path.elemalign, &isNull);
	}

	addRowidAndGid(node, rowid, gid);
}

static void
addInnerRowidAndGid(NestLoopVLEState *node, TupleTableSlot *slot)
{
	Datum gid = (Datum) 0;

	if (node->hasPath)
		gid = slot->tts_values[INNER_EREF_VARNO];

	addRowidAndGid(node, slot->tts_values[INNER_ROWID_VARNO], gid);
}

static void
addRowidAndGid(NestLoopVLEState *node, Datum rowid, Datum gid)
{
	addElem(&node->rowids, datumCopy(rowid, node->rowids.elembyval,
									 node->rowids.elemlength));
	if (gid != (Datum) 0)
		addElem(&node->path, datumCopy(gid, node->path.elembyval,
									   node->path.elemlength));
}

static void
popRowidAndGid(NestLoopVLEState *node)
{
	popElem(&node->rowids);
	if (node->hasPath)
		popElem(&node->path);
}
