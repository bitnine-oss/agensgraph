/*
 * execCypherSet.c
 *	  routines to handle ModifyGraph set nodes.
 *
 * Copyright (c) 2022 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/executor/execCypherSet.c
 */

#include "postgres.h"

#include "executor/execCypherSet.h"
#include "nodes/nodeFuncs.h"
#include "executor/executor.h"
#include "executor/nodeModifyGraph.h"
#include "utils/datum.h"
#include "access/tableam.h"
#include "utils/lsyscache.h"
#include "access/xact.h"

static TupleTableSlot *copyVirtualTupleTableSlot(TupleTableSlot *dstslot,
												 TupleTableSlot *srcslot);
static void findAndReflectNewestValue(ModifyGraphState *mgstate,
									  TupleTableSlot *slot);
static void updateElementTable(ModifyGraphState *mgstate, Datum gid,
							   Datum newelem);

void
AssignSetKinds(ModifyGraphState *mgstate, GSPKind kind, TupleTableSlot *slot)
{
	ExprContext *econtext = mgstate->ps.ps_ExprContext;

	ResetExprContext(econtext);
	econtext->ecxt_scantuple = slot;
	mgstate->setkind = kind;
}

TupleTableSlot *
ExecSetGraphExt(ModifyGraphState *mgstate, TupleTableSlot *slot, GSPKind kind)
{
	mgstate->setkind = kind;
	return ExecSetGraph(mgstate, slot);
}

TupleTableSlot *
ExecSetGraph(ModifyGraphState *mgstate, TupleTableSlot *slot)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	ExprContext *econtext = mgstate->ps.ps_ExprContext;
	ListCell   *ls;
	TupleTableSlot *result = mgstate->ps.ps_ResultTupleSlot;
	GSPKind		kind = mgstate->setkind;

	/*
	 * The results of previous clauses should be preserved. So, shallow
	 * copying is used.
	 */
	copyVirtualTupleTableSlot(result, slot);

	/*
	 * Reflect the newest value all types of scantuple before evaluating
	 * expression.
	 */
	findAndReflectNewestValue(mgstate, econtext->ecxt_scantuple);
	findAndReflectNewestValue(mgstate, econtext->ecxt_innertuple);
	findAndReflectNewestValue(mgstate, econtext->ecxt_outertuple);

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
		AttrNumber	attnum;

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

		/* get original graph element */
		attnum = ((Var *) gsp->es_elem->expr)->varattno;
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

		updateElementTable(mgstate, gid, newelem);

		/*
		 * To use the modified data in the next iteration, modifying the data
		 * in the ExprContext.
		 */
		setSlotValueByAttnum(econtext->ecxt_scantuple, newelem, attnum);
		setSlotValueByAttnum(econtext->ecxt_innertuple, newelem, attnum);
		setSlotValueByAttnum(econtext->ecxt_outertuple, newelem, attnum);
		setSlotValueByAttnum(result, newelem, attnum);
	}

	return (plan->last ? NULL : result);
}

static TupleTableSlot *
copyVirtualTupleTableSlot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	int			natts = srcslot->tts_tupleDescriptor->natts;

	ExecClearTuple(dstslot);
	ExecSetSlotDescriptor(dstslot, srcslot->tts_tupleDescriptor);

	/* shallow copy */
	memcpy(dstslot->tts_values, srcslot->tts_values, natts * sizeof(Datum));
	memcpy(dstslot->tts_isnull, srcslot->tts_isnull, natts * sizeof(bool));

	ExecStoreVirtualTuple(dstslot);

	return dstslot;
}

/*
 * findAndReflectNewestValue
 *
 * If a tuple with already updated exists, the data is taken from the elemTable
 * in ModifyGraphState and reflecting in the tuple data currently working on.
 */
static void
findAndReflectNewestValue(ModifyGraphState *mgstate, TupleTableSlot *slot)
{
	int			i;

	if (slot == NULL)
		return;

	for (i = 0; i < slot->tts_tupleDescriptor->natts; i++)
	{
		bool		found;
		Datum		finalValue;

		if (slot->tts_isnull[i] ||
			slot->tts_tupleDescriptor->attrs[i].attisdropped)
			continue;

		switch (slot->tts_tupleDescriptor->attrs[i].atttypid)
		{
			case VERTEXOID:
				{
					Datum		graphid = getVertexIdDatum(slot->tts_values[i]);

					finalValue = getElementFromEleTable(mgstate, graphid,
														&found);
					if (!found)
					{
						continue;
					}
				}
				break;
			case EDGEOID:
				{
					Datum		graphid = getEdgeIdDatum(slot->tts_values[i]);

					finalValue = getElementFromEleTable(mgstate, graphid,
														&found);
					if (!found)
					{
						continue;
					}
				}
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
ItemPointer
updateElemProp(ModifyGraphState *mgstate, Oid elemtype, Datum gid,
			   Datum elem_datum)
{
	EState	   *estate = mgstate->ps.state;
	TupleTableSlot *elemTupleSlot = mgstate->elemTupleSlot;
	Oid			relid;
	ItemPointer ctid;
	ResultRelInfo *resultRelInfo;
	ResultRelInfo *savedResultRelInfo;
	Relation	resultRelationDesc;
	LockTupleMode lockmode;
	TM_Result	result;
	TM_FailureData tmfd;
	bool		update_indexes;

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

	ExecMaterializeSlot(elemTupleSlot);
	elemTupleSlot->tts_tableOid = RelationGetRelid(resultRelationDesc);

	if (resultRelationDesc->rd_att->constr)
		ExecConstraints(resultRelInfo, elemTupleSlot, estate);

	result = table_tuple_update(resultRelationDesc, ctid, elemTupleSlot,
								mgstate->modify_cid + MODIFY_CID_SET,
								estate->es_snapshot,
								estate->es_crosscheck_snapshot,
								true /* wait for commit */ ,
								&tmfd, &lockmode, &update_indexes);

	switch (result)
	{
		case TM_SelfModified:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("graph element(%hu," UINT64_FORMAT ") has been SET multiple times",
							GraphidGetLabid(DatumGetGraphid(gid)),
							GraphidGetLocid(DatumGetGraphid(gid)))));
		case TM_Ok:
			break;
		case TM_Updated:
			/* TODO: A solution to concurrent update is needed. */
			ereport(ERROR,
					(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("could not serialize access due to concurrent update")));
		default:
			elog(ERROR, "unrecognized heap_update status: %u", result);
	}

	if (resultRelInfo->ri_NumIndices > 0 && update_indexes)
		ExecInsertIndexTuples(elemTupleSlot, estate, false, NULL, NIL);

	graphWriteStats.updateProperty++;

	estate->es_result_relation_info = savedResultRelInfo;

	return &elemTupleSlot->tts_tid;
}

Datum
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

static void
updateElementTable(ModifyGraphState *mgstate, Datum gid, Datum newelem)
{
	ModifiedElemEntry *entry;
	bool		found;

	entry = hash_search(mgstate->elemTable, &gid, HASH_ENTER, &found);
	if (found)
	{
		if (enable_multiple_update)
			pfree(DatumGetPointer(entry->elem));
		else
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("graph element(%hu," UINT64_FORMAT ") has been SET multiple times",
							GraphidGetLabid(entry->key),
							GraphidGetLocid(entry->key))));
	}

	entry->elem = datumCopy(newelem, false, -1);
}
