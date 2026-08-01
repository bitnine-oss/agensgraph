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
#include "catalog/ag_vertex_d.h"
#include "catalog/ag_edge_d.h"
#include "commands/trigger.h"
#include "tcop/tcopprot.h"

#define DatumGetItemPointer(X)	 ((ItemPointer) DatumGetPointer(X))

static TupleTableSlot *copyVirtualTupleTableSlot(TupleTableSlot *dstslot,
												 TupleTableSlot *srcslot);
static void findAndReflectNewestValue(ModifyGraphState *mgstate,
									  TupleTableSlot *slot);
static void updateElementTable(ModifyGraphState *mgstate, Datum gid,
							   Datum newelem);
static Datum GraphTableTupleUpdate(ModifyGraphState *mgstate,
								   Oid tts_value_type, Datum tts_value,
								   int attidx);
static void fillLabelTupleSlot(TupleTableSlot *elemTupleSlot, Relation rel,
							   Oid tts_value_type, Datum tts_value);
static void checkPropMapIsObject(Datum prop_map);
static void takeBackRecheckSlots(ModifyGraphState *mgstate);
static TupleTableSlot *lendRecheckSlot(ModifyGraphState *mgstate,
									   Relation rel, Index rti);
static TupleTableSlot *recheckElement(ModifyGraphState *mgstate,
									  Index scanrelid, bool *readwhatwelent);

/*
 * LegacyExecSetGraph
 *
 * It is used for Merge statements or Eager.
 */
TupleTableSlot *
LegacyExecSetGraph(ModifyGraphState *mgstate, TupleTableSlot *slot, GSPKind kind)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	ExprContext *econtext = mgstate->ps.ps_ExprContext;
	ListCell   *ls;
	TupleTableSlot *result = mgstate->ps.ps_ResultTupleSlot;

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

/*
 * ExecSetGraph
 */
TupleTableSlot *
ExecSetGraph(ModifyGraphState *mgstate, TupleTableSlot *slot)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	bool	   *update_cols = mgstate->update_cols;
	int			i;
	ListCell   *lc;

	if (update_cols == NULL)
	{
		update_cols = palloc(sizeof(bool) * slot->tts_tupleDescriptor->natts);
		mgstate->update_cols = update_cols;

		for (i = 0; i < slot->tts_tupleDescriptor->natts; i++)
		{
			update_cols[i] = false;
			foreach(lc, mgstate->sets)
			{
				char	   *attr_name =
					NameStr(TupleDescAttr(slot->tts_tupleDescriptor, i)->attname);
				GraphSetProp *gsp = lfirst(lc);

				if (strcmp(gsp->variable, attr_name) == 0)
				{
					update_cols[i] = true;
					break;
				}
			}
		}
	}

	for (i = 0; i < slot->tts_tupleDescriptor->natts; i++)
	{
		if (update_cols[i])
		{
			Datum		cur_datum = slot->tts_values[i];
			Oid			element_type = TupleDescAttr(slot->tts_tupleDescriptor, i)->atttypid;
			Datum		affected_datum;

			/*
			 * The element to update may have no value -- for instance one
			 * bound by an OPTIONAL MATCH that did not match.  That shows up
			 * either as a NULL column or as a node/relationship row whose id
			 * is NULL. Either way there is nothing to update, and
			 * dereferencing it would crash, so skip it.
			 */
			if (slot->tts_isnull[i])
				continue;
			if ((element_type == VERTEXOID || element_type == EDGEOID) &&
				graphElementIdIsNull(cur_datum, element_type))
				continue;

			affected_datum = GraphTableTupleUpdate(mgstate,
												   element_type,
												   cur_datum,
												   i);

			if (affected_datum != (Datum) 0)
			{
				slot->tts_values[i] = affected_datum;
			}
		}
	}

	return (plan->last ? NULL : slot);
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
		Oid			type_oid;

		if (slot->tts_isnull[i] ||
			TupleDescAttr(slot->tts_tupleDescriptor, i)->attisdropped)
			continue;

		type_oid = TupleDescAttr(slot->tts_tupleDescriptor, i)->atttypid;

		/*
		 * A node or relationship whose id is NULL was bound by an OPTIONAL
		 * MATCH that did not match; it carries no modification to reflect.
		 */
		if ((type_oid == VERTEXOID || type_oid == EDGEOID) &&
			graphElementIdIsNull(slot->tts_values[i], type_oid))
			continue;

		switch (type_oid)
		{
			case VERTEXOID:
				{
					Datum		graphid = getVertexIdDatum(slot->tts_values[i]);

					finalValue = getElementFromEleTable(mgstate, type_oid, 0,
														graphid,
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

					finalValue = getElementFromEleTable(mgstate, type_oid, 0,
														graphid,
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

/*
 * fillLabelTupleSlot
 *		Build, in `elemTupleSlot', the row of `rel' that the vertex or edge
 *		`tts_value' describes.
 *
 * A label's columns are not those of a vertex or an edge, so the row is filled
 * in by hand rather than by a projection.  Anything already in the slot is
 * dropped first, so this can be used to rebuild the row from a re-examined
 * element: materializing a slot that already holds a tuple keeps that tuple
 * rather than rebuilding it from the values, so a second pass that only wrote
 * the values would persist the first pass's row.
 */
static void
fillLabelTupleSlot(TupleTableSlot *elemTupleSlot, Relation rel,
				   Oid tts_value_type, Datum tts_value)
{
	Datum	   *tts_values;

	ExecClearTuple(elemTupleSlot);
	ExecSetSlotDescriptor(elemTupleSlot, RelationGetDescr(rel));

	tts_values = elemTupleSlot->tts_values;

	if (tts_value_type == VERTEXOID)
	{
		tts_values[Anum_ag_vertex_id - 1] = getVertexIdDatum(tts_value);
		tts_values[Anum_ag_vertex_properties - 1] = getVertexPropDatum(tts_value);
	}
	else
	{
		Assert(tts_value_type == EDGEOID);

		tts_values[Anum_ag_edge_id - 1] = getEdgeIdDatum(tts_value);
		tts_values[Anum_ag_edge_start - 1] = getEdgeStartDatum(tts_value);
		tts_values[Anum_ag_edge_end - 1] = getEdgeEndDatum(tts_value);
		tts_values[Anum_ag_edge_properties - 1] = getEdgePropDatum(tts_value);
	}

	checkPropMapIsObject(tts_values[(tts_value_type == VERTEXOID ?
									 Anum_ag_vertex_properties :
									 Anum_ag_edge_properties) - 1]);

	markUnassignedLabelColsNull(elemTupleSlot,
								tts_value_type == VERTEXOID ?
								Anum_table_vertex_prop_map :
								Anum_table_edge_prop_map);
	ExecStoreVirtualTuple(elemTupleSlot);
}

/*
 * checkPropMapIsObject
 *
 * A property map is an object.  Anything else -- a scalar, an array -- has no
 * properties to store, so a row built from it would answer nothing for every
 * key it used to hold, and every column derived from the map would go null
 * along with it.  Creating an element already refuses this; a write that
 * replaces the map of one that exists has to refuse it for the same reason.
 */
static void
checkPropMapIsObject(Datum prop_map)
{
	if (!JB_ROOT_IS_OBJECT(DatumGetJsonbP(prop_map)))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("jsonb object is expected for property map")));
}

/*
 * takeBackRecheckSlots
 *		Undo every substitution a previous re-examination set up.
 *
 * A re-examination names the scans it is asking about: the one whose row it is
 * re-fetching, and the other members of that row's inheritance set, which it
 * stands in for with nothing.  Neither mark means anything to the element
 * re-examined next -- one silences a scan the next element needs to read, the
 * other answers for a scan the next element is asking about -- so the whole set
 * is taken back here and built again from nothing for each element.
 *
 * A slot cannot be left in place and merely emptied.  A scan whose entry is set
 * reads what the entry holds and nothing else, so an emptied entry turns the
 * scan into one that finds no rows.  Only the pointer is taken out; the slot
 * itself is kept for the next element that re-examines through the same scan,
 * so a statement's slots are as many as its scans rather than as many as its
 * conflicts.
 */
static void
takeBackRecheckSlots(ModifyGraphState *mgstate)
{
	EPQState   *epqstate = &mgstate->mt_epqstate;
	int			rti = -1;

	while ((rti = bms_next_member(mgstate->mt_epq_lent, rti)) >= 0)
	{
		ExecClearTuple(mgstate->mt_epq_slot[rti - 1]);
		epqstate->relsubs_slot[rti - 1] = NULL;

		/*
		 * These only exist once a recheck plan has been built, which is not yet
		 * the case the first time round -- and then nothing is lent either.
		 */
		if (epqstate->relsubs_blocked != NULL)
		{
			epqstate->relsubs_blocked[rti - 1] = false;
			epqstate->relsubs_done[rti - 1] = false;
		}
	}

	bms_free(mgstate->mt_epq_lent);
	mgstate->mt_epq_lent = NULL;
}

/*
 * lendRecheckSlot
 *		The slot the coming re-examination reads for range table index `rti',
 *		emptied and lent to the recheck state.
 *
 * Left empty this stands in for a scan with nothing; filled with a row it
 * supplies that row in place of what the scan would have read.
 */
static TupleTableSlot *
lendRecheckSlot(ModifyGraphState *mgstate, Relation rel, Index rti)
{
	EPQState   *epqstate = &mgstate->mt_epqstate;
	Index		rtsize = mgstate->ps.state->es_range_table_size;

	Assert(rti > 0 && rti <= rtsize);

	if (mgstate->mt_epq_slot == NULL)
		mgstate->mt_epq_slot = (TupleTableSlot **)
			palloc0(rtsize * sizeof(TupleTableSlot *));

	if (mgstate->mt_epq_slot[rti - 1] == NULL)
	{
		/*
		 * No slot for this scan yet.  EvalPlanQualSlot builds one and keeps it
		 * in the recheck state's own entry, which is where it is taken from.
		 */
		Assert(epqstate->relsubs_slot[rti - 1] == NULL);
		mgstate->mt_epq_slot[rti - 1] = EvalPlanQualSlot(epqstate, rel, rti);
	}

	ExecClearTuple(mgstate->mt_epq_slot[rti - 1]);
	epqstate->relsubs_slot[rti - 1] = mgstate->mt_epq_slot[rti - 1];
	mgstate->mt_epq_lent = bms_add_member(mgstate->mt_epq_lent, rti);

	return mgstate->mt_epq_slot[rti - 1];
}

/*
 * recheckElement
 *		Re-run the subplan and return the row it now yields, with the scan named
 *		by `scanrelid' reading what has been lent to it.
 *
 * *readwhatwelent reports whether the scan actually read the row lent to it, so
 * the caller can tell an answer about this row from an answer about some other.
 *
 * This is EvalPlanQual() without the mark it leaves behind on completion.  That
 * mark records that the substituted range table index has nothing further to
 * supply, and the next run honours it; upstream the index is always the
 * statement's target relation, which had that mark to begin with, but here it is
 * a scan, and silencing it would answer the next element's re-examination with
 * an empty side of the join.  The whole substitution set is built afresh for
 * every element instead, and given back as soon as the run is over, so nothing
 * about one element's re-examination is in place for the next one's.
 */
static TupleTableSlot *
recheckElement(ModifyGraphState *mgstate, Index scanrelid,
			   bool *readwhatwelent)
{
	EPQState   *epqstate = &mgstate->mt_epqstate;
	TupleTableSlot *slot;

	/* build or reset the recheck plan */
	EvalPlanQualBegin(epqstate);

	/* the row lent for this scan is there to be read */
	epqstate->relsubs_done[scanrelid - 1] = false;
	epqstate->relsubs_blocked[scanrelid - 1] = false;

	slot = EvalPlanQualNext(epqstate);

	/*
	 * Hold the row independently of the recheck plan's own state, which the
	 * next re-examination reuses -- and of the lent slots, which are given back
	 * below.
	 */
	if (!TupIsNull(slot))
		ExecMaterializeSlot(slot);

	*readwhatwelent = epqstate->relsubs_done[scanrelid - 1];

	takeBackRecheckSlots(mgstate);

	return slot;
}

/*
 * GraphTableTupleUpdate
 * 		Update the tuple in the graph table.
 *
 * See ExecUpdate()
 */
static Datum
GraphTableTupleUpdate(ModifyGraphState *mgstate, Oid tts_value_type,
					  Datum tts_value, int attidx)
{
	EState	   *estate = mgstate->ps.state;
	EPQState   *epqstate = &mgstate->mt_epqstate;
	TupleTableSlot *elemTupleSlot = mgstate->elemTupleSlot;
	Datum	   *tts_values;
	ResultRelInfo *resultRelInfo;
	Relation	resultRelationDesc;
	LockTupleMode lockmode;
	TM_Result	result;
	TM_FailureData tmfd;
	TU_UpdateIndexes update_indexes;
	Datum		gid;
	Oid			relid;
	ItemPointer ctid;
	bool		hash_found;
	ModifiedElemEntry *entry;
	Datum		inserted_datum;
	List	   *recheckIndexes = NIL;
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	TupleTableSlot *epqslot;
	TupleTableSlot *inputslot;
	List	   *scans;
	ListCell   *ls;
	Index		scanrelid;
	bool		readwhatwelent;

	if (tts_value_type == VERTEXOID)
	{
		gid = getVertexIdDatum(tts_value);
		ctid = DatumGetItemPointer(getVertexTidDatum(tts_value));
	}
	else
	{
		ctid = DatumGetItemPointer(getEdgeTidDatum(tts_value));
		gid = getEdgeIdDatum(tts_value);
	}

	hash_search(mgstate->elemTable, &gid, HASH_FIND,
				&hash_found);
	if (hash_found)
	{
		if (!enable_multiple_update)
		{
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("graph element(%hu," UINT64_FORMAT ") has been SET multiple times",
							GraphidGetLabid(DatumGetGraphid(gid)),
							GraphidGetLocid(DatumGetGraphid(gid)))));
		}
		return (Datum) 0;
	}

	relid = get_labid_relid(mgstate->graphid,
							GraphidGetLabid(DatumGetGraphid(gid)));
	resultRelInfo = getResultRelInfo(mgstate, relid);

	resultRelationDesc = resultRelInfo->ri_RelationDesc;

	/*
	 * Create a tuple to store. Attributes of vertex/edge label are not the
	 * same with those of vertex/edge.
	 */
	fillLabelTupleSlot(elemTupleSlot, resultRelationDesc, tts_value_type,
					   tts_value);
	tts_values = elemTupleSlot->tts_values;

lbeforerow:

	/* BEFORE ROW UPDATE Triggers */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_update_before_row)
	{
		/*
		 * Ask the trigger machinery to report a concurrent update back rather
		 * than recheck the row itself, the way it already does for MERGE.
		 * Forming the new tuple for that recheck needs the update projection
		 * that carries an UPDATE's changed columns onto the target row, and a
		 * graph write has none: it fills the tuple directly rather than
		 * projecting it, so it must re-derive the row its own way.
		 */
		if (!ExecBRUpdateTriggers(estate, epqstate, resultRelInfo, ctid,
								  NULL, elemTupleSlot, &result, &tmfd, true))
		{
			/*
			 * A trigger that returned NULL suppressed the row, so there is
			 * nothing to write.  A concurrent update is reported back here
			 * instead, because the trigger machinery cannot re-derive a graph
			 * write's row for it; carry on and let the update below see the same
			 * conflict and re-examine the row, rather than dropping the write.
			 */
			if (result != TM_Updated)
				return (Datum) 0;

			/*
			 * Locking the row for the trigger already followed the update chain,
			 * so the write below would now succeed against the new version and
			 * store values derived from the old one.  Re-examine the row here
			 * instead.  The lock mode an update would have reported is not
			 * available on this path, and an update takes the exclusive one.
			 */
			lockmode = LockTupleExclusive;
			goto lconcurrent;
		}
	}

	/*
	 * Recompute the promoted typed columns from the property bag before the
	 * update.  Reached on the re-examination path too, since that goes back to
	 * the before-row triggers above; the generated columns are always derived
	 * from the same bag that is about to be persisted, so they never diverge
	 * from it.
	 *
	 * This relies on graph-write range table entries carrying no updated-column
	 * bitmap (ExecGetUpdatedCols returns empty), so ExecComputeStoredGenerated
	 * does not skip a column as "independent of the updated columns" -- a SET
	 * rewrites the whole bag, so every promoted column must be recomputed.
	 */
	computeLabelStoredGenerated(resultRelInfo, estate, elemTupleSlot,
								CMD_UPDATE);

	ExecMaterializeSlot(elemTupleSlot);
	elemTupleSlot->tts_tableOid = RelationGetRelid(resultRelationDesc);

	/*
	 * ExecWithCheckOptions() will skip any WCOs which are not of the kind we
	 * are looking for at this point.
	 */
	if (resultRelInfo->ri_WithCheckOptions != NIL)
		ExecWithCheckOptions(WCO_RLS_UPDATE_CHECK,
							 resultRelInfo, elemTupleSlot, estate);

	/*
	 * Check the constraints of the tuple
	 */
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
		case TM_Ok:
			break;
		case TM_Updated:
lconcurrent:
			{
				scanrelid = 0;

				/*
				 * Whatever the last re-examination -- of this element on an
				 * earlier row, or of an earlier element of this row -- asked
				 * about, it is not what this one is asking about.  Take it all
				 * back before naming anything, so an entry that a path out of
				 * here left behind cannot outlive it either.
				 */
				takeBackRecheckSlots(mgstate);

				if (plan->nr_modify > 0)
				{
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("could not serialize access due to concurrent update")));
				}

				if (IsolationUsesXactSnapshot())
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("could not serialize access due to concurrent update")));

				/*
				 * A recheck re-runs the subplan with the re-fetched row standing
				 * in for what a scan reads, so it has to name that scan's range
				 * table index -- not the target label's own entry, which carries
				 * the write permission and is never scanned.  Follow the element
				 * back to the scans that supply it and take the one reading the
				 * relation this row lives in.
				 */
				scans = getElementScanRelIndexes(mgstate, attidx + 1);
				foreach(ls, scans)
				{
					Index		rti = (Index) lfirst_int(ls);

					if (exec_rt_fetch(rti, estate)->relid != relid)
						continue;
					if (scanrelid != 0)
					{
						/* two scans of it: the row belongs to neither */
						scanrelid = 0;
						break;
					}
					scanrelid = rti;
				}

				if (scanrelid == 0)
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("could not serialize access due to concurrent update")));

				/*
				 * Already know that we're going to need to do EPQ, so fetch
				 * tuple directly into the right slot.
				 */
				inputslot = lendRecheckSlot(mgstate, resultRelationDesc,
											scanrelid);

				result = table_tuple_lock(resultRelationDesc, ctid,
										  estate->es_snapshot,
										  inputslot, GetCurrentCommandId(true),
										  lockmode, LockWaitBlock,
										  TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
										  &tmfd);

				switch (result)
				{
					case TM_Ok:

						/*
						 * The chain was followed either just now or, when a
						 * before-row trigger reported the conflict, while it
						 * locked the row for that trigger -- so tmfd.traversed
						 * distinguishes the two entry paths rather than reporting
						 * success, and either way inputslot now holds the row to
						 * re-examine.
						 */

						/*
						 * An inheritance set supplies one column from one scan
						 * per member, so stand in -- with nothing -- for the
						 * members other than the one this row lives in, or the
						 * recheck could answer with one of their rows instead of
						 * the row being re-examined.
						 */
						foreach(ls, scans)
						{
							Index		rti = (Index) lfirst_int(ls);

							if (rti == scanrelid)
								continue;
							(void) lendRecheckSlot(mgstate,
												   ExecGetRangeTableRelation(estate, rti, false),
												   rti);
						}

						epqslot = recheckElement(mgstate, scanrelid,
												 &readwhatwelent);

						/*
						 * The recheck is only an answer about this row if the
						 * scan we stood in for actually read what we supplied.
						 */
						if (!readwhatwelent)
							ereport(ERROR,
									(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
									 errmsg("could not serialize access due to concurrent update")));

						if (TupIsNull(epqslot))
							/* Tuple not passing quals anymore, exiting... */
							return (Datum) 0;

						slot_getallattrs(epqslot);
						Assert(!epqslot->tts_isnull[attidx]);
						tts_value = epqslot->tts_values[attidx];

						if (DatumGetGraphid(tts_value_type == VERTEXOID ?
											getVertexIdDatum(tts_value) :
											getEdgeIdDatum(tts_value)) !=
							DatumGetGraphid(gid))
							ereport(ERROR,
									(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
									 errmsg("could not serialize access due to concurrent update")));

						fillLabelTupleSlot(elemTupleSlot, resultRelationDesc,
										   tts_value_type, tts_value);
						tts_values = elemTupleSlot->tts_values;

						/*
						 * Go back to the before-row triggers rather than
						 * straight to the write.  A trigger sees the row a write
						 * is about to store, and the row has just changed; a
						 * trigger that inspects it -- or refuses it -- has to be
						 * asked about the row being stored, not the one that was
						 * abandoned.  Locking the row for a trigger is also what
						 * reports the conflict in the first place, so on that
						 * entry path the trigger has not run at all yet.
						 */
						goto lbeforerow;
					case TM_Deleted:
						/* tuple already deleted; nothing to do */
						return (Datum) 0;

					case TM_SelfModified:

						/*
						 * This can be reached when following an update chain
						 * from a tuple updated by another session, reaching a
						 * tuple that was already updated in this transaction.
						 * If previously modified by this command, ignore the
						 * redundant update, otherwise error out.
						 *
						 * See also TM_SelfModified response to
						 * table_tuple_update() above.
						 */
						if (tmfd.cmax != estate->es_output_cid)
							ereport(ERROR,
									(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
									 errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
									 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));
						return (Datum) 0;
					default:
						/* see table_tuple_lock call in ExecDelete() */
						elog(ERROR, "unexpected table_tuple_lock status: %u",
							 result);
				}
				break;
			}
		case TM_Deleted:

			/*
			 * The row this write was going to change is gone.  A plain UPDATE
			 * would drop it from its result and carry on, but a graph write
			 * hands the element it was given to whatever clause comes next, and
			 * the element already carries the new value the projection built --
			 * so skipping the write here would report a change that was never
			 * stored, against a row that no longer exists.  There is no way to
			 * withdraw the row from the output, so the conflict has to be
			 * refused outright, in the terms a client retries on.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("could not serialize access due to concurrent delete")));
			break;

		case TM_SelfModified:

			/*
			 * The row was already written by this same command.  A join that
			 * reaches one row twice is the harmless case and keeps the first
			 * write.  The other case is a row changed underneath this command
			 * by a before-row trigger or a volatile function, which cannot be
			 * merged with the write already made and so has to be refused.
			 */
			if (tmfd.cmax != estate->es_output_cid)
				ereport(ERROR,
						(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
						 errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
						 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));
			return (Datum) 0;

		default:
			elog(ERROR, "unrecognized heap_update status: %u", result);
	}

	if (resultRelInfo->ri_NumIndices > 0 && update_indexes)
		recheckIndexes = ExecInsertIndexTuples(resultRelInfo,
											   elemTupleSlot,
											   estate,
											   true,
											   false,
											   NULL,
											   NIL,
											   false);

	graphWriteStats.updateProperty++;

	/* AFTER ROW UPDATE Triggers */
	ExecARUpdateTriggers(estate, resultRelInfo, NULL, NULL, ctid, NULL, elemTupleSlot,
						 recheckIndexes, NULL, false);

	list_free(recheckIndexes);

	entry = hash_search(mgstate->elemTable, &gid, HASH_ENTER, &hash_found);

	if (tts_value_type == VERTEXOID)
	{
		inserted_datum = makeGraphVertexDatum(gid,
											  tts_values[Anum_ag_vertex_properties - 1],
											  PointerGetDatum(&elemTupleSlot->tts_tid));
	}
	else
	{
		inserted_datum = makeGraphEdgeDatum(gid,
											tts_values[Anum_ag_edge_start - 1],
											tts_values[Anum_ag_edge_end - 1],
											tts_values[Anum_ag_edge_properties - 1],
											PointerGetDatum(&elemTupleSlot->tts_tid));
	}
	entry->elem = inserted_datum;
	return inserted_datum;
}

/* See ExecUpdate() */
ItemPointer
LegacyUpdateElemProp(ModifyGraphState *mgstate, Oid elemtype, Datum gid,
					 Datum elem_datum)
{
	EState	   *estate = mgstate->ps.state;
	EPQState   *epqstate = &mgstate->mt_epqstate;
	TupleTableSlot *elemTupleSlot = mgstate->elemTupleSlot;
	Oid			relid;
	ItemPointer ctid;
	ResultRelInfo *resultRelInfo;
	Relation	resultRelationDesc;
	LockTupleMode lockmode;
	TM_Result	result;
	TM_FailureData tmfd;
	TU_UpdateIndexes update_indexes;
	List	   *recheckIndexes = NIL;

	relid = get_labid_relid(mgstate->graphid,
							GraphidGetLabid(DatumGetGraphid(gid)));
	resultRelInfo = getResultRelInfo(mgstate, relid);
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

	checkPropMapIsObject(elemTupleSlot->tts_values[(elemtype == VERTEXOID ?
													Anum_ag_vertex_properties :
													Anum_ag_edge_properties) - 1]);

	markUnassignedLabelColsNull(elemTupleSlot,
								elemtype == VERTEXOID ?
								Anum_table_vertex_prop_map :
								Anum_table_edge_prop_map);
	ExecStoreVirtualTuple(elemTupleSlot);

	/* BEFORE ROW UPDATE Triggers */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_update_before_row)
	{
		/* report a concurrent update back rather than recheck here; see above */
		if (!ExecBRUpdateTriggers(estate, epqstate, resultRelInfo, ctid,
								  NULL, elemTupleSlot, &result, &tmfd, true))
		{
			elog(ERROR, "Trigger must not be NULL on Cypher Clause.");
			return NULL;
		}
	}

	/* Recompute the promoted typed columns from the new bag (see above). */
	computeLabelStoredGenerated(resultRelInfo, estate, elemTupleSlot,
								CMD_UPDATE);

	ExecMaterializeSlot(elemTupleSlot);
	elemTupleSlot->tts_tableOid = RelationGetRelid(resultRelationDesc);

	/*
	 * ExecWithCheckOptions() will skip any WCOs which are not of the kind we
	 * are looking for at this point.
	 */
	if (resultRelInfo->ri_WithCheckOptions != NIL)
		ExecWithCheckOptions(WCO_RLS_UPDATE_CHECK,
							 resultRelInfo, elemTupleSlot, estate);

	/*
	 * Check the constraints of the tuple
	 */
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
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("graph element(%hu," UINT64_FORMAT ") has been SET multiple times",
							GraphidGetLabid(DatumGetGraphid(gid)),
							GraphidGetLocid(DatumGetGraphid(gid)))));
			break;
		case TM_Ok:
			break;
		case TM_Updated:
			/* TODO: A solution to concurrent update is needed. */
			ereport(ERROR,
					(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("could not serialize access due to concurrent update")));
			break;
		case TM_Deleted:

			/*
			 * This path writes each element as it is reached and has no
			 * re-examination of its own, so a row that another transaction
			 * removed leaves it nothing to write.  Report the conflict in the
			 * terms a client retries on, exactly as the concurrent update
			 * above does.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("could not serialize access due to concurrent delete")));
			break;
		default:
			elog(ERROR, "unrecognized heap_update status: %u", result);
			break;
	}

	if (resultRelInfo->ri_NumIndices > 0 && update_indexes)
		recheckIndexes = ExecInsertIndexTuples(resultRelInfo,
											   elemTupleSlot,
											   estate,
											   true,
											   false,
											   NULL,
											   NIL,
											   false);

	graphWriteStats.updateProperty++;

	/* AFTER ROW UPDATE Triggers */
	ExecARUpdateTriggers(estate, resultRelInfo, NULL, NULL, ctid, NULL, elemTupleSlot,
						 recheckIndexes, NULL, false);

	list_free(recheckIndexes);

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
			ereport(WARNING,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("graph element(%hu," UINT64_FORMAT ") has been SET multiple times",
							GraphidGetLabid(entry->key),
							GraphidGetLocid(entry->key))));
	}

	entry->elem = datumCopy(newelem, false, -1);
}
