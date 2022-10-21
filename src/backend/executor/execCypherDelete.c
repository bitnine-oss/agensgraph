/*
 * execCypherDelete.c
 *	  routines to handle ModifyGraph delete nodes.
 *
 * Copyright (c) 2022 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/executor/execCypherDelete.c
 */

#include "postgres.h"

#include "executor/execCypherDelete.h"
#include "executor/executor.h"
#include "executor/nodeModifyGraph.h"
#include "nodes/nodeFuncs.h"
#include "access/tableam.h"
#include "utils/lsyscache.h"
#include "access/heapam.h"
#include "pgstat.h"
#include "utils/arrayaccess.h"
#include "access/xact.h"
#include "commands/trigger.h"
#include "utils/fmgroids.h"

#define DatumGetItemPointer(X)		((ItemPointer) DatumGetPointer(X))
#define ItemPointerGetDatum(X)		PointerGetDatum(X)

static bool ExecDeleteEdgeOrVertex(ModifyGraphState *mgstate, ResultRelInfo *resultRelInfo,
								   Graphid graphid, Datum tupleid,
								   Datum edge_start_id, Datum edge_end_id,
								   Oid typeOid,
								   bool required);
static void ExecDeleteGraphElement(ModifyGraphState *mgstate, Datum elem,
								   Oid type);

static void find_connected_edges(ModifyGraphState *mgstate, Graphid vertex_id);
static void find_connected_edges_internal(ModifyGraphState *mgstate,
										  ModifyGraph *plan,
										  EState *estate,
										  ResultRelInfo *resultRelInfo,
										  AttrNumber attr,
										  Datum vertex_id);

TupleTableSlot *
ExecDeleteGraph(ModifyGraphState *mgstate, TupleTableSlot *slot)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	ExprContext *econtext = mgstate->ps.ps_ExprContext;
	TupleDesc	tupDesc = slot->tts_tupleDescriptor;
	ListCell   *le;

	ResetExprContext(econtext);

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
		 * they will be removed.
		 */
		ExecDeleteGraphElement(mgstate, elem, type);

		/*
		 * The graphpath must be passed to the next plan for deleting vertex
		 * array of the graphpath.
		 */
		if (type == EDGEARRAYOID &&
			TupleDescAttr(tupDesc, attno - 1)->atttypid == GRAPHPATHOID)
			continue;

		setSlotValueByAttnum(slot, (Datum) 0, attno);
	}

	return (plan->last ? NULL : slot);
}

/*
 * deleteElement
 * Delete the graph element.
 */
static bool
ExecDeleteEdgeOrVertex(ModifyGraphState *mgstate, ResultRelInfo *resultRelInfo,
					   Graphid graphid, Datum tupleid,
					   Datum edge_start_id, Datum edge_end_id,
					   Oid typeOid,
					   bool required)
{
	EPQState   *epqstate = &mgstate->mt_epqstate;
	EState	   *estate = mgstate->ps.state;
	Relation	resultRelationDesc;
	TM_Result	result;
	TM_FailureData tmfd;
	bool		hash_found;

	hash_search(mgstate->elemTable, &graphid, HASH_FIND, &hash_found);
	if (hash_found)
		return false;

	resultRelationDesc = resultRelInfo->ri_RelationDesc;

	/* BEFORE ROW DELETE Triggers */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_delete_before_row)
	{
		bool		dodelete;

		dodelete = ExecBRDeleteTriggers(estate, epqstate, resultRelInfo,
										tupleid, NULL, NULL);
		if (!dodelete)
		{
			if (required)
			{
				elog(ERROR, "cannot delete required graph element, because of trigger action.");
			}
			/* "do nothing" */
			return false;
		}
	}

	/* see ExecDelete() */
	if (table_has_extended_am(resultRelationDesc))
	{
		result = table_extended_tuple_delete(epqstate, resultRelInfo, estate,
											 tupleid, NULL,
											 mgstate->modify_cid + MODIFY_CID_OUTPUT,
											 estate->es_snapshot,
											 estate->es_crosscheck_snapshot,
											 true /* wait for commit */ ,
											 &tmfd,
											 false);
	}
	else
	{
		result = table_tuple_delete(resultRelationDesc,
									DatumGetItemPointer(tupleid),
									mgstate->modify_cid + MODIFY_CID_OUTPUT,
									estate->es_snapshot,
									estate->es_crosscheck_snapshot,
									true,
									&tmfd,
									false);
	}

	switch (result)
	{
		case TM_SelfModified:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("modifying the same element more than once cannot happen")));
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

	/* AFTER ROW DELETE Triggers */
	ExecARDeleteTriggers(estate, resultRelInfo, tupleid, NULL,
						 NULL);

	if (typeOid == EDGEOID)
	{
		graphWriteStats.deleteEdge++;

		if (auto_gather_graphmeta)
		{
			agstat_count_edge_delete(
									 GraphidGetLabid(graphid),
									 GraphidGetLabid(edge_start_id),
									 GraphidGetLabid(edge_end_id)
				);
		}
	}
	else
	{
		Assert(typeOid == VERTEXOID);
		graphWriteStats.deleteVertex++;
	}

	hash_search(mgstate->elemTable, &graphid, HASH_ENTER, &hash_found);

	return true;
}

static void
ExecDeleteGraphElement(ModifyGraphState *mgstate, Datum elem, Oid type)
{
	if (type == VERTEXOID)
	{
		Graphid		vertex_id = DatumGetGraphid(getVertexIdDatum(elem));
		Oid			rel_oid = get_labid_relid(mgstate->graphid,
											  GraphidGetLabid(vertex_id));
		ResultRelInfo *resultRelInfo = getResultRelInfo(mgstate, rel_oid);

		find_connected_edges(mgstate, vertex_id);

		ExecDeleteEdgeOrVertex(mgstate, resultRelInfo, vertex_id, getVertexTidDatum(elem),
							   0, 0,
							   VERTEXOID, false);
	}
	else if (type == EDGEOID)
	{
		Graphid		edge_id = DatumGetGraphid(getEdgeIdDatum(elem));
		Oid			rel_oid = get_labid_relid(mgstate->graphid,
											  GraphidGetLabid(edge_id));
		ResultRelInfo *resultRelInfo = getResultRelInfo(mgstate, rel_oid);
		ExecDeleteEdgeOrVertex(mgstate, resultRelInfo, edge_id, getEdgeTidDatum(elem),
							   getEdgeStartDatum(elem), getEdgeEndDatum(elem),
							   EDGEOID, false);
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

		vertices = DatumGetAnyArrayP(elem);
		nvertices = ArrayGetNItems(AARR_NDIM(vertices), AARR_DIMS(vertices));

		get_typlenbyvalalign(AARR_ELEMTYPE(vertices), &typlen,
							 &typbyval, &typalign);

		array_iter_setup(&it, vertices);
		for (i = 0; i < nvertices; i++)
		{
			vtx = array_iter_next(&it, &isnull, i, typlen, typbyval, typalign);
			if (!isnull)
			{
				ExecDeleteGraphElement(mgstate, vtx, VERTEXOID);
			}
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

		edges = DatumGetAnyArrayP(elem);
		nedges = ArrayGetNItems(AARR_NDIM(edges), AARR_DIMS(edges));

		get_typlenbyvalalign(AARR_ELEMTYPE(edges), &typlen,
							 &typbyval, &typalign);

		array_iter_setup(&it, edges);
		for (i = 0; i < nedges; i++)
		{
			edge = array_iter_next(&it, &isnull, i, typlen, typbyval, typalign);
			if (!isnull)
			{
				ExecDeleteGraphElement(mgstate, edge, EDGEOID);
			}
		}
	}
	else
	{
		elog(ERROR, "unexpected graph type %d", type);
	}
}

static void
find_connected_edges(ModifyGraphState *mgstate, Graphid vertex_id)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	DeleteGraphState *delete_graph_state = mgstate->delete_graph_state;
	EState	   *estate = mgstate->ps.state;
	Datum		datum_vertex_id = GraphidGetDatum(vertex_id);
	ResultRelInfo *resultRelInfo = delete_graph_state->edge_labels;
	CommandId	saved_command_id;
	int			i;

	saved_command_id = estate->es_snapshot->curcid;
	estate->es_snapshot->curcid =
		mgstate->modify_cid + MODIFY_CID_NLJOIN_MATCH;

	for (i = 0; i < delete_graph_state->num_edge_labels; i++)
	{
		find_connected_edges_internal(mgstate, plan, estate, resultRelInfo,
									  Anum_table_edge_start, datum_vertex_id);
		find_connected_edges_internal(mgstate, plan, estate, resultRelInfo,
									  Anum_table_edge_end, datum_vertex_id);
		resultRelInfo++;
	}

	estate->es_snapshot->curcid = saved_command_id;
}

static void
find_connected_edges_internal(ModifyGraphState *mgstate,
							  ModifyGraph *plan,
							  EState *estate,
							  ResultRelInfo *resultRelInfo,
							  AttrNumber attr,
							  Datum vertex_id)
{
	TupleTableSlot *slot;
	Relation	relation = resultRelInfo->ri_RelationDesc;
	TableScanDesc scanDesc;
	ScanKeyData skey;

	ScanKeyInit(&skey, attr,
				BTEqualStrategyNumber,
				F_GRAPHID_EQ, vertex_id);
	scanDesc = table_beginscan(relation, estate->es_snapshot, 1, &skey);

	slot = table_slot_create(relation, NULL);
	while (table_scan_getnextslot(scanDesc, ForwardScanDirection, slot))
	{
		if (!plan->detach)
		{
			table_endscan(scanDesc);
			elog(ERROR, "vertices with edges can not be removed");
		}
		ExecDeleteEdgeOrVertex(mgstate,
							   resultRelInfo,
							   slot->tts_values[Anum_table_edge_id - 1],
							   PointerGetDatum(&slot->tts_tid),
							   slot->tts_values[Anum_table_edge_start - 1],
							   slot->tts_values[Anum_table_edge_end - 1],
							   EDGEOID,
							   true);
	}
	table_endscan(scanDesc);
	ExecDropSingleTupleTableSlot(slot);
}
