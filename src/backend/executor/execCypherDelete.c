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

#include "catalog/ag_label_fn.h"
#include "catalog/pg_namespace.h"
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
#include "tcop/tcopprot.h"

#define DatumGetItemPointer(X)		((ItemPointer) DatumGetPointer(X))
#define ItemPointerGetDatum(X)		PointerGetDatum(X)

static bool isDetachRequired(ModifyGraphState *mgstate);
static bool ExecDeleteEdgeOrVertex(ModifyGraphState *mgstate,
								   ResultRelInfo *resultRelInfo,
								   Graphid graphid, ItemPointer tupleid,
								   Oid typeOid, bool required);
static void ExecDeleteGraphElement(ModifyGraphState *mgstate, Datum elem,
								   Oid type);

TupleTableSlot *
ExecDeleteGraph(ModifyGraphState *mgstate, TupleTableSlot *slot)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	ExprContext *econtext = mgstate->ps.ps_ExprContext;
	TupleDesc	tupDesc = slot->tts_tupleDescriptor;
	ListCell   *le;

	ResetExprContext(econtext);

	if (isDetachRequired(mgstate))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("Vertex with connected edges cannot be removed"),
				 errhint("Use DETACH DELETE or remove connected edges before deleting the vertex.")));

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
			continue;

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
		{
			continue;
		}

		setSlotValueByAttnum(slot, (Datum) 0, attno);
	}

	return (plan->last ? NULL : slot);
}

/*
 * isDetachRequired
 *
 * Checks if we have any tuple from right tree of the join, which means
 * there is at least one edge connected to the vertex in the current slot.
 * If so, we cannot delete the vertex without DETACH.
 */
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
	 * slot. (nl_MatchedOuter && !nl_NeedNewOuter) false: No edge exists for
	 * the target vertices in the current slot. (!nl_MatchedOuter &&
	 * nl_NeedNewOuter)
	 */
	return nlstate->nl_MatchedOuter;
}

/*
 * deleteElement
 * Delete the graph element.
 */
static bool
ExecDeleteEdgeOrVertex(ModifyGraphState *mgstate, ResultRelInfo *resultRelInfo,
					   Graphid graphid, ItemPointer tupleid, Oid typeOid,
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
										tupleid, NULL, NULL, &result, &tmfd);
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
	result = table_tuple_delete(resultRelationDesc,
								tupleid,
								mgstate->modify_cid + MODIFY_CID_OUTPUT,
								estate->es_snapshot,
								estate->es_crosscheck_snapshot,
								true,
								&tmfd,
								false);

	switch (result)
	{
		case TM_SelfModified:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("modifying the same element more than once cannot happen")));
			break;
		case TM_Ok:
			break;

		case TM_Updated:
			/* TODO: A solution to concurrent update is needed. */
			ereport(ERROR,
					(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("could not serialize access due to concurrent update")));
			break;
		default:
			elog(ERROR, "unrecognized heap_update status: %u", result);
			break;
	}

	/* AFTER ROW DELETE Triggers */
	ExecARDeleteTriggers(estate, resultRelInfo, tupleid, NULL,
						 NULL, false);

	if (typeOid == EDGEOID)
	{
		graphWriteStats.deleteEdge++;
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
		ItemPointer vertex_tid = (ItemPointer) DatumGetPointer(getVertexTidDatum(elem));
		ExecDeleteEdgeOrVertex(mgstate, resultRelInfo, vertex_id, vertex_tid,
							   VERTEXOID, true);
	}
	else if (type == EDGEOID)
	{
		bool		deleted;
		Graphid		eid = getEdgeIdDatum(elem);
		Labid 		edge_labid = GraphidGetLabid(eid);
		Labid		start_labid = GraphidGetLabid(getEdgeStartDatum(elem));
		Labid		end_labid = GraphidGetLabid(getEdgeEndDatum(elem));
		ItemPointer tid = (ItemPointer) DatumGetPointer(getEdgeTidDatum(elem));
		Oid			rel_oid = get_labid_relid(mgstate->graphid, edge_labid);
		ResultRelInfo *resultRelInfo = getResultRelInfo(mgstate, rel_oid);

		deleted = ExecDeleteEdgeOrVertex(mgstate, resultRelInfo, eid, tid,
							   			 EDGEOID, true);
		if (deleted && auto_gather_graphmeta)
			agstat_count_edge_delete(edge_labid, start_labid, end_labid);
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

