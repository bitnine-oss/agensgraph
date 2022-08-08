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

static bool isDetachRequired(ModifyGraphState *mgstate);
static void enterDelPropTable(ModifyGraphState *mgstate, Datum elem, Oid type);

TupleTableSlot *
ExecDeleteGraph(ModifyGraphState *mgstate, TupleTableSlot *slot)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	ExprContext *econtext = mgstate->ps.ps_ExprContext;
	TupleDesc	tupDesc = slot->tts_tupleDescriptor;
	ListCell   *le;

	ResetExprContext(econtext);

	if (isDetachRequired(mgstate))
		elog(ERROR, "vertices with edges can not be removed");

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
		enterDelPropTable(mgstate, elem, type);

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
 * isDetachRequired
 *
 * This function is related to add_paths_for_cdelete.
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
bool
deleteElement(ModifyGraphState *mgstate, Datum gid, ItemPointer tid, Oid type)
{
	EState	   *estate = mgstate->ps.state;
	Oid			relid;
	ResultRelInfo *resultRelInfo;
	ResultRelInfo *savedResultRelInfo;
	Relation	resultRelationDesc;
	TM_Result	result;
	TM_FailureData tmfd;
	bool		hash_found;

	hash_search(mgstate->elemTable, &gid, HASH_FIND, &hash_found);
	if (hash_found)
		return false;

	relid = get_labid_relid(mgstate->graphid,
							GraphidGetLabid(DatumGetGraphid(gid)));
	resultRelInfo = getResultRelInfo(mgstate, relid);

	savedResultRelInfo = estate->es_result_relation_info;
	estate->es_result_relation_info = resultRelInfo;
	resultRelationDesc = resultRelInfo->ri_RelationDesc;

	/* see ExecDelete() */
	result = heap_delete(resultRelationDesc, tid,
						 mgstate->modify_cid + MODIFY_CID_OUTPUT,
						 estate->es_crosscheck_snapshot, true, &tmfd, false);

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

	/*
	 * NOTE: VACUUM will delete index tuples associated with the heap tuple
	 * later.
	 */

	if (type == VERTEXOID)
		graphWriteStats.deleteVertex++;
	else
	{
		Assert(type == EDGEOID);

		graphWriteStats.deleteEdge++;
	}

	estate->es_result_relation_info = savedResultRelInfo;
	hash_search(mgstate->elemTable, &gid, HASH_ENTER, &hash_found);

	return true;
}

static void
enterDelPropTable(ModifyGraphState *mgstate, Datum elem, Oid type)
{
	Datum		gid;

	if (type == VERTEXOID)
	{
		gid = getVertexIdDatum(elem);

		deleteElement(mgstate, gid,
					  ((ItemPointer) DatumGetPointer(getVertexTidDatum(elem))),
					  type);
	}
	else if (type == EDGEOID)
	{
		bool		deleted;
		Graphid		eid;
		Graphid		start;
		Graphid		end;
		ItemPointer tid;

		gid = getEdgeIdDatum(elem);


		eid = GraphidGetLabid(gid);
		start = GraphidGetLabid(getEdgeStartDatum(elem));
		end = GraphidGetLabid(getEdgeEndDatum(elem));
		tid = (ItemPointer) DatumGetPointer(getEdgeTidDatum(elem));

		deleted = deleteElement(mgstate, gid, tid, type);
		if (deleted && auto_gather_graphmeta)
			agstat_count_edge_delete(eid, start, end);
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
			ItemPointer tid;

			vtx = array_iter_next(&it, &isnull, i, typlen, typbyval, typalign);

			tid = (ItemPointer) DatumGetPointer(getVertexTidDatum(vtx));
			gid = getVertexIdDatum(vtx);

			deleteElement(mgstate, gid, tid, VERTEXOID);
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
			bool		deleted;
			Graphid		eid;
			Graphid		start;
			Graphid		end;

			edge = array_iter_next(&it, &isnull, i, typlen, typbyval, typalign);

			gid = getEdgeIdDatum(edge);
			eid = GraphidGetLabid(gid);
			start = GraphidGetLabid(getEdgeStartDatum(edge));
			end = GraphidGetLabid(getEdgeEndDatum(edge));

			deleted = deleteElement(mgstate, gid,
									((ItemPointer) DatumGetPointer(
																   getEdgeTidDatum(edge))), EDGEOID);

			if (deleted && auto_gather_graphmeta)
				agstat_count_edge_delete(eid, start, end);
		}
	}
	else
	{
		elog(ERROR, "unexpected graph type %d", type);
	}
}
