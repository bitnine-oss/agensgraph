/*
 * nodeModifyGraph.c
 *	  routines to handle ModifyGraph nodes.
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeModifyGraph.c
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/nodeModifyGraph.h"
#include "executor/spi.h"
#include "nodes/nodeFuncs.h"
#include "utils/arrayaccess.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/typcache.h"

#define SQLCMD_BUFLEN				(NAMEDATALEN + 192)
#define SQLCMD_DEL_ELEM				"DELETE FROM ONLY graph.%s WHERE id = $1"
#define SQLCMD_DEL_ELEM_NPARAMS		2
#define SQLCMD_DETACH				"SELECT id FROM graph.edge WHERE " \
									"start_oid = $1 AND start_id = $2 OR " \
									"end_oid = $1 AND end_id = $2"
#define SQLCMD_DETACH_NPARAMS		2
#define SQLCMD_DEL_EDGES			"DELETE FROM graph.edge WHERE " \
									"start_oid = $1 AND start_id = $2 OR " \
									"end_oid = $1 AND end_id = $2"
#define SQLCMD_DEL_EDGES_NPARAMS	2

typedef struct ElemId {
	Oid			oid;
	int64		id;
} ElemId;

typedef struct ArrayAccessTypeInfo {
	int16		typlen;
	bool		typbyval;
	char		typalign;
} ArrayAccessTypeInfo;

static TupleTableSlot *ExecDeleteGraph(ModifyGraphState *mgstate,
									   TupleTableSlot *slot);
static void deleteVertex(Datum vertex, bool detach);
static ElemId DatumGetVid(Datum vertex);
static bool vertexHasEdge(ElemId vid);
static void deleteVertexEdges(ElemId vid);
static ElemId DatumGetEid(Datum edge);
static void deleteElem(char *relname, int64 id);
static void deletePath(Datum graphpath, bool detach);

ModifyGraphState *
ExecInitModifyGraph(ModifyGraph *mgplan, EState *estate, int eflags)
{
	ModifyGraphState *mgstate;

	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	mgstate = makeNode(ModifyGraphState);
	mgstate->ps.plan = (Plan *) mgplan;
	mgstate->ps.state = estate;

	ExecInitResultTupleSlot(estate, &mgstate->ps);
	ExecAssignResultType(&mgstate->ps, ExecTypeFromTL(NIL, false));

	ExecAssignExprContext(estate, &mgstate->ps);

	mgstate->done = false;
	mgstate->subplan = ExecInitNode(mgplan->subplan, estate, eflags);
	mgstate->exprs = (List *) ExecInitExpr((Expr *) mgplan->exprs,
										   (PlanState *) mgstate);

	return mgstate;
}

TupleTableSlot *
ExecModifyGraph(ModifyGraphState *mgstate)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;

	if (mgstate->done)
		return NULL;

	for (;;)
	{
		TupleTableSlot *slot;

		slot = ExecProcNode(mgstate->subplan);
		if (TupIsNull(slot))
			break;

		switch (plan->operation)
		{
			case GWROP_DELETE:
				slot = ExecDeleteGraph(mgstate, slot);
				break;
			default:
				elog(ERROR, "unknown operation");
				break;
		}

		if (slot != NULL)
			return slot;
	}

	mgstate->done = true;

	return NULL;
}

void
ExecEndModifyGraph(ModifyGraphState *mgstate)
{
	ExecFreeExprContext(&mgstate->ps);
	ExecEndNode(mgstate->subplan);
}

static TupleTableSlot *
ExecDeleteGraph(ModifyGraphState *mgstate, TupleTableSlot *slot)
{
	ModifyGraph *plan = (ModifyGraph *) mgstate->ps.plan;
	ExprContext *econtext = mgstate->ps.ps_ExprContext;
	ListCell   *le;

	ResetExprContext(econtext);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	foreach(le, mgstate->exprs)
	{
		ExprState  *e = (ExprState *) lfirst(le);
		Oid			type;
		Datum		dat;
		bool		isNull;
		ExprDoneCond isDone;
		ElemId		elemid;

		type = exprType((Node *) e->expr);
		if (!(type == VERTEXOID || type == EDGEOID || type == GRAPHPATHOID))
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("expected node, relationship, or path")));

		econtext->ecxt_scantuple = slot;
		dat = ExecEvalExpr(e, econtext, &isNull, &isDone);
		if (isNull)
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("deleting NULL is not allowed")));
		if (isDone != ExprSingleResult)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("expected single result")));

		switch (type)
		{
			case VERTEXOID:
				deleteVertex(dat, plan->detach);
				break;
			case EDGEOID:
				elemid = DatumGetEid(dat);
				deleteElem(get_rel_name(elemid.oid), elemid.id);
				break;
			case GRAPHPATHOID:
				deletePath(dat, plan->detach);
				break;
			default:
				elog(ERROR, "expected node, relationship, or path");
		}
	}

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	return (plan->last ? NULL : slot);
}

static void
deleteVertex(Datum vertex, bool detach)
{
	ElemId		elemid;
	char	   *labname;

	elemid = DatumGetVid(vertex);

	if (vertexHasEdge(elemid))
	{
		if (detach)
		{
			deleteVertexEdges(elemid);
		}
		else
		{
			labname = get_rel_name(elemid.oid);

			ereport(ERROR,
					(errcode(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION),
					 errmsg("vertex :%s[%d:" INT64_FORMAT "] has edge(s)",
							labname, elemid.oid, elemid.id)));
		}
	}

	labname = get_rel_name(elemid.oid);
	deleteElem(labname, elemid.id);
}

static ElemId
DatumGetVid(Datum vertex)
{
	HeapTupleHeader	tuphdr;
	Oid			tupType;
	TupleDesc	tupDesc;
	HeapTupleData tuple;
	bool		isnull = false;
	ElemId		vid;

	tuphdr = DatumGetHeapTupleHeader(vertex);

	tupType = HeapTupleHeaderGetTypeId(tuphdr);
	Assert(tupType == VERTEXOID);

	tupDesc = lookup_rowtype_tupdesc(tupType, -1);
	Assert(tupDesc->natts == 3);	// TODO: use Natts_vertex

	tuple.t_len = HeapTupleHeaderGetDatumLength(tuphdr);
	ItemPointerSetInvalid(&tuple.t_self);
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = tuphdr;

	// TODO: use Anum_vertex_...
	vid.oid = DatumGetObjectId(heap_getattr(&tuple, 1, tupDesc, &isnull));
	Assert(!isnull);
	vid.id = DatumGetInt64(heap_getattr(&tuple, 2, tupDesc, &isnull));
	Assert(!isnull);

	ReleaseTupleDesc(tupDesc);

	return vid;
}

static bool
vertexHasEdge(ElemId vid)
{
	Datum		values[SQLCMD_DETACH_NPARAMS];
	Oid			argTypes[SQLCMD_DETACH_NPARAMS];
	int			ret;

	values[0] = ObjectIdGetDatum(vid.oid);
	values[1] = Int64GetDatum(vid.id);
	argTypes[0] = OIDOID;
	argTypes[1] = INT8OID;

	ret = SPI_execute_with_args(SQLCMD_DETACH, SQLCMD_DETACH_NPARAMS, argTypes,
								values, NULL, false, 1);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute failed: %s", SQLCMD_DETACH);

	return (SPI_processed > 0);
}

static void
deleteVertexEdges(ElemId vid)
{
	Datum		values[SQLCMD_DEL_EDGES_NPARAMS];
	Oid			argTypes[SQLCMD_DEL_EDGES_NPARAMS];
	int			ret;

	values[0] = ObjectIdGetDatum(vid.oid);
	values[1] = Int64GetDatum(vid.id);
	argTypes[0] = OIDOID;
	argTypes[1] = INT8OID;

	ret = SPI_execute_with_args(SQLCMD_DEL_EDGES, SQLCMD_DEL_EDGES_NPARAMS,
								argTypes, values, NULL, false, 0);
	if (ret != SPI_OK_DELETE)
		elog(ERROR, "SPI_execute failed: %s", SQLCMD_DEL_EDGES);
}

static ElemId
DatumGetEid(Datum edge)
{
	HeapTupleHeader	tuphdr;
	Oid			tupType;
	TupleDesc	tupDesc;
	HeapTupleData tuple;
	bool		isnull = false;
	ElemId		eid;

	tuphdr = DatumGetHeapTupleHeader(edge);

	tupType = HeapTupleHeaderGetTypeId(tuphdr);
	Assert(tupType == EDGEOID);

	tupDesc = lookup_rowtype_tupdesc(tupType, -1);
	Assert(tupDesc->natts == 7);	// TODO: use Natts_edge

	tuple.t_len = HeapTupleHeaderGetDatumLength(tuphdr);
	ItemPointerSetInvalid(&tuple.t_self);
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = tuphdr;

	// TODO: use Anum_edge_...
	eid.oid = DatumGetObjectId(heap_getattr(&tuple, 1, tupDesc, &isnull));
	Assert(!isnull);
	eid.id = DatumGetInt64(heap_getattr(&tuple, 2, tupDesc, &isnull));
	Assert(!isnull);

	ReleaseTupleDesc(tupDesc);

	return eid;
}

static void
deleteElem(char *relname, int64 id)
{
	char		sqlcmd[SQLCMD_BUFLEN];
	Datum		values[SQLCMD_DEL_ELEM_NPARAMS];
	Oid			argTypes[SQLCMD_DEL_ELEM_NPARAMS];
	int			ret;

	snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_DEL_ELEM, relname);
	values[0] = Int64GetDatum(id);
	argTypes[0] = INT8OID;

	ret = SPI_execute_with_args(sqlcmd, SQLCMD_DEL_ELEM_NPARAMS, argTypes,
								values, NULL, false, 0);
	if (ret != SPI_OK_DELETE)
		elog(ERROR, "SPI_execute failed: %s", sqlcmd);
	if (SPI_processed > 1)
		elog(ERROR, "SPI_execute: only one or no element per execution must be deleted");
}

static void
deletePath(Datum graphpath, bool detach)
{
	HeapTupleHeader	tuphdr;
	Oid			tupType;
	TupleDesc	tupDesc;
	HeapTupleData tuple;
	Datum		values[2];	// TODO: use Natts_graphpath
	bool		isnull[2];
	AnyArrayType *vertices;
	AnyArrayType *edges;
	int			nvertices;
	int			nedges;
	ArrayAccessTypeInfo vertexInfo;
	ArrayAccessTypeInfo edgeInfo;
	array_iter	it;
	Datum		value;
	bool		null;
	ElemId		elemid;
	int			i;

	tuphdr = DatumGetHeapTupleHeader(graphpath);

	tupType = HeapTupleHeaderGetTypeId(tuphdr);
	Assert(tupType == GRAPHPATHOID);

	tupDesc = lookup_rowtype_tupdesc(tupType, -1);
	Assert(tupDesc->natts == 2);

	tuple.t_len = HeapTupleHeaderGetDatumLength(tuphdr);
	ItemPointerSetInvalid(&tuple.t_self);
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = tuphdr;

	heap_deform_tuple(&tuple, tupDesc, values, isnull);
	ReleaseTupleDesc(tupDesc);
	Assert(!isnull[0]);	// TODO: use Anum_graphpath_...
	Assert(!isnull[1]);

	vertices = DatumGetAnyArray(values[0]);
	edges = DatumGetAnyArray(values[1]);

	nvertices = ArrayGetNItems(AARR_NDIM(vertices), AARR_DIMS(vertices));
	nedges = ArrayGetNItems(AARR_NDIM(edges), AARR_DIMS(edges));
	Assert(nvertices == nedges + 1);

	get_typlenbyvalalign(AARR_ELEMTYPE(vertices), &vertexInfo.typlen,
						 &vertexInfo.typbyval, &vertexInfo.typalign);
	get_typlenbyvalalign(AARR_ELEMTYPE(edges), &edgeInfo.typlen,
						 &edgeInfo.typbyval, &edgeInfo.typalign);

	/* delete edges first to avoid vertexHasEdge() */
	array_iter_setup(&it, edges);
	for (i = 0; i < nedges; i++)
	{
		value = array_iter_next(&it, &null, i, edgeInfo.typlen,
								edgeInfo.typbyval, edgeInfo.typalign);
		Assert(!null);

		elemid = DatumGetEid(value);
		deleteElem(get_rel_name(elemid.oid), elemid.id);
	}

	array_iter_setup(&it, vertices);
	for (i = 0; i < nvertices; i++)
	{
		value = array_iter_next(&it, &null, i, vertexInfo.typlen,
								vertexInfo.typbyval, vertexInfo.typalign);
		Assert(!null);

		deleteVertex(value, detach);
	}
}
