/*
 * graph.c
 *	  Functions for vertex and edge data type.
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/graph.c
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "utils/graph.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#define Natts_vertex			3
#define Anum_vertex_oid			1
#define Anum_vertex_id			2
#define Anum_vertex_prop_map	3

#define Natts_edge				7
#define Anum_edge_oid			1
#define Anum_edge_id			2
#define Anum_edge_start_oid		3
#define Anum_edge_start_id		4
#define Anum_edge_end_oid		5
#define Anum_edge_end_id		6
#define Anum_edge_prop_map		7

Datum
vertex_out(PG_FUNCTION_ARGS)
{
	HeapTupleHeader vertex = PG_GETARG_HEAPTUPLEHEADER(0);
	Oid			tupType;
	TupleDesc	tupDesc;
	HeapTupleData tuple;
	Datum		values[Natts_vertex];
	bool		isnull[Natts_vertex];
	StringInfoData si;
	char	   *prop_map;

	tupType = HeapTupleHeaderGetTypeId(vertex);
	tupDesc = lookup_rowtype_tupdesc(tupType, -1);
	Assert(tupDesc->natts == Natts_vertex);

	tuple.t_len = HeapTupleHeaderGetDatumLength(vertex);
	ItemPointerSetInvalid(&tuple.t_self);
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = vertex;

	heap_deform_tuple(&tuple, tupDesc, values, isnull);
	Assert(!isnull[Anum_vertex_oid - 1]);
	Assert(!isnull[Anum_vertex_id - 1]);
	Assert(!isnull[Anum_vertex_prop_map - 1]);

	initStringInfo(&si);
	appendStringInfo(&si, "Node[%u:" INT64_FORMAT "]",
					 DatumGetObjectId(values[Anum_vertex_oid - 1]),
					 DatumGetInt64(values[Anum_vertex_id - 1]));
	prop_map = DatumGetCString(DirectFunctionCall1(jsonb_out,
											values[Anum_vertex_prop_map - 1]));
	appendStringInfoString(&si, prop_map);

	ReleaseTupleDesc(tupDesc);

	PG_RETURN_CSTRING(si.data);
}

Datum
edge_out(PG_FUNCTION_ARGS)
{
	HeapTupleHeader edge = PG_GETARG_HEAPTUPLEHEADER(0);
	Oid			tupType;
	TupleDesc	tupDesc;
	HeapTupleData tuple;
	Datum		values[Natts_edge];
	bool		isnull[Natts_edge];
	Oid			oid;
	char	   *label;
	StringInfoData si;
	char	   *prop_map;

	tupType = HeapTupleHeaderGetTypeId(edge);
	tupDesc = lookup_rowtype_tupdesc(tupType, -1);
	Assert(tupDesc->natts == Natts_edge);

	tuple.t_len = HeapTupleHeaderGetDatumLength(edge);
	ItemPointerSetInvalid(&tuple.t_self);
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = edge;

	heap_deform_tuple(&tuple, tupDesc, values, isnull);
	Assert(!isnull[Anum_edge_oid - 1]);
	Assert(!isnull[Anum_edge_id - 1]);
	Assert(!isnull[Anum_edge_start_oid - 1]);
	Assert(!isnull[Anum_edge_start_id - 1]);
	Assert(!isnull[Anum_edge_end_oid - 1]);
	Assert(!isnull[Anum_edge_end_id - 1]);
	Assert(!isnull[Anum_edge_prop_map - 1]);

	oid = DatumGetObjectId(values[Anum_edge_oid - 1]);
	label = get_rel_name(oid);
	if (label == NULL)
		label = "?";

	initStringInfo(&si);
	appendStringInfo(&si, ":%s[%u:" INT64_FORMAT "]",
					 label, oid, DatumGetInt64(values[Anum_edge_id - 1]));
	prop_map = DatumGetCString(DirectFunctionCall1(jsonb_out,
											values[Anum_edge_prop_map - 1]));
	appendStringInfoString(&si, prop_map);

	ReleaseTupleDesc(tupDesc);

	PG_RETURN_CSTRING(si.data);
}
