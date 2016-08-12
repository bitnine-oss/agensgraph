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
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/array.h"
#include "utils/arrayaccess.h"
#include "utils/graph.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#define Natts_graphid			2
#define Anum_graphid_oid		1
#define Anum_graphid_lid		2

#define Natts_vertex			2
#define Anum_vertex_id			1
#define Anum_vertex_properties	2

#define Natts_edge				4
#define Anum_edge_id			1
#define Anum_edge_start			2
#define Anum_edge_end			3
#define Anum_edge_properties	4

#define Natts_graphpath			2
#define Anum_graphpath_vertices	1
#define Anum_graphpath_edges	2

typedef struct LabelOutData {
	Oid			label_relid;
	NameData	label;
} LabelOutData;

typedef struct GraphpathOutData {
	ArrayMetaState vertex;
	ArrayMetaState edge;
} GraphpathOutData;

static LabelOutData *cache_label(FmgrInfo *flinfo, Oid relid);
static void get_elem_type_output(ArrayMetaState *state, Oid elem_type,
								 MemoryContext mctx);
static Datum array_iter_next_(array_iter *it, int idx, ArrayMetaState *state);

Datum
vertex_out(PG_FUNCTION_ARGS)
{
	HeapTupleHeader vertex = PG_GETARG_HEAPTUPLEHEADER(0);
	Oid			tupType;
	TupleDesc	tupDesc;
	HeapTupleData tuple;
	Datum		values[Natts_vertex];
	bool		isnull[Natts_vertex];
	Graphid		id;
	Jsonb	   *prop_map;
	LabelOutData *my_extra;
	StringInfoData si;

	tupType = HeapTupleHeaderGetTypeId(vertex);
	Assert(tupType == VERTEXOID);

	tupDesc = lookup_rowtype_tupdesc(tupType, -1);
	Assert(tupDesc->natts == Natts_vertex);

	tuple.t_len = HeapTupleHeaderGetDatumLength(vertex);
	ItemPointerSetInvalid(&tuple.t_self);
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = vertex;

	heap_deform_tuple(&tuple, tupDesc, values, isnull);
	ReleaseTupleDesc(tupDesc);
	Assert(!isnull[Anum_vertex_id - 1]);
	Assert(!isnull[Anum_vertex_properties - 1]);

	id = getGraphidStruct(values[Anum_vertex_id - 1]);
	prop_map = DatumGetJsonb(values[Anum_vertex_properties - 1]);

	my_extra = cache_label(fcinfo->flinfo, id.oid);

	initStringInfo(&si);
	appendStringInfo(&si, "%s[%u:" INT64_FORMAT "]",
					 NameStr(my_extra->label), id.oid, id.lid);
	JsonbToCString(&si, &prop_map->root, VARSIZE(prop_map));

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
	Graphid		id;
	Jsonb	   *prop_map;
	LabelOutData *my_extra;
	StringInfoData si;

	tupType = HeapTupleHeaderGetTypeId(edge);
	Assert(tupType == EDGEOID);

	tupDesc = lookup_rowtype_tupdesc(tupType, -1);
	Assert(tupDesc->natts == Natts_edge);

	tuple.t_len = HeapTupleHeaderGetDatumLength(edge);
	ItemPointerSetInvalid(&tuple.t_self);
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = edge;

	heap_deform_tuple(&tuple, tupDesc, values, isnull);
	ReleaseTupleDesc(tupDesc);
	Assert(!isnull[Anum_edge_id - 1]);
	Assert(!isnull[Anum_edge_start - 1]);
	Assert(!isnull[Anum_edge_end - 1]);
	Assert(!isnull[Anum_edge_properties - 1]);

	id = getGraphidStruct(values[Anum_edge_id - 1]);
	prop_map = DatumGetJsonb(values[Anum_edge_properties - 1]);

	my_extra = cache_label(fcinfo->flinfo, id.oid);

	initStringInfo(&si);
	appendStringInfo(&si, ":%s[%u:" INT64_FORMAT "]",
					 NameStr(my_extra->label), id.oid, id.lid);
	JsonbToCString(&si, &prop_map->root, VARSIZE(prop_map));

	PG_RETURN_CSTRING(si.data);
}

static LabelOutData *
cache_label(FmgrInfo *flinfo, Oid relid)
{
	LabelOutData *my_extra;

	AssertArg(flinfo != NULL);

	my_extra = (LabelOutData *) flinfo->fn_extra;
	if (my_extra == NULL)
	{
		flinfo->fn_extra = MemoryContextAlloc(flinfo->fn_mcxt,
											  sizeof(*my_extra));
		my_extra = (LabelOutData *) flinfo->fn_extra;
		my_extra->label_relid = InvalidOid;
		MemSetLoop(NameStr(my_extra->label), '\0', sizeof(my_extra->label));
	}

	if (my_extra->label_relid != relid)
	{
		char *label;

		label = get_rel_name(relid);
		if (label == NULL)
			label = "?";

		my_extra->label_relid = relid;
		strncpy(NameStr(my_extra->label), label, sizeof(my_extra->label));
	}

	return my_extra;
}

Datum
graphpath_out(PG_FUNCTION_ARGS)
{
	const char	delim = ',';
	HeapTupleHeader path = PG_GETARG_HEAPTUPLEHEADER(0);
	Oid			tupType;
	TupleDesc	tupDesc;
	HeapTupleData tuple;
	Datum		values[Natts_graphpath];
	bool		isnull[Natts_graphpath];
	AnyArrayType *vertices;
	AnyArrayType *edges;
	GraphpathOutData *my_extra;
	int			nvertices;
	int			nedges;
	StringInfoData si;
	array_iter	it_v;
	array_iter	it_e;
	Datum		value;
	int			i;

	tupType = HeapTupleHeaderGetTypeId(path);
	Assert(tupType == GRAPHPATHOID);

	tupDesc = lookup_rowtype_tupdesc(tupType, -1);
	Assert(tupDesc->natts == Natts_graphpath);

	tuple.t_len = HeapTupleHeaderGetDatumLength(path);
	ItemPointerSetInvalid(&tuple.t_self);
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = path;

	heap_deform_tuple(&tuple, tupDesc, values, isnull);
	ReleaseTupleDesc(tupDesc);
	Assert(!isnull[Anum_graphpath_vertices - 1]);
	Assert(!isnull[Anum_graphpath_edges - 1]);

	vertices = DatumGetAnyArray(values[Anum_graphpath_vertices - 1]);
	edges = DatumGetAnyArray(values[Anum_graphpath_edges - 1]);

	/* cache vertex/edge output information */
	my_extra = (GraphpathOutData *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL)
	{
		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
													  sizeof(GraphpathOutData));
		my_extra = (GraphpathOutData *) fcinfo->flinfo->fn_extra;
		get_elem_type_output(&my_extra->vertex, AARR_ELEMTYPE(vertices),
							 fcinfo->flinfo->fn_mcxt);
		get_elem_type_output(&my_extra->edge, AARR_ELEMTYPE(edges),
							 fcinfo->flinfo->fn_mcxt);
	}

	nvertices = ArrayGetNItems(AARR_NDIM(vertices), AARR_DIMS(vertices));
	nedges = ArrayGetNItems(AARR_NDIM(edges), AARR_DIMS(edges));
	if (nvertices != nedges + 1)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("the numbers of vertices and edges are mismatched")));

	initStringInfo(&si);
	appendStringInfoChar(&si, '[');

	array_iter_setup(&it_v, vertices);
	array_iter_setup(&it_e, edges);

	value = array_iter_next_(&it_v, 0, &my_extra->vertex);
	appendStringInfoString(&si,
			OutputFunctionCall(&my_extra->vertex.proc, value));

	for (i = 0; i < nedges; i++)
	{
		appendStringInfoChar(&si, delim);

		value = array_iter_next_(&it_e, i, &my_extra->edge);
		appendStringInfoString(&si,
			   OutputFunctionCall(&my_extra->edge.proc, value));

		appendStringInfoChar(&si, delim);

		value = array_iter_next_(&it_v, i + 1, &my_extra->vertex);
		appendStringInfoString(&si,
			   OutputFunctionCall(&my_extra->vertex.proc, value));
	}

	appendStringInfoChar(&si, ']');

	PG_RETURN_CSTRING(si.data);
}

static void
get_elem_type_output(ArrayMetaState *state, Oid elem_type, MemoryContext mctx)
{
	get_type_io_data(elem_type, IOFunc_output,
					 &state->typlen, &state->typbyval, &state->typalign,
					 &state->typdelim, &state->typioparam, &state->typiofunc);
	fmgr_info_cxt(state->typiofunc, &state->proc, mctx);
}

static Datum
array_iter_next_(array_iter *it, int idx, ArrayMetaState *state)
{
	bool		isnull;
	Datum		value;

	value = array_iter_next(it, &isnull, idx,
							state->typlen, state->typbyval, state->typalign);
	Assert(!isnull);

	return value;
}

Graphid
getGraphidStruct(Datum datum)
{
	HeapTupleHeader tuphdr;
	Oid			tupType;
	TupleDesc	tupDesc;
	HeapTupleData tuple;
	Datum		values[Natts_graphid];
	bool		isnull[Natts_graphid];
	Graphid		id;

	tuphdr = DatumGetHeapTupleHeader(datum);

	tupType = HeapTupleHeaderGetTypeId(tuphdr);
	Assert(tupType == GRAPHIDOID);

	tupDesc = lookup_rowtype_tupdesc(tupType, -1);
	Assert(tupDesc->natts == Natts_graphid);

	tuple.t_len = HeapTupleHeaderGetDatumLength(tuphdr);
	ItemPointerSetInvalid(&tuple.t_self);
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = tuphdr;

	heap_deform_tuple(&tuple, tupDesc, values, isnull);
	ReleaseTupleDesc(tupDesc);
	Assert(!isnull[Anum_graphid_oid - 1]);
	Assert(!isnull[Anum_graphid_lid - 1]);

	id.oid = DatumGetObjectId(values[Anum_graphid_oid - 1]);
	id.lid = DatumGetInt64(values[Anum_graphid_lid - 1]);

	return id;
}

Datum
getGraphidDatum(Graphid id)
{
	Datum		values[2];
	bool		isnull[2] = {false, false};
	TupleDesc	tupDesc;
	HeapTuple	tuple;

	values[0] = ObjectIdGetDatum(id.oid);
	values[1] = Int64GetDatum(id.lid);

	tupDesc = lookup_rowtype_tupdesc(GRAPHIDOID, -1);
	Assert(tupDesc->natts == Natts_graphid);

	tuple = heap_form_tuple(tupDesc, values, isnull);

	ReleaseTupleDesc(tupDesc);

	return HeapTupleGetDatum(tuple);
}
