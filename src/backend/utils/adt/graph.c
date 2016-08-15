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

#define GRAPHID_FMTSTR			"%u." INT64_FORMAT
#define GRAPHID_BUFLEN			32	/* "4294967295.18446744073709551615" */

typedef struct LabelOutData {
	Oid			label_relid;
	NameData	label;
} LabelOutData;

typedef struct GraphpathOutData {
	ArrayMetaState vertex;
	ArrayMetaState edge;
} GraphpathOutData;

static void graphid_out_si(StringInfo si, Datum graphid);
static LabelOutData *cache_label(FmgrInfo *flinfo, Oid relid);
static void get_elem_type_output(ArrayMetaState *state, Oid elem_type,
								 MemoryContext mctx);
static Datum array_iter_next_(array_iter *it, int idx, ArrayMetaState *state);
static void deform_tuple(HeapTupleHeader tuphdr, Datum *values, bool *isnull);
static Datum makeArrayTypeDatum(Datum *elems, int nelem, Oid type);

Datum
graphid_in(PG_FUNCTION_ARGS)
{
	const char	GRAPHID_DELIM = '.';
	char	   *str = PG_GETARG_CSTRING(0);
	char	   *next;
	char	   *endptr;
	Graphid		id;

	errno = 0;
	id.oid = strtoul(str, &endptr, 10);
	if (errno != 0 || endptr == str || *endptr != GRAPHID_DELIM)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type graphid: \"%s\"", str)));

	next = endptr + 1;
#ifdef HAVE_STRTOLL
	id.lid = strtoll(next, &endptr, 10);
	if (endptr == next || *endptr != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type graphid: \"%s\"", str)));
#else
	id.lid = DatumGetInt64(DirectFunctionCall1(int8in, CStringGetDatum(next)));
#endif

	PG_RETURN_DATUM(getGraphidDatum(id));
}

Datum
graphid_out(PG_FUNCTION_ARGS)
{
	Graphid		id;
	char	   *buf;

	id = getGraphidStruct(PG_GETARG_DATUM(0));
	buf = palloc(GRAPHID_BUFLEN);
	snprintf(buf, GRAPHID_BUFLEN, GRAPHID_FMTSTR, id.oid, id.lid);

	PG_RETURN_CSTRING(buf);
}

static void
graphid_out_si(StringInfo si, Datum graphid)
{
	Graphid id;

	id = getGraphidStruct(graphid);
	appendStringInfo(si, GRAPHID_FMTSTR, id.oid, id.lid);
}

Datum
vertex_out(PG_FUNCTION_ARGS)
{
	HeapTupleHeader vertex = PG_GETARG_HEAPTUPLEHEADER(0);
	Datum		values[Natts_vertex];
	bool		isnull[Natts_vertex];
	Graphid		id;
	Jsonb	   *prop_map;
	LabelOutData *my_extra;
	StringInfoData si;

	deform_tuple(vertex, values, isnull);

	if (isnull[Anum_vertex_id - 1])
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("id in vertex cannot be NULL")));
	if (isnull[Anum_vertex_properties - 1])
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("properties in vertex cannot be NULL")));

	id = getGraphidStruct(values[Anum_vertex_id - 1]);
	prop_map = DatumGetJsonb(values[Anum_vertex_properties - 1]);

	my_extra = cache_label(fcinfo->flinfo, id.oid);

	initStringInfo(&si);
	appendStringInfo(&si, "%s[" GRAPHID_FMTSTR "]",
					 NameStr(my_extra->label), id.oid, id.lid);
	JsonbToCString(&si, &prop_map->root, VARSIZE(prop_map));

	PG_RETURN_CSTRING(si.data);
}

Datum
edge_out(PG_FUNCTION_ARGS)
{
	HeapTupleHeader edge = PG_GETARG_HEAPTUPLEHEADER(0);
	Datum		values[Natts_edge];
	bool		isnull[Natts_edge];
	Graphid		id;
	Jsonb	   *prop_map;
	LabelOutData *my_extra;
	StringInfoData si;

	deform_tuple(edge, values, isnull);

	if (isnull[Anum_edge_id - 1])
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("id in edge cannot be NULL")));
	if (isnull[Anum_edge_start - 1])
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("start in edge cannot be NULL")));
	if (isnull[Anum_edge_end - 1])
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("end in edge cannot be NULL")));
	if (isnull[Anum_edge_properties - 1])
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("properties in edge cannot be NULL")));

	id = getGraphidStruct(values[Anum_edge_id - 1]);
	prop_map = DatumGetJsonb(values[Anum_edge_properties - 1]);

	my_extra = cache_label(fcinfo->flinfo, id.oid);

	initStringInfo(&si);
	appendStringInfo(&si, ":%s[" GRAPHID_FMTSTR "][",
					 NameStr(my_extra->label), id.oid, id.lid);
	graphid_out_si(&si, values[Anum_edge_start - 1]);
	appendStringInfoChar(&si, ',');
	graphid_out_si(&si, values[Anum_edge_end - 1]);
	appendStringInfoChar(&si, ']');
	JsonbToCString(&si, &prop_map->root, VARSIZE(prop_map));

	PG_RETURN_CSTRING(si.data);
}

static LabelOutData *
cache_label(FmgrInfo *flinfo, Oid relid)
{
	MemoryContext oldMemoryContext;
	LabelOutData *my_extra;

	AssertArg(flinfo != NULL);

	oldMemoryContext = MemoryContextSwitchTo(flinfo->fn_mcxt);

	my_extra = (LabelOutData *) flinfo->fn_extra;
	if (my_extra == NULL)
	{
		flinfo->fn_extra = palloc(sizeof(*my_extra));
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

	MemoryContextSwitchTo(oldMemoryContext);

	return my_extra;
}

Datum
graphpath_out(PG_FUNCTION_ARGS)
{
	const char	delim = ',';
	Datum vertices_datum;
	Datum edges_datum;
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

	getGraphpathArrays(PG_GETARG_DATUM(0), &vertices_datum, &edges_datum);

	vertices = DatumGetAnyArray(vertices_datum);
	edges = DatumGetAnyArray(edges_datum);

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

static void
deform_tuple(HeapTupleHeader tuphdr, Datum *values, bool *isnull)
{
	Oid			tupType;
	TupleDesc	tupDesc;
	HeapTupleData tuple;

	tupType = HeapTupleHeaderGetTypeId(tuphdr);
	tupDesc = lookup_rowtype_tupdesc(tupType, -1);

	tuple.t_len = HeapTupleHeaderGetDatumLength(tuphdr);
	ItemPointerSetInvalid(&tuple.t_self);
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = tuphdr;

	heap_deform_tuple(&tuple, tupDesc, values, isnull);
	ReleaseTupleDesc(tupDesc);
}

Graphid
getGraphidStruct(Datum datum)
{
	HeapTupleHeader tuphdr;
	Datum		values[Natts_graphid];
	bool		isnull[Natts_graphid];
	Graphid		id;

	tuphdr = DatumGetHeapTupleHeader(datum);

	deform_tuple(tuphdr, values, isnull);

	if (isnull[Anum_graphid_oid - 1])
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("oid in graphid cannot be NULL")));
	if (isnull[Anum_graphid_lid - 1])
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("lid in graphid cannot be NULL")));

	id.oid = DatumGetObjectId(values[Anum_graphid_oid - 1]);
	id.lid = DatumGetInt64(values[Anum_graphid_lid - 1]);

	return id;
}

Datum
getGraphidDatum(Graphid id)
{
	Datum		values[Natts_graphid];
	bool		isnull[Natts_graphid] = {false, false};
	TupleDesc	tupDesc;
	HeapTuple	tuple;

	values[Anum_graphid_oid - 1] = ObjectIdGetDatum(id.oid);
	values[Anum_graphid_lid - 1] = Int64GetDatum(id.lid);

	tupDesc = lookup_rowtype_tupdesc(GRAPHIDOID, -1);
	Assert(tupDesc->natts == Natts_graphid);

	tuple = heap_form_tuple(tupDesc, values, isnull);

	ReleaseTupleDesc(tupDesc);

	return HeapTupleGetDatum(tuple);
}

Datum
getVertexIdDatum(Datum datum)
{
	HeapTupleHeader	tuphdr;
	Oid			tupType;
	TupleDesc	tupDesc;
	HeapTupleData tuple;
	bool		isnull = false;
	Datum		id;

	tuphdr = DatumGetHeapTupleHeader(datum);

	tupType = HeapTupleHeaderGetTypeId(tuphdr);
	Assert(tupType == VERTEXOID);

	tupDesc = lookup_rowtype_tupdesc(tupType, -1);
	Assert(tupDesc->natts == Natts_vertex);

	tuple.t_len = HeapTupleHeaderGetDatumLength(tuphdr);
	ItemPointerSetInvalid(&tuple.t_self);
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = tuphdr;

	id = heap_getattr(&tuple, Anum_vertex_id, tupDesc, &isnull);
	ReleaseTupleDesc(tupDesc);
	Assert(!isnull);

	return id;
}

Datum
getEdgeIdDatum(Datum datum)
{
	HeapTupleHeader	tuphdr;
	Oid			tupType;
	TupleDesc	tupDesc;
	HeapTupleData tuple;
	bool		isnull = false;
	Datum		id;

	tuphdr = DatumGetHeapTupleHeader(datum);

	tupType = HeapTupleHeaderGetTypeId(tuphdr);
	Assert(tupType == EDGEOID);

	tupDesc = lookup_rowtype_tupdesc(tupType, -1);
	Assert(tupDesc->natts == Natts_edge);

	tuple.t_len = HeapTupleHeaderGetDatumLength(tuphdr);
	ItemPointerSetInvalid(&tuple.t_self);
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = tuphdr;

	id = heap_getattr(&tuple, Anum_edge_id, tupDesc, &isnull);
	ReleaseTupleDesc(tupDesc);
	Assert(!isnull);

	return id;
}

void
getGraphpathArrays(Datum graphpath, Datum *vertices, Datum *edges)
{
	HeapTupleHeader	tuphdr;
	Oid			tupType;
	TupleDesc	tupDesc;
	HeapTupleData tuple;
	Datum		values[Natts_graphpath];
	bool		isnull[Natts_graphpath];

	tuphdr = DatumGetHeapTupleHeader(graphpath);

	tupType = HeapTupleHeaderGetTypeId(tuphdr);
	Assert(tupType == GRAPHPATHOID);

	tupDesc = lookup_rowtype_tupdesc(tupType, -1);
	Assert(tupDesc->natts == Natts_graphpath);

	tuple.t_len = HeapTupleHeaderGetDatumLength(tuphdr);
	ItemPointerSetInvalid(&tuple.t_self);
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = tuphdr;

	heap_deform_tuple(&tuple, tupDesc, values, isnull);
	ReleaseTupleDesc(tupDesc);
	Assert(!isnull[Anum_graphpath_vertices - 1]);
	Assert(!isnull[Anum_graphpath_edges - 1]);

	*vertices = values[Anum_graphpath_vertices - 1];
	*edges = values[Anum_graphpath_edges - 1];
}

Datum
makeGraphpathDatum(Datum *vertices, int nvertices, Datum *edges, int nedges)
{
	Datum		values[Natts_graphpath];
	bool		isnull[Natts_graphpath] = {false, false};
	TupleDesc	tupDesc;
	HeapTuple	graphpath;

	values[Anum_graphpath_vertices - 1]
					= makeArrayTypeDatum(vertices, nvertices, VERTEXOID);
	values[Anum_graphpath_edges - 1]
					= makeArrayTypeDatum(edges, nedges, EDGEOID);

	tupDesc = lookup_rowtype_tupdesc(GRAPHPATHOID, -1);
	Assert(tupDesc->natts == Natts_graphpath);

	graphpath = heap_form_tuple(tupDesc, values, isnull);

	ReleaseTupleDesc(tupDesc);

	return HeapTupleGetDatum(graphpath);
}

static Datum
makeArrayTypeDatum(Datum *elems, int nelem, Oid type)
{
	int16		typlen;
	bool		typbyval;
	char		typalign;
	ArrayType  *arr;

	get_typlenbyvalalign(type, &typlen, &typbyval, &typalign);

	arr = construct_array(elems, nelem, type, typlen, typbyval, typalign);

	return PointerGetDatum(arr);
}
