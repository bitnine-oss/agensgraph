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

#include "ag_const.h"
#include "access/hash.h"
#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "catalog/ag_graph_fn.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "utils/array.h"
#include "utils/arrayaccess.h"
#include "utils/builtins.h"
#include "utils/graph.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

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

typedef enum EdgeVertexKind {
	EVK_START,
	EVK_END
} EdgeVertexKind;

static void graphid_out_si(StringInfo si, Datum graphid);
static int graphid_cmp(FunctionCallInfo fcinfo);
static LabelOutData *cache_label(FmgrInfo *flinfo, Oid relid);
static void elems_out_si(StringInfo si, AnyArrayType *elems, FmgrInfo *flinfo);
static void get_elem_type_output(ArrayMetaState *state, Oid elem_type,
								 MemoryContext mctx);
static Datum array_iter_next_(array_iter *it, int idx, ArrayMetaState *state);
static void deform_tuple(HeapTupleHeader tuphdr, Datum *values, bool *isnull);
static Datum tuple_getattr(HeapTupleHeader tuphdr, int attnum);
static Datum getEdgeVertex(HeapTupleHeader edge, EdgeVertexKind evk);
static Datum makeArrayTypeDatum(Datum *elems, int nelem, Oid type);
static Datum graphid_minval(void);

Datum
graphid(PG_FUNCTION_ARGS)
{
	Oid			oid = PG_GETARG_OID(0);
	int64		lid = PG_GETARG_INT64(1);
	Graphid	   *id;

	id = palloc0fast(sizeof(*id));
	id->oid = oid;
	id->lid = lid;

	PG_RETURN_GRAPHID_P(id);
}

Datum
graphid_in(PG_FUNCTION_ARGS)
{
	const char	GRAPHID_DELIM = '.';
	char	   *str = PG_GETARG_CSTRING(0);
	char	   *next;
	char	   *endptr;
	Graphid		_id;
	Graphid	   *id;

	errno = 0;
	_id.oid = strtoul(str, &endptr, 10);
	if (errno != 0 || endptr == str || *endptr != GRAPHID_DELIM)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type graphid: \"%s\"", str)));

	next = endptr + 1;
#ifdef HAVE_STRTOLL
	_id.lid = strtoll(next, &endptr, 10);
	if (endptr == next || *endptr != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type graphid: \"%s\"", str)));
#else
	_id.lid = DatumGetInt64(DirectFunctionCall1(int8in, CStringGetDatum(next)));
#endif

	id = palloc0fast(sizeof(*id));
	id->oid = _id.oid;
	id->lid = _id.lid;

	PG_RETURN_GRAPHID_P(id);
}

Datum
graphid_out(PG_FUNCTION_ARGS)
{
	Graphid	   *id = PG_GETARG_GRAPHID_P(0);
	char	   *buf;

	buf = palloc(GRAPHID_BUFLEN);
	snprintf(buf, GRAPHID_BUFLEN, GRAPHID_FMTSTR, id->oid, id->lid);

	PG_RETURN_CSTRING(buf);
}

static void
graphid_out_si(StringInfo si, Datum graphid)
{
	Graphid *id = DatumGetGraphidP(graphid);

	appendStringInfo(si, GRAPHID_FMTSTR, id->oid, id->lid);
}

Datum
graphid_oid(PG_FUNCTION_ARGS)
{
	Graphid *id = PG_GETARG_GRAPHID_P(0);

	PG_RETURN_OID(id->oid);
}

Datum
graphid_lid(PG_FUNCTION_ARGS)
{
	Graphid *id = PG_GETARG_GRAPHID_P(0);

	PG_RETURN_INT64(id->lid);
}

static int
graphid_cmp(FunctionCallInfo fcinfo)
{
	Graphid	   *id1 = PG_GETARG_GRAPHID_P(0);
	Graphid	   *id2 = PG_GETARG_GRAPHID_P(1);

	if (id1->oid < id2->oid)
		return -1;
	if (id1->oid > id2->oid)
		return 1;
	if (id1->lid < id2->lid)
		return -1;
	if (id1->lid > id2->lid)
		return 1;

	return 0;
}

Datum
graphid_eq(PG_FUNCTION_ARGS)
{
	/* use graphid_cmp() here, since graphid have a total ordering */
	PG_RETURN_BOOL(graphid_cmp(fcinfo) == 0);
}

Datum
graphid_ne(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(graphid_cmp(fcinfo) != 0);
}

Datum
graphid_lt(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(graphid_cmp(fcinfo) < 0);
}

Datum
graphid_gt(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(graphid_cmp(fcinfo) > 0);
}

Datum
graphid_le(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(graphid_cmp(fcinfo) <= 0);
}

Datum
graphid_ge(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(graphid_cmp(fcinfo) >= 0);
}

Datum
vertex_out(PG_FUNCTION_ARGS)
{
	HeapTupleHeader vertex = PG_GETARG_HEAPTUPLEHEADER(0);
	Datum		values[Natts_vertex];
	bool		isnull[Natts_vertex];
	Graphid	   *id;
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

	id = DatumGetGraphidP(values[Anum_vertex_id - 1]);
	prop_map = DatumGetJsonb(values[Anum_vertex_properties - 1]);

	my_extra = cache_label(fcinfo->flinfo, id->oid);

	initStringInfo(&si);
	appendStringInfo(&si, "%s[" GRAPHID_FMTSTR "]",
					 NameStr(my_extra->label), id->oid, id->lid);
	JsonbToCString(&si, &prop_map->root, VARSIZE(prop_map));

	PG_RETURN_CSTRING(si.data);
}

Datum
_vertex_out(PG_FUNCTION_ARGS)
{
	AnyArrayType *vertices = PG_GETARG_ANY_ARRAY(0);
	StringInfoData si;

	initStringInfo(&si);
	elems_out_si(&si, vertices, fcinfo->flinfo);

	PG_RETURN_CSTRING(si.data);
}

Datum
vertex_label(PG_FUNCTION_ARGS)
{
	Graphid *id;
	LabelOutData *my_extra;

	id = DatumGetGraphidP(getVertexIdDatum(PG_GETARG_DATUM(0)));

	my_extra = cache_label(fcinfo->flinfo, id->oid);

	PG_RETURN_TEXT_P(cstring_to_text(NameStr(my_extra->label)));
}

Datum
vtojb(PG_FUNCTION_ARGS)
{
	HeapTupleHeader vertex = PG_GETARG_HEAPTUPLEHEADER(0);

	PG_RETURN_DATUM(tuple_getattr(vertex, Anum_vertex_properties));
}

Datum
edge_out(PG_FUNCTION_ARGS)
{
	HeapTupleHeader edge = PG_GETARG_HEAPTUPLEHEADER(0);
	Datum		values[Natts_edge];
	bool		isnull[Natts_edge];
	Graphid	   *id;
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

	id = DatumGetGraphidP(values[Anum_edge_id - 1]);
	prop_map = DatumGetJsonb(values[Anum_edge_properties - 1]);

	my_extra = cache_label(fcinfo->flinfo, id->oid);

	initStringInfo(&si);
	appendStringInfo(&si, "%s[" GRAPHID_FMTSTR "][",
					 NameStr(my_extra->label), id->oid, id->lid);
	graphid_out_si(&si, values[Anum_edge_start - 1]);
	appendStringInfoChar(&si, ',');
	graphid_out_si(&si, values[Anum_edge_end - 1]);
	appendStringInfoChar(&si, ']');
	JsonbToCString(&si, &prop_map->root, VARSIZE(prop_map));

	PG_RETURN_CSTRING(si.data);
}

Datum
_edge_out(PG_FUNCTION_ARGS)
{
	AnyArrayType *edges = PG_GETARG_ANY_ARRAY(0);
	StringInfoData si;

	initStringInfo(&si);
	elems_out_si(&si, edges, fcinfo->flinfo);

	PG_RETURN_CSTRING(si.data);
}

Datum
edge_label(PG_FUNCTION_ARGS)
{
	Graphid *id;
	LabelOutData *my_extra;

	id = DatumGetGraphidP(getEdgeIdDatum(PG_GETARG_DATUM(0)));

	my_extra = cache_label(fcinfo->flinfo, id->oid);

	PG_RETURN_TEXT_P(cstring_to_text(NameStr(my_extra->label)));
}

Datum
etojb(PG_FUNCTION_ARGS)
{
	HeapTupleHeader edge = PG_GETARG_HEAPTUPLEHEADER(0);

	PG_RETURN_DATUM(tuple_getattr(edge, Anum_edge_properties));
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

static void
elems_out_si(StringInfo si, AnyArrayType *elems, FmgrInfo *flinfo)
{
	const char	delim = ',';
	ArrayMetaState *my_extra;
	int			nelems;
	array_iter	it;
	Datum		value;
	int			i;

	my_extra = (ArrayMetaState *) flinfo->fn_extra;
	if (my_extra == NULL)
	{
		flinfo->fn_extra = MemoryContextAlloc(flinfo->fn_mcxt,
											  sizeof(*my_extra));
		my_extra = (ArrayMetaState *) flinfo->fn_extra;
		get_elem_type_output(my_extra, AARR_ELEMTYPE(elems), flinfo->fn_mcxt);
	}

	nelems = ArrayGetNItems(AARR_NDIM(elems), AARR_DIMS(elems));

	appendStringInfoChar(si, '[');
	array_iter_setup(&it, elems);
	if (nelems > 0)
	{
		value = array_iter_next_(&it, 0, my_extra);
		appendStringInfoString(si, OutputFunctionCall(&my_extra->proc, value));
	}
	for (i = 1; i < nelems; i++)
	{
		appendStringInfoChar(si, delim);

		value = array_iter_next_(&it, i, my_extra);
		appendStringInfoString(si, OutputFunctionCall(&my_extra->proc, value));
	}
	appendStringInfoChar(si, ']');
}

Datum
graphpath_out(PG_FUNCTION_ARGS)
{
	const char	delim = ',';
	Datum		vertices_datum;
	Datum		edges_datum;
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
													  sizeof(*my_extra));
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

Datum
graphpath_length(PG_FUNCTION_ARGS)
{
	Datum		edges_datum;
	AnyArrayType *edges;
	int			nedges;

	edges_datum = DirectFunctionCall1(graphpath_edges, PG_GETARG_DATUM(0));
	edges = DatumGetAnyArray(edges_datum);
	nedges = ArrayGetNItems(AARR_NDIM(edges), AARR_DIMS(edges));

	PG_RETURN_INT32(nedges);
}

Datum
graphpath_vertices(PG_FUNCTION_ARGS)
{
	Datum vertices_datum;

	getGraphpathArrays(PG_GETARG_DATUM(0), &vertices_datum, NULL);

	PG_RETURN_DATUM(vertices_datum);
}

Datum
graphpath_edges(PG_FUNCTION_ARGS)
{
	Datum edges_datum;

	getGraphpathArrays(PG_GETARG_DATUM(0), NULL, &edges_datum);

	PG_RETURN_DATUM(edges_datum);
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

static Datum
tuple_getattr(HeapTupleHeader tuphdr, int attnum)
{
	Oid			tupType;
	TupleDesc	tupDesc;
	HeapTupleData tuple;
	bool		isnull = false;
	Datum		attdat;

	tupType = HeapTupleHeaderGetTypeId(tuphdr);
	tupDesc = lookup_rowtype_tupdesc(tupType, -1);

	tuple.t_len = HeapTupleHeaderGetDatumLength(tuphdr);
	ItemPointerSetInvalid(&tuple.t_self);
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = tuphdr;

	attdat = heap_getattr(&tuple, attnum, tupDesc, &isnull);
	ReleaseTupleDesc(tupDesc);
	Assert(!isnull);

	return attdat;
}

Datum
edge_start_vertex(PG_FUNCTION_ARGS)
{
	HeapTupleHeader edge = PG_GETARG_HEAPTUPLEHEADER(0);

	return getEdgeVertex(edge, EVK_START);
}

Datum
edge_end_vertex(PG_FUNCTION_ARGS)
{
	HeapTupleHeader edge = PG_GETARG_HEAPTUPLEHEADER(0);

	return getEdgeVertex(edge, EVK_END);
}

static Datum
getEdgeVertex(HeapTupleHeader edge, EdgeVertexKind evk)
{
	const char *querystr =
			"SELECT (" AG_ELEM_LOCAL_ID ", " AG_ELEM_PROP_MAP ")::vertex "
			"FROM \"%s\"." AG_VERTEX " WHERE " AG_ELEM_LOCAL_ID " = $1";
	char		sqlcmd[256];
	int			attnum = (evk == EVK_START ? Anum_edge_start : Anum_edge_end);
	Datum		values[1];
	Oid			argTypes[1] = {GRAPHIDOID};
	bool		spi_pushed;
	int			ret;
	Datum		vertex;
	bool		isnull;

	snprintf(sqlcmd, sizeof(sqlcmd), querystr, get_graph_path());

	values[0] = tuple_getattr(edge, attnum);

	spi_pushed = SPI_push_conditional();

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	ret = SPI_execute_with_args(sqlcmd, 2, argTypes, values, NULL, false, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute failed: %s", sqlcmd);

	if (SPI_processed != 1)
		elog(ERROR, (evk == EVK_START
					 ? "SPI_execute: only one start vertex of edge exists"
					 : "SPI_execute: only one end vertex of edge exists"));

	vertex = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc,
						   1, &isnull);
	Assert(!isnull);

	vertex = SPI_datumTransfer(vertex, false, -1);

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	SPI_pop_conditional(spi_pushed);

	return vertex;
}

Datum
vertex_labels(PG_FUNCTION_ARGS)
{
	Graphid	   *id;
	LabelOutData *my_extra;
	Datum		label;

	id = DatumGetGraphidP(getVertexIdDatum(PG_GETARG_DATUM(0)));

	my_extra = cache_label(fcinfo->flinfo, id->oid);

	label = CStringGetTextDatum(NameStr(my_extra->label));

	PG_RETURN_ARRAYTYPE_P(makeArrayTypeDatum(&label, 1, TEXTOID));
}

Datum
getVertexIdDatum(Datum datum)
{
	HeapTupleHeader	tuphdr = DatumGetHeapTupleHeader(datum);

	return tuple_getattr(tuphdr, Anum_vertex_id);
}

Datum
getEdgeIdDatum(Datum datum)
{
	HeapTupleHeader	tuphdr = DatumGetHeapTupleHeader(datum);

	return tuple_getattr(tuphdr, Anum_edge_id);
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

	if (vertices != NULL)
		*vertices = values[Anum_graphpath_vertices - 1];
	if (edges != NULL)
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

/*
 * BTree support functions
 */

/* BTORDER_PROC (1) */
Datum
btgraphidcmp(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(graphid_cmp(fcinfo));
}

/*
 * Hash support functions
 */

/* HASHPROC (1) */
Datum
graphid_hash(PG_FUNCTION_ARGS)
{
	Graphid *id = PG_GETARG_GRAPHID_P(0);

	StaticAssertStmt(sizeof(*id) == 16, "the size of graphid must be 16");

	return hash_any((unsigned char *) id, sizeof(*id));
}

/*
 * GIN (as BTree) support functions
 */

/* Note: GIN_COMPARE_PROC (1) is btgraphidcmp() */

/* GIN_EXTRACTVALUE_PROC (2) - called by ginExtractEntries() */
Datum
gin_extract_value_graphid(PG_FUNCTION_ARGS)
{
	const int32	_nentries = 1;
	Graphid	   *graphid = PG_GETARG_GRAPHID_P(0);
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	Datum	   *entries = palloc(sizeof(*entries) * _nentries);

	*nentries = _nentries;
	entries[0] = GraphidPGetDatum(graphid);

	PG_RETURN_POINTER(entries);
}

/*
 * GIN_EXTRACTQUERY_PROC (3) - called by ginNewScanKey()
 *
 * GIN does not have a fixed set of strategies. Instead, the support routines
 * of each operator class interpret the strategy numbers.
 * We use strategy numbers of BTree.
 *
 * nullFlags and searchMode will be set by the caller.
 */
Datum
gin_extract_query_graphid(PG_FUNCTION_ARGS)
{
	const int32	_nentries = 1;
	Datum		graphid = PG_GETARG_DATUM(0);
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	StrategyNumber strategy = PG_GETARG_UINT16(2);
	bool	  **partial_matches = (bool **) PG_GETARG_POINTER(3);
	Pointer   **extra_data = (Pointer **) PG_GETARG_POINTER(4);
	Datum	   *entries;

	*nentries = _nentries;
	*partial_matches = palloc(sizeof(**partial_matches) * _nentries);
	entries = palloc(sizeof(*entries) * _nentries);

	switch (strategy)
	{
		case BTLessStrategyNumber:
		case BTLessEqualStrategyNumber:
			/*
			 * We should start scan from the smallest indexed key until the
			 * scan meets the given graphid. To do this, we set the
			 * entry(query value) to the minimum value of graphid (to make GIN
			 * to find the smallest indexed key), enable partial match, and
			 * store the original graphid into the extra_data for later use in
			 * partial match.
			 */
			{
				(*partial_matches)[0] = true;

				*extra_data = palloc(sizeof(**extra_data) * _nentries);
				(*extra_data)[0] = DatumGetPointer(graphid);

				entries[0] = graphid_minval();
			}
			break;
		case BTEqualStrategyNumber:
			/* exact match */
			(*partial_matches)[0] = false;
			entries[0] = graphid;
			break;
		case BTGreaterEqualStrategyNumber:
		case BTGreaterStrategyNumber:
			(*partial_matches)[0] = true;
			entries[0] = graphid;
			break;
		default:
			elog(ERROR, "unrecognized strategy number: %d", strategy);
	}

	PG_RETURN_POINTER(entries);
}

static Datum
graphid_minval(void)
{
	Graphid *id;

	id = palloc0fast(sizeof(*id));
	id->oid = InvalidOid;
	id->lid = 0;

	return GraphidPGetDatum(id);
}

/* GIN_CONSISTENT_PROC (4) - same as trueConsistentFn() */
Datum
gin_consistent_graphid(PG_FUNCTION_ARGS)
{
	bool *recheck = (bool *) PG_GETARG_POINTER(5);

	*recheck = false;
	PG_RETURN_BOOL(true);
}

/*
 * GIN_COMPARE_PARTIAL_PROC (5)
 *
 * See collectMatchBitmap() for the caller's context
 */
Datum
gin_compare_partial_graphid(FunctionCallInfo fcinfo)
{
	Datum		qrykey = PG_GETARG_DATUM(0);
	Datum		idxkey = PG_GETARG_DATUM(1);
	StrategyNumber strategy = PG_GETARG_UINT16(2);
	Datum		graphid = PG_GETARG_DATUM(3);
	int32		cmp;
	int32		res;

	/*
	 * In these cases, qrykey is the minimum value of graphid. To compare
	 * qrykey with idxkey properly, restore the original graphid from the
	 * extra_data, and set it to qrykey.
	 */
	if (strategy == BTLessStrategyNumber ||
		strategy == BTLessEqualStrategyNumber)
		qrykey = graphid;

	cmp = DatumGetInt32(DirectFunctionCall2Coll(btgraphidcmp,
												PG_GET_COLLATION(),
												idxkey, qrykey));

	switch (strategy)
	{
		case BTLessStrategyNumber:
			/* idxkey < qrykey ? still match : finish scan */
			res = (cmp < 0 ? 0 : 1);
			break;
		case BTLessEqualStrategyNumber:
			res = (cmp <= 0 ? 0 : 1);
			break;
		case BTEqualStrategyNumber:
			res = (cmp == 0 ? 0 : 1);
			break;
		case BTGreaterEqualStrategyNumber:
			res = (cmp >= 0 ? 0 : 1);
			break;
		case BTGreaterStrategyNumber:
			if (cmp > 0)
				res = 0;
			else if (cmp == 0)
				res = -1;		/* not match, continue scan */
			else
				res = 1;
			break;
		default:
			elog(ERROR, "unrecognized strategy number: %d", strategy);
			res = 0;
	}

	PG_RETURN_INT32(res);
}
