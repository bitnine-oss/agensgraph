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

#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/supportnodes.h"
#include "optimizer/optimizer.h"

#include "ag_const.h"
#include "access/hash.h"
#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "catalog/ag_graph_fn.h"
#include "catalog/ag_label.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "utils/array.h"
#include "utils/arrayaccess.h"
#include "utils/builtins.h"
#include "utils/graph.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/regproc.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "catalog/pg_inherits.h"
#include "catalog/ag_vertex_d.h"
#include "catalog/ag_edge_d.h"
#include "catalog/ag_graphpath_d.h"
#include "access/genam.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/pg_am_d.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/relcache.h"

#define GRAPHID_FMTSTR			"%hu." UINT64_FORMAT
#define GRAPHID_BUFLEN			32	/* "65535.281474976710655" */

typedef struct LabelOutData
{
	uint16		label_labid;
	NameData	label;
} LabelOutData;

typedef struct GraphpathOutData
{
	ArrayMetaState vertex;
	ArrayMetaState edge;
} GraphpathOutData;

typedef struct LabelsOutData
{
	MemoryContext mctx;
	uint16		label_labid;
	Jsonb	   *labels;
} LabelsOutData;

static void graphid_out_si(StringInfo si, Datum graphid);
static int	graphid_cmp(FunctionCallInfo fcinfo);
static Jsonb *int_to_jsonb(int i);
static LabelOutData *cache_label(FmgrInfo *flinfo, uint16 labid);
static void elems_out_si(StringInfo si, AnyArrayType *elems, FmgrInfo *flinfo);
static void appendStringInfoDatumOut(StringInfo si, Datum d, bool isnull,
									 FmgrInfo *out);
static void get_elem_type_output(ArrayMetaState *state, Oid elem_type,
								 MemoryContext mctx);
static Datum array_iter_next_(array_iter *it, bool *isnull, int idx,
							  ArrayMetaState *state);
static void deform_tuple(HeapTupleHeader tuphdr, Datum *values, bool *isnull);
static Datum tuple_getattr(HeapTupleHeader tuphdr, int attnum);
static LabelsOutData *cache_labels(FmgrInfo *flinfo, uint16 labid);
static Datum makeArrayTypeDatum(Datum *elems, int nelem, Oid type);
static Datum graphid_minval(void);

Datum
graphid(PG_FUNCTION_ARGS)
{
	int32		labid_i4 = PG_GETARG_INT32(0);
	int64		locid_i8 = PG_GETARG_INT64(1);
	Graphid		id;

	if (labid_i4 < 0 || labid_i4 > (int32) GRAPHID_LABID_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("labid out of range: %d", labid_i4)));

	if (locid_i8 < 0 || locid_i8 > (int64) GRAPHID_LOCID_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("locid out of range: " INT64_FORMAT, locid_i8)));

	GraphidSet(&id, (uint16) labid_i4, (uint64) locid_i8);

	PG_RETURN_GRAPHID(id);
}

Datum
graphid_in(PG_FUNCTION_ARGS)
{
	const char	GRAPHID_DELIM = '.';
	char	   *str = PG_GETARG_CSTRING(0);
	char	   *next;
	char	   *endptr;
	unsigned long labid_ul;
	uint16		labid;
	uint64		locid;
	Graphid		id;

	errno = 0;
	labid_ul = strtoul(str, &endptr, 10);
	if (errno != 0 || endptr == str || *endptr != GRAPHID_DELIM)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type graphid: \"%s\"", str)));
	if (labid_ul > GRAPHID_LABID_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("labid out of range")));
	labid = (uint16) labid_ul;

	next = endptr + 1;
	locid = strtoull(next, &endptr, 10);
	if (errno != 0 || endptr == next || *endptr != '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type graphid: \"%s\"", str)));
	if (locid > GRAPHID_LOCID_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("locid out of range")));

	GraphidSet(&id, labid, locid);

	PG_RETURN_GRAPHID(id);
}

Datum
graphid_out(PG_FUNCTION_ARGS)
{
	Graphid		id = PG_GETARG_GRAPHID(0);
	char	   *buf;

	buf = palloc(GRAPHID_BUFLEN);
	snprintf(buf, GRAPHID_BUFLEN, GRAPHID_FMTSTR,
			 GraphidGetLabid(id), GraphidGetLocid(id));

	PG_RETURN_CSTRING(buf);
}

Datum
graphid_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

	PG_RETURN_GRAPHID((Graphid) pq_getmsgint64(buf));
}

Datum
graphid_send(PG_FUNCTION_ARGS)
{
	Graphid		arg1 = PG_GETARG_GRAPHID(0);
	StringInfoData buf;

	pq_begintypsend(&buf);
	pq_sendint64(&buf, arg1);
	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

static void
graphid_out_si(StringInfo si, Datum graphid)
{
	Graphid		id = DatumGetGraphid(graphid);

	appendStringInfo(si, GRAPHID_FMTSTR,
					 GraphidGetLabid(id), GraphidGetLocid(id));
}

Datum
graphid_labid(PG_FUNCTION_ARGS)
{
	Graphid		id = PG_GETARG_GRAPHID(0);

	PG_RETURN_INT32(GraphidGetLabid(id));
}

Datum
graphid_locid(PG_FUNCTION_ARGS)
{
	Graphid		id = PG_GETARG_GRAPHID(0);

	PG_RETURN_INT64(GraphidGetLocid(id));
}

Datum
graph_labid(PG_FUNCTION_ARGS)
{
	char	   *labname = PG_GETARG_CSTRING(0);
	List	   *names;
	RangeVar   *rv;
	Oid			graphoid;
	uint16		labid;

	names = stringToQualifiedNameList(labname, NULL);
	rv = makeRangeVarFromNameList(names);
	graphoid = get_graphname_oid(rv->schemaname);
	labid = get_labname_labid(rv->relname, graphoid);

	PG_RETURN_INT32((int32) labid);
}

static int
graphid_cmp(FunctionCallInfo fcinfo)
{
	Graphid		id1 = PG_GETARG_GRAPHID(0);
	Graphid		id2 = PG_GETARG_GRAPHID(1);

	if (id1 < id2)
		return -1;
	if (id1 > id2)
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

/*
 * Parse a graphid out of a (not necessarily NUL-terminated) string of the form
 * "labid.locid".  Unlike graphid_in(), a malformed string yields false rather
 * than an error, because this is used to test membership against an arbitrary
 * jsonb array whose elements need not be graphids: a non-graphid element simply
 * cannot be a member and must be skipped, not rejected.
 */
static bool
graphid_from_cstring(const char *str, Graphid *result)
{
	const char	GRAPHID_DELIM = '.';
	char	   *endptr;
	const char *next;
	unsigned long labid_ul;
	uint16		labid;
	uint64		locid;

	errno = 0;
	labid_ul = strtoul(str, &endptr, 10);
	if (errno != 0 || endptr == str || *endptr != GRAPHID_DELIM)
		return false;
	if (labid_ul > GRAPHID_LABID_MAX)
		return false;
	labid = (uint16) labid_ul;

	next = endptr + 1;
	errno = 0;
	locid = strtoull(next, &endptr, 10);
	if (errno != 0 || endptr == next || *endptr != '\0')
		return false;
	if (locid > GRAPHID_LOCID_MAX)
		return false;

	GraphidSet(result, labid, locid);
	return true;
}

/*
 * graphid_in_jsonb_array(graphid, jsonb) -> bool
 *
 * True iff the graphid equals the identity of some element of the jsonb array.
 * Each element is either a serialized vertex/edge object {"id": "labid.locid",
 * ...} (from to_jsonb()/collect()) or a bare graphid string "labid.locid" (from
 * collect(id(u))); we read its "id" (or the string itself), parse it, and
 * compare by graphid identity -- matching the id-only "=" operator, unlike the
 * jsonb "@>" containment this replaces, which compared the full serialization
 * (id *and* properties) and so diverged from "=" once a collected vertex was
 * mutated.  Iterates the array once and short-circuits on the first match.
 */
Datum
graphid_in_jsonb_array(PG_FUNCTION_ARGS)
{
	Graphid		id = PG_GETARG_GRAPHID(0);
	Jsonb	   *arr = PG_GETARG_JSONB_P(1);
	JsonbContainer *jc = &arr->root;
	int			n;
	int			i;

	/*
	 * A naked jsonb scalar is stored as a one-element pseudo-array flagged
	 * JB_FSCALAR, so JsonContainerIsArray() alone would let it through and then
	 * test the scalar as element 0.  Reject it: a non-list RHS has no members.
	 */
	if (!JsonContainerIsArray(jc) || JsonContainerIsScalar(jc))
		PG_RETURN_BOOL(false);

	n = JsonContainerSize(jc);
	for (i = 0; i < n; i++)
	{
		JsonbValue *elem = getIthJsonbValueFromContainer(jc, i);
		JsonbValue	idbuf;
		JsonbValue *idval;
		char		buf[GRAPHID_BUFLEN];
		Graphid		elemid;

		if (elem == NULL)
			continue;

		if (elem->type == jbvString)
		{
			/* a bare graphid string, e.g. from collect(id(u)) */
			idval = elem;
		}
		else if (elem->type == jbvBinary &&
				 JsonContainerIsObject(elem->val.binary.data))
		{
			/* a serialized {"id": ...} vertex/edge object */
			idval = getKeyJsonValueFromContainer(elem->val.binary.data,
												 AG_ELEM_ID, strlen(AG_ELEM_ID),
												 &idbuf);
		}
		else
			continue;			/* number/bool/null/array: cannot match */

		if (idval == NULL || idval->type != jbvString ||
			idval->val.string.len >= GRAPHID_BUFLEN)
			continue;			/* no usable id */

		memcpy(buf, idval->val.string.val, idval->val.string.len);
		buf[idval->val.string.len] = '\0';

		if (graphid_from_cstring(buf, &elemid) && elemid == id)
			PG_RETURN_BOOL(true);
	}

	PG_RETURN_BOOL(false);
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

/* rowid APIs */

#define DatumGetItemPointer(X)		((ItemPointer) DatumGetPointer(X))
#define ItemPointerGetDatum(X)		PointerGetDatum(X)
#define PG_GETARG_ITEMPOINTER(n)	DatumGetItemPointer(PG_GETARG_DATUM(n))

Datum
rowid(PG_FUNCTION_ARGS)
{
	Oid			tableoid = PG_GETARG_OID(0);
	ItemPointer tid = PG_GETARG_ITEMPOINTER(1);
	Rowid	   *result;

	result = (Rowid *) palloc(sizeof(Rowid));
	result->tableoid = tableoid;
	memcpy(&result->tid, tid, sizeof(ItemPointerData));

	PG_RETURN_ROWID(result);
}

Datum
rowid_in(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("\"rowid\" type does not support input function")));

	PG_RETURN_NULL();
}

Datum
rowid_out(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("\"rowid\" type does not support output function")));

	PG_RETURN_NULL();
}

Datum
rowid_tableoid(PG_FUNCTION_ARGS)
{
	Rowid	   *rowid = PG_GETARG_ROWID(0);

	PG_RETURN_OID(rowid->tableoid);
}

Datum
rowid_ctid(PG_FUNCTION_ARGS)
{
	Rowid	   *rowid = PG_GETARG_ROWID(0);
	ItemPointer result;

	result = palloc(sizeof(ItemPointerData));
	ItemPointerCopy(&rowid->tid, result);

	return PointerGetDatum(result);
}

Datum
rowid_eq(PG_FUNCTION_ARGS)
{
	Rowid	   *id0 = PG_GETARG_ROWID(0);
	Rowid	   *id1 = PG_GETARG_ROWID(1);
	Datum		sub_id0;
	Datum		sub_id1;
	bool		result;

	sub_id0 = ObjectIdGetDatum(id0->tableoid);
	sub_id1 = ObjectIdGetDatum(id1->tableoid);
	result = DatumGetBool(DirectFunctionCall2(oideq, sub_id0, sub_id1));
	if (result == false)
		PG_RETURN_BOOL(false);

	sub_id0 = ItemPointerGetDatum(&id0->tid);
	sub_id1 = ItemPointerGetDatum(&id1->tid);
	result = DatumGetBool(DirectFunctionCall2(tideq, sub_id0, sub_id1));
	PG_RETURN_BOOL(result);
}

Datum
rowid_ne(PG_FUNCTION_ARGS)
{
	bool		result;

	result = DatumGetBool(DirectFunctionCall2(
											  rowid_eq, PG_GETARG_DATUM(0), PG_GETARG_DATUM(1)));

	PG_RETURN_BOOL(!result);
}

Datum
rowid_lt(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot apply \"less-then\" to \"rowid\" type")));

	PG_RETURN_NULL();
}

Datum
rowid_gt(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot apply \"greater-then\" to \"rowid\" type")));

	PG_RETURN_NULL();
}

Datum
rowid_le(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot apply \"less-or-equal\" to \"rowid\" type")));

	PG_RETURN_NULL();
}

Datum
rowid_ge(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot apply \"greater-or-equal\" to \"rowid\" type")));

	PG_RETURN_NULL();
}

Datum
btrowidcmp(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("\"rowid\" does not support comparison operation")));

	PG_RETURN_NULL();
}

Datum
vertex_out(PG_FUNCTION_ARGS)
{
	HeapTupleHeader vertex = PG_GETARG_HEAPTUPLEHEADER(0);
	Datum		values[Natts_ag_vertex];
	bool		isnull[Natts_ag_vertex];
	Graphid		id;
	Jsonb	   *prop_map;
	LabelOutData *my_extra;
	StringInfoData si;

	deform_tuple(vertex, values, isnull);

	if (isnull[Anum_ag_vertex_id - 1])
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("id in vertex cannot be NULL")));
	if (isnull[Anum_ag_vertex_properties - 1])
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("properties in vertex cannot be NULL")));

	id = DatumGetGraphid(values[Anum_ag_vertex_id - 1]);
	prop_map = DatumGetJsonbP(values[Anum_ag_vertex_properties - 1]);

	my_extra = cache_label(fcinfo->flinfo, GraphidGetLabid(id));

	initStringInfo(&si);
	appendStringInfo(&si, "%s[" GRAPHID_FMTSTR "]",
					 NameStr(my_extra->label),
					 GraphidGetLabid(id), GraphidGetLocid(id));
	JsonbToCString(&si, &prop_map->root, VARSIZE(prop_map));

	PG_RETURN_CSTRING(si.data);
}

Datum
_vertex_out(PG_FUNCTION_ARGS)
{
	AnyArrayType *vertices = PG_GETARG_ANY_ARRAY_P(0);
	StringInfoData si;

	initStringInfo(&si);
	elems_out_si(&si, vertices, fcinfo->flinfo);

	PG_RETURN_CSTRING(si.data);
}

Datum
vertex_label(PG_FUNCTION_ARGS)
{
	Graphid		id;
	LabelOutData *my_extra;
	char	   *label;
	JsonbValue	jv;

	id = DatumGetGraphid(getVertexIdDatum(PG_GETARG_DATUM(0)));

	my_extra = cache_label(fcinfo->flinfo, GraphidGetLabid(id));

	label = NameStr(my_extra->label);

	jv.type = jbvString;
	jv.val.string.len = (int) strlen(label);
	jv.val.string.val = label;

	PG_RETURN_JSONB_P(JsonbValueToJsonb(&jv));
}

Datum
_vertex_length(PG_FUNCTION_ARGS)
{
	AnyArrayType *vertices = PG_GETARG_ANY_ARRAY_P(0);
	int			nvertices;

	nvertices = ArrayGetNItems(AARR_NDIM(vertices), AARR_DIMS(vertices));

	PG_RETURN_JSONB_P(int_to_jsonb(nvertices));
}

static Jsonb *
int_to_jsonb(int i)
{
	Datum		n;
	JsonbValue	jv;

	n = DirectFunctionCall1(int4_numeric, Int32GetDatum(i));

	jv.type = jbvNumeric;
	jv.val.numeric = DatumGetNumeric(n);

	return JsonbValueToJsonb(&jv);
}

Datum
vtojb(PG_FUNCTION_ARGS)
{
	HeapTupleHeader vertex = PG_GETARG_HEAPTUPLEHEADER(0);

	PG_RETURN_DATUM(tuple_getattr(vertex, Anum_ag_vertex_properties));
}

Datum
vertex_eq(PG_FUNCTION_ARGS)
{
	Datum		id1 = getVertexIdDatum(PG_GETARG_DATUM(0));
	Datum		id2 = getVertexIdDatum(PG_GETARG_DATUM(1));

	PG_RETURN_DATUM(DirectFunctionCall2(graphid_eq, id1, id2));
}

Datum
vertex_ne(PG_FUNCTION_ARGS)
{
	Datum		id1 = getVertexIdDatum(PG_GETARG_DATUM(0));
	Datum		id2 = getVertexIdDatum(PG_GETARG_DATUM(1));

	PG_RETURN_DATUM(DirectFunctionCall2(graphid_ne, id1, id2));
}

Datum
vertex_lt(PG_FUNCTION_ARGS)
{
	Datum		id1 = getVertexIdDatum(PG_GETARG_DATUM(0));
	Datum		id2 = getVertexIdDatum(PG_GETARG_DATUM(1));

	PG_RETURN_DATUM(DirectFunctionCall2(graphid_lt, id1, id2));
}

Datum
vertex_gt(PG_FUNCTION_ARGS)
{
	Datum		id1 = getVertexIdDatum(PG_GETARG_DATUM(0));
	Datum		id2 = getVertexIdDatum(PG_GETARG_DATUM(1));

	PG_RETURN_DATUM(DirectFunctionCall2(graphid_gt, id1, id2));
}

Datum
vertex_le(PG_FUNCTION_ARGS)
{
	Datum		id1 = getVertexIdDatum(PG_GETARG_DATUM(0));
	Datum		id2 = getVertexIdDatum(PG_GETARG_DATUM(1));

	PG_RETURN_DATUM(DirectFunctionCall2(graphid_le, id1, id2));
}

Datum
vertex_ge(PG_FUNCTION_ARGS)
{
	Datum		id1 = getVertexIdDatum(PG_GETARG_DATUM(0));
	Datum		id2 = getVertexIdDatum(PG_GETARG_DATUM(1));

	PG_RETURN_DATUM(DirectFunctionCall2(graphid_ge, id1, id2));
}

Datum
vertex_cmp(PG_FUNCTION_ARGS)
{
	Datum		id1 = getVertexIdDatum(PG_GETARG_DATUM(0));
	Datum		id2 = getVertexIdDatum(PG_GETARG_DATUM(1));

	/* identity-only btree ordering: graphid is a total order */
	PG_RETURN_DATUM(DirectFunctionCall2(btgraphidcmp, id1, id2));
}

Datum
edge_out(PG_FUNCTION_ARGS)
{
	HeapTupleHeader edge = PG_GETARG_HEAPTUPLEHEADER(0);
	Datum		values[Natts_ag_edge];
	bool		isnull[Natts_ag_edge];
	Graphid		id;
	Jsonb	   *prop_map;
	LabelOutData *my_extra;
	StringInfoData si;

	deform_tuple(edge, values, isnull);

	if (isnull[Anum_ag_edge_id - 1])
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("id in edge cannot be NULL")));
	if (isnull[Anum_ag_edge_start - 1])
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("start in edge cannot be NULL")));
	if (isnull[Anum_ag_edge_end - 1])
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("end in edge cannot be NULL")));
	if (isnull[Anum_ag_edge_properties - 1])
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("properties in edge cannot be NULL")));

	id = DatumGetGraphid(values[Anum_ag_edge_id - 1]);
	prop_map = DatumGetJsonbP(values[Anum_ag_edge_properties - 1]);

	my_extra = cache_label(fcinfo->flinfo, GraphidGetLabid(id));

	initStringInfo(&si);
	appendStringInfo(&si, "%s[" GRAPHID_FMTSTR "][",
					 NameStr(my_extra->label),
					 GraphidGetLabid(id), GraphidGetLocid(id));
	graphid_out_si(&si, values[Anum_ag_edge_start - 1]);
	appendStringInfoChar(&si, ',');
	graphid_out_si(&si, values[Anum_ag_edge_end - 1]);
	appendStringInfoChar(&si, ']');
	JsonbToCString(&si, &prop_map->root, VARSIZE(prop_map));

	PG_RETURN_CSTRING(si.data);
}

Datum
_edge_out(PG_FUNCTION_ARGS)
{
	AnyArrayType *edges = PG_GETARG_ANY_ARRAY_P(0);
	StringInfoData si;

	initStringInfo(&si);
	elems_out_si(&si, edges, fcinfo->flinfo);

	PG_RETURN_CSTRING(si.data);
}

Datum
edge_label(PG_FUNCTION_ARGS)
{
	Graphid		id;
	LabelOutData *my_extra;
	char	   *label;
	JsonbValue	jv;

	id = DatumGetGraphid(getEdgeIdDatum(PG_GETARG_DATUM(0)));

	my_extra = cache_label(fcinfo->flinfo, GraphidGetLabid(id));

	label = NameStr(my_extra->label);

	jv.type = jbvString;
	jv.val.string.len = (int) strlen(label);
	jv.val.string.val = label;

	PG_RETURN_JSONB_P(JsonbValueToJsonb(&jv));
}

Datum
_edge_length(PG_FUNCTION_ARGS)
{
	AnyArrayType *edges = PG_GETARG_ANY_ARRAY_P(0);
	int			nedges;

	nedges = ArrayGetNItems(AARR_NDIM(edges), AARR_DIMS(edges));

	PG_RETURN_JSONB_P(int_to_jsonb(nedges));
}

Datum
etojb(PG_FUNCTION_ARGS)
{
	HeapTupleHeader edge = PG_GETARG_HEAPTUPLEHEADER(0);

	PG_RETURN_DATUM(tuple_getattr(edge, Anum_ag_edge_properties));
}

Datum
edge_eq(PG_FUNCTION_ARGS)
{
	Datum		id1 = getEdgeIdDatum(PG_GETARG_DATUM(0));
	Datum		id2 = getEdgeIdDatum(PG_GETARG_DATUM(1));

	PG_RETURN_DATUM(DirectFunctionCall2(graphid_eq, id1, id2));
}

Datum
edge_ne(PG_FUNCTION_ARGS)
{
	Datum		id1 = getEdgeIdDatum(PG_GETARG_DATUM(0));
	Datum		id2 = getEdgeIdDatum(PG_GETARG_DATUM(1));

	PG_RETURN_DATUM(DirectFunctionCall2(graphid_ne, id1, id2));
}

Datum
edge_lt(PG_FUNCTION_ARGS)
{
	Datum		id1 = getEdgeIdDatum(PG_GETARG_DATUM(0));
	Datum		id2 = getEdgeIdDatum(PG_GETARG_DATUM(1));

	PG_RETURN_DATUM(DirectFunctionCall2(graphid_lt, id1, id2));
}

Datum
edge_gt(PG_FUNCTION_ARGS)
{
	Datum		id1 = getEdgeIdDatum(PG_GETARG_DATUM(0));
	Datum		id2 = getEdgeIdDatum(PG_GETARG_DATUM(1));

	PG_RETURN_DATUM(DirectFunctionCall2(graphid_gt, id1, id2));
}

Datum
edge_le(PG_FUNCTION_ARGS)
{
	Datum		id1 = getEdgeIdDatum(PG_GETARG_DATUM(0));
	Datum		id2 = getEdgeIdDatum(PG_GETARG_DATUM(1));

	PG_RETURN_DATUM(DirectFunctionCall2(graphid_le, id1, id2));
}

Datum
edge_ge(PG_FUNCTION_ARGS)
{
	Datum		id1 = getEdgeIdDatum(PG_GETARG_DATUM(0));
	Datum		id2 = getEdgeIdDatum(PG_GETARG_DATUM(1));

	PG_RETURN_DATUM(DirectFunctionCall2(graphid_ge, id1, id2));
}

Datum
edge_cmp(PG_FUNCTION_ARGS)
{
	Datum		id1 = getEdgeIdDatum(PG_GETARG_DATUM(0));
	Datum		id2 = getEdgeIdDatum(PG_GETARG_DATUM(1));

	/* identity-only btree ordering: graphid is a total order */
	PG_RETURN_DATUM(DirectFunctionCall2(btgraphidcmp, id1, id2));
}

static LabelOutData *
cache_label(FmgrInfo *flinfo, uint16 labid)
{
	MemoryContext oldMemoryContext;
	LabelOutData *my_extra;

	Assert(flinfo != NULL);

	oldMemoryContext = MemoryContextSwitchTo(flinfo->fn_mcxt);

	my_extra = (LabelOutData *) flinfo->fn_extra;
	if (my_extra == NULL)
	{
		flinfo->fn_extra = palloc(sizeof(*my_extra));
		my_extra = (LabelOutData *) flinfo->fn_extra;
		my_extra->label_labid = 0;
		MemSet(NameStr(my_extra->label), '\0', sizeof(my_extra->label));
	}

	if (my_extra->label_labid != labid)
	{
		Oid			graphoid = get_graph_path_oid();
		char	   *label;

		label = get_labid_labname(graphoid, labid);
		if (label == NULL)
			elog(ERROR, "cache lookup failed for label %hu", labid);

		my_extra->label_labid = labid;
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
	if (nelems < 1)
	{
		appendBinaryStringInfo(si, "[]", 2);
		return;
	}

	appendStringInfoChar(si, '[');
	array_iter_setup(&it, elems);
	for (i = 0; i < nelems; i++)
	{
		bool		isnull;
		Datum		value;

		if (i > 0)
			appendStringInfoChar(si, delim);

		value = array_iter_next_(&it, &isnull, i, my_extra);
		appendStringInfoDatumOut(si, value, isnull, &my_extra->proc);
	}
	appendStringInfoChar(si, ']');
}

static void
appendStringInfoDatumOut(StringInfo si, Datum d, bool isnull, FmgrInfo *out)
{
	if (isnull)
		appendBinaryStringInfo(si, "NULL", 4);
	else
		appendStringInfoString(si, OutputFunctionCall(out, d));
}

Datum
graphpath_out(PG_FUNCTION_ARGS)
{
	const char	delim = ',';
	Datum		vertices_datum;
	Datum		edges_datum;
	AnyArrayType *vertices;
	AnyArrayType *edges;
	int			nvertices;
	int			nedges;
	GraphpathOutData *my_extra;
	StringInfoData si;
	array_iter	it_v;
	array_iter	it_e;
	bool		isnull;
	Datum		value;
	int			i;

	getGraphpathArrays(PG_GETARG_DATUM(0), &vertices_datum, &edges_datum);

	vertices = DatumGetAnyArrayP(vertices_datum);
	edges = DatumGetAnyArrayP(edges_datum);

	nvertices = ArrayGetNItems(AARR_NDIM(vertices), AARR_DIMS(vertices));
	nedges = ArrayGetNItems(AARR_NDIM(edges), AARR_DIMS(edges));
	if (nvertices < 1)
		PG_RETURN_CSTRING("[]");

	if (nvertices != nedges + 1)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("the numbers of vertices and edges are mismatched")));

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

	initStringInfo(&si);
	appendStringInfoChar(&si, '[');

	array_iter_setup(&it_v, vertices);
	array_iter_setup(&it_e, edges);

	for (i = 0; i < nedges; i++)
	{
		value = array_iter_next_(&it_v, &isnull, i, &my_extra->vertex);
		appendStringInfoDatumOut(&si, value, isnull, &my_extra->vertex.proc);

		appendStringInfoChar(&si, delim);

		value = array_iter_next_(&it_e, &isnull, i, &my_extra->edge);
		appendStringInfoDatumOut(&si, value, isnull, &my_extra->edge.proc);

		appendStringInfoChar(&si, delim);
	}

	value = array_iter_next_(&it_v, &isnull, i, &my_extra->vertex);
	appendStringInfoDatumOut(&si, value, isnull, &my_extra->vertex.proc);

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
array_iter_next_(array_iter *it, bool *isnull, int idx, ArrayMetaState *state)
{
	return array_iter_next(it, isnull, idx,
						   state->typlen, state->typbyval, state->typalign);
}

Datum
_graphpath_length(PG_FUNCTION_ARGS)
{
	AnyArrayType *graphpaths = PG_GETARG_ANY_ARRAY_P(0);
	int			ngraphpaths;

	ngraphpaths = ArrayGetNItems(AARR_NDIM(graphpaths), AARR_DIMS(graphpaths));

	PG_RETURN_JSONB_P(int_to_jsonb(ngraphpaths));
}

Datum
graphpath_length(PG_FUNCTION_ARGS)
{
	Datum		edges_datum;
	AnyArrayType *edges;
	int			nedges;

	edges_datum = DirectFunctionCall1(graphpath_edges, PG_GETARG_DATUM(0));
	edges = DatumGetAnyArrayP(edges_datum);
	nedges = ArrayGetNItems(AARR_NDIM(edges), AARR_DIMS(edges));

	PG_RETURN_JSONB_P(int_to_jsonb(nedges));
}

Datum
graphpath_vertices(PG_FUNCTION_ARGS)
{
	Datum		vertices_datum;

	getGraphpathArrays(PG_GETARG_DATUM(0), &vertices_datum, NULL);

	PG_RETURN_DATUM(vertices_datum);
}

Datum
graphpath_edges(PG_FUNCTION_ARGS)
{
	Datum		edges_datum;

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

/*
 * graphElementIdIsNull
 *		A node or relationship bound by an OPTIONAL MATCH that did not match is
 *		a row whose id attribute is NULL (the row itself is not NULL).  Report
 *		whether that is the case, reading the id without asserting it is
 *		non-NULL.  The argument must be a non-NULL VERTEX or EDGE datum.
 */
bool
graphElementIdIsNull(Datum elem, Oid elemtype)
{
	HeapTupleHeader tuphdr = DatumGetHeapTupleHeader(elem);
	HeapTupleData tuple;
	TupleDesc	tupDesc;
	AttrNumber	idattno;
	bool		isnull = false;

	idattno = (elemtype == EDGEOID) ? Anum_ag_edge_id : Anum_ag_vertex_id;

	tupDesc = lookup_rowtype_tupdesc(HeapTupleHeaderGetTypeId(tuphdr), -1);

	tuple.t_len = HeapTupleHeaderGetDatumLength(tuphdr);
	ItemPointerSetInvalid(&tuple.t_self);
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = tuphdr;

	(void) heap_getattr(&tuple, idattno, tupDesc, &isnull);
	ReleaseTupleDesc(tupDesc);

	return isnull;
}

Datum
edge_start_vertex(PG_FUNCTION_ARGS)
{
	HeapTupleHeader edge = PG_GETARG_HEAPTUPLEHEADER(0);
	Graphid		vertex_id = DatumGetGraphid(tuple_getattr(edge,
														  Anum_ag_edge_start));
	Datum		ret = get_vertex_from_graphid(vertex_id);

	if (ret == ((Datum) 0))
	{
		fcinfo->isnull = true;
	}

	return ret;
}

Datum
edge_end_vertex(PG_FUNCTION_ARGS)
{
	HeapTupleHeader edge = PG_GETARG_HEAPTUPLEHEADER(0);
	Graphid		vertex_id = DatumGetGraphid(tuple_getattr(edge,
														  Anum_ag_edge_end));
	Datum		ret = get_vertex_from_graphid(vertex_id);

	if (ret == ((Datum) 0))
	{
		fcinfo->isnull = true;
	}

	return ret;
}

Datum
get_vertex_from_graphid(Graphid vertex_id)
{
	Labid		vertex_label_id;
	Oid			vertex_label_relid;
	Oid			graph_path_oid = get_graph_path_oid();
	Relation	vertex_rel;
	Relation	id_index_rel = NULL;
	List	   *index_oids;
	ListCell   *lc;
	ScanKeyData scan_key_data;
	TupleTableSlot *slot;
	Datum		ret = (Datum) 0;

	vertex_label_id = GraphidGetLabid(vertex_id);

	vertex_label_relid = get_labid_relid(graph_path_oid, vertex_label_id);
	vertex_rel = table_open(vertex_label_relid, AccessShareLock);
	slot = table_slot_create(vertex_rel, NULL);

	/*
	 * Probe the vertex id primary-key btree rather than scanning the whole
	 * label.  Every vertex label carries a unique btree on id (the PK), so
	 * this turns an O(N) heap scan into an O(log N) index probe -- and this
	 * lookup runs once per vertex materialized along a path, so a full scan
	 * per vertex is quadratic on large labels.  Locate that index by its
	 * leading key column (Anum_table_vertex_id); if it is missing, disabled,
	 * not yet ready, or partial (e.g. under DISABLE INDEX or an in-progress
	 * CREATE INDEX), fall back to the always-correct heap scan.
	 */
	index_oids = RelationGetIndexList(vertex_rel);
	foreach(lc, index_oids)
	{
		Relation	idx = index_open(lfirst_oid(lc), AccessShareLock);

		if (idx->rd_rel->relam == BTREE_AM_OID &&
			idx->rd_index->indisvalid && idx->rd_index->indisready &&
			RelationGetIndexPredicate(idx) == NIL &&
			idx->rd_index->indkey.values[0] == Anum_table_vertex_id)
		{
			id_index_rel = idx;
			break;
		}

		index_close(idx, AccessShareLock);
	}
	list_free(index_oids);

	if (id_index_rel != NULL)
	{
		IndexScanDesc index_scan_desc;

		/* scan key targets the index's leading (id) column */
		ScanKeyInit(&scan_key_data,
					1,
					BTEqualStrategyNumber, F_GRAPHID_EQ,
					GraphidGetDatum(vertex_id));

		index_scan_desc = index_beginscan(vertex_rel, id_index_rel,
										  GetActiveSnapshot(), NULL, 1, 0);
		index_rescan(index_scan_desc, &scan_key_data, 1, NULL, 0);

		if (index_getnext_slot(index_scan_desc, ForwardScanDirection, slot))
		{
			slot_getallattrs(slot);
			ret = make_vertex_from_tuple(slot);
		}

		index_endscan(index_scan_desc);
		index_close(id_index_rel, AccessShareLock);
	}
	else
	{
		TableScanDesc scan_desc;

		ScanKeyInit(&scan_key_data,
					Anum_table_vertex_id,
					BTEqualStrategyNumber, F_GRAPHID_EQ,
					GraphidGetDatum(vertex_id));

		scan_desc = table_beginscan(vertex_rel,
									GetActiveSnapshot(),
									1, &scan_key_data);

		if (table_scan_getnextslot(scan_desc, ForwardScanDirection, slot))
		{
			slot_getallattrs(slot);
			ret = make_vertex_from_tuple(slot);
		}

		table_endscan(scan_desc);
	}

	table_close(vertex_rel, NoLock);
	ExecDropSingleTupleTableSlot(slot);

	return ret;
}

Datum
vertex_labels(PG_FUNCTION_ARGS)
{
	Graphid		id;
	LabelsOutData *my_extra;

	id = DatumGetGraphid(getVertexIdDatum(PG_GETARG_DATUM(0)));

	my_extra = cache_labels(fcinfo->flinfo, GraphidGetLabid(id));

	PG_RETURN_JSONB_P(my_extra->labels);
}

Datum
edge_start_vid(PG_FUNCTION_ARGS)
{

	HeapTupleHeader edge = PG_GETARG_HEAPTUPLEHEADER(0);
	Graphid		vertex_id = DatumGetGraphid(tuple_getattr(edge,
														  Anum_ag_edge_start));

	PG_RETURN_GRAPHID(vertex_id);
}

Datum
edge_end_vid(PG_FUNCTION_ARGS)
{
	HeapTupleHeader edge = PG_GETARG_HEAPTUPLEHEADER(0);
	Graphid		vertex_id = DatumGetGraphid(tuple_getattr(edge,
														  Anum_ag_edge_end));

	PG_RETURN_GRAPHID(vertex_id);
}

static LabelsOutData *
cache_labels(FmgrInfo *flinfo, uint16 labid)
{
	MemoryContext oldMemoryContext;
	LabelsOutData *my_extra;

	Assert(flinfo != NULL);

	oldMemoryContext = MemoryContextSwitchTo(flinfo->fn_mcxt);

	my_extra = (LabelsOutData *) flinfo->fn_extra;
	if (my_extra == NULL)
	{
		flinfo->fn_extra = palloc(sizeof(*my_extra));
		my_extra = (LabelsOutData *) flinfo->fn_extra;
		my_extra->mctx = AllocSetContextCreate(CurrentMemoryContext,
											   "vertex_labels() cache",
											   ALLOCSET_DEFAULT_SIZES);
		my_extra->label_labid = 0;
		my_extra->labels = NULL;
	}

	if (my_extra->label_labid != labid)
	{
		MemoryContext funcMemoryContext;
		Oid			graphoid;
		Oid			label_relid;
		List	   *ancestor_relids;
		JsonbParseState *jpstate = NULL;
		ListCell   *li;
		JsonbValue *labels;

		if (my_extra->labels != NULL)
			pfree(my_extra->labels);
		MemoryContextReset(my_extra->mctx);

		funcMemoryContext = MemoryContextSwitchTo(my_extra->mctx);

		graphoid = get_graph_path_oid();

		/* get relation IDs of ancestor labels */
		label_relid = get_labid_relid(graphoid, labid);
		ancestor_relids = find_all_ancestors(label_relid, AccessShareLock);

		pushJsonbValue(&jpstate, WJB_BEGIN_ARRAY, NULL);

		foreach(li, ancestor_relids)
		{
			Oid			ancestor_relid = lfirst_oid(li);
			HeapTuple	tp;
			char	   *ancestor_labname;

			tp = SearchSysCache1(LABELRELID, ObjectIdGetDatum(ancestor_relid));
			if (HeapTupleIsValid(tp))
			{
				Form_ag_label labtup = (Form_ag_label) GETSTRUCT(tp);

				ancestor_labname = pstrdup(NameStr(labtup->labname));

				ReleaseSysCache(tp);
			}
			else
			{
				elog(ERROR, "cache lookup failed for label %u", ancestor_relid);
			}

			if (strcmp(ancestor_labname, "ag_vertex") != 0)
			{
				JsonbValue	jv;

				jv.type = jbvString;
				jv.val.string.len = (int) strlen(ancestor_labname);
				jv.val.string.val = ancestor_labname;

				pushJsonbValue(&jpstate, WJB_ELEM, &jv);
			}
		}

		labels = pushJsonbValue(&jpstate, WJB_END_ARRAY, NULL);

		MemoryContextSwitchTo(funcMemoryContext);

		my_extra->labels = JsonbValueToJsonb(labels);
	}

	MemoryContextSwitchTo(oldMemoryContext);

	return my_extra;
}

Datum
getVertexIdDatum(Datum datum)
{
	HeapTupleHeader tuphdr = DatumGetHeapTupleHeader(datum);

	return tuple_getattr(tuphdr, Anum_ag_vertex_id);
}

Datum
getVertexPropDatum(Datum datum)
{
	HeapTupleHeader tuphdr = DatumGetHeapTupleHeader(datum);

	return tuple_getattr(tuphdr, Anum_ag_vertex_properties);
}

Datum
getVertexTidDatum(Datum datum)
{
	HeapTupleHeader tuphdr = DatumGetHeapTupleHeader(datum);

	return tuple_getattr(tuphdr, Anum_ag_vertex_tid);
}

Datum
getEdgeIdDatum(Datum datum)
{
	HeapTupleHeader tuphdr = DatumGetHeapTupleHeader(datum);

	return tuple_getattr(tuphdr, Anum_ag_edge_id);
}

Datum
getEdgeStartDatum(Datum datum)
{
	HeapTupleHeader tuphdr = DatumGetHeapTupleHeader(datum);

	return tuple_getattr(tuphdr, Anum_ag_edge_start);
}

Datum
getEdgeEndDatum(Datum datum)
{
	HeapTupleHeader tuphdr = DatumGetHeapTupleHeader(datum);

	return tuple_getattr(tuphdr, Anum_ag_edge_end);
}

Datum
getEdgePropDatum(Datum datum)
{
	HeapTupleHeader tuphdr = DatumGetHeapTupleHeader(datum);

	return tuple_getattr(tuphdr, Anum_ag_edge_properties);
}

Datum
getEdgeTidDatum(Datum datum)
{
	HeapTupleHeader tuphdr = DatumGetHeapTupleHeader(datum);

	return tuple_getattr(tuphdr, Anum_ag_edge_tid);
}

void
getGraphpathArrays(Datum graphpath, Datum *vertices, Datum *edges)
{
	HeapTupleHeader tuphdr;
	Oid			tupType;
	TupleDesc	tupDesc;
	HeapTupleData tuple;
	Datum		values[Natts_ag_graphpath];
	bool		isnull[Natts_ag_graphpath];

	tuphdr = DatumGetHeapTupleHeader(graphpath);

	tupType = HeapTupleHeaderGetTypeId(tuphdr);
	Assert(tupType == GRAPHPATHOID);

	tupDesc = lookup_rowtype_tupdesc(tupType, -1);
	Assert(tupDesc->natts == Natts_ag_graphpath);

	tuple.t_len = HeapTupleHeaderGetDatumLength(tuphdr);
	ItemPointerSetInvalid(&tuple.t_self);
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = tuphdr;

	heap_deform_tuple(&tuple, tupDesc, values, isnull);
	ReleaseTupleDesc(tupDesc);
	Assert(!isnull[Anum_ag_graphpath_vertices - 1]);
	Assert(!isnull[Anum_ag_graphpath_edges - 1]);

	if (vertices != NULL)
		*vertices = values[Anum_ag_graphpath_vertices - 1];
	if (edges != NULL)
		*edges = values[Anum_ag_graphpath_edges - 1];
}

Datum
makeGraphpathDatum(Datum *vertices, int nvertices, Datum *edges, int nedges)
{
	Datum		values[Natts_ag_graphpath];
	bool		isnull[Natts_ag_graphpath] = {false, false};
	TupleDesc	tupDesc;
	HeapTuple	graphpath;

	values[Anum_ag_graphpath_vertices - 1]
		= makeArrayTypeDatum(vertices, nvertices, VERTEXOID);
	values[Anum_ag_graphpath_edges - 1]
		= makeArrayTypeDatum(edges, nedges, EDGEOID);

	tupDesc = lookup_rowtype_tupdesc(GRAPHPATHOID, -1);
	Assert(tupDesc->natts == Natts_ag_graphpath);

	graphpath = heap_form_tuple(tupDesc, values, isnull);

	ReleaseTupleDesc(tupDesc);

	return HeapTupleGetDatum(graphpath);
}

Datum
makeGraphVertexDatum(Datum id, Datum prop_map, Datum tid)
{
	Datum		values[Natts_ag_vertex];
	bool		isnull[Natts_ag_vertex] = {false, false, false};
	TupleDesc	tupDesc;
	HeapTuple	vertex;

	values[Anum_ag_vertex_id - 1] = id;
	values[Anum_ag_vertex_properties - 1] = prop_map;
	values[Anum_ag_vertex_tid - 1] = tid;

	tupDesc = lookup_rowtype_tupdesc(VERTEXOID, -1);
	Assert(tupDesc->natts == Natts_ag_vertex);

	vertex = heap_form_tuple(tupDesc, values, isnull);

	ReleaseTupleDesc(tupDesc);

	return HeapTupleGetDatum(vertex);
}

Datum
makeGraphEdgeDatum(Datum id, Datum start, Datum end, Datum prop_map, Datum tid)
{
	Datum		values[Natts_ag_edge];
	bool		isnull[Natts_ag_edge] = {false, false, false, false, false};
	TupleDesc	tupDesc;
	HeapTuple	edge;

	values[Anum_ag_edge_id - 1] = id;
	values[Anum_ag_edge_start - 1] = start;
	values[Anum_ag_edge_end - 1] = end;
	values[Anum_ag_edge_properties - 1] = prop_map;
	values[Anum_ag_edge_tid - 1] = tid;

	tupDesc = lookup_rowtype_tupdesc(EDGEOID, -1);
	Assert(tupDesc->natts == Natts_ag_edge);

	edge = heap_form_tuple(tupDesc, values, isnull);

	ReleaseTupleDesc(tupDesc);

	return HeapTupleGetDatum(edge);
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
	Graphid		id = PG_GETARG_GRAPHID(0);

	StaticAssertStmt(sizeof(id) == 8, "the size of graphid must be 8");

	return hash_any((unsigned char *) &id, sizeof(id));
}

/* HASHPROC (1) */
Datum
vertex_hash(PG_FUNCTION_ARGS)
{
	Datum		id = getVertexIdDatum(PG_GETARG_DATUM(0));

	PG_RETURN_DATUM(DirectFunctionCall1(graphid_hash, id));
}

/*
 * GIN (as BTree) support functions
 */

/* Note: GIN_COMPARE_PROC (1) is btgraphidcmp() */

/* GIN_EXTRACTVALUE_PROC (2) - called by ginExtractEntries() */
Datum
gin_extract_value_graphid(PG_FUNCTION_ARGS)
{
	const int32 _nentries = 1;
	Datum		graphid = PG_GETARG_DATUM(0);
	int32	   *nentries = (int32 *) PG_GETARG_POINTER(1);
	Datum	   *entries = palloc(sizeof(*entries) * _nentries);

	*nentries = _nentries;
	entries[0] = graphid;

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
	const int32 _nentries = 1;
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
	Graphid		id;

	GraphidSet(&id, 0, 0);

	return GraphidGetDatum(id);
}

/* GIN_CONSISTENT_PROC (4) - same as trueConsistentFn() */
Datum
gin_consistent_graphid(PG_FUNCTION_ARGS)
{
	bool	   *recheck = (bool *) PG_GETARG_POINTER(5);

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
	}

	PG_RETURN_INT32(res);
}

/*
 * countPathArrayElems
 *
 * How many elements a path's vertex or edge array will hold, when that is
 * settled by the pattern rather than by the rows.  A pattern of a fixed shape
 * builds its array by concatenating one element at a time, so the count is
 * there to be read off the expression; a variable-length relationship
 * contributes an array nobody can count until it runs, and reports -1.
 */
static int
countPathArrayElems(Node *node)
{
	if (node == NULL)
		return -1;

	if (IsA(node, ArrayExpr))
	{
		ArrayExpr  *a = (ArrayExpr *) node;

		/* an array of arrays is a concatenation, not a list of elements */
		if (a->multidims)
			return -1;
		return list_length(a->elements);
	}

	if (IsA(node, Const))
	{
		Const	   *c = (Const *) node;
		ArrayType  *arr;

		if (c->constisnull)
			return -1;
		arr = DatumGetArrayTypeP(c->constvalue);
		return ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
	}

	if (IsA(node, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) node;
		Node	   *lexpr;
		Node	   *rexpr;
		int			lcount;

		if (list_length(op->args) != 2)
			return -1;

		lexpr = linitial(op->args);
		rexpr = lsecond(op->args);

		lcount = countPathArrayElems(lexpr);
		if (lcount < 0)
			return -1;

		/*
		 * Appending one element adds one; concatenating another array adds
		 * however many that one holds, which may itself be unknown.
		 */
		if (exprType(rexpr) == exprType(lexpr))
		{
			int			rcount = countPathArrayElems(rexpr);

			if (rcount < 0)
				return -1;
			return lcount + rcount;
		}

		return lcount + 1;
	}

	return -1;
}

/*
 * graphpath_support
 *
 * A path built by a pattern of a fixed shape is assembled per row -- every
 * vertex and every relationship on it, each carrying its whole property map --
 * and a query that only asks how long it is, or for one of its two arrays,
 * throws all of that away again.
 *
 * Answer from the pattern instead, where the pattern settles the answer: the
 * length of a fixed-shape path is a constant, and each array is one of the two
 * expressions the path was going to be built from.  A variable-length
 * relationship settles nothing until it runs, and is left alone.
 */
Datum
graphpath_support(PG_FUNCTION_ARGS)
{
	Node	   *rawreq = (Node *) PG_GETARG_POINTER(0);

	if (IsA(rawreq, SupportRequestSimplify))
	{
		SupportRequestSimplify *req = (SupportRequestSimplify *) rawreq;
		FuncExpr   *fexpr = req->fcall;
		Node	   *arg;
		RowExpr    *path;
		Node	   *vertices;
		Node	   *edges;

		if (list_length(fexpr->args) != 1)
			PG_RETURN_POINTER(NULL);

		arg = linitial(fexpr->args);

		/*
		 * How many entries one of a path's arrays holds is settled by the same
		 * pattern that settles the path's length, so counting it need not build
		 * it.  This is what size(vertices(p)) asks, and by the time it is asked
		 * the accessor has already given way to the expression the array would
		 * have been built from.
		 *
		 * Worth more than the count: an array of nodes is built out of whole
		 * nodes, so building one in order to count it reads every property map
		 * on the path off the heap.  Answering from the pattern reads none.
		 */
		if (fexpr->funcid == F_LENGTH__VERTEX ||
			fexpr->funcid == F_LENGTH__EDGE)
		{
			int			nelems = countPathArrayElems(arg);

			if (nelems < 0 || contain_volatile_functions(arg))
				PG_RETURN_POINTER(NULL);

			PG_RETURN_POINTER(makeConst(JSONBOID, -1, InvalidOid, -1,
										JsonbPGetDatum(int_to_jsonb(nelems)),
										false, false));
		}

		if (!IsA(arg, RowExpr))
			PG_RETURN_POINTER(NULL);

		path = (RowExpr *) arg;
		if (path->row_typeid != GRAPHPATHOID || list_length(path->args) != 2)
			PG_RETURN_POINTER(NULL);

		vertices = linitial(path->args);
		edges = lsecond(path->args);

		/*
		 * Whatever is dropped must be droppable: an expression that does
		 * something as well as producing a value has to keep being evaluated.
		 */
		switch (fexpr->funcid)
		{
			case F_VERTICES:
				if (contain_volatile_functions(edges))
					break;
				PG_RETURN_POINTER(vertices);

			case F_EDGES:
				if (contain_volatile_functions(vertices))
					break;
				PG_RETURN_POINTER(edges);

			case F_LENGTH_GRAPHPATH:
				{
					int			nedges = countPathArrayElems(edges);

					if (nedges < 0)
						break;
					if (contain_volatile_functions(vertices) ||
						contain_volatile_functions(edges))
						break;

					PG_RETURN_POINTER(makeConst(JSONBOID, -1, InvalidOid, -1,
												JsonbPGetDatum(int_to_jsonb(nedges)),
												false, false));
				}

			default:
				break;
		}
	}

	PG_RETURN_POINTER(NULL);
}
