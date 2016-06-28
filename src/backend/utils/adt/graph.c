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

#include "utils/builtins.h"
#include "utils/graph.h"
#include "utils/json.h"
#include "utils/lsyscache.h"

Datum
vertex_in(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type vertex")));

	PG_RETURN_VOID();
}

Datum
vertex_out(PG_FUNCTION_ARGS)
{
	Vertex *v = PG_GETARG_VERTEX(0);
	StringInfoData si;

	initStringInfo(&si);
	appendStringInfo(&si, "Node[%u:" INT64_FORMAT "]", v->id.oid, v->id.vid);
	JsonbToCString(&si, &v->prop_map.root, VARSIZE(&v->prop_map));

	PG_RETURN_CSTRING(si.data);
}

static Jsonb *
build_jsonb_empty_object(void)
{
	FunctionCallInfoData locfcinfo;
	Datum result;

	InitFunctionCallInfoData(locfcinfo, NULL, 0, InvalidOid, NULL, NULL);
	result = jsonb_build_object_noargs(&locfcinfo);
	/* the result never be NULL, skip NULL check */
	Assert(!locfcinfo.isnull);

	return (Jsonb *) result;
}

Datum
vertex_constructor(PG_FUNCTION_ARGS)
{
	Oid			oid = PG_GETARG_OID(0);
	int64		vid = PG_GETARG_INT64(1);
	Jsonb	   *prop_map;
	Size		len;
	Vertex	   *v;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("vertex label OID must not be null")));
	if (PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("vertex ID must not be null")));

	if (PG_ARGISNULL(2))
		prop_map = build_jsonb_empty_object();
	else
		prop_map = PG_GETARG_JSONB(2);

	/* property map should be a JSON object */
	if (!JB_ROOT_IS_OBJECT(prop_map))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("property map must be a binary JSON object")));

	len = offsetof(Vertex, prop_map) + VARSIZE(prop_map);

	v = (Vertex *) palloc(len);
	MemSetLoop(v, 0, offsetof(Vertex, prop_map));
	SET_VARSIZE(v, len);

	v->id.oid = oid;
	v->id.vid = vid;
	memcpy(&v->prop_map, prop_map, VARSIZE(prop_map));

	PG_RETURN_POINTER(v);
}

Datum
vertex_prop(PG_FUNCTION_ARGS)
{
	Vertex	   *v = PG_GETARG_VERTEX(0);
	text	   *key = PG_GETARG_TEXT_P(1);
	FunctionCallInfoData locfcinfo;
	Datum		result;

	InitFunctionCallInfoData(locfcinfo, NULL, 2,
							 PG_GET_COLLATION(), NULL, NULL);

	locfcinfo.arg[0] = PointerGetDatum(&v->prop_map);
	locfcinfo.arg[1] = PointerGetDatum(key);
	locfcinfo.argnull[0] = false;
	locfcinfo.argnull[1] = PG_ARGISNULL(1);

	result = jsonb_object_field(&locfcinfo);

	if (locfcinfo.isnull)
		PG_RETURN_NULL();

	return result;
}

Datum
edge_in(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type edge")));

	PG_RETURN_VOID();
}

Datum
edge_out(PG_FUNCTION_ARGS)
{
	GraphEdge  *e = PG_GETARG_EDGE(0);
	char	   *label;
	StringInfoData si;

	label = get_rel_name(e->oid);
	if (label == NULL)
		label = "?";

	initStringInfo(&si);
	appendStringInfo(&si, ":%s[%u:" INT64_FORMAT "]", label, e->oid, e->eid);
	JsonbToCString(&si, &e->prop_map.root, VARSIZE(&e->prop_map));

	PG_RETURN_CSTRING(si.data);
}

Datum
edge_constructor(PG_FUNCTION_ARGS)
{
	Oid			oid = PG_GETARG_OID(0);
	int64		eid = PG_GETARG_INT64(1);
	Oid			vin_oid = PG_GETARG_OID(2);
	int64		vin_vid = PG_GETARG_INT64(3);
	Oid			vout_oid = PG_GETARG_OID(4);
	int64		vout_vid = PG_GETARG_INT64(5);
	Jsonb	   *prop_map;
	Size		len;
	GraphEdge  *e;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("edge type OID must not be null")));
	if (PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("edge ID must not be null")));
	if (PG_ARGISNULL(2))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("vertex(in) label OID must not be null")));
	if (PG_ARGISNULL(3))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("vertex(in) ID must not be null")));
	if (PG_ARGISNULL(4))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("vertex(out) label OID must not be null")));
	if (PG_ARGISNULL(5))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("vertex(out) ID must not be null")));

	if (PG_ARGISNULL(6))
		prop_map = build_jsonb_empty_object();
	else
		prop_map = PG_GETARG_JSONB(6);

	/* property map should be a JSON object */
	if (!JB_ROOT_IS_OBJECT(prop_map))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("property map must be a binary JSON object")));

	len = offsetof(GraphEdge, prop_map) + VARSIZE(prop_map);

	e = (GraphEdge *) palloc(len);
	MemSetLoop(e, 0, offsetof(GraphEdge, prop_map));
	SET_VARSIZE(e, len);

	e->oid = oid;
	e->eid = eid;
	e->vin.oid = vin_oid;
	e->vin.vid = vin_vid;
	e->vout.oid = vout_oid;
	e->vout.vid = vout_vid;
	memcpy(&e->prop_map, prop_map, VARSIZE(prop_map));

	PG_RETURN_POINTER(e);
}

Datum
edge_prop(PG_FUNCTION_ARGS)
{
	GraphEdge  *e = PG_GETARG_EDGE(0);
	text	   *key = PG_GETARG_TEXT_P(1);
	FunctionCallInfoData locfcinfo;
	Datum		result;

	InitFunctionCallInfoData(locfcinfo, NULL, 2,
							 PG_GET_COLLATION(), NULL, NULL);

	locfcinfo.arg[0] = PointerGetDatum(&e->prop_map);
	locfcinfo.arg[1] = PointerGetDatum(key);
	locfcinfo.argnull[0] = false;
	locfcinfo.argnull[1] = PG_ARGISNULL(1);

	result = jsonb_object_field(&locfcinfo);

	if (locfcinfo.isnull)
		PG_RETURN_NULL();

	return result;
}

Datum
vertex_oid(PG_FUNCTION_ARGS)
{
	Vertex *v = PG_GETARG_VERTEX(0);

	PG_RETURN_OID(v->id.oid);
}

Datum
vertex_vid(PG_FUNCTION_ARGS)
{
	Vertex *v = PG_GETARG_VERTEX(0);

	PG_RETURN_OID(v->id.vid);
}
