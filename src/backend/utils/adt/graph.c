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
	Vertex	   *v = PG_GETARG_VERTEX(0);
	StringInfoData	si;

	initStringInfo(&si);
	appendStringInfo(&si, "Node[%s:" INT64_FORMAT "] ",
					 NameStr(v->label), v->vid);
	JsonbToCString(&si, &v->prop_map.root, VARSIZE(&v->prop_map));

	PG_RETURN_CSTRING(si.data);
}

Datum
vertex_constructor(PG_FUNCTION_ARGS)
{
	Name		label = PG_GETARG_NAME(0);
	int64		vid = PG_GETARG_INT64(1);
	Jsonb	   *prop_map = PG_GETARG_JSONB(2);
	Size		len;
	Vertex	   *v;

	if (PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("vertex ID must not be null")));
	if (PG_ARGISNULL(2))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("property map must not be null")));

	/* property map should be a JSON object */
	if (!JB_ROOT_IS_OBJECT(prop_map))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("property map must be a binary JSON object")));

	len = offsetof(Vertex, prop_map) + VARSIZE(prop_map);

	v = (Vertex *) palloc(len);
	MemSetLoop(v, 0, offsetof(Vertex, prop_map));
	SET_VARSIZE(v, len);

	if (!PG_ARGISNULL(0))
		namecpy(&v->label, label);
	v->vid = vid;
	memcpy(&v->prop_map, prop_map, VARSIZE(prop_map));

	PG_RETURN_POINTER(v);
}

Datum
vertex_prop(PG_FUNCTION_ARGS)
{
	Vertex	   *v = PG_GETARG_VERTEX(0);
	text	   *label = PG_GETARG_TEXT_P(1);
	FunctionCallInfoData locfcinfo;
	Datum		result;

	InitFunctionCallInfoData(locfcinfo, NULL, 2,
							 PG_GET_COLLATION(), NULL, NULL);

	locfcinfo.arg[0] = PointerGetDatum(&v->prop_map);
	locfcinfo.arg[1] = PointerGetDatum(label);
	locfcinfo.argnull[0] = false;
	locfcinfo.argnull[1] = PG_ARGISNULL(1);

	result = jsonb_object_field(&locfcinfo);

	if (locfcinfo.isnull)
		PG_RETURN_NULL();

	return result;
}
