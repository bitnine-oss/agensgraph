/*
 * cypher_funcs.c
 *	  Functions in Cypher expressions.
 *
 * Copyright (c) 2022 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/cypher_empty_funcs.c
 */
#include "postgres.h"

#include "utils/cypher_empty_funcs.h"
#include "utils/fmgrprotos.h"
#include "utils/jsonb.h"

Datum
cypher_to_jsonb(PG_FUNCTION_ARGS)
{
	return to_jsonb(fcinfo);
}

Datum cypher_isempty_jsonb(PG_FUNCTION_ARGS)
{
	Jsonb *jb = PG_GETARG_JSONB_P(0);

	if (JB_ROOT_IS_SCALAR(jb))
	{
		JsonbValue *sjv;

		sjv = getIthJsonbValueFromContainer(&jb->root, 0);
		if (sjv->type == jbvString)
		{
			PG_RETURN_BOOL(sjv->val.string.len <= 0);
		}
	}
	else if (JB_ROOT_IS_ARRAY(jb) || JB_ROOT_IS_OBJECT(jb))
	{
		PG_RETURN_BOOL(JB_ROOT_COUNT(jb) <= 0);
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("isEmpty(): list or object or string is expected but %s",
						   JsonbToCString(NULL, &jb->root, VARSIZE(jb)))));
}
