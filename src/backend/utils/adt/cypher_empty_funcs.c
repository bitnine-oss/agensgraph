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

Datum
cypher_to_jsonb(PG_FUNCTION_ARGS)
{
	return to_jsonb(fcinfo);
}