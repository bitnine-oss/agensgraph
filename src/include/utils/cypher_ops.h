/*
 * cypher_ops.h
 *	  Functions for operators in Cypher expressions.
 *
 * Copyright (c) 2017 by Bitnine Global, Inc.
 *
 * src/include/utils/cypher_ops.h
 */

#ifndef CYPHER_OPS_H
#define CYPHER_OPS_H

#include "fmgr.h"

/* operators for jsonb */
extern Datum jsonb_add(PG_FUNCTION_ARGS);
extern Datum jsonb_sub(PG_FUNCTION_ARGS);
extern Datum jsonb_mul(PG_FUNCTION_ARGS);
extern Datum jsonb_div(PG_FUNCTION_ARGS);
extern Datum jsonb_mod(PG_FUNCTION_ARGS);
extern Datum jsonb_pow(PG_FUNCTION_ARGS);
extern Datum jsonb_uplus(PG_FUNCTION_ARGS);
extern Datum jsonb_uminus(PG_FUNCTION_ARGS);

/* coercions between jsonb and bool */
extern Datum bool_jsonb(PG_FUNCTION_ARGS);
extern Datum jsonb_bool(PG_FUNCTION_ARGS);

/* coercion from jsonb to int8/int4/numeric/float8 */
extern Datum jsonb_int8(PG_FUNCTION_ARGS);
extern Datum jsonb_int4(PG_FUNCTION_ARGS);
extern Datum jsonb_numeric(PG_FUNCTION_ARGS);
extern Datum jsonb_float8(PG_FUNCTION_ARGS);

/* coercion from numeric to graphid */
extern Datum numeric_graphid(PG_FUNCTION_ARGS);

#endif	/* CYPHER_OPS_H */
