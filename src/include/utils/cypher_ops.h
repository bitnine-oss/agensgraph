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

#endif	/* CYPHER_OPS_H */
