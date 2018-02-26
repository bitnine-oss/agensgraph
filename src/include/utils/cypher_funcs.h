/*
 * cypher_funcs.h
 *	  Functions in Cypher expressions.
 *
 * Copyright (c) 2017 by Bitnine Global, Inc.
 *
 * src/include/utils/cypher_funcs.h
 */

#ifndef CYPHER_FUNCS_H
#define CYPHER_FUNCS_H

#include "fmgr.h"

/* scalar */
extern Datum jsonb_head(PG_FUNCTION_ARGS);
extern Datum jsonb_last(PG_FUNCTION_ARGS);
extern Datum jsonb_length(PG_FUNCTION_ARGS);
extern Datum jsonb_toboolean(PG_FUNCTION_ARGS);

/* list */
extern Datum jsonb_keys(PG_FUNCTION_ARGS);
extern Datum jsonb_tail(PG_FUNCTION_ARGS);

/* mathematical - numeric */
extern Datum jsonb_abs(PG_FUNCTION_ARGS);
extern Datum jsonb_ceil(PG_FUNCTION_ARGS);
extern Datum jsonb_floor(PG_FUNCTION_ARGS);
extern Datum jsonb_rand(PG_FUNCTION_ARGS);
extern Datum jsonb_round(PG_FUNCTION_ARGS);
extern Datum jsonb_sign(PG_FUNCTION_ARGS);

/* mathematical - logarithmic */
extern Datum jsonb_exp(PG_FUNCTION_ARGS);
extern Datum jsonb_log(PG_FUNCTION_ARGS);
extern Datum jsonb_log10(PG_FUNCTION_ARGS);
extern Datum jsonb_sqrt(PG_FUNCTION_ARGS);

/* mathematical - trigonometric */
extern Datum jsonb_acos(PG_FUNCTION_ARGS);
extern Datum jsonb_asin(PG_FUNCTION_ARGS);
extern Datum jsonb_atan(PG_FUNCTION_ARGS);
extern Datum jsonb_atan2(PG_FUNCTION_ARGS);
extern Datum jsonb_cos(PG_FUNCTION_ARGS);
extern Datum jsonb_cot(PG_FUNCTION_ARGS);
extern Datum jsonb_degrees(PG_FUNCTION_ARGS);
extern Datum jsonb_radians(PG_FUNCTION_ARGS);
extern Datum jsonb_sin(PG_FUNCTION_ARGS);
extern Datum jsonb_tan(PG_FUNCTION_ARGS);

/* string */
extern Datum jsonb_left(PG_FUNCTION_ARGS);
extern Datum jsonb_ltrim(PG_FUNCTION_ARGS);
extern Datum jsonb_replace(PG_FUNCTION_ARGS);
extern Datum jsonb_reverse(PG_FUNCTION_ARGS);
extern Datum jsonb_right(PG_FUNCTION_ARGS);
extern Datum jsonb_rtrim(PG_FUNCTION_ARGS);
extern Datum jsonb_substr_no_len(PG_FUNCTION_ARGS);
extern Datum jsonb_substr(PG_FUNCTION_ARGS);
extern Datum jsonb_tolower(PG_FUNCTION_ARGS);
extern Datum jsonb_tostring(PG_FUNCTION_ARGS);
extern Datum jsonb_toupper(PG_FUNCTION_ARGS);
extern Datum jsonb_trim(PG_FUNCTION_ARGS);
extern Datum jsonb_string_starts_with(PG_FUNCTION_ARGS);
extern Datum jsonb_string_ends_with(PG_FUNCTION_ARGS);
extern Datum jsonb_string_contains(PG_FUNCTION_ARGS);
extern Datum jsonb_string_regex(PG_FUNCTION_ARGS);

#endif	/* CYPHER_FUNCS_H */
