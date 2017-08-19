/*
 * cypher_ops.c
 *	  Functions for operators in Cypher expressions.
 *
 * Copyright (c) 2017 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/cypher_ops.c
 */

#include "postgres.h"

#include "utils/builtins.h"
#include "utils/cypher_ops.h"
#include "utils/datum.h"
#include "utils/jsonb.h"
#include "utils/numeric.h"

static Jsonb *number_op(PGFunction f, Jsonb *l, Jsonb *r);
static Jsonb *numeric_to_number(Numeric n);
static void ereport_number_op(PGFunction f, Jsonb *l, Jsonb *r);
static void ereport_op(const char *op, Jsonb *l, Jsonb *r);

Datum
jsonb_add(PG_FUNCTION_ARGS)
{
	Jsonb	   *l = PG_GETARG_JSONB(0);
	Jsonb	   *r = PG_GETARG_JSONB(1);
	JsonbIterator *it;
	JsonbValue	ljv;
	JsonbValue	rjv;
	JsonbValue	jv;
	Size		len;
	char	   *buf;
	Datum		n;
	char	   *nstr;

	if (!(JB_ROOT_IS_SCALAR(l) && JB_ROOT_IS_SCALAR(r)))
		ereport_op("+", l, r);

	it = JsonbIteratorInit(&l->root);
	JsonbIteratorNext(&it, &ljv, true);
	Assert(ljv.type == jbvArray);
	JsonbIteratorNext(&it, &ljv, true);

	it = JsonbIteratorInit(&r->root);
	JsonbIteratorNext(&it, &rjv, true);
	Assert(rjv.type == jbvArray);
	JsonbIteratorNext(&it, &rjv, true);

	if (ljv.type == jbvString && rjv.type == jbvString)
	{
		len = ljv.val.string.len + rjv.val.string.len;
		buf = palloc(len + 1);

		strncpy(buf, ljv.val.string.val, ljv.val.string.len);
		strncpy(buf + ljv.val.string.len,
				rjv.val.string.val, rjv.val.string.len);
		buf[len] = '\0';

		jv.type = jbvString;
		jv.val.string.len = len;
		jv.val.string.val = buf;

		PG_RETURN_JSONB(JsonbValueToJsonb(&jv));
	}
	else if (ljv.type == jbvString && rjv.type == jbvNumeric)
	{
		n = DirectFunctionCall1(numeric_out, NumericGetDatum(rjv.val.numeric));
		nstr = DatumGetCString(n);

		len = ljv.val.string.len + strlen(nstr);
		buf = palloc(len + 1);

		strncpy(buf, ljv.val.string.val, ljv.val.string.len);
		strcpy(buf + ljv.val.string.len, nstr);

		jv.type = jbvString;
		jv.val.string.len = len;
		jv.val.string.val = buf;

		PG_RETURN_JSONB(JsonbValueToJsonb(&jv));
	}
	else if (ljv.type == jbvNumeric && rjv.type == jbvString)
	{
		Size		nlen;

		n = DirectFunctionCall1(numeric_out, NumericGetDatum(ljv.val.numeric));
		nstr = DatumGetCString(n);
		nlen = strlen(nstr);

		len = nlen + rjv.val.string.len;
		buf = palloc(len + 1);

		strcpy(buf, nstr);
		strncpy(buf + nlen, rjv.val.string.val, rjv.val.string.len);
		buf[len] = '\0';

		jv.type = jbvString;
		jv.val.string.len = len;
		jv.val.string.val = buf;

		PG_RETURN_JSONB(JsonbValueToJsonb(&jv));
	}
	else if (ljv.type == jbvNumeric && rjv.type == jbvNumeric)
	{
		n = DirectFunctionCall2(numeric_add,
								NumericGetDatum(ljv.val.numeric),
								NumericGetDatum(rjv.val.numeric));

		PG_RETURN_JSONB(numeric_to_number(DatumGetNumeric(n)));
	}
	else
	{
		ereport_op("+", l, r);
	}

	PG_RETURN_NULL();
}

Datum
jsonb_sub(PG_FUNCTION_ARGS)
{
	PG_RETURN_JSONB(number_op(numeric_sub,
							  PG_GETARG_JSONB(0), PG_GETARG_JSONB(1)));
}

Datum
jsonb_mul(PG_FUNCTION_ARGS)
{
	PG_RETURN_JSONB(number_op(numeric_mul,
							  PG_GETARG_JSONB(0), PG_GETARG_JSONB(1)));
}

Datum
jsonb_div(PG_FUNCTION_ARGS)
{
	PG_RETURN_JSONB(number_op(numeric_div,
							  PG_GETARG_JSONB(0), PG_GETARG_JSONB(1)));
}

Datum
jsonb_mod(PG_FUNCTION_ARGS)
{
	PG_RETURN_JSONB(number_op(numeric_mod,
							  PG_GETARG_JSONB(0), PG_GETARG_JSONB(1)));
}

Datum
jsonb_pow(PG_FUNCTION_ARGS)
{
	PG_RETURN_JSONB(number_op(numeric_power,
							  PG_GETARG_JSONB(0), PG_GETARG_JSONB(1)));
}

Datum
jsonb_uplus(PG_FUNCTION_ARGS)
{
	PG_RETURN_JSONB(number_op(numeric_uplus, NULL, PG_GETARG_JSONB(0)));
}

Datum
jsonb_uminus(PG_FUNCTION_ARGS)
{
	PG_RETURN_JSONB(number_op(numeric_uminus, NULL, PG_GETARG_JSONB(0)));
}

static Jsonb *
number_op(PGFunction f, Jsonb *l, Jsonb *r)
{
	FunctionCallInfoData fcinfo;
	JsonbIterator *it;
	JsonbValue	jv;
	Datum		n;

	AssertArg(r != NULL);

	if (!((l == NULL || JB_ROOT_IS_SCALAR(l)) && JB_ROOT_IS_SCALAR(r)))
		ereport_number_op(f, l, r);

	InitFunctionCallInfoData(fcinfo, NULL, 0, InvalidOid, NULL, NULL);

	if (l != NULL)
	{
		it = JsonbIteratorInit(&l->root);
		JsonbIteratorNext(&it, &jv, true);
		Assert(jv.type == jbvArray);
		JsonbIteratorNext(&it, &jv, true);

		if (jv.type != jbvNumeric)
			ereport_number_op(f, l, r);

		fcinfo.arg[fcinfo.nargs] = NumericGetDatum(jv.val.numeric);
		fcinfo.argnull[fcinfo.nargs] = false;
		fcinfo.nargs++;
	}

	it = JsonbIteratorInit(&r->root);
	JsonbIteratorNext(&it, &jv, true);
	Assert(jv.type == jbvArray);
	JsonbIteratorNext(&it, &jv, true);

	if (jv.type != jbvNumeric)
		ereport_number_op(f, l, r);

	fcinfo.arg[fcinfo.nargs] = NumericGetDatum(jv.val.numeric);
	fcinfo.argnull[fcinfo.nargs] = false;
	fcinfo.nargs++;

	n = (*f) (&fcinfo);

	if (fcinfo.isnull)
		elog(ERROR, "function %p returned NULL", (void *) f);

	return numeric_to_number(DatumGetNumeric(n));
}

static Jsonb *
numeric_to_number(Numeric n)
{
	JsonbValue	jv;

	jv.type = jbvNumeric;
	jv.val.numeric = n;

	return JsonbValueToJsonb(&jv);
}

static void
ereport_number_op(PGFunction f, Jsonb *l, Jsonb *r)
{
	const char *op;

	if (f == numeric_add)
		op = "+";
	else if (f == numeric_sub)
		op = "-";
	else if (f == numeric_mul)
		op = "*";
	else if (f == numeric_div)
		op = "/";
	else if (f == numeric_mod)
		op = "%";
	else if (f == numeric_power)
		op = "^";
	else if (f == numeric_uplus)
		op = "+";
	else if (f == numeric_uminus)
		op = "-";
	else
		elog(ERROR, "invalid number operator");

	ereport_op(op, l, r);
}

static void
ereport_op(const char *op, Jsonb *l, Jsonb *r)
{
	const char *msgfmt;
	const char *lstr;
	const char *rstr;

	AssertArg(r != NULL);

	if (l == NULL)
	{
		msgfmt = "invalid expression: %s%s%s";
		lstr = "";
	}
	else
	{
		msgfmt = "invalid expression: %s %s %s";
		lstr = JsonbToCString(NULL, &l->root, VARSIZE(l));
	}
	rstr = JsonbToCString(NULL, &r->root, VARSIZE(r));

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg(msgfmt, lstr, op, rstr)));
}
