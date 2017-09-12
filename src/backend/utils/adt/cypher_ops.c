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
#include "utils/memutils.h"
#include "utils/numeric.h"

static Jsonb *number_op(PGFunction f, Jsonb *l, Jsonb *r);
static Jsonb *numeric_to_number(Numeric n);
static void ereport_number_op(PGFunction f, Jsonb *l, Jsonb *r);
static void ereport_op(const char *op, Jsonb *l, Jsonb *r);
static Datum get_numeric_0_datum(void);

Datum
jsonb_add(PG_FUNCTION_ARGS)
{
	Jsonb	   *l = PG_GETARG_JSONB(0);
	Jsonb	   *r = PG_GETARG_JSONB(1);
	JsonbValue *ljv;
	JsonbValue *rjv;
	JsonbValue	jv;
	Size		len;
	char	   *buf;
	Datum		n;
	char	   *nstr;

	if (!(JB_ROOT_IS_SCALAR(l) && JB_ROOT_IS_SCALAR(r)))
	{
		Datum		j;

		if ((JB_ROOT_IS_SCALAR(l) && JB_ROOT_IS_OBJECT(r)) ||
			(JB_ROOT_IS_OBJECT(l) && JB_ROOT_IS_SCALAR(r)) ||
			(JB_ROOT_IS_OBJECT(l) && JB_ROOT_IS_OBJECT(r)))
			ereport_op("+", l, r);

		j = DirectFunctionCall2(jsonb_concat,
								JsonbGetDatum(l), JsonbGetDatum(r));

		PG_RETURN_DATUM(j);
	}

	ljv = getIthJsonbValueFromContainer(&l->root, 0);
	rjv = getIthJsonbValueFromContainer(&r->root, 0);

	if (ljv->type == jbvString && rjv->type == jbvString)
	{
		len = ljv->val.string.len + rjv->val.string.len;
		buf = palloc(len + 1);

		strncpy(buf, ljv->val.string.val, ljv->val.string.len);
		strncpy(buf + ljv->val.string.len,
				rjv->val.string.val, rjv->val.string.len);
		buf[len] = '\0';

		jv.type = jbvString;
		jv.val.string.len = len;
		jv.val.string.val = buf;

		PG_RETURN_JSONB(JsonbValueToJsonb(&jv));
	}
	else if (ljv->type == jbvString && rjv->type == jbvNumeric)
	{
		n = DirectFunctionCall1(numeric_out,
								NumericGetDatum(rjv->val.numeric));
		nstr = DatumGetCString(n);

		len = ljv->val.string.len + strlen(nstr);
		buf = palloc(len + 1);

		strncpy(buf, ljv->val.string.val, ljv->val.string.len);
		strcpy(buf + ljv->val.string.len, nstr);

		jv.type = jbvString;
		jv.val.string.len = len;
		jv.val.string.val = buf;

		PG_RETURN_JSONB(JsonbValueToJsonb(&jv));
	}
	else if (ljv->type == jbvNumeric && rjv->type == jbvString)
	{
		Size		nlen;

		n = DirectFunctionCall1(numeric_out,
								NumericGetDatum(ljv->val.numeric));
		nstr = DatumGetCString(n);
		nlen = strlen(nstr);

		len = nlen + rjv->val.string.len;
		buf = palloc(len + 1);

		strcpy(buf, nstr);
		strncpy(buf + nlen, rjv->val.string.val, rjv->val.string.len);
		buf[len] = '\0';

		jv.type = jbvString;
		jv.val.string.len = len;
		jv.val.string.val = buf;

		PG_RETURN_JSONB(JsonbValueToJsonb(&jv));
	}
	else if (ljv->type == jbvNumeric && rjv->type == jbvNumeric)
	{
		n = DirectFunctionCall2(numeric_add,
								NumericGetDatum(ljv->val.numeric),
								NumericGetDatum(rjv->val.numeric));

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
	JsonbValue *jv;
	Datum		n;

	AssertArg(r != NULL);

	if (!((l == NULL || JB_ROOT_IS_SCALAR(l)) && JB_ROOT_IS_SCALAR(r)))
		ereport_number_op(f, l, r);

	InitFunctionCallInfoData(fcinfo, NULL, 0, InvalidOid, NULL, NULL);

	if (l != NULL)
	{
		jv = getIthJsonbValueFromContainer(&l->root, 0);
		if (jv->type != jbvNumeric)
			ereport_number_op(f, l, r);

		fcinfo.arg[fcinfo.nargs] = NumericGetDatum(jv->val.numeric);
		fcinfo.argnull[fcinfo.nargs] = false;
		fcinfo.nargs++;
	}

	jv = getIthJsonbValueFromContainer(&r->root, 0);
	if (jv->type != jbvNumeric)
		ereport_number_op(f, l, r);

	fcinfo.arg[fcinfo.nargs] = NumericGetDatum(jv->val.numeric);
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

Datum
jsonb_bool(PG_FUNCTION_ARGS)
{
	Jsonb	   *j = PG_GETARG_JSONB(0);

	if (JB_ROOT_IS_SCALAR(j))
	{
		JsonbValue *jv;

		jv = getIthJsonbValueFromContainer(&j->root, 0);
		switch (jv->type)
		{
			case jbvNull:
				PG_RETURN_NULL();
			case jbvString:
				PG_RETURN_BOOL(jv->val.string.len > 0);
			case jbvNumeric:
				{
					Datum		b;

					if (numeric_is_nan(jv->val.numeric))
						PG_RETURN_BOOL(false);

					b = DirectFunctionCall2(numeric_ne,
											NumericGetDatum(jv->val.numeric),
											get_numeric_0_datum());
					PG_RETURN_BOOL(b);
				}
			case jbvBool:
				PG_RETURN_BOOL(jv->val.boolean);
			default:
				elog(ERROR, "unknown jsonb scalar type");
		}
	}

	Assert(JB_ROOT_IS_OBJECT(j) || JB_ROOT_IS_ARRAY(j));
	PG_RETURN_BOOL(JB_ROOT_COUNT(j) > 0);
}

static Datum
get_numeric_0_datum(void)
{
	static Datum n = 0;

	if (n == 0)
	{
		MemoryContext oldMemoryContext;

		oldMemoryContext = MemoryContextSwitchTo(TopMemoryContext);

		n = DirectFunctionCall1(int8_numeric, Int64GetDatum(0));

		MemoryContextSwitchTo(oldMemoryContext);
	}

	return n;
}

Datum
bool_jsonb(PG_FUNCTION_ARGS)
{
	bool		b = PG_GETARG_BOOL(0);
	JsonbValue	jv;

	jv.type = jbvBool;
	jv.val.boolean = b;

	PG_RETURN_JSONB(JsonbValueToJsonb(&jv));
}
