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
static JsonbValue *IteratorConcat(JsonbIterator **it1, JsonbIterator **it2,
			   JsonbParseState **state);

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

	if(!(JB_ROOT_IS_SCALAR(l) && JB_ROOT_IS_SCALAR(r)))
	{
		JsonbParseState *state = NULL;
		JsonbValue *res = NULL;
		JsonbIterator *it1;
		JsonbIterator *it2;

		if(JB_ROOT_IS_OBJECT(l) && JB_ROOT_IS_OBJECT(r)){
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid concatenation of jsonb objects")));
		}

		if (JB_ROOT_COUNT(l) == 0 && !JB_ROOT_IS_SCALAR(r))
			PG_RETURN_JSONB(r);
		else if (JB_ROOT_COUNT(r) == 0 && !JB_ROOT_IS_SCALAR(l))
			PG_RETURN_JSONB(l);

		it1 = JsonbIteratorInit(&l->root);
		it2 = JsonbIteratorInit(&r->root);

		res = IteratorConcat(&it1, &it2, &state);

		Assert(res != NULL);

		PG_RETURN_JSONB(JsonbValueToJsonb(res));
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

static JsonbValue *
IteratorConcat(JsonbIterator **it1, JsonbIterator **it2,
			   JsonbParseState **state)
{
	JsonbValue	v1;
	JsonbValue	v2,
	*res = NULL;
	JsonbIteratorToken r1;
	JsonbIteratorToken r2;
	JsonbIteratorToken rk1;
	JsonbIteratorToken rk2;

	r1 = rk1 = JsonbIteratorNext(it1, &v1, false);
	r2 = rk2 = JsonbIteratorNext(it2, &v2, false);

	if (rk1 == WJB_BEGIN_ARRAY && rk2 == WJB_BEGIN_ARRAY)
	{
		pushJsonbValue(state, WJB_BEGIN_ARRAY, NULL);

		while ((r1 = JsonbIteratorNext(it1, &v1, true)) != WJB_END_ARRAY)
		{
			Assert(r1 == WJB_ELEM);
			pushJsonbValue(state, r1, &v1);
		}

		while ((r2 = JsonbIteratorNext(it2, &v2, true)) != WJB_END_ARRAY)
		{
			Assert(r2 == WJB_ELEM);
			pushJsonbValue(state, r2, &v2);
		}

		res = pushJsonbValue(state, WJB_END_ARRAY, NULL);
	}
	else if (((rk1 == WJB_BEGIN_ARRAY && !(*it1)->isScalar)
			   && rk2 == WJB_BEGIN_OBJECT) ||
			 (rk1 == WJB_BEGIN_OBJECT && (rk2 == WJB_BEGIN_ARRAY
			   && !(*it2)->isScalar)))
	{
		JsonbIterator **it_array = rk1 == WJB_BEGIN_ARRAY ? it1 : it2;
		JsonbIterator **it_object = rk1 == WJB_BEGIN_OBJECT ? it1 : it2;

		bool		prepend = (rk1 == WJB_BEGIN_OBJECT);

		pushJsonbValue(state, WJB_BEGIN_ARRAY, NULL);

		if (prepend)
		{
			pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
			while ((r1 = JsonbIteratorNext(it_object, &v1, true)) != 0)
				pushJsonbValue(state, r1, r1 != WJB_END_OBJECT ? &v1 : NULL);

			while ((r2 = JsonbIteratorNext(it_array, &v2, true)) != 0)
				res = pushJsonbValue(state, r2,
									   r2 != WJB_END_ARRAY ? &v2 : NULL);
		}
		else
		{
			while ((r1 = JsonbIteratorNext(it_array, &v1, true))
				   != WJB_END_ARRAY)
				pushJsonbValue(state, r1, &v1);

			pushJsonbValue(state, WJB_BEGIN_OBJECT, NULL);
			while ((r2 = JsonbIteratorNext(it_object, &v2, true)) != 0)
				pushJsonbValue(state, r2, r2 != WJB_END_OBJECT ? &v2 : NULL);

			res = pushJsonbValue(state, WJB_END_ARRAY, NULL);
		}
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid concatenation of jsonb objects")));
	}

	return res;
}
