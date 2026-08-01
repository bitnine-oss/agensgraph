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
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/jsonb.h"
#include "utils/jsonfuncs.h"
#include "utils/lsyscache.h"
#include "nodes/miscnodes.h"
#include "catalog/pg_type_d.h"
#include "access/transam.h"
#include "parser/parse_coerce.h"

/*
 * cypher_to_jsonb
 *		Carry a value into the jsonb a Cypher expression works with.
 *
 * to_jsonb represents the types it knows structurally and falls back to the text
 * output of anything else, which lands as a jsonb string.  For a type whose text
 * form is itself JSON that loses the value's shape: a promoted vector property
 * read from its typed column would come back as the string "[1,2,3]" while the
 * same property read from the jsonb bag is the array [1,2,3], so the two ways of
 * reading one property disagree, and list operations on it stop working.
 *
 * Parse the text form of such a type as jsonb and keep it when it parses.  Which
 * types those are is decided from the type a value really is, not the name it
 * goes by: a domain is resolved to the type underneath it, and what is left has
 * to be a type that is neither built-in nor a kind of string nor a label from a
 * fixed set.  Everything else is left alone, because for those the text
 * happening to look like JSON says nothing about the value's shape -- a text
 * column holding "[9,9]" holds those six characters, not a list.  A text form
 * that is not JSON still becomes a jsonb string, exactly as before.
 */
Datum
cypher_to_jsonb(PG_FUNCTION_ARGS)
{
	Oid			argtype;
	Oid			basetype;
	JsonTypeCategory tcategory;
	Oid			outfuncoid;

	if (PG_ARGISNULL(0))
		return to_jsonb(fcinfo);

	argtype = get_fn_expr_argtype(fcinfo->flinfo, 0);
	if (!OidIsValid(argtype))
		return to_jsonb(fcinfo);

	/*
	 * Judge the type this value really is.  A domain is a base type under
	 * another name and holds exactly what the base type holds, so a domain over
	 * a built-in type has to read as that built-in type does.
	 */
	basetype = getBaseType(argtype);

	/* a built-in type's representation is already settled */
	if (basetype < FirstNormalObjectId)
		return to_jsonb(fcinfo);

	/*
	 * A type that is a kind of string holds text, whatever its text happens to
	 * look like: reading it as JSON would turn a string that reads like a number
	 * or a list into one.  The same goes for a label drawn from a fixed set.
	 * What is left is a type whose value has a shape of its own that to_jsonb
	 * has no way to know -- which is the case this exists for.
	 */
	if (TypeCategory(basetype) == TYPCATEGORY_STRING ||
		TypeCategory(basetype) == TYPCATEGORY_ENUM)
		return to_jsonb(fcinfo);

	json_categorize_type(basetype, true, &tcategory, &outfuncoid);
	if (tcategory == JSONTYPE_OTHER && OidIsValid(outfuncoid))
	{
		char	   *str = OidOutputFunctionCall(outfuncoid, PG_GETARG_DATUM(0));
		ErrorSaveContext escontext = {T_ErrorSaveContext};
		FmgrInfo	infinfo;
		Oid			infunc;
		Oid			intypioparam;
		Datum		parsed;

		getTypeInputInfo(JSONBOID, &infunc, &intypioparam);
		fmgr_info(infunc, &infinfo);
		if (InputFunctionCallSafe(&infinfo, str, intypioparam, -1,
								  (Node *) &escontext, &parsed))
			PG_RETURN_DATUM(parsed);
	}

	return to_jsonb(fcinfo);
}

Datum
cypher_isempty_jsonb(PG_FUNCTION_ARGS)
{
	Jsonb	   *jb = PG_GETARG_JSONB_P(0);

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

/*
 * ag_property_text
 *
 * The text of a property, for a promoted column to take its value from -- but
 * only when the value is of the kind the column can hold.
 *
 * A column derived from the bag with ->> takes the value's *text*, and text
 * carries no kind: the string "5" and the number 5 arrive as the same three
 * characters, so a column of integers would answer 5 for a property that holds
 * a string, and one of text would answer "5" for a property that holds a
 * number.  Neither is what the property says.
 *
 * So check the kind first and refuse a value of another one.  What is returned
 * is still the text, because the column's own type is what decides whether the
 * value fits: an integer column must still refuse 5.5 and a number too large,
 * and a vector must still refuse the wrong number of dimensions.  This adds a
 * question that was not being asked; it does not replace the ones that were.
 *
 * A property that is absent, or present and null, has no value to take, and
 * both answer NULL -- the same contract ->> follows and the one Cypher expects
 * of a property that is not there.
 */
Datum
ag_property_text(PG_FUNCTION_ARGS)
{
	Jsonb	   *bag = PG_GETARG_JSONB_P(0);
	text	   *keytext = PG_GETARG_TEXT_PP(1);
	char		expected = PG_GETARG_CHAR(2);
	char	   *key;
	JsonbValue *value;
	const char *found;

	if (!JB_ROOT_IS_OBJECT(bag))
		PG_RETURN_NULL();

	key = text_to_cstring(keytext);
	value = getKeyJsonValueFromContainer(&bag->root, key, strlen(key), NULL);

	if (value == NULL || value->type == jbvNull)
		PG_RETURN_NULL();

	switch (value->type)
	{
		case jbvNumeric:
			found = "a number";
			if (expected == 'n')
				PG_RETURN_TEXT_P(cstring_to_text(DatumGetCString(
					DirectFunctionCall1(numeric_out,
										NumericGetDatum(value->val.numeric)))));
			break;
		case jbvString:
			found = "a string";
			if (expected == 's')
				PG_RETURN_TEXT_P(cstring_to_text_with_len(value->val.string.val,
														  value->val.string.len));
			break;
		case jbvBool:
			found = "a boolean";
			if (expected == 'b')
				PG_RETURN_TEXT_P(cstring_to_text(value->val.boolean ?
												 "true" : "false"));
			break;
		case jbvBinary:
			found = JsonContainerIsArray(value->val.binary.data) ?
				"a list" : "a map";
			break;
		default:
			found = "a value of another kind";
			break;
	}

	ereport(ERROR,
			(errcode(ERRCODE_DATATYPE_MISMATCH),
			 errmsg("property \"%s\" holds %s", key, found),
			 errdetail("A column promoting this property holds %s.",
					   expected == 'n' ? "numbers" :
					   expected == 's' ? "strings" : "booleans")));
	PG_RETURN_NULL();			/* keep the compiler quiet */
}
