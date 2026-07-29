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
