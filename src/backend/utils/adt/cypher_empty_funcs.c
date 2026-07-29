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
 * Parse the text form of such a type as jsonb and keep it when it parses.  This is
 * confined to types an extension defines, because those are the ones to_jsonb
 * cannot know about.  Every built-in type is left alone -- including the ones
 * to_jsonb also has no structural representation for, such as an unknown-typed
 * expression, where the text happening to look like JSON says nothing about the
 * value's shape.  A text form that is not JSON still becomes a jsonb string,
 * exactly as before.
 */
Datum
cypher_to_jsonb(PG_FUNCTION_ARGS)
{
	Oid			argtype;
	JsonTypeCategory tcategory;
	Oid			outfuncoid;

	if (PG_ARGISNULL(0))
		return to_jsonb(fcinfo);

	argtype = get_fn_expr_argtype(fcinfo->flinfo, 0);
	if (!OidIsValid(argtype))
		return to_jsonb(fcinfo);

	if (argtype < FirstNormalObjectId)
		return to_jsonb(fcinfo);

	json_categorize_type(argtype, true, &tcategory, &outfuncoid);
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
