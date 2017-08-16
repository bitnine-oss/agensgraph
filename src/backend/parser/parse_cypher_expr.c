/*
 * parse_cypher_expr.c
 *	  handle Cypher expressions in parser
 *
 * Copyright (c) 2017 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/parser/parse_cypher_expr.c
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_cypher_expr.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/jsonb.h"
#include "utils/numeric.h"

static Node *transformA_Const(ParseState *pstate, A_Const *a_con);
static Datum integerToJsonb(ParseState *pstate, int64 i, int location);
static Datum floatToJsonb(ParseState *pstate, char *f, int location);
static Jsonb *numericToJsonb(Numeric n);
static Datum stringToJsonb(ParseState *pstate, char *s, int location);
static Node *transformTypeCast(ParseState *pstate, TypeCast *tc);

Node *
transformCypherExpr(ParseState *pstate, Node *expr, ParseExprKind exprKind)
{
	if (expr == NULL)
		return NULL;

	check_stack_depth();

	switch (nodeTag(expr))
	{
		case T_A_Const:
			return transformA_Const(pstate, (A_Const *) expr);
		case T_TypeCast:
			return transformTypeCast(pstate, (TypeCast *) expr);
		default:
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(expr));
			return NULL;
	}
}

static Node *
transformA_Const(ParseState *pstate, A_Const *a_con)
{
	Value	   *value = &a_con->val;
	int			location = a_con->location;
	Datum		datum;
	Const	   *con;

	switch (nodeTag(value))
	{
		case T_Integer:
			datum = integerToJsonb(pstate, (int64) intVal(value), location);
			break;
		case T_Float:
			{
				int64		i;

				if (scanint8(strVal(value), true, &i))
					datum = integerToJsonb(pstate, i, location);
				else
					datum = floatToJsonb(pstate, strVal(value), location);
			}
			break;
		case T_String:
			datum = stringToJsonb(pstate, strVal(value), location);
			break;
		case T_Null:
			con = makeConst(UNKNOWNOID, -1, InvalidOid, -2, (Datum) 0, true,
							false);
			con->location = location;
			return (Node *) con;
		default:
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(value));
			return NULL;
	}

	con = makeConst(JSONBOID, -1, InvalidOid, -1, datum, false, false);
	con->location = location;

	return (Node *) con;
}

static Datum
integerToJsonb(ParseState *pstate, int64 i, int location)
{
	ParseCallbackState pcbstate;
	Datum		n;
	Jsonb	   *j;

	setup_parser_errposition_callback(&pcbstate, pstate, location);

	n = DirectFunctionCall1(int8_numeric, Int64GetDatum(i));
	j = numericToJsonb(DatumGetNumeric(n));

	cancel_parser_errposition_callback(&pcbstate);

	return JsonbGetDatum(j);
}

static Datum
floatToJsonb(ParseState *pstate, char *f, int location)
{
	ParseCallbackState pcbstate;
	Datum		n;
	Jsonb	   *j;

	setup_parser_errposition_callback(&pcbstate, pstate, location);

	n = DirectFunctionCall3(numeric_in, CStringGetDatum(f),
							ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
	j = numericToJsonb(DatumGetNumeric(n));

	cancel_parser_errposition_callback(&pcbstate);

	return JsonbGetDatum(j);
}

static Jsonb *
numericToJsonb(Numeric n)
{
	JsonbValue	jv;

	jv.type = jbvNumeric;
	jv.val.numeric = n;

	return JsonbValueToJsonb(&jv);
}

static Datum
stringToJsonb(ParseState *pstate, char *s, int location)
{
	StringInfoData si;
	const char *c;
	bool		escape = false;
	ParseCallbackState pcbstate;
	Datum		j;

	initStringInfo(&si);
	appendStringInfoCharMacro(&si, '"');
	for (c = s; *c != '\0'; c++)
	{
		if (escape)
		{
			appendStringInfoCharMacro(&si, *c);
			escape = false;
		}
		else
		{
			switch (*c)
			{
				case '\\':
					appendStringInfoCharMacro(&si, '\\');
					escape = true;
					break;
				case '"':
					appendStringInfoString(&si, "\\\"");
					break;
				default:
					appendStringInfoCharMacro(&si, *c);
					break;
			}
		}
	}
	appendStringInfoCharMacro(&si, '"');

	setup_parser_errposition_callback(&pcbstate, pstate, location);

	j = DirectFunctionCall3(jsonb_in, CStringGetDatum(si.data),
							ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));

	cancel_parser_errposition_callback(&pcbstate);

	return j;
}

/* This function assumes that TypeCast is for boolean constants only. */
static Node *
transformTypeCast(ParseState *pstate, TypeCast *tc)
{
	A_Const	   *a_con;
	Value	   *value;
	int			location;
	bool		b;
	JsonbValue	jv;
	ParseCallbackState pcbstate;
	Jsonb	   *j;
	Const	   *con;

	Assert(IsA(tc->arg, A_Const));
	a_con = (A_Const *) tc->arg;
	value = &a_con->val;
	Assert(IsA(value, String));
	location = a_con->location;

	parse_bool(value->val.str, &b);

	jv.type = jbvBool;
	jv.val.boolean = b;

	setup_parser_errposition_callback(&pcbstate, pstate, location);

	j = JsonbValueToJsonb(&jv);

	cancel_parser_errposition_callback(&pcbstate);

	con = makeConst(JSONBOID, -1, InvalidOid, -1, JsonbGetDatum(j), false,
					false);
	con->location = location;

	return (Node *) con;
}

List *
transformItemList(ParseState *pstate, List *items, ParseExprKind exprKind)
{
	List	   *targets = NIL;
	ListCell   *li;

	foreach(li, items)
	{
		ResTarget  *item = lfirst(li);
		Node	   *expr;
		char	   *colname;
		TargetEntry *te;

		expr = transformCypherExpr(pstate, item->val, exprKind);
		colname = (item->name == NULL ? "?column?" : item->name);

		te = makeTargetEntry((Expr *) expr,
							 (AttrNumber) pstate->p_next_resno++,
							 colname, false);

		targets = lappend(targets, te);
	}

	return targets;
}
