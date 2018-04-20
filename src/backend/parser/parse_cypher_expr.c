/*
 * parse_cypher_expr.c
 *	  handle Cypher expressions in parser
 *
 * Copyright (c) 2017 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/parser/parse_cypher_expr.c
 *
 *
 * To store all types that Cypher supports in a single column, almost all
 * expressions expect jsonb values as their arguments and return a jsonb value.
 * Exceptions are comparison operators (=, <>, <, >, <=, >=) and boolean
 * operators (OR, AND, NOT). Comparison operators take jsonb values and return
 * a bool value. And boolean operators take bool values and return a bool
 * value. This is because they use the existing implementation to evaluate
 * themselves.
 * We use SQL NULL instead of 'null'::jsonb. This makes it easy to implement
 * "operations on NULL values return NULL".
 */

#include "postgres.h"

#include "ag_const.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/analyze.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_cypher_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"

static Node *transformCypherExprRecurse(ParseState *pstate, Node *expr);
static Node *transformColumnRef(ParseState *pstate, ColumnRef *cref);
static Node *transformListCompColumnRef(ParseState *pstate, ColumnRef *cref,
										char *varname);
static Node *scanRTEForVar(ParseState *pstate, Node *var, RangeTblEntry *rte,
						   char *colname, int location);
static Node *transformFields(ParseState *pstate, Node *basenode, List *fields,
							 int location);
static Node *filterAccessArg(ParseState *pstate, Node *expr, int location,
							 const char *types);
static Node *transformParamRef(ParseState *pstate, ParamRef *pref);
static Node *transformA_Const(ParseState *pstate, A_Const *a_con);
static Datum integerToJsonb(ParseState *pstate, int64 i, int location);
static Datum floatToJsonb(ParseState *pstate, char *f, int location);
static Jsonb *numericToJsonb(Numeric n);
static Datum stringToJsonb(ParseState *pstate, char *s, int location);
static Node *transformTypeCast(ParseState *pstate, TypeCast *tc);
static Node *transformCypherMapExpr(ParseState *pstate, CypherMapExpr *m);
static Node *transformCypherListExpr(ParseState *pstate, CypherListExpr *cl);
static Node *transformCypherListComp(ParseState *pstate, CypherListComp * clc);
static Node *transformCaseExpr(ParseState *pstate, CaseExpr *c);
static Node *transformFuncCall(ParseState *pstate, FuncCall *fn);
static Node *transformCoalesceExpr(ParseState *pstate, CoalesceExpr *c);
static Node *transformSubLink(ParseState *pstate, SubLink *sublink);
static Node *transformIndirection(ParseState *pstate, Node *basenode,
								  List *indirection);
static Node *makeArrayIndex(ParseState *pstate, Node *idx, bool exclusive);
static Node *adjustListIndexType(ParseState *pstate, Node *idx);
static Node *transformAExprOp(ParseState *pstate, A_Expr *a);
static Node *transformAExprIn(ParseState *pstate, A_Expr *a);
static Node *transformBoolExpr(ParseState *pstate, BoolExpr *b);
static Node *coerce_to_jsonb(ParseState *pstate, Node *expr,
							 const char *targetname, bool err);

static List *transformA_Star(ParseState *pstate, int location);

Node *
transformCypherExpr(ParseState *pstate, Node *expr, ParseExprKind exprKind)
{
	Node	   *result;
	ParseExprKind sv_expr_kind;

	Assert(exprKind != EXPR_KIND_NONE);
	sv_expr_kind = pstate->p_expr_kind;
	pstate->p_expr_kind = exprKind;

	result = transformCypherExprRecurse(pstate, expr);
	result = wrapEdgeRefTypes(pstate, result);

	pstate->p_expr_kind = sv_expr_kind;

	return result;
}

static Node *
transformCypherExprRecurse(ParseState *pstate, Node *expr)
{
	if (expr == NULL)
		return NULL;

	check_stack_depth();

	switch (nodeTag(expr))
	{
		case T_ColumnRef:
			return transformColumnRef(pstate, (ColumnRef *) expr);
		case T_ParamRef:
			return transformParamRef(pstate, (ParamRef *) expr);
		case T_A_Const:
			return transformA_Const(pstate, (A_Const *) expr);
		case T_TypeCast:
			return transformTypeCast(pstate, (TypeCast *) expr);
		case T_CypherMapExpr:
			return transformCypherMapExpr(pstate, (CypherMapExpr *) expr);
		case T_CypherListExpr:
			return transformCypherListExpr(pstate, (CypherListExpr *) expr);
		case T_CypherListComp:
			return transformCypherListComp(pstate, (CypherListComp *) expr);
		case T_CaseExpr:
			return transformCaseExpr(pstate, (CaseExpr *) expr);
		case T_CaseTestExpr:
			return expr;
		case T_FuncCall:
			return transformFuncCall(pstate, (FuncCall *) expr);
		case T_CoalesceExpr:
			return transformCoalesceExpr(pstate, (CoalesceExpr *) expr);
		case T_SubLink:
			return transformSubLink(pstate, (SubLink *) expr);
		case T_A_Indirection:
			{
				A_Indirection *indir = (A_Indirection *) expr;
				Node	   *basenode;

				basenode = transformCypherExprRecurse(pstate, indir->arg);
				return transformIndirection(pstate, basenode,
											indir->indirection);
			}
		case T_A_Expr:
			{
				A_Expr	   *a = (A_Expr *) expr;

				switch (a->kind)
				{
					case AEXPR_OP:
						return transformAExprOp(pstate, a);
					case AEXPR_IN:
						return transformAExprIn(pstate, a);
					default:
						elog(ERROR, "unrecognized A_Expr kind: %d", a->kind);
						return NULL;
				}
			}
		case T_NullTest:
			{
				NullTest   *n = (NullTest *) expr;

				n->arg = (Expr *) transformCypherExprRecurse(pstate,
															 (Node *) n->arg);
				n->argisrow = false;

				return expr;
			}
		case T_BoolExpr:
			return transformBoolExpr(pstate, (BoolExpr *) expr);
		default:
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(expr));
			return NULL;
	}
}

/*
 * Because we have to check all the cases of variable references (Cypher
 * variables and variables in ancestor queries if the Cypher query is the
 * subquery) for each level of ParseState, logic and functions in original
 * transformColumnRef() are properly modified, refactored, and then integrated
 * into one.
 */
static Node *
transformColumnRef(ParseState *pstate, ColumnRef *cref)
{
	int			nfields = list_length(cref->fields);
	int			location = cref->location;
	Node	   *node = NULL;
	Node	   *field1;
	Node	   *field2 = NULL;
	Node	   *field3 = NULL;
	Oid			nspid1 = InvalidOid;
	Node	   *field4 = NULL;
	Oid			nspid2 = InvalidOid;
	ParseState *pstate_up;
	int			nindir = nfields - 1;

	if (pstate->p_lc_varname != NULL)
	{
		node = transformListCompColumnRef(pstate, cref, pstate->p_lc_varname);
		if (node != NULL)
			return node;
	}

	field1 = linitial(cref->fields);
	if (nfields >= 2)
		field2 = lsecond(cref->fields);
	if (nfields >= 3)
	{
		field3 = lthird(cref->fields);
		nspid1 = LookupNamespaceNoError(strVal(field1));
	}
	if (nfields >= 4 &&
		strcmp(strVal(field1), get_database_name(MyDatabaseId)) == 0)
	{
		field4 = lfourth(cref->fields);
		nspid2 = LookupNamespaceNoError(strVal(field2));
	}

	pstate_up = pstate;
	while (pstate_up != NULL)
	{
		ListCell   *lni;

		/* find the Var at the current level of ParseState */

		foreach(lni, pstate_up->p_namespace)
		{
			ParseNamespaceItem *nsitem = lfirst(lni);
			RangeTblEntry *rte = nsitem->p_rte;
			Oid			relid2;
			Oid			relid3;

			if (nsitem->p_lateral_only)
			{
				/* If not inside LATERAL, ignore lateral-only items. */
				if (!pstate_up->p_lateral_active)
					continue;

				/*
				 * If the namespace item is currently disallowed as a LATERAL
				 * reference, skip the rest.
				 */
				if (!nsitem->p_lateral_ok)
					continue;
			}

			/*
			 * If this RTE is accessible by unqualified names,
			 * examine `field1`.
			 */
			if (nsitem->p_cols_visible)
			{
				node = scanRTEForVar(pstate, node, rte, strVal(field1),
									 location);
				nindir = 0;
			}

			/*
			 * If this RTE is inaccessible by qualified names,
			 * skip the rest.
			 */
			if (!nsitem->p_rel_visible)
				continue;

			/* examine `field1.field2` */
			if (field2 != NULL &&
				strcmp(rte->eref->aliasname, strVal(field1)) == 0)
			{
				node = scanRTEForVar(pstate, node, rte, strVal(field2),
									 location);
				nindir = 1;
			}

			/* examine `field1.field2.field3` */
			if (OidIsValid(nspid1) &&
				OidIsValid(relid2 = get_relname_relid(strVal(field2),
													  nspid1)) &&
				rte->rtekind == RTE_RELATION &&
				rte->relid == relid2 &&
				rte->alias == NULL)
			{
				node = scanRTEForVar(pstate, node, rte, strVal(field3),
									 location);
				nindir = 2;
			}

			/* examine `field1.field2.field3.field4` */
			if (OidIsValid(nspid2) &&
				OidIsValid(relid3 = get_relname_relid(strVal(field3),
													  nspid2)) &&
				rte->rtekind == RTE_RELATION &&
				rte->relid == relid3 &&
				rte->alias == NULL)
			{
				node = scanRTEForVar(pstate, node, rte, strVal(field4),
									 location);
				nindir = 3;
			}
		}

		if (node != NULL)
			break;

		pstate_up = pstate_up->parentParseState;
	}

	if (node == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("variable does not exist"),
				 parser_errposition(pstate, location)));
		return NULL;
	}

	if (nindir + 1 < nfields)
	{
		List	   *newfields;

		newfields = list_copy_tail(cref->fields, nindir + 1);

		return transformFields(pstate, node, newfields, location);
	}

	return node;
}

static Node *
transformListCompColumnRef(ParseState *pstate, ColumnRef *cref, char *varname)
{
	Node	   *field1 = linitial(cref->fields);
	CypherListCompVar *clcvar;

	if (strcmp(varname, strVal(field1)) != 0)
		return NULL;

	clcvar = makeNode(CypherListCompVar);
	clcvar->varname = pstrdup(varname);
	clcvar->location = cref->location;

	if (list_length(cref->fields) > 1)
	{
		List	   *newfields;

		newfields = list_copy_tail(cref->fields, 1);

		return transformFields(pstate, (Node *) clcvar, newfields,
							   cref->location);
	}

	return (Node *) clcvar;
}

static Node *
scanRTEForVar(ParseState *pstate, Node *var, RangeTblEntry *rte, char *colname,
			  int location)
{
	int			attrno;
	ListCell   *lc;

	attrno = 0;
	foreach(lc, rte->eref->colnames)
	{
		const char *colalias = strVal(lfirst(lc));

		attrno++;

		if (strcmp(colalias, colname) != 0)
			continue;

		if (var != NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_AMBIGUOUS_COLUMN),
					 errmsg("variable reference \"%s\" is ambiguous", colname),
					 parser_errposition(pstate, location)));
			return NULL;
		}

		var = (Node *) make_var(pstate, rte, attrno, location);
		markVarForSelectPriv(pstate, (Var *) var, rte);
	}

	return var;
}

static Node *
transformFields(ParseState *pstate, Node *basenode, List *fields, int location)
{
	Node	   *res;
	Oid			restype;
	ListCell   *lf;
	Value	   *field;
	List	   *path = NIL;
	CypherAccessExpr *a;

	res = basenode;
	restype = exprType(res);

	/* record/composite type */
	foreach(lf, fields)
	{
		if (restype == VERTEXOID || restype == EDGEOID ||
			restype == GRAPHPATHOID)
			break;
		if (!type_is_rowtype(restype))
			break;

		field = lfirst(lf);

		res = ParseFuncOrColumn(pstate, list_make1(field), list_make1(res),
								NULL, location);
		if (res == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" not found in data type %s",
							strVal(field), format_type_be(restype)),
					 parser_errposition(pstate, location)));
			return NULL;
		}
		restype = exprType(res);
	}

	if (lf == NULL)
		return res;

	res = filterAccessArg(pstate, res, location, "map");

	for_each_cell(lf, lf)
	{
		Node	   *elem;

		field = lfirst(lf);

		elem = (Node *) makeConst(TEXTOID, -1, DEFAULT_COLLATION_OID, -1,
								  CStringGetTextDatum(strVal(field)),
								  false, false);

		path = lappend(path, elem);
	}

	a = makeNode(CypherAccessExpr);
	a->arg = (Expr *) res;
	a->path = path;

	return (Node *) a;
}

static Node *
filterAccessArg(ParseState *pstate, Node *expr, int location,
				const char *types)
{
	Oid			exprtype = exprType(expr);

	switch (exprtype)
	{
		case EDGEREFOID:
			{
				EdgeRefProp *erp;

				erp = makeNode(EdgeRefProp);
				erp->arg = (Expr *) expr;

				return (Node *) erp;
			}
			break;

		case VERTEXOID:
		case EDGEOID:
			return ParseFuncOrColumn(pstate,
									 list_make1(makeString(AG_ELEM_PROP_MAP)),
									 list_make1(expr), NULL, location);
		case JSONBOID:
			return expr;

		case JSONOID:
			return coerce_to_target_type(pstate, expr, JSONOID,
										 JSONBOID, -1, COERCION_EXPLICIT,
										 COERCE_IMPLICIT_CAST, location);

		default:
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("%s is expected but %s",
							types, format_type_be(exprtype)),
					 parser_errposition(pstate, location)));
			return NULL;
	}
}

static Node *
transformParamRef(ParseState *pstate, ParamRef *pref)
{
	Node	   *result;

	if (pstate->p_paramref_hook != NULL)
	{
		Oid			restype;

		result = (*pstate->p_paramref_hook) (pstate, pref);
		restype = exprType(result);
		if (restype == UNKNOWNOID)
			result = coerce_type(pstate, result, restype, JSONBOID, -1,
								 COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);
		else
			result = coerce_to_jsonb(pstate, result, "parameter", false);
	}
	else
	{
		result = NULL;
	}

	if (result == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_PARAMETER),
				 errmsg("there is no parameter $%d", pref->number),
				 parser_errposition(pstate, pref->location)));

	return result;
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
			con = makeConst(JSONBOID, -1, InvalidOid, -1, 0, true, false);
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
					/* any character coming next will be escaped */
					appendStringInfoCharMacro(&si, '\\');
					escape = true;
					break;
				case '"':
					/* Escape `"`. `"` and `\"` are the same as `\"`. */
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

/*
 * This function assumes that TypeCast is for boolean constants only and
 * returns a bool value. The reason why it returns a bool value is that there
 * may be a chance to use the value as an argument of a boolean expression.
 * If the value is used as an argument of other expressions, it will be
 * converted into jsonb value implicitly.
 */
static Node *
transformTypeCast(ParseState *pstate, TypeCast *tc)
{
	A_Const	   *a_con;
	Value	   *value;
	bool		b;
	Const	   *con;

	Assert(IsA(tc->arg, A_Const));
	a_con = (A_Const *) tc->arg;
	value = &a_con->val;

	Assert(IsA(value, String));
	parse_bool(strVal(value), &b);

	con = makeConst(BOOLOID, -1, InvalidOid, 1, BoolGetDatum(b), false, true);
	con->location = a_con->location;

	return (Node *) con;
}

static Node *
transformCypherMapExpr(ParseState *pstate, CypherMapExpr *m)
{
	List	   *newkeyvals = NIL;
	ListCell   *le;
	CypherMapExpr *newm;

	Assert(list_length(m->keyvals) % 2 == 0);

	le = list_head(m->keyvals);
	while (le != NULL)
	{
		Node	   *k;
		Node	   *v;
		Node	   *newv;
		Const	   *newk;

		k = lfirst(le);
		le = lnext(le);
		v = lfirst(le);
		le = lnext(le);

		newv = transformCypherExprRecurse(pstate, v);
		newv = coerce_to_jsonb(pstate, newv, "property value", true);

		Assert(IsA(k, String));
		newk = makeConst(TEXTOID, -1, DEFAULT_COLLATION_OID, -1,
						 CStringGetTextDatum(strVal(k)), false, false);

		newkeyvals = lappend(lappend(newkeyvals, newk), newv);
	}

	newm = makeNode(CypherMapExpr);
	newm->keyvals = newkeyvals;
	newm->location = m->location;

	return (Node *) newm;
}

static Node *
transformCypherListExpr(ParseState *pstate, CypherListExpr *cl)
{
	List	   *newelems = NIL;
	ListCell   *le;
	CypherListExpr *newcl;

	foreach(le, cl->elems)
	{
		Node	   *e = lfirst(le);
		Node	   *newe;

		newe = transformCypherExprRecurse(pstate, e);
		newe = coerce_to_jsonb(pstate, newe, "list element", true);

		newelems = lappend(newelems, newe);
	}

	newcl = makeNode(CypherListExpr);
	newcl->elems = newelems;
	newcl->location = cl->location;

	return (Node *) newcl;
}

static Node *
transformCypherListComp(ParseState *pstate, CypherListComp *clc)
{
	Node	   *list;
	Oid			type;
	Node	   *cond;
	Node	   *elem;
	CypherListCompExpr *clcexpr;

	list = transformCypherExprRecurse(pstate, (Node *) clc->list);
	type = exprType(list);
	if (type != JSONBOID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("jsonb is expected but %s", format_type_be(type)),
				 parser_errposition(pstate, clc->location)));

	pstate->p_lc_varname = clc->varname;
	cond = transformCypherWhere(pstate, clc->cond, EXPR_KIND_WHERE);
	pstate->p_lc_varname = NULL;

	pstate->p_lc_varname = clc->varname;
	elem = transformCypherExprRecurse(pstate, clc->elem);
	pstate->p_lc_varname = NULL;
	elem = coerce_to_jsonb(pstate, elem, "jsonb", true);

	clcexpr = makeNode(CypherListCompExpr);
	clcexpr->list = (Expr *) list;
	clcexpr->varname = pstrdup(clc->varname);
	clcexpr->cond = (Expr *) cond;
	clcexpr->elem = (Expr *) elem;
	clcexpr->location = clc->location;

	return (Node *) clcexpr;
}

static Node *
transformCaseExpr(ParseState *pstate, CaseExpr *c)
{
	Node	   *arg;
	CaseTestExpr *placeholder;
	ListCell   *lw;
	CaseWhen   *w;
	List	   *args = NIL;
	List	   *results = NIL;
	bool		is_jsonb = false;
	Node	   *rdefresult;
	Node	   *defresult;
	Oid			restype;
	CaseExpr   *newc;

	arg = transformCypherExprRecurse(pstate, (Node *) c->arg);
	if (arg == NULL)
	{
		placeholder = NULL;
	}
	else
	{
		assign_expr_collations(pstate, arg);

		placeholder = makeNode(CaseTestExpr);
		placeholder->typeId = exprType(arg);
		placeholder->typeMod = exprTypmod(arg);
		placeholder->collation = exprCollation(arg);
	}

	foreach(lw, c->args)
	{
		Node	   *rexpr;
		Node	   *expr;
		Node	   *result;
		CaseWhen   *neww;

		w = lfirst(lw);
		Assert(IsA(w, CaseWhen));

		rexpr = (Node *) w->expr;
		if (placeholder != NULL)
			rexpr = (Node *) makeSimpleA_Expr(AEXPR_OP, "=",
											  (Node *) placeholder, rexpr,
											  w->location);
		expr = transformCypherExprRecurse(pstate, rexpr);
		expr = coerce_to_boolean(pstate, expr, "CASE/WHEN");

		result = transformCypherExprRecurse(pstate, (Node *) w->result);
		if (exprType(result) == JSONBOID)
			is_jsonb = true;

		neww = makeNode(CaseWhen);
		neww->expr = (Expr *) expr;
		neww->result = (Expr *) result;
		neww->location = w->location;

		args = lappend(args, neww);
		results = lappend(results, result);
	}

	rdefresult = (Node *) c->defresult;
	if (rdefresult == NULL)
	{
		A_Const	   *n;

		n = makeNode(A_Const);
		n->val.type = T_Null;
		n->location = -1;
		rdefresult = (Node *) n;
	}
	defresult = transformCypherExprRecurse(pstate, rdefresult);
	if (exprType(defresult) == JSONBOID)
		is_jsonb = true;

	if (is_jsonb)
	{
		restype = JSONBOID;

		foreach(lw, args)
		{
			w = lfirst(lw);

			w->result = (Expr *) coerce_to_jsonb(pstate, (Node *) w->result,
												 "jsonb", true);
		}

		defresult = coerce_to_jsonb(pstate, defresult, "jsonb", true);
	}
	else
	{
		results = lcons(defresult, results);
		restype = select_common_type(pstate, results, "CASE", NULL);

		foreach(lw, args)
		{
			w = lfirst(lw);

			w->result = (Expr *) coerce_to_common_type(pstate,
													   (Node *) w->result,
													   restype, "CASE/WHEN");
		}

		defresult = coerce_to_common_type(pstate, defresult, restype,
										  "CASE/WHEN");
	}

	newc = makeNode(CaseExpr);
	newc->casetype = restype;
	newc->arg = (Expr *) arg;
	newc->args = args;
	newc->defresult = (Expr *) defresult;
	newc->location = c->location;

	return (Node *) newc;
}

static Node *
transformFuncCall(ParseState *pstate, FuncCall *fn)
{
	bool		is_num_agg = false;
	ListCell   *la;
	List	   *args = NIL;

	if (list_length(fn->funcname) == 1)
	{
		char	   *funcname;

		funcname = strVal(linitial(fn->funcname));

		if (strcmp(funcname, "avg") == 0 ||
			strcmp(funcname, "max") == 0 ||
			strcmp(funcname, "min") == 0 ||
			strcmp(funcname, "sum") == 0)
			is_num_agg = true;

		if (strcmp(funcname, "collect") == 0)
		{
			fn->funcname = list_make1(makeString("jsonb_agg"));
		}

		if (strcmp(funcname, "stdev") == 0)
		{
			fn->funcname = list_make1(makeString("stddev_samp"));
			is_num_agg = true;
		}

		if (strcmp(funcname, "stdevp") == 0)
		{
			fn->funcname = list_make1(makeString("stddev_pop"));
			is_num_agg = true;
		}
	}

	foreach(la, fn->args)
	{
		Node	   *arg;

		arg = transformCypherExprRecurse(pstate, lfirst(la));
		arg = wrapEdgeRefTypes(pstate, arg);
		if (is_num_agg)
		{
			Oid			argtype = exprType(arg);
			int			argloc = exprLocation(arg);
			Node	   *newarg;

			newarg = coerce_to_target_type(pstate, arg, argtype,
										   NUMERICOID, -1, COERCION_EXPLICIT,
										   COERCE_IMPLICIT_CAST, argloc);
			if (newarg == NULL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("%s cannot be converted to numeric",
								format_type_be(argtype)),
						 parser_errposition(pstate, argloc)));
				return NULL;
			}

			arg = newarg;
		}

		args = lappend(args, arg);
	}

	if (fn->agg_within_group)
	{
		Assert(fn->agg_order != NIL);

		foreach(la, fn->agg_order)
		{
			SortBy	   *arg = lfirst(la);

			args = lappend(args, transformCypherExpr(pstate, arg->node,
													 EXPR_KIND_ORDER_BY));
		}
	}

	return ParseFuncOrColumn(pstate, fn->funcname, args, fn, fn->location);
}

static Node *
transformCoalesceExpr(ParseState *pstate, CoalesceExpr *c)
{
	ListCell   *la;
	Node	   *arg;
	Node	   *newarg;
	List	   *newargs = NIL;
	bool		is_jsonb = false;
	Oid			coalescetype;
	List	   *newcoercedargs = NIL;
	CoalesceExpr *newc;

	foreach(la, c->args)
	{
		arg = lfirst(la);

		newarg = transformCypherExprRecurse(pstate, arg);
		newarg = wrapEdgeRefTypes(pstate, newarg);
		if (exprType(newarg) == JSONBOID)
			is_jsonb = true;

		newargs = lappend(newargs, newarg);
	}

	if (is_jsonb)
		coalescetype = JSONBOID;
	else
		coalescetype = select_common_type(pstate, newargs, "COALESCE", NULL);

	foreach(la, newargs)
	{
		arg = lfirst(la);

		if (is_jsonb)
			newarg = coerce_to_jsonb(pstate, arg, "jsonb", true);
		else
			newarg = coerce_to_common_type(pstate, arg, coalescetype,
										   "COALESCE");

		newcoercedargs = lappend(newcoercedargs, newarg);
	}

	newc = makeNode(CoalesceExpr);
	newc->coalescetype = coalescetype;
	newc->args = newcoercedargs;
	newc->location = c->location;

	return (Node *) newc;
}

static Node *
transformSubLink(ParseState *pstate, SubLink *sublink)
{
	Query	   *qry;
	const char *err;

	err = NULL;
	switch (pstate->p_expr_kind)
	{
		case EXPR_KIND_NONE:
			Assert(EXPR_KIND_NONE);
			break;
		case EXPR_KIND_OTHER:
			break;
		case EXPR_KIND_JOIN_ON:
		case EXPR_KIND_JOIN_USING:
		case EXPR_KIND_FROM_SUBSELECT:
		case EXPR_KIND_FROM_FUNCTION:
		case EXPR_KIND_WHERE:
		case EXPR_KIND_HAVING:
		case EXPR_KIND_FILTER:
		case EXPR_KIND_WINDOW_PARTITION:
		case EXPR_KIND_WINDOW_ORDER:
		case EXPR_KIND_WINDOW_FRAME_RANGE:
		case EXPR_KIND_WINDOW_FRAME_ROWS:
		case EXPR_KIND_SELECT_TARGET:
		case EXPR_KIND_INSERT_TARGET:
		case EXPR_KIND_UPDATE_SOURCE:
		case EXPR_KIND_UPDATE_TARGET:
		case EXPR_KIND_GROUP_BY:
		case EXPR_KIND_ORDER_BY:
		case EXPR_KIND_DISTINCT_ON:
		case EXPR_KIND_LIMIT:
		case EXPR_KIND_OFFSET:
		case EXPR_KIND_RETURNING:
		case EXPR_KIND_VALUES:
		case EXPR_KIND_POLICY:
			break;
		case EXPR_KIND_CHECK_CONSTRAINT:
		case EXPR_KIND_DOMAIN_CHECK:
			err = _("cannot use subquery in check constraint");
			break;
		case EXPR_KIND_COLUMN_DEFAULT:
		case EXPR_KIND_FUNCTION_DEFAULT:
			err = _("cannot use subquery in DEFAULT expression");
			break;
		case EXPR_KIND_INDEX_EXPRESSION:
			err = _("cannot use subquery in index expression");
			break;
		case EXPR_KIND_INDEX_PREDICATE:
			err = _("cannot use subquery in index predicate");
			break;
		case EXPR_KIND_ALTER_COL_TRANSFORM:
			err = _("cannot use subquery in transform expression");
			break;
		case EXPR_KIND_EXECUTE_PARAMETER:
			err = _("cannot use subquery in EXECUTE parameter");
			break;
		case EXPR_KIND_TRIGGER_WHEN:
			err = _("cannot use subquery in trigger WHEN condition");
			break;
	}
	if (err)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg_internal("%s", err),
				 parser_errposition(pstate, sublink->location)));

	pstate->p_hasSubLinks = true;

	qry = parse_sub_analyze(sublink->subselect, pstate, NULL, false);
	if (!IsA(qry, Query) ||
		qry->commandType != CMD_SELECT ||
		qry->utilityStmt != NULL)
		elog(ERROR, "unexpected non-SELECT command in SubLink");

	sublink->subselect = (Node *) qry;

	if (sublink->subLinkType == EXISTS_SUBLINK)
	{
		sublink->testexpr = NULL;
		sublink->operName = NIL;
	}
	else if (sublink->subLinkType == EXPR_SUBLINK)
	{
		if (count_nonjunk_tlist_entries(qry->targetList) != 1)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("subquery must return only one column"),
					 parser_errposition(pstate, sublink->location)));

		sublink->testexpr = NULL;
		sublink->operName = NIL;
	}
	else
	{
		elog(ERROR, "unexpected SubLinkType: %d", sublink->subLinkType);
	}

	return (Node *) sublink;
}

static Node *
transformIndirection(ParseState *pstate, Node *basenode, List *indirection)
{
	int			location = exprLocation(basenode);
	Node	   *res;
	Oid			restype;
	ListCell   *li;
	CypherAccessExpr *a;
	List	   *path = NIL;

	res = basenode;
	restype = exprType(res);

	/* record/composite or array type */
	foreach(li, indirection)
	{
		Node	   *i = lfirst(li);

		if (IsA(i, String))
		{
			if (restype == VERTEXOID || restype == EDGEOID ||
				restype == GRAPHPATHOID)
				break;
			if (!type_is_rowtype(restype))
				break;

			res = ParseFuncOrColumn(pstate, list_make1(i), list_make1(res),
									NULL, location);
			if (res == NULL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("column \"%s\" not found in data type %s",
								strVal(i), format_type_be(restype)),
						 parser_errposition(pstate, location)));
				return NULL;
			}
		}
		else
		{
			A_Indices  *ind;
			Node	   *lidx = NULL;
			Node	   *uidx = NULL;
			Oid			arrtype;
			int32		arrtypmod;
			Oid			elemtype;
			ArrayRef   *aref;

			Assert(IsA(i, A_Indices));

			if (!type_is_array_domain(restype))
				break;

			ind = (A_Indices *) i;

			if (ind->is_slice && ind->lidx != NULL)
				lidx = makeArrayIndex(pstate, ind->lidx, false);

			if (ind->uidx != NULL)
				uidx = makeArrayIndex(pstate, ind->uidx, ind->is_slice);

			arrtype = restype;
			arrtypmod = exprTypmod(res);
			elemtype = transformArrayType(&arrtype, &arrtypmod);

			aref = makeNode(ArrayRef);
			aref->refarraytype = arrtype;
			aref->refelemtype = elemtype;
			aref->reftypmod = arrtypmod;
			aref->refupperindexpr = list_make1(uidx);
			aref->reflowerindexpr = (ind->is_slice ? list_make1(lidx) : NIL);
			aref->refexpr = (Expr *) res;
			aref->refassgnexpr = NULL;

			res = (Node *) aref;
		}
		restype = exprType(res);
	}

	if (li == NULL)
		return res;

	res = filterAccessArg(pstate, res, location, "map or list");

	if (IsA(res, CypherAccessExpr))
	{
		a = (CypherAccessExpr *) res;

		res = (Node *) a->arg;
		path = a->path;
	}

	for_each_cell(li, li)
	{
		Node	   *i = lfirst(li);
		Node	   *elem;

		if (IsA(i, String))
		{
			elem = (Node *) makeConst(TEXTOID, -1, DEFAULT_COLLATION_OID, -1,
									  CStringGetTextDatum(strVal(i)),
									  false, false);
		}
		else
		{
			A_Indices  *ind;
			CypherIndices *cind;
			Node	   *lidx;
			Node	   *uidx;

			Assert(IsA(i, A_Indices));

			ind = (A_Indices *) i;

			/*
			 * ExecEvalCypherAccess() will handle lidx and uidx properly
			 * based on their types.
			 */

			lidx = transformCypherExprRecurse(pstate, ind->lidx);
			lidx = adjustListIndexType(pstate, lidx);
			uidx = transformCypherExprRecurse(pstate, ind->uidx);
			uidx = adjustListIndexType(pstate, uidx);

			cind = makeNode(CypherIndices);
			cind->is_slice = ind->is_slice;
			cind->lidx = lidx;
			cind->uidx = uidx;

			elem = (Node *) cind;
		}

		path = lappend(path, elem);
	}

	a = makeNode(CypherAccessExpr);
	a->arg = (Expr *) res;
	a->path = path;

	return (Node *) a;
}

static Node *
makeArrayIndex(ParseState *pstate, Node *idx, bool exclusive)
{
	Node	   *idxexpr;
	Node	   *result;
	Node	   *one;

	Assert(idx != NULL);

	idxexpr = transformCypherExprRecurse(pstate, idx);
	result = coerce_to_target_type(pstate, idxexpr, exprType(idxexpr),
								   INT4OID, -1, COERCION_ASSIGNMENT,
								   COERCE_IMPLICIT_CAST, -1);
	if (result == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("array subscript must have type integer"),
				 parser_errposition(pstate, exprLocation(idxexpr))));
		return NULL;
	}

	if (exclusive)
		return result;

	one = (Node *) makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
							 Int32GetDatum(1), false, true);

	result = (Node *) make_op(pstate, list_make1(makeString("+")),
							  result, one, -1);

	return result;
}

static Node *
adjustListIndexType(ParseState *pstate, Node *idx)
{
	if (idx == NULL)
		return NULL;

	switch (exprType(idx))
	{
		case TEXTOID:
		case BOOLOID:
		case JSONBOID:
			return idx;
		default:
			return coerce_to_jsonb(pstate, idx, "list index", true);
	}
}

static Node *
transformAExprOp(ParseState *pstate, A_Expr *a)
{
	Node	   *l;
	Node	   *r;

	l = transformCypherExprRecurse(pstate, a->lexpr);
	r = transformCypherExprRecurse(pstate, a->rexpr);

	if (a->kind == AEXPR_OP && list_length(a->name) == 1)
	{
		const char *opname = strVal(linitial(a->name));

		if (strcmp(opname, "`+`") == 0 ||
			strcmp(opname, "`-`") == 0 ||
			strcmp(opname, "`*`") == 0 ||
			strcmp(opname, "`/`") == 0 ||
			strcmp(opname, "`%`") == 0 ||
			strcmp(opname, "`^`") == 0)
		{
			l = coerce_to_jsonb(pstate, l, "jsonb", true);
			r = coerce_to_jsonb(pstate, r, "jsonb", true);
		}
	}

	return (Node *) make_op(pstate, a->name, l, r, a->location);
}

static Node *
transformAExprIn(ParseState *pstate, A_Expr *a)
{
	Node	   *l;
	Node	   *r;

	l = transformCypherExprRecurse(pstate, a->lexpr);
	l = coerce_to_jsonb(pstate, l, "jsonb", true);

	r = transformCypherExprRecurse(pstate, a->rexpr);
	r = coerce_to_jsonb(pstate, r, "jsonb", true);

	return (Node *) make_op(pstate, list_make1(makeString("@>")), r, l,
							a->location);
}

static Node *
transformBoolExpr(ParseState *pstate, BoolExpr *b)
{
	List	   *args = NIL;
	const char *opname;
	ListCell   *la;

	switch (b->boolop)
	{
		case AND_EXPR:
			opname = "AND";
			break;
		case OR_EXPR:
			opname = "OR";
			break;
		case NOT_EXPR:
			opname = "NOT";
			break;
		default:
			elog(ERROR, "unrecognized boolop: %d", (int) b->boolop);
			return NULL;
	}

	foreach(la, b->args)
	{
		Node	   *arg = lfirst(la);

		arg = transformCypherExprRecurse(pstate, arg);
		arg = coerce_to_boolean(pstate, arg, opname);

		args = lappend(args, arg);
	}

	return (Node *) makeBoolExpr(b->boolop, args, b->location);
}

static Node *
coerce_to_jsonb(ParseState *pstate, Node *expr, const char *targetname,
				bool err)
{
	if (expr == NULL)
		return NULL;

	switch (exprType(expr))
	{
		case UNKNOWNOID:
			elog(ERROR, "coercing UNKNOWNOID to JSONBOID cannot happen");
			return NULL;

		case GRAPHARRAYIDOID:
		case GRAPHIDOID:
		case VERTEXARRAYOID:
		case VERTEXOID:
		case EDGEARRAYOID:
		case EDGEOID:
		case GRAPHPATHARRAYOID:
		case GRAPHPATHOID:
		case EDGEREFARRAYOID:
		case EDGEREFOID:
			if (err)
			{
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("graph object cannot be %s", targetname),
						 parser_errposition(pstate, exprLocation(expr))));
				return NULL;
			}
			else
			{
				return expr;
			}

		case JSONBOID:
			return expr;

		default:
			return ParseFuncOrColumn(pstate,
									 list_make1(makeString("to_jsonb")),
									 list_make1(expr), NULL,
									 exprLocation(expr));
	}
}

Node *
transformCypherMapForSet(ParseState *pstate, Node *expr, List **pathelems,
						 char **varname)
{
	ParseExprKind sv_expr_kind;
	Node	   *aelem;
	List	   *apath;
	Node	   *origelem;
	Node	   *elem;
	Oid			elemtype;
	char	   *resname = NULL;
	ListCell   *le;
	List	   *textlist = NIL;

	sv_expr_kind = pstate->p_expr_kind;
	pstate->p_expr_kind = EXPR_KIND_UPDATE_SOURCE;

	expr = transformCypherExprRecurse(pstate, expr);

	pstate->p_expr_kind = sv_expr_kind;

	if (IsA(expr, CypherAccessExpr))
	{
		CypherAccessExpr *a = (CypherAccessExpr *) expr;

		aelem = (Node *) a->arg;
		apath = a->path;
	}
	else
	{
		aelem = expr;
		apath = NIL;
	}

	/* examine what aelem is */
	if (IsA(aelem, EdgeRefProp))
	{
		origelem = (Node *) ((EdgeRefProp *) aelem)->arg;
		elem = wrapEdgeRef(origelem);
	}
	else if (IsA(aelem, EdgeRefRow))
	{
		origelem = (Node *) ((EdgeRefRow *) aelem)->arg;
		elem = aelem;
	}
	else if (IsA(aelem, FieldSelect))
	{
		origelem = (Node *) ((FieldSelect *) aelem)->arg;
		elem = origelem;
	}
	else
	{
		origelem = aelem;
		elem = origelem;
	}

	elemtype = exprType(elem);
	if (elemtype != VERTEXOID && elemtype != EDGEOID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("vertex or edge is expected but %s",
						format_type_be(elemtype)),
				 parser_errposition(pstate, exprLocation(aelem))));
		return NULL;
	}

	if (IsA(origelem, Var))
	{
		Var		   *var = (Var *) origelem;

		if (var->varlevelsup == 0)
		{
			RangeTblEntry *rte;
			TargetEntry *te;

			rte = GetRTEByRangeTablePosn(pstate, var->varno, 0);
			Assert(rte->rtekind == RTE_SUBQUERY);

			te = list_nth(rte->subquery->targetList, var->varattno - 1);

			resname = pstrdup(te->resname);
		}
	}

	/* FIXME: This is because of the way eager is implemented. */
	if (resname == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Only direct variable reference is supported"),
				 parser_errposition(pstate, exprLocation(aelem))));
		return NULL;
	}

	if (apath == NIL)
	{
		*pathelems = NIL;
		*varname = resname;
		return elem;
	}

	foreach(le, apath)
	{
		Node	   *e = lfirst(le);

		if (IsA(e, Const))
		{
			Assert(exprType(e) == TEXTOID);

			textlist = lappend(textlist, e);
		}
		else
		{
			CypherIndices *cind = (CypherIndices *) e;
			Node	   *t;

			Assert(IsA(e, CypherIndices));

			if (cind->is_slice)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("list slicing on LHS of SET is invalid")));
				return NULL;
			}

			t = coerce_to_target_type(pstate, cind->uidx, exprType(cind->uidx),
									  TEXTOID, -1, COERCION_ASSIGNMENT,
									  COERCE_IMPLICIT_CAST, -1);
			if (t == NULL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("path element must be text"),
						 parser_errposition(pstate,
											exprLocation(cind->uidx))));
				return NULL;
			}

			textlist = lappend(textlist, t);
		}
	}

	*pathelems = textlist;
	*varname = resname;
	return elem;
}

/*
 * edgeref functions
 */

Node *
wrapEdgeRef(Node *node)
{
	EdgeRefRow *newnode;

	newnode = makeNode(EdgeRefRow);
	newnode->arg = (Expr *) node;

	return (Node *) newnode;
}

Node *
wrapEdgeRefArray(Node *node)
{
	EdgeRefRows *newnode;

	newnode = makeNode(EdgeRefRows);
	newnode->arg = (Expr *) node;

	return (Node *) newnode;
}

Node *
wrapEdgeRefTypes(ParseState *pstate, Node *node)
{
	if (!pstate->p_convert_edgeref)
		return node;

	switch (exprType(node))
	{
		case EDGEREFOID:
			return wrapEdgeRef(node);
		case EDGEREFARRAYOID:
			return wrapEdgeRefArray(node);
		default:
			return node;
	}
}

/*
 * clause functions
 */

Node *
transformCypherWhere(ParseState *pstate, Node *clause, ParseExprKind exprKind)
{
	Node	   *qual;

	if (clause == NULL)
		return NULL;

	qual = transformCypherExpr(pstate, clause, exprKind);

	qual = coerce_to_boolean(pstate, qual, "WHERE");

	return qual;
}

Node *
transformCypherLimit(ParseState *pstate, Node *clause,
					 ParseExprKind exprKind, const char *constructName)
{
	Node	   *qual;

	if (clause == NULL)
		return NULL;

	qual = transformCypherExpr(pstate, clause, exprKind);

	qual = coerce_to_specific_type(pstate, qual, INT8OID, constructName);

	/* LIMIT can't refer to any variables of the current query */
	if (contain_vars_of_level(qual, 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("argument of %s must not contain variables",
						constructName),
				 parser_errposition(pstate, locate_var_of_level(qual, 0))));
	}

	return qual;
}

List *
transformCypherOrderBy(ParseState *pstate, List *sortitems, List **targetlist)
{
	const ParseExprKind exprKind = EXPR_KIND_ORDER_BY;
	List	   *sortgroups = NIL;
	ListCell   *lsi;

	/* See findTargetlistEntrySQL99() */
	foreach(lsi, sortitems)
	{
		SortBy	   *sortby = lfirst(lsi);
		Node	   *expr;
		ListCell   *lt;
		TargetEntry *te = NULL;

		expr = transformCypherExpr(pstate, sortby->node, exprKind);

		foreach(lt, *targetlist)
		{
			TargetEntry *tmp;
			Node	   *texpr;

			tmp = lfirst(lt);
			texpr = strip_implicit_coercions((Node *) tmp->expr);
			if (equal(texpr, expr))
			{
				te = tmp;
				break;
			}
		}

		if (te == NULL)
		{
			te = transformTargetEntry(pstate, sortby->node, expr, exprKind,
									  NULL, true);

			*targetlist = lappend(*targetlist, te);
		}

		sortgroups = addTargetToSortList(pstate, te, sortgroups, *targetlist,
										 sortby, true);
	}

	return sortgroups;
}

/*
 * item list functions
 */

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

		if (IsA(item->val, ColumnRef))
		{
			ColumnRef  *cref = (ColumnRef *) item->val;

			/* item is a bare `*` */
			if (list_length(cref->fields) == 1 &&
				IsA(linitial(cref->fields), A_Star))
			{
				targets = list_concat(targets,
									  transformA_Star(pstate, cref->location));
				continue;
			}
		}

		expr = transformCypherExpr(pstate, item->val, exprKind);
		colname = (item->name == NULL ? FigureColname(item->val) : item->name);

		te = makeTargetEntry((Expr *) expr,
							 (AttrNumber) pstate->p_next_resno++,
							 colname, false);

		targets = lappend(targets, te);
	}

	return targets;
}

List *
transformCypherExprList(ParseState *pstate, List *exprlist,
						ParseExprKind exprKind)
{
	List	   *result = NIL;
	ListCell   *le;

	foreach(le, exprlist)
	{
		Node	   *expr = lfirst(le);

		result = lappend(result,
						 transformCypherExpr(pstate, expr, exprKind));
	}

	return result;
}

static List *
transformA_Star(ParseState *pstate, int location)
{
	List	   *targets = NIL;
	bool		visible = false;
	ListCell   *lni;

	foreach(lni, pstate->p_namespace)
	{
		ParseNamespaceItem *nsitem = lfirst(lni);
		RangeTblEntry *rte = nsitem->p_rte;
		int			rtindex;

		/* ignore RTEs that are inaccessible by unqualified names */
		if (!nsitem->p_cols_visible)
			continue;
		visible = true;

		/* should not have any lateral-only items when parsing items */
		Assert(!nsitem->p_lateral_only);

		rtindex = RTERangeTablePosn(pstate, rte, NULL);

		targets = list_concat(targets,
							  expandRelAttrs(pstate, rte, rtindex, 0,
											 location));
	}

	if (!visible)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("RETURN * with no accessible variables is invalid"),
				 parser_errposition(pstate, location)));

	return targets;
}

void
wrapEdgeRefTargetList(ParseState *pstate, List *targetList)
{
	ListCell   *lt;

	foreach(lt, targetList)
	{
		TargetEntry *te = lfirst(lt);

		te->expr = (Expr *) wrapEdgeRefTypes(pstate, (Node *) te->expr);
	}
}

void
unwrapEdgeRefTargetList(List *targetList)
{
	ListCell   *lt;

	foreach(lt, targetList)
	{
		TargetEntry *te = lfirst(lt);

		if (te->ressortgroupref != 0)
			continue;

		switch (nodeTag(te->expr))
		{
			case T_EdgeRefRow:
				te->expr = ((EdgeRefRow *) te->expr)->arg;
				break;
			case T_EdgeRefRows:
				te->expr = ((EdgeRefRows *) te->expr)->arg;
				break;
			default:
				break;
		}
	}
}
