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
#include "nodes/subscripting.h"
#include "optimizer/tlist.h"
#include "parser/analyze.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_cypher_expr.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_graph.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "parser/parse_cypher_utils.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/jsonb.h"
#include "optimizer/optimizer.h"

static Node *transformCypherExprRecurse(ParseState *pstate, Node *expr);
static Node *transformColumnRef(ParseState *pstate, ColumnRef *cref);
static Node *transformListCompColumnRef(ParseState *pstate, ColumnRef *cref,
										char *varname);
static Node *scanNSItemForVar(ParseState *pstate, ParseNamespaceItem *nsitem,
							  char *colname, int location);
static Node *transformFields(ParseState *pstate, Node *basenode, List *fields,
							 int location);
static Node *filterAccessArg(ParseState *pstate, Node *expr, int location,
							 const char *types);
static Node *transformParamRef(ParseState *pstate, ParamRef *pref);
static Node *transformTypeCast(ParseState *pstate, TypeCast *tc);
static Node *transformCypherMapExpr(ParseState *pstate, CypherMapExpr *m);
static Node *transformCypherListExpr(ParseState *pstate, CypherListExpr *cl);
static Node *transformCypherListComp(ParseState *pstate, CypherListComp *clc);
static Node *transformCaseExpr(ParseState *pstate, CaseExpr *c);
static Node *transformFuncCall(ParseState *pstate, FuncCall *fn);
static List *preprocess_func_args(ParseState *pstate, FuncCall *fn);
static FuncCandidateList func_get_best_candidate(ParseState *pstate,
												 FuncCall *fn, int nargs,
												 Oid argtypes[FUNC_MAX_ARGS]);
static int	func_match_argtypes_jsonb(int nargs, Oid argtypes[FUNC_MAX_ARGS],
									  FuncCandidateList raw_candidates,
									  FuncCandidateList *candidates);
static FuncCandidateList func_select_candidate_jsonb(int nargs,
													 Oid argtypes[FUNC_MAX_ARGS],
													 FuncCandidateList candidates);
static int	cypher_match_function(int matchDepth, int nargs,
								  Oid *input_base_typeids,
								  TYPCATEGORY *slot_category,
								  FuncCandidateList *candidates);
static bool cypher_match_function_criteria(int matchDepth, Oid inputBaseType,
										   Oid currentType,
										   TYPCATEGORY slotCategory);
static List *func_get_best_args(ParseState *pstate, List *args,
								Oid argtypes[FUNC_MAX_ARGS],
								FuncCandidateList candidate);
static Node *transformCoalesceExpr(ParseState *pstate, CoalesceExpr *c);
static Node *transformIndirection(ParseState *pstate, A_Indirection *indir);
static Node *makeArrayIndex(ParseState *pstate, Node *idx, Node *arr, bool exclusive);
static Node *adjustListIndexType(ParseState *pstate, Node *idx);
static Node *transformAExprOp(ParseState *pstate, A_Expr *a);
static Node *transformAExprIn(ParseState *pstate, A_Expr *a);
static Node *transformCypherNullIf(ParseState *pstate, A_Expr *a);
static Node *transformBoolExpr(ParseState *pstate, BoolExpr *b);
static Node *coerce_unknown_const(ParseState *pstate, Node *expr, Oid ityp,
								  Oid otyp);
static Datum stringToJsonb(ParseState *pstate, char *s, int location);
static Node *coerce_to_jsonb(ParseState *pstate, Node *expr,
							 const char *targetname);
static bool is_graph_type(Oid type);
static Node *coerce_all_to_jsonb(ParseState *pstate, Node *expr);

static List *transformA_Star(ParseState *pstate, int location);
static Node *build_cypher_cast_expr(Node *expr, Oid otyp, int32 otypmod,
									CoercionContext cctx,
									CoercionForm cform, int loc);
static Node *coerce_cypher_arg_to_boolean(ParseState *pstate, Node *node,
										  const char *constructName);

/* GUC variable (allow/disallow null properties) */
bool		allow_null_properties = false;

Node *
transformCypherExpr(ParseState *pstate, Node *expr, ParseExprKind exprKind)
{
	Node	   *result;
	ParseExprKind sv_expr_kind;

	Assert(exprKind != EXPR_KIND_NONE);
	sv_expr_kind = pstate->p_expr_kind;
	pstate->p_expr_kind = exprKind;

	result = transformCypherExprRecurse(pstate, expr);

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
			return (Node *) make_const(pstate, (A_Const *) expr);
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
			{
				SubLink    *sublink = (SubLink *) expr;
				CypherGenericExpr *cexpr;

				cexpr = makeNode(CypherGenericExpr);
				cexpr->expr = sublink->testexpr;

				sublink->testexpr = (Node *) cexpr;

				return transformExpr(pstate, expr, pstate->p_expr_kind);
			}
		case T_A_Indirection:
			return transformIndirection(pstate, (A_Indirection *) expr);
		case T_A_Expr:
			{
				A_Expr	   *a = (A_Expr *) expr;

				switch (a->kind)
				{
					case AEXPR_OP:
						return transformAExprOp(pstate, a);
					case AEXPR_IN:
						return transformAExprIn(pstate, a);
					case AEXPR_NULLIF:
						return transformCypherNullIf(pstate, a);
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
		case T_Const:
			/* Already transformed */
			return expr;
		case T_SQLValueFunction:
			return transformExpr(pstate, expr, pstate->p_expr_kind);
		default:
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(expr));
			return NULL;
	}
}

/*
 * Because we have to check all the cases of variable references (Cypher
 * variables and variables in ancestor queries) for each level of ParseState,
 * logic and functions in the original transformColumnRef() are properly
 * modified, refactored, and then integrated into one.
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
	int			nmatched = 0;

	/*
	 * The iterator variable used in a list comprehension expression always
	 * hides other variables with the same name.
	 */
	if (pstate->p_lc_varname != NULL)
	{
		node = transformListCompColumnRef(pstate, cref, pstate->p_lc_varname);
		if (node != NULL)
			return node;
	}

	if (pstate->p_pre_columnref_hook != NULL)
	{
		node = (*pstate->p_pre_columnref_hook) (pstate, cref);
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

		/*
		 * Find the Var at the current level of ParseState in a greedy
		 * fashion, i.e., match as many identifiers as possible. This means
		 * that, when a qualified name is given, resolving it as Var has
		 * higher priority than resolving it as property access.
		 *
		 * For example, when `x.y` is given, and if `x.y` can be resolved as
		 * <Column x, Property y> and <RTE x, Column y>, it will be resolved
		 * as the later.
		 *
		 * This is because, in the above case, there is no way to refer to
		 * <Column y> if it stops to resolve the name when <Column x> is
		 * found. However, by using `x['y']`, <Property y> is still
		 * accessible.
		 */

		foreach(lni, pstate_up->p_namespace)
		{
			ParseNamespaceItem *nsitem = lfirst(lni);
			RangeTblEntry *rte = nsitem->p_rte;

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
			 * If this RTE is accessible by unqualified names, examine
			 * `field1`.
			 */
			if (nsitem->p_cols_visible)
			{
				node = scanNSItemForVar(pstate, nsitem, strVal(field1),
										location);
				nmatched = 1;
			}

			/*
			 * If this RTE is inaccessible by qualified names, skip the rest.
			 */
			if (!nsitem->p_rel_visible)
				continue;

			/* examine `field1.field2` */
			if (field2 != NULL &&
				strcmp(rte->eref->aliasname, strVal(field1)) == 0)
			{
				node = scanNSItemForVar(pstate, nsitem, strVal(field2),
										location);
				nmatched = 2;
			}

			/* examine `field1.field2.field3` */
			if (OidIsValid(nspid1))
			{
				Oid			relid2 = get_relname_relid(strVal(field2), nspid1);

				if (OidIsValid(relid2) &&
					rte->rtekind == RTE_RELATION &&
					rte->relid == relid2 &&
					rte->alias == NULL)
				{

					node = scanNSItemForVar(pstate, nsitem, strVal(field3),
											location);
					nmatched = 3;
				}
			}

			/* examine `field1.field2.field3.field4` */
			if (OidIsValid(nspid2))
			{
				Oid			relid3 = get_relname_relid(strVal(field3), nspid2);

				if (OidIsValid(relid3) &&
					rte->rtekind == RTE_RELATION &&
					rte->relid == relid3 &&
					rte->alias == NULL)
				{
					node = scanNSItemForVar(pstate, nsitem,
											strVal(field4), location);
					nmatched = 4;
				}
			}

			if (node != NULL)
				break;
		}

		if (node != NULL)
			break;

		pstate_up = pstate_up->parentParseState;
	}

	if (pstate->p_post_columnref_hook != NULL)
	{
		Node	   *hookresult;

		hookresult = (*pstate->p_post_columnref_hook) (pstate, cref, node);
		if (node == NULL)
		{
			node = hookresult;
			nmatched = nfields;
		}
		else if (hookresult != NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_AMBIGUOUS_COLUMN),
					 errmsg("column reference \"%s\" is ambiguous",
							NameListToString(cref->fields)),
					 parser_errposition(pstate, cref->location)));
			return NULL;
		}
	}

	if (node == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("variable does not exist"),
				 parser_errposition(pstate, location)));
		return NULL;
	}

	/* use remaining fields of the given name as keys for property access */
	if (nmatched < nfields)
	{
		List	   *newfields;

		newfields = list_copy_tail(cref->fields, nmatched);

		return transformFields(pstate, node, newfields, location);
	}

	return node;
}

static Node *
transformListCompColumnRef(ParseState *pstate, ColumnRef *cref, char *varname)
{
	Node	   *field1 = linitial(cref->fields);
	CypherListCompVar *clcvar;

	/*
	 * For PROPERTY INDEX, removes the access "properties" in ColumnRef
	 * because it is granted with "properties" by default.
	 *
	 * i.e, properties->A => A
	 */
	if (pstate->p_expr_kind == EXPR_KIND_INDEX_EXPRESSION &&
		list_length(cref->fields) == 2)
	{
		field1 = llast(cref->fields);
	}

	if (strcmp(varname, strVal(field1)) != 0)
	{
		return NULL;
	}
	else if (pstate->p_expr_kind == EXPR_KIND_INDEX_EXPRESSION)
	{
		cref->fields = list_make1(varname);
	}

	clcvar = makeNode(CypherListCompVar);
	clcvar->varname = pstrdup(varname);
	clcvar->elem_type = pstate->p_lc_elem_type;
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
scanNSItemForVar(ParseState *pstate, ParseNamespaceItem *nsitem, char *colname,
				 int location)
{
	RangeTblEntry *rte = nsitem->p_rte;
	int			attrno;
	ListCell   *lc;
	Var		   *var = NULL;

	attrno = 0;
	foreach(lc, rte->eref->colnames)
	{
		const char *colalias = strVal(lfirst(lc));

		attrno++;

		if (strcmp(colalias, colname) != 0)
			continue;

		var = make_var(pstate, nsitem, attrno, location);
		markVarForSelectPriv(pstate, (Var *) var);
	}

	return (Node *) var;
}

static Node *
transformFields(ParseState *pstate, Node *basenode, List *fields, int location)
{
	Node	   *res;
	Oid			restype;
	ListCell   *lf;
	Node	   *field;
	List	   *path = NIL;

	res = basenode;
	restype = exprType(res);

	/* record/composite type */
	foreach(lf, fields)
	{
		/*
		 * Redirect access to the attributes of `vertex`/`edge` type to
		 * `properties` attribute of them by coercing them to `jsonb` later in
		 * this function.
		 */
		if (restype == VERTEXOID || restype == EDGEOID)
			break;

		/*
		 * Prevent access to the attributes of `graphpath` type directly.
		 * Instead, users may call `nodes()` (or `vertices()`) and
		 * `relationships()` (or `edges()`) to get them.
		 */
		if (restype == GRAPHPATHOID)
			break;

		if (!type_is_rowtype(restype))
			break;

		field = lfirst(lf);

		res = ParseFuncOrColumn(pstate, list_make1(field), list_make1(res),
								pstate->p_last_srf, NULL, false, location);
		if (res == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("attribute \"%s\" not found in type %s",
							strVal(field), format_type_be(restype)),
					 parser_errposition(pstate, location)));
			return NULL;
		}
		restype = exprType(res);
	}

	if (lf == NULL)
		return res;

	/*
	 * Resolve a single-key property access on a graph element (e.g. n.age) to
	 * its promoted typed column when the label has one.  The typed column is the
	 * source of truth, but every use is emitted as cypher_to_jsonb(column): a
	 * projection then returns the typed value (and a column-only query becomes
	 * index-only), and every other use -- WHERE, ORDER BY, function args -- is
	 * byte-identical to the projection, which is what lets DISTINCT / GROUP BY
	 * accept an ORDER BY key over the same property.  The index is not lost:
	 * a comparison is re-bound to the native operator in transformAExprOp when
	 * the other operand is type-compatible, and a free (non-DISTINCT, non-
	 * grouped) ORDER BY is re-pointed to the native column in
	 * updateSortOperatorsForJsonb.  A whole-element reference (RETURN n,
	 * properties(n), ...) is not a field access and is unaffected.
	 */
	if ((restype == VERTEXOID || restype == EDGEOID) &&
		list_length(fields) == 1 &&
		IsA(linitial(fields), String))
	{
		Node	   *promoted;

		promoted = resolvePromotedProperty(pstate, res,
										   strVal(linitial(fields)), location);
		if (promoted != NULL)
			return (Node *) makeFuncExpr(F_CYPHER_TO_JSONB, JSONBOID,
										 list_make1(promoted), InvalidOid,
										 InvalidOid, COERCE_EXPLICIT_CALL);
	}

	res = filterAccessArg(pstate, res, location, "map");

	for_each_cell(lf, fields, lf)
	{
		Node	   *elem;

		field = lfirst(lf);

		elem = (Node *) makeConst(TEXTOID, -1, DEFAULT_COLLATION_OID, -1,
								  CStringGetTextDatum(strVal(field)),
								  false, false);

		path = lappend(path, elem);
	}

	return makeJsonbFuncAccessor(pstate, res, path);
}

static Node *
filterAccessArg(ParseState *pstate, Node *expr, int location,
				const char *types)
{
	Oid			exprtype = exprType(expr);

	switch (exprtype)
	{
		case VERTEXOID:
		case EDGEOID:
			return ParseFuncOrColumn(pstate,
									 list_make1(makeString(AG_ELEM_PROP_MAP)),
									 list_make1(expr), pstate->p_last_srf,
									 NULL, false, location);
		case JSONBOID:
			return expr;

		case JSONOID:
			expr = coerce_expr(pstate, expr, JSONOID, JSONBOID, -1,
							   COERCION_EXPLICIT, COERCE_IMPLICIT_CAST,
							   location);
			Assert(expr != NULL);
			return expr;

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
		result = (*pstate->p_paramref_hook) (pstate, pref);
	else
		result = NULL;

	if (result == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_PARAMETER),
				 errmsg("there is no parameter $%d", pref->number),
				 parser_errposition(pstate, pref->location)));

	return result;
}

static Node *
transformTypeCast(ParseState *pstate, TypeCast *tc)
{
	Oid			otyp;
	int32		otypmod;
	Node	   *expr;
	Oid			ityp;
	int			loc;
	Node	   *node;

	typenameTypeIdAndMod(pstate, tc->typeName, &otyp, &otypmod);

	expr = transformCypherExprRecurse(pstate, tc->arg);

	ityp = exprType(expr);
	if (ityp == InvalidOid)
		return expr;

	loc = tc->location;
	if (loc < 0)
		loc = tc->typeName->location;

	if (otyp != JSONBOID &&
		TypeCategory(getBaseType(otyp)) == TYPCATEGORY_USER &&
		can_coerce_type(1, &ityp, &otyp, COERCION_EXPLICIT))
	{
		node = coerce_to_target_type(pstate, expr, ityp, otyp, otypmod,
									 COERCION_EXPLICIT, COERCE_EXPLICIT_CAST,
									 loc);
	}
	else
		node = coerce_expr(pstate, expr, ityp, otyp, otypmod,
						   COERCION_EXPLICIT, COERCE_EXPLICIT_CAST, loc);
	if (node == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_CANNOT_COERCE),
				 errmsg("cannot cast type %s to %s",
						format_type_be(ityp),
						format_type_be(otyp)),
				 parser_coercion_errposition(pstate, loc, expr)));

	return node;
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
		le = lnext(m->keyvals, le);
		v = lfirst(le);
		le = lnext(m->keyvals, le);

		newv = transformCypherExprRecurse(pstate, v);
		newv = coerce_to_jsonb(pstate, newv, "property value");

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
		newe = coerce_to_jsonb(pstate, newe, "list element");

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
	char	   *save_varname;
	Oid			save_elem_type;
	Node	   *cond;
	Node	   *elem;
	CypherListCompExpr *clcexpr;
	bool		save_hasAggs;
	bool		save_hasWindowFuncs;

	list = transformCypherExprRecurse(pstate, (Node *) clc->list);
	type = exprType(list);

	/*
	 * Determine the element type from the list's array type. This is used for
	 * type checking of the iteration variable in LC.
	 */
	switch (type)
	{
		case JSONBOID:
			pstate->p_lc_elem_type = JSONBOID;
			break;
		case VERTEXARRAYOID:
			pstate->p_lc_elem_type = VERTEXOID;
			break;
		case EDGEARRAYOID:
			pstate->p_lc_elem_type = EDGEOID;
			break;
		default:
			list = coerce_all_to_jsonb(pstate, list);
			pstate->p_lc_elem_type = JSONBOID;
			break;
	}

	save_varname = pstate->p_lc_varname;
	save_elem_type = pstate->p_lc_elem_type;
	pstate->p_lc_varname = clc->varname;

	/*
	 * A list comprehension is a per-element projection, not a grouping context,
	 * so an aggregate or window function in its filter or projection has no
	 * group to evaluate over -- and the per-element evaluator cannot run one
	 * (it crashes the backend).  Detect any that the filter/projection adds and
	 * reject it with a clear error, mirroring Cypher semantics.  Save and clear
	 * the flags around the transform so we observe only what these clauses add.
	 */
	save_hasAggs = pstate->p_hasAggs;
	save_hasWindowFuncs = pstate->p_hasWindowFuncs;
	pstate->p_hasAggs = false;
	pstate->p_hasWindowFuncs = false;

	cond = transformCypherWhere(pstate, clc->cond, pstate->p_expr_kind);
	elem = transformCypherExprRecurse(pstate, clc->elem);

	if (pstate->p_hasAggs)
		ereport(ERROR,
				(errcode(ERRCODE_GROUPING_ERROR),
				 errmsg("aggregate functions are not allowed in a list comprehension"),
				 parser_errposition(pstate, clc->location)));
	if (pstate->p_hasWindowFuncs)
		ereport(ERROR,
				(errcode(ERRCODE_WINDOWING_ERROR),
				 errmsg("window functions are not allowed in a list comprehension"),
				 parser_errposition(pstate, clc->location)));

	pstate->p_hasAggs = save_hasAggs;
	pstate->p_hasWindowFuncs = save_hasWindowFuncs;

	/*
	 * If elem is NULL, implicitly use the iteration variable.
	 */
	if (elem == NULL && clc->varname != NULL)
	{
		CypherListCompVar *clcvar = makeNode(CypherListCompVar);

		clcvar->varname = pstrdup(clc->varname);
		clcvar->elem_type = pstate->p_lc_elem_type;
		clcvar->location = clc->location;
		elem = (Node *) clcvar;
	}

	pstate->p_lc_varname = save_varname;
	pstate->p_lc_elem_type = save_elem_type;

	/*
	 * Determine if coercion to JSONB is needed for the result expression. If
	 * it is identity case (the iteration variable itself) or a graph object
	 * (vertex/edge), no coercion is needed. Otherwise, coerce it to JSONB.
	 */
	if (elem != NULL)
	{
		bool		is_identity_elem;
		bool		is_graph_object_result;
		Oid			elem_result_type;

		/* Check if this is the identity case */
		is_identity_elem = (cond == NULL) &&
			(IsA(elem, CypherListCompVar));

		/* Check if elem returns a graph object (vertex/edge) */
		elem_result_type = exprType(elem);
		is_graph_object_result = (elem_result_type == VERTEXOID ||
								  elem_result_type == EDGEOID);

		if (!is_identity_elem && !is_graph_object_result)
		{
			elem = coerce_to_jsonb(pstate, elem,
								   "list comprehension result");
		}
	}

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
		expr = coerce_cypher_arg_to_boolean(pstate, expr, "CASE/WHEN");

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
		A_Const    *n;

		n = makeNode(A_Const);
		n->isnull = true;
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
												 "CASE/WHEN value");
		}

		defresult = coerce_to_jsonb(pstate, defresult, "CASE/WHEN value");
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
	Node	   *last_srf = pstate->p_last_srf;
	List	   *args;

	if (list_length(fn->funcname) == 1)
	{
		char	   *funcname;

		funcname = strVal(linitial(fn->funcname));

		/*
		 * todo: Later, the sin function will be defined as the basic function
		 * of PG.
		 */

		if (strcmp(funcname, "stdev") == 0)
			fn->funcname = list_make1(makeString("stddev_samp"));
		else if (strcmp(funcname, "stdevp") == 0)
			fn->funcname = list_make1(makeString("stddev_pop"));
		/* translate log() into ln() for cypher queries */
		else if (strcmp(funcname, "log") == 0)
			fn->funcname = list_make1(makeString("ln"));
	}

	args = preprocess_func_args(pstate, fn);

	/*
	 * count(v) / count(DISTINCT v) over a whole graph element only needs the
	 * element's identity.  Cypher equality on vertices and edges is defined on
	 * the graphid alone, and an element's id is NULL exactly when the element
	 * itself is NULL, so counting -- and, under DISTINCT, sorting -- the graphid
	 * is identical to doing so over the full ROW(id, properties, ...) composite,
	 * but without dragging the property jsonb through the aggregate or its sort.
	 * Replace a lone vertex/edge argument to count() with its graphid.
	 */
	if (list_length(fn->funcname) == 1 && list_length(args) == 1 &&
		strcmp(strVal(linitial(fn->funcname)), "count") == 0)
	{
		Node	   *arg = (Node *) linitial(args);
		Oid			argtype = exprType(arg);

		if (argtype == VERTEXOID || argtype == EDGEOID)
			args = list_make1(getExprField((Expr *) arg, AG_ELEM_ID));
	}

	return ParseFuncOrColumn(pstate, fn->funcname, args, last_srf, fn,
							 false, fn->location);
}

/*
 * Assume jsonb arguments are property values and coerce them to the actual
 * argument types of candidate functions.
 */
static List *
preprocess_func_args(ParseState *pstate, FuncCall *fn)
{
	bool		isSubstr = false;
	int			nargs;
	bool		named;
	ListCell   *la;
	List	   *args = NIL;
	Oid			argtype;
	Oid			argtypes[FUNC_MAX_ARGS];
	FuncCandidateList candidate;

	if (list_length(fn->args) > FUNC_MAX_ARGS)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_ARGUMENTS),
				 errmsg("cannot pass more than %d arguments to a function",
						FUNC_MAX_ARGS),
				 parser_errposition(pstate, fn->location)));

	Assert(fn->agg_filter == NULL && !fn->agg_within_group);

	/* test if this is the substring function */
	if (strcmp(strVal(llast(fn->funcname)), "substring") == 0)
		isSubstr = true;

	nargs = 0;
	named = false;
	foreach(la, fn->args)
	{
		Node	   *arg;

		/*
		 * === If this is second argument of the substring function, adjust it
		 * by adding 1 to it because
		 *
		 * 1) Cypher substring uses 0-based index. 2) text version of
		 * substring uses 1-based index. 3) we will always use text version of
		 * substring function.
		 *
		 * NOTE: Remove this code when we define a rule for selecting a
		 * function with given arguments.
		 */
		if (isSubstr && nargs == 1)
		{
			A_Const    *aconst = makeNode(A_Const);

			/* constant value 1 */
			aconst->val.ival.type = T_Integer;
			aconst->val.ival.ival = 1;
			aconst->location = -1;

			/* replace the second argument with `second_arg + 1` */
			la->ptr_value = makeSimpleA_Expr(AEXPR_OP, "+", lfirst(la),
											 (Node *) aconst, -1);
		}

		arg = transformCypherExprRecurse(pstate, lfirst(la));
		if (IsA(arg, NamedArgExpr))
			named = true;

		argtype = exprType(arg);

		/*
		 * collect() takes anyelement, so an unknown-typed literal argument (a
		 * bare NULL or an unadorned string literal) leaves the polymorphic type
		 * unresolvable and errors.  Coerce it to text -- Postgres's default
		 * resolution for an unknown literal -- so collect(NULL) yields [] and
		 * collect('x') yields ["x"] instead of failing.
		 */
		if (argtype == UNKNOWNOID &&
			strcmp(strVal(llast(fn->funcname)), "collect") == 0)
		{
			arg = coerce_to_specific_type(pstate, arg, TEXTOID, "collect");
			argtype = exprType(arg);
		}

		Assert(!(IsA(arg, Param) && argtype == VOIDOID));

		args = lappend(args, arg);
		argtypes[nargs++] = argtype;
	}

	/* This function does not handle named arguments. */
	if (named)
		return args;

	/* unnest() takes its argument as-is, not as a column projection */
	if (strcmp(strVal(llast(fn->funcname)), "unnest") == 0)
		return args;

	/* column projection */
	if (nargs == 1 &&
		fn->agg_order == NIL &&
		!fn->agg_star &&
		!fn->agg_distinct &&
		fn->over == NULL &&
		!fn->func_variadic &&
		list_length(fn->funcname) == 1)
	{
		argtype = argtypes[0];
		if (argtype == RECORDOID || ISCOMPLEX(argtype))
			return args;
	}

	candidate = func_get_best_candidate(pstate, fn, nargs, argtypes);
	if (candidate == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("function %s does not exist",
						func_signature_string(fn->funcname, nargs, NIL,
											  argtypes)),
				 errhint("No function matches the given name and argument types. "
						 "You might need to add explicit type casts."),
				 parser_errposition(pstate, fn->location)));
		return NIL;
	}

	return func_get_best_args(pstate, args, argtypes, candidate);
}

static FuncCandidateList
func_get_best_candidate(ParseState *pstate, FuncCall *fn, int nargs,
						Oid argtypes[FUNC_MAX_ARGS])
{
	FuncCandidateList raw_candidates;
	FuncCandidateList best_candidate;
	FuncCandidateList current_candidates;
	FuncCandidateList previous_candidate;
	int			ncandidates;

	/* get all candidates */
	raw_candidates = FuncnameGetCandidates(fn->funcname, nargs, NIL,
										   true, false, false, false);

	/*
	 * Added to remove jsonb version of substring (jsonb_substr*) from the
	 * list of candidates if the function is substring.
	 *
	 * NOTE: Remove this code when we define a rule for selecting a function
	 * with given arguments.
	 */
	if (strcmp(strVal(llast(fn->funcname)), "substring") == 0)
	{
		for (best_candidate = raw_candidates, previous_candidate = NULL;
			 best_candidate != NULL;
			 best_candidate = best_candidate->next)
		{
			/* if this is a jsonb substring function OID */
			if (best_candidate->oid == 7235 || best_candidate->oid == 7236)
			{
				/* we are at the head of the list */
				if (best_candidate == raw_candidates)
					raw_candidates = best_candidate->next;
				else
					previous_candidate->next = best_candidate->next;
			}
			else
			{
				previous_candidate = best_candidate;
			}
		}
	}

	/* find exact match */
	for (best_candidate = raw_candidates;
		 best_candidate != NULL;
		 best_candidate = best_candidate->next)
	{
		if (memcmp(argtypes, best_candidate->args, nargs * sizeof(Oid)) == 0)
			break;
	}
	if (best_candidate != NULL)
		return best_candidate;

	ncandidates = func_match_argtypes_jsonb(nargs, argtypes, raw_candidates,
											&current_candidates);
	if (ncandidates == 1)
	{
		best_candidate = current_candidates;
	}
	else if (ncandidates > 1)
	{
		best_candidate = func_select_candidate_jsonb(nargs, argtypes,
													 current_candidates);
		if (best_candidate == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_AMBIGUOUS_FUNCTION),
					 errmsg("function %s is not unique",
							func_signature_string(fn->funcname, nargs, NIL,
												  argtypes)),
					 errhint("Could not choose a best candidate function. "
							 "You might need to add explicit type casts."),
					 parser_errposition(pstate, fn->location)));
			return NULL;
		}
	}

	return best_candidate;
}

static int
func_match_argtypes_jsonb(int nargs, Oid argtypes[FUNC_MAX_ARGS],
						  FuncCandidateList raw_candidates,
						  FuncCandidateList *candidates)
{
	FuncCandidateList current_candidate;
	FuncCandidateList next_candidate;
	int			ncandidates = 0;

	*candidates = NULL;

	for (current_candidate = raw_candidates;
		 current_candidate != NULL;
		 current_candidate = next_candidate)
	{
		int			i;

		next_candidate = current_candidate->next;

		for (i = 0; i < nargs; i++)
		{
			/* jsonb can be coerced to any type by calling coerce_expr() */
			if (argtypes[i] == JSONBOID)
				continue;

			/* any type can be coerced to jsonb */
			if (current_candidate->args[i] == JSONBOID)
				continue;

			if (can_coerce_type(1, &argtypes[i], &current_candidate->args[i],
								COERCION_IMPLICIT))
				continue;

			break;
		}
		if (i < nargs)
			continue;

		current_candidate->next = *candidates;
		*candidates = current_candidate;
		ncandidates++;

		if (current_candidate->oid == F_HEAD_ANYARRAY ||
			current_candidate->oid == F_LAST_ANYARRAY ||
			current_candidate->oid == F_TAIL_ANYARRAY)
			return 1;
	}

	return ncandidates;
}

/* see func_select_candidate() */
FuncCandidateList
func_select_candidate_jsonb(int nargs, Oid argtypes[FUNC_MAX_ARGS],
							FuncCandidateList candidates)
{
	int			nunknowns;
	int			i;
	Oid			input_base_typeids[FUNC_MAX_ARGS];
	int			matchDepth = 0;
	int			ncandidates;
	FuncCandidateList current_candidate;
	FuncCandidateList last_candidate;
	Oid		   *current_typeids;
	TYPCATEGORY slot_category[FUNC_MAX_ARGS];
	bool		resolved_unknowns;
	bool		slot_has_preferred_type[FUNC_MAX_ARGS];
	TYPCATEGORY current_category;
	bool		current_is_preferred;
	FuncCandidateList first_candidate;

	nunknowns = 0;
	for (i = 0; i < nargs; i++)
	{
		if (argtypes[i] != UNKNOWNOID)
		{
			input_base_typeids[i] = getBaseType(argtypes[i]);
		}
		else
		{
			input_base_typeids[i] = UNKNOWNOID;
			nunknowns++;
		}
	}

	for (i = 0; i < nargs; i++)
		slot_category[i] = TypeCategory(input_base_typeids[i]);

	/* find our candidates */
	for (matchDepth = 1; matchDepth <= 6; matchDepth++)
	{
		ncandidates = cypher_match_function(matchDepth, nargs,
											input_base_typeids, slot_category,
											&candidates);
		if (ncandidates == 1)
			return candidates;
	}

	if (nunknowns == 0)
		return NULL;

	resolved_unknowns = false;
	for (i = 0; i < nargs; i++)
	{
		bool		have_conflict;

		if (input_base_typeids[i] != UNKNOWNOID)
			continue;

		resolved_unknowns = true;

		slot_category[i] = TYPCATEGORY_INVALID;
		slot_has_preferred_type[i] = false;

		have_conflict = false;

		for (current_candidate = candidates;
			 current_candidate != NULL;
			 current_candidate = current_candidate->next)
		{
			current_typeids = current_candidate->args;

			get_type_category_preferred(current_typeids[i],
										&current_category,
										&current_is_preferred);

			if (slot_category[i] == TYPCATEGORY_INVALID)
			{
				slot_category[i] = current_category;
				slot_has_preferred_type[i] = current_is_preferred;
			}
			else if (current_category == slot_category[i])
			{
				slot_has_preferred_type[i] |= current_is_preferred;
			}
			else
			{
				if (current_category == TYPCATEGORY_STRING)
				{
					slot_category[i] = current_category;
					slot_has_preferred_type[i] = current_is_preferred;
				}
				else
				{
					have_conflict = true;
				}
			}
		}
		if (have_conflict && slot_category[i] != TYPCATEGORY_STRING)
		{
			resolved_unknowns = false;
			break;
		}
	}

	if (resolved_unknowns)
	{
		last_candidate = NULL;
		first_candidate = candidates;
		ncandidates = 0;

		for (current_candidate = candidates;
			 current_candidate != NULL;
			 current_candidate = current_candidate->next)
		{
			bool		keepit = true;

			current_typeids = current_candidate->args;
			for (i = 0; i < nargs; i++)
			{
				if (input_base_typeids[i] != UNKNOWNOID)
					continue;

				get_type_category_preferred(current_typeids[i],
											&current_category,
											&current_is_preferred);

				if (current_category != slot_category[i])
				{
					keepit = false;
					break;
				}
				if (slot_has_preferred_type[i] && !current_is_preferred)
				{
					keepit = false;
					break;
				}
			}
			if (keepit)
			{
				last_candidate = current_candidate;
				ncandidates++;
			}
			else
			{
				if (last_candidate == NULL)
					first_candidate = current_candidate->next;
				else
					last_candidate->next = current_candidate->next;
			}
		}

		if (last_candidate != NULL)
		{
			candidates = first_candidate;
			last_candidate->next = NULL;
		}

		if (ncandidates == 1)
			return candidates;
	}

	if (nunknowns < nargs)
	{
		Oid			known_type = UNKNOWNOID;

		for (i = 0; i < nargs; i++)
		{
			if (input_base_typeids[i] == UNKNOWNOID)
				continue;

			if (known_type == UNKNOWNOID)
			{
				known_type = input_base_typeids[i];
			}
			else if (known_type != input_base_typeids[i])
			{
				known_type = UNKNOWNOID;
				break;
			}
		}

		if (known_type != UNKNOWNOID)
		{
			last_candidate = NULL;
			ncandidates = 0;

			for (current_candidate = candidates;
				 current_candidate != NULL;
				 current_candidate = current_candidate->next)
			{
				current_typeids = current_candidate->args;

				for (i = 0; i < nargs; i++)
				{
					if (known_type == JSONBOID)
						continue;

					if (current_typeids[i] == JSONBOID)
						continue;

					if (can_coerce_type(1, &known_type, &current_typeids[i],
										COERCION_IMPLICIT))
						continue;

					break;
				}
				if (i < nargs)
					continue;

				if (++ncandidates > 1)
					break;

				last_candidate = current_candidate;
			}

			if (ncandidates == 1)
			{
				last_candidate->next = NULL;

				return last_candidate;
			}
		}
	}

	return NULL;
}

/*
 * Function to run the criteria match against the candidate list.
 * The candidates list IS MODIFIED by this function (filtered down).
 * The function returns the number of candidates found.
 */
static int
cypher_match_function(int matchDepth, int nargs, Oid *input_base_typeids,
					  TYPCATEGORY *slot_category, FuncCandidateList *candidates)
{
	int			ncandidates = 0;
	int			bestmatch = 0;
	FuncCandidateList current_candidate = NULL;
	FuncCandidateList last_candidate = NULL;

	/* loop through candidate list by following the next pointer */
	for (current_candidate = *candidates;
		 current_candidate != NULL;
		 current_candidate = current_candidate->next)
	{
		Oid		   *current_typeids;
		int			i;
		int			nmatches = 0;

		current_typeids = current_candidate->args;
		/* loop through the arguments and check for matches */
		for (i = 0; i < nargs; i++)
		{
			if (input_base_typeids[i] == UNKNOWNOID)
				continue;

			if (cypher_match_function_criteria(matchDepth,
											   input_base_typeids[i],
											   current_typeids[i],
											   slot_category[i]))
				nmatches++;
		}

		if (nmatches > bestmatch || last_candidate == NULL)
		{
			*candidates = current_candidate;
			last_candidate = current_candidate;
			ncandidates = 1;
			bestmatch = nmatches;
		}
		else if (nmatches == bestmatch)
		{
			last_candidate->next = current_candidate;
			last_candidate = current_candidate;
			ncandidates++;
		}
	}

	/* make sure that the end of the list is terminated */
	if (last_candidate != NULL)
		last_candidate->next = NULL;

	return ncandidates;
}

/*
 * Function to match a specific criteria, given a specific number of
 * conditions to match. It will return true if a match is found, false
 * otherwise. The larger the include value is, the more conditions it will
 * test for. The conditions higher up in function have more weight than
 * those lower.
 */
static bool
cypher_match_function_criteria(int matchDepth, Oid inputBaseType,
							   Oid currentType, TYPCATEGORY slotCategory)
{
	/* if the match depth is out of range, which shouldn't happen, fail */
	if (matchDepth < 1 || matchDepth > 6)
	{
		elog(ERROR, "Invalid matchDepth value [%d] for cypher_match_function_criteria",
			 matchDepth);
		/* elog ERROR aborts, it will never get here */
		return false;
	}

	/* check for exact match */
	if (currentType == inputBaseType)
		return true;

	/* check for preferred match */
	if (matchDepth >= 2 &&
		IsPreferredType(slotCategory, currentType))
		return true;

	/*
	 * we prioritized NUMERICOID over TEXTOID and BOOLOID due to ambiguity in
	 * matching aggregate functions for jsonb types.
	 */
	if (matchDepth >= 3 &&
		(inputBaseType == JSONBOID && currentType == NUMERICOID))
		return true;

	/*
	 * we prioritize BOOLOID, TEXOID, and JSONBOID over the rest as JSONB has
	 * those types as primitive types
	 */
	if (matchDepth >= 4 &&
		((inputBaseType == JSONBOID &&
		  (currentType == BOOLOID || currentType == TEXTOID)) ||
		 currentType == JSONBOID))
		return true;

	/*
	 * the following types are also number in JSON but they have lower
	 * priority than above types because jsonb stores number using numeric
	 * type internally and this means that we need to type-cast numeric type
	 * to those types.
	 */
	if (matchDepth >= 5 &&
		(inputBaseType == JSONBOID &&
		 (currentType == INT2OID || currentType == INT4OID ||
		  currentType == INT8OID || currentType == FLOAT4OID ||
		  currentType == FLOAT8OID)))
		return true;

	/* we can type-cast jsonb type to any type through CypherTypeCast */
	if (matchDepth >= 6 &&
		inputBaseType == JSONBOID)
		return true;

	/* none found */
	return false;
}

static List *
func_get_best_args(ParseState *pstate, List *args, Oid argtypes[FUNC_MAX_ARGS],
				   FuncCandidateList candidate)
{
	ListCell   *la;
	int			i;
	List	   *newargs = NIL;

	i = 0;
	foreach(la, args)
	{
		Node	   *arg = lfirst(la);

		if (argtypes[i] == JSONBOID || candidate->args[i] == JSONBOID)
			arg = coerce_expr(pstate, arg, argtypes[i], candidate->args[i], -1,
							  COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);

		newargs = lappend(newargs, arg);
		i++;
	}

	return newargs;
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
			newarg = coerce_to_jsonb(pstate, arg, "COALESCE value");
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
transformIndirection(ParseState *pstate, A_Indirection *indir)
{
	Node	   *last_srf = pstate->p_last_srf;
	Node	   *res;
	Oid			restype;
	int			location;
	ListCell   *li;
	List	   *path = NIL;

	res = transformCypherExprRecurse(pstate, indir->arg);
	restype = exprType(res);
	location = exprLocation(res);

	/* record/composite or array type */
	foreach(li, indir->indirection)
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
									last_srf, NULL, false, location);
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
			SubscriptingRef *aref;

			Assert(IsA(i, A_Indices));

			if (!type_is_array_domain(restype))
				break;

			ind = (A_Indices *) i;

			if (ind->is_slice && ind->lidx != NULL)
				lidx = makeArrayIndex(pstate, ind->lidx, res, false);

			if (ind->uidx != NULL)
				uidx = makeArrayIndex(pstate, ind->uidx, res, ind->is_slice);

			arrtype = restype;
			arrtypmod = exprTypmod(res);
			transformContainerType(&arrtype, &arrtypmod);
			getSubscriptingRoutines(arrtype, &elemtype);

			aref = makeNode(SubscriptingRef);
			aref->refcontainertype = arrtype;
			aref->refelemtype = elemtype;
			aref->reftypmod = arrtypmod;
			aref->refupperindexpr = list_make1(uidx);
			aref->reflowerindexpr = (ind->is_slice ? list_make1(lidx) : NIL);
			aref->refexpr = (Expr *) res;
			aref->refassgnexpr = NULL;
			aref->refrestype = ind->is_slice ? aref->refcontainertype :
				aref->refelemtype;

			res = (Node *) aref;
		}
		restype = exprType(res);
	}

	if (li == NULL)
		return res;

	res = filterAccessArg(pstate, res, location, "map or list");

	if (IsJsonbAccessor(res))
	{
		getAccessorArguments(res, &res, &path);
	}

	for_each_cell(li, indir->indirection, li)
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
			 * ExecEvalCypherAccess() will handle lidx and uidx properly based
			 * on their types.
			 */

			lidx = transformCypherExprRecurse(pstate, ind->lidx);
			lidx = adjustListIndexType(pstate, lidx);
			uidx = transformCypherExprRecurse(pstate, ind->uidx);
			uidx = adjustListIndexType(pstate, uidx);

			cind = makeNode(CypherIndices);
			cind->is_slice = ind->is_slice;
			cind->lidx = (Expr *) lidx;
			cind->uidx = (Expr *) uidx;

			elem = (Node *) cind;
		}

		path = lappend(path, elem);
	}

	return makeJsonbFuncAccessor(pstate, res, path);
}

static Node *
makeArrayIndex(ParseState *pstate, Node *idx, Node *arr, bool exclusive)
{
	Node	   *last_srf = pstate->p_last_srf;
	Node	   *idxexpr;
	Node	   *result;
	Node	   *len;
	Node	   *isneg;
	Node	   *fromend;
	Node	   *one;
	CaseWhen   *when;
	CaseExpr   *norm;

	Assert(idx != NULL);

	idxexpr = transformCypherExprRecurse(pstate, idx);
	result = coerce_expr(pstate, idxexpr, exprType(idxexpr), INT4OID, -1,
						 COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);
	if (result == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("array subscript must have type integer"),
				 parser_errposition(pstate, exprLocation(idxexpr))));
		return NULL;
	}

	/*
	 * Cypher lets a negative subscript count from the end of the list, but a
	 * Postgres array subscript does not.  Map i (< 0) to i + array_length(arr,1)
	 * at run time, so negative indices and slice bounds on a vertex[]/edge[]
	 * behave the way they do on a jsonb list (which ExecEvalCypherAccessExpr
	 * already handles).  On an empty array array_length() is NULL, so a negative
	 * subscript resolves to NULL -- "out of range", the same as Cypher.  A
	 * non-negative subscript keeps the CASE's default and is unaffected.
	 */
	len = (Node *) makeFuncExpr(F_ARRAY_LENGTH, INT4OID,
								list_make2(copyObject(arr),
										   makeConst(INT4OID, -1, InvalidOid,
													 sizeof(int32),
													 Int32GetDatum(1),
													 false, true)),
								InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);
	isneg = (Node *) make_op(pstate, list_make1(makeString("<")), copyObject(result),
							 (Node *) makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
												Int32GetDatum(0), false, true),
							 last_srf, -1);
	fromend = (Node *) make_op(pstate, list_make1(makeString("+")), copyObject(result),
							   len, last_srf, -1);

	when = makeNode(CaseWhen);
	when->expr = (Expr *) isneg;
	when->result = (Expr *) fromend;
	when->location = -1;

	norm = makeNode(CaseExpr);
	norm->casetype = INT4OID;
	norm->arg = NULL;
	norm->args = list_make1(when);
	norm->defresult = (Expr *) result;
	norm->location = -1;
	result = (Node *) norm;

	if (exclusive)
		return result;

	one = (Node *) makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
							 Int32GetDatum(1), false, true);

	result = (Node *) make_op(pstate, list_make1(makeString("+")),
							  result, one, last_srf, -1);

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
			return coerce_to_jsonb(pstate, idx, "list index");
	}
}

/*
 * Cypher scalar type families, for type-aware comparison of a promoted column
 * against another operand.  Cypher compares by type: two values compare
 * "natively" only within the same family (all numbers together, all strings
 * together, booleans together); across families they are never equal and order
 * by type rank, which the jsonb comparison path reproduces.  An untyped literal
 * (UNKNOWNOID) is a Cypher string.
 */
typedef enum
{
	CF_OTHER,
	CF_NUMERIC,
	CF_STRING,
	CF_BOOL
}			CypherScalarFamily;

static CypherScalarFamily
cypherScalarFamily(Oid t)
{
	switch (t)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			return CF_NUMERIC;
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
		case UNKNOWNOID:
		case CSTRINGOID:
			return CF_STRING;
		case BOOLOID:
			return CF_BOOL;
		default:
			return CF_OTHER;
	}
}

/* If `n' is cypher_to_jsonb(x), return x (the native value); else NULL. */
static Node *
unboxCypherToJsonb(Node *n)
{
	if (n != NULL && IsA(n, FuncExpr))
	{
		FuncExpr   *f = (FuncExpr *) n;

		if (f->funcid == F_CYPHER_TO_JSONB && list_length(f->args) == 1)
			return (Node *) linitial(f->args);
	}
	return NULL;
}

/*
 * transformPromotedComparison
 *
 * A promoted property access arrives at a comparison boxed as
 * cypher_to_jsonb(column).  When one side is such a box, rebind the comparison
 * by Cypher type: if the other operand is the same Cypher scalar family, unify
 * both to their common type and emit the native operator -- so the column's
 * btree binds, and, e.g., an integer column against a fractional literal still
 * compares as numbers without rounding.  Otherwise compare through jsonb, which
 * yields the correct type-aware result (values of different Cypher types are
 * never equal, and order by type rank).  Returns NULL when neither side is a
 * promoted column, leaving every ordinary comparison exactly as it was.
 */
static Node *
transformPromotedComparison(ParseState *pstate, List *opname,
							Node *l, Node *r, Node *last_srf, int location)
{
	Node	   *lx = unboxCypherToJsonb(l);
	Node	   *rx = unboxCypherToJsonb(r);
	Node	   *lu;
	Node	   *ru;
	CypherScalarFamily lf;

	if (lx == NULL && rx == NULL)
		return NULL;

	lu = (lx != NULL) ? lx : l;
	ru = (rx != NULL) ? rx : r;
	lf = cypherScalarFamily(exprType(lu));

	if (lf != CF_OTHER && lf == cypherScalarFamily(exprType(ru)))
	{
		Oid			common = select_common_type(pstate, list_make2(lu, ru),
												"comparison", NULL);

		lu = coerce_to_common_type(pstate, lu, common, "comparison");
		ru = coerce_to_common_type(pstate, ru, common, "comparison");
		return (Node *) make_op(pstate, opname, lu, ru, last_srf, location);
	}

	l = coerce_to_jsonb(pstate, l, "jsonb");
	r = coerce_to_jsonb(pstate, r, "jsonb");
	return (Node *) make_op(pstate, opname, l, r, last_srf, location);
}

static Node *
transformAExprOp(ParseState *pstate, A_Expr *a)
{
	Node	   *last_srf = pstate->p_last_srf;
	Node	   *l;
	Node	   *r;

	l = transformCypherExprRecurse(pstate, a->lexpr);
	r = transformCypherExprRecurse(pstate, a->rexpr);

	if (a->kind == AEXPR_OP && list_length(a->name) == 1)
	{
		const char *opname = strVal(linitial(a->name));
		Oid			ltype = exprType(l);
		Oid			rtype = exprType(r);

		if (strcmp(opname, "`XOR`") == 0)
		{
			/*
			 * Cypher XOR is boolean inequality: coerce both operands to
			 * boolean (like AND/OR/NOT) and emit a strict boolean "<>"
			 * (boolne), which is NULL when either side is NULL.
			 */
			l = coerce_cypher_arg_to_boolean(pstate, l, "XOR");
			r = coerce_cypher_arg_to_boolean(pstate, r, "XOR");

			return (Node *) make_op(pstate, list_make1(makeString("<>")),
									l, r, last_srf, a->location);
		}

		if (strcmp(opname, "`+`") == 0 ||
			strcmp(opname, "`-`") == 0 ||
			strcmp(opname, "`*`") == 0 ||
			strcmp(opname, "`/`") == 0 ||
			strcmp(opname, "`%`") == 0 ||
			strcmp(opname, "`^`") == 0)
		{
			l = coerce_to_jsonb(pstate, l, "jsonb");
			r = coerce_to_jsonb(pstate, r, "jsonb");

			return (Node *) make_op(pstate, a->name, l, r, last_srf,
									a->location);
		}

		if (strcmp(opname, "+") == 0 ||
			strcmp(opname, "-") == 0 ||
			strcmp(opname, "*") == 0 ||
			strcmp(opname, "/") == 0 ||
			strcmp(opname, "%") == 0 ||
			strcmp(opname, "^") == 0)
		{
			if (ltype == JSONBOID || rtype == JSONBOID ||
				!OidIsValid(LookupOperName(pstate, a->name, ltype, rtype,
										   true, a->location)))
			{
				char	   *newopname;

				l = coerce_to_jsonb(pstate, l, "jsonb");
				r = coerce_to_jsonb(pstate, r, "jsonb");

				newopname = psprintf("`%s`", opname);
				a->name = list_make1(makeString(newopname));

				return (Node *) make_op(pstate, a->name, l, r, last_srf,
										a->location);
			}
		}
		else if (strcmp(opname, "=") == 0 ||
				 strcmp(opname, "<>") == 0 ||
				 strcmp(opname, "<") == 0 ||
				 strcmp(opname, ">") == 0 ||
				 strcmp(opname, "<=") == 0 ||
				 strcmp(opname, ">=") == 0)
		{
			Node	   *promoted;

			/*
			 * If a promoted property is being compared (it arrives boxed as
			 * cypher_to_jsonb(column)), rebind the comparison type-aware: bind
			 * the native, index-usable operator when the other operand is the
			 * same Cypher scalar family, else compare through jsonb (Cypher says
			 * different types are never equal / order by type rank).  Returns
			 * NULL when neither side is a promoted column, leaving every other
			 * comparison exactly as before.
			 */
			promoted = transformPromotedComparison(pstate, a->name, l, r,
												   last_srf, a->location);
			if (promoted != NULL)
				return promoted;

			if (ltype != InvalidOid && rtype == UNKNOWNOID)
				rtype = ltype;
			else if (ltype == UNKNOWNOID && rtype != InvalidOid)
				ltype = rtype;

			if (ltype == GRAPHIDOID || rtype == GRAPHIDOID)
				return (Node *) make_op(pstate, a->name, l, r, last_srf,
										a->location);

			if (ltype == JSONBOID || rtype == JSONBOID ||
				!OidIsValid(LookupOperName(pstate, a->name, ltype, rtype,
										   true, a->location)))
			{
				l = coerce_to_jsonb(pstate, l, "jsonb");
				r = coerce_to_jsonb(pstate, r, "jsonb");

				return (Node *) make_op(pstate, a->name, l, r, last_srf,
										a->location);
			}
		}
		else if (strcmp(opname, "<->") == 0 || strcmp(opname, "<#>") == 0 ||
				 strcmp(opname, "<=>") == 0 || strcmp(opname, "<+>") == 0)
		{
			Node	   *lx = unboxCypherToJsonb(l);
			Node	   *rx = unboxCypherToJsonb(r);

			/*
			 * A vector distance operator has no jsonb form, so a promoted vector
			 * column -- which arrives boxed as cypher_to_jsonb(column) like any
			 * other promoted property -- must be unboxed to the native vector
			 * for the operator, and its HNSW index, to resolve.  Mirrors the
			 * comparison unboxing done in transformPromotedComparison.
			 */
			if (lx != NULL)
				l = lx;
			if (rx != NULL)
				r = rx;

			return (Node *) make_op(pstate, a->name, l, r, last_srf,
									a->location);
		}
	}

	return (Node *) make_op(pstate, a->name, l, r, last_srf, a->location);
}

/*
 * transformCypherNullIf
 *		NULLIF(v1, v2) yields NULL when v1 = v2, otherwise v1.
 *
 *		It is defined in terms of "=", so the comparison is built through the
 *		ordinary cypher "=" handling (transformAExprOp) -- giving it the same
 *		coercion any cypher equality gets: graphid identity for graph elements,
 *		jsonb equality when either operand is jsonb (e.g. a property) or has no
 *		native "=" operator, and the native operator for matching scalar types.
 *		The resulting comparison OpExpr is then retagged to a NullIfExpr (the two
 *		are the same struct) whose result type is the first operand's, reusing
 *		the core NULLIF executor and deparse.
 */
static Node *
transformCypherNullIf(ParseState *pstate, A_Expr *a)
{
	A_Expr	   *cmp;
	Node	   *node;
	OpExpr	   *result;

	cmp = makeSimpleA_Expr(AEXPR_OP, "=", a->lexpr, a->rexpr, a->location);
	node = transformAExprOp(pstate, cmp);

	/* The "=" comparison must be a plain operator yielding a scalar boolean. */
	if (!IsA(node, OpExpr))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("NULLIF requires = operator to yield boolean"),
				 parser_errposition(pstate, a->location)));

	result = (OpExpr *) node;

	if (result->opresulttype != BOOLOID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("NULLIF requires = operator to yield boolean"),
				 parser_errposition(pstate, a->location)));
	if (result->opretset)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("NULLIF must not return a set"),
				 parser_errposition(pstate, a->location)));

	/* The NullIfExpr yields the (possibly coerced) first operand's type. */
	result->opresulttype = exprType((Node *) linitial(result->args));

	/* NullIfExpr and OpExpr are the same struct. */
	NodeSetTag(result, T_NullIfExpr);

	return (Node *) result;
}

/*
 * A graph type whose value is a single id-bearing element (vertex/edge/graphid),
 * hence reducible to a graphid for identity comparison.  A graphpath or an array
 * of graph elements is NOT: it has no single identity, so IN over it keeps the
 * jsonb-containment behavior.
 */
static bool
is_id_reducible_graph_type(Oid type)
{
	return type == GRAPHIDOID || type == VERTEXOID || type == EDGEOID;
}

/*
 * Reduce a graph-membership operand to a graphid, so both sides of IN can be
 * compared by identity (matching the id-only "=" operator):
 *   - a graphid stays as-is;
 *   - a vertex/edge is reduced to its "id" field (a graphid);
 *   - anything else (a numeric graphid literal like 1.1, an unknown literal)
 *     is coerced to graphid via the implicit numeric->graphid cast; if there is
 *     no path to graphid, a clear type error is raised here.
 */
static Node *
reduce_graph_elem_to_id(ParseState *pstate, Node *elem, int location)
{
	Oid			typoid = exprType(elem);
	Node	   *id;

	if (typoid == GRAPHIDOID)
		return elem;
	if (typoid == VERTEXOID || typoid == EDGEOID)
		return getExprField((Expr *) elem, AG_ELEM_ID);

	id = coerce_to_target_type(pstate, elem, typoid, GRAPHIDOID, -1,
							   COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, location);
	if (id == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("cannot compare graph element identity to type %s",
						format_type_be(typoid)),
				 parser_errposition(pstate, location)));
	return id;
}

static Node *
transformAExprIn(ParseState *pstate, A_Expr *a)
{
	Node	   *result = NULL;
	Node	   *lexpr;
	List	   *rexprs;
	List	   *rvars;
	List	   *rnonvars;
	ListCell   *l;

	lexpr = transformCypherExprRecurse(pstate, a->lexpr);

	rexprs = rvars = rnonvars = NIL;

	/*
	 * A graph element (vertex/edge/graphid) on the left of IN is a membership
	 * test by *identity*, matching the id-only "=" operator (vertex_eq and
	 * edge_eq reduce to graphid_eq).  Reduce the LHS to its graphid and compare
	 * ids, rather than the jsonb "@>" containment used for scalars below:
	 * containment compares the full serialization (id + properties), so it is
	 * slower and, worse, diverges from "=" once a collected element is mutated.
	 * (A graphpath / graph-array LHS is not a single element; it falls to the
	 * containment path below, preserving its prior behavior.)
	 */
	if (is_id_reducible_graph_type(exprType(lexpr)))
	{
		Node	   *idLhs = reduce_graph_elem_to_id(pstate, lexpr,
													   exprLocation(a->lexpr));

		if (IsA(a->rexpr, CypherListExpr))
		{
			/*
			 * Literal list: reduce each element to a graphid and fall through
			 * to the shared tail, which lowers this to "id = ANY(graphid[])"
			 * (or a single / OR of "id = ...").  Those fold to graphid Consts,
			 * so the scan-pruning pass can restrict the scan to just the labels
			 * the ids name.  coerce_to_jsonb() rejects graph elements, so the
			 * raw elements are transformed here rather than the whole list.
			 */
			foreach(l, ((CypherListExpr *) a->rexpr)->elems)
			{
				Node	   *elem = transformCypherExprRecurse(pstate, lfirst(l));

				elem = reduce_graph_elem_to_id(pstate, elem, exprLocation(elem));

				rexprs = lappend(rexprs, elem);
				if (contain_vars_of_level(elem, 0))
					rvars = lappend(rvars, elem);
				else
					rnonvars = lappend(rnonvars, elem);
			}
			lexpr = idLhs;
			/* fall through to the shared SAOP/OR tail below */
		}
		else
		{
			/*
			 * Dynamic RHS (collect(u), or an array/jsonb expression): it is
			 * rebuilt per outer row and never const-folded, so test membership
			 * at runtime by graphid identity.
			 */
			Node	   *rexpr = transformCypherExprRecurse(pstate, a->rexpr);
			Oid			rtype = exprType(rexpr);

			/* already graphid[]: a plain "= ANY" SAOP (also prunable) */
			if (rtype == GRAPHIDARRAYOID)
				return (Node *) make_scalar_array_op(pstate, a->name, true,
													 idLhs, rexpr, a->location);

			/* vertex[]/edge[]/anyarray/...: serialize to a jsonb array */
			if (rtype != JSONBOID)
				rexpr = (Node *) makeFuncExpr(F_TO_JSONB, JSONBOID,
											  list_make1(rexpr), InvalidOid,
											  InvalidOid, COERCE_EXPLICIT_CALL);

			/*
			 * id-only membership over the jsonb array: compares just each
			 * element's ->'id' as a graphid (matching "=", unlike "@>"
			 * containment) and short-circuits on the first match.
			 */
			return (Node *) makeFuncExpr(F_GRAPHID_IN_JSONB_ARRAY, BOOLOID,
										 list_make2(idLhs, rexpr), InvalidOid,
										 InvalidOid, COERCE_EXPLICIT_CALL);
		}
	}
	else
	{
		/*
		 * Scalar LHS, or a graph type that is not a single id-bearing element
		 * (a graphpath, or an array of graph elements): membership via jsonb
		 * "@>" containment, exactly as before.  A graph value is serialized with
		 * to_jsonb() (coerce_to_jsonb() rejects graph types) and matched as a
		 * one-element array; a scalar is coerced to jsonb directly.
		 */
		bool		lexpr_is_graph = is_graph_type(exprType(lexpr));
		Node	   *containmentLhs;
		Node	   *rexpr;

		if (lexpr_is_graph)
			lexpr = (Node *) makeFuncExpr(F_TO_JSONB, JSONBOID, list_make1(lexpr),
										  InvalidOid, InvalidOid,
										  COERCE_EXPLICIT_CALL);
		else
			lexpr = coerce_to_jsonb(pstate, lexpr, "jsonb");

		if (lexpr_is_graph)
		{
			/*
			 * `arr @> x` treats x as an array element only when x is a
			 * primitive; for a serialized object it returns false, so wrap the
			 * element in a one-element array: `arr @> [v]` is array-vs-array
			 * containment, which does match.
			 */
			CypherListExpr *wrap = makeNode(CypherListExpr);

			wrap->elems = list_make1(lexpr);
			wrap->location = -1;
			containmentLhs = (Node *) wrap;
		}
		else
			containmentLhs = lexpr;

		rexpr = transformCypherExprRecurse(pstate, a->rexpr);

		switch (nodeTag(rexpr))
		{
			case T_CypherListExpr:
				foreach(l, ((CypherListExpr *) rexpr)->elems)
				{
					Node	   *elem = lfirst(l);

					rexprs = lappend(rexprs, elem);
					if (contain_vars_of_level(elem, 0))
						rvars = lappend(rvars, elem);
					else
						rnonvars = lappend(rnonvars, elem);
				}
				break;
			case T_CypherAccessExpr:
			case T_Var:
				result = (Node *) make_op(pstate, list_make1(makeString("@>")),
										  rexpr, containmentLhs, pstate->p_last_srf,
										  a->location);
				break;
			default:
				{
					/*
					 * For other expr types, check if the result type is
					 * compatible with JSONB containment.
					 */
					Oid			rtype = exprType(rexpr);

					if (rtype == JSONBOID || type_is_array(rtype) || rtype == ANYARRAYOID)
					{
						/* Coerce anyarray to jsonb for containment operator */
						if (rtype == ANYARRAYOID || type_is_array(rtype))
							rexpr = coerce_to_jsonb(pstate, rexpr, "list");

						result = (Node *) make_op(pstate, list_make1(makeString("@>")),
												  rexpr, containmentLhs, pstate->p_last_srf,
												  a->location);
					}
					else
					{
						ereport(ERROR,
								(errcode(ERRCODE_DATATYPE_MISMATCH),
								 errmsg("CypherList is expected but %s",
										format_type_be(rtype)),
								 parser_errposition(pstate, exprLocation(a->rexpr))));
					}
					break;
				}
		}
	}

	/*
	 * ScalarArrayOpExpr is only going to be useful if there's more than one
	 * non-Var righthand item.
	 */
	if (list_length(rnonvars) > 1)
	{
		List	   *allexprs;
		Oid			scalar_type;
		Oid			array_type;

		/*
		 * Try to select a common type for the array elements.  Note that
		 * since the LHS' type is first in the list, it will be preferred when
		 * there is doubt (eg, when all the RHS items are unknown literals).
		 *
		 * Note: use list_concat here not lcons, to avoid damaging rnonvars.
		 */
		allexprs = list_concat(list_make1(lexpr), rnonvars);
		scalar_type = select_common_type(pstate, allexprs, NULL, NULL);

		/*
		 * Do we have an array type to use?  Aside from the case where there
		 * isn't one, we don't risk using ScalarArrayOpExpr when the common
		 * type is RECORD, because the RowExpr comparison logic below can cope
		 * with some cases of non-identical row types.
		 */
		if (OidIsValid(scalar_type) && scalar_type != RECORDOID)
			array_type = get_array_type(scalar_type);
		else
			array_type = InvalidOid;
		if (array_type != InvalidOid)
		{
			/*
			 * OK: coerce all the right-hand non-Var inputs to the common type
			 * and build an ArrayExpr for them.
			 */
			List	   *aexprs;
			ArrayExpr  *newa;

			aexprs = NIL;
			foreach(l, rnonvars)
			{
				Node	   *_rexpr = (Node *) lfirst(l);

				_rexpr = coerce_to_common_type(pstate, _rexpr, scalar_type, "IN");
				aexprs = lappend(aexprs, _rexpr);
			}
			newa = makeNode(ArrayExpr);
			newa->array_typeid = array_type;
			/* array_collid will be set by parse_collate.c */
			newa->element_typeid = scalar_type;
			newa->elements = aexprs;
			newa->multidims = false;
			newa->location = -1;

			result = (Node *) make_scalar_array_op(pstate, a->name, true, lexpr,
												   (Node *) newa, a->location);

			/* Consider only the Vars (if any) in the loop below */
			rexprs = rvars;
		}
	}

	/*
	 * Must do it the hard way, ie, with a boolean expression tree.
	 */
	foreach(l, rexprs)
	{
		Node	   *_rexpr = (Node *) lfirst(l);
		Node	   *cmp;

		/* Ordinary scalar operator */
		cmp = (Node *) make_op(pstate, a->name, copyObject(lexpr), _rexpr,
							   pstate->p_last_srf, a->location);

		cmp = coerce_to_boolean(pstate, cmp, "IN");
		if (result == NULL)
			result = cmp;
		else
			result = (Node *) makeBoolExpr(OR_EXPR, list_make2(result, cmp),
										   a->location);
	}

	/*
	 * Handle empty list case: value IN [] is always FALSE
	 */
	if (result == NULL)
		result = (Node *) makeBoolConst(false, false);

	return result;
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
		arg = coerce_cypher_arg_to_boolean(pstate, arg, opname);

		args = lappend(args, arg);
	}

	return (Node *) makeBoolExpr(b->boolop, args, b->location);
}

/*
 * Helper function to build a CypherTypeCast node.
 *
 * Note: expr is expected to be non NULL.
 *
 * Note: coercion context and type category were added for runtime
 * type casting.
 */
static Node *
build_cypher_cast_expr(Node *expr, Oid otyp, int32 otypmod,
					   CoercionContext cctx, CoercionForm cform, int loc)
{
	CypherTypeCast *tc;
	Oid			obtyp;

	/* validate input */
	Assert(expr != NULL);

	obtyp = getBaseType(otyp);
	tc = makeNode(CypherTypeCast);
	tc->type = obtyp;
	tc->typmod = otypmod;
	tc->cctx = cctx;
	tc->cform = cform;
	tc->typcategory = TypeCategory(obtyp);
	tc->arg = (Expr *) expr;
	tc->location = loc;

	return (Node *) tc;
}

Node *
coerce_expr(ParseState *pstate, Node *expr, Oid ityp, Oid otyp, int32 otypmod,
			CoercionContext cctx, CoercionForm cform, int loc)
{
	Node	   *node;

	/* return expr if it is already coerced or null */
	if (ityp == otyp || expr == NULL)
		return expr;

	node = coerce_unknown_const(pstate, expr, ityp, otyp);
	if (node != NULL)
		return node;

	/*
	 * === Process JSONBOID input and output types here EXCLUDING: JSONOID ->
	 * JSONBOID JSONBOID   -> JSONOID VERTEXOID  -> JSONBOID EDGEOID    ->
	 * JSONBOID UNKNOWNOID -> JSONBOID JSONBOID   -> ANY*OID  ANYOID &
	 * (IsPolymorphicType) We need to let postgres process these.
	 */
	if (ityp != JSONOID && otyp != JSONOID &&
		ityp != VERTEXOID && ityp != EDGEOID &&
		ityp != UNKNOWNOID &&
		otyp != ANYOID &&
		!IsPolymorphicType(otyp))
	{
		if (ityp == JSONBOID)
		{
			if (OidIsValid(get_element_type(otyp)) ||
				otyp == RECORDARRAYOID ||
				type_is_rowtype(otyp))
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot cast type jsonb to %s",
								format_type_be(otyp))));
				return NULL;
			}

			/*
			 * Cast our JSONBOID types here, exec phase will do the rest.
			 * Note: The output of a jsonb string is double quoted (not a
			 * proper format for strings). We defer this to the execution
			 * phase of the cypher type cast.
			 */

			/*
			 * Carry the requested typmod (e.g. "embedding::vector(4)") into the
			 * cast so the result has the correct length/dimension; otherwise
			 * expressions such as a pgvector HNSW property index fail with
			 * "column does not have dimensions".
			 */
			return build_cypher_cast_expr(expr, otyp, otypmod, cctx, cform, loc);
		}

		/* Catch cypher to_jsonb casts here */
		if (otyp == JSONBOID)
		{
			return (Node *) makeFuncExpr(F_CYPHER_TO_JSONB, JSONBOID,
										 list_make1(expr), InvalidOid,
										 InvalidOid, COERCE_EXPLICIT_CALL);
		}
	}

	/*
	 * UNKNOWNOID parameters will be handled by p_coerce_param_hook Note: We
	 * need coerce_to_target_type done last to make sure that cypher JSONBOID
	 * casts happen before any postgres JSONBOID casts. In the event any get
	 * added to postgres in the future.
	 */
	node = coerce_to_target_type(pstate, expr, ityp, otyp, otypmod, cctx,
								 cform, loc);
	return node;
}

static Node *
coerce_unknown_const(ParseState *pstate, Node *expr, Oid ityp, Oid otyp)
{
	Const	   *con;
	char	   *s;
	Datum		datum;
	Oid			coll;
	Const	   *newcon;

	if (!IsA(expr, Const))
		return NULL;

	con = (Const *) expr;
	if (con->constisnull)
	{
		/* This case will be handled later. NULL can be coerced to any type. */
		return NULL;
	}

	if (ityp != UNKNOWNOID)
	{
		/* Other types will be handled later by calling coerce_type(). */
		return NULL;
	}

	if (otyp == JSONBOID)
	{
		s = DatumGetCString(con->constvalue);
		datum = stringToJsonb(pstate, s, con->location);
		coll = InvalidOid;
	}
	else if (otyp == TEXTOID)
	{
		Jsonb	   *j;
		JsonbValue *jv;
		text	   *t;

		s = DatumGetCString(con->constvalue);
		datum = stringToJsonb(pstate, s, con->location);
		j = DatumGetJsonbP(datum);
		Assert(JB_ROOT_IS_SCALAR(j));

		jv = getIthJsonbValueFromContainer(&j->root, 0);
		Assert(jv->type == jbvString);

		t = cstring_to_text_with_len(jv->val.string.val, jv->val.string.len);

		datum = PointerGetDatum(t);
		coll = DEFAULT_COLLATION_OID;
	}
	else
	{
		/*
		 * Other types will be handled later by calling coerce_type().
		 * UNKNOWNOID can be coerced to any type using the input function of
		 * the target type.
		 */
		return NULL;
	}

	newcon = makeConst(otyp, -1, coll, -1, datum, false, false);
	newcon->location = con->location;

	return (Node *) newcon;
}

static Datum
stringToJsonb(ParseState *pstate, char *s, int location)
{
	StringInfoData si;
	const char *c;
	bool		escape = false;
	ParseCallbackState pcbstate;
	Datum		d;

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
					appendBinaryStringInfo(&si, "\\\"", 2);
					break;
				default:
					appendStringInfoCharMacro(&si, *c);
					break;
			}
		}
	}
	appendStringInfoCharMacro(&si, '"');

	setup_parser_errposition_callback(&pcbstate, pstate, location);

	d = DirectFunctionCall1(jsonb_in, CStringGetDatum(si.data));

	cancel_parser_errposition_callback(&pcbstate);

	return d;
}

static Node *
coerce_to_jsonb(ParseState *pstate, Node *expr, const char *targetname)
{
	Oid			type = exprType(expr);

	if (type == InvalidOid)
		return NULL;

	if (is_graph_type(type))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("graph object cannot be %s", targetname),
				 parser_errposition(pstate, exprLocation(expr))));
		return NULL;
	}

	return coerce_all_to_jsonb(pstate, expr);
}

static bool
is_graph_type(Oid type)
{
	switch (type)
	{
		case GRAPHIDARRAYOID:
		case GRAPHIDOID:
		case VERTEXARRAYOID:
		case VERTEXOID:
		case EDGEARRAYOID:
		case EDGEOID:
		case GRAPHPATHARRAYOID:
		case GRAPHPATHOID:
			return true;

		default:
			return false;
	}
}

static Node *
coerce_all_to_jsonb(ParseState *pstate, Node *expr)
{
	Oid			type = exprType(expr);
	TYPCATEGORY tc = TypeCategory(getBaseType(type));

	if (tc == TYPCATEGORY_STRING)
	{
		return (Node *) makeFuncExpr(F_CYPHER_TO_JSONB, JSONBOID,
									 list_make1(expr), InvalidOid,
									 InvalidOid, COERCE_EXPLICIT_CALL);
	}

	if (tc != TYPCATEGORY_USER)
		expr = coerce_expr(pstate, expr, type, JSONBOID, -1,
						   COERCION_EXPLICIT, COERCE_IMPLICIT_CAST,
						   exprLocation(expr));
	Assert(expr != NULL);
	return expr;
}

Node *
transformCypherMapForSet(ParseState *pstate, Node *expr, List **pathelems,
						 char **varname)
{
	ParseExprKind sv_expr_kind;
	Node	   *aelem;
	List	   *apath;
	Node	   *elem;
	Oid			elemtype;
	char	   *resname = NULL;
	ListCell   *le;
	List	   *textlist = NIL;

	sv_expr_kind = pstate->p_expr_kind;
	pstate->p_expr_kind = EXPR_KIND_UPDATE_SOURCE;

	expr = transformCypherExprRecurse(pstate, expr);

	pstate->p_expr_kind = sv_expr_kind;

	if (IsJsonbAccessor(expr))
	{
		getAccessorArguments(expr, &aelem, &apath);
	}
	else
	{
		aelem = expr;
		apath = NIL;
	}

	/* examine what aelem is */
	if (IsA(aelem, FieldSelect))
		elem = (Node *) ((FieldSelect *) aelem)->arg;
	else
		elem = aelem;

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

	if (IsA(elem, Var))
	{
		Var		   *var = (Var *) elem;

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

			t = (Node *) cind->uidx;
			t = coerce_to_target_type(pstate, t, exprType(t), TEXTOID, -1,
									  COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST,
									  -1);
			if (t == NULL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("path element must be text"),
						 parser_errposition(pstate,
											exprLocation((Node *) cind->uidx))));
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
 * coerce_cypher_arg_to_boolean()
 *      Coerce an argument of a construct that requires boolean input
 *      (AND, OR, NOT, etc).  Also check that input is not a set.
 *
 * Returns the possibly-transformed node tree.
 *
 * As with coerce_type, pstate may be NULL if no special unknown-Param
 * processing is wanted.
 */
static Node *
coerce_cypher_arg_to_boolean(ParseState *pstate, Node *node,
							 const char *constructName)
{
	Oid			inputTypeId = exprType(node);

	if (inputTypeId != BOOLOID)
	{
		Node	   *newnode;

		newnode = coerce_expr(pstate, node, inputTypeId, BOOLOID, -1,
							  COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);
		if (newnode == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
			/* translator: first %s is name of a SQL construct, eg WHERE */
					 errmsg("argument of %s must be type %s, not type %s",
							constructName, "boolean",
							format_type_be(inputTypeId)),
					 parser_errposition(pstate, exprLocation(node))));
		}
		node = newnode;
	}

	if (expression_returns_set(node))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
		/* translator: %s is name of a SQL construct, eg WHERE */
				 errmsg("argument of %s must not return a set",
						constructName),
				 parser_errposition(pstate, exprLocation(node))));
	}

	return node;
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

	qual = coerce_cypher_arg_to_boolean(pstate, qual, "WHERE");

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

	/* See findTargetlistEntrySQL92()/findTargetlistEntrySQL99() */
	foreach(lsi, sortitems)
	{
		SortBy	   *sortby = lfirst(lsi);
		Node	   *expr = NULL;
		ListCell   *lt;
		TargetEntry *te = NULL;

		/*
		 * First, if the sort item is a bare name, try to match it to a
		 * RETURN/WITH alias, as SQL does (findTargetlistEntrySQL92).  This
		 * lets "RETURN p.name AS name ORDER BY name" order by the projected
		 * column.
		 */
		if (sortby->node && IsA(sortby->node, ColumnRef))
		{
			ColumnRef  *cref = (ColumnRef *) sortby->node;

			if (list_length(cref->fields) == 1 &&
				IsA(linitial(cref->fields), String))
			{
				char	   *colname = strVal(linitial(cref->fields));

				foreach(lt, *targetlist)
				{
					TargetEntry *tmp = lfirst(lt);

					if (tmp->resname && strcmp(tmp->resname, colname) == 0)
					{
						te = tmp;
						break;
					}
				}
			}
		}

		/*
		 * Otherwise transform the sort expression.  If it matches an item
		 * already in the target list, reuse that entry; if not, add it as a
		 * resjunk entry so that ORDER BY can reference expressions that are
		 * not in the RETURN list (e.g. "RETURN p.name ORDER BY p.age").
		 */
		if (te == NULL)
		{
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
		}

		sortgroups = addTargetToSortList(pstate, te, sortgroups, *targetlist,
										 sortby);
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

void
resolveItemList(ParseState *pstate, List *items)
{
	ListCell   *li;

	foreach(li, items)
	{
		TargetEntry *te = lfirst(li);
		Oid			restype = exprType((Node *) te->expr);

		if (restype == BOOLOID ||
			is_graph_type(restype) ||
			type_is_array(restype))
			continue;

		if (!te->resjunk)
			te->expr = (Expr *) coerce_all_to_jsonb(pstate,
													(Node *) te->expr);
	}
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

		/* ignore RTEs that are inaccessible by unqualified names */
		if (!nsitem->p_cols_visible)
			continue;
		visible = true;

		/* should not have any lateral-only items when parsing items */
		Assert(!nsitem->p_lateral_only);

		targets = list_concat(targets,
							  expandNSItemAttrs(pstate,
												nsitem,
												0,
												true,
												location));
	}

	if (!visible)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("RETURN * with no accessible variables is invalid"),
				 parser_errposition(pstate, location)));

	/*
	 * Hidden promoted-column sentinels are non-junk subquery outputs (so they
	 * cross clause boundaries and pull up), but must never surface as user
	 * columns.  Drop them from the * expansion; for WITH *, the projection
	 * re-appends them keyed to the carried element (see transformCypherProjection).
	 *
	 * expandNSItemAttrs() numbered every expanded column consecutively out of
	 * p_next_resno, so removing some would leave gaps in the target resnos --
	 * which the planner's final apply_tlist_labeling() forbids.  Renumber the
	 * survivors from the same base and rewind p_next_resno to match, so a later
	 * projection item (or the sentinel re-append) keeps contiguous resnos.
	 */
	{
		List	   *filtered = NIL;
		ListCell   *lc;
		AttrNumber	resno;

		if (targets == NIL)
			return targets;
		resno = ((TargetEntry *) linitial(targets))->resno;

		foreach(lc, targets)
		{
			TargetEntry *te = lfirst(lc);

			if (te->resname != NULL && isPromotedSentinelName(te->resname))
				continue;
			te->resno = resno++;
			filtered = lappend(filtered, te);
		}
		pstate->p_next_resno = resno;
		targets = filtered;
	}

	return targets;
}
