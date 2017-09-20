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
#include "parser/parse_coerce.h"
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
static Node *scanRTEForVar(ParseState *pstate, Node *var, RangeTblEntry *rte,
						   char *colname, int location);
static Node *transformFields(ParseState *pstate, Node *basenode, List *fields,
							 int location);
static Node *filterAccessArg(ParseState *pstate, Node *expr, int location,
							 const char *types);
static Node *transformA_Const(ParseState *pstate, A_Const *a_con);
static Datum integerToJsonb(ParseState *pstate, int64 i, int location);
static Datum floatToJsonb(ParseState *pstate, char *f, int location);
static Jsonb *numericToJsonb(Numeric n);
static Datum stringToJsonb(ParseState *pstate, char *s, int location);
static Node *transformTypeCast(ParseState *pstate, TypeCast *tc);
static Node *transformCypherMapExpr(ParseState *pstate, CypherMapExpr *m);
static Node *transformCypherListExpr(ParseState *pstate, CypherListExpr *cl);
static Node *transformIndirection(ParseState *pstate, Node *basenode,
								  List *indirection);
static Node *makeArrayIndex(ParseState *pstate, Node *idx, bool exclusive);
static Node *adjustListIndexType(ParseState *pstate, Node *idx);
static Node *transformAExprOp(ParseState *pstate, A_Expr *a);
static Node *transformBoolExpr(ParseState *pstate, BoolExpr *b);
static Node *coerce_to_jsonb(ParseState *pstate, Node *expr,
							 const char *targetname);

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
		case T_A_Const:
			return transformA_Const(pstate, (A_Const *) expr);
		case T_TypeCast:
			return transformTypeCast(pstate, (TypeCast *) expr);
		case T_CypherMapExpr:
			return transformCypherMapExpr(pstate, (CypherMapExpr *) expr);
		case T_CypherListExpr:
			return transformCypherListExpr(pstate, (CypherListExpr *) expr);
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
	Node	   *field1;
	Node	   *field2 = NULL;
	Node	   *field3 = NULL;
	Oid			nspid1 = InvalidOid;
	Node	   *field4 = NULL;
	Oid			nspid2 = InvalidOid;
	ParseState *orig_pstate = pstate;
	Node	   *var = NULL;
	int			nindir;

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

	while (pstate != NULL)
	{
		ListCell   *lni;

		/* find the Var at the current level of ParseState */

		foreach(lni, pstate->p_namespace)
		{
			ParseNamespaceItem *nsitem = lfirst(lni);
			RangeTblEntry *rte = nsitem->p_rte;
			Oid			relid2;
			Oid			relid3;

			if (nsitem->p_lateral_only)
			{
				/* If not inside LATERAL, ignore lateral-only items. */
				if (!pstate->p_lateral_active)
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
				var = scanRTEForVar(orig_pstate, var, rte, strVal(field1),
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
				var = scanRTEForVar(orig_pstate, var, rte, strVal(field2),
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
				var = scanRTEForVar(orig_pstate, var, rte, strVal(field3),
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
				var = scanRTEForVar(orig_pstate, var, rte, strVal(field4),
									location);
				nindir = 3;
			}
		}

		if (var != NULL)
			break;

		pstate = pstate->parentParseState;
	}

	if (var == NULL)
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

		return transformFields(pstate, var, newfields, location);
	}

	return var;
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
			if (ind->is_slice)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("[..] not supported")));
				return NULL;
			}

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
			return coerce_to_jsonb(pstate, idx, "list index");
	}
}

static Node *
transformAExprOp(ParseState *pstate, A_Expr *a)
{
	Node	   *l;
	Node	   *r;

	l = transformCypherExprRecurse(pstate, a->lexpr);
	l = coerce_to_jsonb(pstate, l, "jsonb");

	r = transformCypherExprRecurse(pstate, a->rexpr);
	r = coerce_to_jsonb(pstate, r, "jsonb");

	return (Node *) make_op(pstate, a->name, l, r, a->location);
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
coerce_to_jsonb(ParseState *pstate, Node *expr, const char *targetname)
{
	if (expr == NULL)
		return NULL;

	switch (exprType(expr))
	{
		case VERTEXARRAYOID:
		case VERTEXOID:
		case EDGEARRAYOID:
		case EDGEOID:
		case GRAPHPATHARRAYOID:
		case GRAPHPATHOID:
		case EDGEREFARRAYOID:
		case EDGEREFOID:
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("graph object cannot be %s", targetname),
					 parser_errposition(pstate, exprLocation(expr))));
			return NULL;

		case JSONBOID:
			return expr;

		default:
			return ParseFuncOrColumn(pstate,
									 list_make1(makeString("to_jsonb")),
									 list_make1(expr), NULL,
									 exprLocation(expr));
	}
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
 * Item list functions
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
