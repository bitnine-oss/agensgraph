/*-------------------------------------------------------------------------
 *
 * deparse.c
 *                Query deparser for hadoop_fdw
 *
 * This file includes functions that examine query WHERE clauses to see
 * whether they're safe to send to the remote server for execution, as
 * well as functions to construct the query text to be sent.  We only 
 * need deparse logic for node types that we consider safe to send.
 *
 * Copyright (c) 2012-2013, BigSQL Development Group
 * Portions Copyright (c) 2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *                hadoop_fdw/src/deparse.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "hadoop_fdw.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "catalog/pg_type.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "parser/parsetree.h"
#include "optimizer/clauses.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/array.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"

typedef struct foreign_glob_cxt
{
	PlannerInfo *root;              /* global planner state */
	RelOptInfo *foreignrel;         /* the foreign relation we are planning for */
} foreign_glob_cxt;

typedef struct deparse_expr_cxt
{
	PlannerInfo	*root;               /* global planner state */
	RelOptInfo	*foreignrel;         /* the foreign relation we are planning for */
	StringInfo	buf;                 /* output buffer to append to */
	List		**params_list;       /* exprs that will become remote Params */
} deparse_expr_cxt;

#define REL_ALIAS_PREFIX	"r"
/* Handy macro to add relation name qualification */
#define ADD_REL_QUALIFIER(buf, varno)	\
		appendStringInfo((buf), "%s%d.", REL_ALIAS_PREFIX, (varno))

static bool foreign_expr_walker(Node *node, foreign_glob_cxt *glob_cxt);
static bool is_builtin(Oid oid);

static void deparseExpr(Expr *expr, deparse_expr_cxt *context);
static void deparseStringLiteral(StringInfo buf, const char *val);
static void deparseVar(Var *node, deparse_expr_cxt *context);
static void deparseConst(Const *node, deparse_expr_cxt *context);
static void deparseParam(Param *node, deparse_expr_cxt *context);
static void deparseFuncExpr(FuncExpr *node, deparse_expr_cxt *context);
static void deparseOpExpr(OpExpr *node, deparse_expr_cxt *context);
static void deparseBoolExpr(BoolExpr *node, deparse_expr_cxt *context);
static void deparseNullTest(NullTest *node, deparse_expr_cxt *context);
static void deparseColumnRef(StringInfo buf, int varno, int varattno,
				 PlannerInfo *root, bool qualify_col);
static void deparseRelabelType(RelabelType *node, deparse_expr_cxt *context);
static void deparseScalarArrayOpExpr(ScalarArrayOpExpr *node,
						 deparse_expr_cxt *context);
static void deparseTargetList(StringInfo buf, PlannerInfo *root, Index rtindex,
							  Relation rel, Bitmapset *attrs_used,
							  bool qualify_col, List **retrieved_attrs);
static void appendConditions(List *exprs, deparse_expr_cxt *context);
static void
deparseExplicitTargetList(List *tlist, List **retrieved_attrs,
						  deparse_expr_cxt *context);
void
appendWhereClause(PlannerInfo *root,
				  RelOptInfo *baserel,
				  List *exprs,
				  bool is_first,
				  List **params,
				  deparse_expr_cxt *context);
void
deparseFromExprForRel(StringInfo buf, PlannerInfo *root, RelOptInfo *foreignrel,
					  bool use_alias, List **params_list);
void
deparseSelectSql(PlannerInfo *root,
				 RelOptInfo *baserel,
				 Bitmapset *attrs_used,
				 List **retrieved_attrs,
				 List *tlist,
				 deparse_expr_cxt *context);

static bool
foreign_expr_walker(Node *node, foreign_glob_cxt *glob_cxt)
{
	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return true;

	switch (nodeTag(node)) {
		case T_Var:
			{
				Var	*var = (Var *) node;

				if (bms_is_member(var->varno, glob_cxt->foreignrel->relids) &&
					var->varlevelsup == 0)
				{
					if (var->varattno < 0 &&
						var->varattno != SelfItemPointerAttributeNumber)
						return false;
				}
			}
			break;
		case T_Const:
			{
				/* Constants are all OK */
			}
			break;
		case T_Param:
			{
				/* Parameters are all OK */
			}
			break;
		case T_FuncExpr:
			{
				FuncExpr   *fe = (FuncExpr *) node;

				/*
				 * If function used by the expression is not built-in, it
				 * can't be sent to remote because it might have incompatible
				 * semantics on remote side.
				 */
				if (!is_builtin(fe->funcid))
					return false;

				/*
				 * What do to with EXPLICIT cast functions? We return false
				 * for now
				 */
				if (fe->funcformat == COERCE_EXPLICIT_CAST)
					return false;

				/*
				 * Not all builtins can be sent to Hive. Additionally some builtins
				 * need to be translated as well. We use the logic in hive_funcs.c
				 * for this
				 */
				if (fe->funcformat != COERCE_IMPLICIT_CAST &&
											!is_hive_builtin(fe))
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) fe->args,
										 glob_cxt))
					return false;
			}
			break;
		case T_OpExpr:
			{
				OpExpr	*oe = (OpExpr *) node;

				/*
				 * Similarly, only built-in operators can be sent to remote.
				 * (If the operator is, surely its underlying function is
				 * too.)
				 */
				if (!is_builtin(oe->opno))
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) oe->args, glob_cxt))
					return false;
			}
			break;
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *oe = (ScalarArrayOpExpr *) node;

				/*
				 * Again, only built-in operators can be sent to remote.
				 */
				if (!is_builtin(oe->opno))
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) oe->args,
										 glob_cxt))
					return false;
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr   *b = (BoolExpr *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) b->args, glob_cxt))
					return false;
			}
			break;
		case T_NullTest:
			{
				NullTest   *nt = (NullTest *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) nt->arg, glob_cxt))
					return false;
			}
			break;
		case T_List:
			{
				List       *l = (List *) node;
				ListCell   *lc;

				/*
				 * Recurse to component subexpressions.
				 */
				foreach(lc, l)
				{
					if (!foreign_expr_walker((Node *) lfirst(lc), glob_cxt))
						return false;
				}
			}
			break;
		case T_RelabelType:
			{
				RelabelType *r = (RelabelType *) node;

				/*
				 * Recurse to input subexpression. We only allow
				 * IMPLICIT casts for now
				 */
				if (r->relabelformat != COERCE_IMPLICIT_CAST)
					return false;

				if (!foreign_expr_walker((Node *) r->arg,
										 glob_cxt))
					return false;
			}
			break;
		default:

			/*
			 * If it's anything else, assume it's unsafe.  This list can be
			 * expanded later, but don't forget to add deparse support below.
			 */
			return false;
	}


	/* It looks OK */
	return true;
}

static bool
is_builtin(Oid oid)
{
	return (oid < FirstBootstrapObjectId);
}

/*
 * Construct a simple SELECT statement that retrieves desired columns
 * of the specified foreign table, and append it to "buf".	The output
 * contains just "SELECT ...". The FROM tblname is appended elsewhere
 *
 * We also create an integer List of the columns being retrieved, which is
 * returned to *retrieved_attrs.
 */
void
deparseSelectSql(PlannerInfo *root,
				 RelOptInfo *baserel,
				 Bitmapset *attrs_used,
				 List **retrieved_attrs,
				 List *tlist,
				 deparse_expr_cxt *context)
{

	StringInfo buf = context->buf;
	/*
	 * Construct SELECT list
	 */
	appendStringInfoString(buf, "SELECT ");

	if (baserel->reloptkind == RELOPT_JOINREL)
	{
		/* For a join relation use the input tlist */
		deparseExplicitTargetList(tlist, retrieved_attrs, context);
	}
	else
	{
		RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
		Relation	rel;

		/*
		 * Core code already has some lock on each rel being planned, so we
		 * can use NoLock here.
		 */
		rel = heap_open(rte->relid, NoLock);

		deparseTargetList(buf, root, baserel->relid, rel, attrs_used,
						  false, retrieved_attrs);

		/* Construct FROM clause outside of this function..  */
		heap_close(rel, NoLock);
	}

}

/*
 * Emit a target list that retrieves the columns specified in attrs_used.
 *
 * The tlist text is appended to buf, and we also create an integer List
 * of the columns being retrieved, which is returned to *retrieved_attrs.
 */
static void
deparseTargetList(StringInfo buf,
				  PlannerInfo *root,
				  Index rtindex,
				  Relation rel,
				  Bitmapset *attrs_used,
				  bool qualify_col,
				  List **retrieved_attrs)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	bool		have_wholerow = false;
	bool		first;
	int			i;

	*retrieved_attrs = NIL;

	/*
	 * If there's a whole-row reference, we'll need all the columns.
	 * Because Hive uses Map-Reduce if explicit columns are passed, for
	 * the whole-row reference case, we pass a "*" instead
	 *
	 * Is the bms_num_members logic good enough? I think so, because the
	 * vars are added via walkers anyways, so other stuff cannot get in
	 * into this bitmap..
	 */
	if (attrs_used != NULL &&
			tupdesc->natts == bms_num_members(attrs_used))
			have_wholerow = true;
	else
		have_wholerow = true;

	first = true;
	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = tupdesc->attrs[i - 1];

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		/*
		 * We need to track retrieved_attrs in the wholerow
		 * case, but not print the individual columns
		 */
		if (have_wholerow ||
			bms_is_member(i - FirstLowInvalidHeapAttributeNumber,
						  attrs_used))
		{
			if (!have_wholerow)
			{
				if (!first)
					appendStringInfoString(buf, ", ");
				first = false;

				deparseColumnRef(buf, rtindex, i, root, qualify_col);
			}
			*retrieved_attrs = lappend_int(*retrieved_attrs, i);
		}
	}

	if (have_wholerow)
		appendStringInfoString(buf, "*");

	return;
}

bool
is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expr)
{
	foreign_glob_cxt glob_cxt;

	/*
	 * Check that the expression consists of nodes that are safe to execute
	 * remotely.
	 */
	glob_cxt.root = root;
	glob_cxt.foreignrel = baserel;
	if (!foreign_expr_walker((Node *) expr, &glob_cxt))
		return false;

	if (contain_mutable_functions((Node *) expr))
	{
		elog(DEBUG2, HADOOP_FDW_NAME
			 ": pushdown prevented because the results of mutable functions "
			 "are not stable.");
		return false;
	}


	elog(DEBUG5, HADOOP_FDW_NAME ": pushdown expression checks passed");

	/* OK to evaluate on the remote server */
	return true;
}

void
appendWhereClause(PlannerInfo *root,
				  RelOptInfo *baserel,
				  List *exprs,
				  bool is_first,
				  List **params,
				  deparse_expr_cxt *context)
{
	StringInfo buf;
	ListCell   *lc;

	if (params)
		*params = NIL;			/* initialize result list to empty */

	buf = context->buf;
	foreach(lc, exprs)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		/* Connect expressions with "AND" and parenthesize each condition. */
		if (is_first)
			appendStringInfoString(buf, " WHERE ");
		else
			appendStringInfoString(buf, " AND ");

		appendStringInfoChar(buf, '(');
		deparseExpr(ri->clause, context);
		appendStringInfoChar(buf, ')');

		is_first = false;
	}

}

static void
deparseExpr(Expr *node, deparse_expr_cxt *context)
{
	if (node == NULL)
		return;

	switch (nodeTag(node))
	{
		case T_Var:
			deparseVar((Var *) node, context);
			break;
		case T_Const:
			deparseConst((Const *) node, context);
			break;
		case T_Param:
			deparseParam((Param *) node, context);
			break;
		case T_FuncExpr:
			deparseFuncExpr((FuncExpr *) node, context);
			break;
		case T_OpExpr:
			deparseOpExpr((OpExpr *) node, context);
			break;
		case T_BoolExpr:
			deparseBoolExpr((BoolExpr *) node, context);
			break;
		case T_NullTest:
			deparseNullTest((NullTest *) node, context);
			break;
		case T_ScalarArrayOpExpr:
			deparseScalarArrayOpExpr((ScalarArrayOpExpr *) node, context);
			break;
		case T_RelabelType:
			deparseRelabelType((RelabelType *) node, context);
			break;
		default:
			elog(ERROR, "unsupported expression type for deparse: %d",
				 (int) nodeTag(node));
			break;
	}
}


/*
 * Deparse given ScalarArrayOpExpr expression.	To avoid problems
 * around priority of operations, we always parenthesize the arguments.
 */
static void
deparseScalarArrayOpExpr(ScalarArrayOpExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	tuple;
	Form_pg_operator form;
	Expr	   *arg1;
	Expr	   *arg2;
	char	   *oprname;

	elog(DEBUG4, HADOOP_FDW_NAME ": pushdown check for T_ScalarArrayOpExpr");

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator) GETSTRUCT(tuple);

	/* Sanity check. */
	Assert(list_length(node->args) == 2);

	/* Always parenthesize the expression. */
	appendStringInfoChar(buf, '(');

	/* Deparse left operand. */
	arg1 = linitial(node->args);
	deparseExpr(arg1, context);
	appendStringInfoChar(buf, ' ');

	/* Deparse operator name */
	oprname = NameStr(form->oprname);

	if (strcmp(oprname, "=") == 0 && node->useOr)
		appendStringInfo(buf, "IN (");
	else if (strcmp(oprname, "<>") == 0 && !node->useOr)
		appendStringInfo(buf, "NOT IN (");
	else
	{
		appendStringInfo(buf, "%s", NameStr(form->oprname));
		appendStringInfo(buf, " %s (", node->useOr ? "ANY" : "ALL");
	}

	/* Deparse right operand. */
	arg2 = lsecond(node->args);
	deparseExpr(arg2, context);

	appendStringInfoChar(buf, ')');

	/* Always parenthesize the expression. */
	appendStringInfoChar(buf, ')');

	ReleaseSysCache(tuple);
}


/*
 * Deparse a RelabelType (binary-compatible cast) node.
 */
static void
deparseRelabelType(RelabelType *node, deparse_expr_cxt *context)
{
	elog(DEBUG4, HADOOP_FDW_NAME ": pushdown check for T_RelabelType");

	deparseExpr(node->arg, context);
	if (node->relabelformat != COERCE_IMPLICIT_CAST)
		appendStringInfo(context->buf, "::%s",
						 format_type_with_typemod(node->resulttype,
											  node->resulttypmod));
}


static void
deparseVar(Var *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	bool		qualify_col = (context->foreignrel->reloptkind == RELOPT_JOINREL);

	elog(DEBUG4, HADOOP_FDW_NAME ": pushdown check for T_Var");

	if (bms_is_member(node->varno, context->foreignrel->relids) &&
		node->varlevelsup == 0)
	{
		/* Var belongs to foreign table */
		deparseColumnRef(buf, node->varno, node->varattno, context->root,
						 qualify_col);
	}
	else
	{
		/* Treat like a Param */
		if (context->params_list)
		{
			int			pindex = 0;
			ListCell   *lc;

			/* find its index in params_list */
			foreach(lc, *context->params_list)
			{
				pindex++;
				if (equal(node, (Node *) lfirst(lc)))
					break;
			}
			if (lc == NULL)
			{
				/* not in list, so add it */
				pindex++;
				*context->params_list = lappend(*context->params_list, node);
			}

			appendStringInfo(buf, "$%d", pindex);
			appendStringInfo(buf, "::%s",
							 format_type_with_typemod(node->vartype,
													  node->vartypmod));
		}
		else
		{
			appendStringInfo(buf, "(SELECT null::%s)",
							 format_type_with_typemod(node->vartype,
													  node->vartypmod));
		}
	}
}


static void
deparseConst(Const *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	Oid			typoutput;
	bool		typIsVarlena;
	char	   *extval;
	bool		iterate = false, isarray = false;
	Oid			iterator_result_type, field_type;
	Datum		value;
	bool		isnull;
	bool		need_delim = false;
	ArrayType  *arr;
	ArrayIterator array_iterator;

	elog(DEBUG4, HADOOP_FDW_NAME ": pushdown check for T_Const");

	if (node->constisnull)
	{
		/* no ::type business with Hive.. */
		appendStringInfo(buf, "NULL");
		return;
	}

	/* Array types need special handling.. */
	if (OidIsValid(get_element_type(node->consttype)))
	{

		arr = DatumGetArrayTypeP(node->constvalue);
		iterator_result_type = ARR_ELEMTYPE(arr);

		/* Create an iterator to step through the array */
#if PG_VERSION_NUM >= 90500
		array_iterator = array_create_iterator(arr, 0, NULL);
#else
		array_iterator = array_create_iterator(arr, 0);
#endif

		iterate = true;
		isarray = true;
		getTypeOutputInfo(iterator_result_type, &typoutput, &typIsVarlena);
		/* Get the first value to seed the do..while loop below */
		array_iterate(array_iterator, &value, &isnull);

		/* We really should not have NULLs in the array.. */
		if (!isnull)
			extval = OidOutputFunctionCall(typoutput, value);
		else
			extval = "NULL";

		field_type = iterator_result_type;
	}
	else
	{
		iterate = false;
		getTypeOutputInfo(node->consttype,
						  &typoutput, &typIsVarlena);
		extval = OidOutputFunctionCall(typoutput, node->constvalue);

		field_type = node->consttype;
	}

	do
	{
		if (need_delim)
			appendStringInfo(buf, ", ");

		/* Hive needs strings in array elements! */
		if (isarray)
			deparseStringLiteral(buf, extval);
		else
		{
			switch (field_type)
			{
				case INT2OID:
				case INT4OID:
				case INT8OID:
				case OIDOID:
				case FLOAT4OID:
				case FLOAT8OID:
				case NUMERICOID:
					{
						/*
						 * No need to quote unless it's a special value such as 'NaN'.
						 * See comments in get_const_expr().
						 */
						if (strspn(extval, "0123456789+-eE.") == strlen(extval))
						{
							if (extval[0] == '+' || extval[0] == '-')
								appendStringInfo(buf, "(%s)", extval);
							else
								appendStringInfoString(buf, extval);
						}
						else
							appendStringInfo(buf, "'%s'", extval);
					}
					break;
				case BITOID:
				case VARBITOID:
					appendStringInfo(buf, "B'%s'", extval);
					break;
				case BOOLOID:
					if (strcmp(extval, "t") == 0)
						appendStringInfoString(buf, "true");
					else
						appendStringInfoString(buf, "false");
					break;
				default:
					deparseStringLiteral(buf, extval);
					break;
			}
		}

		/* Iterate over the array elements */
		if (isarray && array_iterate(array_iterator, &value, &isnull))
		{
			if (!isnull)
				extval = OidOutputFunctionCall(typoutput, value);
			else
				extval = "NULL";
			need_delim = true;
		}
		else
			iterate = false;
	} while (iterate == true);

	if (isarray)
		array_free_iterator(array_iterator);

	/*
	 * There is no ::typename business with constants in Hive. We hope that
	 * Hive can implicitly understand the "right" type..
	 */
}

static void
deparseParam(Param *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	elog(DEBUG4, HADOOP_FDW_NAME ": pushdown check for T_Param");

	if (context->params_list)
	{
		int			pindex = 0;
		ListCell   *lc;

		/* find its index in params_list */
		foreach(lc, *context->params_list)
		{
			pindex++;
			if (equal(node, (Node *) lfirst(lc)))
				break;
		}
		if (lc == NULL)
		{
			/* not in list, so add it */
			pindex++;
			*context->params_list = lappend(*context->params_list, node);
		}

		appendStringInfo(buf, "$%d", pindex);
		appendStringInfo(buf, "::%s",
						 format_type_with_typemod(node->paramtype,
												  node->paramtypmod));
	}
	else
	{
		appendStringInfo(buf, "(SELECT null::%s)",
						 format_type_with_typemod(node->paramtype,
												  node->paramtypmod));
	}
}

/*
 * Deparse a function call.
 *
 * We special case some functions which need to be translated to be
 * usable on the Hive side of things as well.
 */
static void
deparseFuncExpr(FuncExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	proctup;
	Form_pg_proc procform;
	const char *proname, *fname;
	bool		first, skip_first_arg = false;
	ListCell   *arg;

	elog(DEBUG4, HADOOP_FDW_NAME ": pushdown check for T_FuncExpr");

	/*
	 * If the function call came from an implicit coercion, then just show the
	 * first argument.
	 */
	if (node->funcformat == COERCE_IMPLICIT_CAST)
	{
		deparseExpr((Expr *) linitial(node->args), context);
		return;
	}

	/*
	 * Normal function: display as proname(args).
	 */
	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(node->funcid));
	if (!HeapTupleIsValid(proctup))
		elog(ERROR, "cache lookup failed for function %u", node->funcid);
	procform = (Form_pg_proc) GETSTRUCT(proctup);

	/* Deparse the function name ... */
	fname = NameStr(procform->proname);
	proname = hive_translate_function(node, fname);
	appendStringInfo(buf, "%s(", quote_identifier(proname));
	/* ... and all the arguments */

	if (strcmp(fname, "date_part") == 0)
		skip_first_arg = true;

	first = true;
	foreach(arg, node->args)
	{
		if (skip_first_arg)
		{
			skip_first_arg = false;
			continue;
		}

		if (!first)
			appendStringInfoString(buf, ", ");
		deparseExpr((Expr *) lfirst(arg), context);
		first = false;
	}

	/* append a space if lpad, rpad */
	if ((strcmp(proname, "lpad") == 0 ||
			strcmp(proname, "rpad") == 0) &&
				list_length(node->args) == 2)
		appendStringInfoString(buf, ", ' '");

	appendStringInfoChar(buf, ')');

	ReleaseSysCache(proctup);
}

/*
 * Deparse given operator expression.	To avoid problems around
 * priority of operations, we always parenthesize the arguments.
 */
static void
deparseOpExpr(OpExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	tuple;
	Form_pg_operator form;
	char		oprkind;
	ListCell   *arg;
	char	   *oprname;

	elog(DEBUG4, HADOOP_FDW_NAME ": pushdown check for T_OpExpr");

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator) GETSTRUCT(tuple);
	oprkind = form->oprkind;

	/* Sanity check. */
	Assert((oprkind == 'r' && list_length(node->args) == 1) ||
		   (oprkind == 'l' && list_length(node->args) == 1) ||
		   (oprkind == 'b' && list_length(node->args) == 2));

	/* Always parenthesize the expression. */
	appendStringInfoChar(buf, '(');

	/* Deparse left operand. */
	if (oprkind == 'r' || oprkind == 'b')
	{
		arg = list_head(node->args);
		deparseExpr(lfirst(arg), context);
		appendStringInfoChar(buf, ' ');
	}

	/*
	 * Deparse operator name. Special case some operator
	 * names. Right now the list is small, so "if" checks
	 * like below are ok.
	 */
	oprname = NameStr(form->oprname);
	if (strcmp(oprname, "~~") == 0)
		oprname = "LIKE";
	if (strcmp(oprname, "!~~") == 0)
		oprname = "NOT LIKE";

	appendStringInfo(buf, "%s", oprname);

	/* Deparse right operand. */
	if (oprkind == 'l' || oprkind == 'b')
	{
		arg = list_tail(node->args);
		appendStringInfoChar(buf, ' ');
		deparseExpr(lfirst(arg), context);
	}

	appendStringInfoChar(buf, ')');

	ReleaseSysCache(tuple);
}

/*
 * Deparse a BoolExpr node.
 *
 * Note: by the time we get here, AND and OR expressions have been flattened
 * into N-argument form, so we'd better be prepared to deal with that.
 */
static void
deparseBoolExpr(BoolExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	const char *op = NULL;		/* keep compiler quiet */
	bool		first;
	ListCell   *lc;

	elog(DEBUG4, HADOOP_FDW_NAME ": pushdown check for T_BoolExpr");

	switch (node->boolop)
	{
		case AND_EXPR:
			op = "AND";
			break;
		case OR_EXPR:
			op = "OR";
			break;
		case NOT_EXPR:
			appendStringInfoString(buf, "(NOT ");
			deparseExpr(linitial(node->args), context);
			appendStringInfoChar(buf, ')');
			return;
	}

	appendStringInfoChar(buf, '(');
	first = true;
	foreach(lc, node->args)
	{
		if (!first)
			appendStringInfo(buf, " %s ", op);
		deparseExpr((Expr *) lfirst(lc), context);
		first = false;
	}
	appendStringInfoChar(buf, ')');
}

/*
 * Deparse IS [NOT] NULL expression.
 */
static void
deparseNullTest(NullTest *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	elog(DEBUG4, HADOOP_FDW_NAME ": pushdown check for T_NullTest");

	appendStringInfoChar(buf, '(');
	deparseExpr(node->arg, context);
	if (node->nulltesttype == IS_NULL)
		appendStringInfoString(buf, " IS NULL)");
	else
		appendStringInfoString(buf, " IS NOT NULL)");
}

/*
 * Append a SQL string literal representing "val" to buf.
 */
static void
deparseStringLiteral(StringInfo buf, const char *val)
{
	const char *valptr;

	/*
	 * Rather than making assumptions about the remote server's value of
	 * standard_conforming_strings, always use E'foo' syntax if there are any
	 * backslashes.  This will fail on remote servers before 8.1, but those
	 * are long out of support.
	 */
	if (strchr(val, '\\') != NULL)
		appendStringInfoChar(buf, ESCAPE_STRING_SYNTAX);
	appendStringInfoChar(buf, '\'');
	for (valptr = val; *valptr; valptr++)
	{
		char		ch = *valptr;

		if (SQL_STR_DOUBLE(ch, true))
			appendStringInfoChar(buf, ch);
		appendStringInfoChar(buf, ch);
	}
	appendStringInfoChar(buf, '\'');
}

static void
deparseColumnRef(StringInfo buf, int varno, int varattno, PlannerInfo *root, bool qualify_col)
{
	RangeTblEntry *rte;
	char	   *colname = NULL;
	List	   *options;
	ListCell   *lc;

	/* varno must not be any of OUTER_VAR, INNER_VAR and INDEX_VAR. */
	Assert(!IS_SPECIAL_VARNO(varno));

	/* Get RangeTblEntry from array in PlannerInfo. */
	rte = planner_rt_fetch(varno, root);

	/*
	 * If it's a column of a foreign table, and it has the column_name FDW
	 * option, use that value.
	 */
	options = GetForeignColumnOptions(rte->relid, varattno);
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "column_name") == 0)
		{
			colname = defGetString(def);
			break;
		}
	}

	/*
	 * If it's a column of a regular table or it doesn't have column_name FDW
	 * option, use attribute name.
	 */
	if (colname == NULL)
		colname = get_relid_attribute_name(rte->relid, varattno);

		if (qualify_col)
			ADD_REL_QUALIFIER(buf, varno);

	appendStringInfoString(buf, quote_identifier(colname));
}

/* Output join name for given join type */
extern const char *
get_jointype_name(JoinType jointype)
{
	switch (jointype)
	{
		case JOIN_INNER:
			return "INNER";

		case JOIN_LEFT:
			return "LEFT";

		case JOIN_RIGHT:
			return "RIGHT";

		case JOIN_FULL:
			return "FULL";

		default:
			/* Shouldn't come here, but protect from buggy code. */
			elog(ERROR, "unsupported join type %d", jointype);
	}

	/* Keep compiler happy */
	return NULL;
}

void
deparseFromExprForRel(StringInfo buf, PlannerInfo *root, RelOptInfo *foreignrel,
					  bool use_alias, List **params_list)
{
	hadoopFdwRelationInfo *fpinfo = (hadoopFdwRelationInfo *) foreignrel->fdw_private;

	if (foreignrel->reloptkind == RELOPT_JOINREL)
	{
		RelOptInfo *rel_o = fpinfo->outerrel;
		RelOptInfo *rel_i = fpinfo->innerrel;
		StringInfoData join_sql_o;
		StringInfoData join_sql_i;

		/* Deparse outer relation */
		initStringInfo(&join_sql_o);
		deparseFromExprForRel(&join_sql_o, root, rel_o, true, params_list);

		/* Deparse inner relation */
		initStringInfo(&join_sql_i);
		deparseFromExprForRel(&join_sql_i, root, rel_i, true, params_list);

		/*
		 * For a join relation FROM clause entry is deparsed as
		 *
		 * ((outer relation) <join type> (inner relation) ON (joinclauses)
		 */
		appendStringInfo(buf, "(%s %s JOIN %s ON ", join_sql_o.data,
					   get_jointype_name(fpinfo->jointype), join_sql_i.data);

		/* Append join clause; (TRUE) if no join clause */
		if (fpinfo->joinclauses)
		{
			deparse_expr_cxt context;

			context.buf = buf;
			context.foreignrel = foreignrel;
			context.root = root;
			context.params_list = params_list;

			appendStringInfo(buf, "(");
			appendConditions(fpinfo->joinclauses, &context);
			appendStringInfo(buf, ")");
		}
		else
			appendStringInfoString(buf, "(TRUE)");

		/* End the FROM clause entry. */
		appendStringInfo(buf, ")");
	}
	else
	{
		ForeignTable *table;
		Relation	rel;
		ListCell   *lc;
		const char *relname = NULL;
		RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, root);

		rel = heap_open(rte->relid, NoLock);
		table = GetForeignTable(RelationGetRelid(rel));

		/*
		 * Use value of FDW options if any, instead of the name of object itself.
		 */
		foreach(lc, table->options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "table") == 0)
				relname = defGetString(def);
		}

		appendStringInfo(buf, "%s", relname);

		/*
		 * Add a unique alias to avoid any conflict in relation names due to
		 * pulled up subqueries in the query being built for a pushed down
		 * join.
		 */
		if (use_alias)
			appendStringInfo(buf, " %s%d", REL_ALIAS_PREFIX, foreignrel->relid);

		heap_close(rel, NoLock);
	}
	return;
}

static void
appendConditions(List *exprs, deparse_expr_cxt *context)
{
	ListCell   *lc;
	bool		is_first = true;
	StringInfo	buf = context->buf;

	foreach(lc, exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);

		/*
		 * Extract clause from RestrictInfo, if required. See comments in
		 * declaration of PgFdwRelationInfo for details.
		 */
		if (IsA(expr, RestrictInfo))
		{
			RestrictInfo *ri = (RestrictInfo *) expr;

			expr = ri->clause;
		}

		/* Connect expressions with "AND" and parenthesize each condition. */
		if (!is_first)
			appendStringInfoString(buf, " AND ");

		appendStringInfoChar(buf, '(');
		deparseExpr(expr, context);
		appendStringInfoChar(buf, ')');

		is_first = false;
	}
}

/*
 * Build the targetlist for given relation to be deparsed as SELECT clause.
 *
 * The output targetlist contains the columns that need to be fetched from the
 * foreign server for the given relation.
 */
List *
build_tlist_to_deparse(RelOptInfo *foreignrel)
{
	List	   *tlist = NIL;
	hadoopFdwRelationInfo *fpinfo = (hadoopFdwRelationInfo *) foreignrel->fdw_private;

	/*
	 * We require columns specified in foreignrel->reltarget->exprs and those
	 * required for evaluating the local conditions.
	 */
#if (PG_VERSION_NUM < 90600)
	tlist = add_to_flat_tlist(tlist, foreignrel->reltargetlist);
	tlist = add_to_flat_tlist(tlist,
							  pull_var_clause((Node *) fpinfo->local_conds,
											  PVC_RECURSE_AGGREGATES,
											  PVC_RECURSE_PLACEHOLDERS));
#else
	tlist = add_to_flat_tlist(tlist, foreignrel->reltarget->exprs);
	tlist = add_to_flat_tlist(tlist,
							  pull_var_clause((Node *) fpinfo->local_conds,
											  PVC_RECURSE_PLACEHOLDERS));
#endif /* PG_VERSION_NUM < 90600 */

	return tlist;
}

static void
deparseExplicitTargetList(List *tlist, List **retrieved_attrs,
						  deparse_expr_cxt *context)
{
	ListCell   *lc;
	StringInfo	buf = context->buf;
	int			i = 0;

	*retrieved_attrs = NIL;

	foreach(lc, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Var		   *var;

		/* Extract expression if TargetEntry node */
		Assert(IsA(tle, TargetEntry));
		var = (Var *) tle->expr;
		/* We expect only Var nodes here */
		Assert(IsA(var, Var));

		if (i > 0)
			appendStringInfoString(buf, ", ");
		deparseVar(var, context);

		*retrieved_attrs = lappend_int(*retrieved_attrs, i + 1);

		i++;
	}

	if (i == 0)
		appendStringInfoString(buf, "NULL");
}

extern void
deparseSelectStmtForRel(StringInfo buf, PlannerInfo *root, RelOptInfo *baserel,
						List *remote_conds, List **retrieved_attrs, List **params_list,
						hadoopFdwRelationInfo *fpinfo, List *fdw_scan_tlist)
{
	deparse_expr_cxt context;
	/* Set up context struct for recursion */
	context.root = root;
	context.foreignrel = baserel;
	context.buf = buf;
	context.params_list = params_list;

	deparseSelectSql(root, baserel, fpinfo->attrs_used,
					 retrieved_attrs, fdw_scan_tlist, &context);
	appendStringInfo(buf, "%s", " FROM ");

	elog(DEBUG5, HADOOP_FDW_NAME ": built statement: \"%s\"", buf->data);

	deparseFromExprForRel(buf, root, baserel,
						  (baserel->reloptkind == RELOPT_JOINREL),
						  params_list);

	if (remote_conds)
	{
		elog(DEBUG3, HADOOP_FDW_NAME ": remote conditions found for pushdown");
		appendWhereClause(root, baserel, remote_conds,
						  true, params_list, &context);
	}
}
