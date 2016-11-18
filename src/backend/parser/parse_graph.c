/*
 * parse_graph.c
 *	  handle clauses for graph in parser
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/parser/parse_graph.c
 */

#include "postgres.h"

#include "ag_const.h"
#include "access/sysattr.h"
#include "catalog/ag_graph_fn.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "nodes/graphnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "parser/analyze.h"
#include "parser/parse_agg.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_cte.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_graph.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parser.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#define CYPHER_SUBQUERY_ALIAS	"_"
#define CYPHER_OPTLEFT_ALIAS	"_l"
#define CYPHER_OPTRIGHT_ALIAS	"_r"
#define CYPHER_VLR_WITH_ALIAS	"_vlr"
#define CYPHER_VLR_EDGE_ALIAS	"_e"

#define VLR_COLNAME_START		"start"
#define VLR_COLNAME_END			"end"
#define VLR_COLNAME_LEVEL		"level"
#define VLR_COLNAME_PATH		"path"

#define EDGE_UNION_START_ID		"_start"
#define EDGE_UNION_END_ID		"_end"

/* projection (RETURN and WITH) */
static void checkNameInItems(ParseState *pstate, List *items, List *targets);

/* MATCH - connected components */
static List *makeComponents(List *pattern);
static bool isPathConnectedTo(CypherPath *path, List *component);
static bool arePathsConnected(CypherPath *path1, CypherPath *path2);
static bool areNodesEqual(CypherNode *cnode1, CypherNode *cnode2);
/* MATCH - transform */
static Node *transformComponents(ParseState *pstate, List *components,
								 List **targetList);
static Node *transformMatchNode(ParseState *pstate, CypherNode *cnode,
								List **targetList, bool inPath);
static RangeTblEntry *transformMatchRel(ParseState *pstate, CypherRel *crel,
										List **targetList);
static RangeTblEntry *transformMatchSR(ParseState *pstate, CypherRel *crel,
									   List **targetList);
static void getCypherRelType(CypherRel *crel, char **typname, int *typloc);
static RangeTblEntry *transformMatchVLR(ParseState *pstate, CypherRel *crel,
										List **targetList);
static SelectStmt *genSelectLeftVLR(ParseState *pstate, CypherRel *crel);
static SelectStmt *genSelectRightVLR(CypherRel *crel);
static RangeSubselect *genEdgeUnionVLR(char *edge_label);
static SelectStmt *genSelectWithVLR(CypherRel *crel, WithClause *with);
static RangeTblEntry *addEdgeUnion(ParseState *pstate, char *edge_label,
								   int location, Alias *alias);
static Node *genEdgeUnion(char *edge_label, int location);
static Node *addQualRelPath(ParseState *pstate, Node *qual,
							CypherRel *prev_crel, RangeTblEntry *prev_edge,
							CypherRel *crel, RangeTblEntry *edge);
static Node *addQualForRel(ParseState *pstate, Node *qual, CypherRel *crel,
						   RangeTblEntry *edge);
static Node *addQualNodeIn(ParseState *pstate, Node *qual, Node *vertex,
						   CypherRel *crel, RangeTblEntry *edge, bool last);
static Node *addQualForNode(ParseState *pstate, Node *qual, CypherNode *cnode,
							Node *vertex);
static char *getEdgeColname(CypherRel *crel, bool last);
static Node *makePropMapQual(ParseState *pstate, Node *elem, Node *expr);
static Node *addQualUniqueEdges(ParseState *pstate, Node *qual, List *ueids,
								List *ueidarrs);
static RangeTblEntry *transformOptionalClause(ParseState *pstate,
											  CypherClause *clause);
static void getResCols(ParseState *pstate,
					   RangeTblEntry *l_rte, RangeTblEntry *r_rte,
					   List **res_colnames, List **res_colvars);

/* CREATE */
static List *transformCreatePattern(ParseState *pstate, List *pattern,
									List **targetList);
static GraphVertex *transformCreateNode(ParseState *pstate, CypherNode *cnode,
										List **targetList);
static GraphEdge *transformCreateRel(ParseState *pstate, CypherRel *crel,
									 List **targetList);

/* SET/REMOVE */
static List *transformSetPropList(ParseState *pstate, RangeTblEntry *rte,
								  List *items);
static GraphSetProp *transformSetProp(ParseState *pstate, RangeTblEntry *rte,
									  CypherSetProp *sp);

/* common */
static bool isNodeEmpty(CypherNode *cnode);
static bool isNodeForRef(CypherNode *cnode);
static Node *transformPropMap(ParseState *pstate, Node *expr,
							  ParseExprKind exprKind);
static Node *preprocessPropMap(Node *expr);

/* transform */
static RangeTblEntry *transformClause(ParseState *pstate, Node *clause,
									  Alias *alias, bool add);
static RangeTblEntry *transformVLRtoRTE(ParseState *pstate, SelectStmt *vlr,
										Alias *alias);
static void addRTEtoJoinlist(ParseState *pstate, RangeTblEntry *rte,
							 bool visible);
static Node *transformClauseJoin(ParseState *pstate, Node *clause,
								 Alias *alias, RangeTblEntry **rte,
								 ParseNamespaceItem **nsitem);
static RangeTblEntry *findRTEfromNamespace(ParseState *pstate, char *refname);
static List *makeTargetListFromRTE(ParseState *pstate, RangeTblEntry *rte);
static List *makeTargetListFromJoin(ParseState *pstate, RangeTblEntry *rte);
static TargetEntry *makeWholeRowTarget(ParseState *pstate, RangeTblEntry *rte);
static TargetEntry *findTarget(List *targetList, char *resname);
static Node *findLeftVar(ParseState *pstate, char *varname);

/* expression - type */
static Node *makeVertexExpr(ParseState *pstate, RangeTblEntry *rte,
							int location);
static Node *makeEdgeExpr(ParseState *pstate, RangeTblEntry *rte, int location);
static Node *makePathVertexExpr(ParseState *pstate, Node *obj);
static Node *makeGraphpath(List *vertices, List *edges, int location);
static Node *getElemField(ParseState *pstate, Node *elem, char *fname);
/* expression - common */
static Node *getColumnVar(ParseState *pstate, RangeTblEntry *rte,
						  char *colname);
static Node *getExprField(Expr *expr, char *fname);
static Alias *makeAliasNoDup(char *aliasname, List *colnames);
static Alias *makeAliasOptUnique(char *aliasname);
static Node *makeArrayExpr(Oid typarray, Oid typoid, List *elems);
static Node *makeTypedRowExpr(List *args, Oid typoid, int location);
static Node *qualAndExpr(ParseState *pstate, Node *qual, Node *expr);

/* parse node */
static ResTarget *makeSimpleResTarget(char *field, char *name);
static ResTarget *makeFieldsResTarget(List *fields, char *name);
static ResTarget *makeResTarget(Node *val, char *name);
static A_Const *makeIntConst(int val);

/* utils */
static char *genUniqueName(void);

Query *
transformCypherSubPattern(ParseState *pstate, CypherSubPattern *subpat)
{
	List	   *components;
	Query	   *qry;
	Node	   *qual;

	components = makeComponents(subpat->pattern);

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	qual = transformComponents(pstate, components, &qry->targetList);
	if (subpat->kind == CSP_SIZE)
	{
		FuncCall *fc;
		TargetEntry *te;

		fc = makeFuncCall(list_make1(makeString("count")), NIL, -1);
		fc->agg_star = true;

		pstate->p_next_resno = 1;
		te = transformTargetEntry(pstate, (Node *) fc, NULL,
								  EXPR_KIND_SELECT_TARGET, NULL, false);

		qry->targetList = list_make1(te);
	}
	markTargetListOrigins(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasAggs = pstate->p_hasAggs;
	if (qry->hasAggs)
		parseCheckAggregates(pstate, qry);

	assign_query_collations(pstate, qry);

	return qry;
}

Query *
transformCypherProjection(ParseState *pstate, CypherClause *clause)
{
	CypherProjection *detail = (CypherProjection *) clause->detail;
	Query	   *qry;
	RangeTblEntry *rte;
	Node	   *qual = NULL;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	if (detail->where != NULL)
	{
		Node *where = detail->where;

		AssertArg(detail->kind == CP_WITH);

		detail->where = NULL;
		rte = transformClause(pstate, (Node *) clause, NULL, true);
		detail->where = where;

		qry->targetList = makeTargetListFromRTE(pstate, rte);
		markTargetListOrigins(pstate, qry->targetList);

		qual = transformWhereClause(pstate, where, EXPR_KIND_WHERE, "WHERE");
	}
	else if (detail->distinct != NULL || detail->order != NULL ||
			 detail->skip != NULL || detail->limit != NULL)
	{
		List *distinct = detail->distinct;
		List *order = detail->order;
		Node *skip = detail->skip;
		Node *limit = detail->limit;

		/*
		 * detach options so that this funcion passes through this if statement
		 * when the function is called again recursively
		 */
		detail->distinct = NIL;
		detail->order = NIL;
		detail->skip = NULL;
		detail->limit = NULL;
		rte = transformClause(pstate, (Node *) clause, NULL, true);
		detail->distinct = distinct;
		detail->order = order;
		detail->skip = skip;
		detail->limit = limit;

		qry->targetList = makeTargetListFromRTE(pstate, rte);
		markTargetListOrigins(pstate, qry->targetList);

		qry->sortClause = transformSortClause(pstate, order, &qry->targetList,
											  EXPR_KIND_ORDER_BY, true, false);

		if (distinct == NIL)
		{
			/* intentionally blank, do nothing */
		}
		else if (linitial(distinct) == NULL)
		{
			qry->distinctClause = transformDistinctClause(pstate,
														  &qry->targetList,
														  qry->sortClause,
														  false);
		}
		else
		{
			qry->distinctClause = transformDistinctOnClause(pstate, distinct,
															&qry->targetList,
															qry->sortClause);
			qry->hasDistinctOn = true;
		}

		qry->limitOffset = transformLimitClause(pstate, skip, EXPR_KIND_OFFSET,
												"OFFSET");
		qry->limitCount = transformLimitClause(pstate, limit, EXPR_KIND_LIMIT,
											   "LIMIT");
	}
	else
	{
		if (clause->prev != NULL)
			transformClause(pstate, clause->prev, NULL, true);

		qry->targetList = transformTargetList(pstate, detail->items,
											  EXPR_KIND_SELECT_TARGET);

		if (detail->kind == CP_WITH)
			checkNameInItems(pstate, detail->items, qry->targetList);

		markTargetListOrigins(pstate, qry->targetList);

		qry->groupClause = generateGroupClause(pstate, &qry->targetList,
											   qry->sortClause);
	}

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasAggs = pstate->p_hasAggs;
	if (qry->hasAggs)
		parseCheckAggregates(pstate, qry);

	assign_query_collations(pstate, qry);

	return qry;
}

Query *
transformCypherMatchClause(ParseState *pstate, CypherClause *clause)
{
	CypherMatchClause *detail = (CypherMatchClause *) clause->detail;
	Query	   *qry;
	RangeTblEntry *rte;
	Node	   *qual = NULL;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	if (detail->optional && clause->prev != NULL)
	{
		rte = transformOptionalClause(pstate, clause);

		qry->targetList = makeTargetListFromJoin(pstate, rte);
	}
	else
	{
		if (detail->where != NULL)
		{
			Node *where = detail->where;

			/*
			 * detach WHERE clause so that this funcion passes through
			 * this if statement when the function is called again recursively
			 */
			detail->where = NULL;
			rte = transformClause(pstate, (Node *) clause, NULL, true);
			detail->where = where;

			qry->targetList = makeTargetListFromRTE(pstate, rte);

			qual = transformWhereClause(pstate, where, EXPR_KIND_WHERE,
										"WHERE");
		}
		else
		{
			List *components = makeComponents(detail->pattern);

			if (clause->prev != NULL)
			{
				rte = transformClause(pstate, clause->prev, NULL, true);

				/*
				 * To do this at here is safe since it just uses transformed
				 * expression and does not look over the ancestors of `pstate`.
				 */
				qry->targetList = makeTargetListFromRTE(pstate, rte);
			}

			qual = transformComponents(pstate, components, &qry->targetList);
		}
	}

	markTargetListOrigins(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;

	assign_query_collations(pstate, qry);

	return qry;
}

/* See transformFromClauseItem() */
static RangeTblEntry *
transformOptionalClause(ParseState *pstate, CypherClause *clause)
{
	CypherMatchClause *detail = (CypherMatchClause *) clause->detail;
	JoinExpr   *j = makeNode(JoinExpr);
	Alias	   *l_alias;
	RangeTblEntry *l_rte;
	ParseNamespaceItem *l_nsitem;
	Node	   *prevclause;
	Alias	   *r_alias;
	RangeTblEntry *r_rte;
	ParseNamespaceItem *r_nsitem;
	List	   *res_colnames = NIL;
	List	   *res_colvars = NIL;
	RangeTblEntry *rte;
	ParseNamespaceItem *nsitem;
	int			i;

	j->jointype = JOIN_LEFT;

	/* transform LEFT */
	l_alias = makeAliasNoDup(CYPHER_OPTLEFT_ALIAS, NIL);
	j->larg = transformClauseJoin(pstate, clause->prev, l_alias, &l_rte,
								  &l_nsitem);

	/* temporal namespace for RIGHT */
	Assert(pstate->p_namespace == NIL);
	pstate->p_namespace = lappend(pstate->p_namespace, l_nsitem);

	/* Transform RIGHT. Prevent `clause` from being transformed infinitely. */

	prevclause = clause->prev;
	clause->prev = NULL;
	detail->optional = false;

	pstate->p_opt_match = true;
	pstate->p_lateral_active = true;

	r_alias = makeAliasNoDup(CYPHER_OPTRIGHT_ALIAS, NIL);
	j->rarg = transformClauseJoin(pstate, (Node *) clause, r_alias,
								  &r_rte, &r_nsitem);

	pstate->p_lateral_active = false;
	pstate->p_opt_match = false;

	detail->optional = true;
	clause->prev = prevclause;

	pstate->p_namespace = NIL;

	j->quals = makeBoolConst(true, false);

	j->alias = makeAliasNoDup(CYPHER_SUBQUERY_ALIAS, NIL);

	getResCols(pstate, l_rte, r_rte, &res_colnames, &res_colvars);
	rte = addRangeTableEntryForJoin(pstate, res_colnames, j->jointype,
									res_colvars, j->alias, true);
	j->rtindex = RTERangeTablePosn(pstate, rte, NULL);

	for (i = list_length(pstate->p_joinexprs) + 1; i < j->rtindex; i++)
		pstate->p_joinexprs = lappend(pstate->p_joinexprs, NULL);
	pstate->p_joinexprs = lappend(pstate->p_joinexprs, j);
	Assert(list_length(pstate->p_joinexprs) == j->rtindex);

	pstate->p_joinlist = lappend(pstate->p_joinlist, j);

	nsitem = palloc(sizeof(*nsitem));
	nsitem->p_rte = rte;
	nsitem->p_rel_visible = true;
	nsitem->p_cols_visible = true;
	nsitem->p_lateral_only = false;
	nsitem->p_lateral_ok = true;
	pstate->p_namespace = lappend(pstate->p_namespace, nsitem);

	return rte;
}

static void
getResCols(ParseState *pstate,RangeTblEntry *l_rte, RangeTblEntry *r_rte,
		   List **res_colnames, List **res_colvars)
{
	List	   *l_colnames;
	List	   *l_colvars;
	List	   *r_colnames;
	List	   *r_colvars;
	ListCell   *r_lname;
	ListCell   *r_lvar;
	List	   *colnames = NIL;
	List	   *colvars = NIL;

	expandRTE(l_rte, RTERangeTablePosn(pstate, l_rte, NULL), 0, -1, false,
			  &l_colnames, &l_colvars);
	expandRTE(r_rte, RTERangeTablePosn(pstate, r_rte, NULL), 0, -1, false,
			  &r_colnames, &r_colvars);

	*res_colnames = list_concat(*res_colnames, l_colnames);
	*res_colvars = list_concat(*res_colvars, l_colvars);

	forboth(r_lname, r_colnames, r_lvar, r_colvars)
	{
		char	   *r_colname = strVal(lfirst(r_lname));
		ListCell   *lname;
		ListCell   *lvar;
		Var		   *var = NULL;

		forboth(lname, *res_colnames, lvar, *res_colvars)
		{
			char *colname = strVal(lfirst(lname));

			if (strcmp(r_colname, colname) == 0)
			{
				var = lfirst(lvar);
				break;
			}
		}

		if (var == NULL)
		{
			colnames = lappend(colnames, lfirst(r_lname));
			colvars = lappend(colvars, lfirst(r_lvar));
		}
		else
		{
			Var		   *r_var = lfirst(r_lvar);
			Oid			vartype;
			Oid			r_vartype;

			vartype = exprType((Node *) var);
			r_vartype = exprType((Node *) r_var);
			if (vartype != r_vartype)
			{
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("variable type mismatch")));
			}
			if (vartype != VERTEXOID && vartype != EDGEOID)
			{
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("expected node or relationship")));
			}
		}
	}

	*res_colnames = list_concat(*res_colnames, colnames);
	*res_colvars = list_concat(*res_colvars, colvars);
}

Query *
transformCypherCreateClause(ParseState *pstate, CypherClause *clause)
{
	CypherCreateClause *detail;
	CypherClause *prevclause;
	List	   *pattern;
	Query	   *qry;

	detail = (CypherCreateClause *) clause->detail;
	pattern = detail->pattern;
	prevclause = (CypherClause *) clause->prev;

	/* merge previous CREATE clauses into current CREATE clause */
	while (prevclause != NULL &&
		   cypherClauseTag(prevclause) == T_CypherCreateClause)
	{
		List *prevpattern;

		detail = (CypherCreateClause *) prevclause->detail;

		prevpattern = list_copy(detail->pattern);
		pattern = list_concat(prevpattern, pattern);

		prevclause = (CypherClause *) prevclause->prev;
	}

	qry = makeNode(Query);
	qry->commandType = CMD_GRAPHWRITE;
	qry->graph.writeOp = GWROP_CREATE;
	qry->graph.last = (pstate->parentParseState == NULL);

	if (prevclause != NULL)
	{
		RangeTblEntry *rte;

		rte = transformClause(pstate, (Node *) prevclause, NULL, true);

		qry->targetList = makeTargetListFromRTE(pstate, rte);
	}

	qry->graph.pattern = transformCreatePattern(pstate, pattern,
												&qry->targetList);
	markTargetListOrigins(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);

	qry->hasSubLinks = pstate->p_hasSubLinks;

	assign_query_collations(pstate, qry);

	return qry;
}

Query *
transformCypherDeleteClause(ParseState *pstate, CypherClause *clause)
{
	CypherDeleteClause *detail = (CypherDeleteClause *) clause->detail;
	Query	   *qry;
	RangeTblEntry *rte;
	List	   *exprs;
	ListCell   *le;

	/* DELETE cannot be the first clause */
	AssertArg(clause->prev != NULL);

	qry = makeNode(Query);
	qry->commandType = CMD_GRAPHWRITE;
	qry->graph.writeOp = GWROP_DELETE;
	qry->graph.last = (pstate->parentParseState == NULL);
	qry->graph.detach = detail->detach;

	/*
	 * Instead of `resultRelation`, use FROM list because there might be
	 * multiple labels to access.
	 */
	rte = transformClause(pstate, clause->prev, NULL, true);

	/* select all from previous clause */
	qry->targetList = makeTargetListFromRTE(pstate, rte);

	exprs = transformExpressionList(pstate, detail->exprs, EXPR_KIND_OTHER);
	foreach(le, exprs)
	{
		Node	   *expr = lfirst(le);
		Oid			vartype;

		vartype = exprType(expr);
		if (vartype != VERTEXOID && vartype != EDGEOID &&
			vartype != GRAPHPATHOID)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("expected node, relationship, or path"),
					 parser_errposition(pstate, exprLocation(expr))));

		/*
		 * TODO: expr must contain one of the target variables
		 *		 and it mustn't contain aggregate and SubLink's.
		 */
	}
	qry->graph.exprs = exprs;

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);

	qry->hasSubLinks = pstate->p_hasSubLinks;

	assign_query_collations(pstate, qry);

	return qry;
}

Query *
transformCypherSetClause(ParseState *pstate, CypherClause *clause)
{
	CypherSetClause *detail = (CypherSetClause *) clause->detail;
	Query	   *qry;
	RangeTblEntry *rte;

	/* SET/REMOVE cannot be the first clause */
	AssertArg(clause->prev != NULL);

	qry = makeNode(Query);
	qry->commandType = CMD_GRAPHWRITE;
	qry->graph.writeOp = GWROP_SET;
	qry->graph.last = (pstate->parentParseState == NULL);

	rte = transformClause(pstate, clause->prev, NULL, true);

	qry->targetList = makeTargetListFromRTE(pstate, rte);

	qry->graph.sets = transformSetPropList(pstate, rte, detail->items);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);

	qry->hasSubLinks = pstate->p_hasSubLinks;

	assign_query_collations(pstate, qry);

	return qry;
}

Query *
transformCypherLoadClause(ParseState *pstate, CypherClause *clause)
{
	CypherLoadClause *detail = (CypherLoadClause *) clause->detail;
	RangeVar   *rv = detail->relation;
	Query	   *qry;
	RangeTblEntry *rte;
	TargetEntry *te;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	if (clause->prev != NULL)
	{
		rte = transformClause(pstate, clause->prev, NULL, true);

		qry->targetList = makeTargetListFromRTE(pstate, rte);
	}

	if (findTarget(qry->targetList, rv->alias->aliasname) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_ALIAS),
				 errmsg("duplicate variable \"%s\"", rv->alias->aliasname)));

	rte = addRangeTableEntry(pstate, rv, rv->alias,
							 interpretInhOption(rv->inhOpt), true);
	addRTEtoJoinlist(pstate, rte, false);

	te = makeWholeRowTarget(pstate, rte);
	qry->targetList = lappend(qry->targetList, te);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);

	assign_query_collations(pstate, qry);

	return qry;
}

/* check whether resulting columns have a name or not */
static void
checkNameInItems(ParseState *pstate, List *items, List *targets)
{
	ListCell   *li;
	ListCell   *lt;

	forboth(li, items, lt, targets)
	{
		ResTarget *res = lfirst(li);
		TargetEntry *target = lfirst(lt);

		if (res->name != NULL)
			continue;

		if (!IsA(target->expr, Var))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("expression in WITH must be aliased (use AS)"),
					 parser_errposition(pstate, exprLocation(res->val))));
	}
}

/* make connected components */
static List *
makeComponents(List *pattern)
{
	List	   *components = NIL;
	ListCell   *lp;

	foreach(lp, pattern)
	{
		CypherPath *p = lfirst(lp);
		List	   *repr;
		ListCell   *lc;
		List	   *c;
		ListCell   *prev;

		/* find the first connected component */
		repr = NIL;
		foreach(lc, components)
		{
			c = lfirst(lc);

			if (isPathConnectedTo(p, c))
			{
				repr = c;
				break;
			}
		}

		/*
		 * if there is no matched connected component,
		 * make a new connected component which is a list of CypherPath's
		 */
		if (repr == NIL)
		{
			c = list_make1(p);
			components = lappend(components, c);
			continue;
		}

		/* find other connected components and merge them to `repr` */
		prev = lc;
		for_each_cell(lc, lnext(lc))
		{
			c = lfirst(lc);

			if (isPathConnectedTo(p, c))
			{
				list_concat(repr, c);

				list_delete_cell(components, lc, prev);
				lc = prev;
			}
			else
			{
				prev = lc;
			}
		}

		/* add the path to `repr` */
		repr = lappend(repr, p);
	}

	return components;
}

static bool
isPathConnectedTo(CypherPath *path, List *component)
{
	ListCell *lp;

	foreach(lp, component)
	{
		CypherPath *p = lfirst(lp);

		if (arePathsConnected(p, path))
			return true;
	}

	return false;
}

static bool
arePathsConnected(CypherPath *path1, CypherPath *path2)
{
	ListCell *le1;

	foreach(le1, path1->chain)
	{
		CypherNode *cnode1 = lfirst(le1);
		ListCell   *le2;

		/* node variables are the only concern */
		if (!IsA(cnode1, CypherNode))
			continue;

		/* treat it as a unique node */
		if (getCypherName(cnode1->variable) == NULL)
			continue;

		foreach(le2, path2->chain)
		{
			CypherNode *cnode2 = lfirst(le2);

			if (!IsA(cnode2, CypherNode))
				continue;

			if (getCypherName(cnode2->variable) == NULL)
				continue;

			if (areNodesEqual(cnode1, cnode2))
				return true;
		}
	}

	return false;
}

static bool
areNodesEqual(CypherNode *cnode1, CypherNode *cnode2)
{
	char	   *varname1 = getCypherName(cnode1->variable);
	char	   *varname2 = getCypherName(cnode2->variable);
	char	   *label1;
	char	   *label2;

	Assert(varname1 != NULL);
	Assert(varname2 != NULL);

	if (strcmp(varname1, varname2) != 0)
		return false;

	label1 = getCypherName(cnode1->label);
	label2 = getCypherName(cnode2->label);

	if (label1 == NULL && label2 == NULL)
		return true;

	if ((label1 == NULL || label2 == NULL) || strcmp(label1, label2) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("label conflict on node \"%s\"", varname1)));

	return true;
}

static Node *
transformComponents(ParseState *pstate, List *components, List **targetList)
{
	Node	   *qual = NULL;
	ListCell   *lc;

	foreach(lc, components)
	{
		List	   *c = lfirst(lc);
		ListCell   *lp;
		List	   *ueids = NIL;
		List	   *ueidarrs = NIL;

		foreach(lp, c)
		{
			CypherPath *p = lfirst(lp);
			char	   *pathname = getCypherName(p->variable);
			int			pathloc = getCypherNameLoc(p->variable);
			bool		out = (pathname != NULL);
			ListCell   *le;
			CypherNode *cnode;
			Node	   *vertex;
			CypherRel  *prev_crel = NULL;
			RangeTblEntry *prev_edge = NULL;
			List	   *pvs = NIL;
			List	   *pes = NIL;

			if (pathname != NULL && pstate->p_opt_match)
			{
				if (findLeftVar(pstate, pathname) != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_DUPLICATE_ALIAS),
							 errmsg("duplicate variable \"%s\"", pathname),
							 parser_errposition(pstate, pathloc)));
			}
			if (findTarget(*targetList, pathname) != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_ALIAS),
						 errmsg("duplicate variable \"%s\"", pathname),
						 parser_errposition(pstate, pathloc)));

			le = list_head(p->chain);
			for (;;)
			{
				CypherRel  *crel;
				RangeTblEntry *edge;

				cnode = lfirst(le);
				vertex = transformMatchNode(pstate, cnode, targetList, out);

				le = lnext(le);
				/* no more pattern chain (<rel,node> pair) */
				if (le == NULL)
					break;

				crel = lfirst(le);
				if (out && crel->varlen != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("VLR in path is not supported"),
							 parser_errposition(pstate, pathloc)));

				pstate->p_last_vertex = vertex;
				edge = transformMatchRel(pstate, crel, targetList);

				/* join */
				qual = addQualRelPath(pstate, qual,
									  prev_crel, prev_edge, crel, edge);
				qual = addQualNodeIn(pstate, qual, vertex, crel, edge, false);

				/* constraints */
				qual = addQualForRel(pstate, qual, crel, edge);
				qual = addQualForNode(pstate, qual, cnode, vertex);

				/* uniqueness */
				if (crel->varlen == NULL)
				{
					Node *eid;

					eid = getColumnVar(pstate, edge, AG_ELEM_LOCAL_ID);
					ueids = list_append_unique(ueids, eid);
				}
				else
				{
					Node *eidarr;

					eidarr = getColumnVar(pstate, edge, "path");
					ueidarrs = list_append_unique(ueidarrs, eidarr);
				}

				if (out)
				{
					Assert(vertex != NULL);
					pvs = lappend(pvs, makePathVertexExpr(pstate, vertex));
					pes = lappend(pes, makeEdgeExpr(pstate, edge, -1));
				}

				prev_crel = crel;
				prev_edge = edge;

				le = lnext(le);
			}

			qual = addQualNodeIn(pstate, qual, vertex, prev_crel, prev_edge,
								 true);
			qual = addQualForNode(pstate, qual, cnode, vertex);

			if (out)
			{
				Node *graphpath;
				TargetEntry *te;

				Assert(vertex != NULL);
				pvs = lappend(pvs, makePathVertexExpr(pstate, vertex));

				graphpath = makeGraphpath(pvs, pes, pathloc);
				te = makeTargetEntry((Expr *) graphpath,
									 (AttrNumber) pstate->p_next_resno++,
									 pstrdup(pathname),
									 false);

				*targetList = lappend(*targetList, te);
			}
		}

		qual = addQualUniqueEdges(pstate, qual, ueids, ueidarrs);
	}

	return qual;
}

static Node *
transformMatchNode(ParseState *pstate, CypherNode *cnode, List **targetList,
				   bool inPath)
{
	char	   *varname = getCypherName(cnode->variable);
	int			varloc = getCypherNameLoc(cnode->variable);
	TargetEntry *te;
	char	   *labname;
	int			labloc;
	RangeVar   *r;
	Alias	   *alias;
	RangeTblEntry *rte;

	/* this node is just a placeholder for a relationship */
	if (isNodeEmpty(cnode) && !inPath)
		return NULL;

	/*
	 * If a vertex with the same variable is already in the target list,
	 * - the vertex is from the previous clause or
	 * - nodes with the same variable in the pattern are already processed,
	 * so skip `cnode`.
	 * `cnode` without variable is considered as if it has a unique variable,
	 * so process it.
	 */
	te = findTarget(*targetList, varname);
	if (te != NULL)
	{
		if (exprType((Node *) te->expr) != VERTEXOID)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_ALIAS),
					 errmsg("duplicate variable \"%s\"", varname),
					 parser_errposition(pstate, varloc)));

		if (pstate->p_opt_match)
			return (Node *) te;

		rte = findRTEfromNamespace(pstate, varname);
		if (rte == NULL)
			return (Node *) te;		/* from the previous clause */
		else
			return (Node *) rte;	/* in the pattern */
	}

	if (varname != NULL && pstate->p_opt_match)
	{
		Node *res;

		res = findLeftVar(pstate, varname);
		if (res != NULL)
		{
			if (exprType(res) != VERTEXOID)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_ALIAS),
						 errmsg("duplicate variable \"%s\"", varname),
						 parser_errposition(pstate, varloc)));

			te = makeTargetEntry((Expr *) res,
								 (AttrNumber) pstate->p_next_resno++,
								 pstrdup(varname),
								 false);

			*targetList = lappend(*targetList, te);

			return (Node *) te;
		}
	}

	/* find the variable when this pattern is within a subquery or a sublink */
	if (isNodeForRef(cnode))
	{
		Node *col;

		col = colNameToVar(pstate, varname, false, varloc);
		if (col != NULL)
		{
			if (exprType(col) != VERTEXOID)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_ALIAS),
						 errmsg("duplicate variable \"%s\"", varname),
						 parser_errposition(pstate, varloc)));

			te = makeTargetEntry((Expr *) col,
								 (AttrNumber) pstate->p_next_resno++,
								 pstrdup(varname),
								 false);

			*targetList = lappend(*targetList, te);

			return (Node *) te;
		}
	}

	/*
	 * process the newly introduced node
	 */

	labname = getCypherName(cnode->label);
	if (labname == NULL)
		labname = AG_VERTEX;
	labloc = getCypherNameLoc(cnode->label);

	r = makeRangeVar(get_graph_path(), labname, labloc);
	alias = makeAliasOptUnique(varname);

	/* always set `ihn` to true because we should scan all derived tables */
	rte = addRangeTableEntry(pstate, r, alias, true, true);
	addRTEtoJoinlist(pstate, rte, false);

	if (varname != NULL)
	{
		te = makeTargetEntry((Expr *) makeVertexExpr(pstate, rte, varloc),
							 (AttrNumber) pstate->p_next_resno++,
							 pstrdup(varname),
							 false);

		*targetList = lappend(*targetList, te);
	}

	return (Node *) rte;
}

static RangeTblEntry *
transformMatchRel(ParseState *pstate, CypherRel *crel, List **targetList)
{
	char	   *varname = getCypherName(crel->variable);
	int			varloc = getCypherNameLoc(crel->variable);

	/* all relationships must be unique */
	if (findTarget(*targetList, varname) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_ALIAS),
				 errmsg("duplicate variable \"%s\"", varname),
				 parser_errposition(pstate, varloc)));

	if (varname != NULL && pstate->p_opt_match)
	{
		if (findLeftVar(pstate, varname) != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_ALIAS),
					 errmsg("duplicate variable \"%s\"", varname),
					 parser_errposition(pstate, varloc)));
	}

	if (crel->varlen == NULL)
		return transformMatchSR(pstate, crel, targetList);
	else
		return transformMatchVLR(pstate, crel, targetList);
}

static RangeTblEntry *
transformMatchSR(ParseState *pstate, CypherRel *crel, List **targetList)
{
	char	   *varname = getCypherName(crel->variable);
	int			varloc = getCypherNameLoc(crel->variable);
	char	   *typname;
	int			typloc;
	Alias	   *alias;
	RangeTblEntry *rte;

	getCypherRelType(crel, &typname, &typloc);

	alias = makeAliasOptUnique(varname);

	if (crel->direction == CYPHER_REL_DIR_NONE)
	{
		rte = addEdgeUnion(pstate, typname, typloc, alias);
	}
	else
	{
		RangeVar *r;

		r = makeRangeVar(get_graph_path(), typname, typloc);

		rte = addRangeTableEntry(pstate, r, alias, true, true);
	}
	addRTEtoJoinlist(pstate, rte, false);

	if (varname != NULL)
	{
		TargetEntry *te;

		te = makeTargetEntry((Expr *) makeEdgeExpr(pstate, rte, varloc),
							 (AttrNumber) pstate->p_next_resno++,
							 pstrdup(varname),
							 false);

		*targetList = lappend(*targetList, te);
	}

	return rte;
}

static void
getCypherRelType(CypherRel *crel, char **typname, int *typloc)
{
	if (crel->types == NIL)
	{
		*typname = AG_EDGE;
		if (typloc != NULL)
			*typloc = -1;
	}
	else
	{
		Node *type;

		if (list_length(crel->types) > 1)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("multiple types for relationship not supported")));

		type = linitial(crel->types);

		*typname = getCypherName(type);
		if (typloc != NULL)
			*typloc = getCypherNameLoc(type);
	}
}

static RangeTblEntry *
transformMatchVLR(ParseState *pstate, CypherRel *crel, List **targetList)
{
	char	   *varname = getCypherName(crel->variable);
	SelectStmt *u;
	CommonTableExpr *cte;
	WithClause *with;
	SelectStmt *vlr;
	Alias	   *alias;
	RangeTblEntry *rte;

	/* UNION ALL */
	u = makeNode(SelectStmt);
	u->op = SETOP_UNION;
	u->all = true;
	u->larg = genSelectLeftVLR(pstate, crel);
	u->rarg = genSelectRightVLR(crel);

	cte = makeNode(CommonTableExpr);
	cte->ctename = CYPHER_VLR_WITH_ALIAS;
	cte->aliascolnames = list_make4(makeString(VLR_COLNAME_START),
									makeString(VLR_COLNAME_END),
									makeString(VLR_COLNAME_LEVEL),
									makeString(VLR_COLNAME_PATH));
	cte->ctequery = (Node *) u;
	cte->location = -1;

	with = makeNode(WithClause);
	with->ctes = list_make1(cte);
	with->recursive = true;
	with->location = -1;

	vlr = genSelectWithVLR(crel, with);

	alias = makeAliasOptUnique(varname);
	rte = transformVLRtoRTE(pstate, vlr, alias);

	if (varname != NULL)
	{
		TargetEntry *te;
		Node *var;

		var = getColumnVar(pstate, rte, VLR_COLNAME_PATH);
		te = makeTargetEntry((Expr *) var,
							 (AttrNumber) pstate->p_next_resno++,
							 pstrdup(varname),
							 false);

		*targetList = lappend(*targetList, te);
	}

	return rte;
}

/*
 * -- level == 0
 * VALUES (`id(vertex)`, `id(vertex)`, 0, ARRAY[]::graphid[])
 *
 * -- level > 0, CYPHER_REL_DIR_LEFT
 * SELECT start, "end", 1, ARRAY[id]
 * FROM `get_graph_path()`.`typname`
 * WHERE "end" = `id(vertex)` AND properties @> `crel->prop_map`
 *
 * -- level > 0, CYPHER_REL_DIR_RIGHT
 * SELECT start, "end", 1, ARRAY[id]
 * FROM `get_graph_path()`.`typname`
 * WHERE start = `id(vertex)` AND properties @> `crel->prop_map`
 *
 * -- level > 0, CYPHER_REL_DIR_NONE
 * SELECT start, "end", 1, ARRAY[id]
 * FROM `genEdgeUnionVLR(typname)`
 * WHERE start = `id(vertex)` AND properties @> `crel->prop_map`
 */
static SelectStmt *
genSelectLeftVLR(ParseState *pstate, CypherRel *crel)
{
	A_Indices  *indices = (A_Indices *) crel->varlen;
	Node	   *vertex;
	Node	   *vid;
	bool		zero = false;
	A_ArrayExpr *patharr;
	char	   *typname;
	ResTarget  *start;
	ResTarget  *end;
	ResTarget  *level;
	ColumnRef  *id;
	ResTarget  *path;
	Node       *edge;
	List	   *where_args = NIL;
	ColumnRef  *begin;
	A_Expr	   *vidcond;
	SelectStmt *sel;

	vertex = pstate->p_last_vertex;
	if (IsA(vertex, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) vertex;
		ColumnRef *cref;

		cref = makeNode(ColumnRef);
		cref->fields = list_make2(makeString(rte->eref->aliasname),
								  makeString(AG_ELEM_LOCAL_ID));
		cref->location = -1;

		vid = (Node *) cref;
	}
	else
	{
		TargetEntry *te = (TargetEntry *) vertex;
		ColumnRef *cref;

		AssertArg(IsA(vertex, TargetEntry));

		cref = makeNode(ColumnRef);
		cref->fields = list_make2(makeString(CYPHER_SUBQUERY_ALIAS),
								  makeString(te->resname));
		cref->location = -1;

		vid = (Node *) makeFuncCall(list_make1(makeString(AG_ELEM_LOCAL_ID)),
									list_make1(cref), -1);
	}

	if (indices->lidx == NULL)
	{
		if (indices->uidx != NULL)
		{
			A_Const *uidx = (A_Const *) indices->uidx;

			zero = (uidx->val.val.ival == 0);
		}
	}
	else
	{
		A_Const *lidx = (A_Const *) indices->lidx;

		zero = (lidx->val.val.ival == 0);
	}

	if (zero)
	{
		TypeCast   *typecast;
		List	   *values;

		patharr = makeNode(A_ArrayExpr);
		patharr->location = -1;
		typecast = makeNode(TypeCast);
		typecast->arg = (Node *) patharr;
		typecast->typeName = makeTypeName("_graphid");
		typecast->location = -1;

		values = list_make4(vid, vid, makeIntConst(0), typecast);

		sel = makeNode(SelectStmt);
		sel->valuesLists = list_make1(values);

		return sel;
	}

	getCypherRelType(crel, &typname, NULL);

	start = makeSimpleResTarget(AG_START_ID, NULL);
	end = makeSimpleResTarget(AG_END_ID, NULL);

	level = makeResTarget((Node *) makeIntConst(1), NULL);

	id = makeNode(ColumnRef);
	id->fields = list_make1(makeString(AG_ELEM_LOCAL_ID));
	id->location = -1;
	patharr = makeNode(A_ArrayExpr);
	patharr->elements = list_make1(id);
	patharr->location = -1;
	path = makeResTarget((Node *) patharr, NULL);

	if (crel->direction == CYPHER_REL_DIR_NONE)
	{
		RangeSubselect *sub;

		sub = genEdgeUnionVLR(typname);
		sub->alias = makeAliasNoDup(CYPHER_VLR_EDGE_ALIAS, NIL);
		edge = (Node *) sub;
	}
	else
	{
		RangeVar *r;

		r = makeRangeVar(get_graph_path(), typname, -1);
		r->inhOpt = INH_YES;
		edge = (Node *) r;
	}

	if (crel->direction == CYPHER_REL_DIR_LEFT)
	{
		begin = makeNode(ColumnRef);
		begin->fields = list_make1(makeString(AG_END_ID));
		begin->location = -1;
	}
	else
	{
		begin = makeNode(ColumnRef);
		begin->fields = list_make1(makeString(AG_START_ID));
		begin->location = -1;
	}
	vidcond = makeSimpleA_Expr(AEXPR_OP, "=", (Node *) begin, vid, -1);
	where_args = lappend(where_args, vidcond);

	if (crel->prop_map)
	{
		ColumnRef  *prop;
		A_Expr	   *propcond;

		prop = makeNode(ColumnRef);
		prop->fields = list_make1(makeString(AG_ELEM_PROP_MAP));
		prop->location = -1;
		propcond = makeSimpleA_Expr(AEXPR_OP, "@>", (Node *) prop,
									crel->prop_map, -1);
		where_args = lappend(where_args, propcond);
	}

	sel = makeNode(SelectStmt);
	sel->targetList = list_make4(start, end, level, path);
	sel->fromClause = list_make1(edge);
	sel->whereClause = (Node *) makeBoolExpr(AND_EXPR, where_args, -1);

	return sel;
}

/*
 * -- CYPHER_REL_DIR_LEFT
 * SELECT _e.start, _vlr.end, level + 1, array_append(path, id)
 * FROM _vlr, `get_graph_path()`.`typname` AS _e
 * WHERE level < `indices->uidx` AND
 *       _e.end = _vlr.start AND
 *       array_position(path, id) IS NULL AND
 *       properties @> `crel->prop_map`
 *
 * -- CYPHER_REL_DIR_RIGHT
 * SELECT _vlr.start, _e.end, level + 1, array_append(path, id)
 * FROM _vlr, `get_graph_path()`.`typname` AS _e
 * WHERE level < `indices->uidx` AND
 *       _vlr.end = _e.start AND
 *       array_position(path, id) IS NULL AND
 *       properties @> `crel->prop_map`
 *
 * -- CYPHER_REL_DIR_NONE
 * SELECT _vlr.start, _e.end, level + 1, array_append(path, id)
 * FROM _vlr, `genEdgeUnionVLR(typname)` AS _e
 * WHERE level < `indices->uidx` AND
 *       _vlr.end = _e.start AND
 *       array_position(path, id) IS NULL AND
 *       properties @> `crel->prop_map`
 */
static SelectStmt *
genSelectRightVLR(CypherRel *crel)
{
	A_Indices  *indices = (A_Indices *) crel->varlen;
	char	   *typname;
	ResTarget  *start;
	ResTarget  *end;
	ColumnRef  *levelref;
	A_Expr	   *levelexpr;
	ResTarget  *level;
	ColumnRef  *pathref;
	ColumnRef  *id;
	FuncCall   *pathexpr;
	ResTarget  *path;
	RangeVar   *vlr;
	Node       *edge;
	ColumnRef  *prev;
	ColumnRef  *next;
	List	   *where_args = NIL;
	A_Expr	   *joincond;
	List	   *arrpos_args;
	FuncCall   *arrpos;
	NullTest   *dupcond;
	SelectStmt *sel;

	getCypherRelType(crel, &typname, NULL);

	if (crel->direction == CYPHER_REL_DIR_LEFT)
	{
		start = makeFieldsResTarget(
					list_make2(makeString(CYPHER_VLR_EDGE_ALIAS),
							   makeString(AG_START_ID)),
					NULL);
		end = makeFieldsResTarget(list_make2(makeString(CYPHER_VLR_WITH_ALIAS),
											 makeString(VLR_COLNAME_END)),
								  NULL);
	}
	else
	{
		start = makeFieldsResTarget(
					list_make2(makeString(CYPHER_VLR_WITH_ALIAS),
							   makeString(VLR_COLNAME_START)),
					NULL);
		end = makeFieldsResTarget(list_make2(makeString(CYPHER_VLR_EDGE_ALIAS),
											 makeString(AG_END_ID)),
								  NULL);
	}

	levelref = makeNode(ColumnRef);
	levelref->fields = list_make1(makeString(VLR_COLNAME_LEVEL));
	levelref->location = -1;
	levelexpr = makeSimpleA_Expr(AEXPR_OP, "+", (Node *) levelref,
								 (Node *) makeIntConst(1), -1);
	level = makeResTarget((Node *) levelexpr, NULL);

	pathref = makeNode(ColumnRef);
	pathref->fields = list_make1(makeString(VLR_COLNAME_PATH));
	pathref->location = -1;
	id = makeNode(ColumnRef);
	id->fields = list_make1(makeString(AG_ELEM_LOCAL_ID));
	id->location = -1;
	pathexpr = makeFuncCall(list_make1(makeString("array_append")),
							list_make2(pathref, id), -1);
	path = makeResTarget((Node *) pathexpr, NULL);

	vlr = makeRangeVar(NULL, CYPHER_VLR_WITH_ALIAS, -1);

	if (crel->direction == CYPHER_REL_DIR_NONE)
	{
		RangeSubselect *sub;

		sub = genEdgeUnionVLR(typname);
		sub->alias = makeAliasNoDup(CYPHER_VLR_EDGE_ALIAS, NIL);
		edge = (Node *) sub;
	}
	else
	{
		RangeVar *r;

		r = makeRangeVar(get_graph_path(), typname, -1);
		r->alias = makeAliasNoDup(CYPHER_VLR_EDGE_ALIAS, NIL);
		r->inhOpt = INH_YES;
		edge = (Node *) r;
	}

	if (crel->direction == CYPHER_REL_DIR_LEFT)
	{
		prev = makeNode(ColumnRef);
		prev->fields = list_make2(makeString(CYPHER_VLR_WITH_ALIAS),
								  makeString(VLR_COLNAME_START));
		prev->location = -1;

		next = makeNode(ColumnRef);
		next->fields = list_make2(makeString(CYPHER_VLR_EDGE_ALIAS),
								  makeString(AG_END_ID));
		next->location = -1;
	}
	else
	{
		prev = makeNode(ColumnRef);
		prev->fields = list_make2(makeString(CYPHER_VLR_WITH_ALIAS),
								  makeString(VLR_COLNAME_END));
		prev->location = -1;

		next = makeNode(ColumnRef);
		next->fields = list_make2(makeString(CYPHER_VLR_EDGE_ALIAS),
								  makeString(AG_START_ID));
		next->location = -1;
	}

	if (indices->uidx != NULL)
	{
		A_Expr *levelcond;

		levelcond = makeSimpleA_Expr(AEXPR_OP, "<", (Node *) levelref,
									 indices->uidx, -1);
		where_args = lappend(where_args, levelcond);
	}

	joincond = makeSimpleA_Expr(AEXPR_OP, "=", (Node *) prev, (Node *) next,
								-1);
	where_args = lappend(where_args, joincond);

	arrpos_args = list_make2(pathref, id);
	arrpos = makeFuncCall(list_make1(makeString("array_position")),
						  arrpos_args, -1);
	dupcond = makeNode(NullTest);
	dupcond->arg = (Expr *) arrpos;
	dupcond->nulltesttype = IS_NULL;
	dupcond->location = -1;
	where_args = lappend(where_args, dupcond);

	if (crel->prop_map)
	{
		ColumnRef  *prop;
		A_Expr	   *propcond;

		prop = makeNode(ColumnRef);
		prop->fields = list_make1(makeString(AG_ELEM_PROP_MAP));
		prop->location = -1;
		propcond = makeSimpleA_Expr(AEXPR_OP, "@>", (Node *) prop,
									crel->prop_map, -1);
		where_args = lappend(where_args, propcond);
	}

	sel = makeNode(SelectStmt);
	sel->targetList = list_make4(start, end, level, path);
	sel->fromClause = list_make2(vlr, edge);
	sel->whereClause = (Node *) makeBoolExpr(AND_EXPR, where_args, -1);

	return sel;
}

/*
 * SELECT id, properties, start, "end"
 * FROM `get_graph_path()`.`edge_label`
 * UNION
 * SELECT id, properties, "end" as start, start as "end"
 * FROM `get_graph_path()`.`edge_label`
 */
static RangeSubselect *
genEdgeUnionVLR(char *edge_label)
{
	ResTarget  *id;
	ResTarget  *prop_map;
	RangeVar   *r;
	SelectStmt *lsel;
	SelectStmt *rsel;
	SelectStmt *u;
	RangeSubselect *sub;

	id = makeSimpleResTarget(AG_ELEM_LOCAL_ID, NULL);
	prop_map = makeSimpleResTarget(AG_ELEM_PROP_MAP, NULL);

	r = makeRangeVar(get_graph_path(), edge_label, -1);
	r->inhOpt = INH_YES;

	lsel = makeNode(SelectStmt);
	lsel->targetList = list_make2(id, prop_map);
	lsel->fromClause = list_make1(r);

	rsel = copyObject(lsel);

	lsel->targetList = lappend(lsel->targetList,
							   makeSimpleResTarget(AG_START_ID, NULL));
	lsel->targetList = lappend(lsel->targetList,
							   makeSimpleResTarget(AG_END_ID, NULL));

	rsel->targetList = lappend(rsel->targetList,
							   makeSimpleResTarget(AG_END_ID, AG_START_ID));
	rsel->targetList = lappend(rsel->targetList,
							   makeSimpleResTarget(AG_START_ID, AG_END_ID));

	u = makeNode(SelectStmt);
	u->op = SETOP_UNION;
	u->all = true;
	u->larg = lsel;
	u->rarg = rsel;

	sub = makeNode(RangeSubselect);
	sub->subquery = (Node *) u;

	return sub;
}

static SelectStmt *
genSelectWithVLR(CypherRel *crel, WithClause *with)
{
	A_Indices  *indices = (A_Indices *) crel->varlen;
	ResTarget  *start;
	ResTarget  *end;
	ResTarget  *path;
	RangeVar   *vlr;
	SelectStmt *sel;
	Node	   *lidx;

	if (crel->direction == CYPHER_REL_DIR_NONE)
	{
		start = makeSimpleResTarget(VLR_COLNAME_START, EDGE_UNION_START_ID);
		end = makeSimpleResTarget(VLR_COLNAME_END, EDGE_UNION_END_ID);
	}
	else
	{
		start = makeSimpleResTarget(VLR_COLNAME_START, AG_START_ID);
		end = makeSimpleResTarget(VLR_COLNAME_END, AG_END_ID);
	}
	path = makeSimpleResTarget(VLR_COLNAME_PATH, NULL);

	vlr = makeRangeVar(NULL, CYPHER_VLR_WITH_ALIAS, -1);

	sel = makeNode(SelectStmt);
	sel->targetList = list_make3(start, end, path);
	sel->fromClause = list_make1(vlr);

	if (indices->lidx == NULL)
	{
		if (indices->uidx != NULL)
			lidx = indices->uidx;
		else
			lidx = NULL;
	}
	else
	{
		lidx = indices->lidx;
	}

	if (lidx != NULL)
	{
		ColumnRef *level;

		level = makeNode(ColumnRef);
		level->fields = list_make1(makeString(VLR_COLNAME_LEVEL));
		level->location = -1;

		sel->whereClause = (Node *) makeSimpleA_Expr(AEXPR_OP, ">=",
													 (Node *) level, lidx, -1);
	}

	sel->withClause = with;

	return sel;
}

static RangeTblEntry *
addEdgeUnion(ParseState *pstate, char *edge_label, int location, Alias *alias)
{
	Node	   *u;
	Query	   *qry;
	RangeTblEntry *rte;

	AssertArg(alias != NULL);

	Assert(pstate->p_expr_kind == EXPR_KIND_NONE);

	pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

	u = genEdgeUnion(edge_label, location);
	qry = parse_sub_analyze(u, pstate, NULL,
							isLockedRefname(pstate, alias->aliasname));

	pstate->p_expr_kind = EXPR_KIND_NONE;

	rte = addRangeTableEntryForSubquery(pstate, qry, alias, false, true);

	return rte;
}

/*
 * SELECT id, start, "end", properties,
 *        start as _start, "end" as _end
 * FROM `get_graph_path()`.`edge_label`
 * UNION
 * SELECT id, start, "end", properties,
 *        "end" as _start, start as _end
 * FROM `get_graph_path()`.`edge_label`
 */
static Node *
genEdgeUnion(char *edge_label, int location)
{
	ResTarget  *id;
	ResTarget  *start;
	ResTarget  *end;
	ResTarget  *prop_map;
	RangeVar   *r;
	SelectStmt *lsel;
	SelectStmt *rsel;
	SelectStmt *u;

	id = makeSimpleResTarget(AG_ELEM_LOCAL_ID, NULL);
	start = makeSimpleResTarget(AG_START_ID, NULL);
	end = makeSimpleResTarget(AG_END_ID, NULL);
	prop_map = makeSimpleResTarget(AG_ELEM_PROP_MAP, NULL);

	r = makeRangeVar(get_graph_path(), edge_label, location);
	r->inhOpt = INH_YES;

	lsel = makeNode(SelectStmt);
	lsel->targetList = list_make4(id, start, end, prop_map);
	lsel->fromClause = list_make1(r);

	rsel = copyObject(lsel);

	lsel->targetList = lappend(lsel->targetList,
							   makeSimpleResTarget(AG_START_ID,
												   EDGE_UNION_START_ID));
	lsel->targetList = lappend(lsel->targetList,
							   makeSimpleResTarget(AG_END_ID,
												   EDGE_UNION_END_ID));

	rsel->targetList = lappend(rsel->targetList,
							   makeSimpleResTarget(AG_END_ID,
												   EDGE_UNION_START_ID));
	rsel->targetList = lappend(rsel->targetList,
							   makeSimpleResTarget(AG_START_ID,
												   EDGE_UNION_END_ID));

	u = makeNode(SelectStmt);
	u->op = SETOP_UNION;
	u->all = true;
	u->larg = lsel;
	u->rarg = rsel;

	return (Node *) u;
}

static Node *
addQualRelPath(ParseState *pstate, Node *qual, CypherRel *prev_crel,
			   RangeTblEntry *prev_edge, CypherRel *crel, RangeTblEntry *edge)
{
	Node	   *prev_vid;
	Node	   *vid;

	if (prev_crel == NULL || prev_edge == NULL)
		return qual;

	prev_vid = getColumnVar(pstate, prev_edge, getEdgeColname(prev_crel, true));
	vid = getColumnVar(pstate, edge, getEdgeColname(crel, false));

	qual = qualAndExpr(pstate, qual,
					   (Node *) make_op(pstate, list_make1(makeString("=")),
										prev_vid, vid, -1));

	return qual;
}

static Node *
addQualForRel(ParseState *pstate, Node *qual, CypherRel *crel,
			  RangeTblEntry *edge)
{
	/* already done in transformMatchVLR() */
	if (crel->varlen != NULL)
		return qual;

	if (crel->prop_map != NULL)
		qual = qualAndExpr(pstate, qual,
						   makePropMapQual(pstate, (Node *) edge,
										   crel->prop_map));

	return qual;
}

static Node *
addQualNodeIn(ParseState *pstate, Node *qual, Node *vertex, CypherRel *crel,
			  RangeTblEntry *edge, bool last)
{
	Node	   *id;
	Node	   *vid;

	if (vertex == NULL || crel == NULL || edge == NULL)
		return qual;

	id = getElemField(pstate, vertex, AG_ELEM_ID);
	vid = getColumnVar(pstate, edge, getEdgeColname(crel, last));

	qual = qualAndExpr(pstate, qual,
					   (Node *) make_op(pstate, list_make1(makeString("=")),
										id, vid, -1));

	return qual;
}

static Node *
addQualForNode(ParseState *pstate, Node *qual, CypherNode *cnode, Node *vertex)
{
	if (vertex == NULL)
		return qual;

	if (cnode->prop_map)
		qual = qualAndExpr(pstate, qual,
						   makePropMapQual(pstate, vertex, cnode->prop_map));

	return qual;
}

static char *
getEdgeColname(CypherRel *crel, bool last)
{
	if (last)
	{
		if (crel->direction == CYPHER_REL_DIR_NONE)
			return EDGE_UNION_END_ID;
		else if (crel->direction == CYPHER_REL_DIR_LEFT)
			return AG_START_ID;
		else
			return AG_END_ID;
	}
	else
	{
		if (crel->direction == CYPHER_REL_DIR_NONE)
			return EDGE_UNION_START_ID;
		else if (crel->direction == CYPHER_REL_DIR_LEFT)
			return AG_END_ID;
		else
			return AG_START_ID;
	}
}

static Node *
makePropMapQual(ParseState *pstate, Node *elem, Node *expr)
{
	Node	   *prop_map;
	Node	   *constr;

	prop_map = getElemField(pstate, elem, AG_ELEM_PROP_MAP);
	constr = transformPropMap(pstate, expr, EXPR_KIND_WHERE);

	return (Node *) make_op(pstate, list_make1(makeString("@>")),
							prop_map, constr, -1);
}

static Node *
addQualUniqueEdges(ParseState *pstate, Node *qual, List *ueids, List *ueidarrs)
{
	FuncCall   *arrpos;
	ListCell   *le1;
	ListCell   *lea1;

	arrpos = makeFuncCall(list_make1(makeString("array_position")), NIL, -1);

	foreach(le1, ueids)
	{
		Node	   *eid1 = lfirst(le1);
		ListCell   *le2;

		for_each_cell(le2, lnext(le1))
		{
			Node	   *eid2 = lfirst(le2);
			Expr	   *ne;

			ne = make_op(pstate, list_make1(makeString("<>")), eid1, eid2, -1);

			qual = qualAndExpr(pstate, qual, (Node *) ne);
		}

		foreach(lea1, ueidarrs)
		{
			Node	   *eidarr = lfirst(lea1);
			Node	   *arg;
			NullTest   *dupcond;

			arg = ParseFuncOrColumn(pstate,
									list_make1(makeString("array_position")),
									list_make2(eidarr, eid1), arrpos, -1);

			dupcond = makeNode(NullTest);
			dupcond->arg = (Expr *) arg;
			dupcond->nulltesttype = IS_NULL;
			dupcond->argisrow = false;
			dupcond->location = -1;

			qual = qualAndExpr(pstate, qual, (Node *) dupcond);
		}
	}

	foreach(lea1, ueidarrs)
	{
		Node	   *eidarr1 = lfirst(lea1);
		ListCell   *lea2;

		for_each_cell(lea2, lnext(lea1))
		{
			Node	   *eidarr2 = lfirst(lea2);
			Node	   *overlap;
			Node	   *dupcond;

			overlap = ParseFuncOrColumn(pstate,
										list_make1(makeString("arrayoverlap")),
										list_make2(eidarr1, eidarr2), arrpos,
										-1);

			dupcond = (Node *) makeBoolExpr(NOT_EXPR, list_make1(overlap), -1);

			qual = qualAndExpr(pstate, qual, dupcond);
		}
	}

	return qual;
}

static List *
transformCreatePattern(ParseState *pstate, List *pattern, List **targetList)
{
	List	   *graphPattern = NIL;
	ListCell   *lp;

	foreach(lp, pattern)
	{
		CypherPath *p = lfirst(lp);
		char	   *pathname = getCypherName(p->variable);
		int			pathloc = getCypherNameLoc(p->variable);
		List	   *gchain = NIL;
		GraphPath  *gpath;
		ListCell   *le;

		if (findTarget(*targetList, pathname) != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_ALIAS),
					 errmsg("duplicate variable \"%s\"", pathname),
					 parser_errposition(pstate, pathloc)));

		foreach(le, p->chain)
		{
			Node *elem = lfirst(le);

			if (IsA(elem, CypherNode))
			{
				CypherNode *cnode = (CypherNode *) elem;
				GraphVertex *gvertex;

				gvertex = transformCreateNode(pstate, cnode, targetList);

				if (!gvertex->create && list_length(p->chain) <= 1)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("there must be at least one relationship"),
							 parser_errposition(pstate,
										getCypherNameLoc(cnode->variable))));

				gchain = lappend(gchain, gvertex);
			}
			else
			{
				CypherRel  *crel = (CypherRel *) elem;
				GraphEdge  *gedge;

				Assert(IsA(elem, CypherRel));

				gedge = transformCreateRel(pstate, crel, targetList);

				gchain = lappend(gchain, gedge);
			}
		}

		if (pathname != NULL)
		{
			Const *dummy;
			TargetEntry *te;

			dummy = makeNullConst(GRAPHPATHOID, -1, InvalidOid);
			te = makeTargetEntry((Expr *) dummy,
								 (AttrNumber) pstate->p_next_resno++,
								 pstrdup(pathname),
								 false);

			*targetList = lappend(*targetList, te);
		}

		gpath = makeNode(GraphPath);
		if (pathname != NULL)
			gpath->variable = pstrdup(pathname);
		gpath->chain = gchain;

		graphPattern = lappend(graphPattern, gpath);
	}

	return graphPattern;
}

static GraphVertex *
transformCreateNode(ParseState *pstate, CypherNode *cnode, List **targetList)
{
	char	   *varname = getCypherName(cnode->variable);
	int			varloc = getCypherNameLoc(cnode->variable);
	TargetEntry *te;
	bool		create;
	Node	   *prop_map = NULL;
	GraphVertex *gvertex;

	te = findTarget(*targetList, varname);
	if (te != NULL &&
		(exprType((Node *) te->expr) != VERTEXOID || !isNodeForRef(cnode)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_ALIAS),
				 errmsg("duplicate variable \"%s\"", varname),
				 parser_errposition(pstate, varloc)));

	create = (te == NULL);

	if (create)
	{
		if (varname != NULL)
		{
			Const *dummy;

			/*
			 * Create a room for a newly created vertex.
			 * This dummy value will be replaced with the vertex
			 * in ExecCypherCreate().
			 */

			dummy = makeNullConst(VERTEXOID, -1, InvalidOid);
			te = makeTargetEntry((Expr *) dummy,
								 (AttrNumber) pstate->p_next_resno++,
								 pstrdup(varname),
								 false);

			*targetList = lappend(*targetList, te);
		}

		if (cnode->prop_map != NULL)
			prop_map = transformPropMap(pstate, cnode->prop_map,
										EXPR_KIND_INSERT_TARGET);
	}

	gvertex = makeNode(GraphVertex);
	if (varname != NULL)
		gvertex->variable = pstrdup(varname);
	if (cnode->label != NULL)
		gvertex->label = pstrdup(getCypherName(cnode->label));
	gvertex->prop_map = prop_map;
	gvertex->create = create;

	return gvertex;
}

static GraphEdge *
transformCreateRel(ParseState *pstate, CypherRel *crel, List **targetList)
{
	char	   *varname;
	GraphEdge  *gedge;

	if (crel->direction == CYPHER_REL_DIR_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("only directed relationships are allowed in CREATE")));

	if (list_length(crel->types) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("only one relationship type is allowed for CREATE")));

	if (crel->varlen != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("variable length relationship is not allowed for CREATE")));

	varname = getCypherName(crel->variable);

	/*
	 * All relationships must be unique and We cannot reference an edge
	 * from the previous clause in CREATE clause.
	 */
	if (findTarget(*targetList, varname) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_ALIAS),
				 errmsg("duplicate variable \"%s\"", varname),
				 parser_errposition(pstate, getCypherNameLoc(crel->variable))));

	if (varname != NULL)
	{
		TargetEntry *te;

		te = makeTargetEntry((Expr *) makeNullConst(EDGEOID, -1, InvalidOid),
							 (AttrNumber) pstate->p_next_resno++,
							 pstrdup(varname),
							 false);

		*targetList = lappend(*targetList, te);
	}

	gedge = makeNode(GraphEdge);
	switch (crel->direction)
	{
		case CYPHER_REL_DIR_LEFT:
			gedge->direction = GRAPH_EDGE_DIR_LEFT;
			break;
		case CYPHER_REL_DIR_RIGHT:
			gedge->direction = GRAPH_EDGE_DIR_RIGHT;
			break;
		case CYPHER_REL_DIR_NONE:
		default:
			Assert(!"invalid direction");
	}
	if (varname != NULL)
		gedge->variable = pstrdup(varname);
	gedge->label = pstrdup(getCypherName(linitial(crel->types)));
	if (crel->prop_map != NULL)
		gedge->prop_map = transformPropMap(pstate, crel->prop_map,
										   EXPR_KIND_INSERT_TARGET);

	return gedge;
}

static List *
transformSetPropList(ParseState *pstate, RangeTblEntry *rte, List *items)
{
	List	   *sps = NIL;
	ListCell   *li;

	foreach(li, items)
	{
		CypherSetProp *sp = lfirst(li);

		sps = lappend(sps, transformSetProp(pstate, rte, sp));
	}

	return sps;
}

static GraphSetProp *
transformSetProp(ParseState *pstate, RangeTblEntry *rte, CypherSetProp *sp)
{
	Node	   *node;
	List	   *inds;
	Node	   *elem;
	List	   *pathelems = NIL;
	ListCell   *lf;
	Node	   *expr;
	Oid			exprtype;
	Node	   *cexpr;
	GraphSetProp *gsp;

	if (!IsA(sp->prop, ColumnRef) && !IsA(sp->prop, A_Indirection))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("only variable or property is valid for SET target")));

	if (IsA(sp->prop, A_Indirection))
	{
		A_Indirection *ind = (A_Indirection *) sp->prop;

		node = ind->arg;
		inds = ind->indirection;
	}
	else
	{
		node = sp->prop;
		inds = NIL;
	}

	if (IsA(node, ColumnRef))
	{
		ColumnRef  *cref = (ColumnRef *) node;
		char	   *varname = strVal(linitial(cref->fields));

		elem = getColumnVar(pstate, rte, varname);

		if (list_length(cref->fields) > 1)
		{
			for_each_cell(lf, lnext(list_head(cref->fields)))
			{
				pathelems = lappend(pathelems,
									transformJsonKey(pstate, lfirst(lf)));
			}
		}
	}
	else
	{
		Oid elemtype;

		elem = transformExpr(pstate, node, EXPR_KIND_UPDATE_TARGET);

		elemtype = exprType(elem);
		if (elemtype != VERTEXOID && elemtype != EDGEOID)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("expected node or relationship"),
					 parser_errposition(pstate, exprLocation(elem))));
	}

	if (inds != NIL)
	{
		foreach(lf, inds)
		{
			pathelems = lappend(pathelems,
								transformJsonKey(pstate, lfirst(lf)));
		}
	}

	expr = transformExpr(pstate, sp->expr, EXPR_KIND_UPDATE_SOURCE);
	exprtype = exprType(expr);
	cexpr = coerce_to_target_type(pstate, expr, exprtype, JSONBOID, -1,
								  COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST,
								  -1);
	if (cexpr == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("expression must be of type jsonb but %s",
						format_type_be(exprtype)),
				 parser_errposition(pstate, exprLocation(expr))));

	gsp = makeNode(GraphSetProp);
	gsp->elem = elem;
	if (pathelems != NIL)
		gsp->path = makeArrayExpr(TEXTARRAYOID, TEXTOID, pathelems);
	gsp->expr = cexpr;

	return gsp;
}

static bool
isNodeEmpty(CypherNode *cnode)
{
	return (getCypherName(cnode->variable) == NULL &&
			getCypherName(cnode->label) == NULL &&
			cnode->prop_map == NULL);
}

static bool
isNodeForRef(CypherNode *cnode)
{
	return (getCypherName(cnode->variable) != NULL &&
			getCypherName(cnode->label) == NULL &&
			cnode->prop_map == NULL);
}

static Node *
transformPropMap(ParseState *pstate, Node *expr, ParseExprKind exprKind)
{
	Node *prop_map;

	prop_map = transformExpr(pstate, preprocessPropMap(expr), exprKind);
	if (exprType(prop_map) != JSONBOID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("property map must be jsonb type"),
				 parser_errposition(pstate, exprLocation(prop_map))));

	return prop_map;
}

static Node *
preprocessPropMap(Node *expr)
{
	Node *result = expr;

	if (IsA(expr, A_Const))
	{
		A_Const *c = (A_Const *) expr;

		if (IsA(&c->val, String))
			result = (Node *) makeFuncCall(list_make1(makeString("jsonb_in")),
										   list_make1(expr), -1);
	}

	return result;
}

static RangeTblEntry *
transformClause(ParseState *pstate, Node *clause, Alias *alias, bool add)
{
	Query	   *qry;
	RangeTblEntry *rte;
	bool	   lateral = pstate->p_lateral_active;

	AssertArg(IsA(clause, CypherClause));

	if (alias == NULL)
		alias = makeAliasNoDup(CYPHER_SUBQUERY_ALIAS, NIL);

	Assert(pstate->p_expr_kind == EXPR_KIND_NONE);

	pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

	qry = parse_sub_analyze(clause, pstate, NULL,
							isLockedRefname(pstate, alias->aliasname));

	pstate->p_expr_kind = EXPR_KIND_NONE;

	if (!IsA(qry, Query) ||
		(qry->commandType != CMD_SELECT &&
		 qry->commandType != CMD_GRAPHWRITE) ||
		qry->utilityStmt != NULL)
		elog(ERROR, "unexpected command in previous clause");

	rte = addRangeTableEntryForSubquery(pstate, qry, alias, lateral, true);
	if (add)
		addRTEtoJoinlist(pstate, rte, true);

	return rte;
}

static RangeTblEntry *
transformVLRtoRTE(ParseState *pstate, SelectStmt *vlr, Alias *alias)
{
	ParseNamespaceItem *nsitem = NULL;
	ListCell   *lni;
	Query	   *qry;
	RangeTblEntry *rte;

	Assert(!pstate->p_lateral_active);
	Assert(pstate->p_expr_kind == EXPR_KIND_NONE);

	if (IsA(pstate->p_last_vertex, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) pstate->p_last_vertex;

		foreach(lni, pstate->p_namespace)
		{
			ParseNamespaceItem *tmp = lfirst(lni);

			if (tmp->p_rte == rte)
			{
				nsitem = tmp;
				nsitem->p_rel_visible = true;
				break;
			}
		}
	}

	pstate->p_lateral_active = true;
	pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

	qry = parse_sub_analyze((Node *) vlr, pstate, NULL,
							isLockedRefname(pstate, alias->aliasname));
	Assert(qry->commandType == CMD_SELECT);

	pstate->p_lateral_active = false;
	pstate->p_expr_kind = EXPR_KIND_NONE;

	if (nsitem != NULL)
		nsitem->p_rel_visible = false;

	rte = addRangeTableEntryForSubquery(pstate, qry, alias, true, true);
	addRTEtoJoinlist(pstate, rte, true);

	return rte;
}

static void
addRTEtoJoinlist(ParseState *pstate, RangeTblEntry *rte, bool visible)
{
	RangeTblEntry *tmp;
	ParseNamespaceItem *nsitem;
	RangeTblRef *rtr;

	/*
	 * There should be no namespace conflicts because we check a variable
	 * (which becomes an alias) is duplicated. This check remains to prevent
	 * future programming error.
	 */
	tmp = findRTEfromNamespace(pstate, rte->eref->aliasname);
	if (tmp != NULL)
	{
		if (!(rte->rtekind == RTE_RELATION && rte->alias == NULL &&
			  tmp->rtekind == RTE_RELATION && tmp->alias == NULL &&
			  rte->relid != tmp->relid))
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_ALIAS),
					 errmsg("variable \"%s\" specified more than once",
							rte->eref->aliasname)));
	}

	/*
	 * We set `p_lateral_only` to false ahead at here
	 * since there is no LATERAL use in Cypher transform.
	 */
	nsitem = palloc(sizeof(*nsitem));
	nsitem->p_rte = rte;
	nsitem->p_rel_visible = visible;
	nsitem->p_cols_visible = visible;
	nsitem->p_lateral_only = false;
	nsitem->p_lateral_ok = true;

	pstate->p_namespace = lappend(pstate->p_namespace, nsitem);

	rtr = makeNode(RangeTblRef);
	rtr->rtindex = RTERangeTablePosn(pstate, rte, NULL);

	pstate->p_joinlist = lappend(pstate->p_joinlist, rtr);
}

static Node *
transformClauseJoin(ParseState *pstate, Node *clause, Alias *alias,
					RangeTblEntry **rte, ParseNamespaceItem **nsitem)
{
	ParseNamespaceItem *tmp;
	RangeTblRef *rtr;

	*rte = transformClause(pstate, clause, alias, false);

	tmp = (ParseNamespaceItem *) palloc(sizeof(ParseNamespaceItem));
	tmp->p_rte = *rte;
	tmp->p_rel_visible = true;
	tmp->p_cols_visible = true;
	tmp->p_lateral_only = false;
	tmp->p_lateral_ok = true;
	*nsitem = tmp;

	rtr = makeNode(RangeTblRef);
	rtr->rtindex = RTERangeTablePosn(pstate, *rte, NULL);

	return (Node *) rtr;
}

/* just find RTE of `refname` in the current namespace */
static RangeTblEntry *
findRTEfromNamespace(ParseState *pstate, char *refname)
{
	ListCell *lni;

	if (refname == NULL)
		return NULL;

	foreach(lni, pstate->p_namespace)
	{
		ParseNamespaceItem *nsitem = lfirst(lni);
		RangeTblEntry *rte = nsitem->p_rte;

		/* NOTE: skip all checks on `nsitem` */

		if (strcmp(rte->eref->aliasname, refname) == 0)
			return rte;
	}

	return NULL;
}

static List *
makeTargetListFromRTE(ParseState *pstate, RangeTblEntry *rte)
{
	List	   *targetlist = NIL;
	int			rtindex;
	int			varattno;
	ListCell   *ln;
	ListCell   *lt;

	AssertArg(rte->rtekind == RTE_SUBQUERY);

	rtindex = RTERangeTablePosn(pstate, rte, NULL);

	varattno = 1;
	ln = list_head(rte->eref->colnames);
	foreach(lt, rte->subquery->targetList)
	{
		TargetEntry *te = lfirst(lt);
		Var		   *varnode;
		char	   *resname;
		TargetEntry *tmp;

		if (te->resjunk)
			continue;

		Assert(varattno == te->resno);

		/* no transform here, just use `te->expr` */
		varnode = makeVar(rtindex, varattno,
						  exprType((Node *) te->expr),
						  exprTypmod((Node *) te->expr),
						  exprCollation((Node *) te->expr),
						  0);

		resname = strVal(lfirst(ln));

		tmp = makeTargetEntry((Expr *) varnode,
							  (AttrNumber) pstate->p_next_resno++,
							  pstrdup(resname),
							  false);
		targetlist = lappend(targetlist, tmp);

		varattno++;
		ln = lnext(ln);
	}

	return targetlist;
}

static List *
makeTargetListFromJoin(ParseState *pstate, RangeTblEntry *rte)
{
	List	   *targetlist = NIL;
	ListCell   *lt;
	ListCell   *ln;

	AssertArg(rte->rtekind == RTE_JOIN);

	forboth(lt, rte->joinaliasvars, ln, rte->eref->colnames)
	{
		Var		   *varnode = lfirst(lt);
		char	   *resname = strVal(lfirst(ln));
		TargetEntry *tmp;

		tmp = makeTargetEntry((Expr *) varnode,
							  (AttrNumber) pstate->p_next_resno++,
							  pstrdup(resname),
							  false);
		targetlist = lappend(targetlist, tmp);
	}

	return targetlist;
}

static TargetEntry *
makeWholeRowTarget(ParseState *pstate, RangeTblEntry *rte)
{
	int			rtindex;
	Var		   *varnode;

	rtindex = RTERangeTablePosn(pstate, rte, NULL);

	varnode = makeWholeRowVar(rte, rtindex, 0, false);
	varnode->location = -1;

	markVarForSelectPriv(pstate, varnode, rte);

	return makeTargetEntry((Expr *) varnode,
						   (AttrNumber) pstate->p_next_resno++,
						   pstrdup(rte->eref->aliasname),
						   false);
}

static TargetEntry *
findTarget(List *targetList, char *resname)
{
	ListCell *lt;
	TargetEntry *te = NULL;

	if (resname == NULL)
		return NULL;

	foreach(lt, targetList)
	{
		te = lfirst(lt);

		if (te->resjunk)
			continue;

		if (strcmp(te->resname, resname) == 0)
			return te;
	}

	return NULL;
}

static Node *
findLeftVar(ParseState *pstate, char *varname)
{
	RangeTblEntry *rte;

	rte = refnameRangeTblEntry(pstate, NULL, CYPHER_OPTLEFT_ALIAS, -1, NULL);

	return scanRTEForColumn(pstate, rte, varname, -1 , 0, NULL);
}

static Node *
makeVertexExpr(ParseState *pstate, RangeTblEntry *rte, int location)
{
	Node	   *id;
	Node	   *prop_map;

	id = getColumnVar(pstate, rte, AG_ELEM_LOCAL_ID);
	prop_map = getColumnVar(pstate, rte, AG_ELEM_PROP_MAP);

	return makeTypedRowExpr(list_make2(id, prop_map), VERTEXOID, location);
}

static Node *
makeEdgeExpr(ParseState *pstate, RangeTblEntry *rte, int location)
{
	Node	   *id;
	Node	   *start;
	Node	   *end;
	Node	   *prop_map;

	id = getColumnVar(pstate, rte, AG_ELEM_LOCAL_ID);
	start = getColumnVar(pstate, rte, AG_START_ID);
	end = getColumnVar(pstate, rte, AG_END_ID);
	prop_map = getColumnVar(pstate, rte, AG_ELEM_PROP_MAP);

	return makeTypedRowExpr(list_make4(id, start, end, prop_map),
							EDGEOID, location);
}

static Node *
makePathVertexExpr(ParseState *pstate, Node *obj)
{
	if (IsA(obj, RangeTblEntry))
	{
		return makeVertexExpr(pstate, (RangeTblEntry *) obj, -1);
	}
	else
	{
		TargetEntry *te = (TargetEntry *) obj;

		AssertArg(IsA(obj, TargetEntry));
		AssertArg(exprType((Node *) te->expr) == VERTEXOID);

		return (Node *) te->expr;
	}
}

static Node *
makeGraphpath(List *vertices, List *edges, int location)
{
	Node	   *v_arr;
	Node	   *e_arr;

	v_arr = makeArrayExpr(VERTEXARRAYOID, VERTEXOID, vertices);
	e_arr = makeArrayExpr(EDGEARRAYOID, EDGEOID, edges);

	return makeTypedRowExpr(list_make2(v_arr, e_arr), GRAPHPATHOID, location);
}

static Node *
getElemField(ParseState *pstate, Node *elem, char *fname)
{
	if (IsA(elem, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) elem;

		if (strcmp(fname, AG_ELEM_ID) == 0)
			return getColumnVar(pstate, rte, AG_ELEM_LOCAL_ID);
		else
			return getColumnVar(pstate, rte, fname);
	}
	else
	{
		TargetEntry *te = (TargetEntry *) elem;

		AssertArg(IsA(elem, TargetEntry));

		return getExprField(te->expr, fname);
	}
}

static Node *
getColumnVar(ParseState *pstate, RangeTblEntry *rte, char *colname)
{
	ListCell   *lcn;
	int			attrno;
	Var		   *var;

	attrno = 1;
	foreach(lcn, rte->eref->colnames)
	{
		const char *tmp = strVal(lfirst(lcn));

		if (strcmp(tmp, colname) == 0)
		{
			/*
			 * NOTE: no ambiguous reference check here
			 *       since all column names in `rte` are unique
			 */

			var = make_var(pstate, rte, attrno, -1);

			/* require read access to the column */
			markVarForSelectPriv(pstate, var, rte);

			return (Node *) var;
		}

		attrno++;
	}

	elog(ERROR, "column \"%s\" not found (internal error)", colname);
	return NULL;
}

static Node *
getExprField(Expr *expr, char *fname)
{
	Oid			typoid;
	TupleDesc	tupdesc;
	int			idx;
	Form_pg_attribute attr = NULL;
	FieldSelect *fselect;

	typoid = exprType((Node *) expr);

	tupdesc = lookup_rowtype_tupdesc_copy(typoid, -1);
	for (idx = 0; idx < tupdesc->natts; idx++)
	{
		attr = tupdesc->attrs[idx];

		if (namestrcmp(&attr->attname, AG_ELEM_LOCAL_ID) == 0)
			break;
	}
	Assert(idx < tupdesc->natts);

	fselect = makeNode(FieldSelect);
	fselect->arg = expr;
	fselect->fieldnum = idx + 1;
	fselect->resulttype = attr->atttypid;
	fselect->resulttypmod = attr->atttypmod;
	fselect->resultcollid = attr->attcollation;

	return (Node *) fselect;
}

/* same as makeAlias() but no pstrdup(aliasname) */
static Alias *
makeAliasNoDup(char *aliasname, List *colnames)
{
	Alias *alias;

	alias = makeNode(Alias);
	alias->aliasname = aliasname;
	alias->colnames = colnames;

	return alias;
}

static Alias *
makeAliasOptUnique(char *aliasname)
{
	aliasname = (aliasname == NULL ? genUniqueName() : pstrdup(aliasname));
	return makeAliasNoDup(aliasname, NIL);
}

static Node *
makeArrayExpr(Oid typarray, Oid typoid, List *elems)
{
	ArrayExpr *arr = makeNode(ArrayExpr);

	arr->array_typeid = typarray;
	arr->element_typeid = typoid;
	arr->elements = elems;
	arr->multidims = false;
	arr->location = -1;

	return (Node *) arr;
}

static Node *
makeTypedRowExpr(List *args, Oid typoid, int location)
{
	RowExpr *row = makeNode(RowExpr);

	row->args = args;
	row->row_typeid = typoid;
	row->row_format = COERCE_EXPLICIT_CAST;
	row->location = location;

	return (Node *) row;
}

static Node *
qualAndExpr(ParseState *pstate, Node *qual, Node *expr)
{
	if (qual == NULL)
		return expr;

	if (IsA(qual, BoolExpr))
	{
		BoolExpr *bexpr = (BoolExpr *) qual;

		if (bexpr->boolop == AND_EXPR)
		{
			bexpr->args = lappend(bexpr->args, expr);
			return qual;
		}
	}

	return (Node *) makeBoolExpr(AND_EXPR, list_make2(qual, expr), -1);
}

static ResTarget *
makeSimpleResTarget(char *field, char *name)
{
	ColumnRef *cref;

	cref = makeNode(ColumnRef);
	cref->fields = list_make1(makeString(pstrdup(field)));
	cref->location = -1;

	return makeResTarget((Node *) cref, name);
}

static ResTarget *
makeFieldsResTarget(List *fields, char *name)
{
	ColumnRef *cref;

	cref = makeNode(ColumnRef);
	cref->fields = fields;
	cref->location = -1;

	return makeResTarget((Node *) cref, name);
}

static ResTarget *
makeResTarget(Node *val, char *name)
{
	ResTarget *res;

	res = makeNode(ResTarget);
	if (name != NULL)
		res->name = pstrdup(name);
	res->val = val;
	res->location = -1;

	return res;
}

static A_Const *
makeIntConst(int val)
{
	A_Const *c;

	c = makeNode(A_Const);
	c->val.type = T_Integer;
	c->val.val.ival = val;
	c->location = -1;

	return c;
}

/* generate unique name */
static char *
genUniqueName(void)
{
	/* NOTE: safe unless there are more than 2^32 anonymous names at once */
	static uint32 seq = 0;

	char data[NAMEDATALEN];

	snprintf(data, sizeof(data), "<%010u>", seq++);

	return pstrdup(data);
}
