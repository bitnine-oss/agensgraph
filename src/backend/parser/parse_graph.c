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

#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_agg.h"
#include "parser/parse_clause.h"
#include "parser/parse_collate.h"
#include "parser/parse_graph.h"
#include "parser/parse_target.h"
#include "parser/parse_utilcmd.h"

typedef struct SelectInfo
{
	List	   *distinct;	/* DISTINCT clause */
	List	   *target;		/* the target list (of ResTarget) */
	List	   *from;		/* the FROM clause */
	Node	   *where;		/* WHERE qualification */
	List	   *order;		/* a list of SortBy's */
	Node	   *skip;		/* the number of result tuples to skip */
	Node	   *limit;		/* the number of result tuples to return */
} SelectInfo;

#define SELECT_INFO(name) \
	SelectInfo name = {NIL, NIL, NIL, NULL, NIL, NULL, NULL}

typedef struct PatternCtx
{
	Alias	   *alias;		/* alias of previous CypherClause */
	List	   *vertices;	/* vertex columns */
	List	   *edges;		/* edge columns */
	List	   *values;		/* other columns */
} PatternCtx;

/* trasnform */
static Query *transformSelectInfo(ParseState *pstate, SelectInfo *selinfo);
static PatternCtx *makePatternCtx(RangeTblEntry *rte);
static List *makeComponents(List *pattern);
static void findAndUnionComponents(CypherPath *path, List *components);
static bool isPathConnectedTo(CypherPath *path, List *component);
static bool arePathsConnected(CypherPath *p1, CypherPath *p2);
static void transformComponents(List *components, PatternCtx *ctx,
								List **fromClause, List **targetList,
								Node **whereClause);
static Node *transformCypherNode(CypherNode *node, PatternCtx *ctx,
					List **fromClause, List **targetList, Node **whereClause,
					List **nodeVars);
static RangeVar *transformCypherRel(CypherRel *rel, Node *left, Node *right,
					List **fromClause, List **targetList, Node **whereClause,
					List **relVars);
static Node *findNodeVar(List *nodeVars, char *varname);
static bool isNodeInPatternCtx(PatternCtx *ctx, char *varname);
static bool checkDupRelVar(List *relVars, char *varname);
static Node *makePathVertex(Node *nodeVar);
static Alias *makePatternVarAlias(char *aliasname);
static Node *makeLabelTuple(Alias *alias, Oid type_oid);
static Node *makePropMapConstraint(ColumnRef *props, char *qualStr);
static Node *makeDirQual(Node *start, RangeVar *rel, Node *end);
static void makeVertexId(Node *nodeVar, Node **oid, Node **id);
static Node *addNodeDupQual(Node *qual, List *nodeVars);
static Node *addRelDupQual(Node *qual, List *relVars);

/* parse tree */
static RangePrevclause *makeRangePrevclause(Node *clause);
static ColumnRef *makeSimpleColumnRef(char *colname, List *indirection,
									  int location);
static A_Indirection *makeIndirection(Node *arg, List *indirection);
static ResTarget *makeSelectResTarget(Node *value, char *label, int location);
static Alias *makeAliasNoDup(char *aliasname, List *colnames);
static RowExpr *makeTuple(List *args, int location);
static A_ArrayExpr *makeArray(List *elements, int location);
static TypeCast *makeTypeCast(Node *arg, TypeName *typename, int location);
static Node *qualAndExpr(Node *qual, Node *expr);

/* shortcuts */
static ColumnRef *makeAliasIndirection(Alias *alias, Node *indirection);
#define makeAliasStar(alias) \
	makeAliasIndirection(alias, (Node *) makeNode(A_Star))
#define makeAliasColname(alias, colname) \
	makeAliasIndirection(alias, (Node *) makeString(pstrdup(colname)))
#define makeIndirectionColname(arg, colname) \
	makeIndirection(arg, list_make1(makeString(pstrdup(colname))))
#define makeAliasStarTarget(alias) \
	makeSelectResTarget((Node *) makeAliasStar(alias), NULL, -1)
#define makeTypedTuple(args, type_oid) \
	((Node *) makeTypeCast((Node *) makeTuple(args, -1), \
						   makeTypeNameByOid(type_oid), -1))
#define makeTypedArray(elements, type_oid) \
	((Node *) makeTypeCast((Node *) makeArray(elements, -1), \
						   makeTypeNameByOid(type_oid), -1))
static TypeName *makeTypeNameByOid(Oid type_oid);

/* utils */
static char *genUniqueName(void);

Query *
transformCypherMatchClause(ParseState *pstate, CypherClause *clause)
{
	CypherMatchClause *detail = (CypherMatchClause *) clause->detail;
	SELECT_INFO(selinfo);
	RangePrevclause *r;

	if (detail->where != NULL)
	{
		r = makeRangePrevclause((Node *) clause);

		selinfo.target = list_make1(makeAliasStarTarget(r->alias));
		selinfo.from = list_make1(r);
		selinfo.where = detail->where;

		/*
		 * detach WHERE clause so that this funcion passes through
		 * this if statement when the function is called again recursively
		 */
		detail->where = NULL;
	}
	else
	{
		PatternCtx *ctx = NULL;
		List	   *components;

		/*
		 * Transform previous CypherClause as a RangeSubselect first.
		 * It must be transformed at here because when transformComponents(),
		 * variables in the pattern which are in the resulting variables of
		 * the previous CypherClause must be excluded from fromClause.
		 */
		if (clause->prev != NULL)
		{
			RangeTblEntry *rte;

			r = makeRangePrevclause(clause->prev);
			rte = transformRangePrevclause(pstate, r);
			ctx = makePatternCtx(rte);
		}

		components = makeComponents(detail->pattern);

		transformComponents(components, ctx,
							&selinfo.from, &selinfo.target, &selinfo.where);
	}

	return transformSelectInfo(pstate, &selinfo);
}

Query *
transformCypherProjection(ParseState *pstate, CypherClause *clause)
{
	CypherProjection *detail = (CypherProjection *) clause->detail;
	SELECT_INFO(selinfo);
	RangePrevclause *r;

	if (detail->where != NULL)
	{
		AssertArg(detail->kind == CP_WITH);

		r = makeRangePrevclause((Node *) clause);

		selinfo.target = list_make1(makeAliasStarTarget(r->alias));
		selinfo.from = list_make1(r);
		selinfo.where = detail->where;

		detail->where = NULL;
	}
	else if (detail->distinct != NULL || detail->order != NULL ||
			 detail->skip != NULL || detail->limit != NULL)
	{
		r = makeRangePrevclause((Node *) clause);

		selinfo.distinct = detail->distinct;
		selinfo.target = list_make1(makeAliasStarTarget(r->alias));
		selinfo.from = list_make1(r);
		selinfo.order = detail->order;
		selinfo.skip = detail->skip;
		selinfo.limit = detail->limit;

		/*
		 * detach options so that this funcion passes through this if statement
		 * when the function is called again recursively
		 */
		detail->distinct = NIL;
		detail->order = NIL;
		detail->skip = NULL;
		detail->limit = NULL;
	}
	else
	{
		selinfo.target = detail->items;
		if (clause->prev != NULL)
			selinfo.from = list_make1(makeRangePrevclause(clause->prev));
	}

	return transformSelectInfo(pstate, &selinfo);
}

/* composed of some lines from transformSelectStmt() we need */
static Query *
transformSelectInfo(ParseState *pstate, SelectInfo *selinfo)
{
	Query	   *qry;
	Node	   *qual;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	transformFromClause(pstate, selinfo->from);

	qry->targetList = transformTargetList(pstate, selinfo->target,
										  EXPR_KIND_SELECT_TARGET);
	markTargetListOrigins(pstate, qry->targetList);

	qual = transformWhereClause(pstate, selinfo->where,
								EXPR_KIND_WHERE, "WHERE");

	qry->sortClause = transformSortClause(pstate, selinfo->order,
										  &qry->targetList, EXPR_KIND_ORDER_BY,
										  true, false);

	qry->groupClause = generateGroupClause(pstate, &qry->targetList,
										   qry->sortClause);

	if (selinfo->distinct == NIL)
	{
		/* intentionally blank, do nothing */
	}
	else if (linitial(selinfo->distinct) == NULL)
	{
		qry->distinctClause = transformDistinctClause(pstate, &qry->targetList,
													  qry->sortClause, false);
	}
	else
	{
		qry->distinctClause = transformDistinctOnClause(pstate,
														selinfo->distinct,
														&qry->targetList,
														qry->sortClause);
		qry->hasDistinctOn = true;
	}

	qry->limitOffset = transformLimitClause(pstate, selinfo->skip,
											EXPR_KIND_OFFSET, "OFFSET");
	qry->limitCount = transformLimitClause(pstate, selinfo->limit,
										   EXPR_KIND_LIMIT, "LIMIT");

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	qry->hasAggs = pstate->p_hasAggs;
	if (qry->hasAggs)
		parseCheckAggregates(pstate, qry);

	assign_query_collations(pstate, qry);

	return qry;
}

static PatternCtx *
makePatternCtx(RangeTblEntry *rte)
{
	ListCell   *lt;
	List	   *colnames = NIL;
	List       *vertices = NIL;
	List       *edges = NIL;
	List       *values = NIL;
	PatternCtx *ctx;

	AssertArg(rte->rtekind == RTE_SUBQUERY);

	foreach(lt, rte->subquery->targetList)
	{
		TargetEntry *te = lfirst(lt);
		Value	   *colname;
		Oid			vartype;

		Assert(!(te == NULL || te->resjunk));

		colname = makeString(pstrdup(te->resname));

		colnames = lappend(colnames, colname);

		vartype = exprType((Node *) te->expr);
		switch (vartype)
		{
			case VERTEXOID:
				vertices = lappend(vertices, colname);
				break;
			case EDGEOID:
				edges = lappend(edges, colname);
				break;
			default:
				Assert(strcmp(strVal(colname), "?column?") != 0);
				values = lappend(values, colname);
		}
	}

	ctx = palloc(sizeof(*ctx));
	ctx->alias = makeAlias(rte->alias->aliasname, colnames);
	ctx->vertices = vertices;
	ctx->edges = edges;
	ctx->values = values;

	return ctx;
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

		if (components == NIL)
		{
			/* a connected component is a list of CypherPath's */
			List *c = list_make1(p);
			components = list_make1(c);
		}
		else
		{
			findAndUnionComponents(p, components);
		}
	}

	return components;
}

static void
findAndUnionComponents(CypherPath *path, List *components)
{
	List	   *repr;
	ListCell   *lc;
	ListCell   *prev;

	AssertArg(components != NIL);

	/* find the first connected component */
	repr = NIL;
	lc = list_head(components);
	while (lc != NULL)
	{
		List *c = lfirst(lc);

		if (isPathConnectedTo(path, c))
		{
			repr = c;
			break;
		}

		lc = lnext(lc);
	}

	/* there is no matched connected component */
	if (repr == NIL)
	{
		lappend(components, list_make1(path));
		return;
	}

	/* find other connected components and merge them to repr */
	prev = lc;
	lc = lnext(lc);
	while (lc != NULL)
	{
		List *c = lfirst(lc);

		if (isPathConnectedTo(path, c))
		{
			ListCell *next;

			list_concat(repr, c);

			next = lnext(lc);
			list_delete_cell(components, lc, prev);
			lc = next;
		}
		else
		{
			prev = lc;
			lc = lnext(lc);
		}
	}

	/* add the path to repr */
	lappend(repr, path);
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
arePathsConnected(CypherPath *p1, CypherPath *p2)
{
	ListCell *le1;

	foreach(le1, p1->chain)
	{
		CypherNode *n1 = lfirst(le1);
		char	   *var1;
		ListCell   *le2;

		/* node variables are only concern */
		if (!IsA(n1, CypherNode))
			continue;
		var1 = getCypherName(n1->variable);
		if (var1 == NULL)
			continue;

		foreach(le2, p2->chain)
		{
			CypherNode *n2 = lfirst(le2);
			char	   *var2;

			if (!IsA(n2, CypherNode))
				continue;
			var2 = getCypherName(n2->variable);
			if (var2 == NULL)
				continue;

			if (strcmp(var1, var2) == 0)
				return true;
		}
	}

	return false;
}

static void
transformComponents(List *components, PatternCtx *ctx,
					List **fromClause, List **targetList, Node **whereClause)
{
	ListCell *lc;

	if (ctx != NULL)
	{
		/* add all columns in the result of previous CypherClause */
		*targetList = lcons(makeAliasStarTarget(ctx->alias), *targetList);
	}

	foreach(lc, components)
	{
		List	   *c = lfirst(lc);
		ListCell   *lp;
		List	   *nodeVars = NIL;
		List	   *relVars = NIL;

		foreach(lp, c)
		{
			CypherPath *p = lfirst(lp);
			bool		out = (p->variable != NULL);
			List	   *vertices = NIL;
			List	   *edges = NIL;
			ListCell   *e;
			CypherNode *node;
			CypherRel  *rel;
			Node	   *left;
			Node	   *right;
			RangeVar   *r;

			e = list_head(p->chain);
			node = lfirst(e);
			left = transformCypherNode(node, ctx,
									   fromClause, targetList, whereClause,
									   &nodeVars);
			if (out)
				vertices = lappend(vertices, makePathVertex(left));

			for (;;)
			{
				e = lnext(e);

				/* no more pattern chain (<rel,node> pair) */
				if (e == NULL)
					break;

				rel = lfirst(e);

				e = lnext(e);
				node = lfirst(e);
				right = transformCypherNode(node, ctx,
										fromClause, targetList, whereClause,
										&nodeVars);
				if (out)
					vertices = lappend(vertices, makePathVertex(right));

				r = transformCypherRel(rel, left, right,
									   fromClause, targetList, whereClause,
									   &relVars);
				if (out)
					edges = lappend(edges, makeLabelTuple(r->alias, EDGEOID));

				left = right;
			}

			/* add this path to the target list */
			if (out)
			{
				Node	   *varr;
				Node	   *earr;
				Node	   *tuple;
				ResTarget  *target;

				varr = makeTypedArray(vertices, VERTEXARRAYOID);
				earr = makeTypedArray(edges, EDGEARRAYOID);

				tuple = makeTypedTuple(list_make2(varr, earr), GRAPHPATHOID);

				target = makeSelectResTarget(tuple,
											 getCypherName(p->variable),
											 getCypherNameLoc(p->variable));

				*targetList = lappend(*targetList, target);
			}
		}

		/* all unique nodes are different from each other */
		*whereClause = addNodeDupQual(*whereClause, nodeVars);
		/* all relationships are different from each other */
		*whereClause = addRelDupQual(*whereClause, relVars);
	}
}

static Node *
transformCypherNode(CypherNode *node, PatternCtx *ctx,
					List **fromClause, List **targetList, Node **whereClause,
					List **nodeVars)
{
	char	   *varname;
	Node	   *nv;
	char	   *label;
	int			location;
	RangeVar   *r;

	varname = getCypherName(node->variable);

	/*
	 * If a node with the same variable was already processed, skip this node.
	 * A node without variable is considered as if it has a unique variable,
	 * so process it.
	 */
	nv = findNodeVar(*nodeVars, varname);
	if (nv != NULL)
		return (Node *) nv;

	/*
	 * If this node came from the previous CypherClause, add it to nodeVars
	 * to mark it "processed".
	 */
	if (isNodeInPatternCtx(ctx, varname))
	{
		ColumnRef *colref;

		colref = makeSimpleColumnRef(varname, NIL, -1);
		*nodeVars = lappend(*nodeVars, colref);

		return (Node *) colref;
	}

	/*
	 * process the newly introduced node
	 */

	label = getCypherName(node->label);
	if (label == NULL)
		label = AG_VERTEX;
	location = getCypherNameLoc(node->label);

	r = makeRangeVar(AG_GRAPH, label, location);
	r->inhOpt = INH_YES;
	r->alias = makePatternVarAlias(varname);

	*fromClause = lappend(*fromClause, r);

	/* add this node to the result */
	if (varname != NULL)
	{
		Node	   *tuple;
		ResTarget  *target;

		tuple = makeLabelTuple(r->alias, VERTEXOID);

		target = makeSelectResTarget(tuple, varname,
									 getCypherNameLoc(node->variable));

		*targetList = lappend(*targetList, target);
	}

	/* add property map constraint */
	if (node->prop_map != NULL)
	{
		ColumnRef  *prop_map;
		Node	   *constraint;

		prop_map = makeAliasColname(r->alias, "prop_map");
		constraint = makePropMapConstraint(prop_map, node->prop_map);
		*whereClause = qualAndExpr(*whereClause, constraint);
	}

	/* mark this node "processed" */
	*nodeVars = lappend(*nodeVars, r);

	return (Node *) r;
}

/* connect two nodes with a relationship */
static RangeVar *
transformCypherRel(CypherRel *rel, Node *left, Node *right,
				   List **fromClause, List **targetList, Node **whereClause,
				   List **relVars)
{
	char	   *varname;
	char	   *typename;
	int			location;
	RangeVar   *r;

	varname = getCypherName(rel->variable);

	/* all relationships are unique */
	if (checkDupRelVar(*relVars, varname))
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("all relationship must be unique - "
						"\"%s\" specified more than once", varname)));
	}

	if (rel->types == NULL)
	{
		typename = AG_EDGE;
		location = -1;
	}
	else
	{
		Node *type = linitial(rel->types);

		/* TODO: support multiple types */
		typename = getCypherName(type);
		location = getCypherNameLoc(type);
	}

	r = makeRangeVar(AG_GRAPH, typename, location);
	r->inhOpt = INH_YES;
	r->alias = makePatternVarAlias(varname);

	*fromClause = lappend(*fromClause, r);

	/* add this relationship to the result */
	if (varname != NULL)
	{
		Node	   *tuple;
		ResTarget  *target;

		tuple = makeLabelTuple(r->alias, EDGEOID);

		target = makeSelectResTarget(tuple, varname,
									 getCypherNameLoc(rel->variable));

		*targetList = lappend(*targetList, target);
	}

	/* add property map constraint */
	if (rel->prop_map != NULL)
	{
		ColumnRef  *prop_map;
		Node	   *constraint;

		prop_map = makeAliasColname(r->alias, "prop_map");
		constraint = makePropMapConstraint(prop_map, rel->prop_map);
		*whereClause = qualAndExpr(*whereClause, constraint);
	}

	/* <node,rel,node> JOIN conditions */
	if (rel->direction == CYPHER_REL_DIR_NONE)
	{
		Node	   *lexpr;
		Node	   *rexpr;
		Node	   *nexpr;

		lexpr = makeDirQual(right, r, left);
		rexpr = makeDirQual(left, r, right);
		nexpr = (Node *) makeBoolExpr(OR_EXPR, list_make2(lexpr, rexpr), -1);
		*whereClause = qualAndExpr(*whereClause, nexpr);
	}
	else if (rel->direction == CYPHER_REL_DIR_LEFT)
	{
		*whereClause = qualAndExpr(*whereClause, makeDirQual(right, r, left));
	}
	else
	{
		Assert(rel->direction == CYPHER_REL_DIR_RIGHT);
		*whereClause = qualAndExpr(*whereClause, makeDirQual(left, r, right));
	}

	/* remember processed relationships */
	*relVars = lappend(*relVars, r);

	return r;
}

static Node *
findNodeVar(List *nodeVars, char *varname)
{
	ListCell *lv;

	if (varname == NULL)
		return NULL;

	foreach(lv, nodeVars)
	{
		Node *n = lfirst(lv);

		/* a node in the current pattern */
		if (IsA(n, RangeVar))
		{
			if (strcmp(((RangeVar *) n)->alias->aliasname, varname) == 0)
				return n;
		}
		/* a node in the previous CypherClause */
		else
		{
			ColumnRef *colref = (ColumnRef *) n;
			Assert(IsA(colref, ColumnRef));

			if (strcmp(strVal(linitial(colref->fields)), varname) == 0)
				return n;
		}
	}

	return NULL;
}

static bool
isNodeInPatternCtx(PatternCtx *ctx, char *varname)
{
	ListCell *lv;

	if (ctx == NULL || varname == NULL)
		return false;

	foreach(lv, ctx->vertices)
	{
		if (strcmp(strVal(lfirst(lv)), varname) == 0)
			return true;
	}

	return false;
}

static bool
checkDupRelVar(List *relVars, char *varname)
{
	ListCell *lv;

	if (varname == NULL)
		return false;

	foreach(lv, relVars)
	{
		RangeVar *r = lfirst(lv);

		if (strcmp(r->alias->aliasname, varname) == 0)
			return true;
	}

	return false;
}

static Node *
makePathVertex(Node *nodeVar)
{
	if (IsA(nodeVar, RangeVar))
	{
		RangeVar *r = (RangeVar *) nodeVar;

		return makeLabelTuple(r->alias, VERTEXOID);
	}
	else
	{
		ColumnRef *colref = (ColumnRef *) nodeVar;
		Assert(IsA(colref, ColumnRef));

		return copyObject(colref);
	}
}

static Alias *
makePatternVarAlias(char *aliasname)
{
	aliasname = (aliasname == NULL ? genUniqueName() : pstrdup(aliasname));
	return makeAliasNoDup(aliasname, NIL);
}

/* ROW(alias.tableoid, alias.*)::type */
static Node *
makeLabelTuple(Alias *alias, Oid type_oid)
{
	ColumnRef  *oid;
	ColumnRef  *star;
	Node	   *tuple;

	oid = makeAliasColname(alias, "tableoid");
	star = makeAliasStar(alias);
	tuple = makeTypedTuple(list_make2(oid, star), type_oid);

	return tuple;
}

/* jsonb_contains(prop_map, qualStr) */
static Node *
makePropMapConstraint(ColumnRef *prop_map, char *qualStr)
{
	FuncCall   *constraint;
	A_Const	   *qual;

	qual = makeNode(A_Const);
	qual->val.type = T_String;
	qual->val.val.str = qualStr;
	qual->location = -1;

	constraint = makeFuncCall(list_make1(makeString("jsonb_contains")),
							  list_make2(prop_map, qual), -1);

	return (Node *) constraint;
}

static Node *
makeDirQual(Node *start, RangeVar *rel, Node *end)
{
	Node	   *start_oid;
	Node	   *start_id;
	Node	   *s_oid;
	Node	   *s_id;
	Node	   *e_oid;
	Node	   *e_id;
	Node	   *end_oid;
	Node	   *end_id;
	List	   *args;

	makeVertexId(start, &start_oid, &start_id);
	s_oid = (Node *) makeAliasColname(rel->alias, AG_START_OID);
	s_id = (Node *) makeAliasColname(rel->alias, AG_START_ID);
	e_oid = (Node *) makeAliasColname(rel->alias, AG_END_OID);
	e_id = (Node *) makeAliasColname(rel->alias, AG_END_ID);
	makeVertexId(end, &end_oid, &end_id);

	args = list_make4(makeSimpleA_Expr(AEXPR_OP, "=", s_oid, start_oid, -1),
					  makeSimpleA_Expr(AEXPR_OP, "=", s_id, start_id, -1),
					  makeSimpleA_Expr(AEXPR_OP, "=", e_oid, end_oid, -1),
					  makeSimpleA_Expr(AEXPR_OP, "=", e_id, end_id, -1));

	return (Node *) makeBoolExpr(AND_EXPR, args, -1);
}

static void
makeVertexId(Node *nodeVar, Node **oid, Node **id)
{
	if (IsA(nodeVar, RangeVar))
	{
		RangeVar *r = (RangeVar *) nodeVar;

		*oid = (Node *) makeAliasColname(r->alias, "tableoid");
		*id = (Node *) makeAliasColname(r->alias, AG_ELEM_ID);
	}
	else
	{
		ColumnRef *colref = (ColumnRef *) nodeVar;
		AssertArg(IsA(colref, ColumnRef));

		*oid = (Node *) makeIndirectionColname((Node *) colref, "oid");
		*id = (Node *) makeIndirectionColname((Node *) colref, "id");
	}
}

static Node *
addNodeDupQual(Node *qual, List *nodeVars)
{
	ListCell *lv1;

	foreach(lv1, nodeVars)
	{
		Node	   *n1 = lfirst(lv1);
		ListCell   *lv2;

		for_each_cell(lv2, lnext(lv1))
		{
			Node	   *n2 = lfirst(lv2);
			Node	   *oid1;
			Node	   *id1;
			Node	   *oid2;
			Node	   *id2;
			List	   *args;

			makeVertexId(n1, &oid1, &id1);
			makeVertexId(n2, &oid2, &id2);

			args = list_make2(makeSimpleA_Expr(AEXPR_OP, "<>", oid1, oid2, -1),
							  makeSimpleA_Expr(AEXPR_OP, "<>", id1, id2, -1));

			qual = qualAndExpr(qual, (Node *) makeBoolExpr(OR_EXPR, args, -1));
		}
	}

	return qual;
}

static Node *
addRelDupQual(Node *qual, List *relVars)
{
	ListCell *lv1;

	foreach(lv1, relVars)
	{
		RangeVar   *r1 = lfirst(lv1);
		ListCell   *lv2;

		for_each_cell(lv2, lnext(lv1))
		{
			RangeVar   *r2 = lfirst(lv2);
			Node	   *oid1;
			Node	   *id1;
			Node	   *oid2;
			Node	   *id2;
			List	   *args;

			oid1 = (Node *) makeAliasColname(r1->alias, "tableoid");
			id1 = (Node *) makeAliasColname(r1->alias, AG_ELEM_ID);

			oid2 = (Node *) makeAliasColname(r2->alias, "tableoid");
			id2 = (Node *) makeAliasColname(r2->alias, AG_ELEM_ID);

			args = list_make2(makeSimpleA_Expr(AEXPR_OP, "<>", oid1, oid2, -1),
							  makeSimpleA_Expr(AEXPR_OP, "<>", id1, id2, -1));

			qual = qualAndExpr(qual, (Node *) makeBoolExpr(OR_EXPR, args, -1));
		}
	}

	return qual;
}

/* Cypher as a RangeSubselect */
static RangePrevclause *
makeRangePrevclause(Node *clause)
{
	RangePrevclause *r;

	AssertArg(IsA(clause, CypherClause));

	r = (RangePrevclause *) makeNode(RangeSubselect);
	r->subquery = clause;

	r->alias = makeNode(Alias);
	r->alias->aliasname = "_";

	return r;
}

/* colname.indirection[0].indirection[1]... */
static ColumnRef *
makeSimpleColumnRef(char *colname, List *indirection, int location)
{
	ColumnRef *colref;

	colref = makeNode(ColumnRef);
	colref->fields = lcons(makeString(pstrdup(colname)), indirection);
	colref->location = location;

	return colref;
}

/* (arg).indirection[0].indirection[1]... */
static A_Indirection *
makeIndirection(Node *arg, List *indirection)
{
	A_Indirection *ind;

	ind = makeNode(A_Indirection);
	ind->arg = arg;
	ind->indirection = indirection;

	return ind;
}

/* value AS label */
static ResTarget *
makeSelectResTarget(Node *value, char *label, int location)
{
	ResTarget *target;

	target = makeNode(ResTarget);
	target->name = (label == NULL ? NULL : pstrdup(label));
	target->val = value;
	target->location = location;

	return target;
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

static RowExpr *
makeTuple(List *args, int location)
{
	RowExpr *row;

	row = makeNode(RowExpr);
	row->args = args;
	row->row_format = COERCE_IMPLICIT_CAST;	/* abuse */
	row->location = location;

	return row;
}

static A_ArrayExpr *
makeArray(List *elements, int location)
{
	A_ArrayExpr *array;

	array = makeNode(A_ArrayExpr);
	array->elements = elements;
	array->location = location;

	return array;
}

static TypeCast *
makeTypeCast(Node *arg, TypeName *typename, int location)
{
	TypeCast *cast;

	cast = makeNode(TypeCast);
	cast->arg = arg;
	cast->typeName = typename;
	cast->location = location;

	return cast;
}

/* qual AND expr */
static Node *
qualAndExpr(Node *qual, Node *expr)
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

/* aliasname.indirection */
static ColumnRef *
makeAliasIndirection(Alias *alias, Node *indirection)
{
	return makeSimpleColumnRef(alias->aliasname, list_make1(indirection), -1);
}

static TypeName *
makeTypeNameByOid(Oid type_oid)
{
	switch (type_oid)
	{
		case VERTEXARRAYOID:
			return makeTypeName("_vertex");
		case VERTEXOID:
			return makeTypeName("vertex");
		case EDGEARRAYOID:
			return makeTypeName("_edge");
		case EDGEOID:
			return makeTypeName("edge");
		case GRAPHPATHOID:
			return makeTypeName("graphpath");
		default:
			AssertArg(type_oid == VERTEXOID ||
					  type_oid == EDGEOID ||
					  type_oid == GRAPHPATHOID);
			return NULL;
	}
}

/* generate unique name */
static char *
genUniqueName(void)
{
	/* NOTE: safe unless there are more than 2^32 anonymous names */
	static uint32 seq = 0;

	char data[NAMEDATALEN];

	snprintf(data, sizeof(data), "<%010u>", seq++);

	return pstrdup(data);
}
