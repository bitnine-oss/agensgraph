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
#include "access/heapam.h"
#include "catalog/pg_type.h"
#include "nodes/graphnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_agg.h"
#include "parser/parse_clause.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_graph.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "storage/lock.h"

#define CYPHER_SUBQUERY_ALIAS	"_"

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
static void checkItemsName(ParseState *pstate, List *items);
static bool valueHasImplicitName(Node *val);
static Query *transformSelectInfo(ParseState *pstate, SelectInfo *selinfo);
static PatternCtx *makePatternCtx(RangeTblEntry *rte);
static List *makeComponents(List *pattern);
static void findAndUnionComponents(CypherPath *path, List *components);
static bool isPathConnectedTo(CypherPath *path, List *component);
static bool arePathsConnected(CypherPath *p1, CypherPath *p2);
static void transformComponents(List *components, PatternCtx *ctx,
								List **fromClause, List **targetList,
								Node **whereClause);
static Node *transformCypherNode(CypherNode *cnode, PatternCtx *ctx,
					List **fromClause, List **targetList, Node **whereClause,
					List **nodeVars);
static RangeVar *transformCypherRel(CypherRel *crel, Node *left, Node *right,
					List **fromClause, List **targetList, Node **whereClause,
					List **relVars);
static Node *findNodeVar(List *nodeVars, char *varname);
static bool checkDupRelVar(List *relVars, char *varname);
static Node *makePathVertex(Node *nodeVar);
static Node *makeGraphpath(List *vertices, List *edges);
static Alias *makePatternVarAlias(char *aliasname);
static Node *makeGraphidTuple(Alias *alias);
static Node *makeVertexTuple(Alias *alias);
static Node *makeEdgeTuple(Alias *alias);
static Node *makePropMapConstraint(ColumnRef *props, Node *qualJson);
static Node *postprocessPropMapExpr(Node *prop_map_expr);
static Node *makeDirQual(Node *start, RangeVar *rel, Node *end);
static Node *makeVertexId(Node *nodeVar);
static Node *addNodeDupQual(Node *qual, List *nodeVars);
static Node *addRelDupQual(Node *qual, List *relVars);
static List *transformCreatePattern(ParseState *pstate, List *pattern,
									PatternCtx *ctx, List **targetList);
static GraphVertex *transformCreateVertex(ParseState *pstate, CypherNode *cnode,
										  PatternCtx *ctx, List **targetList);
static GraphEdge *transformCreateEdge(ParseState *pstate, CypherRel *crel,
									  PatternCtx *ctx, List **targetList);
static bool isNodeForReference(CypherNode *cnode);
static ResTarget *makeDummyVertex(char *varname);
static ResTarget *makeDummyEdge(char *varname);
static char *getLabelFromElem(Node *elem);
static void preventDropLabel(ParseState *pstate, char *labname);

/* parse tree */
static RangePrevclause *makeRangePrevclause(Node *clause);
static ColumnRef *makeSimpleColumnRef(char *colname, List *indirection,
									  int location);
static ResTarget *makeSelectResTarget(Node *value, char *label, int location);
static Alias *makeAliasNoDup(char *aliasname, List *colnames);
static RowExpr *makeTuple(List *args, int location);
static A_ArrayExpr *makeArray(List *elements, int location);
static TypeCast *makeTypeCast(Node *arg, TypeName *typename, int location);
static Node *qualAndExpr(Node *qual, Node *expr);
static Value *findStringValue(List *list, char *str);
#define isStringValueIn(list, str)	(findStringValue(list, str) != NULL)

/* shortcuts */
static ColumnRef *makeAliasIndirection(Alias *alias, Node *indirection);
#define makeAliasStar(alias) \
	makeAliasIndirection(alias, (Node *) makeNode(A_Star))
#define makeAliasColname(alias, colname) \
	makeAliasIndirection(alias, (Node *) makeString(pstrdup(colname)))
static Node *makeColumnProjection(ColumnRef *colref, char *attname);
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

			/* add all columns in the result of previous CypherClause */
			selinfo.target = lcons(makeAliasStarTarget(ctx->alias),
								   selinfo.target);
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
		if (detail->kind == CP_WITH)
			checkItemsName(pstate, detail->items);

		selinfo.target = detail->items;
		if (clause->prev != NULL)
			selinfo.from = list_make1(makeRangePrevclause(clause->prev));
	}

	return transformSelectInfo(pstate, &selinfo);
}

/* check whether resulting columns have a name or not */
static void
checkItemsName(ParseState *pstate, List *items)
{
	ListCell *item;

	foreach(item, items)
	{
		ResTarget *target = (ResTarget *) lfirst(item);

		if (target->name != NULL)
			continue;

		if (!valueHasImplicitName(target->val))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("Expression in WITH must be aliased (use AS)"),
					 parser_errposition(pstate, exprLocation(target->val))));
	}
}

/*
 * All cases except those three below need an explicit name through AS.
 *
 * See FigureColnameInternal()
 */
static bool
valueHasImplicitName(Node *val)
{
	if (val == NULL)
		return false;

	switch (nodeTag(val))
	{
		case T_ColumnRef:
			return true;
		case T_A_Indirection:
			{
				A_Indirection *ind = (A_Indirection *) val;

				if (IsA(llast(ind->indirection), A_Star))
					return true;
			}
			break;
		case T_A_Expr:
			if (((A_Expr *) val)->kind == AEXPR_PAREN)
				return valueHasImplicitName(((A_Expr *) val)->lexpr);
			break;
		default:
			break;
	}

	return false;
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

	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasAggs = pstate->p_hasAggs;
	if (qry->hasAggs)
		parseCheckAggregates(pstate, qry);

	assign_query_collations(pstate, qry);

	return qry;
}

Query *
transformCypherCreateClause(ParseState *pstate, CypherClause *clause)
{
	CypherCreateClause *detail;
	CypherClause *prevclause;
	List	   *pattern;
	PatternCtx *ctx = NULL;
	List	   *targetList = NIL;
	List	   *queryPattern = NIL;
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

	/* transform previous clause */
	if (prevclause != NULL)
	{
		RangePrevclause *r;
		RangeTblEntry *rte;

		r = makeRangePrevclause((Node *) prevclause);
		rte = transformRangePrevclause(pstate, r);
		ctx = makePatternCtx(rte);

		targetList = lcons(makeAliasStarTarget(ctx->alias), targetList);
	}

	queryPattern = transformCreatePattern(pstate, pattern, ctx, &targetList);

	qry = makeNode(Query);
	qry->commandType = CMD_GRAPHWRITE;
	qry->graph.writeOp = GWROP_CREATE;
	qry->graph.last = (pstate->parentParseState == NULL);

	/*
	 * Although CREATE clause doesn't have FROM list, we must call
	 * transformFromClause() to clean up `lateral_only`.
	 */
	transformFromClause(pstate, NIL);

	qry->targetList = transformTargetList(pstate, targetList,
										  EXPR_KIND_SELECT_TARGET);
	markTargetListOrigins(pstate, qry->targetList);

	qry->graph.pattern = queryPattern;

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
	List	   *fromClause;
	RangeTblEntry *rte;
	int			rtindex;
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
	fromClause = list_make1(makeRangePrevclause(clause->prev));
	transformFromClause(pstate, fromClause);

	/* select all from previous clause */
	rte = refnameRangeTblEntry(pstate, NULL, CYPHER_SUBQUERY_ALIAS, -1, NULL);
	rtindex = RTERangeTablePosn(pstate, rte, NULL);
	qry->targetList = expandRelAttrs(pstate, rte, rtindex, 0, -1);

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

		if (te->resjunk)
			continue;

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

	foreach(lc, components)
	{
		List	   *c = lfirst(lc);
		ListCell   *lp;
		List	   *nodeVars = NIL;
		List	   *relVars = NIL;

		foreach(lp, c)
		{
			CypherPath *p = lfirst(lp);
			char	   *pathname = getCypherName(p->variable);
			bool		out = (pathname != NULL);
			List	   *vertices = NIL;
			List	   *edges = NIL;
			ListCell   *le;
			CypherNode *cnode;
			CypherRel  *crel;
			Node	   *left;
			Node	   *right;
			RangeVar   *r;

			le = list_head(p->chain);
			cnode = lfirst(le);
			left = transformCypherNode(cnode, ctx,
									   fromClause, targetList, whereClause,
									   &nodeVars);
			if (out)
				vertices = lappend(vertices, makePathVertex(left));

			for (;;)
			{
				le = lnext(le);

				/* no more pattern chain (<rel,node> pair) */
				if (le == NULL)
					break;

				crel = lfirst(le);

				le = lnext(le);
				cnode = lfirst(le);
				right = transformCypherNode(cnode, ctx,
										fromClause, targetList, whereClause,
										&nodeVars);
				if (out)
					vertices = lappend(vertices, makePathVertex(right));

				r = transformCypherRel(crel, left, right,
									   fromClause, targetList, whereClause,
									   &relVars);
				if (out)
					edges = lappend(edges, makeEdgeTuple(r->alias));

				left = right;
			}

			/* add this path to the target list */
			if (out)
			{
				ResTarget *target;

				target = makeSelectResTarget(makeGraphpath(vertices, edges),
											 pathname,
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
transformCypherNode(CypherNode *cnode, PatternCtx *ctx,
					List **fromClause, List **targetList, Node **whereClause,
					List **nodeVars)
{
	char	   *varname;
	Node	   *nv;
	char	   *label;
	int			location;
	RangeVar   *r;

	varname = getCypherName(cnode->variable);

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
	if (ctx != NULL && isStringValueIn(ctx->vertices, varname))
	{
		ColumnRef *colref;

		colref = makeSimpleColumnRef(varname, NIL, -1);
		*nodeVars = lappend(*nodeVars, colref);

		return (Node *) colref;
	}

	/*
	 * process the newly introduced node
	 */

	label = getCypherName(cnode->label);
	if (label == NULL)
		label = AG_VERTEX;
	location = getCypherNameLoc(cnode->label);

	r = makeRangeVar(AG_GRAPH, label, location);
	r->inhOpt = INH_YES;
	r->alias = makePatternVarAlias(varname);

	*fromClause = lappend(*fromClause, r);

	/* add this node to the result */
	if (varname != NULL)
	{
		Node	   *tuple;
		ResTarget  *target;

		tuple = makeVertexTuple(r->alias);

		target = makeSelectResTarget(tuple, varname,
									 getCypherNameLoc(cnode->variable));

		*targetList = lappend(*targetList, target);
	}

	/* add property map constraint */
	if (cnode->prop_map != NULL)
	{
		ColumnRef  *prop_map;
		Node	   *constraint;

		prop_map = makeAliasColname(r->alias, AG_ELEM_PROP_MAP);
		constraint = makePropMapConstraint(prop_map, cnode->prop_map);
		*whereClause = qualAndExpr(*whereClause, constraint);
	}

	/* mark this node "processed" */
	*nodeVars = lappend(*nodeVars, r);

	return (Node *) r;
}

/* connect two nodes with a relationship */
static RangeVar *
transformCypherRel(CypherRel *crel, Node *left, Node *right,
				   List **fromClause, List **targetList, Node **whereClause,
				   List **relVars)
{
	char	   *varname;
	char	   *typename;
	int			location;
	RangeVar   *r;

	varname = getCypherName(crel->variable);

	/* all relationships are unique */
	if (checkDupRelVar(*relVars, varname))
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("all relationship must be unique - "
						"\"%s\" specified more than once", varname)));
	}

	if (crel->types == NULL)
	{
		typename = AG_EDGE;
		location = -1;
	}
	else
	{
		Node *type = linitial(crel->types);

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

		tuple = makeEdgeTuple(r->alias);

		target = makeSelectResTarget(tuple, varname,
									 getCypherNameLoc(crel->variable));

		*targetList = lappend(*targetList, target);
	}

	/* add property map constraint */
	if (crel->prop_map != NULL)
	{
		ColumnRef  *prop_map;
		Node	   *constraint;

		prop_map = makeAliasColname(r->alias, AG_ELEM_PROP_MAP);
		constraint = makePropMapConstraint(prop_map, crel->prop_map);
		*whereClause = qualAndExpr(*whereClause, constraint);
	}

	/* <node,rel,node> JOIN conditions */
	if (crel->direction == CYPHER_REL_DIR_NONE)
	{
		Node	   *lexpr;
		Node	   *rexpr;
		Node	   *nexpr;

		lexpr = makeDirQual(right, r, left);
		rexpr = makeDirQual(left, r, right);
		nexpr = (Node *) makeBoolExpr(OR_EXPR, list_make2(lexpr, rexpr), -1);
		*whereClause = qualAndExpr(*whereClause, nexpr);
	}
	else if (crel->direction == CYPHER_REL_DIR_LEFT)
	{
		*whereClause = qualAndExpr(*whereClause, makeDirQual(right, r, left));
	}
	else
	{
		Assert(crel->direction == CYPHER_REL_DIR_RIGHT);
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

		return makeVertexTuple(r->alias);
	}
	else
	{
		ColumnRef *colref = (ColumnRef *) nodeVar;
		Assert(IsA(colref, ColumnRef));

		return copyObject(colref);
	}
}

static Node *
makeGraphpath(List *vertices, List *edges)
{
	Node	   *varr;
	Node	   *earr;
	Node	   *tuple;

	varr = makeTypedArray(vertices, VERTEXARRAYOID);
	earr = makeTypedArray(edges, EDGEARRAYOID);

	tuple = makeTypedTuple(list_make2(varr, earr), GRAPHPATHOID);

	return tuple;
}

static Alias *
makePatternVarAlias(char *aliasname)
{
	aliasname = (aliasname == NULL ? genUniqueName() : pstrdup(aliasname));
	return makeAliasNoDup(aliasname, NIL);
}

/* ROW(alias.tableoid, alias.AG_ELEM_LOCAL_ID)::graphid */
static Node *
makeGraphidTuple(Alias *alias)
{
	ColumnRef  *oid;
	ColumnRef  *lid;

	oid = makeAliasColname(alias, "tableoid");
	lid = makeAliasColname(alias, AG_ELEM_LOCAL_ID);

	return makeTypedTuple(list_make2(oid, lid), GRAPHIDOID);
}

static Node *
makeVertexTuple(Alias *alias)
{
	Node	   *id;
	ColumnRef  *prop_map;

	id = makeGraphidTuple(alias);
	prop_map = makeAliasColname(alias, AG_ELEM_PROP_MAP);

	return makeTypedTuple(list_make2(id, prop_map), VERTEXOID);
}

static Node *
makeEdgeTuple(Alias *alias)
{
	Node	   *id;
	ColumnRef  *start;
	ColumnRef  *end;
	ColumnRef  *prop_map;

	id = makeGraphidTuple(alias);
	start = makeAliasColname(alias, AG_START_ID);
	end = makeAliasColname(alias, AG_END_ID);
	prop_map = makeAliasColname(alias, AG_ELEM_PROP_MAP);

	return makeTypedTuple(list_make4(id, start, end, prop_map), EDGEOID);
}

/* prop_map #> qualJson */
static Node *
makePropMapConstraint(ColumnRef *prop_map, Node *qualJson)
{
	Node	   *processed;
	A_Expr	   *constraint;

	processed = postprocessPropMapExpr(qualJson);
	constraint = makeA_Expr(AEXPR_OP, list_make1(makeString("@>")),
							prop_map, processed, -1);

	return (Node *) constraint;
}

static Node *
postprocessPropMapExpr(Node *prop_map_expr)
{
	Node *result = prop_map_expr;

	if (nodeTag(prop_map_expr) == T_A_Const)
	{
		A_Const *c = (A_Const *) prop_map_expr;

		if (nodeTag(&c->val) == T_String)
			result = (Node *) makeFuncCall(list_make1(makeString("jsonb_in")),
										   list_make1(prop_map_expr), -1);
	}

	return result;
}

static Node *
makeDirQual(Node *start, RangeVar *rel, Node *end)
{
	Node	   *vid_start;
	Node	   *start_id;
	Node	   *end_id;
	Node	   *vid_end;
	List	   *args;

	vid_start = makeVertexId(start);
	start_id = (Node *) makeAliasColname(rel->alias, AG_START_ID);
	end_id = (Node *) makeAliasColname(rel->alias, AG_END_ID);
	vid_end = makeVertexId(end);

	args = list_make2(makeSimpleA_Expr(AEXPR_OP, "=", start_id, vid_start, -1),
					  makeSimpleA_Expr(AEXPR_OP, "=", end_id, vid_end, -1));

	return (Node *) makeBoolExpr(AND_EXPR, args, -1);
}

static Node *
makeVertexId(Node *nodeVar)
{
	if (IsA(nodeVar, RangeVar))
	{
		RangeVar *r = (RangeVar *) nodeVar;

		return makeGraphidTuple(r->alias);
	}
	else
	{
		ColumnRef *colref = (ColumnRef *) nodeVar;
		AssertArg(IsA(colref, ColumnRef));

		return makeColumnProjection(colref, AG_ELEM_ID);
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
			Node	   *id1;
			Node	   *id2;
			List	   *args;

			id1 = makeVertexId(n1);
			id2 = makeVertexId(n2);

			args = list_make1(makeSimpleA_Expr(AEXPR_OP, "<>", id1, id2, -1));

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
			Node	   *id1;
			Node	   *id2;
			List	   *args;

			id1 = makeGraphidTuple(r1->alias);
			id2 = makeGraphidTuple(r2->alias);

			args = list_make1(makeSimpleA_Expr(AEXPR_OP, "<>", id1, id2, -1));

			qual = qualAndExpr(qual, (Node *) makeBoolExpr(OR_EXPR, args, -1));
		}
	}

	return qual;
}

/* NOTE: This function modifies `ctx` and `targetList`. */
static List *
transformCreatePattern(ParseState *pstate, List *pattern, PatternCtx *ctx,
					   List **targetList)
{
	PatternCtx	_ctx = {NULL, NIL, NIL, NIL};
	List	   *queryPattern = NIL;
	ListCell   *lp;

	if (ctx == NULL)
		ctx = &_ctx;

	foreach(lp, pattern)
	{
		CypherPath *p = (CypherPath *) lfirst(lp);
		char	   *pathname = getCypherName(p->variable);
		bool		out = (pathname != NULL);
		List	   *gchain = NIL;
		GraphPath  *gpath;
		ListCell   *le;

		if (out)
		{
			/* check variable name duplication */
			if (isStringValueIn(ctx->vertices, pathname) ||
				isStringValueIn(ctx->edges, pathname) ||
				isStringValueIn(ctx->values, pathname))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("variable \"%s\" already exists", pathname)));
		}

		foreach(le, p->chain)
		{
			Node *elem = (Node *) lfirst(le);

			if (nodeTag(elem) == T_CypherNode)
			{
				CypherNode *cnode = (CypherNode *) elem;
				GraphVertex *gvertex;

				gvertex = transformCreateVertex(pstate, cnode, ctx, targetList);

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

				Assert(nodeTag(elem) == T_CypherRel);

				gedge = transformCreateEdge(pstate, crel, ctx, targetList);

				gchain = lappend(gchain, gedge);
			}

			preventDropLabel(pstate, getLabelFromElem(elem));
		}

		/*
		 * Create a room for a graphpath to the target list.
		 * This dummy value will be replaced with an actual graphpath value
		 * generated at execution time in ExecCypherCreate().
		 */
		if (out)
		{
			ResTarget *target;

			target = makeSelectResTarget(makeGraphpath(NIL, NIL),
										 getCypherName(p->variable),
										 getCypherNameLoc(p->variable));

			*targetList = lappend(*targetList, target);
		}

		gpath = (GraphPath *) makeNode(GraphPath);
		if (pathname != NULL)
			gpath->variable = pstrdup(pathname);
		gpath->chain = gchain;

		queryPattern = lappend(queryPattern, gpath);
	}

	return queryPattern;
}

static GraphVertex *
transformCreateVertex(ParseState *pstate, CypherNode *cnode, PatternCtx *ctx,
					  List **targetList)
{
	char	   *varname;
	bool		create;
	Node	   *prop_map_expr = NULL;
	GraphVertex *gvertex;

	varname = getCypherName(cnode->variable);

	if (isStringValueIn(ctx->edges, varname) ||
		isStringValueIn(ctx->values, varname))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("variable \"%s\" already exists", varname),
				 parser_errposition(pstate,
									getCypherNameLoc(cnode->variable))));

	if (isStringValueIn(ctx->vertices, varname))
	{
		/*
		 * This node references a vertex in the previous clause.
		 * So, it can only have a variable.
		 */
		if (!isNodeForReference(cnode))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("variable \"%s\" already exists", varname),
					 parser_errposition(pstate,
										getCypherNameLoc(cnode->variable))));

		create = false;
	}
	else
	{
		if (varname != NULL)
		{
			ResTarget *target;

			/*
			 * Create a room for a newly created vertex.
			 * This dummy value will be replaced with the vertex
			 * in ExecCypherCreate().
			 */
			target = makeDummyVertex(varname);
			*targetList = lappend(*targetList, target);

			/* to check variable name duplication */
			ctx->vertices = lappend(ctx->vertices,
									makeString(pstrdup(varname)));
		}

		if (cnode->prop_map != NULL)
		{
			prop_map_expr = transformExpr(pstate,
									postprocessPropMapExpr(cnode->prop_map),
									EXPR_KIND_INSERT_TARGET);
			if (exprType(prop_map_expr) != JSONBOID)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("property map must be jsonb type"),
						 parser_errposition(pstate,
											exprLocation(prop_map_expr))));
		}
		create = true;
	}

	gvertex = (GraphVertex *) makeNode(GraphVertex);
	if (varname != NULL)
		gvertex->variable = pstrdup(varname);
	if (cnode->label != NULL)
		gvertex->label = pstrdup(getCypherName(cnode->label));
	gvertex->prop_map = prop_map_expr;
	gvertex->create = create;

	return gvertex;
}

static GraphEdge *
transformCreateEdge(ParseState *pstate, CypherRel *crel, PatternCtx *ctx,
					List **targetList)
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
				 errmsg("a single relationship type must be specified for CREATE")));

	varname = getCypherName(crel->variable);

	/* We cannot reference an edge from the previous clause in CREATE clause. */
	if (isStringValueIn(ctx->vertices, varname) ||
		isStringValueIn(ctx->edges, varname) ||
		isStringValueIn(ctx->values, varname))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("variable \"%s\" already exists", varname),
				 parser_errposition(pstate, getCypherNameLoc(crel->variable))));

	if (varname != NULL)
	{
		ResTarget *target;
		/*
		 * Create a room for a newly created edge.
		 * This dummy value will be replaced with the edge
		 * in ExecCypherCreate().
		 */
		target = makeDummyEdge(varname);
		*targetList = lappend(*targetList, target);

		ctx->edges = lappend(ctx->edges, makeString(pstrdup(varname)));
	}

	gedge = (GraphEdge *) makeNode(GraphEdge);
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
			elog(ERROR, "invalid direction");
	}
	if (varname != NULL)
		gedge->variable = pstrdup(varname);
	gedge->label = pstrdup(getCypherName(linitial(crel->types)));
	if (crel->prop_map != NULL)
		gedge->prop_map = transformExpr(pstate,
										postprocessPropMapExpr(crel->prop_map),
										EXPR_KIND_INSERT_TARGET);

	return gedge;
}

static bool
isNodeForReference(CypherNode *cnode)
{
	return (getCypherName(cnode->variable) != NULL &&
			getCypherName(cnode->label) == NULL && cnode->prop_map == NULL);
}

static ResTarget *
makeDummyVertex(char *varname)
{
	A_Const	   *id;
	A_Const	   *prop_map;
	Node	   *tuple;

	id = makeNode(A_Const);
	id->val.type = T_Null;
	id->location = -1;

	prop_map = makeNode(A_Const);
	prop_map->val.type = T_Null;
	prop_map->location = -1;

	tuple = makeTypedTuple(list_make2(id, prop_map), VERTEXOID);

	return makeSelectResTarget(tuple, varname, -1);
}

static ResTarget *
makeDummyEdge(char *varname)
{
	A_Const	   *id;
	A_Const	   *start;
	A_Const	   *end;
	A_Const	   *prop_map;
	Node	   *tuple;

	id = makeNode(A_Const);
	id->val.type = T_Null;
	id->location = -1;

	start = copyObject(id);
	end = copyObject(id);

	prop_map = makeNode(A_Const);
	prop_map->val.type = T_Null;
	prop_map->location = -1;

	tuple = makeTypedTuple(list_make4(id, start, end, prop_map), EDGEOID);

	return makeSelectResTarget(tuple, varname, -1);
}

static char *
getLabelFromElem(Node *elem)
{
	char *labname;

	Assert(elem != NULL);

	switch (nodeTag(elem))
	{
		case T_CypherNode:
			{
				CypherNode *cnode = (CypherNode *) elem;

				labname = getCypherName(cnode->label);
				if (labname == NULL)
					labname = AG_VERTEX;

				return labname;
			}
			break;
		case T_CypherRel:
			{
				CypherRel *crel = (CypherRel *) elem;

				Assert(list_length(crel->types) == 1);

				return getCypherName(linitial(crel->types));
			}
			break;
		default:
			elog(ERROR, "unrecognized element");
	}

	return NULL;
}

/* TODO: use ag_label, this routine assumes table backed label */
static void
preventDropLabel(ParseState *pstate, char *labname)
{
	RangeVar   *r;
	Relation	rel;

	r = makeRangeVar(AG_GRAPH, labname, -1);
	rel = parserOpenTable(pstate, r, AccessShareLock);

	/*
	 * Open and close the table but keep the access lock till end of
	 * transaction so that the table can't be deleted or have its schema
	 * modified underneath us.
	 */
	heap_close(rel, NoLock);
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
	r->alias->aliasname = CYPHER_SUBQUERY_ALIAS;

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

/* find T_String from the given T_String list */
static Value *
findStringValue(List *list, char *str)
{
	ListCell *lv;

	if (list == NIL || str == NULL)
		return NULL;

	foreach(lv, list)
	{
		Value *v = (Value *) lfirst(lv);

		if (strcmp(strVal(v), str) == 0)
			return v;
	}

	return NULL;
}

/* aliasname.indirection */
static ColumnRef *
makeAliasIndirection(Alias *alias, Node *indirection)
{
	return makeSimpleColumnRef(alias->aliasname, list_make1(indirection), -1);
}

/* attname(colref) - assume colref is record or composite type */
static Node *
makeColumnProjection(ColumnRef *colref, char *attname)
{
	return (Node *) makeFuncCall(list_make1(makeString(attname)),
								 list_make1(colref), -1);
}

static TypeName *
makeTypeNameByOid(Oid type_oid)
{
	switch (type_oid)
	{
		case GRAPHIDOID:
			return makeTypeName("graphid");
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
			AssertArg(type_oid == GRAPHIDOID ||
					  type_oid == VERTEXOID ||
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
