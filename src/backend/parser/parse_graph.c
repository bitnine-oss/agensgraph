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
#include "parser/parse_clause.h"
#include "parser/parse_collate.h"
#include "parser/parse_graph.h"
#include "parser/parse_target.h"
#include "parser/parse_utilcmd.h"

typedef struct PatternCtx
{
	Alias	   *alias;		/* alias of previous CypherClause */
	List	   *vertices;	/* vertex columns */
	List	   *edges;		/* edge columns */
	List	   *values;		/* other columns */
} PatternCtx;

/* trasnform */
static PatternCtx *makePatternCtx(RangeTblEntry *rte);
static List *makeComponents(List *patterns);
static void findAndUnionComponents(CypherPattern *pattern, List *components);
static bool isPatternConnectedTo(CypherPattern *pattern, List *component);
static bool arePatternsConnected(CypherPattern *p1, CypherPattern *p2);
static void transformComponents(List *components, PatternCtx *ctx,
								List **fromClause, List **targetList,
								Node **whereClause);
static Node *transformCypherNode(CypherNode *node, PatternCtx *ctx,
					List **fromClause, List **targetList, Node **whereClause,
					List **nodeVars);
static void transformCypherRel(CypherRel *rel, Node *left, Node *right,
					List **fromClause, List **targetList, Node **whereClause,
					List **relVars);
static Node *findNodeVar(List *nodeVars, char *varname);
static bool isNodeInPatternCtx(PatternCtx *ctx, char *varname);
static bool checkDupRelVar(List *relVars, char *varname);
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
static Alias *makePatternVarAlias(char *aliasname);
static Node *makeTuple(List *args, TypeName *typename);

/* utils */
static char *genUniqueName(void);

Query *
transformCypherMatchClause(ParseState *pstate, CypherClause *clause)
{
	CypherMatchClause *detail = (CypherMatchClause *) clause->detail;
	RangePrevclause *r;
	PatternCtx *ctx = NULL;
	List	   *components;
	List	   *fromClause = NIL;
	List	   *targetList = NIL;
	Node	   *whereClause = NULL;
	Node	   *qual;
	Query	   *qry;

	if (detail->where)
	{
		r = makeRangePrevclause((Node *) clause);
		fromClause = list_make1(r);
		targetList = list_make1(makeAliasStarTarget(r->alias));
		whereClause = detail->where;

		/*
		 * detach WHERE clause so that this funcion passes through
		 * this if statement when the function called again recursively
		 */
		detail->where = NULL;
	}
	else
	{
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

		components = makeComponents(detail->patterns);

		transformComponents(components, ctx,
							&fromClause, &targetList, &whereClause);
	}

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	transformFromClause(pstate, fromClause);

	qry->targetList = transformTargetList(pstate, targetList,
										  EXPR_KIND_SELECT_TARGET);
	markTargetListOrigins(pstate, qry->targetList);

	qual = transformWhereClause(pstate, whereClause, EXPR_KIND_WHERE, "WHERE");

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	assign_query_collations(pstate, qry);

	return qry;
}

Query *
transformCypherReturnClause(ParseState *pstate, CypherClause *clause)
{
	CypherReturnClause *detail = (CypherReturnClause *) clause->detail;
	List	   *fromClause = NIL;
	List	   *targetList;
	List	   *order = detail->order;
	Node	   *skip = detail->skip;
	Node	   *limit = detail->limit;
	Query	   *qry;

	if (order != NULL || skip != NULL || limit != NULL)
	{
		RangePrevclause *r;

		r = makeRangePrevclause((Node *) clause);
		fromClause = list_make1(r);
		targetList = list_make1(makeAliasStarTarget(r->alias));

		/*
		 * detach RETURN options so that this funcion passes through
		 * this if statement when the function called again recursively
		 */
		detail->order = NULL;
		detail->skip = NULL;
		detail->limit = NULL;
	}
	else
	{
		if (clause->prev != NULL)
			fromClause = list_make1(makeRangePrevclause(clause->prev));
		targetList = detail->items;
	}

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	transformFromClause(pstate, fromClause);

	qry->targetList = transformTargetList(pstate, targetList,
										  EXPR_KIND_SELECT_TARGET);
	markTargetListOrigins(pstate, qry->targetList);

	qry->sortClause = transformSortClause(pstate, order,
										  &qry->targetList, EXPR_KIND_ORDER_BY,
										  true, false);

	qry->limitOffset = transformLimitClause(pstate, skip,
											EXPR_KIND_OFFSET, "OFFSET");
	qry->limitCount = transformLimitClause(pstate, limit,
										   EXPR_KIND_LIMIT, "LIMIT");

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);

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
makeComponents(List *patterns)
{
	List	   *components = NIL;
	ListCell   *lp;

	foreach(lp, patterns)
	{
		CypherPattern *p = lfirst(lp);

		if (components == NIL)
		{
			/* a connected component is a list of CypherPatterns */
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
findAndUnionComponents(CypherPattern *pattern, List *components)
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

		if (isPatternConnectedTo(pattern, c))
		{
			repr = c;
			break;
		}

		lc = lnext(lc);
	}

	/* there is no matched connected component */
	if (repr == NIL)
	{
		lappend(components, list_make1(pattern));
		return;
	}

	/* find other connected components and merge them to repr */
	prev = lc;
	lc = lnext(lc);
	while (lc != NULL)
	{
		List *c = lfirst(lc);

		if (isPatternConnectedTo(pattern, c))
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

	/* add the pattern to repr */
	lappend(repr, pattern);
}

static bool
isPatternConnectedTo(CypherPattern *pattern, List *component)
{
	ListCell *lp;

	foreach(lp, component)
	{
		CypherPattern *p = lfirst(lp);

		if (arePatternsConnected(p, pattern))
			return true;
	}

	return false;
}

static bool
arePatternsConnected(CypherPattern *p1, CypherPattern *p2)
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
			CypherPattern *p = lfirst(lp);
			ListCell   *e;
			CypherNode *node;
			CypherRel  *rel;
			Node	   *left;
			Node	   *right;

			e = list_head(p->chain);
			node = lfirst(e);
			left = transformCypherNode(node, ctx,
									   fromClause, targetList, whereClause,
									   &nodeVars);

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

				transformCypherRel(rel, left, right,
								   fromClause, targetList, whereClause,
								   &relVars);

				left = right;
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
		ColumnRef  *oid;
		ColumnRef  *star;
		Node	   *tuple;
		ResTarget  *target;

		oid = makeAliasColname(r->alias, "tableoid");
		star = makeAliasStar(r->alias);

		tuple = makeTuple(list_make2(oid, star), makeTypeName("vertex"));

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
static void
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
		ColumnRef  *oid;
		ColumnRef  *star;
		Node	   *tuple;
		ResTarget  *target;

		oid = makeAliasColname(r->alias, "tableoid");
		star = makeAliasStar(r->alias);

		tuple = makeTuple(list_make2(oid, star), makeTypeName("edge"));

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
			AssertArg(IsA(colref, ColumnRef));

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

static Alias *
makePatternVarAlias(char *aliasname)
{
	aliasname = (aliasname == NULL ? genUniqueName() : pstrdup(aliasname));
	return makeAliasNoDup(aliasname, NIL);
}

static Node *
makeTuple(List *args, TypeName *typename)
{
	RowExpr	   *row;
	TypeCast   *cast;

	row = makeNode(RowExpr);
	row->args = args;
	row->row_format = COERCE_IMPLICIT_CAST;	/* abuse */
	row->location = -1;

	cast = makeNode(TypeCast);
	cast->arg = (Node *) row;
	cast->typeName = typename;
	cast->location = -1;

	return (Node *) cast;
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
