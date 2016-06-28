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

typedef struct PatternCtx
{
	Alias	   *prevalias;
	List	   *vertices;
	List	   *edges;
	List	   *values;
} PatternCtx;

static RangePrevclause *makeRangePrevclause(Node *clause);
static PatternCtx *makePatternCtx(RangeTblEntry *rte);
static List *makeComponents(List *patterns);
static void findAndUnionComponents(CypherPattern *pattern, List *components);
static bool isPatternConnectedTo(CypherPattern *pattern, List *component);
static bool arePatternsConnected(CypherPattern *p1, CypherPattern *p2);
static void transformComponents(List *components, PatternCtx *ctx,
								List **fromClause, List **targetList,
								Node **whereClause);
static void applyPatternCtxTo(PatternCtx *ctx, List **targetList);
static Node *transformCypherNode(CypherNode *node, PatternCtx *ctx,
					List **fromClause, List **targetList, Node **whereClause,
					List **nodeVars);
static void transformCypherRel(CypherRel *rel, Node *left, Node *right,
					List **fromClause, List **targetList, Node **whereClause,
					List **relVars);
static Node *findNodeVar(List *nodeVars, char *varname);
static bool isNodeInPatternCtx(PatternCtx *ctx, char *varname);
static bool checkDupRelVar(List *relVars, char *varname);
static Alias *makeTableAlias(char *name);
static ColumnRef *makeAliasColRef(Alias *alias, char *colname);
static Node *makePropsConstraint(ColumnRef *props, char *qualStr);
static void whereClauseAndExpr(Node **whereClause, Node *expr);
static Node *makeDirExpr(Node *in, RangeVar *rel, Node *out);
static void setVertexId(Node *nodeVar, Node **oid, Node **vid);
static void addNodeDupPred(Node **whereClause, List *nodeVars);
static void addRelDupPred(Node **whereClause, List *relVars);

Query *
transformCypherMatchClause(ParseState *pstate, CypherClause *clause)
{
	CypherMatchClause *detail = (CypherMatchClause *) clause->detail;
	PatternCtx *ctx = NULL;
	List	   *components;
	List	   *fromClause = NIL;
	List	   *targetList = NIL;
	Node	   *whereClause = NULL;
	Node	   *qual;
	Query	   *qry;

	/*
	 * Transform previous CypherClause as a RangeSubselect first.
	 * It must be transformed at here because when transformComponents(),
	 * variables in the pattern which are in the resulting variables from the
	 * previous CypherClause must be excluded from fromClause RangeVars.
	 */
	if (clause->prev != NULL)
	{
		RangePrevclause *r;
		RangeTblEntry *rte;

		r = makeRangePrevclause(clause->prev);
		rte = transformRangePrevclause(pstate, r);
		ctx = makePatternCtx(rte);
	}

	components = makeComponents(detail->patterns);

	transformComponents(components, ctx,
						&fromClause, &targetList, &whereClause);

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
	Query	   *qry;

	if (clause->prev != NULL)
		fromClause = list_make1(makeRangePrevclause(clause->prev));

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	transformFromClause(pstate, fromClause);

	qry->targetList = transformTargetList(pstate, detail->items,
										  EXPR_KIND_SELECT_TARGET);
	markTargetListOrigins(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);

	assign_query_collations(pstate, qry);

	return qry;
}

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
				Assert(strcmp(te->resname, "?column?") != 0);
				values = lappend(values, colname);
		}
	}

	ctx = palloc(sizeof(*ctx));
	ctx->prevalias = makeAlias(rte->alias->aliasname, colnames);
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

	applyPatternCtxTo(ctx, targetList);

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
		addNodeDupPred(whereClause, nodeVars);
		/* all relationships are different from each other */
		addRelDupPred(whereClause, relVars);
	}
}

static void
applyPatternCtxTo(PatternCtx *ctx, List **targetList)
{
	char	   *prevname;
	ColumnRef  *colref;
	ResTarget  *target;

	if (ctx == NULL)
		return;

	prevname = pstrdup(ctx->prevalias->aliasname);

	/* add all columns in the result of previous CypherClause */
	colref = makeNode(ColumnRef);
	colref->fields = list_make2(makeString(prevname), makeNode(A_Star));
	colref->location = -1;

	target = makeNode(ResTarget);
	target->name = NULL;
	target->indirection = NIL;
	target->val = (Node *) colref;
	target->location = -1;

	*targetList = lappend(*targetList, target);
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
	ColumnRef  *props;

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

		colref = makeNode(ColumnRef);
		colref->fields = list_make1(makeString(varname));
		colref->location = -1;

		*nodeVars = lappend(*nodeVars, colref);

		return (Node *) colref;
	}

	/*
	 * process the newly introduced node
	 */

	label = getCypherName(node->label);
	if (label == NULL)
		label = "vertex";
	location = getCypherNameLoc(node->label);

	r = makeRangeVar("graph", label, location);
	r->inhOpt = INH_YES;
	r->alias = makeTableAlias(varname);

	*fromClause = lappend(*fromClause, r);

	/* add this node to the result */
	props = makeAliasColRef(r->alias, "properties");
	if (varname != NULL)
	{
		ColumnRef  *oid;
		ColumnRef  *vid;
		FuncCall   *vertex;
		ResTarget  *target;

		oid = makeAliasColRef(r->alias, "tableoid");
		vid = makeAliasColRef(r->alias, "vid");

		vertex = makeFuncCall(list_make1(makeString("vertex")),
							  list_make3(oid, vid, props), -1);

		target = makeNode(ResTarget);
		target->name = varname;
		target->indirection = NIL;
		target->val = (Node *) vertex;
		target->location = getCypherNameLoc(node->variable);

		*targetList = lappend(*targetList, target);
	}

	/* add property map constraint */
	if (node->prop_map != NULL)
	{
		Node *constraint;

		constraint = makePropsConstraint(props, node->prop_map);
		whereClauseAndExpr(whereClause, constraint);
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
	ColumnRef  *props;

	varname = getCypherName(rel->variable);

	/* all relationships are unique */
	if (checkDupRelVar(*relVars, varname))
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("all relationship must be unique: "
						"\"%s\" specified more than once", varname)));
	}

	if (rel->types == NULL)
	{
		typename = "edge";
		location = -1;
	}
	else
	{
		Node *type = linitial(rel->types);

		/* TODO: support multiple types */
		typename = getCypherName(type);
		location = getCypherNameLoc(type);
	}

	r = makeRangeVar("graph", typename, location);
	r->inhOpt = INH_YES;
	r->alias = makeTableAlias(varname);

	*fromClause = lappend(*fromClause, r);

	/* add this relationship to the result */
	props = makeAliasColRef(r->alias, "properties");
	if (varname != NULL)
	{
		ColumnRef  *oid;
		ColumnRef  *eid;
		ColumnRef  *vin_oid;
		ColumnRef  *vin_vid;
		ColumnRef  *vout_oid;
		ColumnRef  *vout_vid;
		List	   *args;
		FuncCall   *edge;
		ResTarget  *target;

		oid = makeAliasColRef(r->alias, "tableoid");
		eid = makeAliasColRef(r->alias, "eid");
		vin_oid = makeAliasColRef(r->alias, "inoid");
		vin_vid = makeAliasColRef(r->alias, "incoming");
		vout_oid = makeAliasColRef(r->alias, "outoid");
		vout_vid = makeAliasColRef(r->alias, "outgoing");

		args = lcons(oid,
			   lcons(eid,
			   lcons(vin_oid,
			   lcons(vin_vid,
			   lcons(vout_oid,
			   lcons(vout_vid,
			   lcons(props,
					 NIL)))))));

		edge = makeFuncCall(list_make1(makeString("edge")), args, -1);

		target = makeNode(ResTarget);
		target->name = varname;
		target->indirection = NIL;
		target->val = (Node *) edge;
		target->location = getCypherNameLoc(rel->variable);

		*targetList = lappend(*targetList, target);
	}

	/* add property map constraint */
	if (rel->prop_map != NULL)
	{
		Node *constraint;

		constraint = makePropsConstraint(props, rel->prop_map);
		whereClauseAndExpr(whereClause, constraint);
	}

	/* <node,rel,node> JOIN conditions */
	if (rel->direction == CYPHER_REL_DIR_NONE)
	{
		Node	   *lexpr;
		Node	   *rexpr;
		Node	   *nexpr;

		lexpr = makeDirExpr(right, r, left);
		rexpr = makeDirExpr(left, r, right);
		nexpr = (Node *) makeBoolExpr(OR_EXPR, list_make2(lexpr, rexpr), -1);
		whereClauseAndExpr(whereClause, nexpr);
	}
	else if (rel->direction == CYPHER_REL_DIR_LEFT)
	{
		whereClauseAndExpr(whereClause, makeDirExpr(right, r, left));
	}
	else
	{
		Assert(rel->direction == CYPHER_REL_DIR_RIGHT);
		whereClauseAndExpr(whereClause, makeDirExpr(left, r, right));
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
			ColumnRef * colref = (ColumnRef *) n;
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

static Alias *
makeTableAlias(char *aliasname)
{
	static uint32 seq = 0;

	Alias	   *alias;
	char		data[NAMEDATALEN];

	alias = makeNode(Alias);
	if (aliasname == NULL)
	{
		/* generate unique variable name */
		snprintf(data, sizeof(data), "<%010u>", seq++);
		aliasname = pstrdup(data);
	}
	alias->aliasname = aliasname;

	return alias;
}

static ColumnRef *
makeAliasColRef(Alias *alias, char *colname)
{
	ColumnRef *cr;

	cr = makeNode(ColumnRef);
	cr->fields = list_make2(makeString(alias->aliasname), makeString(colname));
	cr->location = -1;

	return cr;
}

static Node *
makePropsConstraint(ColumnRef *props, char *qualStr)
{
	FuncCall   *constraint;
	A_Const	   *qual;

	qual = makeNode(A_Const);
	qual->val.type = T_String;
	qual->val.val.str = qualStr;
	qual->location = -1;

	constraint = makeFuncCall(list_make1(makeString("jsonb_contains")),
							  list_make2(props, qual), -1);

	return (Node *) constraint;
}

static void
whereClauseAndExpr(Node **whereClause, Node *expr)
{
	if (*whereClause == NULL)
	{
		*whereClause = expr;
		return;
	}

	if (IsA(*whereClause, BoolExpr))
	{
		BoolExpr *bexpr = (BoolExpr *) *whereClause;

		if (bexpr->boolop == AND_EXPR)
		{
			bexpr->args = lappend(bexpr->args, expr);
			return;
		}
	}

	*whereClause = (Node *) makeBoolExpr(AND_EXPR,
										 list_make2(*whereClause, expr), -1);
	return;
}

static Node *
makeDirExpr(Node *in, RangeVar *rel, Node *out)
{
	Node	   *in_oid;
	Node	   *in_vid;
	Node	   *vin_oid;
	Node	   *vin_vid;
	Node	   *vout_oid;
	Node	   *vout_vid;
	Node	   *out_oid;
	Node	   *out_vid;
	List	   *args;

	setVertexId(in, &in_oid, &in_vid);
	vin_oid = (Node *) makeAliasColRef(rel->alias, "inoid");
	vin_vid = (Node *) makeAliasColRef(rel->alias, "incoming");
	vout_oid = (Node *) makeAliasColRef(rel->alias, "outoid");
	vout_vid = (Node *) makeAliasColRef(rel->alias, "outgoing");
	setVertexId(out, &out_oid, &out_vid);

	args = list_make4(makeSimpleA_Expr(AEXPR_OP, "=", vin_oid, in_oid, -1),
					  makeSimpleA_Expr(AEXPR_OP, "=", vin_vid, in_vid, -1),
					  makeSimpleA_Expr(AEXPR_OP, "=", vout_oid, out_oid, -1),
					  makeSimpleA_Expr(AEXPR_OP, "=", vout_vid, out_vid, -1));

	return (Node *) makeBoolExpr(AND_EXPR, args, -1);
}

static void
setVertexId(Node *nodeVar, Node **oid, Node **vid)
{
	if (IsA(nodeVar, RangeVar))
	{
		RangeVar *r = (RangeVar *) nodeVar;

		*oid = (Node *) makeAliasColRef(r->alias, "tableoid");
		*vid = (Node *) makeAliasColRef(r->alias, "vid");
	}
	else
	{
		ColumnRef *colref = (ColumnRef *) nodeVar;
		AssertArg(IsA(colref, ColumnRef));

		*oid = (Node *) makeFuncCall(list_make1(makeString("vertex_oid")),
									 list_make1(copyObject(colref)), -1);
		*vid = (Node *) makeFuncCall(list_make1(makeString("vertex_vid")),
									 list_make1(copyObject(colref)), -1);
	}
}

static void
addNodeDupPred(Node **whereClause, List *nodeVars)
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
			Node	   *vid1;
			Node	   *oid2;
			Node	   *vid2;
			List	   *args;

			setVertexId(n1, &oid1, &vid1);
			setVertexId(n2, &oid2, &vid2);

			args = list_make2(makeSimpleA_Expr(AEXPR_OP, "<>", oid1, oid2, -1),
							  makeSimpleA_Expr(AEXPR_OP, "<>", vid1, vid2, -1));

			whereClauseAndExpr(whereClause,
							   (Node *) makeBoolExpr(OR_EXPR, args, -1));
		}
	}
}

static void
addRelDupPred(Node **whereClause, List *relVars)
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
			Node	   *eid1;
			Node	   *oid2;
			Node	   *eid2;
			List	   *args;

			oid1 = (Node *) makeAliasColRef(r1->alias, "tableoid");
			eid1 = (Node *) makeAliasColRef(r1->alias, "eid");

			oid2 = (Node *) makeAliasColRef(r2->alias, "tableoid");
			eid2 = (Node *) makeAliasColRef(r2->alias, "eid");

			args = list_make2(makeSimpleA_Expr(AEXPR_OP, "<>", oid1, oid2, -1),
							  makeSimpleA_Expr(AEXPR_OP, "<>", eid1, eid2, -1));

			whereClauseAndExpr(whereClause,
							   (Node *) makeBoolExpr(OR_EXPR, args, -1));
		}
	}
}
