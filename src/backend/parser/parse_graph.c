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
#include "catalog/pg_type.h"
#include "nodes/graphnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "parser/parse_agg.h"
#include "parser/parse_clause.h"
#include "parser/parse_collate.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parse_graph.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#define CYPHER_SUBQUERY_ALIAS	"_"

#define EDGE_UNION_START_ID		"_start"
#define EDGE_UNION_END_ID		"_end"

/* projection (RETURN and WITH) */
static void checkNameInItems(ParseState *pstate, List *items);
static bool valueHasImplicitName(Node *val);

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
static Node *addQualUnique(ParseState *pstate, Node *qual, List *exprs);

/* CREATE */
static List *transformCreatePattern(ParseState *pstate, List *pattern,
									List **targetList);
static GraphVertex *transformCreateNode(ParseState *pstate, CypherNode *cnode,
										List **targetList);
static GraphEdge *transformCreateRel(ParseState *pstate, CypherRel *crel,
									 List **targetList);

/* common */
static bool isNodeEmpty(CypherNode *cnode);
static bool isNodeForRef(CypherNode *cnode);
static Node *transformPropMap(ParseState *pstate, Node *expr,
							  ParseExprKind exprKind);
static Node *preprocessPropMap(Node *expr);

/* transform */
static RangeTblEntry *transformClause(ParseState *pstate, Node *clause,
									  Alias *alias);
static void addRTEtoJoinlist(ParseState *pstate, RangeTblEntry *rte,
							 bool visible);
static RangeTblEntry *findRTEfromNamespace(ParseState *pstate, char *refname);
static List *makeTargetListFromRTE(ParseState *pstate, RangeTblEntry *rte);
static TargetEntry *makeWholeRowTarget(ParseState *pstate, RangeTblEntry *rte);
static TargetEntry *findTarget(List *targetList, char *resname);

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
static Alias *makeAliasNoDup(char *aliasname, List *colnames);
static Alias *makeAliasOptUnique(char *aliasname);
static Node *makeArrayExpr(Oid typarray, Oid typoid, List *elems);
static Node *makeTypedRowExpr(List *args, Oid typoid, int location);
static Node *qualAndExpr(ParseState *pstate, Node *qual, Node *expr);

/* parse node */
static ResTarget *makeSimpleResTarget(char *field, char *name);

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
		rte = transformClause(pstate, (Node *) clause, NULL);
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
		rte = transformClause(pstate, (Node *) clause, NULL);
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
		if (detail->kind == CP_WITH)
			checkNameInItems(pstate, detail->items);

		if (clause->prev != NULL)
			transformClause(pstate, clause->prev, NULL);

		qry->targetList = transformTargetList(pstate, detail->items,
											  EXPR_KIND_SELECT_TARGET);
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
	Node	   *qual;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	if (detail->where != NULL)
	{
		Node *where = detail->where;

		/*
		 * detach WHERE clause so that this funcion passes through
		 * this if statement when the function is called again recursively
		 */
		detail->where = NULL;
		rte = transformClause(pstate, (Node *) clause, NULL);
		detail->where = where;

		qry->targetList = makeTargetListFromRTE(pstate, rte);

		qual = transformWhereClause(pstate, where, EXPR_KIND_WHERE, "WHERE");
	}
	else
	{
		List *components = makeComponents(detail->pattern);

		if (clause->prev != NULL)
		{
			rte = transformClause(pstate, clause->prev, NULL);

			/*
			 * To do this at here is safe since it just uses transformed
			 * expression and does not look over the ancestors of `pstate`.
			 */
			qry->targetList = makeTargetListFromRTE(pstate, rte);
		}

		qual = transformComponents(pstate, components, &qry->targetList);
	}

	markTargetListOrigins(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;

	assign_query_collations(pstate, qry);

	return qry;
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

		rte = transformClause(pstate, (Node *) prevclause, NULL);

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
	rte = transformClause(pstate, clause->prev, NULL);

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
		rte = transformClause(pstate, clause->prev, NULL);

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
checkNameInItems(ParseState *pstate, List *items)
{
	ListCell *li;

	foreach(li, items)
	{
		ResTarget *res = lfirst(li);

		if (res->name != NULL)
			continue;

		if (!valueHasImplicitName(res->val))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("expression in WITH must be aliased (use AS)"),
					 parser_errposition(pstate, exprLocation(res->val))));
	}
}

/*
 * All cases except `ColumnRef` need an explicit name through AS.
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
		case T_A_Expr:
			{
				A_Expr *expr = (A_Expr *) val;
				if (expr->kind == AEXPR_PAREN)
					return valueHasImplicitName(expr->lexpr);
			}
			break;
		default:
			break;
	}

	return false;
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
		List	   *ues = NIL;

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
				Node	   *eid;

				cnode = lfirst(le);
				vertex = transformMatchNode(pstate, cnode, targetList, out);

				le = lnext(le);
				/* no more pattern chain (<rel,node> pair) */
				if (le == NULL)
					break;

				crel = lfirst(le);
				edge = transformMatchRel(pstate, crel, targetList);

				/* join */
				qual = addQualRelPath(pstate, qual,
									  prev_crel, prev_edge, crel, edge);
				qual = addQualNodeIn(pstate, qual, vertex, crel, edge, false);

				/* constraints */
				qual = addQualForRel(pstate, qual, crel, edge);
				qual = addQualForNode(pstate, qual, cnode, vertex);

				/* uniqueness */
				eid = getColumnVar(pstate, edge, AG_ELEM_LOCAL_ID);
				ues = list_append_unique(ues, eid);

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

		qual = addQualUnique(pstate, qual, ues);
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

		rte = findRTEfromNamespace(pstate, varname);
		if (rte == NULL)
			return (Node *) te;		/* from the previous clause */
		else
			return (Node *) rte;	/* in the pattern */
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
	char	   *typname;
	int			typloc;
	Alias	   *alias;
	RangeTblEntry *rte;

	/* all relationships must be unique */
	if (findTarget(*targetList, varname) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_ALIAS),
				 errmsg("duplicate variable \"%s\"", varname),
				 parser_errposition(pstate, varloc)));

	if (crel->types == NIL)
	{
		typname = AG_EDGE;
		typloc = -1;
	}
	else
	{
		Node *type;

		if (list_length(crel->types) > 1)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("multiple types for relationship not supported")));

		type = linitial(crel->types);

		typname = getCypherName(type);
		typloc = getCypherNameLoc(type);
	}

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

static RangeTblEntry *
addEdgeUnion(ParseState *pstate, char *edge_label, int location, Alias *alias)
{
	Node	   *u;
	Query	   *qry;
	RangeTblEntry *rte;

	AssertArg(alias != NULL);

	Assert(!pstate->p_lateral_active);
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
 * FROM `get_graph_path()`.`typname`
 * UNION
 * SELECT id, start, "end", properties,
 *        "end" as _start, start as _end
 * FROM `get_graph_path()`.`typname`
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

static ResTarget *
makeSimpleResTarget(char *field, char *name)
{
	ColumnRef  *col;
	ResTarget  *res;

	col = makeNode(ColumnRef);
	col->fields = list_make1(makeString(pstrdup(field)));
	col->location = -1;

	res = makeNode(ResTarget);
	if (name != NULL)
		res->name = pstrdup(name);
	res->val = (Node *) col;
	res->location = -1;

	return res;
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
	if (crel->prop_map)
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
addQualUnique(ParseState *pstate, Node *qual, List *exprs)
{
	ListCell *le1;

	foreach(le1, exprs)
	{
		Node	   *e1 = lfirst(le1);
		ListCell   *le2;

		for_each_cell(le2, lnext(le1))
		{
			Node	   *e2 = lfirst(le2);
			Expr	   *ne;

			ne = make_op(pstate, list_make1(makeString("<>")), e1, e2, -1);

			qual = qualAndExpr(pstate, qual, (Node *) ne);
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
transformClause(ParseState *pstate, Node *clause, Alias *alias)
{
	Query *qry;
	RangeTblEntry *rte;

	AssertArg(IsA(clause, CypherClause));

	if (alias == NULL)
		alias = makeAliasNoDup(CYPHER_SUBQUERY_ALIAS, NIL);

	Assert(!pstate->p_lateral_active);
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

	rte = addRangeTableEntryForSubquery(pstate, qry, alias, false, true);
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
		Oid			typoid;
		TupleDesc	tupdesc;
		int			idx;
		Form_pg_attribute attr = NULL;
		FieldSelect *fselect;

		Assert(IsA(elem, TargetEntry));

		typoid = exprType((Node *) te->expr);
		Assert(typoid == VERTEXOID || typoid == EDGEOID);

		tupdesc = lookup_rowtype_tupdesc_copy(typoid, -1);
		for (idx = 0; idx < tupdesc->natts; idx++)
		{
			char *attname;

			attr = tupdesc->attrs[idx];
			attname = NameStr(attr->attname);
			if (strncmp(attname, fname, NAMEDATALEN) == 0)
				break;
		}
		Assert(idx < tupdesc->natts);

		fselect = makeNode(FieldSelect);
		fselect->arg = te->expr;
		fselect->fieldnum = idx + 1;
		fselect->resulttype = attr->atttypid;
		fselect->resulttypmod = attr->atttypmod;
		fselect->resultcollid = attr->attcollation;

		return (Node *) fselect;
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
