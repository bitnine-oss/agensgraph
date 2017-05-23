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
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/ag_graph_fn.h"
#include "catalog/ag_label.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
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
#include "parser/parse_shortestpath.h"
#include "parser/parse_target.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "rewrite/rewriteHandler.h"
#include "utils/builtins.h"
#include "utils/graph.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#define CYPHER_SUBQUERY_ALIAS	"_"
#define CYPHER_OPTMATCH_ALIAS	"_o"
#define CYPHER_MERGEMATCH_ALIAS	"_m"

#define VLE_COLNAME_START		"start"
#define VLE_COLNAME_END			"end"
#define VLE_COLNAME_LEVEL		"level"
#define VLE_COLNAME_ROWIDS		"rowids"
#define VLE_COLNAME_PATH		"path"
#define VLE_COLNAME_EREF		"eref"		/* edgeref */
#define VLE_COLNAME_ROWID		"rowid"

#define EDGE_UNION_START_ID		"_start"
#define EDGE_UNION_END_ID		"_end"

typedef struct
{
	char	   *varname;		/* variable assigned to the node */
	char	   *labname;		/* final label of the vertex */
	bool		prop_constr;	/* has property constraints? */
} NodeInfo;

typedef struct
{
	Node	   *tableoid;
	Node	   *ctid;
} RowId;

typedef struct
{
	Index		varno;			/* of the RTE */
	AttrNumber	varattno;		/* in the target list */
	Node	   *prop_constr;	/* property constraint of the element */
} ElemQual;

typedef struct prop_constr_context
{
	ParseState *pstate;
	Node	   *qual;
	Node	   *prop_map;
	List	   *pathelems;
} prop_constr_context;

typedef struct
{
	Index		varno;			/* of the RTE */
	AttrNumber	varattno;		/* in the target list */
	char	   *labname;		/* label of the vertex */
	bool		nullable;		/* is this nullable? */
	Expr	   *expr;			/* resolved vertex */
} FutureVertex;

#define FVR_DONT_RESOLVE		0x01
#define FVR_IGNORE_NULLABLE		0x02
#define FVR_PRESERVE_VAR_REF	0x04

typedef struct
{
	ParseState *pstate;
	int			flags;
	int			sublevels_up;
} resolve_future_vertex_context;

/* projection (RETURN and WITH) */
static void checkNameInItems(ParseState *pstate, List *items, List *targetList);

/* MATCH - OPTIONAL */
static RangeTblEntry *transformMatchOptional(ParseState *pstate,
											 CypherClause *clause);
/* MATCH - preprocessing */
static bool hasPropConstr(List *pattern);
static List *getFindPaths(List *pattern);
static void appendFindPathsResult(ParseState *pstate, List *fplist,
								  List **targetList);
static void collectNodeInfo(ParseState *pstate, List *pattern);
static void addNodeInfo(ParseState *pstate, CypherNode *cnode);
static NodeInfo *getNodeInfo(ParseState *pstate, char *varname);
static NodeInfo *findNodeInfo(ParseState *pstate, char *varname);
static List *makeComponents(List *pattern);
static bool isPathConnectedTo(CypherPath *path, List *component);
static bool arePathsConnected(CypherPath *path1, CypherPath *path2);
/* MATCH - transform */
static Node *transformComponents(ParseState *pstate, List *components,
								 List **targetList);
static Node *transformMatchNode(ParseState *pstate, CypherNode *cnode,
								bool force, List **targetList);
static RangeTblEntry *transformMatchRel(ParseState *pstate, CypherRel *crel,
										List **targetList);
static RangeTblEntry *transformMatchSR(ParseState *pstate, CypherRel *crel,
									   List **targetList);
static RangeTblEntry *addEdgeUnion(ParseState *pstate, char *edge_label,
								   int location, Alias *alias);
static Node *genEdgeUnion(char *edge_label, int location);
static void setInitialVidForVLE(ParseState *pstate, CypherRel *crel,
								Node *vertex, CypherRel *prev_crel,
								RangeTblEntry *prev_edge);
static RangeTblEntry *transformMatchVLE(ParseState *pstate, CypherRel *crel,
										List **targetList);
static SelectStmt *genVLESubselect(ParseState *pstate, CypherRel *crel,
								   bool out);
static Node *genVLELeftChild(ParseState *pstate, CypherRel *crel, bool out);
static Node *genVLERightChild(ParseState *pstate, CypherRel *crel, bool out);
static Node *genEdgeNode(ParseState *pstate, CypherRel *crel, char *aliasname);
static RangeSubselect *genEdgeUnionVLE(char *edge_label);
static RangeSubselect *genInhEdge(RangeVar *r, Oid parentoid);
static Node *genVLEJoinExpr(CypherRel *crel, Node *larg, Node *rarg);
static List *genQualifiedName(char *name1, char *name2);
static Node *genVLEQual(char *alias, Node *propMap);
static RangeTblEntry *transformVLEtoRTE(ParseState *pstate, SelectStmt *vle,
										Alias *alias);
static bool isZeroLengthVLE(CypherRel *crel);
static void getCypherRelType(CypherRel *crel, char **typname, int *typloc);
static Node *addQualRelPath(ParseState *pstate, Node *qual,
							CypherRel *prev_crel, RangeTblEntry *prev_edge,
							CypherRel *crel, RangeTblEntry *edge);
static Node *addQualNodeIn(ParseState *pstate, Node *qual, Node *vertex,
						   CypherRel *crel, RangeTblEntry *edge, bool prev);
static char *getEdgeColname(CypherRel *crel, bool prev);
static bool isFutureVertexExpr(Node *vertex);
static void setFutureVertexExprId(ParseState *pstate, Node *vertex,
								 CypherRel *crel, RangeTblEntry *edge,
								 bool prev);
static RowId *getEdgeRowId(ParseState *pstate, CypherRel *crel,
						   RangeTblEntry *edge);
static Node *addQualUniqueEdges(ParseState *pstate, Node *qual, List *ues,
								List *uearrs);
/* MATCH - quals */
static void addElemQual(ParseState *pstate, AttrNumber varattno,
						Node *prop_constr);
static void adjustElemQuals(List *elem_quals, RangeTblEntry *rte, int rtindex);
static Node *transformElemQuals(ParseState *pstate, Node *qual);
static Node *transform_prop_constr(ParseState *pstate, Node *qual,
								   Node *prop_map, Node *prop_constr);
static void transform_prop_constr_worker(Node *node, prop_constr_context *ctx);
static bool ginAvail(ParseState *pstate, Index varno, AttrNumber varattno);
static Oid getSourceRelid(ParseState *pstate, Index varno, AttrNumber varattno);
static bool hasGinOnProp(Oid relid);
/* MATCH - future vertex */
static void addFutureVertex(ParseState *pstate, AttrNumber varattno,
							char *labname);
static FutureVertex *findFutureVertex(ParseState *pstate, Index varno,
									  AttrNumber varattno, int sublevels_up);
static List *adjustFutureVertices(List *future_vertices, RangeTblEntry *rte,
								  int rtindex);
static Node *resolve_future_vertex(ParseState *pstate, Node *node, int flags);
static Node *resolve_future_vertex_mutator(Node *node,
										   resolve_future_vertex_context *ctx);
static void resolveFutureVertex(ParseState *pstate, FutureVertex *fv,
								bool ignore_nullable);
static RangeTblEntry *makeVertexRTE(ParseState *parentParseState, char *varname,
									char *labname);
static List *removeResolvedFutureVertices(List *future_vertices);

/* CREATE */
static List *transformCreatePattern(ParseState *pstate, List *pattern,
									List **targetList);
static GraphVertex *transformCreateNode(ParseState *pstate, CypherNode *cnode,
										List **targetList);
static GraphEdge *transformCreateRel(ParseState *pstate, CypherRel *crel,
									 List **targetList);
static Node *makeNewVertex(ParseState *pstate, Relation relation,
						   Node *prop_map);
static Node *makeNewEdge(ParseState *pstate, Relation relation, Node *prop_map);
static Relation openTargetLabel(ParseState *pstate, char *labname);

/* SET/REMOVE */
static List *transformSetPropList(ParseState *pstate, RangeTblEntry *rte,
								  CSetKind kind, List *items);
static GraphSetProp *transformSetProp(ParseState *pstate, RangeTblEntry *rte,
									  CypherSetProp *sp, CSetKind kind,
									  List *gsplist);
static GraphSetProp *findGraphSetProp(List *gsplist, char *varname);

/* MERGE */
static RangeTblEntry *transformMergeMatchSub(ParseState *pstate,
											 CypherClause *clause);
static Query *transformMergeMatch(ParseState *pstate, CypherClause *clause);
static RangeTblEntry *transformMergeMatchJoin(ParseState *pstate,
											  CypherClause *clause);
static RangeTblEntry *transformNullSelect(ParseState *pstate);
static Node *makeMatchForMerge(List *pattern);
static List *transformMergeCreate(ParseState *pstate, List *pattern,
								  RangeTblEntry *prevrte, List *resultList);
static GraphVertex *transformMergeNode(ParseState *pstate, CypherNode *cnode,
									   bool singlenode, List **targetList,
									   List *resultList);
static GraphEdge *transformMergeRel(ParseState *pstate, CypherRel *crel,
									List **targetList, List *resultList);
static List *transformMergeOnSet(ParseState *pstate, List *sets,
								 RangeTblEntry *rte);

/* common */
static bool labelExist(ParseState *pstate, char *labname, int labloc,
					   char labkind, bool throw);
#define vertexLabelExist(pstate, labname, labloc) \
	labelExist(pstate, labname, labloc, LABEL_KIND_VERTEX, true)
#define edgeLabelExist(pstate, labname, labloc) \
	labelExist(pstate, labname, labloc, LABEL_KIND_EDGE, true)
static void createLabelIfNotExist(ParseState *pstate, char *labname, int labloc,
								  char labkind);
#define createVertexLabelIfNotExist(pstate, labname, labloc) \
	createLabelIfNotExist(pstate, labname, labloc, LABEL_KIND_VERTEX)
#define createEdgeLabelIfNotExist(pstate, labname, labloc) \
	createLabelIfNotExist(pstate, labname, labloc, LABEL_KIND_EDGE)
static bool isNodeForRef(CypherNode *cnode);
static Node *transformPropMap(ParseState *pstate, Node *expr,
							  ParseExprKind exprKind);
static Node *preprocessPropMap(Node *expr);

/* transform */
static RangeTblEntry *transformClause(ParseState *pstate, Node *clause);
static RangeTblEntry *transformClauseImpl(ParseState *pstate, Node *clause,
										  Alias *alias);
static RangeTblEntry *incrementalJoinRTEs(ParseState *pstate, JoinType jointype,
										  RangeTblEntry *l_rte,
										  RangeTblEntry *r_rte,
										  Node *qual, Alias *alias);
static void makeJoinResCols(ParseState *pstate,
							RangeTblEntry *l_rte, RangeTblEntry *r_rte,
							List **res_colnames, List **res_colvars);
static void addRTEtoJoinlist(ParseState *pstate, RangeTblEntry *rte,
							 bool visible);
static void makeExtraFromRTE(ParseState *pstate, RangeTblEntry *rte,
							 RangeTblRef **rtr, ParseNamespaceItem **nsitem,
							 bool visible);
static RangeTblEntry *findRTEfromNamespace(ParseState *pstate, char *refname);
static ParseNamespaceItem *findNamespaceItemForRTE(ParseState *pstate,
												   RangeTblEntry *rte);
static List *makeTargetListFromRTE(ParseState *pstate, RangeTblEntry *rte);
static List *makeTargetListFromJoin(ParseState *pstate, RangeTblEntry *rte);
static TargetEntry *makeWholeRowTarget(ParseState *pstate, RangeTblEntry *rte);
static TargetEntry *findTarget(List *targetList, char *resname);

/* expression - type */
static Node *makeVertexExpr(ParseState *pstate, RangeTblEntry *rte,
							int location);
static Node *makeEdgeExpr(ParseState *pstate, RangeTblEntry *rte, int location);
static Node *makePathVertexExpr(ParseState *pstate, Node *obj);
static Node *makeGraphpath(List *vertices, List *edges, int location);
/* expression - common */
static Node *getColumnVar(ParseState *pstate, RangeTblEntry *rte,
						  char *colname);
static Node *getSysColumnVar(ParseState *pstate, RangeTblEntry *rte,
							 int attnum);
static Node *getExprField(Expr *expr, char *fname);
static Alias *makeAliasNoDup(char *aliasname, List *colnames);
static Alias *makeAliasOptUnique(char *aliasname);
static Node *makeArrayExpr(Oid typarray, Oid typoid, List *elems);
static Node *makeTypedRowExpr(List *args, Oid typoid, int location);
static Node *qualAndExpr(Node *qual, Node *expr);

/* parse node */
static ResTarget *makeSimpleResTarget(char *field, char *name);
static ResTarget *makeResTarget(Node *val, char *name);
static Node *makeColumnRef(List *fields);
static Node *makeDummyEdgeRef(Node *ctid);
static bool IsNullAConst(Node *arg);

/* utils */
static char *genUniqueName(void);

Query *
transformCypherSubPattern(ParseState *pstate, CypherSubPattern *subpat)
{
	CypherMatchClause *match;
	CypherClause *clause;
	Query *qry;
	RangeTblEntry *rte;

	if (subpat->kind == CSP_FINDPATH)
	{
		CypherPath *cp;

		Assert(list_length(subpat->pattern) == 1);
		cp = linitial(subpat->pattern);
		if (cp->kind == CPATH_DIJKSTRA)
			return transformDijkstra(pstate, cp);
		else
			return transformShortestPath(pstate, cp);
	}

	match = makeNode(CypherMatchClause);
	match->pattern = subpat->pattern;
	match->where = NULL;
	match->optional = false;

	clause = makeNode(CypherClause);
	clause->detail = (Node *) match;
	clause->prev = NULL;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	rte = transformClause(pstate, (Node *) clause);

	qry->targetList = makeTargetListFromRTE(pstate, rte);
	if (subpat->kind == CSP_SIZE)
	{
		FuncCall *count;
		TargetEntry *te;

		count = makeFuncCall(list_make1(makeString("count")), NIL, -1);
		count->agg_star = true;

		pstate->p_next_resno = 1;
		te = transformTargetEntry(pstate, (Node *) count, NULL,
								  EXPR_KIND_SELECT_TARGET, NULL, false);

		qry->targetList = list_make1(te);
	}
	markTargetListOrigins(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);

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
	int			flags;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	if (detail->where != NULL)
	{
		Node *where = detail->where;

		AssertArg(detail->kind == CP_WITH);

		detail->where = NULL;
		rte = transformClause(pstate, (Node *) clause);
		detail->where = where;

		qry->targetList = makeTargetListFromRTE(pstate, rte);
		wrapEdgeRefTargetList(pstate, qry->targetList);

		qual = transformWhereClause(pstate, where, EXPR_KIND_WHERE, "WHERE");
		qual = resolve_future_vertex(pstate, qual, 0);
	}
	else if (detail->distinct != NULL || detail->order != NULL ||
			 detail->skip != NULL || detail->limit != NULL)
	{
		List *distinct = detail->distinct;
		List *order = detail->order;
		Node *skip = detail->skip;
		Node *limit = detail->limit;

		/*
		 * detach options so that this function passes through this if statement
		 * when the function is called again recursively
		 */
		detail->distinct = NIL;
		detail->order = NIL;
		detail->skip = NULL;
		detail->limit = NULL;
		rte = transformClause(pstate, (Node *) clause);
		detail->distinct = distinct;
		detail->order = order;
		detail->skip = skip;
		detail->limit = limit;

		qry->targetList = makeTargetListFromRTE(pstate, rte);
		wrapEdgeRefTargetList(pstate, qry->targetList);

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
		qry->limitOffset = resolve_future_vertex(pstate, qry->limitOffset, 0);

		qry->limitCount = transformLimitClause(pstate, limit, EXPR_KIND_LIMIT,
											   "LIMIT");
		qry->limitCount = resolve_future_vertex(pstate, qry->limitCount, 0);
	}
	else
	{
		if (clause->prev != NULL)
			transformClause(pstate, clause->prev);

		qry->targetList = transformTargetList(pstate, detail->items,
											  EXPR_KIND_SELECT_TARGET);
		wrapEdgeRefTargetList(pstate, qry->targetList);

		if (detail->kind == CP_WITH)
			checkNameInItems(pstate, detail->items, qry->targetList);

		qry->groupClause = generateGroupClause(pstate, &qry->targetList,
											   qry->sortClause);
	}

	if (pstate->parentParseState != NULL)
		stripEdgeRefTargetList(qry->targetList);

	if (detail->kind == CP_WITH)
	{
		ListCell *lt;

		/* try to resolve all target entries except vertex Var */
		foreach(lt, qry->targetList)
		{
			TargetEntry *te = lfirst(lt);

			if (IsA(te->expr, Var) && exprType((Node *) te->expr) == VERTEXOID)
				continue;

			te->expr = (Expr *) resolve_future_vertex(pstate,
													  (Node *) te->expr, 0);
		}

		flags = FVR_DONT_RESOLVE;
	}
	else
	{
		flags = 0;
	}
	qry->targetList = (List *) resolve_future_vertex(pstate,
													 (Node *) qry->targetList,
													 flags);
	markTargetListOrigins(pstate, qry->targetList);

	qual = qualAndExpr(qual, pstate->p_resolved_qual);

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

	/*
	 * since WHERE clause is part of MATCH,
	 * transform OPTIONAL MATCH with its WHERE clause
	 */
	if (detail->optional && clause->prev != NULL)
	{
		/*
		 * NOTE: Should we return a single row with NULL values
		 *       if OPTIONAL MATCH is the first clause and
		 *       there is no result that matches the pattern?
		 */

		rte = transformMatchOptional(pstate, clause);

		qry->targetList = makeTargetListFromJoin(pstate, rte);
	}
	else
	{
		if (!pstate->p_is_match_quals &&
			(detail->where != NULL || hasPropConstr(detail->pattern)))
		{
			int flags = (pstate->p_is_optional_match ? FVR_IGNORE_NULLABLE: 0);

			pstate->p_is_match_quals = true;
			rte = transformClause(pstate, (Node *) clause);

			qry->targetList = makeTargetListFromRTE(pstate, rte);

			qual = transformWhereClause(pstate, detail->where, EXPR_KIND_WHERE,
										"WHERE");
			qual = transformElemQuals(pstate, qual);
			qual = resolve_future_vertex(pstate, qual, flags);
		}
		else
		{
			List *fplist = NIL;

			fplist = getFindPaths(detail->pattern);
			if (!pstate->p_is_fp_processed && fplist != NULL)
			{
				pstate->p_is_fp_processed = true;
				rte = transformClause(pstate, (Node *) clause);

				qry->targetList = makeTargetListFromRTE(pstate, rte);
				appendFindPathsResult(pstate, fplist, &qry->targetList);
			}
			else
			{
				List *components;

				pstate->p_is_match_quals = false;
				pstate->p_is_fp_processed = false;

				/*
				 * To do this at here is safe since it just uses transformed
				 * expression and does not look over the ancestors of `pstate`.
				 */
				if (clause->prev != NULL)
				{
					rte = transformClause(pstate, clause->prev);

					qry->targetList = makeTargetListFromRTE(pstate, rte);
				}

				collectNodeInfo(pstate, detail->pattern);
				components = makeComponents(detail->pattern);

				qual = transformComponents(pstate, components,
										   &qry->targetList);
				/* there is no need to resolve `qual` here */
			}
		}

		qry->targetList = (List *) resolve_future_vertex(pstate,
													(Node *) qry->targetList,
													FVR_DONT_RESOLVE);
	}
	markTargetListOrigins(pstate, qry->targetList);

	qual = qualAndExpr(qual, pstate->p_resolved_qual);

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

		rte = transformClause(pstate, (Node *) prevclause);

		qry->targetList = makeTargetListFromRTE(pstate, rte);
	}

	qry->graph.pattern = transformCreatePattern(pstate, pattern,
												&qry->targetList);
	qry->graph.targets = pstate->p_target_labels;

	qry->targetList = (List *) resolve_future_vertex(pstate,
													 (Node *) qry->targetList,
													 FVR_DONT_RESOLVE);
	markTargetListOrigins(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, pstate->p_resolved_qual);

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
	rte = transformClause(pstate, clause->prev);

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
					 errmsg("node, relationship, or path is expected"),
					 parser_errposition(pstate, exprLocation(expr))));

		/*
		 * TODO: `expr` must contain one of the target variables
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
	Assert(detail->kind == CSET_NORMAL);

	qry = makeNode(Query);
	qry->commandType = CMD_GRAPHWRITE;
	qry->graph.writeOp = GWROP_SET;
	qry->graph.last = (pstate->parentParseState == NULL);

	rte = transformClause(pstate, clause->prev);

	qry->targetList = makeTargetListFromRTE(pstate, rte);

	qry->graph.sets = transformSetPropList(pstate, rte, detail->kind,
										   detail->items);

	qry->targetList = (List *) resolve_future_vertex(pstate,
													 (Node *) qry->targetList,
													 FVR_DONT_RESOLVE);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, pstate->p_resolved_qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;

	assign_query_collations(pstate, qry);

	return qry;
}

Query *
transformCypherMergeClause(ParseState *pstate, CypherClause *clause)
{
	CypherMergeClause *detail = (CypherMergeClause *) clause->detail;
	Query	   *qry;
	RangeTblEntry *rte;

	if (list_length(detail->pattern) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("MERGE can have only one path")));

	qry = makeNode(Query);
	qry->commandType = CMD_GRAPHWRITE;
	qry->graph.writeOp = GWROP_MERGE;
	qry->graph.last = (pstate->parentParseState == NULL);

	rte = transformMergeMatchSub(pstate, clause);
	Assert(rte->rtekind == RTE_SUBQUERY);

	qry->targetList = makeTargetListFromRTE(pstate, rte);

	/*
	 * Make an expression list to create the MERGE path.
	 * We assume that the previous clause is the first RTE of MERGE MATCH.
	 */
	qry->graph.pattern = transformMergeCreate(pstate, detail->pattern,
									rt_fetch(1, rte->subquery->rtable),
									qry->targetList);
	qry->graph.targets = pstate->p_target_labels;

	qry->graph.sets = transformMergeOnSet(pstate, detail->sets, rte);

	qry->targetList = (List *) resolve_future_vertex(pstate,
													 (Node *) qry->targetList,
													 FVR_DONT_RESOLVE);
	markTargetListOrigins(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, pstate->p_resolved_qual);

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
		rte = transformClause(pstate, clause->prev);

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
checkNameInItems(ParseState *pstate, List *items, List *targetList)
{
	ListCell   *li;
	ListCell   *lt;

	forboth(li, items, lt, targetList)
	{
		ResTarget *res = lfirst(li);
		TargetEntry *te = lfirst(lt);

		if (res->name != NULL)
			continue;

		if (!IsA(te->expr, Var))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("expression in WITH must be aliased (use AS)"),
					 parser_errposition(pstate, exprLocation(res->val))));
	}
}

/* See transformFromClauseItem() */
static RangeTblEntry *
transformMatchOptional(ParseState *pstate, CypherClause *clause)
{
	CypherMatchClause *detail = (CypherMatchClause *) clause->detail;
	RangeTblEntry *l_rte;
	Alias	   *r_alias;
	RangeTblEntry *r_rte;
	Node	   *prevclause;
	Node	   *qual;
	Alias	   *alias;

	/* transform LEFT */
	l_rte = transformClause(pstate, clause->prev);

	/*
	 * Transform RIGHT. Prevent `clause` from being transformed infinitely.
	 * `p_cols_visible` of `l_rte` must be set to allow `r_rte` to see columns
	 * of `l_rte` by their name.
	 */

	prevclause = clause->prev;
	clause->prev = NULL;
	detail->optional = false;

	pstate->p_lateral_active = true;
	pstate->p_is_optional_match = true;

	r_alias = makeAliasNoDup(CYPHER_OPTMATCH_ALIAS, NIL);
	r_rte = transformClauseImpl(pstate, (Node *) clause, r_alias);

	pstate->p_is_optional_match = false;
	pstate->p_lateral_active = false;

	detail->optional = true;
	clause->prev = prevclause;

	qual = makeBoolConst(true, false);
	alias = makeAliasNoDup(CYPHER_SUBQUERY_ALIAS, NIL);

	return incrementalJoinRTEs(pstate, JOIN_LEFT, l_rte, r_rte, qual, alias);
}

static bool
hasPropConstr(List *pattern)
{
	ListCell *lp;

	foreach(lp, pattern)
	{
		CypherPath *p = lfirst(lp);
		ListCell *le;

		foreach(le, p->chain)
		{
			Node *elem = lfirst(le);

			if (IsA(elem, CypherNode))
			{
				CypherNode *cnode = (CypherNode *) elem;

				if (cnode->prop_map != NULL)
					return true;
			}
			else
			{
				CypherRel *crel = (CypherRel *) elem;

				Assert(IsA(elem, CypherRel));

				if (crel->prop_map != NULL)
					return true;
			}
		}
	}

	return false;
}

static List *
getFindPaths(List *pattern)
{
	List	   *fplist = NIL;
	ListCell   *lp;

	foreach(lp, pattern)
	{
		CypherPath *p = lfirst(lp);

		if (p->kind != CPATH_NORMAL)
			fplist = lappend(fplist, p);
	}

	return fplist;
}

static void
appendFindPathsResult(ParseState *pstate, List *fplist, List **targetList)
{
	ListCell *le;

	foreach(le, fplist)
	{
		CypherPath *p = lfirst(le);
		char	   *pathname;
		char	   *weightvar;
		Query	   *fp;
		Alias	   *alias;
		RangeTblEntry *rte;
		TargetEntry *te;

		pathname = getCypherName(p->variable);
		if (pathname == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("a variable name of path must be provided")));

		if (p->kind == CPATH_DIJKSTRA)
			fp = transformDijkstraInMatch(pstate, p);
		else
			fp = transformShortestPathInMatch(pstate, p);

		alias = makeAliasOptUnique(NULL);
		rte = addRangeTableEntryForSubquery(pstate, fp, alias, true, true);
		addRTEtoJoinlist(pstate, rte, true);

		te = makeTargetEntry((Expr *) getColumnVar(pstate, rte, pathname),
							 (AttrNumber) pstate->p_next_resno++,
							 pathname,
							 false);
		*targetList = lappend(*targetList, te);

		weightvar = getCypherName(p->weight_var);
		if (weightvar != NULL)
		{
			te = makeTargetEntry((Expr *) getColumnVar(pstate, rte, weightvar),
								 (AttrNumber) pstate->p_next_resno++,
								 weightvar,
								 false);
			*targetList = lappend(*targetList, te);
		}
	}
}

static void
collectNodeInfo(ParseState *pstate, List *pattern)
{
	ListCell *lp;

	foreach(lp, pattern)
	{
		CypherPath *p = lfirst(lp);
		ListCell *le;

		foreach(le, p->chain)
		{
			CypherNode *cnode = lfirst(le);

			if (IsA(cnode, CypherNode))
				addNodeInfo(pstate, cnode);
		}
	}
}

static void
addNodeInfo(ParseState *pstate, CypherNode *cnode)
{
	char	   *varname = getCypherName(cnode->variable);
	char	   *labname = getCypherName(cnode->label);
	NodeInfo   *ni;

	if (varname == NULL)
		return;

	ni = findNodeInfo(pstate, varname);
	if (ni == NULL)
	{
		ni = palloc(sizeof(*ni));
		ni->varname = varname;
		ni->labname = labname;
		ni->prop_constr = (cnode->prop_map != NULL);

		pstate->p_node_info_list = lappend(pstate->p_node_info_list, ni);
		return;
	}

	if (ni->labname == NULL)
	{
		ni->labname = labname;
	}
	else
	{
		if (labname != NULL && strcmp(ni->labname, labname) != 0)
		{
			int varloc = getCypherNameLoc(cnode->variable);

			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("label conflict on node \"%s\"", varname),
					 parser_errposition(pstate, varloc)));
		}
	}
	ni->prop_constr = (ni->prop_constr || (cnode->prop_map != NULL));
}

static NodeInfo *
getNodeInfo(ParseState *pstate, char *varname)
{
	NodeInfo *ni;

	if (varname == NULL)
		return NULL;

	ni = findNodeInfo(pstate, varname);

	return ni;
}

static NodeInfo *
findNodeInfo(ParseState *pstate, char *varname)
{
	ListCell *le;

	foreach(le, pstate->p_node_info_list)
	{
		NodeInfo *ni = lfirst(le);

		if (strcmp(ni->varname, varname) == 0)
			return ni;
	}

	return NULL;
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
		Assert(lc != NULL);
		prev = lc;
		for_each_cell(lc, lnext(lc))
		{
			c = lfirst(lc);

			if (isPathConnectedTo(p, c))
			{
				list_concat(repr, c);

				components = list_delete_cell(components, lc, prev);
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

	Assert(components != NIL);
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
		char	   *varname1;
		ListCell   *le2;

		/* node variables are the only concern */
		if (!IsA(cnode1, CypherNode))
			continue;

		varname1 = getCypherName(cnode1->variable);
		/* treat it as a unique node */
		if (varname1 == NULL)
			continue;

		foreach(le2, path2->chain)
		{
			CypherNode *cnode2 = lfirst(le2);
			char	   *varname2;

			if (!IsA(cnode2, CypherNode))
				continue;

			varname2 = getCypherName(cnode2->variable);
			if (varname2 == NULL)
				continue;

			if (strcmp(varname1, varname2) == 0)
				return true;
		}
	}

	return false;
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
		List	   *uearrs = NIL;

		foreach(lp, c)
		{
			CypherPath *p = lfirst(lp);
			char	   *pathname = getCypherName(p->variable);
			int			pathloc = getCypherNameLoc(p->variable);
			bool		out = (pathname != NULL);
			TargetEntry *te;
			ListCell   *le;
			CypherNode *cnode;
			Node	   *vertex;
			CypherRel  *prev_crel = NULL;
			RangeTblEntry *prev_edge = NULL;
			List	   *pvs = NIL;
			List	   *pes = NIL;

			te = findTarget(*targetList, pathname);
			if (te != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_ALIAS),
						 errmsg("duplicate variable \"%s\"", pathname),
						 parser_errposition(pstate, pathloc)));

			if (te == NULL && pathname != NULL)
			{
				if (colNameToVar(pstate, pathname, false, pathloc) != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_DUPLICATE_ALIAS),
							 errmsg("duplicate variable \"%s\"", pathname),
							 parser_errposition(pstate, pathloc)));
			}

			le = list_head(p->chain);
			for (;;)
			{
				CypherRel *crel;
				RangeTblEntry *edge;

				cnode = lfirst(le);

				/* `cnode` is the first node in the path */
				if (prev_crel == NULL)
				{
					bool		zero;

					le = lnext(le);

					/* vertex only path */
					if (le == NULL)
					{
						vertex = transformMatchNode(pstate, cnode, true,
													targetList);
						break;
					}

					crel = lfirst(le);

					/*
					 * if `crel` is zero-length VLE, get RTE of `cnode`
					 * because `crel` needs `id` column of the RTE
					 */
					zero = isZeroLengthVLE(crel);
					vertex = transformMatchNode(pstate, cnode,
												(zero || out), targetList);

					if (p->kind != CPATH_NORMAL)
					{
						le = lnext(le);
						continue;
					}

					setInitialVidForVLE(pstate, crel, vertex, NULL, NULL);
					edge = transformMatchRel(pstate, crel, targetList);

					qual = addQualNodeIn(pstate, qual, vertex, crel, edge,
										 false);
				}
				else
				{
					vertex = transformMatchNode(pstate, cnode, out, targetList);
					qual = addQualNodeIn(pstate, qual, vertex,
										 prev_crel, prev_edge, true);

					le = lnext(le);
					/* end of the path */
					if (le == NULL)
						break;

					crel = lfirst(le);
					setInitialVidForVLE(pstate, crel, vertex,
										prev_crel, prev_edge);
					edge = transformMatchRel(pstate, crel, targetList);
					qual = addQualRelPath(pstate, qual,
										  prev_crel, prev_edge, crel, edge);
				}

				/* uniqueness */
				if (crel->varlen == NULL)
				{
					RowId *rowid;

					rowid = getEdgeRowId(pstate, crel, edge);
					ues = list_append_unique(ues, rowid);
				}
				else
				{
					Node *earr;

					earr = getColumnVar(pstate, edge, VLE_COLNAME_ROWIDS);
					uearrs = list_append_unique(uearrs, earr);
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

			if (out && p->kind == CPATH_NORMAL)
			{
				Node *graphpath;
				TargetEntry *te;

				Assert(vertex != NULL);
				pvs = lappend(pvs, makePathVertexExpr(pstate, vertex));

				graphpath = makeGraphpath(pvs, pes, pathloc);
				te = makeTargetEntry((Expr *) graphpath,
									 (AttrNumber) pstate->p_next_resno++,
									 pathname,
									 false);

				*targetList = lappend(*targetList, te);
			}
		}

		qual = addQualUniqueEdges(pstate, qual, ues, uearrs);
	}

	return qual;
}

static Node *
transformMatchNode(ParseState *pstate, CypherNode *cnode, bool force,
				   List **targetList)
{
	char	   *varname = getCypherName(cnode->variable);
	int			varloc = getCypherNameLoc(cnode->variable);
	TargetEntry *te;
	NodeInfo   *ni = NULL;
	char	   *labname;
	int			labloc;
	bool		prop_constr;
	Const	   *id;
	Const	   *prop_map;
	Node	   *vertex;

	/*
	 * If a vertex with the same variable is already in the target list,
	 * - the vertex is from the previous clause or
	 * - a node with the same variable in the pattern are already processed,
	 * so skip `cnode`.
	 */
	te = findTarget(*targetList, varname);
	if (te != NULL)
	{
		RangeTblEntry *rte;

		if (exprType((Node *) te->expr) != VERTEXOID)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_ALIAS),
					 errmsg("duplicate variable \"%s\"", varname),
					 parser_errposition(pstate, varloc)));

		addElemQual(pstate, te->resno, cnode->prop_map);

		rte = findRTEfromNamespace(pstate, varname);
		if (rte == NULL)
		{
			/*
			 * `te` can be from the previous clause or the pattern.
			 * If it is from the pattern, it should be an actual vertex or
			 * a future vertex
			 */

			/*
			 * if the variable is from the previous clause, it should not
			 * have a label constraint
			 */
			if (getCypherName(cnode->label) != NULL && IsA(te->expr, Var))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("label on variable from previous clauses is not allowed"),
						 parser_errposition(pstate,
											getCypherNameLoc(cnode->label))));

			return (Node *) te;
		}
		else
		{
			/* previously returned RTE_RELATION by this function */
			return (Node *) rte;
		}
	}

	/*
	 * try to find the variable when this pattern is within an OPTIONAL MATCH
	 * or a sub-SELECT
	 */
	if (te == NULL && varname != NULL)
	{
		Var *col;

		col = (Var *) colNameToVar(pstate, varname, false, varloc);
		if (col != NULL)
		{
			FutureVertex *fv;

			if (cnode->label != NULL || exprType((Node *) col) != VERTEXOID)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_ALIAS),
						 errmsg("duplicate variable \"%s\"", varname),
						 parser_errposition(pstate, varloc)));

			te = makeTargetEntry((Expr *) col,
								 (AttrNumber) pstate->p_next_resno++,
								 varname,
								 false);

			addElemQual(pstate, te->resno, cnode->prop_map);
			*targetList = lappend(*targetList, te);

			/* `col` can be a future vertex */
			fv = findFutureVertex(pstate, col->varno, col->varattno,
								  col->varlevelsup);
			if (fv != NULL)
				addFutureVertex(pstate, te->resno, fv->labname);

			return (Node *) te;
		}
	}

	if (varname == NULL)
	{
		labname = getCypherName(cnode->label);
		labloc = getCypherNameLoc(cnode->label);
		prop_constr = (cnode->prop_map != NULL);
	}
	else
	{
		ni = getNodeInfo(pstate, varname);
		Assert(ni != NULL);

		labname = ni->labname;
		labloc = -1;
		prop_constr = ni->prop_constr;
	}

	if (labname == NULL)
		labname = AG_VERTEX;
	else
		vertexLabelExist(pstate, labname, labloc);

	/*
	 * If `cnode` has a label constraint or a property constraint, return RTE.
	 *
	 * if `cnode` is in a path, return RTE because the path must consit of
	 * valid vertices.
	 * If there is no previous relationship of `cnode` in the path and
	 * the next relationship of `cnode` is zero-length, return RTE
	 * because the relationship needs starting point.
	 */
	if (strcmp(labname, AG_VERTEX) != 0 || prop_constr || force)
	{
		RangeVar   *r;
		Alias	   *alias;
		RangeTblEntry *rte;

		r = makeRangeVar(get_graph_path(true), labname, labloc);
		alias = makeAliasOptUnique(varname);

		/* set `ihn` to true because we should scan all derived tables */
		rte = addRangeTableEntry(pstate, r, alias, true, true);
		addRTEtoJoinlist(pstate, rte, false);

		if (varname != NULL || prop_constr)
		{
			te = makeTargetEntry((Expr *) makeVertexExpr(pstate, rte, varloc),
								 (AttrNumber) pstate->p_next_resno++,
								 alias->aliasname,
								 false);

			addElemQual(pstate, te->resno, cnode->prop_map);
			*targetList = lappend(*targetList, te);
		}

		/* return RTE to help the caller can access columns directly */
		return (Node *) rte;
	}

	/* this node is just a placeholder for relationships */
	if (varname == NULL)
		return NULL;

	/*
	 * `cnode` is assigned to the variable `varname` but there is a chance to
	 * omit the RTE for `cnode` if no expression uses properties of `cnode`.
	 * So, return a (invalid) future vertex at here for later use.
	 */

	id = makeNullConst(GRAPHIDOID, -1, InvalidOid);
	prop_map = makeNullConst(JSONBOID, -1, InvalidOid);
	vertex = makeTypedRowExpr(list_make2(id, prop_map), VERTEXOID, varloc);
	te = makeTargetEntry((Expr *) vertex,
						 (AttrNumber) pstate->p_next_resno++,
						 varname,
						 false);

	/* there is no need to addElemQual() here */
	*targetList = lappend(*targetList, te);

	addFutureVertex(pstate, te->resno, labname);

	return (Node *) te;
}

static RangeTblEntry *
transformMatchRel(ParseState *pstate, CypherRel *crel, List **targetList)
{
	char	   *varname = getCypherName(crel->variable);
	int			varloc = getCypherNameLoc(crel->variable);
	char	   *typname;
	int			typloc;
	TargetEntry *te;

	/* all relationships must be unique */
	te = findTarget(*targetList, varname);
	if (te != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_ALIAS),
				 errmsg("duplicate variable \"%s\"", varname),
				 parser_errposition(pstate, varloc)));

	if (te == NULL && varname != NULL)
	{
		if (colNameToVar(pstate, varname, false, varloc) != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_ALIAS),
					 errmsg("duplicate variable \"%s\"", varname),
					 parser_errposition(pstate, varloc)));
	}

	getCypherRelType(crel, &typname, &typloc);
	if (strcmp(typname, AG_EDGE) != 0)
		edgeLabelExist(pstate, typname, typloc);

	if (crel->varlen == NULL)
		return transformMatchSR(pstate, crel, targetList);
	else
		return transformMatchVLE(pstate, crel, targetList);
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

		r = makeRangeVar(get_graph_path(true), typname, typloc);

		rte = addRangeTableEntry(pstate, r, alias, true, true);
	}
	addRTEtoJoinlist(pstate, rte, false);

	if (varname != NULL || crel->prop_map != NULL)
	{
		TargetEntry *te;

		te = makeTargetEntry((Expr *) makeEdgeExpr(pstate, rte, varloc),
							 (AttrNumber) pstate->p_next_resno++,
							 alias->aliasname,
							 false);

		addElemQual(pstate, te->resno, crel->prop_map);
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
 * SELECT tableoid, ctid, id, start, "end", properties,
 *        start AS _start, "end" AS _end
 * FROM `get_graph_path()`.`edge_label`
 * UNION
 * SELECT tableoid, ctid, id, start, "end", properties,
 *        "end" AS _start, start AS _end
 * FROM `get_graph_path()`.`edge_label`
 */
static Node *
genEdgeUnion(char *edge_label, int location)
{
	ResTarget  *tableoid;
	ResTarget  *ctid;
	ResTarget  *id;
	ResTarget  *start;
	ResTarget  *end;
	ResTarget  *prop_map;
	RangeVar   *r;
	SelectStmt *lsel;
	SelectStmt *rsel;
	SelectStmt *u;

	tableoid = makeSimpleResTarget("tableoid", NULL);
	ctid = makeSimpleResTarget("ctid", NULL);
	id = makeSimpleResTarget(AG_ELEM_LOCAL_ID, NULL);
	start = makeSimpleResTarget(AG_START_ID, NULL);
	end = makeSimpleResTarget(AG_END_ID, NULL);
	prop_map = makeSimpleResTarget(AG_ELEM_PROP_MAP, NULL);

	r = makeRangeVar(get_graph_path(true), edge_label, location);
	r->inhOpt = INH_YES;

	lsel = makeNode(SelectStmt);
	lsel->targetList = lappend(list_make5(tableoid, ctid, id, start, end),
							   prop_map);
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

static void
setInitialVidForVLE(ParseState *pstate, CypherRel *crel, Node *vertex,
					CypherRel *prev_crel, RangeTblEntry *prev_edge)
{
	ColumnRef  *cref;

	/* nothing to do */
	if (crel->varlen == NULL)
		return;

	if (vertex == NULL || isFutureVertexExpr(vertex))
	{
		if (prev_crel == NULL)
		{
			pstate->p_vle_initial_vid = NULL;
			pstate->p_vle_initial_rte = NULL;
		}
		else
		{
			char *colname;

			colname = getEdgeColname(prev_crel, true);

			cref = makeNode(ColumnRef);
			cref->fields = list_make2(makeString(prev_edge->eref->aliasname),
									  makeString(colname));
			cref->location = -1;

			pstate->p_vle_initial_vid = (Node *) cref;
			pstate->p_vle_initial_rte = prev_edge;
		}

		return;
	}

	if (IsA(vertex, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) vertex;

		Assert(rte->rtekind == RTE_RELATION);

		cref = makeNode(ColumnRef);
		cref->fields = list_make2(makeString(rte->eref->aliasname),
								  makeString(AG_ELEM_LOCAL_ID));
		cref->location = -1;

		pstate->p_vle_initial_vid = (Node *) cref;
		pstate->p_vle_initial_rte = rte;
	}
	else
	{
		TargetEntry *te = (TargetEntry *) vertex;
		Node *vid;

		AssertArg(IsA(vertex, TargetEntry));

		/* vertex or future vertex */

		cref = makeNode(ColumnRef);
		cref->fields = list_make1(makeString(te->resname));
		cref->location = -1;

		vid = (Node *) makeFuncCall(list_make1(makeString(AG_ELEM_ID)),
									list_make1(cref), -1);

		pstate->p_vle_initial_vid = vid;
		pstate->p_vle_initial_rte = NULL;
	}
}

static RangeTblEntry *
transformMatchVLE(ParseState *pstate, CypherRel *crel, List **targetList)
{
	char	   *varname = getCypherName(crel->variable);
	bool		out = (varname != NULL);
	SelectStmt *sel;
	Alias	   *alias;
	RangeTblEntry *rte;

	sel = genVLESubselect(pstate, crel, out);

	alias = makeAliasOptUnique(varname);
	rte = transformVLEtoRTE(pstate, sel, alias);

	if (out)
	{
		TargetEntry *te;
		Node *var;

		var = getColumnVar(pstate, rte, VLE_COLNAME_PATH);
		te = makeTargetEntry((Expr *) var,
							 (AttrNumber) pstate->p_next_resno++,
							 varname,
							 false);

		*targetList = lappend(*targetList, te);
	}

	return rte;
}

/*
 * SELECT l.start, l.end, l.rowids, l.path
 * FROM (
 *     SELECT l.start, l.end,
 *            ARRAY[rowid(l.tableoid, l.ctid)] AS rowids,
 *            ARRAY[l.eref] AS path
 *     FROM edge AS l
 *     WHERE l.start = outer_vid AND l.properties @> ...)
 *   VLE JOIN
 *     LATERAL (
 *     SELECT r.end,
 *            rowid(r.tableoid, r.ctid) AS rowid,
 *            r.eref
 *     FROM edge AS r
 *     WHERE  r.start = l.end AND r.properties @> ...)
 *   ON TRUE
 *
 * NOTE: If the order of the result targets is changed,
 *       `XXX_VARNO` macro definitions in nodeNestloopVle.c
 *       must be synchronized with the changed order.
 */
static SelectStmt *
genVLESubselect(ParseState *pstate, CypherRel *crel, bool out)
{
	Node	   *l_start;
	char	   *start_name;
	ResTarget  *start;
	Node 	   *l_end;
	char	   *end_name;
	ResTarget  *end;
	Node	   *l_rowids;
	ResTarget  *rowids;
	List 	   *tlist;
	Node       *left;
	Node       *right;
	Node	   *join;
	SelectStmt *sel;

	if (crel->direction == CYPHER_REL_DIR_LEFT)
	{
		l_start = makeColumnRef(genQualifiedName("l", AG_END_ID));
		l_end = makeColumnRef(genQualifiedName("l", AG_START_ID));
		start_name = VLE_COLNAME_END;
		end_name = VLE_COLNAME_START;
	}
	else if (crel->direction == CYPHER_REL_DIR_RIGHT)
	{
		l_start = makeColumnRef(genQualifiedName("l", AG_START_ID));
		l_end = makeColumnRef(genQualifiedName("l", AG_END_ID));
		start_name = VLE_COLNAME_START;
		end_name = VLE_COLNAME_END;
	}
	else
	{
		l_start = makeColumnRef(genQualifiedName("l", AG_START_ID));
		l_end = makeColumnRef(genQualifiedName("l", AG_END_ID));
		start_name = EDGE_UNION_START_ID;
		end_name = EDGE_UNION_END_ID;
	}

	start = makeResTarget(l_start, start_name);
	end = makeResTarget(l_end, end_name);

	l_rowids = makeColumnRef(genQualifiedName("l", VLE_COLNAME_ROWIDS));
	rowids = makeResTarget(l_rowids, VLE_COLNAME_ROWIDS);

	tlist = list_make3(start, end, rowids);

	if (out)
	{
		Node	   *l_path;
		ResTarget  *path;

		l_path = makeColumnRef(genQualifiedName("l", VLE_COLNAME_PATH));
		path = makeResTarget(l_path, VLE_COLNAME_PATH);

		tlist = lappend(tlist, path);
	}

	left = genVLELeftChild(pstate, crel, out);
	right = genVLERightChild(pstate, crel, out);

	join = genVLEJoinExpr(crel, left, right);

	sel = makeNode(SelectStmt);
	sel->targetList = tlist;
	sel->fromClause = list_make1(join);

	return sel;
}

static Node *
genVLELeftChild(ParseState *pstate, CypherRel *crel, bool out)
{
	char	   *start_name;
	char	   *end_name;
	Node	   *vid;
	A_ArrayExpr *rowidarr;
	A_ArrayExpr *erefarr;
	List	   *colnames = NIL;
	SelectStmt *sel;
	RangeSubselect *sub;

	if (crel->direction == CYPHER_REL_DIR_LEFT)
	{
		start_name = VLE_COLNAME_END;
		end_name = VLE_COLNAME_START;
	}
	else
	{
		start_name = VLE_COLNAME_START;
		end_name = VLE_COLNAME_END;
	}

	/*
	 * `vid` is NULL only if
	 * (there is no previous edge of the vertex in the path
	 *  and the vertex is transformed first time in the pattern)
	 * and `crel` is not zero-length
	 */
	vid = pstate->p_vle_initial_vid;

	if (isZeroLengthVLE(crel))
	{
		TypeCast   *rowids;
		List	   *values;

		Assert(vid != NULL);

		rowidarr = makeNode(A_ArrayExpr);
		rowidarr->location = -1;
		rowids = makeNode(TypeCast);
		rowids->arg = (Node *) rowidarr;
		rowids->typeName = makeTypeName("_rowid");
		rowids->location = -1;

		values = list_make3(vid, vid, rowids);
		colnames = list_make3(makeString(start_name),
							  makeString(end_name),
							  makeString(VLE_COLNAME_ROWIDS));

		if (out)
		{
			TypeCast *path;

			erefarr = makeNode(A_ArrayExpr);
			erefarr->location = -1;
			path = makeNode(TypeCast);
			path->arg = (Node *) erefarr;
			path->typeName = makeTypeName("_edgeref");
			path->location = -1;

			values = lappend(values, path);
			colnames = lappend(colnames, makeString(VLE_COLNAME_PATH));
		}

		sel = makeNode(SelectStmt);
		sel->valuesLists = list_make1(values);
	}
	else
	{
		ResTarget  *start;
		ResTarget  *end;
		Node	   *tableoid;
		Node	   *ctid;
		FuncCall   *rowid;
		TypeCast   *cast;
		ResTarget  *rowids;
		List	   *tlist = NIL;
		Node	   *from;
		List	   *where_args = NIL;

		start = makeSimpleResTarget(start_name, NULL);
		end = makeSimpleResTarget(end_name, NULL);

		tableoid = makeColumnRef(genQualifiedName(NULL, "tableoid"));
		ctid = makeColumnRef(genQualifiedName(NULL, "ctid"));
		rowid = makeFuncCall(list_make1(makeString("rowid")),
							 list_make2(tableoid, ctid), -1);
		rowidarr = makeNode(A_ArrayExpr);
		rowidarr->elements = list_make1(rowid);
		rowidarr->location = -1;
		cast = makeNode(TypeCast);
		cast->arg = (Node *) rowidarr;
		cast->typeName = makeTypeName("_rowid");
		cast->location = -1;
		rowids = makeResTarget((Node *) cast, VLE_COLNAME_ROWIDS);

		tlist = list_make3(start, end, rowids);

		from = genEdgeNode(pstate, crel, "l");

		if (out)
		{
			Node	   *edgeref;
			ResTarget  *path;

			if (IsA(from, RangeSubselect))
				edgeref = makeColumnRef(genQualifiedName(NULL,
														 VLE_COLNAME_EREF));
			else
				edgeref = makeDummyEdgeRef(copyObject(ctid));

			erefarr = makeNode(A_ArrayExpr);
			erefarr->elements = list_make1(edgeref);
			erefarr->location = -1;
			cast = makeNode(TypeCast);
			cast->arg = (Node *) erefarr;
			cast->typeName = makeTypeName("_edgeref");
			cast->location = -1;
			path = makeResTarget((Node *) cast, VLE_COLNAME_PATH);

			tlist = lappend(tlist, path);
		}

		if (vid != NULL)
		{
			ColumnRef  *begin;
			A_Expr	   *vidcond;

			if (crel->direction == CYPHER_REL_DIR_LEFT)
			{
				begin = makeNode(ColumnRef);
				begin->fields = genQualifiedName("l", AG_END_ID);
				begin->location = -1;
			}
			else
			{
				begin = makeNode(ColumnRef);
				begin->fields = genQualifiedName("l", AG_START_ID);
				begin->location = -1;
			}
			vidcond = makeSimpleA_Expr(AEXPR_OP, "=", (Node *) begin, vid, -1);
			where_args = lappend(where_args, vidcond);
		}

		/* TODO: cannot see properties of future vertices */
		if (crel->prop_map != NULL)
			where_args = lappend(where_args, genVLEQual("l", crel->prop_map));

		sel = makeNode(SelectStmt);
		sel->targetList = tlist;
		sel->fromClause = list_make1(from);
		sel->whereClause = (Node *) makeBoolExpr(AND_EXPR, where_args, -1);
	}

	sub = makeNode(RangeSubselect);
	sub->subquery = (Node *) sel;
	sub->alias = makeAliasNoDup("l", colnames);

	return (Node *) sub;
}

static Node *
genVLERightChild(ParseState *pstate, CypherRel *crel, bool out)
{
	ColumnRef  *prev;
	ColumnRef  *next;
	ResTarget  *end;
	Node	   *from;
	A_Expr	   *joinqual;
	Node	   *tableoid;
	Node	   *ctid;
	FuncCall   *rowid;
	List	   *tlist;
	List	   *where_args = NIL;
	SelectStmt     *sel;
	RangeSubselect *sub;

	if (crel->direction == CYPHER_REL_DIR_LEFT)
	{
		prev = makeNode(ColumnRef);
		prev->fields = genQualifiedName("l", VLE_COLNAME_START);
		prev->location = -1;

		next = makeNode(ColumnRef);
		next->fields = genQualifiedName("r", VLE_COLNAME_END);
		next->location = -1;

		end = makeSimpleResTarget(VLE_COLNAME_START, NULL);
	}
	else
	{
		prev = makeNode(ColumnRef);
		prev->fields = genQualifiedName("l", VLE_COLNAME_END);
		prev->location = -1;

		next = makeNode(ColumnRef);
		next->fields = genQualifiedName("r", VLE_COLNAME_START);
		next->location = -1;

		end = makeSimpleResTarget(VLE_COLNAME_END, NULL);
	}

	from = genEdgeNode(pstate, crel, "r");

	joinqual = makeSimpleA_Expr(AEXPR_OP, "=", (Node *) prev, (Node *) next,
								-1);
	where_args = lappend(where_args, joinqual);
	if (crel->prop_map != NULL)
		where_args = lappend(where_args, genVLEQual("r", crel->prop_map));

	tableoid = makeColumnRef(genQualifiedName(NULL, "tableoid"));
	ctid = makeColumnRef(genQualifiedName(NULL, "ctid"));
	rowid = makeFuncCall(list_make1(makeString("rowid")),
						 list_make2(tableoid, ctid), -1);

	tlist = list_make2(end, makeResTarget((Node *) rowid, VLE_COLNAME_ROWID));

	if (out)
	{
		Node	   *col;
		ResTarget  *eref;

		if (IsA(from, RangeSubselect))
			col = makeColumnRef(genQualifiedName(NULL, VLE_COLNAME_EREF));
		else
			col = makeDummyEdgeRef(copyObject(ctid));

		eref = makeResTarget(col, VLE_COLNAME_EREF);
		tlist = lappend(tlist, eref);
	}

	sel = makeNode(SelectStmt);
	sel->targetList = tlist;
	sel->fromClause = list_make1(from);
	sel->whereClause = (Node *) makeBoolExpr(AND_EXPR, where_args, -1);

	sub = makeNode(RangeSubselect);
	sub->subquery = (Node *) sel;
	sub->alias = makeAliasNoDup("r", NULL);
	sub->lateral = true;

	return (Node *) sub;
}

static Node *
genEdgeNode(ParseState *pstate, CypherRel *crel, char *aliasname)
{
	char	   *typname;
	Alias	   *alias;
	Node	   *edge;

	getCypherRelType(crel, &typname, NULL);
	alias = makeAliasNoDup(aliasname, NIL);

	if (crel->direction == CYPHER_REL_DIR_NONE)
	{
		RangeSubselect *sub;

		sub = genEdgeUnionVLE(typname);
		sub->alias = alias;
		edge = (Node *) sub;
	}
	else
	{
		RangeVar   *r;
		LOCKMODE	lockmode;
		Relation	rel;

		r = makeRangeVar(get_graph_path(true), typname, -1);
		r->inhOpt = INH_YES;
		r->alias = alias;

		lockmode = isLockedRefname(pstate, aliasname)
			? RowShareLock : AccessShareLock;
		rel = parserOpenTable(pstate, r, lockmode);

		if (has_subclass(rel->rd_id))
		{
			RangeSubselect *sub;

			r->inhOpt = INH_NO;
			sub = genInhEdge(r, rel->rd_id);
			sub->alias = alias;
			edge = (Node *) sub;
		}
		else
		{
			edge = (Node *) r;
		}

		heap_close(rel, NoLock);
	}

	return edge;
}

/*
 * SELECT start, "end", tableoid, ctid, 0 AS pathid, properties
 * FROM `get_graph_path()`.`edge_label`
 * UNION
 * SELECT "end" AS start, start AS "end", tableoid, ctid, 0 AS pathid, properties
 * FROM `get_graph_path()`.`edge_label`
 */
static RangeSubselect *
genEdgeUnionVLE(char *edge_label)
{
	ResTarget  *tableoid;
	ResTarget  *ctid;
	Node	   *col;
	ResTarget  *eref;
	ResTarget  *prop_map;
	RangeVar   *r;
	SelectStmt *lsel;
	SelectStmt *rsel;
	SelectStmt *u;
	RangeSubselect *sub;

	tableoid = makeSimpleResTarget("tableoid", NULL);
	ctid = makeSimpleResTarget("ctid", NULL);
	col = makeDummyEdgeRef(makeColumnRef(genQualifiedName(NULL, "ctid")));
	eref = makeResTarget(col, VLE_COLNAME_EREF);
	prop_map = makeSimpleResTarget(AG_ELEM_PROP_MAP, NULL);

	r = makeRangeVar(get_graph_path(true), edge_label, -1);
	r->inhOpt = INH_YES;

	lsel = makeNode(SelectStmt);
	lsel->targetList = list_make4(tableoid, ctid, eref, prop_map);
	lsel->fromClause = list_make1(r);

	rsel = copyObject(lsel);

	lsel->targetList = lcons(makeSimpleResTarget(AG_END_ID, NULL),
							 lsel->targetList);
	lsel->targetList = lcons(makeSimpleResTarget(AG_START_ID, NULL),
							 lsel->targetList);

	rsel->targetList = lcons(makeSimpleResTarget(AG_START_ID, AG_END_ID),
							 rsel->targetList);
	rsel->targetList = lcons(makeSimpleResTarget(AG_END_ID, AG_START_ID),
							 rsel->targetList);

	u = makeNode(SelectStmt);
	u->op = SETOP_UNION;
	u->all = true;
	u->larg = lsel;
	u->rarg = rsel;

	sub = makeNode(RangeSubselect);
	sub->subquery = (Node *) u;

	return sub;
}

static Node *
genVLEJoinExpr(CypherRel *crel, Node *larg, Node *rarg)
{
	A_Const	   *trueconst;
	TypeCast   *truecond;
	A_Indices  *indices;
	int			minHops;
	int			maxHops = -1;
	JoinExpr   *n;

	trueconst = makeNode(A_Const);
	trueconst->val.type = T_String;
	trueconst->val.val.str = "t";
	trueconst->location = -1;
	truecond = makeNode(TypeCast);
	truecond->arg = (Node *) trueconst;
	truecond->typeName = makeTypeNameFromNameList(
			genQualifiedName("pg_catalog", "bool"));
	truecond->location = -1;

	indices = (A_Indices *) crel->varlen;
	minHops = ((A_Const *) indices->lidx)->val.val.ival;
	if (indices->uidx != NULL)
		maxHops = ((A_Const *) indices->uidx)->val.val.ival;

	n = makeNode(JoinExpr);
	n->jointype = JOIN_VLE;
	n->isNatural = false;
	n->larg = larg;
	n->rarg = rarg;
	n->usingClause = NIL;
	n->quals = (Node *) truecond;
	n->minHops = minHops;
	n->maxHops = maxHops;

	return (Node *) n;
}

static List *
genQualifiedName(char *name1, char *name2)
{
	if (name1 == NULL)
		return list_make1(makeString(name2));
	else
		return list_make2(makeString(name1), makeString(name2));
}

static Node *
genVLEQual(char *alias, Node *propMap)
{
	ColumnRef  *prop;
	A_Expr	   *propcond;

	prop = makeNode(ColumnRef);
	prop->fields = genQualifiedName(alias, AG_ELEM_PROP_MAP);
	prop->location = -1;
	propcond = makeSimpleA_Expr(AEXPR_OP, "@>", (Node *) prop, propMap, -1);

	return (Node *) propcond;
}

static RangeSubselect *
genInhEdge(RangeVar *r, Oid parentoid)
{
	ResTarget  *start;
	ResTarget  *end;
	ResTarget  *tableoid;
	ResTarget  *ctid;
	Node	   *col;
	ResTarget  *eref;
	ResTarget  *prop_map;
	SelectStmt *sel;
	SelectStmt *lsel;
	List	   *children;
	ListCell   *el;
	RangeSubselect *sub;

	start = makeSimpleResTarget(AG_START_ID, NULL);
	end = makeSimpleResTarget(AG_END_ID, NULL);
	tableoid = makeSimpleResTarget("tableoid", NULL);
	ctid = makeSimpleResTarget("ctid", NULL);
	col = makeDummyEdgeRef(makeColumnRef(genQualifiedName(NULL, "ctid")));
	eref = makeResTarget(col, VLE_COLNAME_EREF);
	prop_map = makeSimpleResTarget(AG_ELEM_PROP_MAP, NULL);

	sel = makeNode(SelectStmt);
	sel->targetList = list_make4(start, end, tableoid, ctid);
	sel->targetList = lappend(sel->targetList, eref);
	sel->targetList = lappend(sel->targetList, prop_map);
	sel->fromClause = list_make1(r);
	lsel = sel;

	children = find_inheritance_children(parentoid, AccessShareLock);
	foreach(el, children)
	{
		Oid childoid = lfirst_oid(el);
		Relation childrel;
		RangeVar *childrv;
		SelectStmt *rsel;
		SelectStmt *u;

		childrel = heap_open(childoid, AccessShareLock);

		childrv = makeRangeVar(get_graph_path(true),
							   RelationGetRelationName(childrel),
							   -1);
		childrv->inhOpt = INH_YES;

		heap_close(childrel, AccessShareLock);

		rsel = copyObject(sel);
		rsel->fromClause = list_delete_first(rsel->fromClause);
		rsel->fromClause = list_make1(childrv);

		u = makeNode(SelectStmt);
		u->op = SETOP_UNION;
		u->all = true;
		u->larg = lsel;
		u->rarg = rsel;

		lsel = u;
	}

	sub = makeNode(RangeSubselect);
	sub->subquery = (Node *) lsel;

	return sub;
}

static RangeTblEntry *
transformVLEtoRTE(ParseState *pstate, SelectStmt *vle, Alias *alias)
{
	ParseNamespaceItem *nsitem = NULL;
	Query	   *qry;
	RangeTblEntry *rte;
	bool sv_convert_edgeref = pstate->p_convert_edgeref;

	Assert(!pstate->p_lateral_active);
	Assert(pstate->p_expr_kind == EXPR_KIND_NONE);

	/* make the RTE temporarily visible */
	if (pstate->p_vle_initial_rte != NULL)
	{
		nsitem = findNamespaceItemForRTE(pstate, pstate->p_vle_initial_rte);
		Assert(nsitem != NULL);

		nsitem->p_rel_visible = true;
	}

	pstate->p_lateral_active = true;
	pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;
	pstate->p_convert_edgeref = false;

	qry = parse_sub_analyze((Node *) vle, pstate, NULL,
							isLockedRefname(pstate, alias->aliasname));
	Assert(qry->commandType == CMD_SELECT);

	pstate->p_lateral_active = false;
	pstate->p_expr_kind = EXPR_KIND_NONE;
	pstate->p_convert_edgeref = sv_convert_edgeref;

	if (nsitem != NULL)
		nsitem->p_rel_visible = false;

	rte = addRangeTableEntryForSubquery(pstate, qry, alias, true, true);
	rte->isVLE = true;
	addRTEtoJoinlist(pstate, rte, false);

	return rte;
}

static bool
isZeroLengthVLE(CypherRel *crel)
{
	A_Indices  *indices;
	A_Const	   *lidx;

	if (crel == NULL)
		return false;

	if (crel->varlen == NULL)
		return false;

	indices = (A_Indices *) crel->varlen;
	lidx = (A_Const *) indices->lidx;
	return (lidx->val.val.ival == 0);
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

static Node *
addQualRelPath(ParseState *pstate, Node *qual, CypherRel *prev_crel,
			   RangeTblEntry *prev_edge, CypherRel *crel, RangeTblEntry *edge)
{
	Node	   *prev_vid;
	Node	   *vid;

	/*
	 * NOTE: If `crel` is VLE and a node between `prev_crel` and `crel` is
	 *       either a placeholder or a new future vertex,
	 *       the initial vid of `crel` is `prev_vid` already.
	 *       Currently, just add kind of duplicate qual anyway.
	 */

	prev_vid = getColumnVar(pstate, prev_edge, getEdgeColname(prev_crel, true));
	vid = getColumnVar(pstate, edge, getEdgeColname(crel, false));

	qual = qualAndExpr(qual,
					   (Node *) make_op(pstate, list_make1(makeString("=")),
										prev_vid, vid, -1));

	return qual;
}

static Node *
addQualNodeIn(ParseState *pstate, Node *qual, Node *vertex, CypherRel *crel,
			  RangeTblEntry *edge, bool prev)
{
	Node	   *id;
	Node	   *vid;

	/* `vertex` is just a placeholder for relationships */
	if (vertex == NULL)
		return qual;

	if (isFutureVertexExpr(vertex))
	{
		setFutureVertexExprId(pstate, vertex, crel, edge, prev);
		return qual;
	}

	/* already done in transformMatchVLE() */
	if (crel->varlen != NULL && !prev)
		return qual;

	if (IsA(vertex, RangeTblEntry))
	{
		RangeTblEntry *rte = (RangeTblEntry *) vertex;

		Assert(rte->rtekind == RTE_RELATION);

		id = getColumnVar(pstate, rte, AG_ELEM_LOCAL_ID);
	}
	else
	{
		TargetEntry *te = (TargetEntry *) vertex;

		AssertArg(IsA(vertex, TargetEntry));

		id = getExprField(te->expr, AG_ELEM_ID);
	}
	vid = getColumnVar(pstate, edge, getEdgeColname(crel, prev));

	qual = qualAndExpr(qual,
					   (Node *) make_op(pstate, list_make1(makeString("=")),
										id, vid, -1));

	return qual;
}

static char *
getEdgeColname(CypherRel *crel, bool prev)
{
	if (prev)
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

static bool
isFutureVertexExpr(Node *vertex)
{
	TargetEntry *te;
	RowExpr *row;

	AssertArg(vertex != NULL);

	if (!IsA(vertex, TargetEntry))
		return false;

	te = (TargetEntry *) vertex;
	if (!IsA(te->expr, RowExpr))
		return false;

	row = (RowExpr *) te->expr;

	/* a Const node representing a NULL */
	return IsA(lsecond(row->args), Const);
}

static void
setFutureVertexExprId(ParseState *pstate, Node *vertex, CypherRel *crel,
					  RangeTblEntry *edge, bool prev)
{
	TargetEntry *te = (TargetEntry *) vertex;
	RowExpr	   *row;
	Node	   *vid;

	row = (RowExpr *) te->expr;
	vid = getColumnVar(pstate, edge, getEdgeColname(crel, prev));
	row->args = list_make2(vid, lsecond(row->args));
}

static RowId *
getEdgeRowId(ParseState *pstate, CypherRel *crel, RangeTblEntry *edge)
{
	RowId *rowid;

	rowid = palloc(sizeof(*rowid));
	if (crel->direction == CYPHER_REL_DIR_NONE)
	{
		rowid->tableoid = getColumnVar(pstate, edge, "tableoid");
		rowid->ctid = getColumnVar(pstate, edge, "ctid");
	}
	else
	{
		rowid->tableoid = getSysColumnVar(pstate, edge,
										  TableOidAttributeNumber);
		rowid->ctid = getSysColumnVar(pstate, edge,
									  SelfItemPointerAttributeNumber);
	}

	return rowid;
}

static Node *
addQualUniqueEdges(ParseState *pstate, Node *qual, List *ues, List *uearrs)
{
	FuncCall   *arrpos;
	ListCell   *le1;
	ListCell   *lea1;
	FuncCall   *arroverlap;

	arrpos = makeFuncCall(list_make1(makeString("array_position")), NIL, -1);

	foreach(le1, ues)
	{
		RowId	   *e1 = lfirst(le1);
		ListCell   *le2;

		for_each_cell(le2, lnext(le1))
		{
			RowId	   *e2 = lfirst(le2);
			Expr	   *ne_tableoid;
			Expr	   *ne_ctid;
			Expr	   *ne;

			ne_tableoid = make_op(pstate, list_make1(makeString("<>")),
								  e1->tableoid, e2->tableoid, -1);
			ne_ctid = make_op(pstate, list_make1(makeString("<>")),
							  e1->ctid, e2->ctid, -1);
			ne = makeBoolExpr(OR_EXPR, list_make2(ne_tableoid, ne_ctid), -1);

			qual = qualAndExpr(qual, (Node *) ne);
		}

		foreach(lea1, uearrs)
		{
			Node	   *earr = lfirst(lea1);
			FuncCall   *rowid;
			Node	   *funcexpr;
			NullTest   *dupcond;

			rowid = makeFuncCall(list_make1(makeString("rowid")),
								 list_make2(e1->tableoid, e1->ctid), -1);

			funcexpr = ParseFuncOrColumn(pstate, rowid->funcname,
										 list_make2(e1->tableoid, e1->ctid),
										 rowid, -1);
			funcexpr = ParseFuncOrColumn(pstate, arrpos->funcname,
										 list_make2(earr, funcexpr),
										 arrpos, -1);

			dupcond = makeNode(NullTest);
			dupcond->arg = (Expr *) funcexpr;
			dupcond->nulltesttype = IS_NULL;
			dupcond->argisrow = false;
			dupcond->location = -1;

			qual = qualAndExpr(qual, (Node *) dupcond);
		}
	}

	arroverlap = makeFuncCall(list_make1(makeString("arrayoverlap")), NIL, -1);

	foreach(lea1, uearrs)
	{
		Node	   *earr1 = lfirst(lea1);
		ListCell   *lea2;

		for_each_cell(lea2, lnext(lea1))
		{
			Node	   *earr2 = lfirst(lea2);
			Node	   *funcexpr;
			Node	   *dupcond;

			funcexpr = ParseFuncOrColumn(pstate, arroverlap->funcname,
										 list_make2(earr1, earr2), arroverlap,
										 -1);

			dupcond = (Node *) makeBoolExpr(NOT_EXPR, list_make1(funcexpr), -1);

			qual = qualAndExpr(qual, dupcond);
		}
	}

	return qual;
}

static void
addElemQual(ParseState *pstate, AttrNumber varattno, Node *prop_constr)
{
	ElemQual *eq;

	if (prop_constr == NULL)
		return;

	eq = palloc(sizeof(*eq));
	eq->varno = InvalidAttrNumber;
	eq->varattno = varattno;
	eq->prop_constr = prop_constr;

	pstate->p_elem_quals = lappend(pstate->p_elem_quals, eq);
}

static void
adjustElemQuals(List *elem_quals, RangeTblEntry *rte, int rtindex)
{
	ListCell *le;

	AssertArg(rte->rtekind == RTE_SUBQUERY);

	foreach(le, elem_quals)
	{
		ElemQual *eq = lfirst(le);

		eq->varno = rtindex;
	}
}

static Node *
transformElemQuals(ParseState *pstate, Node *qual)
{
	ListCell *le;

	foreach(le, pstate->p_elem_quals)
	{
		ElemQual   *eq = lfirst(le);
		RangeTblEntry *rte;
		Var		   *var;
		Node	   *prop_map;
		bool		is_jsonobj;

		rte = GetRTEByRangeTablePosn(pstate, eq->varno, 0);
		var = make_var(pstate, rte, eq->varattno, -1);
		/* skip markVarForSelectPriv() because `rte` is RTE_SUBQUERY */

		prop_map = getExprField((Expr *) var, AG_ELEM_PROP_MAP);

		is_jsonobj = IsA(eq->prop_constr, JsonObject);

		if (is_jsonobj)
			qual = transform_prop_constr(pstate, qual, prop_map,
										 eq->prop_constr);

		if ((is_jsonobj && ginAvail(pstate, eq->varno, eq->varattno)) ||
			(!is_jsonobj))
		{
			Node	   *prop_constr;
			Expr	   *expr;

			prop_constr = transformPropMap(pstate, eq->prop_constr,
										   EXPR_KIND_WHERE);
			expr = make_op(pstate, list_make1(makeString("@>")),
						   prop_map, prop_constr, -1);

			qual = qualAndExpr(qual, (Node *) expr);
		}
	}

	pstate->p_elem_quals = NIL;
	return qual;
}

static Node *
transform_prop_constr(ParseState *pstate, Node *qual, Node *prop_map,
					  Node *prop_constr)
{
	prop_constr_context ctx;

	ctx.pstate = pstate;
	ctx.qual = qual;
	ctx.prop_map = prop_map;
	ctx.pathelems = NIL;

	transform_prop_constr_worker(prop_constr, &ctx);

	return ctx.qual;
}

static void
transform_prop_constr_worker(Node *node, prop_constr_context *ctx)
{
	JsonObject *jo = (JsonObject *) node;
	ListCell   *lp;

	foreach(lp, jo->keyvals)
	{
		JsonKeyVal *keyval = (JsonKeyVal *) lfirst(lp);
		Node	   *pathelem;
		Oid			pathelemtype;
		int			pathelemloc;
		ListCell   *prev;

		if (IsA(keyval->key, ColumnRef))
		{
			ColumnRef *cref = (ColumnRef *) keyval->key;

			if (list_length(cref->fields) < 2)
			{
				A_Const *c = makeNode(A_Const);

				c->val.type = T_String;
				c->val.val.str = strVal(linitial(cref->fields));
				c->location = cref->location;

				pathelem = transformExpr(ctx->pstate, (Node *) c,
										 EXPR_KIND_WHERE);
			}
			else
			{
				pathelem = transformExpr(ctx->pstate, keyval->key,
										 EXPR_KIND_WHERE);
			}
		}
		else
		{
			pathelem = transformExpr(ctx->pstate, keyval->key,
									 EXPR_KIND_WHERE);
		}
		pathelemtype = exprType(pathelem);
		pathelemloc = exprLocation(pathelem);
		pathelem = coerce_to_target_type(ctx->pstate, pathelem, pathelemtype,
										 TEXTOID, -1,COERCION_ASSIGNMENT,
										 COERCE_IMPLICIT_CAST, -1);
		if (pathelem == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("expression must be of type text but %s",
							format_type_be(pathelemtype)),
					 parser_errposition(ctx->pstate, pathelemloc)));

		prev = list_tail(ctx->pathelems);
		ctx->pathelems = lappend(ctx->pathelems, pathelem);

		if (IsA(keyval->val, JsonObject))
		{
			transform_prop_constr_worker(keyval->val, ctx);
		}
		else
		{
			Node	   *path;
			Expr	   *lval;
			Node	   *rval;
			Oid			rvaltype;
			int			rvalloc;
			Expr	   *expr;

			path = makeArrayExpr(TEXTARRAYOID, TEXTOID,
								 copyObject(ctx->pathelems));
			lval = make_op(ctx->pstate, list_make1(makeString("#>>")),
						   ctx->prop_map, path, -1);

			rval = transformExpr(ctx->pstate, keyval->val, EXPR_KIND_WHERE);
			rvaltype = exprType(rval);
			rvalloc = exprLocation(rval);
			rval = coerce_to_target_type(ctx->pstate, rval, rvaltype, TEXTOID,
										 -1,COERCION_ASSIGNMENT,
										 COERCE_IMPLICIT_CAST, -1);
			if (rval == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("expression must be of type text but %s",
								format_type_be(rvaltype)),
						 parser_errposition(ctx->pstate, rvalloc)));

			expr = make_op(ctx->pstate, list_make1(makeString("=")),
						   (Node *) lval, rval, -1);

			ctx->qual = qualAndExpr(ctx->qual, (Node *) expr);
		}

		ctx->pathelems = list_delete_cell(ctx->pathelems,
										  list_tail(ctx->pathelems),
										  prev);
	}
}

static bool
ginAvail(ParseState *pstate, Index varno, AttrNumber varattno)
{
	Oid			relid;
	List	   *inhoids;
	ListCell   *li;

	relid = getSourceRelid(pstate, varno, varattno);
	if (!OidIsValid(relid))
		return false;

	if (!has_subclass(relid))
		return hasGinOnProp(relid);

	inhoids = find_all_inheritors(relid, AccessShareLock, NULL);
	foreach(li, inhoids)
	{
		Oid inhoid = lfirst_oid(li);

		if (hasGinOnProp(inhoid))
			return true;
	}

	return false;
}

static Oid
getSourceRelid(ParseState *pstate, Index varno, AttrNumber varattno)
{
	FutureVertex *fv;
	List *rtable;

	/*
	 * If the given Var refers to a future vertex, there is no actual RTE for
	 * it. So, find the Var from the list of future vertices first.
	 */
	fv = findFutureVertex(pstate, varno, varattno, 0);
	if (fv != NULL)
	{
		RangeVar *rv = makeRangeVar(get_graph_path(true), fv->labname, -1);

		return RangeVarGetRelid(rv, AccessShareLock, false);
	}

	rtable = pstate->p_rtable;
	for (;;)
	{
		RangeTblEntry *rte;
		Var *var;

		rte = rt_fetch(varno, rtable);
		switch (rte->rtekind)
		{
			case RTE_RELATION:
				/* already locked */
				return rte->relid;
			case RTE_SUBQUERY:
				{
					TargetEntry *te;
					Oid type;

					te = get_tle_by_resno(rte->subquery->targetList, varattno);

					type = exprType((Node *) te->expr);
					if (type != VERTEXOID && type != EDGEOID)
						return InvalidOid;

					/* In RowExpr case, `(id, ...)` is assumed */
					if (IsA(te->expr, Var))
						var = (Var *) te->expr;
					else if (IsA(te->expr, RowExpr))
						var = linitial(((RowExpr *) te->expr)->args);
					else
						return InvalidOid;

					rtable = rte->subquery->rtable;
					varno = var->varno;
					varattno = var->varattno;
				}
				break;
			case RTE_JOIN:
				{
					Expr *expr;

					expr = list_nth(rte->joinaliasvars, varattno - 1);
					if (!IsA(expr, Var))
						return InvalidOid;

					var = (Var *) expr;
					// XXX: Do we need type check?

					varno = var->varno;
					varattno = var->varattno;
				}
				break;
			case RTE_FUNCTION:
			case RTE_VALUES:
			case RTE_CTE:
				return InvalidOid;
			default:
				elog(ERROR, "invalid RTEKind %d", rte->rtekind);
		}
	}
}

/* See get_relation_info() */
static bool
hasGinOnProp(Oid relid)
{
	Relation	rel;
	List	   *indexoidlist;
	ListCell   *li;
	bool		ret = false;

	rel = heap_open(relid, NoLock);

	if (!rel->rd_rel->relhasindex)
	{
		heap_close(rel, NoLock);

		return false;
	}

	indexoidlist = RelationGetIndexList(rel);

	foreach(li, indexoidlist)
	{
		Oid			indexoid = lfirst_oid(li);
		Relation	indexRel;
		Form_pg_index index;
		int			attnum;

		indexRel = index_open(indexoid, NoLock);
		index = indexRel->rd_index;

		if (!IndexIsValid(index))
		{
			index_close(indexRel, NoLock);
			continue;
		}

		if (index->indcheckxmin &&
			!TransactionIdPrecedes(
					HeapTupleHeaderGetXmin(indexRel->rd_indextuple->t_data),
					TransactionXmin))
		{
			index_close(indexRel, NoLock);
			continue;
		}

		if (indexRel->rd_rel->relam != GIN_AM_OID)
		{
			index_close(indexRel, NoLock);
			continue;
		}

		attnum = attnameAttNum(rel, AG_ELEM_PROP_MAP, false);
		if (attnum == InvalidAttrNumber)
		{
			index_close(indexRel, NoLock);
			continue;
		}

		if (index->indkey.values[0] == attnum)
		{
			index_close(indexRel, NoLock);
			ret = true;
			break;
		}

		index_close(indexRel, NoLock);
	}

	list_free(indexoidlist);

	heap_close(rel, NoLock);

	return ret;
}

static void
addFutureVertex(ParseState *pstate, AttrNumber varattno, char *labname)
{
	FutureVertex *fv;

	fv = palloc(sizeof(*fv));
	fv->varno = InvalidAttrNumber;
	fv->varattno = varattno;
	fv->labname = labname;
	fv->nullable = pstate->p_is_optional_match;
	fv->expr = NULL;

	pstate->p_future_vertices = lappend(pstate->p_future_vertices, fv);
}

static FutureVertex *
findFutureVertex(ParseState *pstate, Index varno, AttrNumber varattno,
				 int sublevels_up)
{
	ListCell *le;

	while (sublevels_up-- > 0)
	{
		pstate = pstate->parentParseState;
		Assert(pstate != NULL);
	}

	foreach(le, pstate->p_future_vertices)
	{
		FutureVertex *fv = lfirst(le);

		if (fv->varno == varno && fv->varattno == varattno)
			return fv;
	}

	return NULL;
}

static List *
adjustFutureVertices(List *future_vertices, RangeTblEntry *rte, int rtindex)
{
	ListCell   *prev;
	ListCell   *le;
	ListCell   *next;

	AssertArg(rte->rtekind == RTE_SUBQUERY);

	prev = NULL;
	for (le = list_head(future_vertices); le != NULL; le = next)
	{
		FutureVertex *fv = lfirst(le);
		bool		found;
		ListCell   *lt;

		next = lnext(le);

		/* set `varno` of new future vertex to its `rtindex` */
		if (fv->varno == InvalidAttrNumber)
		{
			fv->varno = rtindex;

			prev = le;
			continue;
		}

		found = false;
		foreach(lt, rte->subquery->targetList)
		{
			TargetEntry *te = lfirst(lt);
			Var *var;

			if (exprType((Node *) te->expr) != VERTEXOID)
				continue;

			/*
			 * skip all forms of vertex (e.g. `(id, properties)::vertex`)
			 * except variables of vertex
			 */
			if (!IsA(te->expr, Var))
				continue;

			var = (Var *) te->expr;
			if (var->varno == fv->varno && var->varattno == fv->varattno &&
				var->varlevelsup == 0)
			{
				fv->varno = rtindex;

				/*
				 * `te->resno` should always be equal to the item's
				 * ordinal position (counting from 1)
				 */
				fv->varattno = te->resno;

				found = true;
			}
		}

		if (!found)
			future_vertices = list_delete_cell(future_vertices, le, prev);
		else
			prev = le;
	}

	return future_vertices;
}

static Node *
resolve_future_vertex(ParseState *pstate, Node *node, int flags)
{
	resolve_future_vertex_context ctx;

	ctx.pstate = pstate;
	ctx.flags = flags;
	ctx.sublevels_up = 0;

	return resolve_future_vertex_mutator(node, &ctx);
}

static Node *
resolve_future_vertex_mutator(Node *node, resolve_future_vertex_context *ctx)
{
	Var *var;

	if (node == NULL)
		return NULL;

	if (IsA(node, Aggref))
	{
		Aggref	   *agg = (Aggref *) node;
		int			agglevelsup = (int) agg->agglevelsup;

		if (agglevelsup == ctx->sublevels_up)
		{
			ListCell *la;

			agg->aggdirectargs = (List *) resolve_future_vertex_mutator(
												(Node *) agg->aggdirectargs,
												ctx);

			foreach(la, agg->args)
			{
				TargetEntry *arg = lfirst(la);

				if (!IsA(arg->expr, Var))
					arg->expr = (Expr *) resolve_future_vertex_mutator(
															(Node *) arg->expr,
															ctx);
			}

			return node;
		}

		if (agglevelsup > ctx->sublevels_up)
			return node;

		/* fall-through */
	}

	if (IsA(node, OpExpr))
	{
		OpExpr *op = (OpExpr *) node;

		switch (op->opno)
		{
			case OID_VERTEX_EQ_OP:
			case OID_VERTEX_NE_OP:
			case OID_VERTEX_LT_OP:
			case OID_VERTEX_GT_OP:
			case OID_VERTEX_LE_OP:
			case OID_VERTEX_GE_OP:
				/* comparing only `id`s is enough */
				return node;
			default:
				break;
		}

		/* fall-through */
	}

	if (IsA(node, FieldSelect))
	{
		FieldSelect *fselect = (FieldSelect *) node;

		if (IsA(fselect->arg, Var))
		{
			var = (Var *) fselect->arg;

			if ((int) var->varlevelsup == ctx->sublevels_up &&
				exprType((Node *) var) == VERTEXOID &&
				fselect->fieldnum == Anum_vertex_id)
				return node;
		}

		/* fall-through */
	}

	if (IsA(node, Var))
	{
		FutureVertex *fv;
		Var *newvar;

		var = (Var *) node;

		if ((int) var->varlevelsup != ctx->sublevels_up)
			return node;

		if (exprType(node) != VERTEXOID)
			return node;

		fv = findFutureVertex(ctx->pstate, var->varno, var->varattno, 0);
		if (fv == NULL)
			return node;

		if (fv->expr == NULL)
		{
			if (ctx->flags & FVR_DONT_RESOLVE)
				return node;

			resolveFutureVertex(ctx->pstate, fv,
								(ctx->flags & FVR_IGNORE_NULLABLE));
		}

		newvar = copyObject(fv->expr);
		if (ctx->flags & FVR_PRESERVE_VAR_REF)
		{
			/* XXX: is this OK? */
			newvar->varno = fv->varno;
			newvar->varattno = fv->varattno;
		}
		newvar->varlevelsup = ctx->sublevels_up;

		return (Node *) newvar;
	}

	if (IsA(node, Query))
	{
		Query *newnode;

		ctx->sublevels_up++;
		newnode = query_tree_mutator((Query *) node,
									 resolve_future_vertex_mutator, ctx, 0);
		ctx->sublevels_up--;

		return (Node *) newnode;
	}

	return expression_tree_mutator(node, resolve_future_vertex_mutator, ctx);
}

static void
resolveFutureVertex(ParseState *pstate, FutureVertex *fv, bool ignore_nullable)
{
	RangeTblEntry *fv_rte;
	TargetEntry *fv_te;
	Var		   *fv_var;
	Node	   *fv_id;
	RangeTblEntry *rte;
	Node	   *vertex;
	FuncCall   *sel_id;
	Node	   *id;
	Node	   *qual;

	AssertArg(fv->expr == NULL);

	fv_rte = GetRTEByRangeTablePosn(pstate, fv->varno, 0);
	Assert(fv_rte->rtekind == RTE_SUBQUERY);

	fv_te = get_tle_by_resno(fv_rte->subquery->targetList, fv->varattno);
	Assert(fv_te != NULL);

	fv_var = make_var(pstate, fv_rte, fv->varattno, -1);
	fv_id = getExprField((Expr *) fv_var, AG_ELEM_ID);

	/*
	 * `p_cols_visible` of previous RTE must be set to allow `rte` to see
	 * columns of the previous RTE by their name
	 */
	rte = makeVertexRTE(pstate, fv_te->resname, fv->labname);

	vertex = getColumnVar(pstate, rte, rte->eref->aliasname);

	sel_id = makeFuncCall(list_make1(makeString(AG_ELEM_ID)), NIL, -1);
	id = ParseFuncOrColumn(pstate, sel_id->funcname, list_make1(vertex),
						   sel_id, -1);

	qual = (Node *) make_op(pstate, list_make1(makeString("=")), fv_id, id, -1);

	if (ignore_nullable)
	{
		addRTEtoJoinlist(pstate, rte, false);

		pstate->p_resolved_qual = qualAndExpr(pstate->p_resolved_qual, qual);
	}
	else
	{
		JoinType	jointype = (fv->nullable ? JOIN_LEFT : JOIN_INNER);
		Node	   *l_jt;
		int			l_rtindex;
		RangeTblEntry *l_rte;
		Alias	   *alias;

		l_jt = llast(pstate->p_joinlist);
		if (IsA(l_jt, RangeTblRef))
		{
			l_rtindex = ((RangeTblRef *) l_jt)->rtindex;
		}
		else
		{
			Assert(IsA(l_jt, JoinExpr));
			l_rtindex = ((JoinExpr *) l_jt)->rtindex;
		}
		l_rte = rt_fetch(l_rtindex, pstate->p_rtable);

		alias = makeAliasNoDup(CYPHER_SUBQUERY_ALIAS, NIL);
		incrementalJoinRTEs(pstate, jointype, l_rte, rte, qual, alias);
	}

	/* modify `fv->expr` to the actual vertex */
	fv->expr = (Expr *) vertex;
}

static RangeTblEntry *
makeVertexRTE(ParseState *parentParseState, char *varname, char *labname)
{
	Alias	   *alias;
	ParseState *pstate;
	Query	   *qry;
	RangeVar   *r;
	RangeTblEntry *rte;
	TargetEntry *te;

	Assert(parentParseState->p_expr_kind == EXPR_KIND_NONE);
	parentParseState->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

	alias = makeAlias(varname, NIL);

	pstate = make_parsestate(parentParseState);
	pstate->p_locked_from_parent = isLockedRefname(pstate, alias->aliasname);

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	r = makeRangeVar(get_graph_path(true), labname, -1);

	rte = addRangeTableEntry(pstate, r, alias, true, true);
	addRTEtoJoinlist(pstate, rte, false);

	te = makeTargetEntry((Expr *) makeVertexExpr(pstate, rte, -1),
						 (AttrNumber) pstate->p_next_resno++,
						 alias->aliasname,
						 false);

	qry->targetList = list_make1(te);
	markTargetListOrigins(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);

	assign_query_collations(pstate, qry);

	parentParseState->p_expr_kind = EXPR_KIND_NONE;

	return addRangeTableEntryForSubquery(parentParseState, qry, alias, false,
										 true);
}

static List *
removeResolvedFutureVertices(List *future_vertices)
{
	ListCell   *prev;
	ListCell   *le;
	ListCell   *next;

	prev = NULL;
	for (le = list_head(future_vertices); le != NULL; le = next)
	{
		FutureVertex *fv = lfirst(le);

		next = lnext(le);
		if (fv->expr == NULL)
			prev = le;
		else
			future_vertices = list_delete_cell(future_vertices, le, prev);
	}

	return future_vertices;
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
								 pathname,
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
	bool		create;
	Oid			relid = InvalidOid;
	TargetEntry	*te;
	GraphVertex	*gvertex;

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
		char	   *labname = getCypherName(cnode->label);
		Relation 	relation;
		Node	   *vertex;

		if (labname == NULL)
		{
			labname = AG_VERTEX;
		}
		else
		{
			createVertexLabelIfNotExist(pstate, labname,
										getCypherNameLoc(cnode->label));
		}

		/* lock the relation of the label and return it */
		relation = openTargetLabel(pstate, labname);

		/* make vertex expression for result plan */
		vertex = makeNewVertex(pstate, relation, cnode->prop_map);
		relid = RelationGetRelid(relation);

		/* keep the lock */
		heap_close(relation, NoLock);

		te = makeTargetEntry((Expr *) vertex,
							 (AttrNumber) pstate->p_next_resno++,
							 (varname == NULL ? "?column?" : varname),
							 false);

		*targetList = lappend(*targetList, te);

		pstate->p_target_labels =
				list_append_unique_oid(pstate->p_target_labels, relid);
	}

	gvertex = makeNode(GraphVertex);
	gvertex->resno = te->resno;
	gvertex->create = create;
	gvertex->relid = relid;

	return gvertex;
}

static GraphEdge *
transformCreateRel(ParseState *pstate, CypherRel *crel, List **targetList)
{
	char	   *varname;
	Node	   *type;
	char	   *typname;
	Relation 	relation;
	Node	   *edge;
	Oid			relid = InvalidOid;
	TargetEntry *te;
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
	 * All relationships must be unique and we cannot reference an edge
	 * from the previous clause in CREATE clause.
	 */
	if (findTarget(*targetList, varname) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_ALIAS),
				 errmsg("duplicate variable \"%s\"", varname),
				 parser_errposition(pstate, getCypherNameLoc(crel->variable))));

	type = linitial(crel->types);
	typname = getCypherName(type);

	createEdgeLabelIfNotExist(pstate, typname, getCypherNameLoc(type));

	relation = openTargetLabel(pstate, typname);

	edge = makeNewEdge(pstate, relation, crel->prop_map);
	relid = RelationGetRelid(relation);

	heap_close(relation, NoLock);

	te = makeTargetEntry((Expr *) edge,
						 (AttrNumber) pstate->p_next_resno++,
						 (varname == NULL ? "?column?" : varname),
						 false);

	*targetList = lappend(*targetList, te);

	pstate->p_target_labels =
			list_append_unique_oid(pstate->p_target_labels, relid);

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
	gedge->resno = te->resno;
	gedge->relid = relid;

	return gedge;
}

static Node *
makeNewVertex(ParseState *pstate, Relation relation, Node *prop_map)
{
	int			id_attnum;
	Node	   *id;
	int			prop_map_attnum;
	Node	   *prop_map_default;
	Node	   *expr;

	id_attnum = attnameAttNum(relation, AG_ELEM_LOCAL_ID, false);
	Assert(id_attnum == 1);
	id = build_column_default(relation, id_attnum);

	prop_map_attnum = attnameAttNum(relation, AG_ELEM_PROP_MAP, false);
	Assert(prop_map_attnum == 2);
	prop_map_default = build_column_default(relation, prop_map_attnum);

	if (prop_map == NULL)
	{
		expr = prop_map_default;
	}
	else
	{
		CoalesceExpr *coalesce;

		expr = transformPropMap(pstate, prop_map, EXPR_KIND_INSERT_TARGET);

		/*
		 * If the evaluated value of the user-supplied expression is NULL,
		 * use the default properties.
		 */
		coalesce = makeNode(CoalesceExpr);
		coalesce->args = list_make2(expr, prop_map_default);
		coalesce->coalescetype = JSONBOID;
		coalesce->location = -1;

		expr = (Node *) coalesce;
	}

	return makeTypedRowExpr(list_make2(id, expr), VERTEXOID, -1);
}

static Node *
makeNewEdge(ParseState *pstate, Relation relation, Node *prop_map)
{
	int			id_attnum;
	Node	   *id;
	Node	   *start;
	Node	   *end;
	int			prop_map_attnum;
	Node	   *prop_map_default;
	Node	   *expr;

	id_attnum = attnameAttNum(relation, AG_ELEM_LOCAL_ID, false);
	Assert(id_attnum == 1);
	id = build_column_default(relation, id_attnum);

	start = (Node *) makeNullConst(GRAPHIDOID, -1, InvalidOid);
	end = (Node *) makeNullConst(GRAPHIDOID, -1, InvalidOid);

	prop_map_attnum = attnameAttNum(relation, AG_ELEM_PROP_MAP, false);
	Assert(prop_map_attnum == 4);
	prop_map_default = build_column_default(relation, prop_map_attnum);

	if (prop_map == NULL)
	{
		expr = prop_map_default;
	}
	else
	{
		CoalesceExpr *coalesce;

		expr = transformPropMap(pstate, prop_map, EXPR_KIND_INSERT_TARGET);

		coalesce = makeNode(CoalesceExpr);
		coalesce->args = list_make2(expr, prop_map_default);
		coalesce->coalescetype = JSONBOID;
		coalesce->location = -1;

		expr = (Node *) coalesce;
	}

	return makeTypedRowExpr(list_make4(id, start, end, expr), EDGEOID, -1);
}

static Relation
openTargetLabel(ParseState *pstate, char *labname)
{
	RangeVar   *rv;
	Relation	relation;

	Assert(labname != NULL);

	rv = makeRangeVar(get_graph_path(true), labname, -1);
	relation = parserOpenTable(pstate, rv, RowExclusiveLock);

	return relation;
}

static List *
transformSetPropList(ParseState *pstate, RangeTblEntry *rte, CSetKind kind,
					 List *items)
{
	List	   *gsplist = NIL;
	ListCell   *li;

	foreach(li, items)
	{
		CypherSetProp *sp = lfirst(li);
		GraphSetProp *gsp;

		gsp = transformSetProp(pstate, rte, sp, kind, gsplist);

		if (gsp != NULL)
			gsplist = lappend(gsplist, gsp);
	}

	return gsplist;
}

static GraphSetProp *
transformSetProp(ParseState *pstate, RangeTblEntry *rte, CypherSetProp *sp,
				 CSetKind kind, List *gsplist)
{
	Node	   *node;
	List	   *inds;
	char	   *varname = NULL;
	Node	   *elem;
	List	   *pathelems = NIL;
	ListCell   *lf;
	Node	   *pathelem;
	Node	   *path = NULL;
	GraphSetProp *gsp;
	Node	   *prop_map;
	Node	   *expr;
	Oid			exprtype;
	GSPKind		gspkind;

	if (!IsA(sp->prop, ColumnRef) && !IsA(sp->prop, A_Indirection))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("only variable or property is valid for SET target")));

	/*
	 * get `elem` and `path` (LHS of the SET clause item)
	 */

	if (IsA(sp->prop, A_Indirection))
	{
		A_Indirection *ind = (A_Indirection *) sp->prop;

		/*
		 * (expr).p...
		 *
		 * `node` is expr part and it can be a ColumnRef.
		 * `inds` is p... part.
		 */
		node = ind->arg;
		inds = ind->indirection;
	}
	else
	{
		/*
		 * v.p...
		 *
		 * `node` is v.p... and it is a ColumnRef.
		 */
		node = sp->prop;
		inds = NIL;
	}

	if (IsA(node, ColumnRef))
	{
		ColumnRef  *cref = (ColumnRef *) node;
		Var		   *var;

		varname = strVal(linitial(cref->fields));
		var = (Var *) getColumnVar(pstate, rte, varname);
		var->location = cref->location;
		elem = (Node *) var;

		if (list_length(cref->fields) > 1)
		{
			for_each_cell(lf, lnext(list_head(cref->fields)))
			{
				pathelem = transformJsonKey(pstate, lfirst(lf),
											EXPR_KIND_UPDATE_SOURCE);

				pathelems = lappend(pathelems, pathelem);
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
					 errmsg("node or relationship is expected"),
					 parser_errposition(pstate, exprLocation(elem))));
	}

	if (inds != NIL)
	{
		foreach(lf, inds)
		{
			pathelem = transformJsonKey(pstate, lfirst(lf),
										EXPR_KIND_UPDATE_SOURCE);

			pathelems = lappend(pathelems, pathelem);
		}
	}

	if (pathelems != NIL)
	{
		path = makeArrayExpr(TEXTARRAYOID, TEXTOID, pathelems);
		path = resolve_future_vertex(pstate, path, FVR_PRESERVE_VAR_REF);
	}

	/*
	 * find the previously processed element with `varname`
	 * to merge property assignments into one expression
	 */
	gsp = findGraphSetProp(gsplist, varname);
	if (gsp == NULL)
	{
		Node *tmp;

		/*
		 * It is the first time to handle the element. Use `elem` to get the
		 * original property map of the element if `elem` is from ColumnRef.
		 * Otherwise, transform `node` to get it.
		 */
		if (IsA(node, ColumnRef))
			tmp = elem;
		else
			tmp = transformExpr(pstate, node, EXPR_KIND_UPDATE_SOURCE);

		/*
		 * get the original property map of the element through type coercion
		 * because we have the type coercion of vertex/edge to jsonb
		 */
		prop_map = coerce_to_target_type(pstate, tmp, exprType(tmp), JSONBOID,
										 -1, COERCION_ASSIGNMENT,
										 COERCE_IMPLICIT_CAST, -1);
	}
	else
	{
		/* use previously modified property map */
		prop_map = gsp->expr;
	}

	/* transform the assigned property */
	expr = transformExpr(pstate, sp->expr, EXPR_KIND_UPDATE_SOURCE);
	expr = resolve_future_vertex(pstate, expr, FVR_PRESERVE_VAR_REF);
	exprtype = exprType(expr);
	expr = coerce_to_target_type(pstate, expr, exprtype, JSONBOID, -1,
								 COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST,
								 -1);
	if (expr == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("expression must be of type jsonb but %s",
						format_type_be(exprtype)),
				 parser_errposition(pstate, exprLocation(expr))));

	if (path == NULL)
	{
		/* LHS is the property map itself */

		if (IsNullAConst(sp->expr))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("cannot set property map to NULL"),
					 errhint("use {} instead of NULL to remove all properties"),
					 parser_errposition(pstate, exprLocation(expr))));

		if (sp->add)
		{
			FuncCall   *concat;

			concat = makeFuncCall(list_make1(makeString("jsonb_concat")), NIL,
								  -1);
			prop_map = ParseFuncOrColumn(pstate, concat->funcname,
										 list_make2(prop_map, expr), concat,
										 -1);
		}
		else
		{
			/* just overwrite the property map */
			prop_map = expr;
		}
	}
	else
	{
		/* LHS is a property in the property map */

		if (sp->add)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("+= operator on a property is not allowed"),
					 parser_errposition(pstate, exprLocation(elem))));

		if (IsNullAConst(sp->expr))
		{
			FuncCall   *delete;

			delete = makeFuncCall(list_make1(makeString("jsonb_delete_path")),
								  NIL, -1);
			prop_map = ParseFuncOrColumn(pstate, delete->funcname,
										 list_make2(prop_map, path), delete,
										 -1);
		}
		else
		{
			FuncCall   *set;

			set = makeFuncCall(list_make1(makeString("jsonb_set")), NIL, -1);
			prop_map = ParseFuncOrColumn(pstate, set->funcname,
										 list_make3(prop_map, path, expr), set,
										 -1);
		}
	}

	switch (kind)
	{
		case CSET_NORMAL:
			gspkind = GSP_NORMAL;
			break;
		case CSET_ON_CREATE:
			gspkind = GSP_ON_CREATE;
			break;
		case CSET_ON_MATCH:
			gspkind = GSP_ON_MATCH;
			break;
		default:
			elog(ERROR, "unexpected CSetKind %d", kind);
	}

	if (gsp == NULL)
	{
		gsp = makeNode(GraphSetProp);
		gsp->kind = gspkind;
		gsp->variable = varname;
		gsp->elem = resolve_future_vertex(pstate, elem, FVR_PRESERVE_VAR_REF);
		gsp->expr = prop_map;

		return gsp;
	}
	else
	{
		Assert(gsp->kind == gspkind);

		gsp->expr = prop_map;

		return NULL;
	}
}

static GraphSetProp *
findGraphSetProp(List *gsplist, char *varname)
{
	ListCell   *le;

	if (varname == NULL)
		return NULL;

	foreach(le, gsplist)
	{
		GraphSetProp *gsp = lfirst(le);

		if (strcmp(gsp->variable, varname) == 0)
			return gsp;
	}

	return NULL;
}

static RangeTblEntry *
transformMergeMatchSub(ParseState *pstate, CypherClause *clause)
{
	ParseState *childParseState;
	Query	   *qry;
	List	   *future_vertices;
	Alias	   *alias;
	RangeTblEntry *rte;
	int			rtindex;

	AssertArg(IsA(clause, CypherClause));

	Assert(pstate->p_expr_kind == EXPR_KIND_NONE);
	pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

	childParseState = make_parsestate(pstate);

	qry = transformMergeMatch(childParseState, clause);

	future_vertices = childParseState->p_future_vertices;

	free_parsestate(childParseState);

	pstate->p_expr_kind = EXPR_KIND_NONE;

	if (!IsA(qry, Query) ||
		(qry->commandType != CMD_SELECT &&
		 qry->commandType != CMD_GRAPHWRITE) ||
		qry->utilityStmt != NULL)
		elog(ERROR, "unexpected command in previous clause");

	alias = makeAliasNoDup(CYPHER_SUBQUERY_ALIAS, NIL);
	rte = addRangeTableEntryForSubquery(pstate, qry, alias,
										pstate->p_lateral_active, true);

	rtindex = RTERangeTablePosn(pstate, rte, NULL);

	future_vertices = removeResolvedFutureVertices(future_vertices);
	future_vertices = adjustFutureVertices(future_vertices, rte, rtindex);
	pstate->p_future_vertices = list_concat(pstate->p_future_vertices,
											future_vertices);

	addRTEtoJoinlist(pstate, rte, true);

	return rte;
}

static Query *
transformMergeMatch(ParseState *pstate, CypherClause *clause)
{
	Query	   *qry;
	RangeTblEntry *rte;
	Node	   *qual = NULL;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	rte = transformMergeMatchJoin(pstate, clause);

	qry->targetList = makeTargetListFromJoin(pstate, rte);
	markTargetListOrigins(pstate, qry->targetList);

	qual = qualAndExpr(qual, pstate->p_resolved_qual);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;

	assign_query_collations(pstate, qry);

	return qry;
}

/* See transformMatchOptional() */
static RangeTblEntry *
transformMergeMatchJoin(ParseState *pstate, CypherClause *clause)
{
	CypherMergeClause *detail = (CypherMergeClause *) clause->detail;
	Node	   *prevclause = clause->prev;
	RangeTblEntry *l_rte;
	Alias	   *r_alias;
	RangeTblEntry *r_rte;
	Node	   *qual;
	Alias	   *alias;

	if (prevclause == NULL)
		l_rte = transformNullSelect(pstate);
	else
		l_rte = transformClause(pstate, prevclause);

	pstate->p_lateral_active = true;

	r_alias = makeAliasNoDup(CYPHER_MERGEMATCH_ALIAS, NIL);
	r_rte = transformClauseImpl(pstate, makeMatchForMerge(detail->pattern),
								r_alias);

	pstate->p_lateral_active = false;

	qual = makeBoolConst(true, false);
	alias = makeAliasNoDup(CYPHER_SUBQUERY_ALIAS, NIL);

	return incrementalJoinRTEs(pstate, JOIN_CYPHER_MERGE, l_rte, r_rte, qual,
							   alias);
}

static RangeTblEntry *
transformNullSelect(ParseState *pstate)
{
	A_Const	   *nullconst;
	ResTarget  *nullres;
	SelectStmt *sel;
	Alias	   *alias;
	Query	   *qry;
	RangeTblEntry *rte;

	nullconst = makeNode(A_Const);
	nullconst->val.type = T_Null;
	nullconst->location = -1;

	nullres = makeResTarget((Node *) nullconst, NULL);

	sel = makeNode(SelectStmt);
	sel->targetList = list_make1(nullres);

	alias = makeAliasNoDup(CYPHER_SUBQUERY_ALIAS, NIL);

	Assert(pstate->p_expr_kind == EXPR_KIND_NONE);
	pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

	qry = parse_sub_analyze((Node *) sel, pstate, NULL,
							isLockedRefname(pstate, alias->aliasname));

	pstate->p_expr_kind = EXPR_KIND_NONE;

	rte = addRangeTableEntryForSubquery(pstate, qry, alias, false, true);
	addRTEtoJoinlist(pstate, rte, false);

	return rte;
}

static Node *
makeMatchForMerge(List *pattern)
{
	CypherMatchClause *match;
	CypherClause *clause;

	match = makeNode(CypherMatchClause);
	match->pattern = pattern;
	match->where = NULL;
	match->optional = false;

	clause = makeNode(CypherClause);
	clause->detail = (Node *) match;
	clause->prev = NULL;

	return (Node *) clause;
}

static List *
transformMergeCreate(ParseState *pstate, List *pattern, RangeTblEntry *prevrte,
					 List *resultList)
{
	List	   *prevtlist;
	CypherPath *path;
	bool		singlenode;
	ListCell   *le;
	List	   *gchain = NIL;
	GraphPath  *gpath;

	AssertArg(prevrte != NULL && prevrte->rtekind == RTE_SUBQUERY);

	/*
	 * Copy the target list of the RTE of the previous clause
	 * to check duplicate variables.
	 */
	prevtlist = copyObject(prevrte->subquery->targetList);

	path = linitial(pattern);
	singlenode = (list_length(path->chain) == 1);
	foreach(le, path->chain)
	{
		Node *elem = lfirst(le);

		if (IsA(elem, CypherNode))
		{
			CypherNode *cnode = (CypherNode *) elem;
			GraphVertex *gvertex;

			gvertex = transformMergeNode(pstate, cnode, singlenode, &prevtlist,
										 resultList);

			gchain = lappend(gchain, gvertex);
		}
		else
		{
			CypherRel  *crel = (CypherRel *) elem;
			GraphEdge  *gedge;

			Assert(IsA(elem, CypherRel));

			gedge = transformMergeRel(pstate, crel, &prevtlist, resultList);

			gchain = lappend(gchain, gedge);
		}
	}

	gpath = makeNode(GraphPath);
	gpath->variable = getCypherName(path->variable);
	gpath->chain = gchain;

	return list_make1(gpath);
}

/* See transformCreateNode() */
static GraphVertex *
transformMergeNode(ParseState *pstate, CypherNode *cnode, bool singlenode,
				   List **targetList, List *resultList)
{
	char	   *varname = getCypherName(cnode->variable);
	int			varloc = getCypherNameLoc(cnode->variable);
	char	   *labname = getCypherName(cnode->label);
	TargetEntry *te;
	Relation 	relation;
	Node	   *vertex = NULL;
	Oid			relid = InvalidOid;
	Node	   *qual = NULL;
	AttrNumber	resno = InvalidAttrNumber;
	GraphVertex	*gvertex;

	te = findTarget(*targetList, varname);
	if (te != NULL &&
		(exprType((Node *) te->expr) != VERTEXOID || !isNodeForRef(cnode) ||
		 singlenode))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_ALIAS),
				 errmsg("duplicate variable \"%s\"", varname),
				 parser_errposition(pstate, varloc)));

	if (labname == NULL)
	{
		labname = AG_VERTEX;
	}
	else
	{
		createVertexLabelIfNotExist(pstate, labname,
									getCypherNameLoc(cnode->label));
	}

	relation = openTargetLabel(pstate, labname);

	vertex = makeNewVertex(pstate, relation, cnode->prop_map);
	relid = RelationGetRelid(relation);

	heap_close(relation, NoLock);

	if (cnode->prop_map != NULL)
	{
		FuncCall   *fc;
		Node	   *prop_expr;

		/* check if null values exist in the properties */
		fc = makeFuncCall(list_make1(makeString("jsonb_has_nulls")), NIL, -1);
		prop_expr = transformPropMap(pstate, cnode->prop_map,
									 EXPR_KIND_SELECT_TARGET);
		qual = ParseFuncOrColumn(pstate, fc->funcname,
								 list_make1(prop_expr), fc, -1);
	}

	te = makeTargetEntry((Expr *) vertex,
						 InvalidAttrNumber,
						 (varname == NULL ? "?column?" : varname),
						 false);

	*targetList = lappend(*targetList, te);

	pstate->p_target_labels =
			list_append_unique_oid(pstate->p_target_labels, relid);

	te = findTarget(resultList, varname);
	if (te != NULL)
		resno = te->resno;

	gvertex = makeNode(GraphVertex);
	gvertex->resno = resno;
	gvertex->create = true;
	gvertex->relid = relid;
	gvertex->expr = vertex;
	gvertex->qual = qual;

	return gvertex;
}

/* See transformCreateRel() */
static GraphEdge *
transformMergeRel(ParseState *pstate, CypherRel *crel, List **targetList,
				  List *resultList)
{
	char	   *varname;
	Node	   *type;
	char	   *typname;
	Relation 	relation;
	Node	   *edge;
	Oid			relid = InvalidOid;
	Node	   *qual = NULL;
	TargetEntry	*te;
	AttrNumber	resno = InvalidAttrNumber;
	GraphEdge  *gedge;

	if (list_length(crel->types) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("only one relationship type is allowed for MERGE")));

	if (crel->varlen != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("variable length relationship is not allowed for MERGE")));

	varname = getCypherName(crel->variable);

	if (findTarget(*targetList, varname) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_ALIAS),
				 errmsg("duplicate variable \"%s\"", varname),
				 parser_errposition(pstate, getCypherNameLoc(crel->variable))));

	type = linitial(crel->types);
	typname = getCypherName(type);

	createEdgeLabelIfNotExist(pstate, typname, getCypherNameLoc(type));

	relation = openTargetLabel(pstate, getCypherName(linitial(crel->types)));

	edge = makeNewEdge(pstate, relation, crel->prop_map);
	relid = RelationGetRelid(relation);

	heap_close(relation, NoLock);

	if (crel->prop_map)
	{
		FuncCall   *fc;
		Node	   *prop_expr;

		fc = makeFuncCall(list_make1(makeString("jsonb_has_nulls")), NIL, -1);
		prop_expr = transformPropMap(pstate, crel->prop_map,
									 EXPR_KIND_SELECT_TARGET);
		qual = ParseFuncOrColumn(pstate, fc->funcname,
								 list_make1(prop_expr), fc, -1);
	}

	te = makeTargetEntry((Expr *) edge,
						 InvalidAttrNumber,
						 (varname == NULL ? "?column?" : varname),
						 false);

	*targetList = lappend(*targetList, te);

	pstate->p_target_labels =
			list_append_unique_oid(pstate->p_target_labels, relid);

	te = findTarget(resultList, varname);
	if (te != NULL)
		resno = te->resno;

	gedge = makeNode(GraphEdge);
	switch (crel->direction)
	{
		case CYPHER_REL_DIR_LEFT:
			gedge->direction = GRAPH_EDGE_DIR_LEFT;
			break;
		case CYPHER_REL_DIR_RIGHT:
		case CYPHER_REL_DIR_NONE:
			/*
			 * According to the TCK of openCypher,
			 * use outgoing direction if direction is unspecified.
			 */
			gedge->direction = GRAPH_EDGE_DIR_RIGHT;
			break;
		default:
			Assert(!"invalid direction");
	}
	gedge->resno = resno;
	gedge->relid = relid;
	gedge->expr = edge;
	gedge->qual = qual;

	return gedge;
}

static List *
transformMergeOnSet(ParseState *pstate, List *sets, RangeTblEntry *rte)
{
	ListCell   *lc;
	List	   *l_oncreate = NIL;
	List	   *l_onmatch = NIL;

	foreach(lc, sets)
	{
		CypherSetClause *detail = lfirst(lc);

		if (detail->kind == CSET_ON_CREATE)
		{
			l_oncreate = list_concat(l_oncreate, detail->items);
		}
		else
		{
			Assert(detail->kind == CSET_ON_MATCH);

			l_onmatch = list_concat(l_onmatch, detail->items);
		}
	}

	l_oncreate = transformSetPropList(pstate, rte, CSET_ON_CREATE, l_oncreate);
	l_onmatch = transformSetPropList(pstate, rte, CSET_ON_MATCH, l_onmatch);

	return list_concat(l_onmatch, l_oncreate);
}

static bool
labelExist(ParseState *pstate, char *labname, int labloc, char labkind,
		   bool throw)
{
	Oid			graphid;
	HeapTuple	tuple;
	char	   *elemstr;
	Form_ag_label labtup;

	graphid = get_graph_path_oid();

	tuple = SearchSysCache2(LABELNAMEGRAPH, PointerGetDatum(labname),
							ObjectIdGetDatum(graphid));
	if (!HeapTupleIsValid(tuple))
	{
		if (throw)
		{
			if (labkind == LABEL_KIND_VERTEX)
				elemstr = "vertex";
			else
				elemstr = "edge";

			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("%s label \"%s\" does not exist", elemstr, labname),
					 parser_errposition(pstate, labloc)));
		}
		else
		{
			return false;
		}
	}

	labtup = (Form_ag_label) GETSTRUCT(tuple);
	if (labtup->labkind != labkind)
	{
		if (labtup->labkind == LABEL_KIND_VERTEX)
			elemstr = "vertex";
		else
			elemstr = "edge";

		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("label \"%s\" is %s label", labname, elemstr),
				 parser_errposition(pstate, labloc)));
	}

	ReleaseSysCache(tuple);

	return true;
}

static void
createLabelIfNotExist(ParseState *pstate, char *labname, int labloc,
					  char labkind)
{
	char	   *keyword;
	char		sqlcmd[128];

	if (labelExist(pstate, labname, labloc, labkind, false))
		return;

	if (labkind == LABEL_KIND_VERTEX)
		keyword = "VLABEL";
	else
		keyword = "ELABEL";

	snprintf(sqlcmd, sizeof(sqlcmd), "CREATE %s %s", keyword, labname);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	SPI_exec(sqlcmd, 0);

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");
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
				 errmsg("property map must be of type jsonb"),
				 parser_errposition(pstate, exprLocation(prop_map))));

	if (exprKind == EXPR_KIND_INSERT_TARGET)
	{
		FuncCall *strip;

		/* keys with NULL value is not allowed */
		strip = makeFuncCall(list_make1(makeString("jsonb_strip_nulls")), NIL,
							 -1);
		prop_map = ParseFuncOrColumn(pstate, strip->funcname,
									 list_make1(prop_map), strip, -1);
	}

	return resolve_future_vertex(pstate, prop_map, 0);
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
transformClause(ParseState *pstate, Node *clause)
{
	Alias *alias;
	RangeTblEntry *rte;

	alias = makeAliasNoDup(CYPHER_SUBQUERY_ALIAS, NIL);
	rte = transformClauseImpl(pstate, clause, alias);
	addRTEtoJoinlist(pstate, rte, true);

	return rte;
}

static RangeTblEntry *
transformClauseImpl(ParseState *pstate, Node *clause, Alias *alias)
{
	ParseState *childParseState;
	Query	   *qry;
	List	   *future_vertices;
	RangeTblEntry *rte;
	int			rtindex;

	AssertArg(IsA(clause, CypherClause));

	Assert(pstate->p_expr_kind == EXPR_KIND_NONE);
	pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

	childParseState = make_parsestate(pstate);
	childParseState->p_is_match_quals = pstate->p_is_match_quals;
	childParseState->p_is_fp_processed = pstate->p_is_fp_processed;
	childParseState->p_is_optional_match = pstate->p_is_optional_match;

	qry = transformStmt(childParseState, clause);

	pstate->p_elem_quals = childParseState->p_elem_quals;
	future_vertices = childParseState->p_future_vertices;

	free_parsestate(childParseState);

	pstate->p_expr_kind = EXPR_KIND_NONE;

	if (!IsA(qry, Query) ||
		(qry->commandType != CMD_SELECT &&
		 qry->commandType != CMD_GRAPHWRITE) ||
		qry->utilityStmt != NULL)
		elog(ERROR, "unexpected command in previous clause");

	rte = addRangeTableEntryForSubquery(pstate, qry, alias,
										pstate->p_lateral_active, true);

	rtindex = RTERangeTablePosn(pstate, rte, NULL);

	adjustElemQuals(pstate->p_elem_quals, rte, rtindex);

	future_vertices = removeResolvedFutureVertices(future_vertices);
	future_vertices = adjustFutureVertices(future_vertices, rte, rtindex);
	pstate->p_future_vertices = list_concat(pstate->p_future_vertices,
											future_vertices);

	return rte;
}

static RangeTblEntry *
incrementalJoinRTEs(ParseState *pstate, JoinType jointype,
					RangeTblEntry *l_rte, RangeTblEntry *r_rte, Node *qual,
					Alias *alias)
{
	ParseNamespaceItem *l_nsitem;
	int			l_rtindex;
	ListCell   *le;
	Node	   *l_jt = NULL;
	RangeTblRef *r_rtr;
	ParseNamespaceItem *r_nsitem;
	List	   *res_colnames = NIL;
	List	   *res_colvars = NIL;
	JoinExpr   *j;
	RangeTblEntry *rte;
	int			i;
	ParseNamespaceItem *nsitem;

	l_nsitem = findNamespaceItemForRTE(pstate, l_rte);
	l_nsitem->p_cols_visible = false;

	/* find JOIN-subtree of `l_rte` */
	l_rtindex = RTERangeTablePosn(pstate, l_rte, NULL);
	foreach(le, pstate->p_joinlist)
	{
		Node	   *jt = lfirst(le);
		int			rtindex;

		if (IsA(jt, RangeTblRef))
		{
			rtindex = ((RangeTblRef *) jt)->rtindex;
		}
		else
		{
			Assert(IsA(jt, JoinExpr));
			rtindex = ((JoinExpr *) jt)->rtindex;
		}

		if (rtindex == l_rtindex)
			l_jt = jt;
	}
	Assert(l_jt != NULL);

	makeExtraFromRTE(pstate, r_rte, &r_rtr, &r_nsitem, false);

	j = makeNode(JoinExpr);
	j->jointype = jointype;
	j->larg = l_jt;
	j->rarg = (Node *) r_rtr;
	j->quals = qual;
	j->alias = alias;

	makeJoinResCols(pstate, l_rte, r_rte, &res_colnames, &res_colvars);
	rte = addRangeTableEntryForJoin(pstate, res_colnames, j->jointype,
									res_colvars, j->alias, true);
	j->rtindex = RTERangeTablePosn(pstate, rte, NULL);

	for (i = list_length(pstate->p_joinexprs) + 1; i < j->rtindex; i++)
		pstate->p_joinexprs = lappend(pstate->p_joinexprs, NULL);
	pstate->p_joinexprs = lappend(pstate->p_joinexprs, j);
	Assert(list_length(pstate->p_joinexprs) == j->rtindex);

	pstate->p_joinlist = list_delete_ptr(pstate->p_joinlist, l_jt);
	pstate->p_joinlist = lappend(pstate->p_joinlist, j);

	makeExtraFromRTE(pstate, rte, NULL, &nsitem, true);
	pstate->p_namespace = lappend(pstate->p_namespace, r_nsitem);
	pstate->p_namespace = lappend(pstate->p_namespace, nsitem);

	return rte;
}

static void
makeJoinResCols(ParseState *pstate, RangeTblEntry *l_rte, RangeTblEntry *r_rte,
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
						 errmsg("node or relationship is expected")));
			}
		}
	}

	*res_colnames = list_concat(*res_colnames, colnames);
	*res_colvars = list_concat(*res_colvars, colvars);
}

static void
addRTEtoJoinlist(ParseState *pstate, RangeTblEntry *rte, bool visible)
{
	RangeTblEntry *tmp;
	RangeTblRef *rtr;
	ParseNamespaceItem *nsitem;

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

	makeExtraFromRTE(pstate, rte, &rtr, &nsitem, visible);
	pstate->p_joinlist = lappend(pstate->p_joinlist, rtr);
	pstate->p_namespace = lappend(pstate->p_namespace, nsitem);
}

static void
makeExtraFromRTE(ParseState *pstate, RangeTblEntry *rte, RangeTblRef **rtr,
				 ParseNamespaceItem **nsitem, bool visible)
{
	if (rtr != NULL)
	{
		RangeTblRef *_rtr;

		_rtr = makeNode(RangeTblRef);
		_rtr->rtindex = RTERangeTablePosn(pstate, rte, NULL);

		*rtr = _rtr;
	}

	if (nsitem != NULL)
	{
		ParseNamespaceItem *_nsitem;

		_nsitem = (ParseNamespaceItem *) palloc(sizeof(ParseNamespaceItem));
		_nsitem->p_rte = rte;
		_nsitem->p_rel_visible = visible;
		_nsitem->p_cols_visible = visible;
		_nsitem->p_lateral_only = false;
		_nsitem->p_lateral_ok = true;

		*nsitem = _nsitem;
	}
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

static ParseNamespaceItem *
findNamespaceItemForRTE(ParseState *pstate, RangeTblEntry *rte)
{
	ListCell *lni;

	foreach(lni, pstate->p_namespace)
	{
		ParseNamespaceItem *nsitem = lfirst(lni);

		if (nsitem->p_rte == rte)
			return nsitem;
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
							  resname,
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
							  resname,
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
						   rte->eref->aliasname,
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
getSysColumnVar(ParseState *pstate, RangeTblEntry *rte, int attnum)
{
	Var *var;

	AssertArg(attnum <= SelfItemPointerAttributeNumber &&
			  attnum >= FirstLowInvalidHeapAttributeNumber);

	var = make_var(pstate, rte, attnum, -1);

	markVarForSelectPriv(pstate, var, rte);

	return (Node *) var;
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

		if (namestrcmp(&attr->attname, fname) == 0)
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
qualAndExpr(Node *qual, Node *expr)
{
	if (qual == NULL)
		return expr;

	if (expr == NULL)
		return qual;

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

static Node *
makeColumnRef(List *fields)
{
	ColumnRef *n = makeNode(ColumnRef);

	n->fields = fields;
	n->location = -1;
	return (Node *)n;
}

static Node *
makeDummyEdgeRef(Node *ctid)
{
	A_Const	   *relid;
	FuncCall   *edgeref;

	/* dummy relid */
	relid = makeNode(A_Const);
	relid->val.type = T_Integer;
	relid->val.val.ival = 0;
	relid->location = -1;

	edgeref = makeFuncCall(list_make1(makeString("edgeref")),
						   list_make2(relid, ctid), -1);

	return (Node *) edgeref;
}

static bool
IsNullAConst(Node *arg)
{
	AssertArg(arg != NULL);

	if (IsA(arg, A_Const))
	{
		A_Const	   *con = (A_Const *) arg;

		if (con->val.type == T_Null)
			return true;
	}
	return false;
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
