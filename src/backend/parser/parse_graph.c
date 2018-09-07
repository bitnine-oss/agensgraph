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
#include "parser/parse_cypher_expr.h"
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
#define CYPHER_DELETEJOIN_ALIAS	"_d"

#define EDGE_UNION_START_ID		"_start"
#define EDGE_UNION_END_ID		"_end"

#define VLE_LEFT_ALIAS			"l"
#define VLE_RIGHT_ALIAS			"r"
#define VLE_VERTEX_ALIAS		"vtx"
#define VLE_COLNAME_IDS			"ids"
#define VLE_COLNAME_EDGES		"edges"
#define VLE_COLNAME_VERTICES	"vertices"
#define VLE_COLNAME_NEXT		"next"
#define VLE_COLNAME_EDGE		"edge"
#define VLE_COLNAME_VERTEX		"vertex"

#define DELETE_VERTEX_ALIAS		"v"
#define DELETE_EDGE_ALIAS		"e"

bool		enable_eager = true;

typedef struct
{
	char	   *varname;		/* variable assigned to the node */
	char	   *labname;		/* final label of the vertex */
	bool		prop_constr;	/* has property constraints? */
} NodeInfo;

typedef struct
{
	Index		varno;			/* of the RTE */
	AttrNumber	varattno;		/* in the target list */
	Node	   *prop_constr;	/* property constraint of the element */
} ElemQual;

typedef struct
{
	TargetEntry *te;
	Node	   *prop_map;
} ElemQualOnly;

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

typedef struct
{
	Query	   *qry;
	int			sublevels_up;
	bool		in_preserved;
	AttrNumber	resno;
	Oid			relid;
} find_target_label_context;

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
								bool force, List **targetList, List **eqoList);
static RangeTblEntry *transformMatchRel(ParseState *pstate, CypherRel *crel,
										List **targetList, List **eqoList,
										bool pathout);
static RangeTblEntry *transformMatchSR(ParseState *pstate, CypherRel *crel,
									   List **targetList, List **eqoList);
static RangeTblEntry *addEdgeUnion(ParseState *pstate, char *edge_label,
								   bool only, int location, Alias *alias);
static Node *genEdgeUnion(char *edge_label, bool only, int location);
static void setInitialVidForVLE(ParseState *pstate, CypherRel *crel,
								Node *vertex, CypherRel *prev_crel,
								RangeTblEntry *prev_edge);
static RangeTblEntry *transformMatchVLE(ParseState *pstate, CypherRel *crel,
										List **targetList, bool pathout);
static SelectStmt *genVLESubselect(ParseState *pstate, CypherRel *crel,
								   bool out, bool pathout);
static Node *genVLELeftChild(ParseState *pstate, CypherRel *crel,
							 bool out, bool pathout);
static Node *genVLERightChild(ParseState *pstate, CypherRel *crel,
							  bool out, bool pathout);
static Node *genVertexSimple(char *aliasname);
static Node *genEdgeSimple(char *aliasname);
static Node *genVLEEdgeSubselect(ParseState *pstate, CypherRel *crel,
								 char *aliasname);
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
static Node *addQualUniqueEdges(ParseState *pstate, Node *qual, List *ueids,
								List *ueidarrs);
/* MATCH - VLE */
static Node *vtxArrConcat(ParseState *pstate, Node *array, Node *elem);
static Node *edgeArrConcat(ParseState *pstate, Node *array, Node *elem);
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
static List *transformCreatePattern(ParseState *pstate, CypherPath *cpath,
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
								  bool is_remove, CSetKind kind, List *items);
static GraphSetProp *transformSetProp(ParseState *pstate, RangeTblEntry *rte,
									  CypherSetProp *sp, bool is_remove,
									  CSetKind kind, List *gsplist);
static GraphSetProp *findGraphSetProp(List *gsplist, char *varname);

/* MERGE */
static Query *transformMergeMatch(ParseState *pstate, Node *parseTree);
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

/* DELETE */
static Query *transformDeleteJoin(ParseState *pstate, Node *parseTree);
static Query *transformDeleteEdges(ParseState *pstate, Node *parseTree);
static RangeTblEntry *transformDeleteJoinRTE(ParseState *pstate,
											 CypherClause *clause);
static A_ArrayExpr *verticesAppend(A_ArrayExpr *vertices, Node *expr);
static Node *verticesConcat(Node *vertices, Node *expr);
static Node *makeSelectEdgesVertices(Node *vertices,
									 CypherDeleteClause *delete,
									 char **edges_resname);
static Node *makeEdgesForDetach(void);
static RangeFunction *makeUnnestVertices(Node *vertices);
static BoolExpr *makeEdgesVertexQual(void);
static List *extractVerticesExpr(ParseState *pstate, List *exprlist,
								 ParseExprKind exprKind);
static List *extractEdgesExpr(ParseState *pstate, List *exprlist,
							  ParseExprKind exprKind);
static char *getDeleteTargetName(ParseState *pstate, Node *expr);

/* graph write */
static List *findAllModifiedLabels(Query *qry);
static Oid find_target_label(Node *node, Query *qry);
static bool find_target_label_walker(Node *node,
									 find_target_label_context *ctx);

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
static Node *stripNullKeys(ParseState *pstate, Node *properties);
static void assign_query_eager(Query *query);

/* transform */
typedef Query *(*TransformMethod) (ParseState *pstate, Node *parseTree);
static RangeTblEntry *transformClause(ParseState *pstate, Node *clause);
static RangeTblEntry *transformClauseBy(ParseState *pstate, Node *clause,
										TransformMethod transform);
static RangeTblEntry *transformClauseImpl(ParseState *pstate, Node *clause,
										  TransformMethod transform,
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
static Node *makeEdgeExpr(ParseState *pstate, CypherRel *crel,
						  RangeTblEntry *rte, int location);
static Node *makePathVertexExpr(ParseState *pstate, Node *obj);
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
static A_Const *makeNullAConst(void);
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
	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
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

		qual = transformCypherWhere(pstate, where, EXPR_KIND_WHERE);
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

		qry->sortClause = transformCypherOrderBy(pstate, order,
												 &qry->targetList);

		if (distinct == NIL)
		{
			/* intentionally blank, do nothing */
		}
		else
		{
			Assert(linitial(distinct) == NULL);

			qry->distinctClause = transformDistinctClause(pstate,
														  &qry->targetList,
														  qry->sortClause,
														  false);
		}

		qry->limitOffset = transformCypherLimit(pstate, skip, EXPR_KIND_OFFSET,
												"SKIP");
		qry->limitOffset = resolve_future_vertex(pstate, qry->limitOffset, 0);

		qry->limitCount = transformCypherLimit(pstate, limit, EXPR_KIND_LIMIT,
											   "LIMIT");
		qry->limitCount = resolve_future_vertex(pstate, qry->limitCount, 0);
	}
	else
	{
		if (clause->prev != NULL)
			transformClause(pstate, clause->prev);

		qry->targetList = transformItemList(pstate, detail->items,
											EXPR_KIND_SELECT_TARGET);

		if (detail->kind == CP_WITH)
			checkNameInItems(pstate, detail->items, qry->targetList);

		qry->groupClause = generateGroupClause(pstate, &qry->targetList,
											   qry->sortClause);
	}

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
	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
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
	if (detail->optional)
	{
		rte = transformMatchOptional(pstate, clause);

		qry->targetList = makeTargetListFromJoin(pstate, rte);
	}
	else
	{
		if (clause->prev != NULL)
		{
			/* MATCH clause cannot follow OPTIONAL MATCH clause */
			if (cypherClauseTag(clause->prev) == T_CypherMatchClause)
			{
				CypherClause *prev;
				CypherMatchClause *prev_detail;

				prev = (CypherClause *) clause->prev;
				prev_detail = (CypherMatchClause *) prev->detail;
				if (prev_detail->optional)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("MATCH right after OPTIONAL MATCH is not allowed"),
							 errhint("Use a WITH clause between them")));
			}
		}

		if (!pstate->p_is_match_quals &&
			(detail->where != NULL || hasPropConstr(detail->pattern)))
		{
			int flags = (pstate->p_is_optional_match ? FVR_IGNORE_NULLABLE: 0);

			pstate->p_is_match_quals = true;
			rte = transformClause(pstate, (Node *) clause);

			qry->targetList = makeTargetListFromRTE(pstate, rte);

			qual = transformCypherWhere(pstate, detail->where,
										EXPR_KIND_WHERE);
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
	CypherPath *cpath;
	Query	   *qry;

	detail = (CypherCreateClause *) clause->detail;
	cpath = llast(detail->pattern);

	/* make a CREATE clause for each path in the pattern */
	if (list_length(detail->pattern) > 1)
	{
		CypherCreateClause *newcreate;
		CypherClause *newprev;

		newcreate = makeNode(CypherCreateClause);
		newcreate->pattern = list_truncate(detail->pattern,
										   list_length(detail->pattern) - 1);

		newprev = makeNode(CypherClause);
		newprev->detail = (Node *) newcreate;
		newprev->prev = clause->prev;

		clause->prev = (Node *) newprev;
	}

	qry = makeNode(Query);
	qry->commandType = CMD_GRAPHWRITE;
	qry->graph.writeOp = GWROP_CREATE;
	qry->graph.last = (pstate->parentParseState == NULL);

	if (clause->prev != NULL)
	{
		RangeTblEntry *rte;

		rte = transformClause(pstate, (Node *) clause->prev);

		qry->targetList = makeTargetListFromRTE(pstate, rte);
	}

	qry->graph.pattern = transformCreatePattern(pstate, cpath,
												&qry->targetList);
	qry->graph.targets = pstate->p_target_labels;
	qry->graph.nr_modify = pstate->p_nr_modify_clause++;

	qry->targetList = (List *) resolve_future_vertex(pstate,
													 (Node *) qry->targetList,
													 FVR_DONT_RESOLVE);
	markTargetListOrigins(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, pstate->p_resolved_qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;

	assign_query_collations(pstate, qry);

	assign_query_eager(qry);

	return qry;
}

Query *
transformCypherDeleteClause(ParseState *pstate, CypherClause *clause)
{
	CypherDeleteClause *detail = (CypherDeleteClause *) clause->detail;
	RangeTblEntry *rte;
	ListCell   *le;
	Query	   *qry;

	/* DELETE cannot be the first clause */
	AssertArg(clause->prev != NULL);

	/* Merge same mode of DELETE clauses for reducing delete join */
	while (cypherClauseTag(clause->prev) == T_CypherDeleteClause)
	{
		CypherClause	   *prev = (CypherClause *) clause->prev;
		CypherDeleteClause *prevDel = (CypherDeleteClause *) prev->detail;

		if (prevDel->detach == detail->detach)
			detail->exprs = list_concat(prevDel->exprs, detail->exprs);

		clause->prev = prev->prev;
	}

	qry = makeNode(Query);
	qry->commandType = CMD_GRAPHWRITE;
	qry->graph.writeOp = GWROP_DELETE;
	qry->graph.last = (pstate->parentParseState == NULL);
	qry->graph.detach = detail->detach;

	rte = transformClauseBy(pstate, (Node *) clause, transformDeleteJoin);

	qry->targetList = makeTargetListFromRTE(pstate, rte);
	qry->graph.exprs = extractVerticesExpr(pstate, detail->exprs,
										   EXPR_KIND_OTHER);
	qry->graph.nr_modify = pstate->p_nr_modify_clause++;

	/*
	 * The edges of the vertices to remove are used only for removal,
	 * not for the next clause.
	 */
	if (detail->detach && pstate->p_delete_edges_resname)
	{
		TargetEntry *te;
		Node		*edges;
		GraphDelElem *gde = makeNode(GraphDelElem);

		/* This assumes the edge array always comes last. */
		te = llast(qry->targetList);
		edges = llast(detail->exprs);

		gde->variable = pstrdup(pstate->p_delete_edges_resname);
		gde->elem = transformCypherExpr(pstate, edges, EXPR_KIND_OTHER);

		/* Add expression for deleting edges related target vertices. */
		qry->graph.exprs = lappend(qry->graph.exprs, gde);

		if (strcmp(te->resname, pstate->p_delete_edges_resname) == 0)
			te->resjunk = true;

		pstate->p_delete_edges_resname = NULL;
	}

	foreach(le, qry->graph.exprs)
	{
		GraphDelElem *gde = lfirst(le);

		gde->elem = resolve_future_vertex(pstate, gde->elem,
										  FVR_PRESERVE_VAR_REF);
	}

	qry->targetList = (List *) resolve_future_vertex(pstate,
													 (Node *) qry->targetList,
													 FVR_DONT_RESOLVE);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);

	qry->hasSubLinks = pstate->p_hasSubLinks;

	assign_query_collations(pstate, qry);

	assign_query_eager(qry);

	findAllModifiedLabels(qry);

	return qry;
}

Query *
transformCypherSetClause(ParseState *pstate, CypherClause *clause)
{
	CypherSetClause *detail = (CypherSetClause *) clause->detail;
	Query	   *qry;
	RangeTblEntry *rte;
	ListCell   *le;

	/* SET/REMOVE cannot be the first clause */
	AssertArg(clause->prev != NULL);
	Assert(detail->kind == CSET_NORMAL);

	qry = makeNode(Query);
	qry->commandType = CMD_GRAPHWRITE;
	qry->graph.writeOp = GWROP_SET;
	qry->graph.last = (pstate->parentParseState == NULL);

	rte = transformClause(pstate, clause->prev);

	qry->targetList = makeTargetListFromRTE(pstate, rte);

	qry->graph.sets = transformSetPropList(pstate, rte, detail->is_remove,
										   detail->kind, detail->items);
	foreach(le, qry->graph.sets)
	{
		GraphSetProp *gsp = lfirst(le);

		gsp->elem = resolve_future_vertex(pstate, gsp->elem,
										  FVR_PRESERVE_VAR_REF);
		gsp->expr = resolve_future_vertex(pstate, gsp->expr,
										  FVR_PRESERVE_VAR_REF);
	}

	qry->graph.nr_modify = pstate->p_nr_modify_clause++;

	qry->targetList = (List *) resolve_future_vertex(pstate,
													 (Node *) qry->targetList,
													 FVR_DONT_RESOLVE);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, pstate->p_resolved_qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;

	assign_query_collations(pstate, qry);
	foreach(le, qry->graph.sets)
	{
		GraphSetProp *gsp = lfirst(le);

		assign_expr_collations(pstate, gsp->expr);
	}

	assign_query_eager(qry);

	findAllModifiedLabels(qry);

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

	rte = transformClauseBy(pstate, (Node *) clause, transformMergeMatch);
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
	qry->graph.nr_modify = pstate->p_nr_modify_clause++;

	qry->targetList = (List *) resolve_future_vertex(pstate,
													 (Node *) qry->targetList,
													 FVR_DONT_RESOLVE);
	markTargetListOrigins(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, pstate->p_resolved_qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;

	assign_query_collations(pstate, qry);

	assign_query_eager(qry);

	findAllModifiedLabels(qry);

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

	rte = addRangeTableEntry(pstate, rv, rv->alias, rv->inh, true);
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

	if (clause->prev == NULL)
	{
		Query	   *qry;
		Alias	   *l_alias;

		/*
		 * To return NULL if OPTIONAL MATCH is the first clause and there is
		 * no result that matches the pattern.
		 */

		qry = makeNode(Query);
		qry->commandType = CMD_SELECT;
		qry->rtable = NIL;
		qry->jointree = makeFromExpr(NIL, NULL);

		l_alias = makeAliasNoDup(CYPHER_SUBQUERY_ALIAS, NIL);
		l_rte = addRangeTableEntryForSubquery(pstate, qry, l_alias,
											  pstate->p_lateral_active, true);

		RTERangeTablePosn(pstate, l_rte, NULL);
		addRTEtoJoinlist(pstate, l_rte, true);
	}
	else
	{
		l_rte = transformClause(pstate, clause->prev);
	}

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
	r_rte = transformClauseImpl(pstate, (Node *) clause, transformStmt,
								r_alias);

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
	List	   *eqoList = NIL;
	Node	   *qual = NULL;
	ListCell   *lc;
	ListCell   *leqo;

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
			TargetEntry *te;
			ListCell   *le;
			CypherNode *cnode;
			Node	   *vertex;
			CypherRel  *prev_crel = NULL;
			RangeTblEntry *prev_edge = NULL;
			Node	   *pvs = makeArrayExpr(VERTEXARRAYOID, VERTEXOID, NIL);
			Node	   *pes = makeArrayExpr(EDGEARRAYOID, EDGEOID, NIL);

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
													targetList, &eqoList);
						break;
					}

					crel = lfirst(le);

					/*
					 * if `crel` is zero-length VLE, get RTE of `cnode`
					 * because `crel` needs `id` column of the RTE
					 */
					zero = isZeroLengthVLE(crel);
					vertex = transformMatchNode(pstate, cnode,
												(zero || out), targetList,
												&eqoList);

					if (p->kind != CPATH_NORMAL)
					{
						le = lnext(le);
						continue;
					}

					setInitialVidForVLE(pstate, crel, vertex, NULL, NULL);
					edge = transformMatchRel(pstate, crel, targetList,
											 &eqoList, out);

					qual = addQualNodeIn(pstate, qual, vertex, crel, edge,
										 false);
				}
				else
				{
					vertex = transformMatchNode(pstate, cnode, out, targetList,
												&eqoList);
					qual = addQualNodeIn(pstate, qual, vertex,
										 prev_crel, prev_edge, true);

					le = lnext(le);
					/* end of the path */
					if (le == NULL)
						break;

					crel = lfirst(le);
					setInitialVidForVLE(pstate, crel, vertex,
										prev_crel, prev_edge);
					edge = transformMatchRel(pstate, crel, targetList,
											 &eqoList, out);
					qual = addQualRelPath(pstate, qual,
										  prev_crel, prev_edge, crel, edge);
				}

				/* uniqueness */
				if (crel->varlen == NULL)
				{
					Node	   *eid;

					eid = getColumnVar(pstate, edge, AG_ELEM_LOCAL_ID);
					ueids = list_append_unique(ueids, eid);
				}
				else
				{
					Node	   *eidarr;

					eidarr = getColumnVar(pstate, edge, VLE_COLNAME_IDS);
					ueidarrs = list_append_unique(ueidarrs, eidarr);
				}

				if (out)
				{
					Assert(vertex != NULL);

					/*
					 * Starting vertex of ZeroLengthVLE is excluded from
					 * the graph path.
					 */
					if (!isZeroLengthVLE(crel))
					{
						pvs = vtxArrConcat(pstate, pvs,
										   makePathVertexExpr(pstate, vertex));
					}

					if (crel->varlen == NULL)
					{
						pes = edgeArrConcat(pstate, pes,
										makeEdgeExpr(pstate, crel, edge, -1));
					}
					else
					{
						pvs = vtxArrConcat(pstate, pvs,
										   getColumnVar(pstate, edge,
														VLE_COLNAME_VERTICES));
						pes = edgeArrConcat(pstate, pes,
											getColumnVar(pstate, edge,
														 VLE_COLNAME_EDGES));
					}
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
				pvs = vtxArrConcat(pstate, pvs,
								   makePathVertexExpr(pstate, vertex));

				graphpath = makeTypedRowExpr(list_make2(pvs, pes),
											 GRAPHPATHOID, pathloc);
				te = makeTargetEntry((Expr *) graphpath,
									 (AttrNumber) pstate->p_next_resno++,
									 pathname,
									 false);

				*targetList = lappend(*targetList, te);
			}
		}

		qual = addQualUniqueEdges(pstate, qual, ueids, ueidarrs);
	}

	/*
	 * Process all ElemQualOnly's at here because there are places that assume
	 * resjunk columns come after non-junk columns.
	 */
	foreach(leqo, eqoList)
	{
		ElemQualOnly *eqo = lfirst(leqo);
		TargetEntry *te = eqo->te;

		te->resno = (AttrNumber) pstate->p_next_resno++;
		addElemQual(pstate, te->resno, eqo->prop_map);
		*targetList = lappend(*targetList, te);
	}

	return qual;
}

static Node *
transformMatchNode(ParseState *pstate, CypherNode *cnode, bool force,
				   List **targetList, List **eqoList)
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
	Const	   *tid;
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
	 * If `cnode` is in a path, return RTE because the path must consist of
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
		r->inh = !cnode->only;
		alias = makeAliasOptUnique(varname);

		/* set `ihn` to true because we should scan all derived tables */
		rte = addRangeTableEntry(pstate, r, alias, r->inh, true);
		addRTEtoJoinlist(pstate, rte, false);

		if (varname != NULL || prop_constr)
		{
			bool		resjunk;
			int			resno;

			/*
			 * If `varname` is NULL, this target has to be ignored when
			 * `RETURN *`.
			 */
			resjunk = (varname == NULL);
			resno = (resjunk ? InvalidAttrNumber : pstate->p_next_resno++);

			te = makeTargetEntry((Expr *) makeVertexExpr(pstate, rte, varloc),
								 (AttrNumber) resno,
								 alias->aliasname,
								 resjunk);

			if (resjunk)
			{
				ElemQualOnly *eqo;

				eqo = palloc(sizeof(*eqo));
				eqo->te = te;
				eqo->prop_map = cnode->prop_map;

				*eqoList = lappend(*eqoList, eqo);
			}
			else
			{
				addElemQual(pstate, te->resno, cnode->prop_map);
				*targetList = lappend(*targetList, te);
			}
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
	tid = makeNullConst(TIDOID, -1, InvalidOid);
	vertex = makeTypedRowExpr(list_make3(id, prop_map, tid), VERTEXOID, varloc);
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
transformMatchRel(ParseState *pstate, CypherRel *crel, List **targetList,
				  List **eqoList, bool pathout)
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
		return transformMatchSR(pstate, crel, targetList, eqoList);
	else
		return transformMatchVLE(pstate, crel, targetList, pathout);
}

static RangeTblEntry *
transformMatchSR(ParseState *pstate, CypherRel *crel, List **targetList,
				 List **eqoList)
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
		rte = addEdgeUnion(pstate, typname, crel->only, typloc, alias);
	}
	else
	{
		RangeVar *r;

		r = makeRangeVar(get_graph_path(true), typname, typloc);
		r->inh = !crel->only;

		rte = addRangeTableEntry(pstate, r, alias, r->inh, true);
	}
	addRTEtoJoinlist(pstate, rte, false);

	if (varname != NULL || crel->prop_map != NULL)
	{
		bool		resjunk;
		int			resno;
		TargetEntry *te;

		resjunk = (varname == NULL);
		resno = (resjunk ? InvalidAttrNumber : pstate->p_next_resno++);

		te = makeTargetEntry((Expr *) makeEdgeExpr(pstate, crel, rte, varloc),
							 (AttrNumber) resno,
							 alias->aliasname,
							 resjunk);

		if (resjunk)
		{
			ElemQualOnly *eqo;

			eqo = palloc(sizeof(*eqo));
			eqo->te = te;
			eqo->prop_map = crel->prop_map;

			*eqoList = lappend(*eqoList, eqo);
		}
		else
		{
			addElemQual(pstate, te->resno, crel->prop_map);
			*targetList = lappend(*targetList, te);
		}
	}

	return rte;
}

static RangeTblEntry *
addEdgeUnion(ParseState *pstate, char *edge_label, bool only, int location,
			 Alias *alias)
{
	Node	   *u;
	Query	   *qry;
	RangeTblEntry *rte;

	AssertArg(alias != NULL);

	Assert(pstate->p_expr_kind == EXPR_KIND_NONE);
	pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

	u = genEdgeUnion(edge_label, only, location);
	qry = parse_sub_analyze(u, pstate, NULL,
							isLockedRefname(pstate, alias->aliasname), true);

	pstate->p_expr_kind = EXPR_KIND_NONE;

	rte = addRangeTableEntryForSubquery(pstate, qry, alias, false, true);

	return rte;
}

/*
 * SELECT id, start, "end", properties, ctid, start AS _start, "end" AS _end
 * FROM `get_graph_path()`.`edge_label`
 * UNION ALL
 * SELECT id, start, "end", properties, ctid, "end" AS _start, start AS _end
 * FROM `get_graph_path()`.`edge_label`
 */
static Node *
genEdgeUnion(char *edge_label, bool only, int location)
{
	ResTarget  *id;
	ResTarget  *start;
	ResTarget  *end;
	ResTarget  *prop_map;
	ResTarget  *tid;
	RangeVar   *r;
	SelectStmt *lsel;
	SelectStmt *rsel;
	SelectStmt *u;

	id = makeSimpleResTarget(AG_ELEM_LOCAL_ID, NULL);
	start = makeSimpleResTarget(AG_START_ID, NULL);
	end = makeSimpleResTarget(AG_END_ID, NULL);
	prop_map = makeSimpleResTarget(AG_ELEM_PROP_MAP, NULL);
	tid = makeSimpleResTarget("ctid", NULL);

	r = makeRangeVar(get_graph_path(true), edge_label, location);
	r->inh = !only;

	lsel = makeNode(SelectStmt);
	lsel->targetList = list_make5(id, start, end, prop_map, tid);
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
transformMatchVLE(ParseState *pstate, CypherRel *crel, List **targetList,
				  bool pathout)
{
	char	   *varname = getCypherName(crel->variable);
	bool		out = (varname != NULL || pathout);
	SelectStmt *sel;
	Alias	   *alias;
	RangeTblEntry *rte;

	sel = genVLESubselect(pstate, crel, out, pathout);

	alias = makeAliasOptUnique(varname);
	rte = transformVLEtoRTE(pstate, sel, alias);

	if (out)
	{
		TargetEntry *te;
		Node	   *var;
		bool		resjunk;
		int			resno;

		resjunk = (varname == NULL);
		resno = (resjunk ? InvalidAttrNumber : pstate->p_next_resno++);

		var = getColumnVar(pstate, rte, VLE_COLNAME_EDGES);
		te = makeTargetEntry((Expr *) var,
							 (AttrNumber) resno,
							 alias->aliasname,
							 resjunk);

		*targetList = lappend(*targetList, te);
	}

	if (pathout)
	{
		TargetEntry *te;
		Node	   *var;

		var = getColumnVar(pstate, rte, VLE_COLNAME_VERTICES);
		te = makeTargetEntry((Expr *) var,
							 InvalidAttrNumber,
							 genUniqueName(),
							 true);

		*targetList = lappend(*targetList, te);
	}

	return rte;
}

/*
 * CYPHER_REL_DIR_NONE
 *
 *     SELECT l._start, l._end, l.ids, l.edges, l.vertices,
 *            r.next, r.id, r.edge, r.vertex
 *     FROM `genVLELeftChild()` VLE JOIN LATERAL `genVLERightChild()` ON TRUE
 *
 * CYPHER_REL_DIR_LEFT
 *
 *     SELECT l.end, l.start, l.ids, l.edges, l.vertices,
 *            r.next, r.id, r.edge, r.vertex
 *     FROM `genVLELeftChild()` VLE JOIN LATERAL `genVLERightChild()` ON TRUE
 *
 * CYPHER_REL_DIR_RIGHT
 *
 *     SELECT l.start, l.end, l.ids, l.edges, l.vertices,
 *            r.next, r.id, r.edge, r.vertex
 *     FROM `genVLELeftChild()` VLE JOIN LATERAL `genVLERightChild()` ON TRUE
 *
 * NOTE: If the order of the result targets is changed,
 *       `XXX_VARNO` macro definitions in nodeNestloopVle.c
 *       must be synchronized with the changed order.
 */
static SelectStmt *
genVLESubselect(ParseState *pstate, CypherRel *crel, bool out, bool pathout)
{
	char	   *prev_colname;
	Node	   *prev_col;
	ResTarget  *prev;
	char	   *curr_colname;
	Node 	   *curr_col;
	ResTarget  *curr;
	Node	   *ids_col;
	ResTarget  *ids;
	Node       *next_col;
	ResTarget  *next;
	Node       *id_col;
	ResTarget  *id;
	List 	   *tlist;
	Node	   *left;
	Node	   *right;
	Node	   *join;
	SelectStmt *sel;

	prev_colname = getEdgeColname(crel, false);
	prev_col = makeColumnRef(genQualifiedName(VLE_LEFT_ALIAS, prev_colname));
	prev = makeResTarget(prev_col, prev_colname);

	curr_colname = getEdgeColname(crel, true);
	curr_col = makeColumnRef(genQualifiedName(VLE_LEFT_ALIAS, curr_colname));
	curr = makeResTarget(curr_col, curr_colname);

	ids_col = makeColumnRef(genQualifiedName(VLE_LEFT_ALIAS, VLE_COLNAME_IDS));
	ids = makeResTarget(ids_col, VLE_COLNAME_IDS);

	tlist = list_make3(prev, curr, ids);

	if (out)
	{
		Node	   *edges_col;
		ResTarget  *edges;

		edges_col = makeColumnRef(genQualifiedName(VLE_LEFT_ALIAS,
												   VLE_COLNAME_EDGES));
		edges = makeResTarget(edges_col, VLE_COLNAME_EDGES);

		tlist = lappend(tlist, edges);
	}

	if (pathout)
	{
		Node	   *vertices_col;
		ResTarget  *vertices;

		vertices_col = makeColumnRef(genQualifiedName(VLE_LEFT_ALIAS,
													  VLE_COLNAME_VERTICES));
		vertices = makeResTarget(vertices_col, VLE_COLNAME_VERTICES);

		tlist = lappend(tlist, vertices);
	}

	next_col = makeColumnRef(genQualifiedName(VLE_RIGHT_ALIAS,
											  VLE_COLNAME_NEXT));
	next = makeResTarget(next_col, VLE_COLNAME_NEXT);

	tlist = lappend(tlist, next);

	id_col = makeColumnRef(genQualifiedName(VLE_RIGHT_ALIAS, AG_ELEM_LOCAL_ID));
	id = makeResTarget(id_col, AG_ELEM_LOCAL_ID);

	tlist = lappend(tlist, id);

	if (out)
	{
		Node       *edge_col;
		ResTarget  *edge;

		edge_col = makeColumnRef(genQualifiedName(VLE_RIGHT_ALIAS,
												  VLE_COLNAME_EDGE));
		edge = makeResTarget(edge_col, VLE_COLNAME_EDGE);

		tlist = lappend(tlist, edge);
	}

	if (pathout)
	{
		Node       *vertex_col;
		ResTarget  *vertex;

		vertex_col = makeColumnRef(genQualifiedName(VLE_RIGHT_ALIAS,
													VLE_COLNAME_VERTEX));
		vertex = makeResTarget(vertex_col, VLE_COLNAME_VERTEX);

		tlist = lappend(tlist, vertex);
	}

	left = genVLELeftChild(pstate, crel, out, pathout);
	right = genVLERightChild(pstate, crel, out, pathout);

	join = genVLEJoinExpr(crel, left, right);

	sel = makeNode(SelectStmt);
	sel->targetList = tlist;
	sel->fromClause = list_make1(join);

	return sel;
}

/*
 * CYPHER_REL_DIR_NONE
 *
 *     SELECT _start, _end, ARRAY[id] AS ids,
 *            ARRAY[(id, start, "end", properties, ctid)::edge] AS edges
 *            ARRAY[NULL::vertex] AS vertices
 *     FROM <edge label with additional _start and _end columns> AS l
 *     WHERE <outer vid> = _start AND l.properties @> ...)
 *
 * CYPHER_REL_DIR_LEFT
 *
 *     SELECT "end", start, ARRAY[id] AS ids,
 *            ARRAY[(id, start, "end", properties, ctid)::edge] AS edges
 *            ARRAY[NULL::vertex] AS vertices
 *     FROM <edge label (and its children)> AS l
 *     WHERE <outer vid> = "end" AND l.properties @> ...)
 *
 * CYPHER_REL_DIR_RIGHT
 *
 *     SELECT start, "end", ARRAY[id] AS ids,
 *            ARRAY[(id, start, "end", properties, ctid)::edge] AS edges
 *            ARRAY[NULL::vertex] AS vertices
 *     FROM <edge label (and its children)> AS l
 *     WHERE <outer vid> = start AND l.properties @> ...)
 *
 * If `isZeroLengthVLE(crel)`, then
 *
 *     CYPHER_REL_DIR_NONE
 *
 *         VALUES (<outer vid>, <outer vid>, ARRAY[]::_graphid,
 *                 ARRAY[]::_edge, ARRAY[]::_vertex)
 *         AS l(_start, _end, ids, edges, vertices)
 *
 *     CYPHER_REL_DIR_LEFT
 *
 *         VALUES (<outer vid>, <outer vid>, ARRAY[]::_graphid,
 *                 ARRAY[]::_edge, ARRAY[]::_vertices)
 *         AS l("end", start, ids, edges, vertices)
 *
 *     CYPHER_REL_DIR_RIGHT
 *
 *         VALUES (<outer vid>, <outer vid>, ARRAY[]::_graphid,
 *                 ARRAY[]::_edge, ARRAY[]::_vertices)
 *         AS l(start, "end", ids, edges, vertices)
 */
static Node *
genVLELeftChild(ParseState *pstate, CypherRel *crel, bool out, bool pathout)
{
	Node	   *vid;
	A_ArrayExpr *idarr;
	A_ArrayExpr *edgearr;
	List	   *colnames = NIL;
	SelectStmt *sel;
	RangeSubselect *sub;

	/*
	 * `vid` is NULL only if
	 * (there is no previous edge of the vertex in the path
	 *  and the vertex is transformed first time in the pattern)
	 * and `crel` is not zero-length
	 */
	vid = pstate->p_vle_initial_vid;

	if (isZeroLengthVLE(crel))
	{
		TypeCast   *ids;
		List	   *values;

		Assert(vid != NULL);

		idarr = makeNode(A_ArrayExpr);
		idarr->location = -1;
		ids = makeNode(TypeCast);
		ids->arg = (Node *) idarr;
		ids->typeName = makeTypeName("_graphid");
		ids->location = -1;

		values = list_make3(vid, vid, ids);
		colnames = list_make3(makeString(getEdgeColname(crel, false)),
							  makeString(getEdgeColname(crel, true)),
							  makeString(VLE_COLNAME_IDS));

		if (out)
		{
			TypeCast *edges;

			edgearr = makeNode(A_ArrayExpr);
			edgearr->location = -1;
			edges = makeNode(TypeCast);
			edges->arg = (Node *) edgearr;
			edges->typeName = makeTypeName("_edge");
			edges->location = -1;

			values = lappend(values, edges);
			colnames = lappend(colnames, makeString(VLE_COLNAME_EDGES));
		}

		if (pathout)
		{
			TypeCast  *vertices;
			A_ArrayExpr *vtxarr;

			vtxarr = makeNode(A_ArrayExpr);
			vtxarr->location = -1;
			vertices = makeNode(TypeCast);
			vertices->arg = (Node*) vtxarr;
			vertices->typeName = makeTypeName("_vertex");
			vertices->location = -1;

			values = lappend(values, vertices);
			colnames = lappend(colnames, makeString(VLE_COLNAME_VERTICES));
		}

		sel = makeNode(SelectStmt);
		sel->valuesLists = list_make1(values);
	}
	else
	{
		List	   *prev_colname;
		Node	   *prev_col;
		ResTarget  *prev;
		ResTarget  *curr;
		Node	   *id;
		TypeCast   *cast;
		ResTarget  *ids;
		List	   *tlist = NIL;
		Node	   *from;
		List	   *where_args = NIL;

		prev_colname = genQualifiedName(NULL, getEdgeColname(crel, false));
		prev_col = makeColumnRef(prev_colname);
		prev = makeResTarget(prev_col, NULL);
		curr = makeSimpleResTarget(getEdgeColname(crel, true), NULL);

		id = makeColumnRef(genQualifiedName(NULL, AG_ELEM_LOCAL_ID));
		idarr = makeNode(A_ArrayExpr);
		idarr->elements = list_make1(id);
		idarr->location = -1;
		cast = makeNode(TypeCast);
		cast->arg = (Node *) idarr;
		cast->typeName = makeTypeName("_graphid");
		cast->location = -1;
		ids = makeResTarget((Node *) cast, VLE_COLNAME_IDS);

		tlist = list_make3(prev, curr, ids);

		from = genVLEEdgeSubselect(pstate, crel, VLE_LEFT_ALIAS);

		if (out)
		{
			ResTarget  *edges;

			edgearr = makeNode(A_ArrayExpr);
			edgearr->elements = list_make1(genEdgeSimple(VLE_LEFT_ALIAS));
			edgearr->location = -1;
			cast = makeNode(TypeCast);
			cast->arg = (Node *) edgearr;
			cast->typeName = makeTypeName("_edge");
			cast->location = -1;
			edges = makeResTarget((Node *) cast, VLE_COLNAME_EDGES);

			tlist = lappend(tlist, edges);
		}

		if (pathout)
		{
			ResTarget  *vertices;

			cast = makeNode(TypeCast);
			cast->arg = (Node*) makeNullAConst();
			cast->typeName = makeTypeName("_vertex");
			cast->location = -1;
			vertices = makeResTarget((Node *) cast, VLE_COLNAME_VERTICES);

			tlist = lappend(tlist, vertices);
		}

		if (vid != NULL)
		{
			A_Expr	   *vidcond;

			vidcond = makeSimpleA_Expr(AEXPR_OP, "=", vid, prev_col, -1);
			where_args = lappend(where_args, vidcond);
		}

		/* TODO: cannot see properties of future vertices */
		if (crel->prop_map != NULL)
			where_args = lappend(where_args, genVLEQual(VLE_LEFT_ALIAS,
														crel->prop_map));

		sel = makeNode(SelectStmt);
		sel->targetList = tlist;
		sel->fromClause = list_make1(from);
		sel->whereClause = (Node *) makeBoolExpr(AND_EXPR, where_args, -1);
	}

	sub = makeNode(RangeSubselect);
	sub->subquery = (Node *) sel;
	sub->alias = makeAliasNoDup(VLE_LEFT_ALIAS, colnames);

	return (Node *) sub;
}

/*
 * CYPHER_REL_DIR_NONE
 *
 *     SELECT l._end AS next, r.id,
 *            (r.id, r.start, r."end", r.properties, r.ctid)::edge AS edge,
 *            (vtx.id, vtx.properties, vtx.ctid)::vertex AS vertex
 *     FROM <edge label with additional _start and _end columns> AS r,
 *          <all vertex label> AS vtx
 *     WHERE l._end = r._start AND l._end = vtx.id AND r.properties @> ...)
 *
 * CYPHER_REL_DIR_LEFT
 *
 *     SELECT r.start AS next, r.id,
 *            (r.id, r.start, r."end", r.properties, r.ctid)::edge AS edge,
 *            (vtx.id, vtx.properties, vtx.ctid)::vertex AS vertex
 *     FROM <edge label (and its children)> AS r,
 *          <all vertex label> AS vtx
 *     WHERE l.start = r.end AND l.start = vtx.id AND r.properties @> ...)
 *
 * CYPHER_REL_DIR_RIGHT
 *
 *     SELECT r."end" AS next, r.id,
 *            (r.id, r.start, r."end", r.properties, r.ctid)::edge AS edge,
 *            (vtx.id, vtx.properties, vtx.ctid)::vertex AS vertex
 *     FROM <edge label (and its children)> AS r,
 *          <all vertex label> AS vtx
 *     WHERE l.end = r.start AND l.end = vtx.id AND r.properties @> ...)
 */
static Node *
genVLERightChild(ParseState *pstate, CypherRel *crel, bool out, bool pathout)
{
	Node	   *colref;
	ResTarget  *next;
	ResTarget  *id;
	List	   *tlist;
	List	   *from = NIL;
	ColumnRef  *prev;
	ColumnRef  *curr;
	A_Expr	   *joinqual;
	List	   *where_args = NIL;
	SelectStmt *sel;
	RangeSubselect *sub;

	colref = makeColumnRef(genQualifiedName(VLE_RIGHT_ALIAS,
											getEdgeColname(crel, true)));
	next = makeResTarget(colref, VLE_COLNAME_NEXT);

	colref = makeColumnRef(genQualifiedName(VLE_RIGHT_ALIAS, AG_ELEM_LOCAL_ID));
	id = makeResTarget(colref, AG_ELEM_LOCAL_ID);
	tlist = list_make2(next, id);

	if (out)
	{
		ResTarget  *edge;

		edge = makeResTarget(genEdgeSimple(VLE_RIGHT_ALIAS), VLE_COLNAME_EDGE);
		tlist = lappend(tlist, edge);
	}

	from = lappend(from, genVLEEdgeSubselect(pstate, crel, VLE_RIGHT_ALIAS));

	prev = makeNode(ColumnRef);
	prev->fields = genQualifiedName(VLE_LEFT_ALIAS,
									getEdgeColname(crel, true));
	prev->location = -1;
	curr = makeNode(ColumnRef);
	curr->fields = genQualifiedName(VLE_RIGHT_ALIAS,
									getEdgeColname(crel, false));
	curr->location = -1;
	joinqual = makeSimpleA_Expr(AEXPR_OP, "=", (Node *) prev, (Node *) curr,
								-1);
	where_args = lappend(where_args, joinqual);
	if (crel->prop_map != NULL)
		where_args = lappend(where_args, genVLEQual(VLE_RIGHT_ALIAS,
													crel->prop_map));

	if (pathout)
	{
		RangeVar   *vtx;
		ResTarget  *vertex;
		ColumnRef  *vtxid;

		vtx = makeRangeVar(get_graph_path(true), AG_VERTEX, -1);
		vtx->alias = makeAliasNoDup(VLE_VERTEX_ALIAS, NULL);
		from = lappend(from, vtx);

		vertex = makeResTarget(genVertexSimple(VLE_VERTEX_ALIAS),
							   VLE_COLNAME_VERTEX);
		tlist = lappend(tlist, vertex);

		vtxid = makeNode(ColumnRef);
		vtxid->fields = genQualifiedName(VLE_VERTEX_ALIAS, AG_ELEM_ID);
		vtxid->location = -1;

		joinqual = makeSimpleA_Expr(AEXPR_OP, "=",
									(Node *) vtxid, (Node *) curr, -1);

		where_args = lappend(where_args, joinqual);
	}

	sel = makeNode(SelectStmt);
	sel->targetList = tlist;
	sel->fromClause = from;
	sel->whereClause = (Node *) makeBoolExpr(AND_EXPR, where_args, -1);

	sub = makeNode(RangeSubselect);
	sub->subquery = (Node *) sel;
	sub->alias = makeAliasNoDup(VLE_RIGHT_ALIAS, NULL);
	sub->lateral = true;

	return (Node *) sub;
}

static Node *
genVertexSimple(char *aliasname)
{
	Node	   *id;
	Node	   *prop_map;
	Node	   *tid;
	RowExpr	   *row;
	TypeCast   *vertex;

	id = makeColumnRef(genQualifiedName(aliasname, AG_ELEM_LOCAL_ID));
	prop_map = makeColumnRef(genQualifiedName(aliasname, AG_ELEM_PROP_MAP));
	tid = makeColumnRef(genQualifiedName(aliasname, "ctid"));

	row = makeNode(RowExpr);
	row->args = list_make3(id, prop_map, tid);
	row->row_format = COERCE_IMPLICIT_CAST;
	row->location = -1;

	vertex = makeNode(TypeCast);
	vertex->arg = (Node *) row;
	vertex->typeName = makeTypeName("vertex");
	vertex->location = -1;

	return (Node *) vertex;
}

static Node *
genEdgeSimple(char *aliasname)
{
	Node	   *id;
	Node	   *start;
	Node	   *end;
	Node	   *prop_map;
	Node	   *tid;
	RowExpr	   *row;
	TypeCast   *edge;

	id = makeColumnRef(genQualifiedName(aliasname, AG_ELEM_LOCAL_ID));
	start = makeColumnRef(genQualifiedName(aliasname, AG_START_ID));
	end = makeColumnRef(genQualifiedName(aliasname, AG_END_ID));
	prop_map = makeColumnRef(genQualifiedName(aliasname, AG_ELEM_PROP_MAP));
	tid = makeColumnRef(genQualifiedName(aliasname, "ctid"));

	row = makeNode(RowExpr);
	row->args = list_make5(id, start, end, prop_map, tid);
	row->row_format = COERCE_IMPLICIT_CAST;
	row->location = -1;

	edge = makeNode(TypeCast);
	edge->arg = (Node *) row;
	edge->typeName = makeTypeName("edge");
	edge->location = -1;

	return (Node *) edge;
}

static Node *
genVLEEdgeSubselect(ParseState *pstate, CypherRel *crel, char *aliasname)
{
	char	   *typname;
	Alias	   *alias;
	Node	   *edge;

	getCypherRelType(crel, &typname, NULL);
	alias = makeAliasNoDup(aliasname, NIL);

	if (crel->direction == CYPHER_REL_DIR_NONE)
	{
		RangeSubselect *sub;

		/* id, start, "end", properties, ctid, _start, _end */
		sub = makeNode(RangeSubselect);
		sub->subquery = genEdgeUnion(typname, crel->only, -1);
		sub->alias = alias;
		edge = (Node *) sub;
	}
	else
	{
		RangeVar   *r;
		LOCKMODE	lockmode;
		Relation	rel;

		r = makeRangeVar(get_graph_path(true), typname, -1);
		r->inh = !crel->only;

		if (isLockedRefname(pstate, aliasname))
			lockmode = RowShareLock;
		else
			lockmode = AccessShareLock;

		rel = parserOpenTable(pstate, r, lockmode);

		/* id, start, "end", properties, ctid */
		if (!crel->only && has_subclass(rel->rd_id))
		{
			RangeSubselect *sub;

			r->inh = false;
			sub = genInhEdge(r, rel->rd_id);
			sub->alias = alias;
			edge = (Node *) sub;
		}
		else
		{
			r->alias = alias;
			edge = (Node *) r;
		}

		heap_close(rel, NoLock);
	}

	return edge;
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
	CypherGenericExpr *cexpr;
	A_Expr	   *propcond;

	prop = makeNode(ColumnRef);
	prop->fields = genQualifiedName(alias, AG_ELEM_PROP_MAP);
	prop->location = -1;

	cexpr = makeNode(CypherGenericExpr);
	cexpr->expr = propMap;

	propcond = makeSimpleA_Expr(AEXPR_OP, "@>", (Node *) prop, (Node *) cexpr,
								-1);

	return (Node *) propcond;
}

/*
 * UNION ALL the relation whose OID is `parentoid` and its child relations.
 *
 * SELECT id, start, "end", properties, ctid FROM `r`
 * UNION ALL
 * SELECT id, start, "end", properties, ctid FROM edge
 * ...
 */
static RangeSubselect *
genInhEdge(RangeVar *r, Oid parentoid)
{
	ResTarget  *id;
	ResTarget  *start;
	ResTarget  *end;
	ResTarget  *prop_map;
	ResTarget  *tid;
	SelectStmt *sel;
	SelectStmt *lsel;
	List	   *children;
	ListCell   *lc;
	RangeSubselect *sub;

	id = makeSimpleResTarget(AG_ELEM_LOCAL_ID, NULL);
	start = makeSimpleResTarget(AG_START_ID, NULL);
	end = makeSimpleResTarget(AG_END_ID, NULL);
	prop_map = makeSimpleResTarget(AG_ELEM_PROP_MAP, NULL);
	tid = makeSimpleResTarget("ctid", NULL);

	sel = makeNode(SelectStmt);
	sel->targetList = list_make5(id, start, end, prop_map, tid);
	sel->fromClause = list_make1(r);
	lsel = sel;

	children = find_inheritance_children(parentoid, AccessShareLock);
	foreach(lc, children)
	{
		Oid			childoid = lfirst_oid(lc);
		Relation	childrel;
		RangeVar   *childrv;
		SelectStmt *rsel;
		SelectStmt *u;

		childrel = heap_open(childoid, AccessShareLock);

		childrv = makeRangeVar(get_graph_path(true),
							   RelationGetRelationName(childrel),
							   -1);
		childrv->inh = true;

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

	qry = parse_sub_analyze((Node *) vle, pstate, NULL,
							isLockedRefname(pstate, alias->aliasname), true);
	Assert(qry->commandType == CMD_SELECT);

	pstate->p_lateral_active = false;
	pstate->p_expr_kind = EXPR_KIND_NONE;

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
										prev_vid, vid, pstate->p_last_srf, -1));

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
										id, vid, pstate->p_last_srf, -1));

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

static Node *
vtxArrConcat(ParseState *pstate, Node *array, Node *elem)
{
	Oid		elemtype = exprType(elem);

	if (elemtype != VERTEXOID && elemtype != VERTEXARRAYOID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("expression must be a vertex, but %s",
						format_type_be(elemtype))));
	}

	if (array == NULL)
		return makeArrayExpr(VERTEXARRAYOID, VERTEXOID, list_make1(elem));

	if (exprType(array) != VERTEXARRAYOID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("expression must be an array of vertex, but %s",
						format_type_be(exprType(array)))));
	}

	return (Node *) make_op(pstate, list_make1(makeString("||")), array,
							elem, pstate->p_last_srf, -1);
}


static Node *
edgeArrConcat(ParseState *pstate, Node *array, Node *elem)
{
	Oid		elemtype = exprType(elem);

	if (elemtype != EDGEOID && elemtype != EDGEARRAYOID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("expression must be an edge, but %s",
						format_type_be(elemtype))));
	}

	if (array == NULL)
		return makeArrayExpr(EDGEARRAYOID, EDGEOID, list_make1(elem));

	if (exprType(array) != EDGEARRAYOID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("expression must be an array of edge, but %s",
						format_type_be(exprType(array)))));
	}

	return (Node *) make_op(pstate, list_make1(makeString("||")), array,
							elem, pstate->p_last_srf, -1);
}

static Node *
addQualUniqueEdges(ParseState *pstate, Node *qual, List *ueids, List *ueidarrs)
{
	FuncCall   *arrpos;
	ListCell   *le1;
	ListCell   *lea1;
	FuncCall   *arroverlap;

	arrpos = makeFuncCall(list_make1(makeString("array_position")), NIL, -1);

	foreach(le1, ueids)
	{
		Node	   *eid1 = lfirst(le1);
		ListCell   *le2;

		for_each_cell(le2, lnext(le1))
		{
			Node	   *eid2 = lfirst(le2);
			Expr	   *ne;

			ne = make_op(pstate, list_make1(makeString("<>")), eid1, eid2,
						 pstate->p_last_srf, -1);

			qual = qualAndExpr(qual, (Node *) ne);
		}

		foreach(lea1, ueidarrs)
		{
			Node	   *eidarr = lfirst(lea1);
			Node	   *arg;
			NullTest   *dupcond;

			arg = ParseFuncOrColumn(pstate, arrpos->funcname,
									list_make2(eidarr, eid1),
									pstate->p_last_srf, arrpos, -1);

			dupcond = makeNode(NullTest);
			dupcond->arg = (Expr *) arg;
			dupcond->nulltesttype = IS_NULL;
			dupcond->argisrow = false;
			dupcond->location = -1;

			qual = qualAndExpr(qual, (Node *) dupcond);
		}
	}

	arroverlap = makeFuncCall(list_make1(makeString("arrayoverlap")), NIL, -1);

	foreach(lea1, ueidarrs)
	{
		Node	   *eidarr1 = lfirst(lea1);
		ListCell   *lea2;

		for_each_cell(lea2, lnext(lea1))
		{
			Node	   *eidarr2 = lfirst(lea2);
			Node	   *funcexpr;
			Node	   *dupcond;

			funcexpr = ParseFuncOrColumn(pstate, arroverlap->funcname,
										 list_make2(eidarr1, eidarr2),
										 pstate->p_last_srf, arroverlap, -1);

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
		TargetEntry *te;
		Oid			type;
		int32		typmod;
		Oid			collid;
		Var		   *var;
		Node	   *prop_map;
		bool		is_cyphermap;

		rte = GetRTEByRangeTablePosn(pstate, eq->varno, 0);
		/* don't use make_var() because `te` can be resjunk */
		te = get_tle_by_resno(rte->subquery->targetList, eq->varattno);
		type = exprType((Node *) te->expr);
		typmod = exprTypmod((Node *) te->expr);
		collid = exprCollation((Node *) te->expr);
		var = makeVar(eq->varno, eq->varattno, type, typmod, collid, 0);
		var->location = -1;
		/* skip markVarForSelectPriv() because `rte` is RTE_SUBQUERY */

		prop_map = getExprField((Expr *) var, AG_ELEM_PROP_MAP);

		is_cyphermap = IsA(eq->prop_constr, CypherMapExpr);

		if (is_cyphermap)
			qual = transform_prop_constr(pstate, qual, prop_map,
										 eq->prop_constr);

		if ((is_cyphermap && ginAvail(pstate, eq->varno, eq->varattno)) ||
			!is_cyphermap)
		{
			Node	   *prop_constr;
			Expr	   *expr;

			prop_constr = transformPropMap(pstate, eq->prop_constr,
										   EXPR_KIND_WHERE);
			expr = make_op(pstate, list_make1(makeString("@>")),
						   prop_map, prop_constr, pstate->p_last_srf, -1);

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
	CypherMapExpr *m = (CypherMapExpr *) node;
	ListCell   *le;

	le = list_head(m->keyvals);
	while (le != NULL)
	{
		Node	   *k;
		Node	   *v;
		Const	   *pathelem;
		ListCell   *prev;

		k = lfirst(le);
		le = lnext(le);
		v = lfirst(le);
		le = lnext(le);

		Assert(IsA(k, String));
		pathelem = makeConst(TEXTOID, -1, DEFAULT_COLLATION_OID, -1,
							 CStringGetTextDatum(strVal(k)), false, false);

		prev = list_tail(ctx->pathelems);
		ctx->pathelems = lappend(ctx->pathelems, pathelem);

		if (IsA(v, CypherMapExpr))
		{
			transform_prop_constr_worker(v, ctx);
		}
		else
		{
			CypherAccessExpr *a;
			Node	   *lval;
			Node	   *rval;
			Oid			rvaltype;
			int			rvalloc;
			Expr	   *expr;

			a = makeNode(CypherAccessExpr);
			a->arg = (Expr *) ctx->prop_map;
			a->path = copyObject(ctx->pathelems);
			lval = (Node *) a;

			rval = transformCypherExpr(ctx->pstate, v, EXPR_KIND_WHERE);
			rvaltype = exprType(rval);
			rvalloc = exprLocation(rval);
			rval = coerce_to_target_type(ctx->pstate, rval, rvaltype, JSONBOID,
										 -1, COERCION_ASSIGNMENT,
										 COERCE_IMPLICIT_CAST, -1);
			if (rval == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("expression must be of type jsonb but %s",
								format_type_be(rvaltype)),
						 parser_errposition(ctx->pstate, rvalloc)));

			expr = make_op(ctx->pstate, list_make1(makeString("=")),
						   lval, rval, ctx->pstate->p_last_srf, -1);

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

		newvar = castNode(Var, copyObject(fv->expr));
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
						   pstate->p_last_srf, sel_id, -1);

	qual = (Node *) make_op(pstate, list_make1(makeString("=")), fv_id, id,
							pstate->p_last_srf, -1);

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
transformCreatePattern(ParseState *pstate, CypherPath *cpath, List **targetList)
{
	List	   *graphPattern = NIL;
	char	   *pathname = getCypherName(cpath->variable);
	int			pathloc = getCypherNameLoc(cpath->variable);
	List	   *gchain = NIL;
	GraphPath  *gpath;
	ListCell   *le;

	if (findTarget(*targetList, pathname) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_ALIAS),
				 errmsg("duplicate variable \"%s\"", pathname),
				 parser_errposition(pstate, pathloc)));

	foreach(le, cpath->chain)
	{
		Node *elem = lfirst(le);

		if (IsA(elem, CypherNode))
		{
			CypherNode *cnode = (CypherNode *) elem;
			GraphVertex *gvertex;

			gvertex = transformCreateNode(pstate, cnode, targetList);

			if (!gvertex->create && list_length(cpath->chain) <= 1)
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
			int			labloc = getCypherNameLoc(cnode->label);

			if (strcmp(labname, AG_VERTEX) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("specifying default label is not allowed"),
						 parser_errposition(pstate, labloc)));

			createVertexLabelIfNotExist(pstate, labname, labloc);
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

	if (strcmp(typname, AG_EDGE) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("cannot create edge on default label"),
				 parser_errposition(pstate, getCypherNameLoc(type))));

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
transformSetPropList(ParseState *pstate, RangeTblEntry *rte, bool is_remove,
					 CSetKind kind, List *items)
{
	List	   *gsplist = NIL;
	ListCell   *li;

	foreach(li, items)
	{
		CypherSetProp *sp = lfirst(li);
		GraphSetProp *gsp;

		gsp = transformSetProp(pstate, rte, sp, is_remove, kind, gsplist);

		if (gsp != NULL)
			gsplist = lappend(gsplist, gsp);
	}

	return gsplist;
}

static GraphSetProp *
transformSetProp(ParseState *pstate, RangeTblEntry *rte, CypherSetProp *sp,
				 bool is_remove, CSetKind kind, List *gsplist)
{
	Node	   *elem;
	List	   *pathelems;
	Node	   *path = NULL;
	char	   *varname;
	GraphSetProp *gsp;
	Node	   *prop_map;
	Node	   *expr;
	Oid			exprtype;
	GSPKind		gspkind;

	elem = transformCypherMapForSet(pstate, sp->prop, &pathelems, &varname);
	if (pathelems != NIL)
		path = makeArrayExpr(TEXTARRAYOID, TEXTOID, pathelems);

	/*
	 * find the previously processed element with `varname`
	 * to merge property assignments into one expression
	 */
	gsp = findGraphSetProp(gsplist, varname);
	if (gsp == NULL)
	{
		/*
		 * It is the first time to handle the element.
		 * Get the original property map of the element.
		 */
		prop_map = ParseFuncOrColumn(pstate,
									 list_make1(makeString(AG_ELEM_PROP_MAP)),
									 list_make1(elem), pstate->p_last_srf,
									 NULL, -1);
	}
	else
	{
		/* use previously modified property map */
		prop_map = gsp->expr;
	}

	/*
	 * Transform the assigned property to get `expr` (RHS of the SET clause
	 * item). `sp->expr` can be a null constant if this `sp` is for REMOVE.
	 */
	expr = transformCypherExpr(pstate, sp->expr, EXPR_KIND_UPDATE_SOURCE);
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

	/*
	 * make the modified property map
	 */

	if (path == NULL)	/* LHS is the property map itself */
	{
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
										 list_make2(prop_map, expr),
										 pstate->p_last_srf, concat, -1);
		}
		else
		{
			/* just overwrite the property map */
			prop_map = expr;
		}
	}
	else				/* LHS is a property in the property map */
	{
		FuncCall   *delete;
		Node	   *del_prop;

		if (sp->add)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("+= operator on a property is not allowed"),
					 parser_errposition(pstate, exprLocation(elem))));

		delete = makeFuncCall(list_make1(makeString("jsonb_delete_path")),
							  NIL, -1);
		del_prop = ParseFuncOrColumn(pstate, delete->funcname,
									 list_make2(prop_map, path),
									 pstate->p_last_srf, delete, -1);

		if (IsNullAConst(sp->expr) && (!allow_null_properties || is_remove))
		{
			/* SET a.b.c = NULL */
			prop_map = del_prop;
		}
		else							/* SET a.b.c = expr */
		{
			FuncCall   *set;
			Node	   *set_prop;
			CoalesceExpr *coalesce;

			/*
			 * UNKNOWNOID 'null' will be passed to jsonb_in()
			 * when ParseFuncOrColumn()
			 */
			if (IsNullAConst(sp->expr))
				expr = (Node *) makeConst(UNKNOWNOID, -1, InvalidOid, -2,
						  CStringGetDatum("null"), false, false);

			set = makeFuncCall(list_make1(makeString("jsonb_set")), NIL, -1);
			set_prop = ParseFuncOrColumn(pstate, set->funcname,
										 list_make3(prop_map, path, expr),
										 pstate->p_last_srf, set, -1);

			/*
			 * The right operand can be null. In this case,
			 * it behaves like REMOVE clause.
			 */
			coalesce = makeNode(CoalesceExpr);
			coalesce->args = list_make2(set_prop, del_prop);
			coalesce->coalescetype = JSONBOID;
			coalesce->location = -1;

			prop_map = (Node *) coalesce;
		}
	}

	/*
	 * set the modified property map
	 */

	if (!allow_null_properties)
		prop_map = stripNullKeys(pstate, prop_map);

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
		gsp->elem = elem;
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

static Query *
transformMergeMatch(ParseState *pstate, Node *parseTree)
{
	CypherClause *clause = (CypherClause *) parseTree;
	Query	   *qry;
	RangeTblEntry *rte;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	rte = transformMergeMatchJoin(pstate, clause);

	qry->targetList = makeTargetListFromJoin(pstate, rte);
	markTargetListOrigins(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, pstate->p_resolved_qual);

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
								transformStmt, r_alias);

	pstate->p_lateral_active = false;

	qual = makeBoolConst(true, false);
	alias = makeAliasNoDup(CYPHER_SUBQUERY_ALIAS, NIL);

	return incrementalJoinRTEs(pstate, JOIN_CYPHER_MERGE, l_rte, r_rte, qual,
							   alias);
}

static RangeTblEntry *
transformNullSelect(ParseState *pstate)
{
	ResTarget  *nullres;
	SelectStmt *sel;
	Alias	   *alias;
	Query	   *qry;
	RangeTblEntry *rte;

	nullres = makeResTarget((Node *) makeNullAConst(), NULL);

	sel = makeNode(SelectStmt);
	sel->targetList = list_make1(nullres);

	alias = makeAliasNoDup(CYPHER_SUBQUERY_ALIAS, NIL);

	Assert(pstate->p_expr_kind == EXPR_KIND_NONE);
	pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

	qry = parse_sub_analyze((Node *) sel, pstate, NULL,
							isLockedRefname(pstate, alias->aliasname), true);

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
		int			labloc = getCypherNameLoc(cnode->label);

		if (strcmp(labname, AG_VERTEX) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("specifying default label is not allowed"),
					 parser_errposition(pstate, labloc)));

		createVertexLabelIfNotExist(pstate, labname, labloc);
	}

	relation = openTargetLabel(pstate, labname);

	vertex = makeNewVertex(pstate, relation, cnode->prop_map);
	relid = RelationGetRelid(relation);

	heap_close(relation, NoLock);

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

	if (strcmp(typname, AG_EDGE) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("cannot create edge on default label"),
				 parser_errposition(pstate, getCypherNameLoc(type))));

	createEdgeLabelIfNotExist(pstate, typname, getCypherNameLoc(type));

	relation = openTargetLabel(pstate, getCypherName(linitial(crel->types)));

	edge = makeNewEdge(pstate, relation, crel->prop_map);
	relid = RelationGetRelid(relation);

	heap_close(relation, NoLock);

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

		Assert(!detail->is_remove);

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

	l_oncreate = transformSetPropList(pstate, rte, false, CSET_ON_CREATE,
									  l_oncreate);
	l_onmatch = transformSetPropList(pstate, rte, false, CSET_ON_MATCH,
									 l_onmatch);

	return list_concat(l_onmatch, l_oncreate);
}

static Query *
transformDeleteJoin(ParseState *pstate, Node *parseTree)
{
	CypherClause *clause = (CypherClause *) parseTree;
	Query	   *qry;
	RangeTblEntry *rte;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	rte = transformDeleteJoinRTE(pstate, clause);
	if (rte->rtekind == RTE_JOIN)
		qry->targetList = makeTargetListFromJoin(pstate, rte);
	else if (rte->rtekind == RTE_SUBQUERY)
		qry->targetList = makeTargetListFromRTE(pstate, rte);
	else
		elog(ERROR, "unexpected rtekind(%d) in DELETE", rte->rtekind);

	markTargetListOrigins(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, pstate->p_resolved_qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;

	assign_query_collations(pstate, qry);

	return qry;
}

static Query *
transformDeleteEdges(ParseState *pstate, Node *parseTree)
{
	CypherClause	   *clause = (CypherClause *) parseTree;
	CypherDeleteClause *detail = (CypherDeleteClause *) clause->detail;
	RangeTblEntry  *rte;
	Query	   *qry;
	List	   *edges = NIL;

	rte = transformClause(pstate, clause->prev);

	edges = extractEdgesExpr(pstate, detail->exprs, EXPR_KIND_OTHER);

	if (!edges)
	{
		qry = makeNode(Query);
		qry->commandType = CMD_SELECT;

		qry->targetList = makeTargetListFromRTE(pstate, rte);
		markTargetListOrigins(pstate, qry->targetList);

		qry->rtable = pstate->p_rtable;
		qry->jointree = makeFromExpr(pstate->p_joinlist,
									pstate->p_resolved_qual);

		qry->hasSubLinks = pstate->p_hasSubLinks;

		assign_query_collations(pstate, qry);

		return qry;
	}

	qry = makeNode(Query);
	qry->commandType = CMD_GRAPHWRITE;
	qry->graph.writeOp = GWROP_DELETE;
	qry->graph.last = false;
	qry->graph.detach = false;
	qry->graph.eager = true;

	qry->targetList = makeTargetListFromRTE(pstate, rte);

	qry->graph.exprs = edges;
	qry->graph.nr_modify = pstate->p_nr_modify_clause++;

	markTargetListOrigins(pstate, qry->targetList);

	qry->targetList = (List *) resolve_future_vertex(pstate,
													 (Node *) qry->targetList,
													 FVR_DONT_RESOLVE);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, pstate->p_resolved_qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;

	assign_query_collations(pstate, qry);

	findAllModifiedLabels(qry);

	return qry;
}

/* See transformMatchOptional() */
static RangeTblEntry *
transformDeleteJoinRTE(ParseState *pstate, CypherClause *clause)
{
	CypherDeleteClause *detail = (CypherDeleteClause *) clause->detail;
	RangeTblEntry *l_rte;
	A_ArrayExpr *vertices_var = NULL;
	Node	   *vertices_nodes = NULL;
	Node	   *vertices;
	List	   *exprs;
	ListCell   *le;
	ListCell   *lp;
	char	   *edges_resname = NULL;
	Node	   *sel_ag_edge;
	Alias	   *r_alias;
	Query	   *r_qry;
	RangeTblEntry *r_rte;
	Node	   *qual;
	RangeTblEntry *jrte;

	/*
	 * Since targets of a DELETE clause refers the result of the previous
	 * clause, it must be transformed first.
	 */
	l_rte = transformClauseBy(pstate, (Node *) clause, transformDeleteEdges);

	/* FIXME: `detail->exprs` is transformed twice */
	exprs = transformCypherExprList(pstate, detail->exprs, EXPR_KIND_OTHER);
	forboth(le, exprs, lp, detail->exprs)
	{
		Node	   *expr = lfirst(le);
		Node	   *pexpr = lfirst(lp);
		Oid			vartype;

		if (!IsA(pexpr, ColumnRef))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("only direct variable reference is supported"),
					 parser_errposition(pstate, exprLocation(expr))));

		vartype = exprType(expr);
		if (vartype == VERTEXOID)
		{
			vertices_var = verticesAppend(vertices_var, pexpr);
		}
		else if (vartype == EDGEOID)
		{
			/* do nothing */
		}
		else if (vartype == GRAPHPATHOID)
		{
			FuncCall   *nodes;

			nodes = makeFuncCall(list_make1(makeString("nodes")),
								 list_make1(pexpr), -1);

			vertices_nodes = verticesConcat(vertices_nodes, (Node *) nodes);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("node, relationship, or path is expected"),
					 parser_errposition(pstate, exprLocation(expr))));
		}

		/*
		 * TODO: `expr` must contain one of the target variables
		 *		 and it mustn't contain aggregate and SubLink's.
		 */
	}

	vertices = verticesConcat((Node *) vertices_var, vertices_nodes);
	if (vertices == NULL)
		return l_rte;

	sel_ag_edge = makeSelectEdgesVertices(vertices, detail, &edges_resname);
	r_alias = makeAliasNoDup(CYPHER_DELETEJOIN_ALIAS, NIL);

	pstate->p_lateral_active = true;
	Assert(pstate->p_expr_kind == EXPR_KIND_NONE);
	pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

	r_qry = parse_sub_analyze(sel_ag_edge, pstate, NULL,
							  isLockedRefname(pstate, r_alias->aliasname),
							  true);
	Assert(r_qry->commandType == CMD_SELECT);

	/*
	 * 'edge' variable is only used to determine if there is an edge
	 * connected to the vertex.
	 */
	if (!detail->detach)
	{
		TargetEntry *edge;

		Assert(list_length(r_qry->targetList) == 1);
		edge = linitial_node(TargetEntry, r_qry->targetList);
		edge->resjunk = true;
	}

	pstate->p_lateral_active = false;
	pstate->p_expr_kind = EXPR_KIND_NONE;

	r_rte = addRangeTableEntryForSubquery(pstate, r_qry, r_alias, true, true);

	qual = makeBoolConst(true, false);

	jrte = incrementalJoinRTEs(pstate, JOIN_CYPHER_DELETE, l_rte, r_rte, qual,
							   makeAliasNoDup(CYPHER_SUBQUERY_ALIAS, NIL));

	pstate->p_delete_edges_resname = edges_resname;
	return jrte;
}

static A_ArrayExpr *
verticesAppend(A_ArrayExpr *vertices, Node *expr)
{
	if (vertices == NULL)
	{
		vertices = makeNode(A_ArrayExpr);
		vertices->elements = list_make1(expr);
		vertices->location = -1;
	}
	else
	{
		vertices->elements = lappend(vertices->elements, expr);
	}

	return vertices;
}

static Node *
verticesConcat(Node *vertices, Node *expr)
{
	FuncCall   *arrcat;

	if (vertices == NULL)
		return expr;
	if (expr == NULL)
		return vertices;

	arrcat = makeFuncCall(list_make1(makeString("array_cat")),
						  list_make2(vertices, expr), -1);

	return (Node *) arrcat;
}

/*
 * if DETACH
 *
 *     SELECT array_agg((id, NULL, NULL, NULL, ctid)::edge) AS <unique-name>
 *     FROM ag_edge AS e, unnest(vertices) AS v
 *     WHERE e.start = v.id OR e.end = v.id
 *
 * else
 *
 *     SELECT NULL::edge
 *     FROM ag_edge AS e, unnest(vertices) AS v
 *     WHERE e.start = v.id OR e.end = v.id
 */
static Node *
makeSelectEdgesVertices(Node *vertices, CypherDeleteClause *delete,
						char **edges_resname)
{
	List	   *targetlist = NIL;
	RangeVar   *ag_edge;
	RangeFunction *unnest;
	SelectStmt *sel;

	AssertArg(vertices != NULL);

	if (delete->detach)
	{
		Node	   *edges;
		char	   *edges_name;
		Node	   *edges_col;

		edges = makeEdgesForDetach();
		edges_name = genUniqueName();
		targetlist = list_make1(makeResTarget(edges, edges_name));

		/* add delete target */
		edges_col = makeColumnRef(genQualifiedName(NULL, edges_name));
		delete->exprs = lappend(delete->exprs, edges_col);

		*edges_resname = edges_name;
	}
	else
	{
		TypeCast   *nulledge;

		nulledge = makeNode(TypeCast);
		nulledge->arg = (Node *) makeNullAConst();
		nulledge->typeName = makeTypeName("edge");
		nulledge->location = -1;

		targetlist = list_make1(makeResTarget((Node *) nulledge, NULL));
	}

	ag_edge = makeRangeVar(get_graph_path(true), AG_EDGE, -1);
	ag_edge->inh = true;
	ag_edge->alias = makeAliasNoDup(DELETE_EDGE_ALIAS, NIL);

	unnest = makeUnnestVertices(vertices);

	sel = makeNode(SelectStmt);
	sel->targetList = targetlist;
	sel->fromClause = list_make2(ag_edge, unnest);
	sel->whereClause = (Node *) makeEdgesVertexQual();

	return (Node *) sel;
}

/* array_agg((id, NULL, NULL, NULL, ctid)::edge) */
static Node *
makeEdgesForDetach(void)
{
	Node	   *id;
	A_Const	   *start;
	A_Const	   *end;
	A_Const	   *prop_map;
	Node	   *tid;
	RowExpr	   *edgerow;
	TypeCast   *edge;
	FuncCall   *edges;

	id = makeColumnRef(genQualifiedName(DELETE_EDGE_ALIAS, AG_ELEM_ID));
	start = makeNullAConst();
	end = makeNullAConst();
	prop_map = makeNullAConst();
	tid = makeColumnRef(genQualifiedName(DELETE_EDGE_ALIAS, "ctid"));

	edgerow = makeNode(RowExpr);
	edgerow->args = list_make5(id, start, end, prop_map, tid);
	edgerow->row_typeid = InvalidOid;
	edgerow->colnames = NIL;
	edgerow->row_format = COERCE_IMPLICIT_CAST;
	edgerow->location = -1;

	edge = makeNode(TypeCast);
	edge->arg = (Node *) edgerow;
	edge->typeName = makeTypeName("edge");
	edge->location = -1;

	edges = makeFuncCall(list_make1(makeString("array_agg")), list_make1(edge),
						 -1);

	return (Node *) edges;
}

static RangeFunction *
makeUnnestVertices(Node *vertices)
{
	FuncCall   *unnest;
	RangeFunction *rf;

	unnest = makeFuncCall(list_make1(makeString("unnest")),
						  list_make1(vertices), -1);

	rf = makeNode(RangeFunction);
	rf->lateral = false;
	rf->ordinality = false;
	rf->is_rowsfrom = false;
	rf->functions = list_make1(list_make2(unnest, NIL));
	rf->alias = makeAliasNoDup(DELETE_VERTEX_ALIAS, NIL);
	rf->coldeflist = NIL;

	return rf;
}

/* e.start = v.id OR e.end = v.id */
static BoolExpr *
makeEdgesVertexQual(void)
{
	Node	   *start;
	Node	   *end;
	Node	   *vid;
	A_Expr	   *eq_start;
	A_Expr	   *eq_end;
	BoolExpr   *or_expr;

	start = makeColumnRef(genQualifiedName(DELETE_EDGE_ALIAS, AG_START_ID));
	end = makeColumnRef(genQualifiedName(DELETE_EDGE_ALIAS, AG_END_ID));
	vid = makeColumnRef(genQualifiedName(DELETE_VERTEX_ALIAS, AG_ELEM_ID));

	eq_start = makeSimpleA_Expr(AEXPR_OP, "=", start, vid, -1);
	eq_end = makeSimpleA_Expr(AEXPR_OP, "=", end, vid, -1);

	or_expr = makeNode(BoolExpr);
	or_expr->boolop = OR_EXPR;
	or_expr->args = list_make2(eq_start, eq_end);
	or_expr->location = -1;

	return or_expr;
}

static List *
extractVerticesExpr(ParseState *pstate, List *exprlist, ParseExprKind exprKind)
{
	List	   *result = NIL;
	ListCell   *le;

	foreach(le, exprlist)
	{
		Node   *expr = lfirst(le);
		Node   *elem = transformCypherExpr(pstate, expr, exprKind);

		switch (exprType(elem))
		{
			case EDGEOID:
			case EDGEARRAYOID:
				continue;

			case GRAPHPATHOID:
				elem = getExprField((Expr *) elem, AG_PATH_VERTICES);
				/* no break */
			case VERTEXOID:
				{
					GraphDelElem *gde = makeNode(GraphDelElem);

					gde->variable = getDeleteTargetName(pstate, expr);
					gde->elem = elem;

					result = lappend(result, gde);
				}
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						errmsg("node, relationship, or path is expected"),
						parser_errposition(pstate, exprLocation(elem))));
		}
	}

	return result;
}

static List *
extractEdgesExpr(ParseState *pstate, List *exprlist, ParseExprKind exprKind)
{
	List	   *result = NIL;
	ListCell   *le;

	foreach(le, exprlist)
	{
		Node   *expr = lfirst(le);
		Node   *elem = transformCypherExpr(pstate, expr, exprKind);

		switch (exprType(elem))
		{
			case VERTEXOID:
				continue;

			case GRAPHPATHOID:
				elem = getExprField((Expr *) elem, AG_PATH_EDGES);
				/* no break */
			case EDGEOID:
				{
					GraphDelElem *gde = makeNode(GraphDelElem);

					gde->variable = getDeleteTargetName(pstate, expr);
					gde->elem = elem;

					result = lappend(result, gde);
				}
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						errmsg("node, relationship, or path is expected"),
						parser_errposition(pstate, exprLocation(elem))));
		}
	}

	return result;
}

static char *
getDeleteTargetName(ParseState *pstate, Node *expr)
{
	ColumnRef  *cr;

	if (!IsA(expr, ColumnRef))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only direct variable reference is supported"),
				 parser_errposition(pstate, exprLocation(expr))));

	cr = (ColumnRef *) expr;
	if (list_length(cr->fields) != 1 ||
		!IsA(linitial(cr->fields), String))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_NAME),
				 errmsg("invalid delete target name"),
				 parser_errposition(pstate, exprLocation(expr))));

	return pstrdup(strVal(linitial(cr->fields)));
}

static List *
findAllModifiedLabels(Query *qry)
{
	List	   *label_oids = NIL;
	ListCell   *lc;

	/* DELETE */
	if (qry->graph.exprs != NIL)
	{
		foreach(lc, qry->graph.exprs)
		{
			GraphDelElem *gde = lfirst(lc);

			label_oids = lappend_oid(label_oids,
									 find_target_label(gde->elem, qry));
		}
	}

	/* SET and MERGE ON SET */
	if (qry->graph.sets != NIL)
	{
		foreach(lc, qry->graph.sets)
		{
			GraphSetProp *gsp = lfirst(lc);

			label_oids = lappend_oid(label_oids,
									 find_target_label(gsp->elem, qry));
		}
	}

	foreach(lc, label_oids)
	{
		Oid			relid = lfirst_oid(lc);
		List	   *child_oids;

		child_oids = find_all_inheritors(relid, AccessShareLock, NULL);
		qry->graph.targets = list_union_oid(qry->graph.targets, child_oids);
	}

	return qry->graph.targets;
}

static Oid
find_target_label(Node *node, Query *qry)
{
	find_target_label_context ctx;

	ctx.qry = qry;
	ctx.sublevels_up = 0;
	ctx.in_preserved = false;
	ctx.resno = InvalidAttrNumber;
	ctx.relid = InvalidOid;

	if (!find_target_label_walker(node, &ctx))
		elog(ERROR, "cannot find target label");

	Assert(ctx.relid != InvalidOid);
	return ctx.relid;
}

static bool
find_target_label_walker(Node *node, find_target_label_context *ctx)
{
	Query	   *qry = ctx->qry;

	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var		   *var = (Var *) node;
		TargetEntry *te;
		RangeTblEntry *rte;

		/*
		 * NOTE: This is related to how `ModifyGraph` does SET, and
		 *       `FVR_PRESERVE_VAR_REF` flag. We need to fix this.
		 */
		if ((qry->graph.writeOp == GWROP_SET ||
			 qry->graph.writeOp == GWROP_DELETE) &&
			ctx->sublevels_up == 0 && !ctx->in_preserved)
		{
			te = get_tle_by_resno(qry->targetList, var->varattno);

			ctx->in_preserved = true;

			if (find_target_label_walker((Node *) te->expr, ctx))
				return true;

			ctx->in_preserved = false;
		}

		if (var->varno <= 0 || var->varno > list_length(qry->rtable))
			elog(ERROR, "invalid varno %u", var->varno);
		rte = rt_fetch(var->varno, qry->rtable);

		/* whole-row Var */
		if (var->varattno == InvalidAttrNumber)
			return false;

		if (rte->rtekind == RTE_RELATION)
		{
			ctx->relid = rte->relid;
			return true;
		}
		else if (rte->rtekind == RTE_SUBQUERY)
		{
			Query	   *subqry = rte->subquery;

			te = get_tle_by_resno(subqry->targetList, var->varattno);

			ctx->qry = subqry;
			ctx->sublevels_up++;
			ctx->resno = te->resno;

			if (find_target_label_walker((Node *) te->expr, ctx))
				return true;

			ctx->qry = qry;
			ctx->sublevels_up--;
			ctx->resno = InvalidAttrNumber;
		}
		else if (rte->rtekind == RTE_JOIN)
		{
			Node	   *joinvar;

			if (var->varattno <= 0 ||
				var->varattno > list_length(rte->joinaliasvars))
				elog(ERROR, "invalid varattno %hd", var->varattno);

			joinvar = list_nth(rte->joinaliasvars, var->varattno - 1);
			if (find_target_label_walker(joinvar, ctx))
				return true;
		}
		else
		{
			elog(ERROR, "unexpected retkind(%d) in find_target_label_walker()",
				 rte->rtekind);
		}

		return false;
	}

	/*
	 * For a CREATE clause, `transformCypherCreateClause()` does not create
	 * RTE's for target labels. So, look through `qry->graph.pattern` to get
	 * the relid of the target label.
	 *
	 * This code assumes that `RowExpr` appears only as root of the expression
	 * in `TargetEntry` when `wrietOp` is `GWROP_CREATE`. This assumption is OK
	 * because users cannot make `RowExpr` in Cypher.
	 */
	if (IsA(node, RowExpr) && qry->graph.writeOp == GWROP_CREATE)
	{
		GraphPath  *gpath;
		ListCell   *le;

		Assert(list_length(qry->graph.pattern) == 1);
		gpath = linitial(qry->graph.pattern);

		foreach(le, gpath->chain)
		{
			Node	   *elem = lfirst(le);

			if (IsA(elem, GraphVertex))
			{
				GraphVertex *gvertex = (GraphVertex *) elem;

				if (gvertex->resno == ctx->resno)
				{
					ctx->relid = gvertex->relid;
					return true;
				}
			}
			else
			{
				GraphEdge  *gedge;

				Assert(IsA(elem, GraphEdge));
				gedge = (GraphEdge *) elem;

				if (gedge->resno == ctx->resno)
				{
					ctx->relid = gedge->relid;
					return true;
				}
			}
		}

		return false;
	}

	/*
	 * It is difficult to find a label for graph elements in a graph path,
	 * so all labels of that type are treated as result labels.
	 */
	if (IsA(node, FieldSelect))
	{
		FieldSelect *fs = castNode(FieldSelect, node);
		Oid		graph_oid = get_graph_path_oid();

		if (exprType((Node *) fs->arg) == GRAPHPATHOID)
		{
			if (exprType(node) == VERTEXARRAYOID)
			{
				ctx->relid = get_laboid_relid(get_labname_laboid(AG_VERTEX,
																 graph_oid));

				return true;
			}
			else if (exprType(node) == EDGEARRAYOID)
			{
				ctx->relid = get_laboid_relid(get_labname_laboid(AG_EDGE,
																 graph_oid));

				return true;
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("invalid fieldnum %s : %hd",
								format_type_be(exprType((Node *) fs->arg)),
								fs->fieldnum)));
		}
	}

	if (expression_tree_walker(node, find_target_label_walker, ctx))
		return true;

	return false;
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

	snprintf(sqlcmd, sizeof(sqlcmd), "CREATE %s \"%s\"", keyword, labname);

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
	Node	   *prop_map;
	Oid			type;

	prop_map = transformCypherExpr(pstate, expr, exprKind);
	type = exprType(prop_map);
	if (type != JSONBOID && type_is_rowtype(type))
	{
		FuncCall   *to_jsonb;

		to_jsonb = makeFuncCall(list_make1(makeString("to_jsonb")), NIL, -1);

		prop_map = ParseFuncOrColumn(pstate, to_jsonb->funcname,
									 list_make1(prop_map), pstate->p_last_srf,
									 to_jsonb, -1);
	}

	if (exprKind == EXPR_KIND_INSERT_TARGET && !allow_null_properties)
		prop_map = stripNullKeys(pstate, prop_map);

	return resolve_future_vertex(pstate, prop_map, 0);
}

static Node *
stripNullKeys(ParseState *pstate, Node *properties)
{
	FuncCall *strip;

	/* keys with NULL value is not allowed */
	strip = makeFuncCall(list_make1(makeString("jsonb_strip_nulls")), NIL, -1);

	return ParseFuncOrColumn(pstate, strip->funcname, list_make1(properties),
							 pstate->p_last_srf, strip, -1);

}

static bool
assign_query_eager_walker(Node *node, Query *nxtQry)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		Query *qry = (Query *) node;

		if (qry->graph.eager == true)
			return true;

		if (qry->commandType == CMD_GRAPHWRITE)
		{
			/* Clauses whose CID is incremented should be run as eager. */
			if (qry->graph.sets != NIL ||
				qry->graph.exprs != NIL ||
				qry->graph.writeOp == GWROP_MERGE)
				qry->graph.eager = true;
			else if (nxtQry->graph.writeOp == GWROP_MERGE &&
					(qry->graph.writeOp == GWROP_CREATE ||
					 qry->graph.writeOp == GWROP_MERGE))
				qry->graph.eager = true;
			else
				qry->graph.eager = false;

			if (qry->graph.eager == true &&
				enable_eager == false)
				elog(ERROR, "eagerness plan is not allowed.");
			return true;
		}

		(void) range_table_walker(qry->rtable,
								  assign_query_eager_walker,
								  (void *)nxtQry,
								  QTW_IGNORE_CTE_SUBQUERIES);
	}

	return false;
}

static void
assign_query_eager(Query *query)
{
	(void) range_table_walker(query->rtable,
							  assign_query_eager_walker,
							  (void *)query,
							  QTW_IGNORE_CTE_SUBQUERIES);

	if (!query->graph.last &&
		(query->graph.sets != NIL ||
		 query->graph.exprs != NIL))
		query->graph.eager = true;

	if (query->graph.eager == true &&
		enable_eager == false)
		elog(ERROR, "eagerness plan is not allowed.");
}

static RangeTblEntry *
transformClause(ParseState *pstate, Node *clause)
{
	return transformClauseBy(pstate, clause, transformStmt);
}

static RangeTblEntry *
transformClauseBy(ParseState *pstate, Node *clause, TransformMethod transform)
{
	Alias	   *alias;
	RangeTblEntry *rte;

	alias = makeAliasNoDup(CYPHER_SUBQUERY_ALIAS, NIL);
	rte = transformClauseImpl(pstate, clause, transform, alias);
	addRTEtoJoinlist(pstate, rte, true);

	return rte;
}

static RangeTblEntry *
transformClauseImpl(ParseState *pstate, Node *clause,
					TransformMethod transform, Alias *alias)
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

	qry = transform(childParseState, clause);

	pstate->p_elem_quals = childParseState->p_elem_quals;
	future_vertices = childParseState->p_future_vertices;
	if (childParseState->p_nr_modify_clause > 0)
		pstate->p_nr_modify_clause = childParseState->p_nr_modify_clause;
	pstate->p_delete_edges_resname = childParseState->p_delete_edges_resname;

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
	Node	   *tid;

	id = getColumnVar(pstate, rte, AG_ELEM_LOCAL_ID);
	prop_map = getColumnVar(pstate, rte, AG_ELEM_PROP_MAP);
	tid = getSysColumnVar(pstate, rte, SelfItemPointerAttributeNumber);

	return makeTypedRowExpr(list_make3(id, prop_map, tid), VERTEXOID, location);
}

static Node *
makeEdgeExpr(ParseState *pstate, CypherRel *crel, RangeTblEntry *rte,
			 int location)
{
	Node	   *id;
	Node	   *start;
	Node	   *end;
	Node	   *prop_map;
	Node	   *tid;

	id = getColumnVar(pstate, rte, AG_ELEM_LOCAL_ID);
	start = getColumnVar(pstate, rte, AG_START_ID);
	end = getColumnVar(pstate, rte, AG_END_ID);
	prop_map = getColumnVar(pstate, rte, AG_ELEM_PROP_MAP);
	if (crel->direction == CYPHER_REL_DIR_NONE)
		tid = getColumnVar(pstate, rte, "ctid");
	else
		tid = getSysColumnVar(pstate, rte, SelfItemPointerAttributeNumber);

	return makeTypedRowExpr(list_make5(id, start, end, prop_map, tid),
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

static A_Const *
makeNullAConst(void)
{
	A_Const	   *nullconst;

	nullconst = makeNode(A_Const);
	nullconst->val.type = T_Null;
	nullconst->location = -1;

	return nullconst;
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
