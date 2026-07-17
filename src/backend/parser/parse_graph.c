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

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/stratnum.h"
#include "access/table.h"
#include "ag_const.h"
#include "catalog/ag_edge_d.h"
#include "catalog/ag_graph_fn.h"
#include "catalog/ag_label.h"
#include "catalog/ag_label_fn.h"
#include "catalog/ag_vertex_d.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "nodes/graphnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/analyze.h"
#include "parser/parse_agg.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_cypher_expr.h"
#include "parser/parse_cypher_utils.h"
#include "parser/parse_func.h"
#include "parser/parse_graph.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parsetree.h"
#include "parser/parse_shortestpath.h"
#include "pgstat.h"
#include "rewrite/rewriteHandler.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#define EDGE_UNION_START_ID		"_start"
#define EDGE_UNION_END_ID		"_end"

#define VLE_LEFT_ALIAS			"l"

#define VLE_COLNAME_IDS			"ids"
#define VLE_COLNAME_EDGES		"edges"
#define VLE_COLNAME_VERTICES	"vertices"

#define DELETE_VERTEX_ALIAS		"v"
#define DELETE_EDGE_ALIAS		"e"

bool		enable_eager = true;

typedef struct
{
	NodeTag		type;
	char	   *varname;		/* variable assigned to the entity */
	char	   *labname;		/* final label of the entity */
	bool		prop_constr;	/* has property constraints? */
	bool		local;			/* declared in current clause? */
}			EntityInfo;

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
	Index		varno;			/* RTE of the (anonymous) element, for
								 * ginAvail */
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

typedef struct
{
	TargetEntry *tle;
	Index		varno;
	AttrNumber	varattno;
	Oid			type;
} ExtTLE;

typedef struct
{
	Node	   *prev_node;
	Query	   *query;
	List	   *tle_lists;
} resolve_var_from_targetlist_context;

/* projection (RETURN and WITH) */
static void checkNameInItems(ParseState *pstate, List *items, List *targetList);
static void checkCypherLetItems(ParseState *pstate, List *targetList);
static void updateSortOperatorsForJsonb(List *sortClause, List *targetList);

/* MATCH - OPTIONAL */
static ParseNamespaceItem *transformMatchOptional(ParseState *pstate,
												  CypherClause *clause);

/* MATCH - preprocessing */
static bool hasPropConstr(List *pattern);
static List *getFindPaths(List *pattern);
static void appendFindPathsResult(ParseState *pstate, List *fplist,
								  List **targetList);
static void collectEntityInfo(ParseState *pstate, List *pattern);
static void addEntityInfo(ParseState *pstate, Node *entity);
static EntityInfo * getEntityInfo(ParseState *pstate, char *varname,
								  NodeTag entity_type, bool local_only);
static EntityInfo * findEntityInfo(ParseState *pstate, char *varname,
								   NodeTag entity_type, bool local_only);
static char *getEntityVarname(Node *entity);
static char *getEntityLabname(Node *entity);
static int	getEntityVarloc(Node *entity);
static bool hasEntityPropConstr(Node *entity);
static List *makeComponents(List *pattern);
static bool isPathConnectedTo(CypherPath *path, List *component);
static bool arePathsConnected(CypherPath *path1, CypherPath *path2);
static bool validate_pattern_labels(ParseState *pstate,
									CypherClause *clause);
static bool labelCreatedByPrevClause(Node *prev, char *labname,
									 char labkind);

/* MATCH - transform */
static Node *transformComponents(ParseState *pstate, List *components,
								 List **targetList);
static Node *transformMatchNode(ParseState *pstate, CypherNode *cnode,
								List **targetList, List **eqoList,
								bool *is_nsitem);
static Node *transformMatchRel(ParseState *pstate,
							   CypherRel *crel,
							   List **targetList, List **eqoList,
							   bool pathout, bool *is_nsitem);
static Node *transformMatchSR(ParseState *pstate, CypherRel *crel,
							  List **targetList, List **eqoList,
							  bool *is_nsitem);
static ParseNamespaceItem *addEdgeUnion(ParseState *pstate, char *edge_label,
										bool only, int location, Alias *alias);
static Node *genEdgeUnion(char *edge_label, bool only, int location);
static void setInitialVidForVLE(ParseState *pstate, CypherRel *crel,
								Node *vertex, bool vertex_is_nsitem,
								CypherRel *prev_crel, Node *prev_edge,
								bool prev_edge_is_nsitem);
static Node *transformMatchVLE(ParseState *pstate, CypherRel *crel,
							   List **targetList, bool pathout,
							   bool *is_nsitem);
static SelectStmt *genVLESubselect(ParseState *pstate, CypherRel *crel,
								   bool out, bool pathout);
static Node *genVLELeftChild(ParseState *pstate, CypherRel *crel, bool out);
static Node *genEdgeSimple(char *aliasname);
static Node *genVLEEdgeSubselect(ParseState *pstate, CypherRel *crel,
								 char *aliasname);
static RangeSubselect *genInhEdge(RangeVar *r, Oid parentoid);
static List *genQualifiedName(char *name1, char *name2);
static Node *genVLEQual(char *alias, Node *propMap);
static ParseNamespaceItem *transformVLEtoNSItem(ParseState *pstate, CypherRel *crel,
												SelectStmt *vle, Alias *alias);
static bool isZeroLengthVLE(CypherRel *crel);
static void getCypherRelType(CypherRel *crel, char **typname, int *typloc);
static Node *addQualRelPath(ParseState *pstate, Node *qual,
							CypherRel *prev_crel, Node *prev_edge,
							bool prev_edge_is_nsitem,
							CypherRel *crel, Node *edge, bool edge_is_nsitem);
static Node *addQualNodeIn(ParseState *pstate, Node *qual, Node *vertex,
						   bool vertex_is_nsitem, CypherRel *crel, Node *edge,
						   bool edge_is_nsitem, bool prev);
static char *getEdgeColname(CypherRel *crel, bool edge_is_nsitem, bool prev);
static bool isFutureVertexExpr(Node *vertex);
static void setFutureVertexExprId(ParseState *pstate, Node *vertex,
								  CypherRel *crel, Node *edge,
								  bool edge_is_nsitem, bool prev);
static Node *addQualUniqueEdges(ParseState *pstate, Node *qual, List *ueids,
								List *ueidarrs);

/* MATCH - VLE */
static Node *vtxArrConcat(ParseState *pstate, Node *array, Node *elem);
static Node *edgeArrConcat(ParseState *pstate, Node *array, Node *elem);

/* MATCH - quals */
static void addElemQual(ParseState *pstate, AttrNumber varattno,
						Node *prop_constr);
static void adjustElemQuals(List *elem_quals, ParseNamespaceItem *nsitem);
static Node *transformElemQuals(ParseState *pstate, Node *qual);
static Node *transform_prop_constr(ParseState *pstate, Node *qual,
								   Node *prop_map, Node *prop_constr);
static void transform_prop_constr_worker(Node *node, prop_constr_context *ctx);
static bool ginAvail(ParseState *pstate, Index varno, AttrNumber varattno);
static Oid	getSourceRelid(ParseState *pstate, Index varno, AttrNumber varattno);
static bool hasGinOnProp(Oid relid);

/* MATCH - future vertex */
static void addFutureVertex(ParseState *pstate, AttrNumber varattno,
							char *labname);
static FutureVertex *findFutureVertex(ParseState *pstate, Index varno,
									  AttrNumber varattno, int sublevels_up);
static List *adjustFutureVertices(List *future_vertices,
								  ParseNamespaceItem *nsitem);
static Node *resolve_future_vertex(ParseState *pstate, Node *node, int flags);
static Node *resolve_future_vertex_mutator(Node *node,
										   resolve_future_vertex_context *ctx);
static void resolveFutureVertex(ParseState *pstate, FutureVertex *fv,
								bool ignore_nullable);
static ParseNamespaceItem *makeVertexNSItem(ParseState *parentParseState,
											char *varname, char *labname);
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
static List *transformSetPropList(ParseState *pstate, bool is_remove,
								  CSetKind kind, List *items);
static GraphSetProp *transformSetProp(ParseState *pstate, CypherSetProp *sp,
									  bool is_remove,
									  CSetKind kind);
static bool resolve_var_from_targetlist_walker(Node *node,
											   resolve_var_from_targetlist_context *ctx);
static void substitute_set_props_as_targetentry(ParseState *pstate,
												Query *query,
												List *graphSetProps);

/* MERGE */
static Query *transformMergeMatch(ParseState *pstate, Node *parseTree);
static ParseNamespaceItem *transformMergeMatchJoin(ParseState *pstate,
												   CypherClause *clause);
static ParseNamespaceItem *transformNullSelect(ParseState *pstate);
static Node *makeMatchForMerge(List *pattern);
static List *transformMergeCreate(ParseState *pstate, List *pattern,
								  RangeTblEntry *prevrte, List *resultList);
static GraphVertex *transformMergeNode(ParseState *pstate, CypherNode *cnode,
									   bool singlenode, List **targetList,
									   List *resultList);
static GraphEdge *transformMergeRel(ParseState *pstate, CypherRel *crel,
									List **targetList, List *resultList);
static List *transformMergeOnSet(ParseState *pstate, List *sets);

/* DELETE */
static Query *transformDeleteJoin(ParseState *pstate, Node *parseTree);
static Query *transformDeleteEdges(ParseState *pstate, Node *parseTree);
static ParseNamespaceItem *transformDeleteJoinNSItem(ParseState *pstate,
													 CypherClause *clause);
static A_ArrayExpr *verticesAppend(A_ArrayExpr *vertices, Node *expr);
static Node *verticesConcat(Node *vertices, Node *expr);
static Node *makeSelectEdgesVertices(Node *vertices,
									 CypherDeleteClause *delete,
									 char **edges_resname);
static Node *makeEdgesForDetach(void);
static RangeFunction *makeRangeFunction(List *func, Node *expr, Alias *alias,
										bool ordinality);
static BoolExpr *makeEdgesVertexQual(void);
static List *extractVerticesExpr(ParseState *pstate, List *exprlist,
								 ParseExprKind exprKind);
static List *extractEdgesExpr(ParseState *pstate, List *exprlist,
							  ParseExprKind exprKind);
static char *getDeleteTargetName(ParseState *pstate, Node *expr);

/* graph write */
static List *addRangeTableAllModifiedLabels(ParseState *pstate, Query *qry,
											List *targets, AclMode requiredPerms);
static void addRangeTableLabels(ParseState *pstate, List *targets, Query *qry,
								AclMode requiredPerms);
static Oid	find_target_label(Node *node, Query *qry);
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
static ParseNamespaceItem *transformClause(ParseState *pstate, Node *clause);
static ParseNamespaceItem *transformClauseBy(ParseState *pstate, Node *clause,
											 TransformMethod transform);
static ParseNamespaceItem *transformClauseImpl(ParseState *pstate,
											   Node *clause,
											   TransformMethod transform,
											   Alias *alias);
static ParseNamespaceItem *incrementalJoinRTEs(ParseState *pstate,
											   JoinType jointype,
											   ParseNamespaceItem *l_nsitem,
											   ParseNamespaceItem *r_nsitem,
											   Node *qual, Alias *alias);
static void makeJoinResCols(ParseState *pstate, ParseNamespaceItem *l_rte,
							ParseNamespaceItem *r_rte,
							List **l_colvars, List **r_colvars,
							List **res_colnames,
							List **res_colvars);
static ParseNamespaceItem *findNamespaceItemForRTE(ParseState *pstate,
												   RangeTblEntry *rte);
static List *makeTargetListFromNSItem(ParseState *pstate,
									  ParseNamespaceItem *nsitem);
static List *makeTargetListFromJoin(ParseState *pstate,
									ParseNamespaceItem *nsitem);
static TargetEntry *makeWholeRowTarget(ParseState *pstate,
									   ParseNamespaceItem *nsitem);
static TargetEntry *findTarget(List *targetList, char *resname);
static void mark_nodes_as_nonlocal(List *nis);

/* expression - type */
static Node *makePathVertexExpr(ParseState *pstate, Node *obj, bool is_nsitem);
static Node *makePathEdgeExpr(ParseState *pstate, CypherRel *crel, Node *obj,
							  bool is_nsitem);

/* getExprField() is declared extern in parse_graph.h (used by parse_cypher_expr.c) */
static Node *getRowExprField(Expr *rowexpr, char *fname);
static Node *qualAndExpr(Node *qual, Node *expr);

/* parse node */
static A_Const *makeBoolAConst(bool state, int location);
static A_Const *makeNullAConst(void);
static Node *makeIntConst(int val, int location);
static bool IsNullAConst(Node *arg);

/* utils */
static Node *resolveVarOrExpr(ParseState *pstate, Node *node,
							  char *colname, bool node_is_nsitem);
static void markRelsAsNulledBy(ParseState *pstate, Node *n, int jindex);
static void preprocess_merge_pattern(ParseState *pstate, List *pattern,
									 ParseNamespaceItem *nsitem);

Query *
transformCypherSubPattern(ParseState *pstate, CypherSubPattern *subpat)
{
	CypherMatchClause *match;
	CypherClause *clause;
	Query	   *qry;
	ParseNamespaceItem *nsitem;

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
	match->where = subpat->where;
	match->optional = subpat->optional;

	clause = makeNode(CypherClause);
	clause->detail = (Node *) match;
	clause->prev = NULL;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	nsitem = transformClause(pstate, (Node *) clause);

	qry->targetList = makeTargetListFromNSItem(pstate, nsitem);
	if (subpat->kind == CSP_SIZE)
		qry->targetList = list_make1(makeCountTargetEntry(pstate));
	markTargetListOrigins(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);

	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
	qry->hasAggs = pstate->p_hasAggs;
	if (qry->hasAggs)
		parseCheckAggregates(pstate, qry);

	qry->hasGraphwriteClause = pstate->p_hasGraphwriteClause;

	assign_query_collations(pstate, qry);

	return qry;
}

/*
 * After the RETURN target list is coerced to jsonb (resolveItemList), any sort
 * key that points at a coerced target entry still carries the comparison
 * operators chosen for the entry's original type.  Re-point such sort keys at
 * the operators for the entry's final (jsonb) type so that ordering matches
 * the returned values.  Sort keys whose type did not change keep their original
 * operators.
 */
static void
updateSortOperatorsForJsonb(List *sortClause, List *targetList)
{
	ListCell   *lc;

	foreach(lc, sortClause)
	{
		SortGroupClause *sortcl = (SortGroupClause *) lfirst(lc);
		TargetEntry *tle;
		Oid			restype;
		Oid			sortop;
		Oid			eqop;
		bool		hashable;
		Oid			opfamily;
		Oid			opcintype;
		CompareType cmptype;

		tle = get_sortgroupref_tle(sortcl->tleSortGroupRef, targetList);
		if (tle == NULL)
			continue;

		restype = exprType((Node *) tle->expr);

		/* What ordering direction does the current operator represent? */
		if (!get_ordering_op_properties(sortcl->sortop,
										&opfamily, &opcintype, &cmptype))
			continue;

		if (cmptype == COMPARE_LT)
			get_sort_group_operators(restype,
									 true, true, false,
									 &sortop, &eqop, NULL,
									 &hashable);
		else if (cmptype == COMPARE_GT)
			get_sort_group_operators(restype,
									 false, true, true,
									 NULL, &eqop, &sortop,
									 &hashable);
		else
			continue;

		sortcl->eqop = eqop;
		sortcl->sortop = sortop;
	}
}

/*
 * IsGraphWriteClause
 *		Is this Cypher clause a graph-write clause (CREATE/DELETE/SET/MERGE)?
 */
bool
IsGraphWriteClause(Node *clause)
{
	if (clause == NULL)
		return false;

	switch (cypherClauseTag(clause))
	{
		case T_CypherCreateClause:
		case T_CypherDeleteClause:
		case T_CypherSetClause:
		case T_CypherMergeClause:
			return true;
		default:
			return false;
	}
}

Query *
transformCypherProjection(ParseState *pstate, CypherClause *clause)
{
	CypherProjection *detail = (CypherProjection *) clause->detail;
	Query	   *qry;
	ParseNamespaceItem *nsitem;
	Node	   *qual = NULL;
	int			flags;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	if (detail->kind == CP_FINISH)
	{
		ParseNamespaceItem *prev_nsitem = NULL;

		if (clause->prev != NULL)
			prev_nsitem = transformClause(pstate, clause->prev);

		/*
		 * FINISH terminates a query and returns no rows.  When the preceding
		 * clause is a graph write, return its subquery so the write still
		 * executes; such a query produces no rows of its own.
		 */
		if (IsGraphWriteClause(clause->prev))
			return prev_nsitem->p_rte->subquery;

		/*
		 * Otherwise the query is read-only: keep the read for planning but
		 * return nothing by applying LIMIT 0.  FINISH has no projection
		 * items, so the target list is left empty.
		 */
		if (!pstate->p_hasGraphwriteClause)
			qry->limitCount = transformCypherLimit(pstate, makeIntConst(0, -1),
												   EXPR_KIND_LIMIT, "LIMIT");
	}
	else if (detail->where != NULL)
	{
		Node	   *where = detail->where;

		Assert(detail->kind == CP_WITH);

		detail->where = NULL;
		nsitem = transformClause(pstate, (Node *) clause);
		detail->where = where;

		qry->targetList = makeTargetListFromNSItem(pstate, nsitem);

		qual = transformCypherWhere(pstate, where, EXPR_KIND_WHERE);
		qual = resolve_future_vertex(pstate, qual, 0);
	}
	else
	{
		/*
		 * Build the projection and apply ORDER BY / DISTINCT / SKIP / LIMIT
		 * at the same query level as the pattern, rather than wrapping the
		 * projection in a subquery.  This lets ORDER BY reference variables
		 * that are not in the RETURN list (e.g. "RETURN p.name ORDER BY
		 * p.age"); transformCypherOrderBy() adds such sort keys as resjunk
		 * target entries.
		 */
		if (clause->prev != NULL)
			transformClause(pstate, clause->prev);
		else if (detail->kind == CP_LET)

			/*
			 * A leading LET has no incoming working table, so its implicit "*"
			 * item -- which otherwise carries the existing bindings forward --
			 * has nothing to expand and would trip the "RETURN * with no
			 * accessible variables" check.  Drop it so the LET assignments
			 * project over the implicit single-row input, just like a leading
			 * RETURN.
			 */
			detail->items = list_delete_first(detail->items);

		qry->targetList = transformItemList(pstate, detail->items,
											EXPR_KIND_SELECT_TARGET);

		if (detail->kind == CP_WITH)
			checkNameInItems(pstate, detail->items, qry->targetList);
		else if (detail->kind == CP_LET)
			checkCypherLetItems(pstate, qry->targetList);

		if (detail->order != NULL)
			qry->sortClause = transformCypherOrderBy(pstate, detail->order,
													 &qry->targetList);

		if (detail->distinct != NULL)
		{
			Assert(linitial(detail->distinct) == NULL);

			qry->distinctClause = transformDistinctClause(pstate,
														  &qry->targetList,
														  qry->sortClause,
														  false);
		}

		qry->limitCount = transformCypherLimit(pstate, detail->limit,
											   EXPR_KIND_LIMIT, "LIMIT");
		qry->limitCount = resolve_future_vertex(pstate, qry->limitCount, 0);

		qry->limitOffset = transformCypherLimit(pstate, detail->skip,
												EXPR_KIND_OFFSET, "SKIP/OFFSET");
		qry->limitOffset = resolve_future_vertex(pstate, qry->limitOffset, 0);

		qry->groupClause = generateGroupClause(pstate, &qry->targetList,
											   qry->sortClause);
	}

	if (detail->kind == CP_WITH || detail->kind == CP_LET)
	{
		ListCell   *lt;

		/* try to resolve all target entries except vertex Var */
		foreach(lt, qry->targetList)
		{
			TargetEntry *te = lfirst(lt);
			Node	   *expr = (Node *) te->expr;

			if (IsA(expr, Const) && exprType(expr) == UNKNOWNOID)
				expr = coerce_type(pstate, expr, UNKNOWNOID, TEXTOID, -1,
								   COERCION_IMPLICIT, COERCE_IMPLICIT_CAST,
								   exprLocation(expr));

			else if (IsA(expr, Var) && exprType(expr) == VERTEXOID)
				continue;

			expr = resolve_future_vertex(pstate, expr, 0);
			te->expr = (Expr *) expr;
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

	if (detail->kind == CP_RETURN)
	{
		resolveItemList(pstate, qry->targetList);

		/*
		 * resolveItemList() coerces returned target entries to jsonb, which
		 * can change the type a sort key resolves to; refresh the sort
		 * operators so they match the final (jsonb) type.
		 */
		if (qry->sortClause != NIL)
			updateSortOperatorsForJsonb(qry->sortClause, qry->targetList);
	}

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
	qry->hasAggs = pstate->p_hasAggs;
	qry->rteperminfos = pstate->p_rteperminfos;
	if (qry->hasAggs)
		parseCheckAggregates(pstate, qry);

	qry->hasGraphwriteClause = pstate->p_hasGraphwriteClause;

	assign_query_collations(pstate, qry);

	return qry;
}

/*
 * labelCreatedByPrevClause
 *
 * Return true if a write clause earlier in this statement (anywhere along the
 * "prev" chain) creates the vertex/edge label named labname.  CREATE and MERGE
 * auto-create a referenced label that does not exist yet (createLabelIfNotExist),
 * so a label can be absent from the catalog when a later clause is parsed but
 * present by the time that clause executes.
 *
 * Clauses are transformed prev-first, but a MATCH validates its own labels
 * before its prev clause is transformed, so the auto-create has not happened
 * yet at validation time.  Walking the still-untransformed prev chain lets the
 * MATCH recognise such a label as one that will exist, instead of treating it
 * as permanently absent and folding the scan to a constant-false One-Time
 * Filter -- which would discard the side-effecting CREATE/MERGE that feeds it
 * (e.g. "CREATE (:L) WITH 1 AS x MATCH (n:L) RETURN n" silently returning
 * nothing).
 */
static bool
labelCreatedByPrevClause(Node *prev, char *labname, char labkind)
{
	CypherClause *clause;
	List	   *pattern = NIL;
	ListCell   *lp;

	check_stack_depth();

	if (prev == NULL || !IsA(prev, CypherClause))
		return false;

	clause = (CypherClause *) prev;

	/* Only CREATE and MERGE auto-create a missing label. */
	if (cypherClauseTag(clause) == T_CypherCreateClause)
		pattern = ((CypherCreateClause *) clause->detail)->pattern;
	else if (cypherClauseTag(clause) == T_CypherMergeClause)
		pattern = ((CypherMergeClause *) clause->detail)->pattern;

	foreach(lp, pattern)
	{
		CypherPath *path = lfirst(lp);
		ListCell   *le;

		foreach(le, path->chain)
		{
			Node	   *elem = lfirst(le);

			if (labkind == LABEL_KIND_VERTEX && IsA(elem, CypherNode))
			{
				char	   *l = getCypherName(((CypherNode *) elem)->label);

				if (l != NULL && strcmp(l, labname) == 0)
					return true;
			}
			else if (labkind == LABEL_KIND_EDGE && IsA(elem, CypherRel))
			{
				CypherRel  *crel = (CypherRel *) elem;
				Node	   *type = crel->types ? linitial(crel->types) : NULL;
				char	   *t = getCypherName(type);

				if (t != NULL && strcmp(t, labname) == 0)
					return true;
			}
		}
	}

	/* Look further back along the chain of clauses. */
	return labelCreatedByPrevClause(clause->prev, labname, labkind);
}

static bool
validate_pattern_labels(ParseState *pstate, CypherClause *clause)
{
	List	   *pattern = ((CypherMatchClause *) clause->detail)->pattern;
	ListCell   *lp = NULL;

	foreach(lp, pattern)
	{
		CypherPath *path = lfirst(lp);
		ListCell   *le = NULL;

		foreach(le, path->chain)
		{
			Node	   *elem = lfirst(le);

			if (IsA(elem, CypherNode))
			{
				CypherNode *cnode = (CypherNode *) elem;
				char	   *labname = getCypherName(cnode->label);
				int			labloc = getCypherNameLoc(cnode->label);

				if (labname == NULL)
					continue;

				if (!labelExist(pstate, labname, labloc, LABEL_KIND_VERTEX, false) &&
					!labelCreatedByPrevClause(clause->prev, labname,
											  LABEL_KIND_VERTEX))
					return false;
			}
			else
			{
				CypherRel  *crel = (CypherRel *) elem;
				Node	   *type = crel->types ? linitial(crel->types) : NULL;
				char	   *typname = getCypherName(type);
				int			typloc = getCypherNameLoc(type);

				if (typname == NULL)
					continue;

				if (!labelExist(pstate, typname, typloc, LABEL_KIND_EDGE, false) &&
					!labelCreatedByPrevClause(clause->prev, typname,
											  LABEL_KIND_EDGE))
					return false;
			}
		}
	}

	return true;
}

Query *
transformCypherMatchClause(ParseState *pstate, CypherClause *clause)
{
	CypherMatchClause *detail = (CypherMatchClause *) clause->detail;
	Query	   *qry;
	ParseNamespaceItem *nsitem;
	Node	   *qual = NULL;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	pstate->p_valid_labels = validate_pattern_labels(pstate, clause);

	if (!pstate->p_valid_labels)
		detail->where = (Node *) makeBoolAConst(false, -1);

	/*
	 * since WHERE clause is part of MATCH, transform OPTIONAL MATCH with its
	 * WHERE clause
	 */
	if (detail->optional)
	{
		nsitem = transformMatchOptional(pstate, clause);

		qry->targetList = makeTargetListFromJoin(pstate, nsitem);
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
			int			flags = (pstate->p_is_optional_match ? FVR_IGNORE_NULLABLE : 0);

			pstate->p_is_match_quals = true;
			nsitem = transformClause(pstate, (Node *) clause);

			qry->targetList = makeTargetListFromNSItem(pstate, nsitem);

			qual = transformCypherWhere(pstate, detail->where,
										EXPR_KIND_WHERE);
			qual = transformElemQuals(pstate, qual);
			qual = resolve_future_vertex(pstate, qual, flags);
		}
		else
		{
			List	   *fplist = NIL;

			fplist = getFindPaths(detail->pattern);
			if (!pstate->p_is_fp_processed && fplist != NULL)
			{
				pstate->p_is_fp_processed = true;
				nsitem = transformClause(pstate, (Node *) clause);

				qry->targetList = makeTargetListFromNSItem(pstate, nsitem);
				appendFindPathsResult(pstate, fplist, &qry->targetList);
			}
			else
			{
				List	   *components;

				pstate->p_is_match_quals = false;
				pstate->p_is_fp_processed = false;

				/*
				 * To do this at here is safe since it just uses transformed
				 * expression and does not look over the ancestors of
				 * `pstate`.
				 */
				if (clause->prev != NULL)
				{
					nsitem = transformClause(pstate, clause->prev);

					qry->targetList = makeTargetListFromNSItem(pstate, nsitem);
				}

				collectEntityInfo(pstate, detail->pattern);
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
	qry->rteperminfos = pstate->p_rteperminfos;
	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasGraphwriteClause = pstate->p_hasGraphwriteClause;

	assign_query_collations(pstate, qry);

	return qry;
}

Query *
transformCypherCreateClause(ParseState *pstate, CypherClause *clause)
{
	CypherCreateClause *detail;
	CypherPath *cpath;
	Query	   *qry;
	AclMode		targetPerms;

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
	qry->g_writeOp = GWROP_CREATE;
	qry->g_last = (pstate->parentParseState == NULL);

	if (clause->prev != NULL)
	{
		ParseNamespaceItem *nsitem;

		nsitem = transformClause(pstate, (Node *) clause->prev);
		qry->targetList = makeTargetListFromNSItem(pstate, nsitem);
	}

	/*
	 * Since they will be created if non-existent
	 */
	pstate->p_valid_labels = true;

	qry->g_pattern = transformCreatePattern(pstate, cpath,
											&qry->targetList);
	targetPerms = ACL_INSERT;
	addRangeTableLabels(pstate, pstate->p_target_labels, qry, targetPerms);
	qry->g_nr_modify = pstate->p_nr_modify_clause++;

	qry->targetList = (List *) resolve_future_vertex(pstate,
													 (Node *) qry->targetList,
													 FVR_DONT_RESOLVE);
	markTargetListOrigins(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, pstate->p_resolved_qual);
	qry->rteperminfos = pstate->p_rteperminfos;
	qry->hasSubLinks = pstate->p_hasSubLinks;

	pstate->p_hasGraphwriteClause = true;
	qry->hasGraphwriteClause = pstate->p_hasGraphwriteClause;

	assign_query_collations(pstate, qry);
	assign_query_eager(qry);

	return qry;
}

Query *
transformCypherDeleteClause(ParseState *pstate, CypherClause *clause)
{
	CypherDeleteClause *detail = (CypherDeleteClause *) clause->detail;
	ParseNamespaceItem *nsitem;
	ListCell   *le;
	Query	   *qry;
	AclMode		targetPerms;

	/* DELETE cannot be the first clause */
	Assert(clause->prev != NULL);

	/* Merge same mode of DELETE clauses for reducing delete join */
	while (cypherClauseTag(clause->prev) == T_CypherDeleteClause)
	{
		CypherClause *prev = (CypherClause *) clause->prev;
		CypherDeleteClause *prevDel = (CypherDeleteClause *) prev->detail;

		if (prevDel->detach == detail->detach)
			detail->exprs = list_concat(prevDel->exprs, detail->exprs);

		clause->prev = prev->prev;
	}

	qry = makeNode(Query);
	qry->commandType = CMD_GRAPHWRITE;
	qry->g_writeOp = GWROP_DELETE;
	qry->g_last = (pstate->parentParseState == NULL);
	qry->g_detach = detail->detach;

	nsitem = transformClauseBy(pstate, (Node *) clause, transformDeleteJoin);

	qry->targetList = makeTargetListFromNSItem(pstate, nsitem);
	qry->g_exprs = extractVerticesExpr(pstate, detail->exprs,
									   EXPR_KIND_OTHER);
	qry->g_nr_modify = pstate->p_nr_modify_clause++;

	/*
	 * The edges of the vertices to remove are used only for removal, not for
	 * the next clause.
	 */
	if (detail->detach && pstate->p_delete_edges_resname)
	{
		TargetEntry *te;
		Node	   *edges;
		GraphDelElem *gde = makeNode(GraphDelElem);

		/* This assumes the edge array always comes last. */
		te = llast(qry->targetList);
		edges = llast(detail->exprs);
		gde->variable = pstrdup(pstate->p_delete_edges_resname);
		gde->elem = transformCypherExpr(pstate, edges, EXPR_KIND_OTHER);

		/* Add expression for deleting edges related target vertices. */
		qry->g_exprs = lappend(qry->g_exprs, gde);
		if (strcmp(te->resname, pstate->p_delete_edges_resname) == 0)
			te->resjunk = true;

		pstate->p_delete_edges_resname = NULL;
	}

	foreach(le, qry->g_exprs)
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

	pstate->p_hasGraphwriteClause = true;
	qry->hasGraphwriteClause = pstate->p_hasGraphwriteClause;

	assign_query_collations(pstate, qry);

	assign_query_eager(qry);

	targetPerms = ACL_DELETE;
	if (pstate->p_valid_labels)
		addRangeTableAllModifiedLabels(pstate, qry, NIL, targetPerms);

	qry->rteperminfos = pstate->p_rteperminfos;

	return qry;
}

Query *
transformCypherSetClause(ParseState *pstate, CypherClause *clause)
{
	CypherSetClause *detail = (CypherSetClause *) clause->detail;
	Query	   *qry;
	ParseNamespaceItem *nsitem;
	ListCell   *le;
	AclMode		targetPerms;

	/* SET/REMOVE cannot be the first clause */
	Assert(clause->prev != NULL);
	Assert(detail->kind == CSET_NORMAL);

	qry = makeNode(Query);
	qry->commandType = CMD_GRAPHWRITE;
	qry->g_writeOp = GWROP_SET;
	qry->g_last = (pstate->parentParseState == NULL);

	while (cypherClauseTag(clause->prev) == T_CypherSetClause)
	{
		CypherClause *prev = (CypherClause *) clause->prev;
		CypherSetClause *prev_clause = (CypherSetClause *) prev->detail;

		if (prev_clause->is_remove == detail->is_remove &&
			prev_clause->kind == detail->kind)
		{
			detail->items = list_concat(prev_clause->items, detail->items);
			clause->prev = prev->prev;
		}
		else
		{
			break;
		}
	}

	nsitem = transformClause(pstate, clause->prev);

	qry->targetList = makeTargetListFromNSItem(pstate, nsitem);

	qry->g_sets = transformSetPropList(pstate, detail->is_remove,
									   detail->kind, detail->items);
	foreach(le, qry->g_sets)
	{
		GraphSetProp *gsp = lfirst(le);

		gsp->elem = resolve_future_vertex(pstate, gsp->elem, 0);
		gsp->expr = resolve_future_vertex(pstate, gsp->expr, 0);
	}

	qry->g_nr_modify = pstate->p_nr_modify_clause++;

	qry->targetList = (List *) resolve_future_vertex(pstate,
													 (Node *) qry->targetList,
													 0);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, pstate->p_resolved_qual);
	qry->hasSubLinks = pstate->p_hasSubLinks;

	pstate->p_hasGraphwriteClause = true;
	qry->hasGraphwriteClause = pstate->p_hasGraphwriteClause;

	substitute_set_props_as_targetentry(pstate, qry, qry->g_sets);

	assign_query_collations(pstate, qry);
	foreach(le, qry->g_sets)
	{
		GraphSetProp *gsp = lfirst(le);

		assign_expr_collations(pstate, gsp->expr);
	}

	assign_query_eager(qry);

	targetPerms = ACL_UPDATE;
	if (pstate->p_valid_labels)
		addRangeTableAllModifiedLabels(pstate, qry, NIL, targetPerms);

	qry->rteperminfos = pstate->p_rteperminfos;

	return qry;
}

Query *
transformCypherMergeClause(ParseState *pstate, CypherClause *clause)
{
	CypherMergeClause *detail = (CypherMergeClause *) clause->detail;
	Query	   *qry;
	ParseNamespaceItem *nsitem;
	RangeTblEntry *prev_rte;
	AclMode		targetPerms;

	if (list_length(detail->pattern) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("MERGE can have only one path")));

	qry = makeNode(Query);
	qry->commandType = CMD_GRAPHWRITE;
	qry->g_writeOp = GWROP_MERGE;
	qry->g_last = (pstate->parentParseState == NULL);

	nsitem = transformClauseBy(pstate, (Node *) clause, transformMergeMatch);
	Assert(nsitem->p_rte->rtekind == RTE_SUBQUERY);

	/*
	 * Since they will be created if non-existent
	 */
	pstate->p_valid_labels = true;
	qry->targetList = makeTargetListFromNSItem(pstate, nsitem);
	targetPerms = ACL_INSERT;

	/*
	 * Make an expression list to create the MERGE path. We assume that the
	 * previous clause is the first RTE of MERGE MATCH.
	 */
	prev_rte = rt_fetch(1, nsitem->p_rte->subquery->rtable);
	qry->g_pattern = transformMergeCreate(pstate, detail->pattern,
										  prev_rte, qry->targetList);

	if (detail->sets)
		targetPerms |= ACL_UPDATE;
	qry->g_sets = transformMergeOnSet(pstate, detail->sets);
	qry->g_nr_modify = pstate->p_nr_modify_clause++;

	qry->targetList = (List *) resolve_future_vertex(pstate,
													 (Node *) qry->targetList,
													 FVR_DONT_RESOLVE);
	markTargetListOrigins(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, pstate->p_resolved_qual);
	qry->hasSubLinks = pstate->p_hasSubLinks;

	pstate->p_hasGraphwriteClause = true;
	qry->hasGraphwriteClause = pstate->p_hasGraphwriteClause;

	assign_query_collations(pstate, qry);

	assign_query_eager(qry);

	addRangeTableAllModifiedLabels(pstate, qry, pstate->p_target_labels,
								   targetPerms);
	qry->rteperminfos = pstate->p_rteperminfos;

	return qry;
}

Query *
transformCypherLoadClause(ParseState *pstate, CypherClause *clause)
{
	CypherLoadClause *detail = (CypherLoadClause *) clause->detail;
	RangeVar   *rv = detail->relation;
	Query	   *qry;
	ParseNamespaceItem *nsitem;
	TargetEntry *te;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	if (clause->prev != NULL)
	{
		nsitem = transformClause(pstate, clause->prev);

		qry->targetList = makeTargetListFromNSItem(pstate, nsitem);
	}

	if (findTarget(qry->targetList, rv->alias->aliasname) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_ALIAS),
				 errmsg("duplicate variable \"%s\"", rv->alias->aliasname)));

	nsitem = addRangeTableEntry(pstate, rv, rv->alias, rv->inh, true);
	addNSItemToJoinlist(pstate, nsitem, false);

	te = makeWholeRowTarget(pstate, nsitem);
	qry->targetList = lappend(qry->targetList, te);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);
	qry->rteperminfos = pstate->p_rteperminfos;
	qry->hasGraphwriteClause = pstate->p_hasGraphwriteClause;

	assign_query_collations(pstate, qry);

	return qry;
}

Query *
transformCypherUnwindClause(ParseState *pstate, CypherClause *clause)
{
	CypherUnwindClause *detail = (CypherUnwindClause *) clause->detail;
	Query	   *qry;
	ResTarget  *target;
	int			targetloc;
	Node	   *expr;
	Oid			type;
	char	   *funcname = NULL;
	FuncCall   *unwind;
	ParseExprKind sv_expr_kind;
	Node	   *last_srf;
	Node	   *funcexpr;
	TargetEntry *te;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	/*
	 * Get all the target variables from the previous clause and add them to
	 * this query as target variables so that next clauses can access them.
	 */
	if (clause->prev != NULL)
	{
		ParseNamespaceItem *nsitem;

		nsitem = transformClause(pstate, clause->prev);
		qry->targetList = makeTargetListFromNSItem(pstate, nsitem);
	}

	target = detail->target;
	targetloc = exprLocation((Node *) target);

	/*--------------------------
	 * If the name (e.g. "n" in "UNWNID v AS n") is the same with the name of
	 * targets from the previous clause, throw an error.
	 *
	 * e.g. MATCH (n)-[]->(m) UNWIND n.a AS m ...
	 *                     ^                ^
	 *--------------------------
	 */
	if (findTarget(qry->targetList, target->name) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_ALIAS),
				 errmsg("duplicate variable \"%s\"", target->name),
				 parser_errposition(pstate, targetloc)));

	expr = transformCypherExpr(pstate, target->val, EXPR_KIND_SELECT_TARGET);
	type = exprType(expr);
	if (type == JSONBOID)
	{
		/*
		 * Only jsonb array works. It throws an error for all other types.
		 * This is the best because we don't know the actual value in the
		 * jsonb value at this point.
		 */
		funcname = "jsonb_array_elements";
	}
	else if (type_is_array(type))
	{
		funcname = "unnest";
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("expression must be jsonb or array, but %s",
						format_type_be(type)),
				 parser_errposition(pstate, targetloc)));
	}
	unwind = makeFuncCall(list_make1(makeString(funcname)), NIL,
						  COERCE_EXPLICIT_CALL, -1);

	/*
	 * The logic here is the same with the one in transformTargetEntry(). We
	 * cannot use this function because we already transformed the target
	 * expression above to get the type of it.
	 */
	sv_expr_kind = pstate->p_expr_kind;
	pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;
	last_srf = pstate->p_last_srf;
	funcexpr = ParseFuncOrColumn(pstate, unwind->funcname, list_make1(expr),
								 last_srf, unwind, false, targetloc);
	pstate->p_expr_kind = sv_expr_kind;
	te = makeTargetEntry((Expr *) funcexpr,
						 (AttrNumber) pstate->p_next_resno++,
						 target->name, false);

	qry->targetList = lappend(qry->targetList, te);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);
	qry->rteperminfos = pstate->p_rteperminfos;
	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;

	qry->hasGraphwriteClause = pstate->p_hasGraphwriteClause;

	assign_query_collations(pstate, qry);

	return qry;
}

/*
 * transformCypherForClause
 *		Transform a GQL FOR clause: "FOR x IN array [WITH OFFSET [AS o]]".
 *		The array expression is unnested as a LATERAL set-returning function
 *		joined with the working table, optionally exposing each element's
 *		0-based position via WITH OFFSET.  The expression is wrapped in a
 *		CypherGenericExpr so it is transformed in Cypher context (yielding a
 *		jsonb array) inside the SQL subquery.
 */
Query *
transformCypherForClause(ParseState *pstate, CypherClause *clause)
{
	CypherForClause *detail = (CypherForClause *) clause->detail;
	Query	   *qry;
	Query	   *subqry;
	CypherGenericExpr *cge;
	RangeFunction *rf;
	SelectStmt *subquery;
	List	   *colnames;
	List	   *targetList;
	ParseNamespaceItem *nsitem;
	bool		with_offset = (detail->offset != NULL);

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	/* Pass through the previous clause's variables. */
	if (clause->prev != NULL)
	{
		nsitem = transformClause(pstate, clause->prev);
		qry->targetList = makeTargetListFromNSItem(pstate, nsitem);
	}

	/* The new variable(s) must not collide with previous ones. */
	if (findTarget(qry->targetList, strVal(detail->resname)) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_ALIAS),
				 errmsg("duplicate variable \"%s\"", strVal(detail->resname))));
	if (with_offset &&
		(strcmp(strVal(detail->resname), strVal(detail->offset)) == 0 ||
		 findTarget(qry->targetList, strVal(detail->offset)) != NULL))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_ALIAS),
				 errmsg("duplicate variable \"%s\"", strVal(detail->offset))));

	cge = makeNode(CypherGenericExpr);
	cge->expr = detail->expr;

	colnames = list_make1(detail->resname);
	targetList = list_make1(makeResTarget(makeColumnRef(list_make1(detail->resname)),
										  NULL));
	if (with_offset)
	{
		Node	   *zero_based;

		colnames = lappend(colnames, detail->offset);

		/*
		 * Per the GQL standard the offset is 0-based, but the ordinality
		 * column produced by WITH ORDINALITY is 1-based.  Project (ordinality
		 * - 1) under the offset variable's name.
		 */
		zero_based = (Node *) makeSimpleA_Expr(AEXPR_OP, "-",
											   makeColumnRef(list_make1(detail->offset)),
											   makeIntConst(1, -1), -1);
		targetList = lappend(targetList,
							 makeResTarget(zero_based, strVal(detail->offset)));
	}

	rf = makeRangeFunction(list_make1(makeString("unnest")), (Node *) cge,
						   makeAliasNoDup(AGENS_DEFAULT_PREFIX "for", colnames),
						   with_offset);

	subquery = makeNode(SelectStmt);
	subquery->targetList = targetList;
	subquery->fromClause = list_make1(rf);

	/*
	 * Analyze the unnest sub-SELECT and join it with the working table.  When
	 * there is a previous clause the subquery is LATERAL, since the array
	 * expression may reference the previous clause's columns.
	 */
	Assert(pstate->p_expr_kind == EXPR_KIND_NONE);
	pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;
	pstate->p_lateral_active = (clause->prev != NULL);

	subqry = parse_sub_analyze((Node *) subquery, pstate, NULL, false, true);

	nsitem = addRangeTableEntryForSubquery(pstate, subqry,
										   makeAliasNoDup(CYPHER_FOR_ALIAS, NIL),
										   pstate->p_lateral_active, true);

	pstate->p_lateral_active = false;
	pstate->p_expr_kind = EXPR_KIND_NONE;

	addNSItemToJoinlist(pstate, nsitem, true);

	qry->targetList = list_concat(qry->targetList,
								  makeTargetListFromNSItem(pstate, nsitem));

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);
	qry->rteperminfos = pstate->p_rteperminfos;
	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
	qry->hasGraphwriteClause = pstate->p_hasGraphwriteClause;

	assign_query_collations(pstate, qry);

	return qry;
}

/*
 * transformCypherYieldCallClause
 *		CALL func(args) YIELD ... -- invoke a table-returning routine and add its
 *		yielded columns to the working table.
 *
 *		The routine is a set-returning or composite-returning function; the call
 *		is analyzed as a function scan and joined in as a subquery RTE, exactly
 *		like the FOR clause's unnest.  It is LATERAL when a previous clause
 *		exists, so the arguments may reference the outer variables; the YIELD
 *		list projects (and optionally renames) the routine's output columns,
 *		which then surface to the clauses that follow.
 */
Query *
transformCypherYieldCallClause(ParseState *pstate, CypherClause *clause)
{
	CypherYieldCallClause *detail = (CypherYieldCallClause *) clause->detail;
	Query	   *qry;
	Query	   *subqry;
	FuncCall   *fc;
	RangeFunction *rf;
	SelectStmt *subquery;
	List	   *args = NIL;
	List	   *body_tlist;
	ParseNamespaceItem *nsitem;
	ListCell   *lc;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	/* Pass through the previous clause's variables. */
	if (clause->prev != NULL)
	{
		nsitem = transformClause(pstate, clause->prev);
		qry->targetList = makeTargetListFromNSItem(pstate, nsitem);
	}

	/* Wrap each argument so it is analyzed with Cypher expression semantics. */
	foreach(lc, detail->args)
	{
		CypherGenericExpr *cge = makeNode(CypherGenericExpr);

		cge->expr = lfirst(lc);
		args = lappend(args, cge);
	}

	fc = makeFuncCall(detail->funcname, args, COERCE_EXPLICIT_CALL,
					  detail->location);

	rf = makeNode(RangeFunction);
	rf->lateral = false;
	rf->ordinality = false;
	rf->is_rowsfrom = false;
	rf->functions = list_make1(list_make2(fc, NIL));
	/*
	 * No alias on the function scan: the YIELD items reference the routine's
	 * own output column names (its OUT parameters / composite fields, or the
	 * routine name for a single-column set-returning function).  An alias here
	 * would rename that single column and hide the natural name.
	 */
	rf->alias = NULL;
	rf->coldeflist = NIL;

	/* SELECT <yield items> FROM func(args) */
	subquery = makeNode(SelectStmt);
	subquery->targetList = detail->yielditems;
	subquery->fromClause = list_make1(rf);

	/*
	 * Analyze the routine invocation and join it with the working table.  It is
	 * LATERAL when there is a previous clause, since the arguments may reference
	 * the previous clause's columns.
	 */
	Assert(pstate->p_expr_kind == EXPR_KIND_NONE);
	pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;
	pstate->p_lateral_active = (clause->prev != NULL);

	subqry = parse_sub_analyze((Node *) subquery, pstate, NULL, false, true);

	pstate->p_lateral_active = false;
	pstate->p_expr_kind = EXPR_KIND_NONE;

	nsitem = addRangeTableEntryForSubquery(pstate, subqry,
										   makeAliasNoDup(CYPHER_YIELD_ALIAS, NIL),
										   (clause->prev != NULL), true);
	addNSItemToJoinlist(pstate, nsitem, true);

	/*
	 * A yielded column may not reuse a variable name already bound in the outer
	 * query (it would shadow the outer one for the clauses that follow), nor may
	 * two yielded columns share a name.  Rename with YIELD ... AS to resolve a
	 * collision.
	 */
	body_tlist = makeTargetListFromNSItem(pstate, nsitem);
	foreach(lc, body_tlist)
	{
		TargetEntry *yte = (TargetEntry *) lfirst(lc);
		ListCell   *lc2;

		if (yte->resjunk || yte->resname == NULL)
			continue;
		if (findTarget(qry->targetList, yte->resname) != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_ALIAS),
					 errmsg("variable \"%s\" yielded by CALL is already bound in the outer query",
							yte->resname),
					 parser_errposition(pstate, detail->location)));

		for_each_cell(lc2, body_tlist, lnext(body_tlist, lc))
		{
			TargetEntry *yte2 = (TargetEntry *) lfirst(lc2);

			if (!yte2->resjunk && yte2->resname != NULL &&
				strcmp(yte->resname, yte2->resname) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_ALIAS),
						 errmsg("variable \"%s\" is yielded more than once by CALL",
								yte->resname),
						 parser_errposition(pstate, detail->location)));
		}
	}

	qry->targetList = list_concat(qry->targetList, body_tlist);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);
	qry->rteperminfos = pstate->p_rteperminfos;
	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
	qry->hasGraphwriteClause = pstate->p_hasGraphwriteClause;

	assign_query_collations(pstate, qry);

	return qry;
}

/*
 * callImportIsStar
 *		The CALL (*) sentinel: an import list that is the single A_Star node,
 *		meaning "import every outer variable".  makeCallImportNSItem skips its
 *		restriction for it so no outer column is hidden from the body.
 */
static bool
callImportIsStar(List *importlist)
{
	return list_length(importlist) == 1 && IsA(linitial(importlist), A_Star);
}

/*
 * makeCallImportNSItem
 *		Build a namespace item exposing ONLY the variables named in a CALL
 *		(var, ...) scope clause, so the subquery body may reference the imported
 *		outer variables but nothing else.  CALL (*) is the exception: it exposes
 *		every outer variable, like an importing WITH that lists them all.
 *
 *		Column resolution finds a name by its position in the range-table
 *		entry's column list and then reads the matching ParseNamespaceColumn; an
 *		entry whose p_varno is 0 is treated as "does not exist".  So we copy the
 *		previous clause's nsitem verbatim -- crucially keeping its p_rte, which
 *		make_var/GetSubLevelsUpByNSItem matches by identity against the range
 *		table to find the subquery level (a fresh copy would break that and a
 *		body that merely RETURNs an imported value, with no MATCH, would fail
 *		with "RTE not found") -- and merely invalidate the per-column entries of
 *		the columns that were not imported.  A reference to a non-imported outer
 *		variable then raises a "does not exist" error instead of silently
 *		capturing it (or, for an uncorrelated CALL, reaching the planner as a
 *		stray outer reference inside a non-lateral subquery -- a crash).
 */
static ParseNamespaceItem *
makeCallImportNSItem(ParseState *pstate, ParseNamespaceItem *prev_nsitem,
					 List *importlist, int location)
{
	ParseNamespaceItem *nsitem;
	ParseNamespaceColumn *nscolumns;
	List	   *erefcolnames = prev_nsitem->p_rte->eref->colnames;
	int			ncols = list_length(erefcolnames);
	bool		import_all = callImportIsStar(importlist);
	int			idx;
	ListCell   *lc;

	/* Every imported variable must name an existing outer variable. */
	if (!import_all)
	{
		foreach(lc, importlist)
		{
			char	   *varname = strVal(lfirst(lc));
			ListCell   *cn;
			bool		found = false;

			foreach(cn, erefcolnames)
			{
				if (strcmp(strVal(lfirst(cn)), varname) == 0)
				{
					found = true;
					break;
				}
			}
			if (!found)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("variable \"%s\" to import into CALL does not exist",
								varname),
						 parser_errposition(pstate, location)));
		}
	}

	/*
	 * Copy prev's per-column data, then invalidate the non-imported columns.
	 * CALL (*) imports every column, so nothing is invalidated.
	 */
	nscolumns = (ParseNamespaceColumn *)
		palloc(ncols * sizeof(ParseNamespaceColumn));
	memcpy(nscolumns, prev_nsitem->p_nscolumns,
		   ncols * sizeof(ParseNamespaceColumn));

	idx = 0;
	foreach(lc, erefcolnames)
	{
		char	   *cname = strVal(lfirst(lc));
		bool		imported = import_all;
		ListCell   *il;

		if (!import_all && cname[0] != '\0')
		{
			foreach(il, importlist)
			{
				if (strcmp(strVal(lfirst(il)), cname) == 0)
				{
					imported = true;
					break;
				}
			}
		}
		if (!imported)
			nscolumns[idx].p_varno = 0;
		idx++;
	}

	nsitem = (ParseNamespaceItem *) palloc(sizeof(ParseNamespaceItem));
	*nsitem = *prev_nsitem;
	nsitem->p_nscolumns = nscolumns;
	nsitem->p_rel_visible = false;
	nsitem->p_cols_visible = true;
	nsitem->p_lateral_only = false;
	nsitem->p_lateral_ok = true;

	return nsitem;
}

/*
 * transformCypherCallClause
 *		CALL { <read subquery> } -- run a subquery as a clause in the pipeline.
 *
 *		The subquery is analyzed into a self-contained Query and joined with the
 *		working table as a subquery RTE.  It is LATERAL when the CALL imports
 *		outer variables (correlated), so the planner can decorrelate it to a
 *		hash/merge join when the body is a plain pattern match, or run it as a
 *		per-row lateral join when the body cannot be decorrelated (aggregation,
 *		ORDER BY/LIMIT, DISTINCT, UNION).  A returning subquery joins outer x
 *		inner; an inner aggregation yields exactly one row per outer row.  The
 *		subquery's RETURN columns surface to the clauses that follow.
 *
 *		Phase 1 is read-only: cypher_read_stmt already excludes write clauses,
 *		and a graph-write body is rejected defensively below.
 */
Query *
transformCypherCallClause(ParseState *pstate, CypherClause *clause)
{
	CypherCallClause *detail = (CypherCallClause *) clause->detail;
	Query	   *qry;
	Query	   *subqry;
	ParseNamespaceItem *nsitem;
	ParseNamespaceItem *prev_nsitem = NULL;
	List	   *saved_namespace;
	bool		lateral;
	bool		has_col;
	ListCell   *lc;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	/* Pass through the previous clause's variables. */
	if (clause->prev != NULL)
	{
		prev_nsitem = transformClause(pstate, clause->prev);
		qry->targetList = makeTargetListFromNSItem(pstate, prev_nsitem);
	}
	else if (detail->importlist != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("CALL has no preceding clause to import variables from"),
				 parser_errposition(pstate, detail->location)));

	/*
	 * Analyze the subquery and join it in.  It is LATERAL exactly when it
	 * imports outer variables; an uncorrelated CALL is an ordinary subquery
	 * the planner may flatten.
	 */
	lateral = (detail->importlist != NIL);

	/*
	 * Restrict the outer variables visible inside the subquery to exactly the
	 * imported ones: a correlated CALL (var, ...) sees only its imports,
	 * while an uncorrelated CALL () / CALL sees no outer variables at all.
	 * Without this, a body referencing a non-imported outer variable would
	 * silently capture it -- or, for an uncorrelated CALL, reach the planner
	 * as a malformed outer reference inside a non-lateral subquery (a crash).
	 */
	saved_namespace = pstate->p_namespace;
	if (lateral)
		pstate->p_namespace =
			list_make1(makeCallImportNSItem(pstate, prev_nsitem,
											detail->importlist, detail->location));
	else
		pstate->p_namespace = NIL;

	Assert(pstate->p_expr_kind == EXPR_KIND_NONE);
	pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;
	pstate->p_lateral_active = lateral;

	subqry = parse_sub_analyze(detail->subquery, pstate, NULL, false, true);

	pstate->p_lateral_active = false;
	pstate->p_expr_kind = EXPR_KIND_NONE;
	pstate->p_namespace = saved_namespace;

	/* A read-only CALL: reject a write body (the grammar also excludes it). */
	if (subqry->commandType != CMD_SELECT)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("a write clause is not allowed in a CALL subquery"),
				 parser_errposition(pstate, detail->location)));

	/*
	 * The subquery must RETURN something (a FINISH-terminated body yields
	 * none).
	 */
	has_col = false;
	foreach(lc, subqry->targetList)
	{
		if (!((TargetEntry *) lfirst(lc))->resjunk)
		{
			has_col = true;
			break;
		}
	}
	if (!has_col)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("CALL subquery must RETURN at least one variable"),
				 parser_errposition(pstate, detail->location)));

	nsitem = addRangeTableEntryForSubquery(pstate, subqry,
										   makeAliasNoDup(CYPHER_CALL_ALIAS, NIL),
										   lateral, true);
	addNSItemToJoinlist(pstate, nsitem, true);

	/*
	 * A CALL subquery may not return a variable already bound in the outer
	 * query: it would shadow the outer one for the clauses that follow the
	 * CALL.  Reject the collision -- without this an uncorrelated body
	 * silently shadows the outer variable, while a correlated one fails later
	 * with a confusing type error.  To return an imported variable the body
	 * must alias it to a fresh name.
	 */
	{
		List	   *body_tlist = makeTargetListFromNSItem(pstate, nsitem);
		ListCell   *bl;

		foreach(bl, body_tlist)
		{
			TargetEntry *bte = (TargetEntry *) lfirst(bl);

			if (bte->resjunk || bte->resname == NULL || bte->resname[0] == '\0')
				continue;
			if (findTarget(qry->targetList, bte->resname) != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_ALIAS),
						 errmsg("variable \"%s\" returned by the CALL subquery is already bound in the outer query",
								bte->resname),
						 parser_errposition(pstate, detail->location)));
		}

		qry->targetList = list_concat(qry->targetList, body_tlist);
	}

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);
	qry->rteperminfos = pstate->p_rteperminfos;
	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
	qry->hasGraphwriteClause = pstate->p_hasGraphwriteClause;

	assign_query_collations(pstate, qry);

	return qry;
}

/*
 * makeCountTargetEntry
 *		Build a "count(*)" target entry (resno 1) in pstate.  Shared by the
 *		CSP_SIZE subpattern path and the COUNT subquery.
 */
TargetEntry *
makeCountTargetEntry(ParseState *pstate)
{
	FuncCall   *count;

	count = makeFuncCall(list_make1(makeString("count")), NIL,
						 COERCE_EXPLICIT_CALL, -1);
	count->agg_star = true;

	pstate->p_next_resno = 1;
	return transformTargetEntry(pstate, (Node *) count, NULL,
								EXPR_KIND_SELECT_TARGET, NULL, false);
}

/*
 * transformCypherCountClause
 *		Transform a COUNT subquery over an arbitrary read statement: count how
 *		many rows it yields.  The statement is analyzed into a self-contained
 *		Query and joined in as a (non-lateral) subquery RTE, then count(*) is
 *		taken over it -- so the statement's own DISTINCT / LIMIT / GROUP BY /
 *		UNION are honored.  Outer variables resolve through the parent ParseState
 *		chain (ordinary correlated-sublink resolution), so the RTE is not lateral.
 */
Query *
transformCypherCountClause(ParseState *pstate, CypherCountClause *clause)
{
	Query	   *qry;
	Query	   *subqry;
	ParseNamespaceItem *nsitem;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	Assert(pstate->p_expr_kind == EXPR_KIND_NONE);
	pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;
	subqry = parse_sub_analyze(clause->subquery, pstate, NULL, false, true);
	pstate->p_expr_kind = EXPR_KIND_NONE;

	if (!IsA(subqry, Query) || subqry->commandType != CMD_SELECT)
		elog(ERROR, "unexpected non-SELECT command in COUNT subquery");

	nsitem = addRangeTableEntryForSubquery(pstate, subqry,
										   makeAliasNoDup(CYPHER_COUNT_ALIAS, NIL),
										   false, true);
	addNSItemToJoinlist(pstate, nsitem, false);

	qry->targetList = list_make1(makeCountTargetEntry(pstate));
	markTargetListOrigins(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);
	qry->rteperminfos = pstate->p_rteperminfos;
	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasAggs = pstate->p_hasAggs;
	if (qry->hasAggs)
		parseCheckAggregates(pstate, qry);

	assign_query_collations(pstate, qry);

	return qry;
}

/*
 * transformCypherModifier
 *		Transform a standalone ORDER BY / SKIP|OFFSET / LIMIT run (folded into a
 *		CypherModifier by preprocess_modifiers()).  All variables from the
 *		previous clause are passed through unchanged; the modifier only orders
 *		and pages the rows.
 */
Query *
transformCypherModifier(ParseState *pstate, CypherClause *clause)
{
	CypherModifier *detail = (CypherModifier *) clause->detail;
	Query	   *qry;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	if (clause->prev != NULL)
	{
		ParseNamespaceItem *nsitem;

		nsitem = transformClause(pstate, clause->prev);
		qry->targetList = makeTargetListFromNSItem(pstate, nsitem);
	}

	qry->sortClause = transformCypherOrderBy(pstate, detail->order,
											 &qry->targetList);

	qry->limitOffset = transformCypherLimit(pstate, detail->skip,
											EXPR_KIND_OFFSET, "SKIP/OFFSET");
	qry->limitOffset = resolve_future_vertex(pstate, qry->limitOffset, 0);

	qry->limitCount = transformCypherLimit(pstate, detail->limit,
										   EXPR_KIND_LIMIT, "LIMIT");
	qry->limitCount = resolve_future_vertex(pstate, qry->limitCount, 0);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);
	qry->rteperminfos = pstate->p_rteperminfos;
	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
	qry->hasGraphwriteClause = pstate->p_hasGraphwriteClause;

	assign_query_collations(pstate, qry);

	return qry;
}

/*
 * transformCypherFilterClause
 *		Transform a standalone FILTER [WHERE] <expr> clause.  Unlike a WHERE
 *		that is part of its containing statement, FILTER is evaluated as a
 *		separate stage: it passes through the previous clause's variables and
 *		applies the condition as this query's WHERE qualification.
 */
Query *
transformCypherFilterClause(ParseState *pstate, CypherClause *clause)
{
	CypherFilterClause *detail = (CypherFilterClause *) clause->detail;
	Query	   *qry;
	Node	   *qual;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	if (clause->prev != NULL)
	{
		ParseNamespaceItem *nsitem;

		nsitem = transformClause(pstate, clause->prev);
		qry->targetList = makeTargetListFromNSItem(pstate, nsitem);
	}

	qual = transformCypherWhere(pstate, detail->expr, EXPR_KIND_WHERE);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);
	qry->rteperminfos = pstate->p_rteperminfos;
	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
	qry->hasGraphwriteClause = pstate->p_hasGraphwriteClause;

	assign_query_collations(pstate, qry);

	return qry;
}

/*
 * transformCypherSubselectClause
 *		Transform a set-operation (UNION / UNION ALL / INTERSECT / EXCEPT)
 *		carried across a NEXT boundary.  For the left-operand form
 *		(A UNION B NEXT C) the union is the sole driving input of the following
 *		clauses (clause->prev == NULL): analyze the set-op SelectStmt as a
 *		self-contained subquery and return the resulting Query.  The caller
 *		(transformClauseImpl) then wraps it once as the consuming query's
 *		driving range-table entry, so the whole union -- with its UNION /
 *		UNION ALL duplicate-elimination semantics -- becomes the input table
 *		that C's clauses read.
 */
Query *
transformCypherSubselectClause(ParseState *pstate, CypherClause *clause)
{
	CypherSubselectClause *detail = (CypherSubselectClause *) clause->detail;

	/* Left-operand form only for now: the union is the entire input. */
	Assert(clause->prev == NULL);

	return parse_sub_analyze(detail->query, pstate, NULL, false, true);
}

/* check whether resulting columns have a name or not */
static void
checkNameInItems(ParseState *pstate, List *items, List *targetList)
{
	ListCell   *li;
	ListCell   *lt;

	forboth(li, items, lt, targetList)
	{
		ResTarget  *res = lfirst(li);
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

/*
 * checkCypherLetItems
 *		Enforce the two rules that LET adds on top of a plain projection: it is
 *		row-wise (no aggregates) and it may only INTRODUCE names -- it must not
 *		redefine a variable that already exists in the working record, nor bind
 *		the same name twice.  The target list here is the implicit "*" expansion
 *		(every existing binding, always uniquely named) followed by the LET
 *		items, so a duplicate output name can only come from a LET assignment.
 */
static void
checkCypherLetItems(ParseState *pstate, List *targetList)
{
	ListCell   *la;

	if (pstate->p_hasAggs)
		ereport(ERROR,
				(errcode(ERRCODE_GROUPING_ERROR),
				 errmsg("aggregate functions are not allowed in LET")));

	foreach(la, targetList)
	{
		TargetEntry *ta = lfirst(la);
		ListCell   *lb;

		if (ta->resjunk || ta->resname == NULL)
			continue;

		for_each_cell(lb, targetList, lnext(targetList, la))
		{
			TargetEntry *tb = lfirst(lb);

			if (!tb->resjunk && tb->resname != NULL &&
				strcmp(ta->resname, tb->resname) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_ALIAS),
						 errmsg("variable \"%s\" already exists", tb->resname),
						 errhint("LET cannot redefine an existing variable; use a different name.")));
		}
	}
}

/* See transformFromClauseItem() */
static ParseNamespaceItem *
transformMatchOptional(ParseState *pstate, CypherClause *clause)
{
	CypherMatchClause *detail = (CypherMatchClause *) clause->detail;
	ParseNamespaceItem *l_nsitem,
			   *r_nsitem;
	Alias	   *r_alias;
	Alias	   *alias;
	Node	   *prevclause;
	Node	   *qual;

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
		l_nsitem = addRangeTableEntryForSubquery(pstate, qry, l_alias,
												 pstate->p_lateral_active, true);

		addNSItemToJoinlist(pstate, l_nsitem, true);
	}
	else
	{
		l_nsitem = transformClause(pstate, clause->prev);
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
	r_nsitem = transformClauseImpl(pstate, (Node *) clause, transformStmt,
								   r_alias);

	pstate->p_is_optional_match = false;
	pstate->p_lateral_active = false;

	detail->optional = true;
	clause->prev = prevclause;

	qual = makeBoolConst(true, false);
	alias = makeAliasNoDup(CYPHER_SUBQUERY_ALIAS, NIL);

	return incrementalJoinRTEs(pstate, JOIN_LEFT, l_nsitem, r_nsitem, qual,
							   alias);
}

static bool
hasPropConstr(List *pattern)
{
	ListCell   *lp;

	foreach(lp, pattern)
	{
		CypherPath *p = lfirst(lp);
		ListCell   *le;

		foreach(le, p->chain)
		{
			Node	   *elem = lfirst(le);

			if (IsA(elem, CypherNode))
			{
				CypherNode *cnode = (CypherNode *) elem;

				if (cnode->prop_map != NULL)
					return true;
			}
			else
			{
				CypherRel  *crel = (CypherRel *) elem;

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
	ListCell   *le;

	foreach(le, fplist)
	{
		CypherPath *p = lfirst(le);
		CypherRel  *crel;
		char	   *pathname;
		char	   *edgename;
		char	   *weightvar;
		Query	   *fp;
		Alias	   *alias;
		ParseNamespaceItem *nsitem;
		TargetEntry *te;

		if (p->kind == CPATH_DIJKSTRA)
			fp = transformDijkstraInMatch(pstate, p);
		else
			fp = transformShortestPathInMatch(pstate, p);

		alias = makeAliasOptUnique(NULL);
		nsitem = addRangeTableEntryForSubquery(pstate, fp, alias, true, true);
		addNSItemToJoinlist(pstate, nsitem, true);

		pathname = getCypherName(p->variable);
		if (pathname != NULL)
		{
			te = makeTargetEntry((Expr *) getColumnVar(pstate, nsitem,
													   pathname),
								 (AttrNumber) pstate->p_next_resno++,
								 pathname,
								 false);
			*targetList = lappend(*targetList, te);
		}

		if (p->kind == CPATH_DIJKSTRA)
		{
			weightvar = getCypherName(p->weight_var);
			if (weightvar != NULL)
			{
				te = makeTargetEntry((Expr *) getColumnVar(pstate,
														   nsitem,
														   weightvar),
									 (AttrNumber) pstate->p_next_resno++,
									 weightvar,
									 false);
				*targetList = lappend(*targetList, te);
			}
		}
		else
		{
			crel = lsecond(p->chain);
			if (crel->variable != NULL)
			{
				edgename = getCypherName(crel->variable);
				te = makeTargetEntry((Expr *) getColumnVar(pstate,
														   nsitem,
														   edgename),
									 (AttrNumber) pstate->p_next_resno++,
									 edgename,
									 false);
				*targetList = lappend(*targetList, te);
			}
		}
	}
}

static void
collectEntityInfo(ParseState *pstate, List *pattern)
{
	ListCell   *lp;

	foreach(lp, pattern)
	{
		CypherPath *p = lfirst(lp);
		ListCell   *le;

		foreach(le, p->chain)
		{
			Node	   *node = lfirst(le);

			addEntityInfo(pstate, node);
		}
	}
}

static char *
getEntityVarname(Node *entity)
{
	if (IsA(entity, CypherNode))
		return getCypherName(((CypherNode *) entity)->variable);
	else if (IsA(entity, CypherRel))
		return getCypherName(((CypherRel *) entity)->variable);
	else
		elog(ERROR, "unexpected entity type: %d", (int) nodeTag(entity));
}

static char *
getEntityLabname(Node *entity)
{
	if (IsA(entity, CypherNode))
		return getCypherName(((CypherNode *) entity)->label);
	else if (IsA(entity, CypherRel))
	{
		CypherRel  *crel = (CypherRel *) entity;

		if (crel->types == NIL)
			return NULL;

		return getCypherName(linitial(crel->types));
	}
	else
		elog(ERROR, "unexpected entity type: %d", (int) nodeTag(entity));
}

static int
getEntityVarloc(Node *entity)
{
	if (IsA(entity, CypherNode))
		return getCypherNameLoc(((CypherNode *) entity)->variable);
	else if (IsA(entity, CypherRel))
		return getCypherNameLoc(((CypherRel *) entity)->variable);
	else
		elog(ERROR, "unexpected entity type: %d", (int) nodeTag(entity));
}

static bool
hasEntityPropConstr(Node *entity)
{
	if (IsA(entity, CypherNode))
		return (((CypherNode *) entity)->prop_map != NULL);
	else if (IsA(entity, CypherRel))
		return (((CypherRel *) entity)->prop_map != NULL);
	else
		elog(ERROR, "unexpected entity type: %d", (int) nodeTag(entity));
}

static void
addEntityInfo(ParseState *pstate, Node *entity)
{
	char	   *varname;
	char	   *labname;
	EntityInfo *ei;
	int			varloc;
	NodeTag		entity_type = nodeTag(entity);

	Assert(entity_type == T_CypherNode ||
		   entity_type == T_CypherRel);

	varname = getEntityVarname(entity);
	labname = getEntityLabname(entity);
	varloc = getEntityVarloc(entity);
	if (varname == NULL)
		return;

	ei = findEntityInfo(pstate, varname, entity_type, true);
	if (ei == NULL)
	{
		ei = palloc(sizeof(EntityInfo));
		ei->type = entity_type;
		ei->varname = varname;
		ei->labname = labname;
		ei->local = true;
		ei->prop_constr = hasEntityPropConstr(entity);

		pstate->p_entity_info_list = lappend(pstate->p_entity_info_list, ei);
		return;
	}
	else if (ei->type == T_CypherRel)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_ALIAS),
				 errmsg("duplicate variable \"%s\"", varname),
				 parser_errposition(pstate, varloc)));
	else if (ei->labname == NULL && labname != NULL)
		ei->labname = labname;
	else if (labname != NULL &&
			 strcmp(ei->labname, labname) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("label conflict on node \"%s\"", varname),
				 parser_errposition(pstate, varloc)));

	ei->prop_constr = (ei->prop_constr || hasEntityPropConstr(entity));
}

static EntityInfo *
getEntityInfo(ParseState *pstate, char *varname,
			  NodeTag entity_type, bool local_only)
{
	EntityInfo *ei;

	if (varname == NULL)
		return NULL;

	ei = findEntityInfo(pstate, varname, entity_type, local_only);

	return ei;
}

static EntityInfo *
findEntityInfo(ParseState *pstate, char *varname, NodeTag entity_type,
			   bool local_only)
{
	ListCell   *le;

	while (pstate != NULL)
	{
		foreach(le, pstate->p_entity_info_list)
		{
			EntityInfo *ei = lfirst(le);

			if (strcmp(ei->varname, varname) == 0 &&
				(ei->type == entity_type) &&
				(ei->local == local_only))
				return ei;
		}

		if (local_only)
			break;

		pstate = pstate->parentParseState;
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
		 * if there is no matched connected component, make a new connected
		 * component which is a list of CypherPath's
		 */
		if (repr == NIL)
		{
			c = list_make1(p);
			components = lappend(components, c);
			continue;
		}

		/* find other connected components and merge them to `repr` */
		Assert(lc != NULL);
		for_each_cell(lc, components, lnext(components, lc))
		{
			c = lfirst(lc);

			if (isPathConnectedTo(p, c))
			{
				repr = list_concat(repr, c);

				components = foreach_delete_current(components, lc);
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
	ListCell   *lp;

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
	ListCell   *le1;

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

/* labid of an ag_vertex/ag_edge child relation, or 0 if it doesn't resolve. */
static int
graphElemLabid(Oid relid)
{
	HeapTuple	tup;
	int			labid = 0;

	tup = SearchSysCache1(LABELRELID, ObjectIdGetDatum(relid));
	if (HeapTupleIsValid(tup))
	{
		labid = (int) ((Form_ag_label) GETSTRUCT(tup))->labid;
		ReleaseSysCache(tup);
	}
	return labid;
}

/* rangetable index / RTE of a transformed element, or 0/NULL if not an nsitem. */
static int
graphElemRti(Node *elem, bool is_nsitem)
{
	return is_nsitem ? ((ParseNamespaceItem *) elem)->p_rtindex : 0;
}

static RangeTblEntry *
graphElemRte(Node *elem, bool is_nsitem)
{
	return is_nsitem ? ((ParseNamespaceItem *) elem)->p_rte : NULL;
}

/*
 * recordGraphmetaNode / recordGraphmetaEdge
 *		Record a MATCH pattern element's connectivity-independent topology on its
 *		RTE, for the planner's graphmeta constraint-propagation pre-pass (see
 *		propagate_graphmeta_constraints / expand_inherited_rtentry).
 *
 * Nodes record their own label id (0 = the universal ag_vertex parent, i.e.
 * "unlabelled / any vertex").  Edges record their label id (0 = unlabelled),
 * direction, kind (directed relation vs undirected/VLE subquery), and the
 * element ids of their two endpoint nodes.  All of this is structural
 * (derived from the pattern text, not from connectivity), so it never goes stale
 * behind a cached plan; the planner resolves it against ag_graphmeta each plan.
 */
static void
recordGraphmetaNode(Node *vertex, bool vertex_is_nsitem,
					Oid graphoid, Oid agvertex_relid)
{
	RangeTblEntry *rte = graphElemRte(vertex, vertex_is_nsitem);

	if (rte == NULL || rte->rtekind != RTE_RELATION || !OidIsValid(graphoid))
		return;

	rte->graphPruneGraph = graphoid;
	rte->graphPruneRole = GRAPHPRUNE_ROLE_NODE;
	rte->graphPruneElemId = graphElemRti(vertex, vertex_is_nsitem);
	rte->graphPruneLabid = (rte->relid == agvertex_relid) ? 0
		: graphElemLabid(rte->relid);
}

static void
recordGraphmetaEdge(Node *edge, bool edge_is_nsitem, CypherRel *crel,
					Oid graphoid, int src_elemid, int dst_elemid)
{
	RangeTblEntry *rte = graphElemRte(edge, edge_is_nsitem);
	char	   *typname;

	if (rte == NULL || !OidIsValid(graphoid))
		return;

	getCypherRelType(crel, &typname, NULL);

	rte->graphPruneGraph = graphoid;
	rte->graphPruneElemId = graphElemRti(edge, edge_is_nsitem);
	rte->graphPruneLabid = (strcmp(typname, AG_EDGE) == 0) ? 0
		: (int) get_labname_labid(typname, graphoid);
	rte->graphPruneSrcElemId = src_elemid;
	rte->graphPruneDstElemId = dst_elemid;
	rte->graphPruneDir = crel->direction;
	rte->graphPruneVleMin = -1;

	if (crel->varlen != NULL)
	{
		A_Const    *lidx = (A_Const *) ((A_Indices *) crel->varlen)->lidx;

		/*
		 * Record the real lower hop bound (the grammar sets lidx to a
		 * constant, defaulting to 1 when omitted).  The planner prunes a
		 * VLE's endpoints only when this is >= 1; a *0.. VLE (which may match
		 * a zero-length path) keeps the barrier behaviour via the -1 sentinel
		 * above if lidx is absent.
		 */
		if (lidx != NULL)
			rte->graphPruneVleMin = intVal(&lidx->val);
		rte->graphPruneRole = GRAPHPRUNE_ROLE_VLE;
	}
	else if (crel->direction == CYPHER_REL_DIR_NONE)
		rte->graphPruneRole = GRAPHPRUNE_ROLE_UNDIR_EDGE;
	else
		rte->graphPruneRole = GRAPHPRUNE_ROLE_DIR_EDGE;
}

static Node *
transformComponents(ParseState *pstate, List *components, List **targetList)
{
	List	   *eqoList = NIL;
	Node	   *qual = NULL;
	ListCell   *lc;
	ListCell   *leqo;
	Oid			gm_graphoid = InvalidOid;
	Oid			gm_agvertex_relid = InvalidOid;

	/* Graph context for graphmeta scan-pruning topology (recordGraphmeta*). */
	if (pstate->p_valid_labels)
	{
		gm_graphoid = get_graph_path_oid();
		if (OidIsValid(gm_graphoid))
			gm_agvertex_relid = get_labid_relid(gm_graphoid,
												get_labname_labid(AG_VERTEX,
																  gm_graphoid));
	}

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
			bool		out;
			TargetEntry *te;
			ListCell   *le;
			CypherNode *cnode;
			Node	   *vertex;
			bool		vertex_is_nsitem;
			CypherRel  *prev_crel = NULL;
			Node	   *prev_edge = NULL;
			bool		prev_edge_is_nsitem;
			Node	   *src_vertex = NULL;	/* source node of prev_crel */
			bool		src_vertex_is_nsitem = false;
			Node	   *pvs = makeArrayExpr(VERTEXARRAYOID, VERTEXOID, NIL);
			Node	   *pes = makeArrayExpr(EDGEARRAYOID, EDGEOID, NIL);

			out = (pathname != NULL ||
				   p->kind == CPATH_SHORTEST ||
				   p->kind == CPATH_SHORTEST_ALL);

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
				CypherRel  *crel;
				Node	   *edge;
				bool		edge_is_nsitem;

				cnode = lfirst(le);

				/* `cnode` is the first node in the path */
				if (prev_crel == NULL)
				{
					le = lnext(p->chain, le);

					/* vertex only path */
					if (le == NULL)
					{
						vertex = transformMatchNode(pstate, cnode,
													targetList, &eqoList,
													&vertex_is_nsitem);
						break;
					}

					crel = lfirst(le);

					/*
					 * if `crel` is zero-length VLE, get RTE of `cnode`
					 * because `crel` needs `id` column of the RTE
					 */
					vertex = transformMatchNode(pstate, cnode, targetList,
												&eqoList, &vertex_is_nsitem);

					if (p->kind != CPATH_NORMAL)
					{
						le = lnext(p->chain, le);
						continue;
					}

					setInitialVidForVLE(pstate, crel, vertex,
										vertex_is_nsitem, NULL, NULL, false);
					edge = transformMatchRel(pstate, crel, targetList,
											 &eqoList, out, &edge_is_nsitem);

					/*
					 * graphmeta: record this (first) node; it is the source
					 * of `crel`, whose edge topology is completed once its
					 * dest node (next in the chain) is transformed.
					 */
					recordGraphmetaNode(vertex, vertex_is_nsitem,
										gm_graphoid, gm_agvertex_relid);
					src_vertex = vertex;
					src_vertex_is_nsitem = vertex_is_nsitem;

					qual = addQualNodeIn(pstate, qual, vertex,
										 vertex_is_nsitem, crel, edge,
										 edge_is_nsitem, false);
				}
				else
				{
					vertex = transformMatchNode(pstate, cnode, targetList,
												&eqoList, &vertex_is_nsitem);

					/*
					 * graphmeta: record this node, then complete
					 * `prev_crel`'s edge topology now that both its endpoints
					 * are known (src = src_vertex, dst = this node).
					 */
					recordGraphmetaNode(vertex, vertex_is_nsitem,
										gm_graphoid, gm_agvertex_relid);
					recordGraphmetaEdge(prev_edge, prev_edge_is_nsitem, prev_crel,
										gm_graphoid,
										graphElemRti(src_vertex, src_vertex_is_nsitem),
										graphElemRti(vertex, vertex_is_nsitem));

					qual = addQualNodeIn(pstate, qual, vertex,
										 vertex_is_nsitem, prev_crel,
										 prev_edge, prev_edge_is_nsitem, true);

					le = lnext(p->chain, le);
					/* end of the path */
					if (le == NULL)
						break;

					crel = lfirst(le);
					setInitialVidForVLE(pstate, crel, vertex, vertex_is_nsitem,
										prev_crel, prev_edge, prev_edge_is_nsitem);
					edge = transformMatchRel(pstate, crel, targetList,
											 &eqoList, out, &edge_is_nsitem);

					/* this node is the source of the next edge `crel` */
					src_vertex = vertex;
					src_vertex_is_nsitem = vertex_is_nsitem;

					qual = addQualRelPath(pstate, qual, prev_crel, prev_edge,
										  prev_edge_is_nsitem, crel,
										  edge, edge_is_nsitem);
				}

				/* uniqueness */
				if (crel->varlen == NULL)
				{
					Node	   *eid;

					eid = resolveVarOrExpr(pstate, edge, AG_ELEM_LOCAL_ID, edge_is_nsitem);
					ueids = list_append_unique(ueids, eid);
				}
				else
				{
					Node	   *eidarr;

					eidarr = getColumnVar(pstate, (ParseNamespaceItem *) edge,
										  VLE_COLNAME_IDS);
					ueidarrs = list_append_unique(ueidarrs, eidarr);
				}

				if (out)
				{
					Assert(vertex != NULL);

					/*
					 * Starting vertex of ZeroLengthVLE is excluded from the
					 * graph path.
					 */
					if (!isZeroLengthVLE(crel))
					{
						pvs = vtxArrConcat(pstate, pvs,
										   makePathVertexExpr(pstate, vertex,
															  vertex_is_nsitem));
					}

					if (crel->varlen == NULL)
					{
						pes = edgeArrConcat(pstate, pes,
											makePathEdgeExpr(pstate, crel, edge,
															 edge_is_nsitem));
					}
					else
					{
						pvs = vtxArrConcat(pstate, pvs,
										   getColumnVar(pstate,
														(ParseNamespaceItem *) edge,
														VLE_COLNAME_VERTICES));
						pes = edgeArrConcat(pstate, pes,
											getColumnVar(pstate,
														 (ParseNamespaceItem *) edge,
														 VLE_COLNAME_EDGES));
					}
				}

				prev_crel = crel;
				prev_edge = edge;
				prev_edge_is_nsitem = edge_is_nsitem;

				le = lnext(p->chain, le);
			}

			if (out && p->kind == CPATH_NORMAL)
			{
				Node	   *graphpath;

				Assert(vertex != NULL);
				pvs = vtxArrConcat(pstate, pvs,
								   makePathVertexExpr(pstate, vertex,
													  vertex_is_nsitem));

				graphpath = makeTypedRowExpr(list_make2(pvs, pes),
											 GRAPHPATHOID, pathloc);

				*targetList = lappend(*targetList,
									  makeTargetEntry((Expr *) graphpath,
													  (AttrNumber) pstate->p_next_resno++,
													  pathname,
													  false));
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
		*targetList = lappend(*targetList, te);

		/*
		 * Apply this anonymous element's property constraint here, inside the
		 * pattern query, referencing the element expression directly.
		 * Deferring it to the enclosing clause instead (via addElemQual() /
		 * transformElemQuals()) would make the outer qual reference this
		 * resjunk targetlist column across the subquery boundary.  PG18's
		 * planner cannot handle that: a subquery rel does not expose its
		 * resjunk columns, so pulling the subquery up or pushing the qual
		 * into it fails (either "could not find replacement targetlist entry"
		 * or an attno-range assertion).  The column is still added to the
		 * target list above so that later clauses can reference the element
		 * as a future vertex.
		 */
		if (eqo->prop_map != NULL)
		{
			Node	   *prop_map;
			bool		is_cyphermap;

			prop_map = getExprField((Expr *) te->expr, AG_ELEM_PROP_MAP);
			is_cyphermap = IsA(eqo->prop_map, CypherMapExpr);

			if (is_cyphermap)
				qual = transform_prop_constr(pstate, qual, prop_map,
											 eqo->prop_map);

			if ((is_cyphermap && ginAvail(pstate, eqo->varno, 1)) ||
				!is_cyphermap)
			{
				Node	   *prop_constr;
				Expr	   *expr;

				prop_constr = transformPropMap(pstate, eqo->prop_map,
											   EXPR_KIND_WHERE);
				expr = make_op(pstate, list_make1(makeString("@>")),
							   prop_map, prop_constr, pstate->p_last_srf, -1);
				qual = qualAndExpr(qual, (Node *) expr);
			}
		}
	}

	return qual;
}

static Node *
transformMatchNode(ParseState *pstate, CypherNode *cnode, List **targetList,
				   List **eqoList, bool *is_nsitem)
{
	char	   *varname = getCypherName(cnode->variable);
	int			varloc = getCypherNameLoc(cnode->variable);
	TargetEntry *te;
	EntityInfo *ei = NULL;
	char	   *labname;
	int			labloc;
	bool		prop_constr;
	RangeVar   *r;
	Alias	   *alias;
	ParseNamespaceItem *nsitem;

	*is_nsitem = false;

	/*--------------------------
	 * If a vertex with the same variable is already in the target list,
	 * - the vertex is from the previous clause or
	 * - a node with the same variable in the pattern are already processed,
	 * so skip `cnode`.
	 *--------------------------
	 */
	te = findTarget(*targetList, varname);
	if (te != NULL)
	{
		if (exprType((Node *) te->expr) != VERTEXOID)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_ALIAS),
					 errmsg("duplicate variable \"%s\"", varname),
					 parser_errposition(pstate, varloc)));

		addElemQual(pstate, te->resno, cnode->prop_map);

		nsitem = scanNameSpaceForRefname(pstate, varname, varloc);
		if (nsitem == NULL)
		{
			/*
			 * `te` can be from the previous clause or the pattern. If it is
			 * from the pattern, it should be an actual vertex or a future
			 * vertex
			 */
			char	   *_labname = getCypherName(cnode->label);

			/*
			 * If the variable is from the previous clause, it should either
			 * have no label constraint or the same label constraint as the
			 * previous clause.
			 */
			if (IsA(te->expr, Var) && _labname != NULL)
			{
				EntityInfo *_ei = getEntityInfo(pstate, varname, T_CypherNode, false);

				if (_ei->labname == NULL || strcmp(_ei->labname, _labname) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("label on variable from previous clauses is not allowed"),
							 parser_errposition(pstate, getCypherNameLoc(cnode->label))));
			}
			return (Node *) te;
		}
		else
		{
			/* previously returned RTE_RELATION by this function */
			*is_nsitem = true;
			return (Node *) nsitem;
		}
	}

	/*
	 * try to find the variable when this pattern is within an OPTIONAL MATCH
	 * or a sub-SELECT
	 */
	else if (te == NULL && varname != NULL)
	{
		Var		   *col;

		col = (Var *) colNameToVar(pstate, varname, false, varloc);
		if (col != NULL)
		{
			FutureVertex *fv;
			EntityInfo *_ei = getEntityInfo(pstate, varname, T_CypherNode, false);
			char	   *_labname = getCypherName(cnode->label);

			if ((_labname != NULL && (_ei->labname == NULL || strcmp(_ei->labname, _labname) != 0)) ||
				exprType((Node *) col) != VERTEXOID)
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
		ei = getEntityInfo(pstate, varname, T_CypherNode, true);
		Assert(ei != NULL);

		labname = ei->labname;
		labloc = -1;
		prop_constr = ei->prop_constr;
	}

	if (labname == NULL || !pstate->p_valid_labels)
		labname = AG_VERTEX;

	r = makeRangeVar(get_graph_path(true),
					 labname,
					 labloc);
	r->inh = !cnode->only;
	alias = makeAliasOptUnique(varname);

	/* set `ihn` to true because we should scan all derived tables */
	nsitem = addRangeTableEntry(pstate, r, alias, r->inh, true);
	addNSItemToJoinlist(pstate, nsitem, true);

	if (varname != NULL || prop_constr)
	{
		bool		resjunk;
		int			resno;

		/*
		 * If `varname` is NULL, this target has to be ignored when `RETURN
		 * *`.
		 */
		resjunk = (varname == NULL);
		resno = (resjunk ? InvalidAttrNumber : pstate->p_next_resno++);

		te = makeTargetEntry((Expr *) makeVertexExpr(pstate,
													 nsitem,
													 varloc),
							 (AttrNumber) resno,
							 alias->aliasname,
							 resjunk);

		if (resjunk)
		{
			ElemQualOnly *eqo;

			eqo = palloc(sizeof(*eqo));
			eqo->te = te;
			eqo->prop_map = cnode->prop_map;
			eqo->varno = nsitem->p_rtindex;

			*eqoList = lappend(*eqoList, eqo);
		}
		else
		{
			addElemQual(pstate, te->resno, cnode->prop_map);
			*targetList = lappend(*targetList, te);
		}
	}

	/* return RTE to help the caller can access columns directly */
	*is_nsitem = true;
	return (Node *) nsitem;
}

static Node *
transformMatchRel(ParseState *pstate, CypherRel *crel, List **targetList,
				  List **eqoList, bool pathout, bool *is_nsitem)
{
	if (crel->varlen == NULL)
		return transformMatchSR(pstate, crel, targetList, eqoList, is_nsitem);
	else
		return transformMatchVLE(pstate, crel, targetList, pathout, is_nsitem);
}

static Node *
transformMatchSR(ParseState *pstate, CypherRel *crel, List **targetList,
				 List **eqoList, bool *is_nsitem)
{
	char	   *varname = getCypherName(crel->variable);
	int			varloc = getCypherNameLoc(crel->variable);
	char	   *typname;
	int			typloc = -1;
	Alias	   *alias;
	ParseNamespaceItem *nsitem;
	TargetEntry *te;

	*is_nsitem = false;

	te = findTarget(*targetList, varname);
	if (te != NULL)
	{
		if (exprType((Node *) te->expr) != EDGEOID)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_ALIAS),
					 errmsg("duplicate variable \"%s\"", varname),
					 parser_errposition(pstate, varloc)));

		addElemQual(pstate, te->resno, crel->prop_map);

		nsitem = scanNameSpaceForRefname(pstate, varname, varloc);

		if (nsitem == NULL)
		{
			char	   *_typname = getEntityLabname((Node *) crel);

			if (IsA(te->expr, Var) && _typname != NULL)
			{
				EntityInfo *_ei = getEntityInfo(pstate, varname, T_CypherRel, false);

				if (_ei->labname == NULL || strcmp(_ei->labname, _typname) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("label on variable from previous clauses is not allowed"),
							 parser_errposition(pstate, varloc)));
			}
			return (Node *) te;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_ALIAS),
					 errmsg("duplicate variable \"%s\"", varname),
					 parser_errposition(pstate, varloc)));
		}
	}

	/*
	 * try to find the variable when this pattern is within an OPTIONAL MATCH
	 * or a sub-SELECT
	 */
	else if (te == NULL && varname != NULL)
	{
		Var		   *col;

		col = (Var *) colNameToVar(pstate, varname, false, varloc);
		if (col != NULL)
		{
			EntityInfo *_ei = getEntityInfo(pstate, varname, T_CypherRel, false);
			char	   *_labname = getEntityLabname((Node *) crel);

			if ((_labname != NULL && (_ei->labname == NULL || strcmp(_ei->labname, _labname) != 0)) ||
				exprType((Node *) col) != EDGEOID)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_ALIAS),
						 errmsg("duplicate variable \"%s\"", varname),
						 parser_errposition(pstate, varloc)));

			te = makeTargetEntry((Expr *) col,
								 (AttrNumber) pstate->p_next_resno++,
								 varname,
								 false);

			addElemQual(pstate, te->resno, crel->prop_map);
			*targetList = lappend(*targetList, te);

			return (Node *) te;
		}
	}

	if (!pstate->p_valid_labels)
		typname = AG_EDGE;
	else
		getCypherRelType(crel, &typname, &typloc);

	alias = makeAliasOptUnique(varname);

	if (crel->direction == CYPHER_REL_DIR_NONE)
	{
		nsitem = addEdgeUnion(pstate, typname, crel->only, typloc, alias);
	}
	else
	{
		RangeVar   *r;

		r = makeRangeVar(get_graph_path(true), typname, typloc);
		r->inh = !crel->only;

		nsitem = addRangeTableEntry(pstate, r, alias, r->inh, true);
	}
	addNSItemToJoinlist(pstate, nsitem, false);

	if (varname != NULL || crel->prop_map != NULL)
	{
		bool		resjunk;
		int			resno;
		TargetEntry *_te;

		resjunk = (varname == NULL);
		resno = (resjunk ? InvalidAttrNumber : pstate->p_next_resno++);

		_te = makeTargetEntry((Expr *) makeEdgeExpr(pstate, crel, nsitem,
													varloc),
							  (AttrNumber) resno,
							  alias->aliasname,
							  resjunk);

		if (resjunk)
		{
			ElemQualOnly *eqo;

			eqo = palloc(sizeof(*eqo));
			eqo->te = _te;
			eqo->prop_map = crel->prop_map;
			eqo->varno = nsitem->p_rtindex;

			*eqoList = lappend(*eqoList, eqo);
		}
		else
		{
			addElemQual(pstate, _te->resno, crel->prop_map);
			*targetList = lappend(*targetList, _te);
		}
	}

	*is_nsitem = true;
	return (Node *) nsitem;
}

static ParseNamespaceItem *
addEdgeUnion(ParseState *pstate, char *edge_label, bool only, int location,
			 Alias *alias)
{
	Node	   *u;
	Query	   *qry;

	Assert(alias != NULL);

	Assert(pstate->p_expr_kind == EXPR_KIND_NONE);
	pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

	u = genEdgeUnion(edge_label, only, location);
	qry = parse_sub_analyze(u, pstate, NULL,
							isLockedRefname(pstate, alias->aliasname), true);

	pstate->p_expr_kind = EXPR_KIND_NONE;

	return addRangeTableEntryForSubquery(pstate, qry, alias, false, true);
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
					bool vertex_is_nsitem, CypherRel *prev_crel,
					Node *prev_edge, bool prev_edge_is_nsitem)
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
			pstate->p_vle_initial_nsitem = NULL;
		}
		else
		{
			char	   *colname = getEdgeColname(prev_crel, prev_edge_is_nsitem, true);

			if (prev_edge_is_nsitem)
			{
				ParseNamespaceItem *pe = (ParseNamespaceItem *) prev_edge;

				Assert(pe->p_rte->rtekind == RTE_RELATION);

				cref = makeNode(ColumnRef);
				cref->fields = list_make2(
										  makeString(pe->p_rte->eref->aliasname),
										  makeString(colname));
				cref->location = -1;

				pstate->p_vle_initial_vid = (Node *) cref;
				pstate->p_vle_initial_nsitem = pe;
			}
			else
			{
				TargetEntry *te = (TargetEntry *) prev_edge;
				Node	   *vid;

				Assert(IsA(prev_edge, TargetEntry));

				cref = makeNode(ColumnRef);
				cref->fields = list_make1(makeString(te->resname));
				cref->location = -1;

				vid = (Node *) makeFuncCall(list_make1(makeString(colname)),
											list_make1(cref),
											COERCE_EXPLICIT_CALL, -1);

				pstate->p_vle_initial_vid = vid;
				pstate->p_vle_initial_nsitem = NULL;
			}
		}

		return;
	}

	if (vertex_is_nsitem)
	{
		ParseNamespaceItem *nsitem = (ParseNamespaceItem *) vertex;

		Assert(nsitem->p_rte->rtekind == RTE_RELATION);

		cref = makeNode(ColumnRef);
		cref->fields = list_make2(
								  makeString(nsitem->p_rte->eref->aliasname),
								  makeString(AG_ELEM_LOCAL_ID));
		cref->location = -1;

		pstate->p_vle_initial_vid = (Node *) cref;
		pstate->p_vle_initial_nsitem = nsitem;
	}
	else
	{
		TargetEntry *te = (TargetEntry *) vertex;
		Node	   *vid;

		Assert(IsA(vertex, TargetEntry));

		/* vertex or future vertex */

		cref = makeNode(ColumnRef);
		cref->fields = list_make1(makeString(te->resname));
		cref->location = -1;

		vid = (Node *) makeFuncCall(list_make1(makeString(AG_ELEM_ID)),
									list_make1(cref),
									COERCE_EXPLICIT_CALL, -1);

		pstate->p_vle_initial_vid = vid;
		pstate->p_vle_initial_nsitem = NULL;
	}
}

static Node *
transformMatchVLE(ParseState *pstate, CypherRel *crel, List **targetList,
				  bool pathout, bool *is_nsitem)
{
	char	   *varname = getCypherName(crel->variable);
	int			varloc = getCypherNameLoc(crel->variable);
	bool		out = (varname != NULL || pathout);
	SelectStmt *sel;
	Alias	   *alias;
	ParseNamespaceItem *nsitem;
	TargetEntry *te;

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

	sel = genVLESubselect(pstate, crel, out, pathout);

	alias = makeAliasOptUnique(varname);
	nsitem = transformVLEtoNSItem(pstate, crel, sel, alias);

	if (out)
	{
		TargetEntry *_te;
		Node	   *var;
		bool		resjunk;
		int			resno;

		resjunk = (varname == NULL);
		resno = (resjunk ? InvalidAttrNumber : pstate->p_next_resno++);

		if (pstate->p_valid_labels)
			var = getColumnVar(pstate, nsitem, VLE_COLNAME_EDGES);
		else
			var = (Node *) makeNullConst(EDGEARRAYOID, -1, InvalidOid);

		_te = makeTargetEntry((Expr *) var,
							  (AttrNumber) resno,
							  alias->aliasname,
							  resjunk);

		*targetList = lappend(*targetList, _te);
	}

	if (pathout)
	{
		TargetEntry *_te;
		Node	   *var;

		if (pstate->p_valid_labels)
			var = getColumnVar(pstate, nsitem, VLE_COLNAME_VERTICES);
		else
			var = (Node *) makeNullConst(VERTEXARRAYOID, -1, InvalidOid);

		_te = makeTargetEntry((Expr *) var,
							  InvalidAttrNumber,
							  genUniqueName(),
							  true);

		*targetList = lappend(*targetList, _te);
	}

	*is_nsitem = true;
	return (Node *) nsitem;
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
	Node	   *curr_col;
	ResTarget  *curr;
	Node	   *ids_col;
	ResTarget  *ids;
	List	   *tlist;
	Node	   *left;
	SelectStmt *sel;
	Node	   *vertices_col;
	ResTarget  *vertices;

	prev_colname = getEdgeColname(crel, true, false);
	prev_col = makeColumnRef(genQualifiedName(VLE_LEFT_ALIAS, prev_colname));
	prev = makeResTarget(prev_col, prev_colname);

	curr_colname = getEdgeColname(crel, true, true);
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

	/* Add Vertices Column */
	vertices_col = makeColumnRef(genQualifiedName(VLE_LEFT_ALIAS,
												  VLE_COLNAME_VERTICES));
	vertices = makeResTarget(vertices_col, VLE_COLNAME_VERTICES);
	tlist = lappend(tlist, vertices);

	left = genVLELeftChild(pstate, crel, out);

	sel = makeNode(SelectStmt);
	sel->targetList = tlist;
	sel->fromClause = list_make1(left);

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
genVLELeftChild(ParseState *pstate, CypherRel *crel, bool out)
{
	Node	   *vid;
	List	   *colnames = NIL;
	SelectStmt *sel;
	RangeSubselect *sub;

	/*
	 * `vid` is NULL only if (there is no previous edge of the vertex in the
	 * path and the vertex is transformed first time in the pattern) and
	 * `crel` is not zero-length
	 */
	vid = pstate->p_vle_initial_vid;

	if (isZeroLengthVLE(crel))
	{
		Node	   *vtxarr = makeAArrayExpr(NIL, VERTEXARRAYOID);
		Node	   *ids;
		List	   *values;

		Assert(vid != NULL);

		ids = makeAArrayExpr(NIL, GRAPHIDARRAYOID);

		values = list_make3(vid, vid, ids);
		colnames = list_make3(makeString(getEdgeColname(crel, true, false)),
							  makeString(getEdgeColname(crel, true, true)),
							  makeString(VLE_COLNAME_IDS));

		if (out)
		{
			Node	   *edge_arr = makeAArrayExpr(NIL, EDGEARRAYOID);

			values = lappend(values, edge_arr);
			colnames = lappend(colnames, makeString(VLE_COLNAME_EDGES));
		}

		values = lappend(values, vtxarr);
		colnames = lappend(colnames, makeString(VLE_COLNAME_VERTICES));

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
		Node	   *id_array;
		ResTarget  *ids;
		List	   *tlist = NIL;
		Node	   *from;
		List	   *where_args = NIL;
		ResTarget  *vertices;
		TypeCast   *cast = makeNode(TypeCast);

		prev_colname = genQualifiedName(NULL, getEdgeColname(crel, true, false));
		prev_col = makeColumnRef(prev_colname);
		prev = makeResTarget(prev_col, NULL);
		curr = makeSimpleResTarget(getEdgeColname(crel, true, true), NULL);

		id = makeColumnRef(genQualifiedName(NULL, AG_ELEM_LOCAL_ID));

		id_array = makeAArrayExpr(list_make1(id), GRAPHIDARRAYOID);
		ids = makeResTarget((Node *) id_array, VLE_COLNAME_IDS);

		tlist = list_make3(prev, curr, ids);

		from = genVLEEdgeSubselect(pstate, crel, VLE_LEFT_ALIAS);

		if (out)
		{
			Node	   *edge_arr = makeAArrayExpr(
												  list_make1(genEdgeSimple(VLE_LEFT_ALIAS)), EDGEARRAYOID);
			ResTarget  *edges = makeResTarget(edge_arr, VLE_COLNAME_EDGES);

			tlist = lappend(tlist, edges);
		}

		cast->arg = (Node *) makeNullAConst();
		cast->typeName = makeTypeNameFromOid(VERTEXARRAYOID, -1);
		cast->location = -1;
		vertices = makeResTarget((Node *) cast, VLE_COLNAME_VERTICES);

		tlist = lappend(tlist, vertices);

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

static Node *
genEdgeSimple(char *aliasname)
{
	Node	   *id;
	Node	   *start;
	Node	   *end;
	Node	   *prop_map;
	Node	   *tid;

	id = makeColumnRef(genQualifiedName(aliasname, AG_ELEM_LOCAL_ID));
	start = makeColumnRef(genQualifiedName(aliasname, AG_START_ID));
	end = makeColumnRef(genQualifiedName(aliasname, AG_END_ID));
	prop_map = makeColumnRef(genQualifiedName(aliasname, AG_ELEM_PROP_MAP));
	tid = makeColumnRef(genQualifiedName(aliasname, "ctid"));

	return (Node *) makeRowExprWithTypeCast(
											list_make5(id, start, end, prop_map, tid), EDGEOID, -1);
}

static Node *
genVLEEdgeSubselect(ParseState *pstate, CypherRel *crel, char *aliasname)
{
	char	   *typname;
	int			typloc = -1;
	Alias	   *alias;
	Node	   *edge;

	if (!pstate->p_valid_labels)
		typname = AG_EDGE;
	else
		getCypherRelType(crel, &typname, &typloc);

	alias = makeAliasNoDup(aliasname, NIL);

	if (crel->direction == CYPHER_REL_DIR_NONE)
	{
		RangeSubselect *sub;

		/* id, start, "end", properties, ctid, _start, _end */
		sub = makeNode(RangeSubselect);
		sub->subquery = genEdgeUnion(typname, crel->only, typloc);
		sub->alias = alias;
		edge = (Node *) sub;
	}
	else
	{
		RangeVar   *r;
		LOCKMODE	lockmode;
		Relation	rel;

		r = makeRangeVar(get_graph_path(true), typname, typloc);
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

		table_close(rel, NoLock);
	}

	return edge;
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

		childrel = table_open(childoid, AccessShareLock);

		childrv = makeRangeVar(get_graph_path(true),
							   RelationGetRelationName(childrel),
							   -1);
		childrv->inh = true;

		table_close(childrel, AccessShareLock);

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

static ParseNamespaceItem *
transformVLEtoNSItem(ParseState *pstate, CypherRel *crel, SelectStmt *vle, Alias *alias)
{
	ParseNamespaceItem *nsitem;
	ParseNamespaceItem *vle_initial_nsitem = NULL;
	Query	   *qry;

	Assert(!pstate->p_lateral_active);
	Assert(pstate->p_expr_kind == EXPR_KIND_NONE);

	/* make the RTE temporarily visible */
	if (pstate->p_vle_initial_nsitem != NULL)
	{
		vle_initial_nsitem = pstate->p_vle_initial_nsitem;
		vle_initial_nsitem->p_rel_visible = true;
	}

	pstate->p_lateral_active = true;
	pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

	qry = parse_sub_analyze((Node *) vle, pstate, NULL,
							isLockedRefname(pstate, alias->aliasname), true);
	Assert(qry->commandType == CMD_SELECT);
	qry->commandType = CMD_SELECT;
	if (crel->prop_map)
		crel->prop_map = transformCypherExpr(pstate,
											 crel->prop_map,
											 EXPR_KIND_WHERE);

	qry->g_vle_rel = (Node *) crel;

	pstate->p_lateral_active = false;
	pstate->p_expr_kind = EXPR_KIND_NONE;

	if (vle_initial_nsitem != NULL)
		vle_initial_nsitem->p_rel_visible = false;

	nsitem = addRangeTableEntryForSubquery(pstate, qry, alias, true, true);
	nsitem->p_rte->isVLE = true;
	addNSItemToJoinlist(pstate, nsitem, false);

	return nsitem;
}

static bool
isZeroLengthVLE(CypherRel *crel)
{
	if (crel == NULL)
		return false;

	if (crel->varlen == NULL)
		return false;

	/* todo: corrects function name */
	return true;
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
		Node	   *type;

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
addQualRelPath(ParseState *pstate, Node *qual,
			   CypherRel *prev_crel, Node *prev_edge, bool prev_edge_is_nsitem,
			   CypherRel *crel, Node *edge, bool edge_is_nsitem)
{
	Node	   *prev_vid;
	Node	   *vid;
	char	   *prev_colname = getEdgeColname(prev_crel, prev_edge_is_nsitem, true);
	char	   *colname = getEdgeColname(crel, edge_is_nsitem, false);

	/*
	 * NOTE: If `crel` is VLE and a node between `prev_crel` and `crel` is
	 * either a placeholder or a new future vertex, the initial vid of `crel`
	 * is `prev_vid` already. Currently, just add kind of duplicate qual
	 * anyway.
	 */

	prev_vid = resolveVarOrExpr(pstate, prev_edge, prev_colname, prev_edge_is_nsitem);
	vid = resolveVarOrExpr(pstate, edge, colname, edge_is_nsitem);

	qual = qualAndExpr(qual,
					   (Node *) make_op(pstate, list_make1(makeString("=")),
										prev_vid, vid, pstate->p_last_srf, -1));

	return qual;
}

static Node *
addQualNodeIn(ParseState *pstate, Node *qual, Node *vertex,
			  bool vertex_is_nsitem, CypherRel *crel, Node *edge,
			  bool edge_is_nsitem, bool prev)
{
	Node	   *id;
	Node	   *vid;
	char	   *edge_colname;

	/* `vertex` is just a placeholder for relationships */
	if (vertex == NULL)
		return qual;

	if (isFutureVertexExpr(vertex))
	{
		setFutureVertexExprId(pstate, vertex, crel, edge, edge_is_nsitem, prev);
		return qual;
	}

	/* already done in transformMatchVLE() */
	if (crel->varlen != NULL && !prev)
		return qual;

	edge_colname = getEdgeColname(crel, edge_is_nsitem, prev);
	id = resolveVarOrExpr(pstate, vertex, AG_ELEM_LOCAL_ID, vertex_is_nsitem);
	vid = resolveVarOrExpr(pstate, edge, edge_colname, edge_is_nsitem);

	qual = qualAndExpr(qual,
					   (Node *) make_op(pstate, list_make1(makeString("=")),
										id, vid, pstate->p_last_srf, -1));

	return qual;
}

static char *
getEdgeColname(CypherRel *crel, bool edge_is_nsitem, bool prev)
{
	if (prev)
	{
		if (crel->direction == CYPHER_REL_DIR_NONE)
			if (edge_is_nsitem)
				return EDGE_UNION_END_ID;
			else
				return AG_END_ID;
		else if (crel->direction == CYPHER_REL_DIR_LEFT)
			return AG_START_ID;
		else
			return AG_END_ID;
	}
	else
	{
		if (crel->direction == CYPHER_REL_DIR_NONE)
			if (edge_is_nsitem)
				return EDGE_UNION_START_ID;
			else
				return AG_START_ID;
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
	RowExpr    *row;

	Assert(vertex != NULL);

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
					  Node *edge, bool edge_is_nsitem, bool prev)
{
	TargetEntry *te = (TargetEntry *) vertex;
	RowExpr    *row;
	Node	   *vid;
	char	   *colname = getEdgeColname(crel, edge_is_nsitem, prev);

	row = (RowExpr *) te->expr;
	vid = resolveVarOrExpr(pstate, edge, colname, edge_is_nsitem);
	row->args = list_make2(vid, lsecond(row->args));
}

static Node *
vtxArrConcat(ParseState *pstate, Node *array, Node *elem)
{
	Oid			elemtype = exprType(elem);

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
	Oid			elemtype = exprType(elem);

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
	ListCell   *le1;
	ListCell   *lea1;

	foreach(le1, ueids)
	{
		Node	   *eid1 = lfirst(le1);
		ListCell   *le2;

		for_each_cell(le2, ueids, lnext(ueids, le1))
		{
			Node	   *eid2 = lfirst(le2);
			OpExpr	   *ne = makeNode(OpExpr);

			ne->opno = OID_GRAPHID_NE_OP;
			ne->opfuncid = F_GRAPHID_NE;
			ne->opresulttype = BOOLOID;
			ne->opretset = false;
			ne->args = list_make2(eid1, eid2);
			ne->location = -1;

			qual = qualAndExpr(qual, (Node *) ne);
		}

		foreach(lea1, ueidarrs)
		{
			Node	   *eidarr = lfirst(lea1);
			FuncExpr   *arg;
			NullTest   *dupcond;

			arg = makeFuncExpr(F_ARRAY_POSITION_ANYCOMPATIBLEARRAY_ANYCOMPATIBLE,
							   INT4OID,
							   list_make2(eidarr, eid1),
							   InvalidOid,
							   InvalidOid,
							   COERCE_EXPLICIT_CALL);

			dupcond = makeNode(NullTest);
			dupcond->arg = (Expr *) arg;
			dupcond->nulltesttype = IS_NULL;
			dupcond->argisrow = false;
			dupcond->location = -1;

			qual = qualAndExpr(qual, (Node *) dupcond);
		}
	}

	foreach(lea1, ueidarrs)
	{
		Node	   *eidarr1 = lfirst(lea1);
		ListCell   *lea2;

		for_each_cell(lea2, ueidarrs, lnext(ueidarrs, lea1))
		{
			Node	   *eidarr2 = lfirst(lea2);
			Node	   *funcexpr;
			Node	   *dupcond;

			funcexpr = (Node *) makeFuncExpr(F_ARRAYOVERLAP,
											 BOOLOID,
											 list_make2(eidarr1, eidarr2),
											 InvalidOid,
											 InvalidOid,
											 COERCE_EXPLICIT_CALL);

			dupcond = (Node *) makeBoolExpr(NOT_EXPR, list_make1(funcexpr), -1);

			qual = qualAndExpr(qual, dupcond);
		}
	}

	return qual;
}

static void
addElemQual(ParseState *pstate, AttrNumber varattno, Node *prop_constr)
{
	ElemQual   *eq;

	if (prop_constr == NULL)
		return;

	eq = palloc(sizeof(*eq));
	eq->varno = InvalidAttrNumber;
	eq->varattno = varattno;
	eq->prop_constr = prop_constr;

	pstate->p_elem_quals = lappend(pstate->p_elem_quals, eq);
}

static void
adjustElemQuals(List *elem_quals, ParseNamespaceItem *nsitem)
{
	ListCell   *le;

	Assert(nsitem->p_rte->rtekind == RTE_SUBQUERY);

	foreach(le, elem_quals)
	{
		ElemQual   *eq = lfirst(le);

		eq->varno = nsitem->p_rtindex;
	}
}

static Node *
transformElemQuals(ParseState *pstate, Node *qual)
{
	ListCell   *le;

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

		k = lfirst(le);
		le = lnext(m->keyvals, le);
		v = lfirst(le);
		le = lnext(m->keyvals, le);

		Assert(IsA(k, String));
		pathelem = makeConst(TEXTOID, -1, DEFAULT_COLLATION_OID, -1,
							 CStringGetTextDatum(strVal(k)), false, false);

		ctx->pathelems = lappend(ctx->pathelems, pathelem);

		if (IsA(v, CypherMapExpr))
		{
			transform_prop_constr_worker(v, ctx);
		}
		else
		{
			Node	   *lval;
			Node	   *rval;
			Oid			rvaltype;
			int			rvalloc;
			Expr	   *expr;

			lval = (Node *) makeJsonbFuncAccessor(ctx->pstate, ctx->prop_map, copyObject(ctx->pathelems));

			rval = transformCypherExpr(ctx->pstate, v, EXPR_KIND_WHERE);
			rvaltype = exprType(rval);
			rvalloc = exprLocation(rval);
			rval = coerce_expr(ctx->pstate, rval, rvaltype, JSONBOID, -1,
							   COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);
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
										  list_tail(ctx->pathelems));
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
		Oid			inhoid = lfirst_oid(li);

		if (hasGinOnProp(inhoid))
			return true;
	}

	return false;
}

static Oid
getSourceRelid(ParseState *pstate, Index varno, AttrNumber varattno)
{
	FutureVertex *fv;
	List	   *rtable;

	/*
	 * If the given Var refers to a future vertex, there is no actual RTE for
	 * it. So, find the Var from the list of future vertices first.
	 */
	fv = findFutureVertex(pstate, varno, varattno, 0);
	if (fv != NULL)
	{
		RangeVar   *rv = makeRangeVar(get_graph_path(true), fv->labname, -1);

		return RangeVarGetRelid(rv, AccessShareLock, false);
	}

	rtable = pstate->p_rtable;
	for (;;)
	{
		RangeTblEntry *rte;
		Var		   *var;

		rte = rt_fetch(varno, rtable);
		switch (rte->rtekind)
		{
			case RTE_RELATION:
				/* already locked */
				return rte->relid;
			case RTE_SUBQUERY:
				{
					TargetEntry *te;
					Oid			type;

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
					Expr	   *expr;

					expr = list_nth(rte->joinaliasvars, varattno - 1);
					if (!IsA(expr, Var))
						return InvalidOid;

					var = (Var *) expr;
					/* XXX: Do we need type check? */

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

	rel = table_open(relid, NoLock);

	if (!rel->rd_rel->relhasindex)
	{
		table_close(rel, NoLock);

		return false;
	}

	indexoidlist = RelationGetIndexList(rel);

	foreach(li, indexoidlist)
	{
		Oid			indexoid = lfirst_oid(li);
		Relation	indexRel;
		Form_pg_index index;
		int			attnum;

		indexRel = index_open(indexoid, AccessShareLock);
		index = indexRel->rd_index;

		if (!index->indisvalid)
		{
			index_close(indexRel, AccessShareLock);
			continue;
		}

		if (index->indcheckxmin &&
			!TransactionIdPrecedes(
								   HeapTupleHeaderGetXmin(indexRel->rd_indextuple->t_data),
								   TransactionXmin))
		{
			index_close(indexRel, AccessShareLock);
			continue;
		}

		if (indexRel->rd_rel->relam != GIN_AM_OID)
		{
			index_close(indexRel, AccessShareLock);
			continue;
		}

		attnum = attnameAttNum(rel, AG_ELEM_PROP_MAP, false);
		if (attnum == InvalidAttrNumber)
		{
			index_close(indexRel, AccessShareLock);
			continue;
		}

		if (index->indkey.values[0] == attnum)
		{
			index_close(indexRel, AccessShareLock);
			ret = true;
			break;
		}

		index_close(indexRel, AccessShareLock);
	}

	list_free(indexoidlist);

	table_close(rel, NoLock);

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
	ListCell   *le;

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
adjustFutureVertices(List *future_vertices, ParseNamespaceItem *nsitem)
{
	ListCell   *le;

	Assert(nsitem->p_rte->rtekind == RTE_SUBQUERY);

	foreach(le, future_vertices)
	{
		FutureVertex *fv = lfirst(le);
		bool		found;
		ListCell   *lt;

		/* set `varno` of new future vertex to its `rtindex` */
		if (fv->varno == InvalidAttrNumber)
		{
			fv->varno = nsitem->p_rtindex;
			continue;
		}

		found = false;
		foreach(lt, nsitem->p_rte->subquery->targetList)
		{
			TargetEntry *te = lfirst(lt);
			Var		   *var;

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
				fv->varno = nsitem->p_rtindex;

				/*
				 * `te->resno` should always be equal to the item's ordinal
				 * position (counting from 1)
				 */
				fv->varattno = te->resno;

				found = true;
			}
		}

		if (!found)
			future_vertices = foreach_delete_current(future_vertices, le);
	}

	return future_vertices;
}

static Node *
resolve_future_vertex(ParseState *pstate, Node *node, int flags)
{
	resolve_future_vertex_context ctx;

	if (node == NULL)
		return NULL;

	ctx.pstate = pstate;
	ctx.flags = flags;
	ctx.sublevels_up = 0;

	return resolve_future_vertex_mutator(node, &ctx);
}

static Node *
resolve_future_vertex_mutator(Node *node, resolve_future_vertex_context *ctx)
{
	Var		   *var;

	if (node == NULL)
		return NULL;

	if (IsA(node, Aggref))
	{
		Aggref	   *agg = (Aggref *) node;
		int			agglevelsup = (int) agg->agglevelsup;

		if (agglevelsup == ctx->sublevels_up)
		{
			ListCell   *la;

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
		OpExpr	   *op = (OpExpr *) node;

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
				fselect->fieldnum == Anum_ag_vertex_id)
				return node;
		}

		/* fall-through */
	}

	if (IsA(node, Var))
	{
		FutureVertex *fv;
		Var		   *newvar;

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
		Query	   *newnode;

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
	ParseNamespaceItem *fv_nsitem;
	TargetEntry *fv_te;
	Var		   *fv_var;
	Node	   *fv_id;
	ParseNamespaceItem *nsitem;
	Node	   *vertex;
	FuncCall   *sel_id;
	Node	   *id;
	Node	   *qual;

	Assert(fv->expr == NULL);

	fv_nsitem = GetNSItemByRangeTablePosn(pstate, fv->varno, 0);
	Assert(fv_nsitem->p_rte->rtekind == RTE_SUBQUERY);

	fv_te = get_tle_by_resno(fv_nsitem->p_rte->subquery->targetList,
							 fv->varattno);
	Assert(fv_te != NULL);

	fv_var = make_var(pstate, fv_nsitem, fv->varattno, -1);
	fv_id = getExprField((Expr *) fv_var, AG_ELEM_ID);

	/*
	 * `p_cols_visible` of previous RTE must be set to allow `rte` to see
	 * columns of the previous RTE by their name
	 */
	nsitem = makeVertexNSItem(pstate, fv_te->resname, fv->labname);

	vertex = getColumnVar(pstate, nsitem, nsitem->p_rte->eref->aliasname);

	sel_id = makeFuncCall(list_make1(makeString(AG_ELEM_ID)), NIL,
						  COERCE_EXPLICIT_CALL, -1);
	id = ParseFuncOrColumn(pstate, sel_id->funcname, list_make1(vertex),
						   pstate->p_last_srf, sel_id, false, -1);

	qual = (Node *) make_op(pstate, list_make1(makeString("=")), fv_id, id,
							pstate->p_last_srf, -1);

	if (ignore_nullable)
	{
		addNSItemToJoinlist(pstate, nsitem, false);

		pstate->p_resolved_qual = qualAndExpr(pstate->p_resolved_qual, qual);
	}
	else
	{
		JoinType	jointype = (fv->nullable ? JOIN_LEFT : JOIN_INNER);
		Node	   *l_jt;
		int			l_rtindex;
		RangeTblEntry *l_rte;
		ParseNamespaceItem *l_nsitem;
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

		l_nsitem = findNamespaceItemForRTE(pstate, l_rte);
		incrementalJoinRTEs(pstate, jointype, l_nsitem, nsitem, qual, alias);
	}

	/* modify `fv->expr` to the actual vertex */
	fv->expr = (Expr *) vertex;
}

static ParseNamespaceItem *
makeVertexNSItem(ParseState *parentParseState, char *varname, char *labname)
{
	Alias	   *alias;
	ParseState *pstate;
	Query	   *qry;
	RangeVar   *r;
	ParseNamespaceItem *nsitem;
	TargetEntry *te;

	Assert(parentParseState->p_expr_kind == EXPR_KIND_NONE);
	parentParseState->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

	alias = makeAlias(varname, NIL);

	pstate = make_parsestate(parentParseState);
	pstate->p_locked_from_parent = isLockedRefname(pstate, alias->aliasname);

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	r = makeRangeVar(get_graph_path(true), labname, -1);

	nsitem = addRangeTableEntry(pstate, r, alias, true, true);
	addNSItemToJoinlist(pstate, nsitem, false);

	te = makeTargetEntry((Expr *) makeVertexExpr(pstate,
												 nsitem,
												 -1),
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
	ListCell   *le;

	foreach(le, future_vertices)
	{
		FutureVertex *fv = lfirst(le);

		if (fv->expr != NULL)
		{
			future_vertices = foreach_delete_current(future_vertices, le);
		}
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
		Node	   *elem = lfirst(le);

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
		Const	   *dummy;
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
	TargetEntry *te;
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
		char	   *labname = getCypherName(cnode->label);
		Relation	relation;
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
		table_close(relation, NoLock);

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
	Relation	relation;
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
	 * All relationships must be unique and we cannot reference an edge from
	 * the previous clause in CREATE clause.
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

	table_close(relation, NoLock);

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
		 * If the evaluated value of the user-supplied expression is NULL, use
		 * the default properties.
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
transformSetPropList(ParseState *pstate, bool is_remove, CSetKind kind,
					 List *items)
{
	List	   *gsplist = NIL;
	ListCell   *li;

	foreach(li, items)
	{
		CypherSetProp *sp = lfirst(li);

		gsplist = lappend(gsplist, transformSetProp(pstate, sp, is_remove,
													kind));
	}

	return gsplist;
}

static GraphSetProp *
transformSetProp(ParseState *pstate, CypherSetProp *sp, bool is_remove,
				 CSetKind kind)
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
	 * Get the original property map of the element.
	 */
	prop_map = ParseFuncOrColumn(pstate,
								 list_make1(makeString(AG_ELEM_PROP_MAP)),
								 list_make1(elem), pstate->p_last_srf,
								 NULL, false, -1);

	/*
	 * Transform the assigned property to get `expr` (RHS of the SET clause
	 * item). `sp->expr` can be a null constant if this `sp` is for REMOVE.
	 */
	expr = transformCypherExpr(pstate, sp->expr, EXPR_KIND_UPDATE_SOURCE);
	exprtype = exprType(expr);

	if (IsA(expr, Var) && exprtype == VERTEXOID)
	{
		FieldSelect *fselect = makeNode(FieldSelect);

		fselect->arg = (Expr *) expr;
		fselect->fieldnum = Anum_ag_vertex_properties;
		fselect->resulttype = JSONBOID;
		fselect->resulttypmod = -1;
		fselect->resultcollid = InvalidOid;
		expr = (Node *) fselect;
	}
	else if (IsA(expr, Var) && exprtype == EDGEOID)
	{
		FieldSelect *fselect = makeNode(FieldSelect);

		fselect->arg = (Expr *) expr;
		fselect->fieldnum = Anum_ag_edge_properties;
		fselect->resulttype = JSONBOID;
		fselect->resulttypmod = -1;
		fselect->resultcollid = InvalidOid;
		expr = (Node *) fselect;
	}
	else
	{
		expr = coerce_expr(pstate, expr, exprtype, JSONBOID, -1,
						   COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);
	}

	if (expr == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("expression must be of type jsonb but %s",
						format_type_be(exprtype)),
				 parser_errposition(pstate, exprLocation(expr))));

	/*
	 * make the modified property map
	 */
	if (path == NULL)			/* LHS is the property map itself */
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
			Node	   *concat_result;
			CoalesceExpr *coalesce;

			concat = makeFuncCall(list_make1(makeString("jsonb_concat")), NIL,
								  COERCE_EXPLICIT_CALL,
								  -1);
			concat_result = ParseFuncOrColumn(pstate, concat->funcname,
											  list_make2(prop_map, expr),
											  pstate->p_last_srf, concat, false, -1);

			/*
			 * Wrap with COALESCE to handle NULL values gracefully. If expr is
			 * NULL, jsonb_concat returns NULL, so we fall back to the
			 * original prop_map.
			 */
			coalesce = makeNode(CoalesceExpr);
			coalesce->args = list_make2(concat_result, prop_map);
			coalesce->coalescetype = JSONBOID;
			coalesce->location = -1;

			prop_map = (Node *) coalesce;
		}
		else
		{
			/* just overwrite the property map */
			prop_map = expr;
		}
	}
	else						/* LHS is a property in the property map */
	{
		FuncCall   *delete;
		Node	   *del_prop;

		if (sp->add)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("+= operator on a property is not allowed"),
					 parser_errposition(pstate, exprLocation(elem))));

		delete = makeFuncCall(list_make1(makeString("jsonb_delete_path")),
							  NIL, COERCE_EXPLICIT_CALL, -1);
		del_prop = ParseFuncOrColumn(pstate, delete->funcname,
									 list_make2(prop_map, path),
									 pstate->p_last_srf, delete, false, -1);

		if (IsNullAConst(sp->expr) && (!allow_null_properties || is_remove))
		{
			/* SET a.b.c = NULL */
			prop_map = del_prop;
		}
		else					/* SET a.b.c = expr */
		{
			FuncCall   *set_lax;
			Node	   *create_if_missing;
			Node	   *null_treatment;

			/*
			 * UNKNOWNOID 'null' will be passed to jsonb_in() when
			 * ParseFuncOrColumn()
			 */
			if (IsNullAConst(sp->expr))
				expr = (Node *) makeConst(UNKNOWNOID, -1, InvalidOid, -2,
										  CStringGetDatum("null"), false, false);

			/*
			 * Use jsonb_set_lax(map, path, val, true, 'delete_key') instead
			 * of COALESCE(jsonb_set(map, path, val), jsonb_delete_path(map,
			 * path)). It is equivalent -- it sets the key when `val` is a
			 * real value (creating it if missing) and deletes the key when
			 * `val` is SQL NULL, so a NULL right operand behaves like REMOVE
			 * -- but it references `map` (prop_map) exactly once.  The
			 * COALESCE form referenced prop_map twice, which combined with
			 * the per-SET-item RowExpr rebuild made the target expression
			 * grow geometrically with the number of properties set on the
			 * same element.
			 */
			create_if_missing = (Node *) makeBoolConst(true, false);
			null_treatment = (Node *) makeConst(TEXTOID, -1,
												DEFAULT_COLLATION_OID, -1,
												CStringGetTextDatum("delete_key"),
												false, false);

			set_lax = makeFuncCall(list_make1(makeString("jsonb_set_lax")), NIL,
								   COERCE_EXPLICIT_CALL, -1);
			prop_map = ParseFuncOrColumn(pstate, set_lax->funcname,
										 list_make5(prop_map, path, expr,
													create_if_missing,
													null_treatment),
										 pstate->p_last_srf, set_lax, false, -1);
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

	gsp = makeNode(GraphSetProp);
	gsp->kind = gspkind;
	gsp->variable = varname;
	gsp->elem = elem;
	gsp->expr = prop_map;

	return gsp;
}

/*
 * resolve_var_from_targetlist_walker
 *
 * This walker substitutes Targetlist's expr from var expression.
 */
static bool
resolve_var_from_targetlist_walker(Node *node,
								   resolve_var_from_targetlist_context *ctx)
{
	if (node == NULL)
		return false;


	if (ctx->prev_node != NULL && IsA(ctx->prev_node, FieldSelect) &&
		IsA(node, Var))
	{
		Var		   *var = (Var *) node;
		FieldSelect *fieldSelect = (FieldSelect *) ctx->prev_node;
		ExtTLE	   *te_ext = NULL;
		ListCell   *lc;

		foreach(lc, ctx->tle_lists)
		{
			ExtTLE	   *temp = lfirst(lc);

			if (temp->varattno == var->varattno && temp->varno == var->varno)
			{
				te_ext = temp;
				break;
			}
		}

		if (te_ext != NULL)
		{
			fieldSelect->arg = (Expr *) ((RowExpr *) te_ext->tle->expr);
		}
	}

	ctx->prev_node = node;
	return expression_tree_walker(node, resolve_var_from_targetlist_walker, ctx);
}

/*
 * substitute_set_props_as_targetentry
 */
static void
substitute_set_props_as_targetentry(ParseState *pstate, Query *query,
									List *graphSetProps)
{
	ListCell   *cell;
	List	   *ext_tle_lists = NIL;

	foreach(cell, query->targetList)
	{
		TargetEntry *te = lfirst(cell);

		if (!te->resjunk)
		{
			ExtTLE	   *ext_tle = palloc(sizeof(ExtTLE));

			ext_tle->tle = te;
			ext_tle->varattno = ((Var *) te->expr)->varattno;
			ext_tle->varno = ((Var *) te->expr)->varno;
			ext_tle->type = exprType((Node *) te->expr);
			if (ext_tle->type == VERTEXOID)
			{
				/* todo: Need to  */
				Expr	   *new_expr = (Expr *) makeTypedRowExpr(
																 list_make3(
																			getExprField(te->expr, AG_ELEM_LOCAL_ID),
																			getExprField(te->expr, AG_ELEM_PROP_MAP),
																			getExprField(te->expr, "tid")),
																 VERTEXOID, -1);

				te->expr = new_expr;
			}
			else if (ext_tle->type == EDGEOID)
			{
				Expr	   *new_expr = (Expr *) makeTypedRowExpr(
																 list_make5(
																			getExprField(te->expr, AG_ELEM_LOCAL_ID),
																			getExprField(te->expr, AG_START_ID),
																			getExprField(te->expr, AG_END_ID),
																			getExprField(te->expr, AG_ELEM_PROP_MAP),
																			getExprField(te->expr, "tid")),
																 EDGEOID, -1);

				te->expr = new_expr;
			}
			ext_tle_lists = lappend(ext_tle_lists, ext_tle);
		}
	}

	foreach(cell, graphSetProps)
	{
		ListCell   *lc2;
		GraphSetProp *gsp = lfirst(cell);
		resolve_var_from_targetlist_context ctx;
		ExtTLE	   *cur_ext_tle = NULL;

		/* Initialize walker context */
		ctx.tle_lists = ext_tle_lists;
		ctx.query = query;
		ctx.prev_node = NULL;
		foreach(lc2, ext_tle_lists)
		{
			ExtTLE	   *tmp_ext_tle = lfirst(lc2);

			if (strcmp(gsp->variable,
					   tmp_ext_tle->tle->resname) == 0)
			{
				cur_ext_tle = tmp_ext_tle;
				resolve_var_from_targetlist_walker(gsp->expr, &ctx);
				break;
			}
		}

		if (cur_ext_tle != NULL)
		{
			TargetEntry *targetEntry = cur_ext_tle->tle;
			Expr	   *prev_expr = targetEntry->expr;

			/*
			 * Rebuild the element as a RowExpr whose property map is the
			 * result of this SET item and whose other fields are carried over
			 * unchanged.  prev_expr is always the RowExpr produced above (for
			 * the first item) or by the previous iteration, so extract the
			 * carried-over fields directly with getRowExprField() rather than
			 * wrapping the whole prev_expr in a FieldSelect per field -- the
			 * latter grows the expression geometrically with the number of
			 * SET items on the same element (see getRowExprField()).
			 */
			if (cur_ext_tle->type == VERTEXOID)
			{
				Expr	   *new_expr = (Expr *) makeTypedRowExpr(
																 list_make3(
																			getRowExprField(prev_expr, AG_ELEM_LOCAL_ID),
																			gsp->expr,
																			getRowExprField(prev_expr, "tid")),
																 VERTEXOID, -1);

				targetEntry->expr = new_expr;
			}
			else if (cur_ext_tle->type == EDGEOID)
			{
				Expr	   *new_expr = (Expr *) makeTypedRowExpr(
																 list_make5(
																			getRowExprField(prev_expr, AG_ELEM_LOCAL_ID),
																			getRowExprField(prev_expr, AG_START_ID),
																			getRowExprField(prev_expr, AG_END_ID),
																			gsp->expr,
																			getRowExprField(prev_expr, "tid")),
																 EDGEOID, -1);

				targetEntry->expr = new_expr;
			}
		}
	}

	foreach(cell, ext_tle_lists)
	{
		pfree(lfirst(cell));
	}
}

static Query *
transformMergeMatch(ParseState *pstate, Node *parseTree)
{
	CypherClause *clause = (CypherClause *) parseTree;
	Query	   *qry;
	ParseNamespaceItem *nsitem;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	nsitem = transformMergeMatchJoin(pstate, clause);

	qry->targetList = makeTargetListFromJoin(pstate, nsitem);
	markTargetListOrigins(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, pstate->p_resolved_qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;

	qry->hasGraphwriteClause = pstate->p_hasGraphwriteClause;

	assign_query_collations(pstate, qry);

	return qry;
}

/* See transformMatchOptional() */
static ParseNamespaceItem *
transformMergeMatchJoin(ParseState *pstate, CypherClause *clause)
{
	CypherMergeClause *detail = (CypherMergeClause *) clause->detail;
	Node	   *prevclause = clause->prev;
	ParseNamespaceItem *l_nsitem,
			   *r_nsitem;
	Alias	   *r_alias;
	Node	   *qual;
	Alias	   *alias;

	if (prevclause == NULL)
		l_nsitem = transformNullSelect(pstate);
	else
		l_nsitem = transformClause(pstate, prevclause);

	pstate->p_lateral_active = true;

	/*
	 * preprocess merge pattern to check for pattern rules, variable
	 * duplication and create non-existent labels if needed.
	 */
	preprocess_merge_pattern(pstate, detail->pattern, l_nsitem);

	r_alias = makeAliasNoDup(CYPHER_MERGEMATCH_ALIAS, NIL);
	r_nsitem = transformClauseImpl(pstate, makeMatchForMerge(detail->pattern),
								   transformStmt, r_alias);

	pstate->p_lateral_active = false;

	qual = makeBoolConst(true, false);
	alias = makeAliasNoDup(CYPHER_SUBQUERY_ALIAS, NIL);

	return incrementalJoinRTEs(pstate, JOIN_CYPHER_MERGE, l_nsitem, r_nsitem,
							   qual, alias);
}

static ParseNamespaceItem *
transformNullSelect(ParseState *pstate)
{
	ResTarget  *nullres;
	SelectStmt *sel;
	Alias	   *alias;
	Query	   *qry;
	ParseNamespaceItem *nsitem;

	nullres = makeResTarget((Node *) makeNullAConst(), NULL);

	sel = makeNode(SelectStmt);
	sel->targetList = list_make1(nullres);

	alias = makeAliasNoDup(CYPHER_SUBQUERY_ALIAS, NIL);

	Assert(pstate->p_expr_kind == EXPR_KIND_NONE);
	pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

	qry = parse_sub_analyze((Node *) sel, pstate, NULL,
							isLockedRefname(pstate, alias->aliasname), true);

	pstate->p_expr_kind = EXPR_KIND_NONE;

	nsitem = addRangeTableEntryForSubquery(pstate, qry, alias, false, true);
	addNSItemToJoinlist(pstate, nsitem, false);

	return nsitem;
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

	Assert(prevrte != NULL && prevrte->rtekind == RTE_SUBQUERY);

	/*
	 * Copy the target list of the RTE of the previous clause to check
	 * duplicate variables.
	 */
	prevtlist = copyObject(prevrte->subquery->targetList);

	path = linitial(pattern);
	singlenode = (list_length(path->chain) == 1);
	foreach(le, path->chain)
	{
		Node	   *elem = lfirst(le);

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
	char	   *labname = getCypherName(cnode->label);
	TargetEntry *te;
	Relation	relation;
	Node	   *vertex = NULL;
	Oid			relid = InvalidOid;
	AttrNumber	resno = InvalidAttrNumber;
	GraphVertex *gvertex;

	te = findTarget(*targetList, varname);
	if (te != NULL &&
		(exprType((Node *) te->expr) != VERTEXOID || !isNodeForRef(cnode) ||
		 singlenode))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_ALIAS),
				 errmsg("duplicate variable \"%s\"", varname),
				 parser_errposition(pstate, getCypherNameLoc(cnode->variable))));

	if (labname == NULL)
		labname = AG_VERTEX;

	relation = openTargetLabel(pstate, labname);

	vertex = makeNewVertex(pstate, relation, cnode->prop_map);
	relid = RelationGetRelid(relation);

	table_close(relation, NoLock);

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
	char	   *typname;
	Relation	relation;
	Node	   *edge;
	Oid			relid = InvalidOid;
	TargetEntry *te;
	AttrNumber	resno = InvalidAttrNumber;
	GraphEdge  *gedge;

	varname = getCypherName(crel->variable);

	if (findTarget(*targetList, varname) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_ALIAS),
				 errmsg("duplicate variable \"%s\"", varname),
				 parser_errposition(pstate, getCypherNameLoc(crel->variable))));

	typname = getCypherName(linitial(crel->types));

	relation = openTargetLabel(pstate, typname);

	edge = makeNewEdge(pstate, relation, crel->prop_map);
	relid = RelationGetRelid(relation);

	table_close(relation, NoLock);

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
			 * According to the TCK of openCypher, use outgoing direction if
			 * direction is unspecified.
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
transformMergeOnSet(ParseState *pstate, List *sets)
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

	l_oncreate = transformSetPropList(pstate, false, CSET_ON_CREATE,
									  l_oncreate);
	l_onmatch = transformSetPropList(pstate, false, CSET_ON_MATCH,
									 l_onmatch);

	return list_concat(l_onmatch, l_oncreate);
}

static Query *
transformDeleteJoin(ParseState *pstate, Node *parseTree)
{
	CypherClause *clause = (CypherClause *) parseTree;
	Query	   *qry;
	ParseNamespaceItem *nsitem;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	nsitem = transformDeleteJoinNSItem(pstate, clause);
	if (nsitem->p_rte->rtekind == RTE_JOIN)
		qry->targetList = makeTargetListFromJoin(pstate, nsitem);
	else if (nsitem->p_rte->rtekind == RTE_SUBQUERY)
		qry->targetList = makeTargetListFromNSItem(pstate, nsitem);
	else
		elog(ERROR, "unexpected rtekind(%d) in DELETE", nsitem->p_rte->rtekind);

	markTargetListOrigins(pstate, qry->targetList);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, pstate->p_resolved_qual);
	qry->rteperminfos = pstate->p_rteperminfos;
	qry->hasSubLinks = pstate->p_hasSubLinks;

	assign_query_collations(pstate, qry);

	return qry;
}

static Query *
transformDeleteEdges(ParseState *pstate, Node *parseTree)
{
	CypherClause *clause = (CypherClause *) parseTree;
	CypherDeleteClause *detail = (CypherDeleteClause *) clause->detail;
	ParseNamespaceItem *nsitem;
	Query	   *qry;
	List	   *edges = NIL;
	AclMode		targetPerms;

	nsitem = transformClause(pstate, clause->prev);

	edges = extractEdgesExpr(pstate, detail->exprs, EXPR_KIND_OTHER);

	if (!edges)
	{
		qry = makeNode(Query);
		qry->commandType = CMD_SELECT;

		qry->targetList = makeTargetListFromNSItem(pstate, nsitem);
		markTargetListOrigins(pstate, qry->targetList);

		qry->rtable = pstate->p_rtable;
		qry->jointree = makeFromExpr(pstate->p_joinlist,
									 pstate->p_resolved_qual);
		qry->rteperminfos = pstate->p_rteperminfos;
		qry->hasSubLinks = pstate->p_hasSubLinks;

		assign_query_collations(pstate, qry);

		return qry;
	}

	qry = makeNode(Query);
	qry->commandType = CMD_GRAPHWRITE;
	qry->g_writeOp = GWROP_DELETE;
	qry->g_last = false;
	qry->g_detach = false;
	qry->g_eager = true;

	qry->targetList = makeTargetListFromNSItem(pstate, nsitem);

	qry->g_exprs = edges;
	qry->g_nr_modify = pstate->p_nr_modify_clause++;

	markTargetListOrigins(pstate, qry->targetList);

	qry->targetList = (List *) resolve_future_vertex(pstate,
													 (Node *) qry->targetList,
													 FVR_DONT_RESOLVE);

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, pstate->p_resolved_qual);
	qry->hasSubLinks = pstate->p_hasSubLinks;

	qry->hasGraphwriteClause = pstate->p_hasGraphwriteClause;

	assign_query_collations(pstate, qry);

	targetPerms = ACL_DELETE;
	if (pstate->p_valid_labels)
		addRangeTableAllModifiedLabels(pstate, qry, NIL, targetPerms);
	qry->rteperminfos = pstate->p_rteperminfos;

	return qry;
}

/* See transformMatchOptional() */
static ParseNamespaceItem *
transformDeleteJoinNSItem(ParseState *pstate, CypherClause *clause)
{
	CypherDeleteClause *detail = (CypherDeleteClause *) clause->detail;
	ParseNamespaceItem *l_nsitem;
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
	ParseNamespaceItem *r_nsitem;
	Node	   *qual;
	ParseNamespaceItem *join_nsitem;
	Oid			graph_oid = get_graph_path_oid();
	ListCell   *lc;

	/*
	 * Since targets of a DELETE clause refers the result of the previous
	 * clause, it must be transformed first.
	 */
	l_nsitem = transformClauseBy(pstate, (Node *) clause, transformDeleteEdges);

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
								 list_make1(pexpr),
								 COERCE_EXPLICIT_CALL, -1);

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
		 * TODO: `expr` must contain one of the target variables and it
		 * mustn't contain aggregate and SubLink's.
		 */
	}

	vertices = verticesConcat((Node *) vertices_var, vertices_nodes);
	if (vertices == NULL)
		return l_nsitem;

	sel_ag_edge = makeSelectEdgesVertices(vertices, detail, &edges_resname);
	r_alias = makeAliasNoDup(CYPHER_DELETEJOIN_ALIAS, NIL);

	pstate->p_lateral_active = true;
	Assert(pstate->p_expr_kind == EXPR_KIND_NONE);
	pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

	r_qry = parse_sub_analyze(sel_ag_edge, pstate, NULL,
							  isLockedRefname(pstate, r_alias->aliasname),
							  true);
	Assert(r_qry->commandType == CMD_SELECT);

	if (graphmeta_baseline_gathered() &&
		!has_pending_graphmeta_writes() &&
		list_length(exprs) == 1 &&
		exprType(linitial(exprs)) == VERTEXOID)
	{
		char	   *varname;
		EntityInfo *einfo;
		RangeTblEntry *rte = NULL;
		ColumnRef  *cref = linitial_node(ColumnRef, detail->exprs);

		foreach(lc, r_qry->rtable)
		{
			rte = lfirst_node(RangeTblEntry, lc);

			if (rte->rtekind == RTE_RELATION &&
				strcmp(rte->eref->aliasname, DELETE_EDGE_ALIAS) == 0)
				break;
		}

		varname = strVal(linitial(cref->fields));
		einfo = getEntityInfo(pstate, varname, T_CypherNode, false);
		if (einfo != NULL && einfo->labname != NULL)
		{
			/*
			 * Mark the delete-join's ag_edge scan for graphmeta scan pruning:
			 * the planner scans only the edge labels ag_graphmeta records as
			 * incident (either orientation) to the deleted vertex's label,
			 * instead of the whole ag_edge hierarchy.  The label set is
			 * resolved at plan time by propagate_graphmeta_constraints and
			 * applied through the shared graphmeta_pruned[] path
			 * (GRAPHPRUNE_ROLE_DELETE_EDGE), so it stays fresh behind a
			 * cached plan and reuses that path's concurrent-drop lock recheck
			 * and ag_graphmeta plan-cache dependency.
			 */
			rte->graphPruneGraph = graph_oid;
			rte->graphPruneRole = GRAPHPRUNE_ROLE_DELETE_EDGE;
			rte->graphPruneLabid = (int) get_labname_labid(einfo->labname,
														   graph_oid);
		}
	}

	/*
	 * edge variable is only used to determine if there is an edge connected
	 * to the vertex.
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

	r_nsitem = addRangeTableEntryForSubquery(pstate, r_qry, r_alias, true, true);

	qual = makeBoolConst(true, false);

	join_nsitem = incrementalJoinRTEs(pstate, JOIN_CYPHER_DELETE, l_nsitem, r_nsitem,
									  qual,
									  makeAliasNoDup(CYPHER_SUBQUERY_ALIAS, NIL));

	pstate->p_delete_edges_resname = edges_resname;
	return join_nsitem;
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
						  list_make2(vertices, expr),
						  COERCE_EXPLICIT_CALL, -1);

	return (Node *) arrcat;
}

/*
 * if DETACH
 *
 *     SELECT array_agg((id, start, edge, NULL, ctid)::edge) AS <unique-name>
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

	Assert(vertices != NULL);

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
		nulledge->typeName = makeTypeNameFromOid(EDGEOID, -1);
		nulledge->location = -1;

		targetlist = list_make1(
								makeResTarget((Node *) nulledge, NULL));
	}

	ag_edge = makeRangeVar(get_graph_path(true), AG_EDGE, -1);
	ag_edge->inh = true;
	ag_edge->alias = makeAliasNoDup(DELETE_EDGE_ALIAS, NIL);

	unnest = makeRangeFunction(list_make1(makeString("unnest")), vertices,
							   makeAliasNoDup(DELETE_VERTEX_ALIAS, NIL), false);

	sel = makeNode(SelectStmt);
	sel->targetList = targetlist;
	sel->fromClause = list_make2(ag_edge, unnest);
	sel->whereClause = (Node *) makeEdgesVertexQual();

	return (Node *) sel;
}

/* array_agg((id, start, end, NULL, ctid)::edge) */
static Node *
makeEdgesForDetach(void)
{
	Node	   *id;
	Node	   *start;
	Node	   *end;
	A_Const    *prop_map;
	Node	   *tid;
	Node	   *edge;

	id = makeColumnRef(genQualifiedName(DELETE_EDGE_ALIAS, AG_ELEM_ID));
	start = makeColumnRef(genQualifiedName(DELETE_EDGE_ALIAS, AG_START_ID));
	end = makeColumnRef(genQualifiedName(DELETE_EDGE_ALIAS, AG_END_ID));
	prop_map = makeNullAConst();
	tid = makeColumnRef(genQualifiedName(DELETE_EDGE_ALIAS, "ctid"));

	edge = makeRowExprWithTypeCast(list_make5(id, start, end, prop_map, tid),
								   EDGEOID, -1);

	return (Node *) makeArrayAggFuncCall(list_make1(edge), -1);
}

static RangeFunction *
makeRangeFunction(List *func, Node *expr, Alias *alias, bool ordinality)
{
	FuncCall   *fc;
	RangeFunction *rf;

	fc = makeFuncCall(func, list_make1(expr), COERCE_EXPLICIT_CALL, -1);

	rf = makeNode(RangeFunction);
	rf->lateral = false;
	rf->ordinality = ordinality;
	rf->is_rowsfrom = false;
	rf->functions = list_make1(list_make2(fc, NIL));
	rf->alias = alias;
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
		Node	   *expr = lfirst(le);
		Node	   *elem = transformCypherExpr(pstate, expr, exprKind);

		switch (exprType(elem))
		{
			case EDGEOID:
			case EDGEARRAYOID:
				continue;

			case GRAPHPATHOID:
				elem = getExprField((Expr *) elem, AG_PATH_VERTICES);
				/* fall through */
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
		Node	   *expr = lfirst(le);
		Node	   *elem = transformCypherExpr(pstate, expr, exprKind);

		switch (exprType(elem))
		{
			case VERTEXOID:
				continue;

			case GRAPHPATHOID:
				elem = getExprField((Expr *) elem, AG_PATH_EDGES);
				/* fall through */
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
addRangeTableAllModifiedLabels(ParseState *pstate, Query *qry,
							   List *targets, AclMode requiredPerms)
{
	List	   *new_targets = NIL;
	List	   *label_oids = NIL;
	ListCell   *lc;

	/* DELETE */
	if (qry->g_exprs != NIL)
	{
		foreach(lc, qry->g_exprs)
		{
			GraphDelElem *gde = lfirst(lc);

			label_oids = lappend_oid(label_oids,
									 find_target_label(gde->elem, qry));
		}
	}

	/* SET and MERGE ON SET */
	if (qry->g_sets != NIL)
	{
		foreach(lc, qry->g_sets)
		{
			GraphSetProp *gsp = lfirst(lc);

			label_oids = lappend_oid(label_oids,
									 find_target_label(gsp->elem, qry));
		}
	}

	new_targets = targets;
	foreach(lc, label_oids)
	{
		Oid			relid = lfirst_oid(lc);
		List	   *child_oids;

		child_oids = find_all_inheritors(relid, AccessShareLock, NULL);
		new_targets = list_union_oid(new_targets, child_oids);
	}

	addRangeTableLabels(pstate, new_targets, qry, requiredPerms);

	return new_targets;
}

static void
addRangeTableLabels(ParseState *pstate, List *targets, Query *qry,
					AclMode requiredPerms)
{
	List	   *resultRelations = NIL;
	ListCell   *lc;

	foreach(lc, targets)
	{
		Oid			relid = lfirst_oid(lc);
		Relation	relation = table_open(relid, AccessShareLock);

		ParseNamespaceItem *nsitem = addRangeTableEntryForRelation(pstate,
																   relation,
																   AccessShareLock,
																   NULL,
																   false,
																   false);

		nsitem->p_perminfo->requiredPerms = requiredPerms;
		table_close(relation, NoLock);
		resultRelations = lappend_int(resultRelations, nsitem->p_rtindex);
	}
	qry->g_resultRelations = resultRelations;
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
		 * `FVR_PRESERVE_VAR_REF` flag. We need to fix this.
		 */
		if ((qry->g_writeOp == GWROP_SET ||
			 qry->g_writeOp == GWROP_DELETE) &&
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
	 * RTE's for target labels. So, look through `qry->g_pattern` to get the
	 * relid of the target label.
	 *
	 * This code assumes that `RowExpr` appears only as root of the expression
	 * in `TargetEntry` when `wrietOp` is `GWROP_CREATE`. This assumption is
	 * OK because users cannot make `RowExpr` in Cypher.
	 */
	if (IsA(node, RowExpr) && qry->g_writeOp == GWROP_CREATE)
	{
		GraphPath  *gpath;
		ListCell   *le;

		Assert(list_length(qry->g_pattern) == 1);
		gpath = linitial(qry->g_pattern);

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
	 * It is difficult to find a label for graph elements in a graph path, so
	 * all labels of that type are treated as result labels.
	 */
	if (IsA(node, FieldSelect))
	{
		FieldSelect *fs = castNode(FieldSelect, node);
		Oid			graph_oid = get_graph_path_oid();

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

	prop_map = transformCypherExpr(pstate, expr, exprKind);
	prop_map = coerce_expr(pstate, prop_map, exprType(prop_map), JSONBOID, -1,
						   COERCION_IMPLICIT, COERCE_IMPLICIT_CAST, -1);

	if (exprKind == EXPR_KIND_INSERT_TARGET && !allow_null_properties)
		prop_map = stripNullKeys(pstate, prop_map);

	return resolve_future_vertex(pstate, prop_map, 0);
}

static Node *
stripNullKeys(ParseState *pstate, Node *properties)
{
	FuncCall   *strip;

	/* keys with NULL value is not allowed */
	strip = makeFuncCall(list_make1(makeString("jsonb_strip_nulls")), NIL,
						 COERCE_EXPLICIT_CALL, -1);

	return ParseFuncOrColumn(pstate, strip->funcname, list_make1(properties),
							 pstate->p_last_srf, strip, false, -1);

}

static bool
assign_query_eager_walker(Node *node, Query *nxtQry)
{
	if (node == NULL)
		return false;

	if (IsA(node, Query))
	{
		Query	   *qry = (Query *) node;

		if (qry->g_eager == true)
			return true;

		if (qry->commandType == CMD_GRAPHWRITE)
		{
			/* Clauses whose CID is incremented should be run as eager. */
			if (qry->g_sets != NIL ||
				qry->g_exprs != NIL ||
				qry->g_writeOp == GWROP_MERGE)
				qry->g_eager = true;
			else if (nxtQry->g_writeOp == GWROP_MERGE &&
					 (qry->g_writeOp == GWROP_CREATE ||
					  qry->g_writeOp == GWROP_MERGE))
				qry->g_eager = true;
			else
				qry->g_eager = false;

			if (qry->g_eager == true &&
				enable_eager == false)
				elog(ERROR, "eagerness plan is not allowed.");
			return true;
		}

		(void) range_table_walker(qry->rtable,
								  assign_query_eager_walker,
								  (void *) nxtQry,
								  QTW_IGNORE_CTE_SUBQUERIES);
	}

	return false;
}

static void
assign_query_eager(Query *query)
{
	(void) range_table_walker(query->rtable,
							  assign_query_eager_walker,
							  (void *) query,
							  QTW_IGNORE_CTE_SUBQUERIES);

	if (!query->g_last &&
		(query->g_sets != NIL ||
		 query->g_exprs != NIL))
		query->g_eager = true;

	if (query->g_eager == true &&
		enable_eager == false)
		elog(ERROR, "eagerness plan is not allowed.");
}

static ParseNamespaceItem *
transformClause(ParseState *pstate, Node *clause)
{
	return transformClauseBy(pstate, clause, transformStmt);
}

static ParseNamespaceItem *
transformClauseBy(ParseState *pstate, Node *clause, TransformMethod transform)
{
	Alias	   *alias;
	ParseNamespaceItem *nsitem;

	alias = makeAliasNoDup(CYPHER_SUBQUERY_ALIAS, NIL);
	nsitem = transformClauseImpl(pstate, clause, transform, alias);
	addNSItemToJoinlist(pstate, nsitem, true);

	return nsitem;
}

static void
mark_nodes_as_nonlocal(List *nis)
{
	ListCell   *lc;

	foreach(lc, nis)
	{
		EntityInfo *ei = lfirst(lc);

		ei->local = false;
	}
}

static ParseNamespaceItem *
transformClauseImpl(ParseState *pstate, Node *clause,
					TransformMethod transform, Alias *alias)
{
	ParseState *childParseState;
	Query	   *qry;
	List	   *future_vertices;
	ParseNamespaceItem *nsitem;

	Assert(IsA(clause, CypherClause));

	Assert(pstate->p_expr_kind == EXPR_KIND_NONE);
	pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

	childParseState = make_parsestate(pstate);
	childParseState->p_is_match_quals = pstate->p_is_match_quals;
	childParseState->p_is_fp_processed = pstate->p_is_fp_processed;
	childParseState->p_is_optional_match = pstate->p_is_optional_match;
	childParseState->p_valid_labels = pstate->p_valid_labels;

	qry = transform(childParseState, clause);

	pstate->p_elem_quals = childParseState->p_elem_quals;
	future_vertices = childParseState->p_future_vertices;
	if (childParseState->p_nr_modify_clause > 0)
		pstate->p_nr_modify_clause = childParseState->p_nr_modify_clause;
	pstate->p_delete_edges_resname = childParseState->p_delete_edges_resname;
	pstate->p_hasGraphwriteClause = childParseState->p_hasGraphwriteClause;
	pstate->p_valid_labels = childParseState->p_valid_labels;

	mark_nodes_as_nonlocal(childParseState->p_entity_info_list);
	pstate->p_entity_info_list = list_concat(pstate->p_entity_info_list,
											 childParseState->p_entity_info_list);

	free_parsestate(childParseState);

	pstate->p_expr_kind = EXPR_KIND_NONE;

	if (!IsA(qry, Query) ||
		(qry->commandType != CMD_SELECT &&
		 qry->commandType != CMD_GRAPHWRITE) ||
		qry->utilityStmt != NULL)
		elog(ERROR, "unexpected command in previous clause");

	nsitem = addRangeTableEntryForSubquery(pstate, qry, alias,
										   pstate->p_lateral_active, true);

	adjustElemQuals(pstate->p_elem_quals, nsitem);

	future_vertices = removeResolvedFutureVertices(future_vertices);
	future_vertices = adjustFutureVertices(future_vertices, nsitem);
	pstate->p_future_vertices = list_concat(pstate->p_future_vertices,
											future_vertices);

	return nsitem;
}

static ParseNamespaceItem *
incrementalJoinRTEs(ParseState *pstate, JoinType jointype,
					ParseNamespaceItem *l_nsitem, ParseNamespaceItem *r_nsitem,
					Node *qual, Alias *alias)
{
	ListCell   *le;
	Node	   *l_jt = NULL;
	RangeTblRef *r_rtr;
	ParseNamespaceColumn *res_nscolumns;
	List	   *res_colnames = NIL,
			   *res_colvars = NIL;
	List	   *l_colnos,
			   *r_colnos;
	JoinExpr   *j;
	int			i;
	ParseNamespaceItem *nsitem;

	l_nsitem->p_cols_visible = false;

	/* find JOIN-subtree of `l_rte` */
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

		if (rtindex == l_nsitem->p_rtindex)
			l_jt = jt;
	}
	Assert(l_jt != NULL);

	makeExtraFromNSItem(r_nsitem, &r_rtr, false);

	j = makeNode(JoinExpr);
	j->jointype = jointype;
	j->larg = l_jt;
	j->rarg = (Node *) r_rtr;
	j->quals = qual;
	j->alias = alias;

	/*
	 * Since this is left join, we need to mark j->rarg as it may potentially
	 * emit NULL. The jindex argument holds rtindex of the join's RTE, which
	 * is created right after j->arg's RTE in this case.
	 */
	markRelsAsNulledBy(pstate, j->rarg, r_nsitem->p_rtindex + 1);

	makeJoinResCols(pstate, l_nsitem, r_nsitem, &l_colnos, &r_colnos, &res_colnames,
					&res_colvars);
	res_nscolumns = (ParseNamespaceColumn *) palloc0((list_length(l_colnos) +
													  list_length(r_colnos)) *
													 sizeof(ParseNamespaceColumn));
	nsitem = addRangeTableEntryForJoin(pstate,
									   res_colnames,
									   res_nscolumns,
									   j->jointype,
									   list_length(j->usingClause),
									   res_colvars,
									   l_colnos,
									   r_colnos,
									   j->join_using_alias,
									   j->alias,
									   true);
	j->rtindex = nsitem->p_rtindex;

	for (i = list_length(pstate->p_joinexprs) + 1; i < j->rtindex; i++)
		pstate->p_joinexprs = lappend(pstate->p_joinexprs, NULL);
	pstate->p_joinexprs = lappend(pstate->p_joinexprs, j);
	Assert(list_length(pstate->p_joinexprs) == j->rtindex);

	pstate->p_joinlist = list_delete_ptr(pstate->p_joinlist, l_jt);
	pstate->p_joinlist = lappend(pstate->p_joinlist, j);

	makeExtraFromNSItem(nsitem, NULL, true);
	pstate->p_namespace = lappend(pstate->p_namespace, r_nsitem);
	pstate->p_namespace = lappend(pstate->p_namespace, nsitem);

	return nsitem;
}

static void
makeJoinResCols(ParseState *pstate, ParseNamespaceItem *l_rte,
				ParseNamespaceItem *r_rte,
				List **l_colvars, List **r_colvars,
				List **res_colnames,
				List **res_colvars)
{
	List	   *l_colnames,
			   *r_colnames;
	ListCell   *r_lname,
			   *r_lvar;
	List	   *colnames = NIL,
			   *colvars = NIL;

	expandRTE(l_rte->p_rte, l_rte->p_rtindex, 0, VAR_RETURNING_DEFAULT, -1,
			  false, &l_colnames, l_colvars);
	expandRTE(r_rte->p_rte, r_rte->p_rtindex, 0, VAR_RETURNING_DEFAULT, -1,
			  false, &r_colnames, r_colvars);

	*res_colnames = list_concat(*res_colnames, l_colnames);
	*res_colvars = list_concat(*res_colvars, *l_colvars);

	forboth(r_lname, r_colnames, r_lvar, *r_colvars)
	{
		char	   *r_colname = strVal(lfirst(r_lname));
		ListCell   *lname;
		ListCell   *lvar;
		Var		   *var = NULL;

		forboth(lname, *res_colnames, lvar, *res_colvars)
		{
			char	   *colname = strVal(lfirst(lname));

			if (strcmp(r_colname, colname) == 0)
			{
				var = lfirst(lvar);
				break;
			}
		}

		if (var == NULL)
		{
			Var		   *v;

			/*
			 * Each join (left) RTE's Var, that references a column of the
			 * right RTE, needs to be marked 'nullable'.
			 */
			v = lfirst(r_lvar);
			markNullableIfNeeded(pstate, v);

			colnames = lappend(colnames, lfirst(r_lname));
			colvars = lappend(colvars, v);
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

static ParseNamespaceItem *
findNamespaceItemForRTE(ParseState *pstate, RangeTblEntry *rte)
{
	ListCell   *lni;

	foreach(lni, pstate->p_namespace)
	{
		ParseNamespaceItem *nsitem = lfirst(lni);

		if (nsitem->p_rte == rte)
			return nsitem;
	}

	return NULL;
}

static List *
makeTargetListFromNSItem(ParseState *pstate, ParseNamespaceItem *nsitem)
{
	List	   *targetlist = NIL;
	int			varattno;
	ListCell   *ln;
	ListCell   *lt;

	Assert(nsitem->p_rte->rtekind == RTE_SUBQUERY);

	varattno = 1;
	ln = list_head(nsitem->p_rte->eref->colnames);
	foreach(lt, nsitem->p_rte->subquery->targetList)
	{
		TargetEntry *te = lfirst(lt);
		Var		   *varnode;
		char	   *resname;
		TargetEntry *tmp;

		if (te->resjunk)
			continue;

		Assert(varattno == te->resno);

		/* no transform here, just use `te->expr` */
		varnode = makeVar(nsitem->p_rtindex, varattno,
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
		ln = lnext(nsitem->p_rte->eref->colnames, ln);
	}

	return targetlist;
}

static List *
makeTargetListFromJoin(ParseState *pstate, ParseNamespaceItem *nsitem)
{
	List	   *targetlist = NIL;
	ListCell   *lt;
	ListCell   *ln;
	RangeTblEntry *rte = nsitem->p_rte;

	Assert(rte->rtekind == RTE_JOIN);

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
makeWholeRowTarget(ParseState *pstate, ParseNamespaceItem *nsitem)
{
	Var		   *varnode;

	varnode = makeWholeRowVar(nsitem->p_rte, nsitem->p_rtindex, 0, false);
	varnode->location = -1;

	markVarForSelectPriv(pstate, varnode);

	return makeTargetEntry((Expr *) varnode,
						   (AttrNumber) pstate->p_next_resno++,
						   nsitem->p_rte->eref->aliasname,
						   false);
}

static TargetEntry *
findTarget(List *targetList, char *resname)
{
	ListCell   *lt;
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
makePathVertexExpr(ParseState *pstate, Node *obj, bool is_nsitem)
{
	if (is_nsitem)
	{
		return makeVertexExpr(pstate, (ParseNamespaceItem *) obj, -1);
	}
	else
	{
		TargetEntry *te = (TargetEntry *) obj;

		Assert(IsA(obj, TargetEntry));
		Assert(exprType((Node *) te->expr) == VERTEXOID);

		return (Node *) te->expr;
	}
}

static Node *
makePathEdgeExpr(ParseState *pstate, CypherRel *crel, Node *obj, bool is_nsitem)
{
	if (is_nsitem)
	{
		return makeEdgeExpr(pstate, crel, (ParseNamespaceItem *) obj, -1);
	}
	else
	{
		TargetEntry *te = (TargetEntry *) obj;

		Assert(IsA(obj, TargetEntry));
		Assert(exprType((Node *) te->expr) == EDGEOID);

		return (Node *) te->expr;
	}
}

Node *
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
		attr = TupleDescAttr(tupdesc, idx);

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

/*
 * getRowExprField
 *
 * Like getExprField(), but for a `rowexpr` that is already a RowExpr built with
 * one argument per column of its row type (as the SET target assembly does for
 * vertices and edges).  Instead of wrapping the whole RowExpr in a FieldSelect,
 * this returns the matching argument sub-expression directly.
 *
 * This matters when several properties are set on the same element.  The SET
 * target for an element is rebuilt as a fresh RowExpr for every SET item, and
 * the unchanged id/start/end/tid fields are carried over from the previous
 * RowExpr.  Re-deriving them with getExprField() embeds a full FieldSelect over
 * the entire previous RowExpr each time, so the target expression grows
 * geometrically with the number of SET items (which the planner then walks and
 * copies once per inherited child table).  Extracting the sub-node keeps the
 * carried-over fields as their original leaf expressions, so the target stays
 * linear in the number of SET items.
 */
static Node *
getRowExprField(Expr *rowexpr, char *fname)
{
	Oid			typoid;
	TupleDesc	tupdesc;
	int			idx;

	Assert(IsA(rowexpr, RowExpr));

	typoid = exprType((Node *) rowexpr);

	tupdesc = lookup_rowtype_tupdesc_copy(typoid, -1);
	for (idx = 0; idx < tupdesc->natts; idx++)
	{
		if (namestrcmp(&TupleDescAttr(tupdesc, idx)->attname, fname) == 0)
			break;
	}
	Assert(idx < tupdesc->natts);
	Assert(idx < list_length(((RowExpr *) rowexpr)->args));

	return (Node *) list_nth(((RowExpr *) rowexpr)->args, idx);
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
		BoolExpr   *bexpr = (BoolExpr *) qual;

		if (bexpr->boolop == AND_EXPR)
		{
			bexpr->args = lappend(bexpr->args, expr);
			return qual;
		}
	}

	return (Node *) makeBoolExpr(AND_EXPR, list_make2(qual, expr), -1);
}

static A_Const *
makeBoolAConst(bool state, int location)
{
	A_Const    *n = makeNode(A_Const);

	n->val.boolval.type = T_Boolean;
	n->val.boolval.boolval = state;
	n->location = location;

	return n;
}

static A_Const *
makeNullAConst(void)
{
	A_Const    *nullconst;

	nullconst = makeNode(A_Const);
	nullconst->isnull = true;
	nullconst->location = -1;

	return nullconst;
}

static Node *
makeIntConst(int val, int location)
{
	A_Const    *n = makeNode(A_Const);

	n->val.ival.type = T_Integer;
	n->val.ival.ival = val;
	n->location = location;

	return (Node *) n;
}

static bool
IsNullAConst(Node *arg)
{
	Assert(arg != NULL);

	if (IsA(arg, A_Const))
	{
		A_Const    *con = (A_Const *) arg;

		if (con->isnull)
			return true;
	}
	return false;
}

/*
 * Retrieve a column variable or expression field from the given node.
 */
static Node *
resolveVarOrExpr(ParseState *pstate, Node *node,
				 char *colname, bool node_is_nsitem)
{
	if (node_is_nsitem)
	{
		ParseNamespaceItem *nsitem = (ParseNamespaceItem *) node;

		Assert(nsitem->p_rte->rtekind == RTE_RELATION ||
			   nsitem->p_rte->rtekind == RTE_SUBQUERY);

		return getColumnVar(pstate, nsitem, colname);
	}
	else
	{
		TargetEntry *te = (TargetEntry *) node;

		Assert(IsA(node, TargetEntry));

		return getExprField(te->expr, colname);
	}
}

/*
 * markRelsAsNulledBy -
 *	  Mark the given jointree node and its children as nulled by join jindex
 */
static void
markRelsAsNulledBy(ParseState *pstate, Node *n, int jindex)
{
	int			varno;
	ListCell   *lc;

	/* Note: we can't see FromExpr here */
	if (IsA(n, RangeTblRef))
	{
		varno = ((RangeTblRef *) n)->rtindex;
	}
	else if (IsA(n, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) n;

		/* recurse to children */
		markRelsAsNulledBy(pstate, j->larg, jindex);
		markRelsAsNulledBy(pstate, j->rarg, jindex);
		varno = j->rtindex;
	}
	else
	{
		elog(ERROR, "unrecognized node type: %d", (int) nodeTag(n));
		varno = 0;				/* keep compiler quiet */
	}

	/*
	 * Now add jindex to the p_nullingrels set for relation varno.  Since we
	 * maintain the p_nullingrels list lazily, we might need to extend it to
	 * make the varno'th entry exist.
	 */
	while (list_length(pstate->p_nullingrels) < varno)
		pstate->p_nullingrels = lappend(pstate->p_nullingrels, NULL);
	lc = list_nth_cell(pstate->p_nullingrels, varno - 1);
	lfirst(lc) = bms_add_member((Bitmapset *) lfirst(lc), jindex);
}

/*
 * Helper function for MERGE clause to check pattern rules,
 * variable duplication, and label existence. If a label
 * involved in pattern does not exist, it will be created.
 */
static void
preprocess_merge_pattern(ParseState *pstate, List *pattern,
						 ParseNamespaceItem *nsitem)
{
	CypherPath *path;
	ListCell   *le;
	char	   *varname;
	TargetEntry *te;
	List	   *targetList;
	bool		singlenode;

	targetList = nsitem->p_rte->subquery->targetList;
	path = linitial(pattern);
	varname = getCypherName(path->variable);
	singlenode = (list_length(path->chain) == 1);

	if (varname && findTarget(targetList, varname))
	{
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_ALIAS),
				 errmsg("duplicate variable \"%s\"", varname),
				 parser_errposition(pstate, getCypherNameLoc(path->variable))));
	}

	foreach(le, path->chain)
	{
		Node	   *elem = lfirst(le);

		if (IsA(elem, CypherNode))
		{
			CypherNode *cnode = (CypherNode *) elem;
			char	   *labname;

			/* Check for duplicate variable */
			varname = getCypherName(cnode->variable);
			te = findTarget(targetList, varname);
			if (te != NULL &&
				(exprType((Node *) te->expr) != VERTEXOID ||
				 !isNodeForRef(cnode) ||
				 singlenode))
			{
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_ALIAS),
						 errmsg("duplicate variable \"%s\"", varname),
						 parser_errposition(pstate, getCypherNameLoc(cnode->variable))));
			}

			/* Check for default label and non-existent label */
			labname = getCypherName(cnode->label);
			if (labname != NULL)
			{
				int			labloc = getCypherNameLoc(cnode->label);

				if (strcmp(labname, AG_VERTEX) == 0)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("specifying default label is not allowed"),
							 parser_errposition(pstate, labloc)));

				createVertexLabelIfNotExist(pstate, labname, labloc);
			}
		}
		else
		{
			CypherRel  *crel = (CypherRel *) elem;
			Node	   *type;
			char	   *typname;

			/* Check for duplicate variable */
			varname = getCypherName(crel->variable);
			if (varname && findTarget(targetList, varname))
			{
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_ALIAS),
						 errmsg("duplicate variable \"%s\"", varname),
						 parser_errposition(pstate, getCypherNameLoc(crel->variable))));
			}

			/* General rule checks */
			if (list_length(crel->types) != 1)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("Exactly one relationship type must be specified for MERGE")));

			if (crel->varlen != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("variable length relationship is not allowed for MERGE")));

			/* Check for default label and non-existent label */
			type = linitial(crel->types);
			typname = getCypherName(type);
			if (typname != NULL)
			{
				int			typloc = getCypherNameLoc(type);

				if (strcmp(typname, AG_EDGE) == 0)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("cannot create edge on default label"),
							 parser_errposition(pstate, typloc)));

				createEdgeLabelIfNotExist(pstate, typname, typloc);
			}
		}
	}

}
