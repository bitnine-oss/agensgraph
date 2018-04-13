/*
 * parse_shortestpath.c
 *	  handle clauses for graph in parser
 *
 * Copyright (c) 2017 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/parser/parse_shortestpath.c
 */

#include "postgres.h"

#include "ag_const.h"
#include "catalog/ag_graph_fn.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "optimizer/var.h"
#include "parser/analyze.h"
#include "parser/parse_agg.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_cypher_expr.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_relation.h"
#include "parser/parse_shortestpath.h"
#include "parser/parse_target.h"
#include "utils/builtins.h"

#define SP_ALIAS_CTE		"_sp"

#define SP_COLNAME_VIDS		"vids"
#define SP_COLNAME_EIDS		"eids"
#define SP_COLNAME_HOPS		"hops"
#define SP_COLNAME_VID		"vid"

/* semantic checks */
static void checkNodeForRef(ParseState *pstate, CypherNode *cnode);
static void checkNodeReferable(ParseState *pstate, CypherNode *cnode);
static void checkRelFormat(ParseState *pstate, CypherRel *rel);
static void checkRelFormatForDijkstra(ParseState *pstate, CypherRel *crel);

/* shortest path */
static Query *makeShortestPathQuery(ParseState *pstate, CypherPath *cpath,
									bool isexpr);
static SelectStmt *makeNonRecursiveTerm(ParseState *pstate, CypherPath *cpath);
static SelectStmt *makeRecursiveTerm(ParseState *pstate, CypherPath *cpath);
static RangeSubselect *makeEdgeUnion(char *edge_label);
static SelectStmt *makeSelectWith(CypherPath *cpath, WithClause *with,
								  bool isexpr);
static RangeSubselect *makeSubselectCTE(CypherPath *cpath);
static Node *makeVerticesSubLink(void);
static Node *makeEdgesSubLink(CypherPath *cpath, bool is_dijkstra);
static void getCypherRelType(CypherRel *crel, char **typname, int *typloc);
static Node *makeVertexIdExpr(Node *vertex);

/* dijkstra */
static Query *makeDijkstraQuery(ParseState *pstate, CypherPath *cpath,
								bool is_expr);
static RangeTblEntry *makeDijkstraFrom(ParseState *parentParseState,
									   CypherPath *cpath);
static RangeTblEntry *makeDijkstraEdgeQuery(ParseState *pstate,
											CypherPath *cpath);
static Node *makeDijkstraEdgeUnion(char *elabel_name, char *row_name);
static Node *makeDijkstraEdge(char *elabel_name, char *row_name,
							  CypherRel *crel);

/* parse node */
static Alias *makeAliasNoDup(char *aliasname, List *colnames);
static Node *makeColumnRef1(char *colname);
static Node *makeColumnRef(List *fields);
static ResTarget *makeSimpleResTarget(char *field, char *name);
static ResTarget *makeArrayAppendResTarget(Node *arr, Node *elem);
static ResTarget *makeResTarget(Node *val, char *name);
static A_Const *makeIntConst(int val);
static Node *makeAArrayExpr(List *elements, char *typeName);
static Node *makeRowExpr(List *args, char *typeName);
static Node *makeSubLink(SelectStmt *sel);

/* utils */
static void addRTEtoJoinlist(ParseState *pstate, RangeTblEntry *rte,
							 bool visible);
static Alias *makeAliasOptUnique(char *aliasname);
static char *genUniqueName(void);

Query *
transformShortestPath(ParseState *pstate, CypherPath *cpath)
{
	Assert(list_length(cpath->chain) == 3);

	checkNodeForRef(pstate, linitial(cpath->chain));
	checkRelFormat(pstate, lsecond(cpath->chain));
	checkNodeForRef(pstate, llast(cpath->chain));

	return makeShortestPathQuery(pstate, cpath, true);
}

Query *
transformShortestPathInMatch(ParseState *parentParseState, CypherPath *cpath)
{
	ParseState *pstate = make_parsestate(parentParseState);
	Query	   *qry;

	Assert(list_length(cpath->chain) == 3);

	checkNodeReferable(pstate, linitial(cpath->chain));
	checkRelFormat(pstate, lsecond(cpath->chain));
	checkNodeReferable(pstate, llast(cpath->chain));

	qry = makeShortestPathQuery(pstate, cpath, false);

	free_parsestate(pstate);

	return qry;
}

static void
checkNodeForRef(ParseState *pstate, CypherNode *cnode)
{
	checkNodeReferable(pstate, cnode);

	if (getCypherName(cnode->label) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("label is not supported"),
				 parser_errposition(pstate, getCypherNameLoc(cnode->label))));

	if (cnode->prop_map != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("property constraint is not supported"),
				 parser_errposition(pstate,
									getCypherNameLoc(cnode->variable))));
}

static void
checkNodeReferable(ParseState *pstate, CypherNode *cnode)
{
	char	   *varname = getCypherName(cnode->variable);
	int			varloc = getCypherNameLoc(cnode->variable);
	Node	   *col;

	if (varname == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("node must be a reference to a specific node")));

	col = colNameToVar(pstate, varname, false, varloc);
	if (col == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("variable \"%s\" does not exist", varname),
				 parser_errposition(pstate, varloc)));
	if (exprType(col) != VERTEXOID)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("variable \"%s\" is not a vertex", varname),
				 parser_errposition(pstate, varloc)));

	if (cnode->prop_map != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("property constraint is not supported"),
				 parser_errposition(pstate, varloc)));
}

static void
checkRelFormat(ParseState *pstate, CypherRel *crel)
{
	if (crel->variable != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("variable is not supported"),
				 parser_errposition(pstate, getCypherNameLoc(crel->variable))));

	if (crel->varlen != NULL)
	{
		A_Indices  *indices = (A_Indices *) crel->varlen;
		A_Const	   *lidx = (A_Const *) indices->lidx;

		if (lidx->val.val.ival > 1)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("only 0 or 1 is allowed for minimal length"),
					 parser_errposition(pstate, lidx->location)));
	}

	if (crel->prop_map != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("property constraint is not supported")));
}

/*
 * WITH _sp(vids, eids, hops, vid) AS (
 *   VALUES (ARRAY[id(`initialVertex`)]::graphid[],
 *           ARRAY[]::rowid[], 0, id(`initialVertex`))
 *   UNION ALL
 *   SELECT array_append(vids, "end"),
 *          array_append(eids, rowid(tableoid, ctid)),
 *          hops + 1,
 *          "end"
 *   FROM _sp, `get_graph_path()`.`typname` AS _e(id, start, "end", properties)
 *   WHERE vid = start AND array_position(vids, "end") IS NULL
 * )
 * SELECT (
 *     (
 *       SELECT array_agg(
 *           (
 *             SELECT (id, properties)::vertex
 *             FROM `get_graph_path()`.ag_vertex
 *             WHERE id = vid
 *           )
 *         )
 *       FROM unnest(vids) AS vid
 *     ),
 *     (
 *       SELECT array_agg(
 *           (
 *             SELECT (id, start, "end", properties)::edge
 *             FROM `get_graph_path()`.`typname`
 *             WHERE tableoid = rowid_tableoid(eid) AND ctid = rowid_ctid(eid)
 *           )
 *         )
 *       FROM unnest(eids) AS eid
 *     )
 *   )::graphpath AS `pathname`
 * FROM
 * (
 *   SELECT vids, eids
 *   FROM _sp
 *   WHERE vid = id(`lastVertex`) AND hops >= `lidx`
 *   LIMIT 1
 * ) AS _r
 */
static Query *
makeShortestPathQuery(ParseState *pstate, CypherPath *cpath, bool isexpr)
{
	SelectStmt *u;
	CommonTableExpr *cte;
	CypherRel  *crel;
	A_Indices  *indices;
	WithClause *with;
	SelectStmt *sp;

	u = makeNode(SelectStmt);
	u->op = SETOP_UNION;
	if (cpath->kind == CPATH_SHORTEST_ALL)
		u->all = true;
	u->larg = makeNonRecursiveTerm(pstate, cpath);
	u->rarg = makeRecursiveTerm(pstate, cpath);

	cte = makeNode(CommonTableExpr);
	cte->ctename = SP_ALIAS_CTE;
	cte->aliascolnames = list_make4(makeString(SP_COLNAME_VIDS),
									makeString(SP_COLNAME_EIDS),
									makeString(SP_COLNAME_HOPS),
									makeString(SP_COLNAME_VID));
	cte->ctequery = (Node *) u;
	cte->ctestop = true;
	cte->location = -1;

	crel = lsecond(cpath->chain);
	indices = (A_Indices *) crel->varlen;
	if (indices == NULL)
	{
		cte->maxdepth = 1 + 1;
	}
	else
	{
		if (indices->uidx != NULL)
		{
			A_Const	*uidx = (A_Const *) indices->uidx;

			cte->maxdepth = uidx->val.val.ival + 1;
		}
	}

	with = makeNode(WithClause);
	with->ctes = list_make1(cte);
	with->recursive = true;
	with->location = -1;

	sp = makeSelectWith(cpath, with, isexpr);

	return transformStmt(pstate, (Node *) sp);
}

/* VALUES (ARRAY[id(`initialVertex`)]::graphid[],
 *         ARRAY[]::rowid[], 0, id(`initialVertex`)) */
static SelectStmt *
makeNonRecursiveTerm(ParseState *pstate, CypherPath *cpath)
{
	CypherNode *cnode;
	Node	   *initialVertex;
	Node	   *col;
	ResTarget  *vids;
	ResTarget  *eids;
	ResTarget  *hops;
	ResTarget  *vid;
	List	   *tlist = NIL;
	SelectStmt *sel;

	cnode = linitial(cpath->chain);
	initialVertex = makeColumnRef1(getCypherName(cnode->variable));
	col = makeAArrayExpr(list_make1(makeVertexIdExpr(initialVertex)),
						 "_graphid");
	vids = makeResTarget(col, SP_COLNAME_VIDS);

	col = makeAArrayExpr(NIL, "_rowid");
	eids = makeResTarget(col, SP_COLNAME_EIDS);

	hops = makeResTarget((Node *) makeIntConst(0), SP_COLNAME_HOPS);

	vid = makeResTarget(makeVertexIdExpr(initialVertex), SP_COLNAME_VID);

	tlist = list_make4(vids, eids, hops, vid);

	sel = makeNode(SelectStmt);
	sel->targetList = tlist;

	return sel;
}

/*
 * SELECT array_append(vids, "end"),
 *        array_append(eids, rowid(tableoid, ctid)),
 *        hops + 1,
 *        "end"
 * FROM _sp, `get_graph_path()`.`typname` AS _e(id, start, "end", properties)
 * WHERE vid = start AND array_position(vids, "end") IS NULL
 */
static SelectStmt *
makeRecursiveTerm(ParseState *pstate, CypherPath *cpath)
{
	Node	   *start;
	Node	   *end;
	SelectStmt *sel;
	Node	   *vids;
	Node	   *eids;
	Node	   *tableoid;
	Node	   *ctid;
	FuncCall   *rowid;
	Node	   *hops;
	RangeVar   *sp;
	CypherRel  *crel;
	char	   *typname;
	Node	   *e;
	List	   *where_args;
	Node	   *last_vid;
	A_Expr	   *joincond;
	FuncCall   *arrpos;
	NullTest   *dupcond;

	start = makeColumnRef1(AG_START_ID);
	end = makeColumnRef1(AG_END_ID);

	sel = makeNode(SelectStmt);

	/* vids */
	vids = makeColumnRef1(SP_COLNAME_VIDS);
	sel->targetList = list_make1(makeArrayAppendResTarget(vids, end));

	/* eids */
	eids = makeColumnRef1(SP_COLNAME_EIDS);
	tableoid = makeColumnRef1("tableoid");
	ctid = makeColumnRef1("ctid");
	rowid = makeFuncCall(list_make1(makeString("rowid")),
						 list_make2(tableoid, ctid), -1);
	sel->targetList = lappend(sel->targetList,
							  makeArrayAppendResTarget(eids, (Node *) rowid));

	/* hops */
	hops = (Node *) makeSimpleA_Expr(AEXPR_OP, "+",
									 makeColumnRef1(SP_COLNAME_HOPS),
									 (Node *) makeIntConst(1), -1);
	sel->targetList = lappend(sel->targetList, makeResTarget(hops, NULL));

	/* vid */
	sel->targetList = lappend(sel->targetList,
							  makeResTarget(copyObject(end), NULL));

	/* FROM */
	sp = makeRangeVar(NULL, SP_ALIAS_CTE, -1);
	crel = lsecond(cpath->chain);
	getCypherRelType(crel, &typname, NULL);
	if (crel->direction == CYPHER_REL_DIR_NONE)
	{
		RangeSubselect *sub;

		sub = makeEdgeUnion(typname);
		sub->alias = makeAliasNoDup("_e", NIL);
		e = (Node *) sub;
	}
	else
	{
		RangeVar   *r;

		r = makeRangeVar(get_graph_path(true), typname, -1);
		r->alias = makeAliasNoDup("_e", NIL);
		if (crel->direction == CYPHER_REL_DIR_LEFT)
		{
			/* swap start with end to preserve the direction */
			r->alias->colnames = list_make4(makeString(AG_ELEM_LOCAL_ID),
											makeString(AG_END_ID),
											makeString(AG_START_ID),
											makeString(AG_ELEM_PROP_MAP));
		}
		r->inh = true;
		e = (Node *) r;
	}
	sel->fromClause = list_make2(sp, e);

	/*
	 * WHERE
	 */

	/* _sp JOIN _e */
	last_vid = makeColumnRef1(SP_COLNAME_VID);
	joincond = makeSimpleA_Expr(AEXPR_OP, "=", last_vid, start, -1);
	where_args = list_make1(joincond);

	/* vertex uniqueness */
	if (cpath->kind == CPATH_SHORTEST_ALL)
	{
		arrpos = makeFuncCall(list_make1(makeString("array_position")),
							  list_make2(vids, end), -1);
		dupcond = makeNode(NullTest);
		dupcond->arg = (Expr *) arrpos;
		dupcond->nulltesttype = IS_NULL;
		dupcond->location = -1;
		where_args = lappend(where_args, dupcond);
	}

	sel->whereClause = (Node *) makeBoolExpr(AND_EXPR, where_args, -1);

	return sel;
}

/*
 * SELECT id, start, "end", tableoid, ctid
 * FROM `get_graph_path()`.`edge_label`
 * UNION
 * SELECT id, "end" AS start, start AS "end", tableoid, ctid
 * FROM `get_graph_path()`.`edge_label`
 */
static RangeSubselect *
makeEdgeUnion(char *edge_label)
{
	ResTarget  *id;
	RangeVar   *r;
	SelectStmt *lsel;
	SelectStmt *rsel;
	SelectStmt *u;
	RangeSubselect *sub;

	id = makeSimpleResTarget(AG_ELEM_LOCAL_ID, NULL);

	r = makeRangeVar(get_graph_path(true), edge_label, -1);
	r->inh = true;

	lsel = makeNode(SelectStmt);
	lsel->targetList = list_make1(id);
	lsel->fromClause = list_make1(r);

	rsel = copyObject(lsel);

	lsel->targetList = lappend(lsel->targetList,
							   makeSimpleResTarget(AG_START_ID, NULL));
	lsel->targetList = lappend(lsel->targetList,
							   makeSimpleResTarget(AG_END_ID, NULL));
	lsel->targetList = lappend(lsel->targetList,
							   makeSimpleResTarget("tableoid", NULL));
	lsel->targetList = lappend(lsel->targetList,
							   makeSimpleResTarget("ctid", NULL));

	rsel->targetList = lappend(rsel->targetList,
							   makeSimpleResTarget(AG_END_ID, AG_START_ID));
	rsel->targetList = lappend(rsel->targetList,
							   makeSimpleResTarget(AG_START_ID, AG_END_ID));
	rsel->targetList = lappend(rsel->targetList,
							   makeSimpleResTarget("tableoid", NULL));
	rsel->targetList = lappend(rsel->targetList,
							   makeSimpleResTarget("ctid", NULL));

	u = makeNode(SelectStmt);
	u->op = SETOP_UNION;
	u->all = true;
	u->larg = lsel;
	u->rarg = rsel;

	sub = makeNode(RangeSubselect);
	sub->subquery = (Node *) u;

	return sub;
}

/*
 * SELECT (
 *     (
 *       SELECT array_agg(
 *           (
 *             SELECT (id, properties)::vertex
 *             FROM `get_graph_path()`.ag_vertex
 *             WHERE id = vid
 *           )
 *         )
 *       FROM unnest(vids) AS vid
 *     ),
 *     (
 *       SELECT array_agg(
 *           (
 *             SELECT (id, start, "end", properties)::edge
 *             FROM `get_graph_path()`.`typname`
 *             WHERE tableoid = rowid_tableoid(eid) AND ctid = rowid_ctid(eid)
 *           )
 *         )
 *       FROM unnest(eids) AS eid
 *     )
 *   )::graphpath AS `pathname`
 * FROM
 * (
 *   SELECT vids, eids
 *   FROM _sp
 *   WHERE vid = id(`lastVertex`) AND hops >= `lidx`
 *   LIMIT 1
 * ) AS _r
 */
static SelectStmt *
makeSelectWith(CypherPath *cpath, WithClause *with, bool isexpr)
{
	SelectStmt *sel;
	Node	   *vertices;
	Node	   *edges;
	Node	   *empty_edges;
	CoalesceExpr *coalesced;
	Node	   *path;
	char	   *pathname;

	sel = makeNode(SelectStmt);
	sel->withClause = with;

	vertices = makeVerticesSubLink();
	edges = makeEdgesSubLink(cpath, false);
	empty_edges = makeAArrayExpr(NIL, "_edge");
	coalesced = makeNode(CoalesceExpr);
	coalesced->args = list_make2(edges, empty_edges);
	coalesced->location = -1;
	path = makeRowExpr(list_make2(vertices, coalesced), "graphpath");
	if (cpath->kind == CPATH_SHORTEST_ALL && isexpr)
	{
		FuncCall *arragg;

		arragg = makeFuncCall(list_make1(makeString("array_agg")),
							  list_make1(path), -1);
		path = (Node *) arragg;
	}
	pathname = getCypherName(cpath->variable);
	sel->targetList = list_make1(makeResTarget(path, pathname));

	sel->fromClause = list_make1(makeSubselectCTE(cpath));

	return sel;
}

/*
 * SELECT vids, eids
 * FROM _sp
 * WHERE vid = id(`lastVertex`) AND hops >= `lidx`
 * LIMIT 1
 */
static RangeSubselect *
makeSubselectCTE(CypherPath *cpath)
{
	SelectStmt *sel;
	RangeVar   *sp;
	List	   *where_args;
	Node	   *vid;
	CypherNode *cnode;
	Node	   *lastVertex;
	Node	   *lastVid;
	CypherRel  *crel;
	A_Indices  *indices;
	Node	   *lidx = NULL;
	Node	   *hops;
	A_Expr	   *qual;
	RangeSubselect *subsel;

	sel = makeNode(SelectStmt);

	/* target list */
	sel->targetList =
			list_make2(makeResTarget(makeColumnRef1(SP_COLNAME_VIDS), NULL),
					   makeResTarget(makeColumnRef1(SP_COLNAME_EIDS), NULL));

	/* FROM */
	sp = makeRangeVar(NULL, SP_ALIAS_CTE, -1);
	sel->fromClause = list_make1(sp);

	/*
	 * WHERE
	 */

	vid = makeColumnRef1(SP_COLNAME_VID);
	cnode = llast(cpath->chain);
	lastVertex = makeColumnRef1(getCypherName(cnode->variable));
	lastVid = makeVertexIdExpr(lastVertex);
	where_args = list_make1(makeSimpleA_Expr(AEXPR_OP, "=", vid, lastVid, -1));

	crel = lsecond(cpath->chain);
	indices = (A_Indices *) crel->varlen;
	lidx = (indices == NULL ? (Node *) makeIntConst(1) : indices->lidx);
	hops = makeColumnRef1(SP_COLNAME_HOPS);
	qual = makeSimpleA_Expr(AEXPR_OP, ">=", hops, lidx, -1);
	where_args = lappend(where_args, qual);

	sel->whereClause = (Node *) makeBoolExpr(AND_EXPR, where_args, -1);

	/* LIMIT */
	if (cpath->kind == CPATH_SHORTEST)
		sel->limitCount = (Node *) makeIntConst(1);

	subsel = makeNode(RangeSubselect);
	subsel->subquery = (Node *) sel;
	subsel->alias = makeAliasNoDup("_r", NIL);

	return subsel;
}

/*
 * SELECT array_agg(
 *     (
 *       SELECT (id, properties)::vertex
 *       FROM `get_graph_path()`.ag_vertex
 *       WHERE id = vid
 *     )
 *   )
 * FROM unnest(vids) AS vid
 */
static Node *
makeVerticesSubLink(void)
{
	Node	   *id;
	SelectStmt *selsub;
	Node	   *vertex;
	RangeVar   *ag_vertex;
	A_Expr	   *qual;
	Node	   *vertices;
	SelectStmt *sel;
	FuncCall   *arragg;
	FuncCall   *unnest;
	RangeFunction *vid;

	/*
	 * SubLink for `array_agg()`
	 */

	id = makeColumnRef1(AG_ELEM_LOCAL_ID);

	selsub = makeNode(SelectStmt);

	vertex = makeRowExpr(list_make3(id,
									makeColumnRef1(AG_ELEM_PROP_MAP),
									makeColumnRef1("ctid")),
						 "vertex");
	selsub->targetList = list_make1(makeResTarget(vertex, NULL));

	ag_vertex = makeRangeVar(get_graph_path(true), AG_VERTEX, -1);
	ag_vertex->inh = true;
	selsub->fromClause = list_make1(ag_vertex);

	qual = makeSimpleA_Expr(AEXPR_OP, "=", id, makeColumnRef1("vid"), -1);
	selsub->whereClause = (Node *) makeBoolExpr(AND_EXPR, list_make1(qual), -1);

	vertices = makeSubLink(selsub);

	/*
	 * SELECT which gets array of vertices from `vids`
	 */

	sel = makeNode(SelectStmt);

	arragg = makeFuncCall(list_make1(makeString("array_agg")),
						  list_make1(vertices), -1);
	sel->targetList = list_make1(makeResTarget((Node *) arragg, NULL));

	unnest = makeFuncCall(list_make1(makeString("unnest")),
						  list_make1(makeColumnRef1(SP_COLNAME_VIDS)), -1);
	vid = makeNode(RangeFunction);
	vid->lateral = false;
	vid->ordinality = false;
	vid->is_rowsfrom = false;
	vid->functions = list_make1(list_make2(unnest, NIL));
	vid->alias = makeAliasNoDup("vid", NIL);
	vid->coldeflist = NIL;
	sel->fromClause = list_make1(vid);

	return makeSubLink(sel);
}

/*
 * SELECT array_agg(
 *     (
 *       SELECT (id, start, "end", properties)::edge
 *       FROM `get_graph_path()`.`typname`
 *
 *       # shortestpath()
 *       WHERE tableoid = rowid_tableoid(eid) AND ctid = rowid_ctid(eid)
 *
 *       # dijkstra()
 *       WHERE id = eid
 *     )
 *   )
 * FROM unnest(eids) AS eid
 */
static Node *
makeEdgesSubLink(CypherPath *cpath, bool is_dijkstra)
{
	Node	   *id;
	SelectStmt *selsub;
	Node	   *edge;
	CypherRel  *crel;
	char	   *typname;
	RangeVar   *e;
	A_Expr	   *qual;
	List	   *where_args = NIL;
	Node	   *edges;
	SelectStmt *sel;
	FuncCall   *arragg;
	FuncCall   *unnest;
	RangeFunction *eid;

	/*
	 * SubLink for `array_agg()`
	 */

	id = makeColumnRef1(AG_ELEM_LOCAL_ID);

	selsub = makeNode(SelectStmt);

	edge = makeRowExpr(list_make5(id,
								  makeColumnRef1(AG_START_ID),
								  makeColumnRef1(AG_END_ID),
								  makeColumnRef1(AG_ELEM_PROP_MAP),
								  makeColumnRef1("ctid")),
					   "edge");
	selsub->targetList = list_make1(makeResTarget(edge, NULL));

	crel = lsecond(cpath->chain);
	getCypherRelType(crel, &typname, NULL);
	e = makeRangeVar(get_graph_path(true), typname, -1);
	e->inh = true;
	selsub->fromClause = list_make1(e);

	if (is_dijkstra)
	{
		qual = makeSimpleA_Expr(AEXPR_OP, "=", copyObject(id),
								makeColumnRef1("eid"), -1);
		where_args = list_make1(qual);
	}
	else
	{
		Node	   *tableoid;
		Node	   *ctid;
		FuncCall   *getid;

		tableoid = makeColumnRef1("tableoid");
		getid = makeFuncCall(list_make1(makeString("rowid_tableoid")),
							 list_make1(makeColumnRef1("eid")), -1);
		qual = makeSimpleA_Expr(AEXPR_OP, "=", tableoid, (Node *) getid, -1);
		where_args = list_make1(qual);

		ctid = makeColumnRef1("ctid");
		getid = makeFuncCall(list_make1(makeString("rowid_ctid")),
							 list_make1(makeColumnRef1("eid")), -1);
		qual = makeSimpleA_Expr(AEXPR_OP, "=", ctid, (Node *) getid, -1);
		where_args = lappend(where_args, qual);
	}

	selsub->whereClause = (Node *) makeBoolExpr(AND_EXPR, where_args, -1);

	edges = makeSubLink(selsub);

	/*
	 * SELECT which gets array of edges from `eids`
	 */

	sel = makeNode(SelectStmt);

	arragg = makeFuncCall(list_make1(makeString("array_agg")),
						  list_make1(edges), -1);
	sel->targetList = list_make1(makeResTarget((Node *) arragg, NULL));

	unnest = makeFuncCall(list_make1(makeString("unnest")),
						  list_make1(makeColumnRef1(SP_COLNAME_EIDS)), -1);
	eid = makeNode(RangeFunction);
	eid->lateral = false;
	eid->ordinality = false;
	eid->is_rowsfrom = false;
	eid->functions = list_make1(list_make2(unnest, NIL));
	eid->alias = makeAliasNoDup("eid", NIL);
	eid->coldeflist = NIL;
	sel->fromClause = list_make1(eid);

	return makeSubLink(sel);
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
makeVertexIdExpr(Node *vertex)
{
	return (Node *) makeFuncCall(list_make1(makeString(AG_ELEM_ID)),
								 list_make1(vertex),
								 -1);
}

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
makeColumnRef1(char *colname)
{
	List *fields;

	fields = list_make1(makeString(pstrdup(colname)));

	return makeColumnRef(fields);
}

static Node *
makeColumnRef(List *fields)
{
	ColumnRef *cref;

	cref = makeNode(ColumnRef);
	cref->fields = fields;
	cref->location = -1;

	return (Node *) cref;
}

static ResTarget *
makeSimpleResTarget(char *field, char *name)
{
	Node *cref;

	cref = makeColumnRef(list_make1(makeString(pstrdup(field))));

	return makeResTarget(cref, name);
}

static ResTarget *
makeArrayAppendResTarget(Node *arr, Node *elem)
{
	FuncCall *append;

	append = makeFuncCall(list_make1(makeString("array_append")),
						  list_make2(arr, elem), -1);

	return makeResTarget((Node *) append, NULL);
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

static Node *
makeAArrayExpr(List *elements, char *typeName)
{
	A_ArrayExpr *arr;
	TypeCast   *cast;

	arr = makeNode(A_ArrayExpr);
	arr->elements = elements;
	arr->location = -1;

	cast = makeNode(TypeCast);
	cast->arg = (Node *) arr;
	cast->typeName = makeTypeName(typeName);
	cast->location = -1;

	return (Node *) cast;
}

static Node *
makeRowExpr(List *args, char *typeName)
{
	RowExpr	   *row;
	TypeCast   *cast;

	row = makeNode(RowExpr);
	row->args = args;
	row->row_typeid = InvalidOid;
	row->colnames = NIL;
	row->row_format = COERCE_EXPLICIT_CAST;
	row->location = -1;

	cast = makeNode(TypeCast);
	cast->arg = (Node *) row;
	cast->typeName = makeTypeName(typeName);
	cast->location = -1;

	return (Node *) cast;
}

static Node *
makeSubLink(SelectStmt *sel)
{
	SubLink *sublink;

	sublink = makeNode(SubLink);
	sublink->subLinkType = EXPR_SUBLINK;
	sublink->subLinkId = 0;
	sublink->testexpr = NULL;
	sublink->operName = NIL;
	sublink->subselect = (Node *) sel;
	sublink->location = -1;

	return (Node *) sublink;
}

Query *
transformDijkstra(ParseState *pstate, CypherPath *cpath)
{
	Assert(list_length(cpath->chain) == 3);

	checkNodeForRef(pstate, linitial(cpath->chain));
	checkRelFormatForDijkstra(pstate, lsecond(cpath->chain));
	checkNodeForRef(pstate, llast(cpath->chain));

	return makeDijkstraQuery(pstate, cpath, true);
}

Query *
transformDijkstraInMatch(ParseState *parentParseState, CypherPath *cpath)
{
	ParseState *pstate = make_parsestate(parentParseState);
	Query	   *qry;

	Assert(list_length(cpath->chain) == 3);

	checkNodeReferable(pstate, linitial(cpath->chain));
	checkRelFormatForDijkstra(pstate, lsecond(cpath->chain));
	checkNodeReferable(pstate, llast(cpath->chain));

	qry = makeDijkstraQuery(pstate, cpath, false);

	free_parsestate(pstate);

	return qry;
}

static void
checkRelFormatForDijkstra(ParseState *pstate, CypherRel *crel)
{
	if (crel->varlen != NULL)
	{
		A_Indices  *indices = (A_Indices *) crel->varlen;
		A_Const	   *lidx = (A_Const *) indices->lidx;

		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("variable length relationship is not supported"),
				 parser_errposition(pstate, lidx->location)));
	}

	if (crel->prop_map != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("property constraint is not supported")));
}

/*
 * path = DIJKSTRA((source)-[:edge_label]->(target), weight, qual, LIMIT n)
 *
 * |
 * v
 *
 * SELECT (
 *     (
 *       SELECT array_agg(
 *           (
 *             SELECT (id, properties)::vertex
 *             FROM `get_graph_path()`.ag_vertex
 *             WHERE id = vid
 *           )
 *         )
 *       FROM unnest(vids) AS vid
 *     ),
 *     (
 *       SELECT array_agg(
 *           (
 *             SELECT (id, start, "end", properties)::edge
 *             FROM `get_graph_path()`.`typname`
 *             WHERE id = eid
 *           )
 *         )
 *       FROM unnest(eids) AS eid
 *     )
 *   )::graphpath AS `pathname`,
 *   weight
 * FROM
 * (
 *   SELECT dijkstra_vids() as vids,
 *          dijkstra_eids() as eids,
 *          weight
 *   FROM `graph_path`.edge_label
 *   WHERE start = id(source) AND `qual`
 *
 *   DIJKSTRA (id(source), id(target), LIMIT n, "end", id)
 * )
 */
static Query *
makeDijkstraQuery(ParseState *pstate, CypherPath *cpath, bool is_expr)
{
	Query 	   *qry;
	RangeTblEntry *rte;
	Node	   *vertices;
	Node	   *edges;
	Node	   *empty_edges;
	CoalesceExpr *coalesced;
	Node	   *path;
	Node	   *expr;
	char	   *pathname;
	TargetEntry *te;

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	rte = makeDijkstraFrom(pstate, cpath);
	addRTEtoJoinlist(pstate, rte, true);

	vertices = makeVerticesSubLink();
	edges = makeEdgesSubLink(cpath, true);
	empty_edges = makeAArrayExpr(NIL, "_edge");
	coalesced = makeNode(CoalesceExpr);
	coalesced->args = list_make2(edges, empty_edges);
	coalesced->location = -1;
	path = makeRowExpr(list_make2(vertices, coalesced), "graphpath");
	if (is_expr)
	{
		FuncCall *arragg;

		arragg = makeFuncCall(list_make1(makeString("array_agg")),
							  list_make1(path), -1);
		path = (Node *) arragg;
	}
	expr = transformExpr(pstate, path, EXPR_KIND_SELECT_TARGET);
	pathname = getCypherName(cpath->variable);
	te = makeTargetEntry((Expr *) expr,
						 (AttrNumber) pstate->p_next_resno++,
						 pathname, false);
	qry->targetList = list_make1(te);

	if (cpath->weight_var)
	{
		char *weight_varname = getCypherName(cpath->weight_var);

		expr = transformExpr(pstate, makeColumnRef1("weight"),
							 EXPR_KIND_SELECT_TARGET);
		te = makeTargetEntry((Expr *) expr,
							 (AttrNumber) pstate->p_next_resno++,
							 weight_varname, false);
		qry->targetList = lappend(qry->targetList, te);
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

/*
 * SELECT dijkstra_vids() as vids,
 *        dijkstra_eids() as eids,
 *        weight
 * FROM `graph_path`.edge_label
 * WHERE start = id(source) AND `qual`
 *
 * DIJKSTRA (id(source), id(target), LIMIT n, "end", id)
 */
static RangeTblEntry *
makeDijkstraFrom(ParseState *parentParseState, CypherPath *cpath)
{
	Alias	   *alias;
	ParseState *pstate;
	Query	   *qry;
	RangeTblEntry *rte;
	Node	   *target;
	TargetEntry *te;
	FuncCall   *fc;
	CypherRel  *crel;
	Oid			wtype;
	Node  	   *start;
	CypherNode *vertex;
	Node	   *param;
	Node	   *vertex_id;
	List	   *where = NIL;
	Node	   *qual;

	Assert(parentParseState->p_expr_kind == EXPR_KIND_NONE);
	parentParseState->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

	alias = makeAlias("_d", NIL);

	pstate = make_parsestate(parentParseState);
	pstate->p_locked_from_parent = isLockedRefname(pstate, alias->aliasname);

	qry = makeNode(Query);
	qry->commandType = CMD_SELECT;

	rte = makeDijkstraEdgeQuery(pstate, cpath);
	addRTEtoJoinlist(pstate, rte, true);

	/* vids */
	fc = makeFuncCall(list_make1(makeString("dijkstra_vids")), NIL, -1);
	target = ParseFuncOrColumn(pstate, fc->funcname, NIL, pstate->p_last_srf,
							   fc, -1);
	te = makeTargetEntry((Expr *) target,
						 (AttrNumber) pstate->p_next_resno++,
						 "vids", false);
	qry->targetList = list_make1(te);

	/* eids */
	fc = makeFuncCall(list_make1(makeString("dijkstra_eids")), NIL, -1);
	target = ParseFuncOrColumn(pstate, fc->funcname, NIL, pstate->p_last_srf,
							   fc, -1);
	te = makeTargetEntry((Expr *) target,
						 (AttrNumber) pstate->p_next_resno++,
						 "eids", false);
	qry->targetList = lappend(qry->targetList, te);

	/* weight */
	target = transformCypherExpr(pstate, cpath->weight,
								 EXPR_KIND_SELECT_TARGET);
	wtype = exprType(target);
	if (wtype != FLOAT8OID)
	{
		Node	   *weight;

		weight = coerce_to_target_type(pstate, target, wtype, FLOAT8OID, -1,
									   COERCION_EXPLICIT, COERCE_EXPLICIT_CAST,
									   -1);
		if (weight == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("weight must be type %s, not type %s",
							format_type_be(FLOAT8OID),
							format_type_be(wtype)),
					 parser_errposition(pstate, exprLocation(target))));

		target = weight;
	}
	if (expression_returns_set(target))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("weight must not return a set"),
				 parser_errposition(pstate, exprLocation(target))));

	te = makeTargetEntry((Expr *) target,
						 (AttrNumber) pstate->p_next_resno++,
						 "weight", cpath->weight_var == NULL);
	qry->targetList = lappend(qry->targetList, te);

	qry->dijkstraWeight = pstate->p_next_resno;
	qry->dijkstraWeightOut = (cpath->weight_var != NULL);

	/* end ID */
	crel = lsecond(cpath->chain);
	if (crel->direction == CYPHER_REL_DIR_LEFT)
		target = transformExpr(pstate, makeColumnRef1("start"),
							   EXPR_KIND_SELECT_TARGET);
	else
		target = transformExpr(pstate, makeColumnRef1("end"),
							   EXPR_KIND_SELECT_TARGET);
	qry->dijkstraEndId = target;

	/* edge ID */
	target = transformExpr(pstate, makeColumnRef1("id"),
						   EXPR_KIND_SELECT_TARGET);
	qry->dijkstraEdgeId = target;

	markTargetListOrigins(pstate, qry->targetList);

	/* WHERE */
	if (crel->direction == CYPHER_REL_DIR_LEFT)
		start = makeColumnRef1("end");
	else
		start = makeColumnRef1("start");

	vertex = linitial(cpath->chain);
	param = makeColumnRef1(getCypherName(vertex->variable));
	vertex_id = makeVertexIdExpr(param);

	where = list_make1(makeSimpleA_Expr(AEXPR_OP, "=", start, vertex_id, -1));

	/* qual */
	if (cpath->qual != NULL)
		where = lappend(where, cpath->qual);

	qual = transformCypherExpr(pstate,
							   (Node *) makeBoolExpr(AND_EXPR, where, -1),
							   EXPR_KIND_WHERE);

	/* Dijkstra source */
	qry->dijkstraSource = transformExpr(pstate,
										(Node *) copyObject(vertex_id),
										EXPR_KIND_SELECT_TARGET);

	/* Dijkstra target */
	vertex = llast(cpath->chain);
	param = makeColumnRef1(getCypherName(vertex->variable));
	vertex_id = makeVertexIdExpr(param);
	qry->dijkstraTarget = transformExpr(pstate,
										vertex_id,
										EXPR_KIND_SELECT_TARGET);

	/* Dijkstra LIMIT */
	qry->dijkstraLimit = transformCypherLimit(pstate, cpath->limit,
											  EXPR_KIND_LIMIT, "LIMIT");

	qry->rtable = pstate->p_rtable;
	qry->jointree = makeFromExpr(pstate->p_joinlist, qual);

	qry->hasSubLinks = pstate->p_hasSubLinks;
	qry->hasAggs = pstate->p_hasAggs;
	if (qry->hasAggs)
		parseCheckAggregates(pstate, qry);

	assign_query_collations(pstate, qry);

	parentParseState->p_expr_kind = EXPR_KIND_NONE;

	return addRangeTableEntryForSubquery(parentParseState, qry, alias, false,
										 true);
}

static RangeTblEntry *
makeDijkstraEdgeQuery(ParseState *pstate, CypherPath *cpath)
{
	CypherRel 	   *crel;
	char 		   *elabel_name;
	char		   *row_name;
	Alias		   *alias;
	RangeTblEntry  *rte;
	Node		   *sub;
	Query		   *qry;

	Assert(pstate->p_expr_kind == EXPR_KIND_NONE);
	pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

	crel = lsecond(cpath->chain);
	getCypherRelType(crel, &elabel_name, NULL);
	row_name = getCypherName(crel->variable);

	if (crel->direction == CYPHER_REL_DIR_NONE)
		sub = makeDijkstraEdgeUnion(elabel_name, row_name);
	else
		sub = makeDijkstraEdge(elabel_name, row_name, crel);

	alias = makeAliasOptUnique(NULL);
	qry = parse_sub_analyze((Node *) sub, pstate, NULL,
							isLockedRefname(pstate, alias->aliasname), true);
	pstate->p_expr_kind = EXPR_KIND_NONE;

	rte = addRangeTableEntryForSubquery(pstate, qry, alias, false, true);

	return rte;
}

/*
 * SELECT start, "end", id, (id, _start, _end, properties)::edge AS row_name
 * FROM (
 *   SELECT start AS _start, "end" AS _end, start, "end", id, properties
 *   FROM `get_graph_path()`.`elabel_name`
 *   UNION
 *   SELECT start AS _start, "end" AS _end, end, start, id, properties
 *   FROM `get_graph_path()`.`elabel_name`
 * )
 */
static Node *
makeDijkstraEdgeUnion(char *elabel_name, char *row_name)
{
	RangeVar   *r;
	SelectStmt *lsel;
	Node	   *row;
	SelectStmt *rsel;
	SelectStmt *u;
	RangeSubselect *sub_sel;
	SelectStmt *sel;

	r = makeRangeVar(get_graph_path(true), elabel_name, -1);
	r->inh = true;

	lsel = makeNode(SelectStmt);
	lsel->targetList = lappend(lsel->targetList,
							   makeSimpleResTarget(AG_START_ID, "_start"));
	lsel->targetList = lappend(lsel->targetList,
							   makeSimpleResTarget(AG_END_ID, "_end"));
	lsel->targetList = lappend(lsel->targetList,
							   makeSimpleResTarget("ctid", NULL));
	lsel->fromClause = list_make1(r);

	rsel = copyObject(lsel);

	lsel->targetList = lappend(lsel->targetList,
							   makeSimpleResTarget(AG_START_ID, NULL));
	lsel->targetList = lappend(lsel->targetList,
							   makeSimpleResTarget(AG_END_ID, NULL));
	lsel->targetList = lappend(lsel->targetList,
							   makeSimpleResTarget(AG_ELEM_LOCAL_ID, NULL));
	if (row_name != NULL)
		lsel->targetList = lappend(lsel->targetList,
								   makeSimpleResTarget(AG_ELEM_PROP_MAP, NULL));

	rsel->targetList = lappend(rsel->targetList,
							   makeSimpleResTarget(AG_END_ID, NULL));
	rsel->targetList = lappend(rsel->targetList,
							   makeSimpleResTarget(AG_START_ID, NULL));
	rsel->targetList = lappend(rsel->targetList,
							   makeSimpleResTarget(AG_ELEM_LOCAL_ID, NULL));
	if (row_name != NULL)
		rsel->targetList = lappend(rsel->targetList,
								   makeSimpleResTarget(AG_ELEM_PROP_MAP, NULL));

	u = makeNode(SelectStmt);
	u->op = SETOP_UNION;
	u->all = true;
	u->larg = lsel;
	u->rarg = rsel;

	sub_sel = makeNode(RangeSubselect);
	sub_sel->subquery = (Node *) u;
	sub_sel->alias = makeAliasOptUnique(NULL);

	sel = makeNode(SelectStmt);
	sel->fromClause = list_make1(sub_sel);

	sel->targetList = list_make4(makeSimpleResTarget(AG_START_ID, NULL),
								 makeSimpleResTarget(AG_END_ID, NULL),
								 makeSimpleResTarget(AG_ELEM_LOCAL_ID, NULL),
								 makeSimpleResTarget("ctid", NULL));
	if (row_name != NULL)
	{
		row = makeRowExpr(list_make5(makeColumnRef1(AG_ELEM_LOCAL_ID),
									 makeColumnRef1("_start"),
									 makeColumnRef1("_end"),
									 makeColumnRef1(AG_ELEM_PROP_MAP),
									 makeColumnRef1("ctid")),
						  "edge");
		sel->targetList = lappend(sel->targetList,
								  makeResTarget(row, row_name));
	}

	return (Node *) sel;
}

/*
 * SELECT start, "end", id, (id, start, end, properties)::edge AS row_name
 * FROM `get_graph_path()`.`elabel_name`
 */
static Node *
makeDijkstraEdge(char *elabel_name, char *row_name, CypherRel *crel)
{
	SelectStmt *sel;
	RangeVar   *r;

	sel = makeNode(SelectStmt);

	r = makeRangeVar(get_graph_path(true), elabel_name, -1);
	r->inh = true;
	sel->fromClause = list_make1(r);

	sel->targetList = list_make4(makeSimpleResTarget(AG_START_ID, NULL),
								 makeSimpleResTarget(AG_END_ID, NULL),
								 makeSimpleResTarget(AG_ELEM_LOCAL_ID, NULL),
								 makeSimpleResTarget("ctid", NULL));
	if (row_name != NULL)
	{
		Node	   *row;

		row = makeRowExpr(list_make5(makeColumnRef1(AG_ELEM_LOCAL_ID),
									 makeColumnRef1(AG_START_ID),
									 makeColumnRef1(AG_END_ID),
									 makeColumnRef1(AG_ELEM_PROP_MAP),
									 makeColumnRef1("ctid")),
						  "edge");
		sel->targetList = lappend(sel->targetList,
								  makeResTarget(row, row_name));
	}

	return (Node *) sel;
}

/* TODO: Remove */

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

static Alias *
makeAliasOptUnique(char *aliasname)
{
	aliasname = (aliasname == NULL ? genUniqueName() : pstrdup(aliasname));
	return makeAliasNoDup(aliasname, NIL);
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
