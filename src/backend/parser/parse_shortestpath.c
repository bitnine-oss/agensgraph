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
#include "parser/analyze.h"
#include "parser/parse_relation.h"
#include "parser/parse_shortestpath.h"

#define SP_ALIAS_CTE		"_sp"

#define SP_COLNAME_VIDS		"vids"
#define SP_COLNAME_EIDS		"eids"
#define SP_COLNAME_HOPS		"hops"

/* semantic checks */
static void checkNodeForRef(ParseState *pstate, CypherNode *cnode);
static void checkNodeReferable(ParseState *pstate, CypherNode *cnode);
static void checkRelFormat(ParseState *pstate, CypherRel *rel);

/* shortest path */
static Query *makeShortestPathQuery(ParseState *pstate, CypherPath *cpath,
									bool isexpr);
static SelectStmt *makeNonRecursiveTerm(ParseState *pstate, CypherNode *cnode);
static SelectStmt *makeRecursiveTerm(ParseState *pstate, CypherPath *cpath);
static RangeSubselect *makeEdgeUnion(char *edge_label);
static SelectStmt *makeSelectWith(CypherPath *cpath, WithClause *with,
								  bool isexpr);
static RangeSubselect *makeSubselectCTE(CypherPath *cpath);
static Node *makeVerticesSubLink(void);
static Node *makeEdgesSubLink(CypherPath *cpath);
static void getCypherRelType(CypherRel *crel, char **typname, int *typloc);
static Node *makeVertexIdExpr(Node *vertex);
static Node *makeLastVidRefExpr(void);

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
				 errmsg("labels on nodes in shortest path are not supported"),
				 parser_errposition(pstate, getCypherNameLoc(cnode->label))));

	if (cnode->prop_map != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("property constraint on nodes in shortest path are not supported"),
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
				 errmsg("nodes in shortest path must be a reference to a specific node")));

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
}

static void
checkRelFormat(ParseState *pstate, CypherRel *crel)
{
	if (crel->variable != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("variable on relationship is not supported"),
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
				 errmsg("property constraint on relationship in shortest path are not supported")));
}

/*
 * WITH _sp(vids, eids, hops) AS (
 *   VALUES (ARRAY[id(`initialVertex`)]::graphid[], ARRAY[]::graphid[], 0)
 *   UNION ALL
 *   SELECT DISTINCT ON ("end")
 *          array_append(vids, "end"), array_append(eids, id), hops + 1
 *   FROM _sp, `get_graph_path()`.`typname` AS _e(id, start, "end", properties)
 *   WHERE vids[array_upper(vids, 1)] = start AND
 *         array_position(vids, "end") IS NULL
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
 *             SELECT (id, start, "end", properties)::vertex
 *             FROM `get_graph_path()`.`typname`
 *             WHERE id = eid
 *           )
 *         )
 *       FROM unnest(eids) AS eid
 *     )
 *   )::graphpath AS `pathname`
 * FROM
 * (
 *   SELECT vids, eids
 *   FROM _sp
 *   WHERE vids[array_upper(vids, 1)] = id(`lastVertex`) AND
 *         hops >= `lidx`
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
	u->all = true;
	u->larg = makeNonRecursiveTerm(pstate, linitial(cpath->chain));
	u->rarg = makeRecursiveTerm(pstate, cpath);

	cte = makeNode(CommonTableExpr);
	cte->ctename = SP_ALIAS_CTE;
	cte->aliascolnames = list_make3(makeString(SP_COLNAME_VIDS),
									makeString(SP_COLNAME_EIDS),
									makeString(SP_COLNAME_HOPS));
	cte->ctequery = (Node *) u;
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

/* VALUES (ARRAY[id(`initialVertex`)]::graphid[], ARRAY[]::graphid[], 0) */
static SelectStmt *
makeNonRecursiveTerm(ParseState *pstate, CypherNode *cnode)
{
	Node	   *initialVertex;
	Node	   *col;
	List	   *values;
	SelectStmt *sel;

	initialVertex = makeColumnRef1(getCypherName(cnode->variable));
	col = makeAArrayExpr(list_make1(makeVertexIdExpr(initialVertex)),
						 "_graphid");
	values = list_make1(col);

	col = makeAArrayExpr(NIL, "_graphid");
	values = lappend(values, col);

	values = lappend(values, makeIntConst(0));

	sel = makeNode(SelectStmt);
	sel->valuesLists = list_make1(values);

	return sel;
}

/*
 * SELECT DISTINCT ON ("end")
 *        array_append(vids, "end"), array_append(eids, id), hops + 1
 * FROM _sp, `get_graph_path()`.`typname` AS _e(id, start, "end", properties)
 * WHERE vids[array_upper(vids, 1)] = start AND
 *       array_position(vids, "end") IS NULL
 */
static SelectStmt *
makeRecursiveTerm(ParseState *pstate, CypherPath *cpath)
{
	Node	   *start;
	Node	   *end;
	SelectStmt *sel;
	Node	   *vids;
	Node	   *eids;
	Node	   *eid;
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

	/* DISTINCT */
	if (cpath->kind == CPATH_SHORTEST)
		sel->distinctClause = list_make1(end);

	/* vids */
	vids = makeColumnRef1(SP_COLNAME_VIDS);
	sel->targetList = list_make1(makeArrayAppendResTarget(vids, end));

	/* eids */
	eids = makeColumnRef1(SP_COLNAME_EIDS);
	eid = makeColumnRef1(AG_ELEM_LOCAL_ID);
	sel->targetList = lappend(sel->targetList,
							  makeArrayAppendResTarget(eids, eid));

	/* hops */
	hops = (Node *) makeSimpleA_Expr(AEXPR_OP, "+",
									 makeColumnRef1(SP_COLNAME_HOPS),
									 (Node *) makeIntConst(1), -1);
	sel->targetList = lappend(sel->targetList, makeResTarget(hops, NULL));

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

		r = makeRangeVar(get_graph_path(), typname, -1);
		r->alias = makeAliasNoDup("_e", NIL);
		if (crel->direction == CYPHER_REL_DIR_LEFT)
		{
			/* swap start with end to preserve the direction */
			r->alias->colnames = list_make4(makeString(AG_ELEM_LOCAL_ID),
											makeString(AG_END_ID),
											makeString(AG_START_ID),
											makeString(AG_ELEM_PROP_MAP));
		}
		r->inhOpt = INH_YES;
		e = (Node *) r;
	}
	sel->fromClause = list_make2(sp, e);

	/*
	 * WHERE
	 */

	/* _sp JOIN _e */
	last_vid = makeLastVidRefExpr();
	joincond = makeSimpleA_Expr(AEXPR_OP, "=", last_vid, start, -1);
	where_args = list_make1(joincond);

	/* vertex uniqueness */
	arrpos = makeFuncCall(list_make1(makeString("array_position")),
						  list_make2(vids, end), -1);
	dupcond = makeNode(NullTest);
	dupcond->arg = (Expr *) arrpos;
	dupcond->nulltesttype = IS_NULL;
	dupcond->location = -1;
	where_args = lappend(where_args, dupcond);

	sel->whereClause = (Node *) makeBoolExpr(AND_EXPR, where_args, -1);

	return sel;
}

/*
 * SELECT id, start, "end"
 * FROM `get_graph_path()`.`edge_label`
 * UNION
 * SELECT id, "end" AS start, start AS "end"
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

	r = makeRangeVar(get_graph_path(), edge_label, -1);
	r->inhOpt = INH_YES;

	lsel = makeNode(SelectStmt);
	lsel->targetList = list_make1(id);
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
 *             SELECT (id, start, "end", properties)::vertex
 *             FROM `get_graph_path()`.`typname`
 *             WHERE id = eid
 *           )
 *         )
 *       FROM unnest(eids) AS eid
 *     )
 *   )::graphpath AS `pathname`
 * FROM
 * (
 *   SELECT vids, eids
 *   FROM _sp
 *   WHERE vids[array_upper(vids, 1)] = id(`lastVertex`) AND
 *         hops >= `lidx`
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
	edges = makeEdgesSubLink(cpath);
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
 * WHERE vids[array_upper(vids, 1)] = id(`lastVertex`) AND
 *       hops >= `lidx`
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

	vid = makeLastVidRefExpr();
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

	vertex = makeRowExpr(list_make2(id, makeColumnRef1(AG_ELEM_PROP_MAP)),
						 "vertex");
	selsub->targetList = list_make1(makeResTarget(vertex, NULL));

	ag_vertex = makeRangeVar(get_graph_path(), AG_VERTEX, -1);
	ag_vertex->inhOpt = INH_YES;
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
 *       SELECT (id, start, "end", properties)::vertex
 *       FROM `get_graph_path()`.`typname`
 *       WHERE id = eid
 *     )
 *   )
 * FROM unnest(eids) AS eid
 */
static Node *
makeEdgesSubLink(CypherPath *cpath)
{
	Node	   *id;
	SelectStmt *selsub;
	Node	   *edge;
	CypherRel  *crel;
	char	   *typname;
	RangeVar   *e;
	A_Expr	   *qual;
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

	edge = makeRowExpr(list_make4(id,
								  makeColumnRef1(AG_START_ID),
								  makeColumnRef1(AG_END_ID),
								  makeColumnRef1(AG_ELEM_PROP_MAP)),
					   "edge");
	selsub->targetList = list_make1(makeResTarget(edge, NULL));

	crel = lsecond(cpath->chain);
	getCypherRelType(crel, &typname, NULL);
	e = makeRangeVar(get_graph_path(), typname, -1);
	e->inhOpt = INH_YES;
	selsub->fromClause = list_make1(e);

	qual = makeSimpleA_Expr(AEXPR_OP, "=", id, makeColumnRef1("eid"), -1);
	selsub->whereClause = (Node *) makeBoolExpr(AND_EXPR, list_make1(qual), -1);

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

/* vids[array_upper(vids, 1)] */
static Node *
makeLastVidRefExpr(void)
{
	Node	   *vids;
	FuncCall   *arrup;
	A_Indices  *ind;
	A_Indirection *ref;

	vids = makeColumnRef1(SP_COLNAME_VIDS);

	arrup = makeFuncCall(list_make1(makeString("array_upper")),
						 list_make2(vids, makeIntConst(1)),
						 -1);

	ind = makeNode(A_Indices);
	ind->is_slice = false;
	ind->lidx = NULL;
	ind->uidx = (Node *) arrup;

	ref = makeNode(A_Indirection);
	ref->arg = vids;
	ref->indirection = list_make1(ind);

	return (Node *) ref;
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
