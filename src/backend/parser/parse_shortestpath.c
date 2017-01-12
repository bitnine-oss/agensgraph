/*
 * parse_shortestpath.c
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
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
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
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

static void checkNode(ParseState *pstate, CypherNode *cnode);
static void checkNodeForRef(ParseState *pstate, CypherNode *cnode);
static void checkNoPropRel(ParseState *pstate, CypherRel *rel);

static Query *makeShortestPathQuery(ParseState *pstate, CypherPath *cpath,
									bool isexpr);
static SelectStmt *makeNonRecursiveTerm(ParseState *pstate, CypherNode *cnode);
static SelectStmt *makeRecursiveTerm(ParseState *pstate, CypherPath *cpath);
static SelectStmt *makeSubquery(CypherPath *cpath, WithClause *with,
								bool isexpr);
static RangeSubselect *makeCTEScan(CypherPath *cpath);
static Node *makeVertexScan(CypherPath *cpath);
static Node *makeEdgeScan(CypherPath *cpath);
static RangeSubselect *makeEdgeUnion(char *edge_label);

static Node *makeColumnRef(List *fields);
static Node *makeColumnRef1(char *colname);
static Node *makeColumnRef2(char *objname, char *colname);
static Node *makeAArrayExpr(List *elements, char *typeName);
static Node *makeArrayAppendResTarget(Node *arr, Node *elem);
static Node *makeLastElem(void);
static Node *makeRowExpr(List *args, char *typeName);
static Node *makeVertexId(Node *vertexExpr);
static Node *makeResTarget(Node *val, char *name);
static Node *makeSimpleResTarget(char *field, char *name);
static A_Const *makeIntConst(int val);
static void getCypherRelType(CypherRel *crel, char **typname, int *typloc);
static Alias *makeAliasNoDup(char *aliasname, List *colnames);
static Node *makeSubLink(Node *sel);

Query *
transformShortestPath(ParseState *pstate, CypherPath *cpath)
{
	Assert(list_length(cpath->chain) == 3);
	checkNodeForRef(pstate, (CypherNode *)linitial(cpath->chain));
	checkNoPropRel(pstate, (CypherRel *)lsecond(cpath->chain));
	checkNodeForRef(pstate, (CypherNode *)lthird(cpath->chain));
	return makeShortestPathQuery(pstate, cpath, true);
}

Query *
transformShortestPathInMatch(ParseState *parentParseState, CypherPath *cpath)
{
	ParseState *pstate = make_parsestate(parentParseState);
	Query	   *query;

	pstate->p_parent_cte = NULL;
	pstate->p_locked_from_parent = false;

	Assert(list_length(cpath->chain) == 3);
	checkNode(pstate, (CypherNode *)linitial(cpath->chain));
	checkNoPropRel(pstate, (CypherRel *)lsecond(cpath->chain));
	checkNode(pstate, (CypherNode *)lthird(cpath->chain));
	query = makeShortestPathQuery(pstate, cpath, false);

	free_parsestate(pstate);

	return query;
}

static void
checkNode(ParseState *pstate, CypherNode *cnode)
{
	char *name = getCypherName(cnode->variable);
	int loc = getCypherNameLoc(cnode->variable);
	Node *col;

	if (name == NULL)
	{
		/* Location */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("a variable name must be provided")));
	}

	col = colNameToVar(pstate, name, false, loc);
	if (col == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("the variable \"%s\" not defined", name),
				 parser_errposition(pstate, loc)));
	}
}

static void
checkNodeForRef(ParseState *pstate, CypherNode *cnode)
{
	int loc = getCypherNameLoc(cnode->variable);

	checkNode (pstate, cnode);

	if (getCypherName(cnode->label) != NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("a label not supported"),
				 parser_errposition(pstate, getCypherNameLoc(cnode->label))));
	}

	if (cnode->prop_map != NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("a prop_map not supported"),
				 parser_errposition(pstate, loc)));
	}
}

static void
checkNoPropRel(ParseState *pstate, CypherRel *rel)
{
	if (rel->prop_map != NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("prop_map not supported")));
	}

	if (rel->variable != NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("label variable not supported")));
	}
}

#define CTE_COLNAME_VARR 	"varr"
#define CTE_COLNAME_EARR 	"earr"
#define CTE_COLNAME_VIDARR 	"vidarr"
#define CTE_COLNAME_EIDARR 	"eidarr"
#define CTE_COLNAME_HOPS 	"hops"
#define CTE_COLNAME_VID 	"vid"
#define CTE_NAME 			"_sp"
#define CTE_ENAME			"_e"
#define CTE_VNAME			"_v"

/*
 * WITH _sp(vidarr, eidarr, hops) AS (
 *   VALUES (
 *     ARRAY[startVertexExprId]::graphid[],
 *     ARRAY[]::graphid[],
 *     0)
 *   UNION ALL
 *   SELECT DISTINCT ON (_e."end")
 *     array_append(vidarr, _e."end"),
 *     array_append(eidarr, _e.id),
 *     hops + 1
 *   FROM _sp, get_graph_path().typename AS _e,
 *   WHERE _sp.vidarr[array_length(_sp.vidarr, 1)] = _e.start
 *     AND array_position(_sp.vidarr, _e."end") IS NULL
 * )
 * SELECT ROW((SELECT array_agg(SELECT ROW(_v.id, _v.properties)::vertex
 *                     FROM get_graph_path().'ag_vertex' AS _v
 *        		       WHERE vid = _v.id) AS varr
 *             FROM unnest(_sp.vidarr) AS vid),
 *            (SELECT array_agg(SELECT ROW(_e.id, _e.start, _e."end", _e.properties)::edge
 * 	                   FROM egde AS _e
 *                     WHERE eid = _e.id) AS earr
 *             FROM unnest(_sp.eidarr) AS eid))::graphpath
 * FROM _sp
 * WHERE _sp.vidarr[array_length(_sp.vidarr, 1)]) = id(endVertexExpr)
 *   AND _sp.hops >= minHops
 * LIMIT 1
 */
static Query *
makeShortestPathQuery(ParseState *pstate, CypherPath *cpath, bool isexpr)
{
	SelectStmt 		*u;
	List	   		*colnames;
	CommonTableExpr *cte;
	CypherRel 		*crel;
	A_Indices  		*varlen;
	WithClause 		*with;
	SelectStmt 		*sp;

	u = makeNode(SelectStmt);
	u->op = SETOP_UNION;
	u->all = true;
	u->larg = makeNonRecursiveTerm(pstate, (CypherNode *)linitial(cpath->chain));
	u->rarg = makeRecursiveTerm(pstate, cpath);

	colnames = list_make3(makeString(CTE_COLNAME_VIDARR),
						  makeString(CTE_COLNAME_EIDARR),
						  makeString(CTE_COLNAME_HOPS));

	cte = makeNode(CommonTableExpr);
	cte->ctename = CTE_NAME;
	cte->aliascolnames = colnames;
	cte->ctequery = (Node *) u;
	cte->spstop = true;
	cte->location = -1;

	crel = (CypherRel *)lsecond(cpath->chain);
	varlen = (A_Indices *) crel->varlen;
	if (varlen != NULL && varlen->uidx != NULL)
	{
		A_Const	*uidx;
		uidx = (A_Const *) varlen->uidx;
		cte->maxdepth = uidx->val.val.ival + 1;
	}

	with = makeNode(WithClause);
	with->ctes = list_make1(cte);
	with->recursive = true;
	with->location = -1;

	sp = makeSubquery(cpath, with, isexpr);

	return transformStmt(pstate, (Node *) sp);
}


/*
 *   VALUES (
 *     ARRAY[startVertexExprId]::graphid[],
 *     ARRAY[]::edge[],
 *     0)
 */
static SelectStmt *
makeNonRecursiveTerm(ParseState *pstate, CypherNode *cnode)
{
	Node 		*sid;
	Node	    *col;
	List	   	*values;
	SelectStmt	*sel;

	sid = makeColumnRef1(getCypherName(cnode->variable));
	col = makeAArrayExpr(list_make1(makeVertexId(sid)), "_graphid");
	values = list_make1(col);

	col = makeAArrayExpr(NIL, "_graphid");
	values = lappend(values, col);

	values = lappend(values, makeIntConst(0));

	sel = makeNode(SelectStmt);
	sel->valuesLists = list_make1(values);

	return sel;
}

static Node *
makeAArrayExpr(List *elements, char *typeName)
{
	TypeCast    *cast;
	A_ArrayExpr *arr;

	arr = makeNode(A_ArrayExpr);
	arr->elements = elements;
	arr->location = -1;

	cast = makeNode(TypeCast);
	cast->arg = (Node *) arr;
	cast->typeName = makeTypeName(typeName);
	cast->location = -1;

	return (Node *) cast;
}

/*
 * SELECT DISTINCT ON (_e."end")
 *   array_append(vidarr, _e."end"),
 *   array_append(eidarr, _e.id),
 *   hops + 1
 * FROM _sp,
 * 	 get_graph_path().typename AS _e,
 * WHERE _sp.vidarr[array_length(_sp.vidarr, 1)] = _e.start
 *   AND array_position(_sp.vidarr, _e."end") IS NULL
 */
static SelectStmt *
makeRecursiveTerm(ParseState *pstate, CypherPath *cpath)
{
	SelectStmt  *sel;
	Node		*eidarr;
	Node		*eidexpr;
	Node		*vidarr;
	Node		*videxpr;
	A_Expr		*incrHops;
	Node	 	*hops;
	RangeVar   	*sp;
	Node       	*edge;
	Node	  	*prev;
	Node  		*next;
	A_Expr	   	*joincond;
	List	   	*where_args;
	FuncCall   	*arrpos;
	NullTest   	*dupcond;
	CypherRel 	*crel;
	char	   	*typname;

	sel = makeNode(SelectStmt);


	if (cpath->spkind == CPATHSP_ONE)
		sel->distinctClause = list_make1(makeColumnRef2(CTE_ENAME, AG_END_ID));

	/* targetList */

	vidarr = makeColumnRef1(CTE_COLNAME_VIDARR);
	videxpr = makeColumnRef2(CTE_ENAME, AG_END_ID);
	sel->targetList = list_make1(makeArrayAppendResTarget(vidarr, videxpr));

	eidarr = makeColumnRef1(CTE_COLNAME_EIDARR);
	eidexpr = makeColumnRef2(CTE_ENAME, AG_ELEM_ID);
	sel->targetList = lappend(sel->targetList,
			makeArrayAppendResTarget(eidarr, eidexpr));

	incrHops = makeSimpleA_Expr(AEXPR_OP, "+",
			(Node *) makeColumnRef1(CTE_COLNAME_HOPS),
			(Node *) makeIntConst(1), -1);
	hops = makeResTarget((Node *) incrHops, NULL);
	sel->targetList = lappend(sel->targetList, hops);

	/* FROM */

	crel = (CypherRel *)lsecond(cpath->chain);

	sp = makeRangeVar(NULL, CTE_NAME, -1);
	getCypherRelType(crel, &typname, NULL);
	if (crel->direction == CYPHER_REL_DIR_NONE)
	{
		RangeSubselect 	*sub;

		sub = makeEdgeUnion(typname);
		sub->alias = makeAliasNoDup(CTE_ENAME, NIL);
		edge = (Node *) sub;
	}
	else
	{
		RangeVar *re;

		re = makeRangeVar(get_graph_path(), typname, -1);
		re->alias = makeAliasNoDup(CTE_ENAME, NIL);
		if (crel->direction == CYPHER_REL_DIR_LEFT)
		{
			re->alias->colnames = list_make4(
					makeString("id"), makeString("end"),
					makeString("start"), makeString("properties"));
		}
		re->inhOpt = INH_YES;
		edge = (Node *) re;
	}

	sel->fromClause = list_make2(sp, edge);

	/* WHERE */

	/* _vp JOIN _e */

	prev = makeLastElem();
	next = makeColumnRef2(CTE_ENAME, AG_START_ID);
	joincond = makeSimpleA_Expr(AEXPR_OP, "=", prev, next, -1);
	where_args = list_make1(joincond);

	/* dup checking */

	vidarr = copyObject(vidarr);
	videxpr = copyObject(videxpr);
	arrpos = makeFuncCall(list_make1(makeString("array_position")),
						  list_make2(vidarr, videxpr), -1);
	dupcond = makeNode(NullTest);
	dupcond->arg = (Expr *) arrpos;
	dupcond->nulltesttype = IS_NULL;
	dupcond->location = -1;
	where_args = lappend(where_args, dupcond);
	sel->whereClause = (Node *) makeBoolExpr(AND_EXPR, where_args, -1);

	return sel;
}

static Node *
makeArrayAppendResTarget(Node *arr, Node *elem)
{
	FuncCall *append;
	append = makeFuncCall(list_make1(makeString("array_append")),
						  list_make2(arr, elem), -1);
	return makeResTarget((Node *) append, NULL);
}

static Node *
makeRowExpr(List *args, char *typeName)
{
    RowExpr 	*row;
	TypeCast    *cast;

	row = makeNode(RowExpr);
	row->args = args;
	row->row_typeid = InvalidOid;
	row->colnames = NIL;
	row->row_format = COERCE_EXPLICIT_CALL;
	row->location = -1;

	cast = makeNode(TypeCast);
	cast->arg = (Node *) row;
	cast->typeName = makeTypeName(typeName);
	cast->location = -1;

	return (Node *) cast;
}

/*
 * SELECT id, start, "end"
 * FROM `get_graph_path()`.`edge_label`
 * UNION
 * SELECT id, "end" as start, start as "end"
 * FROM `get_graph_path()`.`edge_label`
 */
static RangeSubselect *
makeEdgeUnion(char *edge_label)
{
	Node	   *id;
	RangeVar   *r;
	SelectStmt *lsel;
	SelectStmt *rsel;
	SelectStmt *u;
	RangeSubselect *sub;

	lsel = makeNode(SelectStmt);
	id = makeSimpleResTarget(AG_ELEM_ID, NULL);
	lsel->targetList = list_make1(id);
	r = makeRangeVar(get_graph_path(), edge_label, -1);
	r->inhOpt = INH_YES;
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
 * vidarr[array_length(vidarr, 1)]
 */
static Node *
makeLastElem(void)
{
	Node 			*varr1;
	Node 			*varr2;
	FuncCall 		*alen;
	A_Indices 		*ind;
	A_Indirection 	*i;

	varr1 = makeColumnRef1(CTE_COLNAME_VIDARR);
	varr2 = (Node *) copyObject(varr1);

	alen = makeFuncCall(list_make1(makeString("array_length")),
			list_make2(varr2, makeIntConst(1)), -1);

	ind = makeNode(A_Indices);
	ind->is_slice = false;
	ind->lidx = NULL;
	ind->uidx = (Node *) alen;

	i = makeNode(A_Indirection);
	i->arg = varr1;
	i->indirection = list_make1(ind);

	return (Node *) i;
}

/*
 * SELECT ROW((SELECT (SELECT array_agg(ROW(_v.id, _v.properties)::vertex)
 *                     FROM get_graph_path().'ag_vertex' AS _v
 *        		       WHERE vid = _v.id) AS varr
 *             FROM unnest(_sp.vidarr) AS vid),
 *            (SELECT (SELECT array_agg(ROW(_e.id, _e.start, _e."end", _e.properties)::edge) AS earr
 * 	                   FROM unnest(_sp.eidarr) eid, egde AS _e
 *                     WHERE eid = _e.id) AS earr
 *             FROM unnest(_sp.eidarr) AS eid))::graphpath
 * FROM _sp
 * WHERE _sp.vidarr[array_length(_sp.vidarr, 1)]) = id(endVertexExpr)
 *   AND _sp.hops >= minHops
 * LIMIT 1;
 */
static SelectStmt *
makeSubquery(CypherPath *cpath, WithClause *with, bool isexpr)
{
	SelectStmt 		*sel;
	RangeSubselect 	*ctescan;
	Node		 	*varr;
	Node		 	*earr;
	Node			*gpath;
	char			*gpathname;

	ctescan = makeCTEScan(cpath);
	varr = makeVertexScan(cpath);
	earr = makeEdgeScan(cpath);

	sel = makeNode(SelectStmt);
	sel->withClause = with;
	sel->fromClause = list_make1(ctescan);
	gpath = makeRowExpr(list_make2(varr, earr), "graphpath");
	gpathname = getCypherName(cpath->variable);
	if (cpath->spkind == CPATHSP_ALL && isexpr)
	{
		FuncCall *ag;
		ag = makeFuncCall(list_make1(makeString("array_agg")),
						  list_make1(gpath), -1);
		gpath = (Node *)ag;
	}
	sel->targetList = list_make1(makeResTarget(gpath, gpathname));

	return sel;
}

static RangeSubselect *
makeCTEScan(CypherPath *cpath)
{
	SelectStmt 		*sel;
	RangeVar   		*sp;
	Node  			*vid;
	Node	   		*endVid;
	A_Indices  		*indices;
	A_Expr			*cond;
	List			*where_args;
	CypherRel  		*crel;
	CypherNode 		*endNode;
	int				 minHops = 1;
	RangeSubselect 	*ctescan;

	sel = makeNode(SelectStmt);

	sel->targetList = list_make2(
		makeResTarget(makeColumnRef2(CTE_NAME, CTE_COLNAME_VIDARR), NULL),
		makeResTarget(makeColumnRef2(CTE_NAME, CTE_COLNAME_EIDARR), NULL));

	sp = makeRangeVar(NULL, CTE_NAME, -1);
	sel->fromClause = list_make1(sp);

	/* WHERE */

	vid = makeLastElem();
	endNode = (CypherNode *)lthird(cpath->chain);
	endVid = makeVertexId(makeColumnRef1(getCypherName(endNode->variable)));
	cond = makeSimpleA_Expr(AEXPR_OP, "=", vid, (Node *) endVid, -1);
	where_args = list_make1(cond);
	crel = (CypherRel *) lsecond(cpath->chain);
	indices = (A_Indices *) crel->varlen;
	if (indices != NULL)
	{
		if (indices->lidx == NULL)
		{
			if (indices->uidx != NULL)
			{
				A_Const *uidx = (A_Const *) indices->uidx;
				if (uidx->val.val.ival == 0)
					minHops = 0;
			}
		}
		else
		{
			A_Const *lidx = (A_Const *) indices->lidx;
			minHops = lidx->val.val.ival;
			if (minHops > 1)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("minHops are only allowed 0 or 1")));
		}
	}

	if (minHops == 1)
	{
		ColumnRef *hops;
		Node *minHopsCond;

		hops = makeNode(ColumnRef);
		hops->fields = list_make1(makeString(CTE_COLNAME_HOPS));
		hops->location = -1;

		minHopsCond = (Node *) makeSimpleA_Expr(
				AEXPR_OP, "<>", (Node *) hops, (Node *) makeIntConst(0), -1);
		where_args = lappend(where_args, minHopsCond);
	}
	sel->whereClause = (Node *) makeBoolExpr(AND_EXPR, where_args, -1);

	/* LIMIT */

	if (cpath->spkind == CPATHSP_ONE)
	{
		sel->limitCount = (Node *) makeIntConst(1);
	}

	ctescan = makeNode(RangeSubselect);
	ctescan->subquery = (Node *) sel;
	ctescan->alias = makeAliasNoDup(CTE_NAME, NIL);

	return ctescan;
}

/*
 * SELECT (SELECT array_agg(ROW(_v.id, _v.properties)::vertex)
 *         FROM get_graph_path().'ag_vertex' AS _v
 * 		   WHERE vid = _v.id) AS varr
 * FROM unnest(_sp.vidarr) AS vid
 */
static Node *
makeVertexScan(CypherPath *cpath)
{
	SelectStmt 		*sel;
	Node 			*row;
	FuncCall   		*aa;
	Node   			*col;
	FuncCall   		*unnest;
	RangeFunction 	*rf;
	RangeVar   		*v;
	Node  			*vid;
	Node  			*vvid;
	A_Expr			*cond;
	List			*where_args;
	Node			*varr;

	sel = makeNode(SelectStmt);

	row = makeRowExpr(list_make2(
				makeColumnRef2(CTE_VNAME, AG_ELEM_ID),
				makeColumnRef2(CTE_VNAME, AG_ELEM_PROP_MAP)),
			"vertex");

	col = makeResTarget(row, CTE_COLNAME_VARR);

	sel->targetList = list_make1(col);

	v = makeRangeVar(get_graph_path(), AG_VERTEX, -1);
	v->alias = makeAliasNoDup(CTE_VNAME, NIL);
	v->inhOpt = INH_YES;

	sel->fromClause = list_make1(v);

	vid = makeColumnRef1("vid");
	vvid = makeColumnRef2(CTE_VNAME, AG_ELEM_ID);
	cond = makeSimpleA_Expr(AEXPR_OP, "=", vid, vvid, -1);
	where_args = list_make1(cond);

	sel->whereClause = (Node *) makeBoolExpr(AND_EXPR, where_args, -1);

	varr = makeSubLink((Node *) sel);

	sel = makeNode(SelectStmt);
	aa = makeFuncCall(list_make1(makeString("array_agg")), list_make1(varr), -1);
	sel->targetList = list_make1(makeResTarget((Node *) aa, CTE_COLNAME_VARR));

	unnest = makeFuncCall(
			list_make1(makeString("unnest")),
			list_make1(makeColumnRef2(CTE_NAME, CTE_COLNAME_VIDARR)), -1);

	rf = makeNode(RangeFunction);
	rf->lateral = false;
	rf->ordinality = false;
	rf->is_rowsfrom = false;
	rf->functions = list_make1(list_make2(unnest, NIL));
	rf->alias = makeAliasNoDup("vid", NIL);
	rf->coldeflist = NIL;

	sel->fromClause = list_make1(rf);

	return makeSubLink((Node *)sel);
}

/*
 * SELECT (SELECT array_agg(ROW(_e.id, _e.start, _e."end", _e.properties)::edge) AS earr
 * 	       FROM unnest(_sp.eidarr) eid, egde AS _e
 *         WHERE eid = _e.id) AS earr
 * FROM unnest(_sp.eidarr) AS eid
 */
static Node *
makeEdgeScan(CypherPath *cpath)
{
	SelectStmt 		*sel;
	Node 			*row;
	FuncCall   		*aa;
	Node   			*col;
	FuncCall   		*unnest;
	RangeFunction 	*rf;
	CypherRel		*crel;
	char	   		*typname;
	RangeVar   		*e;
	Node  			*col1;
	Node  			*col2;
	A_Expr			*cond;
	List			*where_args;
	Node 			*earr;

	sel = makeNode(SelectStmt);

	row = makeRowExpr(list_make4(
				makeColumnRef2(CTE_ENAME, AG_ELEM_ID),
				makeColumnRef2(CTE_ENAME, AG_START_ID),
				makeColumnRef2(CTE_ENAME, AG_END_ID),
				makeColumnRef2(CTE_ENAME, AG_ELEM_PROP_MAP)),
			"edge");

	col = makeResTarget(row, CTE_COLNAME_EARR);

	sel->targetList = list_make1(col);

	crel = (CypherRel *)lsecond(cpath->chain);
	getCypherRelType(crel, &typname, NULL);
	e = makeRangeVar(get_graph_path(), typname, -1);
	e->alias = makeAliasNoDup(CTE_ENAME, NIL);
	e->inhOpt = INH_YES;

	sel->fromClause = list_make1(e);

	col1 = makeColumnRef1("eid");
	col2 = makeColumnRef2(CTE_ENAME, "id");
	cond = makeSimpleA_Expr(AEXPR_OP, "=", col1, col2, -1);
	where_args = list_make1(cond);

	sel->whereClause = (Node *) makeBoolExpr(AND_EXPR, where_args, -1);

	earr = makeSubLink((Node *) sel);

	sel = makeNode(SelectStmt);
	aa = makeFuncCall(list_make1(makeString("array_agg")), list_make1(earr), -1);
	sel->targetList = list_make1(makeResTarget((Node *) aa, CTE_COLNAME_VARR));

	unnest = makeFuncCall(
			list_make1(makeString("unnest")),
			list_make1(makeColumnRef2(CTE_NAME, CTE_COLNAME_EIDARR)), -1);

	rf = makeNode(RangeFunction);
	rf->lateral = false;
	rf->ordinality = false;
	rf->is_rowsfrom = false;
	rf->functions = list_make1(list_make2(unnest, NIL));
	rf->alias = makeAliasNoDup("eid", NIL);
	rf->coldeflist = NIL;

	sel->fromClause = list_make1(rf);

	return makeSubLink((Node *) sel);
}

static Node *
makeVertexId(Node *vertexExpr)
{
	return (Node *) makeFuncCall(
			list_make1(makeString("id")), list_make1(vertexExpr), -1);
}

static Node *
makeColumnRef1(char *colname)
{
	List *fields;
	fields = list_make1(makeString(colname));
	return makeColumnRef(fields);
}

static Node *
makeColumnRef2(char *objname, char *colname)
{
	List *fields;
	fields = list_make2(makeString(objname), makeString(colname));
	return makeColumnRef(fields);
}

static Node *
makeColumnRef(List *fields)
{
	ColumnRef *cref = makeNode(ColumnRef);
	cref->fields = fields;
	cref->location =-1;
	return (Node *) cref;
}

static Node *
makeResTarget(Node *val, char *name)
{
    ResTarget *res;

    res = makeNode(ResTarget);
    if (name != NULL)
        res->name = pstrdup(name);
    res->val = val;
    res->location = -1;

    return (Node *) res;
}

static Node *
makeSimpleResTarget(char *field, char *name)
{
    ColumnRef *cref;

    cref = makeNode(ColumnRef);
    cref->fields = list_make1(makeString(pstrdup(field)));
    cref->location = -1;

    return makeResTarget((Node *) cref, name);
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
makeSubLink(Node *sel)
{
	SubLink *n = makeNode(SubLink);
	n->subLinkType = EXPR_SUBLINK;
	n->subLinkId = 0;
	n->testexpr = NULL;
	n->operName = NIL;
	n->subselect = sel;
	n->location = -1;
	return (Node *) n;
}
