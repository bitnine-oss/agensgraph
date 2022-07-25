/*
 * parse_cypher_utils.c
 *
 * Copyright (c) 2022 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/include/parser/parse_cypher_utils.c
 */

#include "postgres.h"

#include "access/relscan.h"
#include "catalog/ag_label.h"
#include "nodes/makefuncs.h"
#include "parser/parse_cypher_utils.h"
#include "parser/parse_relation.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "parser/parsetree.h"

Node *makeJsonbFuncAccessor(ParseState *pstate, Node *expr, List *path)
{
	CypherAccessExpr *a = makeNode(CypherAccessExpr);
	a->arg = (Expr *) expr;
	a->path = path;

	return (Node *) a;
}

bool IsJsonbAccessor(Node *expr)
{
	if (IsA(expr, CypherAccessExpr))
	{
		return true;
	}

	return false;
}

void getAccessorArguments(Node *node, Node **expr, List **path)
{
	if (IsA(node, CypherAccessExpr))
	{
		CypherAccessExpr *a = (CypherAccessExpr *) node;

		*expr = (Node *) a->arg;
		*path = a->path;
	}
	else
	{
		elog(ERROR, "cannot extract elements from node");
	}
}

/*
 * When use PROPERTY INDEX, there is no way to specify original column
 * reference, so make vertex_id, edge_start, and edge_end in the form of
 * reserved words to create a way to specify B.
 */
bool ConvertReservedColumnRefForIndex(Node *node, Oid relid)
{
	Form_ag_label labtup;
	Oid laboid = get_relid_laboid(relid);
	HeapTuple tuple = SearchSysCache1(LABELOID, ObjectIdGetDatum(laboid));
	bool isVertex;

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for label (OID=%u)", laboid);

	labtup = (Form_ag_label) GETSTRUCT(tuple);
	isVertex = (labtup->labkind == LABEL_KIND_VERTEX);
	ReleaseSysCache(tuple);

	if (IsA(node, ColumnRef))
	{
		ColumnRef *columnRef = (ColumnRef *) node;
		if (columnRef->fields->length == 1)
		{
			Node *field_name = linitial(columnRef->fields);
			if (IsA(field_name, String))
			{
				char *fieldStr = strVal(field_name);
				if (isVertex && (strcmp(fieldStr, "vertex_id") == 0))
				{
					columnRef->fields = list_make1(makeString("id"));
					return true;
				}
				else if (!isVertex && strcmp(fieldStr, "edge_start") == 0)
				{
					columnRef->fields = list_make1(makeString("start"));
					return true;
				}
				else if (!isVertex && strcmp(fieldStr, "edge_end") == 0)
				{
					columnRef->fields = list_make1(makeString("end"));
					return true;
				}
			}
		}
	}

	return false;
}


Alias *
makeAliasOptUnique(char *aliasname)
{
	aliasname = (aliasname == NULL ? genUniqueName() : pstrdup(aliasname));
	return makeAliasNoDup(aliasname, NIL);
}


Alias *
makeAliasNoDup(char *aliasname, List *colnames)
{
	Alias *alias;

	alias = makeNode(Alias);
	alias->aliasname = aliasname;
	alias->colnames = colnames;

	return alias;
}

/* generate unique name */
char *
genUniqueName(void)
{
	/* NOTE: safe unless there are more than 2^32 anonymous names at once */
	static uint32 seq = 0;

	char data[NAMEDATALEN];

	snprintf(data, sizeof(data), "<%010u>", seq++);

	return pstrdup(data);
}

void
makeExtraFromRTE(ParseState *pstate, RangeTblEntry *rte, RangeTblRef **rtr,
				 ParseNamespaceItem **nsitem, bool visible)
{
	int			rtindex;

	/*
	 * Most callers have just added the RTE to the rangetable, so it's likely
	 * to be the last entry.  Hence, it's a good idea to search the rangetable
	 * back-to-front.
	 */
	for (rtindex = list_length(pstate->p_rtable); rtindex > 0; rtindex--)
	{
		if (rte == rt_fetch(rtindex, pstate->p_rtable))
			break;
	}
	if (rtindex <= 0)
		elog(ERROR, "RTE not found (internal error)");

	if (rtr != NULL)
	{
		RangeTblRef *_rtr;

		_rtr = makeNode(RangeTblRef);
		_rtr->rtindex = rtindex;

		*rtr = _rtr;
	}

	if (nsitem != NULL)
	{
		ParseNamespaceItem *_nsitem;

		_nsitem = (ParseNamespaceItem *) palloc(sizeof(ParseNamespaceItem));
		_nsitem->p_rtindex = rtindex;
		_nsitem->p_rte = rte;
		_nsitem->p_rel_visible = visible;
		_nsitem->p_cols_visible = visible;
		_nsitem->p_lateral_only = false;
		_nsitem->p_lateral_ok = true;

		*nsitem = _nsitem;
	}
}

void
addRTEtoJoinlist(ParseState *pstate, RangeTblEntry *rte, bool visible)
{
	ParseNamespaceItem *conflict_nsitem;
	RangeTblRef *rtr;
	ParseNamespaceItem *nsitem;
	/*
	 * There should be no namespace conflicts because we check a variable
	 * (which becomes an alias) is duplicated. This check remains to prevent
	 * future programming error.
	 */
	conflict_nsitem = scanNameSpaceForRefname(pstate, rte->eref->aliasname, -1);
	if (conflict_nsitem != NULL)
	{
		RangeTblEntry *tmp = conflict_nsitem->p_rte;
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


/*
 * make_var - Copy from a previous version of PG. 
 *		Build a Var node for an attribute identified by RTE and attrno
 */
Var *
make_var(ParseState *pstate, RangeTblEntry *rte, int attrno, int location)
{
	Var		   *result;
	int			vnum,
			sublevels_up;
	Oid			vartypeid;
	int32		type_mod;
	Oid			varcollid;

	vnum = RTERangeTablePosn(pstate, rte, &sublevels_up);
	get_rte_attribute_type(rte, attrno, &vartypeid, &type_mod, &varcollid);
	result = makeVar(vnum, attrno, vartypeid, type_mod, varcollid, sublevels_up);
	result->location = location;
	return result;
}

/*
 * RTERangeTablePosn - Copy from a previous version of PG.
 *		given an RTE, return RT index (starting with 1) of the entry,
 *		and optionally get its nesting depth (0 = current).  If sublevels_up
 *		is NULL, only consider rels at the current nesting level.
 *		Raises error if RTE not found.
 */
int
RTERangeTablePosn(ParseState *pstate, RangeTblEntry *rte, int *sublevels_up)
{
	int			index;
	ListCell   *l;

	if (sublevels_up)
		*sublevels_up = 0;

	while (pstate != NULL)
	{
		index = 1;
		foreach(l, pstate->p_rtable)
		{
			if (rte == (RangeTblEntry *) lfirst(l))
				return index;
			index++;
		}
		pstate = pstate->parentParseState;
		if (sublevels_up)
			(*sublevels_up)++;
		else
			break;
	}

	elog(ERROR, "RTE not found (internal error)");
	return 0;					/* keep compiler quiet */
}