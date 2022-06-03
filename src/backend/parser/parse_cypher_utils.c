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
#include "catalog/pg_type_d.h"
#include "nodes/makefuncs.h"
#include "parser/parse_cypher_utils.h"
#include "parser/parse_relation.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

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