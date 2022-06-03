/*
 * parse_cypher_utils.c
 *
 * Copyright (c) 2022 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/include/parser/parse_cypher_utils.c
 */

#include "postgres.h"

#include "parser/parse_cypher_utils.h"
#include "catalog/pg_type_d.h"
#include "utils/fmgroids.h"
#include "nodes/makefuncs.h"
#include "parser/parse_relation.h"

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