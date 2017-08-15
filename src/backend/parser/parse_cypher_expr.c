/*
 * parse_cypher_expr.c
 *	  handle Cypher expressions in parser
 *
 * Copyright (c) 2017 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/parser/parse_cypher_expr.c
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "parser/parse_cypher_expr.h"

Node *
transformCypherExpr(ParseState *pstate, Node *expr, ParseExprKind exprKind)
{
	Const	   *null;

	if (expr == NULL)
		return NULL;

	null = makeConst(UNKNOWNOID, -1, InvalidOid, -2, (Datum) 0, true, false);
	null->location = -1;

	return (Node *) null;
}

List *
transformItemList(ParseState *pstate, List *items, ParseExprKind exprKind)
{
	List	   *targets = NIL;
	ListCell   *li;

	foreach(li, items)
	{
		ResTarget  *item = lfirst(li);
		Node	   *expr;
		char	   *colname;
		TargetEntry *te;

		expr = transformCypherExpr(pstate, item->val, exprKind);
		colname = (item->name == NULL ? "?column?" : item->name);

		te = makeTargetEntry((Expr *) expr,
							 (AttrNumber) pstate->p_next_resno++,
							 colname, false);

		targets = lappend(targets, te);
	}

	return targets;
}
