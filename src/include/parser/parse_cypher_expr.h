/*
 * parse_cypher_expr.h
 *	  handle Cypher expressions in parser
 *
 * Copyright (c) 2017 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/include/parser/parse_cypher_expr.h
 */

#ifndef PARSE_CYPHER_EXPR_H
#define PARSE_CYPHER_EXPR_H

#include "parser/parse_node.h"

/* GUC variable (enable/disable null properties) */
extern bool allow_null_properties;

extern Node *transformCypherExpr(ParseState *pstate, Node *expr,
								 ParseExprKind exprKind);
extern Node *transformCypherMapForSet(ParseState *pstate, Node *expr,
									  List **pathelems, char **varname);

/* clause functions */
extern Node *transformCypherWhere(ParseState *pstate, Node *clause,
								  ParseExprKind exprKind);
extern Node *transformCypherLimit(ParseState *pstate, Node *clause,
								  ParseExprKind exprKind,
								  const char *constructName);
extern List *transformCypherOrderBy(ParseState *pstate, List *orderlist,
									List **targetlist);

/* item list functions */
extern List *transformItemList(ParseState *pstate, List *items,
							   ParseExprKind exprKind);
extern List *transformCypherExprList(ParseState *pstate, List *exprlist,
									 ParseExprKind exprKind);

#endif	/* PARSE_CYPHER_EXPR_H */
