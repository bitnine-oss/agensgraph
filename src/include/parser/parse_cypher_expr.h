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

extern Node *transformCypherExpr(ParseState *pstate, Node *expr,
								 ParseExprKind exprKind);

extern List *transformItemList(ParseState *pstate, List *items,
							   ParseExprKind exprKind);

#endif	/* PARSE_CYPHER_EXPR_H */
