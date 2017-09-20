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
extern Node *wrapEdgeRef(Node *node);
extern Node *wrapEdgeRefArray(Node *node);
extern Node *wrapEdgeRefTypes(ParseState *pstate, Node *node);

extern List *transformItemList(ParseState *pstate, List *items,
							   ParseExprKind exprKind);
extern void wrapEdgeRefTargetList(ParseState *pstate, List *targetList);
extern void unwrapEdgeRefTargetList(List *targetList);

#endif	/* PARSE_CYPHER_EXPR_H */
