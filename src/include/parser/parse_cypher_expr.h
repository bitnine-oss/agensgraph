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

/* coerce functions */
extern Node *coerce_expr(ParseState *pstate, Node *expr, Oid ityp, Oid otyp,
						 int32 otypmod, CoercionContext cctx,
						 CoercionForm cform, int loc);
extern Node *filterAccessArg(ParseState *pstate, Node *expr, int location,
							 const char *types);

/* clause functions */
extern Node *transformCypherWhere(ParseState *pstate, Node *clause,
								  ParseExprKind exprKind,
								  const char *constructName);
extern Node *transformCypherLimit(ParseState *pstate, Node *clause,
								  ParseExprKind exprKind,
								  const char *constructName);
extern List *transformCypherOrderBy(ParseState *pstate, List *orderlist,
									List **targetlist);

/* item list functions */
extern List *transformItemList(ParseState *pstate, List *items,
							   ParseExprKind exprKind);
extern void resolveItemList(ParseState *pstate, List *items);
extern List *transformCypherExprList(ParseState *pstate, List *exprlist,
									 ParseExprKind exprKind);

/*
 * Rebind a comparison whose promoted-property operand arrives boxed as
 * cypher_to_jsonb(column) to the native, index-usable operator; used by the map
 * property-constraint lowering so "{key: val}" binds the typed column like a
 * WHERE comparison does.
 */
extern Node *transformPromotedComparison(ParseState *pstate, List *opname,
										 Node *l, Node *r, Node *last_srf,
										 int location);

/*
 * Read the identity out of a node or a relationship, or return NULL if the
 * expression is neither.  Comparing, ordering or grouping elements is exactly
 * comparing, ordering or grouping their identities.
 */
extern Node *elementIdentity(Node *elem);

#endif							/* PARSE_CYPHER_EXPR_H */
