/*
 * parse_graph.h
 *	  handle clauses for graph in parser
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/include/parser/parse_graph.h
 */

#ifndef PARSE_GRAPH_H
#define PARSE_GRAPH_H

#include "parser/parse_node.h"

#define AGENS_DEFAULT_PREFIX	"_agens_default_"
#define CYPHER_SUBQUERY_ALIAS	AGENS_DEFAULT_PREFIX"s"
/* hidden column carrying a promoted property across a projection boundary */
#define PROMOTED_SENTINEL_PREFIX	AGENS_DEFAULT_PREFIX "prop:"
#define CYPHER_OPTMATCH_ALIAS	AGENS_DEFAULT_PREFIX"o"
#define CYPHER_MERGEMATCH_ALIAS	AGENS_DEFAULT_PREFIX"m"
#define CYPHER_DELETEJOIN_ALIAS	AGENS_DEFAULT_PREFIX"d"
#define CYPHER_FOR_ALIAS		AGENS_DEFAULT_PREFIX"f"
#define CYPHER_COUNT_ALIAS		AGENS_DEFAULT_PREFIX"c"
#define CYPHER_CALL_ALIAS		AGENS_DEFAULT_PREFIX"call"
#define CYPHER_YIELD_ALIAS		AGENS_DEFAULT_PREFIX"y"

extern bool enable_eager;
extern bool enable_property_promotion;

/*
 * Resolve a graph-element property reference n.key to the promoted typed column
 * projected across the pattern-subquery boundary under a hidden sentinel name.
 * Returns the resolved (native) Var, or NULL to fall back to the jsonb property
 * path.  Defined in parse_graph.c so it can reuse getSourceRelid.
 */
extern bool propertyNeedsPerRelation(ParseState *pstate, Node *basenode,
									 char *key);
extern Node *resolvePromotedProperty(ParseState *pstate, Node *basenode,
									 char *key, int location);

/* True for a reserved hidden promoted-column sentinel output name. */
extern bool isPromotedSentinelName(const char *resname);

extern Query *transformCypherSubPattern(ParseState *pstate,
										CypherSubPattern *subpat);
extern Query *transformCypherProjection(ParseState *pstate,
										CypherClause *clause);
extern Query *transformCypherMatchClause(ParseState *pstate,
										 CypherClause *clause);
extern Query *transformCypherCreateClause(ParseState *pstate,
										  CypherClause *clause);
extern Query *transformCypherDeleteClause(ParseState *pstate,
										  CypherClause *clause);
extern Query *transformCypherSetClause(ParseState *pstate,
									   CypherClause *clause);
extern Query *transformCypherMergeClause(ParseState *pstate,
										 CypherClause *clause);
extern Query *transformCypherLoadClause(ParseState *pstate,
										CypherClause *clause);
extern Query *transformCypherUnwindClause(ParseState *pstate,
										  CypherClause *clause);
extern Query *transformCypherModifier(ParseState *pstate,
									  CypherClause *clause);
extern Query *transformCypherFilterClause(ParseState *pstate,
										  CypherClause *clause);
extern Query *transformCypherForClause(ParseState *pstate,
									   CypherClause *clause);
extern Query *transformCypherCallClause(ParseState *pstate,
										CypherClause *clause);
extern Query *transformCypherYieldCallClause(ParseState *pstate,
											 CypherClause *clause);
extern Query *transformCypherSubselectClause(ParseState *pstate,
											 CypherClause *clause);
extern Query *transformCypherCountClause(ParseState *pstate,
										 CypherCountClause *clause);
extern TargetEntry *makeCountTargetEntry(ParseState *pstate);

extern bool IsGraphWriteClause(Node *clause);

/*
 * Build a FieldSelect that extracts the named field of a composite expression;
 * for a vertex/edge this reduces it to its graphid "id".  Used by the graph
 * membership rewrite in parse_cypher_expr.c.
 */
extern Node *getExprField(Expr *expr, char *fname);

#endif							/* PARSE_GRAPH_H */
