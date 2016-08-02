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

extern Query *transformCypherMatchClause(ParseState *pstate,
										 CypherClause *clause);
extern Query *transformCypherProjection(ParseState *pstate,
										CypherClause *clause);
extern Query *transformCypherCreateClause(ParseState *pstate,
										  CypherClause *clause);
extern Query *transformCypherDeleteClause(ParseState *pstate,
										  CypherClause *clause);

#endif	/* PARSE_GRAPH_H */
