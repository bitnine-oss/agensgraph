/*
 * parse_shortestpath.h
 *	  transform shortestpath() and allshortestpaths()
 *
 * Copyright (c) 2017 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/include/parser/parse_shortestpath.h
 */

#ifndef PARSE_SHORTESTPATH_H
#define PARSE_SHORTESTPATH_H

#include "parser/parse_node.h"

extern Query *transformShortestPath(ParseState *pstate, CypherPath *cpath);
extern Query *transformShortestPathInMatch(ParseState *parentParseState,
										   CypherPath *cpath);

#endif	/* PARSE_SHORTESTPATH_H */
