/*
 * parse_cypher_utils.h
 *
 * Copyright (c) 2022 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/include/parser/parse_cypher_utils.h
 */

#ifndef AGENSGRAPH_PARSE_CYPHER_UTILS_H
#define AGENSGRAPH_PARSE_CYPHER_UTILS_H

#include "parser/parse_node.h"

Node *makeJsonbFuncAccessor(ParseState *pstate, Node *expr, List *path);
bool IsJsonbAccessor(Node *expr);
void getAccessorArguments(Node *node, Node **expr, List **path);
bool ConvertReservedColumnRefForIndex(Node *node, Oid relid);

#endif