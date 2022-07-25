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

extern Node *makeJsonbFuncAccessor(ParseState *pstate, Node *expr, List *path);
extern bool IsJsonbAccessor(Node *expr);
extern void getAccessorArguments(Node *node, Node **expr, List **path);
extern bool ConvertReservedColumnRefForIndex(Node *node, Oid relid);

extern Alias *makeAliasNoDup(char *aliasname, List *colnames);
extern Alias *makeAliasOptUnique(char *aliasname);
extern char *genUniqueName(void);

extern  void makeExtraFromNSItem(ParseNamespaceItem *nsitem, RangeTblRef **rtr,
								 bool visible);
extern void addNSItemToJoinlist(ParseState *pstate, ParseNamespaceItem *nsitem,
								bool visible);

extern Var *make_var(ParseState *pstate, ParseNamespaceItem *nsitem,
					 AttrNumber attnum, int location);

#endif