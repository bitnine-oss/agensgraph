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

#define NULL_COLUMN_NAME		"_N_"

extern Node *makeJsonbFuncAccessor(ParseState *pstate, Node *expr, List *path);
extern bool IsJsonbAccessor(Node *expr);
extern void getAccessorArguments(Node *node, Node **expr, List **path);
extern bool ConvertReservedColumnRefForIndex(Node *node, Oid relid);

extern Alias *makeAliasNoDup(char *aliasname, List *colnames);
extern Alias *makeAliasOptUnique(char *aliasname);
extern char *genUniqueName(void);

extern void makeExtraFromNSItem(ParseNamespaceItem *nsitem, RangeTblRef **rtr,
								bool visible);
extern void addNSItemToJoinlist(ParseState *pstate, ParseNamespaceItem *nsitem,
								bool visible);

extern Var *make_var(ParseState *pstate, ParseNamespaceItem *nsitem,
					 AttrNumber attnum, int location);


extern Node *makeRowExprWithTypeCast(List *args, Oid typeOid, int location);
extern Node *makeTypedRowExpr(List *args, Oid typoid, int location);
extern Node *makeAArrayExpr(List *elements, Oid typeOid);
extern Node *makeArrayExpr(Oid typarray, Oid typoid, List *elems);
extern Node *getColumnVar(ParseState *pstate, ParseNamespaceItem *nsitem,
						  char *colname);
extern Node *getSysColumnVar(ParseState *pstate, ParseNamespaceItem *nsitem,
							 AttrNumber attnum);

extern Node *makeColumnRef(List *fields);

extern Node *makeVertexExpr(ParseState *pstate, ParseNamespaceItem *nsitem,
							int location);
extern Node *makeEdgeExpr(ParseState *pstate, CypherRel *crel,
						  ParseNamespaceItem *nsitem, int location);

extern ResTarget *makeSimpleResTarget(const char *field, const char *name);
extern ResTarget *makeResTarget(Node *val, const char *name);

extern FuncCall *makeArrayAggFuncCall(List *args, int location);

#endif
