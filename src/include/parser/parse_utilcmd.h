/*-------------------------------------------------------------------------
 *
 * parse_utilcmd.h
 *		parse analysis for utility commands
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/parser/parse_utilcmd.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARSE_UTILCMD_H
#define PARSE_UTILCMD_H

#include "parser/parse_node.h"

#define AG_GRAPH			"graph"
#define AG_VERTEX			"vertex"
#define AG_EDGE				"edge"
#define AG_ELEM_LOCAL_ID	"id"
#define AG_ELEM_ID			"id"
#define AG_START_ID			"start"
#define AG_END_ID			"end"
#define AG_ELEM_PROP_MAP	"properties"

extern List *transformCreateStmt(CreateStmt *stmt, const char *queryString);
extern List *transformAlterTableStmt(Oid relid, AlterTableStmt *stmt,
						const char *queryString);
extern IndexStmt *transformIndexStmt(Oid relid, IndexStmt *stmt,
				   const char *queryString);
extern void transformRuleStmt(RuleStmt *stmt, const char *queryString,
				  List **actions, Node **whereClause);
extern List *transformCreateSchemaStmt(CreateSchemaStmt *stmt);

extern List *transformCreateLabelStmt(CreateLabelStmt *labelStmt,
									  const char *queryString);

#endif   /* PARSE_UTILCMD_H */
