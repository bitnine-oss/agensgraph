/*
 * graphcmds.h
 *	  prototypes for graphcmds.c
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * src/include/commands/graphcmds.h
 */
#ifndef GRAPHCMDS_H
#define GRAPHCMDS_H

#include "nodes/params.h"
#include "nodes/parsenodes.h"

extern void CreateGraphCommand(CreateGraphStmt *stmt, const char *queryString,
							   int stmt_location, int stmt_len);
extern void RemoveGraphById(Oid graphid);
extern ObjectAddress RenameGraph(const char *oldname, const char *newname);

extern void CreateLabelCommand(CreateLabelStmt *labelStmt,
							   const char *queryString, int stmt_location,
							   int stmt_len, ParamListInfo params);
extern ObjectAddress RenameLabel(RenameStmt *stmt);
extern void CheckLabelSqlReshape(Oid relid, bool recurse, List *cmds);
extern void CheckLabelType(ObjectType type, Oid laboid, const char *command);
extern void CheckInheritLabel(CreateStmt *stmt);

extern bool RangeVarIsLabel(RangeVar *rel);
extern bool RelationIsLabel(Relation rel);

extern void CreateConstraintCommand(CreateConstraintStmt *constraintStmt,
									const char *queryString, int stmt_location,
									int stmt_len, ParamListInfo params);
extern void DropConstraintCommand(DropConstraintStmt *constraintStmt,
								  const char *queryString, int stmt_location,
								  int stmt_len, ParamListInfo params);

extern Oid	DisableIndexCommand(DisableIndexStmt *disableStmt);

extern bool isEmptyLabel(char *label_name);
extern void deleteRelatedEdges(const char *vlab);

/*
 * ALTER label ADD/DROP [COLUMN] of a promoted property lowers to a generic
 * ALTER TABLE ADD/DROP COLUMN;
 * these bracket that AlterTable() call to keep ag_label_property in sync.
 * Begin runs with the lock held but before the column is added/dropped (it must
 * copy an ADD's ColumnDef before AlterTable mutates it, and capture a DROP's
 * property key before the column vanishes); Finish runs after.  An opaque state
 * is carried between them, or NULL when there is nothing to sync.
 */
typedef struct AlterLabelPropertyState AlterLabelPropertyState;

extern AlterLabelPropertyState *BeginAlterLabelProperties(Oid relid, List *cmds);
extern void FinishAlterLabelProperties(Oid relid,
									   AlterLabelPropertyState *state);

#endif							/* GRAPHCMDS_H */
