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

extern void CreateGraphCommand(CreateGraphStmt *graphStmt,
							   const char *queryString);
extern void RemoveGraphById(Oid graphid);

extern void CreateLabelCommand(CreateLabelStmt *labelStmt,
							   const char *queryString, ParamListInfo params);
extern void CheckDropLabel(ObjectType removeType, Oid labid);
extern void CheckInheritLabel(CreateStmt *stmt);

extern bool RangeVarIsLabel(RangeVar *rel);

#endif	/* GRAPHCMDS_H */
