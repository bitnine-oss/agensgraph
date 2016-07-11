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

#include "catalog/objectaddress.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"

extern void DefineLabel(CreateLabelStmt *labelStmt, const char *queryString,
						ParamListInfo params, ObjectAddress secondaryObject);
extern void RemoveLabels(DropStmt *drop);

#endif	/* GRAPHCMDS_H */
