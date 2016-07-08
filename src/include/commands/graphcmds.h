/*-------------------------------------------------------------------------
 *
 * graphcmds.h
 *	  prototypes for graphcmds.c.
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * src/include/commands/graphcmds.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GRAPHCMDS_H
#define GRAPHCMDS_H

#include "access/htup.h"
#include "catalog/dependency.h"
#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"
#include "nodes/params.h"
#include "storage/lock.h"
#include "utils/relcache.h"

extern void DefineLabel(CreateLabelStmt *labstmt,
						const char *queryString,
						ParamListInfo params,
						ObjectAddress secondaryObject);

extern void RemoveLabels(DropStmt *drop);

#endif   /* GRAPHCMDS_H */
