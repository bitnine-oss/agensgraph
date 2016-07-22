/*-------------------------------------------------------------------------
 *
 * nodeCypherCreate.h
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * TODO : ADD portions
 *
 * src/include/executor/nodeCypherCreate.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODECYPHERCREATE_H
#define NODECYPHERCREATE_H

#include "nodes/execnodes.h"

extern CypherCreateState * ExecInitCypherCreate(CypherCreate *node, EState *estate, int eflags);
extern TupleTableSlot * ExecCypherCreate(CypherCreateState *node);
extern void ExecEndCypherCreate(CypherCreateState *node);

#endif   /* NODECYPHERCREATE_H */
