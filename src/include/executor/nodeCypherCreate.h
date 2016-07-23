/*
 * nodeCypherCreate.h
 *	  Declarations for vertex and edge data type.
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * src/include/executor/nodeCypherCreate.h
 */

#ifndef NODECYPHERCREATE_H
#define NODECYPHERCREATE_H

#include "nodes/execnodes.h"

extern CypherCreateState *ExecInitCypherCreate(CypherCreate *node,
											   EState *estate, int eflags);
extern TupleTableSlot *ExecCypherCreate(CypherCreateState *node);
extern void ExecEndCypherCreate(CypherCreateState *node);

#endif	/* NODECYPHERCREATE_H */
