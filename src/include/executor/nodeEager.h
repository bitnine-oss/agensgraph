/*
 * nodeEager.h
 *
 * Copyright (c) 2017 by Bitnine Global, Inc.
 *
 * src/include/executor/nodeEager.h
 */

#ifndef NODEEAGER_H
#define NODEEAGER_H

#include "nodes/execnodes.h"

extern EagerState *ExecInitEager(Eager *node, EState *estate, int eflags);
extern TupleTableSlot *ExecEager(EagerState *node);
extern void ExecEndEager(EagerState *node);
extern void ExecReScanEager(EagerState *node);

#endif   /* NODEEAGER_H */
