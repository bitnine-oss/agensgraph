/*
 * nodeNestloopVle.h
 *
 * Copyright (c) 2017 by Bitnine Global, Inc.
 *
 * src/include/executor/nodeNestloopVle.h
 */

#ifndef NODENESTLOOPVLE_H
#define NODENESTLOOPVLE_H

#include "nodes/execnodes.h"

extern NestLoopVLEState *ExecInitNestLoopVLE(NestLoopVLE *node, EState *estate,
											 int eflags);
extern void ExecEndNestLoopVLE(NestLoopVLEState *node);
extern void ExecReScanNestLoopVLE(NestLoopVLEState *node);

#endif   /* NODENESTLOOPVLE_H */
