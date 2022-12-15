/*
 * execGraphVle.h
 *
 * Copyright (c) 2022 by Bitnine Global, Inc.
 *
 * src/include/executor/execGraphVle.h
 */

#ifndef AGENSGRAPH_EXECGRAPHVLE_H
#define AGENSGRAPH_EXECGRAPHVLE_H

#include "nodes/execnodes.h"

extern GraphVLEState *ExecInitGraphVLE(GraphVLE *vleplan, EState *estate, int eflags);

extern void ExecReScanGraphVLE(GraphVLEState *vle_state);
extern void ExecEndGraphVLE(GraphVLEState *vle_state);


#endif							/* AGENSGRAPH_EXECGRAPHVLE_H */
