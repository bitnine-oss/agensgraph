/*
 * nodeModifyGraph.h
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * src/include/executor/nodeModifyGraph.h
 */

#ifndef NODEMODIFYGRAPH_H
#define NODEMODIFYGRAPH_H

#include "nodes/execnodes.h"

extern ModifyGraphState *ExecInitModifyGraph(ModifyGraph *mgplan,
											 EState *estate, int eflags);
extern void ExecEndModifyGraph(ModifyGraphState *mgstate);

#endif
