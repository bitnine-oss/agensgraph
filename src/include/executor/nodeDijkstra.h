/*
 * nodeDijkstra.h
 *
 * Copyright (c) 2017 by Bitnine Global, Inc.
 *
 * src/include/executor/nodeDijkstra.h
 */

#ifndef NODEDIJKSTRA_H
#define NODEDIJKSTRA_H

#include "nodes/execnodes.h"

extern DijkstraState *ExecInitDijkstra(Dijkstra *node, EState *estate,
									   int eflags);
extern void ExecEndDijkstra(DijkstraState *node);
extern void ExecReScanDijkstra(DijkstraState *node);

#endif
