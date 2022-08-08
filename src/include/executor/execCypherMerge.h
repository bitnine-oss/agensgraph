/*
 * nodeModifyGraph.h
 *
 * Copyright (c) 2022 by Bitnine Global, Inc.
 *
 * src/include/executor/execCypherMerge.h
 */

#ifndef AGENSGRAPH_EXECCYPHERMERGE_H
#define AGENSGRAPH_EXECCYPHERMERGE_H

#include "tuptable.h"
#include "nodes/execnodes.h"

extern TupleTableSlot *ExecMergeGraph(ModifyGraphState *mgstate,
									  TupleTableSlot *slot);

#endif							/* AGENSGRAPH_EXECCYPHERMERGE_H */
