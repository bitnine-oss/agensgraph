/*
 * nodeModifyGraph.h
 *
 * Copyright (c) 2025 by SKAI Worldwide Co., Ltd.
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
