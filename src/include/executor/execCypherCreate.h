/*
 * nodeModifyGraph.h
 *
 * Copyright (c) 2025 by SKAI Worldwide Co., Ltd.
 *
 * src/include/executor/execCypherCreate.h
 */

#ifndef AGENSGRAPH_EXECCYPHERCREATE_H
#define AGENSGRAPH_EXECCYPHERCREATE_H

#include "tuptable.h"
#include "nodes/execnodes.h"

extern TupleTableSlot *ExecCreateGraph(ModifyGraphState *mgstate,
									   TupleTableSlot *slot);

#endif							/* AGENSGRAPH_EXECCYPHERCREATE_H */
