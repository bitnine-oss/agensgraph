/*
 * nodeModifyGraph.h
 *
 * Copyright (c) 2025 by SKAI Worldwide Co., Ltd.
 *
 * src/include/executor/execCypherDelete.h
 */
#ifndef AGENSGRAPH_EXECCYPHERDELETE_H
#define AGENSGRAPH_EXECCYPHERDELETE_H

#include "tuptable.h"
#include "nodes/execnodes.h"

extern TupleTableSlot *ExecDeleteGraph(ModifyGraphState *mgstate,
									   TupleTableSlot *slot);

#endif							/* AGENSGRAPH_EXECCYPHERDELETE_H */
