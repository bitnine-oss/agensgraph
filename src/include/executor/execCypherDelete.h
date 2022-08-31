/*
 * nodeModifyGraph.h
 *
 * Copyright (c) 2022 by Bitnine Global, Inc.
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
