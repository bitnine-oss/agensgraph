/*
 * nodeModifyGraph.h
 *
 * Copyright (c) 2022 by Bitnine Global, Inc.
 *
 * src/include/executor/execCypherSet.h
 */

#ifndef AGENSGRAPH_EXECCYPHERSET_H
#define AGENSGRAPH_EXECCYPHERSET_H

#include "tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "nodes/graphnodes.h"

extern TupleTableSlot *LegacyExecSetGraph(ModifyGraphState *mgstate,
										  TupleTableSlot *slot, GSPKind kind);
extern TupleTableSlot *ExecSetGraph(ModifyGraphState *mgstate,
									TupleTableSlot *slot);
extern ItemPointer LegacyUpdateElemProp(ModifyGraphState *mgstate, Oid elemtype,
										Datum gid, Datum elem_datum);
extern Datum makeModifiedElem(Datum elem, Oid elemtype,
							  Datum id, Datum prop_map, Datum tid);

#endif							/* AGENSGRAPH_EXECCYPHERSET_H */
