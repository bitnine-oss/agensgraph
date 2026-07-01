/*
 * execGraphMeta.h
 *	  ag_graphmeta connectivity maintenance for non-Cypher edge writes.
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/include/executor/execGraphMeta.h
 */
#ifndef EXECGRAPHMETA_H
#define EXECGRAPHMETA_H

#include "executor/tuptable.h"
#include "utils/rel.h"

extern void GraphmetaRecordEdgeInsertFromSlot(Relation rel, TupleTableSlot *slot);

#endif							/* EXECGRAPHMETA_H */
