/*-------------------------------------------------------------------------
 *
 * nodeShortestpath.h
 *	  prototypes for nodeShortestpath.c
 *
 * Copyright (c) 2018 by Bitnine Global, Inc.
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeShortestpath.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODESHORTESTPATH_H
#define NODESHORTESTPATH_H

#include "nodes/execnodes.h"
#include "storage/buffile.h"

extern ShortestpathState *ExecInitShortestpath(Shortestpath *node, EState *estate, int eflags);
extern void ExecEndShortestpath(ShortestpathState *node);
extern void ExecReScanShortestpath(ShortestpathState *node);

extern void ExecShortestpathSaveTuple(MinimalTuple tuple, uint32 hashvalue,
									  BufFile **fileptr);
extern TupleTableSlot *ExecShortestpathGetSavedTuple(BufFile           *file,
													 uint32            *hashvalue,
													 TupleTableSlot    *tupleSlot);

#endif   /* NODESHORTESTPATH_H */
