/*-------------------------------------------------------------------------
 *
 * nodeHash2Side.h
 *	  prototypes for nodeHash2Side.c
 *
 *
 * Copyright (c) 2018 by Bitnine Global, Inc.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeHash.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEHASH2SIDE_H
#define NODEHASH2SIDE_H

#include "nodes/execnodes.h"

extern Hash2SideState *ExecInitHash2Side(Hash2Side *node, EState *estate, int eflags);
extern Node *MultiExecHash2Side(Hash2SideState *node);
extern void ExecEndHash2Side(Hash2SideState *node);
extern void ExecReScanHash2Side(Hash2SideState *node);

extern HashJoinTable ExecHash2SideTableCreate(Hash2SideState *node, List *hashOperators,
											  double ntuples, double npaths, long hops, Size spacePeak);
extern HashJoinTable ExecHash2SideTableClone(Hash2SideState *node, List *hashOperators,
											 HashJoinTable sourcetable, Size spacePeak);
extern void ExecHash2SideTableDestroy(HashJoinTable hashtable);
extern void ExecHash2SideIncreaseNumBuckets(HashJoinTable hashtable, Hash2SideState *node);
extern bool ExecHash2SideTableInsert(HashJoinTable hashtable,
									 TupleTableSlot *slot,
									 uint32 hashvalue,
									 Hash2SideState *node,
									 ShortestpathState *spstate,
									 long *saved);
extern bool ExecHash2SideTableInsertTuple(HashJoinTable hashtable,
										  MinimalTuple tuple,
										  uint32 hashvalue,
										  Hash2SideState *node,
										  ShortestpathState *spstate,
										  long *saved);
extern bool ExecHash2SideTableInsertGraphid(HashJoinTable hashtable,
											Graphid id,
											uint32 hashvalue,
											Hash2SideState *node,
											ShortestpathState *spstate,
											long *saved);
extern void ExecHash2SideGetBucketAndBatch(HashJoinTable hashtable,
										   uint32 hashvalue,
										   int *bucketno,
										   int *batchno);
extern bool ExecScanHash2SideBucket(Hash2SideState *node,
									ShortestpathState *spstate,
									ExprContext *econtext);
extern void ExecHash2SideTableReset(HashJoinTable hashtable);
extern void ExecChooseHash2SideTableSize(double ntuples,
										 double npaths,
										 int tupwidth,
										 long hops,
										 int *numbuckets,
										 int *numbatches);

#endif   /* NODEHASH2SIDE_H */
