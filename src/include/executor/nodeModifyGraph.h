/*
 * nodeModifyGraph.h
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * src/include/executor/nodeModifyGraph.h
 */

#ifndef NODEMODIFYGRAPH_H
#define NODEMODIFYGRAPH_H

#include "nodes/execnodes.h"

/* for visibility between Cypher clauses */
typedef enum ModifyCid
{
	MODIFY_CID_LOWER_BOUND,		/* for previous clause */
	MODIFY_CID_OUTPUT,			/* for CREATE, MERGE, DELETE */
	MODIFY_CID_SET,				/* for SET, ON MATCH SET, ON CREATE SET */
	MODIFY_CID_NLJOIN_MATCH,	/* for DELETE JOIN, MERGE JOIN */
	MODIFY_CID_MAX
} ModifyCid;

extern bool enable_multiple_update;

extern ModifyGraphState *ExecInitModifyGraph(ModifyGraph *mgplan,
											 EState *estate, int eflags);
extern void ExecEndModifyGraph(ModifyGraphState *mgstate);

#endif
