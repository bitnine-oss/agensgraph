/*-------------------------------------------------------------------------
 *
 * nodeCypherCreate.c
 *	  routines to handle CypherCreate nodes.
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeCypherCreate.c
 *
 *-------------------------------------------------------------------------
 */

/* INTERFACE ROUTINES
 *		ExecInitCypherCreate - initialize the CypherCreate node
 *		ExecCypherCreate	 - create graph patterns
 *		ExecEndCypherCreate	 - shut down the CypherCreate node
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/nodeCypherCreate.h"
#include "executor/spi.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/tqual.h"


/* TODO : looking for limit of cmd */
#define SQLCMD_LENGTH	200

#define SQLCMD_CREATE_VERTEX "INSERT INTO graph.%s VALUES (DEFAULT, '%s')" \
							 "RETURNING tableoid, *"
#define SQLCMD_CREATE_EDGE   "INSERT INTO graph.%s VALUES (DEFAULT, %d, %lld, %d, %lld, '%s')" \
							 "RETURNING tableoid, *"
typedef struct vertexInfo {
	long long 	vid;
	Oid			tableoid;
} vertexInfo;


/*
 * get RETURING information of vertex created by SPI_execute
 */
vertexInfo * getReturnedVertex( vertexInfo * vInfo )
{
	if (SPI_processed != 1)
	{
		elog(ERROR, "SPI_execute : must be created only a vertex per SPI_exec");
	}
	else
	{
		HeapTuple 	tuple = SPI_tuptable->vals[0];
		TupleDesc 	tupDesc = SPI_tuptable->tupdesc;
		bool		isnull;

		/************************************************************
		 * Get the attributes value
		 ************************************************************/
		Assert(SPI_fnumber(tupDesc, "tableoid") == 1 &&
			   SPI_fnumber(tupDesc, "vid") == 2 &&
			   SPI_fnumber(tupDesc, "properties") == 3);

		vInfo->tableoid = (Oid)SPI_getbinval(tuple, tupDesc, 1, &isnull);
		vInfo->vid = (long long)SPI_getbinval(tuple, tupDesc, 2, &isnull);

	}

	return vInfo;
}


/*
 * create a graph pattern
 * TODO : Add to process MATCHed variable
 */
void createPattern(CypherPattern *pattern)
{
	ListCell	   *lc;
	vertexInfo	 	curVertex;
	vertexInfo 		prevVertex;
	bool			NeedMakeRel = false;
	CypherRel	   *relInfo = NULL;

	foreach(lc, pattern->chain)
	{
		Node		   *graphElem = lfirst(lc);
		char 	 		queryCmd[SQLCMD_LENGTH];
		HeapTuple		tuple;
		TupleDesc		tupDesc;
		long long       vid;
		Oid				tableoid;
		int				i;

		switch (nodeTag(graphElem))
		{
			case T_CypherNode:
			{
				CypherNode *cnode = (CypherNode*)graphElem;
				char	   *vlabel= cnode->label ? cnode->label : "vertex";

				snprintf(queryCmd, SQLCMD_LENGTH, SQLCMD_CREATE_VERTEX,
						 vlabel, cnode->prop_map);

				if (SPI_execute(queryCmd, false, 0) != SPI_OK_INSERT_RETURNING)
					elog(ERROR, "SPI_execute failed: %s", queryCmd);

				if (getReturnedVertex( &curVertex ) == NULL )
					elog(ERROR, "failed to getResult in SPI_execute");

				if (relInfo)
				{
					char 	   *reltype = strVal(linitial(relInfo->types));

					if (relInfo->direction == CYPHER_REL_DIR_LEFT ||
						relInfo->direction == CYPHER_REL_DIR_NONE )
					{
						snprintf(queryCmd, SQLCMD_LENGTH, SQLCMD_CREATE_EDGE,
								 reltype, prevVertex.tableoid, prevVertex.vid,
								 curVertex.tableoid, curVertex.vid, relInfo->prop_map);

						if (SPI_execute(queryCmd, false, 0) != SPI_OK_INSERT_RETURNING)
							elog(ERROR, "SPI_execute failed: %s", queryCmd);
					}

					if (relInfo->direction == CYPHER_REL_DIR_RIGHT ||
						relInfo->direction == CYPHER_REL_DIR_NONE )
					{
						snprintf(queryCmd, SQLCMD_LENGTH, SQLCMD_CREATE_EDGE,
								 reltype, curVertex.tableoid, curVertex.vid,
								 prevVertex.tableoid, prevVertex.vid, relInfo->prop_map);

						if (SPI_execute(queryCmd, false, 0) != SPI_OK_INSERT_RETURNING)
							elog(ERROR, "SPI_execute failed: %s", queryCmd);
					}
				}

				prevVertex = curVertex;
			}
				break;
			case T_CypherRel:
			{
				CypherRel	   *crel = (CypherRel*)graphElem;
				char		   *reltype = crel->types;

				Assert( crel->types != NULL && list_length(crel->types) == 1);

				relInfo = crel;
			}
				break;
			default:
				elog(ERROR, "unrecognized node type: %d", nodeTag(graphElem));
				break;

		}
	}

	return NULL;
}

/* ----------------------------------------------------------------
 *		ExecInitCypherCreate
 *		Initialize the CypherCreate State
 * ---------------------------------------------------------------- */
CypherCreateState *
ExecInitCypherCreate(CypherCreate *node, EState *estate, int eflags)
{
	CypherCreateState *ccstate;
	CmdType		operation = node->operation;
	int			nplans = list_length(node->subplans);
	Plan	   *subplan;
	TupleDesc	tupDesc;
	ListCell   *l;
	int			i;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	ccstate = makeNode(CypherCreateState);
	ccstate->ps.plan = (Plan *) node;
	ccstate->ps.state = estate;

	ccstate->operation = operation;
	ccstate->canSetTag = node->canSetTag;
	ccstate->cc_done = false;

	/* Currently, below code is not used. */
	ccstate->cc_plans = (PlanState **) palloc0(sizeof(PlanState *) * nplans);
	ccstate->cc_nplans = nplans;

	i = 0;
	foreach(l, node->subplans)
	{
		subplan = (Plan *) lfirst(l);

		/* Now init the plan for this result rel */
		ccstate->cc_plans[i] = ExecInitNode(subplan, estate, eflags);

		i++;
	}

	/*
	 * We still must construct a dummy result tuple type, because InitPlan
	 * expects one (maybe should change that?).
	 */
	tupDesc = ExecTypeFromTL(NIL, false);
	ExecInitResultTupleSlot(estate, &ccstate->ps);
	ExecAssignResultType(&ccstate->ps, tupDesc);

	ccstate->ps.ps_ExprContext = NULL;

	return ccstate;
}


/* ----------------------------------------------------------------
 *	   ExecCypherCreate
 *
 *		create graph patterns
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecCypherCreate(CypherCreateState *node)
{
	CypherCreate   *plan = (CypherCreate*)node->ps.plan;
	ListCell	   *l;

	/* Open SPI context. */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/* TODO : fetch MATCHed graph elements from Sub-plan */

	foreach(l, plan->graphPatterns)
	{
		CypherPattern  *pattern = (CypherPattern *)lfirst(l);

		createPattern(pattern);
	}

	/* Close SPI context. */
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	return NULL;
}


/* ----------------------------------------------------------------
 *		ExecCypherCreate
 *
 *		Shuts down the plan.
 *
 *		Returns nothing of interest.
 * ----------------------------------------------------------------
 */
void
ExecEndCypherCreate(CypherCreateState *node)
{
	int			i;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->ps.ps_ResultTupleSlot);

	/*
	 * shut down subplans
	 */
	for (i = 0; i < node->cc_nplans; i++)
		ExecEndNode(node->cc_plans[i]);
}
