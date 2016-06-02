/*-------------------------------------------------------------------------
 *
 * nodeCypherCreate.c
 *	  routines to handle CypherCreate nodes.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * TODO : add portions
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeCypherCreate.c
 *
 *-------------------------------------------------------------------------
 */

// TODO : add INTERFACE ROUTINES

/* INTERFACE ROUTINES
 *		ExecInitModifyTable - initialize the ModifyTable node
 *		ExecModifyTable		- retrieve the next tuple from the node
 *		ExecEndModifyTable	- shut down the ModifyTable node
 *		ExecReScanModifyTable - rescan the ModifyTable node
 *
 *	 NOTES
 *		Each ModifyTable node contains a list of one or more subplans,
 *		much like an Append node.  There is one subplan per result relation.
 *		The key reason for this is that in an inherited UPDATE command, each
 *		result relation could have a different schema (more or different
 *		columns) requiring a different plan tree to produce it.  In an
 *		inherited DELETE, all the subplans should produce the same output
 *		rowtype, but we might still find that different plans are appropriate
 *		for different child relations.
 *
 *		If the query specifies RETURNING, then the ModifyTable returns a
 *		RETURNING tuple after completing each row insert, update, or delete.
 *		It must be called again to continue the operation.  Without RETURNING,
 *		we just loop within the node until all the work is done, then
 *		return NULL.  This avoids useless call/return overhead.
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/nodeModifyTable.h"
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

#define SQLCMD_CREATE_VERTEX "INSERT INTO graph.%s VALUES (DEFAULT, '%s')" \
							 "RETURNING vid, tableoid"
#define SQLCMD_CREATE_EDGE   "INSERT INTO graph.%s VALUES (%d, %lld, %d, %lld, '%s')"

typedef struct vertexInfo {
	long long 	vid;
	Oid			tableoid;
} vertexInfo;


/*
 * TODO: Add Comment
 */
vertexInfo * getReturnedVertex( vertexInfo * vInfo )
{
	if (SPI_processed != 1)
	{
		elog(ERROR, "SPI_execute : must be created only a vertex");
	}
	else
	{
		HeapTuple 	tuple = SPI_tuptable->vals[0];
		TupleDesc 	tupDesc = SPI_tuptable->tupdesc;
		bool		isnull;

		/************************************************************
		 * Get the attributes value
		 ************************************************************/
		Assert(SPI_fnumber(tupDesc, "vid") == 1 && SPI_fnumber(tupDesc, "tableoid") == 2);

		vInfo->vid = (long long)SPI_getbinval(tuple, tupDesc, 1, &isnull);
		vInfo->tableoid = (Oid)SPI_getbinval(tuple, tupDesc, 2, &isnull);
	}

	return vInfo;
}


/*
 * create a graph pattern that is separated by comma in create clause.
 * TODO : add to process MATCHed node
 * TODO : Add comment
 */
TupleTableSlot * createPattern(CypherPattern *pattern)
{
	ListCell	   *graphElem;
	vertexInfo	 	curVertex;
	vertexInfo 		prevVertex;
	bool			NeedMakeRel = false;
	CypherRel	   *relInfo = NULL;

	foreach(graphElem, pattern->chain)
	{
		Node		   *n = lfirst(graphElem);
		char 	 		queryCmd[200];
		HeapTuple		tuple;
		TupleDesc		tupDesc;
		long long       vid;
		Oid				tableoid;
		int				i;

		switch (nodeTag(n))
		{
			case T_CypherNode:
			{
				CypherNode *cnode = (CypherNode*)n;
				char	   *vlabel= cnode->label ? cnode->label : "vertex";

				snprintf(queryCmd, 200, SQLCMD_CREATE_VERTEX,
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
						snprintf(queryCmd, 200, SQLCMD_CREATE_EDGE,
								 reltype, prevVertex.tableoid, prevVertex.vid,
								 curVertex.tableoid, curVertex.vid, relInfo->prop_map);

						if (SPI_execute(queryCmd, false, 0) != SPI_OK_INSERT)
							elog(ERROR, "SPI_execute failed: %s", queryCmd);
					}

					if (relInfo->direction == CYPHER_REL_DIR_RIGHT ||
						relInfo->direction == CYPHER_REL_DIR_NONE )
					{
						snprintf(queryCmd, 200, SQLCMD_CREATE_EDGE,
								 reltype, curVertex.tableoid, curVertex.vid,
								 prevVertex.tableoid, prevVertex.vid, relInfo->prop_map);

						if (SPI_execute(queryCmd, false, 0) != SPI_OK_INSERT)
							elog(ERROR, "SPI_execute failed: %s", queryCmd);
					}
				}

				prevVertex = curVertex;
			}
				break;
			case T_CypherRel:
			{
				CypherRel	   *crel = (CypherRel*)n;
				char		   *reltype = crel->types;

				Assert( crel->types != NULL &&
						list_length(crel->types) == 1);

				relInfo = crel;
			}
				break;
			default:
				elog(ERROR, "unrecognized node type: %d", nodeTag(n));
				break;

		}
	}

	return NULL;
}

/* ----------------------------------------------------------------
 *		ExecInitCypherCreate
 *		TODO : Add Comment
 * ----------------------------------------------------------------
 */
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
 *		TODO : add comments
 *		INSERT INTO vertex (properties) VALUES (?) RETURNING vid, tableoid;
 *		INSERT INTO edge (inoid, invid, outoid, outvid, properties) VALUES (?,?,?,?,?) RETURNING EID, tableoid;
 *
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

	foreach(l, plan->graphPatterns)
	{
		CypherPattern  *pattern = (CypherPattern *)lfirst(l);

		createPattern(pattern);

		/*
		 * TODO : 생성된 graph Element들의 변수가 이 후 clause에서 사용된다면
		 * 		  생성된 graph element 정보를 넘겨줘야 함.
		 */
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
	 * shut down subplanse
	 */
	for (i = 0; i < node->cc_nplans; i++)
		ExecEndNode(node->cc_plans[i]);
}
