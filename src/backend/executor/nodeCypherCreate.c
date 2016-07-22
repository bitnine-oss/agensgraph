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
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/nodeCypherCreate.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/graph.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "utils/typcache.h"
#include "funcapi.h"


#define SQLCMD_LENGTH		(NAMEDATALEN + 200)

#define SQLCMD_CREATE_VTX	"INSERT INTO graph.%s VALUES (DEFAULT, $1)" \
							"RETURNING tableoid, *"
#define CREATE_VTX_ARGNUM	1
#define VERTEX_ATTR_NUM		3

#define SQLCMD_CREATE_EDGE	"INSERT INTO graph.%s VALUES " \
							"(DEFAULT, $1, $2, $3, $4, $5) " \
							"RETURNING tableoid, *"
#define CREATE_EDGE_ARGNUM	5

#define AG_PROP_TO_JSONB(a)	((a) ? DirectFunctionCall1(jsonb_in, \
													   CStringGetDatum((a))): \
								   jsonb_build_object_noargs(NULL))

/*
 * This structure only is used for creating edge
 */
typedef struct vertexInfo {
	int64	vid;
	Oid		tableoid;
} vertexInfo;


static Datum createVertex(EState *estate, CypherNode *cnode,
						  vertexInfo *resVtx, TupleTableSlot *slot,
						  bool inNamedpath);
static Datum createEdge(EState *estate, CypherRel *crel,
						vertexInfo *sourceVtx, vertexInfo *destVtx,
						TupleTableSlot *slot, bool inNamedpath);
static TupleTableSlot *createPattern(EState *estate, CypherPath *path,
									 TupleTableSlot *slot);
static Datum getVertexInfo(TupleTableSlot *slot, CypherNode *node,
						   vertexInfo *vInfo);
static void getVertexValues(HeapTupleHeader vertex, Datum *values,
							bool *isnull);
static Datum duplicateNewtuple(EState *estate, HeapTuple tuple, Oid tupType);
static void replaceTupleSlotByVariable(char *varname, Datum tuple,
									   TupleTableSlot *slot);
static Datum* makeDatumList(EState *estate, int listlen);
static Datum makeArrayType(Datum *arrElem, int nElem, Oid typeoid);
static Datum makeGraphPath(EState *estate, int pathlen,
						   Datum *vtxArr, int vtxlen,
						   Datum *edgeArr, int edgelen);


/* ----------------------------------------------------------------
 *		ExecInitCypherCreate
 *		Initialize the CypherCreate State
 * ---------------------------------------------------------------- */
CypherCreateState *
ExecInitCypherCreate(CypherCreate *node, EState *estate, int eflags)
{
	CypherCreateState  *ccstate;
	Plan	   *subplan = node->subplan;
	TupleDesc	tupDesc;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));
	Assert(node->operation == CMD_CYPHERCREATE);

	/*
	 * create state structure
	 */
	ccstate = makeNode(CypherCreateState);
	ccstate->ps.plan = (Plan *) node;
	ccstate->ps.state = estate;
	ccstate->ps.ps_ExprContext = NULL;

	ccstate->operation = node->operation ;

	/* Now init the plan for this subplanl */
	ccstate->cc_plan = ExecInitNode(subplan, estate, eflags);

	/*
	 * We still must construct a dummy result tuple type, because InitPlan
	 * expects one (maybe should change that?).
	 */
	tupDesc = ExecTypeFromTL(NIL, false);
	ExecInitResultTupleSlot(estate, &ccstate->ps);
	ExecAssignResultType(&ccstate->ps, tupDesc);

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
	EState		   *estate = node->ps.state;
	CypherCreate   *plan = (CypherCreate *) node->ps.plan;
	ListCell	   *l;
	TupleTableSlot *slot;
	PlanState	   *subplanstate;

	/* Preload local variables */
	subplanstate = node->cc_plan;

	for (;;)
	{
		slot = ExecProcNode(subplanstate);

		if (TupIsNull(slot))
		{
			break;
		}

		foreach(l, plan->graphPatterns)
		{
			CypherPath  *path = (CypherPath *) lfirst(l);

			slot = createPattern(estate, path, slot);
		}

		return slot;
	}

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
	ExecEndNode(node->cc_plan);
}

/*
 * create a graph pattern
 */
static TupleTableSlot *
createPattern(EState *estate, CypherPath *path, TupleTableSlot * slot)
{
	ListCell	   *lc;
	vertexInfo		currVertex;
	vertexInfo		prevVertex;
	CypherRel	   *relInfo = NULL;
	bool			isNodeOfPrevElem = false;
	char		   *pathname = getCypherName(path->variable);
	Datum		   *vertices = NULL;
	Datum		   *edges = NULL;
	bool			isNamedpath = path->variable != NULL;
	int				vtxIdx;
	int				edgeIdx;
	int 			pathlen;

	/* Open SPI context. */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	if (isNamedpath)
	{
		pathlen = list_length(path->chain);
		Assert(pathlen % 2 == 1);

		/*
		 * vertex must have one more than edge in a path
		 */
		vertices = makeDatumList(estate, (pathlen / 2) + 1);
		edges	 = makeDatumList(estate, pathlen / 2);

		vtxIdx = edgeIdx = 0;
	}

	foreach(lc, path->chain)
	{
		Node *graphElem = lfirst(lc);

		switch (nodeTag(graphElem))
		{
			case T_CypherNode:
			{
				CypherNode	*node = (CypherNode *) graphElem;
				Datum  		 vtx;

				Assert(isNodeOfPrevElem == false);

				if (node->needCreation)
				{
					vtx = createVertex(estate, node, &currVertex,
									   slot, isNamedpath);
				}
				else
				{
					/* reference node must be empty */
					Assert(getCypherName(node->label) == NULL &&
						   node->prop_map == NULL);

					vtx = getVertexInfo(slot, node, &currVertex);
				}

				if (isNamedpath)
					vertices[vtxIdx++] = vtx;

				if (relInfo)
				{
					Datum  edge = (Datum)NULL;

					if (relInfo->direction == CYPHER_REL_DIR_LEFT)
					{
						edge = createEdge(estate, relInfo, &currVertex,
										  &prevVertex, slot, isNamedpath);
					}
					else
					{
						Assert(relInfo->direction == CYPHER_REL_DIR_RIGHT);

						edge = createEdge(estate, relInfo, &prevVertex,
										  &currVertex, slot, isNamedpath);
					}

					if (isNamedpath)
						edges[edgeIdx++] = edge;
				}

				prevVertex = currVertex;

				isNodeOfPrevElem = true;
			}
				break;
			case T_CypherRel:
			{
				CypherRel	   *crel = (CypherRel *) graphElem;

				Assert(isNodeOfPrevElem == true);
				Assert(crel->types != NULL && list_length(crel->types) == 1);

				relInfo = crel;

				isNodeOfPrevElem = false;
			}
				break;
			default:
				elog(ERROR, "unrecognized node type: %d", nodeTag(graphElem));
				break;
		}
	}

	/*
	 * Make a new path and replace it to tupletableslot
	 */
	if (isNamedpath)
	{
		Datum		newPath;

		newPath = makeGraphPath(estate, pathlen, vertices, vtxIdx, edges, edgeIdx);

		replaceTupleSlotByVariable(pathname, newPath, slot);
	}

	/* Close SPI context. */
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	return slot;
}

static Datum
createVertex(EState *estate, CypherNode *cnode,
			 vertexInfo *resVtx, TupleTableSlot *slot, bool inNamedpath)
{
	char	queryCmd[SQLCMD_LENGTH];
	char   *vlabel;
	Datum	values[CREATE_VTX_ARGNUM];
	bool	isnull[CREATE_VTX_ARGNUM];
	Oid		argTypes[CREATE_VTX_ARGNUM] = {JSONBOID};
	Datum	newvtx = (Datum)NULL;

	Assert(CREATE_VTX_ARGNUM == 1);


	vlabel = getCypherName(cnode->label);
	if (vlabel == NULL)
		vlabel = "vertex";
	snprintf(queryCmd, SQLCMD_LENGTH, SQLCMD_CREATE_VTX, vlabel);

	/*
	 * building Jsonb object and get it.
	 */
	values[0] = AG_PROP_TO_JSONB(cnode->prop_map);

	if (SPI_execute_with_args(queryCmd, CREATE_VTX_ARGNUM, argTypes, values,
							  NULL, false, 0) != SPI_OK_INSERT_RETURNING)
	{
		elog(ERROR, "SPI_execute failed: %s", queryCmd);
	}

	if (SPI_processed != 1)
	{
		elog(ERROR, "SPI_execute : must be created only a vertex per SPI_exec");
	}
	else
	{
		HeapTuple 	tuple = SPI_tuptable->vals[0];
		TupleDesc 	tupDesc = SPI_tuptable->tupdesc;
		char	   *varname = getCypherName(cnode->variable);

		/************************************************************
		 * Get the attributes value
		 ************************************************************/
		Assert(SPI_fnumber(tupDesc, "tableoid") == 1 &&
			   SPI_fnumber(tupDesc, "id")		== 2 &&
			   SPI_fnumber(tupDesc, "prop_map") == 3 );

		resVtx->tableoid = DatumGetObjectId(
								SPI_getbinval(tuple, tupDesc, 1, isnull));
		resVtx->vid = DatumGetInt64(SPI_getbinval(tuple, tupDesc, 2, isnull));

		if (varname != NULL || inNamedpath)
			newvtx = duplicateNewtuple(estate, tuple, VERTEXOID);

		if (varname != NULL)
			replaceTupleSlotByVariable(varname, newvtx, slot);
	}

	return newvtx;
}

static Datum
createEdge(EState *estate, CypherRel *crel, vertexInfo *sourceVtx,
		   vertexInfo *destVtx, TupleTableSlot *slot, bool inNamedpath)
{
	char	queryCmd[SQLCMD_LENGTH];
	char   *reltype;
	Datum	newedge = (Datum)NULL;
	Datum	values[CREATE_EDGE_ARGNUM];
	Oid		argTypes[CREATE_EDGE_ARGNUM] = {INT8OID, OIDOID,
											INT8OID, OIDOID,
											JSONBOID};

	Assert(CREATE_EDGE_ARGNUM == 5);
	Assert(list_length(crel->types) == 1);

	reltype = getCypherName(linitial(crel->types));
	snprintf(queryCmd, SQLCMD_LENGTH, SQLCMD_CREATE_EDGE, reltype);

	values[0] = ObjectIdGetDatum(sourceVtx->tableoid);
	values[1] = Int64GetDatum(sourceVtx->vid);
	values[2] = ObjectIdGetDatum(destVtx->tableoid);
	values[3] = Int64GetDatum(destVtx->vid);
	values[4] = AG_PROP_TO_JSONB(crel->prop_map);

	if (SPI_execute_with_args(queryCmd, CREATE_EDGE_ARGNUM, argTypes, values,
							  NULL, false, 0) != SPI_OK_INSERT_RETURNING)
	{
		elog(ERROR, "SPI_execute failed: %s", queryCmd);
	}

	if (SPI_processed != 1)
	{
		elog(ERROR, "SPI_execute : must be created only an edge per SPI_exec");
	}
	else
	{
		HeapTuple 	tuple = SPI_tuptable->vals[0];
		TupleDesc 	tupDesc = SPI_tuptable->tupdesc;
		char	   *varname = getCypherName(crel->variable);

		/************************************************************
		 * Get the attributes value
		 ************************************************************/
		Assert(SPI_fnumber(tupDesc, "tableoid") == 1 &&
			   SPI_fnumber(tupDesc, "id")		== 2 &&
			   SPI_fnumber(tupDesc, "start_oid")== 3 &&
			   SPI_fnumber(tupDesc, "start_id") == 4 &&
			   SPI_fnumber(tupDesc, "end_oid")	== 5 &&
			   SPI_fnumber(tupDesc, "end_id")	== 6 &&
			   SPI_fnumber(tupDesc, "prop_map")	== 7 );

		if(varname != NULL || inNamedpath)
			newedge = duplicateNewtuple(estate, tuple, EDGEOID);

		if (varname != NULL)
			replaceTupleSlotByVariable(varname, newedge, slot);
	}

	return newedge;
}

static void
getVertexValues(HeapTupleHeader vertex, Datum *values, bool *isnull)
{
	Oid			tupType;
	TupleDesc	tupDesc;
	HeapTupleData tuple;

	tupType = HeapTupleHeaderGetTypeId(vertex);
	tupDesc = lookup_rowtype_tupdesc(tupType, -1);
	Assert(tupDesc->natts == 3);

	tuple.t_len = HeapTupleHeaderGetDatumLength(vertex);
	ItemPointerSetInvalid(&tuple.t_self);
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = vertex;

	heap_deform_tuple(&tuple, tupDesc, values, isnull);

	Assert(!isnull[0]);
	Assert(!isnull[1]);
	Assert(!isnull[2]);

	ReleaseTupleDesc(tupDesc);
}

static Datum
getVertexInfo(TupleTableSlot *slot, CypherNode *node, vertexInfo *vInfo)
{
	TupleDesc	tupleDesc = slot->tts_tupleDescriptor;
	int32		i;
	AttrNumber	attrno;
	HeapTupleHeader	tupleHdr;
	Datum		resVtx;
	Datum		values[VERTEX_ATTR_NUM];
	bool		isnull[VERTEX_ATTR_NUM];
	char	   *varname;

	varname = getCypherName(node->variable);

	attrno = InvalidAttrNumber;
	for (i = 0; i < tupleDesc->natts; i++)
	{
		if (namestrcmp(&(tupleDesc->attrs[i]->attname), varname) == 0 &&
			!tupleDesc->attrs[i]->attisdropped)
		{
			attrno = tupleDesc->attrs[i]->attnum;
			break;
		}
	}

	if (attrno == InvalidAttrNumber)
		elog(ERROR, "variable \"%s\" does not exist", varname);

	resVtx = slot->tts_values[attrno-1];
	tupleHdr = (HeapTupleHeader)DatumGetPointer(resVtx);

	getVertexValues(tupleHdr, values, isnull);

	vInfo->tableoid = DatumGetObjectId(values[0]);
	vInfo->vid		= DatumGetInt64(values[1]);

	return resVtx;
}

static Datum
duplicateNewtuple(EState *estate, HeapTuple tuple, Oid tupType)
{
	TupleDesc		tupDesc;
	MemoryContext	oldMemoryContext;
	Datum			newTuple;

	tupDesc = lookup_rowtype_tupdesc(tupType, -1);

	oldMemoryContext = MemoryContextSwitchTo(estate->es_query_cxt);

	newTuple = heap_copy_tuple_as_datum(tuple, tupDesc);

	MemoryContextSwitchTo(oldMemoryContext);

	ReleaseTupleDesc(tupDesc);

	return newTuple;
}

static void
replaceTupleSlotByVariable(char *varname, Datum tuple, TupleTableSlot *slot)
{
	TupleDesc		tupleDesc = slot->tts_tupleDescriptor;
	int				attrno;
	int				i;

	attrno = InvalidAttrNumber;
	for (i = 0; i < tupleDesc->natts; i++)
	{
		if (namestrcmp(&(tupleDesc->attrs[i]->attname), varname) == 0 &&
			!tupleDesc->attrs[i]->attisdropped)
		{
			attrno = tupleDesc->attrs[i]->attnum;
			break;
		}
	}

	if (attrno == InvalidAttrNumber)
		elog(ERROR, "variable \"%s\" does not exist", varname);

	slot->tts_values[attrno-1] = tuple;
}

static Datum*
makeDatumList(EState *estate, int listlen)
{
	MemoryContext	oldMemoryContext;
	Datum		   *result;

	if (listlen == 0)
		return NULL;

	oldMemoryContext = MemoryContextSwitchTo(estate->es_query_cxt);

	result = palloc(listlen * sizeof(Datum));

	MemoryContextSwitchTo(oldMemoryContext);

	return result;
}

static Datum
makeArrayType(Datum *arrElem, int nElem, Oid typeoid)
{
	int16		typlen;
	bool		typbyval;
	char		typalign;
	ArrayType  *resArray;

	get_typlenbyvalalign(typeoid, &typlen, &typbyval, &typalign);

	resArray = construct_array(arrElem, nElem,
							   typeoid, typlen, typbyval, typalign);

	return PointerGetDatum(resArray);
}

static Datum
makeGraphPath(EState *estate, int pathlen, Datum *vtxArr, int vtxlen,
		 Datum *edgeArr, int edgelen)
{
	TupleDesc	tupDesc;
	HeapTuple	newPath;
	Datum		vtxArray;
	Datum		edgeArray;
	Datum		values[2];	/* vertex and edge array */
	bool		isnull[2];	/* vertex and edge array */
	MemoryContext oldMemoryContext;

	Assert(vtxlen == (edgelen + 1) &&
		   pathlen == (vtxlen + edgelen));

	oldMemoryContext = MemoryContextSwitchTo(estate->es_query_cxt);

	tupDesc = lookup_rowtype_tupdesc(GRAPHPATHOID, -1);
	Assert(tupDesc->natts == 2);

	vtxArray  = makeArrayType(vtxArr, vtxlen, VERTEXOID);
	edgeArray = makeArrayType(edgeArr, edgelen, EDGEOID);

	values[0] = vtxArray;
	values[1] = edgeArray;

	isnull[0] = isnull[1] = false;

	newPath = heap_form_tuple(tupDesc, values, isnull);

	ReleaseTupleDesc(tupDesc);

	MemoryContextSwitchTo(oldMemoryContext);

	return HeapTupleGetDatum(newPath);
}
