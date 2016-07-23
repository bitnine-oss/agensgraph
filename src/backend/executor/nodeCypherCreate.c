/*
 * nodeCypherCreate.c
 *	  routines to handle CypherCreate nodes.
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeCypherCreate.c
 */

/*
 * INTERFACE ROUTINES
 *		ExecInitCypherCreate - initialize the CypherCreate node
 *		ExecCypherCreate     - create graph pattern
 *		ExecEndCypherCreate  - shut down the CypherCreate node
 *
 * NOTE: This file is written based on nodeModifyTable.c file.
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "executor/nodeCypherCreate.h"
#include "executor/spi.h"
#include "executor/tuptable.h"
#include "funcapi.h"
#include "parser/parse_utilcmd.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#define SQLCMD_BUFLEN			(NAMEDATALEN + 192)
#define SQLCMD_CREAT_VERTEX		"INSERT INTO graph.%s VALUES (DEFAULT, $1)" \
								"RETURNING tableoid, *"
#define SQLCMD_VERTEX_NPARAMS	1
#define SQLCMD_CREAT_EDGE		"INSERT INTO graph.%s VALUES " \
								"(DEFAULT, $1, $2, $3, $4, $5) " \
								"RETURNING tableoid, *"
#define SQLCMD_EDGE_NPARAMS		5

static Datum CStringGetJsonbDatum(char *str);

typedef struct ElemId {
	Oid			oid;
	int64		id;
} ElemId;

static Datum createVertex(EState *estate, CypherNode *node, ElemId *vid,
						  TupleTableSlot *slot, bool inPath);
static Datum createEdge(EState *estate, CypherRel *crel, ElemId start,
						ElemId end, TupleTableSlot *slot, bool inPath);
static TupleTableSlot *createPath(EState *estate, CypherPath *path,
								  TupleTableSlot *slot);
static Datum findVertex(TupleTableSlot *slot, CypherNode *node, ElemId *vid);
static AttrNumber findAttrInSlotByName(TupleTableSlot *slot, char *name);
static ElemId DatumGetVid(Datum vertex);
static Datum copyTupleAsDatum(EState *estate, HeapTuple tuple, Oid tupType);
static void setSlotValueByName(TupleTableSlot *slot, Datum value, char *name);
static Datum *makeDatumArray(EState *estate, int len);
static Datum makeArrayType(Datum *arrElem, int nElem, Oid typeoid);
static Datum makeGraphpath(EState *estate, Datum *vertices, int nvertices,
						   Datum *edges, int nedges);

/*
 * ExecInitCypherCreate
 *		Initialize the CypherCreate State
 */
CypherCreateState *
ExecInitCypherCreate(CypherCreate *node, EState *estate, int eflags)
{
	CypherCreateState *ccstate;
	TupleDesc tupDesc;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/* create state structure */
	ccstate = makeNode(CypherCreateState);
	ccstate->ps.plan = (Plan *) node;
	ccstate->ps.state = estate;
	ccstate->ps.ps_ExprContext = NULL;
	ccstate->operation = node->operation;
	ccstate->cc_plan = ExecInitNode(node->subplan, estate, eflags);

	tupDesc = ExecTypeFromTL(NIL, false);
	ExecInitResultTupleSlot(estate, &ccstate->ps);
	ExecAssignResultType(&ccstate->ps, tupDesc);

	return ccstate;
}

/*
 * ExecCypherCreate
 *		Create graph pattern
 */
TupleTableSlot *
ExecCypherCreate(CypherCreateState *node)
{
	CypherCreate *plan = (CypherCreate *) node->ps.plan;
	EState	   *estate = node->ps.state;
	PlanState  *subplanstate = node->cc_plan;

	for (;;)
	{
		TupleTableSlot *slot;
		ListCell *l;

		slot = ExecProcNode(subplanstate);

		if (TupIsNull(slot))
			break;

		/* create a pattern, accumulated paths `slot` has */
		foreach(l, plan->graphPattern)
		{
			CypherPath *path = (CypherPath *) lfirst(l);

			slot = createPath(estate, path, slot);
		}

		return slot;
	}

	return NULL;
}

/*
 * ExecCypherCreate
 *		Shuts down the plan
 */
void
ExecEndCypherCreate(CypherCreateState *node)
{
	ExecFreeExprContext(&node->ps);

	ExecClearTuple(node->ps.ps_ResultTupleSlot);

	ExecEndNode(node->cc_plan);
}

/* create a path and accumulate it to the given slot */
static TupleTableSlot *
createPath(EState *estate, CypherPath *path, TupleTableSlot *slot)
{
	char	   *pathname = getCypherName(path->variable);
	bool		out = (pathname != NULL);
	int			pathlen;
	Datum	   *vertices = NULL;
	Datum	   *edges = NULL;
	int			nvertices;
	int			nedges;
	ListCell   *le;
	ElemId		vid;
	ElemId		prevvid;
	CypherRel  *crel = NULL;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	if (out)
	{
		pathlen = list_length(path->chain);
		Assert(pathlen % 2 == 1);

		vertices = makeDatumArray(estate, (pathlen / 2) + 1);
		edges = makeDatumArray(estate, pathlen / 2);

		nvertices = 0;
		nedges = 0;
	}

	foreach(le, path->chain)
	{
		Node *elem = (Node *) lfirst(le);

		if (nodeTag(elem) == T_CypherNode)
		{
			CypherNode *node = (CypherNode *) elem;
			Datum		vertex;

			if (node->create)
				vertex = createVertex(estate, node, &vid, slot, out);
			else
				vertex = findVertex(slot, node, &vid);

			if (out)
				vertices[nvertices++] = vertex;

			if (crel != NULL)
			{
				Datum edge;

				if (crel->direction == CYPHER_REL_DIR_LEFT)
				{
					edge = createEdge(estate, crel, vid, prevvid, slot, out);
				}
				else
				{
					Assert(crel->direction == CYPHER_REL_DIR_RIGHT);

					edge = createEdge(estate, crel, prevvid, vid, slot, out);
				}

				if (out)
					edges[nedges++] = edge;
			}

			prevvid = vid;
		}
		else
		{
			Assert(nodeTag(elem) == T_CypherRel);

			crel = (CypherRel *) elem;
		}
	}

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	/* make a graphpath and set it to the slot */
	if (out)
	{
		Datum graphpath;

		Assert(nvertices == nedges + 1);
		Assert(pathlen == nvertices + nedges);

		graphpath = makeGraphpath(estate, vertices, nvertices, edges, nedges);

		setSlotValueByName(slot, graphpath, pathname);
	}

	return slot;
}

/*
 * createVertex - creates a vertex of a given node
 *
 * NOTE: This function returns a vertex if it must be in the result(`slot`).
 */
static Datum
createVertex(EState *estate, CypherNode *node, ElemId *vid,
			 TupleTableSlot *slot, bool inPath)
{
	char		sqlcmd[SQLCMD_BUFLEN];
	char	   *label;
	Datum		values[SQLCMD_VERTEX_NPARAMS];
	Oid			argTypes[SQLCMD_VERTEX_NPARAMS] = {JSONBOID};
	int			ret;
	TupleDesc	tupDesc;
	HeapTuple	tuple;
	char	   *varname;
	Datum		vertex = (Datum) NULL;

	label = getCypherName(node->label);
	if (label == NULL)
		label = AG_VERTEX;

	snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_CREAT_VERTEX, label);

	values[0] = CStringGetJsonbDatum(node->prop_map);

	ret = SPI_execute_with_args(sqlcmd, SQLCMD_VERTEX_NPARAMS, argTypes,
								values, NULL, false, 0);
	if (ret != SPI_OK_INSERT_RETURNING)
		elog(ERROR, "SPI_execute failed: %s", sqlcmd);
	if (SPI_processed != 1)
		elog(ERROR, "SPI_execute: only one vertex per execution must be created");

	tupDesc = SPI_tuptable->tupdesc;
	tuple = SPI_tuptable->vals[0];

	if (vid != NULL)
	{
		int			oid_attno;
		int			id_attno;
		bool		isnull = false;

		oid_attno = SPI_fnumber(tupDesc, "tableoid");
		Assert(oid_attno != SPI_ERROR_NOATTRIBUTE);

		id_attno = SPI_fnumber(tupDesc, AG_ELEM_ID);
		Assert(id_attno != SPI_ERROR_NOATTRIBUTE);

		vid->oid = DatumGetObjectId(SPI_getbinval(tuple, tupDesc, oid_attno,
												  &isnull));
		Assert(!isnull);

		vid->id = DatumGetInt64(SPI_getbinval(tuple, tupDesc, id_attno,
											  &isnull));
		Assert(!isnull);
	}

	varname = getCypherName(node->variable);

	/* if this vertex is in the result solely or in some paths, */
	if (varname != NULL || inPath)
		vertex = copyTupleAsDatum(estate, tuple, VERTEXOID);

	if (varname != NULL)
		setSlotValueByName(slot, vertex, varname);

	return vertex;
}

static Datum
createEdge(EState *estate, CypherRel *crel, ElemId start, ElemId end,
		   TupleTableSlot *slot, bool inPath)
{
	char		sqlcmd[SQLCMD_BUFLEN];
	char	   *reltype;
	Datum		values[SQLCMD_EDGE_NPARAMS];
	Oid			argTypes[SQLCMD_EDGE_NPARAMS] = {OIDOID, INT8OID,
												 OIDOID, INT8OID,
												 JSONBOID};
	int			ret;
	char	   *varname;
	Datum		edge = (Datum) NULL;

	reltype = getCypherName(linitial(crel->types));
	snprintf(sqlcmd, SQLCMD_BUFLEN, SQLCMD_CREAT_EDGE, reltype);

	values[0] = ObjectIdGetDatum(start.oid);
	values[1] = Int64GetDatum(start.id);
	values[2] = ObjectIdGetDatum(end.oid);
	values[3] = Int64GetDatum(end.id);
	values[4] = CStringGetJsonbDatum(crel->prop_map);

	ret = SPI_execute_with_args(sqlcmd, SQLCMD_EDGE_NPARAMS, argTypes, values,
								NULL, false, 0);
	if (ret != SPI_OK_INSERT_RETURNING)
		elog(ERROR, "SPI_execute failed: %s", sqlcmd);
	if (SPI_processed != 1)
		elog(ERROR, "SPI_execute: only one edge per execution must be created");

	varname = getCypherName(crel->variable);

	if (varname != NULL || inPath)
	{
		HeapTuple tuple = SPI_tuptable->vals[0];

		edge = copyTupleAsDatum(estate, tuple, EDGEOID);
	}

	if (varname != NULL)
		setSlotValueByName(slot, edge, varname);

	return edge;
}

static ElemId
DatumGetVid(Datum vertex)
{
	HeapTupleHeader	tuphdr;
	Oid			tupType;
	TupleDesc	tupDesc;
	HeapTupleData tuple;
	bool		isnull = false;
	ElemId		vid;

	tuphdr = DatumGetHeapTupleHeader(vertex);

	tupType = HeapTupleHeaderGetTypeId(tuphdr);
	Assert(tupType == VERTEXOID);

	tupDesc = lookup_rowtype_tupdesc(tupType, -1);
	Assert(tupDesc->natts == 3);	// TODO: use Natts_vertex

	tuple.t_len = HeapTupleHeaderGetDatumLength(tuphdr);
	ItemPointerSetInvalid(&tuple.t_self);
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = tuphdr;

	// TODO: use Anum_vertex_...
	vid.oid = DatumGetObjectId(heap_getattr(&tuple, 1, tupDesc, &isnull));
	Assert(!isnull);
	vid.id = DatumGetInt64(heap_getattr(&tuple, 2, tupDesc, &isnull));
	Assert(!isnull);

	ReleaseTupleDesc(tupDesc);

	return vid;
}

static Datum
findVertex(TupleTableSlot *slot, CypherNode *node, ElemId *vid)
{
	char	   *varname;
	AttrNumber	attno;
	Datum		vertex;

	varname = getCypherName(node->variable);

	attno = findAttrInSlotByName(slot, varname);

	vertex = slot->tts_values[attno - 1];

	if (vid != NULL)
		*vid = DatumGetVid(vertex);

	return vertex;
}

static AttrNumber
findAttrInSlotByName(TupleTableSlot *slot, char *name)
{
	TupleDesc	tupDesc = slot->tts_tupleDescriptor;
	int			i;

	for (i = 0; i < tupDesc->natts; i++)
	{
		if (namestrcmp(&(tupDesc->attrs[i]->attname), name) == 0 &&
			!tupDesc->attrs[i]->attisdropped)
			return tupDesc->attrs[i]->attnum;
	}

	elog(ERROR, "variable \"%s\" does not exist", name);
	return InvalidAttrNumber;
}

static Datum
copyTupleAsDatum(EState *estate, HeapTuple tuple, Oid tupType)
{
	TupleDesc		tupDesc;
	MemoryContext	oldMemoryContext;
	Datum			value;

	tupDesc = lookup_rowtype_tupdesc(tupType, -1);

	oldMemoryContext = MemoryContextSwitchTo(estate->es_query_cxt);

	value = heap_copy_tuple_as_datum(tuple, tupDesc);

	MemoryContextSwitchTo(oldMemoryContext);

	ReleaseTupleDesc(tupDesc);

	return value;
}

static void
setSlotValueByName(TupleTableSlot *slot, Datum value, char *name)
{
	AttrNumber attno;

	attno = findAttrInSlotByName(slot, name);

	slot->tts_values[attno - 1] = value;
}

static Datum *
makeDatumArray(EState *estate, int len)
{
	MemoryContext oldMemoryContext;
	Datum *result;

	if (len == 0)
		return NULL;

	oldMemoryContext = MemoryContextSwitchTo(estate->es_query_cxt);

	result = palloc(len * sizeof(Datum));

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
makeGraphpath(EState *estate, Datum *vertices, int nvertices, Datum *edges,
			  int nedges)
{
	MemoryContext oldMemoryContext;
	Datum		values[2];
	bool		isnull[2];
	TupleDesc	tupDesc;
	HeapTuple	graphpath;

	oldMemoryContext = MemoryContextSwitchTo(estate->es_query_cxt);

	// TODO: use Anum_graphpath_...
	values[0] = makeArrayType(vertices, nvertices, VERTEXOID);
	values[1] = makeArrayType(edges, nedges, EDGEOID);
	isnull[0] = false;
	isnull[1] = false;

	tupDesc = lookup_rowtype_tupdesc(GRAPHPATHOID, -1);
	Assert(tupDesc->natts == 2);	// TODO: use Natts_graphpath

	graphpath = heap_form_tuple(tupDesc, values, isnull);

	ReleaseTupleDesc(tupDesc);

	MemoryContextSwitchTo(oldMemoryContext);

	return HeapTupleGetDatum(graphpath);
}

static Datum
CStringGetJsonbDatum(char *str)
{
	if (str == NULL)
		return jsonb_build_object_noargs(NULL);
	else
		return DirectFunctionCall1(jsonb_in, CStringGetDatum(str));
}
