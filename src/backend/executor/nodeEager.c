/*
 * nodeEager.c
 *
 * Copyright (c) 2017 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeEager.c
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "executor/execdebug.h"
#include "executor/nodeEager.h"
#include "miscadmin.h"
#include "utils/arrayaccess.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/tuplestore.h"

/* hash entry */
typedef struct ModifiedObjEntry
{
	Graphid	key;
	Datum	properties;
} ModifiedObjEntry;


static void setSlotAttrByAttnum(TupleTableSlot *slot,
								 Datum value, int attnum);
static void enterSetPropTable(EagerState *node, TupleTableSlot *slot, int idx);
static void enterDelPropTable(EagerState *node, TupleTableSlot *slot, int idx);
static Datum getVertexFinalPropMap(EagerState *node,
								   Datum origin, Graphid gid);
static Datum getEdgeFinalPropMap(EagerState *node, Datum origin, Graphid gid);
static List *getGidListInPath(Datum graphpath);
static Datum getPathFinalPropMap(EagerState *node, Datum origin);
/* ----------------------------------------------------------------
 *		ExecEager
 *
 *		Eagers tuples from the outer subtree of the node using tupleEager,
 *		which saves the results in a temporary file or memory. After the
 *		initial call, returns a tuple from the file with each call.
 *
 *		Conditions:
 *		  -- none.
 *
 *		Initial States:
 *		  -- the outer child is prepared to return the first tuple.
 * ----------------------------------------------------------------
 */
TupleTableSlot *
ExecEager(EagerState *node)
{
	EState	   *estate;
	ScanDirection dir;
	TupleTableSlot *result;

	estate = node->ss.ps.state;
	dir = estate->es_direction;

	/*
	 * If first time through, read all tuples from outer plan and pass them to
	 * tuplestore.c. Subsequent calls just fetch tuples from tuplestore.
	 */
	if (!node->child_done)
	{
		PlanState  *outerNode;
		TupleTableSlot *slot;

		/*
		 * Want to scan subplan in the forward direction.
		 */
		estate->es_direction = ForwardScanDirection;

		/*
		 * Initialize tuplestore module.
		 */
		outerNode = outerPlanState(node);
		node->tuplestorestate = tuplestore_begin_heap(false, false, work_mem);

		/*
		 * Scan the subplan and feed all the tuples to tuplestore.
		 */
		for (;;)
		{
			ListCell *lc;

			slot = ExecProcNode(outerNode);

			if (TupIsNull(slot))
				break;

			foreach(lc, node->modifiedList)
			{
				int		idx = lfirst_int(lc);

				if (slot->tts_isnull[idx] != 0)
					continue;

				if (node->gwop == GWROP_SET)
					enterSetPropTable(node, slot, idx);
				else if (node->gwop == GWROP_DELETE)
					enterDelPropTable(node, slot, idx);
				else
					elog(ERROR, "Invalid operation type in ModifyGraph : %d",
						 node->gwop);
			}

			tuplestore_puttupleslot(node->tuplestorestate, slot);
		}

		/*
		 * restore to user specified direction
		 */
		estate->es_direction = dir;

		node->child_done = true;
	}

	result = node->ss.ps.ps_ResultTupleSlot;
	(void) tuplestore_gettupleslot(node->tuplestorestate,
								   ScanDirectionIsForward(dir), false, result);

	/* mark slot as containing a virtual tuple */
	if (!TupIsNull(result))
	{
		int i;
		int	natts = result->tts_tupleDescriptor->natts;

		slot_getallattrs(result);

		for (i = 0; i < natts; i++)
		{
			Graphid gid;
			Oid		type;
			Datum	elem;

			if (result->tts_isnull[i] != 0)
				continue;

			type = result->tts_tupleDescriptor->attrs[i]->atttypid;
			if (type == VERTEXOID)
			{
				gid = getVertexIdDatum(result->tts_values[i]);
				elem = getVertexFinalPropMap(node, result->tts_values[i], gid);

				setSlotAttrByAttnum(result, elem, i + 1);
			}
			else if (type == EDGEOID)
			{
				gid = getEdgeIdDatum(result->tts_values[i]);
				elem = getEdgeFinalPropMap(node, result->tts_values[i], gid);

				setSlotAttrByAttnum(result, elem, i + 1);
			}
			else if (type == GRAPHPATHOID)
			{
				elem = getPathFinalPropMap(node, result->tts_values[i]);

				setSlotAttrByAttnum(result, elem, i + 1);
			}
			else
				elog(ERROR, "Invalid graph element type %d.", type);
		}
	}

	return result;
}

/* ----------------------------------------------------------------
 *		ExecInitEager
 *
 *		Creates the run-time state information for the Eager node
 *		produced by the planner and initializes its outer subtree.
 * ----------------------------------------------------------------
 */
EagerState *
ExecInitEager(Eager *node, EState *estate, int eflags)
{
	EagerState  *Eagerstate;
	HASHCTL ctl;

	/*
	 * create state structure
	 */
	Eagerstate = makeNode(EagerState);
	Eagerstate->ss.ps.plan = (Plan *) node;
	Eagerstate->ss.ps.state = estate;
	Eagerstate->child_done = false;
	Eagerstate->modifiedList = node->modifylist;
	Eagerstate->gwop = node->gwop;

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &Eagerstate->ss.ps);
	ExecInitScanTupleSlot(estate, &Eagerstate->ss);

	/*
	 * initialize child nodes
	 *
	 * We shield the child node from the need to support REWIND, BACKWARD, or
	 * MARK/RESTORE.
	 */
	eflags &= ~(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK);

	outerPlanState(Eagerstate) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * initialize tuple type.  no need to initialize projection info because
	 * this node doesn't do projections.
	 */
	ExecAssignResultTypeFromTL(&Eagerstate->ss.ps);
	ExecAssignScanTypeFromOuterPlan(&Eagerstate->ss);
	Eagerstate->ss.ps.ps_ProjInfo = NULL;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Graphid);
	ctl.entrysize = sizeof(ModifiedObjEntry);
	ctl.hcxt = CurrentMemoryContext;

	Eagerstate->modifiedObject =
					hash_create("Eager modified object table", 128, &ctl,
								HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	return Eagerstate;
}

/* ----------------------------------------------------------------
 *		ExecEndEager(node)
 * ----------------------------------------------------------------
 */
void
ExecEndEager(EagerState *node)
{
	hash_destroy(node->modifiedObject);

	node->modifiedObject = NULL;

	/*
	 * clean out the tuple table
	 * must drop pointer to Eager result tuple
	 */
	ExecClearTuple(node->ss.ss_ScanTupleSlot);
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	/*
	 * Release tupleEager resources
	 */
	if (node->tuplestorestate != NULL)
		tuplestore_end(node->tuplestorestate);
	node->tuplestorestate = NULL;

	/*
	 * shut down the subplan
	 */
	ExecEndNode(outerPlanState(node));
}

void
ExecReScanEager(EagerState *node)
{
	if (!node->child_done)
		return;

	/* must drop pointer to Eager result tuple */
	ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);

	/*
	 * If subnode is to be rescanned then we forget previous Eager results; we
	 * have to re-read the subplan.
	 */
	node->child_done = false;
	tuplestore_end(node->tuplestorestate);
	node->tuplestorestate = NULL;
}

static void
setSlotAttrByAttnum(TupleTableSlot *slot, Datum value, int attnum)
{
	AssertArg(attnum > 0 && attnum <= slot->tts_tupleDescriptor->natts);

	slot->tts_values[attnum - 1] = value;
	slot->tts_isnull[attnum - 1] = (value == (Datum) NULL) ? true : false;
}

static void
enterSetPropTable(EagerState *node, TupleTableSlot *slot, int idx)
{
	Oid		type;
	Datum	gid;
	Datum	properties;
	ModifiedObjEntry *entry;

	type = slot->tts_tupleDescriptor->attrs[idx]->atttypid;
	if (type == VERTEXOID)
	{
		gid = getVertexIdDatum(slot->tts_values[idx]);
		properties = getVertexPropDatum(slot->tts_values[idx]);
	}
	else if (type == EDGEOID)
	{
		gid = getEdgeIdDatum(slot->tts_values[idx]);
		properties = getEdgePropDatum(slot->tts_values[idx]);
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("expected node or relationship, but %s",
						format_type_be(type))));

	entry = hash_search(node->modifiedObject, (void *) &gid, HASH_ENTER, NULL);
	entry->properties = datumCopy(properties, false, -1);
}

static void
enterDelPropTable(EagerState *node, TupleTableSlot *slot, int idx)
{
	Oid		type;
	Datum	gid;
	ModifiedObjEntry *entry;

	type = slot->tts_tupleDescriptor->attrs[idx]->atttypid;
	if (type == VERTEXOID)
	{
		gid = getVertexIdDatum(slot->tts_values[idx]);

		entry = hash_search(node->modifiedObject,
							(void *) &gid, HASH_ENTER, NULL);
		entry->properties = (Datum) NULL;
	}
	else if (type == EDGEOID)
	{
		gid = getEdgeIdDatum(slot->tts_values[idx]);

		entry = hash_search(node->modifiedObject,
							(void *) &gid, HASH_ENTER, NULL);
		entry->properties = (Datum) NULL;
	}
	else
	{
		List	 *gidlist;
		ListCell *lc;

		Assert(type == GRAPHPATHOID);

		gidlist = getGidListInPath(slot->tts_values[idx]);
		foreach(lc, gidlist)
		{
			gid = (Datum) lfirst(lc);

			entry = hash_search(node->modifiedObject,
								(void *) &gid, HASH_ENTER, NULL);
			entry->properties = (Datum) NULL;
		}
	}
}

static Datum
getVertexFinalPropMap(EagerState *node, Datum origin, Graphid gid)
{
	ModifiedObjEntry *entry;
	Datum result;

	entry = hash_search(node->modifiedObject, (void *) &gid, HASH_FIND, NULL);

	if (entry == NULL)	/* un-modified vertex */
	{
		result = origin;
	}
	else
	{
		if (entry->properties == (Datum) NULL)	/* DELETEd vertex */
			result = (Datum ) NULL;
		else
			result = makeGraphVertexDatum(gid, entry->properties);
	}

	return result;
}

static Datum
getEdgeFinalPropMap(EagerState *node, Datum origin, Graphid gid)
{
	ModifiedObjEntry *entry;
	Datum result;

	entry = hash_search(node->modifiedObject, (void *) &gid, HASH_FIND, NULL);

	if (entry == NULL)	/* un-modified edge */
	{
		result = origin;
	}
	else
	{
		if (entry->properties == (Datum) NULL)	/* DELETEd edge */
			result = (Datum ) NULL;
		else
		{
			Datum start, end;

			start = getEdgeStartDatum(origin);
			end = getEdgeEndDatum(origin);

			result = makeGraphEdgeDatum(gid, start, end, entry->properties);
		}
	}

	return result;
}

static List *
getGidListInPath(Datum graphpath)
{
	Datum		vertices_datum;
	Datum		edges_datum;
	int			nvertices;
	int			nedges;
	AnyArrayType *vertices;
	AnyArrayType *edges;
	array_iter	it;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	Datum		value;
	bool		null;
	int			i;
	List	   *result = NIL;

	getGraphpathArrays(graphpath, &vertices_datum, &edges_datum);

	vertices = DatumGetAnyArray(vertices_datum);
	edges = DatumGetAnyArray(edges_datum);

	nvertices = ArrayGetNItems(AARR_NDIM(vertices), AARR_DIMS(vertices));
	nedges = ArrayGetNItems(AARR_NDIM(edges), AARR_DIMS(edges));
	Assert(nvertices == nedges + 1);

	get_typlenbyvalalign(AARR_ELEMTYPE(vertices), &typlen,
						 &typbyval, &typalign);
	array_iter_setup(&it, vertices);
	for (i = 0; i < nvertices; i++)
	{
		value = array_iter_next(&it, &null, i, typlen,typbyval, typalign);
		Assert(!null);

		result = lappend(result, DatumGetPointer(getVertexIdDatum(value)));
	}

	get_typlenbyvalalign(AARR_ELEMTYPE(edges), &typlen, &typbyval, &typalign);
	array_iter_setup(&it, edges);
	for (i = 0; i < nedges; i++)
	{
		value = array_iter_next(&it, &null, i, typlen, typbyval, typalign);
		Assert(!null);

		result = lappend(result, DatumGetPointer(getEdgeIdDatum(value)));
	}

	return result;
}

static Datum
getPathFinalPropMap(EagerState *node, Datum origin)
{
	Datum		vertices_datum;
	Datum		edges_datum;
	int			nvertices;
	int			nedges;
	AnyArrayType *arr_vertices;
	AnyArrayType *arr_edges;
	Datum	   *vertices;
	Datum	   *edges;
	array_iter	it;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	Datum		value;
	bool		null;
	int			i;
	bool		modified = false;
	Datum		result;

	getGraphpathArrays(origin, &vertices_datum, &edges_datum);

	arr_vertices = DatumGetAnyArray(vertices_datum);
	arr_edges = DatumGetAnyArray(edges_datum);

	nvertices = ArrayGetNItems(AARR_NDIM(arr_vertices),
							   AARR_DIMS(arr_vertices));
	nedges = ArrayGetNItems(AARR_NDIM(arr_edges), AARR_DIMS(arr_edges));
	Assert(nvertices == nedges + 1);

	vertices = palloc(nvertices * sizeof(Datum));
	edges = palloc(nedges * sizeof(Datum));

	get_typlenbyvalalign(AARR_ELEMTYPE(arr_vertices), &typlen,
						 &typbyval, &typalign);
	array_iter_setup(&it, arr_vertices);
	for (i = 0; i < nvertices; i++)
	{
		Graphid	gid;
		Datum	vertex;

		value = array_iter_next(&it, &null, i, typlen,typbyval, typalign);
		Assert(!null);

		gid = getVertexIdDatum(value);
		vertex = getVertexFinalPropMap(node, value, gid);

		if (vertex == (Datum) NULL)
			elog(ERROR, "cannot modify the element of graphpath.");
		else if (vertex == value)
			vertices[i] = value;
		else
		{
			modified = true;
			vertices[i] = vertex;
		}
	}

	get_typlenbyvalalign(AARR_ELEMTYPE(arr_edges), &typlen, &typbyval, &typalign);
	array_iter_setup(&it, arr_edges);
	for (i = 0; i < nedges; i++)
	{
		Graphid	gid;
		Datum	edge;

		value = array_iter_next(&it, &null, i, typlen, typbyval, typalign);
		Assert(!null);

		gid = getEdgeIdDatum(value);
		edge = getEdgeFinalPropMap(node, value, gid);

		if (edge == (Datum) NULL)
			elog(ERROR, "cannot modify the element of graphpath.");
		else if (edge == value)
			edges[i] = value;
		else
		{
			modified = true;
			edges[i] = edge;
		}
	}

	if (modified == true)
		result = makeGraphpathDatum(vertices, nvertices, edges, nedges);
	else
		result = origin;

	pfree(vertices);
	pfree(edges);

	return result;
}
