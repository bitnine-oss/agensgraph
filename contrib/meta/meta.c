#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"

extern GraphWriteStats graphWriteStats;

PG_MODULE_MAGIC;

/*
 * Function to return a row containing the columns for the respective values
 * of insertVertex, insertEdge, deleteVertex, deleteEdge, and updateProperty.
 * These values represent the status of the last Cypher graph write (insert,
 * delete, and/or update properties). They are only reset prior to a Cypher
 * graph write.
 */
PG_FUNCTION_INFO_V1(get_last_graph_write_stats);
Datum
get_last_graph_write_stats(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(5);

		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "insertedvertices",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "insertededges",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "deletedvertices",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "deletededges",
						   INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "updatedproperties",
						   INT8OID, -1, 0);

		funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < 1)
	{
		Datum	   *dvalues;
		bool	   *nulls;
		HeapTuple	tuple;
		TupleDesc	tupdesc = funcctx->attinmeta->tupdesc;

		dvalues = (Datum *) palloc(sizeof(Datum) * 5);
		nulls = (bool *) palloc(sizeof(bool) * 5);

		memset(dvalues, 0, sizeof(Datum) * 5);
		memset(nulls, false, sizeof(bool) * 5);

		dvalues[0] = Int64GetDatum(graphWriteStats.insertVertex);
		dvalues[1] = Int64GetDatum(graphWriteStats.insertEdge);
		dvalues[2] = Int64GetDatum(graphWriteStats.deleteVertex);
		dvalues[3] = Int64GetDatum(graphWriteStats.deleteEdge);
		dvalues[4] = Int64GetDatum(graphWriteStats.updateProperty);

		tuple = heap_form_tuple(tupdesc, dvalues, nulls);

		pfree(dvalues);
		pfree(nulls);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}

	SRF_RETURN_DONE(funcctx);
}
