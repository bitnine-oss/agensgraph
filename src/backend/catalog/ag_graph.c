/*
 * ag_graph.c
 *	  routines to support manipulation of the ag_graph relation
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/catalog/ag_graph.c
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/parallel.h"
#include "access/xact.h"
#include "catalog/ag_graph.h"
#include "catalog/ag_graph_fn.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_namespace.h"
#include "commands/schemacmds.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/* a global variable for the GUC variable */
char *graph_path = NULL;
bool enableGraphDML = false;

/* check_hook: validate new graph_path value */
bool
check_graph_path(char **newval, void **extra, GucSource source)
{
	if (IsTransactionState() && !InitializingParallelWorker)
	{
		if (!OidIsValid(get_graphname_oid(*newval)))
		{
			GUC_check_errdetail("graph \"%s\" does not exist.", *newval);
			return false;
		}
	}

	return true;
}

char *
get_graph_path(bool lookup_cache)
{
	if (graph_path == NULL || strlen(graph_path) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_SCHEMA_NAME),
				 errmsg("graph_path is NULL"),
				 errhint("Use SET graph_path")));

	if (lookup_cache && !OidIsValid(get_graphname_oid(graph_path)))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("current graph_path \"%s\" is invalid", graph_path),
				 errhint("Use CREATE GRAPH")));

	return graph_path;
}

Oid
get_graph_path_oid(void)
{
	Oid graphoid;

	if (graph_path == NULL || strlen(graph_path) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_SCHEMA_NAME),
				 errmsg("graph_path is NULL"),
				 errhint("Use SET graph_path")));

	graphoid = get_graphname_oid(graph_path);
	if (!OidIsValid(graphoid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("current graph_path \"%s\" is invalid", graph_path),
				 errhint("Use SET graph_path")));

	return graphoid;
}

/* Create a graph (schema) with the name and owner OID. */
Oid
GraphCreate(CreateGraphStmt *stmt, const char *queryString,
			int stmt_location, int stmt_len)
{
	const char *graphName = stmt->graphname;
	CreateSchemaStmt *schemaStmt;
	Oid			schemaoid;
	Datum		values[Natts_ag_graph];
	bool		isnull[Natts_ag_graph];
	NameData	gname;
	Relation	graphdesc;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Oid			graphoid;
	ObjectAddress graphobj;
	ObjectAddress schemaobj;
	int			i;

	AssertArg(graphName != NULL);

	if (OidIsValid(get_graphname_oid(graphName)))
	{
		if (stmt->if_not_exists)
		{
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_SCHEMA),
					 errmsg("graph \"%s\" already exists, skipping",
							graphName)));

			return InvalidOid;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_SCHEMA),
					 errmsg("graph \"%s\" already exists", graphName)));
		}
	}

	/* create a schema as a graph */

	schemaStmt = makeNode(CreateSchemaStmt);
	schemaStmt->schemaname = stmt->graphname;
	schemaStmt->authrole = stmt->authrole;
	schemaStmt->if_not_exists = stmt->if_not_exists;
	schemaStmt->schemaElts = NIL;

	schemaoid = CreateSchemaCommand(schemaStmt, queryString,
									stmt_location, stmt_len);

	/* initialize nulls and values */
	for (i = 0; i < Natts_ag_graph; i++)
	{
		values[i] = (Datum) NULL;
		isnull[i] = false;
	}
	namestrcpy(&gname, graphName);
	values[Anum_ag_graph_graphname - 1] = NameGetDatum(&gname);
	values[Anum_ag_graph_nspid - 1] = ObjectIdGetDatum(schemaoid);

	graphdesc = heap_open(GraphRelationId, RowExclusiveLock);
	tupDesc = graphdesc->rd_att;

	tup = heap_form_tuple(tupDesc, values, isnull);

	graphoid = CatalogTupleInsert(graphdesc, tup);
	Assert(OidIsValid(graphoid));

	heap_close(graphdesc, RowExclusiveLock);

	graphobj.classId = GraphRelationId;
	graphobj.objectId = graphoid;
	graphobj.objectSubId = 0;
	schemaobj.classId = NamespaceRelationId;
	schemaobj.objectId = schemaoid;
	schemaobj.objectSubId = 0;

	/*
	 * Register dependency from the schema to the graph,
	 * so that the schema will be deleted if the graph is.
	 */
	recordDependencyOn(&schemaobj, &graphobj, DEPENDENCY_INTERNAL);

	return graphoid;
}
