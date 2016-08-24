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
#include "catalog/ag_graph.h"
#include "catalog/ag_graph_fn.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_namespace.h"
#include "commands/schemacmds.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/* Create a graph (schema) with the name and owner OID. */
Oid
GraphCreate(CreateGraphStmt *stmt, const char *queryString)
{
	const char *graphName = stmt->graphname;
	Oid			owner_uid;
	Datum		values[Natts_ag_graph];
	bool		isnull[Natts_ag_graph];
	NameData	gname;
	Relation	graphdesc;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Oid			graphoid;
	CreateSchemaStmt *schemaStmt;
	Oid			schemaoid;
	ObjectAddress graphobj;
	ObjectAddress schemaobj;
	int			i;

	AssertArg(graphName != NULL);

	if (stmt->authrole)
		owner_uid = get_rolespec_oid(stmt->authrole, false);
	else
		owner_uid = GetUserId();

	if (SearchSysCacheExists1(GRAPHNAME, PointerGetDatum(graphName)))
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

	/* initialize nulls and values */
	for (i = 0; i < Natts_ag_graph; i++)
	{
		values[i] = (Datum) NULL;
		isnull[i] = false;
	}
	namestrcpy(&gname, graphName);
	values[Anum_ag_graph_graphname - 1] = NameGetDatum(&gname);
	values[Anum_ag_graph_graphowner - 1] = ObjectIdGetDatum(owner_uid);

	graphdesc = heap_open(GraphRelationId, RowExclusiveLock);
	tupDesc = graphdesc->rd_att;

	tup = heap_form_tuple(tupDesc, values, isnull);

	graphoid = simple_heap_insert(graphdesc, tup);
	Assert(OidIsValid(graphoid));

	CatalogUpdateIndexes(graphdesc, tup);

	heap_close(graphdesc, RowExclusiveLock);

	schemaStmt = makeNode(CreateSchemaStmt);
	schemaStmt->schemaname = stmt->graphname;
	schemaStmt->authrole = stmt->authrole;
	schemaStmt->if_not_exists = stmt->if_not_exists;
	schemaStmt->schemaElts = NIL;

	/* create a schema as a graph */
	schemaoid = CreateSchemaCommand(schemaStmt, queryString);

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
