/*-------------------------------------------------------------------------
 *
 * ag_graph.c
 *	  routines to support manipulation of the ag_graph relation
 *
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/ag_graph.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/ag_graph.h"
#include "catalog/ag_graph_fn.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_namespace.h"
#include "commands/schemacmds.h"
#include "miscadmin.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"

char	*graph_path = NULL;

/* ----------------
 * GraphCreate
 *
 * Create a graph (schema) with the name and owner OID.
 * ---------------
 */
Oid
GraphCreate(CreateGraphStmt *stmt, const char *queryString)
{
	Relation	graphdesc;
	HeapTuple	tup;
	Oid			graphoid;
	Oid			schemaoid;
	bool		nulls[Natts_ag_graph];
	Datum		values[Natts_ag_graph];
	NameData	gname;
	TupleDesc	tupDesc;
	int			i;
	const char *graphName = stmt->graphname;
	Oid			owner_uid;
	CreateSchemaStmt *schema;
	ObjectAddress addr_graph;
	ObjectAddress addr_schema;

	if (stmt->authrole)
		owner_uid = get_rolespec_oid(stmt->authrole, false);
	else
		owner_uid = GetUserId();

	if (!graphName)
	{
		HeapTuple	tuple;

		tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(owner_uid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for role %u", owner_uid);
		graphName =
			pstrdup(NameStr(((Form_pg_authid) GETSTRUCT(tuple))->rolname));
		ReleaseSysCache(tuple);
	}

	/* sanity checks */
	if (!graphName)
		elog(ERROR, "no graph name supplied");

	/* make sure there is no existing namespace of same name */
	if (SearchSysCacheExists1(GRAPHNAME, PointerGetDatum(graphName)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_SCHEMA),
				 errmsg("graph \"%s\" already exists", graphName)));

	/* Create Schema as a graph */
	schema = makeNode(CreateSchemaStmt);
	schema->schemaname = stmt->graphname;
	schema->authrole = stmt->authrole;
	schema->if_not_exists = stmt->if_not_exists;
	schema->schemaElts = NIL;

	schemaoid = CreateSchemaCommand(schema, queryString);

	/* initialize nulls and values */
	for (i = 0; i < Natts_ag_graph ; i++)
	{
		nulls[i] = false;
		values[i] = (Datum) NULL;
	}
	namestrcpy(&gname, graphName);
	values[Anum_ag_graph_graphname - 1] = NameGetDatum(&gname);
	values[Anum_ag_graph_nspid - 1] = ObjectIdGetDatum(schemaoid);

	graphdesc = heap_open(GraphRelationId, RowExclusiveLock);
	tupDesc = graphdesc->rd_att;

	tup = heap_form_tuple(tupDesc, values, nulls);

	graphoid = simple_heap_insert(graphdesc, tup);
	Assert(OidIsValid(graphoid));

	CatalogUpdateIndexes(graphdesc, tup);

	heap_close(graphdesc, RowExclusiveLock);

	addr_graph.classId = GraphRelationId;
	addr_graph.objectId = graphoid;
	addr_graph.objectSubId = 0;
	addr_schema.classId = NamespaceRelationId;
	addr_schema.objectId = schemaoid;
	addr_schema.objectSubId = 0;

	recordDependencyOn(&addr_schema, &addr_graph, DEPENDENCY_INTERNAL);

	return graphoid;
}

void
graph_drop_with_catalog(Oid graphid)
{
	Relation	ag_graph_desc;
	HeapTuple	tup;

	ag_graph_desc = heap_open(GraphRelationId, RowExclusiveLock);

	tup = SearchSysCache1(GRAPHOID, ObjectIdGetDatum(graphid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for graph %u", graphid);

	simple_heap_delete(ag_graph_desc, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(ag_graph_desc, RowExclusiveLock);
}

/*
 * Routines for handling the GUC variable 'graph_path'.
 */

/* check_hook: validate new graph_path value */
bool
check_graph_path(char **newval, void **extra, GucSource source)
{
	/* Only allow clean ASCII chars in the graph name */
	char	   *p;

	for (p = *newval; *p; p++)
	{
		if (*p < 32 || *p > 126)
			*p = '?';
	}

	return true;
}

char *
get_graph_path(void)
{
	if (strlen(graph_path) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_SCHEMA_NAME),
				 errmsg("The graph_path is NULL"),
				 errhint("Use SET graph_path")));

	return graph_path;
}
