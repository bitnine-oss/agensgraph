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
#include "catalog/binary_upgrade.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_namespace.h"
#include "commands/schemacmds.h"
#include "utils/builtins.h"
#include "utils/graph.h"
#include "utils/guc_hooks.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "pgstat.h"

/* a global variable for the GUC variable */
char	   *graph_path = NULL;
bool		enable_graph_dml = false;
bool		cypher_allow_unsafe_ddl = false;

/* Potentially set by pg_upgrade_support functions */
Oid			binary_upgrade_next_ag_graph_oid = InvalidOid;

/* assign_hook for auto_gather_graphmeta */
static bool prev_auto_gather_graphmeta = false;
void
auto_gather_graphmeta_assign(bool newval, void *extra)
{
	/* Turning gathering off: the next enable must regather a fresh baseline. */
	if (!newval)
	{
		prev_auto_gather_graphmeta = false;
		return;
	}

	/*
	 * Already enabled (no transition), or a parallel worker inheriting the
	 * leader's already-gathered state: nothing to do.
	 */
	if (prev_auto_gather_graphmeta || IsParallelWorker())
	{
		prev_auto_gather_graphmeta = true;
		return;
	}

	/*
	 * false->true transition: gather a complete baseline so that "gathering is
	 * on" implies "ag_graphmeta is complete".  Only record the transition as
	 * handled once the regather actually ran -- otherwise (e.g. enabled from
	 * postgresql.conf, before any transaction) we must leave the flag unset so a
	 * later in-transaction SET still regathers, rather than silently leaving
	 * ag_graphmeta stale while reporting "on".
	 */
	if (IsTransactionState())
	{
		regather_graphmeta_internal();
		prev_auto_gather_graphmeta = true;
	}
	else
	{
		ereport(WARNING,
				(errmsg("auto_gather_graphmeta: cannot gather metadata outside transaction"),
				 errhint("Metadata will be gathered when set within a transaction.")));
	}
}

/*
 * graphmeta_baseline_gathered
 *		True iff this backend holds a complete, maintained ag_graphmeta baseline
 *		-- i.e. auto_gather_graphmeta was turned on inside a transaction (or
 *		inherited by a parallel worker), so the false->true regather actually
 *		ran.  Graphmeta scan pruning must gate on this rather than the raw
 *		auto_gather_graphmeta GUC: the GUC can read "on" while no baseline was
 *		ever gathered -- enabled from postgresql.conf outside any transaction,
 *		or restored by rolling back a "SET ... = off" -- and pruning against an
 *		incomplete catalog would silently drop rows.
 */
bool
graphmeta_baseline_gathered(void)
{
	return prev_auto_gather_graphmeta;
}

/* check_hook: validate new graph_path value */
bool
check_graph_path(char **newval, void **extra, GucSource source)
{
	if (!cypher_allow_unsafe_ddl && IsTransactionState() &&
		!InitializingParallelWorker)
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

/*
 * get_graph_path_or_null
 *		Like get_graph_path(true), but returns NULL instead of raising an error
 *		when graph_path is unset or names a graph that no longer exists.  Used
 *		by CURRENT_GRAPH / CURRENT_PROPERTY_GRAPH.
 */
char *
get_graph_path_or_null(void)
{
	if (graph_path == NULL || strlen(graph_path) == 0)
		return NULL;

	if (!OidIsValid(get_graphname_oid(graph_path)))
		return NULL;

	return graph_path;
}

Oid
get_graph_path_oid(void)
{
	Oid			graphoid;

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
	bool		isnull[Natts_ag_graph] = {false,};
	NameData	gname;
	Relation	graphdesc;
	TupleDesc	tupDesc;
	HeapTuple	tup;
	Oid			graphoid;
	ObjectAddress graphobj;
	ObjectAddress schemaobj;

	Assert(graphName != NULL);

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

	namestrcpy(&gname, graphName);

	graphdesc = table_open(GraphRelationId, RowExclusiveLock);
	tupDesc = graphdesc->rd_att;

	if (IsBinaryUpgrade)
	{
		if (!OidIsValid(binary_upgrade_next_ag_graph_oid))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("ag_graph OID value not set when in binary upgrade mode")));
		graphoid = binary_upgrade_next_ag_graph_oid;
		binary_upgrade_next_ag_graph_oid = InvalidOid;
	}
	else
	{
		graphoid = GetNewOidWithIndex(graphdesc,
									  GraphOidIndexId,
									  Anum_ag_graph_oid);
	}

	values[Anum_ag_graph_oid - 1] = graphoid;
	values[Anum_ag_graph_graphname - 1] = NameGetDatum(&gname);
	values[Anum_ag_graph_nspid - 1] = ObjectIdGetDatum(schemaoid);

	tup = heap_form_tuple(tupDesc, values, isnull);

	CatalogTupleInsert(graphdesc, tup);
	Assert(OidIsValid(graphoid));

	table_close(graphdesc, RowExclusiveLock);

	graphobj.classId = GraphRelationId;
	graphobj.objectId = graphoid;
	graphobj.objectSubId = 0;
	schemaobj.classId = NamespaceRelationId;
	schemaobj.objectId = schemaoid;
	schemaobj.objectSubId = 0;

	/*
	 * Register dependency from the schema to the graph, so that the schema
	 * will be deleted if the graph is.
	 */
	recordDependencyOn(&schemaobj, &graphobj, DEPENDENCY_INTERNAL);

	return graphoid;
}
