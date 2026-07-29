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
#include "catalog/ag_graphmeta.h"
#include "catalog/binary_upgrade.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_namespace.h"
#include "commands/schemacmds.h"
#include "utils/builtins.h"
#include "utils/graph.h"
#include "utils/guc.h"
#include "utils/guc_hooks.h"
#include "utils/lsyscache.h"
#include "utils/inval.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "storage/lmgr.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "pgstat.h"

/* a global variable for the GUC variable */
char	   *graph_path = NULL;
bool		enable_graph_dml = false;
bool		enable_graph_ddl = false;
bool		cypher_allow_unsafe_ddl = false;

/* Potentially set by pg_upgrade_support functions */
Oid			binary_upgrade_next_ag_graph_oid = InvalidOid;

/* assign_hook for auto_gather_graphmeta */
static bool prev_auto_gather_graphmeta = false;

/*
 * check_hook: stash the GUC source so the assign hook can tell an explicit
 * interactive SET (which safely regathers a baseline) from a value inherited at
 * session startup (ALTER DATABASE / postgresql.conf), where running a regather
 * is both unsafe -- the snapshot machinery is not ready and it would abort --
 * and wasteful, since every new session would rescan the whole graph.
 */
bool
check_auto_gather_graphmeta(bool *newval, void **extra, GucSource source)
{
	GucSource  *stored = (GucSource *) guc_malloc(LOG, sizeof(GucSource));

	if (stored == NULL)
		return false;
	*stored = source;
	*extra = stored;
	return true;
}

void
auto_gather_graphmeta_assign(bool newval, void *extra)
{
	GucSource	source = (extra != NULL) ? *((GucSource *) extra) : PGC_S_DEFAULT;

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
	 * false->true transition.  Gather a complete baseline only for an explicit
	 * interactive SET in a live session (source >= PGC_S_INTERACTIVE), where
	 * query execution is safe -- this is the "regather on SET" behaviour, kept
	 * intact.  A value applied at session startup (inherited from ALTER DATABASE
	 * / postgresql.conf, source < PGC_S_INTERACTIVE) must NOT regather here: the
	 * snapshot machinery is not ready (it would abort), and re-gathering on every
	 * new session would rescan the whole graph each time.  The write path
	 * maintains ag_graphmeta incrementally from the persistent baseline; a later
	 * interactive SET or regather_graphmeta() refreshes it.
	 */
	if (source >= PGC_S_INTERACTIVE && IsTransactionState())
	{
		regather_graphmeta_internal();
		prev_auto_gather_graphmeta = true;
	}
}

/*
 * graphmeta_baseline_valid
 *		True iff graphmeta scan pruning may trust ag_graphmeta for `graph':
 *		gathering is on (so the catalog is being maintained) AND a complete
 *		baseline is on record for this graph (ag_graph.graphmeta_valid).  The
 *		durable per-graph flag replaces the old per-backend "did this session
 *		regather" static, so a session that merely inherited auto_gather_graphmeta
 *		= on (ALTER DATABASE / postgresql.conf) -- which never runs the assign
 *		hook's regather -- still prunes, without a per-session rescan.  Read
 *		through the GRAPHOID syscache: cheap, and auto-invalidated when the flag
 *		flips (graphmeta_set_valid).  The GUC gate stays because the flag can read
 *		valid while gathering is off (an off write has not happened yet), and an
 *		off session does not maintain the catalog going forward.
 */
bool
graphmeta_baseline_valid(Oid graph)
{
	HeapTuple	tup;
	bool		valid;

	if (!auto_gather_graphmeta)
		return false;

	tup = SearchSysCache1(GRAPHOID, ObjectIdGetDatum(graph));
	if (!HeapTupleIsValid(tup))
		return false;
	valid = ((Form_ag_graph) GETSTRUCT(tup))->graphmeta_valid;
	ReleaseSysCache(tup);

	return valid;
}

/*
 * graphmeta_set_valid
 *		Flip ag_graph.graphmeta_valid for one graph.  regather_graphmeta() sets it
 *		true after rebuilding the baseline; the transaction that commits an edge
 *		write while gathering is off clears it (that write changed connectivity
 *		without maintaining ag_graphmeta).  A per-graph object lock serializes
 *		concurrent flippers, so two off-write committers never collide on the row
 *		and error with "tuple concurrently updated"; the loser re-reads the
 *		committed value and the update is idempotent (a no-op when the flag
 *		already holds `valid').
 */
void
graphmeta_set_valid(Oid graph, bool valid)
{
	Relation	rel;
	HeapTuple	tup;

	if (!OidIsValid(graph))
		return;

	LockDatabaseObject(GraphRelationId, graph, 0, AccessExclusiveLock);

	rel = table_open(GraphRelationId, RowExclusiveLock);
	tup = SearchSysCacheCopy1(GRAPHOID, ObjectIdGetDatum(graph));
	if (HeapTupleIsValid(tup))
	{
		Form_ag_graph g = (Form_ag_graph) GETSTRUCT(tup);

		if (g->graphmeta_valid != valid)
		{
			g->graphmeta_valid = valid;
			CatalogTupleUpdate(rel, &tup->t_self, tup);

			/*
			 * The flag lives on ag_graph, but cached graphmeta-pruned plans
			 * depend on GraphMetaRelationId (see inherit.c), not on ag_graph, so
			 * the automatic GRAPHOID syscache invalidation the update above emits
			 * (which refreshes the read path) would not re-plan them.  Invalidate
			 * ag_graphmeta's relcache too so those plans re-plan against the
			 * changed validity -- mirroring regather and the delta path.
			 */
			CacheInvalidateRelcacheByRelid(GraphMetaRelationId);
		}
		heap_freetuple(tup);
	}
	table_close(rel, RowExclusiveLock);
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

	/*
	 * A genuinely new graph is empty, so its (empty) ag_graphmeta baseline is
	 * trivially complete -- start it valid.  A binary-upgrade recreation is
	 * different: the edge data files are linked in behind the maintained write
	 * path (no COPY, so the off-write clear hook never fires) while ag_graphmeta
	 * is recreated empty by the new cluster's initdb, so it must start invalid
	 * and force a post-upgrade regather_graphmeta().
	 */
	values[Anum_ag_graph_graphmeta_valid - 1] = BoolGetDatum(!IsBinaryUpgrade);

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
