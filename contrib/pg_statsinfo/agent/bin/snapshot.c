/*
 * snapshot.c:
 *
 * Copyright (c) 2009-2018, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "pg_statsinfod.h"

/* XXX: should read SQLs from external files? */
#include "collector_sql.h"
#include "writer_sql.h"

/* printf format specifiers for 64-bit integer */
#ifdef HAVE_LONG_INT_64
#ifndef HAVE_INT64
#define PARAMS_FORMAT_CPUSTATS		"(%ld,%ld,%ld,%ld)"
#endif
#elif defined(HAVE_LONG_LONG_INT_64)
#ifndef HAVE_INT64
#define PARAMS_FORMAT_CPUSTATS		"(%lld,%lld,%lld,%lld)"
#endif
#else
/* neither HAVE_LONG_INT_64 nor HAVE_LONG_LONG_INT_64 */
#error must have a working 64-bit integer datatype
#endif

/* snapshot data */
typedef struct Snap
{
	QueueItem	base;

	char		*comment;		/* snapshot comment, or NULL */
	PGresult	*dbnames;		/* database list { dbid, dbname } */
	List		*instance;		/* per instance snapshot */
	List		*dbsnaps;		/* a list of per database snapshot */
	char		*start;			/* start timestamp */
	bool		 alert_logging;	/* alert logging */
} Snap;

/* cpustats data */
typedef struct CPUstats
{
	int64	 user;
	int64	 system;
	int64	 idle;
	int64	 iowait;
} CPUstats;

static CPUstats	 prev_cpustats = {0, 0, 0, 0};

static const char *instance_gets[] =
{
/*	SQL_SELECT_ACTIVITY,			*/
/*	SQL_SELECT_LONG_TRANSACTION,	*/
/*	SQL_SELECT_CPU,					*/
	SQL_SELECT_DEVICE,
	SQL_SELECT_LOADAVG,
	SQL_SELECT_MEMORY,
	SQL_SELECT_TABLESPACE,
	SQL_SELECT_SETTING,
	SQL_SELECT_ROLE,
	SQL_SELECT_PROFILE,
	SQL_SELECT_LOCK,
	SQL_SELECT_BGWRITER,
#if PG_VERSION_NUM >= 90100
	SQL_SELECT_REPLICATION,
#endif
	SQL_SELECT_XLOG,
#if PG_VERSION_NUM >= 90400
	SQL_SELECT_ARCHIVE,
#endif
	SQL_SELECT_REPLICATION_SLOTS,
/*	SQL_SELECT_STATEMENT,	*/
/*	SQL_SELECT_PLAN,		*/
	NULL
};

static const char *instance_puts[] =
{
	SQL_INSERT_ACTIVITY,
	SQL_INSERT_LONG_TRANSACTION,
	SQL_INSERT_CPU,
	SQL_INSERT_DEVICE,
	SQL_INSERT_LOADAVG,
	SQL_INSERT_MEMORY,
	SQL_INSERT_TABLESPACE,
	SQL_INSERT_SETTING,
	SQL_INSERT_ROLE,
	SQL_INSERT_PROFILE,
	SQL_INSERT_LOCK,
	SQL_INSERT_BGWRITER,
#if PG_VERSION_NUM >= 90100
	SQL_INSERT_REPLICATION,
#endif
	SQL_INSERT_XLOG,
#if PG_VERSION_NUM >= 90400
	SQL_INSERT_ARCHIVE,
#endif
	SQL_INSERT_REPLICATION_SLOTS,
	SQL_INSERT_STATEMENT,
	SQL_INSERT_PLAN,
	NULL
};

static const char *database_gets[] =
{
	SQL_SELECT_SCHEMA,
	SQL_SELECT_TABLE,
	SQL_SELECT_COLUMN,
	SQL_SELECT_INDEX,
	SQL_SELECT_INHERITS,
	SQL_SELECT_FUNCTION,
	NULL
};

static const char *database_puts[] =
{
	SQL_INSERT_SCHEMA,
	SQL_INSERT_TABLE,
	SQL_INSERT_COLUMN,
	SQL_INSERT_INDEX,
	SQL_INSERT_INHERITS,
	SQL_INSERT_FUNCTION,
	NULL
};

static void Snap_free(Snap *snap);
static bool Snap_exec(Snap *snap, PGconn *conn, const char *instid);
static List *do_gets(PGconn *conn, const char *sql[],
					 int nParams, const char **params);
static PGresult *do_get(PGconn *conn, const char *sql,
						int nParams, const char **params);
static bool do_puts(PGconn *conn, const char *sql[], List *src,
					const char *snapid, const char *dbid);
static bool do_put(PGconn *conn, const char *sql, PGresult *src,
				   const char *snapid, const char *dbid);
static bool has_pg_stat_statements(PGconn *conn);
static bool has_pg_store_plans(PGconn *conn);
#if PG_VERSION_NUM < 90400
static bool has_pg_store_plans_hash_query(PGconn *conn);
#endif
static bool has_statsrepo_alert(PGconn *conn);

QueueItem *
get_snapshot(char *comment)
{
	PGconn		*conn = NULL;
	PGresult	*activity = NULL;
	PGresult	*long_xact = NULL;
	PGresult	*cpuinfo = NULL;
	Snap		*snap;
	int64		 cpu_user;
	int64		 cpu_system;
	int64		 cpu_idle;
	int64		 cpu_iowait;
	int			 r;
	int			 rows;
	int			 retry;

	/* allocate a new Snap object */
	snap = pgut_new(Snap);
	memset(snap, 0, sizeof(*snap));

	/*
	 * collect instance statistics
	 */
	elog(DEBUG2, "snapshot (instance)");

	/*
	 * start of the snapshot information getting
	 */
	if ((snap->start = getlocaltimestamp()) == NULL)
	{
		Snap_free(snap);
		return NULL;
	}

	for (retry = 0;
		 shutdown_state < SHUTDOWN_REQUESTED && retry < DB_MAX_RETRY;
		 delay(), retry++)
	{
		/* connect to postgres database and ensure functions are installed */
		if ((conn = collector_connect(NULL)) == NULL)
			continue;

		/* query activities as a separated transaction. */
		if (activity == NULL)
		{
			activity = do_get(conn, SQL_SELECT_ACTIVITY, 0, NULL);
			if (activity == NULL)
				continue;
		}

		/* query long transaction as a separated transaction. */
		if (long_xact == NULL)
		{
			long_xact = do_get(conn, SQL_SELECT_LONG_TRANSACTION, 0, NULL);
			if (long_xact == NULL)
				continue;
		}

		/* query cpuinfo as a separated transaction. */
		if (cpuinfo == NULL)
		{
			const char *params[1];
			char buf[1024];

			snprintf(buf, sizeof(buf), PARAMS_FORMAT_CPUSTATS,
				prev_cpustats.user,
				prev_cpustats.system,
				prev_cpustats.idle,
				prev_cpustats.iowait);

			params[0] = buf;
			cpuinfo = do_get(conn, SQL_SELECT_CPU, 1, params);
			if (cpuinfo == NULL)
				continue;

			parse_int64(PQgetvalue(cpuinfo, 0, 1), &cpu_user);
			parse_int64(PQgetvalue(cpuinfo, 0, 2), &cpu_system);
			parse_int64(PQgetvalue(cpuinfo, 0, 3), &cpu_idle);
			parse_int64(PQgetvalue(cpuinfo, 0, 4), &cpu_iowait);
		}

		/* enum databases */
		if (snap->dbnames == NULL)
		{
			const char *params[1];

			params[0] = excluded_dbnames;
			snap->dbnames = do_get(conn, SQL_SELECT_DATABASE, 1, params);
			if (snap->dbnames == NULL)
				continue;
		}

		/* query other instance-level statistics */
		snap->instance = do_gets(conn, instance_gets, 0, NULL);
		if (snap->instance == NIL)
			continue;

		break;	/* ok */
	}

	if (snap->instance == NIL)
	{
		PQclear(activity);		/* activity has not been assigned yet */
		PQclear(long_xact);		/* long transaction has not been assigned yet */
		PQclear(cpuinfo);		/* cpuinfo has not been assigned yet */
		Snap_free(snap);
		return NULL;
	}

	/* prepend the cpuinfo to the instance statistics */
	snap->instance = lcons(cpuinfo, snap->instance);
	/* prepend the long transaction to the instance statistics */
	snap->instance = lcons(long_xact, snap->instance);
	/* prepend the activity to the instance statistics */
	snap->instance = lcons(activity, snap->instance);

	/* When pg_stat_statements is installed, we collect it */
	if (has_pg_stat_statements(conn))
	{
		StringInfoData	 query;
		PGresult		*stmt;
		const char		*params[] = {stat_statements_exclude_users, stat_statements_max};

		initStringInfo(&query);
#if PG_VERSION_NUM >= 90400
		appendStringInfo(&query, SQL_SELECT_STATEMENT);
#else
		if (has_pg_store_plans_hash_query(conn))
			appendStringInfo(&query, SQL_SELECT_STATEMENT, "pg_store_plans_hash_query(s.query)");
		else
			appendStringInfo(&query, SQL_SELECT_STATEMENT, "NULL");
#endif
		stmt = pgut_execute(conn, query.data, 2, params);
		if (PQresultStatus(stmt) == PGRES_TUPLES_OK)
			snap->instance = lappend(snap->instance, stmt);
		else
		{
			PQclear(stmt);
			snap->instance = lappend(snap->instance, NULL);
		}

		termStringInfo(&query);
	}
	else
		snap->instance = lappend(snap->instance, NULL);

	/* When pg_store_plans is installed, we collect it */
	if (has_pg_store_plans(conn))
	{
		PGresult   *stmt;
		const char *params[] = {stat_statements_exclude_users, stat_statements_max};

		pgut_command(conn, "SET pg_store_plans.plan_format TO 'raw'", 0, NULL);
		stmt = pgut_execute(conn, SQL_SELECT_PLAN, 2, params);
		if (PQresultStatus(stmt) == PGRES_TUPLES_OK)
			snap->instance = lappend(snap->instance, stmt);
		else
		{
			PQclear(stmt);
			snap->instance = lappend(snap->instance, NULL);
		}
	}
	else
		snap->instance = lappend(snap->instance, NULL);

	/* collect database statistics */
	rows = PQntuples(snap->dbnames);
	for (r = 0; r < rows; r++)
	{
		const char *db = PQgetvalue(snap->dbnames, r, 1);
		List	   *dbsnap = NIL;
		const char *params[] = {excluded_schemas};

		elog(DEBUG2, "snapshot (database=%s)", db);
		for (retry = 0;
			 shutdown_state < SHUTDOWN_REQUESTED && retry < DB_MAX_RETRY;
			 delay(), retry++)
		{
			if ((conn = collector_connect(db)) == NULL ||
				(dbsnap = do_gets(conn, database_gets, 1, params)) == NIL)
				continue;

			break;	/* ok */
		}

		snap->dbsnaps = lappend(snap->dbsnaps, dbsnap);
	}

	/* ok, fill other fields. */
	snap->base.type = QUEUE_SNAPSHOT;
	snap->base.free = (QueueItemFree) Snap_free;
	snap->base.exec = (QueueItemExec) Snap_exec;
	snap->comment = comment;
	snap->alert_logging = repolog_min_messages <= ALERT;

	/* update previous values */
	prev_cpustats.user = cpu_user;
	prev_cpustats.system = cpu_system;
	prev_cpustats.idle = cpu_idle;
	prev_cpustats.iowait = cpu_iowait;

	return (QueueItem *) snap;
}

static void
destroy_PGresult_list(List *list)
{
	list_destroy(list, PQclear);
}

static void
Snap_free(Snap *snap)
{
	if (snap)
	{
		free(snap->comment);
		PQclear(snap->dbnames);
		destroy_PGresult_list(snap->instance);
		list_destroy(snap->dbsnaps, destroy_PGresult_list);
		if (snap->start != NULL)
			free(snap->start);
		free(snap);
	}
}

/*
 * insert statstics into the repository server
 */
static bool
Snap_exec(Snap *snap, PGconn *conn, const char *instid)
{
	PGresult   *snapid_res = NULL;
	const char *params[4];
	const char *snapid;
	ListCell   *db;
	int			i;
	PGresult   *repo_size = NULL;
	PGresult   *alerts = NULL;
	PGresult   *update_res = NULL;
	char	   *end = NULL;

	elog(DEBUG2, "write (snapshot)");

	/*
	 * create partition tables
	 */
	params[0] = snap->start;
	if (pgut_command(conn,
		SQL_CREATE_SNAPSHOT_PARTITION, 1, params) != PGRES_TUPLES_OK)
		goto error;
	if (pgut_command(conn,
		SQL_CREATE_REPOLOG_PARTITION, 1, params) != PGRES_TUPLES_OK)
		goto error;

	if (pgut_command(conn, "BEGIN", 0, NULL) != PGRES_COMMAND_OK)
		goto error;

	/* exclusive control for don't run concurrently with the maintenance */
	if (pgut_command(conn,
		"LOCK TABLE statsrepo.instance IN SHARE MODE", 0, NULL) != PGRES_COMMAND_OK)
		goto error;

	/*
	 * get statsrepo schema relation total size
	 */
	repo_size = do_get(conn, SQL_SELECT_REPOSIZE, 0, NULL);
	if (repo_size == NULL)
		goto error;

	params[0] = instid;
	params[1] = snap->start;
	params[2] = snap->comment;
	snapid_res = pgut_execute(conn, SQL_NEW_SNAPSHOT, 3, params);
	if (PQntuples(snapid_res) == 0)
		goto error;

	snapid = PQgetvalue(snapid_res, 0, 0);

	if (!do_put(conn, SQL_INSERT_DATABASE, snap->dbnames, snapid, NULL))
		goto error;

	if (!do_puts(conn, instance_puts, snap->instance, snapid, NULL))
		goto error;

	i = 0;
	foreach(db, snap->dbsnaps)
	{
		List	   *dbsnap = (List*) lfirst(db);
		const char *dbid = PQgetvalue(snap->dbnames, i ,0);

		if (!do_puts(conn, database_puts, dbsnap, snapid, dbid))
			goto error;
		i++;
	}

	/*
	 * call statsrepo.alert(snapid) if exists
	 */
	if (enable_alert && has_statsrepo_alert(conn))
	{
		elog(DEBUG2, "run alert(snapid=%s)", snapid);
		alerts = pgut_execute(conn,
							  "SELECT * FROM statsrepo.alert($1)",
							  1, &snapid);
		if (!do_put(conn, SQL_INSERT_ALERT, alerts, snapid, NULL))
			goto error;

		if (snap->alert_logging)
		{
			const char *fields[24];

			memset(fields, 0, sizeof(fields));
			fields[0] = instid;
			fields[1] = snap->start;  /* timestamp */
			for (i = 0; i < PQntuples(alerts); i++)
			{
				fields[12] = "ALERT";  /* elevel */
				fields[14] = PQgetvalue(alerts, i, 0);  /* message */
				if (pgut_command(conn,
					SQL_INSERT_LOG, lengthof(fields), fields) != PGRES_COMMAND_OK)
					goto error;
			}
		}
	}

	/*
	 * end of the snapshot information getting
	 */
	if ((end = getlocaltimestamp()) == NULL)
		goto error;

	/*
	 * update statsrepo.snapshot
	 */
	params[0] = snapid;
	params[1] = end;
	params[2] = snap->start;
	params[3] = PQgetvalue(repo_size, 0, 0);
	update_res = pgut_execute(conn, SQL_UPDATE_SNAPSHOT, 4, params);
	if (PQresultStatus(update_res) != PGRES_COMMAND_OK)
		goto error;

	if (!pgut_commit(conn))
		goto error;

	/* write alert log */
	if (alerts)
		for (i = 0; i < PQntuples(alerts); i++)
			elog(ALERT, "%s", PQgetvalue(alerts, i, 0));

	free(end);
	PQclear(snapid_res);
	PQclear(repo_size);
	PQclear(alerts);
	PQclear(update_res);
	return true;

error:
	if (end != NULL)
		free(end);
	PQclear(snapid_res);
	PQclear(repo_size);
	PQclear(alerts);
	PQclear(update_res);
	pgut_rollback(conn);
	return false;
}

/*
 * 'sql' must have a NULL sentinel at the end.
 */
static List *
do_gets(PGconn *conn, const char *sql[], int nParams, const char **params)
{
	int		i;
	List   *result = NIL;

	if (pgut_command(conn, "BEGIN", 0, NULL) != PGRES_COMMAND_OK)
		goto error;

	for (i = 0; sql[i]; i++)
	{
		PGresult   *res;

		if ((res = do_get(conn, sql[i], nParams, params)) == NULL)
			goto error;
		result = lappend(result, res);
	}

	if (!pgut_commit(conn))
		goto error;

	return result;	/* ok */

error:
	list_destroy(result, PQclear);
	pgut_rollback(conn);
	return NIL;
}

static PGresult *
do_get(PGconn *conn, const char *sql, int nParams, const char **params)
{
	PGresult *res;

	res = pgut_execute(conn, sql, nParams, params);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		return NULL;
	}

	return res;
}

static bool
do_puts(PGconn *conn,
		const char *sql[],
		List *src,
		const char *snapid,
		const char *dbid)
{
	ListCell   *cell;
	int			i;

	i = 0;
	foreach(cell, src)
	{
		PGresult *res = (PGresult *) lfirst(cell);

		if (res && sql[i])
		{
			if (!do_put(conn, sql[i], res, snapid, dbid))
				return false;
		}
		i++;
	}
	return true;
}

static bool
do_put(PGconn *conn,
	   const char *sql,
	   PGresult *src,
	   const char *snapid,
	   const char *dbid)
{
	const char *params[FUNC_MAX_ARGS];
	int			rows, cols;
	int			r, c;
	int			shift;

	Assert(src);

	cols = PQnfields(src);
	shift = 0;
	params[shift++] = snapid;
	if (dbid)
		params[shift++] = dbid;

	if (shift + cols > FUNC_MAX_ARGS)
	{
		elog(WARNING, "too many columns: %d", cols);
		return false;
	}

	rows = PQntuples(src);
	for (r = 0; r < rows; r++)
	{
		for (c = 0; c < cols; c++)
		{
			if (PQgetisnull(src, r, c))
				params[shift + c] = NULL;
			else
				params[shift + c] = PQgetvalue(src, r, c);
		}

		switch (pgut_command(conn, sql, shift + cols, params))
		{
			case PGRES_COMMAND_OK:
			case PGRES_TUPLES_OK:
				break;
			default:
				return false;
		}
	}

	return true;
}

static bool
has_pg_stat_statements(PGconn *conn)
{
	PGresult   *res;
	bool		result;

	/* check whether pg_stat_statements is installed */
	res = pgut_execute(conn,
			"SELECT relname FROM pg_class"
			" WHERE relname = 'pg_stat_statements' AND relkind = 'v'",
			0, NULL);
	result = (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0);
	PQclear(res);

	return result;
}

static bool
has_pg_store_plans(PGconn *conn)
{
	PGresult   *res;
	bool		result;

	/* check whether pg_store_plans is installed */
	res = pgut_execute(conn,
			"SELECT relname FROM pg_class"
			" WHERE relname = 'pg_store_plans' AND relkind = 'v'",
			0, NULL);
	result = (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0);
	PQclear(res);

	return result;
}

#if PG_VERSION_NUM < 90400
static bool
has_pg_store_plans_hash_query(PGconn *conn)
{
	PGresult   *res;
	bool		result;

	/* check whether pg_store_plans is installed */
	res = pgut_execute(conn,
			"SELECT 1 FROM pg_proc WHERE proname = 'pg_store_plans_hash_query'",
			0, NULL);
	result = (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0);
	PQclear(res);

	return result;
}
#endif

static bool
has_statsrepo_alert(PGconn *conn)
{
	PGresult   *res;
	bool		result;

	/* check whether statsrepo.alert is installed */
	res = pgut_execute(conn,
					   "SELECT 1 FROM pg_proc, pg_namespace n"
					   " WHERE nspname = 'statsrepo'"
					   "   AND proname = 'alert'"
					   "   AND pronamespace = n.oid"
					   " LIMIT 1",
					   0, NULL);
	result = (PQresultStatus(res) == PGRES_TUPLES_OK && PQntuples(res) > 0);
	PQclear(res);

	return result;
}
