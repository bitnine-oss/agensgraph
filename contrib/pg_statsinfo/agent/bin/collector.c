/*
 * collector.c:
 *
 * Copyright (c) 2009-2018, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "pg_statsinfod.h"

#include <time.h>

/* maintenance mode */
#define MaintenanceModeIsSnapshot(mode)	( mode & MAINTENANCE_MODE_SNAPSHOT )
#define MaintenanceModeIsLog(mode)		( mode & MAINTENANCE_MODE_LOG )
#define MaintenanceModeIsRepoLog(mode)	( mode & MAINTENANCE_MODE_REPOLOG )

pthread_mutex_t	reload_lock;
pthread_mutex_t	maintenance_lock;
volatile time_t	collector_reload_time;
volatile char  *snapshot_requested;
volatile char  *maintenance_requested;

static PGconn  *collector_conn = NULL;

static void reload_params(void);
static void do_sample(void);
static void do_snapshot(char *comment);
static void get_server_encoding(void);
static void collector_disconnect(void);
static bool extract_dbname(const char *conninfo, char *dbname, size_t size);

void
collector_init(void)
{
	pthread_mutex_init(&reload_lock, NULL);
	pthread_mutex_init(&maintenance_lock, NULL);

	collector_reload_time = time(NULL);
}

/*
 * collector_main
 */
void *
collector_main(void *arg)
{
	time_t		now;
	time_t		next_sample;
	time_t		next_snapshot;
	pid_t		log_maintenance_pid = 0;
	int			fd_err;

	now = time(NULL);
	next_sample = get_next_time(now, sampling_interval);
	next_snapshot = get_next_time(now, snapshot_interval);

	/* we set actual server encoding to libpq default params. */
	get_server_encoding();

	/* if already passed maintenance time, set one day after */
	if (now >= maintenance_time)
		maintenance_time = maintenance_time + (1 * SECS_PER_DAY);

	while (shutdown_state < SHUTDOWN_REQUESTED)
	{
		now = time(NULL);

		/* reload configuration */
		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			reload_params();
			collector_reload_time = now;
		}

		/* sample */
		if (now >= next_sample)
		{
			elog(DEBUG2, "sample (%d sec for next snapshot)", (int) (next_snapshot - now));
			do_sample();
			now = time(NULL);
			next_sample = get_next_time(now, sampling_interval);
		}

		/* snapshot by manual */
		if (snapshot_requested)
		{
			char *comment;

			pthread_mutex_lock(&reload_lock);
			comment = (char *) snapshot_requested;
			snapshot_requested = NULL;
			pthread_mutex_unlock(&reload_lock);

			if (comment)
				do_snapshot(comment);
		}

		/* snapshot by time */
		if (now >= next_snapshot)
		{
			do_snapshot(NULL);
			now = time(NULL);
			next_snapshot = get_next_time(now, snapshot_interval);
		}

		/* maintenance by manual */
		if (maintenance_requested)
		{
			time_t repository_keep_period;

			pthread_mutex_lock(&reload_lock);
			repository_keep_period = atol((char *) maintenance_requested);
			maintenance_requested = NULL;
			pthread_mutex_unlock(&reload_lock);

			maintenance_snapshot(repository_keep_period);
		}

		/* maintenance by time */
		if (enable_maintenance && now >= maintenance_time)
		{
			if (MaintenanceModeIsSnapshot(enable_maintenance))
			{
				time_t repository_keep_period;
				struct tm *tm;

				/* calculate retention period on the basis of today's 0:00 AM */
				tm = localtime(&now);
				tm->tm_hour = 0;
				tm->tm_min = 0;
				tm->tm_sec = 0;
				repository_keep_period = mktime(tm) - ((time_t) repository_keepday * SECS_PER_DAY);

				maintenance_snapshot(repository_keep_period);
			}

			if (MaintenanceModeIsRepoLog(enable_maintenance))
			{
				time_t repolog_keep_period;
				struct tm *tm;

				/* calculate retention period on the basis of today's 0:00 AM */
				tm = localtime(&now);
				tm->tm_hour = 0;
				tm->tm_min = 0;
				tm->tm_sec = 0;
				repolog_keep_period = mktime(tm) - ((time_t) repolog_keepday * SECS_PER_DAY);

				maintenance_repolog(repolog_keep_period);
			}

			if (MaintenanceModeIsLog(enable_maintenance))
			{
				if (log_maintenance_pid <= 0)
				{
					if ((log_maintenance_pid = maintenance_log(log_maintenance_command, &fd_err)) < 0)
						elog(ERROR, "could not run the log maintenance command");
				}
				else
					elog(WARNING,
						"previous log maintenance is not complete, "
						"so current log maintenance was skipped");
			}

			maintenance_time = maintenance_time + (1 * SECS_PER_DAY);
		}

		/* check the status of log maintenance command */
		if (log_maintenance_pid > 0 &&
			check_maintenance_log(log_maintenance_pid, fd_err))
		{
			/* log maintenance command has been completed */
			log_maintenance_pid = 0;
		}

		usleep(200 * 1000);	/* 200ms */
	}

	collector_disconnect();
	shutdown_progress(COLLECTOR_SHUTDOWN);

	return NULL;
}

static void
reload_params(void)
{
	char	*prev_target_server;

	pthread_mutex_lock(&reload_lock);

	prev_target_server = pgut_strdup(target_server);

	/* read configuration from launcher */
	readopt_from_file(stdin);

	/* if already passed maintenance time, set one day after */
	if (time(NULL) >= maintenance_time)
		maintenance_time = maintenance_time + (1 * SECS_PER_DAY);

	/* if the target_server has changed then disconnect current connection */
	if (strcmp(target_server, prev_target_server) != 0)
		collector_disconnect();

	free(prev_target_server);

	pthread_mutex_unlock(&reload_lock);
}

static void
do_sample(void)
{
	PGconn	   *conn;
	int			retry;

	for (retry = 0;
		 shutdown_state < SHUTDOWN_REQUESTED && retry < DB_MAX_RETRY;
		 delay(), retry++)
	{
		/* connect to postgres database and ensure functions are installed */
		if ((conn = collector_connect(NULL)) == NULL)
			continue;

		pgut_command(conn, "SELECT statsinfo.sample()", 0, NULL);
		break;	/* ok */
	}
}

/*
 * ownership of comment will be granted to snapshot item.
 */
static void
do_snapshot(char *comment)
{
	QueueItem	*snap = NULL;

	/* skip current snapshot if previous snapshot still not complete */
	if (writer_has_queue(QUEUE_SNAPSHOT))
	{
		elog(WARNING, "previous snapshot is not complete, so current snapshot was skipped");
		free(comment);
		return;
	}

	/* exclusive control during snapshot and maintenance */
	pthread_mutex_lock(&maintenance_lock);
	snap = get_snapshot(comment);
	pthread_mutex_unlock(&maintenance_lock);

	if (snap != NULL)
		writer_send(snap);
	else
		free(comment);
}

/*
 * set server encoding
 */
static void
get_server_encoding(void)
{
	PGconn		*conn;
	int			 retry;
	const char	*encode;

	for (retry = 0;
		 shutdown_state < SHUTDOWN_REQUESTED && retry < DB_MAX_RETRY;
		 delay(), retry++)
	{
		/* connect postgres database */
		if ((conn = collector_connect(NULL)) == NULL)
			continue;

		/* 
		 * if we could not find the encodig-string, it's ok.
		 * because PG_SQL_ASCII was already set.
		 */ 
		encode = PQparameterStatus(conn, "server_encoding");
		if (encode != NULL)
			pgut_putenv("PGCLIENTENCODING", encode);
		elog(DEBUG2, "collector set client_encoding : %s", encode);
		break;	/* ok */
	}
}

PGconn *
collector_connect(const char *db)
{
	char		 dbname[NAMEDATALEN];
	char		 info[1024];
	const char	*schema;

	if (db == NULL)
	{
		if (!extract_dbname(target_server, dbname, sizeof(dbname)))
				strncpy(dbname, "postgres", sizeof(dbname));	/* default database */
		schema = "statsinfo";
	}
	else
	{
		strncpy(dbname, db, sizeof(dbname));
		/* no schema required */
		schema = NULL;
	}

	/* disconnect if need to connect another database */
	if (collector_conn)
	{
		char	*pgdb;

		pgdb = PQdb(collector_conn);
		if (pgdb == NULL || strcmp(pgdb, dbname) != 0)
			collector_disconnect();
	}
	else
	{
		ControlFileData	ctrl;

		readControlFile(&ctrl, data_directory);

		/* avoid connection fails during recovery and warm-standby */
		switch (ctrl.state)
		{
			case DB_IN_PRODUCTION:
#if PG_VERSION_NUM >= 90000
			case DB_IN_ARCHIVE_RECOVERY:	/* hot-standby accepts connections */
#endif
				break;			/* ok, do connect */
			default:
				delay();
				return NULL;	/* server is not ready for accepting connections */
		}
	}

#ifdef DEBUG_MODE
	snprintf(info, lengthof(info), "port=%s %s dbname=%s",
		postmaster_port, target_server, dbname);
#else
	snprintf(info, lengthof(info),
		"port=%s %s dbname=%s options='-c log_statement=none'",
		postmaster_port, target_server, dbname);
#endif
	return do_connect(&collector_conn, info, schema);
}

static void
collector_disconnect(void)
{
	pgut_disconnect(collector_conn);
	collector_conn = NULL;
}

static bool
extract_dbname(const char *conninfo, char *dbname, size_t size)
{
	PQconninfoOption	*options;
	PQconninfoOption	*option;

	if ((options = PQconninfoParse(conninfo, NULL)) == NULL)
		return false;

	for (option = options; option->keyword != NULL; option++)
	{
		if (strcmp(option->keyword, "dbname") == 0)
		{
			if (option->val != NULL && option->val[0] != '\0')
			{
				strncpy(dbname, option->val, size);
				PQconninfoFree(options);
				return true;
			}
		}
	}

	PQconninfoFree(options);
	return false;
}
