/*
 * collector.c:
 *
 * Copyright (c) 2009-2016, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "pg_statsinfod.h"

#include <time.h>

/* maintenance mode */
#define MaintenanceModeIsSnapshot(mode)	( mode & MAINTENANCE_MODE_SNAPSHOT )
#define MaintenanceModeIsLog(mode)		( mode & MAINTENANCE_MODE_LOG )
#define MaintenanceModeIsRepoLog(mode)	( mode & MAINTENANCE_MODE_REPOLOG )

pthread_mutex_t	reload_lock;
pthread_mutex_t	maintenance_lock;
volatile time_t	server_reload_time;
volatile time_t	collector_reload_time;
volatile char  *snapshot_requested;
volatile char  *maintenance_requested;

static PGconn  *collector_conn = NULL;

static bool reload_params(void);
static void do_sample(void);
static void do_snapshot(char *comment);
static void get_server_encoding(void);

void
collector_init(void)
{
	pthread_mutex_init(&reload_lock, NULL);
	pthread_mutex_init(&maintenance_lock, NULL);

	collector_reload_time = server_reload_time = time(NULL);
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
		time_t	reload_time;

		now = time(NULL);

		/*
		 * Update settings if reloaded. Copy server_reload_time to the
		 * local variable because the global variable could be updated
		 * during reload. The value must be compared with now because
		 * it could be a future time to wait for SIGHUP propagated.
		 */
		reload_time = server_reload_time;
		if (collector_reload_time < reload_time && reload_time <= now)
		{
			elog(DEBUG2, "collector reloads setting parameters");
			if (reload_params())
				collector_reload_time = reload_time;
			now = time(NULL);
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

	pgut_disconnect(collector_conn);
	collector_conn = NULL;
	shutdown_progress(COLLECTOR_SHUTDOWN);

	return NULL;
}

static bool
reload_params(void)
{
	PGconn	   *conn;
	PGresult   *res;
	int			retry;

	for (retry = 0;
		 shutdown_state < SHUTDOWN_REQUESTED && retry < DB_MAX_RETRY;
		 delay(), retry++)
	{
		/* connect to postgres database and ensure functions are installed */
		if ((conn = collector_connect(NULL)) == NULL)
			continue;

		res = pgut_execute(conn, SQL_SELECT_CUSTOM_SETTINGS, 0, NULL);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			PQclear(res);
			continue;
		}

		pthread_mutex_lock(&reload_lock);
		readopt_from_db(res);
		pthread_mutex_unlock(&reload_lock);
		PQclear(res);

		/* if already passed maintenance time, set one day after */
		if (time(NULL) >= maintenance_time)
			maintenance_time = maintenance_time + (1 * SECS_PER_DAY);

		return true;	/* reloaded */
	}

	return false;
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
	char	   *pgdb;
	char		info[1024];
	const char *schema;

	if (db == NULL)
	{
		/* default connect */
		db = "postgres";
		schema = "statsinfo";
	}
	else
	{
		/* no schema required */
		schema = NULL;
	}

	/* disconnect if need to connect another database */
	if (collector_conn)
	{
		pgdb = PQdb(collector_conn);
		if (pgdb == NULL || strcmp(pgdb, db) != 0)
		{
			pgut_disconnect(collector_conn);
			collector_conn = NULL;
		}
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

#ifdef DEBUG
	snprintf(info, lengthof(info),
		"dbname=%s port=%s", db, postmaster_port);
#else
	snprintf(info, lengthof(info),
		"dbname=%s port=%s options='-c log_statement=none'", db, postmaster_port);
#endif
	return do_connect(&collector_conn, info, schema);
}
