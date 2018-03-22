/*
 * writer.c:
 *
 * Copyright (c) 2009-2018, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "pg_statsinfod.h"

#ifndef WIN32
#include <sys/utsname.h>
#endif
#include <time.h>

#define WRITER_CONN_KEEP_SECS			60	/* secs */
#define WRITER_RECONNECT_DELAY			10	/* secs */

/* repository state */
typedef enum RepositoryState
{
	REPOSITORY_OK,
	REPOSITORY_CONNECT_FAILURE,
	REPOSITORY_INVALID_DATABASE
} RepositoryState;

static pthread_mutex_t	writer_queue_lock;
static List			   *writer_queue = NIL;
static time_t			writer_reload_time = 0;

static PGconn		   *writer_conn = NULL;
static time_t			writer_conn_last_used;
static bool				superuser_connect = false;

/*---- GUC variables ----*/
static char	   *my_repository_server = NULL;
/*-----------------------*/


static void reload_params(void);
static int recv_writer_queue(void);
static PGconn *writer_connect(bool superuser);
static void writer_disconnect(void);
static char *get_instid(PGconn *conn);
static const char *get_nodename(void);
static void destroy_writer_queue(QueueItem *item);
static void set_connect_privileges(void);
static RepositoryState validate_repository(void);
static bool check_repository(PGconn *conn);
static void set_writer_state(WriterState state);
static void writer_delay(void);

void
writer_init(void)
{
	pthread_mutex_init(&writer_queue_lock, NULL);
	writer_queue = NIL;
	writer_reload_time = 0;	/* any values ok as far as before now */
	set_writer_state(WRITER_READY);
}

/*
 * writer_main
 */
void *
writer_main(void *arg)
{
	int	items;
	int	rstate = -1;

	while (shutdown_state < LOGGER_SEND_SHUTDOWN)
	{
		/* update settings if reloaded */
		if (writer_reload_time < collector_reload_time)
		{
			reload_params();

			/* validate a state of the repository server */
			rstate = validate_repository();
			switch (rstate)
			{
				case REPOSITORY_OK:
					set_writer_state(WRITER_NORMAL);
					break;
				case REPOSITORY_CONNECT_FAILURE:
				case REPOSITORY_INVALID_DATABASE:
					if (shutdown_state < SHUTDOWN_REQUESTED)
						set_writer_state(WRITER_FALLBACK);
					break;
			}
		}

		/*
		 * if the repository state is in connect failure,
		 * re-validate a state of the repository server.
		 */
		if (rstate == REPOSITORY_CONNECT_FAILURE)
		{
			rstate = validate_repository();
			if (rstate == REPOSITORY_OK)
				set_writer_state(WRITER_NORMAL);
		}

		/* send queued items into the repository server */
		if (recv_writer_queue() == 0)
		{
			/* disconnect if there are no works for a long time */
			if (writer_conn != NULL &&
				writer_conn_last_used + WRITER_CONN_KEEP_SECS < time(NULL))
			{
				writer_disconnect();
				elog(DEBUG2, "disconnect unused writer connection");
			}
		}

		usleep(200 * 1000);	/* 200ms */
	}

	/* flush remaining items */
	if ((items = recv_writer_queue()) > 0)
		elog(WARNING, "writer discards %d items", items);

	writer_disconnect();
	shutdown_progress(WRITER_SHUTDOWN);

	return NULL;
}

/*
 * writer_send
 *
 * The argument item should be a malloc'ed object. The ownership will be
 * granted to this module.
 */
void
writer_send(QueueItem *item)
{
	AssertArg(item != NULL);

	item->retry = 0;

	pthread_mutex_lock(&writer_queue_lock);
	writer_queue = lappend(writer_queue, item);
	pthread_mutex_unlock(&writer_queue_lock);
}

/*
 * check whether contains the specified type on the writer queue
 */
bool
writer_has_queue(WriterQueueType type)
{
	ListCell	*cell;

	pthread_mutex_lock(&writer_queue_lock);
	foreach(cell, writer_queue)
	{
		if (((QueueItem *) lfirst(cell))->type == type)
		{
			pthread_mutex_unlock(&writer_queue_lock);
			return true;
		}
	}
	pthread_mutex_unlock(&writer_queue_lock);

	return false;
}



/*
 * load guc variables
 */
static void
reload_params(void)
{
	writer_reload_time = collector_reload_time;
	pthread_mutex_lock(&reload_lock);

	free(my_repository_server);
	my_repository_server = pgut_strdup(repository_server);

	pthread_mutex_unlock(&reload_lock);
}

/*
 * recv_writer_queue - return the number of queued items
 */
static int
recv_writer_queue(void)
{
	List	   *queue;
	int			ret;
	char	   *instid = NULL;
	bool		connection_used = false;

	pthread_mutex_lock(&writer_queue_lock);
	queue = writer_queue;
	writer_queue = NIL;
	pthread_mutex_unlock(&writer_queue_lock);

	if (list_length(queue) == 0)
		return 0;

	if (writer_state == WRITER_FALLBACK ||
		writer_connect(superuser_connect) == NULL)
	{
		/* discard current queue */
		elog(WARNING, "writer discards %d items", list_length(queue));
		list_destroy(queue, destroy_writer_queue);
		return 0;
	}

	/* do the writer queue process */
	if ((instid = get_instid(writer_conn)) != NULL)
	{
		connection_used = true;

		while (list_length(queue) > 0)
		{
			QueueItem  *item = (QueueItem *) linitial(queue);

			if (!item->exec(item, writer_conn, instid))
			{
				if (++item->retry < DB_MAX_RETRY)
					break;	/* retry the item */

				/*
				 * discard if the retry count is exceeded to avoid infinite
				 * loops at one bad item.
				 */
				elog(WARNING, "writer discard an item");
			}

			item->free(item);
			queue = list_delete_first(queue);
		}
	}
	free(instid);

	/* delay on error */
	if (list_length(queue) > 0)
		delay();

	/*
	 * When we have failed items, we concatenate to the head of writer queue
	 * and retry them in the next cycle.
	 */
	pthread_mutex_lock(&writer_queue_lock);
	writer_queue = list_concat(queue, writer_queue);
	ret = list_length(writer_queue);
	pthread_mutex_unlock(&writer_queue_lock);

	/* update last used time of the connection. */
	if (connection_used)
		writer_conn_last_used = time(NULL);

	return ret;
}

/*
 * connect to repository server.
 */
static PGconn *
writer_connect(bool superuser)
{
	char	info[1024];
	int		retry = 0;

	if (superuser)
#ifdef DEBUG_MODE
		snprintf(info, lengthof(info), "%s", my_repository_server);
#else
		snprintf(info, lengthof(info),
			"%s options='-c log_statement=none'", my_repository_server);
#endif
	else
		snprintf(info, lengthof(info), "%s", my_repository_server);

	do
	{
		if (do_connect(&writer_conn, info, NULL))
			return writer_conn;
		writer_delay();
	} while (shutdown_state < SHUTDOWN_REQUESTED && ++retry < DB_MAX_RETRY);

	return NULL;
}

/*
 * disconnect from the repository server.
 */
static void
writer_disconnect(void)
{
	pgut_disconnect(writer_conn);
	writer_conn = NULL;
}

static char *
get_instid(PGconn *conn)
{
	PGresult	   *res = NULL;
	const char	   *params[9];
	char		   *instid;

	if (pgut_command(conn, "BEGIN TRANSACTION READ WRITE", 0, NULL) != PGRES_COMMAND_OK)
		goto error;

	params[0] = instance_id;
	params[1] = get_nodename();
	params[2] = postmaster_port;

	res = pgut_execute(conn,
			"SELECT instid, pg_version FROM statsrepo.instance"
			" WHERE name = $1 AND hostname = $2 AND port = $3",
			3, params);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		goto error;

	if (PQntuples(res) > 0)
	{
		/* already registered instance */
		bool	modified;

		instid = pgut_strdup(PQgetvalue(res, 0, 0));
		modified = (strcmp(server_version_string, PQgetvalue(res, 0, 1)) != 0);
		PQclear(res);

		if (modified)
		{
			params[0] = server_version_string;
			params[1] = instid;
			pgut_command(conn,
				"UPDATE statsrepo.instance SET pg_version = $1"
				" WHERE instid = $2",
				2, params);
		}
	}
	else
	{
		/* register as a new instance */
		char	page_size_str[32];
		char	xlog_file_size_str[32];
		char	page_header_size_str[32];
		char	htup_header_size_str[32];
		char	item_id_size_str[32];

		snprintf(page_size_str,
			sizeof(page_size_str), "%u", page_size);
#if PG_VERSION_NUM >= 90300
		snprintf(xlog_file_size_str,
			sizeof(xlog_file_size_str), UINT64_FORMAT,
			(UINT64CONST(0x100000000) / xlog_seg_size) * xlog_seg_size);
#else
		snprintf(xlog_file_size_str,
			sizeof(xlog_file_size_str), "%u",
			(((uint32) 0xffffffff) / xlog_seg_size) * xlog_seg_size);
#endif
		snprintf(page_header_size_str,
			sizeof(page_header_size_str), "%d", page_header_size);
		snprintf(htup_header_size_str,
			sizeof(htup_header_size_str), "%d", htup_header_size);
		snprintf(item_id_size_str,
			sizeof(item_id_size_str), "%d", item_id_size);
		PQclear(res);

		params[3] = server_version_string;
		params[4] = xlog_file_size_str;
		params[5] = page_size_str;
		params[6] = page_header_size_str;
		params[7] = htup_header_size_str;
		params[8] = item_id_size_str;
		res = pgut_execute(conn,
			"INSERT INTO statsrepo.instance "
			"(name, hostname, port, pg_version, xlog_file_size, page_size,"
			" page_header_size, htup_header_size, item_id_size) "
			"VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING instid",
			lengthof(params), params);
		if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) < 1)
			goto error;

		instid = pgut_strdup(PQgetvalue(res, 0, 0));
		PQclear(res);
		res = NULL;
	}

	if (!pgut_commit(conn))
		goto error;

	return instid;

error:
	PQclear(res);
	pgut_rollback(conn);
	return NULL;
}

static const char *
get_nodename(void)
{
#ifndef WIN32
	static struct utsname	name;

	if (!name.nodename[0])
	{
		if (uname(&name) < 0)
			strlcpy(name.nodename, "unknown", lengthof(name.nodename));
	}

	return name.nodename;
#else
	static char nodename[MAX_PATH];

	if (!nodename[0])
	{
		DWORD bufsize = lengthof(nodename);
		if (!GetComputerNameA(nodename, &bufsize))
			strlcpy(nodename, "unknown", lengthof(nodename));
	}

	return nodename;
#endif
}

static void
destroy_writer_queue(QueueItem *item)
{
	item->free(item);
}

static void
set_connect_privileges(void)
{
	PGresult	*res;

	Assert(writer_conn != NULL);

	res = pgut_execute(writer_conn,
		"SELECT rolsuper FROM pg_roles WHERE rolname = current_user", 0, NULL);

	if (PQresultStatus(res) == PGRES_TUPLES_OK &&
		PQntuples(res) > 0 &&
		strcmp(PQgetvalue(res, 0, 0), "t") == 0)
		superuser_connect = true;
	else
		superuser_connect = false;

	PQclear(res);
}

static RepositoryState
validate_repository(void)
{
	/* disconnect the current connection */
	writer_disconnect();

	/* connect to repository server */
	if (writer_connect(false) == NULL)
		return REPOSITORY_CONNECT_FAILURE;

	/* get a privileges of the database connect user */
	set_connect_privileges();

	/*
	 * re-connect to repository server
	 * if privileges of the database connect user is superuser.
	 */
	if (superuser_connect)
	{
		writer_disconnect();
		if (writer_connect(superuser_connect) == NULL)
			return REPOSITORY_CONNECT_FAILURE;
	}

	/* check the condition of repository server */
	if (!check_repository(writer_conn))
		return REPOSITORY_INVALID_DATABASE;

	/* install statsrepo schema if not installed yet */
	if (!ensure_schema(writer_conn, "statsrepo"))
		return REPOSITORY_INVALID_DATABASE;

	/* update last used time of the connection */
	writer_conn_last_used = time(NULL);

	return REPOSITORY_OK;
}

/*
 * check the condition of repository server.
 *  - statsrepo schema version
 * Note:
 * Not use pgut_execute() in this function, because don't want to write
 * the error message defined by pgut_execute().
 */
static bool
check_repository(PGconn *conn)
{
	PGresult	*res;
	char		*query;
	uint32		 version;

	/* check statsrepo schema exists */
	query = "SELECT nspname FROM pg_namespace WHERE nspname = 'statsrepo'";
	res = PQexec(conn, query);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
		goto error;	/* query failed */
	if (PQntuples(res) == 0)
		goto ok;	/* not installed */
	PQclear(res);

	/* check the statsrepo schema version */
	query = "SELECT statsrepo.get_version()";
	res = PQexec(conn, query);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		char *sqlstate;

		sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
		if (sqlstate && strcmp(sqlstate, "42883") == 0)	/* undefined function */
		{
			ereport(ERROR,
				(errmsg("incompatible statsrepo schema: version mismatch")));
			goto bad;
		}
		goto error;	/* query failed */
	}

	/* verify the version of statsrepo schema */
	parse_uint32(PQgetvalue(res, 0, 0), &version);
	if (version != STATSREPO_SCHEMA_VERSION &&
		((version / 100) != (STATSREPO_SCHEMA_VERSION / 100)))
	{
		ereport(ERROR,
			(errmsg("incompatible statsrepo schema: version mismatch")));
		goto bad;
	}

ok:
	PQclear(res);
	return true;

bad:
	PQclear(res);
	return false;

error:
	ereport(ERROR,
		(errmsg("query failed: %s", PQerrorMessage(conn)),
		 errdetail("query was: %s", query)));
	PQclear(res);
	return false;
}

static void
set_writer_state(WriterState state)
{
	switch (state)
	{
		case WRITER_READY:
			break;
		case WRITER_NORMAL:
			elog(writer_state > WRITER_NORMAL ? LOG : DEBUG2,
				"pg_statsinfo is starting in normal mode");
			break;
		case WRITER_FALLBACK:
			elog(writer_state < WRITER_FALLBACK ? LOG : DEBUG2,
				"pg_statsinfo is starting in fallback mode");
			break;
	}

	writer_state = state;
}

static void
writer_delay(void)
{
	if (shutdown_state < SHUTDOWN_REQUESTED)
		sleep(WRITER_RECONNECT_DELAY);
}
