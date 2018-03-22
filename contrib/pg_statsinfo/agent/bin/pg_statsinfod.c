/*
 * pg_statsinfod.c
 *
 * Copyright (c) 2009-2018, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "pg_statsinfod.h"

#include <fcntl.h>
#include <sys/stat.h> 

const char *PROGRAM_VERSION	= "10.0";
const char *PROGRAM_URL		= "http://pgstatsinfo.sourceforge.net/";
const char *PROGRAM_EMAIL   = NULL;

typedef struct ParamMap
{
	char	   *name;
	bool	  (*assign)(const char *value, void *var);
	void	   *value;
} ParamMap;

/*---- system variables ----*/
char		   *instance_id;			/* system identifier */
static pid_t	postmaster_pid;			/* postmaster's pid */
char		   *postmaster_port;		/* postmaster port number as string */
static char	   *share_path;				/* $PGHOME/share */
int				server_version_num;		/* PG_VERSION_NUM */
char		   *server_version_string;	/* PG_VERSION */
int				server_encoding = -1;	/* server character encoding */
char		   *log_timezone_name;
int				page_size;				/* page size */
int				xlog_seg_size;			/* size of each WAL segment */
int				page_header_size;		/* page header size */
int				htup_header_size;		/* tuple header size */
int				item_id_size;			/* itemid size */
pid_t			sil_pid;				/* pg_statsinfo launcher's pid */
/*---- GUC variables (collector) -------*/
char		   *data_directory;
char		   *excluded_dbnames;
char		   *excluded_schemas;
char		   *stat_statements_max;
char		   *stat_statements_exclude_users;
int				sampling_interval;
int				snapshot_interval;
int				enable_maintenance;
time_t			maintenance_time;
int				repository_keepday;
int				repolog_keepday;
char		   *log_maintenance_command;
bool			enable_alert;
char		   *target_server;
/*---- GUC variables (logger) ----------*/
char		   *log_directory;
char		   *log_error_verbosity;
int				syslog_facility;
char		   *syslog_ident;
char		   *syslog_line_prefix;
int				syslog_min_messages;
char		   *textlog_filename;
char		   *textlog_line_prefix;
int				textlog_min_messages;
int				textlog_permission;
int				repolog_min_messages;
int				repolog_buffer;
int				repolog_interval;
bool			adjust_log_level;
char		   *adjust_log_info;
char		   *adjust_log_notice;
char		   *adjust_log_warning;
char		   *adjust_log_error;
char		   *adjust_log_log;
char		   *adjust_log_fatal;
char		   *textlog_nologging_users;
char		   *repolog_nologging_users;
int				controlfile_fsync_interval;
/*---- GUC variables (writer) ----------*/
char		   *repository_server;
/*---- message format ----*/
char		   *msg_debug;
char		   *msg_info;
char		   *msg_notice;
char		   *msg_log;
char		   *msg_warning;
char		   *msg_error;
char		   *msg_fatal;
char		   *msg_panic;
char		   *msg_shutdown;
char		   *msg_shutdown_smart;
char		   *msg_shutdown_fast;
char		   *msg_shutdown_immediate;
char		   *msg_sighup;
char		   *msg_autovacuum;
char		   *msg_autoanalyze;
char		   *msg_checkpoint_starting;
char		   *msg_checkpoint_complete;
char		   *msg_restartpoint_complete;
/*--------------------------------------*/

/* current shutdown state */
volatile ShutdownState	shutdown_state;
static pthread_mutex_t	shutdown_state_lock;

/* current writer state */
volatile WriterState	writer_state;

/* threads */
pthread_t	th_collector;
pthread_t	th_writer;
pthread_t	th_logger;
pthread_t	th_logger_send;

/* signal flag  */
volatile bool	got_SIGHUP = false;

static int help(void);
static bool assign_int(const char *value, void *var);
static bool assign_elevel(const char *value, void *var);
static bool assign_syslog(const char *value, void *var);
static bool assign_string(const char *value, void *var);
static bool assign_bool(const char *value, void *var);
static bool assign_time(const char *value, void *var);
static bool assign_enable_maintenance(const char *value, void *var);
static void readopt(void);
static bool decode_time(const char *field, int *hour, int *min, int *sec);
static int strtoi(const char *nptr, char **endptr, int base);
static bool execute_script(PGconn *conn, const char *script_file);
static void create_lock_file(void);
static void unlink_lock_file(void);
static void load_control_file(ControlFile *ctrl);
static void sighup_handler(SIGNAL_ARGS);
static void check_agent_process(const char *lockfile);

/* parameters */
static struct ParamMap PARAM_MAP[] =
{
	{"instance_id", assign_string, &instance_id},
	{"postmaster_pid", assign_int, &postmaster_pid},
	{"port", assign_string, &postmaster_port},
	{"share_path", assign_string, &share_path},
	{"server_version_num", assign_int, &server_version_num},
	{"server_version_string", assign_string, &server_version_string},
	{"server_encoding", assign_int, &server_encoding},
	{"data_directory", assign_string, &data_directory},
	{"log_timezone", assign_string, &log_timezone_name},
	{"log_directory", assign_string, &log_directory},
	{"log_error_verbosity", assign_string, &log_error_verbosity},
	{"syslog_facility", assign_syslog, &syslog_facility},
	{"syslog_ident", assign_string, &syslog_ident},
	{"page_size", assign_int, &page_size},
	{"xlog_seg_size", assign_int, &xlog_seg_size},
	{"page_header_size", assign_int, &page_header_size},
	{"htup_header_size", assign_int, &htup_header_size},
	{"item_id_size", assign_int, &item_id_size},
	{"sil_pid", assign_int, &sil_pid},
	{GUC_PREFIX ".excluded_dbnames", assign_string, &excluded_dbnames},
	{GUC_PREFIX ".excluded_schemas", assign_string, &excluded_schemas},
	{GUC_PREFIX ".stat_statements_max", assign_string, &stat_statements_max},
	{GUC_PREFIX ".stat_statements_exclude_users", assign_string, &stat_statements_exclude_users},
	{GUC_PREFIX ".repository_server", assign_string, &repository_server},
	{GUC_PREFIX ".sampling_interval", assign_int, &sampling_interval},
	{GUC_PREFIX ".snapshot_interval", assign_int, &snapshot_interval},
	{GUC_PREFIX ".syslog_line_prefix", assign_string, &syslog_line_prefix},
	{GUC_PREFIX ".syslog_min_messages", assign_elevel, &syslog_min_messages},
	{GUC_PREFIX ".textlog_min_messages", assign_elevel, &textlog_min_messages},
	{GUC_PREFIX ".textlog_filename", assign_string, &textlog_filename},
	{GUC_PREFIX ".textlog_line_prefix", assign_string, &textlog_line_prefix},
	{GUC_PREFIX ".textlog_permission", assign_int, &textlog_permission},
	{GUC_PREFIX ".repolog_min_messages", assign_elevel, &repolog_min_messages},
	{GUC_PREFIX ".repolog_buffer", assign_int, &repolog_buffer},
	{GUC_PREFIX ".repolog_interval", assign_int, &repolog_interval},
	{GUC_PREFIX ".adjust_log_level", assign_bool, &adjust_log_level},
	{GUC_PREFIX ".adjust_log_info", assign_string, &adjust_log_info},
	{GUC_PREFIX ".adjust_log_notice", assign_string, &adjust_log_notice},
	{GUC_PREFIX ".adjust_log_warning", assign_string, &adjust_log_warning},
	{GUC_PREFIX ".adjust_log_error", assign_string, &adjust_log_error},
	{GUC_PREFIX ".adjust_log_log", assign_string, &adjust_log_log},
	{GUC_PREFIX ".adjust_log_fatal", assign_string, &adjust_log_fatal},
	{GUC_PREFIX ".textlog_nologging_users", assign_string, &textlog_nologging_users},
	{GUC_PREFIX ".repolog_nologging_users", assign_string, &repolog_nologging_users},
	{GUC_PREFIX ".enable_maintenance", assign_enable_maintenance, &enable_maintenance},
	{GUC_PREFIX ".maintenance_time", assign_time, &maintenance_time},
	{GUC_PREFIX ".repository_keepday", assign_int, &repository_keepday},
	{GUC_PREFIX ".repolog_keepday", assign_int, &repolog_keepday},
	{GUC_PREFIX ".log_maintenance_command", assign_string, &log_maintenance_command},
	{GUC_PREFIX ".controlfile_fsync_interval", assign_int, &controlfile_fsync_interval},
	{GUC_PREFIX ".enable_alert", assign_bool, &enable_alert},
	{GUC_PREFIX ".target_server", assign_string, &target_server},
	{":debug", assign_string, &msg_debug},
	{":info", assign_string, &msg_info},
	{":notice", assign_string, &msg_notice},
	{":log", assign_string, &msg_log},
	{":warning", assign_string, &msg_warning},
	{":error", assign_string, &msg_error},
	{":fatal", assign_string, &msg_fatal},
	{":panic", assign_string, &msg_panic},
	{":shutdown", assign_string, &msg_shutdown},
	{":shutdown_smart", assign_string, &msg_shutdown_smart},
	{":shutdown_fast", assign_string, &msg_shutdown_fast},
	{":shutdown_immediate", assign_string, &msg_shutdown_immediate},
	{":sighup", assign_string, &msg_sighup},
	{":autovacuum", assign_string, &msg_autovacuum},
	{":autoanalyze", assign_string, &msg_autoanalyze},
	{":checkpoint_starting", assign_string, &msg_checkpoint_starting},
	{":checkpoint_complete", assign_string, &msg_checkpoint_complete},
	{":restartpoint_complete", assign_string, &msg_restartpoint_complete},
	{NULL}
};

static int
isTTY(int fd)
{
#ifndef WIN32
	return isatty(fd);
#else
	return !GetNamedPipeInfo((HANDLE) _get_osfhandle(fd), NULL, NULL, NULL, NULL);
#endif
}

int
main(int argc, char *argv[])
{
	ControlFile	 ctrl;

	shutdown_state = STARTUP;

	pgut_init(argc, argv);

	/* stdin must be pipe from server */
	if (isTTY(fileno(stdin)))
		return help();

	/* setup signal handler */
	pqsignal(SIGHUP, sighup_handler);
#if PG_VERSION_NUM >= 90300
	pqsignal(SIGTERM, SIG_IGN);	/* for background worker */
	pqsignal(SIGQUIT, SIG_IGN);	/* for background worker */
#endif

	/* read required parameters */
	readopt();

	if (instance_id == NULL ||
		postmaster_pid == 0 ||
		postmaster_port == NULL ||
		!pg_valid_server_encoding_id(server_encoding) ||
		data_directory == NULL ||
		log_directory == NULL ||
		share_path == NULL ||
		msg_shutdown == NULL ||
		msg_shutdown_smart == NULL ||
		msg_shutdown_fast == NULL ||
		msg_shutdown_immediate == NULL ||
		msg_sighup == NULL ||
		msg_autovacuum == NULL ||
		msg_autoanalyze == NULL)
	{
		ereport(FATAL,
			(errcode(EINVAL),
			 errmsg("cannot run without required parameters")));
	}

	/* check major version */
	if (server_version_num / 100 != PG_VERSION_NUM / 100)
	{
		ereport(FATAL,
			(errcode(EINVAL),
			 errmsg("incompatible server: version mismatch"),
			 errdetail("Server is version %d, %s was built with version %d",
					   server_version_num, PROGRAM_NAME, PG_VERSION_NUM)));
	}

#if defined(USE_DAEMON) && !defined(WIN32)
	/*
	 * Run as a daemon to avoid postmaster's crash even if the statsinfo
	 * process will crash.
	 */
	if (daemon(true, false) != 0)
		ereport(PANIC,
			(errcode_errno(),
			 errmsg("could not run as a daemon: ")));
#endif

	/* setup libpq default parameters */
	pgut_putenv("PGCONNECT_TIMEOUT", "2");
	pgut_putenv("PGCLIENTENCODING", pg_encoding_to_char(server_encoding));

	/* create lock file */
	create_lock_file();

	/*
	 * set the abort level to FATAL so that the daemon should not be
	 * terminated by ERRORs.
	 */
	pgut_abort_level = FATAL;
	pgut_abort_code = STATSINFO_EXIT_FAILED;

	/* init logger, collector, and writer module */
	pthread_mutex_init(&shutdown_state_lock, NULL);
	collector_init();
	writer_init();
	logger_init();

	/* load control file */
	shutdown_state = RUNNING;
	elog(LOG, "start");
	load_control_file(&ctrl);

	/* run the modules in each thread */
	pthread_create(&th_collector, NULL, collector_main, NULL);
	pthread_create(&th_writer, NULL, writer_main, NULL);
	pthread_create(&th_logger, NULL, logger_main, &ctrl);
	pthread_create(&th_logger_send, NULL, logger_send_main, &ctrl);

	/* join the threads */ 
	pthread_join(th_collector, (void **) NULL);
	pthread_join(th_writer, (void **) NULL);
	pthread_join(th_logger, (void **) NULL);
	pthread_join(th_logger_send, (void **) NULL);

	CloseControlFile();

	return STATSINFO_EXIT_SUCCESS;
}

static int
help(void)
{
	printf("%s %s (built with %s)\n",
		PROGRAM_NAME, PROGRAM_VERSION, PACKAGE_STRING);
	printf("  This program must be launched by PostgreSQL server.\n");
	printf("  Add 'pg_statsinfo' to shared_preload_libraries in postgresql.conf.\n");
	printf("\n");
	printf("Read the website for details. <%s>\n", PROGRAM_URL);

	return 1;
}

bool
postmaster_is_alive(void)
{
#ifdef WIN32
	static HANDLE hProcess = NULL;

	if (hProcess == NULL)
	{
		hProcess = OpenProcess(SYNCHRONIZE, false, postmaster_pid);
		if (hProcess == NULL)
			elog(WARNING, "cannot open process (pid=%u): %d", postmaster_pid, GetLastError());
	}
	return WaitForSingleObject(hProcess, 0) == WAIT_TIMEOUT;
#else
	return kill(postmaster_pid, 0) == 0;
#endif
}

/*
 * convert an error level string to an enum value.
 */
int
str_to_elevel(const char *value)
{
	if (msg_debug)
	{
		if (pg_strcasecmp(value, msg_debug) == 0)
			return DEBUG;
		else if (pg_strcasecmp(value, msg_info) == 0)
			return INFO;
		else if (pg_strcasecmp(value, msg_notice) == 0)
			return NOTICE;
		else if (pg_strcasecmp(value, msg_log) == 0)
			return LOG;
		else if (pg_strcasecmp(value, msg_warning) == 0)
			return WARNING;
		else if (pg_strcasecmp(value, msg_error) == 0)
			return ERROR;
		else if (pg_strcasecmp(value, msg_fatal) == 0)
			return FATAL;
		else if (pg_strcasecmp(value, msg_panic) == 0)
			return PANIC;
	}

	if (pg_strcasecmp(value, "ALERT") == 0)
		return ALERT;
	else if (pg_strcasecmp(value, "DISABLE") == 0)
		return DISABLE;
	else
		return parse_elevel(value);
}

/* additionally support ALERT and DISABLE */
const char *
elevel_to_str(int elevel)
{
	if (msg_debug)
	{
		switch (elevel)
		{
		case DEBUG5:
		case DEBUG4:
		case DEBUG3:
		case DEBUG2:
		case DEBUG1:
			return msg_debug;
		case LOG:
			return msg_log;
		case INFO:
			return msg_info;
		case NOTICE:
			return msg_notice;
		case WARNING:
			return msg_warning;
		case COMMERROR:
		case ERROR:
			return msg_error;
		case FATAL:
			return msg_fatal;
		case PANIC:
			return msg_panic;
		}
	}

	switch (elevel)
	{
	case ALERT:
		return "ALERT";
	case DISABLE:
		return "DISABLE";
	default:
		return format_elevel(elevel);
	}
}

/*
 * Connect to the database and install schema if not installed yet.
 * Returns the same value with *conn.
 */
PGconn *
do_connect(PGconn **conn, const char *info, const char *schema)
{
	/* skip reconnection if connected to the database already */
	if (PQstatus(*conn) == CONNECTION_OK)
		return *conn;

	pgut_disconnect(*conn);
	*conn = pgut_connect(info, NO, ERROR);

	if (PQstatus(*conn) == CONNECTION_OK)
	{
		/* adjust setting parameters */
		pgut_command(*conn,
			"SET search_path = 'pg_catalog', 'public'", 0, NULL);

		/* install required schema if requested */
		if (ensure_schema(*conn, schema))
			return *conn;
	}

	/* connection failed */
	pgut_disconnect(*conn);
	*conn = NULL;
	return NULL;
}

/*
 * requires $PGDATA/contrib/pg_{schema}.sql
 */
bool
ensure_schema(PGconn *conn, const char *schema)
{
	PGresult	   *res;
	bool			ok;
	char			path[MAXPGPATH];

	if (!schema || !schema[0])
		return true;	/* no schema required */

	/* check statsinfo schema exists */
	res = pgut_execute(conn,
			"SELECT nspname FROM pg_namespace WHERE nspname = $1",
			1, &schema);
	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		PQclear(res);
		return false;
	}
	ok = (PQntuples(res) > 0);
	PQclear(res);
	/* TODO: check installed schema version */
	if (ok)
		return true;	/* ok, installed */

	/* iff schema is "statsrepo", create language 'PL/pgSQL' */
	if (strcmp(schema, "statsrepo") == 0)
	{
		bool	installed;

		res = pgut_execute(conn,
			"SELECT 1 FROM pg_language WHERE lanname = 'plpgsql'", 0, NULL);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			PQclear(res);
			return false;
		}
		installed = (PQntuples(res) > 0);
		PQclear(res);
		if (!installed)
		{
			if (pgut_command(conn,
				"CREATE LANGUAGE plpgsql", 0, NULL) != PGRES_COMMAND_OK)
				return false;
		}
	}

	/* execute script $PGSHARE/contrib/pg_{schema}.sql */
	snprintf(path, MAXPGPATH, "%s/contrib/pg_%s.sql", share_path, schema);
	elog(LOG, "installing schema: %s", schema);
	if (!execute_script(conn, path))
		return false;

	/* execute script $PGSHARE/contrib/pg_statsrepo.alert.sql */
	if (strstr(schema, "statsrepo") != NULL)
	{
		snprintf(path, MAXPGPATH, "%s/contrib/pg_statsrepo_alert.sql", share_path);
		if (!execute_script(conn, path))
			return false;
	}
	return true;
}

/*
 * set shutdown state
 */
void
shutdown_progress(ShutdownState state)
{
	pthread_mutex_lock(&shutdown_state_lock);

	if (shutdown_state < state)
		shutdown_state = state;

	pthread_mutex_unlock(&shutdown_state_lock);
}

static bool
assign_int(const char *value, void *var)
{
	return parse_int32(value, (int32 *) var);
}

static bool
assign_bool(const char *value, void *var)
{
	return parse_bool(value, var);
}

static bool
assign_time(const char *value, void *var)
{
	struct tm	*tm;
	time_t		 now;
	int			 hour, min, sec;

	if (!decode_time(value, &hour, &min, &sec))
		return false;

	now = time(NULL);
	tm = localtime(&now);
	tm->tm_hour = hour;
	tm->tm_min = min;
	tm->tm_sec = sec;

	*(time_t *)var = mktime(tm);
	return true;
}

static bool
assign_elevel(const char *value, void *var)
{
	*((int *) var) = str_to_elevel(value);
	return true;
}

static bool
assign_syslog(const char *value, void *var)
{
	sscanf(value, "local%d", (int *) var);
	if (*(int *) var < 0 || 7 < *(int *) var)
		*(int *) var = 0;
	return true;
}

static bool
assign_enable_maintenance(const char *value, void *var)
{
	char	*rawstring;
	char	*tok;
	bool	 bool_val;
	int		 mode = 0x00;
	int		 tok_len;

	if (parse_bool(value, &bool_val))
	{
		if (bool_val)
		{
			mode |= MAINTENANCE_MODE_SNAPSHOT;
			mode |= MAINTENANCE_MODE_LOG;
			mode |= MAINTENANCE_MODE_REPOLOG;
		}
		*(int *) var = mode;
		return true;
	}

	rawstring = pgut_strdup(value);
	tok = strtok(rawstring, ",");
	while (tok)
	{
		/* trim leading and trailing whitespace */
		while (*tok && isspace((unsigned char) *tok))
			tok++;
		tok_len = strlen(tok);
		while (tok_len > 0 && isspace((unsigned char) tok[tok_len - 1]))
			tok_len--;
		tok[tok_len] = '\0';

		if (pg_strcasecmp(tok, "snapshot") == 0)
			mode |= MAINTENANCE_MODE_SNAPSHOT;
		else if (pg_strcasecmp(tok, "log") == 0)
			mode |= MAINTENANCE_MODE_LOG;
		else if (pg_strcasecmp(tok, "repolog") == 0)
			mode |= MAINTENANCE_MODE_REPOLOG;
		else
		{
			free(rawstring);
			return false;
		}
		tok = strtok(NULL, ",");
	}

	free(rawstring);
	*(int *) var = mode;
	return true;
}

static bool
assign_string(const char *value, void *var)
{
	free(*(char **)var);
	*(char **)var = pgut_strdup(value);
	return true;
}

static bool
assign_param(const char *name, const char *value)
{
	ParamMap   *param;

	for (param = PARAM_MAP; param->name; param++)
	{
		if (strcmp(name, param->name) == 0)
			break;
	}
	if (param->name == NULL || !param->assign(value, param->value))
	{
		ereport(ERROR,
			(errcode(EINVAL),
			 errmsg("unexpected parameter: %s = %s", name, value)));
		return false;
	}

	return true;
}

/*
 * read required parameters.
 */
static void
readopt(void)
{
	/* read required parameters from stdin */
	readopt_from_file(stdin);

	Assert(postmaster_port);

	if (!(enable_maintenance & MAINTENANCE_MODE_SNAPSHOT))
		elog(NOTICE,
			"automatic maintenance is disable. Please note the data size of the repository");
}

/*
 * Assign options from file. The file format must be:
 *	uint32	name_size
 *	char	name[name_size]
 *	uint32	value_size
 *	char	value[value_size]
 */
void
readopt_from_file(FILE *fp)
{
	StringInfoData	name;
	StringInfoData	value;

	initStringInfo(&name);
	initStringInfo(&value);

	for (;;)
	{
		uint32			size;

		resetStringInfo(&name);
		resetStringInfo(&value);

		/* name-size */
		if (fread(&size, 1, sizeof(size), fp) != sizeof(size))
			goto error;
		if (size == 0)
			goto done;	/* EOF */

		/* name */
		enlargeStringInfo(&name, size);
		if (fread(name.data, 1, size, fp) != size)
			goto error;
		name.data[name.len = size] = '\0';

		/* value-size */
		if (fread(&size, 1, sizeof(size), fp) != sizeof(size))
			goto error;

		/* value */
		enlargeStringInfo(&value, size);
		if (fread(value.data, 1, size, fp) != size)
			goto error;
		value.data[value.len = size] = '\0';

		assign_param(name.data, value.data);
	}

error:
	ereport(ERROR,
		(errcode(EINVAL),
		 errmsg("invalid option file")));
done:
	termStringInfo(&name);
	termStringInfo(&value);
}

/*
 * format of res must be (name text, value text).
 */
void
readopt_from_db(PGresult *res)
{
	int			r;
	int			rows;

	rows = PQntuples(res);
	for (r = 0; r < rows; r++)
	{
		const char *name = PQgetvalue(res, r, 0);
		const char *value = PQgetvalue(res, r, 1);

		assign_param(name, value);
	}

	if (!(enable_maintenance & MAINTENANCE_MODE_SNAPSHOT))
		elog(NOTICE,
			"automatic maintenance is disable. Please note the data size of the repository");
}

/*
 * delay unless shutdown is requested.
 */
void
delay(void)
{
	if (shutdown_state < SHUTDOWN_REQUESTED)
		sleep(1);
}

/*
 * get local timestamp by character string
 * return format : "YYYY-MM-DD HH:MM:SS.FFFFFF"
 */
char *
getlocaltimestamp(void)
{
#ifndef WIN32
	struct timeval	 tv;
	struct tm		*ts;
#else
	SYSTEMTIME		 stTime;
#endif
	char			*tp;

	if ((tp = (char *)pgut_malloc((size_t)32)) == NULL)
		return NULL;

	memset(tp, 0x00, 32);

#ifndef WIN32
	if (gettimeofday(&tv, NULL) != 0)
	{
		free(tp);
		ereport(ERROR,
			(errcode_errno(),
			 errmsg("gettimeofday function call failed")));
		return NULL;
	}

	if ((ts = localtime(&tv.tv_sec)) == NULL)
	{
		free(tp);
		ereport(ERROR,
			(errcode_errno(),
			 errmsg("localtime function call failed")));
		return NULL;
	}

	snprintf(tp, 32, "%04d-%02d-%02d %02d:%02d:%02d.%06ld",
			ts->tm_year + 1900,
			ts->tm_mon + 1,
			ts->tm_mday,
			ts->tm_hour,
			ts->tm_min,
			ts->tm_sec,
			tv.tv_usec);
#else
	GetLocalTime(&stTime);

	snprintf(tp, 32, "%04d-%02d-%02d %02d:%02d:%02d.%06ld",
			stTime.wYear,
			stTime.wMonth,
			stTime.wDay,
			stTime.wHour,
			stTime.wMinute,
			stTime.wSecond,
			stTime.wMilliseconds);

#endif   /* WIN32 */

	return tp;
}

time_t
get_next_time(time_t now, int interval)
{
	if (interval > 0)
		return (now / interval) * interval + interval;
	else
		return now;
}

static bool
decode_time(const char *field, int *hour, int *min, int *sec)
{
	char	*cp;

	errno = 0;
	*hour = strtoi(field, &cp, 10);
	if (errno == ERANGE || *cp != ':')
		return false;
	errno = 0;
	*min = strtoi(cp + 1, &cp, 10);
	if (errno == ERANGE)
		return false;
	if (*cp == '\0')
		*sec = 0;
	else if (*cp == ':')
	{
		errno = 0;
		*sec = strtoi(cp + 1, &cp, 10);
		if (errno == ERANGE || *cp != '\0')
			return false;
	}
	else
		return false;

	/* sanity check */
	if (*hour < 0 || *hour > 23 ||
		*min < 0 || *min > 59 ||
		*sec < 0 || *sec > 59)
		return false;

	return true;
}

static int
strtoi(const char *nptr, char **endptr, int base)
{
	long		val;

	val = strtol(nptr, endptr, base);
#ifdef HAVE_LONG_INT_64
	if (val != (long) ((int32) val))
		errno = ERANGE;
#endif
	return (int) val;
}

static bool
execute_script(PGconn *conn, const char *script_file)
{
	FILE		   *fp;
	StringInfoData	buf;
	bool			ok;

	/* read script into buffer. */
	if ((fp = pgut_fopen(script_file, "rt")) == NULL)
		return false;
	initStringInfo(&buf);
	if ((errno = appendStringInfoFile(&buf, fp)) == 0)
	{
		/* execute the read script contents. */
		switch (pgut_command(conn, buf.data, 0, NULL))
		{
			case PGRES_COMMAND_OK:
			case PGRES_TUPLES_OK:
				ok = true;
				break;
			default:
				ok = false;
				break;
		}
	}
	else
	{
		ereport(ERROR,
			(errcode_errno(),
			 errmsg("could not read file \"%s\": ", script_file)));
		ok = false;
	}

	fclose(fp);
	termStringInfo(&buf);
	return ok;
}

/*
 * create the lock file.
 * store our own PID in the lock file.
 */
static void
create_lock_file(void)
{
	int		fd;
	char	lockfile[MAXPGPATH];
	char	buffer[64];
	pid_t	my_pid;

	my_pid = getpid();

	join_path_components(lockfile, data_directory, STATSINFO_LOCK_FILE);

	/* create the lock file */
	fd = open(lockfile, O_WRONLY | O_CREAT | O_EXCL, 0600);
	if (fd < 0)
	{
		if (errno != EEXIST && errno != EACCES)
			ereport(FATAL,
				(errcode_errno(),
				 errmsg("could not create lock file \"%s\": %m", lockfile)));

		/*
		 * if lock file already exists, do the check to avoid multiple boot.
		 * and if previous process still alive, terminate it.
		 * Note:
		 * lock file is removed when the previous agent terminate.
		 * so, create the lock file when after confirming
		 * the previous agent terminate.
		 */
		check_agent_process(lockfile);

		/* create the lock file */
		fd = open(lockfile, O_WRONLY | O_CREAT, 0600);
		if (fd < 0)
			ereport(FATAL,
				(errcode_errno(),
				 errmsg("could not create lock file \"%s\": %m", lockfile)));
	}

	/* write content to the lock file */
	snprintf(buffer, sizeof(buffer), "%d\n%d\n", my_pid, postmaster_pid);

	errno = 0;
	if (write(fd, buffer, strlen(buffer)) != strlen(buffer))
	{
		int		save_errno = errno;

		close(fd);
		unlink(lockfile);
		/* if write didn't set errno, assume problem is no disk space */
		errno = save_errno ? save_errno : ENOSPC;
		ereport(FATAL,
			(errcode_errno(),
			 errmsg("could not write lock file \"%s\": %m", lockfile)));
	}
	if (fsync(fd) != 0)
	{
		int		save_errno = errno;

		close(fd);
		unlink(lockfile);
		errno = save_errno;
		ereport(FATAL,
			(errcode_errno(),
			 errmsg("could not write lock file \"%s\": %m", lockfile)));
	}
	if (close(fd) != 0)
	{
		int		save_errno = errno;

		unlink(lockfile);
		errno = save_errno;
		ereport(FATAL,
			(errcode_errno(),
			 errmsg("could not write lock file \"%s\": %m", lockfile)));
	}

	/* automatic removal of the lock file at exit */
	atexit(unlink_lock_file);
}

/*
 * atexit() callback to remove a lock file.
 */
static void
unlink_lock_file(void)
{
	char	lockfile[MAXPGPATH];

	join_path_components(lockfile, data_directory, STATSINFO_LOCK_FILE);

	if (unlink(lockfile) != 0)
	{
		if (errno != ENOENT)
			ereport(FATAL,
				(errcode_errno(),
				 errmsg("could not remove lock file \"%s\": %m",
				 	lockfile)));
	}
}

/*
 * load control file.
 */
static void
load_control_file(ControlFile *ctrl)
{
	struct stat		st;

	if (stat(STATSINFO_CONTROL_FILE, &st) != 0)
	{
		OpenControlFile(O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR);
		InitControlFile(ctrl);
	}
	else
	{
		OpenControlFile(O_RDWR, 0);
		if (!ReadControlFile(ctrl))
			InitControlFile(ctrl);
	}
}

static void
sighup_handler(SIGNAL_ARGS)
{
	got_SIGHUP = true;
}

/*
 * check the existence of the previous agent process
 * and terminate it if still alive.
 */
static void
check_agent_process(const char *lockfile)
{
	FILE	*fp;
	pid_t	 lock_si_pid;
	pid_t	 lock_pg_pid;
	int		 retry;

	/* read lock file */
	fp = fopen(lockfile, "r");
	if (fp == NULL)
		ereport(FATAL,
			(errcode_errno(),
			 errmsg("could not open lock file \"%s\": %m", lockfile)));

	errno = 0;
	if (fscanf(fp, "%d\n%d\n", &lock_si_pid, &lock_pg_pid) != 2)
	{
		if (errno == 0)
			elog(FATAL, "bogus data in lock file \"%s\"", lockfile);
		else
			ereport(FATAL,
				(errcode_errno(),
				 errmsg("could not read lock file \"%s\": %m", lockfile)));
	}

	fclose(fp);

	if (kill(lock_si_pid, 0) != 0)	/* process is not alive */
		return;

	/* check the postmaster PID */
	if (lock_pg_pid == postmaster_pid)
		elog(FATAL, "is another pg_statsinfod (PID %d) running",
			lock_si_pid);

	/* terminate the another process still exists */
	for (retry = 0; retry < 5; retry++)
	{
		usleep(200 * 1000);	/* 200ms */

		if (kill(lock_si_pid, 0) != 0)	/* process is end */
			return;
	}

	elog(NOTICE, "terminate the another process still exists (PID %d)",
		lock_si_pid);
	if (kill(lock_si_pid, SIGKILL) != 0)
		elog(ERROR, "could not send kill signal (PID %d): %m",
			lock_si_pid);

	return;
}
