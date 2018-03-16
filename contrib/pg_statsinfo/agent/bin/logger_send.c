/*
 * logger_send.c
 *
 * Copyright (c) 2009-2018, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "pg_statsinfod.h"
#include "writer_sql.h"

#include <dirent.h>
#include <sys/stat.h>

#define LOGGER_EXIT_DELAY	2

typedef enum LogStoreState
{
	LOG_STORE_READY,
	LOG_STORE_RUNNING,
	LOG_STORE_RETRY,
	LOG_STORE_FAILED,
	LOG_STORE_COMPLETE
} LogStoreState;

typedef struct Logger
{
	/* CSV log file */
	char	csv_path[MAXPGPATH];	/* current log file path */
	char   *csv_name;				/* log file name */
	long	csv_offset;				/* parsed bytes in log file */
	FILE   *fp;						/* open log file */

	/* temp buffer */
	StringInfoData	buf;					/* log buffer */
	size_t			fields[CSV_COLS + 1];	/* field offsets in buffer */
} Logger;

typedef struct LogData
{
	char	*record;				/* log record */
	size_t	 fields[CSV_COLS + 1];	/* field offsets in record */
	int		 elevel;				/* message level */
} LogData;

typedef struct LogBuffer
{
	List	*logs;
	int		 num;					/* number of logs in buffer */
	char	 csv_name[MAXPGPATH];	/* name of csvlog that was added in buffer latest */
	long	 csv_offset;			/* offset of csvlog that was added in buffer latest */
} LogBuffer;

typedef struct LogStore
{
	QueueItem		 base;

	LogBuffer		*buffer;
	LogStoreState	 state;
} LogStore;

/*---- GUC variables (logger_send) ----------*/
static char		*my_log_directory;
static int		 my_repolog_min_messages;
static List		*my_repolog_nologging_users;
static int		 my_repolog_buffer;
static int		 my_repolog_interval;
static bool		 my_adjust_log_level;
static char		*my_adjust_log_info;
static char		*my_adjust_log_notice;
static char		*my_adjust_log_warning;
static char		*my_adjust_log_error;
static char		*my_adjust_log_log;
static char		*my_adjust_log_fatal;
/*------------------------------------------*/

static time_t		logger_reload_time = 0;
static List		   *adjust_log_list = NIL;
static LogBuffer	log_buffer;

static void reload_params(void);
static void logger_parse(Logger *logger, const char *csv_name, long csv_offset);
static bool logger_next(Logger *logger);
static bool logger_open(Logger *logger, const char *csv_name, long csv_offset);
static void logger_close(Logger *logger);
static void logger_setup(Logger *logger, ControlFile *ctrl);
static void assign_csvlog_path(Logger *logger, const char *pg_log, const char *csvlog, long offset);
static void buffer_add(Logger *logger, int elevel);
static void buffer_clear(void);
static bool buffer_store(void);
static void destroy_log(LogData *log);
static bool LogStore_exec(LogStore *log_store, PGconn *conn, const char *instid);
static void LogStore_free(LogStore *log_store);

/*
 * logger_send_main
 */
void *
logger_send_main(void *arg)
{
	ControlFile	*ctrl = (ControlFile *) arg;
	Logger		 logger;
	time_t		 now;
	time_t		 next_store;

	memset(&logger, 0, sizeof(logger));
	memset(&log_buffer, 0, sizeof(log_buffer));
	reload_params();

	logger_setup(&logger, ctrl);

	if (my_repolog_min_messages < DISABLE)
		WriteLogStoreData(logger.csv_name, logger.csv_offset);
	else
		WriteLogStoreData("", 0);

	now = time(NULL);
	next_store = get_next_time(now, my_repolog_interval);

	while (shutdown_state < SHUTDOWN_REQUESTED)
	{
		now = time(NULL);

		/* update settings if reloaded */
		if (logger_reload_time < collector_reload_time)
		{
			int prev_repolog_min_messages;

			prev_repolog_min_messages = my_repolog_min_messages;
			reload_params();

			if (prev_repolog_min_messages == DISABLE &&
				my_repolog_min_messages < DISABLE)
				WriteLogStoreData(logger.csv_name, logger.csv_offset);

			if (prev_repolog_min_messages < DISABLE &&
				my_repolog_min_messages == DISABLE)
			{
				buffer_store();
				buffer_clear();
				WriteLogStoreData("", 0);
			}
		}

		/* don't do anything while writer is in fallback mode */
		if (writer_state == WRITER_FALLBACK)
		{
			usleep(100 * 1000);		/* 100ms */
			continue;
		}

		/* store logs to repository */
		if (now >= next_store || log_buffer.num >= my_repolog_buffer)
		{
			if (!buffer_store())
				continue;

			now = time(NULL);
			next_store = get_next_time(now, my_repolog_interval);
		}

		/* parse csv log */
		logger_parse(&logger, NULL, 0);

		usleep(200 * 1000);	/* 200ms */
	}

	/*
	 * exit without store logs remaining in the log buffer.
	 * because, connect to repository is not guaranteed
	 * if repository is on local dbcluster.
	 */
	buffer_clear();
	logger_close(&logger);
	shutdown_progress(LOGGER_SEND_SHUTDOWN);

	return NULL;
}

static void
reload_params(void)
{
	logger_reload_time = collector_reload_time;
	pthread_mutex_lock(&reload_lock);

	free(my_log_directory);
	if (is_absolute_path(log_directory))
		my_log_directory = pgut_strdup(log_directory);
	else
	{
		my_log_directory = pgut_malloc(MAXPGPATH);
		join_path_components(my_log_directory, data_directory, log_directory);
	}

	/* filter */
	my_repolog_min_messages = repolog_min_messages;
	list_destroy(my_repolog_nologging_users, free);
	split_string(repolog_nologging_users, ',', &my_repolog_nologging_users);

	/* adjust log level */
	my_adjust_log_level = adjust_log_level;
	if (my_adjust_log_level)
	{
		free(my_adjust_log_info);
		my_adjust_log_info = pgut_strdup(adjust_log_info);
		free(my_adjust_log_notice);
		my_adjust_log_notice = pgut_strdup(adjust_log_notice);
		free(my_adjust_log_warning);
		my_adjust_log_warning = pgut_strdup(adjust_log_warning);
		free(my_adjust_log_error);
		my_adjust_log_error = pgut_strdup(adjust_log_error);
		free(my_adjust_log_log);
		my_adjust_log_log = pgut_strdup(adjust_log_log);
		free(my_adjust_log_fatal);
		my_adjust_log_fatal = pgut_strdup(adjust_log_fatal);

		list_destroy(adjust_log_list, free);
		adjust_log_list = NIL;
		adjust_log_list = add_adlog(adjust_log_list, FATAL, my_adjust_log_fatal);
		adjust_log_list = add_adlog(adjust_log_list, LOG, my_adjust_log_log);
		adjust_log_list = add_adlog(adjust_log_list, ERROR, my_adjust_log_error);
		adjust_log_list = add_adlog(adjust_log_list, WARNING, my_adjust_log_warning);
		adjust_log_list = add_adlog(adjust_log_list, NOTICE, my_adjust_log_notice);
		adjust_log_list = add_adlog(adjust_log_list, INFO, my_adjust_log_info);
	}

	/* internal */
	my_repolog_buffer = repolog_buffer;
	my_repolog_interval = repolog_interval;

	pthread_mutex_unlock(&reload_lock);
}

/*
 * logger_parse - Parse CSV log and add it to buffer.
 */
static void
logger_parse(Logger *logger, const char *csv_name, long csv_offset)
{
	while (logger_next(logger))
	{
		Log		log;

		init_log(&log, logger->buf.data, logger->fields);

		/* parse operation request logs; those messages are NOT stored. */
		if (log.elevel == LOG)
		{
			/* operation request log */
			if (strcmp(log.message, LOGMSG_SNAPSHOT) == 0 ||
				strcmp(log.message, LOGMSG_MAINTENANCE) == 0 ||
				strcmp(log.message, LOGMSG_AUTOVACUUM_CANCEL_REQUEST) == 0)
				continue;
		}

		/* if log level adjust enabled, adjust the log level */
		if (my_adjust_log_level)
			adjust_log(&log, adjust_log_list);

		/* filter of log */
		if (log_required(log.elevel, my_repolog_min_messages) &&
			!is_nologging_user(&log, my_repolog_nologging_users))
		{
			buffer_add(logger, log.elevel);
			if (log_buffer.num >= my_repolog_buffer)
				break;	/* number of log in buffer exceeds threshold */
		}

		/*
		 * terminate the parse if the current parse position has reached
		 * the specified end position.
		 */
		if (csv_name && strcmp(logger->csv_name, csv_name) >= 0 &&
		   	logger->csv_offset >= csv_offset)
			break;
	}
}

/*
 * logger_next
 */
static bool
logger_next(Logger *logger)
{
	struct stat	st;
	bool		ret;

	if (logger->fp == NULL ||
		stat(logger->csv_path, &st) != 0 ||
		logger->csv_offset >= st.st_size)
	{
		char	csvlog[MAXPGPATH];

		get_csvlog(csvlog, logger->csv_name, my_log_directory);

		if (!csvlog[0])
			return false;	/* logfile not found */
		if (logger->fp && strcmp(logger->csv_name, csvlog) == 0)
			return false;	/* no more logs */

		logger_close(logger);
		if (!logger_open(logger, csvlog, 0))
			return false;
	}

	clearerr(logger->fp);
	fseek(logger->fp, logger->csv_offset, SEEK_SET);
	ret = read_csv(logger->fp, &logger->buf, CSV_COLS, logger->fields);
	logger->csv_offset = ftell(logger->fp);

	if (!ret)
	{
		int save_errno = errno;

		/* close the file unless EOF; it means an error */
		if (!feof(logger->fp))
		{
			errno = save_errno;
			ereport(WARNING,
				(errcode_errno(),
				 errmsg("could not read csvlog file \"%s\": ",
					logger->csv_path)));
			logger_close(logger);
		}
	}

	return ret;
}

static bool
logger_open(Logger *logger, const char *csv_name, long csv_offset)
{
	Assert(!logger->fp);

	assign_csvlog_path(logger,
		my_log_directory, csv_name, csv_offset);

	/* open csvlog file */
	logger->fp = pgut_fopen(logger->csv_path, "rt");
	if (logger->fp == NULL)
		return false;

	return true;
}

static void
logger_close(Logger *logger)
{
	/* close the previous log if open */
	if (logger->fp)
	{
		fclose(logger->fp);
		logger->fp = NULL;
	}
}

static void
logger_setup(Logger *logger, ControlFile *ctrl)
{
	char		 csv_path[MAXPGPATH];
	char		*csv_name = NULL;
	long		 csv_offset;
	struct stat	 st;

	/* setup logger based on control file */
	if (ctrl->send_csv_name[0] != '\0')
	{
		csv_name = ctrl->send_csv_name;
		csv_offset = ctrl->send_csv_offset;
	}
	else if (ctrl->csv_name[0] != '\0')
	{
		csv_name = ctrl->csv_name;
		csv_offset = ctrl->csv_offset;
	}

	if (csv_name)
	{
		join_path_components(csv_path, my_log_directory, csv_name);

		if (stat(csv_path, &st) == 0 && csv_offset <= st.st_size)
		{
			logger_open(logger, csv_name, csv_offset);
			return;
		}

		/* csvlog which stored at last is missed */
		ereport(WARNING,
			(errmsg("csvlog file \"%s\" not found or incorrect offset",
				csv_name)));
	}

	for (;;)
	{
		get_csvlog(csv_path, NULL, my_log_directory);
		if (csv_path[0])
			break;
		usleep(200 * 1000);	/* 200ms */
	}
	logger_open(logger, csv_path, 0);
}

static void
assign_csvlog_path(Logger *logger, const char *pg_log, const char *csvlog, long offset)
{
	if (is_absolute_path(csvlog))
		strlcpy(logger->csv_path, csvlog, MAXPGPATH);
	else
		join_path_components(logger->csv_path, pg_log, csvlog);

	logger->csv_name = last_dir_separator(logger->csv_path) + 1;
	logger->csv_offset = offset;
}

static void
buffer_add(Logger *logger, int elevel)
{
	LogData		*log;

	log = pgut_malloc(sizeof(LogData));

	log->record = pgut_malloc(logger->buf.len);
	memcpy(log->record, logger->buf.data, logger->buf.len);
	memcpy(log->fields, logger->fields,
		sizeof(log->fields[0]) * lengthof(log->fields));
	log->elevel = elevel;
	log_buffer.logs = lappend(log_buffer.logs, log);

	strcpy(log_buffer.csv_name, logger->csv_name);
	log_buffer.csv_offset = logger->csv_offset;
	log_buffer.num++;
}

static void
buffer_clear(void)
{
	list_destroy(log_buffer.logs, destroy_log);
	memset(&log_buffer, 0, sizeof(log_buffer));
}

static bool
buffer_store(void)
{
	LogStore	*log_store;

	if (log_buffer.num == 0)
		return true;

	log_store = pgut_new(LogStore);

	/* send server log to writer */
	log_store->base.type = QUEUE_LOGSTORE;
	log_store->base.free = (QueueItemFree) LogStore_free;
	log_store->base.exec = (QueueItemExec) LogStore_exec;
	log_store->buffer = &log_buffer;
	log_store->state = LOG_STORE_READY;
	writer_send((QueueItem *) log_store);

	/* wait until log is stored to repository */
	while (log_store->state < LOG_STORE_FAILED)
		usleep(100 * 1000);	/* 100ms */

	if (log_store->state == LOG_STORE_FAILED)
	{
		free(log_store);
		return false;
	}

	/* update control file to set log that was stored latest */
	WriteLogStoreData(log_buffer.csv_name, log_buffer.csv_offset);

	/* clear the log buffer */
	buffer_clear();

	free(log_store);
	return true;
}

static void
destroy_log(LogData *log)
{
	free(log->record);
	free(log);
}

#define NULLIF(str1, str2)		strcmp(str1, str2) == 0 ? NULL : str1
#define GET_FIELD(log, field)	NULLIF(log->record + log->fields[field], "")

static bool
LogStore_exec(LogStore *log_store, PGconn *conn, const char *instid)
{
	ListCell	*cell;
	struct tm	 tm;
	int			 prev_year = -1;
	int			 prev_mon = -1;
	int			 prev_mday = -1;

	log_store->state = LOG_STORE_RUNNING;

	/*
	 * create partition tables
	 */
	foreach(cell, log_buffer.logs)
	{
		LogData		*log = (LogData *) lfirst(cell);
		const char	*timestamp = GET_FIELD(log, 0);

		memset(&tm, 0, sizeof(tm));
		strptime(timestamp, "%Y-%m-%d", &tm);

		if (prev_year < tm.tm_year ||
			prev_mon < tm.tm_mon ||
			prev_mday < tm.tm_mday)
		{
			if (pgut_command(conn,
				SQL_CREATE_REPOLOG_PARTITION, 1, &timestamp) != PGRES_TUPLES_OK)
				goto error;
			prev_year = tm.tm_year;
			prev_mon = tm.tm_mon;
			prev_mday = tm.tm_mday;
		}
	}

	if (pgut_command(conn, "BEGIN", 0, NULL) != PGRES_COMMAND_OK)
		goto error;

	if (pgut_command(conn,
			"SET synchronous_commit = off", 0, NULL) != PGRES_COMMAND_OK)
		goto error;

	foreach(cell, log_buffer.logs)
	{
		LogData		*log = (LogData *) lfirst(cell);
		const char	*params[24];

		params[0] = instid;
		params[1] = GET_FIELD(log, 0);				/* timestamp */
		params[2] = GET_FIELD(log, 1);				/* username */
		params[3] = GET_FIELD(log, 2);				/* database */
		params[4] = GET_FIELD(log, 3);				/* pid */
		params[5] = GET_FIELD(log, 4);				/* client_addr */
		params[6] = GET_FIELD(log, 5);				/* session_id */
		params[7] = GET_FIELD(log, 6);				/* session_line_num */
		params[8] = GET_FIELD(log, 7);				/* ps_display */
		params[9] = GET_FIELD(log, 8);				/* session_start */
		params[10] = GET_FIELD(log, 9);				/* vxid */
		params[11] = GET_FIELD(log, 10);			/* xid */
		params[12] = elevel_to_str(log->elevel);	/* elevel */
		params[13] = GET_FIELD(log, 12);			/* sqlstate */
		params[14] = GET_FIELD(log, 13);			/* message */
		params[15] = GET_FIELD(log, 14);			/* detail */
		params[16] = GET_FIELD(log, 15);			/* hint */
		params[17] = GET_FIELD(log, 16);			/* query */
		params[18] = GET_FIELD(log, 17);			/* query_pos */
		params[19] = GET_FIELD(log, 18);			/* context */
		params[20] = GET_FIELD(log, 19);			/* user_query */
		params[21] = GET_FIELD(log, 20);			/* user_query_pos */
		params[22] = GET_FIELD(log, 21);			/* error_location */
#if PG_VERSION_NUM >= 90000
		params[23] = GET_FIELD(log, 22);			/* application_name */
#else
		params[23] = NULL;							/* application_name */
#endif

		if (pgut_command(conn, SQL_INSERT_LOG, lengthof(params), params) != PGRES_COMMAND_OK)
			goto error;
	}

	if (!pgut_commit(conn))
		goto error;

	log_store->state = LOG_STORE_COMPLETE;
	return true;

error:
	log_store->state = LOG_STORE_RETRY;
	pgut_rollback(conn);
	return false;
}

static void
LogStore_free(LogStore *log_store)
{
	/* decide final state of log store */
	if (log_store->state < LOG_STORE_COMPLETE)
		log_store->state = LOG_STORE_FAILED;
}
