/*
 * logger.c
 *
 * Copyright (c) 2009-2018, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "pg_statsinfod.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>

static time_t		logger_reload_time = 0;
static List		   *adjust_log_list = NIL;
static bool			shutdown_message_found = false;

/*---- GUC variables (logger) ----------*/
static char		   *my_log_directory;
static PGErrorVerbosity	my_log_error_verbosity = PGERROR_DEFAULT;
static int			my_syslog_facility;
static char		   *my_syslog_ident;
static char		   *my_syslog_line_prefix;
static int			my_syslog_min_messages;
static char		   *my_textlog_filename;
static char		   *my_textlog_line_prefix;
static int			my_textlog_min_messages;
static int			my_textlog_permission;
static bool			my_adjust_log_level;
static char		   *my_adjust_log_info;
static char		   *my_adjust_log_notice;
static char		   *my_adjust_log_warning;
static char		   *my_adjust_log_error;
static char		   *my_adjust_log_log;
static char		   *my_adjust_log_fatal;
static List		   *my_textlog_nologging_users;
static int		    my_controlfile_fsync_interval;
/*-----------------------*/

/*
 * delay is required because postgres logger might be alive in some seconds
 * after postmaster is terminated.
 */
#define LOGGER_EXIT_DELAY	2

/*
 * delay is required because postgres writes reload log before reloading
 * setting files actually. If we query settings too soon, backends might
 * not yet reload setting files.
 */
#define RELOAD_DELAY		2

typedef struct Logger
{
	/* CSV log file */
	char	csv_path[MAXPGPATH];	/* current log file path */
	char   *csv_name;				/* log file name */
	long	csv_offset;				/* parsed bytes in log file */
	FILE   *fp;						/* open log file */

	/* Text log file */
	FILE   *textlog;
	char	textlog_path[MAXPGPATH];

	/* temp buffer */
	StringInfoData	buf;			/* log buffer */
	size_t	fields[CSV_COLS + 1];	/* field offsets in buffer */
} Logger;

static List			   *log_queue = NIL;
static pthread_mutex_t	log_lock;
static int				recursive_level = 0;

static void reload_params(void);
static void logger_recv(Logger *logger);

static void logger_parse(Logger *logger, const char *pg_log, bool only_routing);

static void logger_route(Logger *logger, const Log *log);

static bool logger_open(Logger *logger, const char *csvlog, long offset);
static void logger_close(Logger *logger);
static bool logger_next(Logger *logger, const char *pg_log);
static void replace_extension(char path[], const char *extension);
static void assign_textlog_path(Logger *logger, const char *pg_log);
static void assign_csvlog_path(Logger *logger, const char *pg_log, const char *csvlog, long offset);
static void do_complement(Logger *logger, ControlFile *ctrl);
static void rename_unknown_textlog(Logger *logger);

static volatile bool	stop_request = false;

/* SIGUSR1: instructs to stop the pg_statsinfod process */
static void
sigusr1_handler(SIGNAL_ARGS)
{
	stop_request = true;
	shutdown_progress(SHUTDOWN_REQUESTED);
}

void
logger_init(void)
{
	pthread_mutex_init(&log_lock, NULL);
	log_queue = NIL;
	logger_reload_time = 0;	/* any values ok as far as before now */
}

/*
 * logger_main
 */
void *
logger_main(void *arg)
{
	ControlFile	*ctrl = (ControlFile *) arg;
	Logger		 logger;
	char		 csvlog[MAXPGPATH];
	time_t		 now;
	time_t		 next_fsync;

	/* Set up signal handlers */
	pqsignal(SIGUSR1, sigusr1_handler);

	memset(&logger, 0, sizeof(logger));
	initStringInfo(&logger.buf);
	reload_params();

	now = time(NULL);
	next_fsync = get_next_time(now, my_controlfile_fsync_interval);

	/* sets latest csvlog to the elements of logger */
	for (;;)
	{
		get_csvlog(csvlog, NULL, my_log_directory);
		if (csvlog[0])
			break;
		usleep(200 * 1000);	/* 200ms */
	}
	assign_csvlog_path(&logger, my_log_directory, csvlog, 0);
	assign_textlog_path(&logger, my_log_directory);

	WriteState(STATSINFO_RUNNING);
	FsyncControlFile();

	/* complement the log until end of the latest csvlog */
	do_complement(&logger, ctrl);

	/*
	 * Logger should not shutdown before any other threads are alive,
	 * or postmaster exists unless shutdown log is not found.
	 */
	while (shutdown_state < WRITER_SHUTDOWN ||
		   (!shutdown_message_found && postmaster_is_alive() && !stop_request))
	{
		now = time(NULL);

		/* update settings if reloaded */
		if (logger_reload_time < collector_reload_time)
		{
			reload_params();

			if (logger.textlog)
			{
				mode_t	mask;

				mask = umask(0777 & ~my_textlog_permission);
				chmod(logger.textlog_path, my_textlog_permission);
				umask(mask);
			}
			else
				assign_textlog_path(&logger, my_log_directory);

			if (my_controlfile_fsync_interval >= 0)
			{
				now = time(NULL);
				next_fsync = get_next_time(now, my_controlfile_fsync_interval);
			}
		}

		logger_parse(&logger, my_log_directory, false);
		usleep(200 * 1000);	/* 200ms */

		/* fsync control file */
		if (my_controlfile_fsync_interval >= 0 && now >= next_fsync)
		{
			FlushControlFile();
			now = time(NULL);
			next_fsync = get_next_time(now, my_controlfile_fsync_interval);
		}

		/* check postmaster pid. */
		if (shutdown_state < SHUTDOWN_REQUESTED && !postmaster_is_alive())
			shutdown_progress(SHUTDOWN_REQUESTED);

		logger_recv(&logger);
	}

	/* exit after some delay */
	if (!shutdown_message_found && !stop_request)
	{
		time_t	until = time(NULL) + LOGGER_EXIT_DELAY;

		for (;;)
		{
			logger_parse(&logger, my_log_directory, false);
			logger_recv(&logger);
			if (shutdown_message_found || time(NULL) > until)
				break;
			usleep(200 * 1000);	/* 200ms */
		}
	}

	/* update pg_statsinfo.control */
	if (shutdown_message_found || !stop_request)
		WriteState(STATSINFO_SHUTDOWNED);
	else
		WriteState(STATSINFO_STOPPED);

	FsyncControlFile();

	/* final shutdown message */
	if (shutdown_message_found)
		elog(LOG, "shutdown");
	else if (stop_request)
		elog(LOG, "shutdown by stop request");
	else
		elog(WARNING, "shutdown because server process exited abnormally");
	logger_recv(&logger);

	logger_close(&logger);
	shutdown_progress(LOGGER_SHUTDOWN);

	return NULL;
}

#ifdef PGUT_OVERRIDE_ELOG

/*
 * We retrieve log_timezone_name from the server because strftime returns
 * platform-depending value, but postgres uses own implementation.
 * Especially, incompatible presentation will be returned on Windows.
 */
static char *
format_log_time(char *buf)
{
	struct timeval	tv;
	struct tm	   *tm;
	time_t			now;
	bool			gmt;
	char			msbuf[8];

	gettimeofday(&tv, NULL);
	now = (time_t) tv.tv_sec;

	/*
	 * Only supports local and GMT timezone because postgres uses own
	 * private timezone catalog. We cannot use it from external processes.
	 */
	gmt = (pg_strcasecmp(log_timezone_name, "GMT") == 0 ||
		   pg_strcasecmp(log_timezone_name, "UTC") == 0);
	tm = gmt ? gmtime(&now) : localtime(&now);
	strftime(buf, LOGTIME_LEN, "%Y-%m-%d %H:%M:%S     ", tm);

	/* 'paste' milliseconds into place... */
	sprintf(msbuf, ".%03d", (int) (tv.tv_usec / 1000));
	strncpy(buf + 19, msbuf, 4);

	/* 'paste' timezone name */
	strcpy(buf + 24, log_timezone_name);

	return buf;
}

/*
 * write messages to stderr for debug or logger is not ready.
 */
static void
write_console(int elevel, const char *msg, const char *detail)
{
	const char *tag = elevel_to_str(elevel);

	if (detail && detail[0])
		fprintf(stderr, "%s: %s\nDETAIL: %s\n", tag, msg, detail);
	else
		fprintf(stderr, "%s: %s\n", tag, msg);
	fflush(stderr);
}

void
pgut_error(int elevel, int code, const char *msg, const char *detail)
{
	Log		   *log;
	char	   *buf;
	size_t		code_len;
	size_t		prefix_len;
	size_t		msg_len;
	size_t		detail_len;

	static char		pid[32];

	/* return if not ready */
	if (shutdown_state < RUNNING || LOGGER_SHUTDOWN <= shutdown_state)
	{
		if (log_required(elevel, pgut_log_level))
			write_console(elevel, msg, detail);
		return;
	}

	/* avoid recursive errors */
	if (pthread_self() == th_logger && recursive_level > 0)
		return;

	if (!pid[0])
		sprintf(pid, "%d", getpid());

	code_len = (code ? LOGCODE_LEN : 0);
	prefix_len = strlen(LOG_PREFIX);
	msg_len = (msg ? strlen(msg) + 1 : 0);
	detail_len = (detail ? strlen(detail) + 1 : 0);
	log = pgut_malloc(sizeof(Log) +
			LOGTIME_LEN + code_len + prefix_len + msg_len + detail_len);
	buf = ((char *) log) + sizeof(Log);

	log->timestamp = format_log_time(buf);
	buf += LOGTIME_LEN;
	log->username = PROGRAM_NAME;
	log->database = "";
	log->pid = pid;
	log->client_addr = "";
	log->session_id = "";
	log->session_line_num = "";
	log->ps_display = PROGRAM_NAME;
	log->session_start = "";
	log->vxid = "";
	log->xid = "";
	log->elevel = elevel;
	if (code != 0)
	{
		log->sqlstate = buf;
		if (code > 0)
			snprintf(buf, code_len, "S%04d", code);
		else
			snprintf(buf, code_len, "SX%03d", -code);
		buf += code_len;
	}
	else
		log->sqlstate = "00000";
	if (msg)
	{
		log->message = buf;
		memcpy(buf, LOG_PREFIX, prefix_len);
		buf += prefix_len;
		memcpy(buf, msg, msg_len);
		buf += msg_len;
	}
	else
		log->message = "";
	if (detail)
	{
		log->detail = buf;
		memcpy(buf, detail, detail_len);
		buf += detail_len;
	}
	else
		log->detail = "";
	log->hint = "";
	log->query = "";
	log->query_pos = "";
	log->context = "";
	log->user_query = "";
	log->user_query_pos = "";
	log->error_location = "";
	log->application_name = PROGRAM_NAME;

	pthread_mutex_lock(&log_lock);
	log_queue = lappend(log_queue, log);
	pthread_mutex_unlock(&log_lock);
}

#endif

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

	/* log_error_verbosity */
	if (pg_strcasecmp(log_error_verbosity, "terse") == 0)
		my_log_error_verbosity = PGERROR_TERSE;
	else if (pg_strcasecmp(log_error_verbosity, "verbose") == 0)
		my_log_error_verbosity = PGERROR_VERBOSE;
	else
		my_log_error_verbosity = PGERROR_DEFAULT;

	/* textlog */
	my_textlog_min_messages = textlog_min_messages;
	free(my_textlog_line_prefix);
	my_textlog_line_prefix = pgut_strdup(textlog_line_prefix);
	free(my_textlog_filename);
	/*
	 * TODO: currently, empty textlog_filename is disallowed in server,
	 * but the ideal behavior might be to use the default log_filename
	 * directly instead of fixed file name.
	 */
	my_textlog_filename = pgut_strdup(textlog_filename);
	my_textlog_permission = textlog_permission & 0666;
	list_destroy(my_textlog_nologging_users, free);
	split_string(textlog_nologging_users, ',', &my_textlog_nologging_users);

	/* syslog */
	my_syslog_min_messages = syslog_min_messages;
	free(my_syslog_line_prefix);
	my_syslog_line_prefix = pgut_strdup(syslog_line_prefix);
	my_syslog_facility = syslog_facility;
	free(my_syslog_ident);
	my_syslog_ident = pgut_strdup(syslog_ident);

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

	/* fsync interval of control file */
	my_controlfile_fsync_interval = controlfile_fsync_interval;

	pgut_log_level = Min(textlog_min_messages, syslog_min_messages);

	pthread_mutex_unlock(&reload_lock);
}

/*
 * logger_recv - receive elog calls
 */
static void
logger_recv(Logger *logger)
{
	List	   *logs;
	ListCell   *cell;

	pthread_mutex_lock(&log_lock);
	logs = log_queue;
	log_queue = NIL;
	pthread_mutex_unlock(&log_lock);

	recursive_level++;
	foreach(cell, logs)
		logger_route(logger, lfirst(cell));
	recursive_level--;

	list_free_deep(logs);
}

/*
 * logger_route
 */
static void
logger_route(Logger *logger, const Log *log)
{
	/* syslog? */
	if (log_required(log->elevel, my_syslog_min_messages))
		write_syslog(log,
					 my_syslog_line_prefix,
					 my_log_error_verbosity,
					 my_syslog_ident,
					 my_syslog_facility);

	/* textlog? */
	if (log_required(log->elevel, my_textlog_min_messages))
	{
		if (logger->textlog == NULL)
		{
			mode_t	mask;

			Assert(logger->textlog_path[0]);

			/* create a new textlog file */
			mask = umask(0777 & ~my_textlog_permission);
			logger->textlog = pgut_fopen(logger->textlog_path, "at");
			umask(mask);
		}

		if (logger->textlog != NULL)
		{
			/* don't write a textlog of the users that are set not logging */
			if (!is_nologging_user(log, my_textlog_nologging_users))
			{
				if (!write_textlog(log,
								   my_textlog_line_prefix,
								   my_log_error_verbosity,
								   logger->textlog))
				{
					/* unexpected error; close the file, and try to reopen */
					fclose(logger->textlog);
					logger->textlog = NULL;
				}
			}
		}
	}
}

/*
 * logger_parse - Parse CSV log and route it into textlog, syslog, or trap.
 */
static void
logger_parse(Logger *logger, const char *pg_log, bool only_routing)
{
	while (logger_next(logger, pg_log))
	{
		Log		log;
		int		save_elevel;

		init_log(&log, logger->buf.data, logger->fields);

		/* parse operation request logs; those messages are NOT routed. */
		if (log.elevel == LOG)
		{
			/* snapshot requested ? */
			if (strcmp(log.message, LOGMSG_SNAPSHOT) == 0)
			{
				if (!only_routing)
				{
					pthread_mutex_lock(&reload_lock);
					free((char *) snapshot_requested);
					snapshot_requested = pgut_strdup(log.detail);
					pthread_mutex_unlock(&reload_lock);
				}
				continue;
			}

			/* maintenance requested ? */
			if (strcmp(log.message, LOGMSG_MAINTENANCE) == 0)
			{
				if (!only_routing)
				{
					pthread_mutex_lock(&reload_lock);
					free((char *) maintenance_requested);
					maintenance_requested = pgut_strdup(log.detail);
					pthread_mutex_unlock(&reload_lock);
				}
				continue;
			}
#if PG_VERSION_NUM >= 90200
			/* autovacuum cancel request ? */
			if (strcmp(log.message, LOGMSG_AUTOVACUUM_CANCEL_REQUEST) == 0)
			{
				if (!only_routing)
					parse_autovacuum_cancel_request(&log);
				continue;
			}
#endif
#ifdef ADJUST_PERFORMANCE_MESSAGE_LEVEL
			/* performance log? */
			if ((my_textlog_min_messages > INFO ||
				 my_syslog_min_messages > INFO) &&
				 is_performance_message(log.message))
			{
				log.elevel = INFO;
				continue;
			}
#endif
		}

		/*
		 * route the log to syslog and textlog.
		 * if log level adjust enabled, adjust the log level.
		 */
		save_elevel = log.elevel;
		if (my_adjust_log_level)
			adjust_log(&log, adjust_log_list);
		logger_route(logger, &log);

		/* update pg_statsinfo.control */
		WriteLogRouteData(logger->csv_name, logger->csv_offset);

		if (!only_routing)
		{
			if (save_elevel == LOG)
			{
				/* checkpoint ? */
				if (is_checkpoint(log.message))
				{
					parse_checkpoint(log.message, log.timestamp);
					continue;
				}

				/* autovacuum ? */
				if (is_autovacuum(log.message))
				{
					parse_autovacuum(log.message, log.timestamp);
					continue;
				}
#if PG_VERSION_NUM < 90300
				/* setting parameters reloaded ? */
				if (strcmp(log.message, msg_sighup) == 0)
				{
					kill(sil_pid, SIGHUP);
					continue;
				}
#endif
				/* shutdown ? */
				if (strcmp(log.message, msg_shutdown) == 0)
				{
					shutdown_message_found = true;
					continue;
				}

				/* shutdown requested ? */
				if (strcmp(log.message, msg_shutdown_smart) == 0 ||
					strcmp(log.message, msg_shutdown_fast) == 0 ||
					strcmp(log.message, msg_shutdown_immediate) == 0)
				{
					shutdown_progress(SHUTDOWN_REQUESTED);
					continue;
				}
			}
#if PG_VERSION_NUM < 90200
			/* autovacuum cancel request ? */
			if (is_autovacuum_cancel_request(save_elevel, log.message))
			{
				parse_autovacuum_cancel_request(&log);
				continue;
			}
#endif
			/* autovacuum cancel ? */
			if (is_autovacuum_cancel(save_elevel, log.message))
			{
				parse_autovacuum_cancel(&log);
				continue;
			}
		}
	}
}

static bool
logger_open(Logger *logger, const char *csvlog, long offset)
{
	mode_t	mask;

	Assert(!logger->fp);
	Assert(!logger->textlog);

	assign_csvlog_path(logger, my_log_directory, csvlog, offset);
	assign_textlog_path(logger, my_log_directory);

	/* open csvlog file */
	logger->fp = pgut_fopen(logger->csv_path, "rt");
	if (logger->fp == NULL)
		return false;

	/* create a new textlog file */
	mask = umask(0777 & ~my_textlog_permission);
	logger->textlog = pgut_fopen(logger->textlog_path, "at");
	umask(mask);
	if (logger->textlog == NULL)
	{
		fclose(logger->fp);
		return false;
	}

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

	/* close textlog and rename to the same name with csv */
	if (logger->textlog)
	{
		struct stat	st;

		fsync(fileno(logger->textlog));
		fclose(logger->textlog);
		logger->textlog = NULL;

		/* overwrite existing .log file; it must be empty.
		 * if .log file is not empty, rename it to .log.copy.
		 * Note:
		 * Some error messages through system() (eg. recovery_command)
		 * outputs to the .log file  from postgres's logger, so sometimes
		 * .log file will not be empty.
		 */
		if (logger->csv_path[0] && stat(logger->csv_path, &st) == 0)
		{
			char	path[MAXPGPATH];

			strlcpy(path, logger->csv_path, MAXPGPATH);
			replace_extension(path, ".log");

			if (stat(path, &st) == 0 && st.st_size > 0)
			{
				char	save_path[MAXPGPATH];

				strlcpy(save_path, path, MAXPGPATH);
				replace_extension(save_path, ".log.copy");
				rename(path, save_path);
			}

			rename(logger->textlog_path, path);
		}
	}
}

/*
 * logger_next
 */
static bool
logger_next(Logger *logger, const char *pg_log)
{
	struct stat	st;
	bool		ret;

	if (logger->fp == NULL ||
		stat(logger->csv_path, &st) != 0 ||
		logger->csv_offset >= st.st_size)
	{
		char	csvlog[MAXPGPATH];
		char	textlog[MAXPGPATH];

		if (shutdown_message_found)
			return false;	/* must end with the current log */

		get_csvlog(csvlog, logger->csv_name, pg_log);

		if (!csvlog[0])
			return false;	/* logfile not found */
		if (logger->fp && strcmp(logger->csv_name, csvlog) == 0)
			return false;	/* no more logs */

		join_path_components(textlog, pg_log, csvlog);
		replace_extension(textlog, ".log");

		/*
		 * csvlog files that have empty *.log have not been parsed yet
		 * because postgres logger make an empty log file.
		 * Note:
		 * Some "cannot stat" error messages are output to *.log,
		 * so we check logger->fp and csvlog again.
		 */
		 if (stat(textlog, &st) == 0 && st.st_size > 0)
		 {
			if (logger->fp && strcmp(logger->csv_name, csvlog) == 0)
				return false;	/* already parsed log */
		 }

		logger_close(logger);
		if (!logger_open(logger, csvlog, 0))
			return false;

		/*
		 * if writer thread working in fallback mode,
		 * write in the textlog that agent is working in fallback mode.
		 */
		if (writer_state == WRITER_FALLBACK)
		{
			elog(LOG, "*** pg_statsinfo is working in fallback mode ***");
			logger_recv(logger);
		}

		elog(DEBUG2, "read csvlog \"%s\"", logger->csv_path);
	}

	clearerr(logger->fp);
	fseek(logger->fp, logger->csv_offset, SEEK_SET);
	ret = read_csv(logger->fp, &logger->buf, CSV_COLS, logger->fields);
	logger->csv_offset = ftell(logger->fp);

	if (!ret)
	{
		int		save_errno = errno;

		/* close the file unless EOF; it means an error */
		if (!feof(logger->fp))
		{
			errno = save_errno;
			ereport(WARNING,
				(errcode_errno(),
				 errmsg("could not read csvlog file \"%s\": ",
					logger->csv_path)));
			fclose(logger->fp);
			logger->fp = NULL;
		}
	}

	return ret;
}

static void
replace_extension(char path[], const char *extension)
{
	char *dot;

	if ((dot = strrchr(path, '.')) != NULL)
		strlcpy(dot, extension, MAXPGPATH - (dot - path));
	else
		strlcat(dot, extension, MAXPGPATH);
}

static void
assign_textlog_path(Logger *logger, const char *pg_log)
{
	if (is_absolute_path(my_textlog_filename))
		strlcpy(logger->textlog_path, my_textlog_filename, MAXPGPATH);
	else
		join_path_components(logger->textlog_path, pg_log, my_textlog_filename);
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
do_complement(Logger *logger, ControlFile *ctrl)
{
	char		prev_csvlog[MAXPGPATH];
	struct stat	st;

	if (ctrl->csv_name[0] == '\0' ||
		ctrl->state == STATSINFO_SHUTDOWNED)
	{
		/*
		 * if the latest textlog file already exists, rename it to
		 * "<latest-csvlog>.err.<seqid>" (eg. postgresql-2012-07-01_000000.err.1)
		 */
		if (stat(logger->textlog_path, &st) == 0)
			rename_unknown_textlog(logger);

		logger_open(logger, logger->csv_name, logger->csv_offset);
		return;
	}

	join_path_components(prev_csvlog, my_log_directory, ctrl->csv_name);

	/*
	 * if state of control file is "STATSINFO_STOPPED",
	 * restore the latest textlog from previous textlog that was renamed.
	 */
	if (ctrl->state == STATSINFO_STOPPED)
	{
		char prev_textlog[MAXPGPATH];

		strlcpy(prev_textlog, prev_csvlog, MAXPGPATH);
		replace_extension(prev_textlog, ".log");

		if (rename(prev_textlog, logger->textlog_path) != 0)
			ereport(ERROR,
				(errcode_errno(),
				 errmsg("could not rename file \"%s\" to \"%s\": %m",
				 	prev_textlog, logger->textlog_path)));
	}

	if (stat(prev_csvlog, &st) != 0 || ctrl->csv_offset > st.st_size)
	{
		/* csvlog which parsed at last is missed */
		ereport(WARNING,
			(errmsg("csvlog file \"%s\" not found or incorrect offset",
				ctrl->csv_name)));

		/*
		 * if the latest textlog file already exists, rename it to
		 * "<latest-csvlog>.err.<seqid>" (eg. postgresql-2012-07-01_000000.err.1)
		 */
		if (stat(logger->textlog_path, &st) == 0)
			rename_unknown_textlog(logger);

		logger_open(logger, logger->csv_name, logger->csv_offset);
		return;
	}

	/* perform the log routing until end of the latest csvlog */
	logger_open(logger, ctrl->csv_name, ctrl->csv_offset);
	logger_parse(logger, my_log_directory, true);
	return;
}

static void
rename_unknown_textlog(Logger *logger)
{
	struct stat	st;
	char		new_path[MAXPGPATH];
	char		extension[32];
	int			seqid = 0;

	strlcpy(new_path, logger->csv_path, sizeof(new_path));

	for (;;)
	{
		snprintf(extension, sizeof(extension), ".err.%d", ++seqid);
		replace_extension(new_path, extension);

		if (stat(new_path, &st) != 0)
			break;
	}

	rename(logger->textlog_path, new_path);
	elog(WARNING,
		"latest textlog file already exists, it renamed to '%s'", new_path);
}
