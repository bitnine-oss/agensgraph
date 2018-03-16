/*-------------------------------------------------------------------------
 *
 * pg_statsinfod.h
 *
 * Copyright (c) 2009-2018, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_STATSINFOD_H
#define PG_STATSINFOD_H

#include "postgres_fe.h"
#include "miscadmin.h"
#include "libpq/pqsignal.h"
#include "pgut/pgut.h"
#include "pgut/pgut-list.h"
#include "pgut/pgut-pthread.h"

#include "../common.h"

///#define USE_DAEMON				/* become daemon? */

#define DB_MAX_RETRY		10		/* max retry count for database operations */
#define LOGTIME_LEN			40		/* buffer size for timestamp */
#define LOGCODE_LEN			6		/* buffer size for sqlcode */
#define SECS_PER_DAY		86400	/* seconds per day */

#define STATSINFO_CONTROL_FILE		"pg_statsinfo.control"
#define STATSINFO_CONTROL_VERSION	100000

#define STATSREPO_SCHEMA_VERSION	100000

/* number of columns of csvlog */
#if PG_VERSION_NUM < 90000
#define CSV_COLS			22
#else
#define CSV_COLS			23
#endif

/* maintenance mode flag */
#define MAINTENANCE_MODE_SNAPSHOT	(1 << 0)
#define MAINTENANCE_MODE_LOG		(1 << 1)
#define MAINTENANCE_MODE_REPOLOG	(1 << 2)

/* shutdown state */
typedef enum ShutdownState
{
	STARTUP,
	RUNNING,
	SHUTDOWN_REQUESTED,
	COLLECTOR_SHUTDOWN,
	LOGGER_SEND_SHUTDOWN,
	WRITER_SHUTDOWN,
	LOGGER_SHUTDOWN
} ShutdownState;

/* writer state */
typedef enum WriterState
{
	WRITER_READY,
	WRITER_NORMAL,
	WRITER_FALLBACK
} WriterState;

/* writer queue type */
typedef enum WriterQueueType
{
	QUEUE_SNAPSHOT,
	QUEUE_CHECKPOINT,
	QUEUE_AUTOVACUUM,
	QUEUE_MAINTENANCE,
	QUEUE_LOGSTORE
} WriterQueueType;

/*
 * System status indicator
 * Note: this is stored in pg_statsinfo.control
 */
typedef enum StatsinfoState
{
	STATSINFO_STARTUP,
	STATSINFO_RUNNING,
	STATSINFO_STOPPED,
	STATSINFO_SHUTDOWNED
} StatsinfoState;

/* pg_statsinfod.c */
extern char		   *instance_id;
extern char		   *postmaster_port;
extern int			server_version_num;
extern char		   *server_version_string;
extern int			server_encoding;
extern char		   *log_timezone_name;
extern int			page_size;
extern int			xlog_seg_size;
extern int			page_header_size;
extern int			htup_header_size;
extern int			item_id_size;
extern pid_t		sil_pid;
/*---- GUC variables (collector) -------*/
extern char		   *data_directory;
extern char		   *excluded_dbnames;
extern char		   *excluded_schemas;
extern char		   *stat_statements_max;
extern char		   *stat_statements_exclude_users;
extern int			sampling_interval;
extern int			snapshot_interval;
extern int		    enable_maintenance;
extern time_t		maintenance_time;
extern int			repository_keepday;
extern int			repolog_keepday;
extern char		   *log_maintenance_command;
extern bool			enable_alert;
extern char		   *target_server;
/*---- GUC variables (logger) ----------*/
extern char		   *log_directory;
extern char		   *log_error_verbosity;
extern int			syslog_facility;
extern char		   *syslog_ident;
extern char		   *syslog_line_prefix;
extern int			syslog_min_messages;
extern char		   *textlog_filename;
extern char		   *textlog_line_prefix;
extern int			textlog_min_messages;
extern int			textlog_permission;
extern int			repolog_min_messages;
extern int			repolog_buffer;
extern int			repolog_interval;
extern bool			adjust_log_level;
extern char		   *adjust_log_info;
extern char		   *adjust_log_notice;
extern char		   *adjust_log_warning;
extern char		   *adjust_log_error;
extern char		   *adjust_log_log;
extern char		   *adjust_log_fatal;
extern char		   *textlog_nologging_users;
extern char		   *repolog_nologging_users;
extern int			controlfile_fsync_interval;
/*---- GUC variables (writer) ----------*/
extern char		   *repository_server;
/*---- message format ----*/
extern char		   *msg_debug;
extern char		   *msg_info;
extern char		   *msg_notice;
extern char		   *msg_log;
extern char		   *msg_warning;
extern char		   *msg_error;
extern char		   *msg_fatal;
extern char		   *msg_panic;
extern char		   *msg_shutdown;
extern char		   *msg_shutdown_smart;
extern char		   *msg_shutdown_fast;
extern char		   *msg_shutdown_immediate;
extern char		   *msg_sighup;
extern char		   *msg_autovacuum;
extern char		   *msg_autoanalyze;
extern char		   *msg_checkpoint_starting;
extern char		   *msg_checkpoint_complete;
extern char		   *msg_restartpoint_complete;
/*--------------------------------------*/

extern volatile ShutdownState	shutdown_state;
extern volatile WriterState		writer_state;

/* threads */
extern pthread_t	th_collector;
extern pthread_t	th_logger;
extern pthread_t	th_writer;

/* signal flag */
extern volatile bool	got_SIGHUP;

/* collector.c */
extern pthread_mutex_t	reload_lock;
extern pthread_mutex_t	maintenance_lock;
extern volatile time_t	collector_reload_time;
extern volatile char   *snapshot_requested;
extern volatile char   *maintenance_requested;

/* queue item for writer */
typedef struct QueueItem	QueueItem;
typedef void (*QueueItemFree)(QueueItem *item);
typedef bool (*QueueItemExec)(QueueItem *item, PGconn *conn, const char *instid);

struct QueueItem
{
	QueueItemFree	free;
	QueueItemExec	exec;
	int				type;	/* queue type */
	int				retry;	/* retry count */
};

/* Log line for logger */
typedef struct Log
{
	const char *timestamp;
	const char *username;
	const char *database;
	const char *pid;
	const char *client_addr;
	const char *session_id;
	const char *session_line_num;
	const char *ps_display;
	const char *session_start;
	const char *vxid;
	const char *xid;
	int			elevel;
	const char *sqlstate;
	const char *message;
	const char *detail;
	const char *hint;
	const char *query;
	const char *query_pos;
	const char *context;
	const char *user_query;
	const char *user_query_pos;
	const char *error_location;
	const char *application_name;
} Log;

/* Contents of pg_statsinfo.control */
typedef struct ControlFile
{
	uint32			control_version;			/* STATSINFO_CONTROL_VERSION */
	StatsinfoState	state;						/* see enum above */
	char			csv_name[MAXPGPATH];		/* latest routed csvlog file name */
	long			csv_offset;					/* latest routed csvlog file offset */
	char			send_csv_name[MAXPGPATH];	/* latest stored csvlog file name */
	long			send_csv_offset;			/* latest stored csvlog file offset */
	pg_crc32		crc;						/* CRC of all above ... MUST BE LAST! */
} ControlFile;

/* collector.c */
extern void collector_init(void);
extern void *collector_main(void *arg);
extern PGconn *collector_connect(const char *db);
/* snapshot.c */
extern QueueItem *get_snapshot(char *comment);
extern void readopt_from_file(FILE *fp);
extern void readopt_from_db(PGresult *res);

/* logger.c */
extern void logger_init(void);
extern void *logger_main(void *arg);
/* logger_send.c */
extern void *logger_send_main(void *arg);
/* logger_common.c */
extern void init_log(Log *log, const char *buf, const size_t fields[]);
extern void get_csvlog(char csvlog[], const char *prev, const char *pg_log);
extern bool is_nologging_user(const Log *log, List *user_list);
extern bool split_string(char *rawstring, char separator, List **elemlist);
extern void adjust_log(Log *log, List *adjust_log_list);
extern List *add_adlog(List *adlog_list, int elevel, char *rawstring);
extern void OpenControlFile(int flags, mode_t mode);
extern void CloseControlFile(void);
extern bool ReadControlFile(ControlFile *ctrl);
extern void InitControlFile(ControlFile *ctrl);
extern void WriteState(StatsinfoState state);
extern void WriteLogRouteData(char *csv_name, long csv_offset);
extern void WriteLogStoreData(char *csv_name, long csv_offset);
extern void FsyncControlFile(void);
extern void FlushControlFile(void);
/* logger_in.c */
extern bool read_csv(FILE *fp, StringInfo buf, int ncolumns, size_t *columns);
extern bool match(const char *str, const char *pattern);
extern List *capture(const char *str, const char *pattern, int nparams);
/* logger_out.c */
extern void write_syslog(const Log *log, const char *prefix,
				PGErrorVerbosity verbosity, const char *ident, int facility);
extern bool write_textlog(const Log *log, const char *prefix,
				PGErrorVerbosity verbosity, FILE *fp);
/* checkpoint.c */
extern bool is_checkpoint(const char *message);
extern bool parse_checkpoint(const char *message, const char *timestamp);
/* autovacuum.c */
extern bool is_autovacuum(const char *message);
extern bool parse_autovacuum(const char *message, const char *timestamp);
extern bool is_autovacuum_cancel(int elevel, const char *message);
extern bool is_autovacuum_cancel_request(int elevel, const char *message);
extern bool parse_autovacuum_cancel(const Log *log);
extern bool parse_autovacuum_cancel_request(const Log *log);

/* writer.c */
extern void writer_init(void);
extern void *writer_main(void *arg);
extern void writer_send(QueueItem *item);
extern bool writer_has_queue(WriterQueueType type);
/* maintenance.c */
extern void maintenance_snapshot(time_t repository_keepday);
extern void maintenance_repolog(time_t repolog_keepday);
extern pid_t maintenance_log(const char *command, int *fd_err);
bool check_maintenance_log(pid_t log_maintenance_pid, int fd_err);

/* pg_statsinfod.c */
extern bool postmaster_is_alive(void);
extern PGconn *do_connect(PGconn **conn, const char *info, const char *schema);
extern bool ensure_schema(PGconn *conn, const char *schema);
extern int str_to_elevel(const char *value);
extern const char *elevel_to_str(int elevel);
extern void shutdown_progress(ShutdownState state);
extern void delay(void);
extern char *getlocaltimestamp(void);
extern time_t get_next_time(time_t now, int interval);

#endif   /* PG_STATSINFOD_H */
