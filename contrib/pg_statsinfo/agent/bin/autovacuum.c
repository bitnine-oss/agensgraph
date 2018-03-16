/*
 * autovacuum.c : parse auto-vacuum and auto-analyze messages
 *
 * Copyright (c) 2009-2018, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "pg_statsinfod.h"

#define SQL_INSERT_AUTOVACUUM "\
INSERT INTO statsrepo.autovacuum VALUES \
($1, $2::timestamptz - interval '1sec' * $17, \
 $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)"

#define SQL_INSERT_AUTOANALYZE "\
INSERT INTO statsrepo.autoanalyze VALUES \
($1, $2::timestamptz - interval '1sec' * $6, $3, $4, $5, $6)"

#define SQL_INSERT_AUTOVACUUM_CANCEL "\
INSERT INTO statsrepo.autovacuum_cancel VALUES \
($1, $2, $3, $4, $5, $6)"

#define SQL_INSERT_AUTOANALYZE_CANCEL "\
INSERT INTO statsrepo.autoanalyze_cancel VALUES \
($1, $2, $3, $4, $5, $6)"

/* pg_rusage (rusage is not localized) */
#if PG_VERSION_NUM >= 100000
#define MSG_RUSAGE \
	"CPU: user: %f s, system: %f s, elapsed: %f s"
#else
#define MSG_RUSAGE \
	"CPU %fs/%fu sec elapsed %f sec"
#endif

/* autovacuum cancel request */
#define MSG_AUTOVACUUM_CANCEL_REQUEST \
	"sending cancel to blocking autovacuum PID %s"

/* autovacuum cancel */
#define MSG_AUTOVACUUM_CANCEL \
	"canceling autovacuum task"

#if PG_VERSION_NUM >= 100000
#define NUM_AUTOVACUUM			20
#elif PG_VERSION_NUM >= 90600
#define NUM_AUTOVACUUM			19
#elif PG_VERSION_NUM >= 90500
#define NUM_AUTOVACUUM			18
#elif PG_VERSION_NUM >= 90400
#define NUM_AUTOVACUUM			17
#elif PG_VERSION_NUM >= 90200
#define NUM_AUTOVACUUM			16
#else
#define NUM_AUTOVACUUM			9
#endif
#define NUM_AUTOANALYZE			4
#define NUM_RUSAGE				3
#define NUM_AUTOVACUUM_CANCEL	5

#define AUTOVACUUM_CANCEL_LIFETIME	300	/* sec */

/* autovacuum log data */
typedef struct AutovacuumLog
{
	QueueItem	base;

	char	finish[LOGTIME_LEN];
	List   *params;
} AutovacuumLog;

/* autovacuum cancel request */
typedef struct AVCancelRequest
{
	time_t	 time;		/* localtime that detected the cancel request */
	char	*w_pid;		/* autovacuum worker PID */
	char	*query;		/* query that caused the cancel */
} AVCancelRequest;

static void Autovacuum_free(AutovacuumLog *av);
static bool Autovacuum_exec(AutovacuumLog *av, PGconn *conn, const char *instid);
static bool Autoanalyze_exec(AutovacuumLog *av, PGconn *conn, const char *instid);
static bool AutovacuumCancel_exec(AutovacuumLog *av, PGconn *conn, const char *instid);
static void refresh_avc_request(void);
static AVCancelRequest *get_avc_request(const char *w_pid);
static void put_avc_request(AVCancelRequest *new_entry);
static void remove_avc_request(AVCancelRequest *entry);

static List		*avc_request = NIL;

/*
 * is_autovacuum
 */
bool
is_autovacuum(const char *message)
{
	/* autovacuum log */
	if (match(message, msg_autovacuum))
		return true;

	/* autoanalyze log */
	if (match(message, msg_autoanalyze))
		return true;

	return false;
}

/*
 * parse_autovacuum
 */
bool
parse_autovacuum(const char *message, const char *timestamp)
{
	AutovacuumLog  *av;
	List		   *params;
	List		   *usage;
	const char	   *str_usage;
	QueueItemExec	exec;

	if ((params = capture(message, msg_autovacuum, NUM_AUTOVACUUM)) != NIL)
		exec = (QueueItemExec) Autovacuum_exec;
	else if ((params = capture(message, msg_autoanalyze, NUM_AUTOANALYZE)) != NIL)
		exec = (QueueItemExec) Autoanalyze_exec;
	else
		return false;

	/*
	 * Re-parse rusage output separatedly. Note that MSG_RUSAGE won't be
	 * localized with any lc_messages.
	 */
	str_usage = llast(params);
	if ((usage = capture(str_usage, MSG_RUSAGE, NUM_RUSAGE)) == NIL)
	{
		elog(WARNING, "cannot parse rusage: %s", str_usage);
		list_free_deep(params);
		return false;	/* should not happen */
	}

	av = pgut_new(AutovacuumLog);
	av->base.type = QUEUE_AUTOVACUUM;
	av->base.free = (QueueItemFree) Autovacuum_free;
	av->base.exec = exec;
	strlcpy(av->finish, timestamp, lengthof(av->finish));
	av->params = list_concat(params, usage);

	writer_send((QueueItem *) av);
	return true;
}

/*
 * is_autovacuum_cancel
 */
bool
is_autovacuum_cancel(int elevel, const char *message)
{
	/* autovacuum cancel log */
	if (elevel == ERROR &&
		match(message, MSG_AUTOVACUUM_CANCEL))
		return true;

	return false;
}

/*
 * is_autovacuum_cancel_request
 */
bool
is_autovacuum_cancel_request(int elevel, const char *message)
{
	/* autovacuum cancel request log */
	if ((elevel == LOG || elevel == DEBUG) &&
		match(message, MSG_AUTOVACUUM_CANCEL_REQUEST))
		return true;

	return false;
}

/*
 * parse_autovacuum_cancel
 */
bool
parse_autovacuum_cancel(const Log *log)
{
	AutovacuumLog	*av;
	AVCancelRequest	*entry;
	List			*params;

	if ((params = capture(log->context,
		"automatic %s of table \"%s.%s.%s\"", 4)) == NIL)
		return false;	/* should not happen */

	/* get the query that caused cancel */
	entry = get_avc_request(log->pid);

	av = pgut_new(AutovacuumLog);
	av->base.type = QUEUE_AUTOVACUUM;
	av->base.free = (QueueItemFree) Autovacuum_free;
	av->base.exec = (QueueItemExec) AutovacuumCancel_exec;
	strlcpy(av->finish, log->timestamp, lengthof(av->finish));
	av->params = params;
	if (entry)
	{
		av->params = lappend(av->params, pgut_strdup(entry->query));
		remove_avc_request(entry);
	}
	else
		av->params = lappend(av->params, NULL);

	writer_send((QueueItem *) av);

	return true;
}

/*
 * parse_autovacuum_cancel_request
 */
bool
parse_autovacuum_cancel_request(const Log *log)
{
	AVCancelRequest	*new_entry;
	List			*params;

	/* remove old entries */
	refresh_avc_request();

	/* add a new entry of the cancel request */
#if PG_VERSION_NUM >= 90200
	if ((params = capture(log->hint,
		MSG_AUTOVACUUM_CANCEL_REQUEST, 1)) == NIL)
#else
	if ((params = capture(log->message,
		MSG_AUTOVACUUM_CANCEL_REQUEST, 1)) == NIL)
#endif
		return false;	/* should not happen */

	new_entry = pgut_malloc(sizeof(AVCancelRequest));
	new_entry->time = time(NULL);
	new_entry->w_pid = pgut_strdup((char *) list_nth(params, 0));
	new_entry->query = pgut_strdup(log->user_query);
	list_free_deep(params);

	put_avc_request(new_entry);

	return true;
}

static void
Autovacuum_free(AutovacuumLog *av)
{
	if (av)
	{
		list_free_deep(av->params);
		free(av);
	}
}

static bool
Autovacuum_exec(AutovacuumLog *av, PGconn *conn, const char *instid)
{
	const char	   *params[18];

	elog(DEBUG2, "write (autovacuum)");
	Assert(list_length(av->params) == NUM_AUTOVACUUM + NUM_RUSAGE);

	memset(params, 0, sizeof(params));

	params[0] = instid;
	params[1] = av->finish;					/* finish */
	params[2] = list_nth(av->params, 0);	/* database */
	params[3] = list_nth(av->params, 1);	/* schema */
	params[4] = list_nth(av->params, 2);	/* table */
	params[5] = list_nth(av->params, 3);	/* index_scans */
	params[6] = list_nth(av->params, 4);	/* page_removed */
	params[7] = list_nth(av->params, 5);	/* page_remain */
#if PG_VERSION_NUM >= 100000
//	params[8] = list_nth(av->params, 6);	/* pinned_pages */
	params[8] = list_nth(av->params, 7);	/* frozen_skipped_pages */
	params[9] = list_nth(av->params, 8);	/* tup_removed */
	params[10] = list_nth(av->params, 9);	/* tup_remain */
	params[11] = list_nth(av->params, 10);	/* tup_dead */
//	params[12] = list_nth(av->params, 11);	/* oldest_xmin */
	params[12] = list_nth(av->params, 12);	/* page_hit */
	params[13] = list_nth(av->params, 13);	/* page_miss */
	params[14] = list_nth(av->params, 14);	/* page_dirty */
	params[15] = list_nth(av->params, 15);	/* read_rate */
	params[16] = list_nth(av->params, 17);	/* write_rate */
#elif PG_VERSION_NUM >= 90600
//	params[8] = list_nth(av->params, 6);	/* pinned_pages */
	params[8] = list_nth(av->params, 7);	/* frozen_skipped_pages */
	params[9] = list_nth(av->params, 8);	/* tup_removed */
	params[10] = list_nth(av->params, 9);	/* tup_remain */
	params[11] = list_nth(av->params, 10);	/* tup_dead */
	params[12] = list_nth(av->params, 11);	/* page_hit */
	params[13] = list_nth(av->params, 12);	/* page_miss */
	params[14] = list_nth(av->params, 13);	/* page_dirty */
	params[15] = list_nth(av->params, 14);	/* read_rate */
	params[16] = list_nth(av->params, 16);	/* write_rate */
#elif PG_VERSION_NUM >= 90500
//	params[8] = list_nth(av->params, 6);	/* pinned_pages */
	params[9] = list_nth(av->params, 7);	/* tup_removed */
	params[10] = list_nth(av->params, 8);	/* tup_remain */
	params[11] = list_nth(av->params, 9);	/* tup_dead */
	params[12] = list_nth(av->params, 10);	/* page_hit */
	params[13] = list_nth(av->params, 11);	/* page_miss */
	params[14] = list_nth(av->params, 12);	/* page_dirty */
	params[15] = list_nth(av->params, 13);	/* read_rate */
	params[16] = list_nth(av->params, 15);	/* write_rate */
#elif PG_VERSION_NUM >= 90400
	params[9] = list_nth(av->params, 6);	/* tup_removed */
	params[10] = list_nth(av->params, 7);	/* tup_remain */
	params[11] = list_nth(av->params, 8);	/* tup_dead */
	params[12] = list_nth(av->params, 9);	/* page_hit */
	params[13] = list_nth(av->params, 10);	/* page_miss */
	params[14] = list_nth(av->params, 11);	/* page_dirty */
	params[15] = list_nth(av->params, 12);	/* read_rate */
	params[16] = list_nth(av->params, 14);	/* write_rate */
#elif PG_VERSION_NUM >= 90200
	params[9] = list_nth(av->params, 6);	/* tup_removed */
	params[10] = list_nth(av->params, 7);	/* tup_remain */
	params[12] = list_nth(av->params, 8);	/* page_hit */
	params[13] = list_nth(av->params, 9);	/* page_miss */
	params[14] = list_nth(av->params, 10);	/* page_dirty */
	params[15] = list_nth(av->params, 11);	/* read_rate */
	params[16] = list_nth(av->params, 13);	/* write_rate */
#else
	params[9] = list_nth(av->params, 6);	/* tup_removed */
	params[10] = list_nth(av->params, 7);	/* tup_remain */
#endif
	params[17] = list_nth(av->params, NUM_AUTOVACUUM + 2);	/* duration */

	return pgut_command(conn, SQL_INSERT_AUTOVACUUM,
						lengthof(params), params) == PGRES_COMMAND_OK;
}

static bool
Autoanalyze_exec(AutovacuumLog *av, PGconn *conn, const char *instid)
{
	const char	   *params[6];

	elog(DEBUG2, "write (autoanalyze)");
	Assert(list_length(av->params) == NUM_AUTOANALYZE + NUM_RUSAGE);

	params[0] = instid;
	params[1] = av->finish;					/* finish */
	params[2] = list_nth(av->params, 0);	/* database */
	params[3] = list_nth(av->params, 1);	/* schema */
	params[4] = list_nth(av->params, 2);	/* table */
	params[5] = list_nth(av->params, NUM_AUTOANALYZE + 2);	/* duration */

	return pgut_command(conn, SQL_INSERT_AUTOANALYZE,
						lengthof(params), params) == PGRES_COMMAND_OK;
}

static bool
AutovacuumCancel_exec(AutovacuumLog *av, PGconn *conn, const char *instid)
{
	const char	*params[6];
	const char	*query;

	elog(DEBUG2, "write (autovacuum cancel)");
	Assert(list_length(av->params) == NUM_AUTOVACUUM_CANCEL);

	params[0] = instid;
	params[1] = av->finish;					/* finish */
	params[2] = list_nth(av->params, 1);	/* database */
	params[3] = list_nth(av->params, 2);	/* schema */
	params[4] = list_nth(av->params, 3);	/* table */
	params[5] = list_nth(av->params, 4);	/* query */

	if (strcmp(list_nth(av->params, 0), "vacuum") == 0)
		query = SQL_INSERT_AUTOVACUUM_CANCEL;
	else
		query = SQL_INSERT_AUTOANALYZE_CANCEL;

	return pgut_command(conn, query,
						lengthof(params), params) == PGRES_COMMAND_OK;
}

static void
refresh_avc_request(void)
{
	ListCell	*cell;
	time_t		 now = time(NULL);

	foreach (cell, avc_request)
	{
		AVCancelRequest	*entry = lfirst(cell);

		if ((now - entry->time) > AUTOVACUUM_CANCEL_LIFETIME)
			remove_avc_request(entry);
	}
}

static AVCancelRequest *
get_avc_request(const char *w_pid)
{
	ListCell	*cell;

	foreach (cell, avc_request)
	{
		AVCancelRequest	*entry = lfirst(cell);

		if (strcmp(entry->w_pid, w_pid) == 0)
			return entry;
	}

	return NULL;	/* not found */
}

static void
put_avc_request(AVCancelRequest *new_entry)
{
	AVCancelRequest	*old_entry;

	/* remove old entry that has same autovacuum worker PID */
	if ((old_entry = get_avc_request(new_entry->w_pid)))
		remove_avc_request(old_entry);

	avc_request = lappend(avc_request, new_entry);
}

static void
remove_avc_request(AVCancelRequest *entry)
{
	if (!entry)
		return;

	free(entry->w_pid);
	free(entry->query);
	free(entry);
	avc_request = list_delete_ptr(avc_request, entry);
}
