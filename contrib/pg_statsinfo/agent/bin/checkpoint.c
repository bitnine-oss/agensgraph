/*
 * checkpoint.c
 *
 * Copyright (c) 2009-2018, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "pg_statsinfod.h"

#define SQL_INSERT_CHECKPOINT "\
INSERT INTO statsrepo.checkpoint VALUES \
($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"

#define NUM_CHECKPOINT_STARTING		2

#if PG_VERSION_NUM >= 90500
#define NUM_CHECKPOINT_COMPLETE		18
#define NUM_RESTARTPOINT_COMPLETE	18
#elif PG_VERSION_NUM >= 90100
#define NUM_CHECKPOINT_COMPLETE		16
#define NUM_RESTARTPOINT_COMPLETE	16
#else
#define NUM_CHECKPOINT_COMPLETE		11
#define NUM_RESTARTPOINT_COMPLETE	8
#endif

typedef enum CheckpointType
{
	CKPT_TYPE_CHECKPOINT,
	CKPT_TYPE_RESTARTPOINT
} CheckpointType;

/* checkpoint log data */
typedef struct CheckpointLog
{
	QueueItem	base;

	char			start[LOGTIME_LEN];
	char			flags[128];
	CheckpointType	type;
	List		   *params;
} CheckpointLog;

static void Checkpoint_free(CheckpointLog *ckpt);
static bool Checkpoint_exec(CheckpointLog *ckpt, PGconn *conn, const char *instid);

/*
 * is_checkpoint
 */
bool
is_checkpoint(const char *message)
{
	/* log for checkpoint starting */
	if (match(message, msg_checkpoint_starting))
		return true;

	/* log for checkpoint complete */
	if (match(message, msg_checkpoint_complete))
		return true;

	/* log for restartpoint complete */
	if (match(message, msg_restartpoint_complete))
		return true;

	return false;
}

/*
 * parse_checkpoint
 */
bool
parse_checkpoint(const char *message, const char *timestamp)
{
	static CheckpointLog	*ckpt = NULL;

	List	*params;

	if ((params = capture(message, msg_checkpoint_starting, NUM_CHECKPOINT_STARTING)) != NIL)
	{
		/* log for checkpoint starting */

		const char		*type = (char *) list_nth(params, 0);
		const char		*flags = (char *) list_nth(params, 1);
		CheckpointType	 ckpt_type;

		if (strcmp(type, "checkpoint") == 0)
			ckpt_type = CKPT_TYPE_CHECKPOINT;
		else if (strcmp(type, "restartpoint") == 0)
			ckpt_type = CKPT_TYPE_RESTARTPOINT;
		else
		{
			/* not a checkpoint log */
			list_free_deep(params);
			return false;
		}

		/* ignore shutdown checkpoint */
		if (strstr(flags, "shutdown"))
		{
			free(ckpt);
			ckpt = NULL;
			list_free_deep(params);
			return true;	/* handled, but forget */
		}

		if (ckpt == NULL)
			ckpt = pgut_new(CheckpointLog);

		/* copy type, flags and start timestamp */
		ckpt->type = ckpt_type;
		strlcpy(ckpt->flags, flags, sizeof(ckpt->flags));
		strlcpy(ckpt->start, timestamp, sizeof(ckpt->start));

		list_free_deep(params);
		return true;
	}

	if ((params = capture(message, msg_checkpoint_complete, NUM_CHECKPOINT_COMPLETE)) != NIL ||
		(params = capture(message, msg_restartpoint_complete, NUM_RESTARTPOINT_COMPLETE)) != NIL)
	{
		/* log for checkpoint complete */

		/* ignore if we have not seen any checkpoint start */
		if (ckpt == NULL)
		{
			list_free_deep(params);
			return true;	/* handled, but forget */
		}

		/* send checkpoint log to writer */
		ckpt->params = params;
		ckpt->base.type = QUEUE_CHECKPOINT;
		ckpt->base.free = (QueueItemFree) Checkpoint_free;
		ckpt->base.exec = (QueueItemExec) Checkpoint_exec;
		writer_send((QueueItem *) ckpt);

		ckpt = NULL;

		return true;
	}

	/* not a checkpoint log */
	return false;
}

static void
Checkpoint_free(CheckpointLog *ckpt)
{
	if (ckpt)
	{
		list_free_deep(ckpt->params);
		free(ckpt);
	}
}

static bool
Checkpoint_exec(CheckpointLog *ckpt, PGconn *conn, const char *instid)
{
	const char	   *params[10];
	char			write_duration[32];	/* for "%ld.%03d" */
	char			sync_duration[32];
	char			total_duration[32];

	elog(DEBUG2, "write (checkpoint)");

	params[0] = instid;						/* instid */
	params[1] = ckpt->start;				/* start */
	params[2] = ckpt->flags;				/* flags */
	params[3] = list_nth(ckpt->params, 0);	/* num_buffers */

#if PG_VERSION_NUM >= 90100
	params[4] = list_nth(ckpt->params, 2);	/* xlog_added */
	params[5] = list_nth(ckpt->params, 3);	/* xlog_removed */
	params[6] = list_nth(ckpt->params, 4);	/* xlog_recycled */

	snprintf(write_duration, lengthof(write_duration), "%s.%s",
		(const char *) list_nth(ckpt->params, 5),
		(const char *) list_nth(ckpt->params, 6));
	snprintf(sync_duration, lengthof(sync_duration), "%s.%s",
		(const char *) list_nth(ckpt->params, 7),
		(const char *) list_nth(ckpt->params, 8));
	snprintf(total_duration, lengthof(total_duration), "%s.%s",
		(const char *) list_nth(ckpt->params, 9),
		(const char *) list_nth(ckpt->params, 10));
#else
	if (ckpt->type == CKPT_TYPE_RESTARTPOINT)
	{
		params[4] = NULL;	/* xlog_added */
		params[5] = NULL;	/* xlog_removed */
		params[6] = NULL;	/* xlog_recycled */

		snprintf(write_duration, lengthof(write_duration), "%s.%s",
			(const char *) list_nth(ckpt->params, 2),
			(const char *) list_nth(ckpt->params, 3));
		snprintf(sync_duration, lengthof(sync_duration), "%s.%s",
			(const char *) list_nth(ckpt->params, 4),
			(const char *) list_nth(ckpt->params, 5));
		snprintf(total_duration, lengthof(total_duration), "%s.%s",
			(const char *) list_nth(ckpt->params, 6),
			(const char *) list_nth(ckpt->params, 7));
	}
	else
	{
		params[4] = list_nth(ckpt->params, 2);	/* xlog_added */
		params[5] = list_nth(ckpt->params, 3);	/* xlog_removed */
		params[6] = list_nth(ckpt->params, 4);	/* xlog_recycled */

		snprintf(write_duration, lengthof(write_duration), "%s.%s",
			(const char *) list_nth(ckpt->params, 5),
			(const char *) list_nth(ckpt->params, 6));
		snprintf(sync_duration, lengthof(sync_duration), "%s.%s",
			(const char *) list_nth(ckpt->params, 7),
			(const char *) list_nth(ckpt->params, 8));
		snprintf(total_duration, lengthof(total_duration), "%s.%s",
			(const char *) list_nth(ckpt->params, 9),
			(const char *) list_nth(ckpt->params, 10));
	}
#endif

	params[7] = write_duration;	/* write_duration */
	params[8] = sync_duration;	/* sync_duration */
	params[9] = total_duration;	/* total_duration */

	return pgut_command(conn,
				SQL_INSERT_CHECKPOINT, 10, params) == PGRES_COMMAND_OK;
}
