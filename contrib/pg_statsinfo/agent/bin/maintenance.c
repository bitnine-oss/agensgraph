/*
 * maintenance.c:
 *
 * Copyright (c) 2009-2016, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "pg_statsinfod.h"

#include <sys/types.h>
#include <sys/wait.h>

#define SQL_DELETE_SNAPSHOT		"SELECT statsrepo.del_snapshot2(CAST($1 AS TIMESTAMPTZ))"
#define SQL_DELETE_REPOLOG		"SELECT statsrepo.del_repolog2(CAST($1 AS TIMESTAMPTZ))"

typedef struct Maintenance
{
	QueueItem	base;

	bool		(*operation)(PGconn *conn, void *param);
	time_t		period;
} Maintenance;

static bool Maintenance_exec(Maintenance *maintenance, PGconn *conn, const char *instid);
static void Maintenance_free(Maintenance *maintenance);
static bool delete_snapshot(PGconn *conn, void *param);
static bool delete_repolog(PGconn *conn, void *param);
static pid_t forkexec(const char *command, int *fd_err);

/*
 * maintenance of the snapshot
 */
void
maintenance_snapshot(time_t repository_keep_period)
{
	Maintenance	*maintenance;

	maintenance = pgut_malloc(sizeof(Maintenance));
	maintenance->base.type = QUEUE_MAINTENANCE;
	maintenance->base.exec = (QueueItemExec) Maintenance_exec;
	maintenance->base.free = (QueueItemFree) Maintenance_free;
	maintenance->operation = delete_snapshot;
	maintenance->period = repository_keep_period;

	writer_send((QueueItem *) maintenance);
}

/*
 * maintenance of the log which is in repository
 */
void
maintenance_repolog(time_t repolog_keep_period)
{
	Maintenance	*maintenance;

	maintenance = pgut_malloc(sizeof(Maintenance));
	maintenance->base.type = QUEUE_MAINTENANCE;
	maintenance->base.exec = (QueueItemExec) Maintenance_exec;
	maintenance->base.free = (QueueItemFree) Maintenance_free;
	maintenance->operation = delete_repolog;
	maintenance->period = repolog_keep_period;

	writer_send((QueueItem *) maintenance);
}

/*
 * maintenance of the log
 */
pid_t
maintenance_log(const char *command, int *fd_err)
{
	char		 logMaintenanceCmd[MAXPGPATH];
	char		*dp;
	char		*endp;
	const char	*sp;

	/* construct the log maintenance command */
	dp = logMaintenanceCmd;
	endp = logMaintenanceCmd + MAXPGPATH - 1;
	*endp = '\0';

	for (sp = log_maintenance_command; *sp; sp++)
	{
		if (*sp == '%')
		{
			switch (sp[1])
			{
				case 'l':
					/* %l: log directory */
					sp++;
					if (is_absolute_path(log_directory))
						StrNCpy(dp, log_directory, endp - dp);
					else
						join_path_components(dp, data_directory, log_directory);
					dp += strlen(dp);
					break;
				case '%':
					/* convert %% to a single % */
					sp++;
					if (dp < endp)
						*dp++ = *sp;
					break;
				default:
					/* otherwise treat the % as not special */
					if (dp < endp)
						*dp++ = *sp;
					break;
			}
		}
		else
		{
			if (dp < endp)
				*dp++ = *sp;
		}
	}
	*dp = '\0';

	/* run the log maintenance command in background */
	return forkexec(logMaintenanceCmd, fd_err);
}

#define	ERROR_MESSAGE_MAXSIZE	256

/*
 * check the status of log maintenance command running in background
 */
bool
check_maintenance_log(pid_t log_maintenance_pid, int fd_err)
{
	int	status;

	switch (waitpid(log_maintenance_pid, &status, WNOHANG))
	{
		case -1:	/* error */
			elog(ERROR,
				"failed to wait of the log maintenance command: %s", strerror(errno));
			close(fd_err);
			return true;
		case 0:		/* running */
			elog(DEBUG2, "log maintenance command is running");
			return false;
		default:	/* completed */
			if (status != 0)
			{
				/* command exit value is abnormally code */
				ssize_t read_size;
				char    errmsg[ERROR_MESSAGE_MAXSIZE];

				if((read_size = read(fd_err, errmsg, sizeof(errmsg) - 1)) >= 0)
					errmsg[read_size] = '\0';
				else
				{
					elog(ERROR, "read() on self-pipe failed: %s", strerror(errno));
					errmsg[0] = '\0';
				}

				if (WIFEXITED(status))
					elog(ERROR,
						"log maintenance command failed with exit code %d: %s",
						WEXITSTATUS(status), errmsg);
				else if (WIFSIGNALED(status))
					elog(ERROR,
						"log maintenance command was terminated by signal %d: %s",
						WTERMSIG(status), errmsg);
				else
					elog(ERROR,
						"log maintenance command exited with unrecognized status %d: %s",
						status, errmsg);
			}
			close(fd_err);
			return true;
	}
}

static bool
delete_snapshot(PGconn *conn, void *param)
{
	const char		*params[1];
	ExecStatusType	 status;

	params[0] = (const char *) param;

	/* exclusive control during snapshot and maintenance */
	pthread_mutex_lock(&maintenance_lock);
	status = pgut_command(conn, SQL_DELETE_SNAPSHOT, 1, params);
	pthread_mutex_unlock(&maintenance_lock);

	return status == PGRES_TUPLES_OK;
}

static bool
delete_repolog(PGconn *conn, void *param)
{
	const char	*params[1];

	params[0] = (const char *) param;
	if (pgut_command(conn, SQL_DELETE_REPOLOG, 1, params) != PGRES_TUPLES_OK)
		return false;

	return true;
}

static bool
Maintenance_exec(Maintenance *maintenance, PGconn *conn, const char *instid)
{
	char	 timestamp[32];

	strftime(timestamp, sizeof(timestamp),
		"%Y-%m-%d %H:%M:%S", localtime(&maintenance->period));

	return maintenance->operation(conn, timestamp);
}

static void
Maintenance_free(Maintenance *maintenance)
{
	free(maintenance);
}

#define R	(0)
#define W	(1)

/*
 * execute a shell command asynchronously
 */
static pid_t
forkexec(const char *command, int *fd_err)
{
	pid_t	cpid;
	int		pipe_fd_err[2];

	/* create pipes */
	if (pipe(pipe_fd_err) < 0)
	{
		elog(ERROR, "could not create pipe: %s", strerror(errno));
		return -1;
	}

	/* invoke processs */
	if ((cpid = fork()) < 0)
	{
		close(pipe_fd_err[R]);
		close(pipe_fd_err[W]);
		elog(ERROR, "fork failed: %s", strerror(errno));
		return -1;
	}

	if (cpid == 0)
	{
		/* in child process */
		close(pipe_fd_err[R]);
		dup2(pipe_fd_err[W], STDERR_FILENO);
		close(pipe_fd_err[W]);

		execlp("/bin/sh", "sh", "-c", command, NULL);
		_exit(127);
	}

	close(pipe_fd_err[W]);

	*fd_err = pipe_fd_err[R];
	return cpid;
}
