/*
 * logger_common.c
 *
 * Copyright (c) 2009-2017, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "pg_statsinfod.h"

#include <fcntl.h>
#include <dirent.h>
#include <sys/stat.h>

typedef struct AdjustLog
{
	char	*sqlstate;	/* SQL STATE to be adjusted */
	int		 elevel;	/* log level */
} AdjustLog;

static volatile int		cf_fd = -1;
static ControlFile		cf_img;
static pthread_mutex_t	cf_lock;

static void WriteControlFile(int fd);
static int SyncFile(int fd);
static char *b_trim(char *str);

void
init_log(Log *log, const char *buf, const size_t fields[])
{
	int		i;

	i = 0;
	log->timestamp = buf + fields[i++];
	log->username = buf + fields[i++];
	log->database = buf + fields[i++];
	log->pid = buf + fields[i++];
	log->client_addr = buf + fields[i++];
	log->session_id = buf + fields[i++];
	log->session_line_num = buf + fields[i++];
	log->ps_display = buf + fields[i++];
	log->session_start = buf + fields[i++];
	log->vxid = buf + fields[i++];
	log->xid = buf + fields[i++];
	log->elevel = str_to_elevel(buf + fields[i++]);
	log->sqlstate = buf + fields[i++];
	log->message = buf + fields[i++];
	log->detail = buf + fields[i++];
	log->hint = buf + fields[i++];
	log->query = buf + fields[i++];
	log->query_pos = buf + fields[i++];
	log->context = buf + fields[i++];
	log->user_query = buf + fields[i++];
	log->user_query_pos = buf + fields[i++];
	log->error_location = buf + fields[i++];
#if PG_VERSION_NUM >= 90000
	log->application_name = buf + fields[i++];
#else
	log->application_name = "";
#endif
	Assert(i == CSV_COLS);
}

/*
 * Get the csvlog path to parse next, or null-string if no logs.
 */
void
get_csvlog(char csvlog[], const char *prev, const char *pg_log)
{
	DIR				*dir;
	struct dirent	*dp;

	csvlog[0] = '\0';

	if ((dir = opendir(pg_log)) == NULL)
	{
		/* pg_log directory might not exist before syslogger started */
		if (errno != ENOENT)
			ereport(WARNING,
				(errcode_errno(),
				 errmsg("could not open directory \"%s\": ", pg_log)));
		return;
	}

	for (dp = readdir(dir); dp != NULL; dp = readdir(dir))
	{
		char		 csv_path[MAXPGPATH];
		struct stat	 st;
		const char	*extension = strrchr(dp->d_name, '.');

		join_path_components(csv_path, pg_log, dp->d_name);

		/* check the extension is .csv */
		if (extension == NULL || strcmp(extension, ".csv") != 0)
			continue;

		/* check the file type is regular file */
		if (lstat(csv_path, &st) != 0 && !S_ISREG(st.st_mode))
			continue;

		if (prev)
		{
			/* get the next log of previous parsed log */
			if (strcmp(prev, dp->d_name) >= 0 ||
				(csvlog[0] && strcmp(csvlog, dp->d_name) < 0))
				continue;
		}
		else
		{
			/* get the latest log */
			if (csvlog[0] && strcmp(csvlog, dp->d_name) > 0)
				continue;
		}

		strlcpy(csvlog, dp->d_name, MAXPGPATH);
	}
	closedir(dir);
}

bool
is_nologging_user(const Log *log, List *user_list)
{
	ListCell	*cell;

	foreach(cell, user_list)
	{
		if (strcmp(log->username, (char *) lfirst(cell)) == 0)
		return true;
	}
	return false;
}

/*
 * Note: this function modify the argument
 */
bool
split_string(char *rawstring, char separator, List **elemlist)
{
	char	*nextp = rawstring;
	bool	 done = false;

	*elemlist = NIL;

	/* skip leading whitespace */
	while (isspace((unsigned char) *nextp))
		nextp++;

	/* allow empty string */
	if (*nextp == '\0')
		return true;

	/* At the top of the loop, we are at start of a new identifier. */
	do
	{
		char *curname;
		char *endp;

		if (*nextp == '\"')
		{
			/* Quoted name --- collapse quote-quote pairs, no downcasing */
			curname = nextp + 1;
			for (;;)
			{
				endp = strchr(nextp + 1, '\"');
				if (endp == NULL)
					return false; /* mismatched quotes */
				if (endp[1] != '\"')
					break; /* found end of quoted name */
				/* Collapse adjacent quotes into one quote, and look again */
				memmove(endp, endp + 1, strlen(endp));
				nextp = endp;
			}
			/* endp now points at the terminating quote */
			nextp = endp + 1;
		}
		else
		{
			/* Unquoted name --- extends to separator or whitespace */
			curname = nextp;
			while (*nextp && *nextp != separator &&
				   !isspace((unsigned char) *nextp))
				nextp++;
			endp = nextp;
			if (curname == nextp)
				return false; /* empty unquoted name not allowed */
		}

		/* skip trailing whitespace */
		while (isspace((unsigned char) *nextp))
			nextp++;

		if (*nextp == separator)
		{
			nextp++;
			while (isspace((unsigned char) *nextp))
				nextp++; /* skip leading whitespace for next */
			/* we expect another name, so done remains false */
		}
		else if (*nextp == '\0')
			done = true;
		else
			return false; /* invalid syntax */

		/* Now safe to overwrite separator with a null */
		*endp = '\0';

		/*
		 * Finished isolating current name --- add it to list
		 */
		*elemlist = lappend(*elemlist, pgut_strdup(curname));

		/* Loop back if we didn't reach end of string */
	} while (!done);

	return true;
}

void
adjust_log(Log *log, List *adjust_log_list)
{
	ListCell	*cell;

	foreach(cell, adjust_log_list)
	{
		AdjustLog *adlog = (AdjustLog *) lfirst(cell);

		if (strcmp(adlog->sqlstate, log->sqlstate) == 0)
		{
			log->elevel = adlog->elevel;
			elog(DEBUG2, "adjust log level -> %d: sqlstate=\"%s\"", log->elevel, log->sqlstate);
			break;
		}
	}
}

List *
add_adlog(List *adlog_list, int elevel, char *rawstring)
{
	char	*token;

	token = strtok(rawstring, ",");
	while (token)
	{
		AdjustLog *adlog = pgut_malloc(sizeof(AdjustLog));;
		adlog->elevel = elevel;
		adlog->sqlstate = b_trim(token);
		adlog_list = lappend(adlog_list, adlog);
		token = strtok(NULL, ",");
	}
	return adlog_list;
}

/*
 * Note: this function modify the argument string
 */
static char *
b_trim(char *str)
{
	size_t	 len;
	char	*start;

	if (str == NULL)
		return NULL;

	/* remove space character from prefix */
	len = strlen(str);
	while (len > 0 && isspace(str[len - 1])) { len--; }
	str[len] = '\0';

	/* remove space character from suffix */
	start = str;
	while (isspace(start[0])) { start++; }
	memmove(str, start, strlen(start) + 1);

	return str;
}

/*
 * I/O routines for ControlFile of pg_statsinfo
 */
void
OpenControlFile(int flags, mode_t mode)
{
	int		fd;

	if ((fd = open(STATSINFO_CONTROL_FILE, flags, mode)) < 0)
	{
		ereport(FATAL,
			(errcode_errno(),
			 errmsg("could not open control file \"%s\": %m",
			 	STATSINFO_CONTROL_FILE)));
		return;
	}

	cf_fd = fd;
	pthread_mutex_init(&cf_lock, NULL);
}

void
CloseControlFile(void)
{
	Assert(cf_fd >= 0);	/* have not been opened the pg_statsinfo.control */

	if (close(cf_fd) < 0)
	{
		ereport(ERROR,
			(errcode_errno(),
			 errmsg("could not close control file \"%s\": %m",
			 	STATSINFO_CONTROL_FILE)));
		return;
	}

	pthread_mutex_destroy(&cf_lock);
}

bool
ReadControlFile(ControlFile *ctrl)
{
	pg_crc32	crc;

	Assert(cf_fd >= 0);	/* have not been opened the pg_statsinfo.control */

	/* read data */
	if (read(cf_fd, ctrl, sizeof(*ctrl)) != sizeof(*ctrl))
	{
		ereport(ERROR,
			(errcode_errno(),
			 errmsg("could not read from control file \"%s\": %m",
			 	STATSINFO_CONTROL_FILE)));
		return false;
	}

	/*
	 * Check for expected pg_statsinfo.control format version.
	 * If this is wrong, the CRC check will likely fail because we'll be
	 * checking the wrong number of bytes.
	 * Complaining about wrong version will probably be more enlightening
	 * than complaining about wrong CRC.
	 */
	if (ctrl->control_version != STATSINFO_CONTROL_VERSION &&
		((ctrl->control_version / 100) != (STATSINFO_CONTROL_VERSION / 100)))
	{
		ereport(ERROR,
			(errmsg("pg_statsinfo.control format incompatible"),
			 errdetail("pg_statsinfo.control was created with STATSINFO_CONTROL_VERSION %d (0x%08x), "
			 		   "but the pg_statsinfo was compiled with STATSINFO_CONTROL_VERSION %d (0x%08x)",
					ctrl->control_version, ctrl->control_version,
					STATSINFO_CONTROL_VERSION, STATSINFO_CONTROL_VERSION)));
		return false;
	}

	/* check the CRC */
	INIT_CRC32(crc);
	COMP_CRC32(crc, (char *) ctrl, offsetof(ControlFile, crc));
	FIN_CRC32(crc);

	if (!EQ_CRC32(crc, ctrl->crc))
	{
		ereport(ERROR,
			(errmsg("incorrect checksum in control file \"%s\"",
				STATSINFO_CONTROL_FILE)));
		return false;
	}

	/* copy the content of control file to image for write */
	memcpy(&cf_img, ctrl, sizeof(cf_img));

	return true;
}

void
InitControlFile(ControlFile *ctrl)
{
	Assert(cf_fd >= 0);	/* have not been opened the pg_statsinfo.control */

	/* initialize the content of control file */
	memset(ctrl, 0, sizeof(ControlFile));
	ctrl->control_version = STATSINFO_CONTROL_VERSION;
	ctrl->state = STATSINFO_STARTUP;

	/* copy the content of control file to image for write */
	memcpy(&cf_img, ctrl, sizeof(cf_img));

	WriteControlFile(cf_fd);
}

void
WriteState(StatsinfoState state)
{
	Assert(cf_fd >= 0);	/* have not been opened the pg_statsinfo.control */

	pthread_mutex_lock(&cf_lock);

	cf_img.state = state;
	WriteControlFile(cf_fd);

	pthread_mutex_unlock(&cf_lock);
}

void
WriteLogRouteData(char *csv_name, long csv_offset)
{
	Assert(cf_fd >= 0);	/* have not been opened the pg_statsinfo.control */

	pthread_mutex_lock(&cf_lock);

	strncpy(cf_img.csv_name,
		csv_name, sizeof(cf_img.csv_name));
	cf_img.csv_offset = csv_offset;
	WriteControlFile(cf_fd);

	pthread_mutex_unlock(&cf_lock);
}

void
WriteLogStoreData(char *csv_name, long csv_offset)
{
	Assert(cf_fd >= 0);	/* have not been opened the pg_statsinfo.control */

	pthread_mutex_lock(&cf_lock);

	strncpy(cf_img.send_csv_name,
		csv_name, sizeof(cf_img.send_csv_name));
	cf_img.send_csv_offset = csv_offset;
	WriteControlFile(cf_fd);

	pthread_mutex_unlock(&cf_lock);
}

static void
WriteControlFile(int fd)
{
	/* contents are protected with a CRC */
	INIT_CRC32(cf_img.crc);
	COMP_CRC32(cf_img.crc,
		(char *) &cf_img, offsetof(ControlFile, crc));
	FIN_CRC32(cf_img.crc);

	if (pwrite(fd, &cf_img, sizeof(cf_img), 0) != sizeof(cf_img))
	{
		/* if write didn't set errno, assume problem is no disk space */
		if (errno == 0)
			errno = ENOSPC;
		ereport(FATAL,
			(errcode_errno(),
			 errmsg("could not write to control file \"%s\": %m",
			 	STATSINFO_CONTROL_FILE)));
	}
}

void
FsyncControlFile(void)
{
	Assert(cf_fd >= 0);	/* have not been opened the pg_statsinfo.control */

	elog(DEBUG2, "fsync control file (fsync method=\"fsync\")");
	if (fsync(cf_fd) != 0)
		ereport(ERROR,
			(errcode_errno(),
				errmsg("could not fsync control file \"%s\": %m",
				STATSINFO_CONTROL_FILE)));
}

void
FlushControlFile(void)
{
	Assert(cf_fd >= 0);	/* have not been opened the pg_statsinfo.control */

	if (SyncFile(cf_fd) != 0)
		ereport(ERROR,
			(errcode_errno(),
				errmsg("could not fsync control file \"%s\": %m",
				STATSINFO_CONTROL_FILE)));
}

static int
SyncFile(int fd)
{
#if defined(HAVE_SYNC_FILE_RANGE)
	elog(DEBUG2, "fsync file (fsync method=\"sync_file_range\")");
	return sync_file_range(fd, 0, 0,
		SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE);
#elif defined(HAVE_FDATASYNC)
	elog(DEBUG2, "fsync file (fsync method=\"fdatasync\")");
	return fdatasync(fd);
#else
	elog(DEBUG2, "fsync file (fsync method=\"fsync\")");
	return fsync(fd);
#endif
}
