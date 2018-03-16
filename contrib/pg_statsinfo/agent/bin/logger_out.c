/*
 * logger_out.c
 *
 * Copyright (c) 2009-2018, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "pg_statsinfod.h"

#ifdef HAVE_SYSLOG
#include <syslog.h>
#endif

static void format_log(const Log *log, const char *prefix,
					   PGErrorVerbosity verbosity, StringInfo buf);
static void append_with_tabs(StringInfo buf, const char *str);

#ifdef HAVE_SYSLOG

/* syslog facility */
static const int FACILITYNAMES[] =
{
	LOG_LOCAL0,
	LOG_LOCAL1,
	LOG_LOCAL2,
	LOG_LOCAL3,
	LOG_LOCAL4,
	LOG_LOCAL5,
	LOG_LOCAL6,
	LOG_LOCAL7
};

#ifndef PG_SYSLOG_LIMIT
#define PG_SYSLOG_LIMIT 1024
#endif

/*
 * write_syslog
 */
void
write_syslog(const Log *log,
			 const char *prefix,
			 PGErrorVerbosity verbosity,
			 const char *ident,
			 int facility)
{
	StringInfoData	buf;
	int				syslog_level;
	int				len;
	const char	   *nlpos;
	const char	   *line;

	static unsigned long seq = 0;
	static int	openlog_done = -1;

	initStringInfo(&buf);
	format_log(log, prefix, verbosity, &buf);

	/* open a connection to syslog */
	if (openlog_done != facility)
	{
		/* support reload? */
		openlog(ident ? ident : "postgres",
			LOG_CONS | LOG_NOWAIT, FACILITYNAMES[facility]);
		openlog_done = facility;
	}

	switch (log->elevel)
	{
		case DISABLE:
		case DEBUG5:
		case DEBUG4:
		case DEBUG3:
		case DEBUG2:
		case DEBUG1:
			syslog_level = LOG_DEBUG;
			break;
		case INFO:
		case LOG:
			syslog_level = LOG_INFO;
			break;
		case NOTICE:
		case WARNING:
			syslog_level = LOG_NOTICE;
			break;
		case ERROR:
		case ALERT:
			syslog_level = LOG_WARNING;
			break;
		case FATAL:
			syslog_level = LOG_ERR;
			break;
		case PANIC:
		default:
			syslog_level = LOG_CRIT;
			break;
	}

	/*
	 * We add a sequence number to each log message to suppress "same"
	 * messages.
	 */
	seq++;

	/*
	 * Our problem here is that many syslog implementations don't handle long
	 * messages in an acceptable manner. While this function doesn't help that
	 * fact, it does work around by splitting up messages into smaller pieces.
	 *
	 * We divide into multiple syslog() calls if message is too long or if the
	 * message contains embedded newline(s).
	 */
	line = buf.data;
	len = strlen(line);
	nlpos = strchr(line, '\n');
	if (len > PG_SYSLOG_LIMIT || nlpos != NULL)
	{
		int			chunk_nr = 0;

		while (len > 0)
		{
			char		buf[PG_SYSLOG_LIMIT + 1];
			int			buflen;
			int			i;

			/* if we start at a newline, move ahead one char */
			if (line[0] == '\n')
			{
				line++;
				len--;
				/* we need to recompute the next newline's position, too */
				nlpos = strchr(line, '\n');
				continue;
			}

			/* copy one line, or as much as will fit, to &buf */
			if (nlpos != NULL)
				buflen = nlpos - line;
			else
				buflen = len;
			buflen = Min(buflen, PG_SYSLOG_LIMIT);
			memcpy(&buf, line, buflen);
			buf[buflen] = '\0';

			/* already word boundary? */
			if (line[buflen] != '\0' &&
				!isspace((unsigned char) line[buflen]))
			{
				/* try to divide at word boundary */
				i = buflen - 1;
				while (i > 0 && !isspace((unsigned char) buf[i]))
					i--;

				if (i > 0)		/* else couldn't divide word boundary */
				{
					buflen = i;
					buf[i] = '\0';
				}
			}

			chunk_nr++;

			syslog(syslog_level, "[%lu-%d] %s", seq, chunk_nr, buf);
			line += buflen;
			len -= buflen;
		}
	}
	else
	{
		/* message short enough */
		syslog(syslog_level, "[%lu] %s", seq, line);
	}

	termStringInfo(&buf);
}

#elif defined(WIN32)

static const int CODEPAGE[] =
{
	0,		/* SQL_ASCII */
	20932,	/* EUC_JP */
	20936,	/* EUC_CN */
	51949,	/* EUC_KR */
	0,		/* EUC_TW */
	20932,	/* EUC_JIS_2004 */
	65001,	/* UTF8 */
	0,		/* MULE_INTERNAL */
	28591,	/* LATIN1 */
	28592,	/* LATIN2 */
	28593,	/* LATIN3 */
	28594,	/* LATIN4 */
	28599,	/* LATIN5 */
	0,		/* LATIN6 */
	0,		/* LATIN7 */
	0,		/* LATIN8 */
	28605,	/* LATIN9 */
	0,		/* LATIN10 */
	1256,	/* WIN1256 */
	1258	/* WIN1258 */,
	866,	/* WIN866 */
	874,	/* WIN874 */
	20866,	/* KOI8R */
	1251,	/* WIN1251 */
	1252,	/* WIN1252 */
	28595,	/* ISO_8859_5 */
	28596,	/* ISO_8859_6 */
	28597,	/* ISO_8859_7 */
	28598,	/* ISO_8859_8 */
	1250,	/* WIN1250 */
	1253,	/* WIN1253 */
	1254,	/* WIN1254 */
	1255,	/* WIN1255 */
	1257,	/* WIN1257 */
	21866,	/* KOI8U */
	932,	/* SJIS */
	950,	/* BIG5 */
	936,	/* GBK */
	0,		/* UHC */
	54936,	/* GB18030 */
	0,		/* JOHAB */
	932,	/* SHIFT_JIS_2004 */
};

void
write_syslog(const Log *log,
			 const char *prefix,
			 PGErrorVerbosity verbosity,
			 const char *ident,
			 int facility)
{
	StringInfoData	buf;
	WCHAR		   *utf16;
	int				eventlevel;

	static HANDLE	evtHandle = INVALID_HANDLE_VALUE;

	initStringInfo(&buf);
	format_log(log, prefix, verbosity, &buf);

	if (evtHandle == INVALID_HANDLE_VALUE)
	{
		evtHandle = RegisterEventSourceA(NULL, "PostgreSQL");
		if (evtHandle == NULL)
		{
			evtHandle = INVALID_HANDLE_VALUE;
			return;
		}
	}

	switch (log->elevel)
	{
		case DISABLE:
		case DEBUG5:
		case DEBUG4:
		case DEBUG3:
		case DEBUG2:
		case DEBUG1:
		case INFO:
		case LOG:
		case NOTICE:
			eventlevel = EVENTLOG_INFORMATION_TYPE;
			break;
		case WARNING:
		case ALERT:
			eventlevel = EVENTLOG_WARNING_TYPE;
			break;
		case ERROR:
		case FATAL:
		case PANIC:
		default:
			eventlevel = EVENTLOG_ERROR_TYPE;
			break;
	}

	/*
	 * Convert message to UTF16 text and write it with ReportEventW.
	 */
	utf16 = pgut_newarray(WCHAR, buf.len + 1);
	MultiByteToWideChar(
		CODEPAGE[server_encoding], 0, buf.data, 0, utf16, buf.len + 1);
	ReportEventW(evtHandle,
				 eventlevel,
				 0,
				 0,		/* All events are Id 0 */
				 NULL,
				 1,
				 0,
				 (LPCWSTR *) &utf16,
				 NULL);
	free(utf16);

	termStringInfo(&buf);
}

#else

void
write_syslog(const Log *log,
			 const char *prefix,
			 PGErrorVerbosity verbosity,
			 const char *ident,
			 int facility)
{
	/* ignore */
}

#endif

/*
 * write_textlog
 */
bool
write_textlog(const Log *log, const char *prefix, PGErrorVerbosity verbosity, FILE *fp)
{
	StringInfoData	buf;

	initStringInfo(&buf);
	format_log(log, prefix, verbosity, &buf);
	if (fwrite(buf.data, 1, buf.len, fp) != buf.len)
		return false;
	fflush(fp);

	termStringInfo(&buf);
	return true;
}

static void
appendLogLinePrefix(const Log *log, const char *prefix, StringInfo buf)
{
	size_t	msg_len;
	size_t	i;

	/*
	 * format prefix
	 */
	msg_len = strlen(prefix);
	for (i = 0; i < msg_len; i++)
	{
		if (prefix[i] != '%')
		{
			/* literal char, just copy */
			appendStringInfoChar(buf, prefix[i]);
			continue;
		}
		/* go to char after '%' */
		i++;
		if (i >= msg_len)
			break;				/* format error - ignore it */

		/* process the option */
		switch (prefix[i])
		{
			case 'a':
			{
				const char *appname = log->application_name;

				if (appname == NULL || *appname == '\0')
					appname = "[unknown]";
				appendStringInfoString(buf, appname);
				break;
			}
			case 'u':
				appendStringInfoString(buf, log->username);
				break;
			case 'd':
				appendStringInfoString(buf, log->database);
				break;
			case 'c':
				appendStringInfoString(buf, log->session_id);
				break;
			case 'p':
				appendStringInfoString(buf, log->pid);
				break;
			case 'l':
				appendStringInfoString(buf, log->session_line_num);
				break;
			case 'm':
				appendStringInfoString(buf, log->timestamp);
				break;
			case 't':
				if (log->timestamp)
				{
					/*
					 * The format is as below, but we skip ".NNN" part.
					 *	123456789012345678901234567
					 *	YYYY-MM-DD HH:MI:SS.NNN ZZZ
					 */
					appendBinaryStringInfo(buf, log->timestamp, 19);
					appendStringInfoString(buf, log->timestamp + 23);
				}
				break;
			case 's':
				appendStringInfoString(buf, log->session_start);
				break;
			case 'i':
				appendStringInfoString(buf, log->ps_display);
				break;
			case 'r':
			case 'h':
				if (log->client_addr && log->client_addr[0])
				{
					const char *sep;

					/* client_addr might be "host:port" or "[local]". */
					if (*log->client_addr == '[' ||
						(sep = strrchr(log->client_addr, ':')) == NULL)
						appendStringInfoString(buf, log->client_addr);
					else
					{
						appendBinaryStringInfo(buf, log->client_addr,
											   sep - log->client_addr);

						/* print "host(port)" for 'r'. */
						if (prefix[i] == 'r')
							appendStringInfo(buf, "(%s)", sep + 1);
					}
				}
				break;
			case 'q':
				if (log->username == NULL || log->username[0] == '\0')
					i = msg_len;
				break;
			case 'v':
				appendStringInfoString(buf, log->vxid);
				break;
			case 'x':
				appendStringInfoString(buf, log->xid);
				break;
			case 'e':
				appendStringInfoString(buf, log->sqlstate);
				break;
			case '%':
				appendStringInfoChar(buf, '%');
				break;
			default:
				/* format error - ignore it */
				break;
		}
	}
}

static void
appendLogField(StringInfo buf, const Log *log, const char *prefix,
			   const char *message, const char *tag)
{
	if (message[0])
	{
		appendLogLinePrefix(log, prefix, buf);
		appendStringInfoString(buf, _(tag));
		append_with_tabs(buf, message);
		appendStringInfoChar(buf, '\n');
	}
}

/*
 * msg_log -  Format tag info for log lines.
 */
static void
format_log(const Log *log,
		   const char *prefix,
		   PGErrorVerbosity verbosity,
		   StringInfo buf)
{
	appendLogLinePrefix(log, prefix, buf);
	appendStringInfo(buf, "%s:  ", elevel_to_str(log->elevel));

	if (verbosity >= PGERROR_VERBOSE)
		appendStringInfo(buf, "%s: ", log->sqlstate);

	if (log->message[0])
		append_with_tabs(buf, log->message);
	else
		append_with_tabs(buf, _("missing error text"));

	if (log->user_query_pos[0])
		appendStringInfo(buf, _(" at character %s"), log->user_query_pos);
	else if (log->query_pos[0])
		appendStringInfo(buf, _(" at character %s"), log->query_pos);

	appendStringInfoChar(buf, '\n');
	if (verbosity >= PGERROR_DEFAULT)
	{
		appendLogField(buf, log, prefix, log->detail, "DETAIL:  ");
		appendLogField(buf, log, prefix, log->hint, "HINT:  ");
		appendLogField(buf, log, prefix, log->query, "QUERY:  ");
		appendLogField(buf, log, prefix, log->context, "CONTEXT:  ");
		if (verbosity >= PGERROR_VERBOSE)
			appendLogField(buf, log, prefix, log->error_location, "LOCATION:  ");
	}
	appendLogField(buf, log, prefix, log->user_query, "STATEMENT:  ");
}

/*
 *	append_with_tabs
 *
 *	Append the string to the StringInfo buffer, inserting a tab after any
 *	newline.
 */
static void
append_with_tabs(StringInfo buf, const char *str)
{
	char		ch;

	while ((ch = *str++) != '\0')
	{
		appendStringInfoChar(buf, ch);
		if (ch == '\n')
			appendStringInfoChar(buf, '\t');
	}
}
