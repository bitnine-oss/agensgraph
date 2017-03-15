/*
 * logger_in.c : parse csvlog
 *
 * Copyright (c) 2009-2017, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "pg_statsinfod.h"

static bool badcsv(int column, const char *message);
static int MatchText(const char *t, size_t tlen,
					 const char *p, size_t plen, List **params);

/*
 * logger_next
 */

#define CSV_READ_RETRY_MAX				3

bool
read_csv(FILE *fp, StringInfo buf, int ncolumns, size_t *columns)
{
	int			column;
	int			retry_count;
	size_t		read_len;
	size_t		restart;

	if (buf->data == NULL)
		initStringInfo(buf);
	else
		resetStringInfo(buf);
	memset(columns, 0, ncolumns * sizeof(size_t));
	column = 0;
	retry_count = 0;
	restart = 0;

retry:
	read_len = 0;
	do
	{
		char   *buffer;
		size_t	len;

		enlargeStringInfo(buf, 1000);
		buffer = buf->data + buf->len;

		if (fgets(buffer, buf->maxlen - buf->len, fp) == NULL)
			break;

		len = strlen(buffer);
		if (len == 0)
			break;
		read_len += len;

		buf->len += len;
	} while (buf->data[buf->len - 1] != '\n');

	if (buf->len == 0)
		return false;

	if (read_len == 0 || buf->data[buf->len - 1] != '\n')
	{
		if (retry_count < CSV_READ_RETRY_MAX)
		{
			usleep(100 * 1000);	/* 100ms */
			retry_count++;
			goto retry;
		}	
		return badcsv(column + 1, buf->data);
	}
	
	/* split log with comma */
	while (column < ncolumns)
	{
		char   *buffer;
		char   *next;

		buffer = buf->data + columns[column];
		if (buffer[0] == '\0')
			break;
		else if (buffer[0] == '"')
		{
			/* quoted field */
			if (restart)
			{
				buffer = buf->data + restart;
				restart = 0;
			}
			else
				buffer++;

			for (;;)
			{
				next = strchr(buffer, '"');
				if (next == NULL)
				{
					/* save parse restart point */
					restart = buf->len;
					goto retry;	/* line-break in quote, needs more buffers */
				}
				else if (next[1] == ',' || next[1] == '\n')
				{
					next[0] = '\0';
					columns[column]++;
					columns[++column] = next - buf->data + 2;
					break;
				}
				else if (next[1] == '"')
				{
					/* combine "" to " */
					memmove(next, next + 1, strlen(next + 1) + 1);
					buf->len--;
					buffer = next + 1;
				}
				else
				{
					return badcsv(column + 1, buf->data);
				}
			}
		}
		else
		{
			/* unquoted field */
			next = strpbrk(buffer, ",\n");
			if (next == NULL)
			{
				return badcsv(column + 1, buf->data);
			}
			else
			{
				next[0] = '\0';
				columns[++column] = next - buf->data + 1;
			}
		}
	}

	/* throw an error if column number does not reach a necessary number. */
	if (column < ncolumns)
		return badcsv(column + 1, buf->data);

	return true;
}

/*
 * badcsv - always return false.
 */
static bool
badcsv(int column, const char *message)
{
	elog(WARNING, "cannot parse csvlog column %d: %s", column, message);
	return false;
}

#define LIKE_TRUE						1
#define LIKE_FALSE						0
#define LIKE_ABORT						(-1)

bool
match(const char *str, const char *pattern)
{
	int		r;

	r = MatchText(str, strlen(str), pattern, strlen(pattern), NULL);

	return (r == LIKE_TRUE);
}

List *
capture(const char *str, const char *pattern, int nparams)
{
	int		r;
	List   *params = NIL;

	r = MatchText(str, strlen(str), pattern, strlen(pattern), &params);
	if (r == LIKE_TRUE && list_length(params) == nparams)
		return params;

	list_free_deep(params);
	return NIL;
}

#define pg_mblen(p)		1
#define NextByte(p, plen)	((p)++, (plen)--)

/* Set up to compile like_match.c for multibyte characters */
#define CHAREQ(p1, p2) wchareq((p1), (p2))
#define NextChar(p, plen) \
	do { int __l = pg_mblen(p); (p) +=__l; (plen) -=__l; } while (0)

static int
MatchText(const char *t, size_t tlen, const char *p, size_t plen, List **params)
{
	while (tlen > 0 && plen > 0)
	{
		if (plen < 2 || *p != '%')
		{
			/* non-wildcard pattern char fails to match text char */
			if (*p != *t)
				return LIKE_FALSE;
		}
		else if (p[1] == '%')
		{
			/* %% is % */
			NextByte(p, plen);
			if (*p != *t)
				return LIKE_FALSE;
		}
		else
		{
			const char *begin = p;
			const char *w = t;
			char		firstpat;

			/* Skip until the type specifer */
			p = strpbrk(begin + 1, "diouxXeEfFgGaAcspm");
			if (p == NULL)
				return LIKE_FALSE;	/* bad format */
			p++;
			plen -= p - begin;
			if (plen <= 0)
			{
				if (params)
					*params = lcons(strdup_with_len(t, tlen), *params);
				return LIKE_TRUE;	/* matches everything. */
			}

			/*
			 * Otherwise, scan for a text position at which we can match the
			 * rest of the pattern.
			 */
			firstpat = *p;

			while (tlen > 0)
			{
				/*
				 * Optimization to prevent most recursion: don't recurse
				 * unless first pattern byte matches first text byte.
				 */
				if (*t == firstpat)
				{
					int		matched = MatchText(t, tlen, p, plen, params);

					if (matched == LIKE_TRUE && params)
						*params = lcons(strdup_with_len(w, t - w), *params);
					if (matched != LIKE_FALSE)
						return matched;		/* TRUE or ABORT */
				}

				NextChar(t, tlen);
			}

			/*
			 * End of text with no match, so no point in trying later places
			 * to start matching this pattern.
			 */
			return LIKE_ABORT;
		}

		NextByte(t, tlen);
		NextByte(p, plen);
	}
	if (tlen > 0)
		return LIKE_FALSE;		/* end of pattern, but not of text */

	/* End of text string.	Do we have matching pattern remaining? */
	while (plen > 0 && *p == '%')
	{
		const char *begin = p;
		p = strpbrk(begin + 1, "diouxXeEfFgGaAcspm");
		if (p == NULL)
			return LIKE_FALSE;	/* bad format */
		p++;
		plen -= p - begin;
	}
	if (plen <= 0)
		return LIKE_TRUE;

	/*
	 * End of text with no match, so no point in trying later places to start
	 * matching this pattern.
	 */
	return LIKE_ABORT;
}
