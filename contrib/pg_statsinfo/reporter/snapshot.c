/*
 * snapshot.c
 *
 * Copyright (c) 2009-2017, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "pg_statsinfo.h"

#define SQL_SELECT_SNAPSHOT_LIST "\
SELECT \
	s.snapid, \
	i.instid, \
	i.hostname, \
	i.port, \
	s.time::timestamp(0), \
	s.comment, \
	s.exec_time::interval(0), \
	statsrepo.pg_size_pretty(s.snapshot_increase_size) \
FROM \
	statsrepo.snapshot s \
	LEFT JOIN statsrepo.instance i ON s.instid = i.instid"

#define SQL_SELECT_SNAPSHOT_SIZE "\
SELECT \
	i.instid, \
	i.name, \
	i.hostname, \
	i.port, \
	count(*), \
	sum(s.snapshot_increase_size)::numeric(1000), \
	max(s.snapid), \
	max(s.time)::timestamp(0) \
FROM \
	statsrepo.snapshot s \
	LEFT JOIN statsrepo.instance i ON s.instid = i.instid \
GROUP BY \
	i.instid, \
	i.name, \
	i.hostname, \
	i.port \
ORDER BY \
	i.instid"

static char *size_pretty(int64 size);

/*
 * show the snapshot list
 */
void
do_list(PGconn *conn, const char *instid)
{
	PGresult		*res;
	StringInfoData	 query;
	int64			 i_id;
	int				 i;

	initStringInfo(&query);
	appendStringInfo(&query, SQL_SELECT_SNAPSHOT_LIST);

	/* get the snapshot list */
	if (instid)
	{
		/* validate value of instance ID */
		if (!parse_int64(instid, &i_id) || i_id <= 0)
			ereport(ERROR,
				(errcode(EINVAL),
				 errmsg("invalid instance ID (--instid) : '%s'", instid)));

		/* get a list of the specified instances snapshot */
		appendStringInfo(&query, "\nWHERE i.instid = $1");
		appendStringInfo(&query, "\nORDER BY s.snapid");
		res = pgut_execute(conn, query.data, 1, &instid);
	}
	else
	{
		/* get a list of all instances snapshot */
		appendStringInfo(&query, "\nORDER BY s.snapid");
		res = pgut_execute(conn, query.data, 0, NULL);
	}

	/* show the snapshot list */
	printf("----------------------------------------\n");
	printf("Snapshot List\n");
	printf("----------------------------------------\n");
	printf("%10s  %10s  %-32s  %8s  %20s  %-20s  %12s  %8s\n",
		"SnapshotID", "InstanceID", "Host", "Port", "Timestamp", "Comment", "Execute Time", "Size");
	printf("-----------------------------------------------------------------------------------------------------------------------------------------\n");
	for(i = 0; i < PQntuples(res); i++)
	{
		printf("%10s  %10s  %-32s  %8s  %20s  %-20s  %12s  %8s\n",
			PQgetvalue(res, i, 0),
			PQgetvalue(res, i, 1),
			PQgetvalue(res, i, 2),
			PQgetvalue(res, i, 3),
			PQgetvalue(res, i, 4),
			PQgetvalue(res, i, 5),
			PQgetvalue(res, i, 6),
			PQgetvalue(res, i, 7));
	}
	PQclear(res);
	termStringInfo(&query);
}

/*
 * show the information about size of snapshot
 */
void
do_size(PGconn *conn)
{
	PGresult		*res;
	StringInfoData	 query;
	int64			 total_size = 0;
	char			*pretty_size;
	int				 i;

	initStringInfo(&query);
	appendStringInfo(&query, SQL_SELECT_SNAPSHOT_SIZE);

	/* get snapshot size of each instance */
	res = pgut_execute(conn, query.data, 0, NULL);

	/* show the snapshot size for each instance */
	printf("----------------------------------------\n");
	printf("Snapshot Size Information\n");
	printf("----------------------------------------\n");
	for (i = 0; i < PQntuples(res); i++)
	{
		int64	 size;

		/* convert the value of snapshot size */
		parse_int64(PQgetvalue(res, i, 5), &size);	/* no need to validate values */
		pretty_size = size_pretty(size);

		printf("Instance ID                : %s\n", PQgetvalue(res, i, 0));
		printf("Database System ID         : %s\n", PQgetvalue(res, i, 1));
		printf("Host                       : %s\n", PQgetvalue(res, i, 2));
		printf("Port                       : %s\n", PQgetvalue(res, i, 3));
		printf("Number Of Snapshots        : %s\n", PQgetvalue(res, i, 4));
		printf("Snapshot Size              : %s\n", pretty_size);
		printf("Latest Snapshot ID         : %s\n", PQgetvalue(res, i, 6));
		printf("Latest Snapshot Timestamp  : %s\n\n", PQgetvalue(res, i, 7));

		total_size += size;
		free(pretty_size);
	}
	/* total size of the snapshot of all instances */
	pretty_size = size_pretty(total_size);
	printf("Total Snapshot Size  : %s\n\n", pretty_size);

	PQclear(res);
	termStringInfo(&query);
	free(pretty_size);
}

/*
 * get a snapshot
 */
void
do_snapshot(PGconn *conn, const char *comment)
{
	/* call a function that get snapshot */
	pgut_command(conn, "SELECT statsinfo.snapshot($1)", 1, &comment);
}

/*
 * delete a snapshot
 */
void
do_delete(PGconn *conn, const char *targetid)
{
	int64	 snapid;

	/* validate value of snapshot ID */
	if (!parse_int64(targetid, &snapid) || snapid <= 0)
		ereport(ERROR,
			(errcode(EINVAL),
			 errmsg("invalid snapshot ID: '%s'", targetid)));

	/* call a function that delete snapshot */
	pgut_command(conn, "SELECT statsrepo.del_snapshot($1::bigint)", 1, &targetid);
}

/*
 * formatting with size units
 */
static char *
size_pretty(int64 size)
{
	char	buf[64];
	int64	limit = 10 * 1024;
	int64	mult = 1;

	if (size < limit * mult)
		snprintf(buf, sizeof(buf), INT64_FORMAT " bytes", size);
	else
	{
		mult *= 1024;
		if (size < limit * mult)
			snprintf(buf, sizeof(buf), INT64_FORMAT " KiB",
					 (size + mult / 2) / mult);
		else
		{
			mult *= 1024;
			if (size < limit * mult)
				snprintf(buf, sizeof(buf), INT64_FORMAT " MiB",
						 (size + mult / 2) / mult);
			else
			{
				mult *= 1024;
				if (size < limit * mult)
					snprintf(buf, sizeof(buf), INT64_FORMAT " GiB",
							 (size + mult / 2) / mult);
				else
				{
					/* Here we have to worry about avoiding overflow */
					int64	val;

					mult *= 1024;
					val = size / mult;
					if ((size % mult) >= (mult / 2))
						val++;
					snprintf(buf, sizeof(buf), INT64_FORMAT " TiB",
							 val);
				}
			}
		}
	}

	return pgut_strdup(buf);
}
