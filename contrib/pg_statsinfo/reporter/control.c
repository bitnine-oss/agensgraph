/*
 * control.c
 *
 * Copyright (c) 2009-2017, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "pg_statsinfo.h"

/*
 * start pg_statsinfo background process
 */
void
do_start(PGconn *conn)
{
	PGresult	*res;

	/*
	 * call a function that start pg_statsinfo background process.
	 * Note:
	 * Not use pgut_command(), because don't want to write the error message
	 * defined by pgut_command() to console.
	 */
	res = PQexec(conn, "SELECT statsinfo.start(60)");

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		fprintf(stderr, "%s", PQerrorMessage(conn));

	PQclear(res);
}

/*
 * stop pg_statsinfo background process
 */
void
do_stop(PGconn *conn)
{
	PGresult	*res;

	/*
	 * call a function that stop pg_statsinfo background process.
	 * Note:
	 * Not use pgut_command(), because don't want to write the error message
	 * defined by pgut_command() to console.
	 */
	res = PQexec(conn, "SELECT statsinfo.stop(60)");

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
		fprintf(stderr, "%s", PQerrorMessage(conn));

	PQclear(res);
}
