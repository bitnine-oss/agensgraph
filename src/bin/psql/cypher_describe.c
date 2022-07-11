/*
 * psql - the PostgreSQL interactive terminal
 *
 * Support for the various \d ("describe") commands.  Note that the current
 * expectation is that all functions in this file will succeed when working
 * with servers of versions 7.4 and up.  It's okay to omit irrelevant
 * information for an old server, but not to fail outright.
 *
 * Copyright (c) 2022 by Bitnine Global, Inc.
 *
 * src/bin/psql/cypher_describe.c
 */
#include "postgres_fe.h"

#include "fe_utils/string_utils.h"

#include "common.h"
#include "pqexpbuffer.h"
#include "settings.h"

#include "cypher_describe.h"
/*
 * \dGi
 *
 * Describes graph property indexes (Agens)
 */
bool
listGraphIndexes(const char *pattern, bool verbose)
{
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;

	initPQExpBuffer(&buf);
	printfPQExpBuffer(&buf,
					  "SELECT pi.graphname AS \"%s\",\n"
					  "  pi.labelname AS \"%s\",\n"
					  "  pi.indexname AS \"%s\",\n"
					  "  pi.unique AS \"%s\",\n"
					  "  pi.owner AS \"%s\",\n"
					  "  pi.indexdef AS \"%s\"",
					  gettext_noop("Graph"), gettext_noop("LabelName"),
					  gettext_noop("IndexName"), gettext_noop("Unique"),
					  gettext_noop("Owner"), gettext_noop("Indexdef"));

	if (verbose)
	{
		appendPQExpBuffer(&buf,
						  ",\n  pi.size AS \"%s\""
						  ",\n  pi.description AS \"%s\"",
						  gettext_noop("Size"), gettext_noop("Description"));
	}

	appendPQExpBuffer(&buf,
					  "\nFROM pg_catalog.ag_property_indexes pi\n");

	processSQLNamePattern(pset.db, &buf, pattern, false, false,
						  "pi.graphname", "pi.indexname", NULL, NULL);
	appendPQExpBufferStr(&buf, "\nORDER BY 1, 2, 3;");

	res = PSQLexec(buf.data);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	if (PQntuples(res) == 0 && !pset.quiet)
	{
		if (pattern == NULL)
			fprintf(pset.queryFout, _("No property indexes found.\n"));
		else
			fprintf(pset.queryFout, _("No matching property indexes found.\n"));
	}
	else
	{
		myopt.nullPrint = NULL;
		myopt.title = _("List of property indexes");
		myopt.translate_header = true;

		printQuery(res, &myopt, pset.queryFout, false, pset.logfile);
	}

	PQclear(res);
	return true;
}
