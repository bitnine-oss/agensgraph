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

#include "fe_utils/mbprint.h"
#include "fe_utils/string_utils.h"

#include "common.h"
#include "common/logging.h"
#include "describe.h"
#include "pqexpbuffer.h"
#include "settings.h"

#include "cypher_describe.h"

static bool describeOneLabelDetails(const char *graphname,
									const char *labelname);

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
						  "pi.graphname", "pi.indexname", NULL, NULL, NULL, NULL);
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

/*
 * \dG
 *
 * Describes graphs (Agens)
 */
bool
listGraphs(const char *pattern, bool verbose)
{
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;

	initPQExpBuffer(&buf);
	printfPQExpBuffer(&buf,
					  "SELECT g.graphname AS \"%s\",\n"
					  "  pg_catalog.pg_get_userbyid(n.nspowner) AS \"%s\"",
					  gettext_noop("Name"), gettext_noop("Owner"));

	if (verbose)
	{
		appendPQExpBufferStr(&buf, ",\n  ");
		printACLColumn(&buf, "n.nspacl");
		appendPQExpBuffer(&buf,
						  ",\n  pg_catalog.obj_description(g.oid, 'ag_graph') AS \"%s\"",
						  gettext_noop("Description"));
	}

	appendPQExpBuffer(&buf,
					  "\nFROM pg_catalog.ag_graph g\n"
					  "  LEFT JOIN pg_catalog.pg_namespace n ON n.oid = g.nspid\n");

	processSQLNamePattern(pset.db, &buf, pattern, false, false,
						  NULL, "g.graphname", NULL, NULL, NULL, NULL);

	appendPQExpBufferStr(&buf, "ORDER BY 1;");

	res = PSQLexec(buf.data);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	if (PQntuples(res) == 0 && !pset.quiet)
	{
		if (pattern == NULL)
			fprintf(pset.queryFout, _("No graphs found.\n"));
		else
			fprintf(pset.queryFout, _("No matching graphs found.\n"));
	}
	else
	{
		myopt.nullPrint = NULL;
		myopt.title = _("List of graphs");
		myopt.translate_header = true;

		printQuery(res, &myopt, pset.queryFout, false, pset.logfile);
	}

	PQclear(res);
	return true;
}

/*
 * \dGl, \dGv, \dGe
 *
 * Describes graph labels (Agens)
 */
bool
listLabels(const char *pattern, bool verbose, const char labkind)
{
	bool		showVertices = (labkind == 'v' || labkind == '\0');
	bool		showEdges = (labkind == 'e' || labkind == '\0');
	PQExpBufferData buf;
	PGresult   *res;
	printQueryOpt myopt = pset.popt;

	initPQExpBuffer(&buf);
	printfPQExpBuffer(&buf,
					  "SELECT g.graphname AS \"%s\",\n"
					  "  l.labname AS \"%s\",\n"
					  "  CASE l.labkind\n"
					  "    WHEN 'v' THEN '%s'\n"
					  "    WHEN 'e' THEN '%s'\n"
					  "  END AS \"%s\",\n"
					  "  pg_catalog.pg_get_userbyid(c.relowner) AS \"%s\"",
					  gettext_noop("Graph"), gettext_noop("Name"),
					  gettext_noop("vertex"), gettext_noop("edge"), gettext_noop("Type"),
					  gettext_noop("Owner"));

	if (verbose)
	{
		appendPQExpBuffer(&buf,
						  ",\n  pg_catalog.pg_size_pretty(pg_catalog.pg_table_size(c.oid)) AS \"%s\""
						  ",\n  pg_catalog.obj_description(l.oid, 'ag_label') AS \"%s\"",
						  gettext_noop("Size"), gettext_noop("Description"));
	}

	appendPQExpBuffer(&buf,
					  "\nFROM pg_catalog.ag_label l\n"
					  "  LEFT JOIN pg_catalog.pg_class c ON c.oid = l.relid\n"
					  "  LEFT JOIN pg_catalog.ag_graph g ON g.oid = l.graphid\n");

	appendPQExpBufferStr(&buf, "WHERE l.labkind IN (");
	if (showVertices)
		appendPQExpBufferStr(&buf, "'v', ");
	if (showEdges)
		appendPQExpBufferStr(&buf, "'e', ");
	appendPQExpBufferStr(&buf, "'')\n");	/* dummy */

	processSQLNamePattern(pset.db, &buf, pattern, true, false,
						  "g.graphname", "l.labname", NULL, NULL, NULL, NULL);

	appendPQExpBufferStr(&buf, "ORDER BY 1, 3, 2;");

	res = PSQLexec(buf.data);
	termPQExpBuffer(&buf);
	if (!res)
		return false;

	if (PQntuples(res) == 0 && !pset.quiet)
	{
		if (pattern == NULL)
			fprintf(pset.queryFout, _("No labels found.\n"));
		else
			fprintf(pset.queryFout, _("No matching labels found.\n"));
	}
	else
	{
		myopt.nullPrint = NULL;
		myopt.title = _("List of labels");
		myopt.translate_header = true;

		printQuery(res, &myopt, pset.queryFout, false, pset.logfile);

		if (labkind == 'v' || labkind == 'e')
		{
			int			i;

			for (i = 0; i < PQntuples(res); i++)
			{
				const char *nspname;
				const char *labname;

				nspname = PQgetvalue(res, i, 0);
				labname = PQgetvalue(res, i, 1);

				if (!describeOneLabelDetails(nspname, labname))
				{
					PQclear(res);
					return false;
				}
			}
		}
	}

	PQclear(res);
	return true;
}

/*
 * describeOneLabelDetails (for \dGv, \dGe)
 *
 * Based on describeOneTableDetails()
 */
static bool
describeOneLabelDetails(const char *graphname, const char *labelname)
{
	PQExpBufferData buf;
	PGresult   *res = NULL;
	printTableOpt myopt = pset.popt.topt;
	printTableContent cont;
	bool		printTableInitialized = false;
	int			i;
	char	   *view_def = NULL;
	char	  **seq_values = NULL;
	char	  **modifiers = NULL;
	char	  **ptr;
	PQExpBufferData title;
	struct
	{
		int16		checks;
		char		relkind;
		char		labkind;
		bool		hasindex;
		Oid			tablespace;
	}			labelinfo;
	bool		retval;

	retval = false;

	myopt.default_footer = false;
	/* This output looks confusing in expanded mode. */
	myopt.expanded = false;

	initPQExpBuffer(&buf);
	initPQExpBuffer(&title);

	/* AgensGraph is based on Postgres 9.5 and later */
	Assert(pset.sversion >= 90500);

	printfPQExpBuffer(&buf,
					  "SELECT c.relchecks, c.relkind, l.labkind, "
					  "c.relhasindex, c.reltablespace\n"
					  "FROM pg_catalog.pg_class c, "
					  "pg_catalog.ag_label l, pg_catalog.ag_graph g\n"
					  "WHERE c.oid = l.relid AND l.graphid = g.oid AND "
					  "l.labname = '%s' AND g.graphname = '%s';",
					  labelname, graphname);

	res = PSQLexec(buf.data);
	if (!res)
		goto error_return;

	/* Did we get anything? */
	if (PQntuples(res) == 0)
	{
		if (!pset.quiet)
			pg_log_error("Did not find any relation for \"%s.%s\"\n",
						 graphname, labelname);
		goto error_return;
	}
	if (PQntuples(res) > 1)
	{
		if (!pset.quiet)
			pg_log_error("Found too many relations for \"%s.%s\"\n",
						 graphname, labelname);
		goto error_return;
	}

	labelinfo.checks = atoi(PQgetvalue(res, 0, 0));
	labelinfo.relkind = *(PQgetvalue(res, 0, 1));
	labelinfo.labkind = *(PQgetvalue(res, 0, 2));
	labelinfo.hasindex = strcmp(PQgetvalue(res, 0, 3), "t") == 0;
	labelinfo.tablespace = atooid(PQgetvalue(res, 0, 4));

	PQclear(res);
	res = NULL;

	/* Make title */
	switch (labelinfo.labkind)
	{
		case 'v':
			printfPQExpBuffer(&title, _("Vertex label \"%s.%s\""),
							  graphname, labelname);
			break;
		case 'e':
			printfPQExpBuffer(&title, _("Edge label \"%s.%s\""),
							  graphname, labelname);
			break;
		default:
			/* untranslated unknown labkind */
			printfPQExpBuffer(&title, "?%c? \"%s.%s\"",
							  labelinfo.labkind, graphname, labelname);
			break;
	}

	printTableInit(&cont, &myopt, title.data, 0, 0);
	printTableInitialized = true;

	/* print property indexes */
	if (labelinfo.hasindex)
	{
		/* Footer information about a table */
		PGresult   *result = NULL;
		int			tuples = 0;

		printfPQExpBuffer(&buf,
						  "SELECT c.relname, i.indisunique, "
						  "i.indisclustered, i.indisvalid, i.indisreplident, "
						  "pg_catalog.ag_get_propindexdef(i.indexrelid), "
						  "c.reltablespace\n"
						  "FROM pg_catalog.pg_index i, pg_catalog.pg_class c, "
						  "pg_catalog.ag_label l, pg_catalog.ag_graph g\n"
						  "WHERE i.indexrelid = c.oid AND "
						  "i.indrelid = l.relid AND l.graphid = g.oid AND "
						  "l.labname = '%s' AND g.graphname = '%s' AND "
						  "i.indisexclusion = false AND i.indexprs IS NOT NULL\n"
						  "ORDER BY 1;",
						  labelname, graphname);

		result = PSQLexec(buf.data);
		if (!result)
			goto error_return;
		else
			tuples = PQntuples(result);

		if (tuples > 0)
		{
			printTableAddFooter(&cont, _("Property Indexes:"));

			for (i = 0; i < tuples; i++)
			{
				const char *indexdef;
				const char *usingpos;

				/* untranslated index name */
				printfPQExpBuffer(&buf, "    \"%s\"",
								  PQgetvalue(result, i, 0));

				if (strcmp(PQgetvalue(result, i, 1), "t") == 0)
					appendPQExpBufferStr(&buf, " UNIQUE");

				/* Everything after "USING" is echoed verbatim */
				indexdef = PQgetvalue(result, i, 5);
				usingpos = strstr(indexdef, " USING ");
				if (usingpos)
					indexdef = usingpos + 7;

				appendPQExpBuffer(&buf, " %s", indexdef);

				/* Add these for all cases */

				if (strcmp(PQgetvalue(result, i, 2), "t") == 0)
					appendPQExpBufferStr(&buf, " CLUSTER");

				if (strcmp(PQgetvalue(result, i, 3), "t") != 0)
					appendPQExpBufferStr(&buf, " INVALID");

				if (strcmp(PQgetvalue(result, i, 4), "t") == 0)
					appendPQExpBuffer(&buf, " REPLICA IDENTITY");

				printTableAddFooter(&cont, buf.data);
				add_tablespace_footer(&cont, 'i',
									  atooid(PQgetvalue(result, i, 6)), false);
			}
		}

		PQclear(result);
	}

	/* print label constraints */
	if (labelinfo.checks || labelinfo.hasindex)
	{
		/* Footer information about a table */
		PGresult   *result = NULL;
		int			tuples = 0;

		printfPQExpBuffer(&buf,
						  "SELECT r.conname, "
						  "pg_catalog.ag_get_graphconstraintdef(r.oid)\n"
						  "FROM pg_catalog.pg_constraint r, "
						  "pg_catalog.ag_label l, pg_catalog.ag_graph g\n"
						  "WHERE r.conrelid = l.relid AND l.graphid = g.oid AND "
						  "l.labname = '%s' AND g.graphname = '%s' AND "
						  "r.contype IN ('c','x')\n"
						  "ORDER BY 1;",
						  labelname, graphname);

		result = PSQLexec(buf.data);
		if (!result)
			goto error_return;
		else
			tuples = PQntuples(result);

		if (tuples > 0)
		{
			printTableAddFooter(&cont, _("Constraints:"));

			for (i = 0; i < tuples; i++)
			{
				/* untranslated contraint name and def */
				printfPQExpBuffer(&buf, "    \"%s\"",
								  PQgetvalue(result, i, 0));

				/*
				 * TODO: transform that removed properties expression and
				 * unique constraint expr
				 */
				appendPQExpBuffer(&buf, " %s", PQgetvalue(result, i, 1));

				printTableAddFooter(&cont, buf.data);
			}
		}

		PQclear(result);
	}

	/*
	 * Finish printing the footer information about a table.
	 */
	if (labelinfo.labkind == 'v' || labelinfo.labkind == 'e')
	{
		PGresult   *result;
		int			tuples;
		const char *ct = _("Child labels");
		int			ctw = pg_wcswidth(ct, strlen(ct), pset.encoding);

		/* print inherited tables */
		printfPQExpBuffer(&buf,
						  "SELECT c.oid::pg_catalog.regclass\n"
						  "FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i, "
						  "pg_catalog.ag_label l, pg_catalog.ag_graph g\n"
						  "WHERE c.oid = i.inhparent AND "
						  "i.inhrelid = l.relid AND l.graphid = g.oid AND "
						  "l.labname = '%s' AND g.graphname = '%s'\n"
						  "ORDER BY inhseqno;",
						  labelname, graphname);

		result = PSQLexec(buf.data);
		if (!result)
			goto error_return;
		else
		{
			const char *s = _("Inherits");
			int			sw = pg_wcswidth(s, strlen(s), pset.encoding);

			tuples = PQntuples(result);

			for (i = 0; i < tuples; i++)
			{
				if (i == 0)
					printfPQExpBuffer(&buf, "%s: %s",
									  s, PQgetvalue(result, i, 0));
				else
					printfPQExpBuffer(&buf, "%*s  %s",
									  sw, "", PQgetvalue(result, i, 0));
				if (i < tuples - 1)
					appendPQExpBufferChar(&buf, ',');

				printTableAddFooter(&cont, buf.data);
			}

			PQclear(result);
		}

		/* print child labels */
		printfPQExpBuffer(&buf,
						  "SELECT c.oid::pg_catalog.regclass\n"
						  "FROM pg_catalog.pg_class c, pg_catalog.pg_inherits i, "
						  "pg_catalog.ag_label l, pg_catalog.ag_graph g\n"
						  "WHERE c.oid = i.inhrelid AND "
						  "i.inhparent = l.relid AND l.graphid = g.oid AND "
						  "l.labname = '%s' AND g.graphname = '%s'\n"
						  "ORDER BY c.oid::pg_catalog.regclass::pg_catalog.text;",
						  labelname, graphname);

		result = PSQLexec(buf.data);
		if (!result)
			goto error_return;
		else
			tuples = PQntuples(result);

		for (i = 0; i < tuples; i++)
		{
			if (i == 0)
				printfPQExpBuffer(&buf, "%s: %s",
								  ct, PQgetvalue(result, i, 0));
			else
				printfPQExpBuffer(&buf, "%*s  %s",
								  ctw, "", PQgetvalue(result, i, 0));
			if (i < tuples - 1)
				appendPQExpBufferChar(&buf, ',');

			printTableAddFooter(&cont, buf.data);
		}

		PQclear(result);

		/* Tablespace info */
		add_tablespace_footer(&cont, labelinfo.relkind, labelinfo.tablespace,
							  true);
	}

	printTable(&cont, pset.queryFout, false, pset.logfile);

	retval = true;

error_return:
	/* clean up */
	if (printTableInitialized)
		printTableCleanup(&cont);
	termPQExpBuffer(&buf);
	termPQExpBuffer(&title);

	if (seq_values)
	{
		for (ptr = seq_values; *ptr; ptr++)
			free(*ptr);
		free(seq_values);
	}

	if (modifiers)
	{
		for (ptr = modifiers; *ptr; ptr++)
			free(*ptr);
		free(modifiers);
	}

	if (view_def)
		free(view_def);

	if (res)
		PQclear(res);

	return retval;
}
