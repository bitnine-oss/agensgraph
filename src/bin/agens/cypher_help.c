/*
 * agens - the AgensGraph interactive terminal
 *
 * Copyright (c) 2018 by Bitnine Global, Inc.
 *
 * src/bin/psql/cypher_help.c
 */

#define N_(x) (x)				/* gettext noop */

#include "postgres_fe.h"
#include "cypher_help.h"

static void
cypher_help_ALTER_ELABEL(PQExpBuffer buf)
{
	appendPQExpBuffer(buf,
					  "ALTER ELABEL [ IF EXISTS ] %s [ * ]\n"
					  "    %s [, ... ]\n"
					  "\n"
					  "%s\n"
					  "\n"
					  "    CLUSTER ON %s\n"
					  "    SET WITHOUT CLUSTER\n"
					  "    SET TABLESPACE %s\n"
					  "    SET { LOGGED | UNLOGGED }\n"
					  "    SET STORAGE { PLAIN | EXTERNAL | EXTENDED | MAIN }\n"
					  "    INHERIT %s\n"
					  "    NO INHERIT %s\n"
					  "    OWNER TO { %s | CURRENT_USER | SESSION_USER }\n"
					  "    REPLICA IDENTITY { DEFAULT | USING INDEX %s | FULL | NOTHING }\n"
					  "    DISABLE INDEX",
					  _("name"),
					  _("action"),
					  _("where action is one of:"),
					  _("index_name"),
					  _("new_tablespace"),
					  _("parent_edge_label"),
					  _("parent_edge_label"),
					  _("new_owner"),
					  _("index_name"));
}

static void
cypher_help_ALTER_GRAPH(PQExpBuffer buf)
{
	appendPQExpBuffer(buf,
					  "ALTER GRAPH %s RENAME TO %s\n"
					  "ALTER GRAPH %s OWNER TO %s\n"
					  "\n"
					  "%s\n"
					  "\n"
					  "    %s\n"
					  "    | CURRENT_USER\n"
					  "    | SESSION_USER",
					  _("graph_name"),
					  _("new_name"),
					  _("graph_name"),
					  _("new_name"),
					  _("where role_specification can be:"),
					  _("user_name"));
}

static void
cypher_help_ALTER_VLABEL(PQExpBuffer buf)
{
	appendPQExpBuffer(buf,
					  "ALTER VLABEL [ IF EXISTS ] %s [ * ]\n"
					  "    %s [, ... ]\n"
					  "\n"
					  "%s\n"
					  "\n"
					  "    CLUSTER ON %s\n"
					  "    SET WITHOUT CLUSTER\n"
					  "    SET TABLESPACE %s\n"
					  "    SET { LOGGED | UNLOGGED }\n"
					  "    SET STORAGE { PLAIN | EXTERNAL | EXTENDED | MAIN }\n"
					  "    INHERIT %s\n"
					  "    NO INHERIT %s\n"
					  "    OWNER TO { %s | CURRENT_USER | SESSION_USER }\n"
					  "    REPLICA IDENTITY { DEFAULT | USING INDEX %s | FULL | NOTHING }\n"
					  "    DISABLE INDEX",
					  _("name"),
					  _("action"),
					  _("where action is one of:"),
					  _("index_name"),
					  _("new_tablespace"),
					  _("parent_vertex_label"),
					  _("parent_vertex_label"),
					  _("new_owner"),
					  _("index_name"));
}

static void
cypher_help_CREATE(PQExpBuffer buf)
{
	appendPQExpBuffer(buf,
					  "CREATE %s [, ...]",
					  _("cypher_pattern"));
}

static void
cypher_help_CREATE_ELABEL(PQExpBuffer buf)
{
	appendPQExpBuffer(buf,
					  "CREATE [ UNLOGGED ] ELABEL [ IF NOT EXISTS ] %s\n"
					  "    [ DISABLE INDEX ]\n"
					  "    [ INHERITS ( %s [, ...] ) ]\n"
					  "    [ WITH ( %s [= %s] [, ...] ) ]\n"
					  "    [ TABLESPACE %s ]",
					  _("name"),
					  _("parent_name"),
					  _("storage_parameter"),
					  _("value"),
					  _("tablespace_name"));
}

static void
cypher_help_CREATE_GRAPH(PQExpBuffer buf)
{
	appendPQExpBuffer(buf,
					  "CREATE GRAPH [ IF NOT EXISTS ] %s [ AUTHORIZATION %s ] \n"
					  "\n"
					  "%s\n"
					  "\n"
					  "    %s\n"
					  "    | CURRENT_USER\n"
					  "    | SESSION_USER",
					  _("graph_name"),
					  _("role_specification"),
					  _("where role_specification can be:"),
					  _("user_name"));
}

static void
cypher_help_CREATE_PROPERTY_INDEX(PQExpBuffer buf)
{
	appendPQExpBuffer(buf,
					  "CREATE [ UNIQUE ] PROPERTY INDEX [ CONCURRENTLY ] [ [ IF NOT EXISTS ] %s ] ON %s [ USING %s ]\n"
					  "    ( { %s | ( %s ) } [ COLLATE %s ] [ %s ] [ ASC | DESC ] [ NULLS { FIRST | LAST } ] [, ...] )\n"
					  "    [ WITH ( %s = %s [, ... ] ) ]\n"
					  "    [ TABLESPACE %s ]\n"
					  "    [ WHERE %s ]",
					  _("name"),
					  _("label_name"),
					  _("method"),
					  _("property_name"),
					  _("cypher_expression"),
					  _("collation"),
					  _("opclass"),
					  _("storage_parameter"),
					  _("value"),
					  _("tablespace_name"),
					  _("predicate"));
}

static void
cypher_help_CREATE_VLABEL(PQExpBuffer buf)
{
	appendPQExpBuffer(buf,
					  "CREATE [ UNLOGGED ] VLABEL [ IF NOT EXISTS ] %s\n"
					  "    [ DISABLE INDEX ]\n"
					  "    [ INHERITS ( %s [, ...] ) ]\n"
					  "    [ WITH ( %s [= %s] [, ...] ) ]\n"
					  "    [ TABLESPACE %s ]",
					  _("name"),
					  _("parent_name"),
					  _("storage_parameter"),
					  _("value"),
					  _("tablespace_name"));
}

static void
cypher_help_DELETE(PQExpBuffer buf)
{
	appendPQExpBuffer(buf,
					  "[ DETACH ] DELETE %s [, ...]\n"
					  "\n"
					  "* If 'detach' is applied, remove all edges associated with cypher_expression.",
					  _("cypher_expression"));
}

static void
cypher_help_DROP_ELABEL(PQExpBuffer buf)
{
	appendPQExpBuffer(buf,
					  "DROP ELABEL [ IF EXISTS ] %s [, ...] [ CASCADE | RESTRICT ]",
					  _("name"));
}

static void
cypher_help_DROP_GRAPH(PQExpBuffer buf)
{
	appendPQExpBuffer(buf,
					  "DROP GRAPH [ IF EXISTS ] %s [, ...] [ CASCADE | RESTRICT ]",
					  _("graph_name"));
}

static void
cypher_help_DROP_PROPERTY_INDEX(PQExpBuffer buf)
{
	appendPQExpBuffer(buf,
					  "DROP PROPERTY INDEX [ CONCURRENTLY ] [ IF EXISTS ] %s [, ...] [ CASCADE | RESTRICT ]",
					  _("name"));
}

static void
cypher_help_DROP_VLABEL(PQExpBuffer buf)
{
	appendPQExpBuffer(buf,
					  "DROP VLABEL [ IF EXISTS ] %s [, ...] [ CASCADE | RESTRICT ]",
					  _("name"));
}

static void
cypher_help_LOAD(PQExpBuffer buf)
{
	appendPQExpBuffer(buf,
					  "LOAD FROM %s AS %s",
					  _("table_name"),
					  _("variable_name"));
}

static void
cypher_help_MATCH(PQExpBuffer buf)
{
	appendPQExpBuffer(buf,
					  "[ OPTIONAL ] MATCH %s [, ...] [ WHERE %s ]",
					  _("cypher_pattern"),
					  _("condition"));
}

static void
cypher_help_MERGE(PQExpBuffer buf)
{
	appendPQExpBuffer(buf,
					  "MERGE %s \n"
					  "    [ ON CREATE SET %s ] [, ...]\n"
					  "    [ ON MATCH SET %s ] [, ...] )\n"
					  "\n"
					  "* If the MERGE clause behaves like 'MATCH', perform cypher_expression after 'ON MATCH SET'.\n"
					  "* Otherwise, if it behaves like 'CREATE', perform cypher_expression after 'ON CREATE SET'.",
					  _("cypher_pattern"),
					  _("cypher_expression"),
					  _("cypher_expression"));
}

static void
cypher_help_REMOVE(PQExpBuffer buf)
{
	appendPQExpBuffer(buf,
					  "REMOVE %s [, ...]",
					  _("cypher_expression"));
}

static void
cypher_help_RETURN(PQExpBuffer buf)
{
	appendPQExpBuffer(buf,
					  "RETURN [ DISTINCT [ ON ( %s [, ...] ) ] ] [ * | %s [ [ AS ] %s ] [, ...]\n"
					  "    [ ORDER BY %s [ ASC | DESC | USING %s ] [ NULLS { FIRST | LAST } ] [, ...] ]\n"
					  "    [ SKIP %s ]\n"
					  "    [ LIMIT { %s | ALL } ]\n"
					  "    [ UNION %s ]",
					  _("cypher_expression"),
					  _("cypher_expression"),
					  _("output_name"),
					  _("cypher_expression"),
					  _("operator"),
					  _("count"),
					  _("count"),
					  _("cypher query ending with a RETURN clause"));
}

static void
cypher_help_SET(PQExpBuffer buf)
{
	appendPQExpBuffer(buf,
					  "SET %s [, ...]",
					  _("cypher_expression"));
}

static void
cypher_help_WITH(PQExpBuffer buf)
{
	appendPQExpBuffer(buf,
					  "WITH [ DISTINCT [ ON ( %s [, ...] ) ] ] [ * | %s [ [ AS ] %s ] [, ...]\n"
					  "    [ ORDER BY %s [ ASC | DESC | USING %s ] [ NULLS { FIRST | LAST } ] [, ...] ]\n"
					  "    [ SKIP %s ]\n"
					  "    [ LIMIT { %s | ALL } ]\n"
					  "    [ WHERE %s ]",
					  _("cypher_expression"),
					  _("cypher_expression"),
					  _("output_name"),
					  _("cypher_expression"),
					  _("operator"),
					  _("count"),
					  _("count"),
					  _("condition"));
}


const struct _helpStruct CYPHER_HELP[] = {
	{ "ALTER ELABEL",
	  N_("change the definition of a edge label"),
	  cypher_help_ALTER_ELABEL,
	  14 },

	{ "ALTER GRAPH",
	  N_("change the definition of a graph"),
	  cypher_help_ALTER_GRAPH,
	  7 },

	{ "ALTER VLABEL",
	  N_("change the definition of a vertex label"),
	  cypher_help_ALTER_VLABEL,
	  14 },

	{ "CREATE",
	  N_("create vertices and edges"),
	  cypher_help_CREATE,
	  0 },

	{ "CREATE ELABEL",
	  N_("define a new edge label"),
	  cypher_help_CREATE_ELABEL,
	  4 },

	{ "CREATE GRAPH",
	  N_("define a new graph"),
	  cypher_help_CREATE_GRAPH,
	  6 },

	{ "CREATE PROPERTY INDEX",
	  N_("define a new property index"),
	  cypher_help_CREATE_PROPERTY_INDEX,
	  4 },

	{ "CREATE VLABEL",
	  N_("define a new vertex label"),
	  cypher_help_CREATE_VLABEL,
	  4 },

	{ "DELETE",
	  N_("removes vertices or edges"),
	  cypher_help_DELETE,
	  2 },

	{ "DROP ELABEL",
	  N_("drops a edge label"),
	  cypher_help_DROP_ELABEL,
	  0 },

	{ "DROP GRAPH",
	  N_("remove a graph"),
	  cypher_help_DROP_GRAPH,
	  0 },

	{ "DROP PROPERTY INDEX",
	  N_("remove a property index"),
	  cypher_help_DROP_PROPERTY_INDEX,
	  0 },

	{ "DROP VLABEL",
	  N_("drops a vertex label"),
	  cypher_help_DROP_VLABEL,
	  0 },

	{ "LOAD",
	  N_("import data from (foreign) table"),
	  cypher_help_LOAD,
	  0 },

	{ "MATCH",
	  N_("search the graph patterns in the graph"),
	  cypher_help_MATCH,
	  0 },

	{ "MERGE",
	  N_("search the graph patterns in the graph and if not exist, create the specified graph pattern"),
	  cypher_help_MERGE,
	  5 },

	{ "REMOVE",
	  N_("removes properties of vertices or edges"),
	  cypher_help_REMOVE,
	  0 },

	{ "RETURN",
	  N_("returns data that matches the graph pattern you are looking for"),
	  cypher_help_RETURN,
	  4 },

	{ "SET",
	  N_("adds, sets, or removes properties of vertices or edges"),
	  cypher_help_SET,
	  0 },

	{ "WITH",
	  N_("passes the result of the preceding WITH clause query to the following WITH clause query"),
	  cypher_help_WITH,
	  4 },


	{ NULL, NULL, NULL }    /* End of list marker */
};
