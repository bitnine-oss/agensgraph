/*
 * graph.h
 *	  Declarations for vertex and edge data type.
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * src/include/utils/graph.h
 */

#ifndef GRAPH_H
#define GRAPH_H

#include "postgres.h"

#include "fmgr.h"
#include "utils/jsonb.h"

#define PG_GETARG_VERTEX(n)	((Vertex *) PG_GETARG_VARLENA_P(n))

typedef struct Vertex
{
	int32		vl_len_;	/* varlena header (do not touch directly!) */

	int32		pad_;		/* for 8-byte alignment */

	NameData	label;		/* name */
	int64		vid;		/* int8 (or BIGINT) */
	Jsonb		prop_map;	/* jsonb; type of root should be object */
} Vertex;

extern Datum vertex_in(PG_FUNCTION_ARGS);
extern Datum vertex_out(PG_FUNCTION_ARGS);
extern Datum vertex_constructor(PG_FUNCTION_ARGS);
extern Datum vertex_prop(PG_FUNCTION_ARGS);

#endif	/* GRAPH_H */
