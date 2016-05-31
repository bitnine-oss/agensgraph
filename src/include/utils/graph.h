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

typedef struct VertexId
{
	Oid			oid;		/* node label (relation) */
	int32		pad_;		/* for 8-byte alignment */
	int64		vid;		/* int8 (or BIGINT) */
} VertexId;

typedef struct Vertex
{
	int32		vl_len_;	/* varlena header (do not touch directly!) */

	int32		pad_;		/* for 8-byte alignment */

	VertexId	id;
	Jsonb		prop_map;	/* jsonb; type of root should be object */
} Vertex;

extern Datum vertex_in(PG_FUNCTION_ARGS);
extern Datum vertex_out(PG_FUNCTION_ARGS);
extern Datum vertex_constructor(PG_FUNCTION_ARGS);
extern Datum vertex_prop(PG_FUNCTION_ARGS);

#define PG_GETARG_EDGE(n)	((GraphEdge *) PG_GETARG_VARLENA_P(n))

typedef struct GraphEdge
{
	int32		vl_len_;	/* varlena header (do not touch directly!) */

	Oid			oid;		/* relationship type (relation) */
	int64		eid;		/* int8 (or BIGINT) */
	VertexId	vin;
	VertexId	vout;
	Jsonb		prop_map;	/* jsonb; type of root should be object */
} GraphEdge;

extern Datum edge_in(PG_FUNCTION_ARGS);
extern Datum edge_out(PG_FUNCTION_ARGS);
extern Datum edge_constructor(PG_FUNCTION_ARGS);
extern Datum edge_prop(PG_FUNCTION_ARGS);

#endif	/* GRAPH_H */
