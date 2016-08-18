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

typedef struct Graphid
{
	Oid			oid;
	int64		lid;
} Graphid;

/* graphid */
extern Datum graphid_in(PG_FUNCTION_ARGS);
extern Datum graphid_out(PG_FUNCTION_ARGS);

/* vertex */
extern Datum vertex_out(PG_FUNCTION_ARGS);
extern Datum _vertex_out(PG_FUNCTION_ARGS);
extern Datum vertex_label(PG_FUNCTION_ARGS);
extern Datum vtojb(PG_FUNCTION_ARGS);

/* edge */
extern Datum edge_out(PG_FUNCTION_ARGS);
extern Datum _edge_out(PG_FUNCTION_ARGS);
extern Datum edge_label(PG_FUNCTION_ARGS);
extern Datum etojb(PG_FUNCTION_ARGS);

/* graphpath */
extern Datum graphpath_out(PG_FUNCTION_ARGS);
extern Datum graphpath_length(PG_FUNCTION_ARGS);
extern Datum graphpath_vertices(PG_FUNCTION_ARGS);
extern Datum graphpath_edges(PG_FUNCTION_ARGS);

/* additional function implementations */
extern Datum edge_start_vertex(PG_FUNCTION_ARGS);
extern Datum edge_end_vertex(PG_FUNCTION_ARGS);
extern Datum vertex_labels(PG_FUNCTION_ARGS);

/* support functions */
extern Graphid getGraphidStruct(Datum datum);
extern Datum getGraphidDatum(Graphid id);
extern Datum getVertexIdDatum(Datum datum);
extern Datum getEdgeIdDatum(Datum datum);
extern void getGraphpathArrays(Datum graphpath, Datum *vertices, Datum *edges);
extern Datum makeGraphpathDatum(Datum *vertices, int nvertices, Datum *edges,
								int nedges);

#endif	/* GRAPH_H */
