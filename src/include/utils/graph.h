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
/* graphid - comparison */
extern Datum graphid_eq(PG_FUNCTION_ARGS);
extern Datum graphid_ne(PG_FUNCTION_ARGS);
extern Datum graphid_lt(PG_FUNCTION_ARGS);
extern Datum graphid_gt(PG_FUNCTION_ARGS);
extern Datum graphid_le(PG_FUNCTION_ARGS);
extern Datum graphid_ge(PG_FUNCTION_ARGS);

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

/* index support - BTree */
extern Datum btgraphidcmp(PG_FUNCTION_ARGS);
/* index support - GIN (as BTree) */
extern Datum gin_extract_value_graphid(PG_FUNCTION_ARGS);
extern Datum gin_extract_query_graphid(PG_FUNCTION_ARGS);
extern Datum gin_consistent_graphid(PG_FUNCTION_ARGS);
extern Datum gin_compare_partial_graphid(PG_FUNCTION_ARGS);

#endif	/* GRAPH_H */
