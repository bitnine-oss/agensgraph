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

extern Datum vertex_out(PG_FUNCTION_ARGS);
extern Datum edge_out(PG_FUNCTION_ARGS);
extern Datum graphpath_out(PG_FUNCTION_ARGS);

extern Graphid getGraphidStruct(Datum datum);
extern Datum getGraphidDatum(Graphid id);

extern Datum getVertexIdDatum(Datum datum);
extern Datum getEdgeIdDatum(Datum datum);

extern void getGraphpathArrays(Datum graphpath, Datum *vertices, Datum *edges);
extern Datum makeGraphpathDatum(Datum *vertices, int nvertices, Datum *edges,
								int nedges);

#endif	/* GRAPH_H */
