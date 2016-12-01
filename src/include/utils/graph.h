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

typedef uint64 Graphid;

#define DatumGetGraphid(d)		DatumGetUInt64(d)
#define GraphidGetDatum(p)		UInt64GetDatum(p)
#define PG_GETARG_GRAPHID(x)	DatumGetGraphid(PG_GETARG_DATUM(x))
#define PG_RETURN_GRAPHID(x)	return UInt64GetDatum(x)

#define GraphidGetLabid(id)		((uint16) (((uint64) (id)) >> (32 + 16)))
#define GraphidGetLocid(id)		(((uint64) (id)) & 0x0000ffffffffffff)

#define GraphidSet(_id, _labid, _locid) \
	do { \
		AssertMacro(PointerIsValid(_id)); \
		*(_id) = (((uint64) (_labid)) << (32 + 16)) | \
				 (((uint64) (_locid)) & 0x0000ffffffffffff); \
	} while (0)

/* graphid */
extern Datum graphid(PG_FUNCTION_ARGS);
extern Datum graphid_in(PG_FUNCTION_ARGS);
extern Datum graphid_out(PG_FUNCTION_ARGS);
extern Datum graphid_labid(PG_FUNCTION_ARGS);
extern Datum graphid_locid(PG_FUNCTION_ARGS);
extern Datum graph_labid(PG_FUNCTION_ARGS);
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
extern Datum getVertexIdDatum(Datum datum);
extern Datum getEdgeIdDatum(Datum datum);
extern void getGraphpathArrays(Datum graphpath, Datum *vertices, Datum *edges);
extern Datum makeGraphpathDatum(Datum *vertices, int nvertices, Datum *edges,
								int nedges);

/* index support - BTree */
extern Datum btgraphidcmp(PG_FUNCTION_ARGS);
/* index support - Hash */
extern Datum graphid_hash(PG_FUNCTION_ARGS);
/* index support - GIN (as BTree) */
extern Datum gin_extract_value_graphid(PG_FUNCTION_ARGS);
extern Datum gin_extract_query_graphid(PG_FUNCTION_ARGS);
extern Datum gin_consistent_graphid(PG_FUNCTION_ARGS);
extern Datum gin_compare_partial_graphid(PG_FUNCTION_ARGS);

#endif	/* GRAPH_H */
