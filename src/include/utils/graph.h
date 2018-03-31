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
#include "storage/itemptr.h"

#define Natts_vertex			3
#define Anum_vertex_id			1
#define Anum_vertex_properties	2
#define Anum_vertex_tid			3

#define Natts_edge				5
#define Anum_edge_id			1
#define Anum_edge_start			2
#define Anum_edge_end			3
#define Anum_edge_properties	4
#define Anum_edge_tid			5

#define Natts_graphpath			2
#define Anum_graphpath_vertices	1
#define Anum_graphpath_edges	2

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

#define GRAPHID_LABID_MAX	PG_UINT16_MAX
#define GRAPHID_LOCID_MAX	((UINT64CONST(1) << (32 + 16)) - 1)

typedef uint64 EdgeRef;

#define DatumGetEdgeRef(d)		((EdgeRef) DatumGetUInt64(d))
#define EdgeRefGetDatum(p)		UInt64GetDatum(p)
#define PG_GETARG_EDGEREF(x)	DatumGetEdgeRef(PG_GETARG_DATUM(x))
#define PG_RETURN_EDGEREF(x)	return EdgeRefGetDatum(x)

#define EdgeRefGetRelid(_ref)			((uint16) ((_ref) >> 48))
#define EdgeRefGetBlockNumber(_ref)		((BlockNumber) ((_ref) >> 16))
#define EdgeRefGetOffsetNumber(_ref)	((OffsetNumber) ((_ref) & 0xffff))

#define EdgeRefSet(_ref, _relid, _ip) \
	do { \
		BlockNumber _blkno = ItemPointerGetBlockNumber(_ip); \
		OffsetNumber _offset = ItemPointerGetOffsetNumber(_ip); \
		(_ref) = (((uint64) (_relid)) << 48) | \
				 (((uint64) (_blkno)) << 16) | \
				 ((uint64) (_offset)); \
	} while (0)

typedef struct {
	Oid			tableoid;
	ItemPointerData tid;
} Rowid;

#define RowidGetDatum(X)	PointerGetDatum(X)
#define PG_GETARG_ROWID(n)	((Rowid *) DatumGetPointer(PG_GETARG_DATUM(n)))
#define PG_RETURN_ROWID(x)	return RowidGetDatum(x)

/* graphid */
extern Datum graphid(PG_FUNCTION_ARGS);
extern Datum graphid_in(PG_FUNCTION_ARGS);
extern Datum graphid_out(PG_FUNCTION_ARGS);
extern Datum graphid_recv(PG_FUNCTION_ARGS);
extern Datum graphid_send(PG_FUNCTION_ARGS);
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

/* edgeref */
extern Datum edgeref(PG_FUNCTION_ARGS);
extern Datum edgeref_in(PG_FUNCTION_ARGS);
extern Datum edgeref_out(PG_FUNCTION_ARGS);

/* rowid */
extern Datum rowid(PG_FUNCTION_ARGS);
extern Datum rowid_in(PG_FUNCTION_ARGS);
extern Datum rowid_out(PG_FUNCTION_ARGS);
extern Datum rowid_tableoid(PG_FUNCTION_ARGS);
extern Datum rowid_ctid(PG_FUNCTION_ARGS);
/* rowid -comparison */
extern Datum rowid_eq(PG_FUNCTION_ARGS);
extern Datum rowid_ne(PG_FUNCTION_ARGS);
extern Datum rowid_lt(PG_FUNCTION_ARGS);
extern Datum rowid_gt(PG_FUNCTION_ARGS);
extern Datum rowid_le(PG_FUNCTION_ARGS);
extern Datum rowid_ge(PG_FUNCTION_ARGS);
/* rowid - BTree */
extern Datum btrowidcmp(PG_FUNCTION_ARGS);

/* vertex */
extern Datum vertex_out(PG_FUNCTION_ARGS);
extern Datum _vertex_out(PG_FUNCTION_ARGS);
extern Datum vertex_label(PG_FUNCTION_ARGS);
extern Datum _vertex_length(PG_FUNCTION_ARGS);
extern Datum vtojb(PG_FUNCTION_ARGS);
/* vertex - comparison */
extern Datum vertex_eq(PG_FUNCTION_ARGS);
extern Datum vertex_ne(PG_FUNCTION_ARGS);
extern Datum vertex_lt(PG_FUNCTION_ARGS);
extern Datum vertex_gt(PG_FUNCTION_ARGS);
extern Datum vertex_le(PG_FUNCTION_ARGS);
extern Datum vertex_ge(PG_FUNCTION_ARGS);

/* edge */
extern Datum edge_out(PG_FUNCTION_ARGS);
extern Datum _edge_out(PG_FUNCTION_ARGS);
extern Datum edge_label(PG_FUNCTION_ARGS);
extern Datum _edge_length(PG_FUNCTION_ARGS);
extern Datum etojb(PG_FUNCTION_ARGS);
/* edge - comparison */
extern Datum edge_eq(PG_FUNCTION_ARGS);
extern Datum edge_ne(PG_FUNCTION_ARGS);
extern Datum edge_lt(PG_FUNCTION_ARGS);
extern Datum edge_gt(PG_FUNCTION_ARGS);
extern Datum edge_le(PG_FUNCTION_ARGS);
extern Datum edge_ge(PG_FUNCTION_ARGS);

/* graphpath */
extern Datum graphpath_out(PG_FUNCTION_ARGS);
extern Datum _graphpath_length(PG_FUNCTION_ARGS);
extern Datum graphpath_length(PG_FUNCTION_ARGS);
extern Datum graphpath_vertices(PG_FUNCTION_ARGS);
extern Datum graphpath_edges(PG_FUNCTION_ARGS);

/* additional function implementations */
extern Datum edge_start_vertex(PG_FUNCTION_ARGS);
extern Datum edge_end_vertex(PG_FUNCTION_ARGS);
extern Datum vertex_labels(PG_FUNCTION_ARGS);

/* support functions */
extern Datum getVertexIdDatum(Datum datum);
extern Datum getVertexPropDatum(Datum datum);
extern Datum getEdgeIdDatum(Datum datum);
extern Datum getEdgeStartDatum(Datum datum);
extern Datum getEdgeEndDatum(Datum datum);
extern Datum getEdgePropDatum(Datum datum);
extern void getGraphpathArrays(Datum graphpath, Datum *vertices, Datum *edges);
extern Datum makeGraphpathDatum(Datum *vertices, int nvertices, Datum *edges,
								int nedges);
extern Datum makeGraphVertexDatum(Datum id, Datum prop_map);
extern Datum makeGraphEdgeDatum(Datum id, Datum start, Datum end,
								Datum prop_map);

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
