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

extern Datum vertex_out(PG_FUNCTION_ARGS);
extern Datum edge_out(PG_FUNCTION_ARGS);

#endif	/* GRAPH_H */
