/*
 * dijkstra.h
 *	  Declarations for dijkstra
 *
 * Copyright (c) 2017 by Bitnine Global, Inc.
 *
 * src/include/utils/dijkstra.h
 */

#ifndef SHORTESTPATH_H
#define SHORTESTPATH_H

#include "postgres.h"

#include "fmgr.h"

extern Datum shortestpath_graphids(PG_FUNCTION_ARGS);
extern Datum shortestpath_rowids(PG_FUNCTION_ARGS);

extern Datum dijkstra_vids(PG_FUNCTION_ARGS);
extern Datum dijkstra_eids(PG_FUNCTION_ARGS);

#endif	/* SHORTESTPATH_H */
