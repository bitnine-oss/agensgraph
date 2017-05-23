/*
 * dijkstra.h
 *	  Declarations for dijkstra
 *
 * Copyright (c) 2017 by Bitnine Global, Inc.
 *
 * src/include/utils/dijkstra.h
 */

#ifndef DIJKSTRA_H
#define DIJKSTRA_H

#include "postgres.h"

#include "fmgr.h"

extern Datum dijkstra_vids(PG_FUNCTION_ARGS);
extern Datum dijkstra_eids(PG_FUNCTION_ARGS);

#endif	/* DIJKSTRA_H */
