/*
 * dijkstrafuncs.c
 *	  Dummy functions for dijkstra
 *
 * Copyright (c) 2017 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/dijkstrafuncs.c
 */

#include "postgres.h"

#include "fmgr.h"
#include "utils/dijkstra.h"

Datum
dijkstra_vids(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}

Datum
dijkstra_eids(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}
