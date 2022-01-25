/*
 * shortestpathfuncs.c
 *	  Dummy functions for shortestpath
 *
 * Copyright (c) 2017 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/shortestpathfuncs.c
 */

#include "postgres.h"

#include "fmgr.h"
#include "utils/shortestpath.h"

Datum
shortestpath_graphids(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}

Datum
shortestpath_rowids(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}

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
