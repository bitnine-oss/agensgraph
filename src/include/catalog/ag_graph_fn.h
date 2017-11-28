/*
 * ag_graph_fn.h
 *	  prototypes for functions in backend/catalog/ag_graph.c
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/include/catalog/ag_graph_fn.h
 */
#ifndef AG_GRAPH_FN_H
#define AG_GRAPH_FN_H

#include "nodes/parsenodes.h"

extern char *graph_path;

extern char *get_graph_path(bool lookup_cache);
extern Oid get_graph_path_oid(void);

extern Oid GraphCreate(CreateGraphStmt *stmt, const char *queryString,
					   int stmt_location, int stmt_len);

#endif	/* AG_GRAPH_FN_H */
