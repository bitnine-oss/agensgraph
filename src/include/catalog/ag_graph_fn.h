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
extern bool enable_graph_dml;
extern bool cypher_allow_unsafe_ddl;

extern char *get_graph_path(bool lookup_cache);
extern char *get_graph_path_or_null(void);
extern Oid	get_graph_path_oid(void);
extern bool graphmeta_baseline_valid(Oid graph);
extern void graphmeta_set_valid(Oid graph, bool valid);

extern Oid	GraphCreate(CreateGraphStmt *stmt, const char *queryString,
						int stmt_location, int stmt_len);

#endif							/* AG_GRAPH_FN_H */
