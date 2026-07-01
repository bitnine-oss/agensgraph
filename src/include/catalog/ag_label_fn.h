/*
 * ag_label_fn.h
 *	  prototypes for functions in backend/catalog/ag_label.c
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/include/catalog/ag_label_fn.h
 */
#ifndef AG_LABEL_FN_H
#define AG_LABEL_FN_H

#include "nodes/parsenodes.h"
#include "utils/graph.h"
#include "utils/snapshot.h"

extern Oid	label_create_with_catalog(RangeVar *label, Oid relid, char labkind,
									  Oid labtablespace, bool is_fixed_id,
									  int32 fixed_id);
extern void label_drop_with_catalog(Oid laboid);
extern List *get_connected_edge_labels_for_vertex(Snapshot snapshot, Oid graph_oid,
												  Labid vertex_labid);
extern void get_vertex_labels_for_edge(Oid graph_oid, Labid edge_labid,
									   List **start_relids, List **end_relids);

#endif							/* AG_LABEL_FN_H */
