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

extern Oid	label_create_with_catalog(RangeVar *label, Oid relid, char labkind,
									  Oid labtablespace, bool is_fixed_id,
									  int32 fixed_id);
extern void label_drop_with_catalog(Oid laboid);

extern void InsertAgLabelProperty(Oid laboid, const char *propname,
								  int16 attnum, char semantics);
extern void DeleteAgLabelProperties(Oid laboid);
extern AttrNumber get_label_property_attnum(Oid laboid, const char *propname);

#endif							/* AG_LABEL_FN_H */
