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

extern Oid label_create_with_catalog(RangeVar *label, Oid relid, char labkind,
									 Oid labtablespace);
extern void label_drop_with_catalog(Oid labid);

#endif	/* AG_LABEL_FN_H */
