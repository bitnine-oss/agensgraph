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
extern void DeleteAgLabelProperty(Oid laboid, const char *propname);
extern AttrNumber get_label_property_attnum(Oid laboid, const char *propname);
extern char *get_label_property_name_by_attnum(Oid laboid, AttrNumber attnum);

/*
 * One promoted typed property of a label, keyed by relid (storage table oid)
 * rather than the ag_label oid, so the Cypher compiler can enumerate/resolve
 * promoted columns starting from a range-table relation.
 */
typedef struct PromotedPropInfo
{
	char		propname[NAMEDATALEN];	/* down-cased Cypher key */
	AttrNumber	attnum;			/* typed column attnum in the label table */
	char		semantics;		/* PROMOTED_SEMANTICS_*; forward-looking, not
								 * consulted by the current read resolution */
} PromotedPropInfo;

extern bool label_relid_has_promoted_property(Oid relid);
extern List *get_label_promoted_properties(Oid relid);

/*
 * Whether a label under `relid' holds `propname' in a column.  A read of such a
 * property is worth settling per relation.
 */
extern bool subtree_has_promoted_property(Oid relid, const char *propname);

/*
 * The `semantics' out-param is forward-looking (for a future typed-semantics
 * opt-in) and is not consulted by the current read resolution; callers pass
 * NULL for it.
 */
extern bool get_label_property_column(Oid relid, const char *propname,
									  AttrNumber *attnum, char *semantics);

#endif							/* AG_LABEL_FN_H */
