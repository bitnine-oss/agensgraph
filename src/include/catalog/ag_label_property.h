/*-------------------------------------------------------------------------
 *
 * ag_label_property.h
 *	  definition of the system "label property" relation (ag_label_property)
 *
 *	  Each row records a property of a graph label that is promoted to a real
 *	  typed column beside the jsonb property bag.  It maps the (down-cased)
 *	  Cypher property key to the storage column that materializes it, so the
 *	  Cypher compiler can resolve n.key to that column, and DDL/dump code can
 *	  enumerate a label's promoted properties.
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * src/include/catalog/ag_label_property.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef AG_LABEL_PROPERTY_H
#define AG_LABEL_PROPERTY_H

#include "catalog/genbki.h"
#include "catalog/ag_label_property_d.h"

/* ----------------
 *		ag_label_property definition.  cpp turns this into
 *		typedef struct FormData_ag_label_property
 * ----------------
 */
CATALOG(ag_label_property,7050,LabelPropertyRelationId) BKI_SCHEMA_MACRO
{
	Oid			laboid;			/* OID of the owning label (ag_label.oid) */
	NameData	propname;		/* the (down-cased) Cypher property key */
	int16		attnum;			/* attnum of the typed column in the label table */
	char		semantics;		/* comparison semantics; see PROMOTED_SEMANTICS_* */
} FormData_ag_label_property;

/* ----------------
 *		Form_ag_label_property corresponds to a pointer to a tuple with
 *		the format of ag_label_property relation.
 * ----------------
 */
typedef FormData_ag_label_property *Form_ag_label_property;

/*
 * semantics: whether comparing/ordering n.key preserves the legacy jsonb
 * behavior byte-for-byte, or uses the column's native typed semantics.
 */
#define PROMOTED_SEMANTICS_LEGACY	'l'
#define PROMOTED_SEMANTICS_TYPED	't'

#define LabelPropertyLabidPropIndexId 7051
DECLARE_UNIQUE_INDEX_PKEY(ag_label_property_laboid_propname_index, 7051, LabelPropertyLabidPropIndexId, ag_label_property, btree(laboid oid_ops, propname name_ops));

MAKE_SYSCACHE(LABELPROPNAME, ag_label_property_laboid_propname_index, 128);

#endif							/* AG_LABEL_PROPERTY_H */
