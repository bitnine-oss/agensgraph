/*-------------------------------------------------------------------------
 *
 * This file has referenced pg_class.h
 *	  definition of the system "label" relation (ag_label)
 *	  along with the relation's initial contents.
 *
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * src/include/catalog/ag_label.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef AG_LABEL_H
#define AG_LABEL_H

#include "catalog/genbki.h"
#include "catalog/ag_label_d.h"

/* ----------------
 *		ag_label definition.  cpp turns this into
 *		typedef struct FormData_ag_label
 * ----------------
 */
CATALOG(ag_label,7045,LabelRelationId) BKI_SCHEMA_MACRO
{
	Oid			oid;			/* oid */
	NameData	labname;		/* label name */
	Oid         graphid;		/* graph oid */
	int32       labid;			/* label ID in a graph */
	Oid         relid;			/* table oid under the label */
	char        labkind;		/* see LABEL_KIND_XXX constants below */
} FormData_ag_label;

/* ----------------
 *		Form_ag_label corresponds to a pointer to a tuple with
 *		the format of ag_label relation.
 * ----------------
 */
typedef FormData_ag_label *Form_ag_label;

#define LABEL_KIND_VERTEX	'v'
#define LABEL_KIND_EDGE		'e'

DECLARE_UNIQUE_INDEX(ag_label_oid_index, 7046, on ag_label using btree(oid oid_ops));
#define LabelOidIndexId 7046
DECLARE_UNIQUE_INDEX(ag_label_labname_graph_index, 7047, on ag_label using btree(labname name_ops, graphid oid_ops));
#define LabelNameGraphIndexId 7047
DECLARE_UNIQUE_INDEX(ag_label_graph_labid_index, 7048, on ag_label using btree(graphid oid_ops, labid int4_ops));
#define LabelGraphLabelIndexId 7048
DECLARE_UNIQUE_INDEX(ag_label_relid_index, 7049, on ag_label using btree(relid oid_ops));
#define LabelRelidIndexId 7049

#endif   /* AG_LABEL_H */
