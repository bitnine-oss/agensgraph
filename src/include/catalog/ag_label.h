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
	NameData	labname;        /* label name */
	Oid         graphid;        /* graph oid */
	int32       labid;            /* label ID in a graph */
	Oid         relid;            /* table oid under the label */
	char        labkind;        /* see LABEL_KIND_XXX constants below */
} FormData_ag_label;

/* ----------------
 *		Form_ag_label corresponds to a pointer to a tuple with
 *		the format of ag_label relation.
 * ----------------
 */
typedef FormData_ag_label *Form_ag_label;

#define LABEL_KIND_VERTEX	'v'
#define LABEL_KIND_EDGE		'e'

#endif   /* AG_LABEL_H */
