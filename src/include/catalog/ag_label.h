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
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef AG_LABEL_H
#define AG_LABEL_H

#include "catalog/genbki.h"

/* ----------------
 *		ag_label definition.  cpp turns this into
 *		typedef struct FormData_ag_label
 * ----------------
 */
#define LabelRelationId	7045

CATALOG(ag_label,7045) BKI_SCHEMA_MACRO
{
	NameData	labname;		/* label name */
	NameData	graphname;		/* graph name */
	Oid			relid;			/* table oid under the label */
	Oid			labowner;		/* label owner oid */
	char		labkind;		/* see LABEL_KIND_XXX constants below */
} FormData_ag_label;

/* ----------------
 *		Form_ag_label corresponds to a pointer to a tuple with
 *		the format of ag_label relation.
 * ----------------
 */
typedef FormData_ag_label *Form_ag_label;

/* ----------------
 *		compiler constants for ag_label
 * ----------------
 */

#define Natts_ag_label			5
#define Anum_ag_label_labname	1
#define Anum_ag_label_graphname	2
#define Anum_ag_label_relid		3
#define Anum_ag_label_labowner	4
#define Anum_ag_label_labkind	5

#define LABEL_KIND_VERTEX	'v'
#define LABEL_KIND_EDGE		'e'

#endif   /* AG_LABEL_H */
