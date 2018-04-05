/*-------------------------------------------------------------------------
 *
 * ag_labmeta.h
 *	  definition of the system "labmeta" relation (ag_labmeta)
 *	  along with the relation's initial contents.
 *
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * src/include/catalog/ag_labmeta.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef AG_LABMETA_H
#define AG_LABMETA_H

#include "catalog/genbki.h"

/* ----------------
 *		ag_labmeta definition.  cpp turns this into
 *		typedef struct FormData_ag_labmeta
 * ----------------
 */
#define LabMetaRelationId	7055

CATALOG(ag_labmeta,7055) BKI_SCHEMA_MACRO BKI_WITHOUT_OIDS
{
	Oid			graph;			/* graph oid */
	int16		edge;			/* edge label id */
	int16		start;			/* start vertex label id */
	int16		end;			/* end vertex label id */
	int64		edgecount;		/* # of edge between start and end */
} FormData_ag_labmeta;

/* ----------------
 *		Form_ag_labmeta corresponds to a pointer to a tuple with
 *		the format of ag_labmeta relation.
 * ----------------
 */
typedef FormData_ag_labmeta *Form_ag_labmeta;

/* ----------------
 *		compiler constants for ag_labmeta
 * ----------------
 */

#define Natts_ag_labmeta			5
#define Anum_ag_labmeta_graph		1
#define Anum_ag_labmeta_edge		2
#define Anum_ag_labmeta_start		3
#define Anum_ag_labmeta_end			4
#define Anum_ag_labmeta_edgecount	5

#endif   /* AG_LABMETA_H */
