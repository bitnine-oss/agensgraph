/*-------------------------------------------------------------------------
 *
 * ag_inherits.h
 *	  definition of the system "inherits" relation (ag_inherits)
 *	  along with the relation's initial contents.
 *
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * src/include/catalog/ag_inherits.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef AG_INHERITS_H
#define AG_INHERITS_H

#include "catalog/genbki.h"

/* ----------------
 *		ag_inherits definition.  cpp turns this into
 *		typedef struct FormData_ag_inherits
 * ----------------
 */
#define InheritsLabelId 3300

CATALOG(ag_inherits,3300) BKI_WITHOUT_OIDS
{
	Oid			inhrelid;
	Oid			inhparent;
	int32		inhseqno;
} FormData_ag_inherits;

/* ----------------
 *		Form_ag_inherits corresponds to a pointer to a tuple with
 *		the format of ag_inherits relation.
 * ----------------
 */
typedef FormData_ag_inherits *Form_ag_inherits;

/* ----------------
 *		compiler constants for ag_inherits
 * ----------------
 */

#define Natts_ag_inherits				3
#define Anum_ag_inherits_inhrelid		1
#define Anum_ag_inherits_inhparent		2
#define Anum_ag_inherits_inhseqno		3

/* ----------------
 *		ag_inherits has no initial contents
 * ----------------
 */

#endif   /* AG_INHERITS_H */
