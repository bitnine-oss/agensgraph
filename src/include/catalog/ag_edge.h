/*-------------------------------------------------------------------------
 *
 * This file has referenced pg_class.h
 *	  definition of the system "label" relation (ag_edge)
 *	  along with the relation's initial contents.
 *
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * src/include/catalog/ag_edge.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef AG_EDGE_H
#define AG_EDGE_H

#include "catalog/genbki.h"
#include "catalog/ag_edge_d.h"

/* ----------------
 *		ag_edge definition.  cpp turns this into
 *		typedef struct FormData_ag_edge
 * ----------------
 */
CATALOG(ag_edge,7020,EdgeRelationId) BKI_BOOTSTRAP BKI_ROWTYPE_OID(7022,EdgeRelation_Rowtype_Id) BKI_SCHEMA_MACRO
{
	graphid		id;				/* id */
	graphid		start;			/* start */
	graphid		end;			/* end */
	jsonb		properties;		/* properties */
	tid			tid;			/* tid */
} FormData_ag_edge;

/* ----------------
 *		Form_ag_edge corresponds to a pointer to a tuple with
 *		the format of ag_edge relation.
 * ----------------
 */
typedef FormData_ag_edge *Form_ag_edge;

#endif							/* AG_EDGE_H */
