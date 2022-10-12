/*-------------------------------------------------------------------------
 *
 * This file has referenced pg_class.h
 *	  definition of the system "label" relation (ag_vertex)
 *	  along with the relation's initial contents.
 *
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * src/include/catalog/ag_vertex.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef AG_VERTEX_H
#define AG_VERTEX_H

#include "catalog/genbki.h"
#include "catalog/ag_vertex_d.h"

/* ----------------
 *		ag_vertex definition.  cpp turns this into
 *		typedef struct FormData_ag_vertex
 * ----------------
 */
CATALOG(ag_vertex,7010,VertexRelationId) BKI_BOOTSTRAP BKI_ROWTYPE_OID(7012,VertexRelation_Rowtype_Id) BKI_SCHEMA_MACRO
{
	graphid		id;				/* id */
	jsonb		properties;		/* properties */
	tid			tid;			/* tid */
} FormData_ag_vertex;

/* ----------------
 *		Form_ag_vertex corresponds to a pointer to a tuple with
 *		the format of ag_vertex relation.
 * ----------------
 */
typedef FormData_ag_vertex *Form_ag_vertex;

#endif							/* AG_VERTEX_H */
