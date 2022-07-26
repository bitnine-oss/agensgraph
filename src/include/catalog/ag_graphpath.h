/*-------------------------------------------------------------------------
 *
 * This file has referenced pg_class.h
 *	  definition of the system "label" relation (ag_graphpath)
 *	  along with the relation's initial contents.
 *
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * src/include/catalog/ag_graphpath.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef AG_GRAPHPATH_H
#define AG_GRAPHPATH_H

#include "catalog/genbki.h"
#include "catalog/ag_graphpath_d.h"

/* ----------------
 *		ag_graphpath definition.  cpp turns this into
 *		typedef struct FormData_ag_graphpath
 * ----------------
 */
CATALOG(ag_graphpath,7030,GraphPathRelationId) BKI_BOOTSTRAP BKI_ROWTYPE_OID(7032,GraphPathRelation_Rowtype_Id) BKI_SCHEMA_MACRO
{
	_vertex		vertices;		/* id */
	_edge		edges;			/* properties */
} FormData_ag_graphpath;

/* ----------------
 *		Form_ag_graphpath corresponds to a pointer to a tuple with
 *		the format of ag_graphpath relation.
 * ----------------
 */
typedef FormData_ag_graphpath *Form_ag_graphpath;

#endif   /* AG_GRAPHPATH_H */
