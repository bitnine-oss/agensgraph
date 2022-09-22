/*-------------------------------------------------------------------------
 *
 * ag_graph.h
 *	  definition of the system "graph" relation (ag_graph)
 *	  along with the relation's initial contents.
 *
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * src/include/catalog/ag_graph.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef AG_GRAPH_H
#define AG_GRAPH_H

#include "catalog/genbki.h"
#include "catalog/ag_graph_d.h"

CATALOG(ag_graph,7040,GraphRelationId) BKI_SCHEMA_MACRO
{
	Oid			oid;
	NameData	graphname;
	Oid			nspid;
} FormData_ag_graph;

/* ----------------
 *		Form_ag_graph corresponds to a pointer to a tuple with
 *		the format of ag_graph relation.
 * ----------------
 */
typedef FormData_ag_graph *Form_ag_graph;

DECLARE_UNIQUE_INDEX_PKEY(ag_graph_oid_index, 7041, on ag_graph using btree(oid oid_ops));
#define GraphOidIndexId 7041
DECLARE_UNIQUE_INDEX(ag_graph_graphname_index, 7042, on ag_graph using btree(graphname name_ops));
#define GraphNameIndexId 7042

#endif   /* AG_GRAPH_H */
