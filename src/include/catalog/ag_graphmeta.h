/*-------------------------------------------------------------------------
 *
 * ag_graphmeta.h
 *	  definition of the system "graphmeta" relation (ag_graphmeta)
 *	  along with the relation's initial contents.
 *
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * src/include/catalog/ag_graphmeta.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef AG_GRAPHMETA_H
#define AG_GRAPHMETA_H

#include "catalog/genbki.h"
#include "pgstat.h"

/* ----------------
 *		ag_graphmeta definition.  cpp turns this into
 *		typedef struct FormData_ag_graphmeta
 * ----------------
 */
#define GraphMetaRelationId	7055

CATALOG(ag_graphmeta,7055) BKI_SCHEMA_MACRO BKI_WITHOUT_OIDS
{
	Oid			graph;			/* graph oid */
	int16		edge;			/* edge label id */
	int16		start;			/* start vertex label id */
	int16		end;			/* end vertex label id */
	int64		edgecount;		/* # of edge between start and end */
} FormData_ag_graphmeta;

/* ----------------
 *		Form_ag_graphmeta corresponds to a pointer to a tuple with
 *		the format of ag_graphmeta relation.
 * ----------------
 */
typedef FormData_ag_graphmeta *Form_ag_graphmeta;

/* ----------------
 *		compiler constants for ag_graphmeta
 * ----------------
 */

#define Natts_ag_graphmeta			5
#define Anum_ag_graphmeta_graph		1
#define Anum_ag_graphmeta_edge		2
#define Anum_ag_graphmeta_start		3
#define Anum_ag_graphmeta_end		4
#define Anum_ag_graphmeta_edgecount	5

typedef struct AgStat_key
{
	Oid		graph;
	Labid	edge;
	Labid	start;
	Labid	end;
} AgStat_key;

typedef struct AgStat_GraphMeta
{
	struct AgStat_key	key;

	PgStat_Counter		edges_inserted;
	PgStat_Counter		edges_deleted;
} AgStat_GraphMeta;

#endif   /* AG_GRAPHMETA_H */
