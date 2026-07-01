/*
 * ag_label.c
 *	  code to create and destroy Agens Graph labels
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/catalog/ag_label.c
 */

#include "postgres.h"

#include "ag_const.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/ag_graph_fn.h"
#include "catalog/ag_label_fn.h"
#include "catalog/ag_graphmeta.h"
#include "catalog/ag_label.h"
#include "catalog/ag_label_fn.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "commands/sequence.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/graph.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "miscadmin.h"
#include "utils/fmgroids.h"

static void InsertAgLabelTuple(Relation ag_label_desc, Oid laboid,
							   RangeVar *label, Oid relid, char labkind,
							   bool is_fixed_id, int32 fixed_id);
static uint16 GetNewLabelId(char *graphname, Oid graphid);

/* Potentially set by pg_upgrade_support functions */
Oid			binary_upgrade_next_ag_label_oid = InvalidOid;

Oid
label_create_with_catalog(RangeVar *label, Oid relid, char labkind,
						  Oid labtablespace, bool is_fixed_id, int32 fixed_id)
{
	Relation	ag_label_desc;
	Oid			laboid;

	ag_label_desc = table_open(LabelRelationId, RowExclusiveLock);

	if (IsBinaryUpgrade)
	{
		if (!OidIsValid(binary_upgrade_next_ag_label_oid))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("ag_label OID value not set when in binary upgrade mode")));
		laboid = binary_upgrade_next_ag_label_oid;
		binary_upgrade_next_ag_label_oid = InvalidOid;
	}
	else
	{
		laboid = GetNewRelFileNumber(labtablespace, ag_label_desc,
									 label->relpersistence);
	}

	InsertAgLabelTuple(ag_label_desc, laboid, label, relid, labkind,
					   is_fixed_id, fixed_id);

	table_close(ag_label_desc, RowExclusiveLock);

	return laboid;
}

/*
 * Remove ag_label row for the given laboid
 *
 * See DeleteRelationTuple()
 */
void
label_drop_with_catalog(Oid laboid)
{
	Relation	ag_label_desc;
	HeapTuple	tup;

	ag_label_desc = table_open(LabelRelationId, RowExclusiveLock);

	tup = SearchSysCache1(LABELOID, ObjectIdGetDatum(laboid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for label %u", laboid);

	simple_heap_delete(ag_label_desc, &tup->t_self);

	ReleaseSysCache(tup);

	table_close(ag_label_desc, RowExclusiveLock);
}

/*
 * InsertAgLabelTuple - register the new label in ag_label
 *
 * See InsertPgClassTuple()
 */
static void
InsertAgLabelTuple(Relation ag_label_desc, Oid laboid, RangeVar *label,
				   Oid relid, char labkind, bool is_fixed_id, int32 fixed_id)
{
	Oid			graphid = get_graphname_oid(label->schemaname);
	char		labname[NAMEDATALEN] = {'\0'};
	int32		labid;
	Datum		values[Natts_ag_label];
	bool		nulls[Natts_ag_label];
	HeapTuple	tup;

	Assert(labkind == LABEL_KIND_VERTEX || labkind == LABEL_KIND_EDGE);

	if (is_fixed_id)
	{
		labid = fixed_id;
	}
	else
		labid = (int32) GetNewLabelId(label->schemaname, graphid);
	strcpy(labname, label->relname);

	values[Anum_ag_label_oid - 1] = ObjectIdGetDatum(laboid);
	values[Anum_ag_label_labname - 1] = CStringGetDatum(labname);
	values[Anum_ag_label_graphid - 1] = ObjectIdGetDatum(graphid);
	values[Anum_ag_label_labid - 1] = Int32GetDatum(labid);
	values[Anum_ag_label_relid - 1] = ObjectIdGetDatum(relid);
	values[Anum_ag_label_labkind - 1] = CharGetDatum(labkind);

	memset(nulls, false, sizeof(nulls));

	tup = heap_form_tuple(RelationGetDescr(ag_label_desc), values, nulls);

	CatalogTupleInsert(ag_label_desc, tup);

	heap_freetuple(tup);
}

static uint16
GetNewLabelId(char *graphname, Oid graphid)
{
	char		sname[128];
	Datum		stext;
	uint16		labid;
	int			cnt;

	snprintf(sname, 128, "\"%s\".\"%s\"", graphname, AG_LABEL_SEQ);
	stext = CStringGetTextDatum(sname);

	cnt = 0;
	for (;;)
	{
		Datum		val;

		val = DirectFunctionCall1(nextval, stext);
		labid = DatumGetUInt16(val);
		if (!labid_exists(graphid, labid))
			break;

		if (++cnt >= GRAPHID_LABID_MAX)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("no more new labels are available")));
	}

	return labid;
}

/*
 * Retrieves a list of edge label OIDs that are connected to a specific vertex label.
 * Finds edges where the given vertex label appears as either start or end vertex.
 */
List *
get_connected_edge_labels_for_vertex(Snapshot snapshot, Oid graph_oid, Labid vertex_labid)
{
	List	   *edge_labels = NIL;
	List	   *seen_edge_labels = NIL;
	int			i;
	CatCList   *tuplist = SearchSysCacheList2(GRAPHMETASTART, graph_oid, vertex_labid);

	for (i = 0; i < tuplist->n_members; i++)
	{
		HeapTuple	tup = &tuplist->members[i]->tuple;
		Form_ag_graphmeta metatup = (Form_ag_graphmeta) GETSTRUCT(tup);
		Labid		edge_labid = metatup->edge;

		/* Avoid duplicates */
		if (!list_member_int(seen_edge_labels, edge_labid))
		{
			Oid			edge_relid = get_labid_relid(graph_oid, edge_labid);

			edge_labels = lappend_oid(edge_labels, edge_relid);
			seen_edge_labels = lappend_int(seen_edge_labels, edge_labid);
		}
	}
	ReleaseCatCacheList(tuplist);

	tuplist = SearchSysCacheList2(GRAPHMETAEND, graph_oid, vertex_labid);
	for (i = 0; i < tuplist->n_members; i++)
	{
		HeapTuple	tup = &tuplist->members[i]->tuple;
		Form_ag_graphmeta metatup = (Form_ag_graphmeta) GETSTRUCT(tup);
		Labid		edge_labid = metatup->edge;

		/* Avoid duplicates */
		if (!list_member_int(seen_edge_labels, edge_labid))
		{
			Oid			edge_relid = get_labid_relid(graph_oid, edge_labid);

			edge_labels = lappend_oid(edge_labels, edge_relid);
			seen_edge_labels = lappend_int(seen_edge_labels, edge_labid);
		}
	}
	ReleaseCatCacheList(tuplist);

	list_free(seen_edge_labels);
	return edge_labels;
}

/*
 * Retrieves, for a specific edge label, the sets of start and end vertex labels
 * it connects.  A single GRAPHMETAFULL list search on (graph, edge) returns
 * every (start, end) connectivity triple recorded for the edge label; we collect
 * the distinct start vertex labels into *start_relids and the distinct end
 * vertex labels into *end_relids (as relation OIDs).  Either list may be NIL if
 * the edge label has no recorded edges, and labels that no longer resolve to a
 * relation are skipped.
 *
 * This is the inverse of get_connected_edge_labels_for_vertex(): given an edge
 * label, which vertex labels can sit at its endpoints.  The planner uses it to
 * prune which vertex label tables a MATCH endpoint must scan.
 */
void
get_vertex_labels_for_edge(Oid graph_oid, Labid edge_labid,
						   List **start_relids, List **end_relids)
{
	List	   *starts = NIL;
	List	   *ends = NIL;
	List	   *seen_start = NIL;
	List	   *seen_end = NIL;
	int			i;
	CatCList   *tuplist = SearchSysCacheList2(GRAPHMETAFULL, graph_oid, edge_labid);

	for (i = 0; i < tuplist->n_members; i++)
	{
		HeapTuple	tup = &tuplist->members[i]->tuple;
		Form_ag_graphmeta metatup = (Form_ag_graphmeta) GETSTRUCT(tup);
		Labid		start_labid = metatup->start;
		Labid		end_labid = metatup->end;

		/* Avoid duplicates; skip labels that no longer resolve */
		if (!list_member_int(seen_start, start_labid))
		{
			Oid			relid = get_labid_relid(graph_oid, start_labid);

			seen_start = lappend_int(seen_start, start_labid);
			if (OidIsValid(relid))
				starts = lappend_oid(starts, relid);
		}
		if (!list_member_int(seen_end, end_labid))
		{
			Oid			relid = get_labid_relid(graph_oid, end_labid);

			seen_end = lappend_int(seen_end, end_labid);
			if (OidIsValid(relid))
				ends = lappend_oid(ends, relid);
		}
	}
	ReleaseCatCacheList(tuplist);

	list_free(seen_start);
	list_free(seen_end);

	*start_relids = starts;
	*end_relids = ends;
}

