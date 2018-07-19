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
#include "catalog/ag_label.h"
#include "catalog/ag_label_fn.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "commands/sequence.h"
#include "utils/builtins.h"
#include "utils/graph.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

static void InsertAgLabelTuple(Relation ag_label_desc, Oid laboid,
							   RangeVar *label, Oid relid, char labkind);
static uint16 GetNewLabelId(char *graphname, Oid graphid);

Oid
label_create_with_catalog(RangeVar *label, Oid relid, char labkind,
						  Oid labtablespace)
{
	Relation	ag_label_desc;
	Oid			laboid;

	ag_label_desc = heap_open(LabelRelationId, RowExclusiveLock);

	laboid = GetNewRelFileNode(labtablespace, ag_label_desc,
							   label->relpersistence);

	InsertAgLabelTuple(ag_label_desc, laboid, label, relid, labkind);

	heap_close(ag_label_desc, RowExclusiveLock);

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

	ag_label_desc = heap_open(LabelRelationId, RowExclusiveLock);

	tup = SearchSysCache1(LABELOID, ObjectIdGetDatum(laboid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for label %u", laboid);

	simple_heap_delete(ag_label_desc, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(ag_label_desc, RowExclusiveLock);
}

/*
 * InsertAgLabelTuple - register the new label in ag_label
 *
 * See InsertPgClassTuple()
 */
static void
InsertAgLabelTuple(Relation ag_label_desc, Oid laboid, RangeVar *label,
				   Oid relid, char labkind)
{
	Oid			graphid = get_graphname_oid(label->schemaname);
	char		labname[NAMEDATALEN]={'\0'};
	int32		labid;
	Datum		values[Natts_ag_label];
	bool		nulls[Natts_ag_label];
	HeapTuple	tup;

	AssertArg(labkind == LABEL_KIND_VERTEX || labkind == LABEL_KIND_EDGE);

	labid = (int32) GetNewLabelId(label->schemaname, graphid);
	strcpy(labname, label->relname);

	values[Anum_ag_label_labname - 1] = CStringGetDatum(labname);
	values[Anum_ag_label_graphid - 1] = ObjectIdGetDatum(graphid);
	values[Anum_ag_label_labid - 1] = Int32GetDatum(labid);
	values[Anum_ag_label_relid - 1] = ObjectIdGetDatum(relid);
	values[Anum_ag_label_labkind - 1] = CharGetDatum(labkind);

	memset(nulls, false, sizeof(nulls));

	tup = heap_form_tuple(RelationGetDescr(ag_label_desc), values, nulls);

	HeapTupleSetOid(tup, laboid);

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
		Datum val;

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
