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

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/ag_inherits.h"
#include "catalog/ag_label.h"
#include "catalog/ag_label_fn.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

static void InsertAgLabelTuple(Relation ag_label_desc, Oid labid,
							   const char *labname, Oid relid, Oid ownerid,
							   char labkind);
static void LabelRemoveInheritance(Oid labid);
static void DeleteLabelTuple(Oid labid);

Oid
label_create_with_catalog(const char *labname, Oid relid, Oid ownerid,
						  char labkind, Oid labtablespace, char labpersistence)
{
	Relation	ag_label_desc;
	Oid			labid;

	ag_label_desc = heap_open(LabelRelationId, RowExclusiveLock);

	labid = GetNewRelFileNode(labtablespace, ag_label_desc, labpersistence);

	InsertAgLabelTuple(ag_label_desc, labid, labname, relid, ownerid, labkind);

	heap_close(ag_label_desc, RowExclusiveLock);

	return labid;
}

void
label_drop_with_catalog(Oid labid)
{
	LabelRemoveInheritance(labid);

	DeleteLabelTuple(labid);
}

/*
 * InsertAgLabelTuple - register the new label in ag_label
 *
 * See InsertPgClassTuple()
 */
static void
InsertAgLabelTuple(Relation ag_label_desc, Oid labid, const char *labname,
				   Oid relid, Oid ownerid, char labkind)
{
	Datum		values[Natts_ag_label];
	bool		nulls[Natts_ag_label];
	HeapTuple	tup;

	AssertArg(labkind == LABEL_KIND_VERTEX || labkind == LABEL_KIND_EDGE);

	values[Anum_ag_label_labname - 1] = CStringGetDatum(labname);
	values[Anum_ag_label_relid - 1] = ObjectIdGetDatum(relid);
	values[Anum_ag_label_labowner- 1] = ObjectIdGetDatum(ownerid);
	values[Anum_ag_label_labkind - 1] = CharGetDatum(labkind);

	memset(nulls, false, sizeof(nulls));

	tup = heap_form_tuple(RelationGetDescr(ag_label_desc), values, nulls);

	HeapTupleSetOid(tup, labid);

	simple_heap_insert(ag_label_desc, tup);

	CatalogUpdateIndexes(ag_label_desc, tup);

	heap_freetuple(tup);
}

/* See RelationRemoveInheritance() */
static void
LabelRemoveInheritance(Oid labid)
{
	Relation	catalogRelation;
	ScanKeyData key;
	SysScanDesc scan;
	HeapTuple	tuple;

	catalogRelation = heap_open(AgInheritsRelationId, RowExclusiveLock);

	ScanKeyInit(&key, Anum_ag_inherits_inhrelid, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(labid));

	scan = systable_beginscan(catalogRelation, AgInheritsRelidSeqnoIndexId,
							  true, NULL, 1, &key);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
		simple_heap_delete(catalogRelation, &tuple->t_self);

	systable_endscan(scan);
	heap_close(catalogRelation, RowExclusiveLock);
}

/*
 * DeleteLabelTuple - remove ag_label row for the given labid
 *
 * See DeleteRelationTuple()
 */
static void
DeleteLabelTuple(Oid labid)
{
	Relation	ag_label_desc;
	HeapTuple	tup;

	ag_label_desc = heap_open(LabelRelationId, RowExclusiveLock);

	tup = SearchSysCache1(LABELOID, ObjectIdGetDatum(labid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for label %u", labid);

	simple_heap_delete(ag_label_desc, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(ag_label_desc, RowExclusiveLock);
}
