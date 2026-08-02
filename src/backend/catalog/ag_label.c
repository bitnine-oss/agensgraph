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
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/ag_graph_fn.h"
#include "catalog/ag_label_fn.h"
#include "catalog/ag_graphmeta.h"
#include "catalog/ag_label.h"
#include "catalog/ag_label_property.h"
#include "catalog/ag_label_fn.h"
#include "catalog/binary_upgrade.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pg_inherits.h"
#include "commands/sequence.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/inval.h"
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

	/* forget any promoted typed properties this label declared */
	DeleteAgLabelProperties(laboid);

	ag_label_desc = table_open(LabelRelationId, RowExclusiveLock);

	tup = SearchSysCache1(LABELOID, ObjectIdGetDatum(laboid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for label %u", laboid);

	simple_heap_delete(ag_label_desc, &tup->t_self);

	ReleaseSysCache(tup);

	table_close(ag_label_desc, RowExclusiveLock);
}

/*
 * InsertAgLabelProperty - register a promoted typed property in
 * ag_label_property, mapping the (down-cased) Cypher key to the storage column
 * that materializes it.
 */
void
InsertAgLabelProperty(Oid laboid, const char *propname, int16 attnum,
					   char semantics)
{
	Relation	desc;
	Datum		values[Natts_ag_label_property];
	bool		nulls[Natts_ag_label_property];
	NameData	pname;
	HeapTuple	tup;

	namestrcpy(&pname, propname);

	desc = table_open(LabelPropertyRelationId, RowExclusiveLock);

	values[Anum_ag_label_property_laboid - 1] = ObjectIdGetDatum(laboid);
	values[Anum_ag_label_property_propname - 1] = NameGetDatum(&pname);
	values[Anum_ag_label_property_attnum - 1] = Int16GetDatum(attnum);
	values[Anum_ag_label_property_semantics - 1] = CharGetDatum(semantics);
	memset(nulls, false, sizeof(nulls));

	tup = heap_form_tuple(RelationGetDescr(desc), values, nulls);
	CatalogTupleInsert(desc, tup);
	heap_freetuple(tup);

	table_close(desc, RowExclusiveLock);
}

/*
 * DeleteAgLabelProperties - remove every ag_label_property row for a label.
 */
void
DeleteAgLabelProperties(Oid laboid)
{
	Relation	desc;
	ScanKeyData skey;
	SysScanDesc scan;
	HeapTuple	tup;

	desc = table_open(LabelPropertyRelationId, RowExclusiveLock);

	ScanKeyInit(&skey, Anum_ag_label_property_laboid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(laboid));

	scan = systable_beginscan(desc, LabelPropertyLabidPropIndexId, true,
							  NULL, 1, &skey);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
		CatalogTupleDelete(desc, &tup->t_self);

	systable_endscan(scan);

	table_close(desc, RowExclusiveLock);
}

/*
 * DeleteAgLabelProperty - remove the single ag_label_property row that maps a
 * promoted property key to a label's typed column.  A no-op if the key is not
 * recorded for the label.  Used when a promoted typed column is dropped and the
 * label must forget it.
 */
void
DeleteAgLabelProperty(Oid laboid, const char *propname)
{
	Relation	desc;
	HeapTuple	tup;

	desc = table_open(LabelPropertyRelationId, RowExclusiveLock);

	tup = SearchSysCache2(LABELPROPNAME, ObjectIdGetDatum(laboid),
						  PointerGetDatum(propname));
	if (HeapTupleIsValid(tup))
	{
		CatalogTupleDelete(desc, &tup->t_self);
		ReleaseSysCache(tup);
	}

	table_close(desc, RowExclusiveLock);
}

/*
 * get_label_property_name_by_attnum - the promoted property key a label
 * materializes at a given typed column attnum, or NULL if no promoted property
 * maps to that column.  Keys by attnum (not name) because the explicit-key form
 * "col type GENERATED (srckey)" records propname = srckey, which differs from
 * the column name; dropping a column names the column but must forget its key.
 */
char *
get_label_property_name_by_attnum(Oid laboid, AttrNumber attnum)
{
	Relation	desc;
	ScanKeyData skey;
	SysScanDesc scan;
	HeapTuple	tup;
	char	   *propname = NULL;

	desc = table_open(LabelPropertyRelationId, AccessShareLock);

	ScanKeyInit(&skey, Anum_ag_label_property_laboid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(laboid));

	scan = systable_beginscan(desc, LabelPropertyLabidPropIndexId, true,
							  NULL, 1, &skey);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_ag_label_property form = (Form_ag_label_property) GETSTRUCT(tup);

		if (form->attnum == attnum)
		{
			propname = pstrdup(NameStr(form->propname));
			break;
		}
	}

	systable_endscan(scan);
	table_close(desc, AccessShareLock);

	return propname;
}

/*
 * get_label_property_attnum - attnum of the typed column that materializes a
 * promoted property key on a label, or InvalidAttrNumber if the key is not a
 * promoted property of that label.
 */
AttrNumber
get_label_property_attnum(Oid laboid, const char *propname)
{
	HeapTuple	tup;
	AttrNumber	attnum = InvalidAttrNumber;

	tup = SearchSysCache2(LABELPROPNAME, ObjectIdGetDatum(laboid),
						  PointerGetDatum(propname));
	if (HeapTupleIsValid(tup))
	{
		attnum = ((Form_ag_label_property) GETSTRUCT(tup))->attnum;
		ReleaseSysCache(tup);
	}

	return attnum;
}

/*
 * label_relid_has_promoted_property - does the label whose storage table is
 * `relid` declare any promoted typed property?  Gates the Cypher compiler so
 * a plain (non-promoted) label emits byte-identical output.
 */
bool
label_relid_has_promoted_property(Oid relid)
{
	Oid			laboid = get_relid_laboid(relid);
	Relation	desc;
	ScanKeyData skey;
	SysScanDesc scan;
	bool		found;

	if (!OidIsValid(laboid))
		return false;

	desc = table_open(LabelPropertyRelationId, AccessShareLock);

	ScanKeyInit(&skey, Anum_ag_label_property_laboid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(laboid));

	scan = systable_beginscan(desc, LabelPropertyLabidPropIndexId, true,
							  NULL, 1, &skey);

	found = HeapTupleIsValid(systable_getnext(scan));

	systable_endscan(scan);
	table_close(desc, AccessShareLock);

	return found;
}

/*
 * scan_own_promoted_properties - the promoted properties declared directly on
 * one label (by its ag_label oid), as a list of palloc'd PromotedPropInfo.
 */
static List *
scan_own_promoted_properties(Oid laboid)
{
	Relation	desc;
	ScanKeyData skey;
	SysScanDesc scan;
	HeapTuple	tup;
	List	   *result = NIL;

	if (!OidIsValid(laboid))
		return NIL;

	desc = table_open(LabelPropertyRelationId, AccessShareLock);

	ScanKeyInit(&skey, Anum_ag_label_property_laboid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(laboid));

	scan = systable_beginscan(desc, LabelPropertyLabidPropIndexId, true,
							  NULL, 1, &skey);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_ag_label_property form = (Form_ag_label_property) GETSTRUCT(tup);
		PromotedPropInfo *info = palloc0(sizeof(PromotedPropInfo));

		strlcpy(info->propname, NameStr(form->propname), NAMEDATALEN);
		info->attnum = form->attnum;
		info->semantics = form->semantics;

		result = lappend(result, info);
	}

	systable_endscan(scan);
	table_close(desc, AccessShareLock);

	return result;
}

/*
 * get_label_promoted_properties - every promoted typed property RESOLVABLE on
 * the label whose storage table is `relid`: its own, plus those it inherits
 * from ancestor labels.
 *
 * A promoted property is recorded in ag_label_property only for the label that
 * declares it, but table inheritance gives a child label a physical copy of the
 * ancestor's typed column (same name, possibly a different attnum).  So each
 * inherited property is re-mapped to the CHILD relation's column by name; the
 * ancestor's attnum is used only to name the column, never reused directly.  A
 * nearer label wins a name clash (ancestors are walked child-first), so a key a
 * child re-declares keeps the child's own column.
 */
List *
get_label_promoted_properties(Oid relid)
{
	List	   *result = NIL;
	List	   *ancestors;
	ListCell   *la;

	/* find_all_ancestors() returns `relid' itself first, then its ancestors */
	ancestors = find_all_ancestors(relid, AccessShareLock);

	foreach(la, ancestors)
	{
		Oid			ancestor = lfirst_oid(la);
		List	   *own = scan_own_promoted_properties(get_relid_laboid(ancestor));
		ListCell   *lp;

		foreach(lp, own)
		{
			PromotedPropInfo *ap = lfirst(lp);
			char	   *colname;
			AttrNumber	child_attnum;
			PromotedPropInfo *info;
			ListCell   *lr;
			bool		seen = false;

			/* a nearer label already resolves this key */
			foreach(lr, result)
			{
				if (strcmp(((PromotedPropInfo *) lfirst(lr))->propname,
						   ap->propname) == 0)
				{
					seen = true;
					break;
				}
			}
			if (seen)
				continue;

			/* map the ancestor's column to the child's own column by name */
			colname = get_attname(ancestor, ap->attnum, true);
			child_attnum = (colname != NULL) ? get_attnum(relid, colname)
				: InvalidAttrNumber;
			if (child_attnum == InvalidAttrNumber)
				continue;

			info = palloc0(sizeof(PromotedPropInfo));
			strlcpy(info->propname, ap->propname, NAMEDATALEN);
			info->attnum = child_attnum;
			info->semantics = ap->semantics;
			result = lappend(result, info);
		}
	}

	return result;
}

/*
 * The property keys some label in this database holds in a column.
 *
 * Asking a relation about its subtree means locking every label under it, and a
 * read has to ask once per property it names.  Nearly every question is about a
 * key no label promotes at all, and that answer is the same for every relation,
 * so the set of promoted keys is settled once and kept until the catalog that
 * could change it is invalidated.  A read of an unpromoted key then costs a walk
 * of that set rather than a walk of the inheritance tree.
 */
static List *promoted_names = NIL;
static bool promoted_names_known = false;
static bool promoted_callback_set = false;

static void
InvalidatePromotedNames(Datum arg, int cacheid, uint32 hashvalue)
{
	promoted_names_known = false;
}

static void
LoadPromotedNames(void)
{
	Relation	desc;
	SysScanDesc scan;
	HeapTuple	tup;
	MemoryContext old;

	if (!promoted_callback_set)
	{
		CacheRegisterSyscacheCallback(LABELPROPNAME, InvalidatePromotedNames,
									  (Datum) 0);
		promoted_callback_set = true;
	}
	else if (promoted_names_known)
		return;

	old = MemoryContextSwitchTo(CacheMemoryContext);
	list_free_deep(promoted_names);
	promoted_names = NIL;
	MemoryContextSwitchTo(old);

	desc = table_open(LabelPropertyRelationId, AccessShareLock);
	scan = systable_beginscan(desc, InvalidOid, false, NULL, 0, NULL);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_ag_label_property prop = (Form_ag_label_property) GETSTRUCT(tup);
		char	   *name = NameStr(prop->propname);
		ListCell   *lc;
		bool		seen = false;

		foreach(lc, promoted_names)
		{
			if (strcmp((char *) lfirst(lc), name) == 0)
			{
				seen = true;
				break;
			}
		}
		if (seen)
			continue;

		old = MemoryContextSwitchTo(CacheMemoryContext);
		promoted_names = lappend(promoted_names, pstrdup(name));
		MemoryContextSwitchTo(old);
	}

	systable_endscan(scan);
	table_close(desc, AccessShareLock);

	promoted_names_known = true;
}

/*
 * Whether any label in this database holds `propname' in a column.  A key no
 * label promotes never needs a relation asked about it.
 */
static bool
AnyLabelPromotes(const char *propname)
{
	ListCell   *lc;

	Assert(propname != NULL);

	LoadPromotedNames();

	foreach(lc, promoted_names)
	{
		if (strcmp((char *) lfirst(lc), propname) == 0)
			return true;
	}
	return false;
}

/*
 * subtree_has_promoted_property - whether a label under `relid' holds `propname'
 * in a column.
 *
 * A scan of a label reads its descendants, and a descendant may hold in a column
 * what the label being read keeps only in its property map.  Reading the column
 * where there is one is the same answer and a cheaper one, so a read has to know
 * that the question is worth asking per relation.
 */
bool
subtree_has_promoted_property(Oid relid, const char *propname)
{
	List	   *inheritors;
	ListCell   *li;
	bool		found = false;

	if (!OidIsValid(relid) || !AnyLabelPromotes(propname))
		return false;

	inheritors = find_all_inheritors(relid, AccessShareLock, NULL);
	foreach(li, inheritors)
	{
		Oid			child = lfirst_oid(li);

		if (child == relid)
			continue;
		if (get_label_property_column(child, propname, NULL, NULL))
		{
			found = true;
			break;
		}
	}

	list_free(inheritors);
	return found;
}

/*
 * get_label_property_column - resolve a (down-cased) Cypher property key to the
 * typed column that materializes it on the label whose storage table is
 * `relid`, whether the key is promoted on the label itself or inherited from an
 * ancestor label.  Returns true and fills *attnum (the CHILD relation's column)
 * / *semantics when the key resolves; false otherwise.  Either out-param may be
 * NULL.
 */
bool
get_label_property_column(Oid relid, const char *propname,
						  AttrNumber *attnum, char *semantics)
{
	Oid			laboid = get_relid_laboid(relid);
	HeapTuple	tup;
	List	   *ancestors;
	ListCell   *la;

	/* fast path: a key the label declares itself */
	if (OidIsValid(laboid))
	{
		tup = SearchSysCache2(LABELPROPNAME, ObjectIdGetDatum(laboid),
							  PointerGetDatum(propname));
		if (HeapTupleIsValid(tup))
		{
			if (attnum != NULL)
				*attnum = ((Form_ag_label_property) GETSTRUCT(tup))->attnum;
			if (semantics != NULL)
				*semantics = ((Form_ag_label_property) GETSTRUCT(tup))->semantics;
			ReleaseSysCache(tup);
			return true;
		}
	}

	/* otherwise look for the key on an ancestor and map it to `relid's column */
	ancestors = find_all_ancestors(relid, AccessShareLock);
	foreach(la, ancestors)
	{
		Oid			ancestor = lfirst_oid(la);
		Oid			alaboid = get_relid_laboid(ancestor);
		AttrNumber	a_attnum;
		char		a_semantics;
		char	   *colname;
		AttrNumber	child_attnum;

		if (ancestor == relid || !OidIsValid(alaboid))
			continue;

		tup = SearchSysCache2(LABELPROPNAME, ObjectIdGetDatum(alaboid),
							  PointerGetDatum(propname));
		if (!HeapTupleIsValid(tup))
			continue;
		a_attnum = ((Form_ag_label_property) GETSTRUCT(tup))->attnum;
		a_semantics = ((Form_ag_label_property) GETSTRUCT(tup))->semantics;
		ReleaseSysCache(tup);

		colname = get_attname(ancestor, a_attnum, true);
		child_attnum = (colname != NULL) ? get_attnum(relid, colname)
			: InvalidAttrNumber;
		if (child_attnum == InvalidAttrNumber)
			continue;

		if (attnum != NULL)
			*attnum = child_attnum;
		if (semantics != NULL)
			*semantics = a_semantics;
		return true;
	}

	return false;
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

