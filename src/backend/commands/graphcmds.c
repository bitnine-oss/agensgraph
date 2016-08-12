/*
 * graphcmds.c
 *	  Commands for creating and altering graph structures and settings
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/commands/graphcmds.c
 */

#include "postgres.h"

#include "ag_const.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/ag_inherits.h"
#include "catalog/ag_label.h"
#include "catalog/ag_label_fn.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_class.h"
#include "catalog/toasting.h"
#include "commands/event_trigger.h"
#include "commands/graphcmds.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "parser/parse_utilcmd.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

static ObjectAddress DefineLabel(CreateStmt *stmt, char labkind);
static void GetSuperOids(List *supers, char labkind, List **supOids);
static void StoreCatalogAgInheritance(Oid labid, List *supers);
static void StoreCatalogAgInheritance1(Oid labid, Oid parentOid, int16 seq,
									   Relation inhRelation);

/* See ProcessUtilitySlow() case T_CreateStmt */
void
CreateLabelCommand(CreateLabelStmt *labelStmt, const char *queryString,
				   ParamListInfo params)
{
	char		labkind;
	List	   *stmts;
	ListCell   *l;

	if (labelStmt->labelKind == LABEL_VERTEX)
		labkind = LABEL_KIND_VERTEX;
	else
		labkind = LABEL_KIND_EDGE;

	stmts = transformCreateLabelStmt(labelStmt, queryString);
	foreach(l, stmts)
	{
		Node *stmt = (Node *) lfirst(l);

		if (IsA(stmt, CreateStmt))
		{
			DefineLabel((CreateStmt *) stmt, labkind);
		}
		else
		{
			/*
			 * Recurse for anything else.  Note the recursive call will stash
			 * the objects so created into our event trigger context.
			 */
			ProcessUtility(stmt, queryString, PROCESS_UTILITY_SUBCOMMAND,
						   params, None_Receiver, NULL);
		}

		CommandCounterIncrement();
	}
}

/* creates a new graph label */
static ObjectAddress
DefineLabel(CreateStmt *stmt, char labkind)
{
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	ObjectAddress reladdr;
	Datum		toast_options;
	Oid			tablespaceId;
	Oid			labid;
	List	   *inheritOids = NIL;
	ObjectAddress labaddr;

	/*
	 * Create the table
	 */

	reladdr = DefineRelation(stmt, RELKIND_RELATION, InvalidOid, NULL);
	EventTriggerCollectSimpleCommand(reladdr, InvalidObjectAddress,
									 (Node *) stmt);

	CommandCounterIncrement();

	/* parse and validate reloptions for the toast table */
	toast_options = transformRelOptions((Datum) 0, stmt->options, "toast",
										validnsps, true, false);
	heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);

	/*
	 * Let NewRelationCreateToastTable decide if this
	 * one needs a secondary relation too.
	 */
	NewRelationCreateToastTable(reladdr.objectId, toast_options);

	/*
	 * Create Label
	 */

	/* current implementation does not get tablespace name; so */
	tablespaceId = GetDefaultTablespace(stmt->relation->relpersistence);

	labid = label_create_with_catalog(stmt->relation->relname, reladdr.objectId,
									  GetUserId(), labkind, tablespaceId,
									  stmt->relation->relpersistence);

	GetSuperOids(stmt->inhRelations, labkind, &inheritOids);
	StoreCatalogAgInheritance(labid, inheritOids);

	/*
	 * Make a dependency link to force the table to be deleted if its
	 * graph label is.
	 */
	labaddr.classId = LabelRelationId;
	labaddr.objectId = labid;
	labaddr.objectSubId = 0;
	recordDependencyOn(&reladdr, &labaddr, DEPENDENCY_INTERNAL);

	return labaddr;
}

static void
GetSuperOids(List *supers, char labkind, List **supOids)
{
	List	   *parentOids = NIL;
	ListCell   *entry;

	foreach(entry, supers)
	{
		RangeVar   *parent = (RangeVar *) lfirst(entry);
		Oid			parent_labid;
		HeapTuple	tuple;
		Form_ag_label labtup;

		parent_labid = get_labname_labid(parent->relname);

		tuple = SearchSysCache1(LABELOID, ObjectIdGetDatum(parent_labid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for parent label (OID=%u)",
				 parent_labid);

		labtup = (Form_ag_label) GETSTRUCT(tuple);
		if (labtup->labkind != labkind)
			elog(ERROR, "parent label has different labkind '%c'", labkind);

		ReleaseSysCache(tuple);

		parentOids = lappend_oid(parentOids, parent_labid);
	}

	*supOids = parentOids;
}

/* This function mimics StoreCatalogInheritance() */
static void
StoreCatalogAgInheritance(Oid labid, List *supers)
{
	Relation	rel;
	int16		seq;
	ListCell   *entry;

	if (supers == NIL)
		return;

	rel = heap_open(AgInheritsRelationId, RowExclusiveLock);

	seq = 1;
	foreach(entry, supers)
	{
		Oid parentOid = lfirst_oid(entry);

		StoreCatalogAgInheritance1(labid, parentOid, seq, rel);

		seq++;
	}

	heap_close(rel, RowExclusiveLock);
}

/* This function mimics StoreCatalogInheritance1() */
static void
StoreCatalogAgInheritance1(Oid labid, Oid parentOid, int16 seqNumber,
						   Relation inhRelation)
{
	TupleDesc	desc = RelationGetDescr(inhRelation);
	Datum		values[Natts_ag_inherits];
	bool		nulls[Natts_ag_inherits];
	ObjectAddress childobject;
	ObjectAddress parentobject;
	HeapTuple	tuple;

	values[Anum_ag_inherits_inhrelid - 1] = ObjectIdGetDatum(labid);
	values[Anum_ag_inherits_inhparent - 1] = ObjectIdGetDatum(parentOid);
	values[Anum_ag_inherits_inhseqno - 1] = Int16GetDatum(seqNumber);

	memset(nulls, 0, sizeof(nulls));

	tuple = heap_form_tuple(desc, values, nulls);
	simple_heap_insert(inhRelation, tuple);
	CatalogUpdateIndexes(inhRelation, tuple);
	heap_freetuple(tuple);

	childobject.classId = LabelRelationId;
	childobject.objectId = labid;
	childobject.objectSubId = 0;
	parentobject.classId = LabelRelationId;
	parentobject.objectId = parentOid;
	parentobject.objectSubId = 0;
	recordDependencyOn(&childobject, &parentobject, DEPENDENCY_NORMAL);

	InvokeObjectPostAlterHookArg(AgInheritsRelationId, labid, 0,
								 parentOid, false);
}

/*
 * DROP VLABEL cannot drop edge and base vertex label.
 * DROP ELABEL cannot drop vertex and base edge label.
 */
void
CheckDropLabel(ObjectType removeType, Oid labid)
{
	HeapTuple	tuple;
	Form_ag_label labtup;

	tuple = SearchSysCache1(LABELOID, ObjectIdGetDatum(labid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for label (OID=%u)", labid);

	labtup = (Form_ag_label) GETSTRUCT(tuple);

	if (removeType == OBJECT_VLABEL && labtup->labkind != LABEL_KIND_VERTEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("DROP VLABEL cannot drop edge label")));
	if (removeType == OBJECT_ELABEL && labtup->labkind != LABEL_KIND_EDGE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("DROP ELABEL cannot drop vertex label")));

	if (namestrcmp(&(labtup->labname), AG_VERTEX) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("cannot drop base vertex label")));
	if (namestrcmp(&(labtup->labname), AG_EDGE) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("cannot drop base edge label")));

	ReleaseSysCache(tuple);
}
