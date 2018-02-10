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
#include "catalog/ag_graph.h"
#include "catalog/ag_graph_fn.h"
#include "catalog/ag_label.h"
#include "catalog/ag_label_fn.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "catalog/toasting.h"
#include "commands/event_trigger.h"
#include "commands/graphcmds.h"
#include "commands/schemacmds.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "parser/parse_utilcmd.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

static ObjectAddress DefineLabel(CreateStmt *stmt, char labkind,
								 const char *queryString);
static void GetSuperOids(List *supers, char labkind, List **supOids);
static void AgInheritanceDependancy(Oid laboid, List *supers);
static void SetMaxStatisticsTarget(Oid laboid);

/* See ProcessUtilitySlow() case T_CreateSchemaStmt */
void
CreateGraphCommand(CreateGraphStmt *stmt, const char *queryString,
				   int stmt_location, int stmt_len)
{
	Oid			graphid;
	List	   *parsetree_list;
	ListCell   *parsetree_item;

	graphid = GraphCreate(stmt, queryString, stmt_location, stmt_len);
	if (!OidIsValid(graphid))
		return;

	CommandCounterIncrement();

	parsetree_list = transformCreateGraphStmt(stmt);

	foreach(parsetree_item, parsetree_list)
	{
		Node	   *stmt = lfirst(parsetree_item);
		PlannedStmt *wrapper;

		wrapper = makeNode(PlannedStmt);
		wrapper->commandType = CMD_UTILITY;
		wrapper->canSetTag = false;
		wrapper->utilityStmt = stmt;
		wrapper->stmt_location = stmt_location;
		wrapper->stmt_len = stmt_len;

		ProcessUtility(wrapper, queryString, PROCESS_UTILITY_SUBCOMMAND,
					   NULL, NULL, None_Receiver, NULL);

		CommandCounterIncrement();
	}

	if (graph_path == NULL || strcmp(graph_path, "") == 0)
		SetConfigOption("graph_path", stmt->graphname,
						PGC_USERSET, PGC_S_SESSION);
}

void
RemoveGraphById(Oid graphid)
{
	Relation	ag_graph_desc;
	HeapTuple	tup;
	Form_ag_graph graphtup;
	NameData	graphname;

	ag_graph_desc = heap_open(GraphRelationId, RowExclusiveLock);

	tup = SearchSysCache1(GRAPHOID, ObjectIdGetDatum(graphid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for graph %u", graphid);

	graphtup = (Form_ag_graph) GETSTRUCT(tup);
	namecpy(&graphname, &graphtup->graphname);

	simple_heap_delete(ag_graph_desc, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(ag_graph_desc, RowExclusiveLock);

	if (graph_path != NULL && namestrcmp(&graphname, graph_path) == 0)
		SetConfigOption("graph_path", NULL, PGC_USERSET, PGC_S_SESSION);
}

ObjectAddress
RenameGraph(const char *oldname, const char *newname)
{
	Oid			graphOid;
	HeapTuple	tup;
	Relation	rel;
	ObjectAddress address;

	rel = heap_open(GraphRelationId, RowExclusiveLock);

	tup = SearchSysCacheCopy1(GRAPHNAME, CStringGetDatum(oldname));
	if (!HeapTupleIsValid(tup))
	{
		ereport(NOTICE,
				(errmsg("graph \"%s\" does not exist, skipping",
						oldname)));
		return InvalidObjectAddress;
	}

	/* renamimg schema and graph should be processed in one lock */
	RenameSchema(oldname, newname);

	graphOid = HeapTupleGetOid(tup);

	/* Skip privilege and error check. It was already done in RenameSchema() */

	/* rename */
	namestrcpy(&(((Form_ag_graph) GETSTRUCT(tup))->graphname), newname);
	CatalogTupleUpdate(rel, &tup->t_self, tup);

	InvokeObjectPostAlterHook(GraphRelationId, HeapTupleGetOid(tup), 0);

	ObjectAddressSet(address, GraphRelationId, graphOid);

	heap_close(rel, NoLock);
	heap_freetuple(tup);

	CommandCounterIncrement();
	if (strcmp(graph_path, oldname) == 0)
		SetConfigOption("graph_path", newname, PGC_USERSET, PGC_S_SESSION);

	return address;
}

/* See ProcessUtilitySlow() case T_CreateStmt */
void
CreateLabelCommand(CreateLabelStmt *labelStmt, const char *queryString,
				   int stmt_location, int stmt_len, ParamListInfo params)
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
			DefineLabel((CreateStmt *) stmt, labkind, queryString);
		}
		else
		{
			PlannedStmt *wrapper;

			/*
			 * Recurse for anything else.  Note the recursive call will stash
			 * the objects so created into our event trigger context.
			 */

			wrapper = makeNode(PlannedStmt);
			wrapper->commandType = CMD_UTILITY;
			wrapper->canSetTag = false;
			wrapper->utilityStmt = stmt;
			wrapper->stmt_location = stmt_location;
			wrapper->stmt_len = stmt_len;

			ProcessUtility(wrapper, queryString, PROCESS_UTILITY_SUBCOMMAND,
						   params, NULL, None_Receiver, NULL);
		}

		CommandCounterIncrement();
	}
}

/* creates a new graph label */
static ObjectAddress
DefineLabel(CreateStmt *stmt, char labkind, const char *queryString)
{
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	ObjectAddress reladdr;
	Datum		toast_options;
	Oid			tablespaceId;
	Oid			laboid;
	List	   *inheritOids = NIL;
	ObjectAddress labaddr;

	/*
	 * Create the table
	 */

	reladdr = DefineRelation(stmt, RELKIND_RELATION, InvalidOid, NULL,
							 queryString);
	EventTriggerCollectSimpleCommand(reladdr, InvalidObjectAddress,
									 (Node *) stmt);

	CommandCounterIncrement();

	if (labkind == LABEL_KIND_EDGE)
		SetMaxStatisticsTarget(reladdr.objectId);

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

	laboid = label_create_with_catalog(stmt->relation, reladdr.objectId,
									   labkind, tablespaceId);

	GetSuperOids(stmt->inhRelations, labkind, &inheritOids);
	AgInheritanceDependancy(laboid, inheritOids);

	/*
	 * Make a dependency link to force the table to be deleted if its
	 * graph label is.
	 */
	labaddr.classId = LabelRelationId;
	labaddr.objectId = laboid;
	labaddr.objectSubId = 0;
	recordDependencyOn(&reladdr, &labaddr, DEPENDENCY_INTERNAL);

	return labaddr;
}

/* See ATExecSetStatistics() */
static void
SetMaxStatisticsTarget(Oid laboid)
{
	Relation	attrelation;
	HeapTuple	tuple;
	Form_pg_attribute attrtuple;
	int			maxtarget = 10000;

	attrelation = heap_open(AttributeRelationId, RowExclusiveLock);

	/* start column */

	tuple = SearchSysCacheCopyAttName(laboid, AG_START_ID);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("edge label must have start column")));

	attrtuple = (Form_pg_attribute) GETSTRUCT(tuple);
	attrtuple->attstattarget = maxtarget;

	CatalogTupleUpdate(attrelation, &tuple->t_self, tuple);

	heap_freetuple(tuple);

	/* end column */

	tuple = SearchSysCacheCopyAttName(laboid, AG_END_ID);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("edge label must have end column")));

	attrtuple = (Form_pg_attribute) GETSTRUCT(tuple);

	attrtuple->attstattarget = maxtarget;

	CatalogTupleUpdate(attrelation, &tuple->t_self, tuple);

	heap_freetuple(tuple);

	heap_close(attrelation, RowExclusiveLock);
}

static void
GetSuperOids(List *supers, char labkind, List **supOids)
{
	List	   *parentOids = NIL;
	ListCell   *entry;

	foreach(entry, supers)
	{
		RangeVar   *parent = lfirst(entry);
		Oid			graphid;
		HeapTuple	tuple;
		Form_ag_label labtup;
		Oid			parent_laboid;

		graphid = get_graphname_oid(parent->schemaname);

		tuple = SearchSysCache2(LABELNAMEGRAPH,
								PointerGetDatum(parent->relname),
								ObjectIdGetDatum(graphid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for parent label \"%s.%s\"",
				 parent->schemaname, parent->relname);

		labtup = (Form_ag_label) GETSTRUCT(tuple);
		if (labtup->labkind != labkind)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("invalid parent label with labkind '%c'",
							labtup->labkind)));

		parent_laboid = HeapTupleGetOid(tuple);
		ReleaseSysCache(tuple);

		parentOids = lappend_oid(parentOids, parent_laboid);
	}

	*supOids = parentOids;
}

/* This function mimics StoreCatalogInheritance() */
static void
AgInheritanceDependancy(Oid laboid, List *supers)
{
	int16		seq;
	ListCell   *entry;

	if (supers == NIL)
		return;

	seq = 1;
	foreach(entry, supers)
	{
		Oid parentOid = lfirst_oid(entry);
		ObjectAddress childobject;
		ObjectAddress parentobject;

		childobject.classId = LabelRelationId;
		childobject.objectId = laboid;
		childobject.objectSubId = 0;
		parentobject.classId = LabelRelationId;
		parentobject.objectId = parentOid;
		parentobject.objectSubId = 0;
		recordDependencyOn(&childobject, &parentobject, DEPENDENCY_NORMAL);

		seq++;
	}
}

ObjectAddress
RenameLabel(RenameStmt *stmt)
{
	Oid			graphid = get_graph_path_oid();
	Relation	rel;
	HeapTuple	tup;
	Oid			laboid;
	ObjectAddress address;

	/* schemaname is NULL always */
	stmt->relation->schemaname = get_graph_path(false);

	rel = heap_open(LabelRelationId, RowExclusiveLock);

	tup = SearchSysCacheCopy2(LABELNAMEGRAPH,
							  CStringGetDatum(stmt->relation->relname),
							  ObjectIdGetDatum(graphid));
	if (!HeapTupleIsValid(tup))
	{
		heap_close(rel, NoLock);

		ereport(NOTICE,
				(errmsg("label \"%s\" does not exist, skipping",
						stmt->relation->relname)));
		return InvalidObjectAddress;
	}

	laboid = HeapTupleGetOid(tup);

	CheckLabelType(stmt->renameType, laboid, "RENAME");

	/* renamimg label and table should be processed in one lock */
	RenameRelation(stmt);

	/* Skip privilege and error check. It was already done in RenameRelation */

	/* rename */
	namestrcpy(&(((Form_ag_label) GETSTRUCT(tup))->labname), stmt->newname);
	CatalogTupleUpdate(rel, &tup->t_self, tup);

	InvokeObjectPostAlterHook(LabelRelationId, HeapTupleGetOid(tup), 0);

	ObjectAddressSet(address, LabelRelationId, laboid);

	heap_close(rel, NoLock);
	heap_freetuple(tup);

	return address;
}

/* check the given `type` and `laboid` matches */
void
CheckLabelType(ObjectType type, Oid laboid, const char *command)
{
	HeapTuple tuple;
	Form_ag_label labtup;

	tuple = SearchSysCache1(LABELOID, ObjectIdGetDatum(laboid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for label (OID=%u)", laboid);

	labtup = (Form_ag_label) GETSTRUCT(tuple);

	if (type == OBJECT_VLABEL && labtup->labkind != LABEL_KIND_VERTEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("%s VLABEL cannot %s edge label", command, command)));
	if (type == OBJECT_ELABEL && labtup->labkind != LABEL_KIND_EDGE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("%s ELABEL cannot %s vertex label", command, command)));

	if (namestrcmp(&(labtup->labname), AG_VERTEX) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("%s base vertex label is prohibited", command)));
	if (namestrcmp(&(labtup->labname), AG_EDGE) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("%s base edge label is prohibited", command)));

	ReleaseSysCache(tuple);
}

void
CheckInheritLabel(CreateStmt *stmt)
{
	ListCell   *entry;

	foreach(entry, stmt->inhRelations)
	{
		RangeVar   *parent = lfirst(entry);

		if (RangeVarIsLabel(parent))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("invalid parent, table cannot inherit label")));
	}
}

bool
RangeVarIsLabel(RangeVar *rel)
{
	Oid			nspid;
	Oid			graphid;
	HeapTuple	nsptuple;
	Form_pg_namespace nspdata;

	nspid = RangeVarGetCreationNamespace(rel);
	nsptuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nspid));
	if (!HeapTupleIsValid(nsptuple))
		elog(ERROR, "cache lookup failed for label (OID=%u)", nspid);

	nspdata = (Form_pg_namespace) GETSTRUCT(nsptuple);
	graphid = get_graphname_oid(NameStr(nspdata->nspname));
	ReleaseSysCache(nsptuple);

	return OidIsValid(get_labname_laboid(rel->relname, graphid));
}

void
CreateConstraintCommand(CreateConstraintStmt *constraintStmt,
						const char *queryString, int stmt_location,
						int stmt_len,ParamListInfo params)
{
	ParseState *pstate;
	Node	   *stmt;
	PlannedStmt *wrapper;

	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = queryString;

	stmt = transformCreateConstraintStmt(pstate, constraintStmt);

	wrapper = makeNode(PlannedStmt);
	wrapper->commandType = CMD_UTILITY;
	wrapper->canSetTag = false;
	wrapper->utilityStmt = stmt;
	wrapper->stmt_location = stmt_location;
	wrapper->stmt_len = stmt_len;

	ProcessUtility(wrapper, queryString, PROCESS_UTILITY_SUBCOMMAND,
				   params, NULL, None_Receiver, NULL);

	CommandCounterIncrement();

	free_parsestate(pstate);
}

void
DropConstraintCommand(DropConstraintStmt *constraintStmt,
					  const char *queryString, int stmt_location,
					  int stmt_len,ParamListInfo params)
{
	ParseState *pstate;
	Node	   *stmt;
	PlannedStmt *wrapper;

	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = queryString;

	stmt = transformDropConstraintStmt(pstate, constraintStmt);

	wrapper = makeNode(PlannedStmt);
	wrapper->commandType = CMD_UTILITY;
	wrapper->canSetTag = false;
	wrapper->utilityStmt = stmt;
	wrapper->stmt_location = stmt_location;
	wrapper->stmt_len = stmt_len;

	ProcessUtility(wrapper, queryString, PROCESS_UTILITY_SUBCOMMAND,
				   params, NULL, None_Receiver, NULL);

	CommandCounterIncrement();

	free_parsestate(pstate);
}

Oid
DisableIndexCommand(DisableIndexStmt *disableStmt)
{
	Oid			relid;
	RangeVar   *relation = disableStmt->relation;

	relid = RangeVarGetRelidExtended(relation, ShareLock, false, false,
									 RangeVarCallbackOwnsTable, NULL);

	if (!RangeVarIsLabel(relation))
		elog(ERROR, "invalid DISABLE INDEX");

	if (!DisableIndexLabel(relid))
		ereport(NOTICE,
				(errmsg("label \"%s\" has no enabled index",
						relation->relname)));

	return relid;
}
