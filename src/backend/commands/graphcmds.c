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
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "catalog/toasting.h"
#include "commands/event_trigger.h"
#include "commands/graphcmds.h"
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

static ObjectAddress DefineLabel(CreateStmt *stmt, char labkind);
static void GetSuperOids(List *supers, char labkind, List **supOids);
static void AgInheritanceDependancy(Oid labid, List *supers);

/* See ProcessUtilitySlow() case T_CreateSchemaStmt */
void
CreateGraphCommand(CreateGraphStmt *stmt, const char *queryString)
{
	List	   *parsetree_list;
	ListCell   *parsetree_item;

	GraphCreate(stmt, queryString);

	parsetree_list = transformCreateGraphStmt(stmt);

	foreach(parsetree_item, parsetree_list)
	{
		Node *stmt = lfirst(parsetree_item);

		ProcessUtility(stmt,
					   queryString,
					   PROCESS_UTILITY_SUBCOMMAND,
					   NULL,
					   None_Receiver,
					   NULL);

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

	labid = label_create_with_catalog(stmt->relation, reladdr.objectId,
									  labkind, tablespaceId);

	GetSuperOids(stmt->inhRelations, labkind, &inheritOids);
	AgInheritanceDependancy(labid, inheritOids);

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

		parent_labid = get_labname_labid(parent->relname, parent->schemaname);

		tuple = SearchSysCache1(LABELOID, ObjectIdGetDatum(parent_labid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for parent label (OID=%u)",
				 parent_labid);

		labtup = (Form_ag_label) GETSTRUCT(tuple);
		if (labtup->labkind != labkind)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("invalid parent label with labkind '%c'",
							labtup->labkind)));

		ReleaseSysCache(tuple);

		parentOids = lappend_oid(parentOids, parent_labid);
	}

	*supOids = parentOids;
}

/* This function mimics StoreCatalogInheritance() */
static void
AgInheritanceDependancy(Oid labid, List *supers)
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
		childobject.objectId = labid;
		childobject.objectSubId = 0;
		parentobject.classId = LabelRelationId;
		parentobject.objectId = parentOid;
		parentobject.objectSubId = 0;
		recordDependencyOn(&childobject, &parentobject, DEPENDENCY_NORMAL);

		seq++;
	}
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

bool
isLabel(RangeVar *rel)
{
	HeapTuple	nsptuple;
	HeapTuple	graphtuple;
	Oid			nspid;
	bool		result = false;
	Form_pg_namespace nspdata;

	nspid = RangeVarGetCreationNamespace(rel);

	nsptuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(nspid));

	if (!HeapTupleIsValid(nsptuple))
		elog(ERROR, "cache lookup failed for label (OID=%u)", nspid);

	nspdata = (Form_pg_namespace) GETSTRUCT(nsptuple);

	graphtuple = SearchSysCache1(GRAPHNAME,
								 CStringGetDatum(NameStr(nspdata->nspname)));

	ReleaseSysCache(nsptuple);

	if (HeapTupleIsValid(graphtuple))
	{
		ReleaseSysCache(graphtuple);
		result = true;
	}

	return result;
}
