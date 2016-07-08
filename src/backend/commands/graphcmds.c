/*-------------------------------------------------------------------------
 *
 * graphcmds.c
 *	  Commands for creating and altering graph structures and settings
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/graphcmds.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/ag_inherits.h"
#include "catalog/ag_label.h"
#include "catalog/catalog.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/toasting.h"
#include "commands/event_trigger.h"
#include "commands/graphcmds.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "parser/parse_utilcmd.h"
#include "storage/lock.h"
#include "tcop/utility.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/* ----------------------------------------------------------------
 *		DefineLabel
 *				Creates a new graph label.
 *
 * ----------------------------------------------------------------
 */
void
DefineLabel(CreateLabelStmt *labstmt,
			const char *queryString,
			ParamListInfo params,
			ObjectAddress secondaryObject)
{
	Oid			labid;
	Oid			relid;
	Relation    ag_label_desc;
	Oid			tablespaceId;
	List	   *stmts;
	ListCell   *l;

	ag_label_desc = heap_open(LabelRelationId, RowExclusiveLock);

	/* Run parse analysis ... */
	stmts = transformCreateLabelStmt(labstmt, queryString);

	/* ... and do it */
	foreach(l, stmts)
	{
		CreateStmt *stmt = (CreateStmt *) lfirst(l);

		if (IsA(stmt, CreateStmt))
		{
			Datum		toast_options;
			static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
			ObjectAddress address;
			List	   *parentOids = NIL;
			ListCell   *entry;
			ObjectAddress myself,
						  referenced;

			/* Create the table itself */
			address = DefineRelation((CreateStmt *) stmt,
									 RELKIND_RELATION,
									 InvalidOid, NULL);
			EventTriggerCollectSimpleCommand(address,
											 secondaryObject,
											 (Node*)stmt);

			/*
			 * Let NewRelationCreateToastTable decide if this
			 * one needs a secondary relation too.
			 */
			CommandCounterIncrement();

			/*
			 * parse and validate reloptions for the toast
			 * table
			 */
			toast_options = transformRelOptions((Datum) 0,
							  ((CreateStmt *) stmt)->options,
												"toast",
												validnsps,
												true,
												false);
			(void) heap_reloptions(RELKIND_TOASTVALUE,
								   toast_options,
								   true);

			NewRelationCreateToastTable(address.objectId,
										toast_options);

			/* Create Label */
			if (stmt->tablespacename)
				tablespaceId = get_tablespace_oid(stmt->tablespacename, false);
			else
				tablespaceId = GetDefaultTablespace(stmt->relation->relpersistence);

			labid = GetNewRelFileNode(tablespaceId,
									  ag_label_desc,
									  stmt->relation->relpersistence);
			relid = address.objectId;

			/* ag_label catalog */
			InsertAgLabelTuple(labid,
							   stmt->relation->relname,
							   labstmt->labkind,
							   relid);

			/* ag_inherit catalog */
			foreach(entry, stmt->inhRelations)
			{
				RangeVar   *parent = (RangeVar *) lfirst(entry);
				Oid			id;

				id = GetSysCacheOid1(LABELNAME,
									 PointerGetDatum(parent->relname));

				parentOids = lappend_oid(parentOids, id);
			}

			StoreCatalogInheritance(InheritsLabelId, labid, parentOids);

			/*
			 * Make a dependency link to force the relation to be deleted if its
			 * graph label is.
			 */
			myself.classId = RelationRelationId;
			myself.objectId = relid;
			myself.objectSubId = 0;
			referenced.classId = LabelRelationId;
			referenced.objectId = labid;
			referenced.objectSubId = 0;
			recordDependencyOn(&myself, &referenced, DEPENDENCY_INTERNAL);
		}
		else
		{
			/*
			 * Recurse for anything else.  Note the recursive
			 * call will stash the objects so created into our
			 * event trigger context.
			 */
			ProcessUtility((Node*)stmt,
						   queryString,
						   PROCESS_UTILITY_SUBCOMMAND,
						   params,
						   None_Receiver,
						   NULL);
		}

		/* Need CCI between commands */
		if (lnext(l) != NULL)
			CommandCounterIncrement();
	}

	heap_close(ag_label_desc, RowExclusiveLock);
}

/*
 * RemoveLabels
 *		Implements DROP VLABEL, DROP ELABEL
 *		Remove catalog tuple from ag_label
 */
void
RemoveLabels(DropStmt *drop)
{
	ListCell *cell;
	ListCell *child;
	List	 *children;
	List	 *relations = NIL;

	foreach(cell, drop->objects)
	{
		RangeVar   *lab = makeRangeVarFromNameList((List *) lfirst(cell));
		Oid			labId;
		Oid			relId;
		List	   *namelist;

		if (lab->schemaname != NULL
			&& strcmp(lab->schemaname, AG_GRAPH) != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_SCHEMA_NAME),
					 errmsg("Graph label \"%s\" must be in the graph schema, skipping",
						 lab->relname)));
		}

		labId = get_labname_labid(lab->relname);
		relId = get_labid_relid(labId);

		if (!OidIsValid(labId))
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("Label \"%s\" does not exist", lab->relname)));
		}

		/* setup list to drop table */
		namelist = list_make2(makeString(AG_GRAPH),
							  makeString(get_rel_name(relId)));
		relations = lappend(relations, namelist);

		/* remove dependancy*/
		deleteDependencyRecordsForClass(RelationRelationId, relId,
										LabelRelationId, DEPENDENCY_INTERNAL);

		/* remove ag_label */
		DeleteLabelTuple(labId);

		/* delete childrens ag_label */
		/* now it is using pg_inherit, NOT ag_inherit */
		children = find_inheritance_children(InheritsLabelId, labId, NoLock);

		foreach(child, children)
		{
			Oid	childoid = lfirst_oid(child);
			DeleteLabelTuple(childoid);
		}

		/* delete ag_inherit */
		RelationRemoveInheritance(InheritsLabelId, labId);
		CommandCounterIncrement();
	}

	/* replace the list from label to relations */
	drop->removeType = OBJECT_TABLE;
	drop->objects = relations;

	RemoveRelations(drop);
}
