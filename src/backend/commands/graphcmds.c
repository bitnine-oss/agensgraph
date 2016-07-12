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

#include "access/heapam.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/ag_inherits.h"
#include "catalog/ag_label.h"
#include "catalog/catalog.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits_fn.h"
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
#include "utils/lsyscache.h"
#include "utils/relcache.h"

/* creates a new graph label (See ProcessUtilitySlow() case T_CreateStmt) */
void
DefineLabel(CreateLabelStmt *labelStmt, const char *queryString,
			ParamListInfo params, ObjectAddress secondaryObject)
{
	char		labelKind;
	Relation	ag_label_desc;
	List	   *stmts;
	ListCell   *l;

	if (labelStmt->labelKind == LABEL_VERTEX)
		labelKind = LABEL_KIND_VERTEX;
	else
		labelKind = LABEL_KIND_EDGE;

	ag_label_desc = heap_open(LabelRelationId, RowExclusiveLock);

	stmts = transformCreateLabelStmt(labelStmt, queryString);
	foreach(l, stmts)
	{
		Node *stmt = (Node *) lfirst(l);

		if (IsA(stmt, CreateStmt))
		{
			static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
			CreateStmt *createStmt = (CreateStmt *) stmt;
			ObjectAddress address;
			Datum		toast_options;
			Oid			tablespaceId;
			Oid			labid;
			Oid			relid;
			ListCell   *inhRel;
			List	   *parents = NIL;
			ObjectAddress myself;
			ObjectAddress referenced;

			/* Create the table itself */
			address = DefineRelation(createStmt, RELKIND_RELATION, InvalidOid,
									 NULL);
			EventTriggerCollectSimpleCommand(address, secondaryObject, stmt);

			CommandCounterIncrement();

			/* parse and validate reloptions for the toast table */
			toast_options = transformRelOptions((Datum) 0, createStmt->options,
												"toast", validnsps, true,
												false);
			heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);

			/*
			 * Let NewRelationCreateToastTable decide if this
			 * one needs a secondary relation too.
			 */
			NewRelationCreateToastTable(address.objectId, toast_options);

			/*
			 * Create Label
			 */

			/* current implementation does not get tablespace name; so */
			tablespaceId =
					GetDefaultTablespace(createStmt->relation->relpersistence);

			labid = GetNewRelFileNode(tablespaceId, ag_label_desc,
									  createStmt->relation->relpersistence);
			relid = address.objectId;

			/* ag_label */
			InsertAgLabelTuple(labid, createStmt->relation->relname, labelKind,
							   relid, GetUserId());

			/* ag_inherit */
			foreach(inhRel, createStmt->inhRelations)
			{
				RangeVar   *parent = (RangeVar *) lfirst(inhRel);
				Oid			parent_labid;

				parent_labid = get_labname_labid(parent->relname);

				parents = lappend_oid(parents, parent_labid);
			}
			StoreCatalogInheritance(InheritsLabelId, labid, parents);

			/*
			 * Make a dependency link to force the table to be deleted if its
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
			 * Recurse for anything else.  Note the recursive call will stash
			 * the objects so created into our event trigger context.
			 */
			ProcessUtility(stmt, queryString, PROCESS_UTILITY_SUBCOMMAND,
						   params, None_Receiver, NULL);
		}

		CommandCounterIncrement();
	}

	heap_close(ag_label_desc, RowExclusiveLock);
}

/* Implements DROP VLABEL/ELABEL. Remove label from ag_label. */
void
RemoveLabels(Oid labid)
{
	/* delete ag_inherit */
	RelationRemoveInheritanceClass(InheritsLabelId, labid);

	/* remove ag_label */
	DeleteLabelTuple(labid);
}
