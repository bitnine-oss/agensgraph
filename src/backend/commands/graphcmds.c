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
#include "catalog/ag_label_property.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/toasting.h"
#include "commands/event_trigger.h"
#include "commands/graphcmds.h"
#include "commands/schemacmds.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "executor/spi.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "parser/parse_utilcmd.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "catalog/pg_inherits.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "access/relation.h"
#include "catalog/pg_attrdef.h"
#include "catalog/heap.h"
#include "utils/fmgroids.h"

static ObjectAddress DefineLabel(CreateStmt *stmt, char labkind,
								 const char *queryString, bool is_fixed_id,
								 int32 fixed_id, List *promoted_props);
static char *extractPromotedSourceKey(Node *raw_expr);
static void CheckPromotedColumnCollation(Oid relid, AttrNumber attnum,
										 const char *colname);
static void CheckPromotedPropertyCoherence(Oid relid, const char *propname,
										   const char *colname);
static void recordPromotedProperties(Oid laboid, Oid relid,
									 List *promoted_props);
static void GetSuperOids(List *supers, char labkind, List **supOids);
static void AgInheritanceDependancy(Oid laboid, List *supers);
static void SetMaxStatisticsTarget(Oid laboid);

static bool IsLabel(const char *label_name, Oid namespaceId);
static void SimpleProcessUtility(Node *node, const char *queryString,
								 int stmt_location, int stmt_len);
static void ReplaceLabelDefaultExpression(RenameStmt *stmt);

/* See ProcessUtilitySlow() case T_CreateSchemaStmt */
void
CreateGraphCommand(CreateGraphStmt *stmt, const char *queryString,
				   int stmt_location, int stmt_len)
{
	CreateSeqStmt *createSeqStmt;
	CreateLabelStmt *createVLabelStmt,
			   *createELabelStmt;

	if (stmt->kind & CGSK_SCHEMA)
	{
		Oid			graphid;

		graphid = GraphCreate(stmt, queryString, stmt_location, stmt_len);
		if (!OidIsValid(graphid))
		{
			/* If it already exists, it does not return the correct Oid. */
			return;
		}
		CommandCounterIncrement();
	}

	if (stmt->kind & CGSK_SEQUENCE)
	{
		/* Create ag_label_seq */
		createSeqStmt = makeDefaultCreateAGLabelSeqStmt(stmt->graphname,
														stmt_location);
		SimpleProcessUtility((Node *) createSeqStmt, queryString, stmt_location,
							 stmt_len);
		CommandCounterIncrement();
	}

	if (stmt->kind & CGSK_VLABEL)
	{
		/* Create ag_vertex table */
		createVLabelStmt = makeDefaultCreateAGLabelStmt(stmt->graphname,
														LABEL_VERTEX, stmt_location);
		createVLabelStmt->only_base = (stmt->kind == CGSK_VLABEL);
		SimpleProcessUtility((Node *) createVLabelStmt, queryString, stmt_location,
							 stmt_len);
		CommandCounterIncrement();
	}

	if (stmt->kind & CGSK_ELABEL)
	{
		/* Create ag_edge table */
		createELabelStmt = makeDefaultCreateAGLabelStmt(stmt->graphname,
														LABEL_EDGE, stmt_location);
		createELabelStmt->only_base = (stmt->kind == CGSK_ELABEL);
		SimpleProcessUtility((Node *) createELabelStmt, queryString, stmt_location,
							 stmt_len);
		CommandCounterIncrement();
	}

	if (stmt->kind == CGSK_ALL &&
		(graph_path == NULL || strcmp(graph_path, "") == 0))
	{
		SetConfigOption("graph_path", stmt->graphname,
						PGC_USERSET, PGC_S_SESSION);
	}
}

void
RemoveGraphById(Oid graphid)
{
	Relation	ag_graph_desc;
	HeapTuple	tup;
	Form_ag_graph graphtup;

	ag_graph_desc = table_open(GraphRelationId, RowExclusiveLock);

	tup = SearchSysCache1(GRAPHOID, ObjectIdGetDatum(graphid));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for graph %u", graphid);

	graphtup = (Form_ag_graph) GETSTRUCT(tup);

	if (graph_path != NULL &&
		strcmp(NameStr(graphtup->graphname), graph_path) == 0)
		SetConfigOption("graph_path", NULL, PGC_USERSET, PGC_S_SESSION);

	simple_heap_delete(ag_graph_desc, &tup->t_self);

	ReleaseSysCache(tup);

	table_close(ag_graph_desc, RowExclusiveLock);
}

ObjectAddress
RenameGraph(const char *oldname, const char *newname)
{
	Oid			graphOid;
	HeapTuple	tup;
	Relation	rel;
	ObjectAddress address;
	Form_ag_graph form_ag_graph;

	rel = table_open(GraphRelationId, RowExclusiveLock);

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

	form_ag_graph = (Form_ag_graph) GETSTRUCT(tup);
	graphOid = form_ag_graph->oid;

	/* Skip privilege and error check. It was already done in RenameSchema() */

	/* rename */
	namestrcpy(&form_ag_graph->graphname, newname);
	CatalogTupleUpdate(rel, &tup->t_self, tup);

	InvokeObjectPostAlterHook(GraphRelationId, graphOid, 0);

	ObjectAddressSet(address, GraphRelationId, graphOid);

	table_close(rel, NoLock);
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
		Node	   *stmt = (Node *) lfirst(l);

		if (IsA(stmt, CreateStmt))
		{
			DefineLabel((CreateStmt *) stmt, labkind, queryString,
						labelStmt->only_base, labelStmt->fixed_id,
						labelStmt->promoted_props);
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

			ProcessUtility(wrapper, queryString, false,
						   PROCESS_UTILITY_SUBCOMMAND, params, NULL,
						   None_Receiver, NULL);
		}

		CommandCounterIncrement();
	}
}

/* creates a new graph label */
static ObjectAddress
DefineLabel(CreateStmt *stmt, char labkind, const char *queryString,
			bool is_fixed_id, int32 fixed_id, List *promoted_props)
{
	static const char *const validnsps[] = HEAP_RELOPT_NAMESPACES;
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
	 * Let NewRelationCreateToastTable decide if this one needs a secondary
	 * relation too.
	 */
	NewRelationCreateToastTable(reladdr.objectId, toast_options);

	/*
	 * Create Label
	 */

	/* current implementation does not get tablespace name; so */
	tablespaceId = GetDefaultTablespace(stmt->relation->relpersistence, false);

	laboid = label_create_with_catalog(stmt->relation, reladdr.objectId,
									   labkind, tablespaceId, is_fixed_id,
									   fixed_id);

	/* register any promoted typed properties in ag_label_property */
	recordPromotedProperties(laboid, reladdr.objectId, promoted_props);

	GetSuperOids(stmt->inhRelations, labkind, &inheritOids);
	AgInheritanceDependancy(laboid, inheritOids);

	/*
	 * Make a dependency link to force the table to be deleted if its graph
	 * label is.
	 */
	labaddr.classId = LabelRelationId;
	labaddr.objectId = laboid;
	labaddr.objectSubId = 0;
	recordDependencyOn(&reladdr, &labaddr, DEPENDENCY_INTERNAL);

	return labaddr;
}

/*
 * extractPromotedSourceKey
 *
 * A shorthand-form promoted property is generated from a single jsonb key:
 * its raw generation expression is (properties ->> 'key')::type.  Return that
 * key, or NULL when the expression is not of that form (e.g. a long-form
 * GENERATED ALWAYS AS (...) column derived from more than one key), in which
 * case the column is not resolvable from a single Cypher property key.
 */
static char *
extractPromotedSourceKey(Node *raw_expr)
{
	A_Expr	   *aexpr;
	ColumnRef  *bag;
	Node	   *bagnode;
	Node	   *keynode;
	A_Const    *key;

	/* peel off the cast to the column's declared type */
	while (raw_expr != NULL && IsA(raw_expr, TypeCast))
		raw_expr = ((TypeCast *) raw_expr)->arg;

	if (raw_expr == NULL)
		return NULL;

	/*
	 * A property whose kind the column's type corresponds to is taken by name
	 * and kind; one with no such correspondence is taken by name alone.  Both
	 * spellings name the property the same way.
	 */
	if (IsA(raw_expr, FuncCall))
	{
		FuncCall   *fc = (FuncCall *) raw_expr;

		if (list_length(fc->funcname) != 1 ||
			strcmp(strVal(llast(fc->funcname)), "ag_property_text") != 0 ||
			list_length(fc->args) != 3)
			return NULL;

		bagnode = linitial(fc->args);
		keynode = lsecond(fc->args);
	}
	else if (IsA(raw_expr, A_Expr))
	{
		aexpr = (A_Expr *) raw_expr;
		if (aexpr->kind != AEXPR_OP ||
			list_length(aexpr->name) != 1 ||
			strcmp(strVal(linitial(aexpr->name)), "->>") != 0)
			return NULL;

		bagnode = aexpr->lexpr;
		keynode = aexpr->rexpr;
	}
	else
		return NULL;

	if (bagnode == NULL || !IsA(bagnode, ColumnRef) || keynode == NULL)
		return NULL;

	/* the extraction must be from the label's own "properties" bag */
	bag = (ColumnRef *) bagnode;
	if (list_length(bag->fields) != 1 ||
		!IsA(linitial(bag->fields), String) ||
		strcmp(strVal(linitial(bag->fields)), AG_ELEM_PROP_MAP) != 0)
		return NULL;

	/*
	 * The key is a string constant.  A dumped-then-reparsed generation
	 * expression spells it as 'key'::text, so peel any cast first.
	 */
	while (keynode != NULL && IsA(keynode, TypeCast))
		keynode = ((TypeCast *) keynode)->arg;
	if (keynode == NULL || !IsA(keynode, A_Const))
		return NULL;

	key = (A_Const *) keynode;
	if (key->isnull || !IsA(&key->val, String))
		return NULL;

	return strVal(&key->val);
}

/*
 * CheckPromotedColumnCollation
 *
 * A promoted column answers for a property, and reading that property has to
 * mean the same thing whether the column answers or the property map does.
 * The map compares its strings with the database default collation, so a column
 * collated any other way orders and compares the same values differently -- and
 * which of the two answers is a decision about how fast the read is, not about
 * what it returns.
 *
 * A collation can arrive without anyone naming one here, through a domain that
 * carries it, so ask the column what it ended up with rather than what was
 * written.
 */
static void
CheckPromotedColumnCollation(Oid relid, AttrNumber attnum, const char *colname)
{
	Oid			typid;
	int32		typmod;
	Oid			collid;

	get_atttypetypmodcoll(relid, attnum, &typid, &typmod, &collid);

	if (!OidIsValid(collid) || collid == DEFAULT_COLLATION_OID)
		return;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("a promoted property cannot have a collation of its own"),
			 errdetail("Column \"%s\" is collated \"%s\", while a property map compares strings with the database default.",
					   colname, get_collation_name(collid)),
			 errhint("Leave the collation unspecified, or use a type that does not carry one.")));
}

/*
 * CheckPromotedPropertyCoherence
 *
 * A property is read by name, and the name has to lead to one column all the
 * way down a label's ancestry -- otherwise the same read means different things
 * depending on which label it goes through.
 *
 * Two ways that can be broken, and both are silent.  A label can promote a key
 * an ancestor already promotes, to a column of its own: the key then answers
 * from one column through the child and another through the parent, with
 * nothing saying the two disagree.  Or it can promote a different key to a
 * column name the ancestor already uses: inheritance merges those into one
 * column, so both keys read the same value and one of them is simply wrong.
 *
 * Adding a property to an existing label already refuses the first of these.
 * Declaring a label that inherits did not refuse either.
 */
static void
CheckPromotedPropertyCoherence(Oid relid, const char *propname,
							   const char *colname)
{
	List	   *ancestors = find_all_ancestors(relid, AccessShareLock);
	ListCell   *la;

	foreach(la, ancestors)
	{
		Oid			ancestor = lfirst_oid(la);
		List	   *props;
		ListCell   *lp;

		if (ancestor == relid)
			continue;

		props = get_label_promoted_properties(ancestor);
		foreach(lp, props)
		{
			PromotedPropInfo *ap = (PromotedPropInfo *) lfirst(lp);
			char	   *anccol = get_attname(ancestor, ap->attnum, true);

			if (anccol == NULL)
				continue;

			if (strcmp(ap->propname, propname) == 0 &&
				strcmp(anccol, colname) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("property \"%s\" is already promoted on graph label \"%s\"",
								propname, get_rel_name(ancestor)),
						 errdetail("It answers from column \"%s\" there and would answer from column \"%s\" here.",
								   anccol, colname),
						 errhint("Promote it to the same column name, or leave it to the label that has it.")));

			if (strcmp(ap->propname, propname) != 0 &&
				strcmp(anccol, colname) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column \"%s\" already promotes property \"%s\" on graph label \"%s\"",
								colname, ap->propname, get_rel_name(ancestor)),
						 errdetail("An inherited column is one column, so both properties would read the same value."),
						 errhint("Promote this property to a column of another name.")));
		}
	}

	list_free(ancestors);
}

/*
 * recordPromotedProperties
 *
 * Register each shorthand-form promoted property of a freshly created label in
 * ag_label_property, mapping its source key to the storage column's attnum.
 */
static void
recordPromotedProperties(Oid laboid, Oid relid, List *promoted_props)
{
	ListCell   *lc;

	foreach(lc, promoted_props)
	{
		ColumnDef  *col = (ColumnDef *) lfirst(lc);
		ListCell   *lcon;

		foreach(lcon, col->constraints)
		{
			Constraint *con = (Constraint *) lfirst(lcon);
			char	   *srckey;
			AttrNumber	attnum;

			if (con->contype != CONSTR_GENERATED)
				continue;

			srckey = extractPromotedSourceKey(con->raw_expr);
			if (srckey == NULL)
				continue;		/* long-form column: not key-resolvable */

			attnum = get_attnum(relid, col->colname);
			if (attnum == InvalidAttrNumber)
				continue;		/* should not happen */

			CheckPromotedColumnCollation(relid, attnum, col->colname);
			CheckPromotedPropertyCoherence(relid, srckey, col->colname);

			InsertAgLabelProperty(laboid, srckey, (int16) attnum,
								  PROMOTED_SEMANTICS_LEGACY);
		}
	}
}

/*
 * State carried from BeginAlterLabelProperties() to FinishAlterLabelProperties()
 * across the AlterTable() that adds/drops the columns.
 */
struct AlterLabelPropertyState
{
	List	   *addDefs;		/* copied ColumnDefs of the added properties */
	List	   *dropKeys;		/* property keys of the dropped columns (char *) */
};

/*
 * BeginAlterLabelProperties
 *
 * Before AlterTable() adds/drops the columns, capture what the catalog sync
 * will need afterwards.  For an added column, copy the ColumnDef now:
 * ATExecAddColumn re-transforms it in place (moving the generation expression
 * out of the CONSTR_GENERATED constraint), which would leave nothing for
 * recordPromotedProperties to read from.  For a dropped column, capture the
 * property key now: the column --
 * and its ag_label_property row's attnum -- is gone once AlterTable() runs, and
 * the key is looked up by attnum because the explicit-key form records a key
 * that differs from the column name.  Returns NULL when there is nothing to
 * sync (e.g. ALTER VLABEL ... SET STORAGE), so plain label alters are untouched.
 */
AlterLabelPropertyState *
BeginAlterLabelProperties(Oid relid, List *cmds)
{
	Oid			laboid = get_relid_laboid(relid);
	List	   *addDefs = NIL;
	List	   *dropKeys = NIL;
	ListCell   *lc;
	AlterLabelPropertyState *state;

	if (!OidIsValid(laboid))
		return NULL;

	foreach(lc, cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

		if (cmd->subtype == AT_AddColumn && cmd->def != NULL)
			addDefs = lappend(addDefs, copyObject(cmd->def));
		else if (cmd->subtype == AT_DropColumn && cmd->name != NULL)
		{
			AttrNumber	attnum;
			char	   *propname;

			/*
			 * Refuse to drop a fixed structural column of the label -- these
			 * define the vertex/edge shape and are not user-droppable.  Any
			 * other column is droppable: a recorded promoted property (whose
			 * ag_label_property row we then forget below), a long-form/derived
			 * generated column (never recorded, so nothing to forget), or a
			 * plain column.  Keying the refusal on structural name rather than
			 * "has no ag_label_property row" is what lets a derived column drop.
			 */
			if (strcmp(cmd->name, AG_ELEM_LOCAL_ID) == 0 ||
				strcmp(cmd->name, AG_ELEM_PROP_MAP) == 0 ||
				strcmp(cmd->name, AG_START_ID) == 0 ||
				strcmp(cmd->name, AG_END_ID) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot drop column \"%s\" of graph label \"%s\"",
								cmd->name, get_rel_name(relid)),
						 errhint("It is a structural column of the graph label.")));

			attnum = get_attnum(relid, cmd->name);

			/* a nonexistent column: let AlterTable report it */
			if (attnum == InvalidAttrNumber)
				continue;

			/* forget the promotion mapping only if the column has one */
			propname = get_label_property_name_by_attnum(laboid, attnum);
			if (propname != NULL)
				dropKeys = lappend(dropKeys, propname);
		}
		else if (cmd->subtype == AT_DropExpression && cmd->name != NULL)
		{
			AttrNumber	attnum;
			char	   *propname;

			/*
			 * The column survives but stops being generated, so it no longer
			 * derives from the property it was promoted from and must not go on
			 * answering reads of it.  Forget the mapping; the property is then
			 * read from the bag again, which still holds it.
			 */
			attnum = get_attnum(relid, cmd->name);
			if (attnum == InvalidAttrNumber)
				continue;

			propname = get_label_property_name_by_attnum(laboid, attnum);
			if (propname != NULL)
				dropKeys = lappend(dropKeys, propname);
		}
	}

	/*
	 * Reject an ADD whose promotion source key is already promoted on the label
	 * (unless the same statement drops it), or is promoted twice in one
	 * statement.  ag_label_property is unique on (laboid, propname), so such a
	 * collision would otherwise surface only in FinishAlterLabelProperties --
	 * after a full table rewrite has already run -- as an opaque unique_violation.
	 * Catch it here, before any rewrite, with an actionable message.
	 */
	{
		ListCell   *la;
		List	   *seen = NIL;		/* source keys added by this statement */

		foreach(la, addDefs)
		{
			ColumnDef  *col = (ColumnDef *) lfirst(la);
			ListCell   *lc2;
			char	   *key = NULL;
			bool		dropped = false;

			/*
			 * A column whose NAME already exists is reported by ALTER TABLE's own
			 * duplicate-column guard (which also runs before any rewrite), and its
			 * "column already exists" is the more direct message.  Leave that case
			 * to it; only pre-check source-key collisions for genuinely new columns.
			 */
			if (get_attnum(relid, col->colname) != InvalidAttrNumber)
				continue;

			foreach(lc2, col->constraints)
			{
				Constraint *con = (Constraint *) lfirst(lc2);

				if (con->contype == CONSTR_GENERATED)
				{
					key = extractPromotedSourceKey(con->raw_expr);
					break;
				}
			}
			if (key == NULL)		/* long-form/derived or non-generated */
				continue;

			foreach(lc2, seen)
				if (strcmp((char *) lfirst(lc2), key) == 0)
					ereport(ERROR,
							(errcode(ERRCODE_DUPLICATE_COLUMN),
							 errmsg("property \"%s\" is promoted more than once in the same statement",
									key)));

			foreach(lc2, dropKeys)
				if (strcmp((char *) lfirst(lc2), key) == 0)
				{
					dropped = true;
					break;
				}

			if (!dropped && get_label_property_column(relid, key, NULL, NULL))
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("property \"%s\" is already promoted on graph label \"%s\"",
								key, get_rel_name(relid)),
						 errhint("Drop the existing promoted column first, or promote the property to a different column name.")));

			seen = lappend(seen, key);
		}
	}

	if (addDefs == NIL && dropKeys == NIL)
		return NULL;

	state = (AlterLabelPropertyState *) palloc0(sizeof(*state));
	state->addDefs = addDefs;
	state->dropKeys = dropKeys;
	return state;
}

/*
 * FinishAlterLabelProperties
 *
 * After AlterTable() has added/dropped the columns, sync ag_label_property:
 * record each added shorthand property (reusing recordPromotedProperties, which
 * resolves the now-existing column's attnum and skips a long-form/derived column
 * that maps to no single key), and forget each dropped property's key.
 */
void
FinishAlterLabelProperties(Oid relid, AlterLabelPropertyState *state)
{
	Oid			laboid;
	ListCell   *lc;

	if (state == NULL)
		return;

	laboid = get_relid_laboid(relid);
	if (!OidIsValid(laboid))
		return;

	/*
	 * Delete dropped keys BEFORE recording added ones so a single statement
	 * that drops and re-adds a column mapping to the same source key
	 * (ALTER ... DROP c, ADD c) does not collide on ag_label_property's unique
	 * (laboid, propname) index.
	 */
	foreach(lc, state->dropKeys)
		DeleteAgLabelProperty(laboid, (char *) lfirst(lc));

	recordPromotedProperties(laboid, relid, state->addDefs);
}

/*
 * Raise the statistics target on an edge label's start/end columns so the
 * planner has good cardinality for traversal joins on a skewed (power-law)
 * degree distribution.
 *
 * NOTE: this used to be 10000 (the maximum).  eqjoinsel is O(MCV^2) over the
 * most-common-value lists, so a 10000-entry MCV makes selectivity estimation
 * cost ~290ms PER edge join on a large edge label -- i.e. multi-hop pattern
 * queries spent hundreds of ms in the PLANNER (a 2-hop query: ~3ms exec but
 * ~290ms plan).  1000 still captures the high-degree hubs that matter for
 * traversal cardinality, at ~1/100th of the planning cost (~3ms/join).
 */
static void
SetMaxStatisticsTarget(Oid laboid)
{
	Relation	attrelation;
	HeapTuple	tuple,
				newtuple;
	int			maxtarget = 1000;
	Datum		repl_val[Natts_pg_attribute];
	bool		repl_null[Natts_pg_attribute];
	bool		repl_repl[Natts_pg_attribute];

	attrelation = table_open(AttributeRelationId, RowExclusiveLock);

	/*
	 * attstattarget is a nullable column, so set it via heap_modify_tuple()
	 * instead of the tuple's Form struct.  Both columns get the same target,
	 * so build the replacement arrays once.
	 */
	memset(repl_null, false, sizeof(repl_null));
	memset(repl_repl, false, sizeof(repl_repl));
	repl_val[Anum_pg_attribute_attstattarget - 1] = Int16GetDatum(maxtarget);
	repl_repl[Anum_pg_attribute_attstattarget - 1] = true;

	/* start column */

	tuple = SearchSysCacheCopyAttName(laboid, AG_START_ID);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("edge label must have start column")));

	newtuple = heap_modify_tuple(tuple, RelationGetDescr(attrelation),
								 repl_val, repl_null, repl_repl);
	CatalogTupleUpdate(attrelation, &tuple->t_self, newtuple);

	heap_freetuple(newtuple);
	heap_freetuple(tuple);

	/* end column */

	tuple = SearchSysCacheCopyAttName(laboid, AG_END_ID);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_COLUMN),
				 errmsg("edge label must have end column")));

	newtuple = heap_modify_tuple(tuple, RelationGetDescr(attrelation),
								 repl_val, repl_null, repl_repl);
	CatalogTupleUpdate(attrelation, &tuple->t_self, newtuple);

	heap_freetuple(newtuple);
	heap_freetuple(tuple);

	table_close(attrelation, RowExclusiveLock);
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

		parent_laboid = labtup->oid;
		ReleaseSysCache(tuple);

		parentOids = lappend_oid(parentOids, parent_laboid);
	}

	*supOids = parentOids;
}

/* This function mimics StoreCatalogInheritance() */
static void
AgInheritanceDependancy(Oid laboid, List *supers)
{
	ListCell   *entry;

	if (supers == NIL)
		return;

	foreach(entry, supers)
	{
		Oid			parentOid = lfirst_oid(entry);
		ObjectAddress childobject;
		ObjectAddress parentobject;

		childobject.classId = LabelRelationId;
		childobject.objectId = laboid;
		childobject.objectSubId = 0;
		parentobject.classId = LabelRelationId;
		parentobject.objectId = parentOid;
		parentobject.objectSubId = 0;
		recordDependencyOn(&childobject, &parentobject, DEPENDENCY_NORMAL);
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
	Form_ag_label form_ag_label;

	/* schemaname is NULL always */
	stmt->relation->schemaname = get_graph_path(false);

	rel = table_open(LabelRelationId, RowExclusiveLock);

	tup = SearchSysCacheCopy2(LABELNAMEGRAPH,
							  CStringGetDatum(stmt->relation->relname),
							  ObjectIdGetDatum(graphid));
	if (!HeapTupleIsValid(tup))
	{
		table_close(rel, NoLock);

		ereport(NOTICE,
				(errmsg("label \"%s\" does not exist, skipping",
						stmt->relation->relname)));
		return InvalidObjectAddress;
	}
	form_ag_label = (Form_ag_label) GETSTRUCT(tup);
	laboid = form_ag_label->oid;

	CheckLabelType(stmt->renameType, laboid, "RENAME");

	/* renamimg label and table should be processed in one lock */
	RenameRelation(stmt);

	/* Skip privilege and error check. It was already done in RenameRelation */

	/* rename */
	namestrcpy(&(((Form_ag_label) GETSTRUCT(tup))->labname), stmt->newname);
	CatalogTupleUpdate(rel, &tup->t_self, tup);

	InvokeObjectPostAlterHook(LabelRelationId, laboid, 0);

	ObjectAddressSet(address, LabelRelationId, laboid);

	table_close(rel, NoLock);
	heap_freetuple(tup);

	ReplaceLabelDefaultExpression(stmt);
	return address;
}

/*
 * CheckLabelSqlReshape
 *
 * Refuse an ALTER TABLE that would reshape a graph label.
 *
 * A label is a table, so ALTER TABLE reaches it -- but a label's columns are
 * part of what makes it a label, and the graph catalog describes them.  The
 * columns every vertex or edge is made of are assumed to be there, and each
 * promoted property is recorded against the column that answers for it.  Adding
 * a column the graph has no account of, or taking one away, or detaching a
 * column from the expression that derived it, leaves a label the graph no longer
 * describes the same way -- and one that cannot be dumped for a binary upgrade,
 * because a column ALTER TABLE put there cannot be put back at the same attnum.
 *
 * ALTER VLABEL / ALTER ELABEL do these things and keep the catalog in step, so
 * point at them.  Restoring a dump has to be able to reshape a label, and so
 * does repairing one, which is what enable_graph_ddl is for.  Asking for it says
 * the reshaping is meant, not that the asker is entitled to it: whoever may alter
 * the label is who decides what happens to it, as for any other relation.
 */
void
CheckLabelSqlReshape(Oid relid, bool recurse, List *cmds)
{
	ListCell   *lc;
	Oid			labelrelid = InvalidOid;

	/*
	 * ALTER TABLE reshapes more than the relation it names: unless ONLY was
	 * asked for it descends the inheritance tree, so a label that inherits from
	 * an ordinary table is reshaped by altering that table.  Judge the whole set
	 * the statement reaches, and name the label that would be reshaped rather
	 * than the relation that was addressed.
	 */
	if (OidIsValid(get_relid_laboid(relid)))
		labelrelid = relid;
	else if (recurse)
	{
		List	   *children = find_all_inheritors(relid, NoLock, NULL);
		ListCell   *cl;

		foreach(cl, children)
		{
			Oid			child = lfirst_oid(cl);

			if (OidIsValid(get_relid_laboid(child)))
			{
				labelrelid = child;
				break;
			}
		}
		list_free(children);
	}

	if (!OidIsValid(labelrelid))
		return;

	/*
	 * The columns every vertex and edge is made of are what a graph is read
	 * and written through, and no reshaping of a label is worth giving them
	 * up.  Reaching them is not a repair, so asking to reshape the label does
	 * not extend to them: one statement on the root of a graph would otherwise
	 * rewrite the value of a property out of every label at once, and say
	 * nothing about it.
	 */
	foreach(lc, cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

		if (cmd->subtype != AT_AlterColumnType)
			continue;

		if (cmd->name == NULL ||
			(strcmp(cmd->name, AG_ELEM_LOCAL_ID) != 0 &&
			 strcmp(cmd->name, AG_ELEM_PROP_MAP) != 0 &&
			 strcmp(cmd->name, AG_START_ID) != 0 &&
			 strcmp(cmd->name, AG_END_ID) != 0))
			continue;

		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot change the type of column \"%s\" of graph label \"%s\"",
						cmd->name, get_rel_name(labelrelid)),
				 errdetail("Every vertex and edge is made of that column.")));
	}

	if (enable_graph_ddl)
		return;

	foreach(lc, cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);
		const char *what;
		const char *hint;
		const char *label_ddl_hint =
			"Use ALTER VLABEL or ALTER ELABEL instead, which keeps the graph catalog in step.";

		switch (cmd->subtype)
		{
			case AT_AddColumn:
				what = "add a column to";
				hint = label_ddl_hint;
				break;
			case AT_DropColumn:
				what = "drop a column of";
				hint = label_ddl_hint;
				break;
			case AT_AlterColumnType:
				what = "change the type of a column of";
				hint = label_ddl_hint;
				break;
			case AT_DropExpression:
				what = "detach a column of";
				hint = "The column would stay, but stop deriving from the property it was promoted from.";
				break;
			case AT_SetExpression:
				what = "change what derives a column of";
				hint = label_ddl_hint;
				break;
			case AT_DropNotNull:
				what = "drop a not-null constraint of";
				hint = "The columns every vertex or edge is made of are relied on to be there.";
				break;
			case AT_DropConstraint:
				what = "drop a constraint of";
				hint = "Use DROP CONSTRAINT on the label instead.";
				break;
			case AT_AddInherit:
				what = "add a parent label to";
				hint = "A label's parents are fixed when it is created.";
				break;
			case AT_DropInherit:
				what = "remove a parent label from";
				hint = "A label's parents are fixed when it is created.";
				break;
			default:
				continue;
		}

		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot %s graph label \"%s\" with ALTER TABLE",
						what, get_rel_name(labelrelid)),
				 errhint("%s", hint)));
	}
}

/* check the given `type` and `laboid` matches */
void
CheckLabelType(ObjectType type, Oid laboid, const char *command)
{
	HeapTuple	tuple;
	Form_ag_label labtup;

	if (cypher_allow_unsafe_ddl)
		return;

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

static bool
IsLabel(const char *label_name, Oid namespaceId)
{
	Oid			graphid;
	HeapTuple	nsptuple;
	Form_pg_namespace nspdata;

	nsptuple = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(namespaceId));
	if (!HeapTupleIsValid(nsptuple))
		elog(ERROR, "cache lookup failed for label (OID=%u)", namespaceId);

	nspdata = (Form_pg_namespace) GETSTRUCT(nsptuple);
	graphid = get_graphname_oid(NameStr(nspdata->nspname));
	ReleaseSysCache(nsptuple);

	return OidIsValid(get_labname_laboid(label_name, graphid));
}

bool
RelationIsLabel(Relation rel)
{
	return IsLabel(RelationGetRelationName(rel), RelationGetNamespace(rel));
}

bool
RangeVarIsLabel(RangeVar *rel)
{
	return IsLabel(rel->relname, RangeVarGetCreationNamespace(rel));
}

void
CreateConstraintCommand(CreateConstraintStmt *constraintStmt,
						const char *queryString, int stmt_location,
						int stmt_len, ParamListInfo params)
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

	ProcessUtility(wrapper, queryString, false, PROCESS_UTILITY_SUBCOMMAND,
				   params, NULL, None_Receiver, NULL);

	CommandCounterIncrement();

	free_parsestate(pstate);
}

void
DropConstraintCommand(DropConstraintStmt *constraintStmt,
					  const char *queryString, int stmt_location,
					  int stmt_len, ParamListInfo params)
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

	ProcessUtility(wrapper, queryString, false, PROCESS_UTILITY_SUBCOMMAND,
				   params, NULL, None_Receiver, NULL);

	CommandCounterIncrement();

	free_parsestate(pstate);
}

Oid
DisableIndexCommand(DisableIndexStmt *disableStmt)
{
	Oid			relid;
	RangeVar   *relation = disableStmt->relation;

	relid = RangeVarGetRelidExtended(relation, ShareLock, 0,
									 RangeVarCallbackMaintainsTable, NULL);

	if (!RangeVarIsLabel(relation))
		elog(ERROR, "invalid DISABLE INDEX");

	if (!DisableIndexLabel(relid))
		ereport(NOTICE,
				(errmsg("label \"%s\" has no enabled index",
						relation->relname)));

	return relid;
}

bool
isEmptyLabel(char *label_name)
{
	int			ret;
	StringInfoData sql;
	bool		result = true;

	initStringInfo(&sql);

	appendStringInfo(&sql, "SELECT 1 FROM %s.%s LIMIT 1",
					 quote_identifier(get_graph_path(false)),
					 quote_identifier(label_name));

	ret = SPI_connect();
	if (ret != SPI_OK_CONNECT)
		elog(ERROR, "isEmptyLabel: SPI_connect returned %d", ret);

	ret = SPI_execute(sql.data, true, 1);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "isEmptyLabel: SPI_execute returned %d: %s",
			 ret, sql.data);

	if (SPI_processed > 0)
		result = false;

	ret = SPI_finish();
	if (ret != SPI_OK_FINISH)
		elog(ERROR, "isEmptyLabel: SPI_finish returned %d", ret);

	return result;
}

void
deleteRelatedEdges(const char *vlab)
{
	Labid		vlabid;
	Oid			graphoid;
	Oid			agedge;
	ListCell   *lc;
	List	   *edges = NIL;
	bool		temp_enable_graph_dml = enable_graph_dml;

	graphoid = get_graph_path_oid();
	vlabid = get_labname_labid(vlab, graphoid);

	/* get all edge's relid */
	agedge = get_laboid_relid(get_labname_laboid(AG_EDGE, graphoid));
	edges = list_make1_oid(agedge);
	edges = list_concat(edges, find_all_inheritors(agedge, NoLock, NULL));

	foreach(lc, edges)
	{
		Oid			edgeoid = lfirst_oid(lc);
		Relation	rel;
		int			ret;
		StringInfoData sql;

		/* Hold the ShareLock to prevent DML on the edge label */
		rel = try_relation_open(edgeoid, ShareLock);
		if (rel == NULL)
			continue;			/* not exist */

		initStringInfo(&sql);

		appendStringInfo(&sql, "DELETE FROM ONLY %s.%s WHERE "
						 "(start >= graphid(%u,0) AND"
						 " start <= graphid(%u," UINT64_FORMAT ")) OR "
						 "(\"end\" >= graphid(%u,0) AND"
						 " \"end\" <= graphid(%u," UINT64_FORMAT "))",
						 quote_identifier(get_graph_path(false)),
						 quote_identifier(RelationGetRelationName(rel)),
						 vlabid, vlabid, GRAPHID_LOCID_MAX,
						 vlabid, vlabid, GRAPHID_LOCID_MAX);

		ret = SPI_connect();
		if (ret != SPI_OK_CONNECT)
			elog(ERROR, "deleteRelatedEdges: SPI_connect returned %d", ret);

		enable_graph_dml = true;
		ret = SPI_execute(sql.data, false, 0);
		if (ret != SPI_OK_DELETE)
			elog(ERROR, "deleteRelatedEdges: SPI_execute returned %d: %s",
				 ret, sql.data);
		enable_graph_dml = temp_enable_graph_dml;

		ret = SPI_finish();
		if (ret != SPI_OK_FINISH)
			elog(ERROR, "deleteRelatedEdges: SPI_finish returned %d", ret);

		relation_close(rel, ShareLock);
	}
}

static void
SimpleProcessUtility(Node *node, const char *queryString, int stmt_location,
					 int stmt_len)
{
	PlannedStmt *wrapper;

	wrapper = makeNode(PlannedStmt);
	wrapper->commandType = CMD_UTILITY;
	wrapper->canSetTag = false;
	wrapper->utilityStmt = node;
	wrapper->stmt_location = stmt_location;
	wrapper->stmt_len = stmt_len;

	ProcessUtility(wrapper, queryString, false, PROCESS_UTILITY_SUBCOMMAND,
				   NULL, NULL, None_Receiver, NULL);
}

/*
 * Update default expression of id column
 * See transformLabelIdDefinition()
 */
static void
ReplaceLabelDefaultExpression(RenameStmt *stmt)
{
	AttrNumber	attnum;
	HeapTuple	tup;
	Form_ag_label form_ag_label;
	Relation	rel;
	FuncExpr   *graphIdFuncExpr,
			   *graphLabidFuncExpr;
	char		buf[NAMEDATALEN] = {0};
	Const	   *n;
	Oid			graphid = get_graph_path_oid();

	tup = SearchSysCacheCopy2(LABELNAMEGRAPH,
							  CStringGetDatum(stmt->relation->relname),
							  ObjectIdGetDatum(graphid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for graph oid %u of label name %s",
			 graphid, stmt->relation->relname);

	form_ag_label = (Form_ag_label) GETSTRUCT(tup);
	attnum = get_attnum(form_ag_label->relid, AG_ELEM_LOCAL_ID);
	rel = relation_open(form_ag_label->relid, AccessExclusiveLock);

	graphIdFuncExpr = stringToNode(rel->rd_att->constr->defval->adbin);

	/* First argument of graphid() is the graph_labid() */
	graphLabidFuncExpr = (FuncExpr *) linitial(graphIdFuncExpr->args);

	snprintf(buf, sizeof(buf), "%s.%s", stmt->relation->schemaname,
			 stmt->newname);

	/* Create a replacement form for the graph_labid argument */
	n = makeNode(Const);
	n->consttype = CSTRINGOID;
	n->consttypmod = -1;
	n->constcollid = InvalidOid;
	n->constlen = -2;
	n->constbyval = false;
	n->location = -1;
	n->constvalue = CStringGetDatum(buf);

	/* Rewrite the function expression */
	graphLabidFuncExpr->args = list_delete_first(graphLabidFuncExpr->args);
	graphLabidFuncExpr->args = lappend(graphLabidFuncExpr->args, n);

	/* Update */
	RemoveAttrDefault(rel->rd_id, attnum, DROP_RESTRICT, false, true);
	StoreAttrDefault(rel, attnum, (Node *) graphIdFuncExpr, true);

	relation_close(rel, AccessExclusiveLock);
	heap_freetuple(tup);
}
