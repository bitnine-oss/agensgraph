/* -------------------------------------------------------------------------
 *
 * pgstat_xact.c
 *	  Transactional integration for the cumulative statistics system.
 *
 * Copyright (c) 2001-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_xact.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/table.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/ag_graphmeta.h"
#include "catalog/ag_graph_fn.h"
#include "catalog/indexing.h"
#include "pgstat.h"
#include "utils/memutils.h"
#include "utils/pgstat_internal.h"
#include "utils/rel.h"
#include "utils/syscache.h"


typedef struct PgStat_PendingDroppedStatsItem
{
	xl_xact_stats_item item;
	bool		is_create;
	dlist_node	node;
} PgStat_PendingDroppedStatsItem;


static void AtEOXact_PgStat_DroppedStats(PgStat_SubXactStatus *xact_state, bool isCommit);
static void AtEOSubXact_PgStat_DroppedStats(PgStat_SubXactStatus *xact_state,
											bool isCommit, int nestDepth);

static PgStat_SubXactStatus *pgStatXactStack = NULL;

/*
 * See above PgStat_SubXactStatus.
 * This struct is designed to store Edge insertion/deletion counts for an open
 * transaction. That counts will be merged into ag_graphmeta catalog during
 * the COMMIT process. We could not use TableXactStatus because each edges in
 * the same elabel can have different 'start' and 'end' vlabel. So another
 * struct AgStat_key is added and HTAB is used.
 */
typedef struct AgStat_SubXactStatus
{
	HTAB	   *htab;
	int			nest_level;		/* subtransaction nest level */
	struct AgStat_SubXactStatus *prev;	/* higher-level subxact if any */
} AgStat_SubXactStatus;

static AgStat_SubXactStatus *agStatXactStack = NULL;

/*
 * Called from access/transam/xact.c at top-level transaction commit/abort.
 */
void
AtEOXact_PgStat(bool isCommit, bool parallel)
{
	PgStat_SubXactStatus *xact_state;

	AtEOXact_PgStat_Database(isCommit, parallel);

	/* handle transactional stats information */
	xact_state = pgStatXactStack;
	if (xact_state != NULL)
	{
		Assert(xact_state->nest_level == 1);
		Assert(xact_state->prev == NULL);

		AtEOXact_PgStat_Relations(xact_state, isCommit);
		AtEOXact_PgStat_DroppedStats(xact_state, isCommit);
	}
	pgStatXactStack = NULL;

	/* Make sure any stats snapshot is thrown away */
	pgstat_clear_snapshot();
}

/*
 * When committing, drop stats for objects dropped in the transaction. When
 * aborting, drop stats for objects created in the transaction.
 */
static void
AtEOXact_PgStat_DroppedStats(PgStat_SubXactStatus *xact_state, bool isCommit)
{
	dlist_mutable_iter iter;
	int			not_freed_count = 0;

	if (xact_state->pending_drops_count == 0)
	{
		Assert(dlist_is_empty(&xact_state->pending_drops));
		return;
	}

	dlist_foreach_modify(iter, &xact_state->pending_drops)
	{
		PgStat_PendingDroppedStatsItem *pending =
		dlist_container(PgStat_PendingDroppedStatsItem, node, iter.cur);
		xl_xact_stats_item *it = &pending->item;

		if (isCommit && !pending->is_create)
		{
			/*
			 * Transaction that dropped an object committed. Drop the stats
			 * too.
			 */
			if (!pgstat_drop_entry(it->kind, it->dboid, it->objoid))
				not_freed_count++;
		}
		else if (!isCommit && pending->is_create)
		{
			/*
			 * Transaction that created an object aborted. Drop the stats
			 * associated with the object.
			 */
			if (!pgstat_drop_entry(it->kind, it->dboid, it->objoid))
				not_freed_count++;
		}

		dlist_delete(&pending->node);
		xact_state->pending_drops_count--;
		pfree(pending);
	}

	if (not_freed_count > 0)
		pgstat_request_entry_refs_gc();
}

/*
 * Called from access/transam/xact.c at subtransaction commit/abort.
 */
void
AtEOSubXact_PgStat(bool isCommit, int nestDepth)
{
	PgStat_SubXactStatus *xact_state;

	/* merge the sub-transaction's transactional stats into the parent */
	xact_state = pgStatXactStack;
	if (xact_state != NULL &&
		xact_state->nest_level >= nestDepth)
	{
		/* delink xact_state from stack immediately to simplify reuse case */
		pgStatXactStack = xact_state->prev;

		AtEOSubXact_PgStat_Relations(xact_state, isCommit, nestDepth);
		AtEOSubXact_PgStat_DroppedStats(xact_state, isCommit, nestDepth);

		pfree(xact_state);
	}
}

/*
 * Like AtEOXact_PgStat_DroppedStats(), but for subtransactions.
 */
static void
AtEOSubXact_PgStat_DroppedStats(PgStat_SubXactStatus *xact_state,
								bool isCommit, int nestDepth)
{
	PgStat_SubXactStatus *parent_xact_state;
	dlist_mutable_iter iter;
	int			not_freed_count = 0;

	if (xact_state->pending_drops_count == 0)
		return;

	parent_xact_state = pgstat_get_xact_stack_level(nestDepth - 1);

	dlist_foreach_modify(iter, &xact_state->pending_drops)
	{
		PgStat_PendingDroppedStatsItem *pending =
		dlist_container(PgStat_PendingDroppedStatsItem, node, iter.cur);
		xl_xact_stats_item *it = &pending->item;

		dlist_delete(&pending->node);
		xact_state->pending_drops_count--;

		if (!isCommit && pending->is_create)
		{
			/*
			 * Subtransaction creating a new stats object aborted. Drop the
			 * stats object.
			 */
			if (!pgstat_drop_entry(it->kind, it->dboid, it->objoid))
				not_freed_count++;
			pfree(pending);
		}
		else if (isCommit)
		{
			/*
			 * Subtransaction dropping a stats object committed. Can't yet
			 * remove the stats object, the surrounding transaction might
			 * still abort. Pass it on to the parent.
			 */
			dlist_push_tail(&parent_xact_state->pending_drops, &pending->node);
			parent_xact_state->pending_drops_count++;
		}
		else
		{
			pfree(pending);
		}
	}

	Assert(xact_state->pending_drops_count == 0);
	if (not_freed_count > 0)
		pgstat_request_entry_refs_gc();
}

/*
 * Save the transactional stats state at 2PC transaction prepare.
 */
void
AtPrepare_PgStat(void)
{
	PgStat_SubXactStatus *xact_state;

	xact_state = pgStatXactStack;
	if (xact_state != NULL)
	{
		Assert(xact_state->nest_level == 1);
		Assert(xact_state->prev == NULL);

		AtPrepare_PgStat_Relations(xact_state);
	}
}

/*
 * Clean up after successful PREPARE.
 *
 * Note: AtEOXact_PgStat is not called during PREPARE.
 */
void
PostPrepare_PgStat(void)
{
	PgStat_SubXactStatus *xact_state;

	/*
	 * We don't bother to free any of the transactional state, since it's all
	 * in TopTransactionContext and will go away anyway.
	 */
	xact_state = pgStatXactStack;
	if (xact_state != NULL)
	{
		Assert(xact_state->nest_level == 1);
		Assert(xact_state->prev == NULL);

		PostPrepare_PgStat_Relations(xact_state);
	}
	pgStatXactStack = NULL;

	/* Make sure any stats snapshot is thrown away */
	pgstat_clear_snapshot();
}

/*
 * Ensure (sub)transaction stack entry for the given nest_level exists, adding
 * it if needed.
 */
PgStat_SubXactStatus *
pgstat_get_xact_stack_level(int nest_level)
{
	PgStat_SubXactStatus *xact_state;

	xact_state = pgStatXactStack;
	if (xact_state == NULL || xact_state->nest_level != nest_level)
	{
		xact_state = (PgStat_SubXactStatus *)
			MemoryContextAlloc(TopTransactionContext,
							   sizeof(PgStat_SubXactStatus));
		dlist_init(&xact_state->pending_drops);
		xact_state->pending_drops_count = 0;
		xact_state->nest_level = nest_level;
		xact_state->prev = pgStatXactStack;
		xact_state->first = NULL;
		pgStatXactStack = xact_state;
	}
	return xact_state;
}

/*
 * Get stat items that need to be dropped at commit / abort.
 *
 * When committing, stats for objects that have been dropped in the
 * transaction are returned. When aborting, stats for newly created objects are
 * returned.
 *
 * Used by COMMIT / ABORT and 2PC PREPARE processing when building their
 * respective WAL records, to ensure stats are dropped in case of a crash / on
 * standbys.
 *
 * The list of items is allocated in CurrentMemoryContext and must be freed by
 * the caller (directly or via memory context reset).
 */
int
pgstat_get_transactional_drops(bool isCommit, xl_xact_stats_item **items)
{
	PgStat_SubXactStatus *xact_state = pgStatXactStack;
	int			nitems = 0;
	dlist_iter	iter;

	if (xact_state == NULL)
		return 0;

	/*
	 * We expect to be called for subtransaction abort (which logs a WAL
	 * record), but not for subtransaction commit (which doesn't).
	 */
	Assert(!isCommit || xact_state->nest_level == 1);
	Assert(!isCommit || xact_state->prev == NULL);

	*items = palloc(xact_state->pending_drops_count
					* sizeof(xl_xact_stats_item));

	dlist_foreach(iter, &xact_state->pending_drops)
	{
		PgStat_PendingDroppedStatsItem *pending =
		dlist_container(PgStat_PendingDroppedStatsItem, node, iter.cur);

		if (isCommit && pending->is_create)
			continue;
		if (!isCommit && !pending->is_create)
			continue;

		Assert(nitems < xact_state->pending_drops_count);
		(*items)[nitems++] = pending->item;
	}

	return nitems;
}

/*
 * Execute scheduled drops post-commit. Called from xact_redo_commit() /
 * xact_redo_abort() during recovery, and from FinishPreparedTransaction()
 * during normal 2PC COMMIT/ABORT PREPARED processing.
 */
void
pgstat_execute_transactional_drops(int ndrops, struct xl_xact_stats_item *items, bool is_redo)
{
	int			not_freed_count = 0;

	if (ndrops == 0)
		return;

	for (int i = 0; i < ndrops; i++)
	{
		xl_xact_stats_item *it = &items[i];

		if (!pgstat_drop_entry(it->kind, it->dboid, it->objoid))
			not_freed_count++;
	}

	if (not_freed_count > 0)
		pgstat_request_entry_refs_gc();
}

static void
create_drop_transactional_internal(PgStat_Kind kind, Oid dboid, Oid objoid, bool is_create)
{
	int			nest_level = GetCurrentTransactionNestLevel();
	PgStat_SubXactStatus *xact_state;
	PgStat_PendingDroppedStatsItem *drop = (PgStat_PendingDroppedStatsItem *)
	MemoryContextAlloc(TopTransactionContext, sizeof(PgStat_PendingDroppedStatsItem));

	xact_state = pgstat_get_xact_stack_level(nest_level);

	drop->is_create = is_create;
	drop->item.kind = kind;
	drop->item.dboid = dboid;
	drop->item.objoid = objoid;

	dlist_push_tail(&xact_state->pending_drops, &drop->node);
	xact_state->pending_drops_count++;
}

/*
 * Create a stats entry for a newly created database object in a transactional
 * manner.
 *
 * I.e. if the current (sub-)transaction aborts, the stats entry will also be
 * dropped.
 */
void
pgstat_create_transactional(PgStat_Kind kind, Oid dboid, Oid objoid)
{
	if (pgstat_get_entry_ref(kind, dboid, objoid, false, NULL))
	{
		ereport(WARNING,
				errmsg("resetting existing stats for type %s, db=%u, oid=%u",
					   (pgstat_get_kind_info(kind))->name, dboid, objoid));

		pgstat_reset(kind, dboid, objoid);
	}

	create_drop_transactional_internal(kind, dboid, objoid, /* create */ true);
}

/*
 * Drop a stats entry for a just dropped database object in a transactional
 * manner.
 *
 * I.e. if the current (sub-)transaction aborts, the stats entry will stay
 * alive.
 */
void
pgstat_drop_transactional(PgStat_Kind kind, Oid dboid, Oid objoid)
{
	create_drop_transactional_internal(kind, dboid, objoid, /* create */ false);
}

/*
 * get_agstat_stack_level - add a new (sub)transaction stack entry if needed
 */
static AgStat_SubXactStatus *
get_agstat_stack_level(int nest_level)
{
	AgStat_SubXactStatus *xact_state;

	xact_state = agStatXactStack;
	if (xact_state == NULL || xact_state->nest_level != nest_level)
	{
		HASHCTL		hash_ctl;

		if (agStatContext == NULL)
			agStatContext = AllocSetContextCreate(TopTransactionContext,
												  "AGGRAPHMETA context",
												  ALLOCSET_SMALL_SIZES);

		/* push new subxactstatus to stack */
		xact_state = (AgStat_SubXactStatus *)
		MemoryContextAlloc(agStatContext,
							sizeof(AgStat_SubXactStatus));
		xact_state->nest_level = nest_level;
		xact_state->prev = agStatXactStack;
		agStatXactStack = xact_state;

		/* initialize */
		memset(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(AgStat_key);
		hash_ctl.entrysize = sizeof(AgStat_GraphMeta);
		hash_ctl.hcxt = agStatContext;

		xact_state->htab = hash_create("edge statistic of sub xact",
									   AGSTAT_EDGE_HASH_SIZE,
									   &hash_ctl,
									   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	return xact_state;
}

void
agstat_count_edge_create(Labid edge, Labid start, Labid end)
{
	int			nest_level;
	bool		found;

	AgStat_key	key;
	AgStat_GraphMeta *graphmeta;
	AgStat_SubXactStatus *xact_state;

	nest_level = GetCurrentTransactionNestLevel();
	xact_state = get_agstat_stack_level(nest_level);

	/*
	 * AgStat_key is 10 byte but aligned to 12 byte. So last 2 byte can have
	 * garbage value. It must be cleaned before use.
	 */
	memset(&key, 0, sizeof(key));
	key.graph = get_graph_path_oid();
	key.edge = edge;
	key.start = start;
	key.end = end;

	graphmeta = (AgStat_GraphMeta *) hash_search(xact_state->htab,
												 (void *) &key,
												 HASH_ENTER, &found);
	if (found)
		graphmeta->edges_inserted++;
	else
	{
		/* key is copied already */
		graphmeta->edges_inserted = 1;
		graphmeta->edges_deleted = 0;
	}
}

void
agstat_count_edge_delete(Labid edge, Labid start, Labid end)
{
	int			nest_level;
	bool		found;

	AgStat_key	key;
	AgStat_GraphMeta *graphmeta;
	AgStat_SubXactStatus *xact_state;

	nest_level = GetCurrentTransactionNestLevel();
	xact_state = get_agstat_stack_level(nest_level);

	memset(&key, 0, sizeof(key));
	key.graph = get_graph_path_oid();
	key.edge = edge;
	key.start = start;
	key.end = end;

	graphmeta = (AgStat_GraphMeta *) hash_search(xact_state->htab,
												 (void *) &key,
												 HASH_ENTER, &found);
	if (found)
		graphmeta->edges_deleted++;
	else
	{
		/* key is copied already */
		graphmeta->edges_inserted = 0;
		graphmeta->edges_deleted = 1;
	}
}

/* ----------
 * AtEOXact_AgStat
 *
 *	Called from access/transam/xact.c at top-level transaction commit/abort.
 * ----------
 */
void
AtEOXact_AgStat(bool isCommit)
{
	AgStat_SubXactStatus *xact_state;

	/*
	 * Transfer transactional insert/update counts into the base tabstat
	 * entries.  We don't bother to free any of the transactional state, since
	 * it's all in TopTransactionContext and will go away anyway.
	 */
	xact_state = agStatXactStack;
	if (xact_state != NULL && isCommit)
	{
		AgStat_GraphMeta *graphmeta;
		HASH_SEQ_STATUS seq;
		Relation	ag_graphmeta;
		HeapTuple	tup;

		hash_seq_init(&seq, xact_state->htab);

		ag_graphmeta = table_open(GraphMetaRelationId, RowExclusiveLock);

		while ((graphmeta = hash_seq_search(&seq)) != NULL)
		{
			tup = SearchSysCache4(GRAPHMETAFULL,
								  graphmeta->key.graph,
								  graphmeta->key.edge,
								  graphmeta->key.start,
								  graphmeta->key.end);

			if (HeapTupleIsValid(tup))
			{
				Form_ag_graphmeta metatup;

				metatup = (Form_ag_graphmeta) GETSTRUCT(tup);

				metatup->edgecount += graphmeta->edges_inserted;
				metatup->edgecount -= graphmeta->edges_deleted;

				if (metatup->edgecount < 0)
					elog(ERROR, "The edge count can not be less than 0.");

				if (metatup->edgecount == 0)
					CatalogTupleDelete(ag_graphmeta, &tup->t_self);
				else
					CatalogTupleUpdate(ag_graphmeta, &tup->t_self, tup);
				ReleaseSysCache(tup);
			}
			else
			{
				Datum		values[Natts_ag_graphmeta];
				bool		isnull[Natts_ag_graphmeta];
				int			i;

				for (i = 0; i < Natts_ag_graphmeta; i++)
				{
					values[i] = (Datum) NULL;
					isnull[i] = false;
				}

				values[Anum_ag_graphmeta_graph - 1] = ObjectIdGetDatum(graphmeta->key.graph);
				values[Anum_ag_graphmeta_edge - 1] = Int16GetDatum(graphmeta->key.edge);
				values[Anum_ag_graphmeta_start - 1] = Int16GetDatum(graphmeta->key.start);
				values[Anum_ag_graphmeta_end - 1] = Int16GetDatum(graphmeta->key.end);
				values[Anum_ag_graphmeta_edgecount - 1] = Int64GetDatum(graphmeta->edges_inserted - graphmeta->edges_deleted);

				tup = heap_form_tuple(RelationGetDescr(ag_graphmeta), values, isnull);

				CatalogTupleInsert(ag_graphmeta, tup);

				heap_freetuple(tup);
			}
		}
		table_close(ag_graphmeta, RowExclusiveLock);
	}
	agStatXactStack = NULL;
}

/* ----------
 * AtEOSubXact_AgStat
 *
 *	Called from access/transam/xact.c at subtransaction commit/abort.
 * ----------
 */
void
AtEOSubXact_AgStat(bool isCommit, int nestDepth)
{
	AgStat_SubXactStatus *xact_state;
	AgStat_SubXactStatus *upper;

	/*
	 * Transfer transactional insert/update counts into the next higher
	 * subtransaction state.
	 */
	xact_state = agStatXactStack;
	if (xact_state != NULL &&
		xact_state->nest_level >= nestDepth)
	{
		upper = xact_state->prev;

		if (isCommit)
		{
			if (upper && upper->nest_level == nestDepth - 1)
			{
				AgStat_GraphMeta *submeta;
				AgStat_GraphMeta *upperMeta;
				HASH_SEQ_STATUS seq;
				bool		found;

				hash_seq_init(&seq, xact_state->htab);
				while ((submeta = hash_seq_search(&seq)) != NULL)
				{
					upperMeta = hash_search(upper->htab,
											(void *) &submeta->key,
											HASH_ENTER, &found);

					if (found)
					{
						upperMeta->edges_inserted += submeta->edges_inserted;
						upperMeta->edges_deleted += submeta->edges_deleted;
					}
					else
					{
						upperMeta->edges_inserted = submeta->edges_inserted;
						upperMeta->edges_deleted = submeta->edges_deleted;
					}
				}
				/* pop stack and merge stat */
				agStatXactStack = upper;
				pfree(xact_state);
			}
			else
			{
				/* Recycle if there is no upper state */
				xact_state->nest_level--;
				agStatXactStack = xact_state;
			}
		}
		else					/* isAbort */
		{
			/* pop stack and free mem */
			agStatXactStack = upper;
			pfree(xact_state);
		}
	}
}