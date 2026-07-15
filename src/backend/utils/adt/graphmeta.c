/*
 * graphmeta.c
 *		Functions for reset and collect graphmeta data.
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/graphmeta.c
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/ag_graphmeta.h"
#include "catalog/ag_label.h"
#include "catalog/ag_edge_d.h"
#include "catalog/indexing.h"
#include "utils/inval.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

static void merge_meta(Relation rel);
static void scan_label(Oid relid, Oid graphid, Labid edge);

static HTAB *htab;
static HASHCTL hash_ctl;

void
merge_meta(Relation rel)
{
	AgStat_GraphMeta *meta_elem;
	HASH_SEQ_STATUS seq;
	HeapTuple	tup;

	hash_seq_init(&seq, htab);

	while ((meta_elem = hash_seq_search(&seq)) != NULL)
	{
		Datum		values[Natts_ag_graphmeta];
		bool		isnull[Natts_ag_graphmeta];
		int			i;

		for (i = 0; i < Natts_ag_graphmeta; i++)
		{
			values[i] = (Datum) NULL;
			isnull[i] = false;
		}

		values[Anum_ag_graphmeta_graph - 1] = ObjectIdGetDatum(meta_elem->key.graph);
		values[Anum_ag_graphmeta_edge - 1] = Int16GetDatum(meta_elem->key.edge);
		values[Anum_ag_graphmeta_start - 1] = Int16GetDatum(meta_elem->key.start);
		values[Anum_ag_graphmeta_end - 1] = Int16GetDatum(meta_elem->key.end);
		values[Anum_ag_graphmeta_edgecount - 1] = Int64GetDatum(meta_elem->edges_inserted);

		tup = heap_form_tuple(RelationGetDescr(rel), values, isnull);

		CatalogTupleInsert(rel, tup);

		heap_freetuple(tup);
	}
}

void
scan_label(Oid relid, Oid graphid, Labid edge)
{
	Relation	rel;
	HeapTuple	tup;
	Snapshot	snapshot;
	TableScanDesc scan;

	AgStat_key	key;
	AgStat_GraphMeta *meta_elem;

	/*
	 * One-entry cache of the last (start, end) pair and its hash entry.  Every
	 * row of an edge-label table shares the same edge labid (the table's own
	 * label), and consecutive edges very often repeat a (start, end) pair too --
	 * an edge label connects one or a few vertex-label pairs and rows of a pair
	 * cluster together -- so on a repeat we bump the cached entry directly and
	 * skip the hash probe entirely.  On the dominant single-triple case (e.g.
	 * Pokec's person-[friend]->person, one triple over tens of millions of
	 * edges) this elides essentially every probe; on a miss it costs only two
	 * integer compares before the hash lookup.  dynahash never relocates
	 * existing entries, so the cached pointer stays valid across HASH_ENTER.
	 */
	AgStat_GraphMeta *last_elem = NULL;
	Labid		last_start = 0;
	Labid		last_end = 0;

	rel = table_open(relid, AccessShareLock);

	/*
	 * Read edge connectivity under the transaction's own snapshot when the
	 * isolation level fixes one (REPEATABLE READ / SERIALIZABLE), so the
	 * rebuilt catalog matches exactly what queries in this transaction see.
	 * Using the latest snapshot here would let a regather triggered mid
	 * transaction (SET auto_gather_graphmeta = on, or regather_graphmeta())
	 * record connectivity from a newer snapshot than the reader's -- and if an
	 * edge the reader still sees was deleted and committed in the meantime, the
	 * now-missing triple would prune that edge's labels away and drop rows.
	 * Under READ COMMITTED the latest snapshot matches per-statement
	 * visibility, so keep it there.
	 */
	snapshot = RegisterSnapshot(IsolationUsesXactSnapshot() ?
								GetTransactionSnapshot() : GetLatestSnapshot());
	scan = table_beginscan(rel, snapshot, 0, NULL);

	memset(&key, 0, sizeof(key));
	/* graph and edge labid are the same for every row of this label */
	key.graph = graphid;
	key.edge = edge;

	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Datum		dat;
		Labid		start;
		Labid		end;
		bool		found;
		bool		isnull;

		/*
		 * Only start/end vary within a label table (the edge labid is constant,
		 * passed in by the caller), so read just those two columns -- and never
		 * the wide properties jsonb that follows them.
		 */
		dat = heap_getattr(tup, Anum_ag_edge_start,
						   RelationGetDescr(rel), &isnull);
		start = GraphidGetLabid(DatumGetGraphid(dat));

		dat = heap_getattr(tup, Anum_ag_edge_end,
						   RelationGetDescr(rel), &isnull);
		end = GraphidGetLabid(DatumGetGraphid(dat));

		/* fast path: same (start, end) as the previous edge -> skip the probe */
		if (last_elem != NULL && start == last_start && end == last_end)
		{
			last_elem->edges_inserted++;
			continue;
		}

		key.start = start;
		key.end = end;
		meta_elem = (AgStat_GraphMeta *) hash_search(htab,
													 (void *) &key,
													 HASH_ENTER, &found);
		if (found)
			meta_elem->edges_inserted++;
		else
		{
			/* key is copied already */
			meta_elem->edges_inserted = 1;
			meta_elem->edges_deleted = 0;
		}

		last_elem = meta_elem;
		last_start = start;
		last_end = end;
	}
	table_endscan(scan);
	UnregisterSnapshot(snapshot);
	table_close(rel, AccessShareLock);
}

Datum
regather_graphmeta(PG_FUNCTION_ARGS)
{
	if (auto_gather_graphmeta)
	{
		ereport(NOTICE,
				(errmsg("Set auto_gather_graphmeta to FALSE before regather_graphmeta()")));
		PG_RETURN_BOOL(false);
	}

	regather_graphmeta_internal();

	PG_RETURN_BOOL(true);
}

void
regather_graphmeta_internal(void)
{
	Relation	rel;
	HeapTuple	tup;
	Snapshot	snapshot;
	TableScanDesc scan;

	ereport(NOTICE,
			(errmsg("regathering graphmeta...")));

	rel = table_open(LabelRelationId, AccessShareLock);
	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scan = table_beginscan(rel, snapshot, 0, NULL);

	/* hash initialize */
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize = sizeof(AgStat_key);
	hash_ctl.entrysize = sizeof(AgStat_GraphMeta);

	htab = hash_create("regather total graphmeta",
					   1024,	/* hash size */
					   &hash_ctl,
					   HASH_ELEM | HASH_BLOBS);

	/* Scan all graph label */
	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Datum		relid;
		Datum		graphid;
		Datum		labid;
		char		labkind;
		bool		isnull;

		labkind = heap_getattr(tup, Anum_ag_label_labkind,
							   RelationGetDescr(rel), &isnull);

		/* Gather meta from only edges */
		if (labkind == LABEL_KIND_VERTEX)
			continue;

		relid = heap_getattr(tup, Anum_ag_label_relid,
							 RelationGetDescr(rel), &isnull);

		graphid = heap_getattr(tup, Anum_ag_label_graphid,
							   RelationGetDescr(rel), &isnull);

		labid = heap_getattr(tup, Anum_ag_label_labid,
							 RelationGetDescr(rel), &isnull);

		scan_label(DatumGetObjectId(relid), DatumGetObjectId(graphid),
				   DatumGetInt16(labid));
	}
	table_endscan(scan);
	UnregisterSnapshot(snapshot);
	table_close(rel, AccessShareLock);

	/* delete meta */
	rel = table_open(GraphMetaRelationId, RowExclusiveLock);
	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scan = table_beginscan(rel, snapshot, 0, NULL);

	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
		simple_heap_delete(rel, &tup->t_self);

	/* merge hash table data with meta catalog */
	merge_meta(rel);

	table_endscan(scan);
	UnregisterSnapshot(snapshot);
	table_close(rel, RowExclusiveLock);

	/*
	 * Rewriting ag_graphmeta's data rows emits only catcache invalidations,
	 * which PlanCacheRelCallback does not observe.  Issue an explicit relcache
	 * invalidation so already-cached, graphmeta-pruned plans (which record a
	 * dependency on GraphMetaRelationId) are re-planned against the refreshed
	 * connectivity -- otherwise a regather that adds triples would leave stale
	 * plans scanning the old, narrower label set and drop rows.  Mirrors the
	 * delta path in AtEOXact_AgStat.
	 */
	CacheInvalidateRelcacheByRelid(GraphMetaRelationId);
}
