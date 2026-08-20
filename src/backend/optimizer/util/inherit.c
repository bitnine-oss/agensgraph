/*-------------------------------------------------------------------------
 *
 * inherit.c
 *	  Routines to process child relations in inheritance trees
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/util/inherit.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/sysattr.h"
#include "access/table.h"
#include "catalog/partition.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/appendinfo.h"
#include "optimizer/inherit.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "parser/parsetree.h"
#include "parser/parse_relation.h"
#include "partitioning/partdesc.h"
#include "partitioning/partprune.h"
#include "utils/rel.h"

/* Agensgraph */
#include "storage/lmgr.h"
#include "utils/syscache.h"
#include "ag_const.h"
#include "catalog/ag_graph_fn.h"
#include "catalog/ag_graphmeta.h"
#include "access/stratnum.h"
#include "catalog/ag_label.h"
#include "catalog/ag_label_fn.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_proc.h"
#include "commands/defrem.h"
#include "nodes/bitmapset.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_graph.h"
#include "utils/jsonb.h"
#include "utils/jsonfuncs.h"
#include "pgstat.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/graph.h"
#include "utils/lsyscache.h"


static void expand_partitioned_rtentry(PlannerInfo *root, RelOptInfo *relinfo,
									   RangeTblEntry *parentrte,
									   Index parentRTindex, Relation parentrel,
									   Bitmapset *parent_updatedCols,
									   PlanRowMark *top_parentrc, LOCKMODE lockmode);
static void expand_single_inheritance_child(PlannerInfo *root,
											RangeTblEntry *parentrte,
											Index parentRTindex, Relation parentrel,
											PlanRowMark *top_parentrc, Relation childrel,
											RangeTblEntry **childrte_p,
											Index *childRTindex_p);
static Bitmapset *translate_col_privs(const Bitmapset *parent_privs,
									  List *translated_vars);
static Bitmapset *translate_col_privs_multilevel(PlannerInfo *root,
												 RelOptInfo *rel,
												 RelOptInfo *parent_rel,
												 Bitmapset *parent_cols);
static void expand_appendrel_subquery(PlannerInfo *root, RelOptInfo *rel,
									  RangeTblEntry *rte, Index rti);


/* ----------------------------------------------------------------
 *		ag_graphmeta constraint-propagation scan pruning
 *
 * propagate_graphmeta_constraints() runs once per query (just before
 * inheritance expansion), reconstructs the Cypher MATCH pattern from the
 * per-RTE topology the parser recorded (graphPrune* fields), and runs
 * arc-consistency over it: each pattern element has a candidate-label domain,
 * and ag_graphmeta connectivity triples (edge,start,end) are the only legal
 * combinations.  Any labelled element anywhere in the pattern narrows every
 * reachable unlabelled element, transitively.  The converged per-element domain
 * is stashed in root->graphmeta_pruned[rti] and consumed by
 * expand_inherited_rtentry().  Narrowing-only + a complete ag_graphmeta
 * (maintained while auto_gather_graphmeta is on) make this sound: a removed
 * label has no connectivity support, so it cannot contribute a matched row.
 * ---------------------------------------------------------------- */

/* Per-rtindex result; non-NULL slot => prune.  See pathnodes.h. */
typedef struct GraphmetaPrune
{
	List	   *relids;			/* child relids to scan besides the parent;
								 * NIL => parent-only (pattern empty here) */
}			GraphmetaPrune;

/* A candidate-label domain over int16 labids during the solve. */
typedef struct GMDomain
{
	bool		universe;		/* true => all labels (set ignored) */
	Bitmapset  *set;			/* candidate labids when !universe */
}			GMDomain;

/* One pattern edge = one connectivity constraint between two node rtindexes. */
typedef struct GMEdge
{
	int			src_rti;		/* source (written-before) node */
	int			dst_rti;		/* dest (written-after) node */
	int			edge_rti;		/* edge's own rtindex (also a domain slot) */
	bool		undirected;		/* match either orientation; don't narrow edge */
	bool		reverse;		/* directed <- : pattern src is graphmeta END
								 * side */
	bool		vle;			/* variable-length edge: bound endpoints only */
	int			labid;			/* vle only: edge label id (edge_rti has no
								 * domain) */

	/*
	 * vle only: memoized startside/endside(E).  These depend only on E's
	 * label, not on the evolving domains, so the arc-consistency loop
	 * computes them once and reuses them every round instead of re-walking
	 * E's subtree.
	 */
	bool		vle_sides_done;
	Bitmapset  *vle_start;
	Bitmapset  *vle_end;
}			GMEdge;

/* relid -> labid (0 if the relation is not a graph label). */
static int
gm_relid_labid(Oid relid)
{
	HeapTuple	tup;
	int			labid = 0;

	tup = SearchSysCache1(LABELRELID, ObjectIdGetDatum(relid));
	if (HeapTupleIsValid(tup))
	{
		labid = (int) ((Form_ag_label) GETSTRUCT(tup))->labid;
		ReleaseSysCache(tup);
	}
	return labid;
}

/* relid -> owning graph oid; false if the relation is not a graph label. */
static bool
gm_relid_graph(Oid relid, Oid *graph)
{
	HeapTuple	tup = SearchSysCache1(LABELRELID, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tup))
		return false;
	*graph = ((Form_ag_label) GETSTRUCT(tup))->graphid;
	ReleaseSysCache(tup);
	return true;
}

static bool
gm_member(GMDomain * d, int labid)
{
	return d->universe || bms_is_member(labid, d->set);
}

/* Narrow *d to support (taking ownership of support); true if the domain shrank. */
static bool
gm_narrow(GMDomain * d, Bitmapset *support)
{
	if (d->universe)
	{
		d->universe = false;
		d->set = support;		/* universe -> finite is always a change */
		return true;
	}
	else
	{
		Bitmapset  *newset = bms_intersect(d->set, support);
		bool		changed = !bms_equal(newset, d->set);

		bms_free(support);
		bms_free(d->set);
		d->set = newset;
		return changed;
	}
}

/* Initial domain of a pattern element: universe if unlabelled, else its subtree. */
static GMDomain
gm_init_domain(Oid graph, int labid)
{
	GMDomain	d;

	d.universe = true;
	d.set = NULL;

	if (labid != 0)
	{
		Oid			relid = get_labid_relid(graph, (uint16) labid);

		if (OidIsValid(relid))
		{
			List	   *inh = find_all_inheritors(relid, NoLock, NULL);
			ListCell   *lc;

			d.universe = false;
			foreach(lc, inh)
			{
				int			l = gm_relid_labid(lfirst_oid(lc));

				if (l != 0)
					d.set = bms_add_member(d.set, l);
			}
			list_free(inh);
		}
	}
	return d;
}

/* Accumulate a (start,end,edge) triple into the support sets. */
#define GM_ADD(set, v)	((set) = bms_add_member((set), (v)))

/*
 * ag_graphmeta's edge/start/end columns are int16, but label ids are uint16
 * (Labid): a labid > 32767 round-trips through the catalog as a negative int16.
 * Recover the original unsigned value before it meets a bitmapset (bms_* reject
 * negative members with elog) or is compared against a positive domain labid.
 */
#define GM_LABID(v)		((int) (uint16) (v))

/*
 * Collect startside(E) and endside(E) from ag_graphmeta: the vertex labids that
 * begin, resp. end, some edge of E's subtree (the same subtree the VLE executor
 * walks -- find_all_inheritors of E, since it is never scanned ONLY).  Caller
 * owns the returned bitmapsets.  Used by gm_revise_vle to bound a VLE's endpoint
 * nodes; a missing (edge,start,end) triple means no such edge exists, so an
 * endpoint label with no supporting triple cannot begin/end the traversal.
 */
static void
gm_vle_sides(Oid graph, int labid, Bitmapset **supStart, Bitmapset **supEnd)
{
	Oid			relid;
	List	   *inh;
	ListCell   *lc;

	*supStart = NULL;
	*supEnd = NULL;

	relid = get_labid_relid(graph, (uint16) labid);
	if (!OidIsValid(relid))
		return;

	inh = find_all_inheritors(relid, NoLock, NULL);
	foreach(lc, inh)
	{
		int			e = gm_relid_labid(lfirst_oid(lc));
		CatCList   *cl;
		int			i;

		if (e == 0)
			continue;
		cl = SearchSysCacheList2(GRAPHMETAFULL, ObjectIdGetDatum(graph),
								 Int16GetDatum((int16) e));
		for (i = 0; i < cl->n_members; i++)
		{
			Form_ag_graphmeta m =
				(Form_ag_graphmeta) GETSTRUCT(&cl->members[i]->tuple);

			GM_ADD(*supStart, GM_LABID(m->start));
			GM_ADD(*supEnd, GM_LABID(m->end));
		}
		ReleaseCatCacheList(cl);
	}
	list_free(inh);
}

/*
 * Recompute a directed constraint's three domains from ag_graphmeta and narrow
 * them.  Returns true if any changed; sets *empty if any domain emptied.
 */
static bool
gm_revise_directed(Oid graph, GMEdge * e, GMDomain * dom, bool *empty)
{
	GMDomain   *edom = &dom[e->edge_rti];
	GMDomain   *adom = e->reverse ? &dom[e->dst_rti] : &dom[e->src_rti];	/* gm start side */
	GMDomain   *bdom = e->reverse ? &dom[e->src_rti] : &dom[e->dst_rti];	/* gm end side */
	Bitmapset  *supS = NULL,
			   *supE = NULL,
			   *supEdge = NULL;
	bool		changed = false;
	int			i;
	int			k;
	CatCList   *cl;

	if (!edom->universe)
	{
		k = -1;
		while ((k = bms_next_member(edom->set, k)) >= 0)
		{
			cl = SearchSysCacheList2(GRAPHMETAFULL, ObjectIdGetDatum(graph),
									 Int16GetDatum((int16) k));
			for (i = 0; i < cl->n_members; i++)
			{
				Form_ag_graphmeta m =
					(Form_ag_graphmeta) GETSTRUCT(&cl->members[i]->tuple);

				if (gm_member(adom, GM_LABID(m->start)) && gm_member(bdom, GM_LABID(m->end)))
				{
					GM_ADD(supS, GM_LABID(m->start));
					GM_ADD(supE, GM_LABID(m->end));
					GM_ADD(supEdge, k);
				}
			}
			ReleaseCatCacheList(cl);
		}
	}
	else if (!adom->universe)
	{
		k = -1;
		while ((k = bms_next_member(adom->set, k)) >= 0)
		{
			cl = SearchSysCacheList2(GRAPHMETASTART, ObjectIdGetDatum(graph),
									 Int16GetDatum((int16) k));
			for (i = 0; i < cl->n_members; i++)
			{
				Form_ag_graphmeta m =
					(Form_ag_graphmeta) GETSTRUCT(&cl->members[i]->tuple);

				if (gm_member(bdom, GM_LABID(m->end)))
				{
					GM_ADD(supS, GM_LABID(m->start));
					GM_ADD(supE, GM_LABID(m->end));
					GM_ADD(supEdge, GM_LABID(m->edge));
				}
			}
			ReleaseCatCacheList(cl);
		}
	}
	else if (!bdom->universe)
	{
		k = -1;
		while ((k = bms_next_member(bdom->set, k)) >= 0)
		{
			cl = SearchSysCacheList2(GRAPHMETAEND, ObjectIdGetDatum(graph),
									 Int16GetDatum((int16) k));
			for (i = 0; i < cl->n_members; i++)
			{
				Form_ag_graphmeta m =
					(Form_ag_graphmeta) GETSTRUCT(&cl->members[i]->tuple);

				if (gm_member(adom, GM_LABID(m->start)))
				{
					GM_ADD(supS, GM_LABID(m->start));
					GM_ADD(supE, GM_LABID(m->end));
					GM_ADD(supEdge, GM_LABID(m->edge));
				}
			}
			ReleaseCatCacheList(cl);
		}
	}
	else
		return false;			/* all universe: nothing to constrain yet */

	changed |= gm_narrow(adom, supS);
	changed |= gm_narrow(bdom, supE);
	changed |= gm_narrow(edom, supEdge);

	if ((!adom->universe && bms_is_empty(adom->set)) ||
		(!bdom->universe && bms_is_empty(bdom->set)) ||
		(!edom->universe && bms_is_empty(edom->set)))
		*empty = true;

	return changed;
}

/*
 * Undirected constraint: narrow the two node endpoints to labels consistent in
 * either orientation.  The edge is a UNION subquery -- used as a fixed filter,
 * never narrowed.
 */
static bool
gm_revise_undirected(Oid graph, GMEdge * e, GMDomain * dom, bool *empty)
{
	GMDomain   *edom = &dom[e->edge_rti];
	GMDomain   *adom = &dom[e->src_rti];
	GMDomain   *bdom = &dom[e->dst_rti];
	Bitmapset  *supA = NULL,
			   *supB = NULL;
	bool		changed = false;
	int			i;
	int			k;
	CatCList   *cl;

	if (!edom->universe)
	{
		k = -1;
		while ((k = bms_next_member(edom->set, k)) >= 0)
		{
			cl = SearchSysCacheList2(GRAPHMETAFULL, ObjectIdGetDatum(graph),
									 Int16GetDatum((int16) k));
			for (i = 0; i < cl->n_members; i++)
			{
				Form_ag_graphmeta m =
					(Form_ag_graphmeta) GETSTRUCT(&cl->members[i]->tuple);

				if (gm_member(adom, GM_LABID(m->start)) && gm_member(bdom, GM_LABID(m->end)))
				{
					GM_ADD(supA, GM_LABID(m->start));
					GM_ADD(supB, GM_LABID(m->end));
				}
				if (gm_member(adom, GM_LABID(m->end)) && gm_member(bdom, GM_LABID(m->start)))
				{
					GM_ADD(supA, GM_LABID(m->end));
					GM_ADD(supB, GM_LABID(m->start));
				}
			}
			ReleaseCatCacheList(cl);
		}
	}
	else if (!adom->universe || !bdom->universe)
	{
		/* drive from whichever node is finite; X is finite, Y is the other */
		GMDomain   *xdom = !adom->universe ? adom : bdom;
		GMDomain   *ydom = !adom->universe ? bdom : adom;
		Bitmapset **supX = !adom->universe ? &supA : &supB;
		Bitmapset **supY = !adom->universe ? &supB : &supA;

		k = -1;
		while ((k = bms_next_member(xdom->set, k)) >= 0)
		{
			cl = SearchSysCacheList2(GRAPHMETASTART, ObjectIdGetDatum(graph),
									 Int16GetDatum((int16) k));
			for (i = 0; i < cl->n_members; i++)
			{
				Form_ag_graphmeta m =
					(Form_ag_graphmeta) GETSTRUCT(&cl->members[i]->tuple);

				if (gm_member(ydom, GM_LABID(m->end)))	/* x as start, y as end */
				{
					GM_ADD(*supX, k);
					GM_ADD(*supY, GM_LABID(m->end));
				}
			}
			ReleaseCatCacheList(cl);

			cl = SearchSysCacheList2(GRAPHMETAEND, ObjectIdGetDatum(graph),
									 Int16GetDatum((int16) k));
			for (i = 0; i < cl->n_members; i++)
			{
				Form_ag_graphmeta m =
					(Form_ag_graphmeta) GETSTRUCT(&cl->members[i]->tuple);

				if (gm_member(ydom, GM_LABID(m->start)))	/* x as end, y as start */
				{
					GM_ADD(*supX, k);
					GM_ADD(*supY, GM_LABID(m->start));
				}
			}
			ReleaseCatCacheList(cl);
		}
	}
	else
		return false;			/* all universe */

	changed |= gm_narrow(adom, supA);
	changed |= gm_narrow(bdom, supB);

	if ((!adom->universe && bms_is_empty(adom->set)) ||
		(!bdom->universe && bms_is_empty(bdom->set)))
		*empty = true;

	return changed;
}

/*
 * VLE constraint: a labelled, directed, min>=1 variable-length edge.  Every hop
 * is an edge of E's subtree, so the graphmeta-start-side endpoint begins the
 * first hop (=> startside(E)) and the graphmeta-end-side endpoint ends the last
 * (=> endside(E)); the two bounds are independent because the number of hops is
 * unknown.  The edge itself is a subquery (no domain slot) and is not narrowed.
 */
static bool
gm_revise_vle(Oid graph, GMEdge * e, GMDomain * dom, bool *empty)
{
	GMDomain   *adom = e->reverse ? &dom[e->dst_rti] : &dom[e->src_rti];	/* gm start side */
	GMDomain   *bdom = e->reverse ? &dom[e->src_rti] : &dom[e->dst_rti];	/* gm end side */
	bool		changed = false;

	/*
	 * startside/endside(E) depend only on E's label, not on the current
	 * domains, so compute them once and reuse across arc-consistency rounds.
	 * gm_narrow takes ownership of the set it is handed, so give it a fresh
	 * copy of the memoized sides each round.
	 */
	if (!e->vle_sides_done)
	{
		gm_vle_sides(graph, e->labid, &e->vle_start, &e->vle_end);
		e->vle_sides_done = true;
	}

	changed |= gm_narrow(adom, bms_copy(e->vle_start));
	changed |= gm_narrow(bdom, bms_copy(e->vle_end));

	if ((!adom->universe && bms_is_empty(adom->set)) ||
		(!bdom->universe && bms_is_empty(bdom->set)))
		*empty = true;

	return changed;
}

/*
 * gm_incident_edge_relids
 *		Edge-label relation OIDs incident (either orientation) to a vertex label,
 *		from ag_graphmeta.  Used to prune a delete-join's ag_edge scan to the
 *		labels that can hold an edge on the deleted vertex.
 *
 * A DETACH DELETE of (a:L) deletes every vertex in L's inheritance subtree, and
 * ag_graphmeta records edges under the LEAF vertex labids -- so the incident
 * edge labels are the union of START/END over every vlabid in the subtree, not
 * just L itself (mirrors how gm_init_domain / gm_vle_sides expand the subtree).
 * Using L alone would miss a child vertex's edges and leave them dangling after
 * the delete.  A row exists in ag_graphmeta iff >=1 live edge of that triple
 * exists, so the result is a superset of the labels actually incident -- it
 * never drops an edge a DETACH DELETE must remove.
 */
static List *
gm_incident_edge_relids(Oid graph, int vlabid)
{
	List	   *relids = NIL;
	Bitmapset  *seen = NULL;
	Oid			vrelid = get_labid_relid(graph, (uint16) vlabid);
	List	   *inh;
	ListCell   *lc;

	if (!OidIsValid(vrelid))
		return NIL;

	inh = find_all_inheritors(vrelid, NoLock, NULL);
	foreach(lc, inh)
	{
		int			v = gm_relid_labid(lfirst_oid(lc));
		int			pass;

		if (v == 0)
			continue;

		/* pass 0: v on the start side; pass 1: on the end side */
		for (pass = 0; pass < 2; pass++)
		{
			CatCList   *cl;
			int			i;

			cl = SearchSysCacheList2(pass == 0 ? GRAPHMETASTART : GRAPHMETAEND,
									 ObjectIdGetDatum(graph),
									 Int16GetDatum((int16) v));
			for (i = 0; i < cl->n_members; i++)
			{
				Form_ag_graphmeta m =
					(Form_ag_graphmeta) GETSTRUCT(&cl->members[i]->tuple);
				int			e = GM_LABID(m->edge);

				if (!bms_is_member(e, seen))
				{
					Oid			relid = get_labid_relid(graph, (uint16) e);

					seen = bms_add_member(seen, e);
					if (OidIsValid(relid))
						relids = lappend_oid(relids, relid);
				}
			}
			ReleaseCatCacheList(cl);
		}
	}
	list_free(inh);

	bms_free(seen);
	return relids;
}

/*
 * propagate_graphmeta_constraints
 *		Plan-time pre-pass: solve the MATCH pattern's label constraints against
 *		ag_graphmeta and stash per-rtindex pruned child sets for
 *		expand_inherited_rtentry().  No-op unless gathering is on, the catalog is
 *		current (no pending same-txn edge writes), and the pattern has at least
 *		one labelled (anchor) element and one edge constraint (or a delete-join
 *		ag_edge scan to prune, see GRAPHPRUNE_ROLE_DELETE_EDGE).
 */
void
propagate_graphmeta_constraints(PlannerInfo *root)
{
	int			n = root->simple_rel_array_size;
	Oid			graph = InvalidOid;
	GMDomain   *dom;
	bool	   *isvar;
	GMEdge	   *edges;
	int			nedges = 0;
	bool		any_anchor = false;
	bool		has_delete_edge = false;
	bool		empty = false;
	int			rti;

	/*
	 * Cheap pre-gate: skip the RTE walk entirely when gathering is off or this
	 * transaction has uncommitted connectivity changes.  The per-graph baseline
	 * check needs the graph oid, so it is applied below once the first pass has
	 * resolved it.
	 */
	if (!auto_gather_graphmeta || has_pending_graphmeta_writes())
		return;

	/* First pass: find the graph and whether there is anything to do. */
	for (rti = 1; rti < n; rti++)
	{
		RangeTblEntry *rte = root->simple_rte_array[rti];

		if (rte == NULL || !OidIsValid(rte->graphPruneGraph))
			continue;
		graph = rte->graphPruneGraph;
		if (rte->graphPruneRole == GRAPHPRUNE_ROLE_VLE)
		{
			/*
			 * A labelled, directed, min>=1 VLE anchors too: it bounds its
			 * endpoint nodes to startside/endside of its edge label (see
			 * gm_revise_vle).  Unlabelled, undirected, or *0.. VLEs stay pure
			 * barriers (the endpoint could match a label outside those
			 * sides).
			 */
			if (rte->graphPruneLabid != 0 &&
				rte->graphPruneDir != CYPHER_REL_DIR_NONE &&
				rte->graphPruneVleMin >= 1)
				any_anchor = true;
		}
		else if (rte->graphPruneRole == GRAPHPRUNE_ROLE_DELETE_EDGE)
		{
			/*
			 * Delete-join ag_edge scan (not a MATCH element): its anchor is
			 * the deleted vertex label, and it is pruned directly below
			 * rather than through the node/edge arc-consistency.
			 */
			if (rte->graphPruneLabid != 0)
				has_delete_edge = true;
		}
		else if (rte->graphPruneLabid != 0)
			any_anchor = true;
	}
	if (!OidIsValid(graph) || (!any_anchor && !has_delete_edge))
		return;

	/*
	 * Now that the target graph is known, require a complete, maintained
	 * ag_graphmeta baseline for it before pruning.  This is the durable per-graph
	 * gate that lets an inherited auto_gather_graphmeta=on session prune (it
	 * never ran the assign-hook regather) yet refuses to prune a graph whose
	 * baseline went stale from an edge write committed while gathering was off.
	 */
	if (!graphmeta_baseline_valid(graph))
		return;

	/*
	 * Delete-join pruning is a direct unary computation (the edge labels
	 * incident to the deleted vertex label), independent of the MATCH
	 * node/edge arc-consistency below -- and a delete-join subquery has
	 * neither a labelled node RTE nor a solver edge -- so handle it here and
	 * return.  It writes the same root->graphmeta_pruned[] that
	 * expand_inherited_rtentry() consumes, so the parent keep/drop, the
	 * concurrent-drop lock recheck, and the ag_graphmeta plan-cache
	 * dependency are all shared with MATCH pruning.
	 */
	if (has_delete_edge)
	{
		root->graphmeta_pruned = (GraphmetaPrune * *)
			palloc0(n * sizeof(GraphmetaPrune *));

		for (rti = 1; rti < n; rti++)
		{
			RangeTblEntry *rte = root->simple_rte_array[rti];
			GraphmetaPrune *gp;

			if (rte == NULL ||
				rte->graphPruneRole != GRAPHPRUNE_ROLE_DELETE_EDGE ||
				!rte->inh)
				continue;

			gp = (GraphmetaPrune *) palloc(sizeof(GraphmetaPrune));
			gp->relids = gm_incident_edge_relids(graph, rte->graphPruneLabid);
			root->graphmeta_pruned[rti] = gp;
		}
		return;
	}

	dom = (GMDomain *) palloc0(n * sizeof(GMDomain));
	isvar = (bool *) palloc0(n * sizeof(bool));
	edges = (GMEdge *) palloc(n * sizeof(GMEdge));

	/*
	 * Second pass (a): init a domain for every solver variable (node or
	 * directed/undirected edge).  Endpoint resolution in pass (b) is derived
	 * per-edge, so no cross-element id map is needed here.
	 */
	for (rti = 1; rti < n; rti++)
	{
		RangeTblEntry *rte = root->simple_rte_array[rti];
		char		role;

		if (rte == NULL || !OidIsValid(rte->graphPruneGraph))
			continue;
		role = rte->graphPruneRole;
		if (role == GRAPHPRUNE_ROLE_NODE ||
			role == GRAPHPRUNE_ROLE_DIR_EDGE ||
			role == GRAPHPRUNE_ROLE_UNDIR_EDGE)
		{
			dom[rti] = gm_init_domain(graph, rte->graphPruneLabid);
			isvar[rti] = true;
		}
	}

	/*
	 * Second pass (b): collect edge constraints.  An edge and its two
	 * endpoint nodes are always emitted by one Cypher clause, and clause
	 * pull-up shifts every rangetable index in that clause by a single
	 * constant offset.  So the edge's own (current rtindex - recorded
	 * parse-time elem id) is exactly that offset; adding it to each
	 * endpoint's recorded parse-time id yields the endpoint's current rtindex
	 * -- without a query-global elem-id map, whose parse-time ids collide
	 * across clauses (each clause numbers from 1) and would otherwise bind an
	 * edge to the wrong clause's node.  The graphPruneElemId cross-check
	 * rejects any offset that does not land on the intended endpoint.  Must
	 * run after pass (a) so isvar[]/domains are set.
	 */
	for (rti = 1; rti < n; rti++)
	{
		RangeTblEntry *rte = root->simple_rte_array[rti];
		char		role;
		bool		is_vle;
		int			shift;
		int			src;
		int			dst;

		if (rte == NULL || !OidIsValid(rte->graphPruneGraph))
			continue;
		role = rte->graphPruneRole;

		/* a labelled, directed, min>=1 VLE bounds its endpoint nodes */
		is_vle = (role == GRAPHPRUNE_ROLE_VLE &&
				  rte->graphPruneLabid != 0 &&
				  rte->graphPruneDir != CYPHER_REL_DIR_NONE &&
				  rte->graphPruneVleMin >= 1);
		if (role != GRAPHPRUNE_ROLE_DIR_EDGE &&
			role != GRAPHPRUNE_ROLE_UNDIR_EDGE && !is_vle)
			continue;

		/* this clause's pull-up offset, derived from the edge itself */
		shift = (int) rti - rte->graphPruneElemId;
		src = rte->graphPruneSrcElemId + shift;
		dst = rte->graphPruneDstElemId + shift;

		/*
		 * Both endpoints must be in-range pattern nodes we have a domain for,
		 * and must be the very elements this edge recorded (elem-id
		 * cross-check guards against a stray offset landing on an unrelated
		 * RTE).
		 */
		if (src > 0 && src < n && dst > 0 && dst < n &&
			isvar[src] && isvar[dst] &&
			root->simple_rte_array[src]->graphPruneElemId == rte->graphPruneSrcElemId &&
			root->simple_rte_array[dst]->graphPruneElemId == rte->graphPruneDstElemId)
		{
			GMEdge	   *E = &edges[nedges++];

			E->src_rti = src;
			E->dst_rti = dst;
			E->edge_rti = rti;
			E->undirected = (role == GRAPHPRUNE_ROLE_UNDIR_EDGE);
			E->reverse = (rte->graphPruneDir == CYPHER_REL_DIR_LEFT);
			E->vle = is_vle;
			E->labid = rte->graphPruneLabid;
			E->vle_sides_done = false;
			E->vle_start = NULL;
			E->vle_end = NULL;
		}
	}

	if (nedges == 0)
		return;					/* anchors but no edges: nothing to propagate */

	/* Arc-consistency to a fixpoint (domains only shrink => terminates). */
	{
		bool		changed = true;

		while (changed && !empty)
		{
			int			i;

			changed = false;
			for (i = 0; i < nedges && !empty; i++)
			{
				if (edges[i].vle)
					changed |= gm_revise_vle(graph, &edges[i], dom, &empty);
				else if (edges[i].undirected)
					changed |= gm_revise_undirected(graph, &edges[i], dom, &empty);
				else
					changed |= gm_revise_directed(graph, &edges[i], dom, &empty);
			}
		}
	}

	/*
	 * Write results for prunable inheritance elements (nodes + directed
	 * edges).
	 */
	root->graphmeta_pruned = (GraphmetaPrune * *)
		palloc0(n * sizeof(GraphmetaPrune *));

	for (rti = 1; rti < n; rti++)
	{
		RangeTblEntry *rte = root->simple_rte_array[rti];
		char		role;
		GraphmetaPrune *gp;

		if (rte == NULL || !isvar[rti] || !rte->inh)
			continue;
		role = rte->graphPruneRole;
		if (role != GRAPHPRUNE_ROLE_NODE && role != GRAPHPRUNE_ROLE_DIR_EDGE)
			continue;

		if (empty)
		{
			gp = (GraphmetaPrune *) palloc(sizeof(GraphmetaPrune));
			gp->relids = NIL;	/* parent-only */
			root->graphmeta_pruned[rti] = gp;
		}
		else if (!dom[rti].universe)
		{
			int			l = -1;

			gp = (GraphmetaPrune *) palloc(sizeof(GraphmetaPrune));
			gp->relids = NIL;
			while ((l = bms_next_member(dom[rti].set, l)) >= 0)
			{
				Oid			relid = get_labid_relid(graph, (uint16) l);

				if (OidIsValid(relid))
					gp->relids = lappend_oid(gp->relids, relid);
			}
			root->graphmeta_pruned[rti] = gp;
		}
		/* universe => leave NULL (no prune; full inheritance scan) */
	}
}

/* attno of the graphid "id" column on ag_vertex / ag_edge (and every label). */
#define GRAPHMETA_ID_ATTNO 1

/*
 * id()-constant scan pruning
 *
 * A graphid encodes its storage label in the high 16 bits (GraphidGetLabid) and
 * is an immutable primary key, so a constant "id = c" (or id IN / = ANY(...))
 * restriction on a vertex/edge inheritance scan can only match rows in the label
 * whose labid is GraphidGetLabid(c).  This prunes the inheritance scan to that
 * one label table, reusing the graphmeta_pruned[] channel.  It is purely
 * structural (no ag_graphmeta), so it runs regardless of auto_gather_graphmeta.
 */

/* Is `node` the graphid id column (attno 1) Var of baserel `rti`? */
static bool
gm_is_id_var(Node *node, Index rti)
{
	if (node && IsA(node, RelabelType))
		node = (Node *) ((RelabelType *) node)->arg;
	if (node && IsA(node, Var))
	{
		Var		   *v = (Var *) node;

		return (v->varno == rti && v->varattno == GRAPHMETA_ID_ATTNO &&
				v->vartype == GRAPHIDOID && v->varlevelsup == 0);
	}
	return false;
}

/*
 * Classify the non-Var side of a graphid equality:
 *   >= 0 : labid of a non-null graphid Const
 *   -1   : a NULL graphid Const (matches no row)
 *   -2   : not a usable graphid Const
 */
static int
gm_const_labid(Node *node)
{
	Const	   *c;

	if (node && IsA(node, RelabelType))
		node = (Node *) ((RelabelType *) node)->arg;
	if (!node || !IsA(node, Const))
		return -2;
	c = (Const *) node;
	if (c->consttype != GRAPHIDOID)
		return -2;
	if (c->constisnull)
		return -1;
	return (int) GraphidGetLabid(DatumGetGraphid(c->constvalue));
}

/*
 * If `clause` is a graphid id-column equality ("id = c") or IN/ANY
 * ("id = ANY(array)") on baserel `rti`, set *matched=true and return the set of
 * candidate labids (a NULL/empty bitmapset then means "matches nothing" ->
 * provably empty).  Otherwise leave *matched=false and return NULL.  A
 * disjunctive array containing a non-Const element is unbounded, so it is
 * treated as no filter (*matched stays false).
 */
static Bitmapset *
gm_id_clause_labids(Node *clause, Index rti, bool *matched)
{
	*matched = false;

	if (IsA(clause, OpExpr))
	{
		OpExpr	   *op = (OpExpr *) clause;
		Node	   *l,
				   *r;
		int			labid;

		if (op->opno != OID_GRAPHID_EQ_OP || list_length(op->args) != 2)
			return NULL;
		l = (Node *) linitial(op->args);
		r = (Node *) lsecond(op->args);

		if (gm_is_id_var(l, rti))
			labid = gm_const_labid(r);
		else if (gm_is_id_var(r, rti))
			labid = gm_const_labid(l);
		else
			return NULL;

		if (labid == -2)
			return NULL;		/* id compared to a non-constant */
		*matched = true;
		if (labid == -1)
			return NULL;		/* NULL const -> matches nothing */
		return bms_make_singleton(labid);
	}

	if (IsA(clause, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) clause;
		Node	   *scalar,
				   *arr;
		Bitmapset  *set = NULL;

		if (saop->opno != OID_GRAPHID_EQ_OP || !saop->useOr ||
			list_length(saop->args) != 2)
			return NULL;
		scalar = (Node *) linitial(saop->args);
		arr = (Node *) lsecond(saop->args);
		if (!gm_is_id_var(scalar, rti))
			return NULL;
		if (IsA(arr, RelabelType))
			arr = (Node *) ((RelabelType *) arr)->arg;

		if (IsA(arr, Const))
		{
			Const	   *ac = (Const *) arr;
			ArrayType  *at;
			Datum	   *elems;
			bool	   *nulls;
			int			nelems,
						i;
			int16		elmlen;
			bool		elmbyval;
			char		elmalign;

			if (ac->consttype != GRAPHIDARRAYOID)
				return NULL;
			*matched = true;
			if (ac->constisnull)
				return NULL;	/* = ANY(NULL) -> matches nothing */
			at = DatumGetArrayTypeP(ac->constvalue);
			get_typlenbyvalalign(GRAPHIDOID, &elmlen, &elmbyval, &elmalign);
			deconstruct_array(at, GRAPHIDOID, elmlen, elmbyval, elmalign,
							  &elems, &nulls, &nelems);
			for (i = 0; i < nelems; i++)
				if (!nulls[i])
					set = bms_add_member(set,
										 (int) GraphidGetLabid(DatumGetGraphid(elems[i])));
			return set;			/* empty (NULL) => matches nothing */
		}
		else if (IsA(arr, ArrayExpr))
		{
			ArrayExpr  *ae = (ArrayExpr *) arr;
			ListCell   *lc;

			foreach(lc, ae->elements)
			{
				int			el = gm_const_labid((Node *) lfirst(lc));

				if (el == -2)
				{
					bms_free(set);
					return NULL;	/* non-Const element -> unbounded, no
									 * filter */
				}
				if (el >= 0)
					set = bms_add_member(set, el);
				/* el == -1 (NULL element): never-true disjunct, skip */
			}
			*matched = true;
			return set;
		}
		return NULL;
	}

	return NULL;
}

/* Intersect two OID lists (small; preserves the order of `a`). */
static List *
gm_oid_list_intersect(List *a, List *b)
{
	List	   *out = NIL;
	ListCell   *lc;

	foreach(lc, a)
	{
		Oid			o = lfirst_oid(lc);

		if (list_member_oid(b, o))
			out = lappend_oid(out, o);
	}
	return out;
}

/* Write/intersect `survivors` into root->graphmeta_pruned[rti]. */
static void
gm_merge_id_prune(PlannerInfo *root, int n, Index rti, List *survivors)
{
	GraphmetaPrune *existing;

	if (root->graphmeta_pruned == NULL)
		root->graphmeta_pruned = (GraphmetaPrune * *)
			palloc0(n * sizeof(GraphmetaPrune *));

	existing = root->graphmeta_pruned[rti];
	if (existing == NULL)
	{
		GraphmetaPrune *gp = (GraphmetaPrune *) palloc(sizeof(GraphmetaPrune));

		gp->relids = survivors; /* NIL => parent-only / provably empty */
		root->graphmeta_pruned[rti] = gp;
	}
	else if (existing->relids != NIL)
	{
		/* connectivity solver already pruned this rti: intersect */
		existing->relids = gm_oid_list_intersect(existing->relids, survivors);
	}
	/* else existing->relids == NIL: already provably empty, leave as-is */
}

/*
 * prune_scans_by_id_const
 *		Plan-time pre-pass: for each vertex/edge inheritance-parent scan carrying
 *		a constant id() equality / IN filter, prune the scan to the label
 *		table(s) the graphid constant(s) identify, via root->graphmeta_pruned[].
 *		Runs after propagate_graphmeta_constraints so it can intersect into
 *		whatever the connectivity solver already produced.  Independent of
 *		ag_graphmeta and its gating (labid is encoded structurally in the
 *		constant), so it prunes even when gathering is off.
 */
void
prune_scans_by_id_const(PlannerInfo *root)
{
	int			n = root->simple_rel_array_size;
	Index		rti;

	for (rti = 1; rti < n; rti++)
	{
		RelOptInfo *rel = root->simple_rel_array[rti];
		RangeTblEntry *rte = root->simple_rte_array[rti];
		Oid			graph;
		Bitmapset  *labids = NULL;
		bool		have_id = false;
		List	   *survivors = NIL;
		ListCell   *lc;

		if (rel == NULL || rte == NULL ||
			rte->rtekind != RTE_RELATION || !rte->inh ||
			!OidIsValid(rte->relid))
			continue;
		if (!gm_relid_graph(rte->relid, &graph))
			continue;			/* not a graph vertex/edge label parent */

		/* AND-intersect the labid set across all id-equality restrictions */
		foreach(lc, rel->baserestrictinfo)
		{
			RestrictInfo *ri = lfirst_node(RestrictInfo, lc);
			bool		this_matched;
			Bitmapset  *thisset = gm_id_clause_labids((Node *) ri->clause,
													  rti, &this_matched);

			if (!this_matched)
			{
				bms_free(thisset);
				continue;
			}
			if (!have_id)
			{
				labids = thisset;
				have_id = true;
			}
			else
			{
				Bitmapset  *tmp = bms_intersect(labids, thisset);

				bms_free(labids);
				bms_free(thisset);
				labids = tmp;
			}
		}

		if (!have_id)
			continue;			/* no id filter on this scan */

		/* map labids -> label relids that exist within this RTE's subtree */
		if (!bms_is_empty(labids))
		{
			List	   *subtree = find_all_inheritors(rte->relid, NoLock, NULL);
			int			labid = -1;

			while ((labid = bms_next_member(labids, labid)) >= 0)
			{
				Oid			crelid = get_labid_relid(graph, (uint16) labid);

				if (OidIsValid(crelid) && list_member_oid(subtree, crelid))
					survivors = lappend_oid(survivors, crelid);
			}
			list_free(subtree);
		}
		bms_free(labids);

		/* NIL survivors => provably empty (parent-only scan) */
		gm_merge_id_prune(root, n, rti, survivors);
	}
}

/*
 * graphmeta_propagate_id_unique
 *		A graphmeta-pruned element is scanned as an appendrel over a set of
 *		vertex/edge label tables.  Their graphid "id" values (attno 1) are
 *		globally unique across labels -- the label id is encoded in the graphid's
 *		high bits, so distinct labels occupy disjoint id ranges -- hence if every
 *		surviving child has a unique index on id, the appendrel parent is itself
 *		unique on id.  Lift one such child index onto the parent rel's indexlist
 *		as a uniqueness proof.
 *
 * Legacy-inheritance parents carry no indexlist (plancat.c get_relation_info
 * sets hasindex=false for them), so without this the join planner cannot see
 * that e.g. b.id is unique, and mis-estimates joins on the pruned element
 * (inflating cardinality and picking merge+sort over hash joins) until ANALYZE
 * independently recovers the distinctness.  This works for any number of
 * surviving labels, and is the same kind of proof propagation PostgreSQL
 * already does for partitioned parents.
 *
 * Only the id column is propagated: it is the one column provably disjoint
 * across labels (other columns may repeat values between labels).  Graph-label
 * children share the parent's column layout, so no attno translation is needed.
 * The parent is an appendrel and never builds index scan paths itself, so the
 * entry is consulted only as a proof.
 */
static void
graphmeta_propagate_id_unique(PlannerInfo *root, RelOptInfo *parentrel,
							  List *child_rtis)
{
	IndexOptInfo *proto = NULL;
	IndexOptInfo *pidx;
	ListCell   *lc;

	if (child_rtis == NIL)
		return;

	/* Require EVERY surviving child to be unique on id; else prove nothing. */
	foreach(lc, child_rtis)
	{
		RelOptInfo *childrel = root->simple_rel_array[lfirst_int(lc)];
		IndexOptInfo *found = NULL;
		ListCell   *il;

		if (childrel == NULL)
			return;

		foreach(il, childrel->indexlist)
		{
			IndexOptInfo *idx = (IndexOptInfo *) lfirst(il);

			if (idx->unique && idx->nkeycolumns == 1 &&
				idx->indexkeys[0] == GRAPHMETA_ID_ATTNO &&
				idx->indpred == NIL)
			{
				found = idx;
				break;
			}
		}
		if (found == NULL)
			return;
		if (proto == NULL)
			proto = found;
	}

	/*
	 * Build the parent's proof entry from scratch rather than shallow-copying
	 * proto.  A memcpy would alias proto's child-owned per-column arrays and,
	 * worse, carry proto's child indexoid: get_actual_variable_range()
	 * (reached when a merge join on the id column is costed) would then
	 * index_open() that child index and probe it against the *parent's* heap
	 * -- a wrong-heap TID probe.  Copy only what the uniqueness/distinctness
	 * proofs read (relation_has_unique_index_for, has_unique_index,
	 * rel_supports_distinctness) -- the id key column, its opclass metadata,
	 * and the unique/immediate flags -- mark the entry hypothetical with no
	 * indexoid so nothing ever opens it, and leave the
	 * ordering/predicate/expression fields unset: this appendrel parent never
	 * builds an index-scan path from the entry.
	 */
	pidx = makeNode(IndexOptInfo);
	pidx->rel = parentrel;
	pidx->ncolumns = 1;
	pidx->nkeycolumns = 1;
	pidx->indexkeys = (int *) palloc(sizeof(int));
	pidx->indexkeys[0] = GRAPHMETA_ID_ATTNO;
	pidx->indexcollations = (Oid *) palloc(sizeof(Oid));
	pidx->indexcollations[0] = proto->indexcollations[0];
	pidx->opfamily = (Oid *) palloc(sizeof(Oid));
	pidx->opfamily[0] = proto->opfamily[0];
	pidx->opcintype = (Oid *) palloc(sizeof(Oid));
	pidx->opcintype[0] = proto->opcintype[0];
	pidx->relam = proto->relam;
	pidx->unique = true;
	pidx->immediate = proto->immediate;
	pidx->indpred = NIL;
	pidx->predOK = false;
	pidx->hypothetical = true;
	pidx->indexoid = InvalidOid;
	parentrel->indexlist = lappend(parentrel->indexlist, pidx);
}


/*
 * expand_inherited_rtentry
 *		Expand a rangetable entry that has the "inh" bit set.
 *
 * "inh" is only allowed in two cases: RELATION and SUBQUERY RTEs.
 *
 * "inh" on a plain RELATION RTE means that it is a partitioned table or the
 * parent of a traditional-inheritance set.  In this case we must add entries
 * for all the interesting child tables to the query's rangetable, and build
 * additional planner data structures for them, including RelOptInfos,
 * AppendRelInfos, and possibly PlanRowMarks.
 *
 * Note that the original RTE is considered to represent the whole inheritance
 * set.  In the case of traditional inheritance, the first of the generated
 * RTEs is an RTE for the same table, but with inh = false, to represent the
 * parent table in its role as a simple member of the inheritance set.  For
 * partitioning, we don't need a second RTE because the partitioned table
 * itself has no data and need not be scanned.
 *
 * "inh" on a SUBQUERY RTE means that it's the parent of a UNION ALL group,
 * which is treated as an appendrel similarly to inheritance cases; however,
 * we already made RTEs and AppendRelInfos for the subqueries.  We only need
 * to build RelOptInfos for them, which is done by expand_appendrel_subquery.
 */
void
expand_inherited_rtentry(PlannerInfo *root, RelOptInfo *rel,
						 RangeTblEntry *rte, Index rti)
{
	Oid			parentOID;
	Relation	oldrelation;
	LOCKMODE	lockmode;
	PlanRowMark *oldrc;
	bool		old_isParent = false;
	int			old_allMarkTypes = 0;

	Assert(rte->inh);			/* else caller error */

	if (rte->rtekind == RTE_SUBQUERY)
	{
		expand_appendrel_subquery(root, rel, rte, rti);
		return;
	}

	Assert(rte->rtekind == RTE_RELATION);

	parentOID = rte->relid;

	/*
	 * We used to check has_subclass() here, but there's no longer any need
	 * to, because subquery_planner already did.
	 */

	/*
	 * The rewriter should already have obtained an appropriate lock on each
	 * relation named in the query, so we can open the parent relation without
	 * locking it.  However, for each child relation we add to the query, we
	 * must obtain an appropriate lock, because this will be the first use of
	 * those relations in the parse/rewrite/plan pipeline.  Child rels should
	 * use the same lockmode as their parent.
	 */
	oldrelation = table_open(parentOID, NoLock);
	lockmode = rte->rellockmode;

	/*
	 * If parent relation is selected FOR UPDATE/SHARE, we need to mark its
	 * PlanRowMark as isParent = true, and generate a new PlanRowMark for each
	 * child.
	 */
	oldrc = get_plan_rowmark(root->rowMarks, rti);
	if (oldrc)
	{
		old_isParent = oldrc->isParent;
		oldrc->isParent = true;
		/* Save initial value of allMarkTypes before children add to it */
		old_allMarkTypes = oldrc->allMarkTypes;
	}

	/* Scan the inheritance set and expand it */
	if (oldrelation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		RTEPermissionInfo *perminfo;

		perminfo = getRTEPermissionInfo(root->parse->rteperminfos, rte);

		/*
		 * Partitioned table, so set up for partitioning.
		 */
		Assert(rte->relkind == RELKIND_PARTITIONED_TABLE);

		/*
		 * Recursively expand and lock the partitions.  While at it, also
		 * extract the partition key columns of all the partitioned tables.
		 */
		expand_partitioned_rtentry(root, rel, rte, rti,
								   oldrelation,
								   perminfo->updatedCols,
								   oldrc, lockmode);
	}
	else
	{
		/*
		 * Ordinary table, so process traditional-inheritance children.  (Note
		 * that partitioned tables are not allowed to have inheritance
		 * children, so it's not possible for both cases to apply.)
		 */
		List	   *inhOIDs;
		ListCell   *l;
		bool		gm_omit_parent PG_USED_FOR_ASSERTS_ONLY = false;
		bool		gm_pruned = false;
		List	   *gm_child_rtis = NIL;

		/* Agensgraph graphmeta scan pruning (MATCH elements and delete-joins) */
		if (root->graphmeta_pruned != NULL &&
			rti < root->simple_rel_array_size &&
			root->graphmeta_pruned[rti] != NULL)
		{
			/*
			 * The graphmeta constraint-propagation pre-pass narrowed this
			 * element to the labels ag_graphmeta says can occur.  Scan only
			 * the parent plus those children (an empty list => parent-only,
			 * i.e. the pattern is provably empty here).
			 */
			GraphmetaPrune *gp = root->graphmeta_pruned[rti];
			bool		include_parent;

			gm_pruned = true;

			/*
			 * The parent table holds the rows of the element's own label --
			 * for the abstract roots ag_vertex / ag_edge that is the
			 * *unlabelled* vertices / edges.  Scan it only if that label is
			 * still in the pruned domain (parentOID present in relids), or if
			 * the domain is empty (parent-only => provably-empty pattern).
			 * Otherwise the parent provably contributes nothing, so we drop
			 * it and the Append/MergeAppend carries only the connected
			 * children -- a tighter plan.  This mirrors how partitioned-table
			 * and UNION ALL appendrels have no parent-self member; the child
			 * loop and rowmark handling below key off (childOID !=
			 * parentOID), not on the parent's presence.
			 */
			include_parent = (gp->relids == NIL) ||
				list_member_oid(gp->relids, parentOID);

			inhOIDs = include_parent ? list_make1_oid(parentOID) : NIL;
			foreach(l, gp->relids)
			{
				Oid			crelid = lfirst_oid(l);

				if (crelid == parentOID || !OidIsValid(crelid))
					continue;

				if (lockmode != NoLock)
				{
					/*
					 * Lock to synchronize against concurrent drop, then
					 * re-check.
					 */
					LockRelationOid(crelid, lockmode);
					if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(crelid)))
					{
						UnlockRelationOid(crelid, lockmode);
						continue;
					}
				}

				inhOIDs = lappend_oid(inhOIDs, crelid);
			}

			/*
			 * If we dropped the parent but every connected child was dropped
			 * concurrently, fall back to a parent-only (empty) scan so
			 * inhOIDs stays valid.
			 */
			if (inhOIDs == NIL)
				inhOIDs = list_make1_oid(parentOID);
			gm_omit_parent = (linitial_oid(inhOIDs) != parentOID);

			/*
			 * Depend on ag_graphmeta so a connectivity change (which fires an
			 * explicit relcache invalidation on ag_graphmeta) re-plans this
			 * pruned, possibly cached, plan.
			 */
			root->glob->relationOids = lappend_oid(root->glob->relationOids,
												   GraphMetaRelationId);
		}
		else
			/* Scan for all members of inheritance set, acquire needed locks */
			inhOIDs = find_all_inheritors(parentOID, lockmode, NULL);

		/*
		 * We used to special-case the situation where the table no longer has
		 * any children, by clearing rte->inh and exiting.  That no longer
		 * works, because this function doesn't get run until after decisions
		 * have been made that depend on rte->inh.  We have to treat such
		 * situations as normal inheritance.  The table itself should always
		 * have been found, though.
		 */
		Assert(inhOIDs != NIL);

		/*
		 * The parent is normally the first member, in its role as a plain
		 * member of the inheritance set.  Graphmeta pruning may legitimately
		 * omit it when its own label is provably disconnected
		 * (gm_omit_parent), leaving a children-only appendrel as
		 * partitioned/UNION ALL ones have.
		 */
		Assert(gm_omit_parent || linitial_oid(inhOIDs) == parentOID);

		/* Expand simple_rel_array and friends to hold child objects. */
		expand_planner_arrays(root, list_length(inhOIDs));

		/*
		 * Expand inheritance children in the order the OIDs were returned by
		 * find_all_inheritors.
		 */
		foreach(l, inhOIDs)
		{
			Oid			childOID = lfirst_oid(l);
			Relation	newrelation;
			RangeTblEntry *childrte;
			Index		childRTindex;

			/* Open rel if needed; we already have required locks */
			if (childOID != parentOID)
				newrelation = table_open(childOID, NoLock);
			else
				newrelation = oldrelation;

			/*
			 * It is possible that the parent table has children that are temp
			 * tables of other backends.  We cannot safely access such tables
			 * (because of buffering issues), and the best thing to do seems
			 * to be to silently ignore them.
			 */
			if (childOID != parentOID && RELATION_IS_OTHER_TEMP(newrelation))
			{
				table_close(newrelation, lockmode);
				continue;
			}

			/* Create RTE and AppendRelInfo, plus PlanRowMark if needed. */
			expand_single_inheritance_child(root, rte, rti, oldrelation,
											oldrc, newrelation,
											&childrte, &childRTindex);

			/* Create the otherrel RelOptInfo too. */
			(void) build_simple_rel(root, childRTindex, rel);

			/*
			 * Remember graphmeta-pruned members for the id-unique proof
			 * below.
			 */
			if (gm_pruned)
				gm_child_rtis = lappend_int(gm_child_rtis, (int) childRTindex);

			/* Close child relations, but keep locks */
			if (childOID != parentOID)
				table_close(newrelation, NoLock);
		}

		/*
		 * For a graphmeta-pruned appendrel, lift the children's id-uniqueness
		 * onto the parent rel so joins on the pruned element are costed like
		 * a directly-labelled one, for any number of surviving labels (see
		 * graphmeta_propagate_id_unique).
		 */
		if (gm_pruned)
			graphmeta_propagate_id_unique(root, rel, gm_child_rtis);
	}

	/*
	 * Some children might require different mark types, which would've been
	 * reported into oldrc.  If so, add relevant entries to the top-level
	 * targetlist and update parent rel's reltarget.  This should match what
	 * preprocess_targetlist() would have added if the mark types had been
	 * requested originally.
	 *
	 * (Someday it might be useful to fold these resjunk columns into the
	 * row-identity-column management used for UPDATE/DELETE.  Today is not
	 * that day, however.)
	 */
	if (oldrc)
	{
		int			new_allMarkTypes = oldrc->allMarkTypes;
		Var		   *var;
		TargetEntry *tle;
		char		resname[32];
		List	   *newvars = NIL;

		/* Add TID junk Var if needed, unless we had it already */
		if (new_allMarkTypes & ~(1 << ROW_MARK_COPY) &&
			!(old_allMarkTypes & ~(1 << ROW_MARK_COPY)))
		{
			/* Need to fetch TID */
			var = makeVar(oldrc->rti,
						  SelfItemPointerAttributeNumber,
						  TIDOID,
						  -1,
						  InvalidOid,
						  0);
			snprintf(resname, sizeof(resname), "ctid%u", oldrc->rowmarkId);
			tle = makeTargetEntry((Expr *) var,
								  list_length(root->processed_tlist) + 1,
								  pstrdup(resname),
								  true);
			root->processed_tlist = lappend(root->processed_tlist, tle);
			newvars = lappend(newvars, var);
		}

		/* Add whole-row junk Var if needed, unless we had it already */
		if ((new_allMarkTypes & (1 << ROW_MARK_COPY)) &&
			!(old_allMarkTypes & (1 << ROW_MARK_COPY)))
		{
			var = makeWholeRowVar(planner_rt_fetch(oldrc->rti, root),
								  oldrc->rti,
								  0,
								  false);
			snprintf(resname, sizeof(resname), "wholerow%u", oldrc->rowmarkId);
			tle = makeTargetEntry((Expr *) var,
								  list_length(root->processed_tlist) + 1,
								  pstrdup(resname),
								  true);
			root->processed_tlist = lappend(root->processed_tlist, tle);
			newvars = lappend(newvars, var);
		}

		/* Add tableoid junk Var, unless we had it already */
		if (!old_isParent)
		{
			var = makeVar(oldrc->rti,
						  TableOidAttributeNumber,
						  OIDOID,
						  -1,
						  InvalidOid,
						  0);
			snprintf(resname, sizeof(resname), "tableoid%u", oldrc->rowmarkId);
			tle = makeTargetEntry((Expr *) var,
								  list_length(root->processed_tlist) + 1,
								  pstrdup(resname),
								  true);
			root->processed_tlist = lappend(root->processed_tlist, tle);
			newvars = lappend(newvars, var);
		}

		/*
		 * Add the newly added Vars to parent's reltarget.  We needn't worry
		 * about the children's reltargets, they'll be made later.
		 */
		add_vars_to_targetlist(root, newvars, bms_make_singleton(0));
	}

	table_close(oldrelation, NoLock);
}

/*
 * expand_partitioned_rtentry
 *		Recursively expand an RTE for a partitioned table.
 */
static void
expand_partitioned_rtentry(PlannerInfo *root, RelOptInfo *relinfo,
						   RangeTblEntry *parentrte,
						   Index parentRTindex, Relation parentrel,
						   Bitmapset *parent_updatedCols,
						   PlanRowMark *top_parentrc, LOCKMODE lockmode)
{
	PartitionDesc partdesc;
	Bitmapset  *live_parts;
	int			num_live_parts;
	int			i;

	check_stack_depth();

	Assert(parentrte->inh);

	partdesc = PartitionDirectoryLookup(root->glob->partition_directory,
										parentrel);

	/* A partitioned table should always have a partition descriptor. */
	Assert(partdesc);

	/*
	 * Note down whether any partition key cols are being updated. Though it's
	 * the root partitioned table's updatedCols we are interested in,
	 * parent_updatedCols provided by the caller contains the root partrel's
	 * updatedCols translated to match the attribute ordering of parentrel.
	 */
	if (!root->partColsUpdated)
		root->partColsUpdated =
			has_partition_attrs(parentrel, parent_updatedCols, NULL);

	/* Nothing further to do here if there are no partitions. */
	if (partdesc->nparts == 0)
		return;

	/*
	 * Perform partition pruning using restriction clauses assigned to parent
	 * relation.  live_parts will contain PartitionDesc indexes of partitions
	 * that survive pruning.  Below, we will initialize child objects for the
	 * surviving partitions.
	 */
	relinfo->live_parts = live_parts = prune_append_rel_partitions(relinfo);

	/* Expand simple_rel_array and friends to hold child objects. */
	num_live_parts = bms_num_members(live_parts);
	if (num_live_parts > 0)
		expand_planner_arrays(root, num_live_parts);

	/*
	 * We also store partition RelOptInfo pointers in the parent relation.
	 * Since we're palloc0'ing, slots corresponding to pruned partitions will
	 * contain NULL.
	 */
	Assert(relinfo->part_rels == NULL);
	relinfo->part_rels = (RelOptInfo **)
		palloc0(relinfo->nparts * sizeof(RelOptInfo *));

	/*
	 * Create a child RTE for each live partition.  Note that unlike
	 * traditional inheritance, we don't need a child RTE for the partitioned
	 * table itself, because it's not going to be scanned.
	 */
	i = -1;
	while ((i = bms_next_member(live_parts, i)) >= 0)
	{
		Oid			childOID = partdesc->oids[i];
		Relation	childrel;
		RangeTblEntry *childrte;
		Index		childRTindex;
		RelOptInfo *childrelinfo;

		/*
		 * Open rel, acquiring required locks.  If a partition was recently
		 * detached and subsequently dropped, then opening it will fail.  In
		 * this case, behave as though the partition had been pruned.
		 */
		childrel = try_table_open(childOID, lockmode);
		if (childrel == NULL)
		{
			relinfo->live_parts = bms_del_member(relinfo->live_parts, i);
			continue;
		}

		/*
		 * Temporary partitions belonging to other sessions should have been
		 * disallowed at definition, but for paranoia's sake, let's double
		 * check.
		 */
		if (RELATION_IS_OTHER_TEMP(childrel))
			elog(ERROR, "temporary relation from another session found as partition");

		/* Create RTE and AppendRelInfo, plus PlanRowMark if needed. */
		expand_single_inheritance_child(root, parentrte, parentRTindex,
										parentrel, top_parentrc, childrel,
										&childrte, &childRTindex);

		/* Create the otherrel RelOptInfo too. */
		childrelinfo = build_simple_rel(root, childRTindex, relinfo);
		relinfo->part_rels[i] = childrelinfo;
		relinfo->all_partrels = bms_add_members(relinfo->all_partrels,
												childrelinfo->relids);

		/* If this child is itself partitioned, recurse */
		if (childrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		{
			AppendRelInfo *appinfo = root->append_rel_array[childRTindex];
			Bitmapset  *child_updatedCols;

			child_updatedCols = translate_col_privs(parent_updatedCols,
													appinfo->translated_vars);

			expand_partitioned_rtentry(root, childrelinfo,
									   childrte, childRTindex,
									   childrel,
									   child_updatedCols,
									   top_parentrc, lockmode);
		}

		/* Close child relation, but keep locks */
		table_close(childrel, NoLock);
	}
}

/*
 * expand_single_inheritance_child
 *		Build a RangeTblEntry and an AppendRelInfo, plus maybe a PlanRowMark.
 *
 * We now expand the partition hierarchy level by level, creating a
 * corresponding hierarchy of AppendRelInfos and RelOptInfos, where each
 * partitioned descendant acts as a parent of its immediate partitions.
 * (This is a difference from what older versions of PostgreSQL did and what
 * is still done in the case of table inheritance for unpartitioned tables,
 * where the hierarchy is flattened during RTE expansion.)
 *
 * PlanRowMarks still carry the top-parent's RTI, and the top-parent's
 * allMarkTypes field still accumulates values from all descendents.
 *
 * "parentrte" and "parentRTindex" are immediate parent's RTE and
 * RTI. "top_parentrc" is top parent's PlanRowMark.
 *
 * The child RangeTblEntry and its RTI are returned in "childrte_p" and
 * "childRTindex_p" resp.
 */
static void
expand_single_inheritance_child(PlannerInfo *root, RangeTblEntry *parentrte,
								Index parentRTindex, Relation parentrel,
								PlanRowMark *top_parentrc, Relation childrel,
								RangeTblEntry **childrte_p,
								Index *childRTindex_p)
{
	Query	   *parse = root->parse;
	Oid			parentOID PG_USED_FOR_ASSERTS_ONLY =
		RelationGetRelid(parentrel);
	Oid			childOID = RelationGetRelid(childrel);
	RangeTblEntry *childrte;
	Index		childRTindex;
	AppendRelInfo *appinfo;
	TupleDesc	child_tupdesc;
	List	   *parent_colnames;
	List	   *child_colnames;

	/*
	 * Build an RTE for the child, and attach to query's rangetable list. We
	 * copy most scalar fields of the parent's RTE, but replace relation OID,
	 * relkind, and inh for the child.  Set the child's securityQuals to
	 * empty, because we only want to apply the parent's RLS conditions
	 * regardless of what RLS properties individual children may have. (This
	 * is an intentional choice to make inherited RLS work like regular
	 * permissions checks.) The parent securityQuals will be propagated to
	 * children along with other base restriction clauses, so we don't need to
	 * do it here.  Other infrastructure of the parent RTE has to be
	 * translated to match the child table's column ordering, which we do
	 * below, so a "flat" copy is sufficient to start with.
	 */
	childrte = makeNode(RangeTblEntry);
	memcpy(childrte, parentrte, sizeof(RangeTblEntry));
	Assert(parentrte->rtekind == RTE_RELATION); /* else this is dubious */
	childrte->relid = childOID;
	childrte->relkind = childrel->rd_rel->relkind;
	/* A partitioned child will need to be expanded further. */
	if (childrte->relkind == RELKIND_PARTITIONED_TABLE)
	{
		Assert(childOID != parentOID);
		childrte->inh = true;
	}
	else
		childrte->inh = false;
	childrte->securityQuals = NIL;

	/* No permission checking for child RTEs. */
	childrte->perminfoindex = 0;

	/* Link not-yet-fully-filled child RTE into data structures */
	parse->rtable = lappend(parse->rtable, childrte);
	childRTindex = list_length(parse->rtable);
	*childrte_p = childrte;
	*childRTindex_p = childRTindex;

	/*
	 * Build an AppendRelInfo struct for each parent/child pair.
	 */
	appinfo = make_append_rel_info(parentrel, childrel,
								   parentRTindex, childRTindex);
	root->append_rel_list = lappend(root->append_rel_list, appinfo);

	/* tablesample is probably null, but copy it */
	childrte->tablesample = copyObject(parentrte->tablesample);

	/*
	 * Construct an alias clause for the child, which we can also use as eref.
	 * This is important so that EXPLAIN will print the right column aliases
	 * for child-table columns.  (Since ruleutils.c doesn't have any easy way
	 * to reassociate parent and child columns, we must get the child column
	 * aliases right to start with.  Note that setting childrte->alias forces
	 * ruleutils.c to use these column names, which it otherwise would not.)
	 */
	child_tupdesc = RelationGetDescr(childrel);
	parent_colnames = parentrte->eref->colnames;
	child_colnames = NIL;
	for (int cattno = 0; cattno < child_tupdesc->natts; cattno++)
	{
		Form_pg_attribute att = TupleDescAttr(child_tupdesc, cattno);
		const char *attname;

		if (att->attisdropped)
		{
			/* Always insert an empty string for a dropped column */
			attname = "";
		}
		else if (appinfo->parent_colnos[cattno] > 0 &&
				 appinfo->parent_colnos[cattno] <= list_length(parent_colnames))
		{
			/* Duplicate the query-assigned name for the parent column */
			attname = strVal(list_nth(parent_colnames,
									  appinfo->parent_colnos[cattno] - 1));
		}
		else
		{
			/* New column, just use its real name */
			attname = NameStr(att->attname);
		}
		child_colnames = lappend(child_colnames, makeString(pstrdup(attname)));
	}

	/*
	 * We just duplicate the parent's table alias name for each child.  If the
	 * plan gets printed, ruleutils.c has to sort out unique table aliases to
	 * use, which it can handle.
	 */
	childrte->alias = childrte->eref = makeAlias(parentrte->eref->aliasname,
												 child_colnames);

	/*
	 * Store the RTE and appinfo in the respective PlannerInfo arrays, which
	 * the caller must already have allocated space for.
	 */
	Assert(childRTindex < root->simple_rel_array_size);
	Assert(root->simple_rte_array[childRTindex] == NULL);
	root->simple_rte_array[childRTindex] = childrte;
	Assert(root->append_rel_array[childRTindex] == NULL);
	root->append_rel_array[childRTindex] = appinfo;

	/*
	 * Build a PlanRowMark if parent is marked FOR UPDATE/SHARE.
	 */
	if (top_parentrc)
	{
		PlanRowMark *childrc = makeNode(PlanRowMark);

		childrc->rti = childRTindex;
		childrc->prti = top_parentrc->rti;
		childrc->rowmarkId = top_parentrc->rowmarkId;
		/* Reselect rowmark type, because relkind might not match parent */
		childrc->markType = select_rowmark_type(childrte,
												top_parentrc->strength);
		childrc->allMarkTypes = (1 << childrc->markType);
		childrc->strength = top_parentrc->strength;
		childrc->waitPolicy = top_parentrc->waitPolicy;

		/*
		 * We mark RowMarks for partitioned child tables as parent RowMarks so
		 * that the executor ignores them (except their existence means that
		 * the child tables will be locked using the appropriate mode).
		 */
		childrc->isParent = (childrte->relkind == RELKIND_PARTITIONED_TABLE);

		/* Include child's rowmark type in top parent's allMarkTypes */
		top_parentrc->allMarkTypes |= childrc->allMarkTypes;

		root->rowMarks = lappend(root->rowMarks, childrc);
	}

	/*
	 * If we are creating a child of the query target relation (only possible
	 * in UPDATE/DELETE/MERGE), add it to all_result_relids, as well as
	 * leaf_result_relids if appropriate, and make sure that we generate
	 * required row-identity data.
	 */
	if (bms_is_member(parentRTindex, root->all_result_relids))
	{
		/* OK, record the child as a result rel too. */
		root->all_result_relids = bms_add_member(root->all_result_relids,
												 childRTindex);

		/* Non-leaf partitions don't need any row identity info. */
		if (childrte->relkind != RELKIND_PARTITIONED_TABLE)
		{
			Var		   *rrvar;

			root->leaf_result_relids = bms_add_member(root->leaf_result_relids,
													  childRTindex);

			/*
			 * If we have any child target relations, assume they all need to
			 * generate a junk "tableoid" column.  (If only one child survives
			 * pruning, we wouldn't really need this, but it's not worth
			 * thrashing about to avoid it.)
			 */
			rrvar = makeVar(childRTindex,
							TableOidAttributeNumber,
							OIDOID,
							-1,
							InvalidOid,
							0);
			add_row_identity_var(root, rrvar, childRTindex, "tableoid");

			/* Register any row-identity columns needed by this child. */
			add_row_identity_columns(root, childRTindex,
									 childrte, childrel);
		}
	}
}

/*
 * get_rel_all_updated_cols
 * 		Returns the set of columns of a given "simple" relation that are
 * 		updated by this query.
 */
Bitmapset *
get_rel_all_updated_cols(PlannerInfo *root, RelOptInfo *rel)
{
	Index		relid;
	RangeTblEntry *rte;
	RTEPermissionInfo *perminfo;
	Bitmapset  *updatedCols,
			   *extraUpdatedCols;

	Assert(root->parse->commandType == CMD_UPDATE);
	Assert(IS_SIMPLE_REL(rel));

	/*
	 * We obtain updatedCols for the query's result relation.  Then, if
	 * necessary, we map it to the column numbers of the relation for which
	 * they were requested.
	 */
	relid = root->parse->resultRelation;
	rte = planner_rt_fetch(relid, root);
	perminfo = getRTEPermissionInfo(root->parse->rteperminfos, rte);

	updatedCols = perminfo->updatedCols;

	if (rel->relid != relid)
	{
		RelOptInfo *top_parent_rel = find_base_rel(root, relid);

		Assert(IS_OTHER_REL(rel));

		updatedCols = translate_col_privs_multilevel(root, rel, top_parent_rel,
													 updatedCols);
	}

	/*
	 * Now we must check to see if there are any generated columns that depend
	 * on the updatedCols, and add them to the result.
	 */
	extraUpdatedCols = get_dependent_generated_columns(root, rel->relid,
													   updatedCols);

	return bms_union(updatedCols, extraUpdatedCols);
}

/*
 * translate_col_privs
 *	  Translate a bitmapset representing per-column privileges from the
 *	  parent rel's attribute numbering to the child's.
 *
 * The only surprise here is that we don't translate a parent whole-row
 * reference into a child whole-row reference.  That would mean requiring
 * permissions on all child columns, which is overly strict, since the
 * query is really only going to reference the inherited columns.  Instead
 * we set the per-column bits for all inherited columns.
 */
static Bitmapset *
translate_col_privs(const Bitmapset *parent_privs,
					List *translated_vars)
{
	Bitmapset  *child_privs = NULL;
	bool		whole_row;
	int			attno;
	ListCell   *lc;

	/* System attributes have the same numbers in all tables */
	for (attno = FirstLowInvalidHeapAttributeNumber + 1; attno < 0; attno++)
	{
		if (bms_is_member(attno - FirstLowInvalidHeapAttributeNumber,
						  parent_privs))
			child_privs = bms_add_member(child_privs,
										 attno - FirstLowInvalidHeapAttributeNumber);
	}

	/* Check if parent has whole-row reference */
	whole_row = bms_is_member(InvalidAttrNumber - FirstLowInvalidHeapAttributeNumber,
							  parent_privs);

	/* And now translate the regular user attributes, using the vars list */
	attno = InvalidAttrNumber;
	foreach(lc, translated_vars)
	{
		Var		   *var = lfirst_node(Var, lc);

		attno++;
		if (var == NULL)		/* ignore dropped columns */
			continue;
		if (whole_row ||
			bms_is_member(attno - FirstLowInvalidHeapAttributeNumber,
						  parent_privs))
			child_privs = bms_add_member(child_privs,
										 var->varattno - FirstLowInvalidHeapAttributeNumber);
	}

	return child_privs;
}

/*
 * translate_col_privs_multilevel
 *		Recursively translates the column numbers contained in 'parent_cols'
 *		to the column numbers of a descendant relation given by 'rel'
 *
 * Note that because this is based on translate_col_privs, it will expand
 * a whole-row reference into all inherited columns.  This is not an issue
 * for current usages, but beware.
 */
static Bitmapset *
translate_col_privs_multilevel(PlannerInfo *root, RelOptInfo *rel,
							   RelOptInfo *parent_rel,
							   Bitmapset *parent_cols)
{
	AppendRelInfo *appinfo;

	/* Fast path for easy case. */
	if (parent_cols == NULL)
		return NULL;

	/* Recurse if immediate parent is not the top parent. */
	if (rel->parent != parent_rel)
	{
		if (rel->parent)
			parent_cols = translate_col_privs_multilevel(root, rel->parent,
														 parent_rel,
														 parent_cols);
		else
			elog(ERROR, "rel with relid %u is not a child rel", rel->relid);
	}

	/* Now translate for this child. */
	Assert(root->append_rel_array != NULL);
	appinfo = root->append_rel_array[rel->relid];
	Assert(appinfo != NULL);

	return translate_col_privs(parent_cols, appinfo->translated_vars);
}

/*
 * expand_appendrel_subquery
 *		Add "other rel" RelOptInfos for the children of an appendrel baserel
 *
 * "rel" is a subquery relation that has the rte->inh flag set, meaning it
 * is a UNION ALL subquery that's been flattened into an appendrel, with
 * child subqueries listed in root->append_rel_list.  We need to build
 * a RelOptInfo for each child relation so that we can plan scans on them.
 */
static void
expand_appendrel_subquery(PlannerInfo *root, RelOptInfo *rel,
						  RangeTblEntry *rte, Index rti)
{
	ListCell   *l;

	foreach(l, root->append_rel_list)
	{
		AppendRelInfo *appinfo = (AppendRelInfo *) lfirst(l);
		Index		childRTindex = appinfo->child_relid;
		RangeTblEntry *childrte;
		RelOptInfo *childrel;

		/* append_rel_list contains all append rels; ignore others */
		if (appinfo->parent_relid != rti)
			continue;

		/* find the child RTE, which should already exist */
		Assert(childRTindex < root->simple_rel_array_size);
		childrte = root->simple_rte_array[childRTindex];
		Assert(childrte != NULL);

		/* Build the child RelOptInfo. */
		childrel = build_simple_rel(root, childRTindex, rel);

		/* Child may itself be an inherited rel, either table or subquery. */
		if (childrte->inh)
			expand_inherited_rtentry(root, childrel, childrte, childRTindex);
	}
}



/*
 * expand_perrelation_property
 *		Read a property from the column this relation holds it in.
 *
 * A property read the parser could not bind to a column is left for here.  It
 * could not because the column was not in scope where the read was written -- a
 * clause between the two collapsed the rows the element was read from -- or
 * because one scan reads every label beneath the one being queried and they need
 * not agree on which column holds the property, or hold it at all.  By here the
 * read has reached the relation it reads, so it can be bound to that relation's
 * column; where there is no column the access stays on the property map, which
 * is the same answer.
 *
 * The rewrite keeps the access's own type, so the clause around it is untouched
 * and everything derived from that clause stays true of it.
 */
typedef struct
{
	List	   *rtable;
} perrelation_context;

/*
 * perrelation_access_key
 *		The property a marked access reads, or NULL if this is not one.
 */
static char *
perrelation_access_key(Node *node)
{
	CypherAccessExpr *a;
	Node	   *key;

	if (node == NULL || !IsA(node, CypherAccessExpr))
		return NULL;

	a = (CypherAccessExpr *) node;
	if (!a->perrelation || list_length(a->path) != 1)
		return NULL;

	key = (Node *) linitial(a->path);
	if (!IsA(key, Const) || ((Const *) key)->constisnull ||
		((Const *) key)->consttype != TEXTOID)
		return NULL;

	return TextDatumGetCString(((Const *) key)->constvalue);
}

/*
 * perrelation_column
 *		The column a marked access reads its property from, as a Var on the
 *		relation that holds it, or NULL where the read is not settled here.
 *
 * The access names the row it reads, so the relation is taken from the access
 * rather than passed in.  A scan that reads the labels beneath this one as well
 * settles nothing here, and is left to each relation the scan is expanded into.
 */
static Var *
perrelation_column(Node *node, perrelation_context *ctx)
{
	char	   *key;
	Node	   *arg;
	Var		   *var;
	RangeTblEntry *rte;
	AttrNumber	attnum;
	Oid			coltype;
	int32		coltypmod;
	Oid			colcoll;

	key = perrelation_access_key(node);
	if (key == NULL)
		return NULL;

	arg = (Node *) ((CypherAccessExpr *) node)->arg;
	if (arg == NULL || !IsA(arg, Var))
		return NULL;

	var = (Var *) arg;
	if (var->varlevelsup != 0 ||
		var->varno < 1 || var->varno > list_length(ctx->rtable))
		return NULL;

	rte = rt_fetch(var->varno, ctx->rtable);
	if (rte->rtekind != RTE_RELATION || !OidIsValid(rte->relid))
		return NULL;
	if (rte->inh && has_subclass(rte->relid))
		return NULL;
	if (var->varattno != get_attnum(rte->relid, AG_ELEM_PROP_MAP))
		return NULL;

	if (!get_label_property_column(rte->relid, key, &attnum, NULL))
		return NULL;
	get_atttypetypmodcoll(rte->relid, attnum, &coltype, &coltypmod, &colcoll);

	return makeVar(var->varno, attnum, coltype, coltypmod, colcoll, 0);
}

/*
 * perrelation_marked_walker
 *		Is there an access here for the planner to settle?
 *
 * Asked before rewriting because the rewrite rebuilds the tree it walks, and
 * every qual of every query is offered to it.
 */
static bool
perrelation_marked_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, CypherAccessExpr) &&
		((CypherAccessExpr *) node)->perrelation)
		return true;

	return expression_tree_walker(node, perrelation_marked_walker, context);
}

/*
 * perrelation_native_const
 *		The constant as a value of the column's own type, or false if it is not
 *		exactly one.
 *
 * Decided by converting it and converting it back: only a value that returns to
 * the identical property map is used.  That refuses a number the column cannot
 * hold, a string where the column takes numbers, and anything whose written form
 * differs from what was asked for, without having to reason about each type.
 */
static bool
perrelation_native_const(Const *c, Oid coltype, int32 coltypmod, Datum *result)
{
	Jsonb	   *jb;
	JsonbValue *jbv;
	char	   *str;
	Oid			typinput;
	Oid			typioparam;
	Oid			typoutput;
	bool		typisvarlena;
	FmgrInfo	inputfn;
	ErrorSaveContext escontext = {T_ErrorSaveContext};
	JsonTypeCategory tcategory;
	Oid			outfuncoid;
	Jsonb	   *back;

	jb = DatumGetJsonbP(c->constvalue);
	if (!JB_ROOT_IS_SCALAR(jb))
		return false;

	jbv = getIthJsonbValueFromContainer(&jb->root, 0);
	switch (jbv->type)
	{
		case jbvNumeric:
			str = DatumGetCString(DirectFunctionCall1(numeric_out,
													  NumericGetDatum(jbv->val.numeric)));
			break;
		case jbvString:
			str = pnstrdup(jbv->val.string.val, jbv->val.string.len);
			break;
		case jbvBool:
			str = jbv->val.boolean ? "true" : "false";
			break;
		default:
			return false;
	}

	/*
	 * The value is converted here, while the query is planned, and the result
	 * kept as a constant.  That is only sound where converting does not depend
	 * on anything about the session: a type whose reading or writing is merely
	 * stable would fix one session's answer into a plan another session reuses.
	 * No property may be promoted to such a type today, and this is what says so
	 * where it matters rather than somewhere else.
	 */
	getTypeInputInfo(coltype, &typinput, &typioparam);
	getTypeOutputInfo(coltype, &typoutput, &typisvarlena);
	if (func_volatile(typinput) != PROVOLATILE_IMMUTABLE ||
		func_volatile(typoutput) != PROVOLATILE_IMMUTABLE)
		return false;

	fmgr_info(typinput, &inputfn);
	if (!InputFunctionCallSafe(&inputfn, str, typioparam, coltypmod,
							   (Node *) &escontext, result))
		return false;

	/* it counts only if it comes back as the very same value */
	json_categorize_type(coltype, true, &tcategory, &outfuncoid);
	back = DatumGetJsonbP(datum_to_jsonb(*result, tcategory, outfuncoid));

	return (VARSIZE(back) == VARSIZE(jb) &&
			memcmp(back, jb, VARSIZE(jb)) == 0);
}

/*
 * perrelation_native_clause
 *		Compare a promoted property in its column's own terms.
 *
 * Reading the column is not on its own enough to use an index on it: the
 * comparison is still between property maps, and an index on the column does not
 * answer that.  Where the value compared against is exactly a value of the
 * column's type, the comparison is rewritten to that type and the index binds.
 *
 * Returns NULL to leave the comparison alone, which reads the property map and
 * is the answer either way.
 */
static Node *
perrelation_native_clause(OpExpr *op, perrelation_context *ctx)
{
	Node	   *args[2];
	int			accessat;
	Const	   *cval;
	Oid			jsonbfam;
	Oid			colfam;
	int			strategy;
	bool		negate = false;
	Oid			nativeop;
	Datum		nativeval;
	Var		   *col;
	Const	   *lit;
	OpExpr	   *rebuilt;

	if (list_length(op->args) != 2)
		return NULL;

	args[0] = (Node *) linitial(op->args);
	args[1] = (Node *) lsecond(op->args);

	if ((col = perrelation_column(args[0], ctx)) != NULL)
		accessat = 0;
	else if ((col = perrelation_column(args[1], ctx)) != NULL)
		accessat = 1;
	else
		return NULL;

	cval = (Const *) args[1 - accessat];
	if (!IsA(cval, Const) || cval->constisnull ||
		cval->consttype != JSONBOID)
		return NULL;

	jsonbfam = get_opclass_family(GetDefaultOpClass(JSONBOID, BTREE_AM_OID));
	colfam = get_opclass_family(GetDefaultOpClass(col->vartype, BTREE_AM_OID));
	if (!OidIsValid(jsonbfam) || !OidIsValid(colfam))
		return NULL;

	strategy = get_op_opfamily_strategy(op->opno, jsonbfam);
	if (strategy == 0)
	{
		/* "not equal" is no member of a btree family; its negator is */
		Oid			neg = get_negator(op->opno);

		if (!OidIsValid(neg) ||
			get_op_opfamily_strategy(neg, jsonbfam) != BTEqualStrategyNumber)
			return NULL;
		strategy = BTEqualStrategyNumber;
		negate = true;
	}

	if (!perrelation_native_const(cval, col->vartype, col->vartypmod,
								  &nativeval))
		return NULL;

	nativeop = get_opfamily_member(colfam, col->vartype, col->vartype,
								   strategy);
	if (!OidIsValid(nativeop))
		return NULL;
	if (negate)
	{
		nativeop = get_negator(nativeop);
		if (!OidIsValid(nativeop))
			return NULL;
	}

	lit = makeConst(col->vartype, col->vartypmod, col->varcollid,
					get_typlen(col->vartype), nativeval, false,
					get_typbyval(col->vartype));

	rebuilt = makeNode(OpExpr);
	rebuilt->opno = nativeop;
	rebuilt->opfuncid = get_opcode(nativeop);
	rebuilt->opresulttype = BOOLOID;
	rebuilt->opretset = false;
	rebuilt->opcollid = InvalidOid;
	rebuilt->inputcollid = col->varcollid;
	rebuilt->location = op->location;

	/* the operands keep the side they were written on */
	if (accessat == 0)
		rebuilt->args = list_make2(col, lit);
	else
		rebuilt->args = list_make2(lit, col);

	return (Node *) rebuilt;
}

static Node *
perrelation_mutator(Node *node, perrelation_context *ctx)
{
	if (node == NULL)
		return NULL;

	/*
	 * A comparison is taken as a whole, so that what replaces it is a
	 * comparison of the same kind and nothing around it has to change.
	 */
	if (IsA(node, OpExpr))
	{
		Node	   *native = perrelation_native_clause((OpExpr *) node, ctx);

		if (native != NULL)
			return native;
	}

	if (IsA(node, CypherAccessExpr))
	{
		Var		   *col = perrelation_column(node, ctx);

		if (col != NULL)
			return (Node *) makeFuncExpr(F_CYPHER_TO_JSONB, JSONBOID,
										 list_make1(col), InvalidOid,
										 InvalidOid, COERCE_EXPLICIT_CALL);
	}

	return expression_tree_mutator(node, perrelation_mutator, ctx);
}

Node *
expand_perrelation_property(PlannerInfo *root, Node *node)
{
	perrelation_context ctx;

	if (node == NULL || !enable_property_promotion)
		return node;
	if (!perrelation_marked_walker(node, NULL))
		return node;

	ctx.rtable = root->parse->rtable;

	return perrelation_mutator(node, &ctx);
}

/*
 * apply_child_basequals
 *		Populate childrel's base restriction quals from parent rel's quals,
 *		translating Vars using appinfo and re-checking for quals which are
 *		constant-TRUE or constant-FALSE when applied to this child relation.
 *
 * If any of the resulting clauses evaluate to constant false or NULL, we
 * return false and don't apply any quals.  Caller should mark the relation as
 * a dummy rel in this case, since it doesn't need to be scanned.  Constant
 * true quals are ignored.
 */
bool
apply_child_basequals(PlannerInfo *root, RelOptInfo *parentrel,
					  RelOptInfo *childrel, RangeTblEntry *childRTE,
					  AppendRelInfo *appinfo)
{
	List	   *childquals;
	Index		cq_min_security;
	ListCell   *lc;

	/*
	 * The child rel's targetlist might contain non-Var expressions, which
	 * means that substitution into the quals could produce opportunities for
	 * const-simplification, and perhaps even pseudoconstant quals. Therefore,
	 * transform each RestrictInfo separately to see if it reduces to a
	 * constant or pseudoconstant.  (We must process them separately to keep
	 * track of the security level of each qual.)
	 */
	childquals = NIL;
	cq_min_security = UINT_MAX;
	foreach(lc, parentrel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
		Node	   *childqual;
		ListCell   *lc2;

		Assert(IsA(rinfo, RestrictInfo));
		childqual = adjust_appendrel_attrs(root,
										   (Node *) rinfo->clause,
										   1, &appinfo);
		childqual = expand_perrelation_property(root, childqual);
		childqual = eval_const_expressions(root, childqual);
		/* check for flat-out constant */
		if (childqual && IsA(childqual, Const))
		{
			if (((Const *) childqual)->constisnull ||
				!DatumGetBool(((Const *) childqual)->constvalue))
			{
				/* Restriction reduces to constant FALSE or NULL */
				return false;
			}
			/* Restriction reduces to constant TRUE, so drop it */
			continue;
		}
		/* might have gotten an AND clause, if so flatten it */
		foreach(lc2, make_ands_implicit((Expr *) childqual))
		{
			Node	   *onecq = (Node *) lfirst(lc2);
			bool		pseudoconstant;
			RestrictInfo *childrinfo;

			/* check for pseudoconstant (no Vars or volatile functions) */
			pseudoconstant =
				!contain_vars_of_level(onecq, 0) &&
				!contain_volatile_functions(onecq);
			if (pseudoconstant)
			{
				/* tell createplan.c to check for gating quals */
				root->hasPseudoConstantQuals = true;
			}
			/* reconstitute RestrictInfo with appropriate properties */
			childrinfo = make_restrictinfo(root,
										   (Expr *) onecq,
										   rinfo->is_pushed_down,
										   rinfo->has_clone,
										   rinfo->is_clone,
										   pseudoconstant,
										   rinfo->security_level,
										   NULL, NULL, NULL);

			/* Restriction is proven always false */
			if (restriction_is_always_false(root, childrinfo))
				return false;
			/* Restriction is proven always true, so drop it */
			if (restriction_is_always_true(root, childrinfo))
				continue;

			childquals = lappend(childquals, childrinfo);
			/* track minimum security level among child quals */
			cq_min_security = Min(cq_min_security, rinfo->security_level);
		}
	}

	/*
	 * In addition to the quals inherited from the parent, we might have
	 * securityQuals associated with this particular child node.  (Currently
	 * this can only happen in appendrels originating from UNION ALL;
	 * inheritance child tables don't have their own securityQuals, see
	 * expand_single_inheritance_child().)  Pull any such securityQuals up
	 * into the baserestrictinfo for the child.  This is similar to
	 * process_security_barrier_quals() for the parent rel, except that we
	 * can't make any general deductions from such quals, since they don't
	 * hold for the whole appendrel.
	 */
	if (childRTE->securityQuals)
	{
		Index		security_level = 0;

		foreach(lc, childRTE->securityQuals)
		{
			List	   *qualset = (List *) lfirst(lc);
			ListCell   *lc2;

			foreach(lc2, qualset)
			{
				Expr	   *qual = (Expr *) lfirst(lc2);

				/* not likely that we'd see constants here, so no check */
				childquals = lappend(childquals,
									 make_restrictinfo(root, qual,
													   true,
													   false, false,
													   false,
													   security_level,
													   NULL, NULL, NULL));
				cq_min_security = Min(cq_min_security, security_level);
			}
			security_level++;
		}
		Assert(security_level <= root->qual_security_level);
	}

	/*
	 * OK, we've got all the baserestrictinfo quals for this child.
	 */
	childrel->baserestrictinfo = childquals;
	childrel->baserestrict_min_security = cq_min_security;

	return true;
}
