/*
 * execGraphVle.c
 *	  AgensGraph VLE Executor.
 *
 * Copyright (c) 2022 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/executor/execGraphVle.c
 */

#include "postgres.h"

#include "executor/execGraphVle.h"
#include "executor/executor.h"
#include "ag_const.h"
#include "catalog/pg_inherits.h"
#include "utils/lsyscache.h"
#include "catalog/ag_graph_fn.h"
#include "access/table.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "access/genam.h"
#include "access/skey.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_index.h"
#include "catalog/pg_type_d.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "catalog/ag_label_fn.h"
#include "nodes/miscnodes.h"
#include "parser/parse_coerce.h"
#include "parser/parse_graph.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/typcache.h"

#define VAR_START_VID	0
#define VAR_END_VID		1
#define VAR_EDGE_IDS	2
#define VAR_EDGES		3
#define VAR_VERTICES	4

#define VLERel(vleplan) ((CypherRel *) ((vleplan)->vle_rel))

#define MAXIMUM_OUTPUT_DEPTH_UNLIMITED  (INT_MAX)

static TupleTableSlot *ExecGraphVLE(PlanState *pstate);

static bool ExecGraphVLEDFS(GraphVLEState *vle_state, Graphid start_id);

static void array_clear(ArrayBuildState *astate);
static void array_pop(ArrayBuildState *astate);
static inline bool array_has(ArrayBuildState *astate, Graphid edge_id);

/*
 * Idea for Edge uniqueness.
 *
 * VLEDepthCtx have rel_index. so, we can use rel_index to identify the target
 * table.
 *
 * Bitmap set designed to 2-byte. but, label id is 6-byte...
 */

typedef struct VLEDepthCtx
{
	IndexScanDesc iss;			/* preferred: btree index scan on start/end */
	TableScanDesc desc;			/* fallback: filtered heap scan */
	int			rel_index;
	Graphid		start_id;
	Graphid		end_id;

	Graphid		prev_end_id;
	uint8		direction_rotate;
} VLEDepthCtx;

static inline bool is_over_max_depth(GraphVLEState *vle_state, int depth);
static bool create_scan_desc(GraphVLEState *vle_state, VLEDepthCtx *vle_depth_ctx);
static bool create_none_direction_scan_desc(GraphVLEState *vle_state,
											VLEDepthCtx *vle_depth_ctx);
static void free_scan_desc(VLEDepthCtx *vle_depth_ctx);

/* expand one hop on rel_index, keyed on `key`, using the start or end index */
static void open_depth_scan(GraphVLEState *vle_state, VLEDepthCtx *ctx,
							bool by_start, Graphid key);

/* end whichever scan (index or heap) is open on ctx */
static void end_depth_scan(VLEDepthCtx *ctx);

/* fetch the next edge from ctx's open scan into slot */
static inline bool
depth_getnext(VLEDepthCtx *ctx, TupleTableSlot *slot)
{
	if (ctx->iss != NULL)
		return index_getnext_slot(ctx->iss, ForwardScanDirection, slot);
	return table_scan_getnextslot(ctx->desc, ForwardScanDirection, slot);
}

/*
 * buildNativePropFilter - ask a property constraint of one label's columns.
 *
 * Fills `filter' with a comparison per key of `tmpl' against the column that
 * `relation' holds that key in, and returns true only if every key resolved:
 * a constraint answered partly from columns and partly from the property map
 * still has to read the map, which is the cost worth avoiding, so there is
 * nothing to gain from resolving some of it.
 *
 * A key is only taken natively where the column's kind is the kind the
 * constraint asks for.  The column's value is guaranteed to be of the kind its
 * type implies -- the generation expression refuses any other -- but text
 * carries no kind, so a column of text holding "5" would answer a constraint
 * asking for the number 5 if the kinds were not compared first.  Anything else
 * -- a value that is not a scalar, a key the label does not hold in a column, a
 * type whose kind is not one the column can be trusted about, a value the
 * column's type will not accept -- leaves the whole constraint to the map,
 * which answers it as it always has.
 */
static bool
buildNativePropFilter(Jsonb *tmpl, Relation relation, VLEPropFilter *filter)
{
	TupleDesc	tupdesc = RelationGetDescr(relation);
	Oid			relid = RelationGetRelid(relation);
	JsonbIterator *it;
	JsonbValue	jv;
	JsonbIteratorToken tok;
	int			nkeys;
	int			i = 0;
	char	   *key = NULL;

	if (!JB_ROOT_IS_OBJECT(tmpl) || JB_ROOT_IS_SCALAR(tmpl))
		return false;

	nkeys = JB_ROOT_COUNT(tmpl);
	if (nkeys <= 0)
		return false;

	filter->attnum = (AttrNumber *) palloc(nkeys * sizeof(AttrNumber));
	filter->value = (Datum *) palloc(nkeys * sizeof(Datum));
	filter->eqproc = (FmgrInfo *) palloc0(nkeys * sizeof(FmgrInfo));
	filter->collation = (Oid *) palloc(nkeys * sizeof(Oid));

	it = JsonbIteratorInit(&tmpl->root);
	while ((tok = JsonbIteratorNext(&it, &jv, true)) != WJB_DONE)
	{
		Form_pg_attribute att;
		AttrNumber	attnum;
		TypeCacheEntry *typentry;
		Oid			basetype;
		ErrorSaveContext escontext = {T_ErrorSaveContext};
		Oid			typinput;
		Oid			typioparam;
		FmgrInfo	inputfn;
		char		expected;
		char	   *valstr;
		Datum		value;

		if (tok == WJB_KEY)
		{
			key = pnstrdup(jv.val.string.val, jv.val.string.len);
			continue;
		}
		if (tok != WJB_VALUE)
			continue;

		Assert(key != NULL);
		if (i >= nkeys)
			return false;		/* nested keys: not a flat constraint */

		if (!get_label_property_column(relid, key, &attnum, NULL))
			return false;
		if (attnum < 1 || attnum > tupdesc->natts)
			return false;

		att = TupleDescAttr(tupdesc, attnum - 1);
		if (att->attisdropped)
			return false;

		/*
		 * A column that cannot hold the property exactly does not answer for
		 * it.  A binary float keeps the nearest value it can represent and a
		 * numeric of a fixed scale rounds to that scale, so two properties the
		 * map tells apart can arrive in the column as one value, and a
		 * constraint that matches neither of them would match both.  Leave such
		 * a key to the map, which holds what was written.
		 */
		basetype = getBaseType(att->atttypid);
		if (basetype == FLOAT4OID || basetype == FLOAT8OID ||
			(basetype == NUMERICOID && att->atttypmod >= 0))
			return false;

		switch (TypeCategory(basetype))
		{
			case TYPCATEGORY_NUMERIC:
				expected = 'n';
				break;
			case TYPCATEGORY_STRING:
				expected = 's';
				break;
			case TYPCATEGORY_BOOLEAN:
				expected = 'b';
				break;
			default:
				return false;
		}

		/* the kind the constraint asks for has to be the column's kind */
		switch (jv.type)
		{
			case jbvNumeric:
				if (expected != 'n')
					return false;
				valstr = DatumGetCString(DirectFunctionCall1(numeric_out,
															NumericGetDatum(jv.val.numeric)));
				break;
			case jbvString:
				if (expected != 's')
					return false;
				valstr = pnstrdup(jv.val.string.val, jv.val.string.len);
				break;
			case jbvBool:
				if (expected != 'b')
					return false;
				valstr = jv.val.boolean ? "true" : "false";
				break;
			default:
				return false;	/* a map, a list, or null */
		}

		/*
		 * Read the constraint's value as the column reads its own, so that what
		 * is compared is what the column would hold.  A value the type will not
		 * accept is not an error here: the map answers instead.
		 */
		getTypeInputInfo(att->atttypid, &typinput, &typioparam);
		fmgr_info(typinput, &inputfn);
		if (!InputFunctionCallSafe(&inputfn, valstr, typioparam, att->atttypmod,
								   (Node *) &escontext, &value))
			return false;

		typentry = lookup_type_cache(att->atttypid, TYPECACHE_EQ_OPR_FINFO);
		if (!OidIsValid(typentry->eq_opr_finfo.fn_oid))
			return false;

		filter->attnum[i] = attnum;
		filter->value[i] = value;
		filter->collation[i] = att->attcollation;
		fmgr_info_copy(&filter->eqproc[i], &typentry->eq_opr_finfo,
					   CurrentMemoryContext);
		i++;
	}

	if (i != nkeys)
		return false;

	filter->nkeys = nkeys;
	return true;
}

GraphVLEState *
ExecInitGraphVLE(GraphVLE *vleplan, EState *estate, int eflags)
{
	GraphVLEState *vle_state;
	CypherRel  *vel_rel = VLERel(vleplan);	/* For searching labels. */
	List	   *scan_label_oids = NIL;
	char	   *label_name;
	Oid			label_rel_id;
	ResultRelInfo *target_rel_infos;
	ListCell   *list_cell;
	A_Indices  *vle_var_len;
	A_Const    *lidx;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	/*
	 * create state structure
	 */
	vle_state = makeNode(GraphVLEState);
	vle_state->ps.plan = (Plan *) vleplan;
	vle_state->ps.state = estate;
	vle_state->ps.ExecProcNode = ExecGraphVLE;

	vle_state->subplan = ExecInitNode(vleplan->subplan, estate, eflags);
	vle_state->need_new_sp_tuple = true;

	vle_var_len = (A_Indices *) (VLERel(vleplan)->varlen);
	lidx = (A_Const *) vle_var_len->lidx;
	vle_state->minimum_output_depth = intVal(&lidx->val);
	if (vle_var_len->uidx != NULL)
	{
		A_Const    *uidx = (A_Const *) vle_var_len->uidx;

		vle_state->maximum_output_depth = intVal(&uidx->val);
	}
	else
	{
		vle_state->maximum_output_depth = MAXIMUM_OUTPUT_DEPTH_UNLIMITED;
	}
	vle_state->cypher_rel_direction = VLERel(vleplan)->direction;

	/*
	 * initialize tuple type and projection info
	 */
	vle_state->slot = ExecAllocTableSlot(&estate->es_tupleTable,
										 NULL,
										 &TTSOpsMinimalTuple);
	vle_state->ps.ps_ResultTupleSlot = vle_state->slot;

	ExecAssignProjectionInfo(&vle_state->ps, NULL);
	vle_state->ps.ps_ResultTupleDesc = ExecTypeFromTL(vleplan->subplan->targetlist);
	ExecSetSlotDescriptor(vle_state->slot,
						  vle_state->ps.ps_ResultTupleDesc);
	ExecAssignExprContext(estate, &vle_state->ps);

	/*
	 * The subplan projects the relationship array, and then the node array,
	 * only where the query reads them, so the columns it does project are a
	 * prefix and their positions never move.  A traversal that nobody asks for
	 * the arrays of must not build them: assembling a relationship for every
	 * hop visited costs far more than the traversal itself, and the hops
	 * visited grow with the branching factor raised to the depth.
	 *
	 * An unread array is dropped by leaving the column out, never by leaving an
	 * empty array in a column something can still name.
	 */
	vle_state->use_edge_output = vle_state->ps.ps_ResultTupleDesc->natts > VAR_EDGES;
	vle_state->use_vertex_output = vle_state->ps.ps_ResultTupleDesc->natts > VAR_VERTICES;

	/* P-Map Jsonb */
	vle_state->has_prop_filter = (VLERel(vleplan)->prop_map != NULL);
	if (vle_state->has_prop_filter)
	{
		ExprContext *econtext = vle_state->ps.ps_ExprContext;
		bool		is_null = false;
		Datum		value;
		ExprState  *prop_map_expr = ExecInitExpr((Expr *) VLERel(vleplan)->prop_map,
												 &vle_state->ps);

		value = ExecEvalExpr(prop_map_expr, econtext, &is_null);

		/*
		 * A null constraint has no keys to ask a relationship about, so no
		 * relationship answers it.  Keep it as a null map rather than reading
		 * one out of a datum that holds nothing.
		 */
		vle_state->jsonb_filter = is_null ? NULL : DatumGetJsonbP(value);
		ResetExprContext(econtext);
	}
	else
	{
		vle_state->jsonb_filter = NULL;
	}


	/*
	 * Find all target labels.
	 */
	label_name = (vel_rel->types == NIL) ?
		AG_EDGE : getCypherName(linitial(vel_rel->types));
	label_rel_id = get_laboid_relid(get_labname_laboid(label_name,
													   get_graph_path_oid()));
	scan_label_oids = lappend_oid(scan_label_oids, label_rel_id);
	if (!vel_rel->only)
	{
		scan_label_oids = list_concat_unique_oid(scan_label_oids,
												 find_all_inheritors(label_rel_id,
																	 AccessShareLock,
																	 NULL));
	}

	/*
	 * Fill ResultRelInfos.
	 */
	target_rel_infos = (ResultRelInfo *) palloc(
												list_length(scan_label_oids) * sizeof(ResultRelInfo));
	vle_state->target_rel_infos = target_rel_infos;
	vle_state->num_target_rel_info = list_length(scan_label_oids);
	vle_state->start_index_rels = (Relation *)
		palloc0(vle_state->num_target_rel_info * sizeof(Relation));
	vle_state->end_index_rels = (Relation *)
		palloc0(vle_state->num_target_rel_info * sizeof(Relation));

	vle_state->scan_tuples = (TupleTableSlot **)
		palloc0(vle_state->num_target_rel_info * sizeof(TupleTableSlot *));
	vle_state->prop_filters = (VLEPropFilter *)
		palloc0(vle_state->num_target_rel_info * sizeof(VLEPropFilter));

	/* Will be filled by below logic. */
	vle_state->current_scan_tuple = NULL;

	{
		int			rel_idx = 0;

		foreach(list_cell, scan_label_oids)
		{
			Oid			label_oid = lfirst_oid(list_cell);
			Relation	relation = table_open(label_oid, AccessShareLock);
			int			k;

			vle_state->scan_tuples[rel_idx] = table_slot_create(relation, NULL);
			InitResultRelInfo(target_rel_infos,
							  relation,
							  0,	/* dummy rangetable index */
							  NULL,
							  0);
			ExecOpenIndices(target_rel_infos, false);

			/*
			 * Identify the (start,end) and (end,start) btree indexes by the
			 * heap attno of their leading key column, so each hop can be
			 * expanded with an index probe instead of a full edge-label scan.
			 * (The id index, if any, leads with a different column and is
			 * ignored; a label with neither falls back to a heap scan.)
			 */
			for (k = 0; k < target_rel_infos->ri_NumIndices; k++)
			{
				Relation	idx = target_rel_infos->ri_IndexRelationDescs[k];
				AttrNumber	leadattr;

				if (idx->rd_rel->relam != BTREE_AM_OID)
					continue;

				/*
				 * Only substitute an index probe for the heap scan when the
				 * index is safe to read in full.  Skip a disabled or
				 * not-yet-ready index (DISABLE INDEX, or an in-progress /
				 * failed CREATE INDEX CONCURRENTLY), whose contents are stale
				 * or incomplete, and skip a partial index, which omits the
				 * edges excluded by its predicate.  Leaving the slot NULL
				 * makes the hop fall back to the always-correct heap scan.
				 */
				if (!idx->rd_index->indisvalid || !idx->rd_index->indisready)
					continue;
				if (RelationGetIndexPredicate(idx) != NIL)
					continue;

				leadattr = idx->rd_index->indkey.values[0];
				if (leadattr == Anum_table_edge_start &&
					vle_state->start_index_rels[rel_idx] == NULL)
					vle_state->start_index_rels[rel_idx] = idx;
				else if (leadattr == Anum_table_edge_end &&
						 vle_state->end_index_rels[rel_idx] == NULL)
					vle_state->end_index_rels[rel_idx] = idx;
			}

			/*
			 * Ask the constraint of this label's own columns where it can be
			 * asked of them.  Each label of the set answers for itself, since
			 * they need not hold the same properties in columns, or any.
			 */
			if (vle_state->jsonb_filter != NULL && enable_property_promotion)
			{
				if (!buildNativePropFilter(vle_state->jsonb_filter, relation,
										   &vle_state->prop_filters[rel_idx]))
					vle_state->prop_filters[rel_idx].nkeys = 0;
			}

			target_rel_infos++;
			rel_idx++;
		}
	}

	list_free(scan_label_oids);

	/*
	 * edge_ids(2) keeps a hop from reusing a relationship and is always built.
	 * edges(3) and vertices(4) are the query's own output, built only where it
	 * reads them.
	 */
	vle_state->hop_cxt = AllocSetContextCreate(CurrentMemoryContext,
											   "GraphVLE hop",
											   ALLOCSET_SMALL_SIZES);

	vle_state->edge_ids = initArrayResult(GRAPHIDOID,
										  CurrentMemoryContext,
										  false);
	if (vle_state->use_edge_output)
	{
		vle_state->edges = initArrayResult(EDGEOID,
										   CurrentMemoryContext,
										   false);
	}
	else
	{
		vle_state->edges = NULL;
	}
	if (vle_state->use_vertex_output)
	{
		vle_state->vertices = initArrayResult(VERTEXOID,
											  CurrentMemoryContext,
											  false);
	}
	else
	{
		vle_state->vertices = NULL;
	}

	return vle_state;
}

static TupleTableSlot *
ExecGraphVLE(PlanState *pstate)
{
	/*
	 * =====================
	 *
	 * Alex: Many columns in tuple. but, really needed?
	 *
	 * - execProc returns below types.
	 *
	 * Case #1 (7). {prev, curr, ids | next, id} + {edges | edge} graphid,
	 * graphid, graphid[], edge[]
	 *
	 * Case #2 (9) {prev, curr, ids, edges | next, id, edge} + {vertices |
	 * vertex} graphid, graphid, graphid[], edge[], vertex[]
	 *
	 * i.e., start, "end", ARRAY[id], ARRAY[ROW(id, start, "end", properties,
	 * ctid)::edge]
	 *
	 * See genVLESubselect().
	 *
	 *
	 * - depth if, VLE low index start from 0. then, depth start from 0. but,
	 * larger than start from 1.
	 */
	GraphVLEState *vle_state = castNode(GraphVLEState, pstate);
	ExprContext *econtext = vle_state->ps.ps_ExprContext;

	/*
	 * The arrays handed back for the previous path were built in per-tuple
	 * memory, and being asked for another one is what says the caller is done
	 * with them.  Release them now: a traversal answers with one set of arrays
	 * per path it finds, so holding every set until the query ends costs the
	 * length of a path times the number of paths, which is unbounded in a way
	 * the traversal itself is not.
	 */
	ResetExprContext(econtext);

	for (;;)
	{
		/* fetch new subplan tuple. */
		if (vle_state->need_new_sp_tuple)
		{
			vle_state->subplan_tuple = ExecProcNode(vle_state->subplan);
			vle_state->need_new_sp_tuple = false;

			/* no more exists.. */
			if (TupIsNull(vle_state->subplan_tuple))
				return NULL;

			if (vle_state->use_edge_output)
				array_clear(vle_state->edges);
			array_clear(vle_state->edge_ids);

			vle_state->first_start_id = DatumGetGraphid(vle_state->subplan_tuple->tts_values[VAR_START_VID]);

			if (vle_state->use_vertex_output)
			{
				MemoryContext oldcxt;

				array_clear(vle_state->vertices);

				oldcxt = MemoryContextSwitchTo(vle_state->hop_cxt);
				accumArrayResult(vle_state->vertices,
								 get_vertex_from_graphid(vle_state->first_start_id),
								 false,
								 VERTEXOID,
								 CurrentMemoryContext);
				MemoryContextSwitchTo(oldcxt);
				MemoryContextReset(vle_state->hop_cxt);
			}

			if (0 >= vle_state->minimum_output_depth &&
				0 <= vle_state->maximum_output_depth)
			{
				return vle_state->subplan_tuple;
			}
		}

		/* Do DFS. */
		if (!ExecGraphVLEDFS(vle_state,
							 vle_state->first_start_id))
		{
			vle_state->need_new_sp_tuple = true;
			continue;
		}
		else
		{
			MemoryContext tupmctx = econtext->ecxt_per_tuple_memory;

			vle_state->subplan_tuple->tts_values[VAR_END_VID] = vle_state->last_end_id;
			vle_state->subplan_tuple->tts_values[VAR_EDGE_IDS] = makeArrayResult(vle_state->edge_ids,
																				 tupmctx);
			if (vle_state->use_edge_output)
			{
				vle_state->subplan_tuple->tts_values[VAR_EDGES] = makeArrayResult(vle_state->edges,
																				  tupmctx);
			}

			if (vle_state->use_vertex_output)
			{
				int			ndims;
				int			dims[1];
				int			lbs[1];

				ndims = (vle_state->vertices->nelems > 0) ? 1 : 0;
				/* latest vertex is added somewhere. */
				dims[0] = vle_state->vertices->nelems - 1;
				lbs[0] = 1;
				vle_state->subplan_tuple->tts_values[VAR_VERTICES] = makeMdArrayResult(vle_state->vertices,
																					   ndims,
																					   dims,
																					   lbs,
																					   tupmctx,
																					   vle_state->vertices->private_cxt);
			}

			return vle_state->subplan_tuple;
		}
	}

	/* No more tuples. */
	return NULL;
}

/*
 * ExecGraphVLEDFS
 *
 * Adds edges, edge_ids using condition cypher relationship. Returns false if
 * there is no tuple that corresponds to the condition.
 */
static bool
ExecGraphVLEDFS(GraphVLEState *vle_state, Graphid start_id)
{
	VLEDepthCtx *vle_depth_ctx = NULL;
	Graphid		edge_id;
	MemoryContext oldcxt;

	/* is first time? */
	if (vle_state->table_scan_desc_list == NIL)
	{
		vle_depth_ctx = (VLEDepthCtx *) palloc(sizeof(VLEDepthCtx));
		vle_depth_ctx->iss = NULL;
		vle_depth_ctx->desc = NULL;
		vle_depth_ctx->rel_index = 0;
		vle_depth_ctx->start_id = start_id;
		vle_depth_ctx->end_id = start_id;

		/* None-directional scanning. */
		vle_depth_ctx->direction_rotate = 0;
		vle_depth_ctx->prev_end_id = start_id;

		create_scan_desc(vle_state, vle_depth_ctx); /* never not failing */
		vle_state->table_scan_desc_list = lappend(vle_state->table_scan_desc_list,
												  vle_depth_ctx);
	}

	for (;;)
	{
		int			vle_scan_depth;
		bool		return_as_results;
		Graphid		new_start_id,
					new_end_id;

		vle_depth_ctx = llast(vle_state->table_scan_desc_list);

		/*
		 * Read into the slot belonging to the label this hop is scanning.  Only
		 * create_scan_desc() moves on to the next label, and it always does so
		 * without fetching, so rel_index names the label the scan below reads
		 * from.
		 */
		Assert(vle_depth_ctx->rel_index < vle_state->num_target_rel_info);
		vle_state->current_scan_tuple = vle_state->scan_tuples[vle_depth_ctx->rel_index];

		if (!depth_getnext(vle_depth_ctx, vle_state->current_scan_tuple))
		{
			/* find next target relation */
			if (!create_scan_desc(vle_state, vle_depth_ctx))
			{
				/* move back depth. */
				free_scan_desc(vle_depth_ctx);
				vle_state->table_scan_desc_list = list_delete_last(vle_state->table_scan_desc_list);
				if (vle_state->table_scan_desc_list == NIL)
				{
					/* terminate current tuple scan */
					return false;
				}
			}
			continue;
		}

		/*
		 * Fetch all attributes.
		 *
		 * todo: Make sure it is necessary from the results and branch out.
		 */
		slot_getallattrs(vle_state->current_scan_tuple);

		/* Property filtering. */
		if (vle_state->has_prop_filter)
		{
			bool		isnull;
			Jsonb	   *val;
			Jsonb	   *tmpl = vle_state->jsonb_filter;
			VLEPropFilter *filter =
				&vle_state->prop_filters[vle_depth_ctx->rel_index];
			JsonbIterator *it1,
					   *it2;

			/* nothing satisfies a constraint that is null */
			if (tmpl == NULL)
				continue;

			/*
			 * Where this label holds every key of the constraint in a column,
			 * compare the columns and leave the property map unread.
			 */
			if (filter->nkeys > 0)
			{
				int			k;
				bool		matched = true;

				for (k = 0; k < filter->nkeys; k++)
				{
					Datum		colval;
					bool		colnull;

					colval = slot_getattr(vle_state->current_scan_tuple,
										  filter->attnum[k], &colnull);

					/*
					 * A column with nothing in it is a property the
					 * relationship does not have, which the constraint asks it
					 * to have.
					 */
					if (colnull ||
						!DatumGetBool(FunctionCall2Coll(&filter->eqproc[k],
														filter->collation[k],
														colval,
														filter->value[k])))
					{
						matched = false;
						break;
					}
				}

				if (!matched)
					continue;
			}
			else
			{
				val = DatumGetJsonbP(slot_getattr(vle_state->current_scan_tuple,
												  Anum_table_edge_prop_map,
												  &isnull));

				/*
				 * A property map is an object, so a constraint that is not one
				 * describes no relationship.  Containment is only asked of two
				 * values of the same shape, the way the containment operator
				 * asks it.
				 */
				if (JB_ROOT_IS_OBJECT(val) != JB_ROOT_IS_OBJECT(tmpl))
					continue;

				it1 = JsonbIteratorInit(&val->root);
				it2 = JsonbIteratorInit(&tmpl->root);

				if (!JsonbDeepContains(&it1, &it2))
					continue;
			}
		}

		edge_id = vle_state->current_scan_tuple->tts_values[Anum_table_edge_id - 1];

		vle_scan_depth = list_length(vle_state->table_scan_desc_list);

		while (vle_state->edge_ids->nelems >= vle_scan_depth)
		{
			if (vle_state->use_edge_output)
				array_pop(vle_state->edges);
			array_pop(vle_state->edge_ids);
			if (vle_state->use_vertex_output)
				array_pop(vle_state->vertices);
		}

		/* It is un-efficient. but, easy. */
		if (array_has(vle_state->edge_ids, edge_id))
		{
			continue;
		}

		/* Will be used from ExecGraphVLE() */
		new_start_id = DatumGetGraphid(vle_state->current_scan_tuple->tts_values[Anum_table_edge_start - 1]);
		new_end_id = DatumGetGraphid(vle_state->current_scan_tuple->tts_values[Anum_table_edge_end - 1]);

		if (vle_state->cypher_rel_direction == CYPHER_REL_DIR_RIGHT)
		{
			vle_state->last_end_id = new_end_id;
		}
		else if (vle_state->cypher_rel_direction == CYPHER_REL_DIR_LEFT)
		{
			vle_state->last_end_id = new_start_id;
		}
		else
		{
			if (vle_depth_ctx->prev_end_id == new_end_id)
			{
				vle_state->last_end_id = new_start_id;
			}
			else
			{
				vle_state->last_end_id = new_end_id;
			}
		}

		oldcxt = MemoryContextSwitchTo(vle_state->hop_cxt);

		if (vle_state->use_edge_output)
		{
			accumArrayResult(vle_state->edges,
							 make_edge_from_tuple(vle_state->current_scan_tuple),
							 false,
							 EDGEOID,
							 CurrentMemoryContext);
		}
		accumArrayResult(vle_state->edge_ids,
						 edge_id,
						 false,
						 GRAPHIDOID,
						 CurrentMemoryContext);

		if (vle_state->use_vertex_output)
		{
			accumArrayResult(vle_state->vertices,
							 get_vertex_from_graphid(vle_state->last_end_id),
							 false,
							 VERTEXOID,
							 CurrentMemoryContext);
		}

		MemoryContextSwitchTo(oldcxt);
		MemoryContextReset(vle_state->hop_cxt);

		return_as_results = vle_state->minimum_output_depth <= vle_scan_depth &&
			vle_state->maximum_output_depth >= vle_scan_depth;

		if (!is_over_max_depth(vle_state, vle_scan_depth + 1))
		{
			/* Move next depth */
			VLEDepthCtx *top_vle_depth_ctx = vle_depth_ctx;

			vle_depth_ctx = (VLEDepthCtx *) palloc(sizeof(VLEDepthCtx));
			vle_depth_ctx->iss = NULL;
			vle_depth_ctx->desc = NULL;
			vle_depth_ctx->rel_index = 0;
			vle_depth_ctx->start_id = new_start_id;
			vle_depth_ctx->end_id = new_end_id;

			/* None-directional scanning. */
			vle_depth_ctx->direction_rotate = 0;
			vle_depth_ctx->prev_end_id = top_vle_depth_ctx->prev_end_id == new_start_id ?
				new_end_id : new_start_id;

			create_scan_desc(vle_state, vle_depth_ctx); /* never not failing */
			vle_state->table_scan_desc_list = lappend(vle_state->table_scan_desc_list,
													  vle_depth_ctx);
		}

		if (return_as_results)
		{
			break;
		}
	}

	return true;
}

static void
end_depth_scan(VLEDepthCtx *vle_depth_ctx)
{
	if (vle_depth_ctx->iss)
	{
		index_endscan(vle_depth_ctx->iss);
		vle_depth_ctx->iss = NULL;
	}
	if (vle_depth_ctx->desc)
	{
		table_endscan(vle_depth_ctx->desc);
		vle_depth_ctx->desc = NULL;
	}
}

static void
free_scan_desc(VLEDepthCtx *vle_depth_ctx)
{
	end_depth_scan(vle_depth_ctx);
	pfree(vle_depth_ctx);
}

/*
 * Begin a one-hop expansion on the current target label (rel_index): fetch the
 * edges whose start (by_start = true) or end column equals `key`.  Uses the
 * matching btree index when present (an O(degree) index probe); otherwise falls
 * back to a filtered heap scan of the whole edge label so correctness never
 * depends on the index existing.
 */
static void
open_depth_scan(GraphVLEState *vle_state, VLEDepthCtx *vle_depth_ctx,
				bool by_start, Graphid key)
{
	ResultRelInfo *result_rel_info =
		vle_state->target_rel_infos + vle_depth_ctx->rel_index;
	Relation	heap_rel = result_rel_info->ri_RelationDesc;
	Relation	index_rel = by_start ?
		vle_state->start_index_rels[vle_depth_ctx->rel_index] :
		vle_state->end_index_rels[vle_depth_ctx->rel_index];
	ScanKeyData scan_key_data;

	if (index_rel != NULL)
	{
		/* index probe: `key` matches the leading (and only scanned) column */
		ScanKeyInit(&scan_key_data,
					1,
					BTEqualStrategyNumber,
					F_GRAPHID_EQ,
					GraphidGetDatum(key));
		vle_depth_ctx->iss = index_beginscan(heap_rel, index_rel,
											 vle_state->ps.state->es_snapshot,
											 NULL, 1, 0);
		index_rescan(vle_depth_ctx->iss, &scan_key_data, 1, NULL, 0);
	}
	else
	{
		/* fallback: filtered heap scan keyed on the heap column */
		ScanKeyInit(&scan_key_data,
					by_start ? Anum_table_edge_start : Anum_table_edge_end,
					BTEqualStrategyNumber,
					F_GRAPHID_EQ,
					GraphidGetDatum(key));
		vle_depth_ctx->desc = table_beginscan(heap_rel,
											  vle_state->ps.state->es_snapshot,
											  1, &scan_key_data);
	}
}

static bool
create_scan_desc(GraphVLEState *vle_state,
				 VLEDepthCtx *vle_depth_ctx)
{
	uint32		cypher_rel_direction = vle_state->cypher_rel_direction;

	if (cypher_rel_direction == CYPHER_REL_DIR_NONE)
	{
		return create_none_direction_scan_desc(vle_state, vle_depth_ctx);
	}

	if (vle_depth_ctx->iss != NULL || vle_depth_ctx->desc != NULL)
	{
		/* current label exhausted; advance to the next target label */
		end_depth_scan(vle_depth_ctx);
		vle_depth_ctx->rel_index++;

		if (vle_depth_ctx->rel_index >= vle_state->num_target_rel_info)
		{
			return false;
		}
	}

	if (cypher_rel_direction == CYPHER_REL_DIR_RIGHT)
	{
		/* outgoing: edges whose start == end_id (use the start index) */
		open_depth_scan(vle_state, vle_depth_ctx, true, vle_depth_ctx->end_id);
	}
	else
	{
		Assert(cypher_rel_direction == CYPHER_REL_DIR_LEFT);
		/* incoming: edges whose end == start_id (use the end index) */
		open_depth_scan(vle_state, vle_depth_ctx, false, vle_depth_ctx->start_id);
	}

	return true;
}

static bool
create_none_direction_scan_desc(GraphVLEState *vle_state,
								VLEDepthCtx *vle_depth_ctx)
{
	if (vle_depth_ctx->iss != NULL || vle_depth_ctx->desc != NULL)
	{
		end_depth_scan(vle_depth_ctx);

		if (vle_depth_ctx->direction_rotate > 0)
		{
			vle_depth_ctx->direction_rotate = -1;
			vle_depth_ctx->rel_index++;

			if (vle_depth_ctx->rel_index >= vle_state->num_target_rel_info)
			{
				return false;
			}
		}

		vle_depth_ctx->direction_rotate++;
	}

	/* rotate 0: edges with start == prev_end_id; rotate 1: end == prev_end_id */
	open_depth_scan(vle_state, vle_depth_ctx,
					vle_depth_ctx->direction_rotate == 0,
					vle_depth_ctx->prev_end_id);

	return true;
}

static inline bool
is_over_max_depth(GraphVLEState *vle_state, int depth)
{
	return vle_state->maximum_output_depth < depth;
}

void
ExecReScanGraphVLE(GraphVLEState *vle_state)
{
	vle_state->need_new_sp_tuple = true;
	ExecReScan(vle_state->subplan);
}

void
ExecEndGraphVLE(GraphVLEState *vle_state)
{
	int			i;
	ListCell   *lc;
	ResultRelInfo *result_rel_info = vle_state->target_rel_infos;

	foreach(lc, vle_state->table_scan_desc_list)
	{
		VLEDepthCtx *vle_depth_ctx = lfirst(lc);

		free_scan_desc(vle_depth_ctx);
	}
	list_free(vle_state->table_scan_desc_list);

	for (i = 0; i < vle_state->num_target_rel_info; i++)
		ExecDropSingleTupleTableSlot(vle_state->scan_tuples[i]);

	for (i = 0; i < vle_state->num_target_rel_info; i++)
	{
		ExecCloseIndices(result_rel_info);
		table_close(result_rel_info->ri_RelationDesc, AccessShareLock);
		result_rel_info++;
	}
	pfree(vle_state->target_rel_infos);
	pfree(vle_state->scan_tuples);
	pfree(vle_state->start_index_rels);
	pfree(vle_state->end_index_rels);

	MemoryContextDelete(vle_state->hop_cxt);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(vle_state->ps.ps_ResultTupleSlot);
	ExecEndNode(vle_state->subplan);
}

static void
array_clear(ArrayBuildState *astate)
{
	if (!astate->typbyval)
	{
		int			i;

		for (i = 0; i < astate->nelems; i++)
			pfree(DatumGetPointer(astate->dvalues[i]));
	}

	astate->nelems = 0;
}

static void
array_pop(ArrayBuildState *astate)
{
	if (astate->nelems > 0)
	{
		astate->nelems--;
		if (!astate->typbyval)
			pfree(DatumGetPointer(astate->dvalues[astate->nelems]));
	}
}

static inline bool
array_has(ArrayBuildState *astate, Graphid edge_id)
{
	int			i;

	for (i = 0; i < astate->nelems; i++)
	{
		Graphid		cur_array_gid = DatumGetGraphid(astate->dvalues[i]);

		if (cur_array_gid == edge_id)
			return true;
	}

	return false;
}
