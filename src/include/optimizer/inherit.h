/*-------------------------------------------------------------------------
 *
 * inherit.h
 *	  prototypes for inherit.c.
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/inherit.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INHERIT_H
#define INHERIT_H

#include "nodes/pathnodes.h"


extern void expand_inherited_rtentry(PlannerInfo *root, RelOptInfo *rel,
									 RangeTblEntry *rte, Index rti);

/* agensgraph: ag_graphmeta constraint-propagation scan-pruning pre-pass */
extern void propagate_graphmeta_constraints(PlannerInfo *root);

/* agensgraph: prune vertex/edge scans by a constant id() filter */
extern void prune_scans_by_id_const(PlannerInfo *root);

extern Bitmapset *get_rel_all_updated_cols(PlannerInfo *root, RelOptInfo *rel);

extern bool apply_child_basequals(PlannerInfo *root, RelOptInfo *parentrel,
								  RelOptInfo *childrel, RangeTblEntry *childRTE,
								  AppendRelInfo *appinfo);

extern Node *expand_perrelation_property(PlannerInfo *root, Node *node,
										 Index childvarno);

#endif							/* INHERIT_H */
