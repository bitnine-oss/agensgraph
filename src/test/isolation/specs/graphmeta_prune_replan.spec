# Cross-session plan-cache invalidation for ag_graphmeta scan pruning.
#
# A cached (generic) pruned plan in one backend must be re-planned when ANOTHER
# backend commits new connectivity, so it never keeps scanning a stale, too-narrow
# label set -- which would silently drop the newly-connected rows.  This is the
# cross-backend counterpart of the single-session gmp7 case: it exercises the
# commit-time CacheInvalidateRelcacheByRelid(GraphMetaRelationId) broadcast.

setup
{
  CREATE GRAPH gcp;
  SET graph_path = gcp;
  CREATE VLABEL n1; CREATE VLABEL n2; CREATE VLABEL n3;
  CREATE ELABEL e1; CREATE ELABEL e2;
  CREATE (:n1)-[:e1]->(:n2);
}

teardown { DROP GRAPH gcp CASCADE; }

session s1
setup
{
  SET graph_path = gcp;
  SET plan_cache_mode = force_generic_plan;
  SET auto_gather_graphmeta = on;
  PREPARE pp AS MATCH (a:n1)-[b]->(c) RETURN label(c) AS c ORDER BY c;
}
# first EXECUTE builds+caches the generic plan (pruned to {n2})
step s1_exec1 { EXECUTE pp; }
# after s2 commits new connectivity, this must re-plan and also scan n3
step s1_exec2 { EXECUTE pp; }
teardown { DEALLOCATE pp; }

session s2
setup
{
  SET graph_path = gcp;
  SET auto_gather_graphmeta = on;
}
# committed (autocommit) new connectivity n1 -e2-> n3
step s2_addedge { CREATE (:n1)-[:e2]->(:n3); }

# s1 caches a plan pruned to {n2}; s2 commits n1->n3; s1 re-executes and must now
# return BOTH n2 and n3 (proving the cross-backend invalidation re-planned it).
permutation s1_exec1 s2_addedge s1_exec2
