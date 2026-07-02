# Regather under a fixed transaction snapshot (SERIALIZABLE).
#
# Companion to graphmeta_prune_regather (REPEATABLE READ): both isolation levels
# take a transaction-wide snapshot, so regather must read the edge tables under
# that snapshot rather than the latest committed state.  This variant confirms
# the SERIALIZABLE path specifically -- the rebuild still sees the edge the
# concurrent session deleted+committed (so the pruned MATCH keeps the row), and
# the transaction is not spuriously aborted by SSI: s1 only reads the edge and
# rewrites ag_graphmeta while s2 deletes the edge, which is a single rw-conflict,
# not a dangerous structure.

setup
{
  CREATE GRAPH gcs;
  SET graph_path = gcs;
  CREATE VLABEL a; CREATE VLABEL b;
  CREATE ELABEL e;
  CREATE (:a)-[:e]->(:b);
}

teardown { DROP GRAPH gcs CASCADE; }

session s1
setup { SET graph_path = gcs; }
step s1_begin  { BEGIN ISOLATION LEVEL SERIALIZABLE; }
# first query fixes the SERIALIZABLE snapshot while the edge is still present
step s1_snap   { MATCH (n:a) RETURN count(*) AS n; }
# enable gathering -> regather; must read edges under s1's snapshot, not latest
step s1_gather { SET auto_gather_graphmeta = on; }
# the edge is still visible in s1's snapshot; the pruned MATCH (z is unlabelled,
# so it is narrowed to the connected label) must still reach the b vertex
step s1_match  { MATCH (x:a)-[:e]->(z) RETURN count(*) AS n; }
step s1_commit { COMMIT; }

session s2
setup { SET graph_path = gcs; }
# delete the only edge and commit, after s1 has fixed its snapshot
step s2_del    { MATCH (x:a)-[r:e]->(z:b) DELETE r; }

permutation s1_begin s1_snap s2_del s1_gather s1_match s1_commit
