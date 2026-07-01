# Regather under a fixed transaction snapshot (REPEATABLE READ).
#
# Enabling auto_gather_graphmeta mid-transaction rebuilds ag_graphmeta by
# scanning the edge tables.  Under REPEATABLE READ that rebuild must read the
# edges under the transaction's own snapshot, not the latest committed state:
# if a concurrent session deleted and committed an edge the reader still sees,
# a latest-snapshot rebuild would omit that edge's connectivity triple, and the
# pruned MATCH -- narrowing the unlabelled endpoint to the connected labels --
# would then scan only the empty abstract parent and drop the row the RR
# snapshot is still entitled to see.

setup
{
  CREATE GRAPH gcr;
  SET graph_path = gcr;
  CREATE VLABEL a; CREATE VLABEL b;
  CREATE ELABEL e;
  CREATE (:a)-[:e]->(:b);
}

teardown { DROP GRAPH gcr CASCADE; }

session s1
setup { SET graph_path = gcr; }
step s1_begin  { BEGIN ISOLATION LEVEL REPEATABLE READ; }
# first query fixes the RR snapshot while the edge is still present
step s1_snap   { MATCH (n:a) RETURN count(*) AS n; }
# enable gathering -> regather; must read edges under s1's snapshot, not latest
step s1_gather { SET auto_gather_graphmeta = on; }
# the edge is still visible in s1's snapshot; the pruned MATCH (z is unlabelled,
# so it is narrowed to the connected label) must still reach the b vertex
step s1_match  { MATCH (x:a)-[:e]->(z) RETURN count(*) AS n; }
step s1_commit { COMMIT; }

session s2
setup { SET graph_path = gcr; }
# delete the only edge and commit, after s1 has fixed its snapshot
step s2_del    { MATCH (x:a)-[r:e]->(z:b) DELETE r; }

permutation s1_begin s1_snap s2_del s1_gather s1_match s1_commit
