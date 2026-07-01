# Concurrent DDL vs a graphmeta-pruned scan.
#
# A pruned MATCH holds AccessShareLock on the label tables it actually scans, so
# a concurrent DROP of a scanned label must block until the scanning transaction
# ends -- the pruned scan can never run against a half-dropped label.

setup
{
  CREATE GRAPH gcd;
  SET graph_path = gcd;
  CREATE VLABEL n1; CREATE VLABEL n2;
  CREATE ELABEL e1;
  CREATE (:n1)-[:e1]->(:n2);
}

teardown { DROP GRAPH gcd CASCADE; }

session s1
setup { SET graph_path = gcd; SET auto_gather_graphmeta = on; }
step s1_begin  { BEGIN; }
# pruned to e1/n2; holds AccessShareLock on the scanned label n2
step s1_scan   { MATCH (a:n1)-[b]->(c) RETURN label(c) AS c; }
step s1_commit { COMMIT; }

session s2
setup { SET graph_path = gcd; }
# needs AccessExclusiveLock on n2 -> must wait for s1's scanning txn to finish
step s2_drop { DROP VLABEL n2 CASCADE; }

permutation s1_begin s1_scan s2_drop s1_commit
