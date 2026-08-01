# A graph write whose row another session deleted must report a conflict a client
# can act on.
#
# When a write finds the row it is about to change already deleted and committed
# by someone else, there is nothing left to write.  Below an isolation level that
# holds one snapshot for the whole transaction that is not an error at all -- the
# statement simply affects one row fewer, which is what the same interleaving does
# to a plain SQL UPDATE or DELETE.  At REPEATABLE READ and above the write cannot
# be retried against a version the transaction is not allowed to see, so the
# conflict belongs to the client and has to arrive as a serialization failure it
# will recognise and retry.
#
# What must not happen -- and is what these cases pin -- is the write reporting an
# internal error.  A status the write path does not recognise reaches a bare elog,
# which produces XX000: no retry loop matches it, so a conflict that is ordinary
# and recoverable is delivered to the application as a bug in the server.
#
# The delete path has a second way in.  When a label carries a before-row delete
# trigger, it is the trigger machinery that locks the row and so the trigger
# machinery that notices it is gone; the write then sees only that the trigger
# declined the row.  Blaming the trigger there names the wrong cause, and for a
# required element it reports that a trigger refused a delete that no trigger ever
# saw.
#
# Each case has one session hold an uncommitted delete, a second block on it, and
# then the first commit -- so the second always reaches the write with the row
# already gone.
#
# Cypher property maps are spelled with braces, which delimit a step here, so the
# patterns below use WHERE and SET instead.

setup
{
  CREATE GRAPH gwd;
  SET graph_path = gwd;

  -- the row a SET is about to write
  CREATE VLABEL acc;
  CREATE (:acc);
  MATCH (n:acc) SET n.a = 1;

  -- the row a DELETE is about to remove
  CREATE VLABEL dd;
  CREATE (:dd);
  MATCH (n:dd) SET n.a = 1;

  -- a label whose before-row delete trigger, not the write itself, is what
  -- notices the row is gone
  CREATE VLABEL trg;
  CREATE (:trg);
  MATCH (n:trg) SET n.a = 1;
  CREATE FUNCTION gwd.keep() RETURNS trigger LANGUAGE plpgsql AS $x$ BEGIN RETURN OLD; END $x$;
  CREATE TRIGGER t_keep BEFORE DELETE ON gwd.trg FOR EACH ROW EXECUTE FUNCTION gwd.keep();

  -- MERGE reaches the write through the eager path, which has no re-examination
  -- of its own and so must fail closed rather than report an internal error
  CREATE VLABEL mrg;
  CREATE (:mrg);
  MATCH (n:mrg) SET n.a = 1;

  -- a promoted property, whose typed column is the only place a lost write would
  -- show up once the bag no longer carries the value
  CREATE VLABEL prom (age int GENERATED);
  CREATE (:prom);
  MATCH (n:prom) SET n.age = 1;
}

teardown { DROP GRAPH gwd CASCADE; }

session s1
setup { SET graph_path = gwd; }
step s1_begin        { BEGIN; }
step s1_del_acc      { MATCH (n:acc) DELETE n; }
step s1_del_dd       { MATCH (n:dd) DELETE n; }
step s1_del_trg      { MATCH (n:trg) DELETE n; }
step s1_del_mrg      { MATCH (n:mrg) DELETE n; }
step s1_del_prom     { MATCH (n:prom) DELETE n; }
step s1_commit       { COMMIT; }

session s2
setup { SET graph_path = gwd; }
step s2_begin        { BEGIN; }
step s2_begin_rr     { BEGIN ISOLATION LEVEL REPEATABLE READ; }
# a repeatable-read transaction has to take its snapshot before the other session
# commits, or it never sees the row at all and there is no conflict to report
step s2_snapshot     { MATCH (n:acc) RETURN n.a; }
step s2_set_acc      { MATCH (n:acc) SET n.a = 2 RETURN n.a; }
step s2_del_dd       { MATCH (n:dd) DELETE n; }
step s2_del_trg      { MATCH (n:trg) DELETE n; }
step s2_merge_mrg    { MERGE (n:mrg) ON MATCH SET n.a = 2 RETURN n.a; }
step s2_set_prom     { MATCH (n:prom) SET n.age = 2 RETURN n.age; }
step s2_commit       { COMMIT; }

# READ COMMITTED: the row is gone, so the write affects nothing and says so
permutation s1_begin s1_del_acc s2_begin s2_set_acc s1_commit s2_commit
permutation s1_begin s1_del_dd s2_begin s2_del_dd s1_commit s2_commit
permutation s1_begin s1_del_prom s2_begin s2_set_prom s1_commit s2_commit

# the trigger path: the row is gone, which is not a trigger refusing the delete
permutation s1_begin s1_del_trg s2_begin s2_del_trg s1_commit s2_commit

# the eager path a MERGE takes fails closed
permutation s1_begin s1_del_mrg s2_begin s2_merge_mrg s1_commit s2_commit

# REPEATABLE READ: the write cannot be retried against a version it may not see,
# so the conflict is the client's to resolve
permutation s2_begin_rr s2_snapshot s1_begin s1_del_acc s1_commit s2_set_acc s2_commit
