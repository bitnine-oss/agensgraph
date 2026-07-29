# A graph SET must re-examine a row another session updated under it.
#
# When SET finds the row it is about to write already updated and committed by
# someone else, it re-locks the latest version and re-runs its own subplan with
# that version standing in for what the scan would read, so the values it writes
# derive from the row as it now stands.
#
# What the substitution has to name is the range table index of the SCAN that
# produced the row -- not the target label's own entry, which exists to carry the
# write permission and is never scanned.  Get that wrong and the recheck reads the
# table again under the statement snapshot and answers with the row it was meant
# to re-examine, or with a different row entirely: an inheritance set supplies one
# scan per member, so a sibling member's row can be returned and then persisted
# into the row being written, taking its whole property bag with it.
#
# Each case has one session hold an uncommitted write, a second block on it, and
# then the first commit -- so the second always takes the re-examination path.
#
# Cypher property maps are spelled with braces, which delimit a step here, so the
# patterns below use WHERE and SET instead.

setup
{
  CREATE GRAPH gsr;
  SET graph_path = gsr;

  CREATE VLABEL acc;
  CREATE (:acc);
  MATCH (n:acc) SET n.a = 1, n.k = 'keep';

  -- an inheritance set: a parent-matched SET reaches both members, so a recheck
  -- must answer with the member the written row lives in
  CREATE VLABEL par;
  CREATE VLABEL kid INHERITS (par);
  CREATE (:par);
  CREATE (:kid);
  MATCH (n:par) WHERE label(n) = 'par' SET n.a = 10, n.who = 'parent';
  MATCH (n:kid) SET n.a = 20, n.who = 'child';

  -- a label whose before-row trigger makes the trigger machinery, rather than the
  -- write itself, be what notices the concurrent update
  CREATE VLABEL trg;
  CREATE (:trg);
  MATCH (n:trg) SET n.a = 1;
  CREATE FUNCTION gsr.mark() RETURNS trigger LANGUAGE plpgsql AS $x$ BEGIN RETURN NEW; END $x$;
  CREATE TRIGGER t_mark BEFORE UPDATE ON gsr.trg FOR EACH ROW EXECUTE FUNCTION gsr.mark();

  -- one label bound twice, writing one end
  CREATE VLABEL pp;
  CREATE ELABEL rr;
  CREATE (:pp)-[:rr]->(:pp);
  MATCH (n:pp) SET n.a = 1;
}

teardown { DROP GRAPH gsr CASCADE; }

session s1
setup { SET graph_path = gsr; }
step s1_begin      { BEGIN; }
# raises the value the other session is about to compute from
step s1_bump       { MATCH (n:acc) SET n.a = 999; }
# moves the row out of the other session's filter
step s1_move       { MATCH (n:acc) SET n.k = 'moved'; }
# contends on the CHILD member of the inheritance set
step s1_bump_kid   { MATCH (n:kid) SET n.a = 999; }
# contends on the PARENT member
step s1_bump_par   { MATCH (n:par) WHERE label(n) = 'par' SET n.a = 999; }
# contends on one end of the self-bound pattern
step s1_bump_pp    { MATCH (n:pp) SET n.a = 999; }
# contends on the label carrying a before-row trigger
step s1_bump_trg   { MATCH (n:trg) SET n.a = 999; }
step s1_commit     { COMMIT; }

session s2
setup { SET graph_path = gsr; }
# derives the new value from the row it read, so a stale read is visible in it
step s2_incr       { MATCH (n:acc) SET n.a = n.a + 1; }
# only applies to a row still carrying k = 'keep'
step s2_filtered   { MATCH (n:acc) WHERE n.k = 'keep' SET n.a = 77; }
# a parent-matched write: reaches both members of the set
step s2_incr_par   { MATCH (n:par) SET n.a = n.a + 1; }
# one label bound twice, writing a single end
step s2_incr_pp    { MATCH (a:pp)-[:rr]->(b:pp) SET a.a = a.a + 1; }
step s2_read       { MATCH (n:acc) RETURN n.a AS a, n.k AS k; }
step s2_read_inh   { MATCH (n:par) RETURN n.who AS who, n.a AS a ORDER BY who; }
step s2_incr_trg   { MATCH (n:trg) SET n.a = n.a + 1; }
step s2_read_pp    { MATCH (n:pp) RETURN n.a AS a ORDER BY a; }
step s2_read_trg   { MATCH (n:trg) RETURN n.a AS a; }

# a must end at 1000: s2 has to recompute from the committed 999, not from the 1
# it originally read.
permutation s1_begin s1_bump s2_incr s1_commit s2_read

# a must stay 1 and k must be 'moved': the row left s2's filter before s2 wrote,
# so s2 must not write it at all.
permutation s1_begin s1_move s2_filtered s1_commit s2_read

# The contended row is the CHILD.  It must end at 1000 and keep who='child'; the
# parent row is written from its own value and is unaffected at 11.  Answering the
# recheck with the parent's row instead would write the parent's properties into
# the child, silently replacing who='child'.
permutation s1_begin s1_bump_kid s2_incr_par s1_commit s2_read_inh

# The same set, contended on the PARENT row: 1000 for the parent, and the child is
# written from its own value.
permutation s1_begin s1_bump_par s2_incr_par s1_commit s2_read_inh

# One label bound twice while writing a single end: the written end recomputes
# from the committed 999, the other end is untouched.
permutation s1_begin s1_bump_pp s2_incr_pp s1_commit s2_read_pp

# A before-row trigger locks the row to build its NEW, so it is the trigger
# machinery that first sees the concurrent update.  It cannot re-derive a graph
# write's row, so it hands the conflict back; the write must then re-examine the
# row itself rather than dropping it or writing what it computed from the old one.
# a must reach 1000, not stay at 999 (write dropped) or become 2 (stale values).
permutation s1_begin s1_bump_trg s2_incr_trg s1_commit s2_read_trg
