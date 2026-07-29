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
# That last failure is why the read-back steps below report the whole property
# bag, not just the number being recomputed.  A recheck that answers with a
# sibling's row writes the sibling's entire bag into the contended row, and the
# arithmetic can still land on a plausible-looking number while every other
# property has been replaced.  Each row therefore carries an identifying property
# a sibling would overwrite.
#
# Each case has one session hold an uncommitted write, a second block on it, and
# then the first commit -- so the second always takes the re-examination path.
#
# A session's connection is held open across every permutation in this file, so a
# step that changes a setting is always paired with one that puts it back --
# otherwise it would leak into the permutations that follow and make the order
# they are written in part of what they assert.
#
# Triggers on a label, and the DELETE / MERGE paths under the same contention,
# are in graph-write-trigger.spec.
#
# Not covered here: the other session DELETING the row a SET is about to write.
# That reaches the graph write with a status it does not recognise and reports an
# internal error rather than skipping the row, so there is no correct outcome to
# pin yet.
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

  -- THREE levels of inheritance, which is where the per-member substitution has
  -- to hold up: a parent-matched write reaches all three members, so one
  -- statement performs a recheck against a different relation each time, and two
  -- members must be stood in for with nothing on each of them.  Every row
  -- carries its own "who", so a recheck answered with another member's row is
  -- visible as that member's name appearing twice.
  CREATE VLABEL g1;
  CREATE VLABEL g2 INHERITS (g1);
  CREATE VLABEL g3 INHERITS (g2);
  CREATE (:g1);
  CREATE (:g2);
  CREATE (:g3);
  MATCH (n:g1) WHERE label(n) = 'g1' SET n.a = 10, n.who = 'g1';
  MATCH (n:g2) WHERE label(n) = 'g2' SET n.a = 20, n.who = 'g2';
  MATCH (n:g3) SET n.a = 30, n.who = 'g3';

  -- two rows of one label written by a single statement through two bindings, so
  -- a recheck for one of them must not disturb the other
  CREATE VLABEL mel;
  CREATE (:mel);
  MATCH (n:mel) SET n.k = 1, n.a = 100;
  CREATE (:mel);
  MATCH (n:mel) WHERE n.k IS NULL SET n.k = 2, n.a = 200;

  -- a relationship whose ends can be written together, and whose head can be
  -- written from a value read off the tail
  CREATE VLABEL lk;
  CREATE ELABEL lke;
  CREATE (:lk)-[:lke]->(:lk);
  MATCH (a:lk)-[:lke]->(b:lk) SET a.a = 1, a.role = 'head', b.a = 5, b.role = 'tail';

  -- a promoted property.  Its typed column is derived from the bag, and the
  -- re-examination rebuilds the label tuple from the re-derived element, so a
  -- stale recheck would show up in the column as well as in the bag.  The
  -- property index gives the planner an index scan and a bitmap heap scan to
  -- choose from, which are the other node shapes the element has to be traced
  -- back through.
  CREATE VLABEL prom (age int GENERATED);
  CREATE (:prom);
  MATCH (n:prom) SET n.age = 1, n.who = 'p';
  CREATE PROPERTY INDEX ON prom (age);
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
# the three-level set, contended at each level in turn, and then at all of them
step s1_bump_g1    { MATCH (n:g1) WHERE label(n) = 'g1' SET n.a = 999; }
step s1_bump_g2    { MATCH (n:g2) WHERE label(n) = 'g2' SET n.a = 999; }
step s1_bump_g3    { MATCH (n:g3) SET n.a = 999; }
step s1_bump_gall  { MATCH (n:g1) SET n.a = 999; }
# contends on one of the two rows a single statement writes
step s1_bump_mel1  { MATCH (n:mel) WHERE n.k = 1 SET n.a = 999; }
step s1_bump_mel2  { MATCH (n:mel) WHERE n.k = 2 SET n.a = 999; }
# contends on the head of the relationship, on the tail, or on both
step s1_bump_head  { MATCH (n:lk) WHERE n.role = 'head' SET n.a = 999; }
step s1_bump_tail  { MATCH (n:lk) WHERE n.role = 'tail' SET n.a = 999; }
step s1_bump_lkall { MATCH (n:lk) SET n.a = 999; }
# contends on the promoted property
step s1_bump_prom  { MATCH (n:prom) SET n.age = 999; }
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
# reaches all three members of the deep set from the root
step s2_incr_g1    { MATCH (n:g1) SET n.a = n.a + 1; }
step s2_read_g     { MATCH (n:g1) RETURN n.who AS who, n.a AS a ORDER BY who; }
# writes both rows of one label through two bindings in one statement
step s2_incr_mel   { MATCH (a:mel), (b:mel) WHERE a.k = 1 AND b.k = 2 SET a.a = a.a + 1, b.a = b.a + 1; }
step s2_read_mel   { MATCH (n:mel) RETURN n.k AS k, n.a AS a ORDER BY k; }
# writes the head from a value read off the tail: the tail is not the row being
# re-examined, so the re-run reads it as the statement's snapshot has it
step s2_head_from_tail { MATCH (a:lk)-[:lke]->(b:lk) SET a.a = b.a + 100; }
# writes both ends of the relationship
step s2_incr_ends  { MATCH (a:lk)-[:lke]->(b:lk) SET a.a = a.a + 1, b.a = b.a + 1; }
# the written element is assembled by the query rather than read by a scan, so it
# cannot be traced back to a scan to stand in at
step s2_incr_unwind { MATCH p = (a:lk)-[:lke]->(b:lk) WITH nodes(p) AS ns UNWIND ns AS n SET n.a = n.a + 1; }
step s2_read_lk    { MATCH (n:lk) RETURN n.role AS role, n.a AS a ORDER BY role; }
# the promoted property, filtered so the index can serve the qual
step s2_incr_prom  { MATCH (n:prom) WHERE n.age > 0 SET n.age = n.age + 1; }
# reads the bag and the typed column together: they must agree
step s2_read_prom  { SELECT (properties ->> 'age') AS bag, age AS col, (properties ->> 'who') AS who FROM gsr.prom; }
# node shapes the element has to be traced back through
step s2_noseq      { SET enable_seqscan = off; }
step s2_nobitmap   { SET enable_indexscan = off; SET enable_indexonlyscan = off; }
step s2_anyscan    { RESET enable_seqscan; RESET enable_indexscan; RESET enable_indexonlyscan; }
# a repeat write to one element within one clause is looked for whether or not
# accumulating it is enabled, so the lookup has to work on the re-examined path
step s2_nomulti    { SET enable_multiple_update = off; }
step s2_multi      { RESET enable_multiple_update; }
step s2_incr_twice { MATCH (a:acc), (b:acc) SET a.a = a.a + 1, b.a = b.a + 1; }
# a snapshot that does not advance cannot be re-examined against, so the conflict
# has to be reported instead of resolved
step s2_rr_begin   { BEGIN ISOLATION LEVEL REPEATABLE READ; }
step s2_rr_read    { MATCH (n:acc) RETURN n.a AS a; }
step s2_end        { COMMIT; }

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

# Three levels, contended at each one in turn.  The two members that are not
# contended must be written from their own values (11 / 21 / 31), the contended one
# must reach 1000, and all three must keep their own "who" -- a recheck answered
# with a sibling's row would both mis-compute the number and duplicate a name.
permutation s1_begin s1_bump_g1 s2_incr_g1 s1_commit s2_read_g
permutation s1_begin s1_bump_g2 s2_incr_g1 s1_commit s2_read_g
permutation s1_begin s1_bump_g3 s2_incr_g1 s1_commit s2_read_g

# All three contended at once: one statement re-examines three rows in three
# different relations, and each must come back with its own.
permutation s1_begin s1_bump_gall s2_incr_g1 s1_commit s2_read_g

# One statement writing two rows of one label, contended on either of them.  The
# uncontended row is written from its own value; only the contended one is
# re-derived.
permutation s1_begin s1_bump_mel1 s2_incr_mel s1_commit s2_read_mel
permutation s1_begin s1_bump_mel2 s2_incr_mel s1_commit s2_read_mel

# One label bound twice while writing a single end: the written end recomputes
# from the committed 999, the other end is untouched.
permutation s1_begin s1_bump_pp s2_incr_pp s1_commit s2_read_pp

# Both ends of a relationship written by one statement, contended on the head and
# then on the tail.  The contended end reaches 1000, the other is written from its
# own value (head 1 -> 2, tail 5 -> 6).
permutation s1_begin s1_bump_head s2_incr_ends s1_commit s2_read_lk
permutation s1_begin s1_bump_tail s2_incr_ends s1_commit s2_read_lk

# KNOWN LIMITATION, recorded rather than endorsed.  With BOTH ends contended, the
# statement reports a serialization failure and writes nothing, where re-examining
# each row in turn would have carried both to 1000 -- each row on its own is
# re-derivable, as the two permutations above show.  The outcome is safe: the whole
# statement aborts and both values stay at the committed 999, so nothing partial
# and nothing stale is stored.  It is the retry that is unnecessary.
permutation s1_begin s1_bump_lkall s2_incr_ends s1_commit s2_read_lk

# The head is written from a value read off the TAIL.  Only the head is
# re-examined; the tail is read again by the re-run, under the statement's own
# snapshot, so the head is computed from the tail as that snapshot has it (5) even
# when the other session has since committed a different tail.  This is the
# snapshot the re-examination runs under, recorded here so a change to it is
# noticed.
permutation s1_begin s1_bump_head s2_head_from_tail s1_commit s2_read_lk
permutation s1_begin s1_bump_lkall s2_head_from_tail s1_commit s2_read_lk

# The written element is built by the query -- collected into a path, unwound out
# of a list -- rather than read by a scan, so there is no scan to stand in at and
# the re-examination cannot reproduce it.  It must report the conflict rather than
# write values derived from the row it first read.
permutation s1_begin s1_bump_head s2_incr_unwind s1_commit s2_read_lk

# A promoted property.  The typed column is recomputed from the bag the
# re-examination derived, so bag and column must both read 1000 -- and the
# element's other properties must survive, which they only do if the label tuple
# is rebuilt from the re-examined element rather than kept from the first attempt.
permutation s1_begin s1_bump_prom s2_incr_prom s1_commit s2_read_prom

# The same, reached through an index scan and then through a bitmap heap scan, so
# the element is traced back through those nodes rather than a sequential scan.
permutation s2_noseq s1_begin s1_bump_prom s2_incr_prom s1_commit s2_read_prom s2_anyscan
permutation s2_noseq s2_nobitmap s1_begin s1_bump_prom s2_incr_prom s1_commit s2_read_prom s2_anyscan

# With accumulating repeated writes disabled, a SET still records what it wrote so
# a second write to the same element is recognised -- on the re-examined path too.
# The second binding's write is refused with a warning, and the first one still
# recomputes from the committed 999.
permutation s2_nomulti s1_begin s1_bump s2_incr_twice s1_commit s2_read s2_multi
permutation s1_begin s1_bump s2_incr_twice s1_commit s2_read

# A transaction whose snapshot does not advance cannot re-examine anything: the
# re-fetched row is not visible to it, so the conflict has to be reported.
permutation s2_rr_begin s2_rr_read s1_begin s1_bump s2_incr s1_commit s2_end s2_read

# A before-row trigger locks the row to build its NEW, so it is the trigger
# machinery that first sees the concurrent update.  It cannot re-derive a graph
# write's row, so it hands the conflict back; the write must then re-examine the
# row itself rather than dropping it or writing what it computed from the old one.
# a must reach 1000, not stay at 999 (write dropped) or become 2 (stale values).
permutation s1_begin s1_bump_trg s2_incr_trg s1_commit s2_read_trg
