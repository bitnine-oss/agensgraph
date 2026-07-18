--
-- Cypher Query Language - NEXT composite-query-chaining clause (GQL)
--
-- "Q1 NEXT Q2" makes Q1's output table the driving (input) table of Q2: a hard
-- scope-resetting boundary.  It is implemented purely in the grammar by turning
-- Q1's terminal RETURN into a WITH (the scope boundary the clause pipeline
-- already builds between clause groups) and continuing Q2's clause chain from
-- it -- so "Q1 NEXT Q2" desugars exactly to "Q1-clauses  WITH <Q1 RETURN cols>
-- Q2-clauses".  No transform or executor support is involved.  NEXT is left-
-- associative ("A NEXT B NEXT C" == "(A NEXT B) NEXT C"); each intermediate
-- RETURN becomes a WITH boundary and only the final projection stays terminal.
--
-- Because the boundary is a WITH, Q1's RETURN columns cross with their NATIVE
-- types (a vertex stays a vertex, an edge an edge, a path a path) and any
-- variable NOT in Q1's RETURN is out of scope in Q2.  Q1 MUST end with RETURN;
-- Q2 (the query after NEXT) is a fresh, standalone query part, so it must begin
-- with a query-head clause (MATCH / OPTIONAL MATCH / CREATE / MERGE / UNWIND /
-- CALL / RETURN) -- it cannot begin with WITH, WHERE, SET, DELETE, REMOVE or an
-- ORDER BY, exactly as a stand-alone Cypher statement cannot.
--
-- A set-operation composite (UNION / UNION ALL, and -- as an agensgraph
-- extension -- INTERSECT / EXCEPT) may be either operand of NEXT.  As the LEFT
-- operand ("A UNION B NEXT C") the whole union drives the next query, so an
-- aggregate after NEXT sees every union row.  As the RIGHT operand
-- ("A NEXT B UNION C") the carried table drives the union and each branch is
-- correlated per driving row, so UNION's DISTINCT is scoped per row; a branch
-- that aggregates over the whole carried table is not supported and is cleanly
-- rejected.  NEXT lives only at the three statement entry
-- points (top level, EXPLAIN, PREPARE), NOT inside a read subquery (EXISTS /
-- COUNT / CALL {}) -- both are deliberate boundaries covered below as negatives.
--
-- The query after NEXT may also begin with a row-operating clause -- WITH,
-- FILTER [WHERE], ORDER BY, SKIP / OFFSET, LIMIT -- acting on the carried table
-- (these may not begin a standalone statement, so they are reachable only after
-- NEXT).
--
-- These tests cover: basic chaining and equality against the hand-written WITH
-- form, scope reset, native-type preservation (vertex / edge / path), grouping
-- across a boundary and aggregate-then-filter, multi-chaining and left-
-- associativity, RETURN *, folding of trailing ORDER BY / SKIP / LIMIT /
-- DISTINCT into the boundary, plan-shape equality with WITH (zero overhead),
-- the guard errors (Q1 not ending in RETURN, FINISH before NEXT, unaliased
-- projection, column-count mismatch, aggregating right-operand branch), Q2 as a
-- write clause, set-operation as the left AND right operand of NEXT (incl.
-- INTERSECT / EXCEPT, both-sides composition, correlated branches, per-row
-- DISTINCT and the Phase-3 rejection), row-operating clauses after NEXT,
-- terminal endings after the last NEXT, the EXPLAIN and PREPARE entry points,
-- backward compatibility of "next" as an ordinary identifier, and the read-
-- subquery boundary.
--

-- Set up
CREATE GRAPH cypher_next;
SET graph_path = cypher_next;

-- Fixtures.  person and knows are created first and in a fixed order so the
-- label ids and local ids embedded in whole-vertex / whole-edge / whole-path
-- output below stay deterministic (person = label 3, knows = label 4).
CREATE (:person {id: 1, name: 'alice', age: 30});
CREATE (:person {id: 2, name: 'bob', age: 25});
CREATE (:person {id: 3, name: 'carol', age: 40});
CREATE (:person {id: 4, name: 'dave', age: 25});
MATCH (a:person {id: 1}), (b:person {id: 2})
CREATE (a)-[:knows {since: 2001}]->(b);
MATCH (a:person {id: 2}), (b:person {id: 3})
CREATE (a)-[:knows {since: 2010}]->(b);
MATCH (a:person {id: 1}), (b:person {id: 3})
CREATE (a)-[:knows {since: 2020}]->(b);

--
-- 1. Basic chaining and equality against the hand-written WITH boundary
--

-- The simplest chain: Q1 projects a column, Q2 re-projects it.  Q1's output
-- table is the whole input of Q2.
MATCH (n:person) RETURN n.name AS name
NEXT
RETURN name ORDER BY name;

-- Flagship equivalence: "Q1 NEXT Q2" must produce exactly the same result as
-- "Q1-clauses WITH <Q1 RETURN cols> Q2-clauses".  Here Q1 returns a vertex and
-- Q2 re-traverses from it.  The two queries below are written out side by side
-- and MUST yield identical output.
MATCH (v:person {id: 1}) RETURN v
NEXT
MATCH (v)-[:knows]->(w) RETURN w.name AS wn ORDER BY wn;

-- Equivalent hand-written WITH-boundary form -- identical result.
MATCH (v:person {id: 1})
WITH v
MATCH (v)-[:knows]->(w) RETURN w.name AS wn ORDER BY wn;

-- Filtering the carried rows in Q2.  A WITH boundary's WHERE cannot be written
-- directly after NEXT (Q2 may not start with WHERE), so the carried vertex is
-- re-bound with a no-op MATCH that then carries a WHERE -- this keeps the row
-- count 1:1 while restricting on a carried scalar.
MATCH (v:person) RETURN v, v.age AS age
NEXT
MATCH (v) WHERE age >= 30 RETURN v.name AS name ORDER BY name;

--
-- 2. Scope reset -- only Q1's RETURN columns are visible in Q2
--

-- A Q1-local variable that is NOT returned is unreferenceable in Q2.
MATCH (n:person) RETURN n.name AS nm
NEXT
RETURN n.age;

-- Likewise a Q1-local edge variable that is not carried across the boundary.
MATCH (a:person)-[e:knows]->(b) RETURN a.name AS an
NEXT
RETURN e;

-- The RETURN alias (not the original expression) is what becomes visible, and
-- it is usable in Q2 like any other binding.
MATCH (n:person {id: 1}) RETURN n.age AS a
NEXT
RETURN a + 1 AS a2;

--
-- 3. Native-type preservation across the boundary
--

-- A vertex returned by Q1 is still a vertex in Q2: it can be re-traversed and
-- passed to id() / labels().
MATCH (v:person {id: 1}) RETURN v
NEXT
MATCH (v)-[:knows]->(w)
RETURN id(v) AS vid, labels(v) AS lbls, w.name AS wn ORDER BY wn;

-- An edge returned by Q1 is still an edge in Q2: type() / properties() /
-- startnode() / endnode() all work on it.
MATCH ()-[e:knows]->() RETURN e
NEXT
RETURN type(e) AS t, properties(e) AS props,
       startnode(e).name AS sn, endnode(e).name AS en ORDER BY sn, en;

-- A path returned by Q1 is still a path in Q2: length() / nodes() /
-- relationships() all work on it.
MATCH p = (:person {id: 1})-[:knows]->(x) RETURN p
NEXT
RETURN length(p) AS len,
       [n IN nodes(p) | n.name] AS names,
       [r IN relationships(p) | type(r)] AS rels
ORDER BY names;

-- A whole path round-trips unchanged through the boundary.
MATCH p = (:person {id: 1})-[:knows]->(:person {id: 2}) RETURN p
NEXT
RETURN p;

--
-- 4. Aggregation across the boundary
--

-- Q1 aggregates into groups; Q2 consumes the grouped rows as its input table.
MATCH (n:person) RETURN n.age AS age, count(*) AS c
NEXT
RETURN age, c ORDER BY age;

-- Aggregate-then-filter: Q1 computes an out-degree per person, Q2 keeps only
-- the heavy hitters.  The group key (a unique person id) is re-matched 1:1 so
-- the filter runs over one row per group.
MATCH (s:person)-[:knows]->() RETURN s.id AS sid, count(*) AS deg
NEXT
MATCH (s:person {id: sid}) WHERE deg >= 2 RETURN sid, deg ORDER BY sid;

-- Aggregation may also happen in Q2, over the rows Q1 handed across.
MATCH (a:person)-[:knows]->(b) RETURN a.name AS an, b.name AS bn
NEXT
RETURN an, count(*) AS outdeg ORDER BY an;

--
-- 5. Multi-chaining and left-associativity
--

-- Three-deep chain: each RETURN is a boundary, only the last stays terminal.
MATCH (n:person) RETURN n.name AS a, n.age AS ag
NEXT
RETURN a, ag
NEXT
RETURN a, ag ORDER BY a;

-- Four-deep chain of pass-through projections.
MATCH (n:person) RETURN n.name AS a
NEXT
RETURN a
NEXT
RETURN a
NEXT
RETURN a ORDER BY a;

-- Left-associativity + scope reset in one shot: "A NEXT B NEXT C" groups as
-- "(A NEXT B) NEXT C".  B drops the "ag" column, so C cannot see it -- proving
-- the boundaries chain left to right and each one re-scopes.
MATCH (n:person) RETURN n.name AS a, n.age AS ag
NEXT
RETURN a
NEXT
RETURN ag;

--
-- 6. RETURN * before NEXT (pass everything through)
--

-- RETURN * carries every current binding forward as the boundary columns.
MATCH (n:person {id: 1})
WITH n.name AS nm, n.age AS ag
RETURN *
NEXT
RETURN nm, ag;

-- RETURN * also carries a whole vertex binding (with its native type) forward.
MATCH (n:person {id: 1}) RETURN *
NEXT
RETURN n.name AS nm, id(n) AS nid;

--
-- 7. Folding of trailing ORDER BY / SKIP / LIMIT / DISTINCT into the boundary
--

-- A trailing ORDER BY + LIMIT on Q1's RETURN is folded into the boundary, so
-- Q2 sees only the top rows.  Here the two oldest people feed Q2.
MATCH (n:person) RETURN n.name AS name, n.age AS age ORDER BY age DESC, name LIMIT 2
NEXT
RETURN name, age ORDER BY name;

-- Trailing SKIP + LIMIT paginate the input to Q2.
MATCH (n:person) RETURN n.name AS name ORDER BY name SKIP 1 LIMIT 2
NEXT
RETURN name ORDER BY name;

-- A trailing DISTINCT deduplicates before the boundary (two people are 25, so
-- Q2 sees three ages, not four).
MATCH (n:person) RETURN DISTINCT n.age AS age
NEXT
RETURN age ORDER BY age;

-- DISTINCT + ORDER BY + LIMIT combined and folded together.
MATCH (n:person) RETURN DISTINCT n.age AS age ORDER BY age LIMIT 2
NEXT
RETURN age ORDER BY age;

--
-- 8. Plan shape -- a NEXT query plans identically to its WITH equivalent
--

-- The boundary is a plain projection, so a NEXT query's plan is byte-identical
-- to the hand-written WITH form: zero planner overhead.  A person-only
-- projection is used so the plan stays minimal and stable.
EXPLAIN (COSTS OFF)
MATCH (n:person) RETURN n.name AS name, n.age AS age
NEXT
RETURN name ORDER BY name;

EXPLAIN (COSTS OFF)
MATCH (n:person)
WITH n.name AS name, n.age AS age
RETURN name ORDER BY name;

--
-- 9. Errors -- a query before NEXT must end with RETURN
--

-- A bare write (no terminating RETURN) before NEXT.
CREATE (:x {id: 1})
NEXT
RETURN 1;

-- A query that ends in WITH (not RETURN) before NEXT.
MATCH (n:person) WITH n.name AS nm
NEXT
RETURN nm;

-- FINISH before NEXT is rejected with its own message.
MATCH (n:person) FINISH
NEXT
RETURN 1;

-- Turning Q1's RETURN into a WITH surfaces the WITH aliasing rule: a non-Var
-- projection item before NEXT must be aliased.
MATCH (n:person) RETURN n.name
NEXT
RETURN 1;

-- An unaliased aggregate before NEXT fails the same way.
MATCH (n:person) RETURN count(*)
NEXT
RETURN 1;

--
-- 10. Set-operation as the LEFT operand of NEXT (Cypher 25)
--
-- NEXT binds looser than the set operators, so "A UNION B NEXT C" groups as
-- "(A UNION B) NEXT C": the whole union is Q1's output table and drives C, and
-- C runs over the ENTIRE union (an aggregate sees every union row).

-- Whole-union aggregation / count / collect.
RETURN 1 AS a UNION RETURN 2 AS a NEXT RETURN sum(a) AS s;
RETURN 1 AS a UNION RETURN 2 AS a NEXT RETURN count(*) AS c;
RETURN 1 AS a UNION RETURN 2 AS a NEXT RETURN collect(a) AS xs;

-- UNION eliminates Q1 duplicates; UNION ALL keeps them.
RETURN 1 AS a UNION RETURN 1 AS a NEXT RETURN count(*) AS c;
RETURN 1 AS a UNION ALL RETURN 1 AS a NEXT RETURN count(*) AS c;

-- A leading FILTER / WITH after the union operates on the carried table.
RETURN 1 AS a UNION RETURN 2 AS a NEXT FILTER a > 1 RETURN a;
RETURN 1 AS a UNION RETURN 2 AS a NEXT WITH a * 10 AS b RETURN b ORDER BY b;

-- Multi-column union carried across the boundary.
RETURN 1 AS a, 2 AS b UNION RETURN 3 AS a, 4 AS b NEXT RETURN a + b AS s ORDER BY s;

-- Chained: the union feeds C, whose RETURN then feeds a further NEXT.
RETURN 1 AS a UNION RETURN 2 AS a NEXT RETURN a * 10 AS a NEXT RETURN sum(a) AS s;

-- Over the graph: the (deduplicated) union of names drives the next query.
MATCH (n:person) RETURN n.name AS nm
UNION
MATCH (n:person) RETURN n.name AS nm
NEXT
RETURN nm ORDER BY nm;

-- INTERSECT / EXCEPT as the left operand also work; standard Cypher has only
-- UNION, so these are an agensgraph extension.
RETURN 1 AS a INTERSECT RETURN 1 AS a NEXT RETURN a;
RETURN 1 AS a EXCEPT RETURN 2 AS a NEXT RETURN a;

-- Explicit parentheses around the set operation also work; they merely group
-- the union.  (Cypher 25 rejects this parenthesized form, so accepting it is an
-- agensgraph extension -- the parentheses are transparent at the NEXT boundary.)
(MATCH (n:person) RETURN n.name AS nm
 UNION
 MATCH (n:person) RETURN n.name AS nm)
NEXT
RETURN nm ORDER BY nm;

-- A multi-branch union (three or more branches) drives the next query; the whole
-- union is one table, so an aggregate after NEXT sees every distinct row.
RETURN 1 AS a UNION RETURN 2 AS a UNION RETURN 3 AS a
NEXT
RETURN sum(a) AS s, count(*) AS c;

-- UNION ALL keeps every branch row (1, 1, 2 -> sum 4, count 3).
RETURN 1 AS a UNION ALL RETURN 1 AS a UNION ALL RETURN 2 AS a
NEXT
RETURN sum(a) AS s, count(*) AS c;

-- Output column names come from the leftmost branch, so the next query refers to
-- the carried table by that name even when a later branch aliased it differently.
-- (Cypher 25 requires all branches to share column names and rejects this; matching
-- by leftmost name / position is an agensgraph extension.)
RETURN 1 AS a UNION RETURN 2 AS b NEXT RETURN a ORDER BY a;

-- UNION eliminates duplicate NULLs (NULLs compare equal for set operations), so
-- two NULL rows collapse to one before the boundary.
RETURN NULL AS a UNION RETURN NULL AS a NEXT RETURN count(*) AS c;

-- An empty union (both branches empty) still forms a table: a count over it is 0
-- (an aggregate over no rows yields its identity, matching a plain empty input).
MATCH (p:person) WHERE p.id > 100 RETURN p.name AS nm
UNION
MATCH (p:person) WHERE p.id > 100 RETURN p.name AS nm
NEXT
RETURN count(*) AS c;

-- Native types survive the union boundary: a whole vertex carried through a UNION
-- (deduplicated by graph identity) is still a vertex in the next query and can be
-- re-traversed.  The union of persons {id 1, id 2} drives a knows-traversal.
MATCH (n:person {id: 1}) RETURN n
UNION
MATCH (n:person {id: 2}) RETURN n
NEXT
MATCH (n)-[:knows]->(w) RETURN w.name AS wn ORDER BY wn;

-- A mixed-type union unifies to the common cypher value type (no error); the two
-- distinct-typed values are both kept.  (Ordering matches Cypher 25.)
RETURN 1 AS a UNION RETURN 'x' AS a NEXT RETURN a ORDER BY a;

-- A leading ORDER BY / LIMIT after the union orders and slices the whole union.
RETURN 3 AS a UNION RETURN 1 AS a UNION RETURN 2 AS a
NEXT
ORDER BY a LIMIT 2 RETURN a ORDER BY a;

-- Negative: the union branches must have the same number of columns.
RETURN 1 AS a UNION RETURN 2 AS a, 3 AS b NEXT RETURN a;

--
-- 11. Set-operation as the RIGHT operand of NEXT (Cypher 25)
--
-- The carried table drives the union and each branch is correlated with the
-- carried columns.  Each branch runs per driving row, so UNION's DISTINCT is
-- scoped PER driving row (identical rows arising from different driving rows
-- are kept, unlike a plain whole-table UNION).

-- Both branches see the carried column; the second is a constant.
RETURN 1 AS a NEXT RETURN a UNION RETURN 2 AS a NEXT RETURN a ORDER BY a;

-- Both branches reference the carried column; deduplicated within the row.
RETURN 5 AS a NEXT RETURN a UNION RETURN a;

-- UNION ALL keeps the per-branch duplicate.
RETURN 1 AS a NEXT RETURN a AS x UNION ALL RETURN a AS x;

-- Per-row DISTINCT: two driving rows each produce the same constant on both
-- branches; the copies are NOT merged across driving rows.
UNWIND [10, 20] AS a RETURN a NEXT RETURN 99 AS x UNION RETURN 99 AS x
NEXT RETURN x ORDER BY x;

-- Both operands are set operations: (A UNION B) drives (C UNION D).
RETURN 1 AS a UNION RETURN 2 AS a NEXT RETURN a UNION RETURN 3 AS a
NEXT RETURN a ORDER BY a;

-- The union feeds a further NEXT, which may then aggregate over the whole table.
RETURN 1 AS a NEXT RETURN a AS a UNION RETURN 2 AS a NEXT RETURN sum(a) AS s;

-- Multiple carried columns are all visible to every branch; each branch picks a
-- different one, so the per-row union yields both.
RETURN 1 AS a, 2 AS b NEXT RETURN a AS x UNION RETURN b AS x NEXT RETURN x ORDER BY x;

-- A carried NULL is visible to the branches like any other value and is kept
-- (per-row, it does not merge with the other branch's non-NULL row).
RETURN NULL AS a NEXT RETURN a AS x UNION RETURN 'k' AS x NEXT RETURN x ORDER BY x;

-- An empty driving table runs no branches, so the whole result is empty (each
-- branch is correlated with the carried row, of which there are none).
MATCH (p:person) WHERE p.id > 100 RETURN p.name AS a
NEXT
RETURN a AS x UNION RETURN 'z' AS x;

-- A three-branch union after NEXT: per driving row the branches are de-duplicated
-- together (1, 1, 2 -> {1, 2}).
RETURN 1 AS a NEXT RETURN a AS x UNION RETURN a AS x UNION RETURN a + 1 AS x
NEXT RETURN x ORDER BY x;

-- INTERSECT / EXCEPT are accepted as the right operand too (an agensgraph
-- extension; standard Cypher has only UNION).  They are still scoped per driving
-- row: INTERSECT of equal branches keeps the row, of unequal branches drops it.
RETURN 1 AS a NEXT RETURN a AS x INTERSECT RETURN a AS x;
RETURN 1 AS a NEXT RETURN a AS x INTERSECT RETURN a + 1 AS x;
-- EXCEPT removes the second branch's rows from the first, per driving row.
RETURN 1 AS a NEXT RETURN a AS x EXCEPT RETURN a + 1 AS x;
RETURN 1 AS a NEXT RETURN a AS x EXCEPT RETURN a AS x;

-- Branches may run correlated MATCHes against a carried vertex.  Per person the
-- union is {friends' names} together with {own name}; identical names arising
-- from different driving rows are NOT merged (per-row DISTINCT), so "carol"
-- appears once per driving person that produces it.  This also exercises the
-- coexistence of the carried-table alias and the correlated-union alias.
MATCH (p:person) RETURN p
NEXT
MATCH (p)-[:knows]->(f) RETURN f.name AS x UNION RETURN p.name AS x
NEXT RETURN x ORDER BY x;

-- A branch may begin with any head clause, e.g. LET, and still see the carried
-- columns.
RETURN 10 AS a NEXT LET b = a * 2 RETURN b AS x UNION RETURN a AS x
NEXT RETURN x ORDER BY x;

-- Detection precision: an aggregate nested inside a subquery expression (here a
-- COUNT { ... } sublink) is NOT a branch-level aggregate, so it is allowed and
-- evaluated per driving row (the per-row union keeps the carried value and the
-- whole-graph vertex count).
RETURN 1 AS a NEXT RETURN a AS x UNION RETURN COUNT { MATCH (p:person) RETURN p } AS x
NEXT RETURN x ORDER BY x;

-- A set-operation right operand may be followed by a further NEXT that filters the
-- per-row union table (combining the row-clause-after-NEXT and right-operand
-- features).
UNWIND [1, 2, 3] AS a RETURN a
NEXT RETURN a AS x UNION RETURN a * 10 AS x
NEXT FILTER x > 5 RETURN x ORDER BY x;

-- A whole vertex may be carried through a right-operand union and re-traversed in
-- the following segment: per person the union is {friends} with {self}, and the
-- next segment follows each of those vertices' knows edges.
MATCH (p:person) RETURN p
NEXT
MATCH (p)-[:knows]->(f) RETURN f AS x UNION RETURN p AS x
NEXT
MATCH (x)-[:knows]->(z) RETURN z.name AS zn ORDER BY zn;

-- Deep composition: set operations on both sides of several NEXT boundaries in a
-- row, ending in a whole-table aggregate.
RETURN 1 AS a UNION RETURN 2 AS a
NEXT RETURN a AS x UNION RETURN 3 AS x
NEXT RETURN x AS y UNION RETURN 99 AS y
NEXT RETURN sum(y) AS z;

-- A branch that aggregates (whole-table, not per-row) is not supported.
RETURN 1 AS a UNION RETURN 2 AS a NEXT RETURN a AS x UNION RETURN count(*) AS x;

-- The aggregating-branch rejection is independent of branch order and of how many
-- branches aggregate.
RETURN 1 AS a NEXT RETURN count(*) AS x UNION RETURN a AS x;
RETURN 1 AS a NEXT RETURN count(*) AS x UNION RETURN count(*) AS x;

-- An aggregate over a CORRELATED carried column is rejected earlier, by the
-- analyzer's lateral-aggregate check (a less specific message, but still a clean
-- error -- never a silently wrong whole-table result).
RETURN 5 AS a NEXT RETURN sum(a) AS x UNION RETURN a AS x;
RETURN 1 AS a NEXT RETURN collect(a) AS x UNION RETURN a AS x;

--
-- 12. NEXT is not available inside a read subquery
--
-- Read subqueries (EXISTS / COUNT / CALL {}) use the read-statement grammar,
-- which does not include NEXT.  These document that boundary; they are syntax
-- errors, not bugs.

MATCH (n:person)
WHERE EXISTS { MATCH (n) RETURN n NEXT RETURN n }
RETURN n.name;

MATCH (n:person)
WHERE COUNT { MATCH (n) RETURN n NEXT RETURN n } > 0
RETURN n.name;

MATCH (n:person)
CALL { MATCH (m:person) RETURN m NEXT RETURN m }
RETURN n.name;

--
-- 13. Backward compatibility -- "next" is an unreserved keyword and stays
--     usable as an ordinary identifier
--

-- as a variable name
MATCH (next:person {id: 1}) RETURN next.name AS name;

-- as a label and as a property key
CREATE (:next {next: 1});
MATCH (x:next) RETURN x.next AS v;

-- as a RETURN alias
RETURN 1 AS next;

-- the NEXT clause keyword itself is case-insensitive
MATCH (n:person {id: 1}) RETURN n.name AS nm
next
RETURN nm;

--
-- 14. Entry points -- a NEXT query can be EXPLAINed and PREPAREd
--

-- (EXPLAIN is also exercised in section 8.)  PREPARE / EXECUTE round-trip.
PREPARE np AS
MATCH (n:person) RETURN n.name AS name, n.age AS age
NEXT
RETURN name ORDER BY name;
EXECUTE np;
DEALLOCATE np;

-- A set-operation LEFT operand lowers to the aggregate reading the union directly
-- (the whole union is one input table with its de-duplication machinery).
EXPLAIN (COSTS OFF)
RETURN 1 AS a UNION RETURN 2 AS a NEXT RETURN sum(a) AS s;

-- A set-operation RIGHT operand lowers to a correlated cross join: the
-- driving table (the person scan) on the outer side of a nested loop, and the
-- per-driving-row union on the inner side -- the plan shape that realizes the
-- per-row DISTINCT semantics.
EXPLAIN (COSTS OFF)
MATCH (p:person) RETURN p.name AS nm
NEXT
RETURN nm AS x UNION RETURN 'z' AS x;

-- PREPARE / EXECUTE round-trip on the set-op shapes (exercises parse-node copy /
-- output support for the carried set-operation clause).
PREPARE npl AS
RETURN 1 AS a UNION RETURN 2 AS a NEXT RETURN sum(a) AS s;
EXECUTE npl;
DEALLOCATE npl;

PREPARE npr AS
MATCH (p:person) RETURN p.name AS nm
NEXT
RETURN nm AS x UNION RETURN 'z' AS x
NEXT RETURN x ORDER BY x LIMIT 3;
EXECUTE npr;
DEALLOCATE npr;

--
-- 15. Q2 as a write clause and terminal endings after the last NEXT
--     (mutations are grouped here, last, so earlier golden output is stable)
--

-- Q2 = CREATE consuming each of Q1's rows.
MATCH (n:person) RETURN n.name AS nm
NEXT
CREATE (:tag {name: nm}) RETURN nm ORDER BY nm;
MATCH (t:tag) RETURN count(*) AS tag_count;

-- Q2 = MERGE consuming Q1's rows: four input ages, three distinct buckets.
MATCH (n:person) RETURN n.age AS age
NEXT
MERGE (:agebucket {age: age}) RETURN age ORDER BY age;
MATCH (b:agebucket) RETURN count(*) AS bucket_count;

-- Q2 = SET, reached by re-binding Q1's carried vertex with a no-op MATCH (SET
-- cannot begin a query, so it follows the reading clause).
MATCH (n:person) WHERE n.age = 25 RETURN n AS v
NEXT
MATCH (v) SET v.flag = true RETURN v.name AS name, v.flag AS flag ORDER BY name;
MATCH (n:person) WHERE n.flag = true RETURN n.name AS name ORDER BY name;

-- Q2 = a bare write (no RETURN) after the last NEXT.
MATCH (n:person) RETURN n.name AS nm
NEXT
CREATE (:tmpd {name: nm});
MATCH (d:tmpd) RETURN count(*) AS tmpd_count;

-- Q2 = DELETE consuming Q1's rows (again via a no-op re-MATCH of the carried
-- vertex), removing what the bare write just created.
MATCH (d:tmpd) RETURN d
NEXT
MATCH (d) DELETE d;
MATCH (d:tmpd) RETURN count(*) AS tmpd_remaining;

-- A write mid-Q1 followed by a RETURN, then NEXT: Q1's write happens and its
-- RETURN feeds Q2.
CREATE (a:wtmp {id: 1, name: 'w1'}) RETURN a.name AS nm
NEXT
RETURN nm;
MATCH (n:wtmp) RETURN count(*) AS wtmp_count;

-- Terminal FINISH after the last NEXT: the combined statement runs for effect
-- and returns no rows.
MATCH (n:person) RETURN n.name AS nm
NEXT
MATCH (m:person {name: nm}) FINISH;

--
-- 16. Row-operating clauses after NEXT (Cypher 25 parity)
--
-- Q2 (the query after NEXT) may begin with a projection / ordering / filter
-- clause -- WITH, FILTER [WHERE], ORDER BY, SKIP / OFFSET, LIMIT -- operating on
-- the table carried across the boundary.  These clauses may NOT begin a
-- standalone statement (that would clash with the surrounding SQL grammar);
-- they are reachable only after NEXT, so no ambiguity arises there.

-- FILTER filters the carried rows.
MATCH (p:person) RETURN p.name AS name, p.age AS age
NEXT
FILTER age >= 30
RETURN name ORDER BY name;

-- FILTER WHERE spelling is accepted too.
MATCH (p:person) RETURN p.name AS name, p.age AS age
NEXT
FILTER WHERE age = 25
RETURN name ORDER BY name;

-- WITH re-projects the carried table.
MATCH (p:person) RETURN p.age AS age
NEXT
WITH age * 2 AS doubled
RETURN doubled ORDER BY doubled;

-- ORDER BY, LIMIT and SKIP order and slice the carried table.
MATCH (p:person) RETURN p.name AS name
NEXT
ORDER BY name
RETURN name;

MATCH (p:person) RETURN p.name AS name
NEXT
ORDER BY name LIMIT 2
RETURN name;

MATCH (p:person) RETURN p.name AS name
NEXT
ORDER BY name SKIP 3
RETURN name;

-- A row clause after NEXT can conclude with RETURN and be chained again.
MATCH (p:person) RETURN p.name AS name, p.age AS age
NEXT
FILTER age = 25 RETURN name
NEXT
RETURN name ORDER BY name;

-- A bare SKIP / OFFSET or LIMIT (no ORDER BY) leads too, slicing the carried
-- table; a following count is order-independent, so the result is deterministic.
MATCH (p:person) RETURN p.name AS name NEXT OFFSET 1 RETURN count(*) AS c;
MATCH (p:person) RETURN p.name AS name NEXT SKIP 1 RETURN count(*) AS c;
MATCH (p:person) RETURN p.name AS name NEXT LIMIT 2 RETURN count(*) AS c;

-- A leading WITH may carry its own WHERE, which reaches the aggregate-then-filter
-- shape (a grouped RETURN in Q1, then WITH ... WHERE on the aggregate after NEXT)
-- that a bare NEXT could not express (Q2 cannot begin with WHERE).
MATCH (s:person)-[:knows]->() RETURN s.id AS sid, count(*) AS deg
NEXT
WITH sid, deg WHERE deg >= 2
RETURN sid, deg ORDER BY sid;

-- A leading WITH DISTINCT de-duplicates the carried table.
MATCH (p:person) RETURN p.age AS age
NEXT
WITH DISTINCT age
RETURN age ORDER BY age;

-- A leading ORDER BY can be followed by SKIP and LIMIT clauses, all applied to
-- the carried table before the final projection.
MATCH (p:person) RETURN p.name AS name, p.age AS age
NEXT
ORDER BY age DESC, name SKIP 1 LIMIT 2
RETURN name ORDER BY name;

-- Native types survive a leading row clause: a carried vertex filtered by FILTER
-- is still a vertex for id() / labels().
MATCH (v:person {id: 1}) RETURN v, v.age AS age
NEXT
FILTER age > 20
RETURN id(v) AS vid, labels(v) AS lbls;

-- Negative: bare WHERE cannot begin Q2 (WHERE is not a standalone clause).
MATCH (p:person) RETURN p.name AS name NEXT WHERE name = 'alice' RETURN name;

-- Negative: a middle segment ending in WITH (no RETURN) does not conclude a
-- query, so it cannot precede another NEXT.
MATCH (p:person) RETURN p.name AS name NEXT WITH name AS n NEXT RETURN n;

-- Tear down
DROP GRAPH cypher_next CASCADE;
