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
-- Phase 1 chains LINEAR queries only; a set-operation composite (UNION /
-- INTERSECT / EXCEPT) as a NEXT operand errors cleanly.  NEXT lives only at the
-- three statement entry points (top level, EXPLAIN, PREPARE), NOT inside a read
-- subquery (EXISTS / COUNT / CALL {}) -- both are deliberate boundaries covered
-- below as negative cases.
--
-- These tests cover: basic chaining and equality against the hand-written WITH
-- form, scope reset, native-type preservation (vertex / edge / path), grouping
-- across a boundary and aggregate-then-filter, multi-chaining and left-
-- associativity, RETURN *, folding of trailing ORDER BY / SKIP / LIMIT /
-- DISTINCT into the boundary, plan-shape equality with WITH (zero overhead),
-- the guard errors (Q1 not ending in RETURN, FINISH before NEXT, unaliased
-- projection, set-op adjacency), Q2 as a write clause, terminal endings after
-- the last NEXT, the EXPLAIN and PREPARE entry points, backward compatibility
-- of "next" as an ordinary identifier, and the read-subquery boundary.
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
-- 10. Set-operation adjacency (Phase-1 limitation, not a bug)
--

-- A set operation as the LEFT operand of NEXT.  NEXT binds looser than the set
-- operators, so "A UNION B NEXT C" groups as "(A UNION B) NEXT C".
MATCH (n:person) RETURN n.name AS nm
UNION
MATCH (n:person) RETURN n.name AS nm
NEXT
RETURN nm;

-- The same with explicit parentheses around the set operation.
(MATCH (n:person) RETURN n.name AS nm
 UNION
 MATCH (n:person) RETURN n.name AS nm)
NEXT
RETURN nm;

-- A set operation as the RIGHT operand of NEXT.
MATCH (n:person {id: 1}) RETURN n.name AS nm
NEXT
MATCH (m:person) RETURN m.name AS nm
UNION
MATCH (m:person) RETURN m.name AS nm;

--
-- 11. NEXT is not available inside a read subquery
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
-- 12. Backward compatibility -- "next" is an unreserved keyword and stays
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
-- 13. Entry points -- a NEXT query can be EXPLAINed and PREPAREd
--

-- (EXPLAIN is also exercised in section 8.)  PREPARE / EXECUTE round-trip.
PREPARE np AS
MATCH (n:person) RETURN n.name AS name, n.age AS age
NEXT
RETURN name ORDER BY name;
EXECUTE np;
DEALLOCATE np;

--
-- 14. Q2 as a write clause and terminal endings after the last NEXT
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
-- Row-operating clauses after NEXT (Cypher 25 parity)
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

-- Negative: bare WHERE cannot begin Q2 (WHERE is not a standalone clause).
MATCH (p:person) RETURN p.name AS name NEXT WHERE name = 'alice' RETURN name;

-- Negative: a middle segment ending in WITH (no RETURN) does not conclude a
-- query, so it cannot precede another NEXT.
MATCH (p:person) RETURN p.name AS name NEXT WITH name AS n NEXT RETURN n;

-- Tear down
DROP GRAPH cypher_next CASCADE;
