--
-- Cypher CALL { subquery } clause
--
-- CALL runs a subquery as a clause in the query pipeline.  Outer variables are
-- imported into the subquery either by a scope clause -- "CALL (x, y) { ... }"
-- (correlated) / "CALL () { ... }" (uncorrelated) -- or by an equivalent
-- leading importing WITH -- "CALL { WITH x, y ... }"; with no import the
-- subquery is uncorrelated ("CALL { ... }").
--
-- The tests are grouped by the ways a CALL clause is supported:
--   1. at the top level of a query
--   2. nested inside another CALL body
--   3. inside an expression subquery (EXISTS / COUNT / COLLECT / ARRAY / VALUE / IN)
--   4. inside a cypher query embedded in a SQL FROM clause
-- followed by mixed combinations and the CALL-as-identifier (CALL_LA) boundary.
--

CREATE GRAPH cypher_call;
SET graph_path = cypher_call;

-- Graph: three people.  Andy(36) has one dog; Timothy(25) has a cat and no dog;
-- Peter(35) has two dogs, Fido (which has a toy) and Ozzy.  KNOWS edges:
-- Andy -> Peter, Andy -> Timothy, Peter -> Timothy (Andy knows 2, Peter 1,
-- Timothy 0), which gives a per-person correlated subquery to exercise.
CREATE (andy:Person {name: 'Andy', age: 36}),
       (timothy:Person {name: 'Timothy', age: 25}),
       (peter:Person {name: 'Peter', age: 35}),
       (andy)-[:HAS_DOG]->(:Dog {name: 'Andy'}),
       (timothy)-[:HAS_CAT]->(:Cat {name: 'Mittens'}),
       (fido:Dog {name: 'Fido'})<-[:HAS_DOG]-(peter)-[:HAS_DOG]->(:Dog {name: 'Ozzy'}),
       (fido)-[:HAS_TOY]->(:Toy {name: 'Banana'}),
       (andy)-[:KNOWS]->(peter),
       (andy)-[:KNOWS]->(timothy),
       (peter)-[:KNOWS]->(timothy);

--
-- 1. Top-level CALL
--

-- 1.1 the four import / correlation forms ----------------------------------

-- (a) scope clause, correlated on the named variable
MATCH (p:Person)
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog ORDER BY name, dog;
-- plan: the correlated body decorrelates to the very join an inline pattern
-- match produces (the subquery is pulled up, not re-executed per outer row)
EXPLAIN (COSTS OFF)
MATCH (p:Person) CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog;
EXPLAIN (COSTS OFF)
MATCH (p:Person)-[:HAS_DOG]->(d:Dog) RETURN p.name AS name, d.name AS dog;
-- (b) empty scope clause, uncorrelated (runs once, cross-joined)
MATCH (p:Person {name: 'Andy'})
CALL () { MATCH (d:Dog) RETURN count(*) AS ndogs }
RETURN p.name AS name, ndogs;
-- plan: the uncorrelated body runs once (an Aggregate evaluated a single time),
-- then is cross-joined with the outer
EXPLAIN (COSTS OFF)
MATCH (p:Person) CALL () { MATCH (:Toy) RETURN count(*) AS ntoys }
RETURN p.name AS name, ntoys;
-- (c) no scope clause, also uncorrelated
MATCH (p:Person {name: 'Andy'})
CALL { MATCH (:Toy) RETURN count(*) AS ntoys }
RETURN p.name AS name, ntoys;
-- (d) importing WITH, correlated -- equivalent to (a)
MATCH (p:Person)
CALL { WITH p MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog ORDER BY name, dog;
-- (a) and (d) are exactly equivalent
MATCH (p:Person)
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog
EXCEPT
MATCH (p:Person)
CALL { WITH p MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog;
-- the scope clause is case-insensitive like any keyword (lower-case call)
MATCH (p:Person {name: 'Peter'})
call (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog ORDER BY dog;
-- whitespace / newlines between CALL and its '(' or '{' do not matter
MATCH (p:Person {name: 'Andy'})
CALL
  (p)
  { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog;
-- (e) star scope clause: import every outer variable -- here equivalent to (a)
MATCH (p:Person)
CALL (*) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog ORDER BY name, dog;
-- plan: CALL (*) imports the same outer variable as (a) and decorrelates to the
-- identical join
EXPLAIN (COSTS OFF)
MATCH (p:Person) CALL (*) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog;
-- with several outer variables, CALL (*) makes all of them visible in the body
MATCH (p:Person {name: 'Andy'}), (f:Person {name: 'Peter'})
CALL (*) { MATCH (p)-[:KNOWS]->(f) RETURN 'knows' AS rel }
RETURN p.name AS name, f.name AS friend, rel;
-- error: CALL (*) with no preceding clause has nothing to import
CALL (*) { MATCH (d:Dog) RETURN d.name AS dog } RETURN dog;

-- 1.2 imported targets: vertex, several vars, edge, computed value ----------

-- three imported variables
MATCH (p:Person {name: 'Andy'}), (f:Person {name: 'Peter'}), (g:Person {name: 'Timothy'})
CALL (p, f, g) { MATCH (p)-[:KNOWS]->(x:Person) WHERE x.name IN [f.name, g.name]
                 RETURN count(x) AS known }
RETURN p.name AS name, known;
-- import an edge variable and use it in the body
MATCH (p:Person {name: 'Andy'})-[r:KNOWS]->(:Person)
CALL (r) { RETURN type(r) AS rel_type }
RETURN DISTINCT rel_type;
-- import a value computed by a preceding WITH
MATCH (p:Person)
WITH p, p.age AS age
CALL (age) { RETURN (age >= 35) AS is_senior }
RETURN p.name AS name, is_senior ORDER BY name;
-- import a variable but use only some of the imported set
MATCH (p:Person {name: 'Peter'}), (q:Person {name: 'Andy'})
CALL (p, q) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog ORDER BY dog;

-- 1.3 body shapes -----------------------------------------------------------

-- aggregation: exactly one row per outer row, including zero
MATCH (p:Person)
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN count(d) AS ndogs }
RETURN p.name AS name, ndogs ORDER BY name;
-- plan: an aggregating body cannot be pulled up, so it stays a per-outer
-- lateral join (an Aggregate under a Nested Loop), driven by the edge's
-- start-key index.  Scans are forced here so the inner access does not flip
-- between an index and a bitmap scan with table size.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF)
MATCH (p:Person) CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN count(d) AS ndogs }
RETURN p.name AS name, ndogs;
RESET enable_bitmapscan;
RESET enable_seqscan;
-- several aggregates at once
MATCH (p:Person)
CALL (p) { MATCH (p)-[:KNOWS]->(f:Person) RETURN count(f) AS nfriends, min(f.age) AS youngest }
RETURN p.name AS name, nfriends, youngest ORDER BY name;
-- DISTINCT over real duplicate-producing data
MATCH (p:Person {name: 'Andy'})
CALL (p) { MATCH (p)-[:KNOWS]->(:Person)-[:KNOWS]->(fof:Person) RETURN DISTINCT fof.name AS f }
RETURN p.name AS name, f ORDER BY f;
-- multiple RETURN columns from the body
MATCH (p:Person {name: 'Peter'})
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog, d.name + '!' AS shout }
RETURN dog, shout ORDER BY dog;
-- ORDER BY DESC + LIMIT (per outer row)
MATCH (p:Person {name: 'Peter'})
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog ORDER BY d.name DESC LIMIT 1 }
RETURN dog;
-- LIMIT 0 (drops every outer row)
MATCH (p:Person)
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog LIMIT 0 }
RETURN p.name AS name, dog;
-- SKIP past the end (drops the outer row)
MATCH (p:Person)
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog ORDER BY d.name SKIP 5 }
RETURN p.name AS name, dog;
-- UNION inside the body
MATCH (p:Person {name: 'Timothy'})
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS x
           UNION MATCH (p)-[:HAS_CAT]->(c:Cat) RETURN c.name AS x }
RETURN p.name AS name, x ORDER BY x;
-- UNION ALL inside the body keeps duplicates
MATCH (p:Person {name: 'Peter'})
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN 1 AS one
           UNION ALL MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN 1 AS one }
RETURN count(one) AS c;
-- EXCEPT inside the body
MATCH (p:Person {name: 'Peter'})
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS x
           EXCEPT MATCH (:Dog {name: 'Ozzy'}) RETURN 'Ozzy' AS x }
RETURN p.name AS name, x ORDER BY x;
-- OPTIONAL MATCH inside the body (keeps the row, yields null)
MATCH (p:Person)
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) OPTIONAL MATCH (d)-[:HAS_TOY]->(t:Toy)
           RETURN d.name AS dog, t.name AS toy }
RETURN p.name AS name, dog, toy ORDER BY name, dog;
-- UNWIND inside the body
MATCH (p:Person {name: 'Andy'})
CALL (p) { UNWIND [1, 2, 3] AS i RETURN i }
RETURN p.name AS name, i ORDER BY i;
-- a multi-clause body (MATCH ... WITH ... WHERE ... RETURN)
MATCH (p:Person)
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) WITH d WHERE d.name <> 'Ozzy' RETURN d.name AS dog }
RETURN p.name AS name, dog ORDER BY name, dog;
-- the body returns a whole vertex
MATCH (p:Person {name: 'Andy'})
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d }
RETURN p.name AS name, (d).name AS dog;

-- 1.4 cardinality and dataflow ----------------------------------------------

-- returning body: outer x inner; a zero-match outer row is dropped
MATCH (p:Person)
CALL (p) { MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name AS friend }
RETURN p.name AS name, friend ORDER BY name, friend;
-- the RETURN columns flow to the clauses that follow the CALL
MATCH (p:Person)
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
WITH p, dog WHERE dog STARTS WITH 'F'
RETURN p.name AS name, dog;
-- ... and can be aggregated by a later clause
MATCH (p:Person)
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN count(dog) AS total_dogs;
-- ... and ordered / sliced by trailing standalone clauses
MATCH (p:Person)
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog ORDER BY dog DESC LIMIT 2;
-- a chain of three CALLs
MATCH (p:Person {name: 'Peter'})
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d AS dog }
CALL (dog) { MATCH (dog)-[:HAS_TOY]->(t:Toy) RETURN t.name AS toy }
CALL () { MATCH (:Person) RETURN count(*) AS npeople }
RETURN (dog).name AS dog, toy, npeople ORDER BY dog;
-- CALL at the head of the query (no preceding clause)
CALL () { MATCH (d:Dog) RETURN d.name AS dog ORDER BY dog }
RETURN dog;
-- CALL at the head, then more clauses
CALL () { MATCH (p:Person) RETURN p.name AS name }
WITH name WHERE name <> 'Timothy'
RETURN name ORDER BY name;
-- an empty outer (no rows) yields no CALL invocations
MATCH (p:Person {name: 'Nobody'})
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog;

-- 1.5 scoping: only imported variables are visible inside the body ----------

-- the imported variable is usable in the body's RETURN as well as its pattern
MATCH (p:Person {name: 'Peter'})
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN p.name AS owner, d.name AS dog }
RETURN owner, dog ORDER BY dog;
-- error: a non-imported outer variable referenced in the body
MATCH (p:Person {name: 'Andy'}), (other:Person {name: 'Peter'})
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) WHERE d.name <> other.name RETURN d.name AS dog }
RETURN dog;
-- error: an uncorrelated CALL cannot see outer variables
MATCH (p:Person {name: 'Andy'})
CALL () { MATCH (d:Dog) WHERE d.name = p.name RETURN d.name AS dog }
RETURN dog;
-- error: a no-scope-clause CALL cannot see outer variables either
MATCH (p:Person {name: 'Andy'})
CALL { MATCH (d:Dog) WHERE d.name = p.name RETURN d.name AS dog }
RETURN dog;
-- error: importing a variable that does not exist
MATCH (p:Person {name: 'Andy'})
CALL (nosuchvar) { MATCH (d:Dog) RETURN d.name AS dog }
RETURN dog;
-- error: the importing-WITH form restricts scope the same way
MATCH (p:Person {name: 'Andy'}), (other:Person {name: 'Peter'})
CALL { WITH p MATCH (p)-[:HAS_DOG]->(d:Dog) WHERE d.name <> other.name RETURN d.name AS dog }
RETURN dog;
-- a variable reused inside the body shadows nothing of the outer (uncorrelated)
MATCH (p:Person {name: 'Andy'})
CALL () { MATCH (p:Person {name: 'Peter'})-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS outer_name, dog ORDER BY dog;

-- 1.6 errors ----------------------------------------------------------------

-- a write clause is not allowed in a CALL subquery (each write kind)
MATCH (p:Person) CALL (p) { CREATE (:Foo {n: p.name}) RETURN 1 AS one } RETURN p.name;
MATCH (p:Person) CALL (p) { SET p.x = 1 RETURN p.name AS n } RETURN p.name;
MATCH (p:Person) CALL (p) { DETACH DELETE p RETURN 1 AS one } RETURN p.name;
MATCH (p:Person) CALL (p) { MERGE (:Foo {n: p.name}) RETURN 1 AS one } RETURN p.name;
MATCH (p:Person) CALL (p) { REMOVE p.age RETURN p.name AS n } RETURN p.name;
-- the subquery must return at least one variable
MATCH (p:Person) CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) FINISH } RETURN p.name;
-- a scope clause with no preceding clause to import from
CALL (p) { MATCH (d:Dog) RETURN d.name AS dog } RETURN dog;

--
-- 2. Nested CALL (a CALL inside another CALL's body)
--

-- scope-clause inner CALL
MATCH (p:Person {name: 'Peter'})
CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog)
  CALL (d) { MATCH (d)-[:HAS_TOY]->(t:Toy) RETURN t.name AS toy }
  RETURN d.name AS dog, toy
}
RETURN p.name AS name, dog, toy ORDER BY dog;
-- plan: both CALL levels collapse into a single join tree -- no nested
-- per-row subquery
EXPLAIN (COSTS OFF)
MATCH (p:Person {name: 'Peter'})
CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog)
  CALL (d) { MATCH (d)-[:HAS_TOY]->(t:Toy) RETURN t.name AS toy }
  RETURN d.name AS dog, toy
}
RETURN p.name AS name, dog, toy;
-- importing-WITH inner CALL
MATCH (p:Person {name: 'Peter'})
CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog)
  CALL { WITH d MATCH (d)-[:HAS_TOY]->(t:Toy) RETURN t.name AS toy }
  RETURN d.name AS dog, toy
}
RETURN p.name AS name, dog, toy ORDER BY dog;
-- uncorrelated inner CALL
MATCH (p:Person {name: 'Peter'})
CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog)
  CALL () { MATCH (:Toy) RETURN count(*) AS ntoys }
  RETURN d.name AS dog, ntoys
}
RETURN p.name AS name, dog, ntoys ORDER BY dog;
-- two correlated levels with an aggregating inner CALL (zero kept by aggregation)
MATCH (p:Person {name: 'Peter'})
CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog)
  CALL (d) { MATCH (d)-[:HAS_TOY]->(t:Toy) RETURN count(t) AS ntoys }
  RETURN d.name AS dog, ntoys
}
RETURN p.name AS name, dog, ntoys ORDER BY dog;
-- three levels of nesting
MATCH (p:Person {name: 'Peter'})
CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog)
  CALL (d) {
    MATCH (d)-[:HAS_TOY]->(t:Toy)
    CALL (t) { RETURN t.name AS tn }
    RETURN tn
  }
  RETURN d.name AS dog, tn
}
RETURN dog, tn ORDER BY dog;
-- two sibling CALLs inside one body
MATCH (p:Person {name: 'Peter'})
CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog)
  CALL (d) { MATCH (d)-[:HAS_TOY]->(t:Toy) RETURN count(t) AS ntoys }
  CALL (p) { MATCH (p)-[:KNOWS]->(f:Person) RETURN count(f) AS nfriends }
  RETURN d.name AS dog, ntoys, nfriends
}
RETURN dog, ntoys, nfriends ORDER BY dog;
-- nested CALL across all outer rows (only Peter's Fido reaches a toy)
MATCH (p:Person)
CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog)
  CALL (d) { MATCH (d)-[:HAS_TOY]->(t:Toy) RETURN t.name AS toy }
  RETURN d.name AS dog, toy
}
RETURN p.name AS name, dog, toy ORDER BY name, dog;
-- the inner CALL imports an outer-outer variable (transitive correlation)
MATCH (p:Person {name: 'Andy'})
CALL (p) {
  MATCH (p)-[:KNOWS]->(f:Person)
  CALL (p, f) { MATCH (p)-[:KNOWS]->(f) RETURN 'knows' AS rel }
  RETURN f.name AS friend, rel
}
RETURN friend, rel ORDER BY friend;

--
-- 3. CALL inside an expression subquery
--

-- EXISTS / NOT EXISTS
MATCH (p:Person)
WHERE EXISTS { MATCH (p)-[:HAS_DOG]->(d:Dog)
              CALL (d) { MATCH (d)-[:HAS_TOY]->(t:Toy) RETURN t } RETURN d }
RETURN p.name AS name ORDER BY name;
-- plan: the EXISTS becomes a SubPlan; the CALL inside it decorrelates into
-- that subplan's join (no further per-row nesting for the CALL)
EXPLAIN (COSTS OFF)
MATCH (p:Person)
WHERE EXISTS { MATCH (p)-[:HAS_DOG]->(d:Dog)
              CALL (d) { MATCH (d)-[:HAS_TOY]->(t:Toy) RETURN t } RETURN d }
RETURN p.name AS name;
MATCH (p:Person)
WHERE NOT EXISTS { MATCH (p)-[:HAS_DOG]->(d:Dog)
                   CALL (d) { MATCH (d)-[:HAS_TOY]->(t:Toy) RETURN t } RETURN d }
RETURN p.name AS name ORDER BY name;
-- EXISTS with a CALL, used in RETURN
MATCH (p:Person)
RETURN p.name AS name,
       EXISTS { MATCH (p)-[:HAS_DOG]->(d:Dog)
                CALL (d) { MATCH (d)-[:HAS_TOY]->(t:Toy) RETURN t } RETURN d } AS has_toy_dog
ORDER BY name;
-- COUNT with a CALL, plain and compared
MATCH (p:Person {name: 'Peter'})
RETURN COUNT { MATCH (p)-[:HAS_DOG]->(d:Dog)
               CALL (d) { MATCH (d) RETURN d.name AS n } RETURN n } AS c;
MATCH (p:Person)
WHERE COUNT { MATCH (p)-[:HAS_DOG]->(d:Dog) CALL (d) { MATCH (d) RETURN d } RETURN d } > 1
RETURN p.name AS name ORDER BY name;
-- COLLECT / ARRAY with a CALL
MATCH (p:Person {name: 'Peter'})
RETURN COLLECT { MATCH (p)-[:HAS_DOG]->(d:Dog)
                 CALL (d) { MATCH (d) RETURN d.name AS n } RETURN n ORDER BY n } AS dogs;
MATCH (p:Person {name: 'Peter'})
RETURN ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog)
               CALL (d) { MATCH (d) RETURN d.name AS n } RETURN n ORDER BY n } AS dogs;
-- VALUE with a CALL (match and no-match)
MATCH (p:Person {name: 'Peter'})
RETURN VALUE { MATCH (p)-[:HAS_DOG]->(d:Dog {name: 'Fido'})
               CALL (d) { MATCH (d)-[:HAS_TOY]->(t:Toy) RETURN t.name AS toy } RETURN toy } AS v;
MATCH (p:Person {name: 'Andy'})
RETURN VALUE { MATCH (p)-[:HAS_DOG]->(d:Dog)
               CALL (d) { MATCH (d)-[:HAS_TOY]->(t:Toy) RETURN t.name AS toy } RETURN toy } AS v;
-- IN / NOT IN with a CALL
MATCH (p:Person {name: 'Peter'})
RETURN 'Banana' IN { MATCH (p)-[:HAS_DOG]->(d:Dog)
                     CALL (d) { MATCH (d)-[:HAS_TOY]->(t:Toy) RETURN t.name AS toy } RETURN toy } AS yes;
MATCH (p:Person {name: 'Peter'})
RETURN 'Bone' NOT IN { MATCH (p)-[:HAS_DOG]->(d:Dog)
                       CALL (d) { MATCH (d)-[:HAS_TOY]->(t:Toy) RETURN t.name AS toy } RETURN toy } AS yes;
-- the importing-WITH form also works inside an expression subquery
MATCH (p:Person {name: 'Peter'})
RETURN COLLECT { MATCH (p)-[:HAS_DOG]->(d:Dog)
                 CALL { WITH d MATCH (d) RETURN d.name AS n } RETURN n ORDER BY n } AS dogs;
-- a CALL whose body itself contains an expression subquery, all inside EXISTS
MATCH (p:Person)
WHERE EXISTS {
  MATCH (p)-[:HAS_DOG]->(d:Dog)
  CALL (d) { MATCH (d) WHERE EXISTS { MATCH (d)-[:HAS_TOY]->(:Toy) } RETURN d.name AS n }
  RETURN n
}
RETURN p.name AS name ORDER BY name;

--
-- 4. CALL inside a cypher query embedded in a SQL FROM clause
--

-- scope-clause form
SELECT t.name, t.dog FROM (
  MATCH (p:Person) CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
  RETURN p.name AS name, dog
) t ORDER BY t.name, t.dog;
-- plan: the CALL inside the FROM-embedded cypher query decorrelates to a plain
-- join, just as it does at the top level
EXPLAIN (COSTS OFF)
SELECT t.name, t.dog FROM (
  MATCH (p:Person) CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
  RETURN p.name AS name, dog
) t;
-- importing-WITH form
SELECT t.name, t.n FROM (
  MATCH (p:Person) CALL { WITH p MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN count(d) AS n }
  RETURN p.name AS name, n
) t ORDER BY t.name;
-- a SQL aggregate over the CALL result
SELECT count(*) AS rows FROM (
  MATCH (p:Person) CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
  RETURN p.name AS name, dog
) t;
-- a SQL WHERE over the CALL result (cypher values surface as jsonb, so the
-- string is compared against a jsonb literal)
SELECT t.name, t.dog FROM (
  MATCH (p:Person) CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
  RETURN p.name AS name, dog
) t WHERE t.dog = '"Fido"';
-- a SQL GROUP BY over the CALL result
SELECT t.name, count(*) AS ndogs FROM (
  MATCH (p:Person) CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
  RETURN p.name AS name, dog
) t GROUP BY t.name ORDER BY t.name;
-- a SQL JOIN between two cypher CALL queries
SELECT a.name, a.dog, b.nfriends FROM (
  MATCH (p:Person) CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
  RETURN p.name AS name, dog
) a JOIN (
  MATCH (p:Person) CALL (p) { MATCH (p)-[:KNOWS]->(f:Person) RETURN count(f) AS nfriends }
  RETURN p.name AS name, nfriends
) b ON a.name = b.name
ORDER BY a.name, a.dog;
-- a CALL query inside a SQL CTE
WITH cte AS (
  MATCH (p:Person) CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
  RETURN p.name AS name, dog
)
SELECT name, dog FROM cte ORDER BY name, dog;
-- a nested CALL inside a FROM-embedded cypher query
SELECT t.name, t.dog, t.toy FROM (
  MATCH (p:Person)
  CALL (p) {
    MATCH (p)-[:HAS_DOG]->(d:Dog)
    CALL (d) { MATCH (d)-[:HAS_TOY]->(x:Toy) RETURN x.name AS toy }
    RETURN d.name AS dog, toy
  }
  RETURN p.name AS name, dog, toy
) t ORDER BY t.name, t.dog;

--
-- 5. Mixed / multiple subqueries / odd combinations
--

-- two CALL clauses in one pipeline (the second aggregates per surviving row)
MATCH (p:Person {name: 'Peter'})
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
CALL (p) { MATCH (p)-[:HAS_DOG]->(d2:Dog) RETURN count(d2) AS ndogs }
RETURN p.name AS name, dog, ndogs ORDER BY dog;
-- plan: the first (returning) CALL decorrelates into the join; the second
-- (aggregating) CALL stays a per-outer lateral join on top.  Scans forced so
-- the inner access does not vary with table size.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF)
MATCH (p:Person {name: 'Peter'})
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
CALL (p) { MATCH (p)-[:HAS_DOG]->(d2:Dog) RETURN count(d2) AS ndogs }
RETURN p.name AS name, dog, ndogs;
RESET enable_bitmapscan;
RESET enable_seqscan;
-- a CALL result alongside several other subquery kinds in one projection
MATCH (p:Person)
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN count(d) AS ndogs }
RETURN p.name AS name, ndogs,
       EXISTS { MATCH (p)-[:HAS_CAT]->(:Cat) } AS has_cat,
       COUNT { MATCH (p)-[:KNOWS]->(:Person) } AS nfriends,
       COLLECT { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS dogs
ORDER BY name;
-- a CALL combined with UNWIND and FOR
MATCH (p:Person {name: 'Peter'})
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
UNWIND [1, 2] AS n
RETURN dog, n ORDER BY dog, n;
-- an EXISTS nested in a CALL nested in an EXISTS
MATCH (p:Person)
WHERE EXISTS {
  MATCH (p)-[:HAS_DOG]->(d:Dog)
  CALL (d) { MATCH (d) WHERE EXISTS { MATCH (d)-[:HAS_TOY]->(:Toy) } RETURN d.name AS n }
  RETURN n
}
RETURN p.name AS name ORDER BY name;
-- a cypher CALL query (aggregating body) consumed and ordered by SQL
SELECT t.name, t.ndogs FROM (
  MATCH (p:Person)
  CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN count(d) AS ndogs }
  RETURN p.name AS name, ndogs
) t ORDER BY t.name;
-- a CALL feeding a CALL where the second imports the first's output column
MATCH (p:Person)
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d AS dog }
CALL (dog) { MATCH (dog)-[:HAS_TOY]->(t:Toy) RETURN t.name AS toy }
RETURN p.name AS name, (dog).name AS dog, toy ORDER BY name, dog;

--
-- 6. CALL as a clause vs "call" as an identifier (the CALL_LA lookahead)
--
-- The lexer rewrites CALL to CALL_LA only when it is immediately followed by
-- '(' or '{'.  Everywhere else "call" remains an ordinary identifier.

-- a cypher variable named call (followed by a label, or used in an expression)
MATCH (call:Person) WHERE call.age > 30 RETURN call.name AS n ORDER BY n;
-- "call" as a vertex label (a label is followed by ')' / whitespace, not '('
-- or '{', so it is an ordinary identifier)
CREATE (:call);
MATCH (c:call) RETURN count(c) AS ncall;
-- "call" as a property key, and read back
MATCH (p:Person {name: 'Andy'}) RETURN {call: p.name} AS m;
-- "call" as a function argument (an ordinary identifier here)
MATCH (call:Person {name: 'Andy'}) RETURN id(call) IS NOT NULL AS ok;
-- a SQL column / alias named call
SELECT 1 AS call;
SELECT call FROM (SELECT 1 AS call) s;
SELECT s.call FROM (SELECT 1 AS call) s WHERE s.call = 1;
-- the SQL "CALL procedure()" statement is unaffected (CALL followed by a name):
-- the procedure does not exist, but that is name resolution, not a syntax error
CALL no_such_procedure_xyz();

-- accepted limitation: "call" immediately followed by '(' or '{' is taken as
-- the start of a CALL clause, so these two spellings no longer parse -- quote
-- the identifier (SQL "call", cypher `call`) to keep the old meaning.
-- a function call literally named call(...)
SELECT call(1);
-- a node variable named call carrying an inline property map and no label
MATCH (call {name: 'Andy'}) RETURN call.name;
-- the workarounds: a label after the cypher variable, or a quoted SQL identifier
MATCH (call:Person {name: 'Andy'}) RETURN call.name AS n;
SELECT "call" FROM (SELECT 1 AS "call") s;

-- cleanup
DROP GRAPH cypher_call CASCADE;
