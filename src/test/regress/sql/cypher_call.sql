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
--   5. mixed combinations
--   6. OPTIONAL CALL, the input-preserving form
--   7. the CALL-as-identifier (CALL_LA) boundary
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
-- a CALL subquery may not return a name already bound in the outer query: it
-- would shadow the outer variable for the clauses that follow (an uncorrelated
-- body)
MATCH (t:Person) CALL () { RETURN 1 AS t } RETURN t;
-- the same for a correlated body returning a fresh value under an outer name
MATCH (p:Person {name: 'Andy'}), (q:Person {name: 'Peter'})
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS q } RETURN q;

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
WHERE COUNT { MATCH (p)-[:HAS_DOG]->(d:Dog) CALL (d) { MATCH (d) RETURN d AS dd } RETURN d } > 1
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
-- 6. OPTIONAL CALL
--
-- OPTIONAL CALL is CALL that preserves its input.  A plain CALL cross-joins the
-- body in, so an input row whose body yields no rows drops out of the pipeline;
-- OPTIONAL CALL keeps that row and binds every variable the body returns to
-- null for it.  The nulling is per input row: a correlated body that matches
-- nothing for one row nulls only that row and leaves the others alone.
--
-- It is the CALL counterpart of OPTIONAL MATCH -- and the primitive OPTIONAL
-- MATCH is itself defined in terms of, since the standard rewrites
-- "OPTIONAL MATCH p" to "OPTIONAL CALL (imports) { MATCH p RETURN vars }".  The
-- two therefore agree wherever both can express the same thing, which several
-- cases below assert directly, in results and in plan shape.
--
-- Two correlated bodies drive most of the cases, chosen so that a single query
-- covers the zero / one / many multiplicities at once and so that the row that
-- gets nulled differs between them: HAS_DOG gives Andy one dog, Peter two and
-- Timothy none, while KNOWS gives Andy two friends, Peter one and Timothy none.
--

-- 6.1 per-input-row nulling and cardinality ----------------------------------

-- Timothy's row survives with dog null; Andy and Peter are untouched, and Peter
-- still yields one row per dog.  Nulling is therefore per input row, not a
-- property of the query.
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog ORDER BY name, dog;
-- the same body under a plain CALL drops Timothy entirely
MATCH (p:Person)
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog ORDER BY name, dog;
-- and the OPTIONAL MATCH this rewrite is defined from agrees, row for row: an
-- EXCEPT in both directions yields nothing
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog
EXCEPT
MATCH (p:Person)
OPTIONAL MATCH (p)-[:HAS_DOG]->(d:Dog)
RETURN p.name AS name, d.name AS dog;
MATCH (p:Person)
OPTIONAL MATCH (p)-[:HAS_DOG]->(d:Dog)
RETURN p.name AS name, d.name AS dog
EXCEPT
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog;
-- the binding is a real SQL null, not a jsonb null
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog IS NULL AS missing ORDER BY name, missing;
-- the whole multiplicity spectrum in one query: a body yielding two rows for
-- one input row, one for another and none for a third contributes 2, 1 and 1
-- output rows, the last of them preserved rather than matched
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:KNOWS]->(f:Person) RETURN f.name AS friend }
RETURN p.name AS name, count(*) AS rows, count(friend) AS matched
ORDER BY name;
-- ... and the same body with the multiplicities running the other way round,
-- to show the preserved row is picked per row and not by position
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, count(*) AS rows, count(dog) AS matched
ORDER BY name;
-- a body that yields exactly one row for every input row behaves as a plain
-- CALL does: nothing is preserved because nothing was going to be dropped
MATCH (p:Person)
OPTIONAL CALL (p) { RETURN p.name AS echo }
RETURN echo ORDER BY echo;
-- a null the body itself produces is not the same event as a preserved row: the
-- body here yields a row only for Peter, and its second column tells the two
-- apart -- null-because-the-body-said-so for Peter, null-because-preserved for
-- the others
MATCH (p:Person)
OPTIONAL CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog {name: 'Fido'}) RETURN null AS x, 1 AS present
}
RETURN p.name AS name, x IS NULL AS x_null, present IS NULL AS present_null
ORDER BY name;
-- OPTIONAL preserves input rows, it does not invent them: an empty input still
-- produces no output
MATCH (p:Person {name: 'Nobody'})
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog;
-- a preserved row is an ordinary row afterwards: a later non-empty body
-- multiplies it just like any other
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
OPTIONAL CALL () { UNWIND [1, 2] AS k RETURN k }
RETURN p.name AS name, dog, k ORDER BY name, dog, k;

-- 6.2 the type matrix of a nulled binding ------------------------------------

-- Every variable the body returns is nulled together, whatever its type: a
-- vertex, an edge, a path, a list, a map, a string / numeric / boolean value, a
-- computed expression and a constant.  IS NULL is asserted rather than the
-- values, so the check does not depend on the label ids a printed vertex, edge
-- or path carries.
MATCH (p:Person)
OPTIONAL CALL (p) {
  MATCH pth = (p)-[r:HAS_DOG]->(d:Dog)
  RETURN d AS v, r AS e, pth AS pa, [d.name] AS li, {n: d.name} AS mp,
         d.name AS st, p.age AS nu, 1.5 AS fl, true AS bo,
         d.name + '!' AS cx, 7 AS k
}
RETURN p.name AS name,
       v IS NULL AS v_n, e IS NULL AS e_n, pa IS NULL AS pa_n,
       li IS NULL AS li_n, mp IS NULL AS mp_n, st IS NULL AS st_n,
       nu IS NULL AS nu_n, fl IS NULL AS fl_n, bo IS NULL AS bo_n,
       cx IS NULL AS cx_n, k IS NULL AS k_n
ORDER BY name, st_n;
-- The nulled binding stays null through every accessor, rather than raising or
-- producing an empty value: property lookup on a nulled vertex or map,
-- subscripting a nulled list, and the functions that unpack a graph element.
MATCH (p:Person)
OPTIONAL CALL (p) {
  MATCH pth = (p)-[r:HAS_DOG]->(d:Dog)
  RETURN d AS v, r AS e, pth AS pa, [d.name] AS li, {n: d.name} AS mp
}
RETURN p.name AS name, (v).name AS vname, label(v) AS vlabel,
       properties(v) AS vprops, id(v) IS NULL AS vid_null,
       type(e) AS etype, length(pa) AS plen, li[0] AS first, mp.n AS mapped
ORDER BY name, vname;

-- 6.3 the import forms -------------------------------------------------------

-- importing WITH, equivalent to the scope clause above
MATCH (p:Person)
OPTIONAL CALL { WITH p MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog ORDER BY name, dog;
-- CALL (*) imports every outer variable
MATCH (p:Person)
OPTIONAL CALL (*) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog ORDER BY name, dog;
-- CALL (*) with several outer variables in scope
MATCH (p:Person {name: 'Andy'}), (f:Person {name: 'Peter'})
OPTIONAL CALL (*) { MATCH (p)-[:KNOWS]->(f) RETURN 'knows' AS rel }
RETURN p.name AS name, f.name AS friend, rel;
-- several imported variables under an explicit scope clause
MATCH (p:Person), (f:Person {name: 'Timothy'})
OPTIONAL CALL (p, f) { MATCH (p)-[:KNOWS]->(f) RETURN 'knows' AS rel }
RETURN p.name AS name, rel ORDER BY name;
-- uncorrelated with an empty body: every outer row is kept and nulled
MATCH (p:Person)
OPTIONAL CALL () { MATCH (:Fish) RETURN 1 AS one }
RETURN p.name AS name, one ORDER BY name;
-- uncorrelated with a non-empty body: nothing is nulled, it is a cross join
MATCH (p:Person)
OPTIONAL CALL { MATCH (t:Toy) RETURN t.name AS toy }
RETURN p.name AS name, toy ORDER BY name;

-- 6.4 bodies that can yield nothing ------------------------------------------

-- WHERE false
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (d:Dog) WHERE false RETURN d.name AS dog }
RETURN p.name AS name, dog ORDER BY name;
-- a WITH ... WHERE inside the body that filters every row away
MATCH (p:Person)
OPTIONAL CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog) WITH d WHERE d.name = 'Zeus' RETURN d.name AS dog
}
RETURN p.name AS name, dog ORDER BY name;
-- LIMIT 0
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog LIMIT 0 }
RETURN p.name AS name, dog ORDER BY name;
-- SKIP past the end: Andy has one dog and Peter two, so only Peter keeps a value
MATCH (p:Person)
OPTIONAL CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog ORDER BY dog SKIP 1
}
RETURN p.name AS name, dog ORDER BY name, dog;
-- ORDER BY with LIMIT 1 picks one row per input row where there is one at all
MATCH (p:Person)
OPTIONAL CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog ORDER BY dog DESC LIMIT 1
}
RETURN p.name AS name, dog ORDER BY name;
-- DISTINCT over an empty result is still empty
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN DISTINCT d.name AS dog }
RETURN p.name AS name, dog ORDER BY name, dog;
-- a UNION whose branches are both empty
MATCH (p:Person)
OPTIONAL CALL (p) {
  MATCH (p)-[:HAS_FISH]->(f) RETURN f.name AS pet
  UNION
  MATCH (p)-[:HAS_BIRD]->(b) RETURN b.name AS pet
}
RETURN p.name AS name, pet ORDER BY name;
-- a UNION where only one branch contributes (Timothy's cat, the others' dogs)
MATCH (p:Person)
OPTIONAL CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS pet
  UNION
  MATCH (p)-[:HAS_CAT]->(c:Cat) RETURN c.name AS pet
}
RETURN p.name AS name, pet ORDER BY name, pet;
-- an INTERSECT only one person's dogs survive
MATCH (p:Person)
OPTIONAL CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS pet
  INTERSECT
  MATCH (:Dog {name: 'Fido'}) RETURN 'Fido' AS pet
}
RETURN p.name AS name, pet ORDER BY name;
-- an EXCEPT that subtracts everything the body found
MATCH (p:Person)
OPTIONAL CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS pet
  EXCEPT
  MATCH (dd:Dog) RETURN dd.name AS pet
}
RETURN p.name AS name, pet ORDER BY name;
-- a grouping aggregate: no rows in, no groups out
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog, count(*) AS n }
RETURN p.name AS name, dog, n ORDER BY name, dog;
-- UNWIND of an empty list
MATCH (p:Person)
OPTIONAL CALL (p) { UNWIND [] AS e RETURN e }
RETURN p.name AS name, e ORDER BY name;
-- a variable-length pattern that reaches nothing
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_FISH*1..2]->(f) RETURN f.name AS fish }
RETURN p.name AS name, fish ORDER BY name;
-- a shortestpath the body cannot find
MATCH (p:Person)
OPTIONAL CALL (p) {
  MATCH sp = shortestpath((p)-[:HAS_TOY*..3]->(t:Toy)) RETURN length(sp) AS len
}
RETURN p.name AS name, len ORDER BY name;

-- 6.5 bodies that can never yield nothing ------------------------------------

-- an ungrouped aggregate always produces exactly one row, so OPTIONAL changes
-- nothing and Timothy gets 0, not null
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN count(d) AS ndogs }
RETURN p.name AS name, ndogs, ndogs IS NULL AS is_null ORDER BY name;
-- likewise collect(), which yields an empty list rather than no row.  The list
-- is probed by length and emptiness rather than printed, since collect() fixes
-- no order among the elements it gathers.
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN collect(d.name) AS dogs }
RETURN p.name AS name, jsonb_array_length(dogs) AS n,
       dogs = [] AS empty, dogs IS NULL AS is_null
ORDER BY name;
-- a one-element list is order-free, so its contents can be shown
MATCH (p:Person {name: 'Andy'})
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN collect(d.name) AS dogs }
RETURN dogs;
-- an aggregate over a variable-length pattern is the same story: zero, not null
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:KNOWS*1..2]->(q:Person) RETURN count(q) AS reach }
RETURN p.name AS name, reach ORDER BY name;
-- an OPTIONAL MATCH inside the body already keeps the row, so the outer
-- OPTIONAL never fires
MATCH (p:Person)
OPTIONAL CALL (p) { OPTIONAL MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog ORDER BY name, dog;
-- a body of nothing but a RETURN of constants always has its one row to give
MATCH (p:Person)
OPTIONAL CALL () { RETURN 1 AS one }
RETURN p.name AS name, one ORDER BY name;

-- 6.6 a leading OPTIONAL CALL ------------------------------------------------

-- with no previous clause the input is the single empty row a query starts
-- from, so an empty body yields exactly one all-null row rather than no rows
OPTIONAL CALL { MATCH (:Fish) RETURN 1 AS one } RETURN one;
-- exactly as a leading OPTIONAL MATCH does
OPTIONAL MATCH (f:Fish) RETURN f.name AS one;
-- an explicitly empty scope clause reads the same way
OPTIONAL CALL () { MATCH (:Fish) RETURN 1 AS one } RETURN one;
-- a non-empty leading body just yields its own rows
OPTIONAL CALL { MATCH (d:Dog) RETURN d.name AS dog } RETURN dog ORDER BY dog;
-- a leading OPTIONAL CALL followed by more clauses
OPTIONAL CALL { MATCH (:Fish) RETURN 1 AS one }
WITH one, 2 AS two
RETURN one, two;
-- the preserved single row then drives the clauses after it, so a following
-- MATCH cross-joins with it
OPTIONAL CALL { MATCH (:Fish) RETURN 1 AS one }
MATCH (d:Dog)
RETURN one, d.name AS dog ORDER BY dog;

-- 6.7 chaining and nesting ---------------------------------------------------

-- two OPTIONAL CALLs in a row, the second correlated on the first's output: once
-- the first nulls a row the second cannot match for it either
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
OPTIONAL CALL (dog) {
  MATCH (d:Dog)-[:HAS_TOY]->(t:Toy) WHERE d.name = dog RETURN t.name AS toy
}
RETURN p.name AS name, dog, toy ORDER BY name, dog;
-- three OPTIONAL CALLs in a row, each importing the one before
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d AS dog }
OPTIONAL CALL (dog) { MATCH (dog)-[:HAS_TOY]->(t:Toy) RETURN t AS toy }
OPTIONAL CALL (toy) { MATCH (toy)-[:HAS_TOY]->(u:Toy) RETURN u.name AS subtoy }
RETURN p.name AS name, (dog).name AS dog, (toy).name AS toy, subtoy
ORDER BY name, dog;
-- two independent OPTIONAL CALLs off the same input variable
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
OPTIONAL CALL (p) { MATCH (p)-[:HAS_CAT]->(c:Cat) RETURN c.name AS cat }
RETURN p.name AS name, dog, cat ORDER BY name, dog;
-- a plain CALL after an OPTIONAL CALL re-imposes the inner join, dropping the
-- rows the OPTIONAL had preserved
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
CALL (dog) {
  MATCH (d:Dog)-[:HAS_TOY]->(t:Toy) WHERE d.name = dog RETURN t.name AS toy
}
RETURN p.name AS name, dog, toy ORDER BY name, dog;
-- an OPTIONAL CALL after a WITH
MATCH (p:Person)
WITH p, p.age AS age
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, age, dog ORDER BY name, dog;
-- a plain CALL nested inside an OPTIONAL CALL body empties that body, so the
-- outer OPTIONAL preserves the row (Andy's dog has no toy, Timothy has no dog)
MATCH (p:Person)
OPTIONAL CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog)
  CALL (d) { MATCH (d)-[:HAS_TOY]->(t:Toy) RETURN t.name AS toy }
  RETURN d.name AS dog, toy
}
RETURN p.name AS name, dog, toy ORDER BY name, dog;
-- making the inner CALL optional too keeps every dog
MATCH (p:Person)
OPTIONAL CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog)
  OPTIONAL CALL (d) { MATCH (d)-[:HAS_TOY]->(t:Toy) RETURN t.name AS toy }
  RETURN d.name AS dog, toy
}
RETURN p.name AS name, dog, toy ORDER BY name, dog;
-- an OPTIONAL CALL nested inside a PLAIN CALL body: the inner OPTIONAL keeps the
-- body non-empty, so the outer plain CALL drops only the input rows whose body
-- found no dog at all
MATCH (p:Person)
CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog)
  OPTIONAL CALL (d) { MATCH (d)-[:HAS_TOY]->(t:Toy) RETURN t.name AS toy }
  RETURN d.name AS dog, toy
}
RETURN p.name AS name, dog, toy ORDER BY name, dog;
-- three levels of OPTIONAL nesting
MATCH (p:Person)
OPTIONAL CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog)
  OPTIONAL CALL (d) {
    MATCH (d)-[:HAS_TOY]->(t:Toy)
    OPTIONAL CALL (t) { MATCH (t)-[:HAS_TOY]->(u:Toy) RETURN u.name AS subtoy }
    RETURN t.name AS toy, subtoy
  }
  RETURN d.name AS dog, toy, subtoy
}
RETURN p.name AS name, dog, toy, subtoy ORDER BY name, dog;
-- an OPTIONAL inside a plain inside an OPTIONAL
MATCH (p:Person)
OPTIONAL CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog)
  CALL (d) {
    MATCH (d)
    OPTIONAL CALL (d) { MATCH (d)-[:HAS_TOY]->(t:Toy) RETURN t.name AS toy }
    RETURN toy
  }
  RETURN d.name AS dog, toy
}
RETURN p.name AS name, dog, toy ORDER BY name, dog;
-- a leading OPTIONAL CALL inside another CALL's body: the body has no previous
-- clause of its own, so it preserves the single empty row the same way a
-- leading OPTIONAL CALL does at the top level
MATCH (p:Person {name: 'Andy'})
CALL { OPTIONAL CALL { MATCH (:Fish) RETURN 1 AS one } RETURN one }
RETURN p.name AS name, one;

-- 6.8 null semantics in the clauses that follow ------------------------------

-- IS NULL selects exactly the preserved rows, IS NOT NULL exactly the matched
-- ones
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
WITH p, dog WHERE dog IS NULL
RETURN p.name AS name;
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
WITH p, dog WHERE dog IS NOT NULL
RETURN p.name AS name, dog ORDER BY name, dog;
-- three-valued logic: an ordinary comparison against the nulled column is
-- unknown, so the preserved row fails both the predicate and its negation
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
WITH p, dog WHERE dog = 'Fido'
RETURN p.name AS name, dog;
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
WITH p, dog WHERE NOT (dog = 'Fido')
RETURN p.name AS name, dog ORDER BY name, dog;
-- ... and an explicit IS NULL disjunct brings it back
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
WITH p, dog WHERE dog = 'Fido' OR dog IS NULL
RETURN p.name AS name, dog ORDER BY name, dog;
-- COALESCE substitutes for the preserved row
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, coalesce(dog, 'none') AS dog ORDER BY name, dog;
-- count(dog) skips the nulls, count(*) does not
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN count(*) AS rows, count(dog) AS dogs;
-- the aggregates that ignore nulls do so over the nulled column too
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:KNOWS]->(f:Person) RETURN f.age AS friend_age }
RETURN count(*) AS rows, count(friend_age) AS matched, min(friend_age) AS youngest,
       max(friend_age) AS oldest, sum(friend_age) AS total, avg(friend_age) AS mean;
-- collect() drops the nulls, and so does its DISTINCT form
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN collect(dog) AS dogs, count(DISTINCT dog) AS ndistinct,
       collect(DISTINCT dog) AS distinct_dogs;
-- the OPTIONAL MATCH idiom: implicit grouping keeps the person with no dogs, at
-- a count of zero
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, count(dog) AS ndogs ORDER BY name;
-- DISTINCT treats the nulls as one value
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN DISTINCT dog ORDER BY dog;
-- null placement in ORDER BY: last ascending, first descending, and movable
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog ORDER BY dog, name;
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog ORDER BY dog DESC, name;
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog ORDER BY dog NULLS FIRST, name;
-- SKIP / LIMIT count the preserved rows like any other
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog ORDER BY name, dog SKIP 1 LIMIT 2;
-- RETURN * exposes the body's variables alongside the outer ones
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
WITH p.name AS name, dog
RETURN * ORDER BY name, dog;

-- 6.9 the other pipeline clauses ---------------------------------------------

-- clauses that can precede an OPTIONAL CALL and supply its imports
UNWIND ['Andy', 'Timothy'] AS nm
OPTIONAL CALL (nm) {
  MATCH (q:Person)-[:HAS_DOG]->(d:Dog) WHERE q.name = nm RETURN d.name AS dog
}
RETURN nm, dog ORDER BY nm, dog;
FOR nm IN ['Andy', 'Timothy']
OPTIONAL CALL (nm) {
  MATCH (q:Person)-[:HAS_DOG]->(d:Dog) WHERE q.name = nm RETURN d.name AS dog
}
RETURN nm, dog ORDER BY nm, dog;
MATCH (p:Person)
LET nm = p.name
OPTIONAL CALL (nm) {
  MATCH (q:Person)-[:HAS_DOG]->(d:Dog) WHERE q.name = nm RETURN d.name AS dog
}
RETURN nm, dog ORDER BY nm, dog;
-- an OPTIONAL CALL after a write clause sees the rows that clause produced
CREATE (x:Note {name: 'X'})
OPTIONAL CALL (x) { MATCH (x)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN x.name AS name, dog;
MATCH (n:Note) DETACH DELETE n;
-- clauses that can follow: LET augments the preserved row, FILTER restricts it
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
LET missing = dog IS NULL
RETURN p.name AS name, dog, missing ORDER BY name, dog;
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
FILTER dog IS NULL
RETURN p.name AS name ORDER BY name;
-- UNWIND and FOR after an OPTIONAL CALL multiply the preserved row too
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
UNWIND [1, 2] AS k
RETURN p.name AS name, dog, k ORDER BY name, dog, k;
-- NEXT carries the nulled column across the boundary into a fresh query part
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog
NEXT RETURN count(*) AS rows, count(dog) AS dogs;
-- FINISH discards the rows without projecting them
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
FINISH;
-- OPTIONAL MATCH and OPTIONAL CALL compose in either order
MATCH (p:Person)
OPTIONAL MATCH (p)-[:HAS_CAT]->(c:Cat)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, c.name AS cat, dog ORDER BY name, dog;
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
OPTIONAL MATCH (p)-[:HAS_CAT]->(c:Cat)
RETURN p.name AS name, dog, c.name AS cat ORDER BY name, dog;
-- an OPTIONAL MATCH anchored on a nulled vertex matches nothing and keeps the
-- row nulled
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d AS dog }
OPTIONAL MATCH (dog)-[:HAS_TOY]->(t:Toy)
RETURN p.name AS name, (dog).name AS dog, t.name AS toy ORDER BY name, dog;
-- A MATCH may follow an OPTIONAL CALL and join on the preserved variable, which
-- drops the nulled rows.  A MATCH directly after an OPTIONAL MATCH is rejected
-- and needs an intervening WITH: a pattern leaves deferred state behind, and
-- OPTIONAL MATCH strands it pointing at the nullable side of its join.  A CALL
-- body is analyzed as a self-contained subquery and leaves nothing behind, so
-- the only deferred state crossing this join belongs to the previous clause,
-- which is on the non-nullable side and stays resolvable.
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d AS dog }
MATCH (dog)-[:HAS_TOY]->(t:Toy)
RETURN p.name AS name, t.name AS toy ORDER BY name, toy;
MATCH (p:Person)
OPTIONAL MATCH (p)-[:HAS_DOG]->(d:Dog)
MATCH (d)-[:HAS_TOY]->(t:Toy)
RETURN p.name AS name, t.name AS toy;
-- The deferred state that rule is about is what an unlabelled pattern endpoint
-- produces, so carry one of those across an OPTIONAL CALL and resolve it after:
-- b is bound by an endpoint with no label, and every way of reading it must
-- still work on the far side of the join.
MATCH (a:Person)-[:KNOWS]->(b)
OPTIONAL CALL (a) { MATCH (a)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN a.name AS name, b.name AS known, label(b) AS lbl, dog
ORDER BY name, known, dog;
-- the same, with the WITH-separated form that is always legal, as ground truth
MATCH (a:Person)-[:KNOWS]->(b)
WITH a, b
OPTIONAL MATCH (a)-[:HAS_DOG]->(d:Dog)
RETURN a.name AS name, b.name AS known, label(b) AS lbl, d.name AS dog
ORDER BY name, known, dog;
-- and once more with the deferred endpoint itself nullable, so a nulled vertex
-- from an OPTIONAL MATCH and a nulled column from an OPTIONAL CALL coexist
MATCH (a:Person)
OPTIONAL MATCH (a)-[:KNOWS]->(b)
OPTIONAL CALL (a) { MATCH (a)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN a.name AS name, b.name AS known, properties(b) IS NULL AS b_null, dog
ORDER BY name, known, dog;
-- a set operation composed of OPTIONAL CALL query parts: NEXT aggregates the
-- whole union, so the count covers both branches' preserved rows
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS pet }
RETURN p.name AS name, pet
UNION
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_CAT]->(c:Cat) RETURN c.name AS pet }
RETURN p.name AS name, pet
NEXT RETURN count(*) AS rows, count(pet) AS matched;
-- INTERSECT of the two: no person has a dog and a cat of the same name, and the
-- preserved rows differ between the branches, so nothing survives
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS pet }
RETURN p.name AS name, pet
INTERSECT
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_CAT]->(c:Cat) RETURN c.name AS pet }
RETURN p.name AS name, pet;

-- 6.10 read-subquery and SQL contexts ----------------------------------------

-- Inside an expression subquery the OPTIONAL CALL must follow a clause it can
-- import from.  (A CALL with an import list as the leading clause of an
-- expression subquery is rejected for lack of a preceding clause -- a pre-
-- existing restriction shared with plain CALL, covered under errors below.)

-- EXISTS: because the OPTIONAL preserves the row, the subquery is non-empty
-- whenever its leading MATCH is, even for a body that finds nothing
MATCH (p:Person)
WHERE EXISTS {
  MATCH (p) OPTIONAL CALL (p) { MATCH (p)-[:HAS_TOY]->(t:Toy) RETURN t.name AS tn }
  RETURN tn
}
RETURN p.name AS name ORDER BY name;
MATCH (p:Person)
WHERE NOT EXISTS {
  MATCH (p) OPTIONAL CALL (p) { MATCH (p)-[:HAS_TOY]->(t:Toy) RETURN t.name AS tn }
  RETURN tn
}
RETURN p.name AS name ORDER BY name;
-- COUNT counts the preserved row as one
MATCH (p:Person)
RETURN p.name AS name,
       COUNT { MATCH (p)
               OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dn }
               RETURN dn } AS c
ORDER BY name;
-- COLLECT and ARRAY keep the null as a list element
MATCH (p:Person)
RETURN p.name AS name,
       COLLECT { MATCH (p)
                 OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dn }
                 RETURN dn ORDER BY dn } AS dogs
ORDER BY name;
MATCH (p:Person)
RETURN p.name AS name,
       ARRAY { MATCH (p)
               OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dn }
               RETURN dn ORDER BY dn } AS dogs
ORDER BY name;
-- VALUE of a single preserved row is null
MATCH (p:Person {name: 'Timothy'})
RETURN VALUE { MATCH (p)
               OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dn }
               RETURN dn } AS v;
-- IN against a list that contains only the preserved null is unknown
MATCH (p:Person {name: 'Timothy'})
RETURN 'Fido' IN { MATCH (p)
                   OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dn }
                   RETURN dn } AS found;
-- an OPTIONAL CALL nested in a CALL body inside an expression subquery
MATCH (p:Person)
RETURN p.name AS name,
       COLLECT { MATCH (p)-[:HAS_DOG]->(d:Dog)
                 OPTIONAL CALL (d) { MATCH (d)-[:HAS_TOY]->(t:Toy) RETURN t.name AS toy }
                 RETURN toy ORDER BY toy } AS toys
ORDER BY name;
-- a cypher query embedded in a SQL FROM clause
SELECT name, dog FROM (
  MATCH (p:Person)
  OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
  RETURN p.name AS name, dog
) c ORDER BY name, dog;
-- ... aggregated by SQL, which sees the null as SQL sees any null
SELECT count(*) AS rows, count(dog) AS dogs FROM (
  MATCH (p:Person)
  OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
  RETURN p.name AS name, dog
) c;
-- ... and filtered by a SQL CTE
WITH cte AS (
  MATCH (p:Person)
  OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
  RETURN p.name AS name, dog
)
SELECT name FROM cte WHERE dog IS NULL ORDER BY name;
-- A prepared statement carries the clause through plan caching unchanged.  It
-- is executed past the point where the cache switches from a custom plan to a
-- generic one, since that switch re-plans from a copy of the raw parse tree and
-- so is what proves the clause survives being copied.
PREPARE optional_call_plan AS
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog ORDER BY name, dog;
EXECUTE optional_call_plan;
EXECUTE optional_call_plan;
EXECUTE optional_call_plan;
EXECUTE optional_call_plan;
EXECUTE optional_call_plan;
EXECUTE optional_call_plan;
DEALLOCATE optional_call_plan;

-- 6.11 write clauses after an OPTIONAL CALL ----------------------------------

-- a write clause sees the null and stores it
MATCH (p:Person {name: 'Timothy'})
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
CREATE (:Note {who: p.name, dog: dog})
RETURN p.name AS name;
MATCH (n:Note) RETURN n.who AS who, n.dog AS dog, n.dog IS NULL AS dog_null;
MATCH (n:Note) DETACH DELETE n;
-- MERGE keys on the null the same way
MATCH (p:Person {name: 'Timothy'})
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
MERGE (:Note {dog: dog})
RETURN p.name AS name;
MATCH (n:Note) RETURN n.dog IS NULL AS dog_null;
MATCH (n:Note) DETACH DELETE n;
-- a write aimed at a nulled variable is a no-op for the rows it was nulled in.
-- No Person has a HAS_TOY edge, so every row below is nulled and the Toy comes
-- through all four write kinds untouched.
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_TOY]->(t:Toy) RETURN t AS toy }
SET toy.marked = true
RETURN p.name AS name ORDER BY name;
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_TOY]->(t:Toy) RETURN t AS toy }
REMOVE toy.name
RETURN count(*) AS rows;
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_TOY]->(t:Toy) RETURN t AS toy }
DELETE toy
RETURN count(*) AS rows;
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_TOY]->(t:Toy) RETURN t AS toy }
DETACH DELETE toy
RETURN count(*) AS rows;
MATCH (t:Toy) RETURN t.name AS toy, t.marked AS marked;
-- deleting the Note vertices leaves the label behind, so drop it too and keep
-- this section from altering what the teardown at the end of the file reports
DROP VLABEL note;

-- 6.12 plans -----------------------------------------------------------------

-- Decorrelation is part of the contract: a correlated optional body must be
-- pulled up into a plain left join rather than re-executed per outer row.  Each
-- import form is checked, and the equivalent OPTIONAL MATCH is planned beside it
-- to show the two produce the very same join tree.
EXPLAIN (COSTS OFF)
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog;
EXPLAIN (COSTS OFF)
MATCH (p:Person)
OPTIONAL MATCH (p)-[:HAS_DOG]->(d:Dog)
RETURN p.name AS name, d.name AS dog;
EXPLAIN (COSTS OFF)
MATCH (p:Person)
OPTIONAL CALL { WITH p MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog;
EXPLAIN (COSTS OFF)
MATCH (p:Person)
OPTIONAL CALL (*) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog;
-- two independent optional bodies decorrelate into two stacked left joins, the
-- same shape two OPTIONAL MATCHes get
EXPLAIN (COSTS OFF)
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
OPTIONAL CALL (p) { MATCH (p)-[:HAS_CAT]->(c:Cat) RETURN c.name AS cat }
RETURN p.name AS name, dog, cat;
EXPLAIN (COSTS OFF)
MATCH (p:Person)
OPTIONAL MATCH (p)-[:HAS_DOG]->(d:Dog)
OPTIONAL MATCH (p)-[:HAS_CAT]->(c:Cat)
RETURN p.name AS name, d.name AS dog, c.name AS cat;
-- ... and so does a chain where the second body imports the first's output
EXPLAIN (COSTS OFF)
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d AS dog }
OPTIONAL CALL (dog) { MATCH (dog)-[:HAS_TOY]->(t:Toy) RETURN t.name AS toy }
RETURN p.name AS name, toy;
EXPLAIN (COSTS OFF)
MATCH (p:Person)
OPTIONAL MATCH (p)-[:HAS_DOG]->(d:Dog)
OPTIONAL MATCH (d)-[:HAS_TOY]->(t:Toy)
RETURN p.name AS name, t.name AS toy;
-- an uncorrelated optional body runs once and left-joins
EXPLAIN (COSTS OFF)
MATCH (p:Person)
OPTIONAL CALL () { MATCH (t:Toy) RETURN t.name AS toy }
RETURN p.name AS name, toy;
-- a body that cannot be pulled up (LIMIT) becomes a per-row lateral left join,
-- which is still correct -- only the shape differs
EXPLAIN (COSTS OFF)
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog LIMIT 1 }
RETURN p.name AS name, dog;
-- an aggregating body likewise stays a per-outer lateral left join.  Scans are
-- forced here so the inner access does not flip between an index and a bitmap
-- scan with table size.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (COSTS OFF)
MATCH (p:Person)
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN count(d) AS ndogs }
RETURN p.name AS name, ndogs;
RESET enable_bitmapscan;
RESET enable_seqscan;
-- a body that contains an outer join of its own is not pulled up: the join
-- order is fixed by the nesting, so the outer optional stays a lateral left
-- join over it.  This is a property of the CALL body being a subquery, not of
-- OPTIONAL -- a plain CALL over the same body, and a body whose inner outer
-- join comes from OPTIONAL MATCH, both plan identically.
EXPLAIN (COSTS OFF)
MATCH (p:Person)
OPTIONAL CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog)
  OPTIONAL CALL (d) { MATCH (d)-[:HAS_TOY]->(t:Toy) RETURN t.name AS toy }
  RETURN d.name AS dog, toy
}
RETURN p.name AS name, dog, toy;
EXPLAIN (COSTS OFF)
MATCH (p:Person)
CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog)
  OPTIONAL MATCH (d)-[:HAS_TOY]->(t:Toy)
  RETURN d.name AS dog, t.name AS toy
}
RETURN p.name AS name, dog, toy;

-- 6.13 errors ----------------------------------------------------------------

-- OPTIONAL relaxes none of the plain CALL restrictions.  A body may not bind a
-- variable the working table already carries, whether the body is uncorrelated,
-- correlated, or rebinding the very name it imported.
MATCH (t:Person) OPTIONAL CALL () { RETURN 1 AS t } RETURN t;
MATCH (p:Person {name: 'Andy'}), (q:Person {name: 'Peter'})
OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS q } RETURN q;
MATCH (p:Person) OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS p }
RETURN p;
-- a name bound by a preceding OPTIONAL MATCH counts as bound as well
MATCH (p:Person) OPTIONAL MATCH (p)-[:HAS_CAT]->(c:Cat)
OPTIONAL CALL () { RETURN 1 AS c } RETURN c;
-- the body must return something
MATCH (p:Person) OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) FINISH } RETURN p.name;
-- no write clause of any kind is allowed in the body
MATCH (p:Person) OPTIONAL CALL (p) { CREATE (:Foo {n: p.name}) RETURN 1 AS one } RETURN p.name;
MATCH (p:Person) OPTIONAL CALL (p) { SET p.x = 1 RETURN p.name AS n } RETURN p.name;
MATCH (p:Person) OPTIONAL CALL (p) { DETACH DELETE p RETURN 1 AS one } RETURN p.name;
MATCH (p:Person) OPTIONAL CALL (p) { MERGE (:Foo {n: p.name}) RETURN 1 AS one } RETURN p.name;
MATCH (p:Person) OPTIONAL CALL (p) { REMOVE p.age RETURN p.name AS n } RETURN p.name;
-- there must be a preceding clause to import from, for either import form.
-- (Shared with plain CALL: an import list can never lead a query, a CALL body or
-- an expression subquery.)
OPTIONAL CALL (p) { MATCH (d:Dog) RETURN d.name AS dog } RETURN dog;
OPTIONAL CALL (*) { MATCH (d:Dog) RETURN d.name AS dog } RETURN dog;
OPTIONAL CALL { WITH p MATCH (d:Dog) RETURN d.name AS dog } RETURN dog;
MATCH (p:Person)
WHERE EXISTS { OPTIONAL CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dn }
               RETURN dn }
RETURN p.name;
-- an imported variable must exist
MATCH (p:Person) OPTIONAL CALL (nosuchvar) { MATCH (d:Dog) RETURN d.name AS dog } RETURN dog;
-- scope is still restricted to the imports: an unimported outer variable is not
-- visible inside the body, under any of the three forms
MATCH (p:Person), (other:Person {name: 'Peter'})
OPTIONAL CALL (p) {
  MATCH (p)-[:HAS_DOG]->(d:Dog) WHERE d.name <> other.name RETURN d.name AS dog
}
RETURN dog;
MATCH (p:Person), (other:Person {name: 'Peter'})
OPTIONAL CALL { WITH p MATCH (p)-[:HAS_DOG]->(d:Dog) WHERE d.name <> other.name
                RETURN d.name AS dog }
RETURN dog;
MATCH (p:Person {name: 'Andy'})
OPTIONAL CALL () { MATCH (d:Dog) WHERE d.name = p.name RETURN d.name AS dog }
RETURN dog;
-- OPTIONAL prefixes a call clause and nothing else
MATCH (p:Person) OPTIONAL OPTIONAL CALL (p) { RETURN 1 AS one } RETURN one;
OPTIONAL RETURN 1;
OPTIONAL UNWIND [1] AS x RETURN x;

-- 6.14 "optional" is still an ordinary identifier -----------------------------

MATCH (optional:Person {name: 'Andy'}) RETURN optional.name AS n;
MATCH (p:Person {name: 'Andy'}) RETURN {optional: p.name} AS m;
UNWIND [1, 2] AS optional RETURN optional ORDER BY optional;
MATCH (p:Person {name: 'Andy'}) WITH 1 AS optional RETURN optional;
MATCH (p:Person {name: 'Andy'}) RETURN p.name AS optional;
MATCH (optional:Person {name: 'Andy'}) RETURN id(optional) IS NOT NULL AS ok;
SELECT 1 AS optional;
SELECT optional FROM (SELECT 1 AS optional) s;
-- unlike "call", "optional" needs no lookahead rewrite, so a node variable named
-- optional carrying an inline property map and no label still parses (here it
-- matches both the person and the dog named Andy)
MATCH (optional {name: 'Andy'}) RETURN label(optional) AS lbl ORDER BY lbl;
-- and an OPTIONAL CALL still parses right after a clause that binds one
UNWIND [1] AS optional
OPTIONAL CALL () { MATCH (:Fish) RETURN 1 AS one }
RETURN optional, one;
-- keyword case folding
MATCH (p:Person) optional call (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog ORDER BY name, dog;
MATCH (p:Person) Optional Call (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN p.name AS name, dog ORDER BY name, dog;

--
-- 7. CALL as a clause vs "call" as an identifier (the CALL_LA lookahead)
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

--
-- 8. An ORDER BY key naming a projected item, either side of the CALL boundary
--
-- A CALL body is its own query level, so a sort key in the body resolves the
-- body's own items, and a key outside resolves the outer projection's items --
-- the columns the CALL adds being ordinary pipeline variables.  These go last in
-- the file because each anonymous edge pattern consumes a generated alias
-- number, which every EXPLAIN plan above prints.
--

-- the body's key names the item the body returns, whole and inside a larger
-- expression; both pick Ozzy, as spelling the property out does
MATCH (p:Person {name: 'Peter'})
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog ORDER BY dog DESC LIMIT 1 }
RETURN dog;
MATCH (p:Person {name: 'Peter'})
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog
           ORDER BY toUpper(dog) DESC LIMIT 1 }
RETURN dog;

-- the outer key names what the CALL added to the pipeline
MATCH (p:Person {name: 'Peter'})
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN dog ORDER BY toUpper(dog) DESC;

-- an item of the outer projection is not in scope inside a CALL body: the body
-- resolves its own names only, so this is an error and not a silent correlation
MATCH (p:Person {name: 'Peter'})
CALL (p) { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dog }
RETURN dog AS nm ORDER BY COUNT { MATCH (q:Person) WHERE q.name = nm };

-- cleanup
DROP GRAPH cypher_call CASCADE;
