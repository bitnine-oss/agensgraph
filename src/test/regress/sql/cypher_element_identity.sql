--
-- Comparing nodes and relationships by their identity
--
-- Every comparison a graph element has -- vertex_cmp, vertex_hash and their
-- edge counterparts -- reads the element's graphid and nothing else.  So a
-- query that orders or compares whole elements is answered by ordering or
-- comparing the identities instead: the same answer, reached without reading
-- the property map, so the element itself need not be built at all.
--
-- Exactness rests on what the comparison reads, not on an identity being
-- unique.  The same graphid can appear under two labels (a label's id column
-- has a default, not a constraint), and both spellings answer the same there
-- too -- covered below.
--
-- Oracle for every result-returning query: the rows must equal what the
-- explicitly id-spelled query returns.  EXPLAIN (costs off) asserts that the
-- element no longer reaches the comparison.
--
CREATE GRAPH ei;
SET graph_path = ei;

CREATE VLABEL p;
CREATE VLABEL q;
CREATE ELABEL k;

CREATE (:p {n: 'a'}), (:p {n: 'b'}), (:p {n: 'c'});
MATCH (x:p), (y:p) WHERE x.n < y.n CREATE (x)-[:k {w: 1}]->(y);
CREATE (:q {n: 'z'});

ANALYZE ei.p;
ANALYZE ei.k;

--
-- ORDER BY a node or a relationship
--

-- the sort keys on the identity, and the element it was read from is not
-- projected through the sort at all
EXPLAIN (VERBOSE, COSTS OFF)
MATCH (x:p) RETURN x.n ORDER BY x;

-- and because the key is the identity, the label's primary key can supply the
-- order outright
SET enable_seqscan = off;
EXPLAIN (VERBOSE, COSTS OFF)
MATCH (x:p) RETURN x.n ORDER BY x;
RESET enable_seqscan;

MATCH (x:p) RETURN x.n ORDER BY x;
MATCH (x:p) RETURN x.n ORDER BY id(x);

MATCH (x:p) RETURN x.n ORDER BY x DESC;
MATCH (x:p) RETURN x.n ORDER BY id(x) DESC;

-- ordering by an element that is also returned reuses the projected identity
-- rather than adding a second sort column
EXPLAIN (VERBOSE, COSTS OFF)
MATCH (x:p) RETURN id(x) ORDER BY x;

-- relationships order the same way
MATCH ()-[r:k]->() RETURN r.w ORDER BY r;
MATCH ()-[r:k]->() RETURN r.w ORDER BY id(r);

-- an element carried through WITH
MATCH (x:p) WITH x ORDER BY x RETURN x.n;

-- a missing OPTIONAL MATCH element sorts as a null, not as an element whose
-- identity happens to be null
MATCH (x:p) OPTIONAL MATCH (x)-[r:k]->() RETURN x.n, r.w ORDER BY r, x.n;
MATCH (x:p) OPTIONAL MATCH (x)-[r:k]->() RETURN x.n, r.w ORDER BY id(r), x.n;
MATCH (x:p) OPTIONAL MATCH (x)-[r:k]->() RETURN x.n, r.w
  ORDER BY r NULLS FIRST, x.n;

--
-- Comparing two nodes, or two relationships
--

-- the whole element never reaches the comparison: the join is on graphid, and
-- since it is the primary key on both sides one scan is redundant
EXPLAIN (VERBOSE, COSTS OFF)
MATCH (x:p), (y:p) WHERE x = y RETURN count(*);

-- across two labels the join stays, but it joins identities and neither
-- element is built
EXPLAIN (VERBOSE, COSTS OFF)
MATCH (x:p), (y:q) WHERE x = y RETURN count(*);

MATCH (x:p), (y:p) WHERE x = y RETURN count(*);
MATCH (x:p), (y:p) WHERE id(x) = id(y) RETURN count(*);

MATCH (x:p), (y:p) WHERE x <> y RETURN count(*);
MATCH (x:p), (y:p) WHERE id(x) <> id(y) RETURN count(*);

MATCH (x:p), (y:p) WHERE x < y RETURN x.n, y.n ORDER BY x.n, y.n;
MATCH (x:p), (y:p) WHERE id(x) < id(y) RETURN x.n, y.n ORDER BY x.n, y.n;

MATCH (x:p), (y:p) WHERE x >= y RETURN count(*);
MATCH (x:p), (y:p) WHERE id(x) >= id(y) RETURN count(*);

-- relationships compare the same way
MATCH ()-[r:k]->(), ()-[s:k]->() WHERE r = s RETURN count(*);
MATCH ()-[r:k]->(), ()-[s:k]->() WHERE r <> s RETURN count(*);

-- an element compared with a missing one is unknown, not false
MATCH (x:p) OPTIONAL MATCH (y:q {n: 'absent'})
  RETURN count(*) AS rows, count(x = y) AS decided;

-- comparing an element with itself
MATCH (x:p) WHERE x = x RETURN count(*);

-- an element still has no comparison against a bare identity
MATCH (x:p), (y:p) WHERE x = id(y) RETURN count(*);

--
-- The identity need not be unique for either spelling to be right
--
-- Give a q vertex a p vertex's graphid.  Both spellings then match the pair,
-- because both ask the same question.
--
SET enable_graph_dml = on;
INSERT INTO ei.q (id, properties)
  SELECT id, '{"n": "clone"}'::jsonb FROM ei.p ORDER BY id LIMIT 1;
RESET enable_graph_dml;

MATCH (x:p), (y:q) WHERE x = y RETURN x.n, y.n;
MATCH (x:p), (y:q) WHERE id(x) = id(y) RETURN x.n, y.n;

SET enable_graph_dml = on;
DELETE FROM ei.q WHERE properties->>'n' = 'clone';
RESET enable_graph_dml;

--
-- NULLIF yields its first operand, so that operand stays the element
--
MATCH (x:p), (y:p) WHERE x.n = 'a' AND y.n = 'a'
  RETURN nullIf(x, y) IS NULL AS same_is_null;
MATCH (x:p), (y:p) WHERE x.n = 'a' AND y.n = 'b'
  RETURN nullIf(x, y) = x AS differs_yields_first;

DROP GRAPH ei CASCADE;
