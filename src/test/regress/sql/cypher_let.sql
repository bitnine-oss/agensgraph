--
-- Cypher Query Language - LET clause (GQL)
--
-- LET binds new variables and AUGMENTS the working record: it keeps every
-- existing binding and appends the new ones.  It is exactly "WITH *, expr AS v"
-- semantically -- contrast plain "WITH expr AS v", which projects only the
-- listed items and drops everything else.  LET is modeled as a CP_LET
-- projection whose item list is an implicit leading "*" (carrying the existing
-- bindings through) followed by the LET assignments.
--
-- These tests cover: augmentation of every prior binding kind, native-type
-- preservation across a LET, the RHS expression surface, sequential LETs vs.
-- intra-LET references, the three guard errors (rebind / duplicate name /
-- aggregate), clause placement and modifier folding, read-subquery contexts,
-- downstream interactions (DISTINCT / ORDER BY / GROUP BY), and backward
-- compatibility of "let" as an ordinary unreserved identifier.
--

-- Set up
CREATE GRAPH cypher_let;
SET graph_path = cypher_let;

-- Fixtures.  person and knows are created first and in a fixed order so the
-- label ids embedded in whole-vertex/edge output below stay deterministic.
CREATE (:person {id: 1, name: 'alice', age: 30});
CREATE (:person {id: 2, name: 'bob', age: 25});
CREATE (:person {id: 3, name: 'carol', age: 40});
MATCH (a:person {id: 1}), (b:person {id: 2})
CREATE (a)-[:knows {since: 2001, weight: 1.5}]->(b);
MATCH (a:person {id: 2}), (b:person {id: 3})
CREATE (a)-[:knows {since: 2010, weight: 2.0}]->(b);

--
-- 1. Augmentation -- every prior binding kind survives, alongside the new var
--

-- Bind one binding of every kind before LET (vertex v, edge e, path p, a scalar
-- from a property, a jsonb list, a jsonb map, a bare property value), then LET
-- appends "tag".  All eight must appear together in the result.
MATCH p = (v:person {id: 1})-[e:knows]->(w:person)
WITH v, e, p, w.id AS sc, [1, 2, 3] AS lst, {a: 1, b: 'x'} AS mp, v.age AS pv
LET tag = 'added'
RETURN v, e, nodes(p) AS pnodes, sc, lst, mp, pv, tag;

-- The augmented vertex/edge print with their native representation (label id
-- and graphid), NOT as a jsonb object -- proof the LET did not coerce them.
MATCH (v:person {id: 1})-[e:knows]->(w)
LET k = 1
RETURN v AS vv, e AS ee, k;

--
-- 2. Single item, many items, and an item over multiple prior variables
--

-- Single LET item added to a scalar working record.
MATCH (n:person)
LET doubled = n.age * 2
RETURN n.name AS name, n.age AS age, doubled ORDER BY name;

-- Many independent items of assorted types in one LET.
MATCH (n:person {id: 1})
LET a = 1, b = 'two', c = [3], d = {e: 4}, f = true
RETURN a, b, c, d, f;

-- One item computed from several prior variables.
MATCH (a:person {id: 1}), (b:person {id: 3})
LET agesum = a.age + b.age
RETURN agesum;

--
-- 3. Native-type preservation across a LET
--

-- A vertex/edge bound before LET is still usable as a vertex/edge afterward:
-- the pattern (a)-[e]->(c) re-traverses the same edge bound before the LET.
MATCH (a:person {id: 1})-[e:knows]->(b)
LET tag = 'x'
MATCH (a)-[e]->(c)
RETURN a.name AS an, c.name AS cn, tag ORDER BY an;

-- Graph functions over the pre-LET vertex/edge still resolve after the LET.
MATCH (a:person {id: 1})-[e:knows]->(b)
LET w = e.weight
RETURN id(a) IS NOT NULL AS has_id, labels(a) AS albls, type(e) AS etype,
       properties(e) AS eprops, startnode(e).name AS sn, endnode(e).name AS en, w;

-- A path variable survives and remains a path (length/nodes still work).
MATCH p = (a:person {id: 1})-[:knows]->(b)
LET plen = length(p)
RETURN plen, nodes(p) IS NOT NULL AS has_nodes;

--
-- 4. RHS expression variety
--

-- Arithmetic, string concat, list/map literals, boolean, CASE, COALESCE,
-- NULLIF, a list comprehension, a function call, and a NULL literal.
MATCH (n:person {id: 1})
LET arith = 1 + 2 * 3,
    concat = n.name + '!',
    jlist = [1, 2, 3],
    jmap = {k: 'v', n: 1},
    bool = (n.age > 20),
    casev = CASE WHEN n.age >= 30 THEN 'old' ELSE 'young' END,
    coal = coalesce(NULL, n.name),
    nsame = nullif(1, 1),
    ndiff = nullif(n.name, 'zzz'),
    lcomp = [x IN [1, 2, 3] WHERE x > 1 | x * 10],
    fcall = toupper(n.name),
    nul = NULL
RETURN arith, concat, jlist, jmap, bool, casev, coal, nsame, ndiff, lcomp, fcall, nul;

-- A parameter on the RHS.  A bare untyped "$1" cannot be prepared (unknown
-- cannot resolve to jsonb) -- this is inherited verbatim from WITH -- so a
-- parameter is supplied with an explicit ::jsonb cast, as elsewhere in Cypher.
PREPARE letp AS MATCH (n:person {id: 1}) LET p = $1::jsonb RETURN n.name AS name, p;
EXECUTE letp ('99');
EXECUTE letp ('"str"');
DEALLOCATE letp;

--
-- 5. Sequential LETs vs. intra-LET references
--

-- Two separate LETs: the second may reference a variable bound by the first.
MATCH (n:person {id: 1})
LET a = 10
LET b = a + 5
RETURN a, b;

-- But within one LET, an item may NOT reference an earlier item of the same
-- LET: they are all evaluated against the incoming record.
MATCH (n:person {id: 1})
LET a = 1, b = a + 1
RETURN a, b;

-- Referencing a variable that does not exist at all is likewise an error.
MATCH (n:person {id: 1})
LET bad = nope + 1
RETURN bad;

--
-- 6. Guard errors -- LET may only INTRODUCE names, never redefine, and is
--    row-wise (no aggregates)
--

-- Rebinding a variable that already exists (here the MATCH variable n).
MATCH (n:person)
LET n = 1
RETURN n;

-- Rebinding a variable introduced by a preceding projection.
MATCH (x:person {id: 1})
WITH x.name AS nm
LET nm = 'other'
RETURN nm;

-- Rebinding a variable bound by an earlier LET.
MATCH (n:person {id: 1})
LET a = 1
LET a = 2
RETURN a;

-- Binding the same name twice within a single LET.
MATCH (n:person {id: 1})
LET z = 1, z = 2
RETURN z;

-- A plain aggregate in LET.
MATCH (n:person)
LET c = count(*)
RETURN c;

-- An aggregate buried inside a larger LET expression.
MATCH (n:person)
LET s = 1 + sum(n.age)
RETURN s;

--
-- 7. Clause placement and modifier folding
--

-- LET may be the first clause: it projects over the implicit single-row input.
LET a = 1 RETURN a;

-- A leading LET may bind several variables at once.
LET greeting = 'hi', doubled = 2 * 3 RETURN greeting, doubled;

-- LET may also open the statement right after NEXT, carrying the prior table.
MATCH (n:person) RETURN n.name AS name
NEXT
LET tag = 'seen' RETURN name, tag ORDER BY name;

-- LET cannot be the terminal clause: a query must end with RETURN/FINISH/update.
MATCH (n:person) LET a = 1;

-- A trailing standalone ORDER BY / SKIP / LIMIT folds into the LET projection.
-- age DESC over {40,30,25}, drop the first, keep one -> alice (age 30).
MATCH (n:person)
LET k = n.age * 2
ORDER BY k DESC SKIP 1 LIMIT 1
RETURN n.name AS name, k;

-- The folded key may also name the LET variable inside a larger expression, not
-- only as the whole key.  "k * -1" ascending is "k" descending, so this picks
-- the same row as the case above.
MATCH (n:person)
LET k = n.age * 2
ORDER BY k * -1 SKIP 1 LIMIT 1
RETURN n.name AS name, k;

-- A key may mix the LET variable with a variable LET carried through.
MATCH (n:person)
LET k = n.age * 2
ORDER BY k * -1, n.name
RETURN n.name AS name, k;

-- LET feeding a following FILTER.
MATCH (n:person)
LET adult = (n.age >= 30)
FILTER adult
RETURN n.name AS name ORDER BY name;

-- LET feeding a subsequent MATCH via WHERE.
MATCH (a:person {id: 1})
LET target = 2
MATCH (b:person) WHERE b.id = target
RETURN b.name AS name;

-- LET feeding a following WITH ... WHERE.
MATCH (n:person)
LET t = n.age + 1
WITH t AS t2 WHERE t2 > 30
RETURN t2 ORDER BY t2;

--
-- 8. Cardinality preservation
--

-- LET never adds or drops rows: three persons in, three out.
MATCH (n:person)
LET x = 1
RETURN count(*) AS c;

-- Preserved on a multiplying source too (UNWIND then LET).
UNWIND [1, 2, 3] AS u
LET y = u * 10
RETURN u, y ORDER BY u;

--
-- 9. LET inside read-subquery contexts (EXISTS / COUNT / CALL / SQL FROM)
--

-- LET inside an EXISTS subquery.
MATCH (p:person)
WHERE EXISTS { MATCH (p)-[:knows]->(q) LET one = 1 RETURN q }
RETURN p.name AS name ORDER BY name;

-- LET inside a COUNT subquery (carol has no outgoing knows -> 0).
MATCH (p:person)
RETURN p.name AS name,
       COUNT { MATCH (p)-[:knows]->(q) LET one = 1 RETURN q } AS c
ORDER BY name;

-- LET inside a CALL subquery, exporting the LET-bound variable.
MATCH (p:person {id: 1})
CALL (p) { MATCH (p)-[:knows]->(q) LET qn = q.name RETURN qn }
RETURN qn ORDER BY qn;

-- LET inside a Cypher subquery used in a SQL FROM.
SELECT name FROM (MATCH (n:person) LET nm = n.name RETURN nm AS name) t
ORDER BY name;

--
-- 10. Downstream interactions
--

-- WITH DISTINCT over a LET-bound variable.
MATCH (n:person)
LET bucket = (n.age >= 30)
WITH DISTINCT bucket
RETURN bucket ORDER BY bucket;

-- ORDER BY a LET-bound variable that is NOT itself returned (resjunk sort key).
MATCH (n:person)
LET k = n.age
RETURN n.name AS name ORDER BY k DESC;

-- GROUP BY a LET-bound variable in a later RETURN aggregate.
MATCH (n:person)
LET decade = (n.age / 10)
RETURN decade, count(*) AS c ORDER BY decade;

-- LET following a write clause: the CREATE-bound vertex is usable in the LET.
CREATE (a:wtmp {id: 1, name: 'wnode'})
LET nm = a.name
RETURN nm;
MATCH (n:wtmp) RETURN count(*) AS c;

--
-- 11. "let" is a reserved keyword: it can no longer be a bare variable name or
--     label, but it remains usable as a quoted label, as a property key, in
--     property access, and as a RETURN alias.
--

-- as a bare variable name or bare label: reserved, so these are syntax errors
MATCH (let:person {id: 1}) RETURN 1;
MATCH (x:let) RETURN 1;

-- as a quoted label, with "let" as a property key and in property access
CREATE (:"let" {let: 42});
MATCH (x:"let") RETURN x.let AS v;

-- as a RETURN alias
RETURN 1 AS let;

--
-- 12. Case-insensitive spelling
--

MATCH (n:person {id: 1}) let a = 1 RETURN a;
MATCH (n:person {id: 1}) LeT b = 2 RETURN b;

--
-- 13. Plan shape
--

-- A LET is a plain projection folded into the scan's target list: no Sort,
-- Aggregate, Group, or Subquery Scan node is introduced by the LET itself,
-- and the projection subquery is pulled up (zero overhead).
EXPLAIN (COSTS OFF)
MATCH (n:person)
LET d = n.age * 2
RETURN n.name AS name, d;

-- Tear down
DROP GRAPH cypher_let CASCADE;
