--
-- Elision of unused graph-pattern endpoints
--
-- A MATCH endpoint vertex the query never uses is removed from the plan: the
-- vertex id is a primary key (so the endpoint join is 1:1 and cannot change
-- row multiplicity) and every edge endpoint references a live vertex (so the
-- join filters nothing), hence the scan and join are redundant.  Elision must
-- never change results, and -- since it runs after the graphmeta constraint
-- solver -- must not cost a surviving neighbour its scan pruning.  It is
-- structural and works with or without ag_graphmeta gathering.
--
-- Oracle for every result-returning query: the rows returned with
-- enable_graph_endpoint_elision on must equal the rows returned with it off
-- (ORDER BY makes the comparison deterministic).  EXPLAIN (costs off) asserts
-- the plan shape: the elided endpoint's scan/join is gone, a kept endpoint's is
-- not.
--
CREATE GRAPH ee;
SET graph_path = ee;

CREATE VLABEL person;
CREATE VLABEL city;
CREATE VLABEL company;
CREATE VLABEL country;
CREATE ELABEL knows;
CREATE ELABEL lives_in;
CREATE ELABEL works_at;
CREATE ELABEL located_in;

SET auto_gather_graphmeta = true;

CREATE (:person {name:'alice'}), (:person {name:'bob'}),
       (:city {name:'paris'}), (:company {name:'acme'}),
       (:country {name:'france'});
MATCH (a:person {name:'alice'}), (b:person {name:'bob'})     CREATE (a)-[:knows]->(b);
MATCH (a:person {name:'bob'}),   (b:person {name:'alice'})   CREATE (a)-[:knows]->(b);
MATCH (a:person {name:'alice'}), (c:city {name:'paris'})     CREATE (a)-[:lives_in]->(c);
MATCH (a:person {name:'bob'}),   (k:company {name:'acme'})   CREATE (a)-[:works_at]->(k);
MATCH (c:city {name:'paris'}),   (n:country {name:'france'}) CREATE (c)-[:located_in]->(n);

-- connectivity: knows(person,person), lives_in(person,city),
--               works_at(person,company), located_in(city,country)
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";

-- ============================================================
-- ELIDE: terminal endpoint
-- ============================================================
-- directed: the end of (a)-[e]->() is never used; its scan+join is elided.
EXPLAIN (costs off) MATCH (a:person)-[e:knows]->() RETURN a.name AS a;
SET enable_graph_endpoint_elision = false;
EXPLAIN (costs off) MATCH (a:person)-[e:knows]->() RETURN a.name AS a;
SET enable_graph_endpoint_elision = true;
-- ... and the result is identical with elision on and off.
MATCH (a:person)-[e:knows]->() RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[e:knows]->() RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- reverse: ()<-[e]-(a) -- the near (left) node is the edge end and is elided.
EXPLAIN (costs off) MATCH ()<-[e:knows]-(a:person) RETURN a.name AS a;
MATCH ()<-[e:knows]-(a:person) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH ()<-[e:knows]-(a:person) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- undirected: (a)-[e]-() -- the far endpoint of an undirected edge is elided too.
EXPLAIN (costs off) MATCH (a:person)-[e:knows]-() RETURN a.name AS a;
MATCH (a:person)-[e:knows]-() RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[e:knows]-() RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- A named but unused endpoint is elided just the same (b is assigned, never read).
EXPLAIN (costs off) MATCH (a:person)-[e:knows]->(b) RETURN a.name AS a;
MATCH (a:person)-[e:knows]->(b) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[e:knows]->(b) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- ============================================================
-- ELIDE: intermediate endpoint
-- ============================================================
-- The middle () is elided and the two edges are joined directly (e1.end = e2.start,
-- no middle vertex scan).
EXPLAIN (costs off) MATCH (a:person)-[]->()-[]->(c) RETURN a.name AS a, label(c) AS c;
MATCH (a:person)-[]->()-[]->(c) RETURN a.name AS a, label(c) AS c ORDER BY a, c;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[]->()-[]->(c) RETURN a.name AS a, label(c) AS c ORDER BY a, c;
SET enable_graph_endpoint_elision = true;

-- labelled edges either side make the direct edge-to-edge join explicit in EXPLAIN.
EXPLAIN (costs off) MATCH (a:person)-[b:lives_in]->()-[d:located_in]->(e) RETURN a.name AS a, label(e) AS e;
MATCH (a:person)-[b:lives_in]->()-[d:located_in]->(e) RETURN a.name AS a, label(e) AS e ORDER BY a, e;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[b:lives_in]->()-[d:located_in]->(e) RETURN a.name AS a, label(e) AS e ORDER BY a, e;
SET enable_graph_endpoint_elision = true;

-- longer chain: two consecutive anonymous middles are both elided.
EXPLAIN (costs off) MATCH (a:person)-[]->()-[]->()-[]->(d) RETURN a.name AS a, label(d) AS d;
MATCH (a:person)-[]->()-[]->()-[]->(d) RETURN a.name AS a, label(d) AS d ORDER BY a, d;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[]->()-[]->()-[]->(d) RETURN a.name AS a, label(d) AS d ORDER BY a, d;
SET enable_graph_endpoint_elision = true;

-- ============================================================
-- ELIDE: both endpoints unused
-- ============================================================
-- MATCH ()-[e]->() collapses to a bare edge scan.
EXPLAIN (costs off) MATCH ()-[e:knows]->() RETURN count(*);
MATCH ()-[e:knows]->() RETURN count(*);
SET enable_graph_endpoint_elision = false;
MATCH ()-[e:knows]->() RETURN count(*);
SET enable_graph_endpoint_elision = true;

-- both endpoints elided, feeding a WITH/aggregate pipeline.
EXPLAIN (costs off) MATCH ()-[e:knows]->() WITH count(e) AS c RETURN c;
MATCH ()-[e:knows]->() WITH count(e) AS c RETURN c;
SET enable_graph_endpoint_elision = false;
MATCH ()-[e:knows]->() WITH count(e) AS c RETURN c;
SET enable_graph_endpoint_elision = true;

-- reverse form: ()<-[e]-() -- both anonymous endpoints of an incoming edge are
-- elided too, collapsing to the same bare edge scan as ()-[e]->().
EXPLAIN (costs off) MATCH ()<-[e:knows]-() RETURN count(*);
MATCH ()<-[e:knows]-() RETURN count(*);
SET enable_graph_endpoint_elision = false;
MATCH ()<-[e:knows]-() RETURN count(*);
SET enable_graph_endpoint_elision = true;

-- ============================================================
-- ELIDE: multiple independent endpoints / comma-separated components
-- ============================================================
-- Two comma-separated components, each with its own trailing unused endpoint:
-- both anonymous endpoints are elided independently in a single query.
EXPLAIN (costs off)
  MATCH (a:person)-[b:knows]->(), (c:city)-[d:located_in]->()
  RETURN a.name AS a, label(c) AS c;
MATCH (a:person)-[b:knows]->(), (c:city)-[d:located_in]->()
  RETURN a.name AS a, label(c) AS c ORDER BY a, c;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[b:knows]->(), (c:city)-[d:located_in]->()
  RETURN a.name AS a, label(c) AS c ORDER BY a, c;
SET enable_graph_endpoint_elision = true;

-- ============================================================
-- ELIDE: aggregates over elided-endpoint patterns
-- ============================================================
-- count(*) over a pattern whose endpoint is elided
EXPLAIN (costs off) MATCH (a:person)-[e:knows]->() RETURN count(*);
MATCH (a:person)-[e:knows]->() RETURN count(*);
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[e:knows]->() RETURN count(*);
SET enable_graph_endpoint_elision = true;

-- count(DISTINCT a) -- the anchor survives; the endpoint is elided
MATCH (a:person)-[e:knows]->() RETURN count(DISTINCT a) AS n;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[e:knows]->() RETURN count(DISTINCT a) AS n;
SET enable_graph_endpoint_elision = true;

-- grouped aggregate: Cypher groups by the non-aggregate return expression id(a);
-- the endpoint is elided while the group key survives.
EXPLAIN (costs off) MATCH (a:person)-[e:knows]->() RETURN id(a) AS a, count(*) AS n;
MATCH (a:person)-[e:knows]->() RETURN id(a) AS a, count(*) AS n ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[e:knows]->() RETURN id(a) AS a, count(*) AS n ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- ============================================================
-- ELIDE: WITH pipelines, ORDER BY / LIMIT
-- ============================================================
EXPLAIN (costs off)
  MATCH (a:person)-[e:knows]->() WITH a.name AS n ORDER BY n LIMIT 1 RETURN n;
MATCH (a:person)-[e:knows]->() WITH a.name AS n ORDER BY n LIMIT 1 RETURN n;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[e:knows]->() WITH a.name AS n ORDER BY n LIMIT 1 RETURN n;
SET enable_graph_endpoint_elision = true;

MATCH (a:person)-[e:knows]->() WITH a ORDER BY id(a) RETURN a.name AS a;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[e:knows]->() WITH a ORDER BY id(a) RETURN a.name AS a;
SET enable_graph_endpoint_elision = true;

-- ============================================================
-- ELIDE: works without ag_graphmeta statistics
-- ============================================================
-- Elision is structural, so it fires with gathering off (no pruning available).
SET auto_gather_graphmeta = false;
EXPLAIN (costs off) MATCH (a:person)-[e:knows]->() RETURN a.name AS a;
MATCH (a:person)-[e:knows]->() RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[e:knows]->() RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;
-- intermediate elision is likewise structural
EXPLAIN (costs off) MATCH (a:person)-[]->()-[]->(c) RETURN a.name AS a, label(c) AS c;
SET auto_gather_graphmeta = true;

-- ============================================================
-- NOT ELIDE: labelled endpoint (the join is a label filter)
-- ============================================================
-- terminal labelled endpoint
EXPLAIN (costs off) MATCH (a:person)-[:works_at]->(:company) RETURN a.name AS a;
MATCH (a:person)-[:works_at]->(:company) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[:works_at]->(:company) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- reverse (start-side) labelled endpoint, otherwise unused
EXPLAIN (costs off) MATCH (:company)<-[:works_at]-(a) RETURN a.name AS a;

-- intermediate labelled endpoint: the middle (:city) label filter must be kept
EXPLAIN (costs off) MATCH (a:person)-[b:lives_in]->(:city)-[d]->(e) RETURN a.name AS a, label(e) AS e;
MATCH (a:person)-[b:lives_in]->(:city)-[d]->(e) RETURN a.name AS a, label(e) AS e ORDER BY a, e;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[b:lives_in]->(:city)-[d]->(e) RETURN a.name AS a, label(e) AS e ORDER BY a, e;
SET enable_graph_endpoint_elision = true;

-- ============================================================
-- NOT ELIDE: endpoint referenced (RETURN / WHERE / WITH / ORDER BY)
-- ============================================================
-- referenced in RETURN
EXPLAIN (costs off) MATCH (a:person)-[:knows]->(c) RETURN label(c) AS c;
-- referenced in WHERE (single-relation baserestrictinfo)
EXPLAIN (costs off) MATCH (a:person)-[:knows]->(x) WHERE x.name = 'bob' RETURN a.name AS a;
MATCH (a:person)-[:knows]->(x) WHERE x.name = 'bob' RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[:knows]->(x) WHERE x.name = 'bob' RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;
-- referenced downstream in a WITH pipeline
EXPLAIN (costs off) MATCH (a:person)-[:knows]->(x) WITH x MATCH (x)-[:lives_in]->(c) RETURN label(c) AS c;
-- referenced in ORDER BY
EXPLAIN (costs off) MATCH (a:person)-[:knows]->(x) RETURN a.name AS a ORDER BY id(x);

-- ============================================================
-- NOT ELIDE: inline property / WHERE-constrained endpoint
-- ============================================================
-- inline property filter on the endpoint must run
EXPLAIN (costs off) MATCH (a:person)-[:knows]->({name:'bob'}) RETURN a.name AS a;
MATCH (a:person)-[:knows]->({name:'bob'}) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[:knows]->({name:'bob'}) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- ============================================================
-- NOT ELIDE: non-mergejoinable two-relation qual on the endpoint
-- ============================================================
-- A two-relation <> that is not equivalence-absorbed lives in joininfo, not an EC;
-- dropping the endpoint would drop the filter, so it must be kept.
EXPLAIN (costs off) MATCH (a:person)-[:knows]->(x) WHERE x.name <> a.name RETURN a.name AS a;
MATCH (a:person)-[:knows]->(x) WHERE x.name <> a.name RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[:knows]->(x) WHERE x.name <> a.name RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- ============================================================
-- NOT ELIDE: variable-length edge endpoint (VLE)
-- ============================================================
-- terminal VLE endpoint
EXPLAIN (costs off) MATCH (a:person)-[:knows*1..2]->() RETURN a.name AS a;
MATCH (a:person)-[:knows*1..2]->() RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[:knows*1..2]->() RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;
-- intermediate node adjacent to a VLE: the VLE anchoring keeps the endpoint
EXPLAIN (costs off) MATCH (a:person)-[:knows*1..2]->()-[d]->(e) RETURN a.name AS a, label(e) AS e;
-- and a plain-edge middle whose other side is a VLE
EXPLAIN (costs off) MATCH (a:person)-[b]->()-[:knows*1..2]->(c) RETURN a.name AS a;

-- ============================================================
-- NOT ELIDE: OPTIONAL MATCH endpoint (outer join)
-- ============================================================
EXPLAIN (costs off) MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->() RETURN a.name AS a;
MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->() RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->() RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- ============================================================
-- NOT ELIDE: named path captures the endpoint
-- ============================================================
EXPLAIN (costs off) MATCH p = (a:person)-[:knows]->() RETURN length(p) AS len;
MATCH p = (a:person)-[:knows]->() RETURN length(p) AS len ORDER BY len;
SET enable_graph_endpoint_elision = false;
MATCH p = (a:person)-[:knows]->() RETURN length(p) AS len ORDER BY len;
SET enable_graph_endpoint_elision = true;

-- ============================================================
-- NOT ELIDE: self-loop / reused variable
-- ============================================================
EXPLAIN (costs off) MATCH (a:person)-[:knows]->(a) RETURN a.name AS a;
MATCH (a:person)-[:knows]->(a) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[:knows]->(a) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- ============================================================
-- NOT ELIDE: disconnected anonymous node (load-bearing cardinality)
-- ============================================================
-- MATCH (a), () is a cartesian scan whose row count depends on the () scan;
-- eliding it would change count(*), so it must be kept.
EXPLAIN (costs off) MATCH (a:person), () RETURN a.name AS a;
MATCH (a:person), () RETURN count(*);
SET enable_graph_endpoint_elision = false;
MATCH (a:person), () RETURN count(*);
SET enable_graph_endpoint_elision = true;

DROP GRAPH ee CASCADE;

-- ============================================================
-- COEXISTENCE WITH SCAN PRUNING
-- ============================================================
-- Elision runs after the graphmeta solver, so eliding a middle () must not
-- widen a surviving neighbour's Append.  Built on a branching multi-label graph
-- so the neighbour prunes to a STRICT subset visible in EXPLAIN.
--   n1 -e1-> n2 ; n2 -e2-> n3 ; n2 -e2b-> n3b ; n3 -e3-> n4
CREATE GRAPH ce;
SET graph_path = ce;
CREATE VLABEL n1; CREATE VLABEL n2; CREATE VLABEL n3; CREATE VLABEL n3b; CREATE VLABEL n4;
CREATE ELABEL e1; CREATE ELABEL e2; CREATE ELABEL e2b; CREATE ELABEL e3;
SET auto_gather_graphmeta = true;
CREATE (:n1)-[:e1]->(m:n2)-[:e2]->(:n3) CREATE (m)-[:e2b]->(:n3b);
MATCH (x:n3) CREATE (x)-[:e3]->(:n4);
-- connectivity: e1(n1,n2), e2(n2,n3), e2b(n2,n3b), e3(n3,n4)
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";

-- Anchor n1 fixes the middle to n2, so the unlabelled neighbour d prunes to the
-- edges leaving n2 = {e2,e2b} and the named neighbour f to {n3,n3b}.  With elision
-- ON the middle n2 scan disappears, yet d's and f's pruned Appends are exactly
-- {e2,e2b} and {n3,n3b} -- identical to elision OFF.  Eliding the middle must not
-- widen a neighbour's scan set back to every label.
-- (The join method may differ once a relation is removed; what must not change is
-- the surviving neighbours' pruned label sets.)
EXPLAIN (costs off) MATCH (a:n1)-[b:e1]->()-[d]->(f) RETURN label(f) AS f;
SET enable_graph_endpoint_elision = false;
EXPLAIN (costs off) MATCH (a:n1)-[b:e1]->()-[d]->(f) RETURN label(f) AS f;
SET enable_graph_endpoint_elision = true;
MATCH (a:n1)-[b:e1]->()-[d]->(f) RETURN label(f) AS f ORDER BY f;
SET enable_graph_endpoint_elision = false;
MATCH (a:n1)-[b:e1]->()-[d]->(f) RETURN label(f) AS f ORDER BY f;
SET enable_graph_endpoint_elision = true;

-- longer chain: two anonymous middles (n2, n3) elided, yet the far neighbours g
-- and h keep their pruned single-label scans ({e3}, {n4}).
EXPLAIN (costs off) MATCH (a:n1)-[b:e1]->()-[d:e2]->()-[g]->(h) RETURN label(h) AS h;
SET enable_graph_endpoint_elision = false;
EXPLAIN (costs off) MATCH (a:n1)-[b:e1]->()-[d:e2]->()-[g]->(h) RETURN label(h) AS h;
SET enable_graph_endpoint_elision = true;
MATCH (a:n1)-[b:e1]->()-[d:e2]->()-[g]->(h) RETURN label(h) AS h ORDER BY h;
SET enable_graph_endpoint_elision = false;
MATCH (a:n1)-[b:e1]->()-[d:e2]->()-[g]->(h) RETURN label(h) AS h ORDER BY h;
SET enable_graph_endpoint_elision = true;

DROP GRAPH ce CASCADE;

-- ============================================================
-- ELIDE: reverse / mixed-direction intermediate endpoints
-- ============================================================
-- A middle () is shared by its two edges through whichever endpoint columns the
-- pattern uses -- not only end->start.  Convergent (both edges point IN, so the
-- middle is end = end) and divergent (both point OUT, start = start) middles must
-- elide and reconnect the surviving edges on the right columns, exactly as the
-- forward end = start chain does.  The hub has 2 incoming and 2 outgoing edges so
-- each pattern returns real rows; results must be identical with elision on/off.
--   s1 -r1-> hub ; s2 -r1-> hub ; hub -r2-> d1 ; hub -r2-> d2
CREATE GRAPH re;
SET graph_path = re;
CREATE VLABEL src; CREATE VLABEL hub; CREATE VLABEL dst;
CREATE ELABEL r1; CREATE ELABEL r2;
SET auto_gather_graphmeta = true;
CREATE (:hub);
MATCH (h:hub) CREATE (:src {n:'s1'})-[:r1]->(h);
MATCH (h:hub) CREATE (:src {n:'s2'})-[:r1]->(h);
MATCH (h:hub) CREATE (h)-[:r2]->(:dst {n:'d1'});
MATCH (h:hub) CREATE (h)-[:r2]->(:dst {n:'d2'});
-- connectivity: r1(src,hub), r2(hub,dst)
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";

-- convergent middle: (x)-[b]->()<-[d]-(y) -- the middle is the END of both edges;
-- eliding it reconnects them on b.end = d.end.
EXPLAIN (costs off) MATCH (x:src)-[b:r1]->()<-[d:r1]-(y:src) RETURN x.n AS x, y.n AS y;
MATCH (x:src)-[b:r1]->()<-[d:r1]-(y:src) RETURN x.n AS x, y.n AS y ORDER BY x, y;
SET enable_graph_endpoint_elision = false;
MATCH (x:src)-[b:r1]->()<-[d:r1]-(y:src) RETURN x.n AS x, y.n AS y ORDER BY x, y;
SET enable_graph_endpoint_elision = true;

-- divergent middle: (x)<-[b]-()-[d]->(y) -- the middle is the START of both edges;
-- eliding it reconnects them on b.start = d.start.
EXPLAIN (costs off) MATCH (x:dst)<-[b:r2]-()-[d:r2]->(y:dst) RETURN x.n AS x, y.n AS y;
MATCH (x:dst)<-[b:r2]-()-[d:r2]->(y:dst) RETURN x.n AS x, y.n AS y ORDER BY x, y;
SET enable_graph_endpoint_elision = false;
MATCH (x:dst)<-[b:r2]-()-[d:r2]->(y:dst) RETURN x.n AS x, y.n AS y ORDER BY x, y;
SET enable_graph_endpoint_elision = true;

-- fully-reverse chain: (y)<-[d]-()<-[b]-(x) -- a forward chain written right to
-- left; the middle elides and reconnects on b.end = d.start.
EXPLAIN (costs off) MATCH (y:dst)<-[d:r2]-()<-[b:r1]-(x:src) RETURN x.n AS x, y.n AS y;
MATCH (y:dst)<-[d:r2]-()<-[b:r1]-(x:src) RETURN x.n AS x, y.n AS y ORDER BY x, y;
SET enable_graph_endpoint_elision = false;
MATCH (y:dst)<-[d:r2]-()<-[b:r1]-(x:src) RETURN x.n AS x, y.n AS y ORDER BY x, y;
SET enable_graph_endpoint_elision = true;

DROP GRAPH re CASCADE;

-- ============================================================
-- WRITE-PATH ROBUSTNESS
-- ============================================================
-- A MATCH with an unused endpoint that feeds a write must produce exactly the
-- same graph mutation with elision on and off, and must never elide the write's
-- result relation.  Each write is wrapped in BEGIN/ROLLBACK and run once with
-- elision on and once off; the two read-back blocks must be identical.
CREATE GRAPH we;
SET graph_path = we;
CREATE VLABEL person;
CREATE VLABEL note;
CREATE ELABEL knows;
CREATE ELABEL tagged;
SET auto_gather_graphmeta = true;
CREATE (:person {name:'alice'})-[:knows]->(:person {name:'bob'});
MATCH (b:person {name:'bob'}) CREATE (b)-[:knows]->(:person {name:'carol'});
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";

-- CREATE: for each (a)-[knows]->() the endpoint () is elided; the CREATE target
-- (a new note + tagged edge) is never elided.
SET enable_graph_endpoint_elision = true;
BEGIN;
  MATCH (a:person)-[b:knows]->() CREATE (a)-[:tagged]->(:note {v:1});
  MATCH (a:person)-[t:tagged]->(n:note) RETURN a.name AS a, n.v AS v ORDER BY a, v;
ROLLBACK;
SET enable_graph_endpoint_elision = false;
BEGIN;
  MATCH (a:person)-[b:knows]->() CREATE (a)-[:tagged]->(:note {v:1});
  MATCH (a:person)-[t:tagged]->(n:note) RETURN a.name AS a, n.v AS v ORDER BY a, v;
ROLLBACK;
SET enable_graph_endpoint_elision = true;

-- SET: the anchor a is the modify target and survives; the endpoint () is elided.
SET enable_graph_endpoint_elision = true;
BEGIN;
  MATCH (a:person)-[b:knows]->() SET a.seen = true;
  MATCH (p:person) RETURN p.name AS p, p.seen AS seen ORDER BY p;
ROLLBACK;
SET enable_graph_endpoint_elision = false;
BEGIN;
  MATCH (a:person)-[b:knows]->() SET a.seen = true;
  MATCH (p:person) RETURN p.name AS p, p.seen AS seen ORDER BY p;
ROLLBACK;
SET enable_graph_endpoint_elision = true;

-- DELETE: the edge b is the delete target (an edge, never an elidable node);
-- the endpoint () is elided.
SET enable_graph_endpoint_elision = true;
BEGIN;
  MATCH (a:person)-[b:knows]->() DELETE b;
  MATCH ()-[r:knows]->() RETURN count(*) AS knows_left;
ROLLBACK;
SET enable_graph_endpoint_elision = false;
BEGIN;
  MATCH (a:person)-[b:knows]->() DELETE b;
  MATCH ()-[r:knows]->() RETURN count(*) AS knows_left;
ROLLBACK;
SET enable_graph_endpoint_elision = true;

-- DETACH DELETE: the anchor a survives (it is used), the endpoint () is elided,
-- and DETACH still removes a's incident edges.
SET enable_graph_endpoint_elision = true;
BEGIN;
  MATCH (a:person {name:'alice'})-[b:knows]->() DETACH DELETE a;
  MATCH (v:person) RETURN v.name AS v ORDER BY v;
  MATCH ()-[r:knows]->() RETURN count(*) AS knows_left;
ROLLBACK;
SET enable_graph_endpoint_elision = false;
BEGIN;
  MATCH (a:person {name:'alice'})-[b:knows]->() DETACH DELETE a;
  MATCH (v:person) RETURN v.name AS v ORDER BY v;
  MATCH ()-[r:knows]->() RETURN count(*) AS knows_left;
ROLLBACK;
SET enable_graph_endpoint_elision = true;

-- MERGE: the outer MATCH endpoint () is elided; the MERGE pattern is unaffected.
SET enable_graph_endpoint_elision = true;
BEGIN;
  MATCH (a:person)-[b:knows]->() MERGE (a)-[:tagged]->(:note {v:9});
  MATCH (a:person)-[t:tagged]->(n:note) RETURN a.name AS a, n.v AS v ORDER BY a, v;
ROLLBACK;
SET enable_graph_endpoint_elision = false;
BEGIN;
  MATCH (a:person)-[b:knows]->() MERGE (a)-[:tagged]->(:note {v:9});
  MATCH (a:person)-[t:tagged]->(n:note) RETURN a.name AS a, n.v AS v ORDER BY a, v;
ROLLBACK;
SET enable_graph_endpoint_elision = true;

-- MATCH ... WITH ... write: the endpoint () before a WITH pipeline is elided,
-- and the downstream SET still runs on the carried anchor.
SET enable_graph_endpoint_elision = true;
BEGIN;
  MATCH (a:person)-[b:knows]->() WITH DISTINCT a SET a.w = 1;
  MATCH (p:person) RETURN p.name AS p, p.w AS w ORDER BY p;
ROLLBACK;
SET enable_graph_endpoint_elision = false;
BEGIN;
  MATCH (a:person)-[b:knows]->() WITH DISTINCT a SET a.w = 1;
  MATCH (p:person) RETURN p.name AS p, p.w AS w ORDER BY p;
ROLLBACK;
SET enable_graph_endpoint_elision = true;

DROP GRAPH we CASCADE;
