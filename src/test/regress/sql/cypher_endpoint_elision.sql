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
-- An endpoint that names a label joins for a second reason: its scan is what
-- tests the label.  That test does not need the scan either, because a vertex's
-- label is the top sixteen bits of its graph id -- so it is written instead as a
-- range on the id the adjacent edge already holds, and the endpoint goes the
-- same way as any other.
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
-- ELIDE: labelled endpoint, its label becoming a range on the edge
-- ============================================================
-- A label is the top sixteen bits of a vertex's graph id, so the ids of one
-- label are one stretch of the graph id order and the label an endpoint names
-- can be tested against the id the adjacent edge already holds.  The endpoint's
-- scan goes; the label survives as a range on the edge's own endpoint column,
-- which the edge's index on that column can seek to.
-- terminal labelled endpoint
EXPLAIN (costs off) MATCH (a:person)-[:works_at]->(:company) RETURN a.name AS a;
MATCH (a:person)-[:works_at]->(:company) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[:works_at]->(:company) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- reverse (start-side) labelled endpoint, otherwise unused
EXPLAIN (costs off) MATCH (:company)<-[:works_at]-(a) RETURN a.name AS a;

-- intermediate labelled endpoint: the middle (:city) label is tested on both
-- edges that meet there, since an equivalence carries an equality to every
-- member but not a range
EXPLAIN (costs off) MATCH (a:person)-[b:lives_in]->(:city)-[d]->(e) RETURN a.name AS a, label(e) AS e;
MATCH (a:person)-[b:lives_in]->(:city)-[d]->(e) RETURN a.name AS a, label(e) AS e ORDER BY a, e;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[b:lives_in]->(:city)-[d]->(e) RETURN a.name AS a, label(e) AS e ORDER BY a, e;
SET enable_graph_endpoint_elision = true;

-- The label still filters.  A relationship whose far end is a city answers
-- nothing when the pattern asks for a company, and everything when it asks for
-- a city -- which is the whole point of the range that replaced the join.
EXPLAIN (costs off) MATCH (a:person)-[:lives_in]->(:company) RETURN a.name AS a;
MATCH (a:person)-[:lives_in]->(:company) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[:lives_in]->(:company) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;
MATCH (a:person)-[:lives_in]->(:city) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[:lives_in]->(:city) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- ONLY reads the one table, so the label stands alone and the range is exact
EXPLAIN (costs off) MATCH (a:person)-[:works_at]->(:company ONLY) RETURN a.name AS a;
MATCH (a:person)-[:works_at]->(:company ONLY) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:person)-[:works_at]->(:company ONLY) RETURN a.name AS a ORDER BY a;
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
--
-- Reading the path itself reaches the endpoint, so the join that produces it
-- has to stay.  Asking only how long the path is does not: a pattern of a fixed
-- shape settles that without building the path at all, which leaves the
-- endpoint unread and lets it be elided like any other.
-- ============================================================
EXPLAIN (costs off) MATCH p = (a:person)-[:knows]->() RETURN nodes(p) AS ns;
MATCH p = (a:person)-[:knows]->() RETURN size(nodes(p)) AS n ORDER BY n;
SET enable_graph_endpoint_elision = false;
MATCH p = (a:person)-[:knows]->() RETURN size(nodes(p)) AS n ORDER BY n;
SET enable_graph_endpoint_elision = true;

-- ELIDE: the length of a fixed-shape path reads nothing
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

--
-- Counting what a fixed-shape path holds
--
-- Counting a path's nodes or relationships is the same question its length
-- answers, so it is answered the same way and neither array is built.  An array
-- of nodes is built out of whole nodes, so counting one by building it reads
-- every property map on the path; the plans below read none.
--
CREATE GRAPH cnt_g;
SET graph_path = cnt_g;
CREATE VLABEL cn;
CREATE ELABEL cr;
CREATE (:cn {nm: 'a'})-[:cr]->(:cn {nm: 'b'});

EXPLAIN (COSTS OFF, VERBOSE)
MATCH p=(a:cn)-[:cr]->(b:cn) RETURN size(vertices(p));
EXPLAIN (COSTS OFF, VERBOSE)
MATCH p=(a:cn)-[:cr]->(b:cn) RETURN size(edges(p));

-- size() answers with length(), so the two spellings are one question and have
-- to be answered alike
EXPLAIN (COSTS OFF, VERBOSE)
MATCH p=(a:cn)-[:cr]->(b:cn) RETURN length(vertices(p));

MATCH p=(a:cn)-[:cr]->(b:cn)
RETURN size(vertices(p)), length(vertices(p)),
       size(edges(p)), length(edges(p)), length(p);

-- a variable-length relationship settles no count until it runs, so its array
-- is still counted at run time
EXPLAIN (COSTS OFF)
MATCH p=(a:cn)-[:cr*1..2]->(b) RETURN size(vertices(p));

DROP GRAPH cnt_g CASCADE;

-- ============================================================
-- ELIDE: a labelled endpoint whose label is inherited by others
-- ============================================================
-- Label ids are handed out in creation order, so a label and the labels
-- inheriting from it are not next to each other and one stretch of the graph id
-- order will not describe them: the test becomes one stretch per label of the
-- subtree.  Creating an unrelated label in between makes the ids genuinely
-- non-adjacent, so a single stretch would be wrong here rather than merely
-- imprecise.  Its own graph, so the vertex counts of the tests above stand.
CREATE GRAPH eeh;
SET graph_path = eeh;

CREATE VLABEL giver;
CREATE VLABEL org;
CREATE VLABEL unrelated_gap;
CREATE VLABEL charity INHERITS (org);
CREATE ELABEL donates_to;

-- Every target label is given a giver of its own, so that a range wider than
-- the label it was written for shows up as a giver that should not be there.  A
-- giver reaching two of the targets would let an over-broad range hide behind
-- the row it is already entitled to return.
CREATE (:giver {name:'alice'}), (:giver {name:'bob'}), (:giver {name:'carol'}),
       (:org {name:'plain_org'}), (:charity {name:'oxfam'}),
       (:unrelated_gap {name:'gap'});
MATCH (a:giver {name:'alice'}), (o:org {name:'plain_org'})
  CREATE (a)-[:donates_to]->(o);
MATCH (a:giver {name:'bob'}), (c:charity {name:'oxfam'})
  CREATE (a)-[:donates_to]->(c);
MATCH (a:giver {name:'carol'}), (g:unrelated_gap {name:'gap'})
  CREATE (a)-[:donates_to]->(g);

-- the label ids the subtree occupies, and the unrelated one sitting between them
SELECT labname, labid FROM pg_catalog.ag_label
WHERE graphid = (SELECT oid FROM pg_catalog.ag_graph WHERE graphname = 'eeh')
  AND labname IN ('org', 'unrelated_gap', 'charity')
ORDER BY labid;

-- the parent label matches itself and the child, and not the unrelated label
EXPLAIN (costs off) MATCH (a:giver)-[:donates_to]->(:org) RETURN a.name AS a;
MATCH (a:giver)-[:donates_to]->(:org) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:giver)-[:donates_to]->(:org) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- ONLY the parent excludes the child
EXPLAIN (costs off) MATCH (a:giver)-[:donates_to]->(:org ONLY) RETURN a.name AS a;
MATCH (a:giver)-[:donates_to]->(:org ONLY) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:giver)-[:donates_to]->(:org ONLY) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- and the child alone matches only itself -- a label nothing inherits is one
-- stretch, so one range and no alternatives
EXPLAIN (costs off) MATCH (a:giver)-[:donates_to]->(:charity) RETURN a.name AS a;
MATCH (a:giver)-[:donates_to]->(:charity) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:giver)-[:donates_to]->(:charity) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- the label whose id sits between the parent's and the child's answers only its
-- own giver: neither range reaches it, and the pair of them does not close over
-- the gap between them
EXPLAIN (costs off) MATCH (a:giver)-[:donates_to]->(:unrelated_gap) RETURN a.name AS a;
MATCH (a:giver)-[:donates_to]->(:unrelated_gap) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:giver)-[:donates_to]->(:unrelated_gap) RETURN a.name AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

DROP GRAPH eeh CASCADE;

-- ============================================================
-- ELIDE: a label whose id sets the top bit of a graph id
-- ============================================================
-- A label id occupies the top sixteen bits of a graph id, so a label id of
-- 32768 or more sets the id's sign bit.  Graph ids are compared as unsigned, so
-- such ids still sort above every id of a lower label and the stretch a range
-- names is still the stretch the index holds; read as signed they would sort
-- below, and a range written for one label would answer for another.
--
-- Label ids come from a per-graph sequence, so pushing that sequence past 32767
-- is enough to get one.  Every target label is given a source of its own, so
-- that a range reaching a label it was not written for shows up as a source that
-- should not be there rather than hiding behind a row already expected.
CREATE GRAPH eeb;
SET graph_path = eeb;

CREATE VLABEL src;
CREATE VLABEL low_t;
-- past the sign bit from here on
SELECT setval('eeb.ag_label_seq', 40000);
CREATE VLABEL high_t;
CREATE VLABEL high_u;
-- created after high_u, so high_t's subtree is two stretches with high_u's
-- between them
CREATE VLABEL high_kid INHERITS (high_t);
CREATE ELABEL links;

CREATE (:src {n:'to_low'})-[:links]->(:low_t),
       (:src {n:'to_high_t'})-[:links]->(:high_t),
       (:src {n:'to_high_u'})-[:links]->(:high_u),
       (:src {n:'to_high_kid'})-[:links]->(:high_kid);

-- the label ids in play: one below the sign bit, three above it
SELECT labname, labid FROM pg_catalog.ag_label
WHERE graphid = (SELECT oid FROM pg_catalog.ag_graph WHERE graphname = 'eeb')
  AND labname IN ('low_t', 'high_t', 'high_u', 'high_kid')
ORDER BY labid;

-- the ordering the ranges rest on: the first id of a label past the sign bit is
-- above the last id of a label below it
SELECT '40001.0'::graphid > '4.281474976710655'::graphid AS high_above_low;

-- a label below the sign bit answers only for itself, though every other vertex
-- in the graph has an id whose top bit is set
EXPLAIN (costs off) MATCH (a:src)-[:links]->(:low_t) RETURN a.n AS a;
MATCH (a:src)-[:links]->(:low_t) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:src)-[:links]->(:low_t) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- one label past the sign bit, read alone
EXPLAIN (costs off) MATCH (a:src)-[:links]->(:high_t ONLY) RETURN a.n AS a;
MATCH (a:src)-[:links]->(:high_t ONLY) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:src)-[:links]->(:high_t ONLY) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- the label sitting between the two the subtree below occupies: its own source,
-- and neither neighbour's range reaches it
EXPLAIN (costs off) MATCH (a:src)-[:links]->(:high_u) RETURN a.n AS a;
MATCH (a:src)-[:links]->(:high_u) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:src)-[:links]->(:high_u) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- a subtree of two labels past the sign bit: two stretches, and the label
-- between them stays out
EXPLAIN (costs off) MATCH (a:src)-[:links]->(:high_t) RETURN a.n AS a;
MATCH (a:src)-[:links]->(:high_t) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:src)-[:links]->(:high_t) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- the child of that subtree, alone
EXPLAIN (costs off) MATCH (a:src)-[:links]->(:high_kid) RETURN a.n AS a;
MATCH (a:src)-[:links]->(:high_kid) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:src)-[:links]->(:high_kid) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

DROP GRAPH eeb CASCADE;

-- ============================================================
-- ELIDE / NOT ELIDE: how many stretches a range test is written for
-- ============================================================
-- A label inherited by others needs one stretch per label of its subtree, and
-- past eight of them the endpoint keeps the join it would have had anyway.  Both
-- sides of that boundary out of one hierarchy: eight reads eight labels and
-- elides, and the label eight itself inherits from reads nine and does not.  An
-- unrelated label created among the children keeps the ids from running
-- together, so eight stretches really are eight.
CREATE GRAPH eew;
SET graph_path = eew;

CREATE VLABEL src;
CREATE VLABEL nine;
CREATE VLABEL eight INHERITS (nine);
CREATE VLABEL w1 INHERITS (eight);
CREATE VLABEL w2 INHERITS (eight);
CREATE VLABEL gap;
CREATE VLABEL w3 INHERITS (eight);
CREATE VLABEL w4 INHERITS (eight);
CREATE VLABEL w5 INHERITS (eight);
CREATE VLABEL w6 INHERITS (eight);
CREATE VLABEL w7 INHERITS (eight);
CREATE ELABEL to_t;

-- one source per target label, named after it, so the answer says which labels
-- the ranges admitted
CREATE (:src {n:'nine'})-[:to_t]->(:nine),
       (:src {n:'eight'})-[:to_t]->(:eight),
       (:src {n:'w1'})-[:to_t]->(:w1),
       (:src {n:'w2'})-[:to_t]->(:w2),
       (:src {n:'w3'})-[:to_t]->(:w3),
       (:src {n:'w4'})-[:to_t]->(:w4),
       (:src {n:'w5'})-[:to_t]->(:w5),
       (:src {n:'w6'})-[:to_t]->(:w6),
       (:src {n:'w7'})-[:to_t]->(:w7),
       (:src {n:'gap'})-[:to_t]->(:gap);

SELECT labname, labid FROM pg_catalog.ag_label
WHERE graphid = (SELECT oid FROM pg_catalog.ag_graph WHERE graphname = 'eew')
  AND labname <> 'ag_vertex' AND labname <> 'ag_edge'
ORDER BY labid;

-- eight labels: elided, one stretch each, and neither `nine' nor `gap'
EXPLAIN (costs off) MATCH (a:src)-[:to_t]->(:eight) RETURN a.n AS a;
MATCH (a:src)-[:to_t]->(:eight) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:src)-[:to_t]->(:eight) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- nine labels: the endpoint keeps its scan and its join, and answers the same
EXPLAIN (costs off) MATCH (a:src)-[:to_t]->(:nine) RETURN a.n AS a;
MATCH (a:src)-[:to_t]->(:nine) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:src)-[:to_t]->(:nine) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- ONLY brings either of them back to one table and one stretch
EXPLAIN (costs off) MATCH (a:src)-[:to_t]->(:nine ONLY) RETURN a.n AS a;
MATCH (a:src)-[:to_t]->(:nine ONLY) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:src)-[:to_t]->(:nine ONLY) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

DROP GRAPH eew CASCADE;

-- ============================================================
-- ELIDE: one relationship label reaching several vertex labels
-- ============================================================
-- Where a relationship label's rows begin at more than one vertex label, or end
-- at more than one, scan pruning cannot narrow that relationship by the label
-- the pattern asks for -- the relationship's own table holds rows for every one
-- of them.  The range on its endpoint column is then the only thing separating
-- them, so this is the shape where an over-broad range would return rows the
-- pattern did not ask for.
CREATE GRAPH eem;
SET graph_path = eem;

CREATE VLABEL author;
CREATE VLABEL editor;
CREATE VLABEL book;
CREATE VLABEL journal;
CREATE ELABEL wrote;

CREATE (:author {n:'ann'})-[:wrote]->(:book {n:'b1'});
CREATE (:editor {n:'ed'})-[:wrote]->(:book {n:'b2'});
MATCH (a:author {n:'ann'}) CREATE (a)-[:wrote]->(:journal {n:'j1'});

-- the start side elided: `wrote' begins at both author and editor, so the range
-- on wrote.start is what tells them apart
EXPLAIN (costs off) MATCH (:author)-[:wrote]->(x) RETURN x.n AS x;
MATCH (:author)-[:wrote]->(x) RETURN x.n AS x ORDER BY x;
MATCH (:editor)-[:wrote]->(x) RETURN x.n AS x ORDER BY x;
SET enable_graph_endpoint_elision = false;
MATCH (:author)-[:wrote]->(x) RETURN x.n AS x ORDER BY x;
MATCH (:editor)-[:wrote]->(x) RETURN x.n AS x ORDER BY x;
SET enable_graph_endpoint_elision = true;

-- the end side elided: `wrote' ends at both book and journal
EXPLAIN (costs off) MATCH (a)-[:wrote]->(:journal) RETURN a.n AS a;
MATCH (a)-[:wrote]->(:book) RETURN a.n AS a ORDER BY a;
MATCH (a)-[:wrote]->(:journal) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a)-[:wrote]->(:book) RETURN a.n AS a ORDER BY a;
MATCH (a)-[:wrote]->(:journal) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- both endpoints labelled and both elided: the pattern becomes one scan of the
-- relationship carrying a range on each of its endpoint columns, and the pair
-- that no relationship joins answers nothing
EXPLAIN (costs off) MATCH (:author)-[:wrote]->(:journal) RETURN count(*);
MATCH (:author)-[:wrote]->(:book) RETURN count(*);
MATCH (:author)-[:wrote]->(:journal) RETURN count(*);
MATCH (:editor)-[:wrote]->(:journal) RETURN count(*);
SET enable_graph_endpoint_elision = false;
MATCH (:author)-[:wrote]->(:book) RETURN count(*);
MATCH (:author)-[:wrote]->(:journal) RETURN count(*);
MATCH (:editor)-[:wrote]->(:journal) RETURN count(*);
SET enable_graph_endpoint_elision = true;

DROP GRAPH eem CASCADE;

-- ============================================================
-- NOT ELIDE: a labelled endpoint the query has another use for
-- ============================================================
-- Everything that keeps an unlabelled endpoint keeps a labelled one too: naming
-- a label gives the planner a way to test the label without the scan, not a
-- reason to drop a scan something else reads.
CREATE GRAPH eek;
SET graph_path = eek;

CREATE VLABEL emp;
CREATE VLABEL dept;
CREATE VLABEL site;
CREATE ELABEL in_dept;
CREATE ELABEL at_site;
CREATE ELABEL reports_to;

CREATE (:emp {n:'e1'})-[:reports_to]->(:emp {n:'e2'})-[:reports_to]->(:emp {n:'e3'});
MATCH (a:emp {n:'e1'}) CREATE (a)-[:in_dept]->(:dept {n:'d1'});
MATCH (a:emp {n:'e2'}) CREATE (a)-[:in_dept]->(:dept {n:'d2'});
MATCH (d:dept {n:'d1'}) CREATE (d)-[:at_site]->(:site {n:'s1'});

-- read in RETURN
EXPLAIN (costs off) MATCH (a:emp)-[:in_dept]->(d:dept) RETURN d.n AS d;
-- read in WHERE
EXPLAIN (costs off) MATCH (a:emp)-[:in_dept]->(d:dept) WHERE d.n = 'd1' RETURN a.n AS a;
MATCH (a:emp)-[:in_dept]->(d:dept) WHERE d.n = 'd1' RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:emp)-[:in_dept]->(d:dept) WHERE d.n = 'd1' RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;
-- read in ORDER BY
EXPLAIN (costs off) MATCH (a:emp)-[:in_dept]->(d:dept) RETURN a.n AS a ORDER BY id(d);
MATCH (a:emp)-[:in_dept]->(d:dept) RETURN a.n AS a ORDER BY id(d);
SET enable_graph_endpoint_elision = false;
MATCH (a:emp)-[:in_dept]->(d:dept) RETURN a.n AS a ORDER BY id(d);
SET enable_graph_endpoint_elision = true;

-- an inline property constraint on the labelled endpoint has to run
EXPLAIN (costs off) MATCH (a:emp)-[:in_dept]->(:dept {n:'d1'}) RETURN a.n AS a;
MATCH (a:emp)-[:in_dept]->(:dept {n:'d1'}) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:emp)-[:in_dept]->(:dept {n:'d1'}) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- a two-relation qual that no equivalence absorbs lives in joininfo, and goes
-- with the endpoint if the endpoint goes
EXPLAIN (costs off) MATCH (a:emp)-[:in_dept]->(d:dept) WHERE d.n <> a.n RETURN a.n AS a;
MATCH (a:emp)-[:in_dept]->(d:dept) WHERE d.n <> a.n RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:emp)-[:in_dept]->(d:dept) WHERE d.n <> a.n RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- a named path reaches the endpoint, so the join that builds it stays; asking
-- only how long the path is reaches nothing and both endpoints go, leaving the
-- relationship carrying a range on each of its endpoint columns
EXPLAIN (costs off) MATCH p = (a:emp)-[:in_dept]->(:dept) RETURN nodes(p) AS ns;
EXPLAIN (costs off) MATCH p = (a:emp)-[:in_dept]->(:dept) RETURN length(p) AS len;
MATCH p = (a:emp)-[:in_dept]->(:dept) RETURN length(p) AS len ORDER BY len;
SET enable_graph_endpoint_elision = false;
MATCH p = (a:emp)-[:in_dept]->(:dept) RETURN length(p) AS len ORDER BY len;
SET enable_graph_endpoint_elision = true;

-- a variable-length relationship anchors on its far endpoint, labelled or not
EXPLAIN (costs off) MATCH (a:emp)-[:reports_to*1..2]->(:emp) RETURN a.n AS a;
MATCH (a:emp)-[:reports_to*1..2]->(:emp) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:emp)-[:reports_to*1..2]->(:emp) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- an OPTIONAL MATCH endpoint is an outer join's inner side
EXPLAIN (costs off) MATCH (a:emp) OPTIONAL MATCH (a)-[:in_dept]->(:dept) RETURN a.n AS a;
MATCH (a:emp) OPTIONAL MATCH (a)-[:in_dept]->(:dept) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:emp) OPTIONAL MATCH (a)-[:in_dept]->(:dept) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- a labelled endpoint that is what the statement writes to
EXPLAIN (costs off) MATCH (a:emp)-[:in_dept]->(d:dept) SET d.seen = true;

-- and coexistence with pruning at a labelled middle: the middle (:dept) goes,
-- yet the neighbours beyond it keep the single-label scans the solver pruned
-- them to ({at_site}, {site}) -- exactly what they have with elision off
EXPLAIN (costs off) MATCH (a:emp)-[:in_dept]->(:dept)-[g]->(h) RETURN a.n AS a, label(h) AS h;
SET enable_graph_endpoint_elision = false;
EXPLAIN (costs off) MATCH (a:emp)-[:in_dept]->(:dept)-[g]->(h) RETURN a.n AS a, label(h) AS h;
SET enable_graph_endpoint_elision = true;
MATCH (a:emp)-[:in_dept]->(:dept)-[g]->(h) RETURN a.n AS a, label(h) AS h ORDER BY a, h;
SET enable_graph_endpoint_elision = false;
MATCH (a:emp)-[:in_dept]->(:dept)-[g]->(h) RETURN a.n AS a, label(h) AS h ORDER BY a, h;
SET enable_graph_endpoint_elision = true;

DROP GRAPH eek CASCADE;

-- ============================================================
-- NOT ELIDE: an endpoint reading only the root of the vertex hierarchy
-- ============================================================
-- An endpoint naming no label reads the whole vertex hierarchy, and its join is
-- redundant because every relationship endpoint is some vertex.  Read with ONLY
-- it is a different question -- it asks for the vertices held in the root table
-- itself, which is where a pattern that names no label puts the vertices it
-- creates -- and the join that answers it removes rows rather than none.  So
-- that endpoint keeps its join: there is no label id to write a range for.
CREATE GRAPH eeo;
SET graph_path = eeo;

CREATE VLABEL tagged;
CREATE ELABEL points;

CREATE (:tagged {n:'to_tagged'})-[:points]->(:tagged {n:'plain'});
-- an unlabelled target: this one is stored in the root table
CREATE (:tagged {n:'to_rootborn'})-[:points]->({n:'rootborn'});

SELECT count(*) AS rootborn FROM ONLY eeo.ag_vertex;

-- the root alone: the join is kept and answers for the one relationship whose
-- far end is held there
EXPLAIN (costs off) MATCH (a:tagged)-[r:points]->(x:ag_vertex ONLY) RETURN a.n AS a;
MATCH (a:tagged)-[r:points]->(x:ag_vertex ONLY) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:tagged)-[r:points]->(x:ag_vertex ONLY) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- the whole hierarchy: the join is redundant and goes, and both relationships
-- answer
EXPLAIN (costs off) MATCH (a:tagged)-[r:points]->(x) RETURN a.n AS a;
MATCH (a:tagged)-[r:points]->(x) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:tagged)-[r:points]->(x) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

-- a leaf label read with ONLY is one stretch and elides, root or not
EXPLAIN (costs off) MATCH (a:tagged)-[r:points]->(x:tagged ONLY) RETURN a.n AS a;
MATCH (a:tagged)-[r:points]->(x:tagged ONLY) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = false;
MATCH (a:tagged)-[r:points]->(x:tagged ONLY) RETURN a.n AS a ORDER BY a;
SET enable_graph_endpoint_elision = true;

DROP GRAPH eeo CASCADE;

--
-- a labelled endpoint's label is tested on the edge columns that hold its id
--
-- located_in starts at people and at companies alike; a scan of it by the
-- country it ends at is confined to the starts of the label the pattern names
CREATE GRAPH eel;
SET graph_path = eel;
CREATE VLABEL person;
CREATE VLABEL company;
CREATE VLABEL country;
CREATE ELABEL located_in;
CREATE (:country {name: 'a'}), (:country {name: 'b'});
MATCH (c:country) UNWIND range(1, 20) AS i CREATE (:person {n: i})-[:located_in]->(c);
MATCH (c:country) UNWIND range(1, 3) AS i CREATE (:company {n: i})-[:located_in]->(c);
ANALYZE eel.located_in;

-- the company label is tested on the edge's start
EXPLAIN (costs off) MATCH (x:company)-[:located_in]->(c:country {name: 'a'}) RETURN x.n AS n;
MATCH (x:company)-[:located_in]->(c:country {name: 'a'}) RETURN x.n AS n ORDER BY n;

-- an endpoint under an optional pattern keeps its label to itself
EXPLAIN (costs off) MATCH (c:country) OPTIONAL MATCH (x:company)-[:located_in]->(c) RETURN c.name AS c, x.n AS n;
MATCH (c:country) OPTIONAL MATCH (x:company)-[:located_in]->(c) RETURN c.name AS c, x.n AS n ORDER BY c, n;

DROP GRAPH eel CASCADE;
