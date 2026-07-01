--
-- ag_graphmeta-based scan pruning
--
-- Verifies that MATCH endpoints scan only the vertex labels ag_graphmeta says
-- connect, without ever changing query results, that pruned plans are
-- invalidated when connectivity changes, and that non-Cypher writes keep
-- ag_graphmeta complete.
--
CREATE GRAPH gmp;
SET graph_path = gmp;

CREATE VLABEL person;
CREATE VLABEL city;
CREATE VLABEL company;
CREATE ELABEL knows;
CREATE ELABEL lives_in;
CREATE ELABEL works_at;

SET auto_gather_graphmeta = true;

CREATE (:person {name:'alice'}), (:person {name:'bob'}),
       (:city {name:'paris'}), (:company {name:'acme'});
MATCH (a:person {name:'alice'}), (b:person {name:'bob'})   CREATE (a)-[:knows]->(b);
MATCH (a:person {name:'alice'}), (c:city {name:'paris'})   CREATE (a)-[:lives_in]->(c);
MATCH (a:person {name:'bob'}),   (k:company {name:'acme'}) CREATE (a)-[:works_at]->(k);

-- connectivity: knows(person,person), lives_in(person,city), works_at(person,company)
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";

-- Pruning must NEVER change results: on vs off must match.
SET auto_gather_graphmeta = true;
MATCH (a)-[:lives_in]->(b) RETURN properties(b)->>'name' AS b ORDER BY b;
SET auto_gather_graphmeta = false;
MATCH (a)-[:lives_in]->(b) RETURN properties(b)->>'name' AS b ORDER BY b;
SET auto_gather_graphmeta = true;

-- labelled endpoint: end(knows) = person
MATCH (a:person)-[:knows]->(b) RETURN properties(b)->>'name' AS b ORDER BY b;

-- empty-pattern proof: lives_in never ends at company
MATCH (a)-[:lives_in]->(b:company) RETURN b;

-- chain intersection: end(lives_in)=city INTERSECT start(knows)=person = empty
MATCH (a)-[:lives_in]->(b)-[:knows]->(c) RETURN c;

-- undirected unions both sides
MATCH (a)-[:lives_in]-(b) RETURN properties(b)->>'name' AS b ORDER BY b;

-- variable-length edge falls back to full scan, still correct
MATCH (a:person {name:'alice'})-[:knows*1..2]->(b) RETURN properties(b)->>'name' AS b ORDER BY b;

-- plan-cache invalidation: a prepared pruned plan must see new connectivity
PREPARE lp AS MATCH (a)-[:lives_in]->(b) RETURN properties(b)->>'name' AS b ORDER BY b;
EXECUTE lp;
MATCH (a:person {name:'alice'}), (k:company {name:'acme'}) CREATE (a)-[:lives_in]->(k);
EXECUTE lp;
DEALLOCATE lp;

-- bypass maintenance: a direct SQL INSERT of new connectivity is recorded
SET enable_graph_dml = on;
INSERT INTO gmp.knows (id, start, "end", properties)
SELECT graphid(el.labid, 999), c.id, p.id, jsonb_build_object()
FROM ag_label el,
     (SELECT id FROM gmp.city   ORDER BY id LIMIT 1) c,
     (SELECT id FROM gmp.person ORDER BY id LIMIT 1) p
WHERE el.labname = 'knows'
  AND el.graphid = (SELECT oid FROM ag_graph WHERE graphname = 'gmp');
SET enable_graph_dml = off;
-- now knows(city,person) is present in addition to knows(person,person)
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";

DROP GRAPH gmp CASCADE;

-- ============================================================
-- node-label-driven propagation through UNLABELLED edges
-- A labelled node must constrain adjacent unlabelled edges and far nodes,
-- transitively -- the generalization over the labelled-edge-only case above.
-- Chain n1 -e1-> n2 -e2-> n3, plus an unrelated x -ex-> y.
-- ============================================================
CREATE GRAPH gmp2;
SET graph_path = gmp2;
CREATE VLABEL n1; CREATE VLABEL n2; CREATE VLABEL n3;
CREATE VLABEL x;  CREATE VLABEL y;
CREATE ELABEL e1; CREATE ELABEL e2; CREATE ELABEL ex;
SET auto_gather_graphmeta = true;
CREATE (:n1)-[:e1]->(:n2)-[:e2]->(:n3);
CREATE (:x)-[:ex]->(:y);
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";

-- headline: (a:n1)-[b]->(c) must scan only e1 for b and only n2 for c
EXPLAIN (COSTS off) MATCH (a:n1)-[b]->(c) RETURN c;
MATCH (a:n1)-[b]->(c) RETURN label(b) AS edge, label(c) AS node;

-- multi-hop node-driven propagation: a:n1 -[]-> () -[]-> (d) reaches only n3
MATCH (a:n1)-[]->()-[]->(d) RETURN label(d) AS node;

-- ONLY anchor still drives the adjacent edge / far node
MATCH (a:n1 ONLY)-[b]->(c) RETURN label(b) AS edge, label(c) AS node;

-- node-driven empty proof: n3 starts no edge => no rows
MATCH (a:n3)-[b]->(c) RETURN c;

-- self-referential start label is fine: x starts only ex into y
MATCH (a:x)-[b]->(c) RETURN label(b) AS edge, label(c) AS node;

-- result-equivalence: node-driven prune must match the unpruned (GUC off) scan
SET auto_gather_graphmeta = false;
MATCH (a:n1)-[b]->(c) RETURN label(b) AS edge, label(c) AS node;
SET auto_gather_graphmeta = true;

DROP GRAPH gmp2 CASCADE;

-- ============================================================
-- parent-self drop vs retention, multi-child Append, and Cypher
-- writes under a pruned MATCH.
-- ============================================================
CREATE GRAPH gmp3;
SET graph_path = gmp3;
CREATE VLABEL human; CREATE VLABEL robot;
CREATE ELABEL knows;
SET auto_gather_graphmeta = true;

-- knows ends at BOTH human and robot (two children, no unlabelled endpoint yet)
CREATE (:human {n:'a'})-[:knows]->(:human {n:'b'});
CREATE (:human {n:'a2'})-[:knows]->(:robot {n:'r'});
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";

-- multi-child, parent dropped: b scans {human, robot} with NO ag_vertex parent
EXPLAIN (COSTS off) MATCH (a:human)-[:knows]->(b) RETURN b;
MATCH (a:human)-[:knows]->(b) RETURN label(b) AS l ORDER BY l;
SET auto_gather_graphmeta = false;
MATCH (a:human)-[:knows]->(b) RETURN label(b) AS l ORDER BY l;
SET auto_gather_graphmeta = true;

-- now add an UNLABELLED endpoint: CREATE (n) goes into ag_vertex, so knows now
-- connects to ag_vertex's own label => the abstract parent MUST be retained.
CREATE (:human {n:'c'})-[:knows]->();
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";

-- parent-self RETAINED: b's scan must now include ag_vertex, and the unlabelled
-- vertex must be returned (proves the keep-condition fires).
EXPLAIN (COSTS off) MATCH (a:human)-[:knows]->(b) RETURN b;
MATCH (a:human)-[:knows]->(b) RETURN label(b) AS l ORDER BY l;

-- Cypher write under a pruned MATCH: routed by labid, independent of the scan.
MATCH (a:human {n:'a'})-[r:knows]->(b) SET r.tag = 1;
MATCH ()-[r:knows]->() WHERE (r).tag = 1 RETURN count(*);
MATCH (a:human {n:'a'})-[r:knows]->(b) DELETE r;
MATCH (a:human {n:'a'})-[r:knows]->(b) RETURN count(*);

DROP GRAPH gmp3 CASCADE;

-- ============================================================
-- multi-clause MATCH and read-your-own-writes under a cached plan.
-- Pattern-element ids are numbered per Cypher clause, so cross-clause endpoint
-- resolution must be derived per-edge (not via a query-global id map, whose
-- ids collide across clauses).  And a cached pruned plan must observe an
-- uncommitted same-transaction edge write.  Both must match the unpruned scan.
-- ============================================================
CREATE GRAPH gmp4;
SET graph_path = gmp4;
CREATE VLABEL person; CREATE VLABEL company; CREATE VLABEL city;
CREATE ELABEL knows; CREATE ELABEL works_at; CREATE ELABEL lives_in;
SET auto_gather_graphmeta = true;
CREATE (:person {n:'a'})-[:knows]->(:person {n:'b'});
CREATE (p:person {n:'c'}) CREATE (k:company {n:'acme'}) CREATE (p)-[:works_at]->(k);
CREATE (p:person {n:'d'}) CREATE (c:city {n:'paris'}) CREATE (p)-[:lives_in]->(c);
-- connectivity: knows(person,person), works_at(person,company), lives_in(person,city)
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";

-- two independent clauses: e1's endpoint q must be constrained by its OWN clause
-- (knows => person), not by the second clause.  On must equal off.
MATCH (p:person)-[e1:knows]->(q) MATCH (m)-[e2:works_at]->(n:company)
  RETURN label(q) AS q, label(m) AS m ORDER BY q, m;
SET auto_gather_graphmeta = false;
MATCH (p:person)-[e1:knows]->(q) MATCH (m)-[e2:works_at]->(n:company)
  RETURN label(q) AS q, label(m) AS m ORDER BY q, m;
SET auto_gather_graphmeta = true;

-- three clauses reusing the same labels (maximal clause-local id collisions)
MATCH (a:person)-[:knows]->(b) MATCH (c:person)-[:works_at]->(d) MATCH (e:person)-[:lives_in]->(f)
  RETURN label(b) AS b, label(d) AS d, label(f) AS f ORDER BY b, d, f;
SET auto_gather_graphmeta = false;
MATCH (a:person)-[:knows]->(b) MATCH (c:person)-[:works_at]->(d) MATCH (e:person)-[:lives_in]->(f)
  RETURN label(b) AS b, label(d) AS d, label(f) AS f ORDER BY b, d, f;
SET auto_gather_graphmeta = true;

-- cached (generic) pruned plan must see an uncommitted same-transaction write:
-- lives_in end is {city}; after creating lives_in(person,company) in the same txn,
-- the re-EXECUTE must include company too.
SET plan_cache_mode = force_generic_plan;
PREPARE cp AS MATCH (a)-[:lives_in]->(b) RETURN label(b) AS b ORDER BY b;
EXECUTE cp;
BEGIN;
  MATCH (p:person {n:'d'}), (k:company {n:'acme'}) CREATE (p)-[:lives_in]->(k);
  EXECUTE cp;
ROLLBACK;
DEALLOCATE cp;
SET plan_cache_mode = auto;

DROP GRAPH gmp4 CASCADE;

-- ============================================================
-- comprehensive MATCH scan-pruning coverage on a linear chain plus an
-- unrelated component.  EXPLAIN (costs off) asserts the pruned plan shape; the
-- accompanying query asserts the matched labels.  A result-equivalence sweep at
-- the end re-runs the patterns with gathering OFF to prove pruning never
-- changes rows.
-- ============================================================
CREATE GRAPH gmp5;
SET graph_path = gmp5;
CREATE VLABEL n1; CREATE VLABEL n2; CREATE VLABEL n3; CREATE VLABEL n4; CREATE VLABEL n5;
CREATE VLABEL q1; CREATE VLABEL q2; CREATE VLABEL q3;
CREATE ELABEL e1; CREATE ELABEL e2; CREATE ELABEL e3; CREATE ELABEL e4;
CREATE ELABEL eX; CREATE ELABEL eY;
SET auto_gather_graphmeta = true;
-- main chain n1 -e1-> n2 -e2-> n3 -e3-> n4 -e4-> n5
CREATE (:n1)-[:e1]->(:n2)-[:e2]->(:n3)-[:e3]->(:n4)-[:e4]->(:n5);
-- unrelated component: must never appear in an anchored plan
CREATE (:q1)-[:eX]->(:q2)-[:eY]->(:q3);
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";

-- ---------- single hop ----------
-- Case 1: first node labelled -> b={e1}, c={n2}
EXPLAIN (costs off) MATCH (a:n1)-[b]->(c) RETURN c;
MATCH (a:n1)-[b]->(c) RETURN label(b) AS b, label(c) AS c ORDER BY b, c;
-- Case 2: far node labelled -> b={e1}, a={n1}
EXPLAIN (costs off) MATCH (a)-[b]->(c:n2) RETURN c;
MATCH (a)-[b]->(c:n2) RETURN label(a) AS a, label(b) AS b ORDER BY a, b;
-- Case 3: both nodes labelled -> b={e1}
EXPLAIN (costs off) MATCH (a:n1)-[b]->(c:n2) RETURN c;
MATCH (a:n1)-[b]->(c:n2) RETURN label(b) AS b ORDER BY b;
-- Case 4: edge labelled -> a={n1}, c={n2}
EXPLAIN (costs off) MATCH (a)-[b:e1]->(c) RETURN c;
MATCH (a)-[b:e1]->(c) RETURN label(a) AS a, label(c) AS c ORDER BY a, c;

-- ---------- multi hop ----------
-- Case 5: first node labelled drives the whole chain
EXPLAIN (costs off) MATCH (a:n1)-[b]->(c)-[d]->(e) RETURN e;
MATCH (a:n1)-[b]->(c)-[d]->(e) RETURN label(c) AS c, label(d) AS d, label(e) AS e ORDER BY c, d, e;
-- Case 6: middle node labelled
EXPLAIN (costs off) MATCH (a)-[b]->(c:n2)-[d]->(e) RETURN e;
MATCH (a)-[b]->(c:n2)-[d]->(e) RETURN label(a) AS a, label(b) AS b, label(e) AS e ORDER BY a, b, e;
-- Case 7: last node labelled
EXPLAIN (costs off) MATCH (a)-[b]->(c)-[d]->(e:n3) RETURN e;
MATCH (a)-[b]->(c)-[d]->(e:n3) RETURN label(a) AS a, label(c) AS c ORDER BY a, c;
-- Case 8: all nodes labelled
EXPLAIN (costs off) MATCH (a:n1)-[b]->(c:n2)-[d]->(e:n3) RETURN e;
MATCH (a:n1)-[b]->(c:n2)-[d]->(e:n3) RETURN label(b) AS b, label(d) AS d ORDER BY b, d;
-- Case 9: first edge labelled drives the chain transitively
EXPLAIN (costs off) MATCH (a)-[b:e1]->(c)-[d]->(e)-[f]->(g) RETURN g;
MATCH (a)-[b:e1]->(c)-[d]->(e)-[f]->(g) RETURN label(c) AS c, label(e) AS e, label(g) AS g ORDER BY c, e, g;
-- Case 10: middle edge labelled
EXPLAIN (costs off) MATCH (a)-[b]->(c)-[d:e2]->(e)-[f]->(g) RETURN g;
MATCH (a)-[b]->(c)-[d:e2]->(e)-[f]->(g) RETURN label(a) AS a, label(g) AS g ORDER BY a, g;
-- Case 11: last edge labelled
EXPLAIN (costs off) MATCH (a)-[b]->(c)-[d]->(e)-[f:e4]->(g) RETURN g;
MATCH (a)-[b]->(c)-[d]->(e)-[f:e4]->(g) RETURN label(a) AS a, label(g) AS g ORDER BY a, g;
-- Case 12: all edges labelled
EXPLAIN (costs off) MATCH (a)-[b:e1]->(c)-[d:e2]->(e)-[f:e3]->(g) RETURN g;
MATCH (a)-[b:e1]->(c)-[d:e2]->(e)-[f:e3]->(g) RETURN label(a) AS a, label(g) AS g ORDER BY a, g;

-- ---------- LEFT direction (edge maps to graphmeta end/start reversed) ----------
-- (a)<-[b]-(c:n1): c=n1 is the start side, so a prunes to {n2}
EXPLAIN (costs off) MATCH (a)<-[b]-(c:n1) RETURN a;
MATCH (a)<-[b]-(c:n1) RETURN label(a) AS a, label(b) AS b ORDER BY a, b;
-- (a:n2)<-[b]-(c): a=n2 is the end side, so c prunes to {n1}
EXPLAIN (costs off) MATCH (a:n2)<-[b]-(c) RETURN c;
MATCH (a:n2)<-[b]-(c) RETURN label(b) AS b, label(c) AS c ORDER BY b, c;

-- ---------- undirected (node endpoints pruned; edge subquery not) ----------
-- edge labelled: both endpoints prune to start(e1) UNION end(e1) = {n1,n2}
EXPLAIN (costs off) MATCH (a)-[b:e1]-(c) RETURN c;
MATCH (a)-[b:e1]-(c) RETURN label(a) AS a, label(c) AS c ORDER BY a, c;
-- node labelled: far endpoint prunes to n1's undirected neighbours = {n2}
EXPLAIN (costs off) MATCH (a:n1)-[b]-(c) RETURN c;
MATCH (a:n1)-[b]-(c) RETURN label(c) AS c ORDER BY c;
-- far node labelled: near endpoint prunes to n2's undirected neighbours = {n1,n3}
MATCH (a)-[b]-(c:n2) RETURN label(a) AS a ORDER BY a;

-- ---------- ONLY anchor still drives adjacent elements ----------
EXPLAIN (costs off) MATCH (a:n1 ONLY)-[b]->(c) RETURN c;
MATCH (a:n1 ONLY)-[b]->(c) RETURN label(b) AS b, label(c) AS c ORDER BY b, c;

-- ---------- multiple comma-separated components: no false cross-component arc ----------
EXPLAIN (costs off) MATCH (a:n1)-[b]->(c), (x)-[y:eX]->(z) RETURN c, z;
MATCH (a:n1)-[b]->(c), (x)-[y:eX]->(z)
  RETURN label(c) AS c, label(x) AS x, label(z) AS z ORDER BY c, x, z;

-- ---------- multi-anchor: middle node constrained from both sides ----------
EXPLAIN (costs off) MATCH (a:n1)-[b]->(c:n2)-[d]->(e) RETURN e;
MATCH (a:n1)-[b]->(c:n2)-[d]->(e) RETURN label(e) AS e ORDER BY e;

-- ---------- impossible patterns prune to an empty scan (0 rows) ----------
-- n1 has no direct edge to n3
EXPLAIN (costs off) MATCH (a:n1)-[b]->(c:n3) RETURN c;
MATCH (a:n1)-[b]->(c:n3) RETURN c;
-- n5 starts no edge
EXPLAIN (costs off) MATCH (a:n5)-[b]->(c) RETURN c;
MATCH (a:n5)-[b]->(c) RETURN c;

-- ---------- VLE endpoint pruning ----------
-- A labelled, DIRECTED, min>=1 VLE bounds its endpoint NODES -- the start-side
-- endpoint to startside(E), the end-side endpoint to endside(E) -- because every
-- hop is an E-edge (its own subquery scan is never inheritance-pruned).  min=0,
-- undirected, and unlabelled VLEs stay pure barriers.
-- pruned: far endpoint c prunes to endside(e1) = {n2}
EXPLAIN (costs off) MATCH (a:n1)-[:e1*1..2]->(c) RETURN c;
-- pruned: both endpoints (a -> startside(e1)={n1}, c -> endside(e1)={n2})
EXPLAIN (costs off) MATCH (a)-[:e1*2..5]->(c) RETURN c;
-- pruned, LEFT: the near endpoint is the graphmeta end side (a->{n2}), far the start ({n1})
EXPLAIN (costs off) MATCH (a)<-[:e1*1..2]-(c) RETURN c;
-- barrier: min=0 (a zero-length path ends at the start node, outside endside(E))
EXPLAIN (costs off) MATCH (a:n1)-[:e1*0..2]->(c) RETURN c;
-- barrier: undirected (a hop may run backwards)
EXPLAIN (costs off) MATCH (a:n1)-[:e1*1..2]-(c) RETURN c;
-- barrier: unlabelled
EXPLAIN (costs off) MATCH (a)-[*1..2]->(c) RETURN c;
-- result-equivalence (on then off): the barrier counterexamples below are rows a
-- naive prune would drop -- min=0 keeps the zero-length n1, undirected keeps the
-- reverse hop -- so on and off must be identical.
MATCH (a:n1)-[:e1*1..2]->(c) RETURN label(c) AS c ORDER BY c;
MATCH (a)<-[:e1*1..2]-(c) RETURN label(a) AS a, label(c) AS c ORDER BY a, c;
MATCH (a:n1)-[:e1*0..2]->(c) RETURN label(c) AS c ORDER BY c;
MATCH (a:n2)-[:e1*1..1]-(c) RETURN label(c) AS c ORDER BY c;
SET auto_gather_graphmeta = false;
MATCH (a:n1)-[:e1*1..2]->(c) RETURN label(c) AS c ORDER BY c;
MATCH (a)<-[:e1*1..2]-(c) RETURN label(a) AS a, label(c) AS c ORDER BY a, c;
MATCH (a:n1)-[:e1*0..2]->(c) RETURN label(c) AS c ORDER BY c;
MATCH (a:n2)-[:e1*1..1]-(c) RETURN label(c) AS c ORDER BY c;
SET auto_gather_graphmeta = true;

-- ---------- multiple edge labels between the same node labels (disambiguation) ----------
-- second, parallel chain: n1 -eA-> n2 -eB-> n3 -eC-> n4 -eD-> n5, plus na..ne chain
CREATE ELABEL eA; CREATE ELABEL eB; CREATE ELABEL eC; CREATE ELABEL eD;
CREATE VLABEL na; CREATE VLABEL nb; CREATE VLABEL nc; CREATE VLABEL nd; CREATE VLABEL ne;
CREATE (:n1)-[:eA]->(:n2)-[:eB]->(:n3)-[:eC]->(:n4)-[:eD]->(:n5);
CREATE (:na)-[:eA]->(:nb)-[:eB]->(:nc)-[:eC]->(:nd)-[:eD]->(:ne);
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";
-- n1 now starts BOTH e1 and eA into n2: b scans {e1,eA}, c still {n2}
EXPLAIN (costs off) MATCH (a:n1)-[b]->(c) RETURN c;
MATCH (a:n1)-[b]->(c) RETURN label(b) AS b, label(c) AS c ORDER BY b, c;
-- eA connects {n1,na}->{n2,nb}: both endpoints scan two labels each
EXPLAIN (costs off) MATCH (a)-[b:eA]->(c) RETURN c;
MATCH (a)-[b:eA]->(c) RETURN label(a) AS a, label(c) AS c ORDER BY a, c;
-- multi-hop across a parallel-labelled graph, edge-anchored
MATCH (a)-[b:eA]->(c)-[d]->(e) RETURN label(c) AS c, label(e) AS e ORDER BY c, e;

-- ---------- result-equivalence: pruning must never change rows ----------
-- Re-run representative patterns with gathering ON then OFF on the SAME data;
-- the two blocks must be row-identical.
-- gathering ON:
MATCH (a:n1)-[b]->(c) RETURN label(b) AS b, label(c) AS c ORDER BY b, c;
MATCH (a)-[b:e1]->(c) RETURN label(a) AS a, label(c) AS c ORDER BY a, c;
MATCH (a:n1)-[b]->(c)-[d]->(e) RETURN label(c) AS c, label(d) AS d, label(e) AS e ORDER BY c, d, e;
MATCH (a)<-[b]-(c:n1) RETURN label(a) AS a, label(b) AS b ORDER BY a, b;
MATCH (a)-[b:e1]-(c) RETURN label(a) AS a, label(c) AS c ORDER BY a, c;
MATCH (a:n1)-[b]->(c), (x)-[y:eX]->(z) RETURN label(c) AS c, label(x) AS x, label(z) AS z ORDER BY c, x, z;
MATCH (a:n1)-[b]->(c:n3) RETURN c;
MATCH (a)-[b:eA]->(c) RETURN label(a) AS a, label(c) AS c ORDER BY a, c;
-- gathering OFF (rows must match the ON block above):
SET auto_gather_graphmeta = false;
MATCH (a:n1)-[b]->(c) RETURN label(b) AS b, label(c) AS c ORDER BY b, c;
MATCH (a)-[b:e1]->(c) RETURN label(a) AS a, label(c) AS c ORDER BY a, c;
MATCH (a:n1)-[b]->(c)-[d]->(e) RETURN label(c) AS c, label(d) AS d, label(e) AS e ORDER BY c, d, e;
MATCH (a)<-[b]-(c:n1) RETURN label(a) AS a, label(b) AS b ORDER BY a, b;
MATCH (a)-[b:e1]-(c) RETURN label(a) AS a, label(c) AS c ORDER BY a, c;
MATCH (a:n1)-[b]->(c), (x)-[y:eX]->(z) RETURN label(c) AS c, label(x) AS x, label(z) AS z ORDER BY c, x, z;
MATCH (a:n1)-[b]->(c:n3) RETURN c;
MATCH (a)-[b:eA]->(c) RETURN label(a) AS a, label(c) AS c ORDER BY a, c;
SET auto_gather_graphmeta = true;

DROP GRAPH gmp5 CASCADE;

-- ============================================================
-- self-loops and cycles: the solver must terminate and prune correctly when a
-- label reaches itself or the pattern forms a cycle.
-- ============================================================
CREATE GRAPH gmp6;
SET graph_path = gmp6;
CREATE VLABEL sl;
CREATE VLABEL t1; CREATE VLABEL t2; CREATE VLABEL t3;
CREATE ELABEL loop;
CREATE ELABEL te;
SET auto_gather_graphmeta = true;
-- self-loop sl -loop-> sl
CREATE (:sl);
MATCH (s:sl) CREATE (s)-[:loop]->(s);
-- triangle t1 -te-> t2 -te-> t3 -te-> t1
CREATE (:t1)-[:te]->(:t2)-[:te]->(:t3);
MATCH (a:t3), (b:t1) CREATE (a)-[:te]->(b);
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";

-- self-loop: far node is the same label
EXPLAIN (costs off) MATCH (a:sl)-[b]->(c) RETURN c;
MATCH (a:sl)-[b]->(c) RETURN label(b) AS b, label(c) AS c ORDER BY b, c;

-- cycle: three hops around the triangle land back on t1
EXPLAIN (costs off) MATCH (a:t1)-[b]->(c)-[d]->(e)-[f]->(g) RETURN g;
MATCH (a:t1)-[b]->(c)-[d]->(e)-[f]->(g)
  RETURN label(c) AS c, label(e) AS e, label(g) AS g ORDER BY c, e, g;

-- equivalence for the cyclic shapes
SET auto_gather_graphmeta = false;
MATCH (a:sl)-[b]->(c) RETURN label(b) AS b, label(c) AS c ORDER BY b, c;
MATCH (a:t1)-[b]->(c)-[d]->(e)-[f]->(g) RETURN label(c) AS c, label(e) AS e, label(g) AS g ORDER BY c, e, g;
SET auto_gather_graphmeta = true;

DROP GRAPH gmp6 CASCADE;

-- ============================================================
-- prepared statements adapt to connectivity changes: a cached (generic) pruned
-- plan must be re-planned when new labels/edges extend what the pattern reaches,
-- so it never keeps scanning a stale, too-narrow label set (which would drop the
-- newly-connected rows).
-- ============================================================
CREATE GRAPH gmp7;
SET graph_path = gmp7;
CREATE VLABEL n1; CREATE VLABEL n2;
CREATE ELABEL e1;
SET auto_gather_graphmeta = true;
CREATE (:n1)-[:e1]->(:n2);
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";

-- force the generic (cached) plan so we actually exercise plan-cache reuse
SET plan_cache_mode = force_generic_plan;
PREPARE pp AS MATCH (a:n1)-[b]->(c) RETURN label(b) AS b, label(c) AS c ORDER BY b, c;

-- initial cached plan: b scans only {e1}, c only {n2}
EXPLAIN (costs off) EXECUTE pp;
EXECUTE pp;

-- modify the graph: a NEW edge label + vertex label that n1 now also reaches
CREATE VLABEL n3;
CREATE ELABEL e2;
CREATE (:n1)-[:e2]->(:n3);
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";

-- the cached plan must be re-planned: b now scans {e1,e2}, c now {n2,n3}
EXPLAIN (costs off) EXECUTE pp;
EXECUTE pp;

-- extend once more with another new endpoint label reached from n1
CREATE VLABEL n4;
CREATE (:n1)-[:e2]->(:n4);
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";
EXPLAIN (costs off) EXECUTE pp;
EXECUTE pp;

-- an UNRELATED new component must NOT widen n1's pruned set (still {e1,e2}/{n2,n3,n4})
CREATE VLABEL u1; CREATE VLABEL u2; CREATE ELABEL eu;
CREATE (:u1)-[:eu]->(:u2);
EXPLAIN (costs off) EXECUTE pp;
EXECUTE pp;

-- ground truth: the prepared (pruned) result equals the unpruned scan
SET auto_gather_graphmeta = false;
MATCH (a:n1)-[b]->(c) RETURN label(b) AS b, label(c) AS c ORDER BY b, c;
SET auto_gather_graphmeta = true;

DEALLOCATE pp;
SET plan_cache_mode = auto;
DROP GRAPH gmp7 CASCADE;

-- ============================================================
-- bulk COPY maintenance: the copyfrom.c hook must record connectivity for
-- edges loaded via COPY (not just Cypher CREATE), or pruning would drop the
-- bulk-loaded rows.  (Label ids in a fresh graph are deterministic: ag_vertex=1,
-- ag_edge=2, then user labels in creation order; localids start at 1 -- hence
-- the graphid literals below.)
-- ============================================================
CREATE GRAPH gmp8;
SET graph_path = gmp8;
CREATE VLABEL src;             -- labid 3
CREATE VLABEL mid;             -- labid 4
CREATE VLABEL dst;             -- labid 5
CREATE ELABEL rel;             -- labid 6
SET auto_gather_graphmeta = true;
CREATE (:src);                 -- graphid 3.1
CREATE (:mid);                 -- graphid 4.1
CREATE (:dst);                 -- graphid 5.1
-- bulk-load two edges via COPY: src -rel-> mid and mid -rel-> dst
COPY gmp8.rel (id, start, "end", properties) FROM stdin;
6.1	3.1	4.1	{}
6.2	4.1	5.1	{}
\.
-- COPY must have recorded both triples
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";
-- pruning uses the COPY-loaded connectivity: chain from src reaches dst
EXPLAIN (costs off) MATCH (a:src)-[b]->(c)-[d]->(e) RETURN e;
MATCH (a:src)-[b]->(c)-[d]->(e) RETURN label(c) AS c, label(e) AS e ORDER BY c, e;
-- result-equivalence on COPY-loaded data
SET auto_gather_graphmeta = false;
MATCH (a:src)-[b]->(c)-[d]->(e) RETURN label(c) AS c, label(e) AS e ORDER BY c, e;
SET auto_gather_graphmeta = true;
DROP GRAPH gmp8 CASCADE;

-- ============================================================
-- direct-UPDATE maintenance: a non-Cypher UPDATE that rewires an edge endpoint
-- records the NEW connectivity triple (nodeModifyTable.c hook).  The old triple
-- remains (deliberately not decremented -- overstating only causes a harmless
-- extra empty scan, never a dropped row), so pruning still accommodates the new
-- endpoint and results stay correct.
-- ============================================================
CREATE GRAPH gmp9;
SET graph_path = gmp9;
CREATE VLABEL p; CREATE VLABEL c1; CREATE VLABEL c2;
CREATE ELABEL rel;
SET auto_gather_graphmeta = true;
CREATE (:p)-[:rel]->(:c1);
CREATE (:c2);
-- before: rel connects only p -> c1
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";
MATCH (a:p)-[b:rel]->(c) RETURN label(c) AS c ORDER BY c;
-- rewire the edge's endpoint from the c1 vertex to the c2 vertex via direct SQL
SET enable_graph_dml = on;
UPDATE gmp9.rel SET "end" = (SELECT id FROM gmp9.c2 ORDER BY id LIMIT 1);
SET enable_graph_dml = off;
-- after: the new triple (p,rel,c2) is recorded; the old (p,rel,c1) remains (loose)
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";
-- pruning now includes c2 (the new endpoint); the actual row resolves to c2
EXPLAIN (costs off) MATCH (a:p)-[b:rel]->(c) RETURN c;
MATCH (a:p)-[b:rel]->(c) RETURN label(c) AS c ORDER BY c;
-- result-equivalence after the rewire
SET auto_gather_graphmeta = false;
MATCH (a:p)-[b:rel]->(c) RETURN label(c) AS c ORDER BY c;
SET auto_gather_graphmeta = true;
DROP GRAPH gmp9 CASCADE;

-- ============================================================
-- label inheritance: a labelled element whose label has CHILD labels covers the
-- whole subtree; ag_graphmeta records the LEAF labids; and pruning narrows WITHIN
-- the subtree by actual connectivity (a subtree member that participates in no
-- matching edge is dropped from the scan).  Covers vertex- and edge-label trees.
-- ============================================================
CREATE GRAPH gmp10;
SET graph_path = gmp10;
CREATE VLABEL animal;
CREATE VLABEL dog INHERITS (animal);
CREATE VLABEL cat INHERITS (animal);
CREATE ELABEL rel;
CREATE ELABEL subrel INHERITS (rel);
SET auto_gather_graphmeta = true;
CREATE (:animal)-[:rel]->(:dog);
CREATE (:dog)-[:subrel]->(:cat);
-- leaf labids recorded: (animal,rel,dog), (dog,subrel,cat)
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";

-- parent vertex-label anchor: a scans {animal,dog} (cat starts nothing), c scans
-- {dog,cat} (nothing ends at an animal-direct vertex), b covers {rel,subrel}
EXPLAIN (costs off) MATCH (a:animal)-[b]->(c) RETURN c;
MATCH (a:animal)-[b]->(c) RETURN label(a) AS la, label(b) AS lb, label(c) AS lc ORDER BY la, lb, lc;

-- child vertex-label anchor: (a:dog) scans only dog; dog starts subrel -> cat
EXPLAIN (costs off) MATCH (a:dog)-[b]->(c) RETURN c;
MATCH (a:dog)-[b]->(c) RETURN label(b) AS lb, label(c) AS lc ORDER BY lb, lc;

-- parent edge-label anchor: b covers the {rel,subrel} subtree; endpoints span both hops
EXPLAIN (costs off) MATCH (a)-[b:rel]->(c) RETURN c;
MATCH (a)-[b:rel]->(c) RETURN label(a) AS la, label(b) AS lb, label(c) AS lc ORDER BY la, lb, lc;

-- child edge-label anchor: only the subrel hop dog -> cat
MATCH (a)-[b:subrel]->(c) RETURN label(a) AS la, label(c) AS lc ORDER BY la, lc;

-- result-equivalence (on vs off) for the inheritance cases
SET auto_gather_graphmeta = false;
MATCH (a:animal)-[b]->(c) RETURN label(a) AS la, label(b) AS lb, label(c) AS lc ORDER BY la, lb, lc;
MATCH (a:dog)-[b]->(c) RETURN label(b) AS lb, label(c) AS lc ORDER BY lb, lc;
MATCH (a)-[b:rel]->(c) RETURN label(a) AS la, label(b) AS lb, label(c) AS lc ORDER BY la, lb, lc;
MATCH (a)-[b:subrel]->(c) RETURN label(a) AS la, label(c) AS lc ORDER BY la, lc;
SET auto_gather_graphmeta = true;

-- VLE EXPLAIN observability: the Graph VLE node names the edge label it walks
-- and marks subtree expansion (rel has child subrel); VERBOSE lists the actual
-- edge tables walked and the traversal direction (the per-hop edge scan is
-- internal to the node, so this is the only view of what it scans).
EXPLAIN (costs off) MATCH (a:animal)-[:rel*1..2]->(c) RETURN c;
EXPLAIN (verbose, costs off) MATCH (a:animal)-[:rel*1..2]->(c) RETURN c;

DROP GRAPH gmp10 CASCADE;

-- ============================================================
-- COPY + BEFORE-INSERT trigger: the copyfrom.c maintenance hook must record an
-- edge's endpoints AFTER any BEFORE-INSERT trigger has run.  A trigger that
-- rewrites start/end changes which vertex labels the edge connects; recording
-- the pre-trigger endpoints would register the wrong connectivity, and a pruned
-- MATCH would then drop the rows actually stored.  (labids: ag_vertex=1,
-- ag_edge=2, decoy=3, hub=4, rel=5; localids from 1.)
-- ============================================================
CREATE GRAPH gmp11;
SET graph_path = gmp11;
CREATE VLABEL decoy;           -- labid 3
CREATE VLABEL hub;             -- labid 4
CREATE ELABEL rel;             -- labid 5
CREATE (:hub);                 -- graphid 4.1, the real endpoint
-- BEFORE-INSERT trigger on the edge table: force both endpoints onto the hub
-- vertex, regardless of the endpoints handed to COPY.
CREATE FUNCTION gmp11_reroute() RETURNS trigger AS $$
BEGIN
    NEW."start" := (SELECT id FROM gmp11.hub ORDER BY id LIMIT 1);
    NEW."end"   := (SELECT id FROM gmp11.hub ORDER BY id LIMIT 1);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER gmp11_reroute_trg BEFORE INSERT ON gmp11.rel
    FOR EACH ROW EXECUTE FUNCTION gmp11_reroute();
SET auto_gather_graphmeta = true;
-- COPY an edge whose given endpoints point at the decoy label (3.x); the trigger
-- rewrites them to the hub vertex (4.1) before the row is stored.
COPY gmp11.rel (id, start, "end", properties) FROM stdin;
5.1	3.1	3.2	{}
\.
-- the recorded triple must be (hub,rel,hub) = (4,5,4), reflecting the stored row,
-- NOT the pre-trigger decoy endpoints (3,5,3)
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";
-- pruning must keep the hub label; the stored edge connects hub -> hub
EXPLAIN (costs off) MATCH (a:hub)-[b:rel]->(c:hub) RETURN c;
MATCH (a:hub)-[b:rel]->(c:hub) RETURN count(*) AS n;
-- result-equivalence (on vs off): both must see the stored edge
SET auto_gather_graphmeta = false;
MATCH (a:hub)-[b:rel]->(c:hub) RETURN count(*) AS n;
SET auto_gather_graphmeta = true;
DROP GRAPH gmp11 CASCADE;

-- ============================================================
-- shortest-path patterns are lowered to a separate dijkstra/shortestpath
-- subquery, not to inheritance-expanded label scans, so the graphmeta pre-pass
-- records no topology for them and can never mis-prune a shortest-path
-- endpoint.  Confirm shortest paths return complete results with gathering on
-- (alone, and sharing an endpoint variable with a normal pruned MATCH) and
-- match the gathering-off results.
-- ============================================================
CREATE GRAPH gmp12;
SET graph_path = gmp12;
CREATE VLABEL src; CREATE VLABEL mid; CREATE VLABEL dst;
CREATE ELABEL e;
SET auto_gather_graphmeta = true;
CREATE (:src)-[:e]->(:mid)-[:e]->(:dst);
-- only (src,e,mid) and (mid,e,dst) are recorded
SELECT * FROM ag_graphmeta_view ORDER BY start, edge, "end";
-- pure shortest path across the chain: length 2 (src -> mid -> dst)
MATCH p = shortestpath((x:src)-[:e*]->(y:dst)) RETURN length(p) AS len;
-- shortest path sharing endpoint variable a with a normal pruned hop: the hop
-- (a:src)-[:e]->(m) prunes m to {mid}; the shared node a also anchors a
-- shortest path to dst.  Both parts must resolve with gathering on.
MATCH (a:src)-[:e]->(m), p = shortestpath((a)-[:e*]->(d:dst))
  RETURN label(m) AS m, length(p) AS len;
-- allshortestpaths variant, likewise unaffected by pruning
MATCH q = allshortestpaths((x:src)-[:e*]->(y:dst)) RETURN length(q) AS len;
-- result-equivalence (on vs off)
SET auto_gather_graphmeta = false;
MATCH p = shortestpath((x:src)-[:e*]->(y:dst)) RETURN length(p) AS len;
MATCH (a:src)-[:e]->(m), p = shortestpath((a)-[:e*]->(d:dst))
  RETURN label(m) AS m, length(p) AS len;
MATCH q = allshortestpaths((x:src)-[:e*]->(y:dst)) RETURN length(q) AS len;
SET auto_gather_graphmeta = true;
DROP GRAPH gmp12 CASCADE;
