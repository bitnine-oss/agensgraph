--
-- Cypher Query Language - DML
--

-- prepare

DROP TABLE IF EXISTS history;

CREATE TABLE history (year, event) AS VALUES
(1996, 'PostgreSQL'),
(2016, 'Graph');

DROP GRAPH agens CASCADE;
CREATE GRAPH agens;

--
-- RETURN
--

RETURN 3 + 4, 'hello' + ' agens';

RETURN 3 + 4 AS lucky, 'hello' + ' agens' AS greeting;

RETURN (SELECT event FROM history WHERE year = 2016);

SELECT * FROM (RETURN 3 + 4, 'hello' + ' agens') AS _(lucky, greeting);

--
-- CREATE
--
CREATE VLABEL repo;
CREATE ELABEL lib;
CREATE ELABEL doc;

CREATE (g:repo {name: 'agens-graph',
                year: (SELECT year FROM history WHERE event = 'Graph')})
RETURN properties(g) AS g;

MATCH (g:repo)
CREATE (j:repo {name: 'agens-graph-jdbc', year: 2016}),
       (d:repo {name: 'agens-graph-docs', year: 2016})
CREATE (g)-[l:lib {lang: 'java'}]->(j),
       p=(g)
         -[:lib {lang: 'c'}]->
         (:repo {name: 'agens-graph-odbc', year: 2016}),
       (g)-[e:doc {lang: 'en'}]->(d)
RETURN properties(l) AS lj, properties(j) AS j,
       properties((edges(p))[0]) AS lc, properties((vertices(p))[1]) AS c,
       properties(e) AS e, properties(d) AS d;

CREATE ()-[a:lib]->(a);
CREATE a=(), (a);
CREATE (a), (a {});
CREATE (a), (a);
CREATE (=0);
CREATE ()-[]-();
CREATE ()-[]->();
CREATE ()-[:lib|doc]->();
CREATE (a)-[a:lib]->();
CREATE ()-[a:lib]->()-[a:doc]->();
CREATE a=(), ()-[a:doc]->();
CREATE ()-[:lib =0]->();
CREATE (a), a=();
CREATE ()-[a:lib]->(), a=();
CREATE a=(), a=();
CREATE (:lib);
CREATE ()-[:repo]->();
CREATE (:ag_vertex);
CREATE ()-[:ag_edge]->();

CREATE (=null)-[:lib =null]->();
CREATE TABLE t1 (prop jsonb);
CREATE (=(SELECT prop FROM t1))-[:lib =(SELECT prop FROM t1)]->();

MATCH (a) WHERE a.name IS NULL DETACH DELETE a;
DROP TABLE t1;

CREATE GRAPH g_create;
SET GRAPH_PATH = g_create;

CREATE ELABEL e1;

CREATE p=()-[:e1]->() RETURN p;

CREATE (a {name:'agens'}), (b {name:a.name});

DROP GRAPH g_create CASCADE;

--
-- MATCH
--

SET GRAPH_PATH = agens;

MATCH (a) RETURN a.name AS a;
MATCH (a), (a) RETURN a.name AS a;

CREATE ();
MATCH (a:repo) RETURN a.name AS name, a['year'] AS year;

MATCH p=(a)-[b]-(c)
RETURN a.name AS a, b.lang AS b, c.name AS c
       ORDER BY a, b, c;

MATCH (a)<-[b]-(c)-[d]->(e)
RETURN a.name AS a, b.lang AS b, c.name AS c,
       d.lang AS d, e.name AS e
       ORDER BY a, b, c, d, e;

MATCH (a)<-[b]-(c), (c)-[d]->(e)
RETURN a.name AS a, b.lang AS b, c.name AS c,
       d.lang AS d, e.name AS e
       ORDER BY a, b, c, d, e;

MATCH (a)<-[b]-(c) MATCH (c)-[d]->(e)
RETURN a.name AS a, b.lang AS b, c.name AS c,
       d.lang AS d, e.name AS e
       ORDER BY a, b, c, d, e;

MATCH (a)<-[b]-(c), (f)-[g]->(h), (c)-[d]->(e)
RETURN a.name AS a, b.lang AS b, c.name AS c,
       d.lang AS d, e.name AS e,
       f.name AS f, g.lang AS g, h.name AS h
       ORDER BY a, b, c, d, e, f, g, h;

MATCH (a {name: 'agens-graph'}), (a {year: 2016}) RETURN properties(a) AS a;
MATCH p=(a)-[]->({name: 'agens-graph-jdbc'}) RETURN a.name AS a;
MATCH p=()-[:lib]->(a) RETURN a.name AS a;
MATCH p=()-[{lang: 'en'}]->(a) RETURN a.name AS a;

MATCH (a {year: (SELECT to_jsonb(year) FROM history WHERE event = 'Graph')})
WHERE a.name = 'agens-graph'
RETURN a.name AS a;

MATCH (a), (a:repo) RETURN a.name AS a;

MATCH p=({name: 'agens-graph'})-[{lang: 'java'}]->(m) RETURN *;

MATCH ();
MATCH ()-[a]-(), (a) RETURN *;
MATCH a=(), (a) RETURN *;
MATCH (a =0) RETURN *;
MATCH ()-[a]-(a) RETURN *;
MATCH ()-[a]-()-[a]-() RETURN *;
MATCH a=(), ()-[a]-() RETURN *;
MATCH p=()-[:lib|doc]->() RETURN *;
MATCH ()-[a =0]-() RETURN *;
MATCH (a), a=() RETURN *;
MATCH ()-[a]->(), a=() RETURN *;
MATCH a=(), a=() RETURN *;
MATCH (:lib) RETURN *;
MATCH ()-[:repo]->() RETURN *;

MATCH (a {name: properties.name}) RETURN *;
MATCH (a) RETURN a.properties;

-- MATCH ONLY

CREATE VLABEL vl1;
CREATE VLABEL vl2 INHERITS(vl1);
CREATE VLABEL vl3 INHERITS(vl2);

CREATE ELABEL el1;
CREATE ELABEL el2 INHERITS(el1);
CREATE ELABEL el3 INHERITS(el2);

CREATE (:vl1 {id:1});
CREATE (:vl2 {id:2});
CREATE (:vl3 {id:3});

MATCH (A:vl1 {id:1}), (B:vl2 {id:2}) MERGE (A)-[:el1]->(B);
MATCH (A:vl1 {id:1}), (C:vl3 {id:3}) MERGE (A)-[:el2]->(C);
MATCH (B:vl2 {id:2}), (C:vl3 {id:3}) MERGE (B)-[:el3]->(C);

MATCH (N:vl1) RETURN N;
MATCH (N:vl2) RETURN N;
MATCH (N:vl3) RETURN N;
MATCH (N:vl1 ONLY) RETURN N;
MATCH (N:vl2 ONLY) RETURN N;
MATCH (N ONLY) RETURN N;

MATCH (A)-[r:el1]->(B) RETURN A.id, r, B.id;
MATCH (A)-[r:el2]->(B) RETURN A.id, r, B.id;
MATCH (A)-[r:el3]->(B) RETURN A.id, r, B.id;
MATCH (A)-[r:el1 ONLY]->(B) RETURN A.id, r, B.id;
MATCH (A)-[r:el2 ONLY]->(B) RETURN A.id, r, B.id;
MATCH (A)-[r ONLY]->(B) RETURN A.id, r, B.id;

MATCH (A)<-[r:el1]-(B) RETURN A.id, r, B.id;
MATCH (A)<-[r:el2]-(B) RETURN A.id, r, B.id;
MATCH (A)<-[r:el3]-(B) RETURN A.id, r, B.id;
MATCH (A)<-[r:el1 ONLY]-(B) RETURN A.id, r, B.id;
MATCH (A)<-[r:el2 ONLY]-(B) RETURN A.id, r, B.id;

MATCH (A)-[r:el1]-(B) RETURN A.id, r, B.id;
MATCH (A)-[r:el2]-(B) RETURN A.id, r, B.id;
MATCH (A)-[r:el3]-(B) RETURN A.id, r, B.id;
MATCH (A)-[r:el1 ONLY]-(B) RETURN A.id, r, B.id;
MATCH (A)-[r:el2 ONLY]-(B) RETURN A.id, r, B.id;

MATCH (A)-[r:el1 *1..3]->(B) RETURN A.id, r, B.id;
MATCH (A)-[r:el2 *1..3]->(B) RETURN A.id, r, B.id;
MATCH (A)-[r:el3 *1..3]->(B) RETURN A.id, r, B.id;
MATCH (A)-[r:el1 ONLY *1..3]->(B) RETURN A.id, r, B.id;
MATCH (A)-[r:el2 ONLY *1..3]->(B) RETURN A.id, r, B.id;

MATCH (A)<-[r:el1 *1..3]-(B) RETURN A.id, r, B.id;
MATCH (A)<-[r:el2 *1..3]-(B) RETURN A.id, r, B.id;
MATCH (A)<-[r:el3 *1..3]-(B) RETURN A.id, r, B.id;
MATCH (A)<-[r:el1 ONLY *1..3]-(B) RETURN A.id, r, B.id;
MATCH (A)<-[r:el2 ONLY *1..3]-(B) RETURN A.id, r, B.id;

MATCH (A)-[r:el1 *1..3]-(B) RETURN A.id, r, B.id;
MATCH (A)-[r:el2 *1..3]-(B) RETURN A.id, r, B.id;
MATCH (A)-[r:el3 *1..3]-(B) RETURN A.id, r, B.id;
MATCH (A)-[r:el1 ONLY *1..3]-(B) RETURN A.id, r, B.id;
MATCH (A)-[r:el2 ONLY *1..3]-(B) RETURN A.id, r, B.id;

MATCH (A:vl1) DETACH DELETE A;
MATCH (B:vl2) DETACH DELETE B;
MATCH (C:vl3) DETACH DELETE C;

-- OPTIONAL MATCH

CREATE GRAPH o;
SET graph_path = o;

CREATE VLABEL person;
CREATE ELABEL knows;

CREATE (:person {name: 'someone'})-[:knows]->(:person {name: 'somebody'}),
       (:person {name: 'anybody'})-[:knows]->(:person {name: 'nobody'});

OPTIONAL MATCH (n)-[r]->(p), (m)-[s]->(q)
RETURN n.name AS n, type(r) AS r, p.name AS p,
       m.name AS m, type(s) AS s, q.name AS q
ORDER BY n, p, m, q;

MATCH (n:person), (m:person) WHERE id(n) <> id(m)
OPTIONAL MATCH (n)-[r]->(p), (m)-[s]->(q)
RETURN n.name AS n, type(r) AS r, p.name AS p,
       m.name AS m, type(s) AS s, q.name AS q
ORDER BY n, p, m, q;

MATCH (n:person), (m:person) WHERE id(n) <> id(m)
OPTIONAL MATCH (n)-[r]->(p), (m)-[s]->(q) WHERE m.name = 'someone'
RETURN n.name AS n, type(r) AS r, p.name AS p,
       m.name AS m, type(s) AS s, q.name AS q
ORDER BY n, p, m, q;

-- Variable Length Relationship

CREATE GRAPH t;
SET graph_path = t;

CREATE VLABEL time;
CREATE ELABEL goes;

CREATE (:time {sec: 1})-[:goes]->
       (:time {sec: 2})-[:goes]->
       (:time {sec: 3})-[:goes]->
       (:time {sec: 4})-[:goes]->
       (:time {sec: 5})-[:goes]->
       (:time {sec: 6})-[:goes]->
       (:time {sec: 7})-[:goes]->
       (:time {sec: 8})-[:goes]->
       (:time {sec: 9});

CREATE (:time {sec: 9})-[:goes*1..2]->(:time {sec: 10});

MATCH (a:time)-[x:goes*3]->(b:time)
RETURN a.sec AS a, length(x) AS x, b.sec AS b;

MATCH (a:time)-[x:goes*0]->(b:time)
RETURN a.sec AS a, x, b.sec AS b;

MATCH (a:time)-[x:goes*0..1]->(b:time)
RETURN a.sec AS a, length(x) AS x, b.sec AS b;

MATCH (a:time)-[x:goes*..1]->(b:time)
RETURN a.sec AS a, length(x) AS x, b.sec AS b;

MATCH (a:time)-[x:goes*0..]->(b:time)
RETURN a.sec AS a, length(x) AS x, b.sec AS b;

MATCH (a:time)-[x:goes*3..6]->(b:time)
RETURN a.sec AS a, length(x) AS x, b.sec AS b;

MATCH (a:time)-[x:goes*2]->(b:time)-[y:goes]->(c:time)-[z:goes*2]->(d:time)
RETURN a.sec AS a, length(x) AS x,
       b.sec AS b, type(y) AS y,
       c.sec AS c, length(z) AS z, d.sec AS d;

MATCH (a:time)-[x:goes*2]->(b:time)
MATCH (b)-[y:goes]->(c:time)
MATCH (c)-[z:goes*2]->(d:time)
RETURN a.sec AS a, length(x) AS x,
       b.sec AS b, type(y) AS y,
       c.sec AS c, length(z) AS z, d.sec AS d;

MATCH (d:time)<-[z:goes*2]-(c:time)<-[y:goes]-(b:time)<-[x:goes*2]-(a:time)
RETURN d.sec AS d, length(z) AS z,
       c.sec AS c, type(y) AS y,
       b.sec AS b, length(x) AS x, a.sec AS a;

MATCH (d:time)<-[z:goes*2]-(c:time)
MATCH (c)<-[y:goes]-(b:time)
MATCH (b)<-[x:goes*2]-(a:time)
RETURN d.sec AS d, length(z) AS z,
       c.sec AS c, type(y) AS y,
       b.sec AS b, length(x) AS x, a.sec AS a;

MATCH (a:time)-[x*0..2]-(b)
RETURN a.sec AS a, length(x) AS x, b.sec AS b;

CREATE (:time {sec: 11})-[:goes {int: 1}]->
       (:time {sec: 12})-[:goes {int: 1}]->
       (:time {sec: 13})-[:goes {int: 2}]->
       (:time {sec: 15})-[:goes {int: 1}]->
       (:time {sec: 16})-[:goes {int: 1}]->
       (:time {sec: 17});

MATCH (a:time)-[x:goes*1..2 {int: 1}]->(b:time)
RETURN a.sec AS a, length(x) AS x, b.sec AS b;

CREATE VLABEL person;
CREATE ELABEL knows;

-- 1->2->3->4
CREATE (:person {id: 1})-[:knows]->
       (:person {id: 2})-[:knows]->
       (:person {id: 3})-[:knows]->
       (:person {id: 4});

MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person) RETURN a.id, b.id, x;

-- 1->2->3->4
-- `->5
MATCH (a:person {id: 1}) CREATE (a)-[:knows]->(:person {id: 5});

MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person) RETURN a.id, b.id, x;

-- 1<->2->3->4
-- `->5
MATCH (a:person {id: 2}), (b:person {id: 1}) CREATE (a)-[:knows]->(b);

MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person) RETURN a.id, b.id, x;

MATCH (a:person {id: 1})-[x:knows*0..0]->(b:person) RETURN a.id, b.id, x;

MATCH (a:person {id: 1})-[x:knows*0..1]->(b:person) RETURN a.id, b.id, x;

MATCH (a:person {id: 1})-[x:knows*2..2]->(b:person) RETURN a.id, b.id, x;

MATCH (a:person {id: 2})-[x:knows*1..1]->(b:person) RETURN a.id, b.id, x;

MATCH (a:person)-[x:knows*1..1]->(b:person) RETURN a.id, b.id, x;

MATCH (a:person)-[x:knows*]->(b:person) RETURN a.id, b.id, x;

MATCH (a:person {id: 1})-[x:knows*0..3]->(b:person) RETURN a.id, b.id, x;

MATCH (a:person {id: 1})-[x*1..2]-(b:person) RETURN a.id, b.id, x;

-- 1->2->3->4
-- `->5
MATCH (a:person {id: 2})-[k:knows]->(b:person {id: 1}) DELETE k;

-- +<----+
-- 1->2->3->4
-- `->5
MATCH (a:person {id: 3}), (b:person {id: 1}) CREATE (a)-[:knows]->(b);

MATCH (a:person {id: 1})-[x:knows*1..]->(b:person) RETURN a.id, b.id, x;

-- edgeref

-- 1->2->3->4
-- `->5
MATCH (a:person {id: 3})-[k:knows]->(b:person {id: 1}) DELETE k;
MATCH (a:person {id: 1})-[k:knows]->(b:person {id: 5}) DELETE k;

CREATE ELABEL friendships INHERITS (knows);

MATCH (a:person {id: 1}) CREATE (a)-[:friendships {fromdate: '2014-11-24'}]->(:person {id: 5});

MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
RETURN a.id, b.id, x[0].fromdate;

MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
WHERE x[0].fromdate IS NOT NULL
RETURN a.id, b.id, x[0].fromdate;

MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
WITH x[0].fromdate AS fromdate
RETURN fromdate;

MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
WITH x[0] AS x1
RETURN x1.fromdate, x1;

MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
WHERE x[1].fromdate IS NOT NULL
WITH x[0] AS x1, length(x) AS l
RETURN x1, l;

MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
WITH x[0] AS x1, length(x) AS l
RETURN x1, l;

MATCH (a:person {id: 1})-[x:knows*1..2]-(b:person)
WITH x[0] AS x1, length(x) AS l
RETURN x1, l;

CREATE ELABEL familyship INHERITS (friendships);

MATCH (a:person {id: 5}) CREATE (a)-[:familyship {fromdate: '2015-12-24'}]->(:person {id: 6});

MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
WITH x[0] AS x1, x[1] AS x2, length(x) AS l
RETURN x1, x2, l;

EXPLAIN VERBOSE
MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
WITH x[0] AS x1, x[1] AS x2 ORDER BY x2 RETURN x1;

EXPLAIN VERBOSE
MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
WITH max(b.id) AS id, x[0] AS x RETURN *;

EXPLAIN VERBOSE
MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
WITH DISTINCT x AS path RETURN *;

EXPLAIN VERBOSE
MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
WITH max(b.id) AS id, x AS x RETURN *;

EXPLAIN VERBOSE
MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
WITH max(length(x)) AS x, b.id AS id RETURN *;

EXPLAIN VERBOSE
MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
RETURN x, x IS NOT NULL, x[0] IS NULL;

EXPLAIN VERBOSE
MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
WHERE x[0] IS NOT NULL RETURN x[0];

EXPLAIN VERBOSE
SELECT * FROM (
  MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
  WHERE x[0] IS NOT NULL RETURN x[0]
  UNION ALL
  MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
  RETURN x[1]
) AS foo;

-- shortestpath(), allshortestpaths()

CREATE OR REPLACE FUNCTION ids(vertex[]) RETURNS int[] AS $$
DECLARE
  v vertex;
  vids int[];
BEGIN
  IF $1 IS NULL THEN
    RETURN ARRAY[]::int[];
  END IF;
  FOREACH v IN ARRAY $1 LOOP
    vids = array_append(vids, (v->>'id')::int);
  END LOOP;
  RETURN vids;
END;
$$ LANGUAGE plpgsql;

-- 1->2->3->4->5
MATCH (a:person {id: 1})-[k:knows]->(b:person {id: 5}) DELETE k;
MATCH (a:person {id: 5})-[k:knows]->(b:person {id: 6}) DELETE k;
MATCH (a:person {id: 6}) DELETE a;
MATCH (a:person {id: 4}), (b:person {id: 5}) CREATE (a)-[:knows]->(b);

MATCH (p:person), (f:person) WHERE p.id = 3 AND f.id = 4
RETURN ids(nodes(shortestpath((p)-[:knows]->(f)))) AS ids;

MATCH (p:person), (f:person) WHERE p.id = 3 AND f.id = 5
RETURN ids(nodes(shortestpath((p)-[:knows]->(f)))) AS ids;

MATCH (p:person), (f:person) WHERE p.id = 3
RETURN ids(nodes(shortestpath((p)<-[:knows]-(f)))) AS ids;

MATCH (p:person), (f:person) WHERE p.id = 3
RETURN ids(nodes(shortestpath((p)-[:knows*]-(f)))) AS ids;

MATCH (p:person), (f:person), x=shortestpath((p)-[:knows*]-(f))
WHERE p.id = 3
RETURN ids(nodes(x)) AS ids;

MATCH x=shortestpath((p:person)-[:knows*]-(f:person))
WHERE p.id = 3
RETURN ids(nodes(x)) AS ids;

MATCH (p:person), (f:person) WHERE p.id = 3
RETURN ids(nodes(shortestpath((p)-[:knows*0..1]-(f)))) AS ids;

MATCH (p:person), (f:person) WHERE p.id = 1
RETURN ids(nodes(shortestpath((p)-[:knows*2..]->(f)))) AS ids;

MATCH (p:person), (f:person) WHERE p.id = 3 AND f.id = 5
CREATE (p)-[:knows]->(:person {id: 6})-[:knows]->(f);

MATCH (p:person), (f:person) WHERE p.id = 1 AND f.id = 5
RETURN length(allshortestpaths((p)-[:knows*]-(f))) AS cnt;

CREATE VLABEL v;
CREATE ELABEL e;

CREATE (:v {id: 0});
CREATE (:v {id: 1});
CREATE (:v {id: 2});
CREATE (:v {id: 3});
CREATE (:v {id: 4});
CREATE (:v {id: 5});
CREATE (:v {id: 6});

MATCH (v1:v {id: 0}), (v2:v {id: 4})
CREATE (v1)-[:e {weight: 3}]->(v2);
MATCH (v1:v {id: 0}), (v2:v {id: 1})
CREATE (v1)-[:e {weight: 7}]->(v2);
MATCH (v1:v {id: 0}), (v2:v {id: 5})
CREATE (v1)-[:e {weight: 10}]->(v2);

MATCH (v1:v {id: 4}), (v2:v {id: 6})
CREATE (v1)-[:e {weight: 5}]->(v2);
MATCH (v1:v {id: 4}), (v2:v {id: 3})
CREATE (v1)-[:e {weight: 11}]->(v2);
MATCH (v1:v {id: 4}), (v2:v {id: 1})
CREATE (v1)-[:e {weight: 2}]->(v2);

MATCH (v1:v {id: 1}), (v2:v {id: 3})
CREATE (v1)-[:e {weight: 10}]->(v2);
MATCH (v1:v {id: 1}), (v2:v {id: 2})
CREATE (v1)-[:e {weight: 4}]->(v2);
MATCH (v1:v {id: 1}), (v2:v {id: 5})
CREATE (v1)-[:e {weight: 6}]->(v2);

MATCH (v1:v {id: 5}), (v2:v {id: 3})
CREATE (v1)-[:e {weight: 9}]->(v2);

MATCH (v1:v {id: 6}), (v2:v {id: 3})
CREATE (v1)-[:e {weight: 4}]->(v2);

MATCH (v1:v {id: 2}), (v2:v {id: 3})
CREATE (v1)-[:e {weight: 2}]->(v2);

MATCH (v1:v {id: 0}), (v2:v {id: 3}),
      path=dijkstra((v1)-[e:e]->(v2), e.weight)
RETURN nodes(path);

MATCH (v1:v {id: 0}), (v2:v {id: 3}),
      path=dijkstra((v2)<-[e:e]-(v1), e.weight)
RETURN nodes(path);

MATCH (v1:v {id: 0}), (v2:v {id: 3}),
      path=dijkstra((v1)-[e:e]-(v2), e.weight)
RETURN nodes(path);

MATCH (v1:v {id: 0}), (v2:v {id: 3}),
      path=dijkstra((v2)-[e:e]-(v1), e.weight)
RETURN nodes(path);

MATCH (v1:v {id: 0}), (v2:v {id: 3}),
      path=dijkstra((v1)-[e:e]->(v2), e.weight + 1)
RETURN nodes(path);

MATCH (v1:v {id: 0}), (v2:v {id: 3}),
      path=dijkstra((v1)-[e:e]->(v2), e.weight, e.weight >= 5)
RETURN nodes(path);

MATCH (v1:v {id: 0}), (v2:v {id: 3}),
      path=dijkstra((v1)-[e:e]->(v2), e.weight, e.weight)
RETURN nodes(path);

MATCH (v1:v {id: 0}), (v2:v {id: 3}),
      path=dijkstra((v1:v {id: 0})-[e:e]->(v2), e.weight)
RETURN nodes(path);

MATCH (v1:v {id: 0}), (v2:v {id: 3}),
      path=dijkstra((v1:v)-[e:e*1..]->(v2), e.weight)
RETURN nodes(path);

MATCH (v1:v {id: 6}), (v2:v {id: 2}),
      path=dijkstra((v1:v)-[e:e]->(v2), e.weight)
RETURN nodes(path);

MATCH (v1:v), (v2:v),
      path=dijkstra((v1:v)-[e:e]->(v2), e.weight)
WHERE v1.id = 6 AND v2.id = 2
RETURN nodes(path);

MATCH (v1:v {id: 0}), (v2:v {id: 3})
RETURN dijkstra((v1)-[e:e]->(v2), e.weight);

MATCH (v1:v {id: 0}), (v2:v {id: 0})
RETURN dijkstra((v1)-[e:e]->(v2), e.weight);

MATCH (v1:v {id: 0}), (v2:v {id: 3}),
      p=dijkstra((v1)-[:e]->(v2), 1)
RETURN nodes(p);

MATCH (v1:v {id: 0}), (v2:v {id: 3}),
      p=dijkstra((v1)-[:e]->(v2), 1, LIMIT 10)
RETURN nodes(p);

MATCH (:v {id: 4})-[e:e]-(:v {id: 6})
SET e.weight = 4;

MATCH (v1:v {id: 0}), (v2:v {id: 3}),
	  (path, x)=dijkstra((v1)-[e:e]->(v2), e.weight, LIMIT 2)
RETURN nodes(path), x;

MATCH (v1:v {id: 0}), (v2:v {id: 3}),
	  (path, x)=dijkstra((v2)<-[e:e]->(v1), e.weight, LIMIT 2)
RETURN nodes(path), x;

MATCH (v1:v {id: 0}), (v2:v {id: 3})
RETURN dijkstra((v1)-[e:e]->(v2), e.weight, LIMIT 2);

MATCH (v1:v {id: 0}), (v2:v {id: 3}),
	  (path, x)=dijkstra((v1)-[e:e]->(v2), e.weight, LIMIT 0)
RETURN nodes(path), x;

MATCH (:v {id: 4})-[e:e]-(:v {id: 6})
SET e.weight = -1;

MATCH (v1:v {id: 0}), (v2:v {id: 3}),
	  (path, x)=dijkstra((v1)-[e:e]->(v2), e.weight, LIMIT 10)
return nodes(path), x;

SET graph_path = agens;

--
-- DISTINCT
--

MATCH (a:repo)-[]-() RETURN DISTINCT a.name AS a ORDER BY a;

--
-- ORDER BY
--

MATCH (a:repo) RETURN a.name AS a ORDER BY a;
MATCH (a:repo) RETURN a.name AS a ORDER BY a ASC;
MATCH (a:repo) RETURN a.name AS a ORDER BY a DESC;

--
-- SKIP and LIMIT
--

MATCH (a:repo) RETURN a.name AS a ORDER BY a SKIP 1 LIMIT 1;

--
-- WITH
--

MATCH (a:repo) WITH a.name AS name RETURN name;

MATCH (a)
WITH a WHERE label(a) = 'repo'
MATCH p=(a)-[]->(b)
RETURN b.name AS b ORDER BY b;

MATCH (a) WITH a RETURN b;
MATCH (a) WITH a.name RETURN *;
MATCH () WITH a AS z RETURN a;

--
-- UNION
--

MATCH (a:repo)
RETURN a.name AS a
UNION ALL
MATCH ()-[b:lib]->()
RETURN DISTINCT b.lang AS b
UNION ALL
MATCH ()-[c:doc]->()
RETURN DISTINCT c.lang AS c;

MATCH (a)
RETURN a
UNION
MATCH (b)
RETURN b.name;

--
-- aggregates
--

MATCH (a)-[]-(b) RETURN count(a) AS a, b.name AS b ORDER BY a, b;

--
-- EXISTS
--

MATCH (a:repo) WHERE exists((a)-[]->()) RETURN a.name AS a;

--
-- SIZE
--

MATCH (a:repo) RETURN a.name AS a, size((a)-[]->()) AS s;

--
-- LOAD
--

MATCH (a) LOAD FROM history AS a RETURN *;

CREATE VLABEL feature;
CREATE ELABEL supported;

MATCH (a:repo {name: 'agens-graph'})
LOAD FROM history AS h
CREATE (:feature {name: h.event})-[:supported]->(a);

MATCH p=(a)-[:supported]->() RETURN properties(a) AS a ORDER BY a;

--
-- DELETE
--

MATCH (a) DELETE a;

MATCH p=()-[:lib]->() DETACH DELETE (vertices(p))[1];
MATCH (a:repo) RETURN a.name AS a;

MATCH ()-[a:doc]->() DETACH DELETE end_vertex(a);
MATCH (a:repo) RETURN a.name AS a;

MATCH (a) DETACH DELETE a;
MATCH (a) RETURN a;

SELECT count(*) FROM agens.ag_edge;

-- attempt to delete null object

CREATE ({name: 'agensgraph'})-[:made_by]->({name: 'bitnine'});

MATCH (a {name: 'agensgraph'}), (g {name: 'bitnine'})
OPTIONAL MATCH (a)-[r:made_by]-(g)
DELETE r;

MATCH (a {name: 'agensgraph'}), (g {name: 'bitnine'})
OPTIONAL MATCH (a)-[r:made_by]-(g)
DELETE r;

MATCH (a) DETACH DELETE a;

--
-- Uniqueness
--

CREATE GRAPH u;
SET graph_path = u;

CREATE ELABEL rel;

CREATE (s {id: 1})-[:rel {p: 'a'}]->({id: 2})-[:rel {p: 'b'}]->(s);

MATCH (s)-[r1]-(m)-[r2]-(x)
RETURN s.id AS s, r1.p AS r1, m.id AS m, r2.p AS r2, x.id AS x
       ORDER BY s, r1, m, r2, x;

--
-- SET/REMOVE
--

CREATE GRAPH p;
SET graph_path = p;

CREATE ELABEL rel;

CREATE ({name: 'someone'})-[:rel {k: 'v'}]->({name: 'somebody'});

MATCH (n)-[r]->(m) SET r.l = 'w', n = m, r.k = NULL
RETURN properties(n) AS n, properties(r) AS r, properties(m) AS m;

MATCH (n)-[r]->(m) REMOVE m.name
RETURN properties(n) AS n, properties(r) AS r, properties(m) AS m;

MATCH (n)-[r]->(m)
RETURN properties(n) AS n, properties(r) AS r, properties(m) AS m;

MATCH (n) DETACH DELETE (n);

-- overwrite (Standard SQL)
CREATE ({age: 10});
MATCH (a) SET a.age = 11, a.age = a.age + 1
RETURN properties(a);
MATCH (a) RETURN properties(a);
MATCH (a) DETACH DELETE (a);

-- multiple SET's

CREATE ({age: 10});
MATCH (a) SET a.age = 11 SET a.age = a.age + 1
RETURN properties(a);
MATCH (a) RETURN properties(a);
MATCH (a) DETACH DELETE (a);

CREATE ()-[:rel {k: 'v'}]->();
MATCH ()-[r]->() SET r.l = 'x' SET r.l = 'y'
RETURN properties(r) AS r;
MATCH ()-[r]->() RETURN properties(r) AS r;
MATCH (a) DETACH DELETE (a);

CREATE ({age: 1})-[:rel]->({age: 2});
MATCH (a)-[]->(b)
SET a.age = a.age + 1, b.age = a.age + b.age
RETURN properties(a) AS a, properties(b) AS b;
MATCH (a)-[]->(b) RETURN properties(a) AS a, properties(b) AS b;
MATCH (a) DETACH DELETE (a);

CREATE ({val: 1})-[:rel]->({val: 2});
MATCH (a)-[]->(b)
SET a.val = b.val, b.val = a.val;
MATCH (a)-[]->(b) RETURN properties(a) AS a, properties(b) AS b;
MATCH (a) DETACH DELETE (a);

CREATE ({val: 1})-[:rel]->({val: 2});
MATCH (a)-[]->(b)
SET a.val = b.val SET b.val = a.val;
MATCH (a)-[]->(b) RETURN properties(a) AS a, properties(b) AS b;
MATCH (a) DETACH DELETE (a);

-- += operator

CREATE ({age: 10});
MATCH (a) SET a += {name: 'bitnine', age: 3}
RETURN properties(a);
MATCH (a) RETURN properties(a);

MATCH (a) SET a += NULL;
MATCH (a) SET a.name += NULL;
MATCH (a) SET a.name += 'someone';

MATCH (a) DETACH DELETE (a);

-- CREATE ... SET ...
CREATE p=(a {no:1})-[r1:rel]->(b {no:2})-[r2:rel]->(c {no:3})
SET a.no = 4, b.no = 5, c.no = 6
SET r1.name = 'agens', r2.name = 'graph'
RETURN properties(a), properties(r1), properties(b), properties(r2), properties(c);

MATCH (a)-[r]->(b) RETURN a.no, r.name, b.no;

MATCH (a) DETACH DELETE (a);

-- remove

CREATE ({a: 'a', b: 'b', c: 'c'});
MATCH (a) SET a.a = NULL REMOVE a.b
RETURN properties(a);
MATCH (a) RETURN properties(a);

MATCH (a) SET a = NULL;

MATCH (a) DETACH DELETE (a);

-- referring to undefined attributes
CREATE ({name: 'bitnine'});
CREATE ({age: 10});
MATCH (a) SET a.age = a.age + 1
RETURN properties(a);
MATCH (a) RETURN properties(a);

MATCH (a) SET a.age = 2017 - a.undefined_attr;
MATCH (a) RETURN properties(a);

-- working with NULL
CREATE VLABEL person;
CREATE (:person {name: 'bitnine', age: NULL});
MATCH (a:person {name: 'bitnine'}) RETURN properties(a) AS a;
MATCH (a:person {age: NULL}) RETURN properties(a) AS a;
MATCH (a:person) WHERE a.age IS NULL RETURN properties(a) AS a;

CREATE (:person {name: 'agens', key1: 1, key2: 2, key3: 3});
MATCH (a:person {name: 'agens'})
  SET a.key1 = NULL
  RETURN properties(a);
MATCH (a:person {name: 'agens'})
  SET a.key2 = null
  RETURN properties(a);
MATCH (a:person {name: 'agens'})
  SET a.key3 = {first: 1, last: null}
  RETURN properties(a);
MATCH (a:person {name: 'agens'})
  SET a = {name: 'agens', key4: null}
  RETURN properties(a);

MATCH (a:person {name: 'agens'}) RETURN properties(a);

--
-- MERGE
--

CREATE GRAPH gm;
SET GRAPH_PATH = gm;
CREATE VLABEL v1;
CREATE VLABEL v2;
CREATE ELABEL e1;

MERGE (a);
MATCH (a) DELETE a;

CREATE (:v1 {name: 'foo'}), (:v1 {name: 'bar'}), (:v1 {name: 'foo'}), (:v1 {name: 'bar'});
MATCH (a:v1)
MERGE (b:v2 {name: a.name})
  ON CREATE SET b.created = true ON MATCH SET b.matched = true;
MATCH (a:v2) RETURN properties(a);

MATCH (a:v1)
MERGE (a)-[r:e1 {type: 'same name'}]->(b:v2 {name: a.name})
  ON CREATE SET r.created = true, r.matched = null
  ON MATCH SET r.matched = true, r.created = null;
MATCH (a)-[r:e1]->(b) RETURN properties(a), properties(r), properties(b);

MATCH (a:v1)
MERGE (a)-[r:e1 {type: 'same name'}]->(b:v2 {name: a.name})
  ON CREATE SET r.created = true, r.matched = null
  ON MATCH SET r.matched = true, r.created = null;
MATCH (a)-[r:e1]->(b) RETURN properties(a), properties(r), properties(b);

MATCH (a:v2) RETURN properties(a);

MERGE (a:v1)-[r1:e1]->(b:v2)
MERGE (a)-[r2:e1]->(b)
  ON CREATE SET r2.created = true;
MATCH p=(a)-[r:e1 {created: true}]->(b) RETURN count(p);

CREATE (:v1 {name: 'v1-1'});
MERGE (a:v1 {name: 'v1-1'})-[:e1]->(b:v2 {name: 'v2-1'});

MATCH (a:v1 {name: 'v1-1'})-[r]->(b) RETURN properties(a), properties(b);
MATCH (a:v1 {name: 'v1-1'}) RETURN count(a);
MATCH (a:v1 {name: 'v1-1'}) DETACH DELETE a;

CREATE (:v1 {name: 'v1-1'});
MERGE (a:v1 {name: 'v1-1'})
MERGE (b:v2 {name: 'v2-1'})
MERGE (a)-[:e1]->(b);

MATCH (a:v1 {name: 'v1-1'})-[r]->(b) RETURN properties(a), properties(b);
MATCH (a:v1 {name: 'v1-1'}) RETURN count(a);
MATCH (a:v1 {name: 'v1-1'}) DETACH DELETE a;

CREATE VLABEL person;
CREATE VLABEL city;
CREATE ELABEL hometown;

CREATE (:person {name: 'a', bornin: 'seoul'}),
       (:person {name: 'b', bornin: 'san jose'}),
       (:person {name: 'c', bornin: 'jeju'}),
       (:person {name: 'd', bornin: 'san jose'}),
       (:person {name: 'e', bornin: 'seoul'}),
       (:person {name: 'f', bornin: 'los angeles'});

MATCH (a:person)
MERGE (b:city {name: a.bornin})
  ON CREATE SET b.population = 1
  ON MATCH SET b.population = b.population + 1;
MATCH (c:city)
RETURN c.name, c.population ORDER BY name;

MATCH (a:person)
MERGE (a)-[:hometown]->(b:city {name: a.bornin});
MATCH (:city)<-[r]-(:person) RETURN count(r);

MATCH (a:city) DETACH DELETE a;

CREATE CONSTRAINT ON city ASSERT name IS UNIQUE;

MATCH (a:person)
MERGE (a)-[:hometown]->(b:city {name: a.bornin});

MATCH (a:city) DETACH DELETE a;

-- unspecified direction
CREATE (a {id: 2}), (b {id: 1});

MATCH (a {id: 2}), (b {id: 1})
MERGE (a)-[r:e1]-(b)
RETURN properties(startnode(r)) as s, properties(endnode(r)) as e;

MATCH (a {id: 1}), (b {id: 2})
MERGE (a)-[r:e1]-(b)
RETURN properties(a), properties(b);

MATCH (a) DETACH DELETE a;

CREATE (a {id: 2}), (b {id: 1}), (c {id: 1}), (d {id: 2})
CREATE (a)-[:e1 {name: 'ab'}]->(b)
CREATE (c)-[:e1 {name: 'cd'}]->(d);

MATCH (a {id: 2})-[]-(b {id: 1})
MERGE (a)-[r:e1]-(b)
RETURN properties(r);

MATCH (a) DETACH DELETE a;

-- update clauses
CREATE (a:v1 {name: 'bitnine'}) MERGE (:v2 {name: a.name});
CREATE (a:v1 {name: 'AgensGraph'})
MERGE (b:v2 {name: a.name})
RETURN properties(a), properties(b);

MERGE (a:v1 {name: 'bitnine'})
MERGE (b:v1 {name: 'AgensGraph'})
CREATE p=(a)-[r:e1 {name: a.name + b.name}]->(b)
RETURN properties(a), properties(r), properties(b), count(p);

MERGE (a {name: 'bitnine'})
CREATE (b:v1 {name: a.name})
MERGE (c:v1 {name: 'bitnine'})
  ON MATCH SET c.matched = true
  ON CREATE SET c.matched = false;
MATCH (a) RETURN properties(a);

MATCH (a) DETACH DELETE a;

-- wrong case
MERGE (a:v1) MERGE (b:v2 {name: a.notexistent});
MERGE (a:v1) ON MATCH SET a.matched = true
MERGE (b:v2 {name: a.name});
MERGE (a:v1) MATCH (b:v2 {name: a.name}) RETURN a, b;
MERGE (a:v1) MERGE (b:v2 {name: a.name}) MERGE (a);
MERGE (a)-[r]->(b);
MERGE (a)-[r:e1]->(b) MERGE (a);
MERGE (a)-[r:e1]->(b) MERGE (a)-[r:e1]->(b);
MERGE (a)-[:e1]->(a:v1);
MERGE (=10);
MERGE ()-[:e1 =10]->();
MERGE (:ag_vertex);
MERGE ()-[:ag_edge]->();

DROP GRAPH gm CASCADE;

--
-- null properties
--

CREATE GRAPH np;
SET GRAPH_PATH = np;

SHOW allow_null_properties;

SET allow_null_properties = off;

CREATE (:v {z: null});
CREATE (:v {z: (SELECT 'null'::jsonb)});
CREATE (:v {z: {z: null}});
CREATE (:v {z: (SELECT '{"z": null}'::jsonb)});
CREATE (n:v {p: 0}) SET n.z = null, n.p = null;
CREATE (n:v) SET n.z = (SELECT 'null'::jsonb);
CREATE (n:v) SET n.z = {z: null};
CREATE (n:v) SET n.z = (SELECT '{"z": null}'::jsonb);
MATCH (n:v) RETURN n;

CREATE (n:v {z: 0}) SET n.z = null;
CREATE (n:v {z: 0}) SET n.z = (SELECT 'null'::jsonb);
CREATE (n:v {z: 0}) REMOVE n.z;
MATCH (n:v) RETURN n;

SET allow_null_properties = on;

CREATE (:w {z: null});
CREATE (:w {z: (SELECT 'null'::jsonb)});
CREATE (:w {z: {z: null}});
CREATE (:w {z: (SELECT '{"z": null}'::jsonb)});
CREATE (n:w {p: 0}) SET n.z = null, n.p = null;
CREATE (n:w) SET n.z = (SELECT 'null'::jsonb);
CREATE (n:w) SET n.z = {z: null};
CREATE (n:w) SET n.z = (SELECT '{"z": null}'::jsonb);
MATCH (n:w) RETURN n;

CREATE (n:w {z: 0}) SET n.z = null;
CREATE (n:w {z: 0}) SET n.z = (SELECT 'null'::jsonb);
CREATE (n:w {z: 0}) REMOVE n.z;
MATCH (n:w) RETURN n;

SET allow_null_properties = off;

--
-- String Matching
--

-- starts with

RETURN 'abc' STARTS WITH 'a';
RETURN 'abc' STARTS WITH '';
RETURN 'abc' STARTS WITH 'bc';
RETURN 'abc' STARTS WITH 'abcd';
RETURN 'abc' STARTS WITH 1;

-- ends with

RETURN 'abc' ENDS WITH 'c';
RETURN 'abc' ENDS WITH '';
RETURN 'abc' ENDS WITH 'ab';
RETURN 'abc' ENDS WITH 'abcd';
RETURN 'abc' ENDS WITH 1;

-- contains

RETURN 'abc' CONTAINS 'b';
RETURN 'abc' CONTAINS '';
RETURN 'abc' CONTAINS 'abcd';
RETURN 'abc' CONTAINS 1;

-- =~

RETURN 'abc' =~ 'abc';
RETURN 'abc' =~ '';
RETURN 'abc' =~ 'a';
RETURN 'abc' =~ 'abcd';
RETURN 'abc' =~ '(?i)A';
RETURN 'abc' =~ 'a(b{1})c';
RETURN 'abc' =~ 1;

--
-- graphid comparison
--

CREATE GRAPH gid;
SET GRAPH_PATH = gid;

CREATE ();
CREATE ();

MATCH (n) WHERE id(n) = '1.1' RETURN n;
MATCH (n) WHERE id(n) > 1.1 RETURN n;
MATCH (n) WHERE id(n) < '1.2' RETURN n;
MATCH (n) WHERE id(n) >= 1.1 RETURN n;
MATCH (n) WHERE id(n) <= 1.2 RETURN n;
MATCH (n) WHERE id(n) <> 1.1 RETURN n;

-- cleanup

DROP GRAPH gid CASCADE;
DROP GRAPH np CASCADE;
DROP GRAPH p CASCADE;
DROP GRAPH u CASCADE;
DROP GRAPH t CASCADE;
DROP GRAPH o CASCADE;

SET graph_path = agens;

DROP VLABEL feature;
DROP ELABEL supported;
DROP VLABEL repo;
DROP ELABEL lib;
DROP ELABEL doc;

DROP GRAPH agens CASCADE;

DROP TABLE history;
