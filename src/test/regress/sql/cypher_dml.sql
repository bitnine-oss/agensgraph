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
-- zero-length _vertex, _edge, and graphpath
--

SELECT ARRAY[]::_vertex;
SELECT ARRAY[]::_edge;
SELECT (ARRAY[]::_vertex, ARRAY[]::_edge)::graphpath;

--
-- _vertex, _edge, and graphpath with NULL values
--

SELECT ARRAY[NULL, NULL, NULL]::_vertex;
SELECT ARRAY[NULL, NULL]::_edge;
SELECT (ARRAY[NULL, NULL, NULL]::_vertex, ARRAY[NULL, NULL]::_edge)::graphpath;

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
-- INSERT
--
CREATE GRAPH test_insert;
SET graph_path = test_insert;

CREATE VLABEL repo;
CREATE ELABEL lib;
CREATE ELABEL doc;

INSERT (g:repo {name: 'agens-graph',
                year: (SELECT year FROM history WHERE event = 'Graph')})
RETURN properties(g) AS g;

MATCH (g:repo)
INSERT (j:repo {name: 'agens-graph-jdbc', year: 2016}),
       (d:repo {name: 'agens-graph-docs', year: 2016})
INSERT (g)-[l:lib {lang: 'java'}]->(j),
       p=(g)
         -[:lib {lang: 'c'}]->
         (:repo {name: 'agens-graph-odbc', year: 2016}),
       (g)-[e:doc {lang: 'en'}]->(d)
RETURN properties(l) AS lj, properties(j) AS j,
       properties((edges(p))[0]) AS lc, properties((vertices(p))[1]) AS c,
       properties(e) AS e, properties(d) AS d;

INSERT ()-[a:lib]->(a);
INSERT a=(), (a);
INSERT (a), (a {});
INSERT (a), (a);
INSERT (=0);
INSERT ()-[]-();
INSERT ()-[]->();
INSERT ()-[:lib|doc]->();
INSERT (a)-[a:lib]->();
INSERT ()-[a:lib]->()-[a:doc]->();
INSERT a=(), ()-[a:doc]->();
INSERT ()-[:lib =0]->();
INSERT (a), a=();
INSERT ()-[a:lib]->(), a=();
INSERT a=(), a=();
INSERT (:lib);
INSERT ()-[:repo]->();
INSERT (:ag_vertex);
INSERT ()-[:ag_edge]->();

INSERT (=null)-[:lib =null]->();
CREATE TABLE t1 (prop jsonb);
INSERT (=(SELECT prop FROM t1))-[:lib =(SELECT prop FROM t1)]->();

MATCH (a) WHERE a.name IS NULL DETACH DELETE a;
DROP TABLE t1;

CREATE GRAPH g_insert;
SET GRAPH_PATH = g_insert;

CREATE ELABEL e1;

INSERT p=()-[:e1]->() RETURN p;

INSERT (a {name:'agens'}), (b {name:a.name});

DROP GRAPH g_insert CASCADE;
DROP GRAPH test_insert CASCADE;

--
-- FINISH
--
CREATE GRAPH finish_test;
SET graph_path = finish_test;

CREATE VLABEL person;
CREATE ELABEL knows;
CREATE (:person {name: 'A'}), (:person {name: 'B'}), (:person {name: 'C'});

-- a read-only query ending in FINISH returns no rows
MATCH (n) FINISH;
MATCH (n) WHERE n.name = 'A' FINISH;
MATCH (n)-[r]->(m) FINISH;
MATCH p=(n)-[r]->(m) FINISH;

-- a query whose last clause is a graph write still performs the write,
-- and FINISH returns no rows
CREATE (:person {name: 'D'}) FINISH;
MERGE (:person {name: 'E'}) FINISH;
MATCH (n:person {name: 'A'}) CREATE (n)-[:knows]->(:person {name: 'F'}) FINISH;

-- the writes above must have taken effect
MATCH (n:person) RETURN count(n) AS persons;
MATCH (:person)-[r:knows]->(:person) RETURN count(r) AS knows;

-- FINISH must be the last clause
MATCH (n) FINISH RETURN n;

-- plan structure: a read + FINISH applies LIMIT 0; a write + FINISH runs the write
EXPLAIN (COSTS OFF) MATCH (n) FINISH;
EXPLAIN (COSTS OFF) CREATE (:person {name: 'G'}) FINISH;

DROP GRAPH finish_test CASCADE;

--
-- ORDER BY / SKIP / OFFSET / LIMIT as standalone clauses
--
CREATE GRAPH modifiers;
SET graph_path = modifiers;

CREATE ({name: 'A', id: 1, grp: 1});
CREATE ({name: 'B', id: 2, grp: 2});
CREATE ({name: 'C', id: 3, grp: 1});
CREATE ({name: 'D', id: 4, grp: 2});
CREATE ({name: 'E', id: 5, grp: 1});

-- ORDER BY: ASC (default/explicit), DESC, multi-key with mixed direction,
-- and an expression sort key
MATCH (a) ORDER BY a.name RETURN a.name AS name;
MATCH (a) ORDER BY a.name ASC RETURN a.name AS name;
MATCH (a) ORDER BY a.name DESC RETURN a.name AS name;
MATCH (a) ORDER BY a.grp, a.name DESC RETURN a.grp AS grp, a.name AS name;
MATCH (a) ORDER BY a.id % 2, a.id RETURN a.id AS id;

-- SKIP, OFFSET (synonym), LIMIT; alone and combined; expression arguments
MATCH (a) ORDER BY a.name SKIP 1 RETURN a.name AS name;
MATCH (a) ORDER BY a.name OFFSET 1 RETURN a.name AS name;
MATCH (a) ORDER BY a.name LIMIT 2 RETURN a.name AS name;
MATCH (a) ORDER BY a.name SKIP 1 LIMIT 2 RETURN a.name AS name;
MATCH (a) ORDER BY a.name OFFSET 1 LIMIT 2 RETURN a.name AS name;
MATCH (a) ORDER BY a.name SKIP 1 + 1 LIMIT 5 - 3 RETURN a.name AS name;

-- boundaries: LIMIT 0, and SKIP/LIMIT past the row count
MATCH (a) ORDER BY a.name LIMIT 0 RETURN a.name AS name;
MATCH (a) ORDER BY a.name SKIP 10 RETURN a.name AS name;
MATCH (a) ORDER BY a.name LIMIT 10 RETURN a.name AS name;

-- standalone modifiers compose as a pipeline: "LIMIT 2 SKIP 1" (take 2,
-- then skip 1) differs from the canonical "SKIP 1 LIMIT 2"
MATCH (a) ORDER BY a.name LIMIT 2 SKIP 1 RETURN a.name AS name;
MATCH (a) ORDER BY a.name SKIP 1 LIMIT 2 RETURN a.name AS name;

-- modifiers in the middle of a chain (no projection beneath -> CypherModifier)
MATCH (a) ORDER BY a.id DESC LIMIT 3 WITH a RETURN a.id AS id ORDER BY id;
MATCH (a) ORDER BY a.id LIMIT 2 WITH a ORDER BY a.id DESC RETURN a.id AS id;

-- after WITH (re-attached to the WITH projection)
MATCH (a) WITH a ORDER BY a.id DESC LIMIT 2 SKIP 1 RETURN a.id AS id;
MATCH (a) WITH a ORDER BY a.id DESC SKIP 1 LIMIT 2 RETURN a.id AS id;

-- with DISTINCT
MATCH (a) RETURN DISTINCT a.grp AS grp ORDER BY a.grp DESC;

-- ORDER BY a RETURN alias; and a value not in the RETURN list (with paging)
MATCH (a) RETURN a.id AS x ORDER BY x DESC;
MATCH (a) RETURN a.name AS name ORDER BY a.id DESC;
MATCH (a) RETURN a.name AS name ORDER BY a.id DESC SKIP 1 LIMIT 2;

-- UNWIND followed by standalone ORDER BY / LIMIT
UNWIND [3, 1, 2, 5, 4] AS x ORDER BY x LIMIT 3 RETURN x ORDER BY x;

-- as trailing RETURN clauses, canonical order ORDER BY -> OFFSET/SKIP -> LIMIT
MATCH (a) RETURN a.name AS name ORDER BY a.name OFFSET 1 LIMIT 1;
MATCH (a) RETURN a.name AS name ORDER BY a.name OFFSET 1;

-- NULLS FIRST / NULLS LAST (one vertex has no name -> NULL)
CREATE ({id: 6});
MATCH (a) ORDER BY a.name NULLS FIRST RETURN a.name AS name;
MATCH (a) ORDER BY a.name DESC NULLS LAST RETURN a.name AS name;

-- a bare modifier with no preceding clause is not a valid query
ORDER BY 1;
SKIP 1;
OFFSET 1;
LIMIT 1;

-- non-canonical order as RETURN trailing clauses is rejected
MATCH (a) RETURN a.name AS name ORDER BY a.name LIMIT 1 SKIP 1;
MATCH (a) RETURN a.name AS name OFFSET 1 SKIP 1;
MATCH (a) RETURN a.name AS name LIMIT 1 ORDER BY a.name;

DROP GRAPH modifiers CASCADE;

--
-- FILTER clause
--
CREATE GRAPH test_filter;
SET graph_path = test_filter;

CREATE ({name: 'foo', age: 13});
CREATE ({name: 'bar', age: 14});
CREATE ({name: 'baz', age: 15});
CREATE ({age: 16});

-- FILTER keeps rows satisfying the condition; FILTER WHERE is equivalent
MATCH (n) FILTER n.age > 13 RETURN n.age AS age ORDER BY age;
MATCH (n) FILTER WHERE n.age > 13 RETURN n.age AS age ORDER BY age;

-- boolean expressions
MATCH (n) FILTER n.age >= 14 AND n.age <= 15 RETURN n.age AS age ORDER BY age;
MATCH (n) FILTER n.age = 13 OR n.age = 16 RETURN n.age AS age ORDER BY age;
MATCH (n) FILTER NOT (n.age > 14) RETURN n.age AS age ORDER BY age;
MATCH (n) FILTER n.name IS NOT NULL RETURN n.name AS name ORDER BY name;

-- a FILTER predicate may use a property/subquery predicate
MATCH (n) FILTER exists(n.name) RETURN n.age AS age ORDER BY age;

-- chained FILTERs apply as successive stages
MATCH (n) FILTER n.age > 13 FILTER n.age < 16 RETURN n.age AS age ORDER BY age;

-- filtering everything out, and keeping everything
MATCH (n) FILTER n.age > 100 RETURN n.age AS age;
MATCH (n) FILTER n.age > 0 RETURN n.age AS age ORDER BY age;

-- FILTER after WITH
MATCH (n) WITH n FILTER n.age > 14 RETURN n.age AS age ORDER BY age;

-- FILTER in the middle of a chain (after UNWIND, before MATCH)
UNWIND [13, 14, 15] AS age FILTER age < 15
MATCH (n {age: age}) RETURN n.name AS name ORDER BY name;
UNWIND [13, 14, 15] AS age FILTER WHERE age < 15
MATCH (n {age: age}) RETURN n.name AS name ORDER BY name;

-- FILTER after a write clause (the write runs, then rows are filtered)
MERGE (n {age: 14}) FILTER n.name IS NOT NULL RETURN n.name AS name;
MERGE (n {age: 14}) FILTER WHERE n.name IS NOT NULL RETURN n.name AS name;

-- a bare FILTER with no preceding clause is not a valid query
FILTER true;

-- a query may not end with FILTER
MATCH (n) FILTER n.age > 13;

DROP GRAPH test_filter CASCADE;

--
-- Graph reference values: CURRENT_GRAPH / CURRENT_PROPERTY_GRAPH
--
CREATE GRAPH graphref;
SET graph_path = graphref;

-- the current graph, in SQL and in Cypher; CURRENT_PROPERTY_GRAPH is a synonym
SELECT CURRENT_GRAPH, CURRENT_PROPERTY_GRAPH;
RETURN CURRENT_GRAPH AS g, CURRENT_PROPERTY_GRAPH AS pg;

-- follows graph_path
CREATE GRAPH graphref2;
SET graph_path = graphref2;
SELECT CURRENT_GRAPH;
DROP GRAPH graphref2 CASCADE;
SET graph_path = graphref;

-- SQL reference values may be used inside a Cypher query (the values are
-- non-deterministic, so assert only that they are produced)
RETURN CURRENT_GRAPH AS g,
       (CURRENT_DATE IS NOT NULL) AS has_date,
       (CURRENT_TIMESTAMP IS NOT NULL) AS has_timestamp,
       (LOCALTIMESTAMP IS NOT NULL) AS has_localts,
       (CURRENT_USER IS NOT NULL) AS has_user,
       (SESSION_USER IS NOT NULL) AS has_session_user,
       (CURRENT_CATALOG IS NOT NULL) AS has_catalog,
       (CURRENT_SCHEMA IS NOT NULL) AS has_schema;

-- result type is name, and the two spellings are synonyms
SELECT pg_typeof(CURRENT_GRAPH) AS typ,
       (CURRENT_GRAPH = CURRENT_PROPERTY_GRAPH) AS same;

-- usable in ordinary SQL expressions and predicates
SELECT CURRENT_GRAPH::text || '_x' AS concat;
SELECT 1 AS ok WHERE CURRENT_GRAPH = 'graphref';

-- precision variants of the SQL time functions inside Cypher
RETURN (CURRENT_TIME(2) IS NOT NULL) AS has_time2,
       (CURRENT_TIMESTAMP(0) IS NOT NULL) AS has_ts0,
       (LOCALTIME IS NOT NULL) AS has_localtime;

-- deparses back to CURRENT_GRAPH / CURRENT_PROPERTY_GRAPH (ruleutils)
CREATE VIEW graphref_view AS
	SELECT CURRENT_GRAPH AS g, CURRENT_PROPERTY_GRAPH AS pg;
SELECT pg_get_viewdef('graphref_view'::regclass, true);
DROP VIEW graphref_view;

-- CURRENT_GRAPH is NULL (not an error) when graph_path is invalid
DROP GRAPH graphref CASCADE;
SELECT CURRENT_GRAPH IS NULL AS is_null;

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

MATCH (A)-[r:el1 *1..3]-(B)
RETURN A.id as aid, r, B.id as bid, id(r[0]) as rid
ORDER BY aid, bid, rid;
MATCH (A)-[r:el2 *1..3]-(B) RETURN A.id, r, B.id;
MATCH (A)-[r:el3 *1..3]-(B) RETURN A.id, r, B.id;
MATCH (A)-[r:el1 ONLY *1..3]-(B) RETURN A.id, r, B.id;
MATCH (A)-[r:el2 ONLY *1..3]-(B) RETURN A.id, r, B.id;

MATCH (A:vl1) DETACH DELETE A;
MATCH (B:vl2) DETACH DELETE B;
MATCH (C:vl3) DETACH DELETE C;

-- Non-existent vertex/edge labels. Should return NULL instead of erroring out
MATCH (n)-[:v]-() RETURN n;
MATCH (n)-[:emissing]-() RETURN n;
MATCH (n:e1)-[]-() RETURN n;
MATCH (n:vmissing)-[]-() RETURN n;
MATCH (:e1)-[r]-() RETURN r;
MATCH (:vmissing)-[r]-() RETURN r;
MATCH (n),(:e1) RETURN n;
MATCH (n),()-[:v]-() RETURN n;
MATCH (n)-[:v *]->() RETURN n;

EXPLAIN VERBOSE MATCH (n:e)-[v:v]-() RETURN n,v;
EXPLAIN VERBOSE MATCH (n:e)-[v:v *]-() RETURN n,v;

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

OPTIONAL MATCH (n:person {name: 'unknown'})
RETURN n.name;

OPTIONAL MATCH (n:person {name: 'unknown'}) MATCH (m:person {name: 'someone'})
RETURN n, m.name;

OPTIONAL MATCH (n:person {name: 'unknown'}) WITH n MATCH (m:person {name: 'someone'})
RETURN n, m.name;

OPTIONAL MATCH (n:person {name: 'unknown'}) WITH n MATCH (m:person {name: 'unknown'})
RETURN n, m.name;

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
RETURN a.sec AS a, length(x) AS x, b.sec AS b ORDER BY a;

MATCH (a:time)-[x:goes*0]->(b:time)
RETURN a.sec AS a, x, b.sec AS b ORDER BY a;

MATCH (a:time)-[x:goes*0..1]->(b:time)
RETURN a.sec AS a, length(x) AS x, b.sec AS b ORDER BY a;

MATCH (a:time)-[x:goes*..1]->(b:time)
RETURN a.sec AS a, length(x) AS x, b.sec AS b ORDER BY a;

MATCH (a:time)-[x:goes*0..]->(b:time)
RETURN a.sec AS a, length(x) AS x, b.sec AS b ORDER BY a;

MATCH (a:time)-[x:goes*3..6]->(b:time)
RETURN a.sec AS a, length(x) AS x, b.sec AS b ORDER BY a;

MATCH (a:time)-[x:goes*2]->(b:time)-[y:goes]->(c:time)-[z:goes*2]->(d:time)
RETURN a.sec AS a, length(x) AS x,
       b.sec AS b, type(y) AS y,
       c.sec AS c, length(z) AS z, d.sec AS d ORDER BY a;

MATCH (a:time)-[x:goes*2]->(b:time)
MATCH (b)-[y:goes]->(c:time)
MATCH (c)-[z:goes*2]->(d:time)
RETURN a.sec AS a, length(x) AS x,
       b.sec AS b, type(y) AS y,
       c.sec AS c, length(z) AS z, d.sec AS d ORDER BY a;

MATCH (d:time)<-[z:goes*2]-(c:time)<-[y:goes]-(b:time)<-[x:goes*2]-(a:time)
RETURN d.sec AS d, length(z) AS z,
       c.sec AS c, type(y) AS y,
       b.sec AS b, length(x) AS x, a.sec AS a ORDER BY d;

MATCH (d:time)<-[z:goes*2]-(c:time)
MATCH (c)<-[y:goes]-(b:time)
MATCH (b)<-[x:goes*2]-(a:time)
RETURN d.sec AS d, length(z) AS z,
       c.sec AS c, type(y) AS y,
       b.sec AS b, length(x) AS x, a.sec AS a ORDER BY d;

MATCH (a:time)-[x*0..2]-(b)
RETURN a.sec AS a, length(x) AS x, b.sec AS b ORDER BY a, b, x;

-- VLE with graph path
MATCH p = (:time)-[:goes*0]->(:time)
RETURN properties(nodes(p)[0]) AS first, properties(vertices(p)[1]) AS second, properties(relationships(p)[0]) AS rel;

MATCH p = (:time)-[r:goes*0..2]->(:time)
RETURN properties(nodes(p)[0]) AS first, properties(vertices(p)[1]) AS second, properties(vertices(p)[2]) AS third,
	   id(nodes(p)[0]) = id(startnode(r[0])) AS check_start_of_first_edge,
	   id(nodes(p)[1]) = id(endnode(r[0])) AS check_end_of_first_edge,
	   id(nodes(p)[1]) = id(startnode(r[1])) AS check_start_of_second_edge,
	   id(nodes(p)[2]) = id(endnode(r[1])) AS check_end_of_second_edge,
	   length(edges(p));

MATCH p = (:time)-[:goes*0]->(:time)-[:goes*2..4]->(:time)-[:goes*0]-(:time)
RETURN properties(nodes(p)[0]) AS first, properties(vertices(p)[1]) AS second, properties(vertices(p)[2]) AS third,
	   properties(nodes(p)[3]) AS fourth, properties(vertices(p)[4]) AS fifth, properties(vertices(p)[5]) AS sixth,
	   length(edges(p))
ORDER BY first, second, third, fourth, fifth, sixth;

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

MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
RETURN a.id as aid, b.id as bid, x ORDER BY aid, bid;

MATCH (a:person {id: 1})-[x:knows*0..0]->(b:person) RETURN a.id, b.id, x;

MATCH (a:person {id: 1})-[x:knows*0..1]->(b:person) RETURN a.id, b.id, x;

MATCH (a:person {id: 1})-[x:knows*2..2]->(b:person) RETURN a.id as aid, b.id as bid, x ORDER BY aid, bid;

MATCH (a:person {id: 2})-[x:knows*1..1]->(b:person) RETURN a.id as aid, b.id as bid, x ORDER BY aid, bid;

MATCH (a:person)-[x:knows*1..1]->(b:person)
RETURN a.id as aid, b.id as bid, x, id(x[0]) as xid ORDER BY aid, bid, xid;

MATCH (a:person)-[x:knows*]->(b:person)
RETURN a.id as aid, b.id as bid, x, id(x[0]) as xid ORDER BY aid, bid, xid;

MATCH (a:person {id: 1})-[x:knows*0..3]->(b:person)
RETURN a.id as aid, b.id as bid, x
ORDER BY length(x), aid, bid DESC;

MATCH (a:person {id: 1})-[x*1..2]-(b:person)
RETURN a.id as aid, b.id as bid, x, id(x[0]) as xid ORDER BY aid, bid, xid;

-- 1->2->3->4
-- `->5
MATCH (a:person {id: 2})-[k:knows]->(b:person {id: 1}) DELETE k;

-- +<----+
-- 1->2->3->4
-- `->5
MATCH (a:person {id: 3}), (b:person {id: 1}) CREATE (a)-[:knows]->(b);

MATCH (a:person {id: 1})-[x:knows*1..]->(b:person)
RETURN a.id as aid, b.id as bid, x, id(x[0]) as xid ORDER BY aid, bid, xid;

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

EXPLAIN (VERBOSE, COSTS OFF)
MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
WITH x[0] AS x1, x[1] AS x2 ORDER BY x2 RETURN x1;

EXPLAIN (VERBOSE, COSTS OFF)
MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
WITH max(b.id::"numeric") AS id, x[0] AS x RETURN *;

EXPLAIN (VERBOSE, COSTS OFF)
MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
WITH max(b.id) AS id, x[0] AS x RETURN *;

EXPLAIN (VERBOSE, COSTS OFF)
MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
WITH DISTINCT x AS path RETURN *;

EXPLAIN (VERBOSE, COSTS OFF)
MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
WITH max(b.id::"numeric") AS id, x AS x RETURN *;

EXPLAIN (VERBOSE, COSTS OFF)
MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
WITH max(b.id) AS id, x AS x RETURN *;

EXPLAIN (VERBOSE, COSTS OFF)
MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
WITH max(length(x)::"numeric") AS x, b.id AS id RETURN *;

EXPLAIN (VERBOSE, COSTS OFF)
MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
WITH max(length(x)) AS x, b.id AS id RETURN *;

EXPLAIN (VERBOSE, COSTS OFF)
MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
RETURN x, x IS NOT NULL, x[0] IS NULL;

EXPLAIN (VERBOSE, COSTS OFF)
MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
WHERE x[0] IS NOT NULL RETURN x[0];

EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM (
  MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
  WHERE x[0] IS NOT NULL RETURN x[0]
  UNION ALL
  MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person)
  RETURN x[1]
) AS foo;

-- AG-154, CS-34 - VLE returns incoreect result with sequential scan

CREATE GRAPH ag154;
SET graph_path = ag154;

CREATE ({id:1})-[:rel]->({id:11});
MATCH (a {id:11}) CREATE (a)-[:rel]->({id:111});
MATCH (a {id:111}) CREATE (a)-[:rel]->({id:1111});
MATCH (a {id:111}) CREATE (a)-[:rel]->({id:1112});
MATCH (a {id:111}) CREATE (a)-[:rel]->({id:1113});
MATCH (a {id:11}) CREATE (a)-[:rel]->({id:112});
MATCH (a {id:112}) CREATE (a)-[:rel]->({id:1121});
MATCH (a {id:112}) CREATE (a)-[:rel]->({id:1122});
MATCH (a {id:11}) CREATE (a)-[:rel]->({id:113});
MATCH (a {id:113}) CREATE (a)-[:rel]->({id:1131});
MATCH (a {id:113}) CREATE (a)-[:rel]->({id:1132});

SET enable_indexscan = f;
SET enable_seqscan = t;
MATCH ({id:1})-[r:rel*]->() RETURN length(r) AS len ORDER BY len;

SET enable_indexscan = t;
SET enable_seqscan = f;
MATCH ({id:1})-[r:rel*]->() RETURN length(r) AS len ORDER BY len;

SET enable_indexscan = default;
SET enable_seqscan = default;

-- AG-216 VLE throws "btree index keys must be ordered by attribute"

CREATE GRAPH ag216;
SET graph_path = ag216;

CREATE (:v1)-[:e]->(:v2)-[:e]->(:v3);

SET enable_seqscan = off;
MATCH p=(:v1)-[*]->(:v3) RETURN p;
SET enable_seqscan = on;

CREATE GRAPH ag216a;
SET graph_path = ag216a;

CREATE (n:v1)-[:e1]->(:v2 {lv: 1}), (n)-[:e1]->(:v2 {lv: 1});

MATCH (n:v2)
CREATE (n)-[:e2]->(:v2 {lv: 2}), (n)-[:e2]->(:v2 {lv: 2});

MATCH (n:v2 {lv: 2})
CREATE (n)-[:e3]->(:v3), (n)-[:e3]->(:v3);

MATCH p=(:v1)-[*3]->() RETURN p;

-- Test cases for variable reuse
CREATE GRAPH variable_reuse;
SET graph_path = variable_reuse;

-- add data
CREATE (:v);
CREATE (:v {i: 0});
CREATE (:v {i: 1});
CREATE (:v1 {id:'initial'})-[:e1]->(:v1 {id:'middle'})-[:e1]->(:v1 {id:'end'});
CREATE (:v2 {id:'initial'})<-[:e2]-(:v2 {id:'middle'})-[:e2]->(:v2 {id:'end'});
CREATE (:v3 {id:'initial'})-[:e3]->(:v3 {id:'middle'})<-[:e3]-(:v3 {id:'end'});

-- valid variable reuse for edge labels across clauses
MATCH ()-[r0]->() MATCH ()-[r0]->() RETURN r0;
MATCH ()-[r0:e1]->() MATCH ()-[r0:e1]->() RETURN r0;
MATCH ()-[r0:e2]->() MATCH ()-[r0:e2]->() RETURN r0;
MATCH ()-[r0:e1]->()-[r1]->() RETURN r0,r1;
MATCH p0=()-[:e1]->() MATCH p1=()-[:e2]->() RETURN p1;
MATCH ()-[r0:e1]->()-[r1]->() MATCH ()-[r0:e1]->()-[r1]->() RETURN r0,r1;
MATCH ()-[]->() MATCH ()-[r1:e2]->() RETURN r1;
MATCH ()-[r0:e1]->() MATCH ()-[r1:e2]->() RETURN r0,r1;
MATCH ()-[e:e1]->() WITH e OPTIONAL MATCH (a)-[e]->(c) RETURN a, c;
MATCH ()-[e:e1]->() WITH e WHERE EXISTS((:v1)-[e]->()) RETURN e;
MATCH ()-[e:e1]->() WITH e WHERE EXISTS((:v2)-[e]->()) RETURN e;

-- valid variable reuse for vertex labels across clauses
MATCH (r1:v2), (r1:v2) RETURN r1;
MATCH (r1), (r1) RETURN r1;
MATCH (r1:v2), (r1) RETURN r1;
MATCH (r1:v2), (r1), (r1), (r1:v2) RETURN r1;
MATCH (r1:v2)-[]->(r1)-[]->(r1:v2)-[]->(r1) RETURN r1;
MATCH (r1:v2)-[]->()-[]->()-[]->(r1:v2) RETURN r1;
MATCH ()-[r0:e1]->() MATCH ()-[r0]->() RETURN r0;
MATCH (a:v1) WITH a OPTIONAL MATCH (a)-[:e1]->(c) RETURN a, c;
MATCH (a:v2) WITH a WHERE EXISTS((a)-[:e1]->()) RETURN a;

-- Property constraints on reused variables
MATCH (r1:v2 {id: 'initial'}), (r2:v2 {id: 'middle'}) CREATE (r1)-[:e1 {id: 1}]->(r2);
MATCH (r1:v2 {id: 'initial'}), (r2:v2 {id: 'middle'}) CREATE (r1)-[:e1 {id: 2}]->(r2);
MATCH (r1:v2), (r1:v2 {id: 'initial'}) RETURN r1;
MATCH (r1:v2) MATCH (r1:v2 {id: 'middle'}) RETURN r1;
MATCH (r1:v2) MATCH (r1:v2), (r1 {id: 'end'}) RETURN r1;
MATCH (r1:v2) MATCH (r1 {id: 'initial'})-[e:e1]->(r2) RETURN r1,e,r2;
MATCH (r1:v2) MATCH (r1 {id: 'initial'})-[e:e1]->(r2) MATCH (r1)-[e:e1 {id: 1}]->(r2) RETURN r1,e,r2;
MATCH (r1:v2) MATCH (r1 {id: 'initial'})-[e:e1]->(r2) MATCH (r1)-[e:e1 {id: 2}]->(r2) RETURN r1,e,r2;

-- invalid variable reuse for labels across clauses
MATCH (r1:v1), (r1:v2) RETURN r1;
MATCH (r1:e1), (r1:e2) return r1;
MATCH (r0)-[r0]->() MATCH ()-[]->() RETURN r0;
MATCH (r0)-[]->() MATCH ()-[r0]->() RETURN r0;
MATCH ()-[r0]->() MATCH ()-[]->(r0) RETURN r0;
MATCH ()-[r0:e1]->() MATCH ()-[r0:e2]->() RETURN r0;
MATCH ()-[r0]->() MATCH ()-[r0:e2]->() RETURN r0;
MATCH ()-[r0:e1]->() MATCH ()-[r0:e2]->() RETURN r0;
MATCH ()-[r0:e1]->()-[r0]->() MATCH ()-[r0:e2]->() RETURN r0;
MATCH ()-[r0:e1]->()-[r1]->() MATCH ()-[r1:e1]->()-[r0]->() RETURN r0;
MATCH ()-[r0:e1]->()-[r1]->() MATCH ()-[r0:e1]->()-[r0]->() RETURN r0;
MATCH ()-[r0 *]->() MATCH ()-[r0]->() RETURN r0;
MATCH ()-[r0]->() MATCH ()-[r0 *]->() RETURN r0;

DROP GRAPH variable_reuse CASCADE;

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

--
-- count
--
MATCH (a)-[]-(b) RETURN count(a) AS a, b.name AS b ORDER BY a, b;
MATCH (u) RETURN count(u.name), count(DISTINCT u.name);
MATCH (u) RETURN count(u.year), count(DISTINCT u.year);

-- should return 0
RETURN count(NULL);

-- should fail
RETURN count();

--
-- collect
--
MATCH (u) RETURN collect(u.name), collect(u.year);
MATCH (u) RETURN collect(u.year), collect(u.year);
RETURN collect(5);

-- should return an empty array
RETURN collect(NULL::jsonb);
MATCH (u) WHERE u.name =~ 'does not exist' RETURN collect(u.name);

-- should fail
RETURN collect();

-- test DISTINCT inside aggregate functions
MATCH (u) RETURN (u);
MATCH (u) RETURN collect(u.name), collect(DISTINCT u.name);
MATCH (u) RETURN collect(u.year), collect(DISTINCT u.year);

--
-- min/max
--
MATCH (u) RETURN min(u.year), max(u.year), count(u.year), count(*);
MATCH (u) RETURN min(u.name), max(u.name), count(u.name), count(*);

-- should return null
RETURN min(NULL);
RETURN max(NULL);
SELECT min(NULL);
SELECT min(null::jsonb);
SELECT max(NULL);
SELECT max(null::jsonb);

-- should fail
RETURN min();
RETURN max();
SELECT min();
SELECT max();

--
-- EXISTS
--

MATCH (a:repo) WHERE exists((a)-[]->()) RETURN a.name AS a;
MATCH (a:repo) WHERE exists(a.name) RETURN a;
MATCH (a:repo) WHERE NOT exists(a.name) RETURN a;
MATCH (a:repo) RETURN a, exists(a.name) AS b;
-- exists() is false for an absent property
MATCH (a:repo) WHERE NOT exists(a.stars) RETURN a.name AS name ORDER BY name;
MATCH (a:repo) RETURN a.name AS name, exists(a.year) AS has_year, exists(a.stars) AS has_stars ORDER BY name;

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

-- AG-163 : DELETE plan passes 'edge' variable to the next plan.
CREATE ({name:'AG-163'});

MATCH (a {name:'AG-163'}) DELETE a RETURN *;

-- AG-160
CREATE ()-[:AG160]->();

MATCH ()-[r:AG160]->() DETACH DELETE r;

MATCH (a) DETACH DELETE a;

CREATE ({name:'ag-160 left'})-[:AG160]->({name:'ag-160 right'});

MATCH (a)-[r:AG160]->(b)
DELETE r
DELETE a, b;

MATCH ()-[]->() RETURN count(*);
MATCH () RETURN count(*);

CREATE ({name:'ag-160 left'})-[:AG160]->({name:'ag-160 right'});

MATCH (a)-[r:AG160]->(b)
DELETE r, a
DELETE b;

MATCH ()-[]->() RETURN count(*);
MATCH () RETURN count(*);

CREATE ({name:'ag-160 left'})-[:AG160]->({name:'ag-160 right'});

MATCH (a {name:'ag-160 left'})-[r:AG160]->(b)
DELETE r
DELETE a, b;

MATCH ()-[]->() RETURN count(*);
MATCH () RETURN count(*);

-- AG-138

CREATE ()-[:rel]->()-[:rel]->();

MATCH (a)-[r:rel]->(b)
DELETE a, b, r;

MATCH (a) RETURN count(a);
MATCH ()-[r:rel]->() RETURN count(r);

CREATE ()-[:rel]->()-[:rel]->();

MATCH (a)-[r:rel]->(b), (c), p=(d)
DELETE a, b, r, c, d, p;

MATCH (a) RETURN count(a);
MATCH ()-[r:rel]->() RETURN count(r);

--AG-2 : failed DELETE graph path

CREATE ()-[:rel]->()-[:rel]->();

MATCH p = ()-[:rel]->(), ()-[r:rel]->()
DELETE r
RETURN *;

MATCH p = ()-[:rel]->(), (a)-[:rel]->()
DELETE a
RETURN *;

MATCH p = ()-[:rel]->()
DELETE p;

MATCH (a) RETURN count(a);
MATCH ()-[r:rel]->() RETURN count(r);

CREATE ()-[:rel]->()-[:rel]->();

MATCH p = ()-[:rel]->(), gp = ()-[:rel]->(), (a)
DELETE p, gp, a;

MATCH (a) RETURN count(a);
MATCH ()-[r:rel]->() RETURN count(r);

-- AG-159
CREATE (:v1), (:v2);

MATCH p=(a:v1), (b:v2)
DETACH DELETE a
RETURN label(b);

MATCH p=(a:v2)
DELETE a
RETURN p;

CREATE (:v1);

MATCH (a:v1)
DELETE a
DETACH DELETE a
DELETE a;

MATCH (a:v1) RETURN a;

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

-- special multiple set from a bug (AG-279) - this should be successful
CREATE (:bug_AG279 {"a1234567890123456789": 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', "b1234567890123456789": 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', "c1234567890123456789": 'ccccccccccccccccccccccccccccccc', "d1234567890123456789": '123456789012345678901234', "e1234567890123456789": 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee', "f": '12345678901234567890123456789012345678901234567'});
MATCH (v:bug_AG279) return v;

-- enable_multiple_update
SET enable_multiple_update = false;
CREATE (:multiple_update {no:1}), (:multiple_update {no:1});

MATCH (a:multiple_update), (b:multiple_update)
SET a.no = a.no + 1
RETURN a.no;

MATCH (a:multiple_update)
RETURN a.no;

MATCH (a:multiple_update)
SET a.no = 5
SET a.no = 6
SET a.no = 7
SET a.no = 8
RETURN a.no;

MATCH (a:multiple_update)
RETURN a.no;

SET enable_multiple_update = true;

MATCH (a:multiple_update), (b:multiple_update)
SET a.no = a.no + 1
RETURN a.no;

MATCH (a:multiple_update)
RETURN a.no;

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

-- ag-322: Auto create non-existent labels
MERGE (a:v3);
MERGE (a:v4) MERGE (b:v5) MERGE (a)-[:e3]->(b);
MATCH (a) RETURN a;

MERGE p=(a:v4)-[:e4]->(b:v5) RETURN p;

-- fix: unrecognized node type: 121
MERGE (j {j: null IS NULL}) return j;

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
MATCH (n:v) RETURN n, graphid_labid(id(n)), pg_typeof(id(n));

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
RETURN ['abc' STARTS WITH 'a'];

-- ends with

RETURN 'abc' ENDS WITH 'c';
RETURN 'abc' ENDS WITH '';
RETURN 'abc' ENDS WITH 'ab';
RETURN 'abc' ENDS WITH 'abcd';
RETURN 'abc' ENDS WITH 1;
RETURN ['abc' ENDS WITH 'c'];

-- contains

RETURN 'abc' CONTAINS 'b';
RETURN 'abc' CONTAINS '';
RETURN 'abc' CONTAINS 'abcd';
RETURN 'abc' CONTAINS 1;
RETURN ['abc' CONTAINS 'b'];

-- =~

RETURN 'abc' =~ 'abc';
RETURN 'abc' =~ '';
RETURN 'abc' =~ 'a';
RETURN 'abc' =~ 'abcd';
RETURN 'abc' =~ '(?i)A';
RETURN 'abc' =~ 'a(b{1})c';
RETURN 'abc' =~ 1;
RETURN ['abc' =~ 'abc'];

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

--
-- implicit load
--

CREATE GRAPH impload;
SET GRAPH_PATH = impload;

CREATE TABLE external_table (id int, name varchar(255));
INSERT INTO external_table VALUES (1, '1');

LOAD FROM external_table AS r CREATE (=r);

MATCH (n) RETURN n;

--
-- SRF
--

CREATE GRAPH srf;
SET graph_path = srf;

CREATE (:v {id: 1})-[:e]->(:v {id: 2});
MATCH p=()-[]->() RETURN unnest(nodes(p)).id;

-- AG-161
CREATE GRAPH ag161;
SET GRAPH_PATH = ag161;

CREATE (:v1 {no:1});
CREATE (:v2 {no:2});
CREATE (:v3 {no:3});

ALTER DATABASE regression SET GRAPH_PATH TO ag161;
SET PARALLEL_SETUP_COST = 0;

EXPLAIN (VERBOSE, COSTS OFF) MATCH (a) RETURN count(a);
MATCH (a) RETURN count(a);

ALTER DATABASE regression SET GRAPH_PATH TO DEFAULT;
DROP GRAPH ag161 CASCADE;

-- AG-169, AG-170
CREATE GRAPH ag170;
SET GRAPH_PATH = ag170;

BEGIN;
CREATE (:foo {bar : 'a'});
MATCH (a:foo {bar : 'a'}) RETURN properties(a);
END;

MATCH (a:foo {bar : 'a'})
DELETE a
RETURN count(*);

BEGIN;
CREATE (:foo {bar : 'a'});
CREATE (:foo {bar : 'b'});
MATCH (a:foo) RETURN properties(a);
END;

MATCH (a:foo)
DELETE a
RETURN count(*);

BEGIN;
CREATE (a:foo {bar : 'a'})
MERGE (b:foo {bar : 'b'})
	ON CREATE SET b.bar = 'a';
MATCH (a:foo) RETURN properties(a);
END;

MATCH (a:foo)
DELETE a
RETURN count(*);

\set AUTOCOMMIT OFF

\echo :AUTOCOMMIT

CREATE (:foo {bar : 'a'});
MATCH (a:foo {bar : 'a'}) RETURN count(*);

COMMIT;

MATCH (a:foo {bar : 'a'}) RETURN count(*);

COMMIT;

\set AUTOCOMMIT ON

\echo :AUTOCOMMIT

DROP GRAPH ag170 CASCADE;


-- AG-183 Unable to prepare cypher statement
CREATE graph ag183;
SET graph_path TO ag183;

CREATE VLABEL v;

PREPARE stmt1 (text, int, bool) AS CREATE (:v {name: $1, age: $2, married: $3});
PREPARE stmt2 (text, int, bool) AS MERGE (a:v {name: $1, age: $2, married: $3}) RETURN properties(a);

EXECUTE stmt1 ('p1', 30, false);
EXECUTE stmt2 ('p2', 40, false);
EXECUTE stmt2 ('p2', 40, false);

MATCH (a:v) RETURN properties(a);

PREPARE stmt3 AS MATCH (a:v) SET a.age = a.age + 1 RETURN properties(a);
EXECUTE stmt3;

PREPARE stmt4 (text) AS MATCH (a {name:$1}) RETURN properties(a);
EXECUTE stmt4 ('p1');

PREPARE stmt5 (bool) AS MATCH (a {married:$1}) DELETE a;
EXECUTE stmt5 (false);

MATCH (a:v) RETURN properties(a);

DROP GRAPH ag183 CASCADE;

-- AG-189

CREATE graph ag189;
SET graph_path TO ag189;

CREATE (vt1:TEST{name:'isaac', age:32});
MATCH (vt1:TEST{name:'isaac'}) WITH vt1, vt1.age AS my_age
SET vt1.age = my_age +1
RETURN properties(vt1), my_age;

MATCH (vt1:TEST)
RETURN properties(vt1);

DROP GRAPH ag189 CASCADE;

--
-- UNWIND
--

UNWIND [1, 2, 3] AS i RETURN i;

CREATE GRAPH test_unwind;
CREATE ({a: [1, 2, 3]}), ({a: [4, 5, 6]});
MATCH (n) WITH n.a AS a UNWIND a AS i RETURN *;
DROP GRAPH test_unwind CASCADE;

PREPARE t(_jsonb) AS UNWIND $1 AS i UNWIND i.a AS j UNWIND j AS k RETURN k;
EXECUTE t(ARRAY['{"a": [[1, 2], [3, 4]]}'::jsonb,
                '{"a": [[5, 6], [7, 8]]}'::jsonb]);
DEALLOCATE t;

-- LIMIT clause causes VLE relations to crash, issue AG-254

CREATE GRAPH asterisk;

CREATE VLABEL vertex;
CREATE ELABEL edge;

CREATE VLABEL city; -- additional VLABELs cause crashes (used or not)
CREATE ELABEL road; -- additional ELABELs cause crashes (used or not)

CREATE (a0:vertex {name: 'A'})
CREATE (b0:vertex {name: 'B'})
CREATE (q0:vertex {name: 'Q'})
CREATE (x0:vertex {name: 'X'})
MERGE (a0)-[:edge]->(b0)
MERGE (q0)-[:edge]->(a0)
MERGE (b0)-[:edge]->(q0)
MERGE (a0)-[:edge]->(x0)
MERGE (x0)-[:edge]->(b0);

-- 4 row set
MATCH p=((u:vertex {name: 'A'})-[*]->(v:vertex {name: 'B'}))
RETURN p LIMIT 4; --crash

-- 22 row set
MATCH p=((u)-[*0..3]->(v)) RETURN p LIMIT 0; --no crash (nc)
MATCH p=((u)-[*0..3]->(v)) RETURN p LIMIT 1; --nc
MATCH p=((u)-[*0..3]->(v)) RETURN p LIMIT 4; --nc/memory corrupted (mem)
MATCH p=((u)-[*0..3]->(v)) RETURN p LIMIT 5; --crash

-- 18 row set
MATCH p=((u)-[*..3]->(v)) RETURN p LIMIT 0; -- nc
MATCH p=((u)-[*..3]->(v)) RETURN p LIMIT 1; -- nc
MATCH p=((u)-[*..3]->(v)) RETURN p LIMIT 4; -- nc/mem
MATCH p=((u)-[*..3]->(v)) RETURN p LIMIT 5; -- crash

DROP GRAPH asterisk CASCADE;

--
-- CTES
--
SET graph_path = agens;
WITH graph as (MERGE (n) return n) SELECT * FROM graph;
WITH graph as (CREATE (n) return n) SELECT * FROM graph;
WITH graph as (MATCH (n) RETURN n) SELECT * FROM graph;
WITH graph as (LOAD FROM history AS n RETURN n) SELECT * FROM graph;
WITH graph as (MATCH (n) SET n.vertex = true RETURN n) SELECT * FROM graph;

--
-- Github issue #635
--
SET graph_path = agens;
MATCH (n) WITH n, 'n' as name RETURN name;
CREATE (n1 {name: 'test'}) WITH NULL AS a0 RETURN a0;
MATCH (n1) WITH n1, 'test' as name WHERE n1.name = name RETURN n1;

--
-- AGV2-416
--
MATCH (m1:movie),(m2:review) 
MERGE (m1)-[e:reviewed]->(m2);

MATCH (n:non) SET n.id = 1;
MERGE (n:non) SET n.id = 1;
CREATE (n:none) SET n.id = 1;

MATCH (source:base {entity_id: 'CompanyA'})
WITH source
MATCH (target:base {entity_id: 'ProductX'})
MERGE (source)-[r:DIRECTED]-(target)
SET r += {'weight': 1.0, 'description': 'CompanyA develops ProductX', 'keywords': 'develop, produce', 'source_id': 'chunk-eeec0036b909839e8ec4fa150c939eec', 'file_path': 'custom_kg', 'created_at': 1751270491}
RETURN r, source, target;

CREATE (source:base {entity_id: 'CompanyA'}), 
       (target:base {entity_id: 'ProductX'});

MATCH (source:base {entity_id: 'CompanyA'})
WITH source
MATCH (target:base {entity_id: 'ProductX'})
MERGE (source)-[r:DIRECTED]-(target)
SET r += {'weight': 1.0, 'description': 'CompanyA develops ProductX', 'keywords': 'develop, produce', 'source_id': 'chunk-eeec0036b909839e8ec4fa150c939eec', 'file_path': 'custom_kg', 'created_at': 1751270491}
RETURN r, source, target;

--
-- Github issue #747
--
MATCH (n0) MERGE (n0 {k:1}) RETURN count(n0);
MERGE (n0) MERGE (n0 {k:1}) RETURN count(n0);
MATCH ()-[r]->() MERGE ()-[r {k:1}]->() RETURN count (r);
MERGE ()-[r:idk]->() MERGE ()-[r {k:1}]->() RETURN count (r);
MATCH p=() MERGE p=() RETURN p;
MATCH p=() MERGE a=(p) RETURN p;

--
-- AGV2-422
--
CREATE GRAPH rename_test;
SET graph_path = rename_test;
CREATE (:v1)-[:e1]->(:v1);
MATCH (v1:v1)-[e1:e1]->(v2:v1) RETURN v1,e1,v2;
ALTER ELABEL e1 RENAME TO new_e1;
ALTER VLABEL v1 RENAME TO new_v1;
CREATE (:new_v1)-[:new_e1]->(:new_v1);
MATCH (v1:new_v1)-[e1:new_e1]->(v2:new_v1) RETURN v1,e1,v2;

--
-- DELETE/DETACH DELETE optimization
--
CREATE GRAPH delete_opt;
SET graph_path = delete_opt;
CREATE (:n1)-[:e1]->(:n2);
CREATE (:n3)-[:e2]->(:n4);
CREATE (:n5);

-- Without auto_gather_graphmeta, append node should have all relations
EXPLAIN MATCH (n:n1) DETACH DELETE (n);
EXPLAIN MATCH (n:n2) DETACH DELETE (n);
EXPLAIN MATCH (n:n3) DETACH DELETE (n);
EXPLAIN MATCH (n:n4) DETACH DELETE (n);
EXPLAIN MATCH (n:n5) DETACH DELETE (n);

SET auto_gather_graphmeta = true;
SELECT edge, start, "end"
FROM ag_graphmeta
ORDER BY edge, start, "end";

-- Append node should not have relation e2, since n1 and n2 are not connected to e2 edge label.
EXPLAIN MATCH (n:n1) DETACH DELETE (n);
EXPLAIN MATCH (n:n2) DETACH DELETE (n);
-- Append node should not have relation e1, since n3 is not connected to e1 edge label.
EXPLAIN MATCH (n:n3) DETACH DELETE (n);
EXPLAIN MATCH (n:n4) DETACH DELETE (n);
-- Append node should not have any relations, since n5 is not connected to any edge labels
EXPLAIN MATCH (n:n5) DETACH DELETE (n);

--
-- AGV2-315
--
CREATE GRAPH agv2_315;
SET graph_path = agv2_315;

CREATE (:person {name: 'Alice'})-[:knows {since: 2020}]->(:person {name: 'Bob'});
CREATE (:person {name: 'Bob'})-[:knows {since: 2021}]->(:person {name: 'Charlie'});

MATCH p=(:person)-[r]->(:person) RETURN [l in relationships(p)];
MATCH p=(:person)-[r]->(:person) RETURN [l in relationships(p) | l];
MATCH p=(:person)-[r]->(:person) RETURN [l in relationships(p) WHERE type(l) = 'knows'];
MATCH p=(:person)-[r]->(:person) RETURN [l in relationships(p) WHERE type(l) = 'knows' | l];
MATCH p=(:person)-[r]->(:person) RETURN [l in relationships(p) WHERE type(l) = 'knows' | type(l)];
MATCH p=(:person)-[r]->(:person) RETURN [l in relationships(p) WHERE label(startNode(l)) = 'person' | startNode(l)];
MATCH p=(:person)-[r]->(:person) RETURN [l in relationships(p) | type(l)];
MATCH p=(:person)-[r]->(:person) RETURN [l in relationships(p) | l.since];
MATCH p=(:person)-[r]->(:person) RETURN [n in nodes(p)];
MATCH p=(:person)-[r]->(:person) RETURN [n in nodes(p) | n];
MATCH p=(:person)-[r]->(:person) RETURN [n in nodes(p) WHERE n.name = 'Bob'];
MATCH p=(:person)-[r]->(:person) RETURN [n in nodes(p) WHERE n.name = 'Bob' | n];
MATCH p=(:person)-[r]->(:person) RETURN [n in nodes(p) WHERE label(n) = 'person' | n.name];
MATCH p=(:person)-[r]->(:person) RETURN [n in nodes(p) | label(n)];
MATCH p=(:person)-[r]->(:person) RETURN [n in nodes(p) | n.name];

MATCH p=(n1)-[r*1..2]->(n2) WHERE all(x in r where x.since >= 2020) RETURN count(p);
MATCH p=(n1)-[r*1..2]->(n2) WHERE all(x in r where x.since IS NOT NULL) RETURN count(p);
MATCH p=(n1)-[r*1..2]->(n2) WHERE any(x in r where x.since = 2021) RETURN count(p);

MATCH p=(:person)-[r]->(:person) WHERE length([l in relationships(p) | type(l)]) > 0 RETURN count(p);

--
-- AGV2-308
--
CREATE GRAPH agv2_308;
SET graph_path = agv2_308;
CREATE (:test);
UNWIND [{}] AS row
MATCH (t:test)
SET t += row.id
RETURN t;

--
-- AGV2-324: ORDER BY may reference variables/expressions not in RETURN
--
CREATE GRAPH ag324;
SET graph_path = ag324;
CREATE (:person {name: 'Alice', age: 30}),
       (:person {name: 'Bob', age: 5}),
       (:person {name: 'Charlie', age: 100}),
       (:person {name: 'Dave', age: 30});

-- order by a property that is not in the RETURN list (numeric, not lexical)
MATCH (p:person) RETURN p.name AS name ORDER BY p.age;
MATCH (p:person) RETURN p.name AS name ORDER BY p.age DESC;

-- order by a RETURN alias
MATCH (p:person) RETURN p.name AS name ORDER BY name;

-- mix of alias and non-returned expression
MATCH (p:person) RETURN p.name AS name ORDER BY p.age, name;
MATCH (p:person) RETURN p.name AS name ORDER BY name, p.age DESC;

-- a returned key keeps jsonb (numeric) ordering after coercion
MATCH (p:person) RETURN p.name AS name, p.age AS age ORDER BY age;

-- DISTINCT together with ORDER BY
MATCH (p:person) RETURN DISTINCT p.age AS age ORDER BY age DESC;

-- ORDER BY an aggregate: count() is bigint and is coerced to jsonb in the
-- target list, so the sort operator must follow the final (jsonb) type
MATCH (p:person) RETURN p.age AS age, count(*) AS cnt ORDER BY cnt, age;

--
-- AGV2-394: FOR clause -- unnest an array expression and join it with the
-- working table, optionally exposing the element ordinality via WITH OFFSET.
--
CREATE GRAPH agv2_394;
SET graph_path = agv2_394;
CREATE (:Person {Id: 1, name: 'Alice', tags: ['x','y']})-[:Owns]->(:Account {no: 100}),
       (:Person {Id: 2, name: 'Bob',   tags: ['z']})-[:Owns]->(:Account {no: 200}),
       (:Person {Id: 3, name: 'Carol', tags: []})-[:Owns]->(:Account {no: 300});

-- == element variety ==
-- integers
FOR x in [1,2,3] RETURN x ORDER BY x;
-- strings
FOR x in ['a','b','c'] RETURN x ORDER BY x;
-- a single element
FOR x in [42] RETURN x;
-- an empty array -> no rows
FOR x in [] RETURN x;
-- elements that are themselves arrays
FOR x in [[1,2],[3]] RETURN x ORDER BY x;
-- elements that are maps
FOR x in [{a: 1},{a: 2}] RETURN x.a AS a ORDER BY a;

-- == the array from an expression ==
-- range() with and without a step, ascending and descending
FOR x in range(1,5) RETURN x ORDER BY x;
FOR x in range(0,10,2) RETURN x ORDER BY x;
FOR x in range(5,1,-1) RETURN x ORDER BY x;
-- a variable bound to an array (UNWIND binds lst, then FOR unnests it)
UNWIND [[10,20,30]] AS lst FOR x in lst RETURN x ORDER BY x;
-- array concatenation
FOR x in [1,2] + [3,4] RETURN x ORDER BY x;
-- a list-valued property (LATERAL: depends on the matched row)
MATCH (p:Person) FOR t in p.tags RETURN p.name AS name, t ORDER BY name, t;
-- range() whose bound comes from the matched row (LATERAL)
MATCH (p:Person) WHERE p.Id <> 1
FOR x in range(1, p.Id) RETURN p.Id, x ORDER BY Id, x;

-- == WITH OFFSET ==
-- default offset variable name "offset", 0-based per the GQL standard
FOR x in ['a','b','c'] WITH OFFSET RETURN x, "offset" ORDER BY "offset";
-- a named offset variable
FOR x in ['a','b','c'] WITH OFFSET AS i RETURN x, i ORDER BY i;
-- offset over an empty array -> no rows
FOR x in [] WITH OFFSET AS i RETURN x, i;
-- offset over a typed NULL array -> no rows
FOR x in NULL::_text WITH OFFSET RETURN x, "offset";
FOR x in NULL::jsonb WITH OFFSET AS i RETURN x, i;
-- offset after MATCH (LATERAL)
MATCH (p:Person)-[:Owns]->(a:Account)
FOR alert in ['all','some'] WITH OFFSET
RETURN p.Id, alert AS alert_type, "offset"
ORDER BY Id, alert_type, "offset";

-- == composition / chaining ==
-- standalone FOR, then WITH, then RETURN
FOR x in [1,2,3] WITH x AS col RETURN col ORDER BY col;
-- FOR, then WITH ... WHERE to filter the rows
FOR x in [1,2,3,4] WITH x WHERE x > 2 RETURN x ORDER BY x;
-- FOR, then a standalone ORDER BY / LIMIT and ORDER BY / SKIP
FOR x in [5,3,1,4,2] RETURN x ORDER BY x DESC LIMIT 2;
FOR x in [1,2,3,4,5] RETURN x ORDER BY x SKIP 3;
-- FOR, then aggregation
FOR x in [1,2,3,4] RETURN count(*) AS c, sum(x) AS s, collect(x) AS xs;
-- a WITH-projected list feeding FOR
MATCH (p:Person) WITH collect(p.Id) AS ids FOR x in ids RETURN x ORDER BY x;
-- WITH OFFSET immediately followed by a regular WITH (WITH must not be
-- swallowed as WITH OFFSET)
FOR x in [1,2,3] WITH OFFSET WITH x AS col RETURN col ORDER BY col;
-- FOR over the elements produced by a previous FOR (nested unnest)
FOR x in [[1,2],[3,4,5]] FOR y in x RETURN y ORDER BY y;
-- two independent FORs -> cartesian product
FOR a in [1,2] FOR b in [10,20] RETURN a, b ORDER BY a, b;
-- UNWIND then FOR (the FOR array depends on the unwound value)
UNWIND [1,2] AS u FOR x in [u, u * 10] RETURN u, x ORDER BY u, x;
-- FOR then UNWIND
FOR x in [1,2] UNWIND [x, x + 100] AS u RETURN x, u ORDER BY x, u;
-- FOR feeds the keys of a following MATCH
FOR id in [1,3] MATCH (p:Person {Id: id}) RETURN p.name AS name ORDER BY name;

-- == FOR drives write clauses ==
FOR x in [10,20,30] CREATE (:item {v: x});
MATCH (n:item) RETURN n.v AS v ORDER BY v;
FOR x in [10,20,40] MERGE (:item {v: x});
MATCH (n:item) RETURN n.v AS v ORDER BY v;
FOR x in [10,20] MATCH (n:item {v: x}) DELETE n;
MATCH (n:item) RETURN n.v AS v ORDER BY v;
MATCH (p:Person {Id: 1}) FOR x in [99] SET p.score = x RETURN p.name AS name, p.score AS score;

-- == FOR in the read path (both branches of a UNION) ==
FOR x in [1,2] RETURN x UNION ALL FOR x in [3,4] RETURN x;

-- == errors and edge cases ==
-- a scalar integer is not an array
FOR x in 1 RETURN x;
-- a scalar jsonb (property) is not an array
MATCH (p:Person) FOR x in p.Id RETURN x;
-- the element variable must not collide with an existing variable
MATCH (p:Person) FOR p in [1,2] RETURN p;
-- the offset variable must not collide with the element variable
FOR x in [1,2] WITH OFFSET AS x RETURN x;
-- the offset variable must not collide with an existing variable
MATCH (p:Person) FOR x in [1] WITH OFFSET AS p RETURN x;

--
-- EXISTS subquery -- true iff the subquery yields at least one row (never NULL)
--
CREATE GRAPH exists_subquery;
SET graph_path = exists_subquery;
CREATE (andy:Person {name: 'Andy', age: 36}),
       (timothy:Person {name: 'Timothy', age: 25}),
       (peter:Person {name: 'Peter', age: 35}),
       (andy)-[:HAS_DOG]->(:Dog {name: 'Andy'}),
       (timothy)-[:HAS_CAT]->(:Cat {name: 'Mittens'}),
       (fido:Dog {name: 'Fido'})<-[:HAS_DOG]-(peter)-[:HAS_DOG]->(:Dog {name: 'Ozzy'}),
       (fido)-[:HAS_TOY]->(:Toy {name: 'Banana'});

-- bare pattern, correlated to the outer row
MATCH (person:Person)
WHERE EXISTS { (person)-[:HAS_DOG]->(:Dog) }
RETURN person.name AS name ORDER BY name;

-- pattern with an inner WHERE referencing both outer and inner variables
MATCH (person:Person)
WHERE EXISTS { (person)-[:HAS_DOG]->(dog:Dog) WHERE person.name = dog.name }
RETURN person.name AS name ORDER BY name;

-- explicit MATCH form
MATCH (person:Person)
WHERE EXISTS { MATCH (person)-[:HAS_DOG]->(dog:Dog) WHERE person.name = dog.name }
RETURN person.name AS name ORDER BY name;

-- nested EXISTS
MATCH (person:Person)
WHERE EXISTS {
    MATCH (person)-[:HAS_DOG]->(dog:Dog)
    WHERE EXISTS { MATCH (dog)-[:HAS_TOY]->(toy:Toy) WHERE toy.name = 'Banana' }
}
RETURN person.name AS name ORDER BY name;

-- SQL SELECT wrapping a Cypher read
MATCH (person:Person)
WHERE EXISTS { SELECT * FROM (MATCH (person)-[:HAS_DOG]->(dog:Dog) RETURN person.name) t }
RETURN person.name AS name ORDER BY name;

-- a trailing RETURN inside the subquery is ignored (only row existence matters)
MATCH (person:Person)
WHERE EXISTS { MATCH (person)-[:HAS_DOG]->(:Dog) RETURN person.name }
RETURN person.name AS name ORDER BY name;

-- EXISTS as a returned boolean value (never NULL)
MATCH (person:Person)
RETURN person.name AS name, EXISTS { (person)-[:HAS_DOG]->(:Dog) } AS hasDog
ORDER BY name;

-- NOT EXISTS
MATCH (person:Person)
WHERE NOT EXISTS { (person)-[:HAS_DOG]->(:Dog) }
RETURN person.name AS name ORDER BY name;

-- combined with AND / OR
MATCH (person:Person)
WHERE EXISTS { (person)-[:HAS_DOG]->(:Dog) }
  AND EXISTS { (person)-[:HAS_CAT]->(:Cat) }
RETURN person.name AS name ORDER BY name;

MATCH (person:Person)
WHERE EXISTS { (person)-[:HAS_DOG]->(:Dog) }
   OR EXISTS { (person)-[:HAS_CAT]->(:Cat) }
RETURN person.name AS name ORDER BY name;

-- OPTIONAL MATCH always yields a row, so EXISTS is always true
MATCH (person:Person)
WHERE EXISTS { OPTIONAL MATCH (person)-[:HAS_CAT]->(:Cat) }
RETURN person.name AS name ORDER BY name;

-- uncorrelated: any dog exists at all
MATCH (person:Person)
WHERE EXISTS { (:Dog) }
RETURN person.name AS name ORDER BY name;

-- a multi-hop pattern
MATCH (person:Person)
WHERE EXISTS { (person)-[:HAS_DOG]->(:Dog)-[:HAS_TOY]->(:Toy) }
RETURN person.name AS name ORDER BY name;

-- several comma-separated patterns must all match
MATCH (person:Person)
WHERE EXISTS { (person)-[:HAS_DOG]->(:Dog), (person)-[:HAS_CAT]->(:Cat) }
RETURN person.name AS name ORDER BY name;

-- an inline property constraint in the pattern
MATCH (person:Person)
WHERE EXISTS { (person)-[:HAS_DOG]->(:Dog {name: 'Fido'}) }
RETURN person.name AS name ORDER BY name;

-- a multi-clause GQL read statement inside the subquery
MATCH (person:Person)
WHERE EXISTS {
    MATCH (person)-[:HAS_DOG]->(dog:Dog) WITH dog WHERE dog.name = 'Fido' RETURN dog
}
RETURN person.name AS name ORDER BY name;

-- a UNION inside the subquery (has a dog or a cat)
MATCH (person:Person)
WHERE EXISTS {
    MATCH (person)-[:HAS_DOG]->(:Dog) RETURN 1
    UNION
    MATCH (person)-[:HAS_CAT]->(:Cat) RETURN 1
}
RETURN person.name AS name ORDER BY name;

-- EXISTS in a WITH ... WHERE stage
MATCH (person:Person)
WITH person
WHERE EXISTS { (person)-[:HAS_DOG]->(:Dog) }
RETURN person.name AS name ORDER BY name;

-- combined with a scalar predicate
MATCH (person:Person)
WHERE person.age > 30 AND EXISTS { (person)-[:HAS_DOG]->(:Dog) }
RETURN person.name AS name ORDER BY name;

-- the older parenthesized EXISTS ( pattern ) form still works alongside
MATCH (person:Person)
WHERE EXISTS ( (person)-[:HAS_DOG]->(:Dog) )
RETURN person.name AS name ORDER BY name;

-- a nested NOT EXISTS (a dog that has no toy)
MATCH (person:Person)
WHERE EXISTS {
    MATCH (person)-[:HAS_DOG]->(dog:Dog)
    WHERE NOT EXISTS { (dog)-[:HAS_TOY]->(:Toy) }
}
RETURN person.name AS name ORDER BY name;

-- correlated inner WHERE with inequality
MATCH (person:Person)
WHERE EXISTS { (person)-[:HAS_DOG]->(dog:Dog) WHERE dog.name <> person.name }
RETURN person.name AS name ORDER BY name;

-- an update is not allowed inside an EXISTS subquery
MATCH (person:Person)
WHERE EXISTS { CREATE (:Foo) }
RETURN person.name AS name;

--
-- COUNT subquery -- the number of rows the subquery yields (bigint, never NULL).
-- Reuses the EXISTS-subquery graph (Person/Dog/Cat/Toy).
--

-- correlated count in RETURN (Andy has 1 dog, Peter 2, Timothy 0)
MATCH (person:Person)
RETURN person.name AS name, COUNT { (person)-[:HAS_DOG]->(:Dog) } AS dogs
ORDER BY name;

-- correlated count in WHERE
MATCH (person:Person)
WHERE COUNT { (person)-[:HAS_DOG]->(:Dog) } > 1
RETURN person.name AS name ORDER BY name;

-- inner WHERE referencing both outer and inner variables
MATCH (person:Person)
RETURN person.name AS name,
       COUNT { (person)-[:HAS_DOG]->(dog:Dog) WHERE person.name = dog.name } AS c
ORDER BY name;

-- explicit MATCH form
MATCH (person:Person)
RETURN person.name AS name, COUNT { MATCH (person)-[:HAS_DOG]->(:Dog) } AS c
ORDER BY name;

-- OPTIONAL MATCH always yields a row, so the count is at least 1
MATCH (person:Person)
RETURN person.name AS name, COUNT { OPTIONAL MATCH (person)-[:HAS_CAT]->(:Cat) } AS c
ORDER BY name;

-- uncorrelated: how many dogs exist at all
RETURN COUNT { (:Dog) } AS dogs;

-- never NULL: zero when nothing matches
MATCH (person:Person)
RETURN person.name AS name, COUNT { (person)-[:NO_SUCH_REL]->() } AS c
ORDER BY name;

-- the subquery's own DISTINCT / UNION are honored (counted, not over-counted)
RETURN COUNT { MATCH (:Person)-[:HAS_DOG]->() RETURN DISTINCT 1 } AS distinct_rows;
RETURN COUNT { MATCH (d:Dog) RETURN d.name UNION MATCH (c:Cat) RETURN c.name } AS names;
-- LIMIT / OFFSET are honored via the SQL SELECT form (3 dogs)
RETURN COUNT { SELECT * FROM (MATCH (d:Dog) RETURN d) t LIMIT 1 } AS limited;
RETURN COUNT { SELECT * FROM (MATCH (d:Dog) RETURN d) t OFFSET 2 } AS offset_2;

-- a multi-column RETURN inside (rows are counted, not columns)
RETURN COUNT { MATCH (p:Person)-[:HAS_DOG]->(d:Dog) RETURN p, d } AS edges;

-- a correlated full read statement with a trailing RETURN
MATCH (person:Person)
RETURN person.name AS name, COUNT { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d } AS c
ORDER BY name;

-- SQL SELECT wrapping a Cypher read
MATCH (person:Person)
RETURN person.name AS name,
       COUNT { SELECT * FROM (MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d) t } AS c
ORDER BY name;

-- in an arithmetic expression
MATCH (person:Person)
RETURN person.name AS name, COUNT { (person)-[:HAS_DOG]->(:Dog) } + 1 AS c
ORDER BY name;

-- in a WITH ... WHERE stage
MATCH (person:Person)
WITH person
WHERE COUNT { (person)-[:HAS_DOG]->(:Dog) } >= 1
RETURN person.name AS name ORDER BY name;

-- nested COUNT inside COUNT (dogs that own at least one toy)
MATCH (person:Person)
RETURN person.name AS name,
       COUNT { MATCH (person)-[:HAS_DOG]->(dog:Dog)
               WHERE COUNT { (dog)-[:HAS_TOY]->(:Toy) } > 0 } AS c
ORDER BY name;

-- a multi-hop pattern (dogs that have a toy)
MATCH (person:Person)
RETURN person.name AS name,
       COUNT { (person)-[:HAS_DOG]->(:Dog)-[:HAS_TOY]->(:Toy) } AS c
ORDER BY name;

-- several comma-separated patterns (joined: dogs x cats per person)
MATCH (person:Person)
RETURN person.name AS name,
       COUNT { (person)-[:HAS_DOG]->(:Dog), (person)-[:HAS_CAT]->(:Cat) } AS c
ORDER BY name;

-- an inline property constraint in the pattern
MATCH (person:Person)
RETURN person.name AS name,
       COUNT { (person)-[:HAS_DOG]->(:Dog {name: 'Fido'}) } AS c
ORDER BY name;

-- inner WHERE on the inner variable only
MATCH (person:Person)
RETURN person.name AS name,
       COUNT { (person)-[:HAS_DOG]->(dog:Dog) WHERE dog.name = 'Fido' } AS c
ORDER BY name;

-- a subquery that aggregates: counts the produced groups
RETURN COUNT { MATCH (:Person)-[:HAS_DOG]->(d:Dog) RETURN d.name, count(*) } AS c;

-- in a CASE expression
MATCH (person:Person)
RETURN person.name AS name,
       CASE WHEN COUNT { (person)-[:HAS_DOG]->(:Dog) } > 1 THEN 'pack' ELSE 'few' END AS k
ORDER BY name;

-- comparing two COUNT subqueries
MATCH (person:Person)
WHERE COUNT { (person)-[:HAS_DOG]->(:Dog) } > COUNT { (person)-[:HAS_CAT]->(:Cat) }
RETURN person.name AS name ORDER BY name;

-- difference of two COUNT subqueries
MATCH (person:Person)
RETURN person.name AS name,
       COUNT { (person)-[:HAS_DOG]->(:Dog) } - COUNT { (person)-[:HAS_CAT]->(:Cat) } AS diff
ORDER BY name;

-- COUNT = 0 to find the absence of a relationship
MATCH (person:Person)
WHERE COUNT { (person)-[:HAS_DOG]->(:Dog) } = 0
RETURN person.name AS name ORDER BY name;

-- in ORDER BY
MATCH (person:Person)
RETURN person.name AS name
ORDER BY COUNT { (person)-[:HAS_DOG]->(:Dog) } DESC, name;

-- in a WITH projection
MATCH (person:Person)
WITH person.name AS name, COUNT { (person)-[:HAS_DOG]->(:Dog) } AS c
RETURN name, c ORDER BY name;

-- the count() aggregate function still works alongside COUNT { }
MATCH (person:Person) RETURN count(*) AS n;

-- as the value of a SET
MATCH (person:Person {name: 'Andy'})
SET person.howManyDogs = COUNT { (person)-[:HAS_DOG]->(:Dog) }
RETURN person.howManyDogs AS dogs;

-- a write is not allowed inside a COUNT subquery
MATCH (person:Person)
WHERE COUNT { CREATE (:Foo) } > 0
RETURN person.name AS name;

--
-- ORDER BY / SKIP / LIMIT inside a cypher read subquery -- a trailing
-- projection tail (sort / skip / limit) after RETURN is accepted in the
-- read-statement form of EXISTS / COUNT / COLLECT subqueries, just like a
-- top-level read query.  Reuses the EXISTS-subquery graph (3 dogs).
--

-- LIMIT caps the rows the subquery yields (COUNT counts the kept rows)
RETURN COUNT { MATCH (d:Dog) RETURN d.name LIMIT 2 } AS limited;
-- SKIP drops leading rows
RETURN COUNT { MATCH (d:Dog) RETURN d.name SKIP 1 } AS skipped;
-- ORDER BY then SKIP + LIMIT slice a window
RETURN COUNT { MATCH (d:Dog) RETURN d.name ORDER BY d.name SKIP 1 LIMIT 1 } AS sliced;
-- ORDER BY alone is accepted (and harmless) in a counted subquery
RETURN COUNT { MATCH (d:Dog) RETURN d.name ORDER BY d.name DESC } AS ordered;
-- LIMIT 0 makes an EXISTS subquery false; LIMIT 1 keeps it true
RETURN EXISTS { MATCH (d:Dog) RETURN d.name LIMIT 0 } AS none;
RETURN EXISTS { MATCH (d:Dog) RETURN d.name LIMIT 1 } AS some;

-- cleanup

DROP GRAPH exists_subquery CASCADE;
DROP GRAPH agv2_394 CASCADE;
DROP GRAPH ag324 CASCADE;
DROP GRAPH agv2_308 CASCADE;
DROP GRAPH srf CASCADE;
DROP GRAPH impload CASCADE;
DROP GRAPH gid CASCADE;
DROP GRAPH np CASCADE;
DROP GRAPH p CASCADE;
DROP GRAPH u CASCADE;
DROP GRAPH ag216a CASCADE;
DROP GRAPH ag216 CASCADE;
DROP GRAPH ag154 CASCADE;
DROP GRAPH t CASCADE;
DROP GRAPH o CASCADE;
DROP GRAPH delete_opt CASCADE;
DROP GRAPH agv2_315 CASCADE;

SET graph_path = agens;

DROP VLABEL feature;
DROP ELABEL supported;
DROP VLABEL repo;
DROP ELABEL lib;
DROP ELABEL doc;

DROP GRAPH agens CASCADE;

DROP TABLE history;

DROP GRAPH rename_test CASCADE;