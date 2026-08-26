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

-- an item of the projection may also be named inside a larger sort key, not
-- only as the whole key; how a key resolves a name is covered in full by the
-- "ORDER BY name resolution" section below
MATCH (a) RETURN a.id AS x ORDER BY x * -1;
MATCH (a) WITH a.id AS x ORDER BY x * -1 RETURN x;

-- UNWIND followed by standalone ORDER BY / LIMIT
UNWIND [3, 1, 2, 5, 4] AS x ORDER BY x LIMIT 3 RETURN x ORDER BY x;
UNWIND [3, 1, 2, 5, 4] AS x ORDER BY x * -1 LIMIT 3 RETURN x ORDER BY x;

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

-- The arrays a traversal answers with are built in per-tuple memory, which is
-- released when the next path is asked for.  Read every one of them back on a
-- query that produces many paths: a release that came too early shows up as the
-- earlier paths' edges and nodes no longer being there to read.
MATCH (a:time)-[x:goes*1..8]->(b:time)
WITH a, b, x
RETURN a.sec AS a, b.sec AS b, size(x) AS hops,
       id(startnode(x[0])) = id(a) AS first_edge_starts_at_a,
       id(endnode(x[size(x) - 1])) = id(b) AS last_edge_ends_at_b,
       b.sec - a.sec = size(x) AS contiguous
ORDER BY a, b;

-- and with the node array materialised as well
MATCH p = (a:time)-[:goes*1..8]->(b:time)
RETURN a.sec AS a, b.sec AS b, length(p) AS len,
       size(nodes(p)) = length(p) + 1 AS nodes_match_length,
       (nodes(p)[0]).sec = a.sec AS first_node_is_start,
       (nodes(p)[length(p)]).sec = b.sec AS last_node_is_end
ORDER BY a, b;

-- A property constraint on a variable-length relationship is carried on the
-- pattern rather than in the query, so it has no row of another element to read
-- from.  Constants are fine; a reference to another element is refused, with
-- the comparison that does work suggested in its place.
MATCH (a:time)-[x:goes*1..2 {sec: 1}]->(b:time) RETURN count(*);
MATCH (a:time)-[x:goes*1..2 {sec: 1 + 1}]->(b:time) RETURN count(*);
MATCH (a:time) MATCH (c:time)-[x:goes*1..2 {sec: a.sec}]->(d:time) RETURN count(*);
MATCH (a:time) WITH a MATCH (c:time)-[x:goes*1..2 {sec: a.sec}]->(d:time) RETURN count(*);
-- the form the hint points at
MATCH (a:time) MATCH (c:time)-[x:goes*1..2]->(d:time)
WHERE c.sec = a.sec RETURN count(*);

-- A property map may also be written as an expression, and the traversal reads
-- whatever it is given as a property map.  One that is not a map describes no
-- relationship, and answers so -- which is what the same constraint on a
-- fixed-length relationship answers.
MATCH ()-[x:goes*1..2 = 5]->() RETURN count(*);
MATCH ()-[x:goes*1..2 = true]->() RETURN count(*);
MATCH ()-[x:goes*1..2 = NULL]->() RETURN count(*);
MATCH ()-[x:goes*1..2 = 1.5]->() RETURN count(*);
MATCH ()-[x:goes*1..2 = 'abc']->() RETURN count(*);
-- the fixed-length forms, which have always answered this way
MATCH ()-[x:goes = 5]->() RETURN count(*);
MATCH ()-[x:goes = true]->() RETURN count(*);
MATCH ()-[x:goes = NULL]->() RETURN count(*);
-- an expression that really is a map is still read as one: an empty map asks
-- nothing of a relationship and so admits every one of them, where a map naming
-- a key these relationships do not carry admits none
MATCH ()-[x:goes*1..2]->() RETURN count(*);
MATCH ()-[x:goes*1..2 = jsonb_build_object()]->() RETURN count(*);
MATCH ()-[x:goes*1..2 = jsonb_build_object('sec', 1)]->() RETURN count(*);
-- and a relationship constraint says nothing about a path of no relationships
MATCH ()-[x:goes*0..1 = 5]->() RETURN count(*);

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

-- negative indices count from the end on native vertex[]/edge[] arrays too, not
-- just jsonb lists (#778: nodes(p)[-1] used to return NULL)
MATCH p = (:time {sec: 1})-[:goes*3]->(:time {sec: 4})
RETURN nodes(p)[-1].sec AS last, nodes(p)[-4].sec AS first, nodes(p)[-5] IS NULL AS oob,
       id(relationships(p)[-1]) = id(relationships(p)[2]) AS rel_last,
       [x IN nodes(p)[-2..] | x.sec] AS last2, [x IN nodes(p)[..-1] | x.sec] AS but_last;

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

-- a variable-length traversal builds the relationship array and the node array
-- only where the query reads them.  An unread array is dropped by leaving the
-- column out, so the columns projected are always a prefix: naming neither
-- leaves ids alone, naming the relationships adds edges, and asking for a path
-- adds vertices on top.  Results must not depend on which of them is built.
EXPLAIN (VERBOSE, COSTS OFF)
MATCH (a:person {id: 1})-[:knows*1..2]->(b:person) RETURN count(*);

EXPLAIN (VERBOSE, COSTS OFF)
MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person) RETURN count(x);

EXPLAIN (VERBOSE, COSTS OFF)
MATCH p = (a:person {id: 1})-[:knows*1..2]->(b:person) RETURN count(p);

MATCH (a:person {id: 1})-[:knows*1..2]->(b:person) RETURN count(*);
MATCH (a:person {id: 1})-[x:knows*1..2]->(b:person) RETURN count(*);
MATCH p = (a:person {id: 1})-[:knows*1..2]->(b:person) RETURN count(*);

-- the arrays a path does read still agree with the traversal that built them
MATCH p = (a:person {id: 1})-[x:knows*1..2]->(b:person)
RETURN length(p), size(nodes(p)), size(relationships(p)), size(x)
ORDER BY 1, 2, 3, 4;

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

-- An ORDER BY / SKIP / LIMIT written after a UNION belongs to the RETURN it
-- follows, so it sorts and pages that branch alone: the rows of the first branch
-- come out untouched, all of them, ahead of the ordered and limited rows of the
-- second.  This is what the ordering clause is attached to in GQL, where a
-- branch is a linear query statement and a union of them has no place for one.
-- To order a union, nest it -- "CALL { ... UNION ... } RETURN ... ORDER BY ..."
-- -- which sorts the union's own rows.
MATCH (a:repo) RETURN a.name AS n
UNION ALL
MATCH ()-[b:lib]->() RETURN b.lang AS n ORDER BY n LIMIT 2;

CALL {
  MATCH (a:repo) RETURN a.name AS n
  UNION ALL
  MATCH ()-[b:lib]->() RETURN b.lang AS n
}
RETURN n ORDER BY n LIMIT 2;

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
-- an unknown-typed literal argument is coerced rather than raising a
-- "could not determine polymorphic type" error (#629)
RETURN collect(NULL);
RETURN collect('abc');

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
-- A pattern used as an existence test compares against the outside variable in
-- the qualification of the query the test is answered from, so the two sides are
-- joined once instead of the pattern being matched again for every row.  The
-- endpoints name a label to keep the plan to the tables being asserted about.
MATCH (a:repo) WHERE exists((a)-[:lib]->(:repo)) RETURN a.name AS name ORDER BY name;
EXPLAIN (costs off) MATCH (a:repo) WHERE exists((a)-[:lib]->(:repo)) RETURN a.name AS name;
MATCH (a:repo) WHERE NOT exists((a)-[:lib]->(:repo)) RETURN a.name AS name ORDER BY name;
EXPLAIN (costs off) MATCH (a:repo) WHERE NOT exists((a)-[:lib]->(:repo)) RETURN a.name AS name;
MATCH (a:repo) WHERE EXISTS { (a)-[:lib]->(:repo) } RETURN a.name AS name ORDER BY name;
-- reached through OR, so no join can answer it; the rows must still be right
MATCH (a:repo) WHERE exists((a)-[:doc]->(:repo)) OR a.year = 2016 RETURN a.name AS name ORDER BY name;
-- A property constraint on an anonymous element is applied beside the pattern,
-- so it asks nothing of an enclosing clause and the pattern is still joined
-- once.  A constraint on an element that has a variable is applied by the
-- enclosing clause, which is the one that keeps the clause.
MATCH (a:repo) WHERE exists((a)-[:lib]->(:repo {year: 2016})) RETURN a.name AS name ORDER BY name;
EXPLAIN (costs off) MATCH (a:repo) WHERE exists((a)-[:lib]->(:repo {year: 2016})) RETURN a.name AS name;
MATCH (a:repo) WHERE exists((a)-[:lib {lang: 'java'}]->(:repo)) RETURN a.name AS name ORDER BY name;
MATCH (a:repo) WHERE exists((a)-[:lib]->(b:repo {year: 2016})) RETURN a.name AS name ORDER BY name;

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

--
-- what a delete hands on
--
-- Removing an element from the graph does not unbind the variable that named
-- it: the value it was read with is still the value it holds, so a clause after
-- the delete can report what was removed.  Each case below gets its own row,
-- because the case before it removed the one it used.
CREATE VLABEL delret;
CREATE ELABEL delrete;
CREATE (:delret {no: 1, nm: 'a'}), (:delret {no: 2, nm: 'b'}),
       (:delret {no: 3, nm: 'c'}), (:delret {no: 4, nm: 'd'}),
       (:delret {no: 5, nm: 'e'}), (:delret {no: 6, nm: 'f'}),
       (:delret {no: 7, nm: 'g'}), (:delret {no: 8, nm: 'h'});
MATCH (a:delret {no: 1}), (b:delret {no: 2})
	CREATE (a)-[:delrete {w: 9}]->(b);

MATCH (n:delret {no: 3}) DELETE n RETURN n AS deleted;
MATCH (n:delret {no: 4}) DELETE n
	RETURN n.nm AS nm, label(n) AS label, properties(n) AS props;

-- an edge, and a vertex that still had one
MATCH ()-[r:delrete]->() DELETE r RETURN r.w AS w, properties(r) AS props;
MATCH (n:delret {no: 1}) DETACH DELETE n RETURN n.nm AS nm;

-- every column bound to the removed element reads alike, whether or not it is
-- the one the delete named
MATCH (n:delret {no: 5}) WITH n, n AS copy DELETE n RETURN copy.nm AS d;
MATCH (a:delret {no: 6}), (b:delret)
	DELETE a
	RETURN b.no AS b_no ORDER BY b_no;

-- deleting the same element twice removes it once
MATCH (n:delret {no: 2}) DELETE n DELETE n RETURN n.nm AS d;

-- an element that has been removed has no row to write to
MATCH (n:delret {no: 7}) DELETE n CREATE (n)-[:delrete]->(:delret);
MATCH (n:delret {no: 7}) DELETE n SET n.nm = 'z';
MATCH (n:delret {no: 8}) DELETE n REMOVE n.nm;

MATCH (n:delret) DETACH DELETE n;
DROP ELABEL delrete;
DROP VLABEL delret;

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
-- Uniqueness: which pairs are asked about
--
-- A pattern may not walk the same relationship twice, which is settled by asking
-- of every pair of its relationships whether they are the same one.
-- Relationships of different labels cannot be: a label id is part of a graph id,
-- so no id belongs to two labels, and the answer is settled before the question
-- is asked.  Those questions are not asked, which shows in a plan as the absence
-- of the `<>' between the two relationships' ids.
--
-- Two things are asserted of every case: whether that `<>' is in the plan, and
-- the rows.  Where the `<>' must be present the rows are the ones it leaves --
-- dropping it would return more.  Where it must be absent the rows are reached
-- without it.  Its own graph, created and dropped here.
--
CREATE GRAPH ru;
SET graph_path = ru;

CREATE VLABEL nd;
CREATE ELABEL twin;
CREATE ELABEL other;
CREATE ELABEL kin;
CREATE ELABEL kid INHERITS (kin);

CREATE (:nd {id: 1}), (:nd {id: 2});
MATCH (a:nd {id: 1}), (b:nd {id: 2}) CREATE (a)-[:twin  {p: 't1'}]->(b);
MATCH (a:nd {id: 2}), (b:nd {id: 1}) CREATE (a)-[:twin  {p: 't2'}]->(b);
MATCH (a:nd {id: 1}), (b:nd {id: 2}) CREATE (a)-[:other {p: 'o1'}]->(b);
MATCH (a:nd {id: 1}), (b:nd {id: 2}) CREATE (a)-[:kin   {p: 'k1'}]->(b);
MATCH (a:nd {id: 2}), (b:nd {id: 1}) CREATE (a)-[:kid   {p: 'd1'}]->(b);

-- One label named twice: the pair can be the same relationship, so the question
-- is asked.  Four rows, not the eight the pattern reaches without it -- the same
-- guard as the two `:rel' relationships above, with the label named.
EXPLAIN (costs off)
MATCH (s:nd)-[r1:twin]-(m:nd)-[r2:twin]-(x:nd)
RETURN s.id AS s, r1.p AS r1, m.id AS m, r2.p AS r2, x.id AS x;
MATCH (s:nd)-[r1:twin]-(m:nd)-[r2:twin]-(x:nd)
RETURN s.id AS s, r1.p AS r1, m.id AS m, r2.p AS r2, x.id AS x
       ORDER BY s, r1, m, r2, x;

-- Two labels: the same pattern with one relationship retyped, where the question
-- has no answer to give and is gone.  Every row it reaches is a row it keeps.
EXPLAIN (costs off)
MATCH (s:nd)-[r1:twin]-(m:nd)-[r2:other]-(x:nd)
RETURN s.id AS s, r1.p AS r1, m.id AS m, r2.p AS r2, x.id AS x;
MATCH (s:nd)-[r1:twin]-(m:nd)-[r2:other]-(x:nd)
RETURN s.id AS s, r1.p AS r1, m.id AS m, r2.p AS r2, x.id AS x
       ORDER BY s, r1, m, r2, x;

-- One side naming no label is every label, which meets everything: asked.
-- Sixteen rows of the twenty the pattern reaches.
EXPLAIN (costs off)
MATCH (s:nd)-[r1:twin]-(m:nd)-[r2]-(x:nd) RETURN count(*);
MATCH (s:nd)-[r1:twin]-(m:nd)-[r2]-(x:nd) RETURN count(*);

-- A label and a label inheriting from it: the parent's set holds the child, so
-- the two meet and the question is asked.  Two rows, not four: `:kin' reaches
-- the child's relationship too, and the pair where both reach it is the pair the
-- question refuses.
EXPLAIN (costs off)
MATCH (s:nd)-[r1:kin]-(m:nd)-[r2:kid]-(x:nd)
RETURN s.id AS s, r1.p AS r1, m.id AS m, r2.p AS r2, x.id AS x;
MATCH (s:nd)-[r1:kin]-(m:nd)-[r2:kid]-(x:nd)
RETURN s.id AS s, r1.p AS r1, m.id AS m, r2.p AS r2, x.id AS x
       ORDER BY s, r1, m, r2, x;

-- ONLY on the parent narrows its set to the one label, which the child's set no
-- longer meets: the same pattern, and the question is gone.
EXPLAIN (costs off)
MATCH (s:nd)-[r1:kin ONLY]-(m:nd)-[r2:kid]-(x:nd)
RETURN s.id AS s, r1.p AS r1, m.id AS m, r2.p AS r2, x.id AS x;
MATCH (s:nd)-[r1:kin ONLY]-(m:nd)-[r2:kid]-(x:nd)
RETURN s.id AS s, r1.p AS r1, m.id AS m, r2.p AS r2, x.id AS x
       ORDER BY s, r1, m, r2, x;

-- A relationship carried in from an earlier clause arrives as an id and nothing
-- else, so its label is every label and the question is asked -- even against a
-- relationship whose label it cannot share.  Written as one clause, where the
-- pattern still knows both labels, the same question is gone and the plan is
-- otherwise the same plan.
EXPLAIN (costs off)
MATCH (a:nd)-[r:twin]->(b:nd) WITH a, r, b
MATCH (a)-[r]->(b)-[r2:other]->(c:nd) RETURN r.p AS r, r2.p AS r2;
MATCH (a:nd)-[r:twin]->(b:nd) WITH a, r, b
MATCH (a)-[r]->(b)-[r2:other]->(c:nd) RETURN r.p AS r, r2.p AS r2
       ORDER BY r, r2;
EXPLAIN (costs off)
MATCH (a:nd)-[r:twin]->(b:nd)-[r2:other]->(c:nd) RETURN r.p AS r, r2.p AS r2;
MATCH (a:nd)-[r:twin]->(b:nd)-[r2:other]->(c:nd) RETURN r.p AS r, r2.p AS r2
       ORDER BY r, r2;

-- Three relationships whose labels meet nowhere: all three questions gone.
EXPLAIN (costs off)
MATCH (s:nd)-[r1:twin]-(m:nd)-[r2:other]-(x:nd)-[r3:kin ONLY]-(y:nd)
RETURN count(*);
MATCH (s:nd)-[r1:twin]-(m:nd)-[r2:other]-(x:nd)-[r3:kin ONLY]-(y:nd)
RETURN count(*);

-- Where a relationship label is named once in a pattern, the question that has
-- gone was the only reader of that relationship's id, and the scan that fetched
-- it stops reading the table: an index-only scan where it was an index scan and a
-- heap fetch.  The two plans below differ in the label of one relationship, and
-- so in that and in nothing else.  Sequential and bitmap scans are turned off so
-- that both plans reach the relationships the same way and the difference left is
-- whether the index answers alone.
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN (costs off)
MATCH (s:nd)-[r1:twin]->(m:nd)-[r2:other]->(x:nd) RETURN s.id AS s, x.id AS x;
EXPLAIN (costs off)
MATCH (s:nd)-[r1:twin]->(m:nd)-[r2:twin]->(x:nd) RETURN s.id AS s, x.id AS x;
RESET enable_seqscan;
RESET enable_bitmapscan;

MATCH (s:nd)-[r1:twin]->(m:nd)-[r2:other]->(x:nd)
RETURN s.id AS s, r1.p AS r1, r2.p AS r2, x.id AS x ORDER BY s, r1, r2, x;
MATCH (s:nd)-[r1:twin]->(m:nd)-[r2:twin]->(x:nd)
RETURN s.id AS s, r1.p AS r1, r2.p AS r2, x.id AS x ORDER BY s, r1, r2, x;

DROP GRAPH ru CASCADE;

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

-- A SET that ends the statement is not eager, so it takes a different path from
-- the cases above, which all carry a RETURN.  It still records what it wrote, so
-- that a second write to the same element is recognised.  Kept on its own label
-- so it does not disturb the rows the cases below read.
CREATE (:trailing_set {no:1}), (:trailing_set {no:1});

MATCH (a:trailing_set)
SET a.no = 20;

MATCH (a:trailing_set)
RETURN a.no;

-- two variables binding the same element: the second write is refused
MATCH (a:trailing_set), (b:trailing_set)
SET a.no = 30, b.no = 40;

MATCH (a:trailing_set)
RETURN a.no;

MATCH (a:trailing_set) DETACH DELETE (a);
DROP VLABEL trailing_set;

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

-- count(v) / count(DISTINCT v) over a whole vertex count the graphid, not the
-- ROW(id, properties, ctid) composite: the scan need not output properties, and
-- DISTINCT de-duplicates on the 8-byte id.  Same value, cheaper.
EXPLAIN (VERBOSE, COSTS OFF) MATCH (a) RETURN count(DISTINCT a);
MATCH (a) RETURN count(DISTINCT a);

ALTER DATABASE regression SET GRAPH_PATH TO DEFAULT;
DROP GRAPH ag161 CASCADE;

-- count() over a graph element counts by graphid; verify it holds for edges and,
-- crucially, that count() skips a NULL (unmatched OPTIONAL MATCH) element exactly
-- as count(*) would -- the id is NULL precisely when the element is.
CREATE GRAPH ag_count;
SET graph_path = ag_count;
CREATE (:person {name:'a'})-[:knows]->(:person {name:'b'});
CREATE (:person {name:'c'});
-- one knows edge: count(r) and count(DISTINCT r) are both 1
MATCH ()-[r:knows]->() RETURN count(r) AS cr, count(DISTINCT r) AS cdr;
-- only 'a' has an outgoing :knows, so count(t) is 0 for b and c (t is NULL there)
MATCH (p:person) OPTIONAL MATCH (p)-[:knows]->(t)
  RETURN p.name AS p, count(t) AS c ORDER BY p;
SET graph_path = DEFAULT;
DROP GRAPH ag_count CASCADE;

-- ORDER BY over an integer/numeric aggregate sorts the native value rather than
-- cypher_to_jsonb(agg): identical order (jsonb number order matches numeric
-- order, and strict cypher_to_jsonb keeps a NULL aggregate's nulls_first
-- placement), but the sort compares fixed-width numbers instead of jsonb.
CREATE GRAPH ag_pd;
SET graph_path = ag_pd;
CREATE (:p {g:'a', v:10}); CREATE (:p {g:'a', v:20});
CREATE (:p {g:'b', v:5}); CREATE (:p {g:'c'});
-- the sort key is the native count(*), not cypher_to_jsonb(count(*))
EXPLAIN (COSTS OFF) MATCH (n:p) RETURN n.g AS g, count(*) AS c ORDER BY c DESC;
MATCH (n:p) RETURN n.g AS g, count(*) AS c ORDER BY c DESC, g;
-- a NULL aggregate (avg over the value-less group 'c') keeps its position:
-- last ascending, first descending
MATCH (n:p) RETURN n.g AS g, avg(n.v) AS a ORDER BY a DESC, g;
MATCH (n:p) RETURN n.g AS g, avg(n.v) AS a ORDER BY a ASC, g;
SET graph_path = DEFAULT;
DROP GRAPH ag_pd CASCADE;

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
-- AGV2-547
--
-- A list written out holds what it is given.  A node, a relationship or a path
-- in one stays that, the way it does in a list a function returns, because the
-- jsonb such an element casts to keeps only its properties.
CREATE GRAPH agv2_547;
SET graph_path = agv2_547;

CREATE (:person {name: 'Alice'})-[:knows {since: 2020}]->(:person {name: 'Bob'});
CREATE (:person {name: 'Carol'})-[:likes]->(:person {name: 'Dave'});

-- the quantifiers over a list holding a relationship
MATCH (:person)-[r:knows]->(:person) RETURN all(x IN [r] WHERE x.since >= 2020) AS ok;
MATCH (:person)-[r:knows]->(:person) RETURN any(x IN [r] WHERE x.since >= 2020) AS ok;
MATCH (:person)-[r:knows]->(:person) RETURN none(x IN [r] WHERE x.since >= 2020) AS ok;
MATCH (:person)-[r:knows]->(:person) RETURN single(x IN [r] WHERE x.since >= 2020) AS ok;
-- a relationship with no such property leaves the condition unknown
MATCH (:person)-[r:likes]->(:person) RETURN all(x IN [r] WHERE x.since >= 2020) IS NULL AS unknown;

-- the elements themselves
MATCH ()-[r:knows]->() RETURN [r] AS l;
MATCH (n:person {name: 'Alice'}) RETURN [n] AS l;
MATCH (n:person {name: 'Alice'}), (m:person {name: 'Bob'}) RETURN [n, m] AS l;
MATCH p=(:person)-[:knows]->(:person) RETURN [p] AS l;

-- and what can be done with such a list
MATCH ()-[r:knows]->() RETURN [r][0].since AS since, size([r]) AS n;
MATCH (n:person {name: 'Alice'}) UNWIND [n] AS x RETURN x.name AS name;
MATCH (n:person {name: 'Alice'}), (m:person {name: 'Bob'}) RETURN n IN [n, m] AS found;

-- a list of paths is iterated as paths, not as a picture of them
MATCH p=(:person)-[]->(:person) RETURN [x IN collect(p) | length(x)] AS lens;
MATCH p=(:person)-[]->(:person) WITH collect(p) AS ps RETURN ps[0] AS first;

-- a list of anything else is unchanged
MATCH ()-[r:knows]->() RETURN [r.since] AS l;
RETURN [1, 2, 3] AS l, [] AS empty;
RETURN all(x IN [1, 2, 3] WHERE x > 0) AS ok;
-- a node and a number have no common list to share
MATCH (n:person {name: 'Alice'}) RETURN [1, n] AS l;

-- a property holds a value, so it takes neither an element nor a list of them
MATCH (n:person {name: 'Alice'}) SET n.p = [n];
MATCH p=(:person)-[]->(:person) WITH p LIMIT 1
	MATCH (n:person {name: 'Alice'}) SET n.p = p;
MATCH (n:person) WITH collect(n) AS ns
	MATCH (m:person {name: 'Alice'}) SET m.p = ns;
-- naming an element on its own still assigns the property map it carries
MATCH (n:person {name: 'Alice'}), (m:person {name: 'Bob'}) SET n.copy = m
	RETURN n.copy AS copy;

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
-- ORDER BY name resolution
--
-- A sort key resolves a name against two scopes: the variables of the clause the
-- projection reads (a pattern variable, whether or not the projection outputs
-- it) and the projection's own output items (a RETURN/WITH alias).  Both must
-- work wherever the name appears -- as the whole key and inside a larger
-- expression -- and both must work in the same key, so every combination of the
-- two is covered below.
--
-- The namespace is searched first, so a pattern variable outranks an item of
-- the same name; a list comprehension's iteration variable outranks both.  An
-- item is named at its own query level only, so a subquery inside a key
-- resolves its own names, not the items.
--
CREATE GRAPH sortkeys;
SET graph_path = sortkeys;

CREATE VLABEL person;
CREATE ELABEL knows;

-- Two persons share an age, so a single key never fully orders the rows and
-- every query below needs a tiebreaker; one person has no name, which makes
-- NULL placement observable.  Names are lowercase ASCII with distinct first
-- letters, so any collation orders them identically.
CREATE (:person {id: 1, name: 'a', age: 30, grp: 1}),
       (:person {id: 2, name: 'b', age: 20, grp: 2}),
       (:person {id: 3, name: 'c', age: 30, grp: 1}),
       (:person {id: 4, name: 'd', age: 10, grp: 2}),
       (:person {id: 5, age: 40, grp: 1});

-- A chain 1 -> 2 -> 3 -> 4 for paths, VLE, shortestpath and future vertices.
MATCH (p:person {id: 1}), (q:person {id: 2}) CREATE (p)-[:knows {w: 1}]->(q);
MATCH (p:person {id: 2}), (q:person {id: 3}) CREATE (p)-[:knows {w: 2}]->(q);
MATCH (p:person {id: 3}), (q:person {id: 4}) CREATE (p)-[:knows {w: 3}]->(q);

-- 1. the two scopes, alone and together

-- an item as the whole key
MATCH (p:person) RETURN p.name AS name ORDER BY name;
-- a variable the projection does not output, as the whole key
MATCH (p:person) RETURN p.name AS name ORDER BY p.id DESC;
-- an item inside a larger key
MATCH (p:person) RETURN p.id AS x ORDER BY x * -1;
-- a variable the projection does not output, inside a larger key
MATCH (p:person) RETURN p.name AS name ORDER BY p.age * -1, p.id;
-- both scopes in one key
MATCH (p:person) RETURN p.id AS x ORDER BY x + p.age, x;
-- both scopes across two keys
MATCH (p:person) RETURN p.id AS x ORDER BY p.grp, x * -1;

-- 2. how an item is named

-- by its AS alias, or by the name figured for it when it has none
MATCH (p:person) RETURN p.id AS x ORDER BY x % 2, x DESC;
MATCH (p:person) RETURN p.id ORDER BY id * -1;
-- the same item twice in one key, and in two keys
MATCH (p:person) RETURN p.id AS x ORDER BY x * x DESC, x;
MATCH (p:person) RETURN p.id AS x ORDER BY x % 2, x * -1;
-- two different items in one key
MATCH (p:person) RETURN p.id AS x, p.age AS y ORDER BY x + y DESC, x;
-- an item of one projection and an item of the next
MATCH (p:person) WITH p.id AS x, p.age AS y ORDER BY y DESC, x
    RETURN x + y AS sum ORDER BY sum DESC;

-- 3. the expression shapes a key may be

MATCH (p:person) RETURN p.age AS age, p.id AS id ORDER BY abs(age - 25), id;
MATCH (p:person) RETURN p.name AS name ORDER BY toUpper(name) DESC;
MATCH (p:person) RETURN p.name AS name ORDER BY name + 'z' DESC;
MATCH (p:person) RETURN p.id AS x ORDER BY toString(x) DESC;
MATCH (p:person) RETURN p.id AS x ORDER BY CASE WHEN x > 2 THEN 0 ELSE 1 END, x;
MATCH (p:person) RETURN p.id AS x ORDER BY x + 1 = 2, x;
MATCH (p:person) RETURN p.id AS x ORDER BY x IN [2, 4], x;
MATCH (p:person) RETURN p.name AS name, p.id AS id
    ORDER BY coalesce(name, 'z'), id;

-- a parameter beside an item name
PREPARE sortkeys_dir(jsonb) AS
MATCH (p:person) RETURN p.id AS x ORDER BY x * $1, x;
EXECUTE sortkeys_dir('1');
EXECUTE sortkeys_dir('-1');
DEALLOCATE sortkeys_dir;

-- 4. WITH projections, including the WITH ... WHERE form

MATCH (p:person) WITH p.id AS x ORDER BY x * -1 RETURN x;
MATCH (p:person) WITH p.id AS x, p.grp AS grp ORDER BY grp DESC, x * -1
    RETURN x, grp;
MATCH (p:person) WITH p.name AS name ORDER BY p.id DESC RETURN name;
-- a WITH that has a WHERE is wrapped, so its ORDER BY is transformed inside
-- the wrap; a key there resolves the same two scopes
MATCH (p:person) WITH p.id AS x WHERE x > 1 ORDER BY x * -1 RETURN x;
MATCH (p:person) WITH p.name AS name WHERE name IS NOT NULL
    ORDER BY toUpper(name) DESC RETURN name;
MATCH (p:person) WITH p.name AS name WHERE name IS NOT NULL ORDER BY p.id DESC
    RETURN name;
-- Paging beside a WITH ... WHERE pages what the filter passed on, in the clause
-- order WHERE, then ORDER BY, then SKIP/LIMIT: the two smallest ids above 1,
-- not the two smallest of all.  A sort key that reads a variable the projection
-- does not pass on keeps working there.
MATCH (p:person) WITH p.id AS x WHERE x > 1 ORDER BY x LIMIT 2 RETURN x;
MATCH (p:person) WITH p.id AS x WHERE x > 1 ORDER BY x SKIP 1 RETURN x;
MATCH (p:person) WITH p.id AS x WHERE x > 1 ORDER BY x SKIP 1 LIMIT 1 RETURN x;
MATCH (p:person) WITH p.id AS x WHERE x > 1 ORDER BY x * -1 LIMIT 2 RETURN x;
MATCH (p:person) WITH p.id AS x WHERE x > 1 ORDER BY p.name DESC LIMIT 2
    RETURN x;
MATCH (p:person) WITH DISTINCT p.age AS age WHERE age > 20 ORDER BY age LIMIT 1
    RETURN age;

-- 5. an item that is a whole vertex, edge or path

-- a field after the item's name reads a property of that value
MATCH (p:person) WITH p AS v ORDER BY v.id DESC LIMIT 2 RETURN v.id AS id;
MATCH (p:person) WITH p AS v ORDER BY v.age, v.id RETURN v.id AS id;
MATCH (p:person) WITH p AS v ORDER BY v.name NULLS FIRST, v.id
    RETURN v.name AS name;
MATCH (p:person) WITH p AS v ORDER BY properties(v).id DESC RETURN v.id AS id;
MATCH (p:person) RETURN p AS v ORDER BY v.id DESC LIMIT 2;
MATCH (a:person)-[e:knows]->(b:person) WITH e AS ee ORDER BY ee.w DESC
    RETURN ee.w AS w;
MATCH (a:person)-[e:knows]->(b:person) WITH e AS ee, a.id AS aid
    ORDER BY ee.w * -1, aid RETURN aid, ee.w AS w;
MATCH pth = (a:person)-[:knows]->(b:person) WITH pth AS pp, a.id AS aid
    ORDER BY length(pp), aid DESC RETURN aid, length(pp) AS len;

-- the right endpoint of a pattern is a future vertex when the key is
-- transformed, so the copy in the key has to be resolved along with the item
MATCH (n:person)-[e:knows]->(m:person) WITH m AS mm ORDER BY mm.id
    RETURN mm.id AS id;
MATCH (n:person)-[e:knows]->(m:person) WITH m AS mm ORDER BY mm.id * -1
    RETURN mm.id AS id;
MATCH (n:person)-[e:knows]->(m:person) WITH n AS a1, m AS a2
    ORDER BY a1.id DESC, a2.id RETURN a1.id AS nid, a2.id AS mid;
MATCH (n:person)-[e:knows]->(m:person) RETURN m.id AS mid ORDER BY mid * -1;

-- 6. aggregates and the implicit GROUP BY

MATCH (p:person) RETURN p.grp AS grp, count(*) AS cnt ORDER BY cnt DESC, grp;
MATCH (p:person) RETURN p.grp AS grp, count(*) AS cnt ORDER BY cnt * -1, grp;
MATCH (p:person) RETURN p.grp AS grp, count(*) AS cnt ORDER BY count(*) DESC, grp;
MATCH (p:person) RETURN p.grp AS grp, max(p.age) AS mx ORDER BY mx + 0, grp;
MATCH (p:person) RETURN p.grp AS grp, avg(p.age) AS av ORDER BY av DESC, grp;
MATCH (p:person) RETURN p.grp AS grp, collect(p.id) AS ids
    ORDER BY jsonb_array_length(ids) DESC, grp;
-- naming a grouping item in a key does not turn the key into a second group key
MATCH (p:person) RETURN p.grp AS grp, count(*) AS cnt ORDER BY grp * -1;

-- 7. DISTINCT

MATCH (p:person) RETURN DISTINCT p.grp AS grp ORDER BY grp;
MATCH (p:person) RETURN DISTINCT p.grp AS grp ORDER BY p.grp;
MATCH (p:person) WITH DISTINCT p.grp AS grp ORDER BY grp DESC RETURN grp;
-- with DISTINCT a key must be an output item itself, as in SQL: an expression
-- over one, and a variable that is not output, are both rejected
MATCH (p:person) RETURN DISTINCT p.grp AS grp ORDER BY grp * -1;
MATCH (p:person) RETURN DISTINCT p.grp AS grp ORDER BY p.id;

-- 8. paging, sort direction and NULL placement

MATCH (p:person) RETURN p.id AS x ORDER BY x * -1 SKIP 1 LIMIT 2;
MATCH (p:person) RETURN p.id AS x ORDER BY x * -1 OFFSET 1;
MATCH (p:person) RETURN p.id AS x ORDER BY x * -1 LIMIT 0;
MATCH (p:person) RETURN p.id AS x ORDER BY x * -1 ASC;
MATCH (p:person) RETURN p.id AS x ORDER BY x * -1 DESC;
MATCH (p:person) RETURN p.name AS name ORDER BY name NULLS FIRST;
MATCH (p:person) RETURN p.name AS name ORDER BY name DESC NULLS LAST;
MATCH (p:person) RETURN p.name AS name, p.id AS id
    ORDER BY toUpper(name) NULLS FIRST, id;
MATCH (p:person) RETURN p.name AS name, p.id AS id
    ORDER BY toUpper(name) DESC NULLS LAST, id;

-- 9. OPTIONAL MATCH, VLE and shortestpath

-- an item bound by an OPTIONAL MATCH is null for the rows that did not match
MATCH (p:person) OPTIONAL MATCH (p)-[:knows]->(q:person)
    WITH p.id AS id, q AS qq ORDER BY qq.id NULLS FIRST, id
    RETURN id, qq.id AS qid;
MATCH (p:person) OPTIONAL MATCH (p)-[:knows]->(q:person)
    RETURN p.id AS id, q.id AS qid ORDER BY qid * -1 NULLS LAST, id;
MATCH pth = (a:person {id: 1})-[:knows*1..3]->(b:person)
    WITH pth AS pp, b.id AS bid ORDER BY length(pp) DESC, bid
    RETURN bid, length(pp) AS len;
MATCH pth = (a:person {id: 1})-[:knows*1..3]->(b:person)
    RETURN b.id AS bid, length(pth) AS len ORDER BY len * -1, bid;
MATCH pth = shortestpath((a:person {id: 1})-[:knows*]->(b:person {id: 4}))
    RETURN length(pth) AS len ORDER BY len * -1;

-- 10. standalone modifier clauses

-- A standalone ORDER BY passes the previous clause's variables through, so the
-- names it can use come from the namespace whichever shape the key has.
MATCH (p:person) ORDER BY p.id DESC LIMIT 3 WITH p RETURN p.id AS id
    ORDER BY id * -1;
MATCH (p:person) ORDER BY p.id * -1 LIMIT 2 WITH p RETURN p.id AS id ORDER BY id;
UNWIND [3, 1, 2] AS x ORDER BY x * -1 RETURN x;
UNWIND [3, 1, 2] AS x RETURN x AS y ORDER BY y * -1;
MATCH (p:person) UNWIND [1, 2] AS k ORDER BY k * -1, p.id
    RETURN p.id AS id, k;
MATCH (p:person) FILTER p.id > 1 ORDER BY p.id * -1 RETURN p.id AS id;

-- 11. precedence

-- A pattern variable outranks an item of the same name, whether the key is the
-- name alone or an expression over it: "p" in these keys is the vertex, so the
-- names come out ordered by id and not by themselves.
MATCH (p:person) WITH p.name AS p ORDER BY p.id DESC RETURN p;
MATCH (p:person) WITH p.name AS p ORDER BY p.id * -1 RETURN p;
-- A list comprehension's iteration variable outranks an item of its own name.
-- With "k" the item is reached, so the list differs per row and orders by i
-- descending; with "i" the iteration variable hides the item, so the list is
-- the same for every row and the order comes from the second key.
MATCH (p:person) RETURN p.id AS i ORDER BY [k IN [1, 2, 3] | i * -1], i;
MATCH (p:person) RETURN p.id AS i ORDER BY [i IN [1, 2, 3] | i * -1], i;
-- the item and the iteration variable in one comprehension body
MATCH (p:person) RETURN p.id AS i ORDER BY [k IN [1, 2] | k + i], i;
MATCH (p:person) RETURN p.id AS i ORDER BY ANY(k IN [1, 2] WHERE k = i), i;

-- 12. a subquery inside a key is its own query level

-- A Cypher subquery in a key resolves its own names against the enclosing
-- namespace, so a pattern variable reaches it -- but an item does not, because
-- an item exists only as a target entry of this projection.
MATCH (p:person) RETURN p.id AS x
    ORDER BY EXISTS { MATCH (b:person) WHERE b.id > p.id }, x;
MATCH (p:person) RETURN p.id AS x
    ORDER BY EXISTS { MATCH (b:person) WHERE b.id = x }, x;
-- A CALL subquery's output is a variable of the pipeline, not an item, so it
-- resolves through the namespace like any other variable.
MATCH (p:person) CALL { MATCH (q:person) RETURN count(*) AS c }
    RETURN p.id AS x, c AS cc ORDER BY cc, x * -1;
-- A SQL sublink written in a key has its own query level, and an item, which
-- lives at the level of the projection that outputs it, is not a column there --
-- so a sublink cannot read one.  The name to reach for inside a sublink is a
-- pattern variable, and even that is beyond it, as the case below shows.
MATCH (p:person) RETURN p.id AS x ORDER BY (SELECT 1 WHERE x IS NOT NULL), x;
-- (a SQL sublink cannot see a pattern variable in any context, ORDER BY or not)
MATCH (p:person) RETURN p.name AS name ORDER BY (SELECT p.id), p.id;

-- 13. an item's expression is evaluated a second time for the key

-- A key that computes with an item compiles to a COPY of the item's
-- expression, so the item is evaluated once for the value returned and once
-- for the key.  A volatile item would then be sorted by values other than the
-- ones it returns, so such a key is refused; the name alone stays available,
-- as that sorts on the item's own target entry, evaluated once.
CREATE SEQUENCE sortkeys_seq;
MATCH (p:person) WITH p.id AS x, nextval('sortkeys_seq') AS n ORDER BY n * -1
    RETURN x ORDER BY x;
MATCH (p:person) RETURN nextval('sortkeys_seq') AS n ORDER BY abs(n);
SELECT setval('sortkeys_seq', 1, false);
MATCH (p:person) WITH p.id AS x, nextval('sortkeys_seq') AS n ORDER BY n DESC
    RETURN x ORDER BY x;
SELECT currval('sortkeys_seq') AS evaluations;
DROP SEQUENCE sortkeys_seq;

-- A stable item is evaluated twice as well, but both evaluations agree, so the
-- key is exact: the statement timestamp is one value for every row, which
-- leaves the second key to decide the order (the timestamp itself is not
-- returned, as it differs from run to run).
MATCH (p:person) WITH p.id AS x, now() AS t ORDER BY t, x RETURN x;

-- A set-returning item expands in step with the key built from it, so each row
-- is ordered by its own value -- the same result the key written out gives.
RETURN generate_series(1, 4) AS s ORDER BY s * -1;
RETURN generate_series(1, 4) AS s ORDER BY generate_series(1, 4) * -1;

-- 14. plan shape

-- Naming an item costs nothing: the key becomes the item's own expression, so
-- the plan is the one the qualified spelling gives.
EXPLAIN (COSTS OFF)
MATCH (p:person) RETURN p.id AS x, p.name AS name ORDER BY x * -1, name;
EXPLAIN (COSTS OFF)
MATCH (p:person) RETURN p.id AS x, p.name AS name ORDER BY p.id * -1, p.name;
-- A key that is the name alone reuses the item's entry, adding no column; a
-- key that is an expression over it adds one resjunk column and no more.
EXPLAIN (VERBOSE, COSTS OFF)
MATCH (p:person) RETURN p.id AS x ORDER BY x DESC;
EXPLAIN (VERBOSE, COSTS OFF)
MATCH (p:person) RETURN p.id AS x ORDER BY x * -1;
-- A promoted property reached through an item name keeps its typed column and
-- its index; cypher_typed_column.sql covers that in sections 2 and 5.  A vector
-- distance key (ORDER BY embedding <=> $1) needs pgvector, which a plain
-- `make check` temporary install does not have.

-- 15. errors

-- a name that is neither a variable nor an item
MATCH (p:person) RETURN p.id AS x ORDER BY nosuch;
MATCH (p:person) RETURN p.id AS x ORDER BY nosuch * -1;
-- a key never resolves to another key: the entry a key appends is unnamed
MATCH (p:person) RETURN p.name AS name ORDER BY p.id, id;
-- a field on an item that holds a scalar fails the same way it does elsewhere
MATCH (p:person) RETURN p.id AS x ORDER BY x.foo, x;
-- A RETURN may carry one name twice, which is fine until a key reads it: which
-- of the two to sort by would be a guess, so both spellings of the key say so.
MATCH (p:person) RETURN p.id AS x, p.age AS x;
MATCH (p:person) RETURN p.id AS x, p.age AS x ORDER BY x;
MATCH (p:person) RETURN p.id AS x, p.age AS x ORDER BY x * -1;
-- A WITH names everything it passes on and a later clause reads it by name, so
-- there the collision itself is refused -- including one made by expanding "*".
MATCH (p:person) WITH p.id AS x, p.age AS x ORDER BY x * -1 RETURN x;
MATCH (p:person) WITH *, p.id AS p RETURN p;

DROP GRAPH sortkeys CASCADE;
SET graph_path = ag324;

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

-- == FOR over a null or scalar source ==
-- a null source is an empty list: zero rows, with and without an offset,
-- as a leading clause and joined with a working table
FOR x in NULL RETURN x;
FOR x in NULL WITH OFFSET AS o RETURN x, o;
MATCH (p:Person) FOR x in NULL RETURN p.name, x;
FOR x in (null) RETURN x;
-- a null property answers the same way
MATCH (p:Person {Id: 1}) FOR x in p.no_such_key RETURN x;
-- a string literal is a scalar, like a stored string
FOR x in 'abc' RETURN x;

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

--
-- COLLECT subquery -- every value the single-column read subquery yields,
-- gathered into a cypher list (a jsonb array): order-preserving, NULL-preserving,
-- and [] (never NULL) when empty.  Reuses the EXISTS-subquery graph
-- (Person/Dog/Cat/Toy): Andy -> [Dog Andy], Peter -> [Dog Fido, Dog Ozzy],
-- Timothy -> [] (Timothy -> [Cat Mittens]); Fido -> [Toy Banana].
--

-- correlated COLLECT in RETURN (one list per person; [] for Timothy)
MATCH (person:Person)
RETURN person.name AS name,
       COLLECT { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS dogs
ORDER BY name;

-- uncorrelated COLLECT (all dog names)
RETURN COLLECT { MATCH (d:Dog) RETURN d.name ORDER BY d.name } AS dogs;

-- empty subquery yields [] (never NULL)
RETURN COLLECT { MATCH (x:NoSuchLabel) RETURN x.name } AS empty;

-- a correlated empty list is [] too (Timothy has no dog)
MATCH (person:Person {name: 'Timothy'})
RETURN COLLECT { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name } AS dogs;

-- the list order comes from the subquery: DESC
RETURN COLLECT { MATCH (d:Dog) RETURN d.name ORDER BY d.name DESC } AS dogs_desc;

-- ordering by a key other than the collected value (order by age, return name)
RETURN COLLECT { MATCH (p:Person) RETURN p.name ORDER BY p.age } AS names_by_age;

-- NULLs are preserved: a property absent on every dog -> [null, null, null]
RETURN COLLECT { MATCH (d:Dog) RETURN d.nickname ORDER BY d.name } AS nicknames;

-- a literal NULL per row is preserved
RETURN COLLECT { MATCH (d:Dog) RETURN NULL ORDER BY d.name } AS nulls;

-- OPTIONAL MATCH keeps a NULL row (contrast: plain MATCH gives [] for Timothy)
MATCH (person:Person)
RETURN person.name AS name,
       COLLECT { OPTIONAL MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS dogs
ORDER BY name;

-- collecting numbers
RETURN COLLECT { MATCH (p:Person) RETURN p.age ORDER BY p.age } AS ages;

-- collecting a computed expression
RETURN COLLECT { MATCH (p:Person) RETURN p.age * 2 ORDER BY p.age } AS doubled;

-- collecting map values
RETURN COLLECT { MATCH (p:Person) RETURN {name: p.name, age: p.age} ORDER BY p.name } AS people;

-- collecting list values (a list of lists)
RETURN COLLECT { MATCH (p:Person) RETURN [p.name, p.age] ORDER BY p.name } AS pairs;

-- collecting graph-derived maps via properties()
RETURN COLLECT { MATCH (d:Dog) RETURN properties(d) ORDER BY d.name } AS dog_props;

-- the subquery's own DISTINCT is honored
RETURN COLLECT { MATCH (:Person)-[:HAS_DOG]->(d:Dog) RETURN DISTINCT d.name ORDER BY d.name } AS distinct_dogs;

-- the subquery's own LIMIT is honored
RETURN COLLECT { MATCH (d:Dog) RETURN d.name ORDER BY d.name LIMIT 2 } AS first_two;

-- the subquery's own SKIP is honored
RETURN COLLECT { MATCH (d:Dog) RETURN d.name ORDER BY d.name SKIP 1 } AS after_first;

-- an aggregate inside the subquery (one row -> single-element list)
RETURN COLLECT { MATCH (:Person)-[:HAS_DOG]->(d:Dog) RETURN count(*) } AS total;

-- SQL SELECT form over generate_series
RETURN COLLECT { SELECT i FROM generate_series(1, 3) AS i ORDER BY i } AS nums;

-- SQL SELECT wrapping a Cypher read
RETURN COLLECT { SELECT t.name FROM (MATCH (d:Dog) RETURN d.name AS name) t ORDER BY t.name } AS dogs;

-- SQL SELECT form: DISTINCT + UNION honored (sorted for determinism)
RETURN COLLECT {
    SELECT n FROM (MATCH (d:Dog) RETURN d.name AS n
                   UNION
                   MATCH (c:Cat) RETURN c.name AS n) u
    ORDER BY n
} AS dog_or_cat;

-- membership: x IN COLLECT { ... }
MATCH (person:Person)
WHERE 'Fido' IN COLLECT { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name }
RETURN person.name AS name ORDER BY name;

-- negated membership
MATCH (person:Person)
WHERE NOT ('Fido' IN COLLECT { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name })
RETURN person.name AS name ORDER BY name;

-- ============================================================
-- graph-element membership: `x IN list` for vertex/edge/graphid.
-- Matches by identity (the id-only `=` operator), not jsonb containment.
-- ============================================================

-- vertex IN [vertex, vertex] (literal list; previously errored)
MATCH (fido:Dog {name: 'Fido'}), (ozzy:Dog {name: 'Ozzy'})
MATCH (d:Dog)
WHERE d IN [fido, ozzy]
RETURN d.name AS dog ORDER BY dog;

-- negated: everything NOT in the pair
MATCH (fido:Dog {name: 'Fido'}), (ozzy:Dog {name: 'Ozzy'})
MATCH (d:Dog)
WHERE NOT (d IN [fido, ozzy])
RETURN d.name AS dog ORDER BY dog;

-- edge IN [edge, edge] (literal list; previously errored)
MATCH ()-[a:HAS_DOG]->(:Dog {name: 'Fido'})
MATCH ()-[b:HAS_DOG]->(:Dog {name: 'Ozzy'})
MATCH ()-[e:HAS_DOG]->(d:Dog)
WHERE e IN [a, b]
RETURN d.name AS dog ORDER BY dog;

-- id(v) IN [id(v), id(v)] -- explicit graphid operands
MATCH (fido:Dog {name: 'Fido'}), (ozzy:Dog {name: 'Ozzy'})
MATCH (d:Dog)
WHERE id(d) IN [id(fido), id(ozzy)]
RETURN d.name AS dog ORDER BY dog;

-- mixed: a graphid expression and a bare numeric graphid literal (non-existent)
MATCH (fido:Dog {name: 'Fido'})
MATCH (d:Dog)
WHERE id(d) IN [id(fido), 999.999]
RETURN d.name AS dog ORDER BY dog;

-- single-element list
MATCH (fido:Dog {name: 'Fido'})
MATCH (d:Dog)
WHERE d IN [fido]
RETURN d.name AS dog;

-- empty list -> always false
MATCH (d:Dog)
WHERE d IN []
RETURN count(*) AS n;

-- v IN collect(u): dynamic RHS, membership by identity
MATCH (d:Dog) WITH collect(d) AS dogs
MATCH (fido:Dog {name: 'Fido'})
RETURN fido IN dogs AS present;

-- a non-member (a Person is not among the collected Dogs)
MATCH (d:Dog) WITH collect(d) AS dogs
MATCH (p:Person {name: 'Andy'})
RETURN p IN dogs AS present;

-- mutate-after-collect: identity membership survives a property change.
-- (jsonb `@>` containment compared the full serialization and returned false
-- here; id-only identity correctly stays true since the id is unchanged.)
MATCH (d:Dog) WITH collect(d) AS dogs
MATCH (fido:Dog {name: 'Fido'})
SET fido.tagged = true
RETURN fido IN dogs AS still_member;
-- restore the mutated vertex so later tests observe the original graph
MATCH (fido:Dog {name: 'Fido'}) REMOVE fido.tagged;

-- a graphpath is not a single id-bearing element: membership falls back to
-- jsonb containment (unchanged behavior) and still returns a boolean
MATCH q=(:Person)-[:HAS_DOG]->(:Dog)
WITH collect(q) AS paths
MATCH p=(:Person)-[:HAS_DOG]->(:Dog {name: 'Fido'})
RETURN p IN paths AS present;

-- list equality against a literal
MATCH (person:Person {name: 'Peter'})
RETURN COLLECT { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } = ['Fido', 'Ozzy'] AS eq;

-- a COLLECT subquery directly as a function argument
RETURN length(COLLECT { MATCH (d:Dog) RETURN d.name }) AS num_dogs;

-- length(), indexing, slicing and concatenation on the result (bound via WITH)
MATCH (person:Person {name: 'Peter'})
WITH COLLECT { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS dogs
RETURN length(dogs) AS n, dogs[0] AS first, dogs[1..] AS rest, dogs + ['Rex'] AS appended;

-- length() of a COLLECT in WHERE (bind via WITH first)
MATCH (person:Person)
WITH person, COLLECT { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name } AS dogs
WHERE length(dogs) > 1
RETURN person.name AS name ORDER BY name;

-- UNWIND the collected list
MATCH (person:Person {name: 'Peter'})
WITH COLLECT { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS dogs
UNWIND dogs AS dogname
RETURN dogname ORDER BY dogname;

-- in a CASE expression (bind via WITH first)
MATCH (person:Person)
WITH person, COLLECT { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name } AS dogs
RETURN person.name AS name,
       CASE WHEN length(dogs) > 1 THEN 'pack' ELSE 'few' END AS kind
ORDER BY name;

-- in a WITH ... WHERE stage (keep only people with at least one dog)
MATCH (person:Person)
WITH person.name AS name,
     COLLECT { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS dogs
WHERE length(dogs) > 0
RETURN name, dogs ORDER BY name;

-- in ORDER BY (most dogs first; bind via WITH)
MATCH (person:Person)
WITH person, COLLECT { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name } AS dogs
RETURN person.name AS name
ORDER BY length(dogs) DESC, name;

-- correlated inner WHERE referencing both outer and inner variables
MATCH (person:Person)
RETURN person.name AS name,
       COLLECT { MATCH (person)-[:HAS_DOG]->(d:Dog) WHERE d.name <> person.name
                 RETURN d.name ORDER BY d.name } AS others
ORDER BY name;

-- a multi-hop pattern (toys reachable through a dog)
RETURN COLLECT { MATCH (:Person)-[:HAS_DOG]->(:Dog)-[:HAS_TOY]->(t:Toy) RETURN t.name ORDER BY t.name } AS toys;

-- nested COLLECT inside COLLECT (a list of each person's dog-name list)
RETURN COLLECT {
    MATCH (person:Person)
    RETURN COLLECT { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name }
    ORDER BY person.name
} AS nested;

-- hybrid: COLLECT alongside COUNT and EXISTS in one projection
MATCH (person:Person)
RETURN person.name AS name,
       COUNT { MATCH (person)-[:HAS_DOG]->(:Dog) } AS num,
       EXISTS { MATCH (person)-[:HAS_DOG]->(:Dog) } AS has_dog,
       COLLECT { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS names
ORDER BY name;

-- hybrid: gate with EXISTS, gather with COLLECT
MATCH (person:Person)
WHERE EXISTS { MATCH (person)-[:HAS_DOG]->(:Dog) }
RETURN person.name AS name,
       COLLECT { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS names
ORDER BY name;

-- COLLECT list length agrees with COUNT (bind via WITH)
MATCH (person:Person)
WITH person, COLLECT { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name } AS dogs
WHERE length(dogs) = COUNT { MATCH (person)-[:HAS_DOG]->(:Dog) }
RETURN person.name AS name ORDER BY name;

-- "collect" is unreserved, so it still works as a variable name
RETURN COLLECT { MATCH (collect:Dog) RETURN collect.name ORDER BY collect.name } AS dogs;

-- the collect() aggregate still works alongside COLLECT { } (ordered for determinism)
MATCH (d:Dog) WITH d ORDER BY d.name RETURN collect(d.name) AS names;

-- a LET/WITH-bound text literal inside a value subquery.  The projection casts
-- the value to jsonb, so the subquery's result column is (correctly) uncollated.
-- On an assert-enabled build these used to abort ("collation ... does not match
-- ...") because the group-collation repair stamped the text collation onto the
-- jsonb result even when it was not a grouping key.
RETURN COLLECT { LET tag = 'x' RETURN tag } AS tags;
RETURN VALUE { LET v = 'y' RETURN v } AS val;
RETURN COLLECT { MATCH (p:Person) WITH 'lit' AS w RETURN w ORDER BY w LIMIT 1 } AS ws;
MATCH (person:Person)
RETURN person.name AS name, VALUE { LET t = 'x' RETURN t } AS t
ORDER BY name;

--
-- SQL + Cypher hybrid -- COLLECT returns jsonb, so it bridges the two
-- languages: a Cypher subquery's COLLECT can be projected/filtered by an
-- enclosing SQL query, and COLLECT can wrap a SQL SELECT over Cypher rows.
--

-- a SQL SELECT projecting a Cypher query that builds lists with COLLECT
SELECT t.name, t.dogs
FROM (MATCH (p:Person)
      RETURN p.name AS name,
             COLLECT { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS dogs) t
ORDER BY t.name;

-- a SQL jsonb function consuming a Cypher COLLECT result
SELECT jsonb_array_length(t.dogs) AS n
FROM (MATCH (p:Person {name: 'Peter'})
      RETURN COLLECT { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name } AS dogs) t;

-- SQL unnesting a Cypher COLLECT list with jsonb_array_elements
SELECT v AS dog
FROM (MATCH (p:Person {name: 'Peter'})
      RETURN COLLECT { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS dogs) t,
     jsonb_array_elements(t.dogs) AS v
ORDER BY v;

-- a SQL WHERE filtering rows by a Cypher COLLECT result
SELECT t.name
FROM (MATCH (p:Person)
      RETURN p.name AS name,
             COLLECT { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name } AS dogs) t
WHERE jsonb_array_length(t.dogs) > 0
ORDER BY t.name;

-- COLLECT over a SQL SELECT that filters Cypher-produced rows (a Cypher value
-- is jsonb, so the SQL-side comparison uses a jsonb literal)
RETURN COLLECT {
    SELECT t.name FROM (MATCH (:Person)-[:HAS_DOG]->(d:Dog) RETURN d.name AS name) t
    WHERE t.name <> '"Andy"'::jsonb
    ORDER BY t.name
} AS non_andy_dogs;

-- COLLECT over a SQL aggregate computed on Cypher-produced rows
RETURN COLLECT {
    SELECT count(*) AS c FROM (MATCH (:Person)-[:HAS_DOG]->(d:Dog) RETURN d.name) t
} AS dog_count;

-- a SQL CTE wrapping a Cypher query that uses COLLECT
WITH per_person AS (
    MATCH (p:Person)
    RETURN p.name AS name,
           COLLECT { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS dogs
)
SELECT name, dogs FROM per_person WHERE jsonb_array_length(dogs) > 1 ORDER BY name;

-- as the value of a SET (graph-write context)
MATCH (person:Person {name: 'Peter'})
SET person.dogNames = COLLECT { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name }
RETURN person.dogNames AS dogNames;

-- as a property value in CREATE (graph-write context)
CREATE (s:Summary {dogs: COLLECT { MATCH (d:Dog) RETURN d.name ORDER BY d.name }})
RETURN s.dogs AS dogs;

-- error: the subquery must return exactly one column (Cypher form)
RETURN COLLECT { MATCH (p:Person)-[:HAS_DOG]->(d:Dog) RETURN p.name, d.name };

-- error: the subquery must return exactly one column (SQL form)
RETURN COLLECT { SELECT 1, 2 };

-- error: writes are not allowed inside a COLLECT subquery
RETURN COLLECT { CREATE (:Foo) RETURN 1 };
RETURN COLLECT { MATCH (a:Person) SET a.x = 1 RETURN a.x };
RETURN COLLECT { MATCH (a:Person) DETACH DELETE a RETURN a };

--
-- VALUE subquery -- the single value a one-column read subquery yields.
-- EXPR_SUBLINK semantics: one row -> that value, zero rows -> NULL, more than
-- one row -> runtime error; exactly one column required.  The result keeps the
-- produced column's type (no jsonb cast).  Reuses the EXISTS-subquery graph:
-- Andy(age 36)->[Dog Andy], Peter(35)->[Fido,Ozzy], Timothy(25)->[] (->[Cat]).
--

-- core: a scalar from a uniquely-matched node
RETURN VALUE { MATCH (p:Person {name: 'Andy'}) RETURN p.age } AS age;
RETURN VALUE { MATCH (p:Person {name: 'Andy'}) RETURN p.name } AS name;

-- zero rows -> NULL
RETURN VALUE { MATCH (x:NoSuchLabel) RETURN x.name } AS missing;

-- a single row selected with ORDER BY ... LIMIT 1
RETURN VALUE { MATCH (d:Dog) RETURN d.name ORDER BY d.name LIMIT 1 } AS first_dog;

-- correlated scalar aggregate (one value per person)
MATCH (p:Person)
RETURN p.name AS name, VALUE { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN count(d) } AS num_dogs
ORDER BY name;

-- correlated single value with ORDER BY ... LIMIT 1 (NULL when none)
MATCH (p:Person)
RETURN p.name AS name,
       VALUE { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name LIMIT 1 } AS a_dog
ORDER BY name;

-- comparison and arithmetic
RETURN 36 = VALUE { MATCH (p:Person {name: 'Andy'}) RETURN p.age } AS eq;
RETURN VALUE { MATCH (p:Person {name: 'Andy'}) RETURN p.age } + 1 AS next_age;

-- in WHERE (people as old as Andy)
MATCH (p:Person)
WHERE p.age = VALUE { MATCH (q:Person {name: 'Andy'}) RETURN q.age }
RETURN p.name AS name ORDER BY name;

-- in a CASE expression
MATCH (p:Person)
RETURN p.name AS name,
       CASE WHEN VALUE { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN count(d) } > 1
            THEN 'many' ELSE 'few' END AS dogs
ORDER BY name;

-- in a WITH ... WHERE stage
MATCH (p:Person)
WITH p.name AS name, VALUE { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN count(d) } AS n
WHERE n > 0
RETURN name, n ORDER BY name;

-- the result keeps its type: a graph-derived map survives (no jsonb cast forced)
RETURN VALUE { MATCH (d:Dog {name: 'Fido'}) RETURN properties(d) } AS props;

-- SQL SELECT form: a literal scalar
RETURN VALUE { SELECT 42 } AS answer;

-- SQL SELECT form: an aggregate over Cypher-produced rows
RETURN VALUE { SELECT count(*) FROM (MATCH (d:Dog) RETURN d) t } AS dog_count;

-- nested: VALUE inside VALUE
RETURN VALUE { MATCH (p:Person {name: 'Andy'})
               RETURN VALUE { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN count(d) } } AS n;

-- nested: COLLECT of a per-row VALUE
RETURN COLLECT {
    MATCH (p:Person)
    RETURN VALUE { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN count(d) }
    ORDER BY p.name
} AS counts;

-- nested: VALUE returning a COLLECT list
RETURN VALUE {
    MATCH (p:Person {name: 'Peter'})
    RETURN COLLECT { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name }
} AS peters_dogs;

-- SQL + Cypher hybrid: SQL projecting a Cypher query that uses VALUE
SELECT t.name, t.n
FROM (MATCH (p:Person)
      RETURN p.name AS name, VALUE { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN count(d) } AS n) t
ORDER BY t.name;

-- SQL + Cypher hybrid: SQL WHERE on the Cypher VALUE result (a jsonb, so the
-- SQL-side comparison uses a jsonb literal)
SELECT t.name
FROM (MATCH (p:Person)
      RETURN p.name AS name, VALUE { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN count(d) } AS n) t
WHERE t.n > '1'::jsonb
ORDER BY t.name;

-- VALUE result types: boolean, list and map values all flow through
RETURN VALUE { MATCH (p:Person {name: 'Andy'}) RETURN p.age > 30 } AS is_old;
RETURN VALUE { MATCH (p:Person {name: 'Andy'}) RETURN [p.name, p.age] } AS pair;
-- property access on a map returned by VALUE (no jsonb cast forced)
RETURN (VALUE { MATCH (d:Dog {name: 'Fido'}) RETURN properties(d) }).name AS dogname;

-- VALUE as an element of a list / map literal
RETURN [VALUE { MATCH (p:Person {name: 'Andy'}) RETURN p.age },
        VALUE { MATCH (p:Person {name: 'Peter'}) RETURN p.age }] AS ages;
RETURN {oldest: VALUE { MATCH (p:Person {name: 'Andy'}) RETURN p.name }} AS m;

-- arithmetic mixing a VALUE and a COUNT subquery
MATCH (p:Person {name: 'Peter'})
RETURN VALUE { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN count(d) }
       - COUNT { (p)-[:HAS_CAT]->(:Cat) } AS dogs_minus_cats;

-- comparing two VALUE subqueries
RETURN VALUE { MATCH (p:Person {name: 'Andy'}) RETURN p.age }
       > VALUE { MATCH (p:Person {name: 'Peter'}) RETURN p.age } AS andy_older;

-- a NULL VALUE (zero rows) feeding coalesce
RETURN coalesce(VALUE { MATCH (x:NoSuchLabel) RETURN x.name }, 'none') AS v;

-- VALUE as an element of an IN list
MATCH (p:Person)
WHERE p.name IN [VALUE { MATCH (q:Person {name: 'Andy'}) RETURN q.name }]
RETURN p.name AS name ORDER BY name;

-- a correlated VALUE in ORDER BY (most dogs first)
MATCH (p:Person)
RETURN p.name AS name
ORDER BY VALUE { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN count(d) } DESC, name;

-- ORDER BY ... SKIP ... LIMIT picks a specific row
RETURN VALUE { MATCH (d:Dog) RETURN d.name ORDER BY d.name SKIP 1 LIMIT 1 } AS second_dog;

-- SQL + Cypher hybrid: a SQL CTE wrapping a Cypher query that uses VALUE
WITH per AS (
    MATCH (p:Person)
    RETURN p.name AS name, VALUE { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN count(d) } AS n
)
SELECT name, n FROM per WHERE n > '0'::jsonb ORDER BY name;

-- SQL + Cypher hybrid: VALUE wrapping a SQL aggregate (WHERE) over Cypher rows
RETURN VALUE {
    SELECT count(*) FROM (MATCH (p:Person) RETURN p.age AS age) t WHERE t.age > '30'::jsonb
} AS over_30;

-- SQL + Cypher hybrid: VALUE wrapping a SQL DISTINCT count over Cypher rows
RETURN VALUE {
    SELECT count(DISTINCT t.name)
    FROM (MATCH (:Person)-[:HAS_DOG]->(d:Dog) RETURN d.name AS name) t
} AS distinct_dogs;

-- SQL + Cypher hybrid: a SQL jsonb function on a Cypher VALUE result
SELECT jsonb_typeof(t.n) AS typ
FROM (MATCH (p:Person {name: 'Andy'})
      RETURN VALUE { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN count(d) } AS n) t;

-- as the value of a SET (graph-write context)
MATCH (p:Person {name: 'Andy'})
SET p.dogCount = VALUE { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN count(d) }
RETURN p.dogCount AS dogCount;

-- error: the subquery returns more than one row
RETURN VALUE { MATCH (d:Dog) RETURN d.name };

-- error: the subquery must return only one column
RETURN VALUE { MATCH (p:Person) RETURN p.name, p.age };

-- error: a write is not allowed inside a VALUE subquery
RETURN VALUE { CREATE (:Foo) RETURN 1 };

--
-- ARRAY subquery -- the SQL/GQL-standard spelling of COLLECT: gathers a
-- one-column read subquery's values into a cypher list (a jsonb array) with the
-- same semantics (order-preserving, NULL-preserving, empty -> []).  Reuses the
-- EXISTS-subquery graph (Person/Dog/Cat/Toy).
--

-- ARRAY is identical to COLLECT for the same subquery
RETURN ARRAY { MATCH (d:Dog) RETURN d.name ORDER BY d.name }
       = COLLECT { MATCH (d:Dog) RETURN d.name ORDER BY d.name } AS same;
MATCH (p:Person)
RETURN p.name AS name,
       ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name }
       = COLLECT { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS same
ORDER BY name;

-- correlated ARRAY in RETURN (one list per person; [] for Timothy)
MATCH (p:Person)
RETURN p.name AS name,
       ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS dogs
ORDER BY name;

-- uncorrelated; empty -> []; order DESC
RETURN ARRAY { MATCH (d:Dog) RETURN d.name ORDER BY d.name } AS all_dogs;
RETURN ARRAY { MATCH (x:NoSuchLabel) RETURN x.name } AS empty;
RETURN ARRAY { MATCH (d:Dog) RETURN d.name ORDER BY d.name DESC } AS dogs_desc;

-- NULLs are preserved
RETURN ARRAY { MATCH (d:Dog) RETURN d.nickname ORDER BY d.name } AS nicknames;

-- element types: numbers, maps, graph-derived maps
RETURN ARRAY { MATCH (p:Person) RETURN p.age ORDER BY p.age } AS ages;
RETURN ARRAY { MATCH (p:Person) RETURN {name: p.name, age: p.age} ORDER BY p.name } AS people;
RETURN ARRAY { MATCH (d:Dog) RETURN properties(d) ORDER BY d.name } AS props;

-- subquery clauses: DISTINCT, ORDER BY ... LIMIT, ... SKIP
RETURN ARRAY { MATCH (:Person)-[:HAS_DOG]->(d:Dog) RETURN DISTINCT d.name ORDER BY d.name } AS distinct_dogs;
RETURN ARRAY { MATCH (d:Dog) RETURN d.name ORDER BY d.name LIMIT 2 } AS first_two;
RETURN ARRAY { MATCH (d:Dog) RETURN d.name ORDER BY d.name SKIP 1 } AS after_first;

-- SQL SELECT forms
RETURN ARRAY { SELECT i FROM generate_series(1, 3) AS i ORDER BY i } AS nums;
RETURN ARRAY { SELECT t.name FROM (MATCH (d:Dog) RETURN d.name AS name) t ORDER BY t.name } AS dogs;

-- membership and equality
MATCH (p:Person)
WHERE 'Fido' IN ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name }
RETURN p.name AS name ORDER BY name;
MATCH (p:Person)
WHERE NOT ('Fido' IN ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name })
RETURN p.name AS name ORDER BY name;
MATCH (p:Person {name: 'Peter'})
RETURN ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } = ['Fido', 'Ozzy'] AS eq;

-- length / indexing / slicing / concatenation (bound via WITH) and UNWIND
MATCH (p:Person {name: 'Peter'})
WITH ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS dogs
RETURN length(dogs) AS n, dogs[0] AS first, dogs[1..] AS rest, dogs + ['Rex'] AS appended;
MATCH (p:Person {name: 'Peter'})
WITH ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS dogs
UNWIND dogs AS dogname
RETURN dogname ORDER BY dogname;

-- contexts: WHERE (WITH-bound length), CASE, ORDER BY
MATCH (p:Person)
WITH p, ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name } AS dogs
WHERE length(dogs) > 1
RETURN p.name AS name ORDER BY name;
MATCH (p:Person)
WITH p, ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name } AS dogs
RETURN p.name AS name, CASE WHEN length(dogs) > 1 THEN 'pack' ELSE 'few' END AS kind
ORDER BY name;
MATCH (p:Person)
WITH p, ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name } AS dogs
RETURN p.name AS name ORDER BY length(dogs) DESC, name;

-- nested ARRAY-in-ARRAY (a list of each person's dog-name list)
RETURN ARRAY {
    MATCH (p:Person)
    RETURN ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name }
    ORDER BY p.name
} AS nested;

-- ARRAY mixed with COLLECT / COUNT / EXISTS / VALUE in one projection
MATCH (p:Person)
RETURN p.name AS name,
       ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS arr,
       COUNT { MATCH (p)-[:HAS_DOG]->(:Dog) } AS cnt,
       EXISTS { MATCH (p)-[:HAS_DOG]->(:Dog) } AS has,
       VALUE { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN count(d) } AS val
ORDER BY name;

-- SQL + Cypher hybrid: SQL projecting a Cypher query that uses ARRAY
SELECT t.name, t.dogs
FROM (MATCH (p:Person)
      RETURN p.name AS name,
             ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS dogs) t
ORDER BY t.name;

-- SQL + Cypher hybrid: a SQL jsonb function consuming an ARRAY result
SELECT jsonb_array_length(t.dogs) AS n
FROM (MATCH (p:Person {name: 'Peter'})
      RETURN ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name } AS dogs) t;

-- SQL + Cypher hybrid: ARRAY wrapping a SQL SELECT over Cypher rows
RETURN ARRAY {
    SELECT t.name FROM (MATCH (:Person)-[:HAS_DOG]->(d:Dog) RETURN d.name AS name) t
    WHERE t.name <> '"Andy"'::jsonb
    ORDER BY t.name
} AS non_andy_dogs;

-- order by a key other than the collected value (by age, return name)
RETURN ARRAY { MATCH (p:Person) RETURN p.name ORDER BY p.age } AS names_by_age;

-- a computed expression
RETURN ARRAY { MATCH (p:Person) RETURN p.age * 2 ORDER BY p.age } AS doubled;

-- a list of lists
RETURN ARRAY { MATCH (p:Person) RETURN [p.name, p.age] ORDER BY p.name } AS pairs;

-- a literal NULL per row is preserved
RETURN ARRAY { MATCH (d:Dog) RETURN NULL ORDER BY d.name } AS nulls;

-- an aggregate inside the subquery (one row -> single-element list)
RETURN ARRAY { MATCH (:Person)-[:HAS_DOG]->(d:Dog) RETURN count(*) } AS total;

-- OPTIONAL MATCH keeps a NULL row (Timothy -> [null]; plain MATCH gives [])
MATCH (p:Person)
RETURN p.name AS name,
       ARRAY { OPTIONAL MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS dogs
ORDER BY name;

-- a multi-hop pattern (toys reachable through a dog)
RETURN ARRAY { MATCH (:Person)-[:HAS_DOG]->(:Dog)-[:HAS_TOY]->(t:Toy) RETURN t.name ORDER BY t.name } AS toys;

-- correlated inner WHERE referencing both outer and inner variables
MATCH (p:Person)
RETURN p.name AS name,
       ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) WHERE d.name <> p.name RETURN d.name ORDER BY d.name } AS others
ORDER BY name;

-- ARRAY as an element of a list / map literal
RETURN [ARRAY { MATCH (d:Dog) RETURN d.name ORDER BY d.name }, ['x']] AS lol;
RETURN {dogs: ARRAY { MATCH (d:Dog) RETURN d.name ORDER BY d.name }} AS m;

-- ARRAY directly as a function argument
RETURN length(ARRAY { MATCH (d:Dog) RETURN d.name }) AS n;

-- nested with VALUE: an ARRAY of a per-row VALUE, and a VALUE returning an ARRAY
RETURN ARRAY {
    MATCH (p:Person)
    RETURN VALUE { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN count(d) }
    ORDER BY p.name
} AS counts;
RETURN VALUE {
    MATCH (p:Person {name: 'Peter'})
    RETURN ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name }
} AS peters_dogs;

-- SQL + Cypher hybrid: a SQL CTE wrapping a Cypher query that uses ARRAY
WITH per AS (
    MATCH (p:Person)
    RETURN p.name AS name, ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS dogs
)
SELECT name, dogs FROM per WHERE jsonb_array_length(dogs) > 0 ORDER BY name;

-- SQL + Cypher hybrid: unnest an ARRAY result with jsonb_array_elements
SELECT v AS dog
FROM (MATCH (p:Person {name: 'Peter'})
      RETURN ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS dogs) t,
     jsonb_array_elements(t.dogs) AS v
ORDER BY v;

-- SQL + Cypher hybrid: a SQL WHERE filtering rows by an ARRAY result
SELECT t.name
FROM (MATCH (p:Person)
      RETURN p.name AS name, ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name } AS dogs) t
WHERE jsonb_array_length(t.dogs) > 1
ORDER BY t.name;

-- SQL + Cypher hybrid: ARRAY wrapping a SQL UNION over Cypher rows
RETURN ARRAY {
    SELECT n FROM (MATCH (d:Dog) RETURN d.name AS n
                   UNION
                   MATCH (c:Cat) RETURN c.name AS n) u
    ORDER BY n
} AS dog_or_cat;

-- SQL + Cypher hybrid: ARRAY wrapping a SQL aggregate over Cypher rows
RETURN ARRAY { SELECT count(*) FROM (MATCH (:Person)-[:HAS_DOG]->(d:Dog) RETURN d.name) t } AS dog_count;

-- as the value of a SET (graph-write context)
MATCH (p:Person {name: 'Peter'})
SET p.dogNamesArr = ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name }
RETURN p.dogNamesArr AS dogNamesArr;

-- as a property value in CREATE (graph-write context)
CREATE (s:ArrSummary {dogs: ARRAY { MATCH (d:Dog) RETURN d.name ORDER BY d.name }})
RETURN s.dogs AS dogs;

-- error: the subquery must return only one column
RETURN ARRAY { MATCH (p:Person)-[:HAS_DOG]->(d:Dog) RETURN p.name, d.name };

-- error: writes are not allowed inside an ARRAY subquery
RETURN ARRAY { CREATE (:Foo) RETURN 1 };
RETURN ARRAY { MATCH (a:Person) SET a.x = 1 RETURN a.x };
RETURN ARRAY { MATCH (a:Person) DETACH DELETE a RETURN a };

--
-- IN subquery -- "x IN { subquery }" is a membership predicate: true if x
-- matches a row the subquery yields, false if none, NULL under SQL three-valued
-- logic.  It is built as a native ANY_SUBLINK ("x = ANY (subquery)") so the
-- planner can use a semi-join or hashed subplan rather than materializing the
-- whole result set the way "x IN COLLECT { ... }" does.  It accepts a GQL read
-- subquery or a SQL SELECT, with or without NOT.
--

-- core membership: a value that appears, and one that does not
RETURN 'Fido' IN { MATCH (:Person)-[:HAS_DOG]->(d:Dog) RETURN d.name } AS yes;
RETURN 'Rex' IN { MATCH (:Person)-[:HAS_DOG]->(d:Dog) RETURN d.name } AS no;

-- numeric membership (the left operand is coerced to jsonb to match the jsonb
-- column a cypher RETURN produces)
RETURN 35 IN { MATCH (p:Person) RETURN p.age } AS has35;
RETURN 99 IN { MATCH (p:Person) RETURN p.age } AS has99;

-- the membership value may be a list, and the left operand may itself be an
-- expression rather than a literal
RETURN [1, 2] IN { MATCH (p:Person) RETURN [1, 2] LIMIT 1 } AS list_member;
MATCH (p:Person {name: 'Andy'})
RETURN p.age + 0 IN { MATCH (q:Person) RETURN q.age } AS yes;
RETURN toUpper('fido') IN { MATCH (d:Dog) RETURN toUpper(d.name) } AS yes;

-- whole-vertex membership (node equality)
MATCH (p:Person {name: 'Peter'})
RETURN p IN { MATCH (q:Person) RETURN q } AS present;

-- correlated to the outer row: who owns a dog named 'Fido'
MATCH (person:Person)
WHERE 'Fido' IN { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name }
RETURN person.name AS name ORDER BY name;

-- NOT IN: who does not own a dog named 'Fido'
MATCH (person:Person)
WHERE 'Fido' NOT IN { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name }
RETURN person.name AS name ORDER BY name;

-- "NOT (x IN {...})" is equivalent to "x NOT IN {...}"
MATCH (person:Person)
WHERE NOT ('Fido' IN { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name })
RETURN person.name AS name ORDER BY name;

-- NOT IN as a boolean projection
RETURN 'Mittens' NOT IN { MATCH (:Person)-[:HAS_DOG]->(d:Dog) RETURN d.name } AS t;

-- two IN tests combined with AND
MATCH (person:Person)
WHERE 'Fido' IN { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name }
  AND 'Ozzy' IN { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name }
RETURN person.name AS name;

-- the subquery may use WHERE / DISTINCT / ORDER BY / LIMIT internally
RETURN 'Ozzy' IN {
    MATCH (:Person)-[:HAS_DOG]->(d:Dog) WHERE d.name <> 'Fido' RETURN d.name
} AS yes;
RETURN 'Fido' IN {
    MATCH (:Person)-[:HAS_DOG]->(d:Dog) RETURN DISTINCT d.name ORDER BY d.name LIMIT 1
} AS first_only;

-- UNION, WITH + aggregate, and OPTIONAL MATCH inside the subquery
RETURN 'Mittens' IN { MATCH (d:Dog) RETURN d.name UNION MATCH (c:Cat) RETURN c.name } AS yes;
RETURN 2 IN { MATCH (p:Person)-[:HAS_DOG]->(d:Dog) WITH p, count(d) AS c RETURN c } AS yes;
RETURN 'Andy' IN { MATCH (p:Person) OPTIONAL MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name } AS yes;

-- empty subquery: IN -> false, NOT IN -> true
RETURN 'x' IN { MATCH (d:Dog) WHERE false RETURN d.name } AS f;
RETURN 'x' NOT IN { MATCH (d:Dog) WHERE false RETURN d.name } AS t;

-- three-valued logic: a NULL on either side yields NULL, not false
RETURN (NULL IN { MATCH (d:Dog) RETURN d.name }) IS NULL AS lhs_null;
RETURN ('Rex' IN { MATCH (d:Dog) RETURN NULL }) IS NULL AS rhs_null;
RETURN ('Rex' NOT IN { MATCH (d:Dog) RETURN NULL }) IS NULL AS rhs_null_not;
-- the classic NOT-IN-with-a-NULL-in-the-set trap: a non-member is NULL, not true
RETURN ('Spot' NOT IN { SELECT 'Fido' UNION ALL SELECT NULL }) IS NULL AS notin_null_trap;

-- contexts: CASE, OR of two subqueries, WITH ... WHERE
RETURN CASE WHEN 'Fido' IN { MATCH (d:Dog) RETURN d.name }
            THEN 'known' ELSE 'unknown' END AS label;
MATCH (person:Person)
WHERE 'Fido' IN { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name }
   OR 'Mittens' IN { MATCH (person)-[:HAS_CAT]->(c:Cat) RETURN c.name }
RETURN person.name AS name ORDER BY name;
MATCH (person:Person)
WITH person
WHERE person.name IN { MATCH (:Person)-[:HAS_DOG]->(d:Dog) RETURN d.name }
RETURN person.name AS name ORDER BY name;

-- nested: owners of a dog that itself has a toy, matched via IN
MATCH (person:Person)-[:HAS_DOG]->(dog:Dog)
WHERE dog.name IN { MATCH (d:Dog)-[:HAS_TOY]->(:Toy) RETURN d.name }
RETURN person.name AS owner, dog.name AS dog ORDER BY owner, dog;

-- inside a list comprehension's WHERE filter: keep the elements that the
-- subquery yields (the iteration variable is the membership value)
RETURN [n IN ['Fido', 'Rex', 'Ozzy'] WHERE n IN { MATCH (:Person)-[:HAS_DOG]->(d:Dog) RETURN d.name }] AS dogs;
RETURN [n IN ['Fido', 'Rex', 'Mittens'] WHERE n NOT IN { MATCH (:Person)-[:HAS_DOG]->(d:Dog) RETURN d.name }] AS not_dogs;
-- the same, correlated to the outer row (per-person dog filtering)
MATCH (person:Person)
WITH person, ['Fido', 'Ozzy', 'Andy'] AS cand
RETURN person.name AS name,
       [c IN cand WHERE c IN { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name }] AS owned
ORDER BY name;
-- a SQL subquery as the list comprehension's membership source
RETURN [x IN [1, 2, 3, 4] WHERE x IN { SELECT to_jsonb(g) FROM generate_series(2, 3) g }] AS kept;

-- IN { ... } alongside the other subquery forms in one projection
MATCH (person:Person)
RETURN person.name AS name,
       'Fido' IN { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name } AS has_fido,
       EXISTS { MATCH (person)-[:HAS_DOG]->(:Dog) } AS has_dog,
       COUNT { MATCH (person)-[:HAS_DOG]->(:Dog) } AS num_dogs
ORDER BY name;

-- SQL subquery on the right-hand side (no jsonb cast; the column keeps its type)
RETURN 42 IN { SELECT 42 } AS yes;
RETURN 1 IN { SELECT g FROM generate_series(2, 4) g } AS no;
RETURN 3 IN { SELECT g FROM generate_series(2, 4) g } AS yes;
RETURN 5 NOT IN { SELECT g FROM generate_series(2, 4) g } AS t;
RETURN 2 IN { VALUES (1), (2), (3) } AS yes;
RETURN 9 NOT IN { VALUES (1), (2), (3) } AS t;
RETURN 3 IN { SELECT a FROM (VALUES (1), (2), (3)) v(a) GROUP BY a HAVING count(*) >= 1 } AS yes;

-- coexistence: IN over a list literal, a bound list, an indexed list, a map
-- field, COLLECT { ... } and a parenthesised SELECT must all still work
RETURN 20 IN [10, 20, 30] AS list_lit;
MATCH (p:Person {name: 'Peter'}) WITH [10, 20, 30] AS lst RETURN 20 IN lst AS list_var;
RETURN 3 IN ([[1, 2], [3, 4]])[1] AS indexed;
RETURN 2 IN ({x: [1, 2, 3]}).x AS map_field;
RETURN 'Fido' IN COLLECT { MATCH (d:Dog) RETURN d.name } AS via_collect;
RETURN 7 IN (SELECT 7) AS via_paren_select;

-- map literals still parse everywhere (the IN right-hand side excludes only a
-- bare leading map; map keys may even be reserved words)
RETURN {name: 'Andy', age: 36} AS m;
RETURN {MATCH: 1, RETURN: 2} AS reserved_keys;
RETURN ({a: 1}).a AS field;

-- SQL + Cypher hybrid: a SQL SELECT over cypher rows filtered by IN { ... }
SELECT t.name
FROM (MATCH (person:Person)
      WHERE 'Fido' IN { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name }
      RETURN person.name AS name) t
ORDER BY t.name;

-- SQL + Cypher hybrid: a SQL aggregate counting cypher rows that pass IN { ... }
SELECT count(*) AS dog_owners
FROM (MATCH (person:Person)
      WHERE 'Fido' IN { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name }
         OR 'Andy' IN { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name }
      RETURN person.name AS name) t;

-- SQL + Cypher hybrid: a SQL CTE wrapping a cypher query that uses IN { ... }
WITH owners AS (
    MATCH (person:Person)
    WHERE person.name IN { MATCH (:Person)-[:HAS_DOG]->(d:Dog) RETURN d.name }
    RETURN person.name AS name
)
SELECT name FROM owners ORDER BY name;

-- SQL + Cypher hybrid: IN over a SQL subquery that itself wraps cypher rows
-- (the left operand is cast to jsonb so it matches the cypher column type)
RETURN 'Fido'::jsonb IN {
    SELECT t.name FROM (MATCH (:Person)-[:HAS_DOG]->(d:Dog) RETURN d.name AS name) t
} AS yes;

-- SQL + Cypher hybrid: IN over a SQL UNION of cypher dog and cat names
RETURN 'Mittens'::jsonb IN {
    SELECT n FROM (MATCH (d:Dog) RETURN d.name AS n
                   UNION
                   MATCH (c:Cat) RETURN c.name AS n) u
} AS yes;

-- SQL + Cypher hybrid: IN over a subquery on a plain SQL table
CREATE TABLE in_names (nm text);
INSERT INTO in_names VALUES ('Fido'), ('Rex'), ('Spot');
RETURN 'Fido'::jsonb IN { SELECT to_jsonb(nm) FROM in_names } AS yes;
RETURN 'Ozzy' NOT IN { TABLE in_names } AS t;
DROP TABLE in_names;

-- nested: another subquery inside the IN subquery's body
-- ... a WHERE that uses EXISTS
RETURN 'Fido' IN {
    MATCH (:Person)-[:HAS_DOG]->(d:Dog) WHERE EXISTS { MATCH (d)-[:HAS_TOY]->(:Toy) }
    RETURN d.name
} AS yes;
-- ... a RETURN that is a COUNT
RETURN 2 IN { MATCH (p:Person) RETURN COUNT { (p)-[:HAS_DOG]->(:Dog) } } AS yes;
-- ... a RETURN that is a VALUE (which yields NULL for the no-match rows)
RETURN 'Fido' IN {
    MATCH (p:Person) RETURN VALUE { MATCH (p)-[:HAS_DOG]->(d:Dog {name: 'Fido'}) RETURN d.name }
} AS yes;
-- ... three levels deep: IN over (IN over (EXISTS))
RETURN 'Fido' IN {
    MATCH (d:Dog)
    WHERE d.name IN {
        MATCH (x:Dog) WHERE EXISTS { MATCH (x)-[:HAS_TOY]->(:Toy) } RETURN x.name
    }
    RETURN d.name
} AS yes;

-- the IN predicate nested inside the other subquery kinds
-- ... inside an EXISTS body
MATCH (p:Person)
WHERE EXISTS {
    MATCH (p)-[:HAS_DOG]->(d:Dog)
    WHERE d.name IN { MATCH (x:Dog)-[:HAS_TOY]->(:Toy) RETURN x.name }
}
RETURN p.name AS name ORDER BY name;
-- ... inside a COUNT body
MATCH (p:Person)
RETURN p.name AS name,
       COUNT {
           MATCH (p)-[:HAS_DOG]->(d:Dog)
           WHERE d.name IN { MATCH (x:Dog)-[:HAS_TOY]->(:Toy) RETURN x.name }
       } AS toy_dogs
ORDER BY name;
-- ... inside a CASE inside a subquery
RETURN 'has' IN {
    MATCH (p:Person)
    RETURN CASE WHEN 'Fido' IN { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name }
                THEN 'has' ELSE 'no' END
} AS yes;

-- "x IN COLLECT { ... }" and "x IN { ... }" agree on the same set
MATCH (p:Person {name: 'Peter'})
RETURN ('Fido' IN COLLECT { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name })
     = ('Fido' IN { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name }) AS same;
-- IN over an ARRAY { ... } subquery (membership against the gathered list)
MATCH (p:Person {name: 'Peter'})
RETURN 'Ozzy' IN ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name } AS yes;

-- all six subquery kinds combined in one projection
MATCH (p:Person {name: 'Peter'})
RETURN EXISTS { MATCH (p)-[:HAS_DOG]->(:Dog) } AS ex,
       COUNT { MATCH (p)-[:HAS_DOG]->(:Dog) } AS cnt,
       COLLECT { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS coll,
       ARRAY { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name } AS arr,
       VALUE { MATCH (p)-[:HAS_DOG]->(d:Dog {name: 'Fido'}) RETURN d.name } AS val,
       'Fido' IN { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name } AS inq;

-- a WHERE that mixes EXISTS, IN and a COUNT comparison
MATCH (p:Person)
WHERE EXISTS { MATCH (p)-[:HAS_DOG]->(:Dog) }
  AND 'Fido' IN { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name }
  AND COUNT { MATCH (p)-[:HAS_DOG]->(:Dog) } > 1
RETURN p.name AS name ORDER BY name;
-- NOT EXISTS combined with NOT IN
MATCH (p:Person)
WHERE NOT EXISTS { MATCH (p)-[:HAS_CAT]->(:Cat) }
  AND 'Mittens' NOT IN { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name }
RETURN p.name AS name ORDER BY name;

-- IN over a list literal built from COUNT subqueries
MATCH (p:Person {name: 'Peter'})
RETURN 2 IN [COUNT { (p)-[:HAS_DOG]->(:Dog) }, COUNT { (p)-[:HAS_CAT]->(:Cat) }] AS yes;
-- a map literal whose values are several different subqueries
MATCH (p:Person {name: 'Peter'})
RETURN {dogs: COLLECT { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name ORDER BY d.name },
        n: COUNT { (p)-[:HAS_DOG]->(:Dog) },
        hasFido: 'Fido' IN { MATCH (p)-[:HAS_DOG]->(d:Dog) RETURN d.name }} AS m;

-- error: the subquery must return only one column
RETURN 1 IN { MATCH (p:Person)-[:HAS_DOG]->(d:Dog) RETURN p.name, d.name };
RETURN 1 NOT IN { MATCH (p:Person)-[:HAS_DOG]->(d:Dog) RETURN p.name, d.name };

-- error: writes are not allowed inside an IN subquery
RETURN 1 IN { CREATE (:Foo) RETURN 1 };
RETURN 1 IN { MATCH (a:Person) SET a.x = 1 RETURN a.x };
RETURN 1 IN { MATCH (a:Person) DETACH DELETE a RETURN a };

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
DROP GRAPH agv2_547 CASCADE;

SET graph_path = agens;

DROP VLABEL feature;
DROP ELABEL supported;
DROP VLABEL repo;
DROP ELABEL lib;
DROP ELABEL doc;

DROP GRAPH agens CASCADE;

DROP TABLE history;

DROP GRAPH rename_test CASCADE;

-- A text value extracted from the jsonb property bag (n.prop #>> '{}') carries
-- no collation.  A DISTINCT, GROUP BY, ORDER BY or min()/max() over such a value
-- is compared with the database default collation -- the same collation jsonb
-- uses for its own strings -- so these must run and give a stable result, incl.
-- when a key is NULL (a missing property).  (Regression: PostgreSQL 18 refuses
-- to hash or compare an uncollated string, so each once failed with "could not
-- determine which collation to use".)
CREATE GRAPH collate_jsonb;
SET graph_path = collate_jsonb;
CREATE VLABEL person;
CREATE (:person {name: 'bob', city: 'rome'}),
       (:person {name: 'amy', city: 'rome'}),
       (:person {name: 'bob', city: 'oslo'}),
       (:person {name: 'cat', city: 'oslo'}),
       (:person {city: 'oslo'});                    -- no name: the key is NULL text
-- terminal RETURN DISTINCT over the extracted text (hash + sort), incl. the NULL
MATCH (n:person) RETURN DISTINCT (n.name #>> '{}') AS nm ORDER BY nm NULLS FIRST;
-- RETURN DISTINCT of a text carried through WITH, DESC with NULLS LAST
MATCH (n:person) WITH (n.name #>> '{}') AS nm RETURN DISTINCT nm ORDER BY nm DESC NULLS LAST;
-- a non-terminal WITH that orders by the extracted text
MATCH (n:person) WITH (n.name #>> '{}') AS nm, count(*) AS c ORDER BY nm NULLS FIRST RETURN nm, c;
-- an implicit GROUP BY on two extracted-text keys
MATCH (n:person)
RETURN (n.name #>> '{}') AS nm, (n.city #>> '{}') AS ct, count(*) AS c
ORDER BY nm NULLS FIRST, ct;
-- min()/max() over the extracted text
MATCH (n:person) RETURN min(n.name #>> '{}') AS lo, max(n.name #>> '{}') AS hi;
-- collect(DISTINCT ...) and count(DISTINCT ...) over the extracted text
MATCH (n:person)
RETURN collect(DISTINCT (n.name #>> '{}')) AS names, count(DISTINCT (n.city #>> '{}')) AS ncity;
-- DISTINCT on two extracted-text keys with a compound ORDER BY
MATCH (n:person)
RETURN DISTINCT (n.name #>> '{}') AS nm, (n.city #>> '{}') AS ct ORDER BY ct, nm NULLS LAST;
DROP GRAPH collate_jsonb CASCADE;
--
-- A terminal write carries only the elements it names
--
-- Nothing runs after the clause, so a pattern variable the clause does not
-- remove need not be assembled -- and once nothing reads it, the joins beneath
-- stop carrying its property map too.  Results must not depend on any of that.
--
CREATE GRAPH write_projection;
SET graph_path = write_projection;

CREATE VLABEL wv;
CREATE ELABEL wk;

CREATE (:wv {n: 1})-[:wk {w: 1}]->(:wv {n: 2}),
       (:wv {n: 3})-[:wk {w: 2}]->(:wv {n: 4}),
       (:wv {n: 5});
ANALYZE write_projection.wv;
ANALYZE write_projection.wk;

-- the endpoints are named by the pattern but not by the clause, so neither is
-- assembled: both come through as a null of their own type
EXPLAIN (VERBOSE, COSTS OFF)
MATCH (a:wv)-[r:wk]->(b:wv) DELETE r;

-- an endpoint the clause does remove is carried
EXPLAIN (VERBOSE, COSTS OFF)
MATCH (a:wv)-[r:wk]->(b:wv) DELETE r, a;

-- with a clause after it, the elements that clause reads are carried; the rest
-- still are not, since a delete no longer hides its inputs from the planner
EXPLAIN (VERBOSE, COSTS OFF)
MATCH (a:wv)-[r:wk]->(b:wv) DELETE r RETURN b.n;

-- and the answers are the same either way
MATCH (a:wv)-[r:wk]->(b:wv) DELETE r RETURN b.n AS kept ORDER BY kept;
MATCH ()-[r:wk]->() RETURN count(*) AS edges_left;
MATCH (n:wv) RETURN count(*) AS vertices_left;

CREATE (:wv {n: 6})-[:wk {w: 6}]->(:wv {n: 7});
MATCH (a:wv {n: 6}) DETACH DELETE a;
MATCH (n:wv) RETURN n.n AS n ORDER BY n;
MATCH ()-[r:wk]->() RETURN count(*) AS edges_after_detach;

CREATE (:wv {n: 8})-[:wk]->(:wv {n: 9});
MATCH (a:wv {n: 8})-[r:wk]->(b:wv) DELETE r, a, b;
MATCH (n:wv) WHERE n.n IN [8, 9] RETURN count(*) AS both_removed;

DROP GRAPH write_projection CASCADE;

--
-- A write clause after an aggregating projection
--
-- Inheritance makes the label a write resolves to observable: a write reached
-- through the parent label touches the child's rows too, and one reached
-- through the child does not touch the parent's.
--
CREATE GRAPH agg_write;
SET graph_path = agg_write;
CREATE VLABEL par;
CREATE VLABEL chi INHERITS (par);
CREATE ELABEL rel;
CREATE (:par {name: 'p1'});
CREATE (:par {name: 'p2'});
CREATE (:chi {name: 'c1'});
MATCH (a:par {name: 'p1'}), (b:chi {name: 'c1'}) CREATE (a)-[:rel {w: 1}]->(b);

-- SET through the parent label reaches every row, the child's included
MATCH (p:par) WITH p, count(p) AS n SET p.seen = n;
MATCH (n:par) RETURN label(n), n.name, n.seen ORDER BY n.name;

-- and through the child label, only the child's
MATCH (p:chi) WITH p, count(p) AS n SET p.only = n;
MATCH (n:par) RETURN label(n), n.name, n.only ORDER BY n.name;

-- the other aggregates
MATCH (p:par) WITH p, collect(p.name) AS c SET p.coll = size(c);
MATCH (n:par) RETURN label(n), n.name, n.coll ORDER BY n.name;

-- a relationship
MATCH ()-[r:rel]->() WITH r, count(r) AS n SET r.ew = n;
MATCH ()-[r:rel]->() RETURN r.w, r.ew;

-- REMOVE
MATCH (p:par) WITH p, count(p) AS n REMOVE p.seen;
MATCH (n:par) RETURN label(n), n.name, n.seen ORDER BY n.name;

-- the target need not be the element the aggregate counted
MATCH (p:par), (q:chi) WITH p, q, count(p) AS n SET q.other = n;
MATCH (n:par) RETURN label(n), n.name, n.other ORDER BY n.name;

-- nested aggregating projections
MATCH (p:par) WITH p, count(p) AS a WITH p, count(a) AS b SET p.two = b;
MATCH (n:par) RETURN label(n), n.name, n.two ORDER BY n.name;

-- a filter, an ordering and an unwind after the aggregate
MATCH (p:par) WITH p, count(p) AS n WHERE n > 0 SET p.hav = n;
MATCH (p:par) WITH p, count(p) AS n ORDER BY n SET p.ord = n;
MATCH (p:par) WITH p, count(p) AS n UNWIND [7] AS u SET p.unw = u;
MATCH (n:par) RETURN label(n), n.name, n.hav, n.ord, n.unw ORDER BY n.name;

-- DELETE and DETACH DELETE
MATCH ()-[r:rel]->() WITH r, count(r) AS n DELETE r;
MATCH ()-[r:rel]->() RETURN count(*);
MATCH (p:chi) WITH p, count(p) AS n DETACH DELETE p;
MATCH (n:par) RETURN label(n), n.name ORDER BY n.name;

-- CREATE and MERGE take their label from their own pattern
MATCH (p:par) WITH p, count(p) AS n CREATE (:par {name: 'made', seq: n});
MATCH (p:par) WITH p, count(p) AS n MERGE (:par {name: 'merged'});
MATCH (n:par) RETURN count(*);

DROP GRAPH agg_write CASCADE;

--
-- A write refused for being a write is refused by name
--
-- A read-only transaction and a function that promises not to write both name
-- the statement they refuse.  A Cypher write had no name to be refused by, so
-- both printed one it could not read and warned about the command type first.
--
CREATE GRAPH ro_name;
SET graph_path = ro_name;
CREATE VLABEL t;
CREATE (:t {a: 1});

BEGIN;
SET TRANSACTION READ ONLY;
-- a read is allowed
MATCH (n:t) RETURN count(*);
-- a write is not, and the refusal says what it refused
CREATE (:t {a: 2});
ROLLBACK;

-- the same write inside a function that declared itself non-volatile
CREATE FUNCTION ro_name.w() RETURNS void LANGUAGE sql STABLE AS $$ CREATE (:t {a: 3}) $$;
SELECT ro_name.w();
DROP FUNCTION ro_name.w();

-- and outside either, the write reports what it wrote
CREATE (:t {a: 4});
MATCH (n:t) RETURN count(*);

DROP GRAPH ro_name CASCADE;

--
-- a name in an element's property map means the element, not the table the
-- element is matched from
--

CREATE GRAPH elem_value;
SET graph_path = elem_value;

CREATE VLABEL t;
CREATE ELABEL e;

-- x carries a property named id, which is not the same as its graph id
CREATE (:t {name: 'x', id: 'p1'});
CREATE (:t {name: 'y'});
CREATE (:t {name: 'z'});
MATCH (a:t {name: 'x'}), (b:t {name: 'y'}) CREATE (a)-[:e]->(b);
MATCH (a:t {name: 'x'}), (b:t {name: 'z'}) CREATE (a)-[:e]->(b);
-- y.k holds x's id property; z.k holds x's graph id
MATCH (n:t {name: 'y'}) SET n.k = 'p1';
MATCH (a:t {name: 'x'}), (n:t {name: 'z'}) SET n.k = id(a)::text;

-- the property, so y, whether or not the element carrying the map has a name
MATCH (a:t {name: 'x'})-[:e]->(:t {k: a.id, name: 'y'}) RETURN count(*) AS reads_property;
MATCH (a:t {name: 'x'})-[:e]->(:t {k: a.id, name: 'z'}) RETURN count(*) AS reads_graph_id;
MATCH (a:t {name: 'x'})-[:e]->(b:t {k: a.id, name: 'y'}) RETURN count(*) AS named;

-- a variable is readable there, and names the element itself
MATCH (a:t)-[:e]->(:t {k: a.name}) RETURN count(*) AS by_name;
MATCH (a:t)-[:e]->(:t {k: a}) RETURN count(*) AS by_element;

DROP GRAPH elem_value CASCADE;

--
-- what a definition holding a graph expression prints can be read back
--

CREATE GRAPH viewdef;
SET graph_path = viewdef;

CREATE VLABEL person;
CREATE (:person {name: 'Alice', addr: {city: 'Seoul'}});
CREATE (:person {name: 'Bob', addr: {city: 'Busan'}});

-- a view over a property read, and the same view built from what it prints
CREATE VIEW v AS SELECT * FROM (MATCH (n:person) RETURN n.name AS name) q;
SELECT pg_get_viewdef('v'::regclass, true);
CREATE VIEW v_reloaded AS
 SELECT name
   FROM ( SELECT (_agens_default_s.n).properties.'name' AS name
           FROM ( SELECT ROW(n.id, n.properties, n.ctid)::vertex AS n
                   FROM viewdef.person n) _agens_default_s) q;
SELECT name FROM v ORDER BY name;
SELECT name FROM v_reloaded ORDER BY name;
-- the same column type on both sides, so the reload is the view it printed
SELECT c.relname, format_type(a.atttypid, a.atttypmod) AS type
FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
WHERE c.relname IN ('v', 'v_reloaded') AND a.attnum > 0
ORDER BY c.relname;

-- a path of more than one key
CREATE VIEW vn AS SELECT * FROM (MATCH (n:person) RETURN n.addr.city AS city) q;
SELECT pg_get_viewdef('vn'::regclass, true);

-- a key read from a map that a column holds, rather than from an element
CREATE VIEW vb AS
SELECT * FROM (MATCH (n:person) WITH properties(n) AS p RETURN p.name AS r) q;
SELECT pg_get_viewdef('vb'::regclass, true);

-- a quoted key reads a map, and says so when what it is given is not one
SELECT (42).'k';
SELECT ('abc'::text).'k';

DROP VIEW v_reloaded;
DROP VIEW vb;
DROP VIEW vn;
DROP VIEW v;
DROP GRAPH viewdef CASCADE;
RESET graph_path;
