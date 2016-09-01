--
-- Cypher Query Language - DML
--

-- prepare

DROP TABLE IF EXISTS history;

CREATE TABLE history (year, event) AS VALUES
(1996, 'PostgreSQL'),
(2016, 'Graph');

--
-- RETURN
--

RETURN 3 + 4, 'hello' || ' agens';

RETURN 3 + 4 AS lucky, 'hello' || ' agens' AS greeting;

RETURN (SELECT event FROM history WHERE year = 2016);

SELECT * FROM (RETURN 3 + 4, 'hello' || ' agens') AS _(lucky, greeting);

--
-- CREATE
--

CREATE VLABEL repo;
CREATE ELABEL lib;
CREATE ELABEL doc;

CREATE (g:repo {'name': 'agens-graph',
                'year': (SELECT year FROM history WHERE event = 'Graph')})
RETURN properties(g) AS g;

MATCH (g:repo)
CREATE (j:repo '{"name": "agens-graph-jdbc", "year": 2016}'),
       (d:repo =jsonb_build_object('name', 'agens-graph-docs', 'year', 2016))
CREATE (g)-[l:lib {'lang': 'Java'}]->(j),
       p=(g)
         -[:lib {'lang': 'C'}]->
         (:repo {'name': 'agens-graph-odbc', 'year': 2016}),
       (g)-[e:doc {'lang': 'en'}]->(d)
RETURN properties(l) AS lj, properties(j) AS j,
       properties((edges(p))[1]) AS lc, properties((vertices(p))[2]) AS c,
       properties(e) AS e, properties(d) AS d;

CREATE ()-[a:r]->(a);
CREATE a=(), (a);
CREATE (a), (a {});
CREATE (a), (a);
CREATE (=0);
CREATE ()-[]-();
CREATE ()-[]->();
CREATE ()-[:r|z]->();
CREATE (a)-[a:r]->();
CREATE ()-[a:r]->()-[a:z]->();
CREATE a=(), ()-[a:z]->();
CREATE ()-[:r =0]->();
CREATE (a), a=();
CREATE ()-[a:r]->(), a=();
CREATE a=(), a=();

--
-- MATCH
--

MATCH (a) RETURN (a).name AS a;
MATCH (a), (a) RETURN (a).name AS a;

CREATE ();
MATCH (a:repo) RETURN (a).name AS name, (a)['year'] AS year;

MATCH p=(a)-[b]-(c)
RETURN (a).name AS a, (b).lang AS b, (c).name AS c;

MATCH (a)<-[b]-(c)-[d]->(e)
RETURN (a).name AS a, (b).lang AS b, (c).name AS c,
       (d).lang AS d, (e).name AS e;

MATCH (a)<-[b]-(c), (c)-[d]->(e)
RETURN (a).name AS a, (b).lang AS b, (c).name AS c,
       (d).lang AS d, (e).name AS e;

MATCH (a)<-[b]-(c) MATCH (c)-[d]->(e)
RETURN (a).name AS a, (b).lang AS b, (c).name AS c,
       (d).lang AS d, (e).name AS e;

MATCH (a)<-[b]-(c), (f)-[g]->(h), (c)-[d]->(e)
RETURN (a).name AS a, (b).lang AS b, (c).name AS c,
       (d).lang AS d, (e).name AS e,
       (f).name AS f, (g).lang AS g, (h).name AS h;

MATCH (a {'name': 'agens-graph'}), (a {'year': 2016}) RETURN properties(a) AS a;
MATCH p=(a)-[]->({'name': 'agens-graph-jdbc'}) RETURN (a).name AS a;
MATCH p=()-[:lib]->(a) RETURN (a).name AS a;
MATCH p=()-[{'lang': 'en'}]->(a) RETURN (a).name AS a;

MATCH (a {'year': (SELECT year FROM history WHERE event = 'Graph')})
WHERE (a).name = '"agens-graph"'
RETURN (a).name AS a;

MATCH ();
MATCH (a), (a:repo) RETURN *;
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

MATCH (a {'name': properties->'name'}) RETURN *;
MATCH (a {'name': a.properties->'name'}) RETURN *;

--
-- DISTINCT
--

MATCH (a:repo)-[]-() RETURN DISTINCT (a).name AS a;

MATCH (a:repo)-[b]-(c)
RETURN DISTINCT ON (a) (a).name AS a, (b).lang AS b, (c).name AS c;

--
-- ORDER BY
--

MATCH (a:repo) RETURN (a).name AS a ORDER BY a;
MATCH (a:repo) RETURN (a).name AS a ORDER BY a ASC;
MATCH (a:repo) RETURN (a).name AS a ORDER BY a DESC;

--
-- SKIP and LIMIT
--

MATCH (a:repo) RETURN (a).name AS a ORDER BY a SKIP 1 LIMIT 1;

--
-- WITH
--

MATCH (a:repo) WITH (a).name AS name RETURN name;

MATCH (a)
WITH a WHERE label(a) = 'repo'
MATCH p=(a)-[]->(b)
RETURN (b).name AS b;

MATCH (a) WITH a RETURN b;
MATCH (a) WITH (a).name RETURN *;
MATCH () WITH a AS z RETURN a;

--
-- UNION
--

MATCH (a:repo)
RETURN (a).name AS a
UNION ALL
MATCH (b:lib)
RETURN (b).lang AS b
UNION ALL
MATCH (c:doc)
RETURN (c).lang AS c;

MATCH (a)
RETURN a
UNION
MATCH (b)
RETURN (b).name;

--
-- aggregates
--

MATCH (a)-[]-(b) RETURN count(a) AS a, (b).name AS b ORDER BY a;

--
-- EXISTS
--

MATCH (a:repo) WHERE exists((a)-[]->()) RETURN (a).name AS a;

--
-- SIZE
--

MATCH (a:repo) RETURN (a).name AS a, size((a)-[]->()) AS s;

--
-- LOAD
--

MATCH (a) LOAD FROM history AS a RETURN *;

CREATE VLABEL feature;
CREATE ELABEL supported;

MATCH (a:repo {'name': 'agens-graph'})
LOAD FROM history AS h
CREATE (:feature {'name': (h).event})-[:supported]->(a);

MATCH p=(a)-[:supported]->() RETURN properties(a) AS a;

--
-- DELETE
--

MATCH (a) DELETE a;

MATCH p=()-[:lib]->() DETACH DELETE (vertices(p))[2];
MATCH (a:repo) RETURN (a).name AS a;

MATCH ()-[a:doc]->() DETACH DELETE end_vertex(a);
MATCH (a:repo) RETURN (a).name AS a;

MATCH (a) DETACH DELETE a;
MATCH (a) RETURN a;

SELECT count(*) FROM graph.edge;

-- cleanup

DROP VLABEL feature;
DROP ELABEL supported;

DROP VLABEL repo;
DROP ELABEL lib;
DROP ELABEL doc;

DROP TABLE history;
