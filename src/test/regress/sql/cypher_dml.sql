--
-- Cypher Query Language - DML
--

-- initialize

DROP vlabel regvlabel1;
DROP vlabel regvlabel2;
DROP vlabel regvlabel3;

DROP elabel regelabel1;
DROP elabel regelabel2;

CREATE vlabel regvlabel1;
CREATE vlabel regvlabel2;
CREATE vlabel regvlabel3;

CREATE elabel regelabel1;
CREATE elabel regelabel2;

CREATE (v1:regvlabel1 {'name': 'regv1-1', 'id': 1}),
	   (v2:regvlabel1 {'name': 'regv1-2', 'id': 2}),
	   (v3:regvlabel1 {'name': 'regv1-3', 'id': 3}),

	   (v4:regvlabel2 {'name': 'regv2-1', 'id': 4}),
	   (v5:regvlabel2 {'name': 'regv2-2', 'id': 5}),
	   (v6:regvlabel2 {'name': 'regv2-3', 'id': 6}),
	   (v7:regvlabel2 {'name': 'regv2-4', 'id': 7}),
	   (v8:regvlabel2 {'name': 'regv2-5', 'id': 8}),

	   (v1)-[:regelabel1 {'name': 'rege1-1', 'id': 1}]->(v4),
	   (v2)-[:regelabel1 {'name': 'rege1-2', 'id': 2}]->(v5),
	   (v3)-[:regelabel1 {'name': 'rege1-3', 'id': 3}]->(v6),

	   (v1)<-[:regelabel2 {'name': 'rege2-1', 'id': 4}]-(v8),
	   (v2)<-[:regelabel2 {'name': 'rege2-2', 'id': 5}]-(v7),
	   (v3)<-[:regelabel2 {'name': 'rege2-3', 'id': 6}]-(v6);

--
-- MATCH & RETURN clause
--

-- simple matches

MATCH (a)
RETURN properties(a) AS a;

MATCH (a)-[]->(b)
RETURN properties(a) AS a, properties(b) AS b;

MATCH (a)-[]-(b)
RETURN properties(a) AS a, properties(b) AS b;

MATCH (a)<-[]->(b)
RETURN properties(a) AS a, properties(b) AS b;

-- wrong case (last clause)
MATCH (a);

-- WHERE in MATCH
MATCH (a:regvlabel1)
WHERE (a).name = to_jsonb('regv1-1'::text)
RETURN properties(a);

-- TODO: label filtering by WHERE clause

-- TODO: relationship filtering by WHERE clause

-- aliasing

MATCH (a:regvlabel1)
RETURN properties(a) AS props;

MATCH (a:regvlabel1)
RETURN (a).id AS empid;

MATCH (a:regvlabel1)
RETURN (id(a)).lid;

-- ORDER BY

MATCH (a:regvlabel1)
RETURN properties(a) AS a ORDER BY (a).id;

MATCH (a:regvlabel1)
RETURN properties(a) AS a ORDER BY (a).id ASC;

MATCH (a:regvlabel1)
RETURN properties(a) AS a ORDER BY (a).id DESC;

MATCH (a:regvlabel1)
RETURN (a).id AS empid ORDER BY empid;

-- wrong case (refer alias)
MATCH (a:regvlabel1)
RETURN (a).id AS empid ORDER BY a;

-- LIMIT & LIMIT

MATCH (a:regvlabel1)
RETURN properties(a) AS a ORDER BY (a).id LIMIT 1;

MATCH (a:regvlabel1)
RETURN properties(a) AS a ORDER BY (a).id DESC LIMIT 1;

MATCH (a:regvlabel2)
RETURN properties(a) AS a ORDER BY (a).id SKIP 3;

MATCH (a:regvlabel2)
RETURN properties(a) AS a ORDER BY (a).id SKIP 1 LIMIT 1;

MATCH (a:regvlabel2)
RETURN properties(a) AS a ORDER BY (a).id DESC SKIP 1 LIMIT 1;

-- wrong case (syntax)
MATCH (a:regvlabel2)
RETURN properties(a) AS a ORDER BY (a).id LIMIT 1 SKIP 1;

--
-- WITH clause
--

MATCH (a:regvlabel1)-[]-(b:regvlabel2)
WITH a, count(b) AS rel_count
RETURN properties(a) AS a, rel_count;

-- wrong case
MATCH (a:regvlabel1), (b:regvlabel2)
WITH a
RETURN b;

-- DISTINCT

MATCH (a:regvlabel1), (b:regvlabel2)
RETURN properties(a) AS a;

MATCH (a:regvlabel1), (b:regvlabel2)
WITH DISTINCT a
RETURN properties(a) AS a;

MATCH (a:regvlabel1)-[]-(b:regvlabel2)
RETURN properties(a) AS a, properties(b) AS b;

MATCH (a:regvlabel1)-[]-(b:regvlabel2)
WITH DISTINCT *
RETURN properties(a) AS a, properties(b) AS b;

--
-- UNION
--

MATCH (a:regvlabel1)
RETURN properties(a) AS all_vertex
UNION ALL
MATCH (b:regvlabel2)
RETURN properties(b) AS all_vertex;

MATCH (a:regvlabel1)
RETURN properties(a) AS all_vertex
UNION ALL
MATCH (b:regvlabel2)
RETURN properties(b) AS akk_vertex;

-- wrong case (type mismatch)
MATCH (a:regvlabel1)
RETURN properties(a) AS a
UNION ALL
MATCH (b:regvlabel2)
RETURN b;

--
-- re-initialize
--

DROP vlabel regvlabel1;
DROP vlabel regvlabel2;
DROP vlabel regvlabel3;

DROP elabel regelabel1;
DROP elabel regelabel2;

CREATE vlabel regvlabel1;
CREATE vlabel regvlabel2;
CREATE vlabel regvlabel3;

CREATE elabel regelabel1;
CREATE elabel regelabel2;

--
-- CREATE clause
--

-- normal case

CREATE (),
	   (a),
	   (b:regvlabel1),
	   (c:regvlabel2 {'name': 'test1'}),
	   p=(d:regvlabel3 {'name': 'test2', 'age': 123})-[:regelabel1]->();

MATCH (a) RETURN (id(a)).lid, properties(a);

MATCH (n) DETACH DELETE n;

-- refer previous variable

CREATE (a:regvlabel1 {'name': 'elem1'}),
	   (a)-[d:regelabel1 {'rel': 1}]->(b:regvlabel2 {'name': 'elem2'}),
	   (b)<-[e:regelabel2 {'rel': 2}]-(c:regvlabel2 {'name': 'elem3'})
RETURN properties(a), properties(d), properties(b), properties(e), properties(c);

MATCH (a)-[b]-(c)
RETURN properties(a), properties(b), properties(c);

MATCH (n) DETACH DELETE n;

-- relationship

CREATE (a:regvlabel1 {'name': 'insert v1-1'})-[b:regelabel1]->(c:regvlabel2 {'name': 'insert v2-1'});
CREATE (a:regvlabel1 {'name': 'insert v1-2'})<-[b:regelabel1]-(c:regvlabel2 {'name': 'insert v2-2'});

MATCH (a)<-[b]->(c)
RETURN properties(a), properties(b), properties(c);

-- bi-directional relationship

CREATE (:regvlabel2 {'name': 'regvlabel2-1'}),
	   (:regvlabel2 {'name': 'regvlabel2-2'}),
	   (:regvlabel2 {'name': 'regvlabel2-3'});

CREATE (a:regvlabel1 {'name': 'regvlabel1'})
MATCH (b:regvlabel2)
CREATE (a)-[c:regelabel1 {'name': 'e1'}]->(b), (b)-[d:regelabel1 {'name': 'e2'}]->(a)
RETURN id(a) = start(c) AND "end"(c) = id(b) AS atob,
	   id(b) = start(d) AND "end"(d) = id(a) AS btoa;

-- wrong cases (duplicated variable)
CREATE (a), (a);
CREATE (a:regvlabel1), (b:regvlabel2), (a:regvlabel);

-- wrong cases (label)
CREATE (a:regvlabel1:regvlabel2);
CREATE (:regvlabel1)-[{'reltype': 'empty'}]->(:regvlabel2);

-- wrong cases (bi-directional relationship)
CREATE (:regvlabel1 {'name': 'insert v1-3'})-[:regelabel1]-(:regvlabel2 {'name': 'insert v2-3'});
CREATE (:regvlabel1 {'name': 'insert v1-4'})<-[:regelabel1]->(:regvlabel2 {'name': 'insert v2-4'});

-- multiple CREATE's
MATCH (a:regvlabel1), (b:regvlabel2)
CREATE p=(a)-[:regelabel1 {'name': 'edge1'}]->(b)
CREATE (b)<-[:regelabel2 {'name': 'edge2'}]-(c:regvlabel3 {'name': 'v3'})
RETURN properties(c);

-- cleanup

DROP elabel regelabel1;
DROP elabel regelabel2;

DROP vlabel regvlabel1;
DROP vlabel regvlabel2;
DROP vlabel regvlabel3;
