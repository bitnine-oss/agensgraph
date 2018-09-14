--
-- PL/agCypher - Use Parameter
--

-- setup

DROP GRAPH IF EXISTS g4 CASCADE;
DROP FUNCTION IF EXISTS func_create();
DROP FUNCTION IF EXISTS func_delete();
DROP FUNCTION IF EXISTS func_set();
DROP FUNCTION IF EXISTS func_merge();
DROP FUNCTION IF EXISTS func_remove();
DROP FUNCTION IF EXISTS func_complex1();
DROP FUNCTION IF EXISTS func_complex2();

CREATE GRAPH g4;
SET GRAPH_PATH=g4;

CREATE ELABEL block;
CREATE (a:person{name : 'Anders'})-[:knows {name:'friend1'}]->(b:person{name : 'Dilshad'}),
(a)-[:knows {name:'friend2'}]->(c:person{name : 'Cesar'}),
(a)-[:knows {name:'friend3'}]->(d:person{name : 'Becky'}),
(b)-[:knows {name:'friend4'}]->(:person{name : 'Filipa'}),
(c)-[:knows {name:'friend5'}]->(e:person{name : 'Emil'}),
(d)-[:knows {name:'friend6'}]->(e);

-- test graphwrite type ( create )
CREATE OR REPLACE FUNCTION func_create() RETURNS void AS $$
BEGIN
CREATE( :person{name : 'Bossman'} );
END;
$$ LANGUAGE plagcypher;

MATCH (a) RETURN properties(a);

SELECT func_create();

MATCH (a) RETURN properties(a);

CREATE OR REPLACE FUNCTION func_delete() RETURNS void AS $$
BEGIN
MATCH (a)
WHERE a.name = 'Becky'
DELETE a;
END;
$$ LANGUAGE plagcypher;

MATCH (a) RETURN properties(a);

SELECT func_delete();

MATCH (a) RETURN properties(a);

CREATE OR REPLACE FUNCTION func_set() RETURNS void AS $$
BEGIN
MATCH (a)
WHERE a.name = 'Becky'
SET a.name = 'lucy';
END;
$$ LANGUAGE plagcypher;

MATCH (a) RETURN properties(a);

SELECT func_set();

MATCH (a) RETURN properties(a);

CREATE OR REPLACE FUNCTION func_merge() RETURNS void AS $$
BEGIN
MATCH (a) , (b)
WHERE a.name = 'Cesar' AND b.name = 'Filipa'
MERGE (a)-[e:block {name:'block'}]->(b);
END;
$$ LANGUAGE plagcypher;

MATCH (a)-[b]->(c)
WHERE b.name = 'block'
RETURN a , b , c;

SELECT func_merge();

MATCH (a)-[b]->(c)
WHERE b.name = 'block'
RETURN a , b , c;

CREATE OR REPLACE FUNCTION func_remove() RETURNS void AS $$
BEGIN
MATCH (a)
REMOVE a.name;
END;
$$ LANGUAGE plagcypher;

MATCH (a) RETURN properties(a);

SELECT func_remove();

MATCH (a) RETURN properties(a);

CREATE VLABEL v;
CREATE ELABEL e;

CREATE OR REPLACE FUNCTION func_complex1() RETURNS void AS $$
DECLARE
var1 edge;
var2 edge;
BEGIN
MATCH (a:v) , (b:v) , (c:v)
CREATE (a)-[z:e {name:'edge1', id:'1'}]->(b)
CREATE (b)-[r:e {name:'edge2', id:'2'}]->(c)
RETURN z , r INTO var1 , var2;
END;
$$ LANGUAGE plagcypher;

SELECT func_complex1();

CREATE OR REPLACE FUNCTION func_complex2() RETURNS void AS $$
DECLARE
var1 edge;
BEGIN
MATCH (a:v) , (b:v)
MERGE (a)-[r:e {name:'edge' , id:'0'}]-(b)
ON CREATE SET r.created = true, r.matched = null
ON MATCH SET r.matched = true, r.created = null
RETURN r INTO var1;
END;
$$ LANGUAGE plagcypher;

SELECT func_complex2();

DROP FUNCTION IF EXISTS func_complex2();
DROP FUNCTION IF EXISTS func_complex1();
DROP FUNCTION IF EXISTS func_remove();
DROP FUNCTION IF EXISTS func_merge();
DROP FUNCTION IF EXISTS func_set();
DROP FUNCTION IF EXISTS func_delete();
DROP FUNCTION IF EXISTS func_create();
DROP GRAPH IF EXISTS g4 CASCADE;
