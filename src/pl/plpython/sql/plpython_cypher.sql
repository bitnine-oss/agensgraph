--
-- Cypher Query Language - User Defined Function
--


-- setup

DROP FUNCTION IF EXISTS udf_graphwrite();
DROP GRAPH IF EXISTS udf CASCADE;

CREATE GRAPH udf;

SET GRAPH_PATH = udf;

CREATE OR REPLACE FUNCTION udf_graphwrite() RETURNS void AS $$
plan1 = plpy.prepare( "CREATE (a:person{name : 'Anders'})-[:knows {name:'friend1'}]->(b:person{name : 'Dilshad'}), (a)-[:knows {name:'friend2'}]->(c:person{name : 'Cesar'}), (a)-[:knows {name:'friend3'}]->(d:person{name : 'Becky'}), (b)-[:knows {name:'friend4'}]->(:person{name : 'Filipa'}), (c)-[:knows {name:'friend5'}]->(e:person{name : 'Emil'})" )
res1 = plpy.execute( plan1 )

plan2 = plpy.prepare( "MATCH (a:person{name : 'Becky'}) , (b:person{name : 'Emil'}) MERGE (a)-[r:knows {name:'friend6'}]-(b) ON CREATE SET r.created = true, r.matched = null ON MATCH SET r.matched = true, r.created = null RETURN r" )
res2 = plpy.execute( plan2 )

plpy.notice( res2 )
plpy.notice( res2[0]["r"] )

$$ LANGUAGE plpythonu;

SELECT udf_graphwrite();

SET ALLOW_GRAPHWRITE_TYPE = true;

SELECT udf_graphwrite();

SET ALLOW_GRAPHWRITE_TYPE = false;

-- teardown

DROP GRAPH udf CASCADE;
DROP FUNCTION udf_graphwrite();
