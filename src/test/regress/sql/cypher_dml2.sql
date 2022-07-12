CREATE GRAPH cypher_dml2;

CREATE VLABEL v1;

EXPLAIN ( analyze false, verbose true, costs false, buffers false, timing false )
MATCH (p:v1)
return max(collect(p.name)) as col;

MATCH (p:v1)
return max(collect(p.name)) as col;

MATCH (p:v1)
with collect(p.name) as col
RETURN max(col);

CREATE ELABEL e1;

-- AGV2-29, Predicates functions want jsonb, not list
MATCH p=(n1)-[r:e1*2]->(n2)
WHERE all(x in r where x.id is null)
RETURN count(p);

DROP GRAPH cypher_dml2 CASCADE;