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

DROP GRAPH cypher_dml2 CASCADE;