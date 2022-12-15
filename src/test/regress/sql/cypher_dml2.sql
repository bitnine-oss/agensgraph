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

-- AGV2-26, head/tail/last returns array
CREATE(:v_user{name:'userA'});
CREATE(:v_title{name:'TitleA'});
CREATE(:v_type{name:'TypeA'});
CREATE(:v_sub{name:'SubA'});

MATCH(v1:v_user{name:'userA'}), (v2:v_title{name:'TitleA'})
CREATE (v1)-[:e_user_title{name:'(1)', val:1}]->(v2);

MATCH(v1:v_title{name:'TitleA'}), (v2:v_type{name:'TypeA'})
CREATE (v1)-[:e_title_type{name:'(2)', val:2}]->(v2);

MATCH(v1:v_type{name:'TypeA'}), (v2:v_sub{name:'SubA'})
CREATE (v1)-[:e_title_type{name:'(3)', val:3}]->(v2);

MATCH(n)-[e*3]->(n3) RETURN e;
MATCH(n)-[e*3]->(n3) RETURN head(e);
MATCH(n)-[e*3]->(n3) RETURN tail(e);
MATCH(n)-[e*3]->(n3) RETURN last(e);


DROP GRAPH cypher_dml2 CASCADE;

CREATE GRAPH cypher_dml2;
SET GRAPH_PATH to cypher_dml2;

CREATE ({id: 1})-[:e1]->({id: 2})-[:e1]->({id: 3})-[:e1]->({id: 4})
RETURN *;

MATCH (a {id: 1}), (b {id: 1})
CREATE (b)-[:e1]->(a)
RETURN *;

MATCH (a)
RETURN *;

MATCH (a)-[]-(a) RETURN *;
MATCH p=(a)-[]-(a) RETURN *;

EXPLAIN VERBOSE MATCH (a)-[]-(a) RETURN a;
EXPLAIN VERBOSE MATCH (a)-[]-(a) RETURN *;
EXPLAIN VERBOSE MATCH p=(a)-[]-(a) RETURN *;

DROP GRAPH cypher_dml2 CASCADE;
