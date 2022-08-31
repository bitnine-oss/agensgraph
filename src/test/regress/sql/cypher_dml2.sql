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

-- Trigger
CREATE TEMPORARY TABLE tmp (a graphid PRIMARY KEY);

CREATE OR REPLACE FUNCTION v1_test_trigger_func()
returns trigger
AS $$
DECLARE
BEGIN
    CASE WHEN new.id IS NULL
    THEN
        DELETE FROM tmp WHERE tmp.a = old.id;
        RETURN new;
    ELSE
        INSERT INTO tmp VALUES (new.id);
        RETURN new;
    END CASE;
END; $$
LANGUAGE 'plpgsql';

create trigger v1_test_trigger
    after insert or delete or update on cypher_dml2.v1
	for each row
    execute procedure v1_test_trigger_func();

CREATE (v1:v1 {name: 'trigger_item'}) RETURN v1;

SELECT a, pg_typeof(a) FROM tmp;

MATCH (n) DETACH DELETE n;

SELECT a, pg_typeof(a) FROM tmp;

DROP GRAPH cypher_dml2 CASCADE;
