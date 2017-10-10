--
-- Cypher Query Language - Property Index
--
DROP ROLE IF EXISTS regressrole;
CREATE ROLE regressrole SUPERUSER;
SET ROLE regressrole;

--
-- CREATE GRAPH
--

SHOW graph_path;
CREATE GRAPH g;
SHOW graph_path;


CREATE VLABEL regv1;

CREATE PROPERTY INDEX ON regv1 (name);
CREATE PROPERTY INDEX ON regv1 (name.first, name.last);
CREATE PROPERTY INDEX ON regv1 ((name.first + name.last));
CREATE PROPERTY INDEX ON regv1 (age);
CREATE PROPERTY INDEX ON regv1 ((body.weight / body.height));

\d g.regv1
\dGi
DROP VLABEL regv1;

-- Check property name & access method type
CREATE VLABEL regv1;

CREATE PROPERTY INDEX ON regv1 (name);
CREATE PROPERTY INDEX ON regv1 USING btree (name.first);
CREATE PROPERTY INDEX ON regv1 USING hash (name.first);
CREATE PROPERTY INDEX ON regv1 USING brin (name.first);
CREATE PROPERTY INDEX ON regv1 USING gin (name);
CREATE PROPERTY INDEX ON regv1 USING gist (name);

--CREATE PROPERTY INDEX ON regv1 USING gin ((self_intro::tsvector));
--CREATE PROPERTY INDEX ON regv1 USING gist ((hobby::tsvector));

\d g.regv1
\dGv+ regv1
\dGi

DROP VLABEL regv1;

-- Concurrently build & if not exist
CREATE VLABEL regv1;

CREATE PROPERTY INDEX CONCURRENTLY ON regv1 (name.first);
CREATE PROPERTY INDEX IF NOT EXISTS regv1_first_idx ON regv1 (name.first);

-- Collation & Sort & NULL order
--CREATE PROPERTY INDEX ON regv1 (name.first COLLATE "C" ASC NULLS FIRST);

-- Tablespace
CREATE PROPERTY INDEX ON regv1 (name) TABLESPACE pg_default;

-- Storage parameter & partial index
CREATE PROPERTY INDEX ON regv1 (name.first) WITH (fillfactor = 80);
CREATE PROPERTY INDEX ON regv1 (name.first) WHERE (name IS NOT NULL);

\d g.regv1
\dGv+ regv1
\dGi
DROP VLABEL regv1;

-- Unique property index
CREATE VLABEL regv1;

CREATE UNIQUE PROPERTY INDEX ON regv1 (id);
CREATE (:regv1 {id: 100});
CREATE (:regv1 {id: 100});

\d g.regv1
\dGv+ regv1
\dGi
DROP VLABEL regv1;

-- Multi-column unique property index
CREATE VLABEL regv1;

CREATE UNIQUE PROPERTY INDEX ON regv1 (name.first, name.last);
CREATE (:regv1 {name: {first: 'agens'}});
CREATE (:regv1 {name: {first: 'agens'}});
CREATE (:regv1 {name: {first: 'agens', last: 'graph'}});
CREATE (:regv1 {name: {first: 'agens', last: 'graph'}});

\d g.regv1
\dGv+ regv1
\dGi
DROP VLABEL regv1;

-- DROP PROPERTY INDEX
CREATE VLABEL regv1;

CREATE PROPERTY INDEX regv1_idx ON regv1 (name);

DROP PROPERTY INDEX regv1_idx;
DROP PROPERTY INDEX IF EXISTS regv1_idx;
DROP PROPERTY INDEX regv1_pkey;

DROP VLABEL regv1;

CREATE ELABEL rege1;
CREATE PROPERTY INDEX rege1_idx ON rege1 (reltype);

DROP PROPERTY INDEX rege1_idx;
DROP PROPERTY INDEX IF EXISTS rege1_idx;
DROP PROPERTY INDEX rege1_id_idx;
DROP PROPERTY INDEX rege1_start_idx;
DROP PROPERTY INDEX rege1_end_idx;

DROP ELABEL rege1;

CREATE VLABEL regv1;

CREATE PROPERTY INDEX regv1_multi_col ON regv1 (name.first, name.middle, name.last);
\dGv+ regv1
\dGi
DROP PROPERTY INDEX regv1_multi_col;

CREATE PROPERTY INDEX regv1_multi_expr ON regv1 ((name.first + name.last), age);
\dGv+ regv1
\dGi
DROP PROPERTY INDEX regv1_multi_expr;

DROP VLABEL regv1;

-- wrong case
CREATE VLABEL regv2;

CREATE PROPERTY INDEX regv2_index_key1 ON regv2 (key1);
CREATE PROPERTY INDEX regv2_index_key1 ON regv2 (key1);

CREATE PROPERTY INDEX ON nonexsist_name (key1);

DROP VLABEL regv2;
--
-- DROP GRAPH
--
DROP GRAPH g CASCADE;

RESET ROLE;
DROP ROLE regressrole;
