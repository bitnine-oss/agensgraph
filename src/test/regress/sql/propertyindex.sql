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
CREATE GRAPH propidx;
SHOW graph_path;


CREATE VLABEL piv1;

CREATE PROPERTY INDEX ON piv1 (name);
CREATE PROPERTY INDEX ON piv1 (name.first, name.last);
CREATE PROPERTY INDEX ON piv1 ((name.first + name.last));
CREATE PROPERTY INDEX ON piv1 (age);
CREATE PROPERTY INDEX ON piv1 ((body.weight / body.height));

\d propidx.piv1
\dGi piv1*

-- Check property name & access method type
CREATE VLABEL piv2;

CREATE PROPERTY INDEX ON piv2 (name);
CREATE PROPERTY INDEX ON piv2 USING btree (name.first);
CREATE PROPERTY INDEX ON piv2 USING hash (name.first);
CREATE PROPERTY INDEX ON piv2 USING brin (name.first);
CREATE PROPERTY INDEX ON piv2 USING gin (name);
CREATE PROPERTY INDEX ON piv2 USING gist (name);

--CREATE PROPERTY INDEX ON piv2 USING gin ((self_intro::tsvector));
--CREATE PROPERTY INDEX ON piv2 USING gist ((hobby::tsvector));

\d propidx.piv2
\dGv+ piv2
\dGi piv2*

-- Concurrently build & if not exist
CREATE VLABEL piv3;

CREATE PROPERTY INDEX CONCURRENTLY ON piv3 (name.first);
CREATE PROPERTY INDEX IF NOT EXISTS piv3_first_idx ON piv3 (name.first);

-- Collation & Sort & NULL order
--CREATE PROPERTY INDEX ON piv3 (name.first COLLATE "C" ASC NULLS FIRST);

-- Tablespace
CREATE PROPERTY INDEX ON piv3 (name) TABLESPACE pg_default;

-- Storage parameter & partial index
CREATE PROPERTY INDEX ON piv3 (name.first) WITH (fillfactor = 80);
CREATE PROPERTY INDEX ON piv3 (name.first) WHERE (name IS NOT NULL);

\d propidx.piv3
\dGv+ piv3
\dGi piv3*

-- Unique property index
CREATE VLABEL piv4;

CREATE UNIQUE PROPERTY INDEX ON piv4 (id);
CREATE (:piv4 {id: 100});
CREATE (:piv4 {id: 100});

\d propidx.piv4
\dGv+ piv4
\dGi piv4*

-- Multi-column unique property index
CREATE VLABEL piv5;

CREATE UNIQUE PROPERTY INDEX ON piv5 (name.first, name.last);
CREATE (:piv5 {name: {first: 'agens'}});
CREATE (:piv5 {name: {first: 'agens'}});
CREATE (:piv5 {name: {first: 'agens', last: 'graph'}});
CREATE (:piv5 {name: {first: 'agens', last: 'graph'}});

\d propidx.piv5
\dGv+ piv5
\dGi piv5*

-- DROP PROPERTY INDEX
CREATE VLABEL piv6;

CREATE PROPERTY INDEX piv6_idx ON piv6 (name);

DROP PROPERTY INDEX piv6_idx;
DROP PROPERTY INDEX IF EXISTS piv6_idx;
DROP PROPERTY INDEX piv6_pkey;

DROP VLABEL piv6;

CREATE ELABEL pie1;
CREATE PROPERTY INDEX pie1_idx ON pie1 (reltype);

DROP PROPERTY INDEX pie1_idx;
DROP PROPERTY INDEX IF EXISTS pie1_idx;
DROP PROPERTY INDEX pie1_id_idx;
DROP PROPERTY INDEX pie1_start_idx;
DROP PROPERTY INDEX pie1_end_idx;

DROP ELABEL pie1;

CREATE VLABEL piv7;

CREATE PROPERTY INDEX piv7_multi_col ON piv7 (name.first, name.middle, name.last);
\dGv+ piv7
\dGi piv7*
DROP PROPERTY INDEX piv7_multi_col;

CREATE PROPERTY INDEX piv7_multi_expr ON piv7 ((name.first + name.last), age);
\dGv+ piv7
\dGi piv7*
DROP PROPERTY INDEX piv7_multi_expr;

DROP VLABEL piv7;

-- wrong case
CREATE VLABEL piv8;

CREATE PROPERTY INDEX piv8_index_key1 ON piv8 (key1);
CREATE PROPERTY INDEX piv8_index_key1 ON piv8 (key1);

CREATE PROPERTY INDEX ON nonexsist_name (key1);

DROP VLABEL piv8;

CREATE VLABEL piv9;

CREATE PROPERTY INDEX piv9_property_index_key1 ON piv9 (key1);
DROP INDEX propidx.piv9_property_index_key1;

CREATE INDEX piv9_index_key1 ON propidx.piv9 (properties);
DROP PROPERTY INDEX piv9_index_key1;

DROP VLABEL piv9;

-- Subscripted property keys
--
-- A subscript is the name of the property being read only where it is a string.
-- Every other constant a subscript may hold was read as one anyway, which took
-- the backend down, so each of them is covered here.
CREATE VLABEL piv10;

-- a string subscript is the name, and the last one in the path wins
CREATE PROPERTY INDEX ON piv10 (m['a']);
CREATE PROPERTY INDEX ON piv10 (m['a'][0]);
-- a number, a boolean and a null name nothing, so the property the path starts
-- from names the index
CREATE PROPERTY INDEX ON piv10 (tags[0]);
CREATE PROPERTY INDEX ON piv10 (tags[-1]);
CREATE PROPERTY INDEX ON piv10 (t1[1.5]);
CREATE PROPERTY INDEX ON piv10 (t2[true]);
CREATE PROPERTY INDEX ON piv10 (t3[NULL]);
-- nor does a slice or a subscript that is not a constant at all
CREATE PROPERTY INDEX ON piv10 (t4[1..3]);
CREATE PROPERTY INDEX ON piv10 (t5[1 + 1]);
-- one subscript of each kind in the one index
CREATE PROPERTY INDEX ON piv10 (m['a'], tags[0]);

\dGi piv10*

-- With rows to build over, the subscript is evaluated, and it answers what a
-- read of it answers: a position in a list and a key in a map resolve, a string
-- into a list and a number into a map are refused.
CREATE VLABEL piv11;
CREATE (:piv11 {tags: ['x', 'y'], m: {a: 5}});

CREATE PROPERTY INDEX ON piv11 (tags[0]);
CREATE PROPERTY INDEX ON piv11 (m['a']);
CREATE PROPERTY INDEX ON piv11 (tags['a']);
CREATE PROPERTY INDEX ON piv11 (m[0]);

-- and the index over a list position answers the read that matches it
MATCH (n:piv11) WHERE n.tags[0] = 'x' RETURN count(*);

\dGi piv11*

-- A subscript is printed back as the value it is, in an index's key, in the
-- predicate of a partial one and in a constraint's assertion.  Printed without
-- its quotes it reads back as a different thing -- the property a rather than
-- the key a of the property m -- so a dump would carry a different index, and
-- for a unique one a different constraint.
CREATE VLABEL piv12;

CREATE UNIQUE PROPERTY INDEX piv12_uq ON piv12 (m['a']);
CREATE PROPERTY INDEX piv12_part ON piv12 (name) WHERE (m['a'] IS NOT NULL);
CREATE CONSTRAINT piv12_chk ON piv12 ASSERT m['b'] IS NOT NULL;

\dGv+ piv12

-- the unique index refuses a second row with the same m.a
CREATE (:piv12 {m: {a: 1, b: 1}});
CREATE (:piv12 {m: {a: 1, b: 1}});

-- and rebuilt from exactly what is printed above, it refuses it still
DROP PROPERTY INDEX piv12_uq;
CREATE UNIQUE PROPERTY INDEX piv12_uq ON piv12 USING btree (m['a']);
CREATE (:piv12 {m: {a: 1, b: 1}});
MATCH (n:piv12) RETURN count(*);

-- teardown

RESET ROLE;

-- Drop the test role and everything it still owns, so the regression database
-- does not carry objects owned by a role a freshly initialized cluster lacks
-- (which would break the pg_dump/pg_restore and pg_upgrade round-trips exercised
-- by src/bin/pg_upgrade).  Silence the cascade notices for stable output.
SET client_min_messages = warning;
DROP GRAPH propidx CASCADE;
DROP ROLE regressrole;
RESET client_min_messages;
