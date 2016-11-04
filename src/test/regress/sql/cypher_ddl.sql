--
-- Cypher Query Language - DDL
--

-- setup

DROP ROLE IF EXISTS graph_role;
CREATE ROLE graph_role SUPERUSER;
SET ROLE graph_role;

--
-- CREATE GRAPH
--

SHOW graph_path;
CREATE GRAPH g;
SHOW graph_path;

-- check default graph objects
\dGl

--
-- ALTER GRAPH
--

CREATE ROLE temp;
ALTER GRAPH g RENAME TO p;
\dG
ALTER GRAPH p RENAME TO g;

ALTER GRAPH g OWNER TO temp;
\dG
ALTER GRAPH g OWNER TO graph_role;

--
-- SET graph_path
--

SET graph_path = n;
SET graph_path = n, m;

--
-- CREATE label
--

CREATE VLABEL v0;

CREATE VLABEL v00 INHERITS (v0);
CREATE VLABEL v01 INHERITS (v0);
CREATE VLABEL v1 INHERITS (v00, v01);

CREATE ELABEL e0;
CREATE ELABEL e01 INHERITS (e0);
CREATE ELABEL e1;

SELECT labname, labkind FROM pg_catalog.ag_label;

SELECT child.labname AS child, parent.labname AS parent
FROM pg_catalog.ag_label AS parent,
     pg_catalog.ag_label AS child,
     pg_catalog.pg_inherits AS inh
WHERE child.relid = inh.inhrelid AND parent.relid = inh.inhparent
ORDER BY 1, 2;

-- wrong cases

CREATE VLABEL wrong_parent INHERITS (e1);
CREATE ELABEL wrong_parent INHERITS (v1);

-- CREATE UNLOGGED
CREATE UNLOGGED VLABEL unlog;
SELECT l.labname as name, c.relpersistence as persistence
FROM pg_catalog.ag_label l
     LEFT JOIN pg_catalog.pg_class c ON c.oid = l.relid
ORDER BY 1;

-- IF NOT EXISTS
CREATE VLABEL dup;
CREATE VLABEL dup;
CREATE VLABEL IF NOT EXISTS dup;

-- WITH
CREATE VLABEL stor
WITH (fillfactor=90, autovacuum_enabled, autovacuum_vacuum_threshold=100);
SELECT l.labname as name, c.reloptions as options
FROM pg_catalog.ag_label l
     LEFT JOIN pg_catalog.pg_class c ON c.oid = l.relid
ORDER BY 1;

-- TABLESPACE
CREATE VLABEL tblspc TABLESPACE pg_default;

--
-- COMMENT and \dG commands
--

COMMENT ON GRAPH g IS 'a graph for regression tests';
COMMENT ON VLABEL v1 IS 'multiple inheritance test';

\dG+
\dGv+
\dGe+

--
-- ALTER LABEL
--

-- skip alter tablespace test, tablespace location must be an absolute path

ALTER VLABEL v0 SET STORAGE external;
\d+ g.v0

ALTER VLABEL v0 RENAME TO vv;
\dGv
ALTER VLABEL vv RENAME TO v0;

SELECT relname, rolname FROM pg_class c, pg_roles r
WHERE relname='v0' AND c.relowner = r.oid;
ALTER VLABEL v0 owner TO temp;

SELECT relname, rolname FROM pg_class c, pg_roles r
WHERE relname='v0' AND c.relowner = r.oid;
ALTER VLABEL v0 owner TO graph_role;
DROP ROLE temp;

SELECT indisclustered FROM pg_index WHERE indrelid = 'g.v0'::regclass;
ALTER VLABEL v0 CLUSTER ON v0_pkey;
SELECT indisclustered FROM pg_index WHERE indrelid = 'g.v0'::regclass;
ALTER VLABEL v0 SET WITHOUT CLUSTER;
SELECT indisclustered FROM pg_index WHERE indrelid = 'g.v0'::regclass;

SELECT relpersistence FROM pg_class WHERE relname = 'v0';
ALTER VLABEL v0 SET UNLOGGED;
SELECT relpersistence FROM pg_class WHERE relname = 'v0';
ALTER VLABEL v0 SET LOGGED;
SELECT relpersistence FROM pg_class WHERE relname = 'v0';

\d g.v1
ALTER VLABEL v1 NO INHERIT v00;
\d g.v1
ALTER VLABEL v1 INHERIT v00;
\d g.v1
ALTER VLABEL v1 INHERIT ag_vertex;		--should fail
ALTER VLABEL v1 NO INHERIT ag_vertex;	--should fail

SELECT relreplident FROM pg_class WHERE relname = 'v0';
ALTER VLABEL v0 REPLICA IDENTITY full;
SELECT relreplident FROM pg_class WHERE relname = 'v0';
ALTER VLABEL v0 REPLICA IDENTITY default;

--
-- DROP LABEL
--

-- wrong cases

DROP TABLE g.v1;
DROP TABLE g.e1;

DROP VLABEL unknown;
DROP ELABEL unknown;

DROP VLABEL e1;
DROP ELABEL v1;

DROP VLABEL v0;
DROP VLABEL v00;
DROP ELABEL e0;

DROP VLABEL ag_vertex CASCADE;
DROP ELABEL ag_edge CASCADE;

-- drop all

DROP VLABEL v01 CASCADE;
SELECT labname, labkind FROM pg_catalog.ag_label ORDER BY 2, 1;
DROP VLABEL v0 CASCADE;
DROP ELABEL e0 CASCADE;
DROP ELABEL e1;
SELECT labname, labkind FROM pg_catalog.ag_label;

--
-- CONSTRAINT
--
\set VERBOSITY terse

-- simple unique constraint
CREATE VLABEL regv1;

CREATE CONSTRAINT ON regv1 ASSERT a.b IS UNIQUE;

CREATE (:regv1 {'a':{'b':'agens', 'c':'graph'}});
CREATE (:regv1 {'a':{'b':'agens', 'c':'graph'}});
CREATE (:regv1 {'a':{'b':'agens'}});
CREATE (:regv1 {'a':{'b':'c'}});
CREATE (:regv1 {'a':'b'});
CREATE (:regv1 {'a':'agens-graph'});

DROP VLABEL regv1;

-- expr unique constraint
CREATE ELABEL rege1;

CREATE CONSTRAINT ON rege1 ASSERT c || d IS UNIQUE;

CREATE ()-[:rege1 {'c':'agens', 'd':'graph'}]->();
CREATE ()-[:rege1 {'c':'agens', 'd':'graph'}]->();
CREATE ()-[:rege1 {'c':'agens', 'd':'rdb'}]->();
CREATE ()-[:rege1 {'c':'agen', 'd':'sgraph'}]->();

DROP ELABEL rege1;

-- simple not null constraint
CREATE VLABEL regv2;

CREATE CONSTRAINT ON regv2 ASSERT name IS NOT NULL;

CREATE (:regv2 {'name':'agens'});
CREATE (:regv2 {'age':'0'});
CREATE (:regv2 {'age':'0', 'name':'graph'});
CREATE (:regv2 {'name':NULL});

DROP VLABEL regv2;

-- multi not null constraint
CREATE VLABEL regv3;

CREATE CONSTRAINT ON regv3 ASSERT (name.first, name.last) IS NOT NULL;

CREATE (:regv3 {'name':'agens'});
CREATE (:regv3 {'name':{'first':'agens', 'last':'graph'}});
CREATE (:regv3 {'name':{'first':'agens'}});
CREATE (:regv3 {'name':{'last':'graph'}});
CREATE (:regv3 {'name':{'first':NULL, 'last':NULL}});

DROP VLABEL regv3;

-- simple check constraint
CREATE ELABEL rege2;

CREATE CONSTRAINT ON rege2 ASSERT a != b;

CREATE ()-[:rege2 {'a':'agens', 'b':'graph'}]->();
CREATE ()-[:rege2 {'a':'agens', 'b':'agens'}]->();
CREATE ()-[:rege2 {'a':'agens', 'b':'AGENS'}]->();
CREATE ()-[:rege2 {'a':'agens', 'd':'graph'}]->();

DROP ELABEL rege2;

-- expression check constraint
CREATE VLABEL regv4;

CREATE CONSTRAINT ON regv4 ASSERT (length(password) > 8 AND length(password) < 16);

CREATE (:regv4 {'password':'12345678'});
CREATE (:regv4 {'password':'123456789'});
CREATE (:regv4 {'password':'123456789012345'});
CREATE (:regv4 {'password':'1234567890123456'});

DROP VLABEL regv4;

-- IN check constraint
CREATE ELABEL rege3;

CREATE CONSTRAINT ON rege3 ASSERT type IN ('friend','lover','parent');

CREATE ()-[:rege3 {'type':'friend', 'name':'agens'}]->();
CREATE ()-[:rege3 {'type':'love', 'name':'graph'}]->();
CREATE ()-[:rege3 {'type':'parents', 'name':'AGENS'}]->();
CREATE ()-[:rege3 {'type':'lover', 'name':'GRAPH'}]->();

DROP ELABEL rege3;

-- case check constraint
CREATE VLABEL regv5;

CREATE CONSTRAINT ON regv5 ASSERT lower(btrim(id)) IS UNIQUE;

CREATE (:regv5 {'id':'agens'});
CREATE (:regv5 {'id':' agens'});
CREATE (:regv5 {'id':'agens '});
CREATE (:regv5 {'id':'AGENS'});
CREATE (:regv5 {'id':' AGENS '});
CREATE (:regv5 {'id':'GRAPH'});
CREATE (:regv5 {'id':' graph '});

DROP VLABEL regv5;

-- typecast check constraint
CREATE VLABEL regv6;

CREATE CONSTRAINT ON regv6 ASSERT age::int > 0 AND age::int < 128;

CREATE (:regv6 {'age':'0'});
CREATE (:regv6 {'age':'1'});
CREATE (:regv6 {'age':'127'});
CREATE (:regv6 {'age':'128'});
CREATE (:regv6 {'age':'-10'});
CREATE (:regv6 {'age':1 + 1});
CREATE (:regv6 {'age':1 + 127});

DROP VLABEL regv6;

-- IS NULL constraint
CREATE ELABEL rege4;

CREATE CONSTRAINT rege4_name_isnull_constraint ON rege4 ASSERT id IS NULL;

CREATE ()-[:rege4 {'id':NULL, 'name':'agens'}]->();
CREATE ()-[:rege4 {'id':10, 'name':'agens'}]->();
CREATE ()-[:rege4 {'name':'graph'}]->();

DROP CONSTRAINT rege4_name_isnull_constraint ON ag_edge;
DROP CONSTRAINT ON rege4;
DROP CONSTRAINT rege4_name_isnull_constraint ON rege4;
DROP ELABEL rege4;

-- Indirection constraint

CREATE VLABEL regv7;

CREATE CONSTRAINT ON regv7 ASSERT a.b[0].c IS NOT NULL;

CREATE (:regv7 {'a':{'b':ARRAY[{'c':'d'},{'c':'e'}]}});
CREATE (:regv7 {'a':{'b':ARRAY[{'c':'d'},{'e':'e'}]}});
CREATE (:regv7 {'a':{'b':ARRAY[{'d':'d'},{'e':'e'}]}});

DROP VLABEL regv7;

-- wrong case

CREATE VLABEL regv8;

CREATE CONSTRAINT ON regv8 ASSERT (select * from graph.regv8).c IS NOT NULL;
CREATE CONSTRAINT ON regv8 ASSERT (1).c IS NOT NULL;
CREATE CONSTRAINT ON regv8 ASSERT ($1).c IS NOT NULL;

DROP VLABEL regv8;
--
-- DROP GRAPH
--

DROP GRAPH g;
DROP GRAPH g CASCADE;
SELECT labname, labkind FROM ag_label;

-- teardown

RESET ROLE;
DROP ROLE graph_role;
