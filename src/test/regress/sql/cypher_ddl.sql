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
SELECT graphname, labname, labkind FROM pg_catalog.ag_label;

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
SELECT labname, labkind FROM pg_catalog.ag_label;
DROP VLABEL v0 CASCADE;
DROP ELABEL e0 CASCADE;
DROP ELABEL e1;
SELECT labname, labkind FROM pg_catalog.ag_label;

--
-- DROP GRAPH
--

DROP GRAPH g;
DROP GRAPH g CASCADE;
SELECT labname, labkind FROM ag_label;

-- teardown

RESET ROLE;
DROP ROLE graph_role;
