--
-- SQL Restriction with Graph objects
--

-- prepare
CREATE GRAPH g;
CREATE ROLE tmp;

--
-- ALTER SCHEMA
--

ALTER SCHEMA g RENAME TO s;
ALTER SCHEMA g OWNER TO tmp;

--
-- CREATE TABLE
--

CREATE TABLE g.t (i int);
CREATE TABLE t (i int) INHERITS (g.ag_vertex);

--
-- ALTER TABLE
--

GRANT ALL ON DATABASE regression TO tmp;
SET ROLE tmp;

CREATE GRAPH t;
SET graph_path = t;

CREATE VLABEL v;

ALTER TABLE t.v ADD COLUMN tmp int;

-- one the reshape gate does not cover, so it is the superuser restriction that
-- refuses it even though this role owns the label
ALTER TABLE t.v ADD CONSTRAINT tmp_chk CHECK (true);

-- and one that neither refuses: the label's owner may still tune it
ALTER TABLE t.v ALTER COLUMN properties SET STATISTICS 100;

RESET ROLE;

SET graph_path = g;

CREATE VLABEL v;
ALTER TABLE g.v RENAME TO e;

--
-- TRUNCATE TABLE
--
-- A label holding rows cannot be truncated: TRUNCATE would bypass the Cypher
-- graph-consistency machinery (orphaning incident edges, staling ag_graphmeta).
CREATE (:v);
TRUNCATE TABLE g.v;
-- Truncating an empty label is a no-op and is allowed, so a freshly created
-- label can be reloaded by a parallel pg_restore.
CREATE VLABEL vacant;
TRUNCATE TABLE g.vacant;

--
-- TRIGGER
--

CREATE TRIGGER tt AFTER INSERT ON g.v
FOR EACH STATEMENT EXECUTE PROCEDURE ff();

--
-- RULE
--

CREATE VLABEL v2;
CREATE RULE rr AS ON INSERT TO g.v DO INSTEAD
	INSERT INTO g.v2 VALUES (new.id, new.properties);

--
-- DML
--

INSERT INTO g.v VALUES ('1234.56', NULL);
UPDATE g.v SET properties='{"update":"impossible"}' WHERE id = '1234.56';
DELETE FROM g.v;

-- cleanup
REVOKE ALL ON DATABASE regression FROM tmp;
DROP GRAPH t CASCADE;
DROP ROLE tmp;
DROP GRAPH g CASCADE;
