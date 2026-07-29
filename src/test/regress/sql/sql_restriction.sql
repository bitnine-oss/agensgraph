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

-- MERGE writes a label the same way the three above do
CREATE TABLE mergesrc (k int);
MERGE INTO g.v AS t USING mergesrc AS s ON (t.properties ->> 'k')::int = s.k
	WHEN MATCHED THEN UPDATE SET properties = '{"merge":"impossible"}'
	WHEN NOT MATCHED THEN INSERT (properties) VALUES ('{"merge":"impossible"}');

-- The refusal does not depend on which actions the MERGE carries, because any of
-- them can write: an insert-only one, an update-only one, a delete-only one, and
-- one whose only action is to do nothing are all refused, so nothing has to be
-- known about what the statement would have done.
INSERT INTO mergesrc VALUES (1);
MERGE INTO g.v AS t USING mergesrc AS s ON (t.properties ->> 'k')::int = s.k
	WHEN NOT MATCHED THEN INSERT (properties) VALUES ('{"merge":"impossible"}');
MERGE INTO g.v AS t USING mergesrc AS s ON (t.properties ->> 'k')::int = s.k
	WHEN MATCHED THEN UPDATE SET properties = '{"merge":"impossible"}';
MERGE INTO g.v AS t USING mergesrc AS s ON (t.properties ->> 'k')::int = s.k
	WHEN MATCHED THEN DELETE;
MERGE INTO g.v AS t USING mergesrc AS s ON (t.properties ->> 'k')::int = s.k
	WHEN NOT MATCHED THEN DO NOTHING;
-- nothing was written by any of them
SELECT count(*) AS label_rows FROM g.v;

-- A label as the SOURCE of a MERGE is only read, so it is not refused: the target
-- is what decides, exactly as it does for INSERT ... SELECT.
CREATE TABLE mergetgt (k int);
MERGE INTO mergetgt AS t USING g.v AS s ON t.k = 1
	WHEN NOT MATCHED THEN INSERT (k) VALUES (1);
SELECT count(*) AS target_rows FROM mergetgt;
DROP TABLE mergetgt;

-- And it is the same permission the other three ask for: with it granted the
-- MERGE goes through, and the label really is written.
SET enable_graph_dml = on;
MERGE INTO g.v AS t USING mergesrc AS s ON (t.properties ->> 'k')::int = s.k
	WHEN NOT MATCHED THEN INSERT (properties) VALUES ('{"merge":"allowed"}');
RESET enable_graph_dml;
SELECT count(*) AS written FROM g.v WHERE properties ? 'merge';
MERGE INTO g.v AS t USING mergesrc AS s ON (t.properties ->> 'k')::int = s.k
	WHEN MATCHED THEN UPDATE SET properties = '{"merge":"impossible"}';
SELECT properties FROM g.v WHERE properties ? 'merge';
DROP TABLE mergesrc;

-- cleanup
REVOKE ALL ON DATABASE regression FROM tmp;
DROP GRAPH t CASCADE;
DROP ROLE tmp;
DROP GRAPH g CASCADE;
