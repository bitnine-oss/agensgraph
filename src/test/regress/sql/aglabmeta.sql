--
-- AgensGraph catalog aglabmeta test
--
CREATE GRAPH labmeta;
SET graph_path = labmeta;

-- create edge

CREATE (:human)-[:know]->(:human {age:1});
CREATE (:human)-[:know]->(:human {age:2});
CREATE (:human)-[:know]->(:human {age:3});
CREATE (:dog)-[:follow]->(:human);
CREATE (:dog)-[:likes]->(:dog);

SELECT * FROM ag_labname_meta ORDER BY start, edge, "end";

-- create multiple edges

CREATE (:human)-[:know]->(:human)-[:follow]->(:human)-[:hate]->(:human)-[:love]->(:human);
CREATE (:human)-[:know]->(:human)-[:follow]->(:human)-[:hate]->(:human)-[:love]->(:human);
CREATE (:human)-[:know]->(:human)-[:follow]->(:human)-[:hate]->(:human)-[:love]->(:human);

SELECT * FROM ag_labname_meta ORDER BY start, edge, "end";

-- create repeated edges;

CREATE (:human)-[:know]->(:human)-[:know]->(:human)-[:know]->(:human)-[:know]->(:human);

SELECT * FROM ag_labname_meta ORDER BY start, edge, "end";

-- delete edge

MATCH (a)-[r:love]->(b)
DELETE r;

SELECT * FROM ag_labname_meta ORDER BY start, edge, "end";

-- drop elabel

DROP ELABEL hate CASCADE;

SELECT * FROM ag_labname_meta ORDER BY start, edge, "end";

-- drop vlabel

DROP VLABEL human CASCADE;

SELECT * FROM ag_labname_meta ORDER BY start, edge, "end";

-- drop graph

DROP GRAPH labmeta CASCADE;

SELECT * FROM ag_labname_meta ORDER BY start, edge, "end";

-- Sub Transaction

-- Rollback
CREATE GRAPH labmeta;

BEGIN;
	CREATE (:dog)-[:follow]->(:human);
	SAVEPOINT sv1;
	CREATE (:dog)-[:likes]->(:cat);
	ROLLBACK TO SAVEPOINT sv1;
	CREATE (:human)-[:love]->(:dog);
	SELECT * FROM ag_labname_meta ORDER BY start, edge, "end";
COMMIT;

SELECT * FROM ag_labname_meta ORDER BY start, edge, "end";

-- Release
BEGIN;
	CREATE (:dog)-[:likes]->(:cat);
	SAVEPOINT sv2;
	CREATE (:cat)-[:ignore]->(:dog);
	RELEASE SAVEPOINT sv2;
COMMIT;

SELECT * FROM ag_labname_meta ORDER BY start, edge, "end";

-- cleanup

DROP GRAPH labmeta CASCADE;
