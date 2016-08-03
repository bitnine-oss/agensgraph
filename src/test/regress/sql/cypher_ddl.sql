--
-- Cypher Query Language - DDL
--

-- check default graph objects
SELECT labname, labkind FROM ag_label;

--
-- CREATE/DROP LABEL
--

CREATE VLABEL vlabel_p1;
CREATE VLABEL vlabel_p2;

CREATE VLABEL vlabel_c1 inherits (vlabel_p1);
CREATE VLABEL vlabel_c2 inherits (vlabel_c1);
CREATE VLABEL vlabel_c3 inherits (vlabel_c2);
CREATE VLABEL vlabel_c4 inherits (vlabel_c3);
CREATE VLABEL vlabel_c5 inherits (vlabel_c4);
CREATE VLABEL vlabel_c6 inherits (vlabel_c4);
CREATE VLABEL vlabel_c7 inherits (vlabel_c1, vlabel_p2);

CREATE ELABEL elabel_p1;
CREATE ELABEL elabel_c1 inherits (elabel_p1);
CREATE ELABEL elabel_c2 inherits (elabel_c1);
CREATE ELABEL elabel_c3 inherits (elabel_c2);

SELECT labname, labkind FROM ag_label;

SELECT childlab.labname AS child, parentlab.labname AS parent
FROM ag_label AS parentlab, ag_label AS childlab, ag_inherits AS inh
WHERE childlab.oid = inh.inhrelid AND parentlab.oid = inh.inhparent;

DROP VLABEL vlabel_c4 CASCADE;

SELECT labname, labkind FROM ag_label;

SELECT childlab.labname AS child, parentlab.labname AS parent 
FROM ag_label AS parentlab, ag_label AS childlab, ag_inherits AS inh
WHERE childlab.oid = inh.inhrelid AND parentlab.oid = inh.inhparent;

-- wrong cases

CREATE VLABEL wrong_parent inherits (elabel_p1);
CREATE ELABEL wrong_parent inherits (vlabel_p1);

DROP TABLE vlabel_c7;
DROP TABLE elabel_c3;

DROP VLABEL nothing;

DROP ELABEL vlabel_c7;

DROP VLABEL vlabel_p1;
DROP VLABEL vlabel_c1;

DROP VLABEL vertex CASCADE;
DROP ELABEL edge CASCADE;

--
-- DROP ALL LABELS
--

DROP VLABEL vlabel_p1 CASCADE;
DROP VLABEL vlabel_p2 CASCADE;
DROP ELABEL elabel_p1 CASCADE;

SELECT labname, labkind FROM ag_label;

SELECT childlab.labname AS child, parentlab.labname AS parent 
FROM ag_label AS parentlab, ag_label AS childlab, ag_inherits AS inh
WHERE childlab.oid = inh.inhrelid AND parentlab.oid = inh.inhparent;
