--
-- vertex_labels()
--

-- prepare
DROP GRAPH IF EXISTS vertex_labels_simple CASCADE;
DROP GRAPH IF EXISTS vertex_labels_complex1 CASCADE;
DROP GRAPH IF EXISTS vertex_labels_complex2 CASCADE;
CREATE GRAPH vertex_labels_simple;
CREATE GRAPH vertex_labels_complex1;
CREATE GRAPH vertex_labels_complex2;

-- simple test

SET graph_path = vertex_labels_simple;

--         a
--         |
-- b       c
-- |       |
-- +-- d --+
CREATE VLABEL a;
CREATE VLABEL b;
CREATE VLABEL c INHERITS (a);
CREATE VLABEL d INHERITS (b, c);

CREATE (:a {name: 'a'});
CREATE (:b {name: 'b'});
CREATE (:c {name: 'c'});
CREATE (:d {name: 'd'});

MATCH (n) RETURN n.name, label(n);

MATCH (n) RETURN n.name, labels(n);
MATCH (n) RETURN n.name, labels(n)[0];
MATCH (n:c) RETURN n.name, labels(n)[1];
MATCH (n:d) RETURN n.name, labels(n)[2], labels(n)[3];

-- complex test 1

SET graph_path = vertex_labels_complex1;

--             a
--             |
--             b       c
--             |       |
-- +-- d --+   +-- e --+   f   +-- g
-- |   |   |       |       |   |   |
-- h   i   j       +------ k --+   |
--     |   |               |       |
--     +---+------ l ------+-------+
CREATE VLABEL a;
CREATE VLABEL b INHERITS (a);
CREATE VLABEL c;
CREATE VLABEL d;
CREATE VLABEL e INHERITS (b, c);
CREATE VLABEL f;
CREATE VLABEL g;
CREATE VLABEL h INHERITS (d);
CREATE VLABEL i INHERITS (d);
CREATE VLABEL j INHERITS (d);
CREATE VLABEL k INHERITS (e, f, g);
CREATE VLABEL l INHERITS (i, j, k, g);

CREATE (:a {name: 'a'});
CREATE (:b {name: 'b'});
CREATE (:c {name: 'c'});
CREATE (:d {name: 'd'});
CREATE (:e {name: 'e'});
CREATE (:f {name: 'f'});
CREATE (:g {name: 'g'});
CREATE (:h {name: 'h'});
CREATE (:i {name: 'i'});
CREATE (:j {name: 'j'});
CREATE (:k {name: 'k'});
CREATE (:l {name: 'l'});

MATCH (n) RETURN n.name, label(n), labels(n);

-- complex test 2

SET graph_path = vertex_labels_complex2;

-- +-- a ----------+
-- |   |       b   |
-- |   |       |   |
-- |   +-- d --+   |
-- |       |       |
-- |       e --+-- f
-- |           |
-- +-- c       g
--     |       |
--     +-- h --+-- i
--         |       |
--         +-- j --+
CREATE VLABEL a;
CREATE VLABEL b;
CREATE VLABEL c INHERITS (a);
CREATE VLABEL d INHERITS (a, b);
CREATE VLABEL e INHERITS (d);
CREATE VLABEL f INHERITS (a);
CREATE VLABEL g INHERITS (e, f);
CREATE VLABEL h INHERITS (c, g);
CREATE VLABEL i INHERITS (g);
CREATE VLABEL j INHERITS (h, i);

CREATE (:a {name: 'a'});
CREATE (:b {name: 'b'});
CREATE (:c {name: 'c'});
CREATE (:d {name: 'd'});
CREATE (:e {name: 'e'});
CREATE (:f {name: 'f'});
CREATE (:g {name: 'g'});
CREATE (:h {name: 'h'});
CREATE (:i {name: 'i'});
CREATE (:j {name: 'j'});

MATCH (n) RETURN n.name, label(n), labels(n);

-- cleanup
DROP GRAPH vertex_labels_complex2 CASCADE;
DROP GRAPH vertex_labels_complex1 CASCADE;
DROP GRAPH vertex_labels_simple CASCADE;
