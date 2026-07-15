CREATE EXTENSION meta;

-- meta.graphs()
SELECT meta.graphs();
CREATE GRAPH graph1;
CREATE GRAPH graph2;
SELECT meta.graphs();
DROP GRAPH graph1 CASCADE;
SELECT meta.graphs();

-- clean up
DROP GRAPH graph2 CASCADE;

--
-- meta.labels(graph_name), meta.vertex_labels(graph_name), meta.edge_labels(graph_name)
--
SELECT meta.labels();
CREATE GRAPH graph1;
SET graph_path = graph1;
SELECT meta.labels();
SELECT meta.vertex_labels();
SELECT meta.edge_labels();
CREATE (:person {name: 'Alice'});
CREATE (:person {name: 'Bob'});
CREATE (:animal {name: 'Fluffy'});
CREATE (:animal {name: 'Spot'});
MATCH (a:person {name: 'Alice'}), (b:animal {name: 'Fluffy'})
CREATE (a)-[:"HAS_PET"]->(b);

SELECT meta.labels();
SELECT meta.labels('graph1');
SELECT meta.vertex_labels();
SELECT meta.vertex_labels('graph1');
SELECT meta.edge_labels();
SELECT meta.edge_labels('graph1');

--
-- meta.label_id(label_name, graph_name), meta.label_kind(label_name, graph_name), meta.label_name(label_id, graph_name)
--
SELECT meta.label_id('person');
SELECT meta.label_id('HAS_PET');

SELECT meta.label_kind('person');
SELECT meta.label_kind('HAS_PET');

SELECT meta.label_name(1);
SELECT meta.label_name(2);
SELECT meta.label_name(3);
SELECT meta.label_name(4);

SELECT meta.label_id('hello');
SELECT meta.label_kind('hello');
SELECT meta.label_name(100);

--
-- meta.graph_size(graph_name), meta.label_size(label_name, graph_name)
--
SELECT meta.graph_size();
SELECT meta.graph_size('graph1');
SELECT meta.graph_size('graph1', pretty => false);

SELECT meta.label_size('person');
SELECT meta.label_size('animal');
SELECT meta.label_size('HAS_PET', pretty => false);

SELECT meta.graph_size('unknown');
SELECT meta.label_size('unknown');

--
-- meta.count(), meta.estimated_count()
--
SELECT meta.count('person');
SELECT meta.estimated_count('person');

ANALYZE graph1.person;
SELECT meta.estimated_count('person');

SELECT meta.count('unknown');
SELECT meta.estimated_count('unknown');

--
-- meta.graph_exists(graph_name), meta.label_exists(label_name, graph_name)
-- meta.vertex_label_exists(label_name, graph_name), meta.edge_label_exists(label_name, graph_name)
--
SET graph_path = graph1;
SELECT meta.graph_exists('graph1');
SELECT meta.graph_exists('unknown');

SELECT meta.label_exists('person');
SELECT meta.label_exists('HAS_PET', 'graph1');
SELECT meta.label_exists('unknown');

SELECT meta.vertex_label_exists('person');
SELECT meta.vertex_label_exists('animal', 'graph1');
SELECT meta.vertex_label_exists('HAS_PET');

SELECT meta.edge_label_exists('HAS_PET');
SELECT meta.edge_label_exists('person');
SELECT meta.edge_label_exists('unknown');

--
-- meta.get_child_labels(label_name, graph_name), meta.get_parent_labels(label_name, graph_name)
--
CREATE VLABEL employee INHERITS (person);

SELECT meta.get_child_labels('person');
SELECT meta.get_child_labels('person', 'graph1');
SELECT meta.get_child_labels('employee');

SELECT meta.get_parent_labels('employee');
SELECT meta.get_parent_labels('employee', 'graph1');
SELECT meta.get_parent_labels('person');

--
-- meta.cypher_type(anyelement), meta.cypher_types(jsonb), meta.is_cypher_type(anyelement)
--
SELECT meta.cypher_type(123);
SELECT meta.cypher_type(123.45);
SELECT meta.cypher_type('hello'::text);
SELECT meta.cypher_type(true);
SELECT meta.cypher_type('{"key": "value"}'::jsonb);
SELECT meta.cypher_type('["a", "b", "c"]'::jsonb);
SELECT meta.cypher_type('null'::jsonb);
SELECT meta.cypher_type(NULL);

SELECT meta.cypher_types('{"key": "value", "arr": [1, 2, 3], "num": 123, "float": 45.67, "bool": true, "null": null}'::jsonb);
SELECT meta.cypher_types('["a", 1, 2.3, true, {"key": "value"}, null]'::jsonb);
SELECT meta.cypher_types('null'::jsonb);
SELECT meta.cypher_types(NULL);

SELECT meta.is_cypher_type(123, 'Integer');
SELECT meta.is_cypher_type(123.45, 'Float');
SELECT meta.is_cypher_type('hello'::text, 'String');
SELECT meta.is_cypher_type(true, 'Boolean');
SELECT meta.is_cypher_type('{"key": "value"}'::jsonb, 'Map');
SELECT meta.is_cypher_type('["a", "b", "c"]'::jsonb, 'List');
SELECT meta.is_cypher_type('null'::jsonb, 'Null');
SELECT meta.is_cypher_type(123, 'String');

SELECT meta.is_cypher_type('hello'::text, 'BIGINT');

--
-- meta.count_self_loops(relationship_name, graph_name)
--
SELECT meta.count_self_loops();

MATCH (a:person {name: 'Bob'})
CREATE (a)-[:"KNOWS"]->(a);

MATCH (a:animal {name: 'Spot'})
CREATE (a)-[:"HAS_PET"]->(a);

SELECT meta.count_self_loops(graph=>'graph1');

SELECT meta.count_self_loops('HAS_PET');
SELECT meta.count_self_loops('HAS_PET', 'graph1');

SELECT meta.count_self_loops('unknown');

--
-- meta.edge_density(graph_name), meta.estimated_edge_density(graph_name)
--
SELECT meta.edge_density();
SELECT meta.edge_density('graph1');

SELECT meta.estimated_edge_density();

ANALYZE;

SELECT meta.estimated_edge_density('graph1');

SELECT meta.edge_density('unknown');
SELECT meta.estimated_edge_density('unknown');

--
-- The table-returning meta functions work as Cypher CALL ... YIELD routines.
-- CALL ... YIELD is a non-leading clause, so a preceding clause drives it; a
-- single Alice keeps the routine's output un-multiplied.
--
MATCH (a:person {name: 'Alice'}) CALL meta.labels() YIELD label_name
RETURN label_name ORDER BY label_name;
MATCH (a:person {name: 'Alice'}) CALL meta.vertex_labels() YIELD label_name AS v
RETURN v ORDER BY v;
MATCH (a:person {name: 'Alice'}) CALL meta.edge_labels() YIELD label_name AS e
RETURN e ORDER BY e;
-- meta.graphs() returns SETOF name; the single column takes the routine's name
MATCH (a:person {name: 'Alice'}) CALL meta.graphs() YIELD graphs AS g
RETURN g ORDER BY g;
-- an explicit graph-name argument, with YIELD *
MATCH (a:person {name: 'Alice'}) CALL meta.labels('graph1') YIELD *
RETURN label_name ORDER BY label_name;
-- filter the yielded rows with a trailing FILTER clause
MATCH (a:person {name: 'Alice'}) CALL meta.labels() YIELD label_name
FILTER label_name = 'person' RETURN label_name;

-- clean up
DROP GRAPH graph1 CASCADE;

