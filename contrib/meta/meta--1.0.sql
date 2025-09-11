-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION meta" to load this file. \quit

CREATE FUNCTION meta.graphs()
RETURNS SETOF name
LANGUAGE sql
STABLE
AS $$
    SELECT graphname
    FROM ag_graph;
$$;

COMMENT ON FUNCTION meta.graphs() IS
'Returns list of graphs.';

CREATE FUNCTION meta.labels(graph name DEFAULT NULL)
RETURNS TABLE(label_name name)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    schema_name name;
BEGIN
    IF graph IS NULL THEN
        BEGIN
            schema_name := current_setting('graph_path');
            IF schema_name IS NULL OR schema_name = '' THEN
                RAISE NOTICE 'graph_path is not set. Provide graph name or set graph_path';
                RETURN;
            END IF;
            graph := schema_name;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'graph_path is not set. Provide graph name or set graph_path';
            RETURN;
        END;
    END IF;

    RETURN QUERY
    SELECT a.labname
    FROM ag_label a
    JOIN ag_graph g ON a.graphid = g.oid
    WHERE a.labname NOT IN ('ag_vertex','ag_edge')
      AND g.graphname = graph;
END;
$$;

COMMENT ON FUNCTION meta.labels(name) IS
'Returns all labels for a given graph. Uses graph_path if graph name is not provided.';

CREATE FUNCTION meta.vertex_labels(graph name DEFAULT NULL)
RETURNS TABLE(label_name name)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    schema_name name;
BEGIN
    IF graph IS NULL THEN
        BEGIN
            schema_name := current_setting('graph_path');
            IF schema_name IS NULL OR schema_name = '' THEN
                RAISE NOTICE 'graph_path is not set. Provide graph name or set graph_path';
                RETURN;
            END IF;
            graph := schema_name;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'graph_path is not set. Provide graph name or set graph_path';
            RETURN;
        END;
    END IF;

    RETURN QUERY
    SELECT a.labname
    FROM ag_label a
    JOIN ag_graph g ON a.graphid = g.oid
    WHERE a.labkind = 'v'
      AND a.labname <> 'ag_vertex'
      AND g.graphname = graph;
END;
$$;

COMMENT ON FUNCTION meta.vertex_labels(name) IS
'Returns all vertex labels for a given graph. Uses graph_path if graph name is not provided.';

CREATE FUNCTION meta.edge_labels(graph name DEFAULT NULL)
RETURNS TABLE(label_name name)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    schema_name name;
BEGIN
    IF graph IS NULL THEN
        BEGIN
            schema_name := current_setting('graph_path');
            IF schema_name IS NULL OR schema_name = '' THEN
                RAISE NOTICE 'graph_path is not set. Provide graph name or set graph_path';
                RETURN;
            END IF;
            graph := schema_name;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'graph_path is not set. Provide graph name or set graph_path';
            RETURN;
        END;
    END IF;

    RETURN QUERY
    SELECT a.labname
    FROM ag_label a
    JOIN ag_graph g ON a.graphid = g.oid
    WHERE a.labkind = 'e'
      AND a.labname <> 'ag_edge'
      AND g.graphname = graph;
END;
$$;

COMMENT ON FUNCTION meta.edge_labels(name) IS
'Returns all edge labels for a given graph. Uses graph_path if graph name is not provided.';

CREATE FUNCTION meta.label_id(label name, graph name DEFAULT NULL)
RETURNS INT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    schema_name name;
    result INT;
BEGIN
    IF graph IS NULL THEN
        BEGIN
            schema_name := current_setting('graph_path');
            IF schema_name IS NULL OR schema_name = '' THEN
                RAISE NOTICE 'graph_path is not set. Provide graph name or set graph_path';
                RETURN NULL;
            END IF;
            graph := schema_name;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'graph_path is not set. Provide graph name or set graph_path';
            RETURN NULL;
        END;
    END IF;

    SELECT a.labid
    INTO result
    FROM ag_label a
    JOIN ag_graph g ON a.graphid = g.oid
    WHERE a.labname = label
      AND g.graphname = graph;

    IF result IS NULL THEN
        RAISE EXCEPTION 'Label % does not exist in graph %', label, graph;
    END IF;

    RETURN result;
END;
$$;

COMMENT ON FUNCTION meta.label_id(name, name) IS
'Returns the ID of a label. Uses graph_path if graph name is not provided.';

CREATE FUNCTION meta.label_kind(label name, graph name DEFAULT NULL)
RETURNS CHAR
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    schema_name name;
    result CHAR;
BEGIN
    IF graph IS NULL THEN
        BEGIN
            schema_name := current_setting('graph_path');
            IF schema_name IS NULL OR schema_name = '' THEN
                RAISE NOTICE 'graph_path is not set. Provide graph name or set graph_path';
                RETURN NULL;
            END IF;
            graph := schema_name;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'graph_path is not set. Provide graph name or set graph_path';
            RETURN NULL;
        END;
    END IF;

    SELECT a.labkind
    INTO result
    FROM ag_label a
    JOIN ag_graph g ON a.graphid = g.oid
    WHERE a.labname = label
      AND g.graphname = graph;

    IF result IS NULL THEN
        RAISE NOTICE 'Label % does not exist in graph %', label, graph;
    END IF;

    RETURN result;
END;
$$;

COMMENT ON FUNCTION meta.label_kind(name, name) IS
'Returns the kind of a label (v or e). Uses graph_path if graphname is not provided.';

CREATE FUNCTION meta.label_name(labelid INT, graph name DEFAULT NULL)
RETURNS TEXT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    schema_name name;
    result TEXT;
BEGIN
    IF graph IS NULL THEN
        BEGIN
            schema_name := current_setting('graph_path');
            IF schema_name IS NULL OR schema_name = '' THEN
                RAISE NOTICE 'graph_path is not set. Provide graph or set graph_path';
                RETURN NULL;
            END IF;
            graph := schema_name;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'graph_path is not set. Provide graph or set graph_path';
            RETURN NULL;
        END;
    END IF;

    SELECT a.labname
    INTO result
    FROM ag_label a
    JOIN ag_graph g ON a.graphid = g.oid
    WHERE (a.labid = labelid OR a.oid = labelid)
      AND g.graphname = graph;

    IF result IS NULL THEN
        RAISE NOTICE 'Label with id % does not exist in graph %', labelid, graph;
    END IF;

    RETURN result;
END;
$$;

COMMENT ON FUNCTION meta.label_name(INT, name) IS
'Returns the name of a label given its ID or OID. Uses graph_path if graph is not provided.';

CREATE FUNCTION meta.graph_size(graph_name name DEFAULT NULL, pretty BOOLEAN DEFAULT TRUE)
RETURNS TEXT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    schema_name name;
    total_bytes BIGINT;
BEGIN
    IF graph_name IS NULL THEN
        BEGIN
            schema_name := current_setting('graph_path');
            IF schema_name IS NULL OR schema_name = '' THEN
                RAISE NOTICE 'graph_path is not set. Provide graph name or set graph_path';
                RETURN NULL;
            END IF;
            graph_name := schema_name;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'graph_path is not set. Provide graph name or set graph_path';
            RETURN NULL;
        END;
    END IF;

    SELECT SUM(pg_total_relation_size(c.oid))
    INTO total_bytes
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = graph_name;

    IF total_bytes IS NULL THEN
        total_bytes := 0;
    END IF;

    IF pretty THEN
        RETURN pg_size_pretty(total_bytes);
    ELSE
        RETURN total_bytes::TEXT;
    END IF;
END;
$$;

COMMENT ON FUNCTION meta.graph_size(name, boolean) IS
'Returns total size of a graph. uses graph_path if graph_name not provided';

CREATE FUNCTION meta.label_size(labelname TEXT, graph name DEFAULT NULL, pretty BOOLEAN DEFAULT TRUE)
RETURNS TEXT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    size_bytes BIGINT;
    schema_name name;
    full_label TEXT;
BEGIN
    IF graph IS NULL OR graph = '' THEN
        schema_name := current_setting('graph_path');
        IF schema_name IS NULL OR schema_name = '' THEN
            RAISE NOTICE 'graph_path is not set. Provide graph or set graph_path';
            RETURN NULL;
        END IF;
        graph := schema_name;
    END IF;

    full_label := format('"%s"."%s"', graph, labelname);

    SELECT pg_total_relation_size(full_label) INTO size_bytes;

    IF pretty THEN
        RETURN pg_size_pretty(size_bytes);
    ELSE
        RETURN size_bytes::TEXT;
    END IF;
END;
$$;

COMMENT ON FUNCTION meta.label_size(TEXT, name, BOOLEAN) IS
'Returns the total size of a label (including indexes). Uses graph_path if graph is not provided.';

CREATE FUNCTION meta.count(labelname TEXT, graph name DEFAULT NULL)
RETURNS BIGINT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    result BIGINT;
    schema_name name;
    full_label TEXT;
BEGIN
    IF graph IS NULL OR graph = '' THEN
        schema_name := current_setting('graph_path');
        IF schema_name IS NULL OR schema_name = '' THEN
            RAISE NOTICE 'graph_path is not set. Provide graph or set graph_path';
            RETURN NULL;
        END IF;
        graph := schema_name;
    END IF;

    full_label := format('"%s"."%s"', graph, labelname);

    EXECUTE format('SELECT COUNT(1) FROM %s', full_label)
    INTO result;

    RETURN result;
END;
$$;

COMMENT ON FUNCTION meta.count(TEXT, name) IS
'Returns the exact count of nodes or edges for a given label. Uses graph_path if graph is not provided.';

CREATE FUNCTION meta.estimated_count(labelname TEXT, graph name DEFAULT NULL)
RETURNS BIGINT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    result BIGINT;
    schema_name name;
    full_label TEXT;
BEGIN
    IF graph IS NULL OR graph = '' THEN
        schema_name := current_setting('graph_path');
        IF schema_name IS NULL OR schema_name = '' THEN
            RAISE NOTICE 'graph_path is not set. Provide graph or set graph_path';
            RETURN NULL;
        END IF;
        graph := schema_name;
    END IF;

    full_label := schema_name || '.' || labelname;

    EXECUTE format('SELECT reltuples::BIGINT FROM pg_class WHERE oid = %L::regclass', full_label)
    INTO result;

    IF result < 0 THEN
        RAISE NOTICE 'Statistics not available for label %. Run ANALYZE % to gather statistics.', full_label, full_label;
        RETURN NULL;
    END IF;

    RETURN result;
END;
$$;

COMMENT ON FUNCTION meta.estimated_count(TEXT, name) IS
'Returns estimated tuple count for a label using Postgresql statistics (pg_class.reltuples). Uses graph_path if graph is not provided.';

CREATE FUNCTION meta.graph_exists(graph name)
RETURNS BOOLEAN
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1
        FROM ag_graph
        WHERE graphname = graph
    );
END;
$$;

COMMENT ON FUNCTION meta.graph_exists(name) IS
'Returns true if a graph with the given name exists.';

CREATE FUNCTION meta.label_exists(labelname TEXT, graph name DEFAULT NULL)
RETURNS BOOLEAN
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    schema_name name;
BEGIN
    IF graph IS NULL OR graph = '' THEN
        schema_name := current_setting('graph_path');
        IF schema_name IS NULL OR schema_name = '' THEN
            RAISE NOTICE 'graph_path is not set. Provide graph or set graph_path';
            RETURN NULL;
        END IF;
        graph := schema_name;
    END IF;

    RETURN EXISTS (
        SELECT 1
        FROM ag_label a
        JOIN ag_graph g ON a.graphid = g.oid
        WHERE a.labname = labelname
          AND g.graphname = graph
    );
END;
$$;

COMMENT ON FUNCTION meta.label_exists(TEXT, name) IS
'Returns true if a label exists in the given graph. Uses graph_path if graph is not provided.';

CREATE FUNCTION meta.vertex_label_exists(labelname TEXT, graph name DEFAULT NULL)
RETURNS BOOLEAN
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    schema_name name;
BEGIN
    IF graph IS NULL OR graph = '' THEN
        schema_name := current_setting('graph_path');
        IF schema_name IS NULL OR schema_name = '' THEN
            RAISE NOTICE 'graph_path is not set. Provide graph or set graph_path';
            RETURN NULL;
        END IF;
        graph := schema_name;
    END IF;

    RETURN EXISTS (
        SELECT 1
        FROM ag_label a
        JOIN ag_graph g ON a.graphid = g.oid
        WHERE a.labname = labelname
          AND g.graphname = graph
          AND a.labkind = 'v'
    );
END;
$$;

COMMENT ON FUNCTION meta.label_exists(TEXT, name) IS
'Returns true if a vertex label exists in the given graph. Uses graph_path if graph is not provided.';

CREATE FUNCTION meta.edge_label_exists(labelname TEXT, graph name DEFAULT NULL)
RETURNS BOOLEAN
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    schema_name name;
BEGIN
    IF graph IS NULL OR graph = '' THEN
        schema_name := current_setting('graph_path');
        IF schema_name IS NULL OR schema_name = '' THEN
            RAISE NOTICE 'graph_path is not set. Provide graph or set graph_path';
            RETURN NULL;
        END IF;
        graph := schema_name;
    END IF;

    RETURN EXISTS (
        SELECT 1
        FROM ag_label a
        JOIN ag_graph g ON a.graphid = g.oid
        WHERE a.labname = labelname
          AND g.graphname = graph
          AND a.labkind = 'e'
    );
END;
$$;

COMMENT ON FUNCTION meta.label_exists(TEXT, name) IS
'Returns true if an edge label exists in the given graph. Uses graph_path if graph is not provided.';

CREATE FUNCTION meta.get_child_labels(parent_label TEXT, graph name DEFAULT NULL)
RETURNS TABLE(labelname name)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    schema_name name;
BEGIN
    IF graph IS NULL OR graph = '' THEN
        schema_name := current_setting('graph_path');
        IF schema_name IS NULL OR schema_name = '' THEN
            RAISE NOTICE 'graph_path is not set. Provide graph or set graph_path';
            RETURN;
        END IF;
        graph := schema_name;
    END IF;

    RETURN QUERY
    SELECT c.relname AS labelname
    FROM pg_inherits i
    JOIN pg_class c ON c.oid = i.inhrelid
    JOIN pg_class p ON p.oid = i.inhparent
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE p.relname = parent_label
      AND n.nspname = graph;
END;
$$;

COMMENT ON FUNCTION meta.get_child_labels(TEXT, name) IS
'Returns the child labels of a given label. Uses graph_path if graph is not provided.';

CREATE FUNCTION meta.get_parent_labels(child_label TEXT, graph name DEFAULT NULL)
RETURNS TABLE(labelname name)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    schema_name name;
BEGIN
    IF graph IS NULL OR graph = '' THEN
        schema_name := current_setting('graph_path');
        IF schema_name IS NULL OR schema_name = '' THEN
            RAISE NOTICE 'graph_path is not set. Provide graph or set graph_path';
            RETURN;
        END IF;
        graph := schema_name;
    END IF;

    RETURN QUERY
    SELECT p.relname AS labelname
    FROM pg_inherits i
    JOIN pg_class c ON c.oid = i.inhrelid
    JOIN pg_class p ON p.oid = i.inhparent
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = child_label
      AND n.nspname = graph;
END;
$$;

COMMENT ON FUNCTION meta.get_parent_labels(TEXT, name) IS
'Returns the parent labels of a given label. Uses graph_path if graph is not provided.';

CREATE FUNCTION meta.cypher_type(element anyelement)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    elem_type text;
BEGIN
    IF pg_typeof(element)::text = 'jsonb' THEN
        elem_type := jsonb_typeof(element::jsonb);

        IF elem_type = 'number' THEN
            IF element::text ~ '^\d+$' THEN
                RETURN 'INTEGER';
            ELSIF element::text ~ '^\d+\.\d+$' THEN
                RETURN 'FLOAT';
            ELSE
                RETURN 'NUMBER';
            END IF;
        ELSE
            CASE UPPER(elem_type)
                WHEN 'OBJECT' THEN RETURN 'MAP';
                WHEN 'ARRAY'  THEN RETURN 'LIST';
                ELSE RETURN UPPER(elem_type);
            END CASE;
        END IF;

    ELSE
        elem_type := pg_typeof(element)::text;

        CASE elem_type
            WHEN 'integer'          THEN RETURN 'INTEGER';
            WHEN 'bigint'           THEN RETURN 'INTEGER';
            WHEN 'smallint'         THEN RETURN 'INTEGER';
            WHEN 'numeric'          THEN RETURN 'FLOAT';
            WHEN 'real'             THEN RETURN 'FLOAT';
            WHEN 'double precision' THEN RETURN 'FLOAT';
            WHEN 'boolean'          THEN RETURN 'BOOLEAN';
            WHEN 'text'             THEN RETURN 'STRING';
            WHEN 'json'             THEN RETURN 'MAP';
            WHEN 'jsonb'            THEN RETURN 'MAP';
            ELSE RETURN UPPER(elem_type);
        END CASE;
    END IF;
END;
$$;

COMMENT ON FUNCTION meta.cypher_type(anyelement) IS 'Returns Cypher type name for a single value.';

CREATE FUNCTION meta.cypher_types(properties jsonb)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    key   TEXT;
    value jsonb;
    result jsonb := '{}'::jsonb;
BEGIN
    IF properties IS NULL THEN
        RETURN result;
    END IF;

    IF jsonb_typeof(properties) <> 'object' THEN
        RAISE EXCEPTION 'expected object, got %', jsonb_typeof(properties);
    END IF;

    FOR key, value IN
        SELECT * FROM jsonb_each(properties)
    LOOP
        result := result || jsonb_build_object(key, meta.cypher_type(value));
    END LOOP;

    RETURN result;
END;
$$;

COMMENT ON FUNCTION meta.cypher_types(jsonb) IS 'Returns Cypher type mapping for all keys in a JSONB object.';

CREATE OR REPLACE FUNCTION meta.is_cypher_type(value anyelement, expected_type TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    IF value IS NULL THEN
        RETURN UPPER(expected_type) = 'NULL';
    END IF;

    IF UPPER(expected_type) NOT IN ('NULL', 'INTEGER', 'MAP', 'LIST', 'FLOAT', 'STRING', 'BOOLEAN', 'NUMBER') THEN
        RAISE EXCEPTION 'Unsupported expected_type: %', expected_type;
    END IF;

    RETURN meta.cypher_type(value) = UPPER(expected_type);
END;
$$;

COMMENT ON FUNCTION meta.is_cypher_type(anyelement, TEXT)
IS 'Checks if the type of value matches the specified Cypher type (NULL, INTEGER, MAP, LIST, FLOAT, STRING, BOOLEAN, NUMBER).';

CREATE OR REPLACE FUNCTION meta.count_self_loops(relationship TEXT DEFAULT NULL, graph TEXT DEFAULT NULL)
RETURNS BIGINT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    schema TEXT;
    result BIGINT;
BEGIN
    IF graph IS NULL THEN
        BEGIN
            schema := current_setting('graph_path');
            IF schema IS NULL OR schema = '' THEN
                RAISE NOTICE 'graph_path is not set. Provide graph name or set graph_path';
                RETURN NULL;
            END IF;
            graph := schema;
        EXCEPTION WHEN OTHERS THEN  
            RAISE NOTICE 'graph_path is not set. Provide graph name or set graph_path';
            RETURN NULL;
        END;
    END IF;

    IF relationship IS NULL THEN
        relationship := 'ag_edge';
    END IF;

    EXECUTE format(
        'SELECT COUNT(1) FROM %I.%I WHERE start = "end"',
        graph, relationship
    )
    INTO result;

    RETURN result;
END;
$$;

COMMENT ON FUNCTION meta.count_self_loops(TEXT, TEXT)
IS 'Returns the number of self-loops in a graph relationship, defaulting to all relationships.';

CREATE FUNCTION meta.edge_density(graph name DEFAULT NULL)
RETURNS FLOAT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    schema TEXT;
    num_nodes BIGINT;
    num_edges BIGINT;
BEGIN
    IF graph IS NULL THEN
        BEGIN
            schema := current_setting('graph_path');
            IF schema IS NULL OR schema = '' THEN
                RAISE NOTICE 'graph_path is not set. Provide graph or set graph_path';
                RETURN 0;
            END IF;
            graph := schema;
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'graph_path is not set. Provide graph or set graph_path';
            RETURN 0;
        END;
    END IF;

    EXECUTE format('SELECT count(*) FROM %I.ag_vertex', graph) INTO num_nodes;
    EXECUTE format('SELECT count(*) FROM %I.ag_edge', graph) INTO num_edges;

    IF num_nodes < 2 THEN
        RETURN 0;
    ELSE
        RETURN (num_edges * 1.0) / (num_nodes * (num_nodes - 1));
    END IF;
END;
$$;

COMMENT ON FUNCTION meta.edge_density(name)
IS 'Calculates edge density of a graph. Uses graph_path if graph name is not provided.';

CREATE FUNCTION meta.estimated_edge_density(graph NAME DEFAULT NULL)
RETURNS FLOAT
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
    schema_name TEXT;
    v_count FLOAT := 0;
    e_count FLOAT := 0;
BEGIN
    IF graph IS NULL THEN
        BEGIN
            schema_name := current_setting('graph_path');
            IF schema_name IS NULL OR schema_name = '' THEN
                RAISE NOTICE 'graph_path is not set. Provide graph name or set graph_path';
                RETURN NULL;
            END IF;
            graph := schema_name;
        EXCEPTION WHEN OTHERS THEN  
            RAISE NOTICE 'graph_path is not set. Provide graph name or set graph_path';
            RETURN NULL;
        END;
    END IF;

    SELECT COALESCE(SUM(c.reltuples), 0) INTO v_count
    FROM (
        SELECT 'ag_vertex' AS tbl
        UNION ALL
        SELECT label_name FROM meta.vertex_labels(graph)
    ) AS t
    JOIN pg_class c ON c.relname = t.tbl
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = graph;

    SELECT COALESCE(SUM(c.reltuples), 0) INTO e_count
    FROM (
        SELECT 'ag_edge' AS tbl
        UNION ALL
        SELECT label_name FROM meta.edge_labels(graph)
    ) AS t
    JOIN pg_class c ON c.relname = t.tbl
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = graph;

    IF v_count < 2 THEN
        RETURN 0;
    ELSE
        RETURN (e_count * 1.0) / (v_count * (v_count - 1));
    END IF;
END;
$$;

COMMENT ON FUNCTION meta.estimated_edge_density(NAME)
IS 'Estimates edge density using Postgresql statistics (pg_class.reltuples). Uses graph_path if graph name is not provided.';
