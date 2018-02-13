--
-- Cypher Query Language - User Defined Function
--


-- setup

DROP FUNCTION IF EXISTS udf_var(jsonb);
DROP FUNCTION IF EXISTS udf_param(jsonb);
DROP GRAPH IF EXISTS udf CASCADE;

CREATE GRAPH udf;
SET graph_path = udf;

CREATE (:v {id: 1, refs: [2, 3, 4]}), (:v {id: 2});


-- test param and scope of the iterator variable used in a list comprehension

CREATE FUNCTION udf_param(id jsonb) RETURNS jsonb AS $$
DECLARE
  r jsonb;
BEGIN
  MATCH (n:v) WHERE n.id = id RETURN [id IN n.refs WHERE id < 3] INTO r;
  RETURN r;
END;
$$ LANGUAGE plpgsql;

RETURN udf_param(1);


-- test var

CREATE FUNCTION udf_var(id jsonb) RETURNS jsonb AS $$
DECLARE
  i jsonb;
  p jsonb;
BEGIN
  i := id;
  MATCH (n:v) WHERE n.id = i RETURN properties(n) INTO p;
  RETURN p;
END;
$$ LANGUAGE plpgsql;

RETURN udf_var(2);


-- teardown

DROP FUNCTION udf_var(jsonb);
DROP FUNCTION udf_param(jsonb);
DROP GRAPH udf CASCADE;
