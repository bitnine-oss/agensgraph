--
-- Test access privileges for graph element
--

-- Suppress NOTICE messages when users/groups don't exist

SET client_min_messages TO 'warning';

-- Drop, If exists

DROP GRAPH IF EXISTS graph_priv_test CASCADE;

-- Create graph and Set graph path

CREATE GRAPH graph_priv_test;
SET graph_path = graph_priv_test;

-- Create user

CREATE USER readonly_user LOGIN PASSWORD 'ru';
CREATE USER new_user LOGIN PASSWORD 'nu';

-- Create role

CREATE ROLE readonly LOGIN;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA graph_priv_test TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA graph_priv_test TO readonly;
GRANT USAGE ON SCHEMA graph_priv_test TO readonly;

-- Grant a role to the user

GRANT readonly TO "readonly_user";

-- Give a select priv to the user

ALTER DEFAULT PRIVILEGES IN SCHEMA graph_priv_test GRANT SELECT ON SEQUENCES TO group readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA graph_priv_test GRANT SELECT ON TABLES TO group readonly;
ALTER DEFAULT PRIVILEGES FOR USER new_user GRANT SELECT ON TABLES TO group readonly;

-- Create vertices

create (:v1{'property':'1'});
create (:v1{'property':'2'});
create (:v1{'property':'3'});
create (:v1{'property':'4'});
create (:v1{'property':'a'});
create (:v1{'property':'b'});
create (:v1{'property':'c'});
create (:v1{'property':'d'});

-- Create edges

match (n1:v1 {'property':'1'} ), (n2:v1 {'property':'a'}) create (n1)-[n3:e1 {property:'1a'}]->(n2);
match (n1:v1 {'property':'2'} ), (n2:v1 {'property':'b'}) create (n1)-[n3:e1 {property:'2b'}]->(n2);
match (n1:v1 {'property':'3'} ), (n2:v1 {'property':'c'}) create (n1)-[n3:e1 {property:'3c'}]->(n2);
match (n1:v1 {'property':'4'} ), (n2:v1 {'property':'d'}) create (n1)-[n3:e1 {property:'4d'}]->(n2);

-- Set current session as readonly user

SET SESSION AUTHORIZATION readonly_user;

-- MATCH cypher query by readonly user

MATCH (n{'property':'1'}) WHERE EXISTS ((n)-[*1]-()) return n;

-- CREATE vertex by readonly user

create (n1:v1 {'property':'5'} ), (n2:v1 {'property':'e'});

-- CREATE edge by readonly user

match (n1:v1 {'property':'1'} ), (n2:v1 {'property':'a'}) create (n1)-[n3:e1 {property:'nopriv'}]->(n2);

-- DELETE cypher query by readonly user

MATCH (n{'property':'1'}) WHERE EXISTS ((n)-[*1]-()) delete n; -- issue #624, issue #512

-- SET cypher query by readonly user

MATCH (n{'property':'1'}) SET n.property = 'modified_1';  -- issue #512

-- MERGE cypher query by readonly user

MERGE (n{'property':'1'}) return n;

-- Clear all
RESET SESSION AUTHORIZATION;
DROP GRAPH IF EXISTS graph_priv_test CASCADE;