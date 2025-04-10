--
-- Test access privileges for graph element
--

-- Suppress NOTICE messages when users/groups don't exist

SET client_min_messages TO 'warning';

-- drop, if exists

DROP GRAPH IF EXISTS graph_priv_test CASCADE;

-- create, set graph

CREATE GRAPH graph_priv_test;
SET graph_path = graph_priv_test;

-- create user

CREATE USER readonly_user LOGIN PASSWORD 'ru';
CREATE USER new_user LOGIN PASSWORD 'nu';

-- create role

CREATE ROLE readonly LOGIN;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA graph_priv_test TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA graph_priv_test TO readonly;
GRANT USAGE ON SCHEMA graph_priv_test TO readonly;

-- grant a role to user

GRANT readonly TO "readonly_user";

-- give select priv to user

ALTER DEFAULT PRIVILEGES IN SCHEMA graph_priv_test GRANT SELECT ON SEQUENCES TO group readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA graph_priv_test GRANT SELECT ON TABLES TO group readonly;
ALTER DEFAULT PRIVILEGES FOR USER new_user GRANT SELECT ON TABLES TO group readonly;

-- create vertices, by superuser

create (:v1{'property':'1'});
create (:v1{'property':'2'});
create (:v1{'property':'3'});
create (:v1{'property':'4'});
create (:v1{'property':'a'});
create (:v1{'property':'b'});
create (:v1{'property':'c'});
create (:v1{'property':'d'});

-- create edges, by superuser

match (n1:v1 {'property':'1'} ), (n2:v1 {'property':'a'}) create (n1)-[n3:e1 {property:'1a'}]->(n2);
match (n1:v1 {'property':'2'} ), (n2:v1 {'property':'b'}) create (n1)-[n3:e1 {property:'2b'}]->(n2);
match (n1:v1 {'property':'3'} ), (n2:v1 {'property':'c'}) create (n1)-[n3:e1 {property:'3c'}]->(n2);
match (n1:v1 {'property':'4'} ), (n2:v1 {'property':'d'}) create (n1)-[n3:e1 {property:'4d'}]->(n2);

-- delete vertex, by readonly user

SET SESSION AUTHORIZATION readonly_user;

MATCH (n{'property':'1'}) WHERE NOT EXISTS ((n)-[*1]-()) return n;
MATCH (n{'property':'1'}) WHERE NOT EXISTS ((n)-[*1]-()) delete n; -- #624 issue
MATCH (n{'property':'1'}) WHERE NOT EXISTS ((n)-[*1]-()) return n;

-- clear all
RESET SESSION AUTHORIZATION;
DROP GRAPH IF EXISTS graph_priv_test CASCADE;