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

CREATE (:v1{'property':'1'});
CREATE (:v1{'property':'2'});
CREATE (:v1{'property':'3'});
CREATE (:v1{'property':'4'});
CREATE (:v1{'property':'a'});
CREATE (:v1{'property':'b'});
CREATE (:v1{'property':'c'});
CREATE (:v1{'property':'d'});

-- Create edges

MATCH (n1:v1 {'property':'1'} ), (n2:v1 {'property':'a'}) CREATE (n1)-[n3:e1 {property:'1a'}]->(n2);
MATCH (n1:v1 {'property':'2'} ), (n2:v1 {'property':'b'}) CREATE (n1)-[n3:e1 {property:'2b'}]->(n2);
MATCH (n1:v1 {'property':'3'} ), (n2:v1 {'property':'c'}) CREATE (n1)-[n3:e1 {property:'3c'}]->(n2);
MATCH (n1:v1 {'property':'4'} ), (n2:v1 {'property':'d'}) CREATE (n1)-[n3:e1 {property:'4d'}]->(n2);

-- Set current session as readonly user

SET SESSION AUTHORIZATION readonly_user;

-- MATCH cypher query by readonly user

MATCH (n{'property':'1'}) WHERE EXISTS ((n)-[*1]-()) return n;

-- CREATE vertex by readonly user

CREATE (n1:v1 {'property':'5'} ), (n2:v1 {'property':'e'});

-- CREATE edge by readonly user

MATCH (n1:v1 {'property':'1'} ), (n2:v1 {'property':'a'}) CREATE (n1)-[n3:e1 {property:'nopriv'}]->(n2);

-- DELETE cypher query by readonly user

MATCH (n{'property':'1'}) WHERE EXISTS ((n)-[*1]-()) DELETE n; -- issue #624, issue #512

-- SET cypher query by readonly user

MATCH (n{'property':'1'}) SET n.property = 'modified_1';  -- issue #512

-- MERGE cypher query by readonly user

MERGE (n{'property':'1'}) return n;

-- Clear all
RESET SESSION AUTHORIZATION;

--
-- Test ROW LEVEL SECURITY
--
CREATE ROLE group1;
CREATE ROLE role2 IN ROLE group1;
CREATE ROLE role3 IN ROLE group1;

MATCH (a:v1) RETURN a;

-- To allow group1 to access the graph_priv_test graph,
-- USAGE on graph_priv_test is required
GRANT USAGE ON SCHEMA graph_priv_test TO group1;

-- Allow group1 to use MATCH, CREATE/MERGE, SET/REMOVE on v1 label
GRANT SELECT, INSERT, UPDATE, DELETE ON graph_priv_test."v1" TO group1;
GRANT SELECT, INSERT, UPDATE, DELETE ON graph_priv_test."ag_vertex" TO group1;
GRANT SELECT, INSERT, UPDATE, DELETE ON graph_priv_test."ag_edge" TO group1;
GRANT USAGE ON graph_priv_test."v1_id_seq" TO group1;
ALTER TABLE graph_priv_test."v1" ENABLE ROW LEVEL SECURITY;

-- Switch to role2
SET role role2;

-- Since we enabled rls without any policy, role2 should not be able to access/insert any data
MATCH (a:v1) RETURN a;
CREATE (a:v1 {name: 'A'});

RESET ROLE;

-- Add policy to allow role2 to access/insert data and role3 to access data
CREATE POLICY v1_policy ON graph_priv_test.v1 TO group1
USING (current_role = 'role2' OR current_role = 'role3')
WITH CHECK (current_role = 'role2');

-- Switch to role2 again
SET role role2;

MATCH (a:v1) RETURN a;
CREATE (a:v1 {name: 'A'});
MATCH (a:v1 {name: 'A'}) SET a.name = 'B' RETURN a;
MERGE (a:v1 {name: 'A'}) return a;

RESET ROLE;

-- Switch to role3
SET role role3;

MATCH (a:v1) RETURN a;

-- Should pass because its same as MATCH, as no new row will be created
MERGE (a:v1 {name: 'A'});

-- Should fail as role3
CREATE (a:v1 {name: 'C'});
MATCH (a:v1 {name: 'A'}) SET a.name = 'C' RETURN a;
MERGE (a:v1 {name: 'C'}) return a;

RESET ROLE;

-- Allow role3 to access/update data where name is 'A'
ALTER POLICY v1_policy ON graph_priv_test."v1"
USING (current_role = 'role2' OR (current_role = 'role3' AND properties->>'name' = 'A'))
WITH CHECK (current_role = 'role2' OR (current_role = 'role3' AND properties->>'name' = 'A'));

-- Switch to role3 again
SET role role3;

MATCH (a:v1 {name: 'A'}) SET a.age=40 RETURN a;
MATCH (a:v1 {name: 'A'}) DELETE a;

-- Should fail as role3 can only update where name is 'A'
MATCH (a:v1 {name: 'B'}) SET a.age=40;
MATCH (a:v1 {name: 'B'}) DELETE a;

RESET ROLE;

--
-- RLS: the USING clauses of UPDATE and DELETE policies decide which rows a
-- graph write may touch.  Reading stays open here on purpose, so every row
-- reaches the write and the write's own policies are what refuse it.
--
CREATE VLABEL rls2;
CREATE ELABEL rls2_e;
CREATE (:rls2 {name: 'alice'});
CREATE (:rls2 {name: 'bob'});
MATCH (a:rls2 {name: 'alice'}), (b:rls2 {name: 'bob'})
	CREATE (a)-[:rls2_e]->(b);
GRANT SELECT, INSERT, UPDATE, DELETE ON graph_priv_test."rls2" TO group1;
GRANT SELECT, INSERT, UPDATE, DELETE ON graph_priv_test."rls2_e" TO group1;
-- DETACH deletes through every edge label, so it needs the earlier one too
GRANT SELECT, INSERT, UPDATE, DELETE ON graph_priv_test."e1" TO group1;
ALTER TABLE graph_priv_test."rls2" ENABLE ROW LEVEL SECURITY;
CREATE POLICY rls2_sel ON graph_priv_test.rls2 FOR SELECT USING (true);

SET role role2;

-- every row is readable
MATCH (n:rls2) RETURN n.name ORDER BY n.name;

-- but with no DELETE and no UPDATE policy, the default-deny policy refuses
-- the rows the write asks for
MATCH (n:rls2 {name: 'bob'}) DETACH DELETE n;
MATCH (n:rls2 {name: 'bob'}) SET n.age = 1;

RESET role;

-- narrow policies: only alice may be updated, and nobody may be deleted
CREATE POLICY rls2_upd ON graph_priv_test.rls2 FOR UPDATE
	USING (properties->>'name' = 'alice') WITH CHECK (true);
CREATE POLICY rls2_del ON graph_priv_test.rls2 FOR DELETE
	USING (properties->>'name' = 'nobody');

SET role role2;

-- bob is outside the UPDATE policy's USING; alice is inside it
MATCH (n:rls2 {name: 'bob'}) SET n.age = 99;
MATCH (n:rls2 {name: 'alice'}) SET n.age = 30 RETURN n.name, n.age;

-- no row satisfies the DELETE policy
MATCH (n:rls2 {name: 'bob'}) DETACH DELETE n;

-- MERGE updates the row it matches, so the UPDATE policies apply to it
MERGE (n:rls2 {name: 'bob'}) ON MATCH SET n.age = 1;
MERGE (n:rls2 {name: 'alice'}) ON MATCH SET n.age = 31;

RESET role;

-- the rows the policies protected are untouched, and alice carries the two
-- updates the policies allowed
MATCH (n:rls2) OPTIONAL MATCH (n)-[e:rls2_e]-()
	RETURN n.name, n.age, count(e) ORDER BY n.name;

--
-- INSERT policies: the WITH CHECK clause reads the row being inserted --
-- its property map, not the pattern that created it -- and an edge label
-- enforces its policies the same way a vertex label does
--
CREATE POLICY rls2_ins ON graph_priv_test.rls2 FOR INSERT
	WITH CHECK (properties->>'name' <> 'forbidden');
ALTER TABLE graph_priv_test."rls2_e" ENABLE ROW LEVEL SECURITY;
GRANT USAGE ON graph_priv_test."rls2_id_seq" TO group1;
GRANT USAGE ON graph_priv_test."rls2_e_id_seq" TO group1;

SET role role2;

-- the policy reads the new row's properties
CREATE (:rls2 {name: 'carol'});
CREATE (:rls2 {name: 'forbidden'});
MERGE (n:rls2 {name: 'forbidden'});

-- the edge label has row security and no INSERT policy: default deny
MATCH (a:rls2 {name: 'alice'}), (b:rls2 {name: 'carol'})
	CREATE (a)-[:rls2_e]->(b);

RESET role;

-- an INSERT policy on the edge label admits it again
CREATE POLICY rls2_e_ins ON graph_priv_test.rls2_e FOR INSERT WITH CHECK (true);
CREATE POLICY rls2_e_sel ON graph_priv_test.rls2_e FOR SELECT USING (true);

SET role role2;
MATCH (a:rls2 {name: 'alice'}), (b:rls2 {name: 'carol'})
	CREATE (a)-[:rls2_e]->(b);
RESET role;

-- only the rows the INSERT policies admitted exist
MATCH (n:rls2) OPTIONAL MATCH (n)-[e:rls2_e]-()
	RETURN n.name, count(e) ORDER BY n.name;

--
-- RLS on an inherited label: a row answers to the policies of the label it
-- belongs to, whichever label the query names to reach it
--
CREATE VLABEL rls_parent;
CREATE VLABEL rls_child INHERITS (rls_parent);
CREATE (:rls_parent {name: 'alice', age: 30});
CREATE (:rls_child  {name: 'emp1',  age: 41});
GRANT SELECT, INSERT, UPDATE, DELETE ON graph_priv_test."rls_parent" TO group1;
GRANT SELECT, INSERT, UPDATE, DELETE ON graph_priv_test."rls_child" TO group1;
ALTER TABLE graph_priv_test."rls_child" ENABLE ROW LEVEL SECURITY;
CREATE POLICY c_sel ON graph_priv_test.rls_child FOR SELECT USING (true);
CREATE POLICY c_upd ON graph_priv_test.rls_child FOR UPDATE
	USING (true) WITH CHECK (false);
-- no DELETE policy on rls_child: default deny

SET role role2;

-- the child row is refused through either label, and with no DELETE policy
-- the default-deny policy holds through the parent label too
MATCH (n:rls_child  {name: 'emp1'}) SET n.age = 100;
MATCH (n:rls_parent {name: 'emp1'}) SET n.age = 101;
MATCH (n:rls_parent {name: 'emp1'}) DELETE n;

-- a row of the parent label has no row security, whichever label reached it
MATCH (n:rls_parent {name: 'alice'}) SET n.age = 32;

RESET role;

-- the child row is untouched, the parent row carries its update
MATCH (n:rls_parent) RETURN n.name, n.age ORDER BY n.name;

-- the named label's policies do not reach another label's rows: let the
-- child's policy admit its row while the parent's refuses everything, and
-- the child row still updates under its own label's policy
ALTER POLICY c_upd ON graph_priv_test.rls_child
	WITH CHECK (properties->>'name' = 'emp1');
ALTER TABLE graph_priv_test."rls_parent" ENABLE ROW LEVEL SECURITY;
CREATE POLICY p_sel2 ON graph_priv_test.rls_parent FOR SELECT USING (true);
CREATE POLICY p_upd2 ON graph_priv_test.rls_parent FOR UPDATE
	USING (true) WITH CHECK (false);

SET role role2;
MATCH (n:rls_parent {name: 'emp1'}) SET n.age = 103 RETURN n.age;
MATCH (n:rls_parent {name: 'alice'}) SET n.age = 33;
RESET role;

MATCH (n:rls_parent) RETURN n.name, n.age ORDER BY n.name;

-- Clean up
DROP GRAPH IF EXISTS graph_priv_test CASCADE;

-- Dropping the graph removes the schema-scoped default privileges, but the
-- "FOR USER new_user" grant near the top is global (not schema-scoped), so it
-- survives as a pg_default_acl row referencing new_user and readonly.  Reverse
-- it so the regression database keeps no default-ACL entry pointing at these
-- test roles; otherwise a plain pg_dump/pg_restore or pg_upgrade of the
-- database into a freshly initialized cluster (which lacks the roles) fails.
-- The roles themselves may remain, like the many regress_* roles other tests
-- leave behind, because nothing in the database references them any more.
ALTER DEFAULT PRIVILEGES FOR USER new_user REVOKE SELECT ON TABLES FROM group readonly;