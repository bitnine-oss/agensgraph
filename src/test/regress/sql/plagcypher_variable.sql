--
-- PL/agCypher - Use Parameter
--

-- setup

drop graph if exists g3 cascade;
drop function if exists variable_vertex();
drop function if exists variable_edge();
drop function if exists variable_graphpath();
drop function if exists variable_graphid();
drop function if exists variable();

create graph g3;
set graph_path=g3;

create (:person{name : 'Anders'}), (:person{name : 'Dilshad'}), (:person{name : 'Cesar'}), (:person{name : 'Becky'}), (:person{name : 'Filipa'}), (:person{name : 'Emil'});

match (a), (b)
where a.name = 'Anders' and b.name = 'Dilshad'
create (a)-[e:knows{name:'friend1'}]->(b)
return e;

match (a), (b)
where a.name = 'Anders' and b.name = 'Cesar'
create (a)-[e:knows{name:'friend2'}]->(b)
return e;

match (a), (b)
where a.name = 'Anders' and b.name = 'Becky'
create (a)-[e:knows{name:'friend3'}]->(b)
return e;

match (a), (b)
where a.name = 'Dilshad' and b.name = 'Filipa'
create (a)-[e:knows{name:'friend4'}]->(b)
return e;

match (a), (b)
where a.name = 'Cesar' and b.name = 'Emil'
create (a)-[e:knows{name:'friend5'}]->(b)
return e;

match (a), (b)
where a.name = 'Becky' and b.name = 'Emil'
create (a)-[e:knows{name:'friend6'}]->(b)
return e;

-- test variable( vertex )

create or replace function variable_vertex() returns vertex as $$
declare
var1 vertex;
begin
match (x)-[z]->(y) where z.name = 'friend2' and y.name = 'Cesar' return x into var1;
return var1;
end;
$$ language plagcypher;

select variable_vertex();

-- test variable( edge )

create or replace function variable_edge() returns edge as $$
declare
var1 edge;
begin
match (x)-[z]-(y) where x.name = 'Anders' and y.name = 'Becky' return z into var1;
return var1;
end;
$$ language plagcypher;

select variable_edge();

-- test variable( graphpath )

create or replace function variable_graphpath() returns graphpath as $$
declare
var1 graphpath;
begin
match p=allshortestpaths( (n)-[*..4]-(m) ) where n.name = 'Filipa' and m.name = 'Becky' return p into var1;
return var1;
end;
$$ language plagcypher;

select variable_graphpath();

-- test variable( graphid )

create or replace function variable_graphid() returns graphid as $$
declare
var1 graphid;
begin
match (a) where a.name = 'Dilshad' return id(a) into var1;
return var1;
end;
$$ language plagcypher;

select variable_graphid();

create or replace function variable() returns void as $$
declare
var1 vertex;
var2 edge;
var3 vertex;
begin
match (x)-[z]-(y) where x.name = 'Anders' and y.name = 'Becky' return x, z, y into var1, var2, var3;
raise notice 'var1 : %' , var1;
raise notice 'var2 : %' , var2;
raise notice 'var3 : %' , var3;
end;
$$ language plagcypher;

select variable();

-- clean up

drop function if exists variable();
drop function if exists variable_graphid();
drop function if exists variable_graphpath();
drop function if exists variable_edge();
drop function if exists variable_vertex();
drop graph if exists g3 cascade;
