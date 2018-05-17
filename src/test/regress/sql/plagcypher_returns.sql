--
-- PL/agCypher - Use Parameter
--

-- setup

drop graph if exists g2 cascade;
drop function if exists returns_vertex();
drop function if exists returns_edge();
drop function if exists returns_graphpath();
drop function if exists returns_graphid();

create graph g2;
set graph_path=g2;

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

-- test returns( vertex )

create or replace function returns_vertex() returns vertex as $$
declare
var1 vertex;
begin
match (x)-[z]->(y) where z.name = 'friend2' and y.name = 'Cesar' return x into var1;
return var1;
end;
$$ language plagcypher;

select returns_vertex();

-- test returns( edge )

create or replace function returns_edge() returns edge as $$
declare
var1 edge;
begin
match (x)-[z]->(y) where x.name = 'Dilshad' and y.name = 'Filipa' return z into var1;
return var1;
end;
$$ language plagcypher;

select returns_edge();

-- test returns( graphpath )

create or replace function returns_graphpath() returns graphpath as $$
declare
var1 graphpath;
begin
match p=allshortestpaths( (n)-[*..4]-(m) ) where n.name = 'Dilshad' and m.name = 'Filipa' return p into var1;
return var1;
end;
$$ language plagcypher;

select returns_graphpath();

-- test returns( graphid )

create or replace function returns_graphid() returns graphid as $$
declare
var1 graphid;
begin
match (x)-[z]-(y) where x.name = 'Emil' and y.name = 'Cesar' return id(z) into var1;
return var1;
end;
$$ language plagcypher;

select returns_graphid();

-- cleanup

drop function if exists returns_graphid();
drop function if exists returns_graphpath();
drop function if exists returns_edge();
drop function if exists returns_vertex();
drop graph if exists g2 cascade;
