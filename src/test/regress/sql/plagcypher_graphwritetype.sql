--
-- PL/agCypher - Use Parameter
--

-- setup

drop graph if exists g4 cascade;
drop function if exists func_create();
drop function if exists func_delete();
drop function if exists func_set();
drop function if exists func_merge();
drop function if exists func_remove();

create graph g4;
set graph_path=g4;

create (:person{name : 'Anders'}), (:person{name : 'Dilshad'}), (:person{name : 'Cesar'}), (:person{name : 'Becky'}), (:person{name : 'Filipa'}), (:person{name : 'Emil'});
create elabel block;

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

-- test graphwrite type ( create )
create or replace function func_create() returns void as $$
begin
create( :person{name : 'Bossman'} );
end;
$$ language plagcypher;

match (a) return a;

select func_create();

match (a) return a;

create or replace function func_delete() returns void as $$
begin
match (a)
where a.name = 'Becky'
delete a;
end;
$$ language plagcypher;

match (a) return a;

select func_delete();

match (a) return a;

create or replace function func_set() returns void as $$
begin
match (a)
where a.name = 'Becky'
set a.name = 'lucy';
end;
$$ language plagcypher;

match (a) return a;

select func_set();

match (a) return a;

create or replace function func_merge() returns void as $$
begin
match (a) , (b)
where a.name = 'Cesar' and b.name = 'Filipa'
merge (a)-[e:block {name:'block'}]->(b);
end
$$ language plagcypher;

match (a)-[b]->(c)
where b.name = 'block'
return a , b , c;

select func_merge();

match (a)-[b]->(c)
where b.name = 'block'
return a , b , c;

create or replace function func_remove() returns void as $$
begin
match (a)
remove a.name;
end;
$$ language plagcypher;

match (a) return a;

select func_remove();

match (a) return a;

drop function if exists func_set();
drop function if exists func_remove();
drop function if exists func_merge();
drop function if exists func_delete();
drop function if exists func_create();
drop graph if exists g4 cascade;
