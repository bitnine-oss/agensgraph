--
-- Cypher Query Language - User Defined Function
--

-- prepare
drop graph ag41 cascade;
drop table t1;
drop function func_usingparam( integer , integer );
drop function func_usingvariable1( integer , integer );
drop function func_usingvariable2( integer , varchar , integer , varchar );
drop function func_usingvariable3( integer , integer );
drop function func_usingvariable4( integer , varchar , integer , varchar );

create graph ag41;
set graph_path = ag41;

-- +---a-----------+
-- |   |       b   |
-- |   |       |   |
-- |   +---d---+   |
-- |       |       |
-- |       e-+   +-f
-- |         |   |
-- +---c     +-g-+
--     |      | |
--     +---h--+ +--i
--         |       |
--         +---j---+

create vlabel a;
create vlabel b;
create vlabel c inherits( a );
create vlabel d inherits( a , b );
create vlabel e inherits( d );
create vlabel f inherits( a );
create vlabel g inherits( e , f );
create vlabel h inherits( c , g );
create vlabel i inherits( g );
create vlabel j inherits( h , i );

create (n:a {id:1 , name:'nA'} ) return n;
create (n:b {id:2 , name:'nB'} ) return n;
create (n:c {id:3 , name:'nC'} ) return n;
create (n:d {id:4 , name:'nD'} ) return n;
create (n:e {id:5 , name:'nE'} ) return n;
create (n:f {id:6 , name:'nF'} ) return n;
create (n:g {id:7 , name:'nG'} ) return n;
create (n:h {id:8 , name:'nH'} ) return n;
create (n:i {id:9 , name:'nI'} ) return n;
create (n:j {id:10 , name:'nJ'} ) return n;

match (x:a) , (y:d) where x.name = 'nA' and y.name = 'nD' create (x)-[e:e1{id:51 , name:'edge1'}]->(y) return e;
match (x:b) , (y:d) where x.name = 'nB' and y.name = 'nD' create (x)-[e:e2{id:52 , name:'edge2'}]->(y) return e;
match (x:d) , (y:e) where x.name = 'nD' and y.name = 'nE' create (x)-[e:e3{id:53 , name:'edge3'}]->(y) return e;
match (x:a) , (y:f) where x.name = 'nA' and y.name = 'nF' create (x)-[e:e4{id:54 , name:'edge4'}]->(y) return e;
match (x:e) , (y:g) where x.name = 'nE' and y.name = 'nG' create (x)-[e:e5{id:55 , name:'edge5'}]->(y) return e;
match (x:f) , (y:g) where x.name = 'nF' and y.name = 'nG' create (x)-[e:e6{id:56 , name:'edge6'}]->(y) return e;
match (x:c) , (y:h) where x.name = 'nC' and y.name = 'nH' create (x)-[e:e7{id:57 , name:'edge7'}]->(y) return e;
match (x:g) , (y:h) where x.name = 'nG' and y.name = 'nH' create (x)-[e:e8{id:58 , name:'edge8'}]->(y) return e;
match (x:g) , (y:i) where x.name = 'nG' and y.name = 'nI' create (x)-[e:e9{id:59 , name:'edge9'}]->(y) return e;
match (x:h) , (y:j) where x.name = 'nH' and y.name = 'nJ' create (x)-[e:e10{id:60 , name:'edge10'}]->(y) return e;
match (x:i) , (y:j) where x.name = 'nI' and y.name = 'nJ' create (x)-[e:e11{id:61 , name:'edge11'}]->(y) return e;
match (x:a) , (y:c) where x.name = 'nA' and y.name = 'nC' create (x)-[e:e12{id:62 , name:'edge12'}]->(y) return e;

create table t1( c1 integer , c2 varchar(10) );



-- User Defined Function
create or replace function func_usingparam( p1 integer , p2 integer ) returns jsonb as $$
declare
var1 jsonb;
var2 jsonb;
var3 jsonb;
begin
match (x)-[e]->(y) where x.id = to_jsonb(p1) and y.id = to_jsonb(p2)
return x , e , y into var1 , var2 , var3;
raise notice 'match (x)-[e]->(y) where x.id = to_jsonb(p1) and y.id = to_jsonb(p2) return x , e , y into var1 , var2 , var3;';
raise notice 'var1 : %' , var1;
raise notice 'var2 : %' , var2;
raise notice 'var3 : %' , var3;
return var2;
end;
$$ language plpgsql;

select func_usingparam( 5 , 7 );

create or replace function func_usingvariable1( p1 integer , p2 integer ) returns jsonb as $$
declare
var1 jsonb;
var2 jsonb;
var3 jsonb;
var4 integer;
var5 integer;
begin
var4 := p1;
var5 := p2;
match (x)-[e]->(y) where x.id = to_jsonb(var4) and y.id = to_jsonb(var5) return x , e , y into var1 , var2 , var3;
raise notice 'match (x)-[e]->(y) where x.id = to_jsonb(var4) and y.id = to_jsonb(var5) return x , e , y into var1 , var2 , var3;';
raise notice 'var1 : %' , var1;
raise notice 'var2 : %' , var2;
raise notice 'var3 : %' , var3;
return var2;
end;
$$ language plpgsql;

select func_usingvariable1( 1 , 3 );

create or replace function func_usingvariable2( start_id integer , start_name varchar(2) , end_id integer , end_name varchar(2) ) returns jsonb as $$
declare
var_start t1%rowtype;
var_end t1%rowtype;
var1 jsonb;
begin
var_start.c1 := start_id;
var_start.c2 := start_name;
var_end.c1 := end_id;
var_end.c2 := end_name;
match (x)-[e]->(y)
where x.id = to_jsonb(var_start.c1) and x.name = to_jsonb(var_start.c2) and y.id = to_jsonb(var_end.c1) and y.name = to_jsonb(var_end.c2)
return e into var1;
raise notice 'match (x)-[e]->(y) where x.id = to_jsonb(var_start.c1) and x.name = to_jsonb(var_start.c2) and y.id = to_jsonb(var_end.c1) and y.name = to_jsonb(var_end.c2) return e into var1;';
raise notice 'var1 : %' , var1;
return var1;
end;
$$ language plpgsql;

select func_usingvariable2( 1 , 'nA' , 6 , 'nF' );

create or replace function func_usingvariable3( p1 integer , p2 integer ) returns jsonb as $$
declare
var1 jsonb;
var2 jsonb;
var3 jsonb;
begin
<<label1>>
declare
var4 integer;
var5 integer;
begin
var4 := p1;
var5 := p2;
match (x)-[e]->(y) where x.id = to_jsonb(label1.var4) and y.id = to_jsonb(label1.var5) return x , e , y into var1 , var2 , var3;
raise notice 'match (x)-[e]->(y) where x.id = to_jsonb(label1.var4) and y.id = to_jsonb(label1.var5) return x , e , y into var1 , var2 , var3;';
raise notice 'var1 : %' , var1;
raise notice 'var2 : %' , var2;
raise notice 'var3 : %' , var3;
return var2;
end;
end;
$$ language plpgsql;

select func_usingvariable3( 1 , 3 );

create or replace function func_usingvariable4( start_id integer , start_name varchar(2) , end_id integer , end_name varchar(2) ) returns jsonb as $$
declare
var1 jsonb;
begin
<<label1>>
declare
var_start t1%rowtype;
var_end t1%rowtype;
begin
var_start.c1 := start_id;
var_start.c2 := start_name;
var_end.c1 := end_id;
var_end.c2 := end_name;
match (x)-[e]->(y)
where x.id = to_jsonb(label1.var_start.c1) and x.name = to_jsonb(label1.var_start.c2) and y.id = to_jsonb(label1.var_end.c1) and y.name = to_jsonb(label1.var_end.c2)
return e into var1;
raise notice 'match (x)-[e]->(y) where x.id = to_jsonb(label1.var_start.c1) and x.name = to_jsonb(label1.var_start.c2) and y.id = to_jsonb(label1.var_end.c1) and y.name = to_jsonb(label1.var_end.c2) return e into var1;';
raise notice 'var1 : %' , var1;
return var1;
end;
end;
$$ language plpgsql;

select func_usingvariable4( 1 , 'nA' , 6 , 'nF' );



-- finalize
drop function func_usingvariable4( integer , varchar , integer , varchar );
drop function func_usingvariable3( integer , integer );
drop function func_usingvariable2( integer , varchar , integer , varchar );
drop function func_usingvariable1( integer , integer );
drop function func_usingparam( integer , integer );
drop table t1;
drop graph ag41 cascade;
