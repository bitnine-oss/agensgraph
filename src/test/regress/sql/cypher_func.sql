--
-- labels()
--



--
-- simple test
--

--
-- prepare
--
drop graph simpletest cascade;
drop graph complextest1 cascade;
drop graph complextest2 cascade;



--
-- set graph path
--
create graph simpletest;
set graph_path=simpletest;

--
-- make vlabels
--          A
--          |
--  B       C
--  |       |
--  ----D----
--
create vlabel a;
create vlabel b;
create vlabel c inherits ( a );
create vlabel d inherits ( b , c );

--
-- make nodes
--
create (n:a {name:'nA'} ) return n;
create (n:b {name:'nB'} ) return n;
create (n:c {name:'nC'} ) return n;
create (n:d {name:'nD'} ) return n;

--
-- label()
--
match (x:a) return x.name, label(x);
match (x:b) return x.name, label(x);
match (x:c) return x.name, label(x);
match (x:d) return x.name, label(x);

--
-- lables()
--
match (x:a) return x.name, labels(x);
match (x:b) return x.name, labels(x);
match (x:c) return x.name, labels(x);
match (x:d) return x.name, labels(x);
match (x:a) return x.name, labels(x)[0];
match (x:b) return x.name, labels(x)[0];
match (x:c) return x.name, labels(x)[0];
match (x:d) return x.name, labels(x)[0];
match (x:a) return x.name, labels(x)[1];
match (x:b) return x.name, labels(x)[1];
match (x:c) return x.name, labels(x)[1];
match (x:d) return x.name, labels(x)[1];
match (x:a) return x.name, labels(x)[2];
match (x:b) return x.name, labels(x)[2];
match (x:c) return x.name, labels(x)[2];
match (x:d) return x.name, labels(x)[2];



--
-- complex test 1
--

--
-- set graph path
--
create graph complextest1;
set graph_path=complextest1;

--
-- make vlabels
--                     A
--                     |
--                     B       C
--                     |       |
-- --------D--------   ----E----   F   ----G
-- |       |       |       |       |   |   |
-- H       I       J       --------K----   |
--         |       |               |       |
--         ----------------L----------------
--
create vlabel a;
create vlabel b inherits( a );
create vlabel c;
create vlabel d;
create vlabel e inherits( b , c );
create vlabel f;
create vlabel g;
create vlabel h inherits( d );
create vlabel i inherits( d );
create vlabel j inherits( d );
create vlabel k inherits( e , f , g );
create vlabel l inherits( i , j , k , g );

--
-- make nodes
--
create (n:a {name:'nA'} ) return n;
create (n:b {name:'nB'} ) return n;
create (n:c {name:'nC'} ) return n;
create (n:d {name:'nD'} ) return n;
create (n:e {name:'nE'} ) return n;
create (n:f {name:'nF'} ) return n;
create (n:g {name:'nG'} ) return n;
create (n:h {name:'nH'} ) return n;
create (n:i {name:'nI'} ) return n;
create (n:j {name:'nJ'} ) return n;
create (n:k {name:'nK'} ) return n;
create (n:l {name:'nL'} ) return n;

--
-- label()
--
match (x:a) return x.name, label(x);
match (x:b) return x.name, label(x);
match (x:c) return x.name, label(x);
match (x:d) return x.name, label(x);
match (x:e) return x.name, label(x);
match (x:f) return x.name, label(x);
match (x:g) return x.name, label(x);
match (x:h) return x.name, label(x);
match (x:i) return x.name, label(x);
match (x:j) return x.name, label(x);
match (x:k) return x.name, label(x);
match (x:l) return x.name, label(x);

--
-- labels()
--
match (x:a) return x.name, labels(x);
match (x:b) return x.name, labels(x);
match (x:c) return x.name, labels(x);
match (x:d) return x.name, labels(x);
match (x:e) return x.name, labels(x);
match (x:f) return x.name, labels(x);
match (x:g) return x.name, labels(x);
match (x:h) return x.name, labels(x);
match (x:i) return x.name, labels(x);
match (x:j) return x.name, labels(x);
match (x:k) return x.name, labels(x);
match (x:l) return x.name, labels(x);
match (x:a) return x.name, labels(x)[0];
match (x:b) return x.name, labels(x)[0];
match (x:c) return x.name, labels(x)[0];
match (x:d) return x.name, labels(x)[0];
match (x:e) return x.name, labels(x)[0];
match (x:f) return x.name, labels(x)[0];
match (x:g) return x.name, labels(x)[0];
match (x:h) return x.name, labels(x)[0];
match (x:i) return x.name, labels(x)[0];
match (x:j) return x.name, labels(x)[0];
match (x:k) return x.name, labels(x)[0];
match (x:l) return x.name, labels(x)[0];
match (x:a) return x.name, labels(x)[1];
match (x:b) return x.name, labels(x)[1];
match (x:c) return x.name, labels(x)[1];
match (x:d) return x.name, labels(x)[1];
match (x:e) return x.name, labels(x)[1];
match (x:f) return x.name, labels(x)[1];
match (x:g) return x.name, labels(x)[1];
match (x:h) return x.name, labels(x)[1];
match (x:i) return x.name, labels(x)[1];
match (x:j) return x.name, labels(x)[1];
match (x:k) return x.name, labels(x)[1];
match (x:l) return x.name, labels(x)[1];
match (x:a) return x.name, labels(x)[2];
match (x:b) return x.name, labels(x)[2];
match (x:c) return x.name, labels(x)[2];
match (x:d) return x.name, labels(x)[2];
match (x:e) return x.name, labels(x)[2];
match (x:f) return x.name, labels(x)[2];
match (x:g) return x.name, labels(x)[2];
match (x:h) return x.name, labels(x)[2];
match (x:i) return x.name, labels(x)[2];
match (x:j) return x.name, labels(x)[2];
match (x:k) return x.name, labels(x)[2];
match (x:l) return x.name, labels(x)[2];
match (x:a) return x.name, labels(x)[3];
match (x:b) return x.name, labels(x)[3];
match (x:c) return x.name, labels(x)[3];
match (x:d) return x.name, labels(x)[3];
match (x:e) return x.name, labels(x)[3];
match (x:f) return x.name, labels(x)[3];
match (x:g) return x.name, labels(x)[3];
match (x:h) return x.name, labels(x)[3];
match (x:i) return x.name, labels(x)[3];
match (x:j) return x.name, labels(x)[3];
match (x:k) return x.name, labels(x)[3];
match (x:l) return x.name, labels(x)[3];
match (x:a) return x.name, labels(x)[4];
match (x:b) return x.name, labels(x)[4];
match (x:c) return x.name, labels(x)[4];
match (x:d) return x.name, labels(x)[4];
match (x:e) return x.name, labels(x)[4];
match (x:f) return x.name, labels(x)[4];
match (x:g) return x.name, labels(x)[4];
match (x:h) return x.name, labels(x)[4];
match (x:i) return x.name, labels(x)[4];
match (x:j) return x.name, labels(x)[4];
match (x:k) return x.name, labels(x)[4];
match (x:l) return x.name, labels(x)[4];
match (x:a) return x.name, labels(x)[5];
match (x:b) return x.name, labels(x)[5];
match (x:c) return x.name, labels(x)[5];
match (x:d) return x.name, labels(x)[5];
match (x:e) return x.name, labels(x)[5];
match (x:f) return x.name, labels(x)[5];
match (x:g) return x.name, labels(x)[5];
match (x:h) return x.name, labels(x)[5];
match (x:i) return x.name, labels(x)[5];
match (x:j) return x.name, labels(x)[5];
match (x:k) return x.name, labels(x)[5];
match (x:l) return x.name, labels(x)[5];
match (x:a) return x.name, labels(x)[6];
match (x:b) return x.name, labels(x)[6];
match (x:c) return x.name, labels(x)[6];
match (x:d) return x.name, labels(x)[6];
match (x:e) return x.name, labels(x)[6];
match (x:f) return x.name, labels(x)[6];
match (x:g) return x.name, labels(x)[6];
match (x:h) return x.name, labels(x)[6];
match (x:i) return x.name, labels(x)[6];
match (x:j) return x.name, labels(x)[6];
match (x:k) return x.name, labels(x)[6];
match (x:l) return x.name, labels(x)[6];
match (x:a) return x.name, labels(x)[7];
match (x:b) return x.name, labels(x)[7];
match (x:c) return x.name, labels(x)[7];
match (x:d) return x.name, labels(x)[7];
match (x:e) return x.name, labels(x)[7];
match (x:f) return x.name, labels(x)[7];
match (x:g) return x.name, labels(x)[7];
match (x:h) return x.name, labels(x)[7];
match (x:i) return x.name, labels(x)[7];
match (x:j) return x.name, labels(x)[7];
match (x:k) return x.name, labels(x)[7];
match (x:l) return x.name, labels(x)[7];
match (x:a) return x.name, labels(x)[8];
match (x:b) return x.name, labels(x)[8];
match (x:c) return x.name, labels(x)[8];
match (x:d) return x.name, labels(x)[8];
match (x:e) return x.name, labels(x)[8];
match (x:f) return x.name, labels(x)[8];
match (x:g) return x.name, labels(x)[8];
match (x:h) return x.name, labels(x)[8];
match (x:i) return x.name, labels(x)[8];
match (x:j) return x.name, labels(x)[8];
match (x:k) return x.name, labels(x)[8];
match (x:l) return x.name, labels(x)[8];
match (x:a) return x.name, labels(x)[9];
match (x:b) return x.name, labels(x)[9];
match (x:c) return x.name, labels(x)[9];
match (x:d) return x.name, labels(x)[9];
match (x:e) return x.name, labels(x)[9];
match (x:f) return x.name, labels(x)[9];
match (x:g) return x.name, labels(x)[9];
match (x:h) return x.name, labels(x)[9];
match (x:i) return x.name, labels(x)[9];
match (x:j) return x.name, labels(x)[9];
match (x:k) return x.name, labels(x)[9];
match (x:l) return x.name, labels(x)[9];
match (x:a) return x.name, labels(x)[10];
match (x:b) return x.name, labels(x)[10];
match (x:c) return x.name, labels(x)[10];
match (x:d) return x.name, labels(x)[10];
match (x:e) return x.name, labels(x)[10];
match (x:f) return x.name, labels(x)[10];
match (x:g) return x.name, labels(x)[10];
match (x:h) return x.name, labels(x)[10];
match (x:i) return x.name, labels(x)[10];
match (x:j) return x.name, labels(x)[10];
match (x:k) return x.name, labels(x)[10];
match (x:l) return x.name, labels(x)[10];



--
-- complex test 2
--

--
-- set graph path
--
create graph complextest2;
set graph_path=complextest2;

--
-- make vlabels
-- ----A------------
-- |   |       B   |
-- |   |       |   |
-- |   ----D----   |
-- |       |       |
-- |       E--   --F
-- |         |   |
-- ----C     --G--
--     |      | |
--     ----H--- ---I
--         |       |
--         ----J----
--

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

--
-- make nodes
--
create (n:a {name:'nA'} ) return n;
create (n:b {name:'nB'} ) return n;
create (n:c {name:'nC'} ) return n;
create (n:d {name:'nD'} ) return n;
create (n:e {name:'nE'} ) return n;
create (n:f {name:'nF'} ) return n;
create (n:g {name:'nG'} ) return n;
create (n:h {name:'nH'} ) return n;
create (n:i {name:'nI'} ) return n;
create (n:j {name:'nJ'} ) return n;

--
-- label()
--
match (x:a) return x.name, label(x);
match (x:b) return x.name, label(x);
match (x:c) return x.name, label(x);
match (x:d) return x.name, label(x);
match (x:e) return x.name, label(x);
match (x:f) return x.name, label(x);
match (x:g) return x.name, label(x);
match (x:h) return x.name, label(x);
match (x:i) return x.name, label(x);
match (x:j) return x.name, label(x);

--
-- labels()
--
match (x:a) return x.name, labels(x);
match (x:b) return x.name, labels(x);
match (x:c) return x.name, labels(x);
match (x:d) return x.name, labels(x);
match (x:e) return x.name, labels(x);
match (x:f) return x.name, labels(x);
match (x:g) return x.name, labels(x);
match (x:h) return x.name, labels(x);
match (x:i) return x.name, labels(x);
match (x:j) return x.name, labels(x);
match (x:a) return x.name, labels(x)[0];
match (x:b) return x.name, labels(x)[0];
match (x:c) return x.name, labels(x)[0];
match (x:d) return x.name, labels(x)[0];
match (x:e) return x.name, labels(x)[0];
match (x:f) return x.name, labels(x)[0];
match (x:g) return x.name, labels(x)[0];
match (x:h) return x.name, labels(x)[0];
match (x:i) return x.name, labels(x)[0];
match (x:j) return x.name, labels(x)[0];
match (x:a) return x.name, labels(x)[1];
match (x:b) return x.name, labels(x)[1];
match (x:c) return x.name, labels(x)[1];
match (x:d) return x.name, labels(x)[1];
match (x:e) return x.name, labels(x)[1];
match (x:f) return x.name, labels(x)[1];
match (x:g) return x.name, labels(x)[1];
match (x:h) return x.name, labels(x)[1];
match (x:i) return x.name, labels(x)[1];
match (x:j) return x.name, labels(x)[1];
match (x:a) return x.name, labels(x)[2];
match (x:b) return x.name, labels(x)[2];
match (x:c) return x.name, labels(x)[2];
match (x:d) return x.name, labels(x)[2];
match (x:e) return x.name, labels(x)[2];
match (x:f) return x.name, labels(x)[2];
match (x:g) return x.name, labels(x)[2];
match (x:h) return x.name, labels(x)[2];
match (x:i) return x.name, labels(x)[2];
match (x:j) return x.name, labels(x)[2];
match (x:a) return x.name, labels(x)[3];
match (x:b) return x.name, labels(x)[3];
match (x:c) return x.name, labels(x)[3];
match (x:d) return x.name, labels(x)[3];
match (x:e) return x.name, labels(x)[3];
match (x:f) return x.name, labels(x)[3];
match (x:g) return x.name, labels(x)[3];
match (x:h) return x.name, labels(x)[3];
match (x:i) return x.name, labels(x)[3];
match (x:j) return x.name, labels(x)[3];
match (x:a) return x.name, labels(x)[4];
match (x:b) return x.name, labels(x)[4];
match (x:c) return x.name, labels(x)[4];
match (x:d) return x.name, labels(x)[4];
match (x:e) return x.name, labels(x)[4];
match (x:f) return x.name, labels(x)[4];
match (x:g) return x.name, labels(x)[4];
match (x:h) return x.name, labels(x)[4];
match (x:i) return x.name, labels(x)[4];
match (x:j) return x.name, labels(x)[4];
match (x:a) return x.name, labels(x)[5];
match (x:b) return x.name, labels(x)[5];
match (x:c) return x.name, labels(x)[5];
match (x:d) return x.name, labels(x)[5];
match (x:e) return x.name, labels(x)[5];
match (x:f) return x.name, labels(x)[5];
match (x:g) return x.name, labels(x)[5];
match (x:h) return x.name, labels(x)[5];
match (x:i) return x.name, labels(x)[5];
match (x:j) return x.name, labels(x)[5];
match (x:a) return x.name, labels(x)[6];
match (x:b) return x.name, labels(x)[6];
match (x:c) return x.name, labels(x)[6];
match (x:d) return x.name, labels(x)[6];
match (x:e) return x.name, labels(x)[6];
match (x:f) return x.name, labels(x)[6];
match (x:g) return x.name, labels(x)[6];
match (x:h) return x.name, labels(x)[6];
match (x:i) return x.name, labels(x)[6];
match (x:j) return x.name, labels(x)[6];
match (x:a) return x.name, labels(x)[7];
match (x:b) return x.name, labels(x)[7];
match (x:c) return x.name, labels(x)[7];
match (x:d) return x.name, labels(x)[7];
match (x:e) return x.name, labels(x)[7];
match (x:f) return x.name, labels(x)[7];
match (x:g) return x.name, labels(x)[7];
match (x:h) return x.name, labels(x)[7];
match (x:i) return x.name, labels(x)[7];
match (x:j) return x.name, labels(x)[7];
match (x:a) return x.name, labels(x)[8];
match (x:b) return x.name, labels(x)[8];
match (x:c) return x.name, labels(x)[8];
match (x:d) return x.name, labels(x)[8];
match (x:e) return x.name, labels(x)[8];
match (x:f) return x.name, labels(x)[8];
match (x:g) return x.name, labels(x)[8];
match (x:h) return x.name, labels(x)[8];
match (x:i) return x.name, labels(x)[8];
match (x:j) return x.name, labels(x)[8];
match (x:a) return x.name, labels(x)[9];
match (x:b) return x.name, labels(x)[9];
match (x:c) return x.name, labels(x)[9];
match (x:d) return x.name, labels(x)[9];
match (x:e) return x.name, labels(x)[9];
match (x:f) return x.name, labels(x)[9];
match (x:g) return x.name, labels(x)[9];
match (x:h) return x.name, labels(x)[9];
match (x:i) return x.name, labels(x)[9];
match (x:j) return x.name, labels(x)[9];



--
-- cleanup
--
drop graph complextest2 cascade;
drop graph complextest1 cascade;
drop graph simpletest cascade;
