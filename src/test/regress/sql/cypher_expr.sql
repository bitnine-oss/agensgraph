--
-- Cypher Query Language - Expression
--

-- Set up
CREATE GRAPH test_cypher_expr;
SET graph_path = test_cypher_expr;

-- Numeric, string, and boolean literal
RETURN '"', '\"', '\\"', 17, true, false;

-- Octal and hexadecimal literal
RETURN 021, 0x11, 0X11;

-- List and map literal
RETURN [7, 7.0, '"list\nliteral\"', true, false, NULL, [0, 1, 2], {p: 'p'}];
RETURN {i: 7, r: 7.0, s: '"map\nliteral\"', t: true, f: false, 'z': NULL,
        '\n': '\n', l: [0, 1, 2], o: {p: 'p'}};

-- String concatenation
RETURN '1' + '1', '1' + 1, 1 + '1';

-- Arithmetic operation
RETURN 1 + 1, 1 - 1, 2 * 2, 2 / 2, 2 % 2, 2 ^ 2, +1, -1;

-- List concatenation
RETURN 's' + [], 0 + [], true + [],
       [] + 's', [] + 0, [] + true,
       [0] + [1], [] + {}, {} + [];

-- Invalid expression
RETURN '' + false;
RETURN '' + {};
RETURN 0 + false;
RETURN 0 + {};
RETURN false + '';
RETURN false + 0;
RETURN false + false;
RETURN false + {};
RETURN {} + '';
RETURN {} + 0;
RETURN {} + false;
RETURN {} + {};
RETURN '' - '';
RETURN '' - 0;
RETURN '' - false;
RETURN '' - [];
RETURN '' - {};
RETURN 0 - '';
RETURN 0 - false;
RETURN 0 - [];
RETURN 0 - {};
RETURN false - '';
RETURN false - 0;
RETURN false - false;
RETURN false - [];
RETURN false - {};
RETURN [] - '';
RETURN [] - 0;
RETURN [] - false;
RETURN [] - [];
RETURN [] - {};
RETURN {} - '';
RETURN {} - 0;
RETURN {} - false;
RETURN {} - [];
RETURN {} - {};
RETURN '' * '';
RETURN '' * 0;
RETURN '' * false;
RETURN '' * [];
RETURN '' * {};
RETURN 0 * '';
RETURN 0 * false;
RETURN 0 * [];
RETURN 0 * {};
RETURN false * '';
RETURN false * 0;
RETURN false * false;
RETURN false * [];
RETURN false * {};
RETURN [] * '';
RETURN [] * 0;
RETURN [] * false;
RETURN [] * [];
RETURN [] * {};
RETURN {} * '';
RETURN {} * 0;
RETURN {} * false;
RETURN {} * [];
RETURN {} * {};
RETURN '' / '';
RETURN '' / 0;
RETURN '' / false;
RETURN '' / [];
RETURN '' / {};
RETURN 0 / '';
RETURN 0 / false;
RETURN 0 / [];
RETURN 0 / {};
RETURN false / '';
RETURN false / 0;
RETURN false / false;
RETURN false / [];
RETURN false / {};
RETURN [] / '';
RETURN [] / 0;
RETURN [] / false;
RETURN [] / [];
RETURN [] / {};
RETURN {} / '';
RETURN {} / 0;
RETURN {} / false;
RETURN {} / [];
RETURN {} / {};
RETURN '' % '';
RETURN '' % 0;
RETURN '' % false;
RETURN '' % [];
RETURN '' % {};
RETURN 0 % '';
RETURN 0 % false;
RETURN 0 % [];
RETURN 0 % {};
RETURN false % '';
RETURN false % 0;
RETURN false % false;
RETURN false % [];
RETURN false % {};
RETURN [] % '';
RETURN [] % 0;
RETURN [] % false;
RETURN [] % [];
RETURN [] % {};
RETURN {} % '';
RETURN {} % 0;
RETURN {} % false;
RETURN {} % [];
RETURN {} % {};
RETURN '' ^ '';
RETURN '' ^ 0;
RETURN '' ^ false;
RETURN '' ^ [];
RETURN '' ^ {};
RETURN 0 ^ '';
RETURN 0 ^ false;
RETURN 0 ^ [];
RETURN 0 ^ {};
RETURN false ^ '';
RETURN false ^ 0;
RETURN false ^ false;
RETURN false ^ [];
RETURN false ^ {};
RETURN [] ^ '';
RETURN [] ^ 0;
RETURN [] ^ false;
RETURN [] ^ [];
RETURN [] ^ {};
RETURN {} ^ '';
RETURN {} ^ 0;
RETURN {} ^ false;
RETURN {} ^ [];
RETURN {} ^ {};
RETURN +'';
RETURN +false;
RETURN +[];
RETURN +{};
RETURN -'';
RETURN -false;
RETURN -[];
RETURN -{};

CREATE (:v0 {
  o: {i: 7, r: 7.0, s: '"map\nliteral\"', t: true, f: false, 'z': NULL,
      '\n': '\n'},
  l: [7, 7.0, '"list\nliteral\"', true, false, NULL, [0, 1, 2, 3, 4], {p: 'p'}],
  t: {i: 1, s: 's', b: true, l: [0], o: {p: 'p'}},
  f: {i: 0, s: '', b: false, l: [], o: {}}
});

-- Property access
MATCH (n:v0) RETURN n.o.i, n.o.'i', n.o['i'];
MATCH (n:v0) RETURN n.l[0], n.l[6][0],
                    n.l[6][1..], n.l[6][..3], n.l[6][1..3],
                    n.l[6][-4..], n.l[6][..-2], n.l[6][-4..-2],
                    n.l[6][1..6], n.l[6][-7..-2], n.l[6][1..3][0],
                    n.l[7].p,n.l[7].'p', n.l[7]['p'];

-- Null test
RETURN '' IS NULL, '' IS NOT NULL, NULL IS NULL, NULL IS NOT NULL;
MATCH (n:v0) RETURN n.o.z IS NULL, n.l[5] IS NOT NULL;

-- Boolean
MATCH (n:v0) WHERE n.t.i RETURN COUNT(*);
MATCH (n:v0) WHERE n.t.s RETURN COUNT(*);
MATCH (n:v0) WHERE n.t.b RETURN COUNT(*);
MATCH (n:v0) WHERE n.t.l RETURN COUNT(*);
MATCH (n:v0) WHERE n.t.o RETURN COUNT(*);
MATCH (n:v0) WHERE n.f.i RETURN COUNT(*);
MATCH (n:v0) WHERE n.f.s RETURN COUNT(*);
MATCH (n:v0) WHERE n.f.b RETURN COUNT(*);
MATCH (n:v0) WHERE n.f.l RETURN COUNT(*);
MATCH (n:v0) WHERE n.f.o RETURN COUNT(*);

-- Case expression

CREATE (:v1 {i: -1}), (:v1 {i: 0}), (:v1 {i: 1});

MATCH (n:v1)
RETURN CASE n.i WHEN 0 THEN true ELSE false END,
       CASE WHEN n.i = 0 THEN true ELSE false END;

-- IN expression
MATCH (n:v0) RETURN true IN n.l;
MATCH (n:v0) RETURN 0 IN n.l;
MATCH (n:v0) RETURN NULL IN n.l;
MATCH (n:v0) WITH n.l[0] AS i RETURN [(i IN [0, 1, 2, 3, 4]), true];

-- List comprehension
RETURN [x IN [0, 1, 2, 3, 4]];
RETURN [x IN [0, 1, 2, 3, 4] WHERE x % 2 = 0];
RETURN [x IN [0, 1, 2, 3, 4] | x + 1];
RETURN [x IN [0, 1, 2, 3, 4] WHERE x % 2 = 0 | x + 1];

-- List predicate functions
RETURN ALL(x in [] WHERE x = 0);
RETURN ALL(x in [0] WHERE x = 0);
RETURN ALL(x in [0, 1, 2, 3, 4] WHERE x = 0);
RETURN ALL(x in [0, 1, 2, 3, 4] WHERE x >= 0);
RETURN ALL(x in [0, 1, 2, 3, 4] WHERE x = 5);
RETURN ANY(x in [] WHERE x = 0);
RETURN ANY(x in [0] WHERE x = 0);
RETURN ANY(x in [0, 1, 2, 3, 4] WHERE x = 0);
RETURN ANY(x in [0, 1, 2, 3, 4] WHERE x >= 0);
RETURN ANY(x in [0, 1, 2, 3, 4] WHERE x = 5);
RETURN NONE(x in [] WHERE x = 0);
RETURN NONE(x in [0] WHERE x = 0);
RETURN NONE(x in [0, 1, 2, 3, 4] WHERE x = 0);
RETURN NONE(x in [0, 1, 2, 3, 4] WHERE x >= 0);
RETURN NONE(x in [0, 1, 2, 3, 4] WHERE x = 5);
RETURN SINGLE(x in [] WHERE x = 0);
RETURN SINGLE(x in [0] WHERE x = 0);
RETURN SINGLE(x in [0, 1, 2, 3, 4] WHERE x = 0);
RETURN SINGLE(x in [0, 1, 2, 3, 4] WHERE x >= 0);
RETURN SINGLE(x in [0, 1, 2, 3, 4] WHERE x = 5);

-- Functions

CREATE (:coll {name: 'AgensGraph'});
MATCH (n:coll) SET n.l = tolower(n.name);
MATCH (n:coll) SET n.u = toupper(n.name);
MATCH (n:coll) RETURN n;

-- Tear down
DROP GRAPH test_cypher_expr CASCADE;
