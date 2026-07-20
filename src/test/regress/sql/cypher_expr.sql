--
-- Cypher Query Language - Expression
--

-- Set up
CREATE GRAPH test_cypher_expr;
SET graph_path = test_cypher_expr;

-- String (jsonb)
RETURN '"', '\"', '\\', '\/', '\b', '\f', '\n', '\r', '\t';

-- Decimal (int4, int8, numeric)
RETURN -2147483648, 2147483647;
RETURN -9223372036854775808, 9223372036854775807;
RETURN -9223372036854775809, 9223372036854775808;
-- Hexadecimal (int4)
RETURN -0x7fffffff, 0x7fffffff;
-- Octal (int4)
RETURN -0o17777777777, 0o17777777777;
-- Float (numeric)
RETURN 3.14, -3.14, 6.02E23;

-- true, false, null
RETURN true, false, null;

-- String (text)
RETURN '"'::text, '\"'::text, '\\'::text, '\/'::text,
       '\b'::text, '\f'::text, '\n'::text, '\r'::text, '\t'::text;

-- Parameter - UNKNOWNOID::jsonb (string)
PREPARE tmp AS RETURN $1;
EXECUTE tmp ('"\""');
DEALLOCATE tmp;

-- Parameter - UNKNOWNOID::text
PREPARE tmp AS RETURN $1::text;
EXECUTE tmp ('\"');
DEALLOCATE tmp;

-- ::bool
RETURN ''::jsonb::bool, 0::jsonb::bool, false::jsonb::bool,
       []::bool, {}::bool;
RETURN 's'::jsonb::bool, 1::jsonb::bool, true::jsonb::bool,
       [0]::bool, {p: 0}::bool;

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

--
-- NULLIF expression
--

-- literals: NULL when the operands are equal, otherwise the first operand
RETURN nullIf(1, 1), nullIf(1, 2), nullIf('a', 'a'), nullIf('a', 'b');
-- keyword is case-insensitive (nullIf / NULLIF / nullif are the same)
RETURN NULLIF(1, 1), nullif(1, 2);
-- NULL operands: NULLIF(NULL, x) = NULL, NULLIF(x, NULL) = x
RETURN nullIf(NULL, 1), nullIf(1, NULL), nullIf(NULL, NULL);
-- jsonb property vs a literal (equal -> NULL, else the property value)
MATCH (n:v1) RETURN n.i, nullIf(n.i, 0) ORDER BY n.i;
-- jsonb list / map equality
RETURN nullIf([1, 2], [1, 2]), nullIf([1, 2], [1, 3]), nullIf({a: 1}, {a: 1});
-- graph-element identity: graphid and whole-vertex equality
MATCH (n:v1 {i: 0})
RETURN nullIf(id(n), id(n)) IS NULL AS same_id,
       nullIf(n, n) IS NULL AS same_vertex;
-- in WHERE: keep only the rows whose value equals 0 (NULLIF -> NULL)
MATCH (n:v1) WHERE nullIf(n.i, 0) IS NULL RETURN n.i;
-- in WITH: carry the folded value forward
MATCH (n:v1) WITH nullIf(n.i, -1) AS x RETURN x ORDER BY x;

-- The groups below exhaust the operand kinds, NULL positions, composition,
-- clause positions and error paths so new variants can be appended per group.
-- They reuse the existing v0 (one property of every kind) and v1 (small
-- integers) vertices; no new label is created here, so the shared graph's
-- label ids -- and every later section's golden output -- stay unchanged.
-- Whole-edge identity needs an edge, so that one fixture and test live at the
-- very end of the file (see "NULLIF: whole-edge identity" near the teardown).

-- number operands (jsonb): NULL when equal, else the first operand
RETURN nullIf(0, 0), nullIf(0, 1), nullIf(-1, -1), nullIf(3.14, 3.14),
       nullIf(3.14, 2.72), nullIf(1, 1.0), nullIf(1, 2.0);
-- int8 and beyond-int8 magnitudes
RETURN nullIf(9223372036854775807, 9223372036854775807),
       nullIf(9223372036854775808, 1);
-- string operands (jsonb)
RETURN nullIf('a', 'a'), nullIf('a', 'b'), nullIf('', ''), nullIf('a', '');
-- boolean operands (native bool)
RETURN nullIf(true, true), nullIf(true, false),
       nullIf(false, false), nullIf(false, true);
-- list / map operands (jsonb equality, element/entry-wise)
RETURN nullIf([1, 2], [1, 2]), nullIf([1, 2], [2, 1]), nullIf([], []),
       nullIf({a: 1, b: 2}, {a: 1, b: 2}), nullIf({a: 1}, {a: 2}),
       nullIf({}, {}), nullIf([1, 2], [1, 2, 3]);
-- mixed operand kinds: the comparison coerces through cypher "=", where a
-- bare numeric-looking string literal parses as a jsonb number (so '1' = 1)
RETURN nullIf(1, '1'), nullIf('1', 1), nullIf(true, 1),
       nullIf(1, true), nullIf([1], 1);

-- property operands (the everyday use): equal folds away, else the value.
-- v0 carries a number (o.i = 7), a bool (o.t = true), a list (l), a map (f)
-- and a json null (o.z); a stored number is NOT equal to a stored string.
MATCH (n:v0)
RETURN nullIf(n.o.i, 7) AS eq_num, nullIf(n.o.i, 8) AS ne_num,
       nullIf(n.o.i, n.o.i) AS prop_prop, nullIf(n.o.t, true) AS eq_bool,
       nullIf(n.o.i, 's') AS num_vs_str, nullIf(n.l, n.l) AS eq_list,
       nullIf(n.f, n.f) AS eq_map, nullIf(n.o.z, 1) AS null_prop;

-- graph-element identity: whole vertices and graphids compare by identity
MATCH (a:v1 {i: 0}), (b:v1 {i: 1})
RETURN nullIf(a, a) IS NULL AS same_vtx, nullIf(a, b) IS NULL AS diff_vtx,
       nullIf(id(a), id(a)) IS NULL AS same_id,
       nullIf(id(a), id(b)) IS NULL AS diff_id,
-- distinct elements are unequal, so NULLIF returns the first one unchanged
       nullIf(a, b) = a AS returns_first,
       nullIf(id(a), id(b)) = id(a) AS id_first;
-- a whole vertex has no "=" against a bare graphid: report the missing operator
MATCH (a:v1 {i: 0}) RETURN nullIf(a, id(a));

-- NULL in either or both positions: NULLIF(NULL, x) / NULLIF(NULL, NULL) = NULL,
-- NULLIF(x, NULL) = x (the comparison is never true, so the first arg survives)
RETURN nullIf(NULL, NULL), nullIf(NULL, 1), nullIf(1, NULL),
       nullIf(NULL, 'a'), nullIf(NULL, [1]), nullIf([1], NULL);

-- composition: inside COALESCE (replace a sentinel value with a default)
MATCH (n:v1) RETURN n.i, coalesce(nullIf(n.i, 0), -99) AS c ORDER BY n.i;
-- inside CASE
MATCH (n:v1)
RETURN n.i, CASE WHEN nullIf(n.i, 0) IS NULL THEN 'zero' ELSE 'nonzero' END AS lbl
ORDER BY n.i;
-- inside arithmetic (the NULL from NULLIF propagates)
MATCH (n:v1) RETURN n.i, nullIf(n.i, 0) + 10 AS a ORDER BY n.i;
-- inside a list literal and a list comprehension (projection and filter)
RETURN [nullIf(1, 1), nullIf(2, 3), nullIf('x', 'x')] AS lst;
RETURN [x IN [0, 1, 2, 3] | nullIf(x, 2)] AS proj;
RETURN [x IN [0, 1, 2, 3] WHERE nullIf(x, 2) IS NULL] AS filt;
-- nested NULLIF
RETURN nullIf(nullIf(5, 5), 3) AS a, nullIf(nullIf(5, 4), 5) AS b,
       nullIf(nullIf(5, 4), 3) AS c;

-- aggregation: the classic "aggregate the values that are not the sentinel"
MATCH (n:v1) RETURN count(nullIf(n.i, 0)) AS non_zero, count(*) AS total;
MATCH (n:v1) RETURN sum(nullIf(n.i, 0)) AS s;
-- NULLIF as a grouping key (the NULL group is kept)
MATCH (n:v1) RETURN nullIf(n.i, 0) AS k, count(*) AS c ORDER BY k;

-- clause positions: WHERE, WITH, ORDER BY (NULLs sort last by default)
MATCH (n:v1) WHERE nullIf(n.i, 1) IS NOT NULL RETURN n.i ORDER BY n.i;
MATCH (n:v1) WITH nullIf(n.i, 0) AS k WHERE k IS NOT NULL RETURN k ORDER BY k;
MATCH (n:v1) RETURN n.i ORDER BY nullIf(n.i, 0), n.i;

-- IS [NOT] NULL and the three-valued list predicates
RETURN nullIf(1, 1) IS NULL, nullIf(1, 2) IS NULL,
       nullIf(1, 1) IS NOT NULL, nullIf(1, 2) IS NOT NULL;
RETURN any(x IN [1, 2, 3] WHERE nullIf(x, 2) IS NULL) AS any_null,
       all(x IN [1, 2, 3] WHERE nullIf(x, 2) IS NOT NULL) AS all_nn,
       none(x IN [1, 2, 3] WHERE nullIf(x, 0) IS NULL) AS none_null,
       single(x IN [1, 2, 3] WHERE nullIf(x, 2) IS NULL) AS single_null;

-- parameters: string params arrive as JSON, numeric-looking strings fold to
-- numbers, so nullIf($1, 1) with '1' is NULL and with '2' is 2
PREPARE np AS RETURN nullIf($1, $2);
EXECUTE np ('"a"', '"a"');
EXECUTE np ('"a"', '"b"');
DEALLOCATE np;
PREPARE np1 AS RETURN nullIf($1, 1);
EXECUTE np1 ('1');
EXECUTE np1 ('2');
DEALLOCATE np1;

-- case-insensitive spelling: NULLIF / nullif / mixed are the same keyword
RETURN NULLIF(1, 1) AS upper, nullif(1, 1) AS lower, nUlLiF(1, 1) AS mixed;

-- single evaluation of the first operand: over v1's three rows the sequence
-- advances by exactly 3 (not 6), so nextval() runs once per NULLIF
CREATE SEQUENCE nullif_eval_seq;
MATCH (n:v1) WITH nullIf(nextval('nullif_eval_seq'), -1) AS s
RETURN count(s) AS rows;
SELECT currval('nullif_eval_seq') AS seq_value;
DROP SEQUENCE nullif_eval_seq;
-- structurally the plan carries a genuine NULLIF node (single-eval by design)
EXPLAIN (VERBOSE, COSTS OFF) MATCH (n:v1) RETURN nullIf(n.i, 0);

-- self-check: for non-null operands, NULLIF(a, b) IS NULL exactly when a = b
MATCH (n:v0)
RETURN (nullIf(n.o.i, 7) IS NULL) = (n.o.i = 7) AS o1,
       (nullIf(n.o.i, 8) IS NULL) = (n.o.i = 8) AS o2,
       (nullIf(n.o.t, true) IS NULL) = (n.o.t = true) AS o3,
       (nullIf(n.l, n.l) IS NULL) = (n.l = n.l) AS o4;

-- SQL-side NULLIF still works and agrees with the cypher form on the same
-- inputs (only the rendering differs: text vs jsonb)
SELECT NULLIF(1, 1) AS a, NULLIF(1, 2) AS b,
       NULLIF('a'::text, 'a'::text) AS c, NULLIF('a'::text, 'b'::text) AS d;

-- error paths: NULLIF takes exactly two arguments
RETURN nullIf(1);
RETURN nullIf(1, 2, 3);
RETURN nullIf();

-- IN expression

MATCH (n:v0) RETURN true IN n.l;
MATCH (n:v0) RETURN 0 IN n.l;
MATCH (n:v0) RETURN NULL IN n.l;
MATCH (n:v0) WITH n.l[0] AS i RETURN [(i IN [0, 1, 2, 3, 4]), true];

CREATE (:v2 {i: 0}), (:v2 {i: 1}), (:v2 {i: 2}), (:v2 {i: 3}),
       (:v2 {i: 4}), (:v2 {i: 5}), (:v2 {i: 6}), (:v2 {i: 7}),
       (:v2 {i: 8}), (:v2 {i: 9}), (:v2 {i: 10}), (:v2 {i: 11}),
       (:v2 {i: 12}), (:v2 {i: 13}), (:v2 {i: 14}), (:v2 {i: 15});
CREATE (:v2 {i: 7, name: 'seven'}), (:v2 {i: 9, name: 'nine'});
CREATE PROPERTY INDEX ON v2 (i);

-- check grammar
RETURN 1 IN 1;
RETURN 1 IN [1];

-- SubLink
CREATE TABLE t1 (i int);
INSERT INTO t1 VALUES (1), (2), (3);
MATCH (n:v2) WHERE n.i IN (SELECT to_jsonb(i) FROM t1)
RETURN count(n);

-- plan : index scan
SET enable_seqscan = off;
EXPLAIN (costs off)
  MATCH (n:v2) WHERE n.i IN [1, 2, 3]
  RETURN n;
EXPLAIN (costs off)
  MATCH (n1:v2 {name: 'seven'}), (n2:v2 {name: 'nine'})
  MATCH (n:v2) WHERE n.i IN [n1.i, 8, n2.i]
  RETURN n;
SET enable_seqscan = on;

-- plan : seq scan
CREATE (:v3 {a: [1, 2, 3, 4, 5, 6, 7, 8, 9]});
EXPLAIN (costs off)
  MATCH (n1:v2), (n2:v3) WHERE n1.i IN n2.a
  RETURN n1;
EXPLAIN (costs off)
  MATCH (n2:v3) WITH n2.a AS a
  MATCH (n1:v2) WHERE n1.i IN a
  RETURN n1;

-- List comprehension
RETURN [x IN [0, 1, 2, 3, 4]];
RETURN [x IN [0, 1, 2, 3, 4] WHERE x % 2 = 0];
RETURN [x IN [0, 1, 2, 3, 4] | x + 1];
RETURN [x IN [0, 1, 2, 3, 4] WHERE x % 2 = 0 | x + 1];
-- nested use of variables
RETURN [x IN [[0], [1]] WHERE length([y IN x]) = 1 | [y IN x]];
-- an aggregate in the projection or filter has no group here: reject cleanly
-- rather than crash the backend (#793)
RETURN [x IN [1, 2, 3] | sum(x)];
RETURN [x IN [1, 2, 3] WHERE sum(x) > 0 | x];

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

-- a null list element is the Cypher null value: it is filtered by IS NOT NULL
-- and does not prematurely end the iteration (#780)
RETURN [x IN [1, null, 3] WHERE x IS NOT NULL];
RETURN [x IN [null, null] | x IS NULL];
RETURN ALL(x in [null, null, null] WHERE x IS NOT NULL);
RETURN ANY(x in [null, null, null] WHERE x IS NOT NULL);
RETURN NONE(x in [null, null, null] WHERE x IS NOT NULL);
RETURN ALL(x in [1, null, 3] WHERE x IS NOT NULL);
RETURN ANY(x in [1, null, 3] WHERE x IS NOT NULL);
RETURN NONE(x in [1, null, 3] WHERE x IS NOT NULL);

-- three-valued logic: when the predicate is unknown (null) for elements and no
-- element gives a definite deciding result, the list predicate returns null,
-- not a definite boolean (#779).  Comments give the Neo4j-equivalent result.
RETURN ANY(x in [null] WHERE x > 10);           -- null
RETURN ALL(x in [null] WHERE x > 10);           -- null
RETURN NONE(x in [null] WHERE x > 10);          -- null
RETURN SINGLE(x in [null] WHERE x > 10);        -- null
RETURN ANY(x in [1, null] WHERE x > 10);        -- null (no true, one unknown)
RETURN ANY(x in [20, null] WHERE x > 10);       -- true (a definite true short-circuits)
RETURN ALL(x in [1, null] WHERE x > 10);        -- false (a definite false decides it)
RETURN ALL(x in [20, null] WHERE x > 10);       -- null (true so far, one unknown)
RETURN NONE(x in [1, null] WHERE x > 10);       -- null (no true, one unknown)
RETURN NONE(x in [20, null] WHERE x > 10);      -- false (a definite true)
RETURN SINGLE(x in [20, null] WHERE x > 10);    -- null (one true, an unknown could be a second)
RETURN SINGLE(x in [20, 30] WHERE x > 10);      -- false (two true)
RETURN SINGLE(x in [1, 20] WHERE x > 10);       -- true (exactly one true, no unknown)
-- a null list argument is unknown throughout
RETURN ANY(x in NULL WHERE x > 10);             -- null

-- Functions

CREATE (:coll {name: 'AgensGraph'});
MATCH (n:coll) SET n.l = tolower(n.name);
MATCH (n:coll) SET n.u = toupper(n.name);
MATCH (n:coll) RETURN n;

-- Text matching

CREATE (:ts {v: 'a fat cat sat on a mat and ate a fat rat'::tsvector});
MATCH (n:ts) WHERE n.v::tsvector @@ 'cat & rat'::tsquery RETURN n;

--
-- Default alias check
--
MATCH (_{i:0}) RETURN _;
MATCH ({i:0}) MATCH (_{i:0}) RETURN 0;
MATCH ({i:0}) MATCH (_{i:0}) RETURN _;
MATCH (my_agens_default_{i:0}) RETURN my_agens_default_;
MATCH ({i:0}) MATCH (my_agens_default_{i:0}) RETURN my_agens_default_;

-- these should fail as they are prefixed with _agens_default_ which is only for internal use
MATCH (_agens_default_) RETURN _agens_default_;
MATCH (_agens_default_a) RETURN _agens_default_a;
MATCH (_agens_default_whatever) RETURN 0;

-- Test to retain typecast to user-defined datatype
CREATE (:tc {i: 1, s: 'test', b: true, l: [1, 2, 3], o: {p: 'p'}});
SELECT pg_typeof(n) FROM (MATCH (n:tc) RETURN n::jsonb::json as n)a;
SELECT pg_typeof(n) FROM (RETURN '08:00:2b:01:02:03'::macaddr as n)a;
SELECT pg_typeof(n) FROM (MATCH (n:tc) return n.s::tsvector as n)a;

EXPLAIN VERBOSE MATCH (n:tc) RETURN n::jsonb::json as n;
EXPLAIN RETURN '08:00:2b:01:02:03'::macaddr as n;
EXPLAIN VERBOSE MATCH (n:tc) return n.s::tsvector as n;

-- AGV2-437
CREATE GRAPH agv2_437;
SET graph_path = agv2_437;

CREATE (:item {id: '1', tags: '1,2,3', name: 'Alice'});
CREATE (:item {id: '2', tags: '4,5,6', name: 'Bob'});
CREATE (:item {id: '3', tags: '1,5,9', name: 'Charlie'});
CREATE (:item)-[:KNOWS]->(:item);
CREATE (:item)-[:LIKES]->(:item);

MATCH (n:item) WHERE n.id IN split(n.tags, ',') RETURN n.id;
MATCH (n:item) WHERE '1' IN split(n.tags, ',') RETURN n.id;
MATCH (n:item) WHERE '5' IN split(n.tags, ',') RETURN n.id;

RETURN 1 IN split('1,2,3', ',');
RETURN '1' IN split('1,2,3', ',');
RETURN 'x' IN split('a,b,c', ',');

MATCH (n:item) WHERE n.id IN ['1', '2'] RETURN n.id;
MATCH (n:item) WHERE n.name IN ['Alice', 'Bob'] RETURN n.name;
MATCH (n:item) WHERE toLower(n.name) IN ['alice', 'charlie'] RETURN n.name;

MATCH ()-[r]->() WHERE type(r) IN ['KNOWS', 'LIKES'] RETURN type(r);
MATCH ()-[r]->() WHERE type(r) IN ['KNOWS'] RETURN type(r);

RETURN 1 IN [];
RETURN 'a' IN [];

DROP GRAPH agv2_437 CASCADE;

-- XOR (boolean inequality; NULL-propagating; same precedence as OR, below AND)
RETURN true XOR true;
RETURN true XOR false;
RETURN false XOR true;
RETURN false XOR false;
-- NULL propagation (three-valued logic)
RETURN true XOR NULL;
RETURN NULL XOR false;
RETURN NULL XOR NULL;
-- precedence: OR and XOR share one left-assoc level; AND binds tighter
RETURN true OR false XOR true;
RETURN true XOR true AND false;
RETURN true XOR false XOR true;

-- NULLIF: whole-edge identity
-- This is the only NULLIF case that needs an edge.  Its label is created here,
-- at the very end, so it does not renumber any label used by the sections
-- above (their golden output stays byte-identical); only the teardown cascade
-- below gains this one label.  The endpoints reuse existing v1 vertices.
SET graph_path = test_cypher_expr;
MATCH (a:v1 {i: 0}), (b:v1 {i: 1}) CREATE (a)-[:NEDGE {w: 1}]->(b);
MATCH ()-[r:NEDGE]->()
RETURN nullIf(r, r) IS NULL AS same_edge,
       nullIf(id(r), id(r)) IS NULL AS same_edge_id;

-- ALL quantifier in aggregate functions (GQL clause 22.15; Neo4j 5.15+).  ALL is
-- the explicit dual of DISTINCT: it keeps every value, including duplicates, so
-- "agg(ALL x)" is identical to the unquantified "agg(x)", whereas DISTINCT drops
-- duplicates.  A label with repeated values makes the difference observable.  It
-- is created here, at the end, so it does not renumber the labels above.
SET graph_path = test_cypher_expr;
CREATE (:aggnum {v: 1});
CREATE (:aggnum {v: 1});
CREATE (:aggnum {v: 2});
CREATE (:aggnum {v: 3});
CREATE (:aggnum {v: 3});

-- count / sum / collect: ALL matches the plain form and keeps duplicates,
-- DISTINCT drops them.
MATCH (n:aggnum) RETURN count(n.v) AS c_plain, count(ALL n.v) AS c_all, count(DISTINCT n.v) AS c_dist;
MATCH (n:aggnum) RETURN sum(n.v) AS s_plain, sum(ALL n.v) AS s_all, sum(DISTINCT n.v) AS s_dist;
MATCH (n:aggnum) RETURN collect(ALL n.v) AS all_list, collect(DISTINCT n.v) AS dist_list ORDER BY 1;
MATCH (n:aggnum) RETURN avg(ALL n.v) AS a_all;

-- ALL over an arbitrary expression, and over a whole graph element.
MATCH (n:aggnum) RETURN count(ALL n.v + 1) AS c, sum(ALL n.v * 2) AS s;
MATCH (n:aggnum) RETURN count(ALL n) AS c;

-- The ALL quantifier leaves the all()/any()/none()/single() list predicates
-- untouched ...
RETURN all(x IN [1, 2, 3] WHERE x > 0) AS a,
       any(x IN [1, 2, 3] WHERE x > 2) AS b,
       none(x IN [1, 2, 3] WHERE x > 5) AS c,
       single(x IN [1, 2, 3] WHERE x = 2) AS d;
-- ... and IN membership unaffected ...
RETURN 2 IN [1, 2, 3] AS present, 9 IN [1, 2, 3] AS absent;
-- ... and an all() predicate passed as an aggregate argument stays the predicate.
MATCH (n:aggnum) RETURN count(all(x IN [n.v] WHERE x > 0)) AS c;

-- ALL and DISTINCT cannot be combined, and ALL is not allowed before '*'.
MATCH (n:aggnum) RETURN count(ALL DISTINCT n.v);
MATCH (n:aggnum) RETURN count(DISTINCT ALL n.v);
MATCH (n:aggnum) RETURN count(ALL *);

-- min() and max() over jsonb compare by value and return the true extremes; the
-- plain, ALL, and DISTINCT forms all agree on them.
MATCH (n:aggnum) RETURN min(n.v) AS mn, max(n.v) AS mx,
                        min(ALL n.v) AS mn_all, max(DISTINCT n.v) AS mx_dist;

-- Tear down
DROP TABLE t1;
DROP GRAPH test_cypher_expr CASCADE;
