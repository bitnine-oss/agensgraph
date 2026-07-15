--
-- Cypher Query Language - CALL func(args) YIELD ... (GQL named-routine call)
--
-- "CALL func(args) YIELD ..." invokes a table-returning routine (a set-returning
-- or composite/TABLE-returning PostgreSQL function -- the analog of a GQL table-
-- valued function / Neo4j procedure) and adds its yielded columns to the working
-- table.  It is a NON-HEAD cypher clause: it keys off a plain CALL (the lexer
-- only rewrites CALL to CALL_LA before '(' or '{'; here CALL is followed by a
-- function name), so it never collides with either the SQL CALL statement or the
-- existing "CALL { subquery }" clause.
--
-- The clause is transformed into a function scan analyzed as a subquery join:
-- "SELECT <yield items> FROM func(args)".  When a previous clause exists (which
-- it always does, since the clause cannot lead a query) the subquery is LATERAL,
-- so the arguments may reference variables bound earlier -- a correlated call is
-- then executed per outer row.  With constant (uncorrelated) arguments the
-- routine runs once and cross-joins with the outer rows.  The YIELD list projects
-- and optionally renames the routine's own output columns, which surface to the
-- clauses that follow.
--
-- YIELD is mandatory in this form.  "YIELD *" adds every output column; "YIELD c"
-- keeps the routine's column name c; "YIELD c AS a" renames it to a.  A yielded
-- name that collides with a variable already in scope is rejected (rename it with
-- AS).  The clause is non-terminal (a query cannot end with CALL ... YIELD) and
-- non-leading (it cannot be the first clause -- that falls to the SQL CALL
-- statement); a stand-alone top-level "CALL f() YIELD ..." is likewise a syntax
-- error.  All three are deliberate boundaries, covered below as negative cases.
--
-- Cypher expression arguments are typed naturally where possible (an integer or
-- string literal keeps its SQL type), but a property access yields jsonb; passing
-- a jsonb property to a routine whose parameter is a non-jsonb SQL type (e.g. int)
-- therefore fails resolution.  Routines taking jsonb / text parameters accept
-- properties directly.  This interop constraint is covered as a negative case.
--
-- These tests cover: multi-column TABLE routines with every YIELD form, composite
-- / OUT-parameter routines and single-column set-returning functions yielded by
-- natural name, uncorrelated cross-join cardinality, correlated per-row execution
-- (with a plan-shape check), AS-renaming and the in-scope collision error, the
-- zero-argument form, composition with FILTER / WITH / aggregation / a following
-- MATCH, chaining several CALL ... YIELD clauses (including one correlated on a
-- prior yielded column), the placement guard errors, the jsonb-property/int-
-- parameter typing boundary, empty-result inner-join semantics, and backward
-- compatibility of "yield" as an ordinary identifier plus keyword case-folding.
--

-- Set up
CREATE GRAPH cypher_yield;
SET graph_path = cypher_yield;

-- Fixtures.  person is created first so its label id stays low and stable; this
-- suite never returns a whole vertex/edge, so only property and scalar values
-- appear in the golden output and label ids are never rendered.
CREATE (:person {id: 1, name: 'alice', age: 30});
CREATE (:person {id: 2, name: 'bob',   age: 25});
CREATE (:person {id: 3, name: 'carol', age: 40});
CREATE (:person {id: 4, name: 'dave',  age: 25});

-- Test routines.  Defined here and dropped in teardown.  Each models a distinct
-- table-returning shape the clause must support.

-- A multi-column TABLE routine returning a fixed set of rows.
CREATE FUNCTION yc_table() RETURNS TABLE(a int, b text) LANGUAGE sql AS
$$ SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) v(a, b) $$;

-- A multi-column routine whose parameters are jsonb / int, so a property (jsonb)
-- may be passed as an argument for a correlated per-row call.  It emits k rows,
-- each tagged with the label argument.
CREATE FUNCTION yc_dup(label jsonb, k int) RETURNS TABLE(seq int, tag jsonb)
LANGUAGE sql AS
$$ SELECT g, label FROM generate_series(1, k) g $$;

-- A composite routine defined via OUT parameters: its output column names are the
-- OUT-parameter names lo / hi, which YIELD references directly.
CREATE FUNCTION yc_out(IN n int, OUT lo int, OUT hi int) RETURNS SETOF record
LANGUAGE sql AS
$$ SELECT g, g * 10 FROM generate_series(1, n) g $$;

-- A zero-argument routine.
CREATE FUNCTION yc_zero() RETURNS TABLE(z int) LANGUAGE sql AS $$ SELECT 42 $$;

-- A routine returning person ids, used to feed a following MATCH and to correlate
-- a second CALL.
CREATE FUNCTION yc_ids() RETURNS TABLE(pid int) LANGUAGE sql AS
$$ SELECT * FROM (VALUES (1), (2)) v(pid) $$;

-- A jsonb-argument routine that returns one row only when the argument is >= 30,
-- and zero rows otherwise -- used to exercise empty-result inner-join semantics.
CREATE FUNCTION yc_ge30(age jsonb) RETURNS TABLE(ok int) LANGUAGE sql AS
$$ SELECT 1 WHERE (age #>> '{}')::int >= 30 $$;

-- A routine whose single parameter is a plain int, used only to demonstrate the
-- jsonb-property/int-parameter typing boundary.
CREATE FUNCTION yc_int(n int) RETURNS TABLE(x int) LANGUAGE sql AS $$ SELECT n $$;

--
-- 1. Multi-column TABLE routine -- every YIELD form
--

-- YIELD naming both output columns by their routine names.
MATCH (p:person {id: 1}) CALL yc_table() YIELD a, b
RETURN a, b ORDER BY a;

-- YIELD renaming both columns with AS.
MATCH (p:person {id: 1}) CALL yc_table() YIELD a AS x, b AS y
RETURN x, y ORDER BY x;

-- YIELD * adds every output column of the routine.
MATCH (p:person {id: 1}) CALL yc_table() YIELD *
RETURN a, b ORDER BY a;

-- YIELD a proper subset of the output columns (only a; b is not yielded and is
-- not visible afterwards).
MATCH (p:person {id: 1}) CALL yc_table() YIELD a
RETURN a ORDER BY a;

--
-- 2. Composite / OUT-parameter routine and single-column set-returning functions,
--    yielded by natural column name
--

-- An OUT-parameter routine: its columns are the OUT-parameter names lo / hi.
MATCH (p:person {id: 1}) CALL yc_out(3) YIELD lo, hi
RETURN lo, hi ORDER BY lo;

-- A single-column SRF: generate_series' output column is named generate_series.
MATCH (p:person {id: 1}) CALL generate_series(1, 3) YIELD generate_series
RETURN generate_series ORDER BY generate_series;

-- jsonb_array_elements' output column is named value; a string literal argument is
-- accepted by its jsonb parameter.
MATCH (p:person {id: 1}) CALL jsonb_array_elements('[10, 20, 30]') YIELD value
RETURN value ORDER BY value;

-- The natural single-column name is renamable with AS like any other.
MATCH (p:person {id: 1}) CALL generate_series(1, 2) YIELD generate_series AS g
RETURN g ORDER BY g;

--
-- 3. Uncorrelated call (constant arguments) -- the routine runs once and cross-
--    joins with the outer rows
--

-- Four outer rows times three routine rows = twelve rows: the cross-join
-- cardinality of an uncorrelated CALL.
MATCH (p:person) CALL yc_table() YIELD a, b
RETURN p.id AS id, a, b ORDER BY id, a;

-- A one-row outer times a three-row routine is a plain three-row cross join.
MATCH (p:person {id: 1}) CALL yc_table() YIELD a
RETURN p.name AS nm, a ORDER BY a;

--
-- 4. Correlated call (arguments reference a prior variable) -- executed per outer
--    row as a LATERAL join
--

-- The argument p.name (a jsonb property) references the prior MATCH, so the
-- routine is called once per person; yc_dup emits two rows per call.
MATCH (p:person) WHERE p.id <= 2 CALL yc_dup(p.name, 2) YIELD seq, tag
RETURN p.id AS id, seq, tag ORDER BY id, seq;

-- An argument may also reference a WITH-introduced scalar, not only a raw
-- property.  age is a jsonb value carried across the WITH boundary.
MATCH (p:person) WITH p, p.age AS age CALL yc_ge30(age) YIELD ok
RETURN p.name AS nm, ok ORDER BY nm;

-- Plan shape of a correlated call: the LATERAL dependency forces the person scan
-- to drive a per-row Function Scan on the inner side of a Nested Loop.
EXPLAIN (COSTS OFF)
MATCH (p:person) CALL yc_dup(p.name, 2) YIELD seq, tag
RETURN p.id AS id, seq, tag;

--
-- 5. YIELD ... AS renaming and the in-scope collision error
--

-- A yielded column whose name equals a variable already in scope (the person is
-- bound as a) is rejected: it would shadow the outer binding.
MATCH (a:person {id: 1}) CALL yc_table() YIELD a
RETURN a;

-- Renaming the yielded column with AS resolves the collision; a stays the person.
MATCH (a:person {id: 1}) CALL yc_table() YIELD a AS av, b
RETURN a.name AS nm, av, b ORDER BY av;

-- The collision check also applies to YIELD *: yc_ids yields pid, which collides
-- with the WITH-introduced pid.
MATCH (p:person) WITH p.id AS pid CALL yc_ids() YIELD *
RETURN pid;

-- Two yielded columns may not share a name -- a duplicate within one YIELD list
-- (the same column twice, or two columns aliased to the same name) is rejected.
MATCH (p:person {id: 1}) CALL yc_table() YIELD a, a RETURN a;
MATCH (p:person {id: 1}) CALL yc_table() YIELD a AS x, b AS x RETURN x;

--
-- 6. Composition with the clauses that follow
--

-- A trailing FILTER after YIELD is a separate clause and restricts the yielded
-- rows (there is no WHERE built into the YIELD clause itself).
MATCH (p:person {id: 1}) CALL yc_table() YIELD a, b FILTER a >= 2
RETURN a, b ORDER BY a;

-- YIELD feeding a WITH boundary and then an aggregation: three rows per person,
-- one filtered out, so each person contributes a count of two.
MATCH (p:person) CALL yc_table() YIELD a
WITH p, a WHERE a <> 2
RETURN p.name AS nm, count(*) AS c ORDER BY nm;

-- CALL ... YIELD after a WITH clause (the previous clause is the WITH).
MATCH (p:person) WITH p.name AS nm CALL yc_table() YIELD a
RETURN nm, a ORDER BY nm, a;

-- A yielded column used by a following MATCH: yc_ids yields person ids, which the
-- next MATCH uses to look those persons up.
MATCH (p:person {id: 1}) CALL yc_ids() YIELD pid
MATCH (q:person) WHERE q.id = pid
RETURN q.name AS nm ORDER BY nm;

--
-- 7. Zero-argument routine
--

MATCH (p:person {id: 1}) CALL yc_zero() YIELD z
RETURN z;

--
-- 8. Placement guard errors
--

-- Leading: CALL ... YIELD cannot be the first clause of a query.  A leading CALL
-- is parsed as the SQL CALL statement, so YIELD is unexpected.
CALL yc_table() YIELD a, b RETURN a, b;

-- Terminal: a query cannot end with CALL ... YIELD; it must be followed by a
-- terminating clause.
MATCH (p:person {id: 1}) CALL yc_table() YIELD a, b;

-- Stand-alone top-level CALL ... YIELD is likewise a syntax error (it collides
-- with the SQL CALL statement).
CALL yc_table() YIELD a, b;

-- YIELD is mandatory: a mid-query CALL of a function without YIELD does not parse.
MATCH (p:person {id: 1}) CALL yc_table() RETURN 1;

--
-- 9. Chaining several CALL ... YIELD clauses
--

-- Two uncorrelated calls chained: yc_zero (one row) then yc_table (three rows).
MATCH (p:person {id: 1}) CALL yc_zero() YIELD z CALL yc_table() YIELD a, b
RETURN z, a, b ORDER BY a;

-- The second call correlated on the first's yielded column: yc_ids yields pid,
-- which becomes the row-count argument to yc_dup, so pid = 1 yields one row and
-- pid = 2 yields two.
MATCH (p:person {id: 1}) CALL yc_ids() YIELD pid
CALL yc_dup(p.name, pid) YIELD seq, tag
RETURN pid, seq, tag ORDER BY pid, seq;

-- CALL ... YIELD nested inside a larger pipeline: MATCH, CALL, FILTER, WITH,
-- aggregation, terminal RETURN.
MATCH (p:person) WHERE p.age = 25 CALL yc_table() YIELD a, b
FILTER a <= 2
WITH p.name AS nm, b
RETURN nm, count(*) AS c ORDER BY nm;

--
-- 10. Backward compatibility -- "yield" is an unreserved keyword, still usable as
--     an ordinary identifier; and the clause keywords fold case
--

-- as a variable name
MATCH (yield:person {id: 1}) RETURN yield.name AS name;

-- as a label and a property key
CREATE (:yield {yield: 1});
MATCH (x:yield) RETURN x.yield AS v;

-- as a RETURN alias
RETURN 1 AS yield;

-- the YIELD keyword itself is case-insensitive
MATCH (p:person {id: 1}) CALL yc_table() yield a RETURN a ORDER BY a;
MATCH (p:person {id: 1}) CALL yc_table() YiElD a RETURN a ORDER BY a;

-- the CALL keyword folds case too
MATCH (p:person {id: 1}) Call yc_table() YIELD a RETURN a ORDER BY a;

--
-- 11. Typing boundary (negative) -- a jsonb property cannot satisfy an int
--     parameter
--
-- p.age is a jsonb property; yc_int takes a plain int, so no overload matches.
-- This documents the Cypher-argument/SQL-parameter interop constraint.

MATCH (p:person) CALL yc_int(p.age) YIELD x RETURN x;

--
-- 12. Empty results -- a routine that returns zero rows for some inputs
--
-- yc_ge30 returns a row only for age >= 30, so persons younger than 30 produce no
-- yielded row and, under the CALL's inner-join semantics, are dropped from the
-- output entirely (OPTIONAL CALL is not implemented).

MATCH (p:person) CALL yc_ge30(p.age) YIELD ok
RETURN p.name AS nm, ok ORDER BY nm;

--
-- 13. Built-in PostgreSQL set-returning functions as CALL routines
--
-- Any set-returning / table function whose parameters accept Cypher-typed
-- arguments (jsonb, or literals that fold to the expected type) works as a CALL
-- routine, yielded by the routine's own output column names.

-- jsonb_each: a two-column (key, value) table function
MATCH (p:person {id: 1}) CALL jsonb_each('{"a": 1, "b": 2}') YIELD key, value
RETURN key, value ORDER BY key;
-- jsonb_object_keys: a single column named after the function
MATCH (p:person {id: 1}) CALL jsonb_object_keys('{"x": 1, "y": 2}') YIELD jsonb_object_keys AS k
RETURN k ORDER BY k;
-- regexp_split_to_table and string_to_table: text-splitting table functions
MATCH (p:person {id: 1}) CALL regexp_split_to_table('a,b,c', ',') YIELD regexp_split_to_table AS part
RETURN part ORDER BY part;
MATCH (p:person {id: 1}) CALL string_to_table('x|y|z', '|') YIELD string_to_table AS s
RETURN s ORDER BY s;
-- correlated: explode a vertex's own property map, once per matched row
MATCH (p:person {id: 1}) CALL jsonb_each(properties(p)) YIELD key, value
RETURN key, value ORDER BY key;

-- A set-returning function is reachable with or without CALL ... YIELD: the CALL
-- form yields the same rows as the native list unnest and as plain SQL.
MATCH (p:person {id: 1}) CALL jsonb_array_elements('[10, 20, 30]') YIELD value AS v
RETURN v ORDER BY v;
MATCH (p:person {id: 1}) UNWIND [10, 20, 30] AS v
RETURN v ORDER BY v;
SELECT value FROM jsonb_array_elements('[10, 20, 30]'::jsonb) AS value ORDER BY value;

-- Tear down
DROP FUNCTION yc_table();
DROP FUNCTION yc_dup(jsonb, int);
DROP FUNCTION yc_out(int);
DROP FUNCTION yc_zero();
DROP FUNCTION yc_ids();
DROP FUNCTION yc_ge30(jsonb);
DROP FUNCTION yc_int(int);
DROP GRAPH cypher_yield CASCADE;
