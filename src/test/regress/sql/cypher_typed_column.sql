--
-- Cypher Query Language - Typed-Column Property Promotion (read resolution)
--
-- A graph label can promote a jsonb-bag property to a typed STORED generated
-- column:  CREATE VLABEL doc (age int GENERATED, ...).  The column is derived
-- from the bag as (properties ->> 'key')::type and can carry a btree / HNSW
-- index.  With enable_property_promotion = on (the default) a Cypher property
-- access n.prop that targets a promoted key is compiled against the typed
-- column instead of the jsonb bag, so filters, ORDER BY, GROUP BY, DISTINCT and
-- a projected n.prop bind the column's index and use typed (native) semantics.
-- The jsonb bag remains the complete source of truth; writes and whole-element
-- reads (RETURN n, properties(n), keys(n)) still go through the bag.
--
-- Unlike a cost-only planner GUC, enable_property_promotion CHANGES RESULTS:
-- native comparison / ordering / collation semantics differ from jsonb.  The
-- correctness bar is Cypher / typed semantics, NOT byte-for-byte identical
-- output with the jsonb path.  Many cases therefore run with the GUC on and
-- then off.  For type-agnostic behavior (integer equality, membership,
-- grouping) the two result blocks are identical.  For type-sensitive behavior
-- (native text collation, special float values) the blocks intentionally
-- differ, and the on result is the typed-correct one; each such case says so
-- in its comment.
--
-- Vector / HNSW coverage lives in cypher_typed_column_vector.sql because it
-- needs the pgvector extension, which is not present in a plain `make check`
-- temporary install.  This file has NO pgvector dependency.
--
-- Semantics these tests pin down (all intended behavior):
--   * Write domain: a promoted key's declared type constrains what may be
--     written.  A value that the plain bag would accept but the typed column
--     cannot parse (a fractional / out-of-range / non-numeric-string / composite
--     value for an int column) aborts the Cypher write with the column's cast
--     error.  Integer-parseable strings ('42') are accepted and read back typed.
--   * Special floats: NaN / Infinity / -Infinity cannot be jsonb numbers,
--     so a program can only store them as strings; the float column parses those
--     strings into real floats.  With promotion on n.ratio is therefore a float
--     and orders / compares by float semantics; off it is the stored string.
--     This is the same "a value's stored type differs from the column's"
--     behavior as the string-in-int case, made visible for floats.
--   * Text / collation: both the jsonb path and the promoted column order
--     and compare text with the database's default collation, so they coincide
--     (on == off) for all text, including mixed case -- there is no divergence.
--     The absolute order is whatever the server collation dictates.
--   * Inheritance: a child label resolves a promoted key inherited from an
--     ancestor to the child's own copy of the typed column (read and index); a
--     nearer label wins a name clash.
--
-- Like every cypher test, this creates and drops its own graph so the
-- regression database round-trips cleanly.
-- Sections:
--   1  DDL and catalog (what promotes, what is rejected)
--   2  Plan shape (EXPLAIN): promotion binds the typed column / its index
--   3  WHERE filters (on == off)
--   4  Cross-type comparison -- Cypher-strict (on == off)
--   5  ORDER BY -- native numeric semantics (on == off) and text
--   6  GROUP BY / DISTINCT -- the by-value grouping path (on == off)
--   7  Aggregates over a promoted property (on == off)
--   8  Text predicates (on == off) and native collation
--   9  Special / edge-case numeric values (NaN, Infinity, -0.0)
--   10 Multi-clause resolution: multi-MATCH, WITH carry, deep chains
--   11 Subqueries / correlation / UNION / paths
--   12 NULL / missing-key semantics
--   13 Expressions, functions, coexisting keys, inheritance
--   14 Write path (bag stays source of truth) and re-read
--   15 ALTER label ADD / DROP a promoted column (backfill, catalog, recursion)
--   16 Structural names never promote
--   17 GUC behavior
--   18 Known limitations (labeled)
--   CLEANUP
--

DROP GRAPH IF EXISTS tc CASCADE;
CREATE GRAPH tc;
SET graph_path = tc;

-- ============================================================================
-- SECTION 1 -- DDL and catalog
-- ============================================================================

-- 1a. The core fixture label promotes one column of every promotable kind.
--     age/big are by-value numerics, ratio a float, score an arbitrary-scale
--     numeric, active a boolean, title text.  "name" stays an ordinary
--     (non-promoted) bag key used to identify rows deterministically.
CREATE VLABEL doc (
	age    int      GENERATED,
	big    bigint   GENERATED,
	ratio  float    GENERATED,
	score  numeric  GENERATED,
	active bool     GENERATED,
	title  text     GENERATED
);
CREATE VLABEL plain;                       -- promotes nothing
CREATE VLABEL othertype (age text GENERATED);   -- same key name, different type
CREATE ELABEL knows;                       -- no promoted edge property
CREATE ELABEL rel (weight int GENERATED);  -- promoted edge property

-- 1b. Long-form and source-key-shorthand GENERATED both work.
CREATE VLABEL ddlvar1 (score numeric GENERATED ALWAYS AS ((properties->>'score')::numeric) STORED);
CREATE VLABEL ddlvar2 (yrs int GENERATED (age));   -- column yrs derived from bag key "age"
DROP VLABEL ddlvar1;
DROP VLABEL ddlvar2;

-- 1c. A source key is down-cased when the column is derived: "GENERATED (Key)"
--     materialises from the bag key "key", so the PROMOTED PROPERTY is the
--     lower-cased key (recorded in the catalog, indexed, and resolved) while the
--     column may carry a different name.  Here column "v" promotes property
--     "mixedkey" from source spelling "MixedKey".
CREATE VLABEL mixed (v int GENERATED (MixedKey));
CREATE (:mixed {name:'m1', mixedkey:5}), (:mixed {name:'m2', mixedkey:9});

-- 1d. Rejections: temporal casts are not immutable, so timestamptz / date
--     GENERATED columns are refused at DDL time.
CREATE VLABEL bad_ts (ts timestamptz GENERATED);
CREATE VLABEL bad_date (d date GENERATED);

-- 1e. A bare, non-generated promoted column (a type with no GENERATED clause)
--     is the deferred write-routed form and is refused with a message that
--     points at GENERATED.  The old "col type PROPERTY" spelling is retired --
--     PROPERTY is no longer a column form (it is reserved for CREATE / DROP
--     PROPERTY INDEX) -- so it is now a plain syntax error.
CREATE VLABEL bad_prop (x int);
CREATE VLABEL bad_prop (x int PROPERTY);

-- 1f. A VIRTUAL generated column cannot carry an index and is refused.
CREATE VLABEL bad_virt (x int GENERATED ALWAYS AS ((properties->>'x')::int) VIRTUAL);

-- 1g. A promoted name may not collide with a reserved element column, on a
--     VERTEX (id / properties) or an EDGE (start / end / id / properties).
--     "end" is a grammar keyword, so it is rejected at parse time rather than by
--     the reserved-name check -- both are clean refusals.
CREATE VLABEL bad_resv_id (id int GENERATED);
CREATE VLABEL bad_resv_props (properties int GENERATED);
CREATE ELABEL bad_estart (start int GENERATED);
CREATE ELABEL bad_eend (end int GENERATED);
CREATE ELABEL bad_eid (id int GENERATED);
CREATE ELABEL bad_eprops (properties int GENERATED);

-- 1h. A full GENERATED expression over MORE THAN ONE bag key (a derived column)
--     is not a single-key promoted property: it is NOT recorded in the catalog
--     and a Cypher access of that column's key is NOT resolved to the column --
--     it stays on the jsonb bag (which has no such key -> NULL).
CREATE VLABEL derived (net numeric GENERATED ALWAYS AS
        (((properties->>'up')::numeric) - ((properties->>'down')::numeric)) STORED);
CREATE (:derived {name:'d1', up:10, down:3});

-- 1i. Adding or dropping a promoted column after the label exists is done with
--     ALTER VLABEL / ELABEL ... ADD [COLUMN] / DROP [COLUMN].  The full lifecycle
--     -- backfill of existing rows, catalog sync, index routing, inheritance
--     recursion, the graph-aware write-domain error, and every rejection
--     (reserved names, the "end" keyword, the non-STORED and retired PROPERTY
--     forms, a non-immutable cast, a bare non-generated column, and the retired
--     ADD PROPERTY spelling)
--     -- is covered in SECTION 15.

-- 1j. Bind indexes.  A promoted key -> a real btree on the typed column; a
--     NON-promoted key -> a jsonb (properties ->> 'key') expression index; a
--     promoted edge key -> a btree on the edge's typed column.
CREATE PROPERTY INDEX ON doc (age);
CREATE PROPERTY INDEX ON doc (big);
CREATE PROPERTY INDEX ON doc (ratio);
CREATE PROPERTY INDEX ON doc (score);
CREATE PROPERTY INDEX ON doc (active);
CREATE PROPERTY INDEX ON doc (title);
CREATE PROPERTY INDEX ON doc (city);       -- city is NOT promoted -> jsonb-expr
CREATE PROPERTY INDEX ON mixed (mixedkey); -- down-cased key -> typed column "v"
CREATE PROPERTY INDEX ON rel (weight);     -- promoted edge column

-- 1k. A promoted key indexes the typed column directly (btree (age)); a
--     non-promoted key indexes the jsonb expression; the down-cased key binds
--     its typed column (btree (v)).
SELECT indexname, indexdef FROM pg_indexes
 WHERE schemaname = 'tc' AND indexname IN ('doc_age_idx','doc_city_idx','mixed_mixedkey_idx')
 ORDER BY indexname;

-- 1l. The label's on-disk shape: fixed id/properties first, promoted columns
--     appended after, each a STORED generated column.
\d tc.doc

-- 1m. The promotion catalog records one row per single-key promoted property.
--     Note "derived" is absent (multi-key) and "mixed" records "mixedkey".
SELECT l.labname, p.propname, p.attnum, p.semantics
  FROM pg_catalog.ag_label_property p
  JOIN pg_catalog.ag_label l ON l.oid = p.laboid
 WHERE l.graphid = (SELECT oid FROM pg_catalog.ag_graph WHERE graphname = 'tc')
 ORDER BY l.labname, p.propname;

-- 1n. \dGi (the ag_property_indexes view) lists property indexes on promoted
--     typed columns (a btree over the column) alongside jsonb-expression
--     property indexes; a PLAIN index is NOT a property index and does not
--     appear.  Queried through the view directly, selecting the owner-
--     independent columns (\dGi also prints an Owner that is the environment's
--     role and would not be reproducible).  (HNSW on a vector -> _vector suite.)
CREATE VLABEL idxdemo (age int GENERATED);
CREATE PROPERTY INDEX ON idxdemo (age);        -- property index (typed) -> listed
CREATE INDEX idxdemo_plain ON tc.idxdemo (id); -- plain index -> NOT listed
SELECT labelname, indexname, "unique", indexdef
  FROM pg_catalog.ag_property_indexes
 WHERE graphname = 'tc' AND labelname IN ('doc','rel','idxdemo')
 ORDER BY labelname, indexname;
DROP VLABEL idxdemo;

-- 1o. Dropping a promoted label removes its ag_label_property rows (the catalog
--     is cleaned up, not leaked).  Use a throwaway label so the fixture stands.
CREATE VLABEL dropme (v1 int GENERATED, v2 text GENERATED);
SELECT count(*) AS rows_before
  FROM pg_catalog.ag_label_property p JOIN pg_catalog.ag_label l ON l.oid = p.laboid
 WHERE l.labname = 'dropme';
DROP VLABEL dropme;
SELECT count(*) AS rows_after
  FROM pg_catalog.ag_label_property p JOIN pg_catalog.ag_label l ON l.oid = p.laboid
 WHERE l.labname = 'dropme';

-- ----------------------------------------------------------------------------
-- Fixture data.  Deliberate duplicates make grouping / DISTINCT meaningful
-- (age 40 twice, big 200 twice, ratio 2.5 twice, score 2.50 thrice, title
-- 'alpha' twice); n6 leaves the promoted keys absent to exercise NULL / missing.
-- Titles are equal-first-letter-distinct lowercase ASCII so any collation
-- orders them a < b < c < d identically -- see the text sections.
-- ----------------------------------------------------------------------------
CREATE (:doc {name:'n1', age:20, big:100, ratio:1.5, score:2.50, active:true,  title:'alpha'}),
       (:doc {name:'n2', age:40, big:200, ratio:2.5, score:2.50, active:false, title:'bravo'}),
       (:doc {name:'n3', age:60, big:300, ratio:3.5, score:3.50, active:true,  title:'alpha'}),
       (:doc {name:'n4', age:30, big:200, ratio:2.5, score:2.50, active:false, title:'charlie'}),
       (:doc {name:'n5', age:40, big:400, ratio:9.5, score:1.00, active:true,  title:'delta'}),
       (:doc {name:'n6', age:50});
CREATE (:plain {name:'p1', age:20}), (:plain {name:'p2', age:40});
CREATE (:othertype {name:'o1', age:'hello'}), (:othertype {name:'o2', age:'world'});

-- deterministic knows topology: outgoing degree n1=2, n2=1, n3=1 (self-loop).
MATCH (a:doc {name:'n1'}), (b:doc {name:'n2'}) CREATE (a)-[:knows]->(b);
MATCH (a:doc {name:'n1'}), (b:doc {name:'n4'}) CREATE (a)-[:knows]->(b);
MATCH (a:doc {name:'n2'}), (b:doc {name:'n3'}) CREATE (a)-[:knows]->(b);
MATCH (a:doc {name:'n3'})                      CREATE (a)-[:knows]->(a);
MATCH (a:doc {name:'n1'}), (b:doc {name:'n2'}) CREATE (a)-[:rel {weight:7}]->(b);

ANALYZE tc.doc;
ANALYZE tc.plain;
ANALYZE tc.rel;
ANALYZE tc.mixed;
ANALYZE tc.derived;

-- ============================================================================
-- SECTION 2 -- Plan shape: promotion binds the typed column and its index
--
-- enable_seqscan is disabled so the planner must reach for the promoted
-- column's index; these prove the property access compiled to the column, not
-- to a jsonb bag expression.  COSTS OFF keeps the output stable.
-- ============================================================================
SET enable_seqscan = off;

-- 2a. Equality on a promoted int binds its btree as an Index Cond.
EXPLAIN (COSTS OFF)
MATCH (n:doc) WHERE n.age = 40 RETURN n.title;

-- 2b. A range on a promoted int binds the same index.
EXPLAIN (COSTS OFF)
MATCH (n:doc) WHERE n.age > 30 RETURN n.name;

-- 2c. A column-only read is index-only over the promoted bigint.
EXPLAIN (COSTS OFF)
MATCH (n:doc) WHERE n.big > 150 RETURN n.big;

-- 2d. ORDER BY a promoted int with LIMIT is served from the index in order --
--     no Sort node (a free ORDER BY re-points to the native column).
EXPLAIN (COSTS OFF)
MATCH (n:doc) RETURN n.name ORDER BY n.age LIMIT 2;

-- 2e. Equality on promoted text binds the text btree.
EXPLAIN (COSTS OFF)
MATCH (n:doc) WHERE n.title = 'alpha' RETURN n.name;

-- 2f. GROUP BY a promoted property groups on the native typed column (the
--     returned value stays as cypher_to_jsonb(column), a function of
--     the group key), so the group reads the column index-only and compares
--     native values instead of jsonb.
EXPLAIN (VERBOSE, COSTS OFF)
MATCH (n:doc) RETURN n.age AS age, count(*) ORDER BY age;

-- 2g. A promoted edge property resolves to its column on a directed pattern.
EXPLAIN (VERBOSE, COSTS OFF)
MATCH (a:doc)-[e:rel]->(b:doc) RETURN e.weight;

-- 2h. A label with no promoted column keeps its Seq-Scan-on-properties plan.
SET enable_seqscan = on;
EXPLAIN (COSTS OFF)
MATCH (n:plain) WHERE n.age > 25 RETURN n.age;
SET enable_seqscan = off;

-- 2i. A structural name is the jsonb property, never the base column:
--     n.id is the property "id", not the relation's graphid id column.
EXPLAIN (VERBOSE, COSTS OFF)
MATCH (n:doc) WHERE n.id = 5 RETURN n.age;

-- 2j. Baseline: with promotion off, a promoted key falls back to the jsonb bag.
SET enable_property_promotion = off;
EXPLAIN (COSTS OFF)
MATCH (n:doc) WHERE n.age = 40 RETURN n.title;
SET enable_property_promotion = on;

-- 2k. A promoted EDGE property binds its btree on a directed pattern.
EXPLAIN (COSTS OFF)
MATCH (a:doc)-[e:rel]->(b:doc) WHERE e.weight > 5 RETURN a.name;

-- 2l. On an UNDIRECTED pattern the edge scan is an Append over the two direction
--     arms, each binding the same typed edge index.
EXPLAIN (COSTS OFF)
MATCH (a:doc)-[e:rel]-(b:doc) WHERE e.weight > 5 RETURN a.name;

-- 2m. A down-cased source key resolves to its typed column and binds that
--     column's index (Index Cond over the column "v", not a jsonb expression).
EXPLAIN (COSTS OFF)
MATCH (n:mixed) WHERE n.mixedkey > 6 RETURN n.name;

-- 2n. A derived (multi-key) column's key is NOT resolved: it stays a jsonb bag
--     expression and does not bind the generated column.
EXPLAIN (VERBOSE, COSTS OFF)
MATCH (n:derived) WHERE n.net > 5 RETURN n.name;

SET enable_seqscan = on;

-- ============================================================================
-- SECTION 3 -- WHERE filters: results identical with promotion on and off
-- ============================================================================

-- 3a. Equality / range over a promoted int.
SET enable_property_promotion = on;
MATCH (n:doc) WHERE n.age > 30 RETURN n.name AS name, n.age AS age ORDER BY age, name;
MATCH (n:doc) WHERE n.age >= 30 AND n.age <= 50 RETURN n.name AS name ORDER BY name;
SET enable_property_promotion = off;
MATCH (n:doc) WHERE n.age > 30 RETURN n.name AS name, n.age AS age ORDER BY age, name;
MATCH (n:doc) WHERE n.age >= 30 AND n.age <= 50 RETURN n.name AS name ORDER BY name;

-- 3b. Boundary equality: integer literal 40 binds natively; numeric literal 40.0
--     is the same number and must still match (native numeric comparison).
SET enable_property_promotion = on;
MATCH (n:doc) WHERE n.age = 40 RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE n.age = 40.0 RETURN n.name AS name ORDER BY name;
SET enable_property_promotion = off;
MATCH (n:doc) WHERE n.age = 40 RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE n.age = 40.0 RETURN n.name AS name ORDER BY name;

-- 3c. Negative and zero boundaries; a promoted float and numeric.
SET enable_property_promotion = on;
MATCH (n:doc) WHERE n.age >= -5 RETURN count(*) AS c;
MATCH (n:doc) WHERE n.ratio > 0 RETURN count(*) AS c;
MATCH (n:doc) WHERE n.score >= 2.50 RETURN n.name AS name ORDER BY name;
SET enable_property_promotion = off;
MATCH (n:doc) WHERE n.age >= -5 RETURN count(*) AS c;
MATCH (n:doc) WHERE n.ratio > 0 RETURN count(*) AS c;
MATCH (n:doc) WHERE n.score >= 2.50 RETURN n.name AS name ORDER BY name;

-- 3d. A promoted bigint against a literal beyond int4 range.
SET enable_property_promotion = on;
MATCH (n:doc) WHERE n.big > 250 RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE n.big < 9999999999 RETURN count(*) AS c;
SET enable_property_promotion = off;
MATCH (n:doc) WHERE n.big > 250 RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE n.big < 9999999999 RETURN count(*) AS c;

-- 3e. A promoted boolean in WHERE, and comparing two promoted columns.
SET enable_property_promotion = on;
MATCH (n:doc) WHERE n.active RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE n.age < n.big RETURN count(*) AS c;
MATCH (n:doc) WHERE n.age = n.big RETURN count(*) AS c;
SET enable_property_promotion = off;
MATCH (n:doc) WHERE n.active RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE n.age < n.big RETURN count(*) AS c;
MATCH (n:doc) WHERE n.age = n.big RETURN count(*) AS c;

-- 3f. Membership (n.prop IN [...]) resolves to a native "col = ANY(array)" on
--     the typed column (index-bound where indexed) when every element shares
--     the column's scalar family.  A null element keeps three-valued logic
--     (a non-match is null, not false), an unlike-type element and an empty
--     list keep the jsonb membership path, and NOT IN negates the same test.
--     on == off in every case.
SET enable_property_promotion = on;
MATCH (n:doc) WHERE n.age IN [20,40,60] RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE n.age IN [20,null,60] RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE n.age IN [20,'x',60] RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE n.age IN [] RETURN count(*) AS c;
MATCH (n:doc) WHERE n.age NOT IN [20,40] RETURN count(*) AS c;
MATCH (n:doc) WHERE n.name IN ['w1','w2'] RETURN n.name AS name ORDER BY name;
SET enable_property_promotion = off;
MATCH (n:doc) WHERE n.age IN [20,40,60] RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE n.age IN [20,null,60] RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE n.age IN [20,'x',60] RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE n.age IN [] RETURN count(*) AS c;
MATCH (n:doc) WHERE n.age NOT IN [20,40] RETURN count(*) AS c;
MATCH (n:doc) WHERE n.name IN ['w1','w2'] RETURN n.name AS name ORDER BY name;

-- ============================================================================
-- SECTION 4 -- Cross-type comparison: Cypher-strict (on == off)
--
-- Cypher compares by type.  A promoted column against an operand of a different
-- scalar family is never equal; the on path reproduces the jsonb type-aware
-- result, so both blocks agree.  A numeric literal that equals the value (40.0)
-- DOES match because it is the same Cypher (numeric) family -- no rounding.
-- ============================================================================
SET enable_property_promotion = on;
MATCH (n:doc) WHERE n.age = '40'   RETURN count(*) AS int_vs_string;
MATCH (n:doc) WHERE n.age = 2.5    RETURN count(*) AS int_vs_frac;   -- no age is 2.5
MATCH (n:doc) WHERE n.active = 1   RETURN count(*) AS bool_vs_int;
MATCH (n:doc) WHERE n.active = 'true' RETURN count(*) AS bool_vs_str;
MATCH (n:doc) WHERE n.title = 5    RETURN count(*) AS text_vs_int;
SET enable_property_promotion = off;
MATCH (n:doc) WHERE n.age = '40'   RETURN count(*) AS int_vs_string;
MATCH (n:doc) WHERE n.age = 2.5    RETURN count(*) AS int_vs_frac;
MATCH (n:doc) WHERE n.active = 1   RETURN count(*) AS bool_vs_int;
MATCH (n:doc) WHERE n.active = 'true' RETURN count(*) AS bool_vs_str;
MATCH (n:doc) WHERE n.title = 5    RETURN count(*) AS text_vs_int;

-- ============================================================================
-- SECTION 5 -- ORDER BY
-- ============================================================================

-- 5a. Ascending / descending / LIMIT over promoted numerics (on == off).
SET enable_property_promotion = on;
MATCH (n:doc) RETURN n.name AS name, n.age AS age ORDER BY age ASC, name LIMIT 3;
MATCH (n:doc) RETURN n.name AS name, n.ratio AS ratio ORDER BY ratio DESC, name;
SET enable_property_promotion = off;
MATCH (n:doc) RETURN n.name AS name, n.age AS age ORDER BY age ASC, name LIMIT 3;
MATCH (n:doc) RETURN n.name AS name, n.ratio AS ratio ORDER BY ratio DESC, name;

-- 5b. Explicit NULLS FIRST / NULLS LAST placement (n6 has NULL age) (on == off).
SET enable_property_promotion = on;
MATCH (n:doc) RETURN n.name AS name, n.age AS age ORDER BY age ASC NULLS FIRST, name;
MATCH (n:doc) RETURN n.name AS name, n.age AS age ORDER BY age DESC NULLS LAST, name;
SET enable_property_promotion = off;
MATCH (n:doc) RETURN n.name AS name, n.age AS age ORDER BY age ASC NULLS FIRST, name;
MATCH (n:doc) RETURN n.name AS name, n.age AS age ORDER BY age DESC NULLS LAST, name;

-- 5c. ORDER BY mixing a promoted key, an aggregate and an expression (on == off).
SET enable_property_promotion = on;
MATCH (n:doc) RETURN n.active AS act, count(*) AS c, max(n.age) AS mx ORDER BY c DESC, act;
SET enable_property_promotion = off;
MATCH (n:doc) RETURN n.active AS act, count(*) AS c, max(n.age) AS mx ORDER BY c DESC, act;

-- 5d. ORDER BY promoted text.  Both the promoted column and the jsonb path
--     order text with the database's default collation, so the two blocks are
--     identical (on == off).  The values here are lowercase ASCII with distinct
--     first letters, so the order is the same under any collation.
SET enable_property_promotion = on;
MATCH (n:doc) WHERE n.title IS NOT NULL RETURN n.name AS name, n.title AS title ORDER BY title, name;
SET enable_property_promotion = off;
MATCH (n:doc) WHERE n.title IS NOT NULL RETURN n.name AS name, n.title AS title ORDER BY title, name;

-- ============================================================================
-- SECTION 6 -- GROUP BY / DISTINCT (the by-value grouping path)
--
-- A promoted property groups and de-duplicates by value, not by the jsonb
-- Datum's pointer bits.  GROUP BY compiles the key to the native typed column
-- (the projected cypher_to_jsonb(column) stays as the returned value, a function
-- of the group key), so grouping compares native values and can read the column
-- index-only; DISTINCT keeps the cypher_to_jsonb(column) key.  Either way
-- the result rows are identical, and all on == off.
-- ============================================================================

-- 6a. GROUP BY each promoted numeric with a duplicate value.
SET enable_property_promotion = on;
MATCH (n:doc) RETURN n.age AS age, count(*) AS c ORDER BY age;
MATCH (n:doc) RETURN n.ratio AS ratio, count(*) AS c ORDER BY ratio;
MATCH (n:doc) RETURN n.big AS big, count(*) AS c ORDER BY big;
MATCH (n:doc) RETURN n.score AS score, count(*) AS c ORDER BY score;
SET enable_property_promotion = off;
MATCH (n:doc) RETURN n.age AS age, count(*) AS c ORDER BY age;
MATCH (n:doc) RETURN n.ratio AS ratio, count(*) AS c ORDER BY ratio;
MATCH (n:doc) RETURN n.big AS big, count(*) AS c ORDER BY big;
MATCH (n:doc) RETURN n.score AS score, count(*) AS c ORDER BY score;

-- 6b. GROUP BY a promoted boolean.
SET enable_property_promotion = on;
MATCH (n:doc) RETURN n.active AS act, count(*) AS c ORDER BY act;
SET enable_property_promotion = off;
MATCH (n:doc) RETURN n.active AS act, count(*) AS c ORDER BY act;

-- 6c. DISTINCT on a promoted by-value numeric.
SET enable_property_promotion = on;
MATCH (n:doc) RETURN DISTINCT n.age AS age ORDER BY age;
MATCH (n:doc) RETURN DISTINCT n.ratio AS ratio ORDER BY ratio;
SET enable_property_promotion = off;
MATCH (n:doc) RETURN DISTINCT n.age AS age ORDER BY age;
MATCH (n:doc) RETURN DISTINCT n.ratio AS ratio ORDER BY ratio;

-- 6d. DISTINCT with a compound / DESC / multi-key ORDER BY on a promoted key.
SET enable_property_promotion = on;
MATCH (n:doc) RETURN DISTINCT n.age AS age, n.active AS act ORDER BY age DESC, act;
MATCH (n:doc) RETURN DISTINCT n.age AS a ORDER BY a DESC;
SET enable_property_promotion = off;
MATCH (n:doc) RETURN DISTINCT n.age AS age, n.active AS act ORDER BY age DESC, act;
MATCH (n:doc) RETURN DISTINCT n.age AS a ORDER BY a DESC;

-- 6e. A property whose KEY is a reserved element-column name (id) can still be
--     promoted -- to a column with a NON-reserved name (the column may not be
--     named id, but the source key may be id).  n.id then binds that typed
--     column (never the graphid), so a filter over n.id is native and a GROUP BY
--     n.id compiles to the native key -- the "top by a promoted id" shape (q15).
CREATE VLABEL idp (uid int GENERATED (id));
CREATE (:idp {id:5, name:'a'}), (:idp {id:9, name:'b'}), (:idp {id:5, name:'c'});
SET enable_property_promotion = on;
-- n.id reads the promoted column (the property 5/5/9), not the graphid
MATCH (n:idp) WHERE n.id = 5 RETURN n.name AS name ORDER BY name;
EXPLAIN (VERBOSE, COSTS OFF)
MATCH (n:idp) WHERE n.id = 5 RETURN n.name AS name;
-- GROUP BY n.id groups on the native column; identical rows on == off
EXPLAIN (VERBOSE, COSTS OFF)
MATCH (n:idp) RETURN n.id AS id, count(*) AS c ORDER BY c DESC, id;
MATCH (n:idp) RETURN n.id AS id, count(*) AS c ORDER BY c DESC, id;
SET enable_property_promotion = off;
MATCH (n:idp) RETURN n.id AS id, count(*) AS c ORDER BY c DESC, id;

-- 6f. A property index is droppable by the DDL that created it, whichever form
--     the key resolved to.  CREATE PROPERTY INDEX builds a typed-column index for
--     a promoted key and a jsonb expression index otherwise, and both are
--     property indexes; only an index over something that is not a property --
--     an ordinary column added to the label directly -- is not.
CREATE PROPERTY INDEX idx_prom ON doc (age);
CREATE PROPERTY INDEX idx_srckey ON idp (id);
CREATE PROPERTY INDEX idx_expr ON doc (city);
SELECT labelname, indexname FROM ag_property_indexes
 WHERE indexname IN ('idx_prom', 'idx_srckey', 'idx_expr') ORDER BY indexname;
DROP PROPERTY INDEX idx_prom;
DROP PROPERTY INDEX idx_srckey;
DROP PROPERTY INDEX idx_expr;
SELECT count(*) AS remaining FROM ag_property_indexes
 WHERE indexname IN ('idx_prom', 'idx_srckey', 'idx_expr');

-- an ordinary column is not a property: neither listed nor droppable as one
ALTER TABLE tc.doc ADD COLUMN notaprop text;
CREATE INDEX idx_plain ON tc.doc (notaprop);
SELECT count(*) AS listed FROM ag_property_indexes WHERE indexname = 'idx_plain';
DROP PROPERTY INDEX idx_plain;
DROP INDEX tc.idx_plain;
ALTER TABLE tc.doc DROP COLUMN notaprop;

-- ============================================================================
-- SECTION 7 -- Aggregates over a promoted property (on == off)
-- ============================================================================
SET enable_property_promotion = on;
MATCH (n:doc) RETURN min(n.age) AS mn, max(n.age) AS mx, sum(n.age) AS s, avg(n.age) AS av, count(DISTINCT n.age) AS dc;
MATCH (n:doc) RETURN min(n.ratio) AS mn, max(n.ratio) AS mx, sum(n.ratio) AS s;
MATCH (n:doc) RETURN collect(n.age) AS coll;
SET enable_property_promotion = off;
MATCH (n:doc) RETURN min(n.age) AS mn, max(n.age) AS mx, sum(n.age) AS s, avg(n.age) AS av, count(DISTINCT n.age) AS dc;
MATCH (n:doc) RETURN min(n.ratio) AS mn, max(n.ratio) AS mx, sum(n.ratio) AS s;
MATCH (n:doc) RETURN collect(n.age) AS coll;

-- 7b. min()/max() over promoted text.  Both the promoted column and the jsonb
--     path use the database's default collation, so min = 'alpha' and
--     max = 'delta' both on and off.
SET enable_property_promotion = on;
MATCH (n:doc) WHERE n.title IS NOT NULL RETURN min(n.title) AS mn, max(n.title) AS mx;
SET enable_property_promotion = off;
MATCH (n:doc) WHERE n.title IS NOT NULL RETURN min(n.title) AS mn, max(n.title) AS mx;

-- ============================================================================
-- SECTION 8 -- Text predicates
-- ============================================================================

-- 8a. Equality and pattern predicates are byte-exact and locale-independent
--     (on == off).
SET enable_property_promotion = on;
MATCH (n:doc) WHERE n.title = 'alpha' RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE n.title STARTS WITH 'a' RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE n.title ENDS WITH 'a' RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE n.title CONTAINS 'r' RETURN n.name AS name ORDER BY name;
SET enable_property_promotion = off;
MATCH (n:doc) WHERE n.title = 'alpha' RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE n.title STARTS WITH 'a' RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE n.title ENDS WITH 'a' RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE n.title CONTAINS 'r' RETURN n.name AS name ORDER BY name;

-- ============================================================================
-- SECTION 9 -- Special / edge-case numeric values
--
-- NaN / Infinity / -Infinity cannot be jsonb numbers, so a Cypher program can
-- only store them as strings.  The float column then parses those strings into
-- real floats, so with promotion on n.ratio is a float (NaN, Inf) while the bag
-- keeps a string.  This changes ORDER BY and comparison, not just display, so
-- promotion on and off intentionally give different results here.
-- ============================================================================
CREATE VLABEL special (ratio float GENERATED, age int GENERATED);
CREATE (:special {name:'sp_nan',  ratio:'NaN'}),
       (:special {name:'sp_inf',  ratio:'Infinity'}),
       (:special {name:'sp_ninf', ratio:'-Infinity'}),
       (:special {name:'sp_nz',   ratio:-0.0}),
       (:special {name:'sp_one',  ratio:1.0});

-- 9a. Ordering: with promotion on this uses native float order
--     (-Inf < 0 < 1 < +Inf < NaN); with it off it uses jsonb order over the
--     stored strings and number.  The two intentionally differ.
SET enable_property_promotion = on;
MATCH (n:special) RETURN n.name AS name, n.ratio AS ratio ORDER BY ratio, name;
SET enable_property_promotion = off;
MATCH (n:special) RETURN n.name AS name, n.ratio AS ratio ORDER BY ratio, name;

-- 9b. Comparison: with promotion on Inf/NaN are floats > 0; with it off a
--     string is compared against a number (no match under Cypher type rules).
--     The two intentionally differ.
SET enable_property_promotion = on;
MATCH (n:special) WHERE n.ratio > 0 RETURN n.name AS name ORDER BY name;
SET enable_property_promotion = off;
MATCH (n:special) WHERE n.ratio > 0 RETURN n.name AS name ORDER BY name;

-- ============================================================================
-- SECTION 10 -- Multi-clause resolution (on == off)
-- ============================================================================

-- 10a. Comma-separated multi-element MATCH: each element resolves its own column.
SET enable_property_promotion = on;
MATCH (a:doc), (b:doc) WHERE a.name='n1' AND b.age > 50 RETURN a.name AS an, b.name AS bn, b.age AS bage ORDER BY bn;
SET enable_property_promotion = off;
MATCH (a:doc), (b:doc) WHERE a.name='n1' AND b.age > 50 RETURN a.name AS an, b.name AS bn, b.age AS bage ORDER BY bn;

-- 10b. Two separate MATCH clauses joined by a promoted-column predicate.
SET enable_property_promotion = on;
MATCH (a:doc) MATCH (b:doc) WHERE a.age < b.age AND a.name='n1' RETURN b.name AS bn ORDER BY bn;
SET enable_property_promotion = off;
MATCH (a:doc) MATCH (b:doc) WHERE a.age < b.age AND a.name='n1' RETURN b.name AS bn ORDER BY bn;

-- 10c. A WITH pipeline carrying a promoted property, plain and renamed.
SET enable_property_promotion = on;
MATCH (n:doc) WITH n WHERE n.age > 25 RETURN n.name AS name, n.age AS age ORDER BY age, name;
MATCH (n:doc) WITH n AS m WHERE m.age > 25 RETURN m.name AS name, m.age AS age ORDER BY age DESC, name;
SET enable_property_promotion = off;
MATCH (n:doc) WITH n WHERE n.age > 25 RETURN n.name AS name, n.age AS age ORDER BY age, name;
MATCH (n:doc) WITH n AS m WHERE m.age > 25 RETURN m.name AS name, m.age AS age ORDER BY age DESC, name;

-- 10d. A deep WITH chain with renames (n -> m -> k).
SET enable_property_promotion = on;
MATCH (n:doc) WITH n WHERE n.age > 20 WITH n AS m WHERE m.age < 60 WITH m AS k
  RETURN k.name AS name, k.age AS age ORDER BY age, name;
SET enable_property_promotion = off;
MATCH (n:doc) WITH n WHERE n.age > 20 WITH n AS m WHERE m.age < 60 WITH m AS k
  RETURN k.name AS name, k.age AS age ORDER BY age, name;

-- 10e. GROUP BY / DISTINCT / ORDER BY a carried-and-renamed promoted property.
SET enable_property_promotion = on;
MATCH (n:doc) WITH n AS m RETURN m.age AS age, count(*) AS c ORDER BY age;
MATCH (n:doc) WITH n AS m RETURN DISTINCT m.age AS age ORDER BY age;
SET enable_property_promotion = off;
MATCH (n:doc) WITH n AS m RETURN m.age AS age, count(*) AS c ORDER BY age;
MATCH (n:doc) WITH n AS m RETURN DISTINCT m.age AS age ORDER BY age;

-- ============================================================================
-- SECTION 11 -- Subqueries / correlation / UNION / paths (on == off)
-- ============================================================================

-- 11a. Correlated EXISTS / NOT EXISTS / COUNT over the bound element.
SET enable_property_promotion = on;
MATCH (n:doc) WHERE EXISTS { MATCH (n)-[:knows]->(:doc) } RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE NOT EXISTS { MATCH (n)-[:knows]->(:doc) } RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE COUNT { MATCH (n)-[:knows]->(:doc) } > 1 RETURN n.name AS name ORDER BY name;
SET enable_property_promotion = off;
MATCH (n:doc) WHERE EXISTS { MATCH (n)-[:knows]->(:doc) } RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE NOT EXISTS { MATCH (n)-[:knows]->(:doc) } RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE COUNT { MATCH (n)-[:knows]->(:doc) } > 1 RETURN n.name AS name ORDER BY name;

-- 11b. A promoted-column filter AND a correlated EXISTS in the same WHERE.
SET enable_property_promotion = on;
MATCH (n:doc) WHERE n.age > 15 AND EXISTS { MATCH (n)-[:knows]->(:doc) } RETURN n.name AS name, n.age AS age ORDER BY age, name;
SET enable_property_promotion = off;
MATCH (n:doc) WHERE n.age > 15 AND EXISTS { MATCH (n)-[:knows]->(:doc) } RETURN n.name AS name, n.age AS age ORDER BY age, name;

-- 11c. UNION carrying a promoted property from each branch.
SET enable_property_promotion = on;
MATCH (n:doc) WHERE n.age < 30 RETURN n.age AS age UNION MATCH (n:doc) WHERE n.age > 50 RETURN n.age AS age;
SET enable_property_promotion = off;
MATCH (n:doc) WHERE n.age < 30 RETURN n.age AS age UNION MATCH (n:doc) WHERE n.age > 50 RETURN n.age AS age;

-- 11d. A pattern-size expression and a list comprehension referencing the element.
SET enable_property_promotion = on;
MATCH (n:doc) RETURN n.name AS name, size((n)-[:knows]->()) AS deg ORDER BY name;
MATCH (n:doc) RETURN n.name AS name, [x IN [10,30,60] WHERE x <= n.age | x] AS lst ORDER BY name;
SET enable_property_promotion = off;
MATCH (n:doc) RETURN n.name AS name, size((n)-[:knows]->()) AS deg ORDER BY name;
MATCH (n:doc) RETURN n.name AS name, [x IN [10,30,60] WHERE x <= n.age | x] AS lst ORDER BY name;

-- 11e. shortestpath / named path referencing bound elements.
SET enable_property_promotion = on;
MATCH (a:doc), (b:doc) WHERE a.name='n1' AND b.name='n3'
  RETURN length(shortestpath((a)-[:knows*..5]->(b))) AS len;
MATCH p=(a:doc)-[:knows]->(b:doc) RETURN a.name AS an, b.name AS bn ORDER BY an, bn;
SET enable_property_promotion = off;
MATCH (a:doc), (b:doc) WHERE a.name='n1' AND b.name='n3'
  RETURN length(shortestpath((a)-[:knows*..5]->(b))) AS len;
MATCH p=(a:doc)-[:knows]->(b:doc) RETURN a.name AS an, b.name AS bn ORDER BY an, bn;

-- 11e2. A promoted value is carried into jsonb with the shape the bag holds.
--       to_jsonb falls back to the text form of a type it cannot represent, which
--       would land as a jsonb string and disagree with the same property read from
--       the bag; a type whose text form is JSON keeps its shape instead.  What must
--       NOT happen is the converse: a genuine string that merely looks like JSON is
--       a type to_jsonb does represent, so it stays a string.  (The type this
--       matters for in practice is vector, which needs the external extension and
--       is covered outside this suite.)
SET enable_property_promotion = on;
MATCH (n:doc) WHERE n.title = 'alpha' RETURN n.title AS t;
SET enable_property_promotion = off;
MATCH (n:doc) WHERE n.title = 'alpha' RETURN n.title AS t;
SET enable_property_promotion = on;
CREATE VLABEL jlike (s text GENERATED);
CREATE (:jlike {s: '[9,9]'}), (:jlike {s: '{"a":1}'}), (:jlike {s: 'plain'});
MATCH (n:jlike) RETURN n.s AS s ORDER BY s;
SET enable_property_promotion = off;
MATCH (n:jlike) RETURN n.s AS s ORDER BY s;

-- 11f. A CALL body reading a promoted property of an IMPORTED element.  The
--      sentinel that carries the typed column is not a Cypher variable and so is
--      never named by an import list, but it belongs to the element it describes
--      and has to travel with it: an element imported into a subquery body keeps
--      the enclosing scope's column list, and a column left out of the import is
--      reported as nonexistent rather than absent, so the read would fail rather
--      than fall back.
SET enable_property_promotion = on;
MATCH (n:doc) CALL { WITH n RETURN n.name AS nm } RETURN nm ORDER BY nm;
MATCH (n:doc) CALL { WITH n RETURN n.age AS a } RETURN a ORDER BY a;
MATCH (n:doc) CALL (*) { RETURN n.name AS nm } RETURN nm ORDER BY nm;
SET enable_property_promotion = off;
MATCH (n:doc) CALL { WITH n RETURN n.name AS nm } RETURN nm ORDER BY nm;
MATCH (n:doc) CALL { WITH n RETURN n.age AS a } RETURN a ORDER BY a;
MATCH (n:doc) CALL (*) { RETURN n.name AS nm } RETURN nm ORDER BY nm;

-- 11g. The import restriction itself is unchanged: a variable that was not
--      imported stays invisible inside the body, and so does its sentinel.
SET enable_property_promotion = on;
MATCH (n:doc), (o:doc) CALL { WITH n RETURN o.name AS nm } RETURN nm;
MATCH (n:doc) CALL () { RETURN n.name AS nm } RETURN nm;

-- ============================================================================
-- SECTION 12 -- NULL / missing-key semantics (on == off)
--
-- n6 leaves the promoted keys absent -> column NULL.  A key explicitly set to
-- jsonb null and a wholly-absent key must both read as SQL NULL.
-- ============================================================================
CREATE VLABEL nullcase (age int GENERATED);
CREATE (:nullcase {name:'nc_null', age:null}),
       (:nullcase {name:'nc_missing'}),
       (:nullcase {name:'nc_has', age:5});
SET enable_property_promotion = on;
MATCH (n:nullcase) WHERE n.age IS NULL RETURN n.name AS name ORDER BY name;
MATCH (n:nullcase) WHERE n.age IS NOT NULL RETURN n.name AS name, n.age AS age ORDER BY name;
MATCH (n:doc) WHERE n.ratio IS NULL RETURN n.name AS name ORDER BY name;
SET enable_property_promotion = off;
MATCH (n:nullcase) WHERE n.age IS NULL RETURN n.name AS name ORDER BY name;
MATCH (n:nullcase) WHERE n.age IS NOT NULL RETURN n.name AS name, n.age AS age ORDER BY name;
MATCH (n:doc) WHERE n.ratio IS NULL RETURN n.name AS name ORDER BY name;

-- ============================================================================
-- SECTION 13 -- Expressions, functions, coexisting keys, inheritance
-- ============================================================================

-- 13a. Arithmetic on a promoted column stays on the jsonb path but is correct.
SET enable_property_promotion = on;
MATCH (n:doc) WHERE n.age + 1 > 41 RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE n.age IS NOT NULL RETURN n.name AS name, n.age * 2 AS twice ORDER BY name;
SET enable_property_promotion = off;
MATCH (n:doc) WHERE n.age + 1 > 41 RETURN n.name AS name ORDER BY name;
MATCH (n:doc) WHERE n.age IS NOT NULL RETURN n.name AS name, n.age * 2 AS twice ORDER BY name;

-- 13b. CASE and coalesce over a promoted column.
SET enable_property_promotion = on;
MATCH (n:doc) RETURN n.name AS name, CASE WHEN n.age >= 40 THEN 'senior' ELSE 'junior' END AS band ORDER BY name;
MATCH (n:doc) RETURN n.name AS name, coalesce(n.ratio, -1.0) AS r ORDER BY name;
SET enable_property_promotion = off;
MATCH (n:doc) RETURN n.name AS name, CASE WHEN n.age >= 40 THEN 'senior' ELSE 'junior' END AS band ORDER BY name;
MATCH (n:doc) RETURN n.name AS name, coalesce(n.ratio, -1.0) AS r ORDER BY name;

-- 13c. The same key name on labels with different promotion: doc.age (int),
--      othertype.age (text), plain.age (non-promoted) each resolve correctly.
SET enable_property_promotion = on;
MATCH (n:doc) WHERE n.age = 20 RETURN n.name AS name;
MATCH (n:othertype) WHERE n.age = 'hello' RETURN n.name AS name;
MATCH (n:plain) WHERE n.age = 20 RETURN n.name AS name;
SET enable_property_promotion = off;
MATCH (n:doc) WHERE n.age = 20 RETURN n.name AS name;
MATCH (n:othertype) WHERE n.age = 'hello' RETURN n.name AS name;
MATCH (n:plain) WHERE n.age = 20 RETURN n.name AS name;

-- 13d. Inheritance -- resolution is inheritance-aware.  A child resolves a key
--      promoted on an ancestor to the child's OWN copy of the typed column (read
--      AND index), and its own promoted key too.  Hierarchy: par(age) <- chld
--      (score) <- gchld(extra), so gchld promotes age (2 levels up), score (1
--      level up) and extra (own).
CREATE VLABEL par (age int GENERATED);
CREATE VLABEL chld (score numeric GENERATED) INHERITS (par);
CREATE VLABEL gchld (extra int GENERATED) INHERITS (chld);
CREATE (:par {name:'par1', age:10});
CREATE (:chld {name:'chld1', age:20, score:5.5});
CREATE (:gchld {name:'gch1', age:30, score:7.7, extra:1});

-- An index on an inherited key builds a real btree on the child's typed column
-- (btree (age)), not a jsonb expression -- and one on the child's own key too.
CREATE PROPERTY INDEX ON chld (age);
CREATE PROPERTY INDEX ON gchld (age);
SELECT indexname, indexdef FROM pg_indexes
 WHERE schemaname = 'tc' AND indexname IN ('chld_age_idx','gchld_age_idx')
 ORDER BY indexname;
-- The inherited-key typed index appears as a property index (via the
-- ag_property_indexes view behind \dGi; owner column omitted for determinism).
SELECT labelname, indexname, indexdef
  FROM pg_catalog.ag_property_indexes
 WHERE graphname = 'tc' AND labelname IN ('chld','gchld')
 ORDER BY labelname, indexname;
ANALYZE tc.chld;
ANALYZE tc.gchld;

-- A filter on the inherited key binds the typed index: a child-typed MATCH
-- scans the child and its own descendant (Append), each an Index Scan on its
-- typed age column -- NOT a Filter over a jsonb expression.
SET enable_property_promotion = on;
SET enable_seqscan = off;
EXPLAIN (COSTS OFF)
MATCH (n:chld) WHERE n.age > 5 RETURN n.name;
-- The grandchild alone binds its own typed age index.
EXPLAIN (COSTS OFF)
MATCH (n:gchld) WHERE n.age > 5 RETURN n.name;
SET enable_seqscan = on;

-- Results: a parent-typed MATCH spans the whole hierarchy; a child-typed MATCH
-- resolves the inherited key and its own key.  All on == off.
SET enable_property_promotion = on;
MATCH (n:par) WHERE n.age >= 10 RETURN n.name AS name, n.age AS age ORDER BY age, name;
MATCH (n:chld) RETURN n.name AS name, n.age AS age, n.score AS score ORDER BY age, name;
MATCH (n:gchld) RETURN n.name AS name, n.age AS age, n.score AS score, n.extra AS extra ORDER BY name;
SET enable_property_promotion = off;
MATCH (n:par) WHERE n.age >= 10 RETURN n.name AS name, n.age AS age ORDER BY age, name;
MATCH (n:chld) RETURN n.name AS name, n.age AS age, n.score AS score ORDER BY age, name;
MATCH (n:gchld) RETURN n.name AS name, n.age AS age, n.score AS score, n.extra AS extra ORDER BY name;
SET enable_property_promotion = on;

-- 13e. A down-cased source key (column "v" <- bag key "mixedkey") is read via
--      the lower-cased property key and resolves to the typed column (on == off).
SET enable_property_promotion = on;
MATCH (n:mixed) WHERE n.mixedkey > 6 RETURN n.name AS name, n.mixedkey AS mk ORDER BY name;
SET enable_property_promotion = off;
MATCH (n:mixed) WHERE n.mixedkey > 6 RETURN n.name AS name, n.mixedkey AS mk ORDER BY name;

-- 13f. A derived (multi-key) column's key is not a promoted property: n.net
--      reads the jsonb bag, which has no "net" key, so it is NULL -- the derived
--      value in the column is not exposed as a property (on == off).
SET enable_property_promotion = on;
MATCH (n:derived) RETURN n.name AS name, n.net AS net ORDER BY name;
MATCH (n:derived) WHERE n.net > 5 RETURN count(*) AS c;
SET enable_property_promotion = off;
MATCH (n:derived) RETURN n.name AS name, n.net AS net ORDER BY name;
MATCH (n:derived) WHERE n.net > 5 RETURN count(*) AS c;
SET enable_property_promotion = on;

-- ============================================================================
-- SECTION 14 -- Write path: the bag stays the source of truth, columns mirror
-- ============================================================================

-- 14a. CREATE materializes the columns from the map; read them back.
CREATE (:doc {name:'w1', age:11, ratio:1.1, active:true, title:'wr'});
MATCH (n:doc {name:'w1'}) RETURN n.age AS age, n.ratio AS ratio;

-- 14b. SET recomputes the columns.
MATCH (n:doc {name:'w1'}) SET n.age = 12, n.ratio = 2.2;
MATCH (n:doc {name:'w1'}) RETURN n.age AS age, n.ratio AS ratio;

-- 14c. SET += merges; SET = {map} replaces (dropped keys -> column NULL).
MATCH (n:doc {name:'w1'}) SET n += {age:13, extra:99};
MATCH (n:doc {name:'w1'}) RETURN n.age AS age;
MATCH (n:doc {name:'w1'}) SET n = {name:'w1', age:14, title:'wr2'};
MATCH (n:doc {name:'w1'}) RETURN n.age AS age, n.ratio AS ratio, n.title AS title;

-- 14d. REMOVE a promoted key -> its column goes NULL, others untouched.
MATCH (n:doc {name:'w1'}) REMOVE n.age;
MATCH (n:doc {name:'w1'}) RETURN n.age AS age, n.title AS title;

-- 14e. MERGE on both branches keeps columns in sync.
MERGE (n:doc {name:'w1'}) ON MATCH SET n.age = 15;
MERGE (n:doc {name:'w2'}) ON CREATE SET n.age = 99, n.ratio = 9.9;
MATCH (n:doc) WHERE n.name IN ['w1','w2'] RETURN n.name AS name, n.age AS age ORDER BY name;

-- 14f. CREATE-then-read of the created element reads the bag (correct value, no
--      index), directly and across a WITH.
CREATE (n:doc {name:'w3', age:77}) RETURN n.age AS age;
CREATE (n:doc {name:'w4', age:88}) WITH n RETURN n.age AS age;

-- 14g. After the writes, promotion on and off still return the same rows.
SET enable_property_promotion = on;
MATCH (n:doc) WHERE n.age >= 14 RETURN n.name AS name, n.age AS age ORDER BY age, name;
SET enable_property_promotion = off;
MATCH (n:doc) WHERE n.age >= 14 RETURN n.name AS name, n.age AS age ORDER BY age, name;
SET enable_property_promotion = on;

-- 14h. A promoted int column constrains its write domain.  It accepts an
--      integer value and an integer-parseable string, but rejects any value the
--      bag would have accepted yet the column cannot parse -- a fractional
--      literal (5.0 or 5.5), an out-of-int-range literal, a non-numeric string,
--      or a composite -- aborting the whole Cypher write with the column's cast
--      error.  Declaring a strict typed column constrains what the key may hold.
CREATE (:doc {name:'wok', age:5});          -- accepted
CREATE (:doc {name:'wstr', age:'42'});      -- integer-parseable string: accepted
MATCH (n:doc {name:'wok'}) RETURN n.age AS age;
MATCH (n:doc {name:'wstr'}) RETURN n.age AS typed, properties(n) ->> 'age' AS bag;
CREATE (:doc {name:'wbad0', age:5.0});      -- rejected (fractional text "5.0")
MATCH (n:doc {name:'w1'}) SET n.age = 5.5;  -- rejected
CREATE (:doc {name:'wbad1', age:9999999999}); -- rejected (out of int range)
CREATE (:doc {name:'wbad2', age:[1,2,3]});  -- rejected (composite)
CREATE (:doc {name:'wbad3', age:'not-a-number'}); -- rejected (non-numeric string)
MATCH (n:doc) WHERE n.name IN ['wok','wstr'] DETACH DELETE n;

-- 14i. DETACH DELETE removes the write-path rows; restore the initial shape.
MATCH (n:doc) WHERE n.name IN ['w1','w2','w3','w4'] DETACH DELETE n;
MATCH (n:doc) RETURN n.name AS name ORDER BY name;

-- 14j. When a value is stored under a key with a type different from the
--      column's -- e.g. the string "42" in an int column -- n.age reads the
--      typed 42 from the column, while the whole-element bag keeps the raw
--      value "42".  The value is duplicated in the bag, so the two forms differ.
CREATE (:doc {name:'mp', age:'42'});
MATCH (n:doc {name:'mp'}) RETURN n.age AS typed_age;
MATCH (n:doc {name:'mp'}) RETURN properties(n) ->> 'age' AS bag_age;
MATCH (n:doc {name:'mp'}) DETACH DELETE n;

-- ============================================================================
-- SECTION 15 -- ALTER label: ADD / DROP a promoted column
--
-- ALTER VLABEL / ELABEL <lbl> ADD [COLUMN] <label_prop> and
-- ALTER VLABEL / ELABEL <lbl> DROP [COLUMN] <name> [RESTRICT|CASCADE] add and
-- remove a promoted typed column after the label already exists.  ADD reuses the
-- exact CREATE-time GENERATED promoted-column grammar and lowers to a generic
-- ALTER TABLE ADD COLUMN, so PostgreSQL re-cooks the generation expression,
-- checks immutability, forces a full table rewrite that BACKFILLS existing
-- rows from the jsonb bag, and recurses to child labels; a hook then syncs the
-- ag_label_property catalog.  DROP lowers to ALTER TABLE DROP COLUMN and clears
-- the catalog row; it refuses only the fixed structural columns (id / properties
-- / start / end), so a derived or plain column drops cleanly (15t), and a
-- combined DROP + re-ADD of the same key in one statement is collision-free
-- (15u).  Reads (n.prop), CREATE PROPERTY INDEX routing and the graph-aware
-- write-domain error are all ALTER-aware, exactly as for a column declared at
-- CREATE time.  Fresh label names are used so the doc / rel / mixed fixtures
-- above are untouched; DROP GRAPH at the end removes everything.
-- ============================================================================

-- 15a. A label that starts with NO promoted column, loaded with rows that
--      already carry the bag key (a3 leaves it absent, to exercise the backfill
--      NULL).
CREATE VLABEL altlbl;
CREATE (:altlbl {name:'a1', age:20}),
       (:altlbl {name:'a2', age:40}),
       (:altlbl {name:'a3'});                  -- no age key -> NULL after backfill

-- 15b. ADD the promoted column.  The on-disk shape gains a STORED generated
--      column derived from the bag, appended after id / properties.
ALTER VLABEL altlbl ADD COLUMN age int GENERATED;
\d tc.altlbl

-- 15c. The promotion catalog now records the ALTER-added column (one row, the
--      column appended after id / properties so attnum = 3).
SELECT p.propname, p.attnum, p.semantics
  FROM pg_catalog.ag_label_property p
  JOIN pg_catalog.ag_label l ON l.oid = p.laboid
 WHERE l.labname = 'altlbl';

-- 15d. Existing rows were backfilled from the bag: rows that carried the key get
--      the typed value, the row without it reads NULL.
MATCH (n:altlbl) RETURN n.name AS name, n.age AS age ORDER BY name;

-- 15e. A read now resolves to the typed column.  Even before any index exists the
--      filter is on the native column (age > 25), NOT a jsonb bag expression.
EXPLAIN (COSTS OFF)
MATCH (n:altlbl) WHERE n.age > 25 RETURN n.age;

-- 15f. CREATE PROPERTY INDEX on the ALTER-added key builds a real btree on the
--      column (not a jsonb expression); it is listed as a property index (via the
--      ag_property_indexes view behind \dGi, owner column omitted); and with a
--      scan forced onto the index it binds as an Index Cond.
CREATE PROPERTY INDEX ON altlbl (age);
SELECT indexname, indexdef FROM pg_indexes
 WHERE schemaname = 'tc' AND indexname = 'altlbl_age_idx';
SELECT labelname, indexname, indexdef
  FROM pg_catalog.ag_property_indexes
 WHERE graphname = 'tc' AND labelname = 'altlbl'
 ORDER BY indexname;
ANALYZE tc.altlbl;
SET enable_seqscan = off;
EXPLAIN (COSTS OFF)
MATCH (n:altlbl) WHERE n.age > 25 RETURN n.name;
SET enable_seqscan = on;

-- 15g. The ALTER-added int column constrains the key's write domain exactly like
--      a CREATE-time promoted column: a fractional value aborts the write with
--      the column's cast error, wrapped in the graph-aware context that names the
--      label (here "altlbl", proving the error path is ALTER-aware too).
MATCH (n:altlbl {name:'a1'}) SET n.age = 5.5 RETURN n.age;

-- 15h. DROP the promoted column.  RESTRICT is the default here; because the
--      column carries only a single-column property index, standard PostgreSQL
--      auto-drops that index together with the column even under RESTRICT (a
--      single-column index is not an independent object that RESTRICT blocks -- so
--      RESTRICT does NOT error).  The column, its catalog row and its index all
--      go, and the read falls back to the jsonb bag (which still holds the key).
ALTER VLABEL altlbl DROP COLUMN age RESTRICT;
\d tc.altlbl
SELECT count(*) AS prop_rows
  FROM pg_catalog.ag_label_property p
  JOIN pg_catalog.ag_label l ON l.oid = p.laboid
 WHERE l.labname = 'altlbl';
SELECT count(*) AS idx_rows FROM pg_indexes
 WHERE schemaname = 'tc' AND indexname = 'altlbl_age_idx';
EXPLAIN (COSTS OFF)
MATCH (n:altlbl) WHERE n.age > 25 RETURN n.name;
MATCH (n:altlbl) RETURN n.name AS name, n.age AS age ORDER BY name;

-- 15i. CASCADE likewise drops a promoted column together with its single-column
--      property index (both RESTRICT above and CASCADE here succeed for a
--      single-column index -- RESTRICT does not block it, CASCADE is not required).
CREATE VLABEL altcasc (v int GENERATED);
CREATE (:altcasc {name:'c1', v:3});
CREATE PROPERTY INDEX ON altcasc (v);
ALTER VLABEL altcasc DROP COLUMN v CASCADE;
SELECT count(*) AS prop_rows
  FROM pg_catalog.ag_label_property p
  JOIN pg_catalog.ag_label l ON l.oid = p.laboid
 WHERE l.labname = 'altcasc';
SELECT count(*) AS idx_rows FROM pg_indexes
 WHERE schemaname = 'tc' AND indexname = 'altcasc_v_idx';

-- 15j. ADD rejections mirror the CREATE-time guards.  A reserved element-column
--      name (id / properties on a vertex; start / id / properties on an edge) is
--      refused with the reserved-name error; "end" is a grammar keyword, rejected
--      at parse time (syntax error) rather than by the reserved-name check; a
--      VIRTUAL (non-STORED) generated column is refused; the retired "col type
--      PROPERTY" form is now a plain syntax error (PROPERTY is no longer a column
--      form); a non-immutable temporal cast is refused; a bare non-generated
--      column is refused with the "not supported yet -- use GENERATED" message;
--      and the retired ADD PROPERTY spelling is no longer grammar.
ALTER VLABEL doc ADD COLUMN id int GENERATED;
ALTER VLABEL doc ADD COLUMN properties int GENERATED;
ALTER VLABEL doc ADD COLUMN end int GENERATED;
ALTER ELABEL rel ADD COLUMN start int GENERATED;
ALTER ELABEL rel ADD COLUMN end int GENERATED;
ALTER VLABEL doc ADD COLUMN x int GENERATED ALWAYS AS ((properties->>'x')::int) VIRTUAL;
ALTER VLABEL doc ADD COLUMN x int PROPERTY;
ALTER VLABEL doc ADD COLUMN ts timestamptz GENERATED;
ALTER VLABEL doc ADD COLUMN foo int;
ALTER VLABEL doc ADD PROPERTY z int GENERATED;

-- 15k. DROP rejections.  A column that does not exist is reported by the generic
--      ALTER TABLE machinery.  A fixed STRUCTURAL column (id / properties on a
--      vertex; start / end likewise on an edge) is refused up front with a
--      graph-specific message and a hint -- these define the element shape and
--      are never user-droppable.  (Any NON-structural column, including a
--      long-form/derived generated column, IS droppable -- see 15t.)
ALTER VLABEL doc DROP COLUMN nosuchcol;
ALTER VLABEL doc DROP COLUMN properties;
ALTER VLABEL doc DROP COLUMN id;
ALTER ELABEL rel DROP COLUMN start;

-- 15l. ADD / DROP recurse to child labels (PostgreSQL inheritance recursion).
--      Build a 2-level hierarchy, load rows, then ADD the promoted column on the
--      PARENT: the column appears on the parent AND every descendant, but the
--      catalog records the promotion on the parent only (a child inherits the
--      column, it does not own a separate promotion).  A read on a child resolves
--      the inherited key to the child's own typed column, and a property index on
--      a child builds a typed btree.  DROP on the parent removes the column (and
--      the children's dependent indexes) from the whole hierarchy and clears the
--      catalog row.
CREATE VLABEL altpar;
CREATE VLABEL altchild INHERITS (altpar);
CREATE VLABEL altgc INHERITS (altchild);
CREATE (:altpar {name:'ap', age:10});
CREATE (:altchild {name:'ac', age:20});
CREATE (:altgc {name:'agc', age:30});
ALTER VLABEL altpar ADD COLUMN age int GENERATED;

-- The generated column is present on the parent and both descendants.
SELECT table_name,
       string_agg(column_name, ',' ORDER BY column_name) AS generated_cols
  FROM information_schema.columns
 WHERE table_schema = 'tc'
   AND table_name IN ('altpar','altchild','altgc')
   AND is_generated = 'ALWAYS'
 GROUP BY table_name ORDER BY table_name;

-- The catalog records the promotion on the parent only.
SELECT l.labname, p.propname
  FROM pg_catalog.ag_label_property p
  JOIN pg_catalog.ag_label l ON l.oid = p.laboid
 WHERE l.labname IN ('altpar','altchild','altgc')
 ORDER BY l.labname;

-- Existing rows across the whole hierarchy were backfilled.
MATCH (n:altpar) RETURN n.name AS name, n.age AS age ORDER BY age, name;

-- A property index on the inherited key on each child builds a typed btree, and a
-- child-typed MATCH binds it (an Append of Index Scans over the sub-hierarchy).
CREATE PROPERTY INDEX ON altchild (age);
CREATE PROPERTY INDEX ON altgc (age);
SELECT indexname, indexdef FROM pg_indexes
 WHERE schemaname = 'tc' AND indexname IN ('altchild_age_idx','altgc_age_idx')
 ORDER BY indexname;
ANALYZE tc.altpar;
ANALYZE tc.altchild;
ANALYZE tc.altgc;
SET enable_seqscan = off;
EXPLAIN (COSTS OFF)
MATCH (n:altchild) WHERE n.age > 5 RETURN n.name;
SET enable_seqscan = on;
MATCH (n:altchild) WHERE n.age > 5 RETURN n.name AS name, n.age AS age ORDER BY age, name;

-- DROP on the parent removes the column from the whole hierarchy and clears the
-- catalog; the children's dependent single-column indexes go with it.
ALTER VLABEL altpar DROP COLUMN age;
SELECT table_name,
       string_agg(column_name, ',' ORDER BY column_name) AS generated_cols
  FROM information_schema.columns
 WHERE table_schema = 'tc'
   AND table_name IN ('altpar','altchild','altgc')
   AND is_generated = 'ALWAYS'
 GROUP BY table_name ORDER BY table_name;
SELECT count(*) AS prop_rows
  FROM pg_catalog.ag_label_property p
  JOIN pg_catalog.ag_label l ON l.oid = p.laboid
 WHERE l.labname IN ('altpar','altchild','altgc');
SELECT count(*) AS idx_rows FROM pg_indexes
 WHERE schemaname = 'tc' AND indexname IN ('altchild_age_idx','altgc_age_idx');

-- 15m. ELABEL parity: ADD / DROP a promoted edge property records / clears the
--      catalog exactly as for a vertex label, and a read resolves the edge column.
--      (The edge reserved-name and "end"-keyword guards on ADD were exercised in
--      15j via ALTER ELABEL rel.)  The COLUMN keyword is optional, exactly as in
--      ALTER TABLE, so the bare ADD / DROP spelling is exercised here.
CREATE ELABEL altedge;
ALTER ELABEL altedge ADD weight int GENERATED;
SELECT p.propname, p.attnum, p.semantics
  FROM pg_catalog.ag_label_property p
  JOIN pg_catalog.ag_label l ON l.oid = p.laboid
 WHERE l.labname = 'altedge';
MATCH (a:doc {name:'n1'}), (b:doc {name:'n2'}) CREATE (a)-[:altedge {weight:7}]->(b);
MATCH (a:doc)-[e:altedge]->(b:doc) RETURN e.weight AS weight;
ALTER ELABEL altedge DROP weight;
SELECT count(*) AS prop_rows
  FROM pg_catalog.ag_label_property p
  JOIN pg_catalog.ag_label l ON l.oid = p.laboid
 WHERE l.labname = 'altedge';

-- 15n. ADD accepts the long-form and explicit-source-key GENERATED spellings
--      exactly as CREATE does (SECTION 1b).  A single-key long-form
--      GENERATED ALWAYS AS ((properties->>'key')::type) STORED is still a
--      resolvable promoted property -- its source key is extracted from the
--      expression -- and GENERATED (srckey) promotes a differently-named column
--      from an explicit (down-cased) bag key.  Both are recorded in
--      ag_label_property and both resolve, with pre-existing rows backfilled.
CREATE VLABEL altforms;
CREATE (:altforms {name:'f1', score:2.50, yrs:5}),
       (:altforms {name:'f2', score:9.75, yrs:9});
ALTER VLABEL altforms ADD COLUMN score numeric
       GENERATED ALWAYS AS ((properties->>'score')::numeric) STORED;
ALTER VLABEL altforms ADD COLUMN age int GENERATED (yrs); -- column "age" <- key "yrs"
SELECT p.propname, p.attnum, p.semantics
  FROM pg_catalog.ag_label_property p
  JOIN pg_catalog.ag_label l ON l.oid = p.laboid
 WHERE l.labname = 'altforms' ORDER BY p.propname;
MATCH (n:altforms) WHERE n.score > 5 RETURN n.name AS name, n.score AS score ORDER BY name;
MATCH (n:altforms) WHERE n.yrs >= 9 RETURN n.name AS name, n.yrs AS yrs ORDER BY name;

-- 15o. ADD is refused when the column name already exists on the label: the
--      generic ALTER TABLE duplicate-column guard fires and names the label
--      ("score" was just added above).
ALTER VLABEL altforms ADD COLUMN score numeric GENERATED;

-- 15p. IF EXISTS turns an ALTER on a missing label into a NOTICE-and-skip no-op,
--      for both VLABEL and ELABEL; without IF EXISTS a missing label is an error.
ALTER VLABEL IF EXISTS nosuchlabel ADD COLUMN x int GENERATED;
ALTER ELABEL IF EXISTS nosuchedge DROP COLUMN x;
ALTER VLABEL nosuchlabel ADD COLUMN x int GENERATED;

-- 15q. DROP then re-ADD the same property.  PostgreSQL never reuses an attnum, so
--      the DROP leaves a dropped-column gap in the physical layout and the re-ADD
--      appends the column at a HIGHER attnum; the catalog is rebuilt to point at
--      the new attnum.  The read resolves to the new column and pre-existing rows
--      are re-backfilled from the bag.  This is the state that stresses attnum
--      reconstruction (the explicit-key form records a key by attnum lookup).
CREATE VLABEL altreadd;
CREATE (:altreadd {name:'r1', age:20}),
       (:altreadd {name:'r2', age:40}),
       (:altreadd {name:'r3'});                 -- no age -> NULL after backfill
ALTER VLABEL altreadd ADD COLUMN age int GENERATED;   -- appended at attnum 3
SELECT p.propname, p.attnum FROM pg_catalog.ag_label_property p
  JOIN pg_catalog.ag_label l ON l.oid = p.laboid WHERE l.labname = 'altreadd';
ALTER VLABEL altreadd DROP COLUMN age;
ALTER VLABEL altreadd ADD COLUMN age int GENERATED;   -- re-added at attnum 4 (gap at 3)
-- the catalog now records the re-added column's higher attnum, not the old one
SELECT p.propname, p.attnum FROM pg_catalog.ag_label_property p
  JOIN pg_catalog.ag_label l ON l.oid = p.laboid WHERE l.labname = 'altreadd';
-- the physical layout carries the dropped-column gap at attnum 3 (\d hides it, so
-- read pg_attribute directly): id / properties / <dropped> / age.
SELECT attnum, attname, attisdropped FROM pg_catalog.pg_attribute
 WHERE attrelid = 'tc.altreadd'::regclass AND attnum > 0 ORDER BY attnum;
-- the read resolves to the re-added column and rows are backfilled again
MATCH (n:altreadd) RETURN n.name AS name, n.age AS age ORDER BY name;

-- 15r. RESTRICT vs CASCADE.  A single-column property index does NOT make
--      DROP ... RESTRICT fail (15h / 15i): PostgreSQL drops that index together
--      with the column under both behaviors.  A genuine dependency does block
--      RESTRICT: a view over the promoted column makes DROP ... RESTRICT error and
--      requires CASCADE, which drops the view along with the column.
CREATE VLABEL altdep (v int GENERATED);
CREATE (:altdep {name:'d1', v:3});
CREATE VIEW tc.altdep_v AS SELECT v FROM tc.altdep;
ALTER VLABEL altdep DROP COLUMN v RESTRICT;    -- blocked by the dependent view
ALTER VLABEL altdep DROP COLUMN v CASCADE;     -- succeeds, dropping the view too
SELECT count(*) AS prop_rows FROM pg_catalog.ag_label_property p
  JOIN pg_catalog.ag_label l ON l.oid = p.laboid WHERE l.labname = 'altdep';

-- 15s. ELABEL parity for the on-disk shape.  After ADD then DROP of a promoted
--      edge property, the four fixed edge columns (id / start / end / properties)
--      are intact and only the promoted column leaves a dropped-column gap.
CREATE ELABEL altedge2;
ALTER ELABEL altedge2 ADD weight int GENERATED;
ALTER ELABEL altedge2 DROP weight;
SELECT attnum, attname, attisdropped FROM pg_catalog.pg_attribute
 WHERE attrelid = 'tc.altedge2'::regclass AND attnum > 0 ORDER BY attnum;

-- 15t. A long-form DERIVED generated column -- one computed from more than one
--      bag key, so it maps to no single property and is never recorded in
--      ag_label_property -- is droppable.  The DROP refusal is keyed on a column
--      being a fixed structural column, not on "has no catalog row", so a derived
--      column (like a plain column) drops cleanly; only the label's RECORDED
--      promoted columns, their catalog rows and their data are left untouched.
CREATE VLABEL altderiv (age int GENERATED,
        net numeric GENERATED ALWAYS AS
        (((properties->>'up')::numeric) - ((properties->>'down')::numeric)) STORED);
CREATE (:altderiv {name:'q1', age:20, up:10, down:3}),
       (:altderiv {name:'q2', age:40, up:9,  down:9});
-- the catalog records only the single-key promoted column (age); "net" is derived
SELECT p.propname, p.attnum FROM pg_catalog.ag_label_property p
  JOIN pg_catalog.ag_label l ON l.oid = p.laboid WHERE l.labname = 'altderiv'
 ORDER BY p.propname;
-- dropping the derived column now SUCCEEDS (it was previously refused)
ALTER VLABEL altderiv DROP COLUMN net;
-- "net" is gone; the promoted "age" column, its catalog row and its data survive
SELECT string_agg(column_name, ',' ORDER BY column_name) AS generated_cols
  FROM information_schema.columns
 WHERE table_schema = 'tc' AND table_name = 'altderiv' AND is_generated = 'ALWAYS';
SELECT p.propname, p.attnum FROM pg_catalog.ag_label_property p
  JOIN pg_catalog.ag_label l ON l.oid = p.laboid WHERE l.labname = 'altderiv'
 ORDER BY p.propname;
MATCH (n:altderiv) WHERE n.age >= 20 RETURN n.name AS name, n.age AS age ORDER BY name;

-- 15u. DROP and re-ADD the SAME promoted key in ONE statement.  The catalog sync
--      forgets dropped keys before recording added ones, so a combined
--      DROP COLUMN c, ADD COLUMN c ... that maps back to the same source key does
--      not collide on ag_label_property's unique (laboid, propname).  The catalog
--      ends with exactly one row for the key, at the re-added column's new
--      (higher) attnum, and the read resolves to the new column.
CREATE VLABEL altcombo;
CREATE (:altcombo {name:'x1', age:5}), (:altcombo {name:'x2', age:7});
ALTER VLABEL altcombo ADD COLUMN age int GENERATED;                    -- attnum 3
ALTER VLABEL altcombo DROP COLUMN age, ADD COLUMN age int GENERATED;   -- one statement
SELECT count(*) AS age_rows, max(p.attnum) AS attnum
  FROM pg_catalog.ag_label_property p
  JOIN pg_catalog.ag_label l ON l.oid = p.laboid
 WHERE l.labname = 'altcombo' AND p.propname = 'age';
MATCH (n:altcombo) RETURN n.name AS name, n.age AS age ORDER BY name;

-- 15v. ADD is refused when its promotion SOURCE KEY is already promoted on the
--      label, or is promoted twice in the same statement.  ag_label_property is
--      unique on (laboid, propname), so the collision is caught up front -- before
--      any table rewrite -- with an actionable message, not as an opaque
--      unique_violation after the rewrite has run.  A key freed by a DROP in the
--      same statement (15u) is exempt; here nothing is dropped, so both adds fail.
CREATE VLABEL altdup (age int GENERATED);
CREATE (:altdup {name:'d1', age:1, weight:2});
-- a different column NAME but the SAME already-promoted source key -> refused
ALTER VLABEL altdup ADD COLUMN age2 int GENERATED (age);
-- the same source key promoted twice within one statement -> refused
ALTER VLABEL altdup ADD COLUMN a int GENERATED (weight), ADD COLUMN b int GENERATED (weight);
-- neither rewrite ran: the catalog still records only the original "age"
SELECT p.propname, p.attnum FROM pg_catalog.ag_label_property p
  JOIN pg_catalog.ag_label l ON l.oid = p.laboid WHERE l.labname = 'altdup'
 ORDER BY p.propname;
SELECT string_agg(column_name, ',' ORDER BY column_name) AS generated_cols
  FROM information_schema.columns
 WHERE table_schema = 'tc' AND table_name = 'altdup' AND is_generated = 'ALWAYS';

-- ============================================================================
-- SECTION 16 -- Structural names never resolve to a promoted column
--
-- n.id / e.start / e.end are jsonb properties (absent here -> NULL); id(n) is
-- the element's graphid.  None must resolve to a base / promoted column.
-- ============================================================================
MATCH (n:doc) WHERE n.name = 'n1' RETURN n.id AS prop_id, id(n) IS NOT NULL AS has_gid;
MATCH (a:doc)-[e:rel]->(b:doc)
  RETURN e.start AS prop_start, e.end AS prop_end, e.weight AS w, id(e) IS NOT NULL AS has_eid;

-- ============================================================================
-- SECTION 17 -- GUC behavior
-- ============================================================================

-- 17a. Default is on.
SHOW enable_property_promotion;

-- 17b. Toggled mid-transaction (PGC_USERSET) -- both give the correct rows.
BEGIN;
SET enable_property_promotion = on;
MATCH (n:doc) WHERE n.age = 40 RETURN count(*) AS c;
SET enable_property_promotion = off;
MATCH (n:doc) WHERE n.age = 40 RETURN count(*) AS c;
COMMIT;
RESET enable_property_promotion;

-- ============================================================================
-- SECTION 18 -- Known limitations (labeled; on == off, nothing crashes)
-- ============================================================================

-- 18a. An undirected self-loop returns its edge twice (pre-existing behavior,
--      unrelated to promotion; identical on and off).
SET enable_property_promotion = on;
MATCH (a:doc)-[e:knows]-(b:doc) WHERE a.name='n3' RETURN a.name AS an, b.name AS bn ORDER BY an, bn;
SET enable_property_promotion = off;
MATCH (a:doc)-[e:knows]-(b:doc) WHERE a.name='n3' RETURN a.name AS an, b.name AS bn ORDER BY an, bn;

-- 18b. A variable-length relationship's endpoint property stays on the jsonb bag
--      (not index-bound) but returns correct rows.
SET enable_property_promotion = on;
MATCH (a:doc)-[:knows*1..2]->(b:doc) WHERE b.age > 30 RETURN a.name AS an, b.name AS bn ORDER BY an, bn;
SET enable_property_promotion = off;
MATCH (a:doc)-[:knows*1..2]->(b:doc) WHERE b.age > 30 RETURN a.name AS an, b.name AS bn ORDER BY an, bn;

-- ============================================================================
-- SECTION 19 -- Columns a Cypher write does not assign
-- ============================================================================
--
-- A label is a table, so a column can be added to one directly with ALTER
-- TABLE.  Such a column is neither a dropped attribute nor a STORED generated
-- one, and the write path assigns only the label's structural columns (id and
-- the property bag for a vertex; id/start/end and the bag for an edge).  Every
-- attribute past that prefix is therefore unassigned and must be marked null
-- before the tuple is formed, or forming it reads an unassigned Datum.  These
-- cases cover each write operation, for a vertex and an edge, and mix the three
-- kinds -- ordinary, dropped and promoted -- on one label.

SET enable_property_promotion = on;

-- 19a. CREATE, SET, MERGE and REMOVE on a vertex label carrying an ordinary
--      column.  The column reads as null rather than as uninitialized memory.
CREATE VLABEL pcv;
ALTER TABLE tc.pcv ADD COLUMN extra text;
CREATE (:pcv {k: 1, name: 'a'});
MATCH (n:pcv) RETURN n.k, n.name;
SELECT count(*) AS unassigned_is_null FROM tc.pcv WHERE extra IS NULL;
MATCH (n:pcv) SET n.k = 2;
MATCH (n:pcv) RETURN n.k;
SELECT count(*) AS still_null_after_set FROM tc.pcv WHERE extra IS NULL;
MERGE (n:pcv {k: 2}) ON MATCH SET n.name = 'b';
MATCH (n:pcv) RETURN n.k, n.name;
MERGE (n:pcv {k: 99}) ON CREATE SET n.name = 'c';
MATCH (n:pcv) WHERE n.k = 99 RETURN n.k, n.name;
MATCH (n:pcv) WHERE n.k = 99 REMOVE n.name;
MATCH (n:pcv) WHERE n.k = 99 RETURN n.name;

-- 19b. The same on an edge label, whose structural prefix is four columns.
CREATE VLABEL pcev;
CREATE ELABEL pce;
ALTER TABLE tc.pce ADD COLUMN extra text;
CREATE (:pcev {a: 1})-[:pce {w: 10}]->(:pcev {a: 2});
MATCH ()-[r:pce]->() RETURN r.w;
SELECT count(*) AS edge_unassigned_is_null FROM tc.pce WHERE extra IS NULL;
MATCH ()-[r:pce]->() SET r.w = 20;
MATCH ()-[r:pce]->() RETURN r.w;

-- 19c. An ordinary column beside a dropped-column gap and a promoted column.
--      All three are unassigned for the same reason, so one rule covers them,
--      and the promoted column is still derived from the bag and recomputed on
--      update.
CREATE VLABEL pcmix (age int GENERATED);
ALTER TABLE tc.pcmix ADD COLUMN gap text;
ALTER TABLE tc.pcmix DROP COLUMN gap;
ALTER TABLE tc.pcmix ADD COLUMN extra text;
CREATE (:pcmix {age: 30, tag: 'x'});
MATCH (n:pcmix) RETURN n.age, n.tag;
SELECT age FROM tc.pcmix;
SELECT count(*) AS mix_unassigned_is_null FROM tc.pcmix WHERE extra IS NULL;
MATCH (n:pcmix) SET n.age = 31;
SELECT age FROM tc.pcmix;
MATCH (n:pcmix) WHERE n.age = 31 RETURN n.age;
-- the write-domain contract still holds for the promoted column
MATCH (n:pcmix) SET n.age = 'not-a-number';

-- 19d. A Cypher write resets an ordinary column to null.  Cypher cannot name a
--      column that is not a promoted property, so a write has no value to carry
--      forward; the previous value would have to come from the old tuple, which
--      this path does not fetch.  A deliberate contract, recorded here.
SET enable_graph_dml = on;
UPDATE tc.pcv SET extra = 'set-by-sql' WHERE properties ->> 'k' = '2';
SET enable_graph_dml = off;
SELECT extra FROM tc.pcv WHERE properties ->> 'k' = '2';
MATCH (n:pcv) WHERE n.k = 2 SET n.name = 'd';
SELECT extra FROM tc.pcv WHERE properties ->> 'k' = '2';

-- ----------------------------------------------------------------------------
-- CLEANUP
-- ----------------------------------------------------------------------------
RESET enable_property_promotion;
RESET enable_seqscan;
DROP GRAPH tc CASCADE;
RESET graph_path;
