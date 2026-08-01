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

-- Several sections below reshape a label with plain ALTER TABLE, to put it into
-- a shape a label is not supposed to be in and check that the rest of the system
-- copes -- an ordinary column, a dropped-column gap, a de-generated promoted
-- column.  That is refused by default; section 22 covers the refusal itself.
SET enable_graph_ddl = on;

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

-- 2f. GROUP BY a promoted property groups on the native typed column, which is
--     also what the query returns, so the group reads the column index-only,
--     compares native values instead of jsonb, and the grouping already
--     delivers the order the sort would have produced.
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

-- 2o. An ORDER BY key that names a RETURN item costs no more than spelling the
--     property out: the key becomes the item's own expression, so a promoted
--     property still arrives as its typed column and still binds its index.
--     Each pair below is the aliased spelling followed by the qualified one and
--     the two plans must match.  The bare-name key is served from the index in
--     order (no Sort); the expression key sorts, as the qualified spelling does.
EXPLAIN (COSTS OFF)
MATCH (n:doc) RETURN n.name AS name, n.age AS age ORDER BY age LIMIT 2;
EXPLAIN (COSTS OFF)
MATCH (n:doc) RETURN n.name AS name, n.age AS age ORDER BY n.age LIMIT 2;
EXPLAIN (COSTS OFF)
MATCH (n:doc) RETURN n.name AS name, n.age AS age ORDER BY age * -1 LIMIT 2;
EXPLAIN (COSTS OFF)
MATCH (n:doc) RETURN n.name AS name, n.age AS age ORDER BY n.age * -1 LIMIT 2;

-- 2p. The same through a whole-vertex item: a field on the item's name reads
--     the promoted column, so the key binds the column's index here too.
EXPLAIN (COSTS OFF)
MATCH (n:doc) WITH n AS v ORDER BY v.age LIMIT 2 RETURN v.name;
EXPLAIN (VERBOSE, COSTS OFF)
MATCH (n:doc) RETURN n.title AS title ORDER BY toUpper(title) LIMIT 2;

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

-- 5e. A key may name a RETURN item inside an expression, not only as the whole
--     key; the name resolves to the item's own value, which for a promoted
--     property is the typed column (on == off).
SET enable_property_promotion = on;
MATCH (n:doc) RETURN n.name AS name, n.age AS age ORDER BY age * -1, name;
MATCH (n:doc) WHERE n.title IS NOT NULL RETURN n.title AS title ORDER BY toUpper(title) DESC;
SET enable_property_promotion = off;
MATCH (n:doc) RETURN n.name AS name, n.age AS age ORDER BY age * -1, name;
MATCH (n:doc) WHERE n.title IS NOT NULL RETURN n.title AS title ORDER BY toUpper(title) DESC;

-- 5f. Every promoted type reached through an item name inside a larger key.
--     The item's value is the typed column, so the key computes over that type
--     rather than over the jsonb bag -- and the rows come out in the same order
--     either way (on == off).  n6 has none of these keys, so it sorts as NULL.
SET enable_property_promotion = on;
MATCH (n:doc) RETURN n.name AS name, n.big AS big ORDER BY big * -1, name;
MATCH (n:doc) RETURN n.name AS name, n.ratio AS ratio ORDER BY ratio * 2 DESC, name;
MATCH (n:doc) RETURN n.name AS name, n.score AS score ORDER BY score + 0, name;
MATCH (n:doc) RETURN n.name AS name, n.active AS act
    ORDER BY CASE WHEN act THEN 0 ELSE 1 END, name;
MATCH (n:doc) RETURN n.name AS name, n.title AS title
    ORDER BY toUpper(title) NULLS FIRST, name;
SET enable_property_promotion = off;
MATCH (n:doc) RETURN n.name AS name, n.big AS big ORDER BY big * -1, name;
MATCH (n:doc) RETURN n.name AS name, n.ratio AS ratio ORDER BY ratio * 2 DESC, name;
MATCH (n:doc) RETURN n.name AS name, n.score AS score ORDER BY score + 0, name;
MATCH (n:doc) RETURN n.name AS name, n.active AS act
    ORDER BY CASE WHEN act THEN 0 ELSE 1 END, name;
MATCH (n:doc) RETURN n.name AS name, n.title AS title
    ORDER BY toUpper(title) NULLS FIRST, name;

-- 5g. An item that is the whole vertex: a field on the item's name reads the
--     promoted column, so ordering through it matches ordering by the property
--     spelled out (on == off).
SET enable_property_promotion = on;
MATCH (n:doc) WITH n AS v ORDER BY v.age, v.name RETURN v.name AS name, v.age AS age;
MATCH (n:doc) WITH n AS v ORDER BY v.age * -1, v.name RETURN v.name AS name, v.age AS age;
MATCH (n:doc) WITH n AS v ORDER BY v.title NULLS LAST, v.name RETURN v.name AS name;
SET enable_property_promotion = off;
MATCH (n:doc) WITH n AS v ORDER BY v.age, v.name RETURN v.name AS name, v.age AS age;
MATCH (n:doc) WITH n AS v ORDER BY v.age * -1, v.name RETURN v.name AS name, v.age AS age;
MATCH (n:doc) WITH n AS v ORDER BY v.title NULLS LAST, v.name RETURN v.name AS name;

-- 5h. An aggregate over a promoted property, named in a key inside a larger
--     expression: the grouping stays on the typed column and the key aggregates
--     the same value the projection returns (on == off).
SET enable_property_promotion = on;
MATCH (n:doc) RETURN n.active AS act, max(n.age) AS mx ORDER BY mx * -1, act;
MATCH (n:doc) RETURN n.active AS act, count(n.title) AS c ORDER BY c * -1, act;
MATCH (n:doc) RETURN n.title AS title, count(*) AS c
    ORDER BY c DESC, toUpper(title) NULLS FIRST;
SET enable_property_promotion = off;
MATCH (n:doc) RETURN n.active AS act, max(n.age) AS mx ORDER BY mx * -1, act;
MATCH (n:doc) RETURN n.active AS act, count(n.title) AS c ORDER BY c * -1, act;
MATCH (n:doc) RETURN n.title AS title, count(*) AS c
    ORDER BY c DESC, toUpper(title) NULLS FIRST;

-- 5i. A promoted property carried through a transparent WITH keeps resolving to
--     its typed column when a later key names the carried item (on == off).
SET enable_property_promotion = on;
MATCH (n:doc) WITH n, n.name AS nm RETURN nm, n.age AS age ORDER BY age * -1, nm;
MATCH (n:doc) WITH n AS v, n.age AS age ORDER BY age * -1, v.name
    RETURN v.name AS name, age;
SET enable_property_promotion = off;
MATCH (n:doc) WITH n, n.name AS nm RETURN nm, n.age AS age ORDER BY age * -1, nm;
MATCH (n:doc) WITH n AS v, n.age AS age ORDER BY age * -1, v.name
    RETURN v.name AS name, age;

-- ============================================================================
-- SECTION 6 -- GROUP BY / DISTINCT (the by-value grouping path)
--
-- A promoted property groups and de-duplicates by value, not by the jsonb
-- Datum's pointer bits.  The key and the returned value are both the native
-- typed column, so grouping compares native values and can read the column
-- index-only.  The result rows are the same values either way; with promotion
-- on they are rendered in the column's own type rather than as jsonb.
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

-- 6g. The recognition matches a key element on ATTNUM against the label's promoted
--     properties, which is what makes it hold where a key and its column do not
--     line up in the obvious way.  A key INHERITED from an ancestor is the case
--     that matters: the child carries its own copy of the column, and at its own
--     attnum, because a column of its own was declared first.
CREATE VLABEL pipar (age int GENERATED);
CREATE VLABEL pikid (own int GENERATED) INHERITS (pipar);
-- the layout: "age" is attnum 3 on the parent and attnum 3 on the child only by
-- coincidence of declaration order -- "own" is what the child declared first
SELECT c.relname, a.attname, a.attnum
FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
WHERE c.relnamespace = 'tc'::regnamespace
  AND c.relname IN ('pipar', 'pikid')
  AND a.attnum >= 3 AND NOT a.attisdropped
ORDER BY c.relname, a.attnum;
CREATE PROPERTY INDEX idx_inh ON pikid (age);
SELECT labelname, indexname FROM ag_property_indexes WHERE indexname = 'idx_inh';
DROP PROPERTY INDEX idx_inh;
-- several keys in one index: two promoted columns, and a promoted key beside one
-- that resolved to a jsonb expression.  Every element addresses a property, so
-- both are property indexes.
CREATE PROPERTY INDEX idx_two ON pikid (age, own);
SELECT labelname, indexname FROM ag_property_indexes WHERE indexname = 'idx_two';
DROP PROPERTY INDEX idx_two;
CREATE PROPERTY INDEX idx_mixkeys ON pikid (age, city);
SELECT labelname, indexname FROM ag_property_indexes WHERE indexname = 'idx_mixkeys';
DROP PROPERTY INDEX idx_mixkeys;
-- one key addressing a promoted column and one an ORDINARY one: not every element
-- is a property, so the index as a whole is not a property index and the property
-- DDL must not claim it
ALTER TABLE tc.pikid ADD COLUMN notaprop text;
CREATE INDEX idx_halfprop ON tc.pikid (age, notaprop);
SELECT count(*) AS listed FROM ag_property_indexes WHERE indexname = 'idx_halfprop';
DROP PROPERTY INDEX idx_halfprop;
DROP INDEX tc.idx_halfprop;
ALTER TABLE tc.pikid DROP COLUMN notaprop;
DROP VLABEL pikid;
DROP VLABEL pipar;

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
-- only write them as strings -- and a string is not a number.  A promoted
-- column takes the property's value, not a re-reading of its text, so these are
-- refused rather than turned into floats the property never held.  Refusing
-- them is also what keeps promotion off the answer: reinterpreting the string
-- would make ORDER BY and comparison differ depending on whether the column is
-- consulted.
-- ============================================================================
CREATE VLABEL special (ratio float GENERATED, age int GENERATED);

CREATE (:special {name:'sp_nan',  ratio:'NaN'});
CREATE (:special {name:'sp_inf',  ratio:'Infinity'});
CREATE (:special {name:'sp_ninf', ratio:'-Infinity'});

-- the values that are numbers are stored, including negative zero
CREATE (:special {name:'sp_nz',   ratio:-0.0}),
       (:special {name:'sp_one',  ratio:1.0});

-- 9a. Ordering is the same whether or not the column is consulted.
SET enable_property_promotion = on;
MATCH (n:special) RETURN n.name AS name, n.ratio AS ratio ORDER BY ratio, name;
SET enable_property_promotion = off;
MATCH (n:special) RETURN n.name AS name, n.ratio AS ratio ORDER BY ratio, name;

-- 9b. So is comparison.
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

-- 11e1. A literal property constraint gives every key a qual of its own, so the
--       whole-map containment test is only there to let a GIN index on the bag
--       carry the whole test.  A key answered from a promoted column is outside
--       that index's reach, so the containment test must be dropped where every
--       key is promoted -- otherwise it re-tests per row what the native quals
--       already decided -- and kept where some key really does come from the bag.
--       Note ginAvail walks the label's inheritors, so an index on an unrelated
--       label of the same graph must not bring it back either.
SET enable_property_promotion = on;
CREATE VLABEL gintc (age int GENERATED);
CREATE VLABEL ginother;
CREATE (:gintc {age: 5, city: 'x'}), (:gintc {age: 6, city: 'y'});
-- no GIN anywhere: native only
EXPLAIN (VERBOSE, COSTS OFF) MATCH (n:gintc {age: 5}) RETURN id(n);
CREATE INDEX gintc_gin ON tc.gintc USING gin (properties);
-- promoted key with a GIN index present: still native only
EXPLAIN (VERBOSE, COSTS OFF) MATCH (n:gintc {age: 5}) RETURN id(n);
MATCH (n:gintc {age: 5}) RETURN count(*) AS c;
-- a key that is not promoted keeps the containment test
EXPLAIN (VERBOSE, COSTS OFF) MATCH (n:gintc {city: 'x'}) RETURN id(n);
MATCH (n:gintc {city: 'x'}) RETURN count(*) AS c;
-- mixed: one key needs the bag, so the containment test stays
MATCH (n:gintc {age: 5, city: 'x'}) RETURN count(*) AS c;
-- a GIN index on an unrelated label of the same graph must not reinstate it
DROP INDEX tc.gintc_gin;
CREATE INDEX ginother_gin ON tc.ginother USING gin (properties);
EXPLAIN (VERBOSE, COSTS OFF) MATCH (n:gintc {age: 5}) RETURN id(n);
-- identical rows with promotion off
SET enable_property_promotion = off;
MATCH (n:gintc {age: 5}) RETURN count(*) AS c;
MATCH (n:gintc {city: 'x'}) RETURN count(*) AS c;
MATCH (n:gintc {age: 5, city: 'x'}) RETURN count(*) AS c;
DROP INDEX tc.ginother_gin;

-- 11e1b. The rest of the matrix, and the thing that actually matters about it:
--        WHICH ROWS come back.  The per-key quals were always the complete test,
--        so removing the containment test must not change the row set for any
--        shape of map, under any index, with promotion either way -- and the
--        containment test must still be there for a key that really is read from
--        the bag, or a GIN index cannot carry it.
--
--        Enough rows that a lost or an extra one is visible, and rows chosen to
--        exercise where the two ways of reading a key can disagree: a key STORED
--        as a string although its column is an int (the column parses it, the bag
--        holds a string), a key absent altogether, and a row missing the
--        non-promoted key instead.
CREATE VLABEL ginrow (age int GENERATED);
CREATE (:ginrow {nm: 'r1', age: 5, city: 'x'});
CREATE (:ginrow {nm: 'r2', age: 5, city: 'y'});
CREATE (:ginrow {nm: 'r3', age: 6, city: 'x'});
CREATE (:ginrow {nm: 'r4', age: '5', city: 'x'});
CREATE (:ginrow {nm: 'r5', city: 'x'});
CREATE (:ginrow {nm: 'r6', age: 5});

-- The three shapes of map, with no GIN index anywhere.  The all-promoted map
-- carries no containment test; the one with a bag key does; the mixed one does,
-- because its bag key can still be served by an index.
SET enable_property_promotion = on;
EXPLAIN (VERBOSE, COSTS OFF) MATCH (n:ginrow {age: 5}) RETURN n.nm;
EXPLAIN (VERBOSE, COSTS OFF) MATCH (n:ginrow {city: 'x'}) RETURN n.nm;
EXPLAIN (VERBOSE, COSTS OFF) MATCH (n:ginrow {age: 5, city: 'x'}) RETURN n.nm;
MATCH (n:ginrow {age: 5}) RETURN n.nm AS nm ORDER BY nm;
MATCH (n:ginrow {city: 'x'}) RETURN n.nm AS nm ORDER BY nm;
MATCH (n:ginrow {age: 5, city: 'x'}) RETURN n.nm AS nm ORDER BY nm;
-- the promotion-off baseline for the same three: identical row sets
SET enable_property_promotion = off;
MATCH (n:ginrow {age: 5}) RETURN n.nm AS nm ORDER BY nm;
MATCH (n:ginrow {city: 'x'}) RETURN n.nm AS nm ORDER BY nm;
MATCH (n:ginrow {age: 5, city: 'x'}) RETURN n.nm AS nm ORDER BY nm;

-- The same three with a GIN index ON THIS LABEL.  Only the shapes that read the
-- bag gain the containment test, and the row sets are unchanged.
CREATE INDEX ginrow_gin ON tc.ginrow USING gin (properties);
SET enable_property_promotion = on;
EXPLAIN (VERBOSE, COSTS OFF) MATCH (n:ginrow {age: 5}) RETURN n.nm;
EXPLAIN (VERBOSE, COSTS OFF) MATCH (n:ginrow {city: 'x'}) RETURN n.nm;
EXPLAIN (VERBOSE, COSTS OFF) MATCH (n:ginrow {age: 5, city: 'x'}) RETURN n.nm;
MATCH (n:ginrow {age: 5}) RETURN n.nm AS nm ORDER BY nm;
MATCH (n:ginrow {city: 'x'}) RETURN n.nm AS nm ORDER BY nm;
MATCH (n:ginrow {age: 5, city: 'x'}) RETURN n.nm AS nm ORDER BY nm;
SET enable_property_promotion = off;
MATCH (n:ginrow {age: 5}) RETURN n.nm AS nm ORDER BY nm;
MATCH (n:ginrow {city: 'x'}) RETURN n.nm AS nm ORDER BY nm;
MATCH (n:ginrow {age: 5, city: 'x'}) RETURN n.nm AS nm ORDER BY nm;
-- and with the index actually chosen, so the containment test is shown to be
-- doing the job it is kept for rather than merely being present
SET enable_seqscan = off;
SET enable_property_promotion = on;
EXPLAIN (VERBOSE, COSTS OFF) MATCH (n:ginrow {city: 'x'}) RETURN n.nm;
MATCH (n:ginrow {city: 'x'}) RETURN n.nm AS nm ORDER BY nm;
SET enable_seqscan = on;

-- The same three with the GIN index on an UNRELATED label of the same graph.
-- ginAvail walks the label's inheritors, so the presence of that index is what
-- used to bring the containment test back for a wholly promoted map.
DROP INDEX tc.ginrow_gin;
CREATE INDEX ginrow_other_gin ON tc.ginother USING gin (properties);
SET enable_property_promotion = on;
EXPLAIN (VERBOSE, COSTS OFF) MATCH (n:ginrow {age: 5}) RETURN n.nm;
EXPLAIN (VERBOSE, COSTS OFF) MATCH (n:ginrow {age: 5, city: 'x'}) RETURN n.nm;
MATCH (n:ginrow {age: 5}) RETURN n.nm AS nm ORDER BY nm;
MATCH (n:ginrow {age: 5, city: 'x'}) RETURN n.nm AS nm ORDER BY nm;
SET enable_property_promotion = off;
MATCH (n:ginrow {age: 5}) RETURN n.nm AS nm ORDER BY nm;
MATCH (n:ginrow {age: 5, city: 'x'}) RETURN n.nm AS nm ORDER BY nm;
DROP INDEX tc.ginrow_other_gin;

-- 11e1c. Two promoted keys in one map, so nothing at all is read from the bag;
--        and an EDGE property constraint, which is transformed through the other
--        of the two call sites (a pattern element inside a path, rather than a
--        standalone element qual).
SET enable_property_promotion = on;
CREATE VLABEL gintwo (age int GENERATED, big bigint GENERATED);
CREATE (:gintwo {age: 1, big: 10}), (:gintwo {age: 1, big: 20});
EXPLAIN (VERBOSE, COSTS OFF) MATCH (n:gintwo {age: 1, big: 10}) RETURN id(n);
MATCH (n:gintwo {age: 1, big: 10}) RETURN count(*) AS c;
SET enable_property_promotion = off;
MATCH (n:gintwo {age: 1, big: 10}) RETURN count(*) AS c;

SET enable_property_promotion = on;
CREATE VLABEL ginend;
CREATE ELABEL gine (weight int GENERATED);
CREATE (:ginend)-[:gine {weight: 3, kind: 'k'}]->(:ginend);
CREATE (:ginend)-[:gine {weight: 4, kind: 'k'}]->(:ginend);
-- every key promoted: no containment test on the edge either
EXPLAIN (VERBOSE, COSTS OFF) MATCH ()-[r:gine {weight: 3}]->() RETURN id(r);
MATCH ()-[r:gine {weight: 3}]->() RETURN count(*) AS c;
-- a key the edge reads from the bag: kept
EXPLAIN (VERBOSE, COSTS OFF) MATCH ()-[r:gine {kind: 'k'}]->() RETURN id(r);
MATCH ()-[r:gine {kind: 'k'}]->() RETURN count(*) AS c;
MATCH ()-[r:gine {weight: 3, kind: 'k'}]->() RETURN count(*) AS c;
SET enable_property_promotion = off;
MATCH ()-[r:gine {weight: 3}]->() RETURN count(*) AS c;
MATCH ()-[r:gine {kind: 'k'}]->() RETURN count(*) AS c;
MATCH ()-[r:gine {weight: 3, kind: 'k'}]->() RETURN count(*) AS c;

-- 11e1d. A NESTED map as a constrained value.  A nested map is matched by
--        containment -- the row's value has to contain it -- so a key whose value
--        is a map only matches a row whose value is a map containing it, and an
--        EMPTY nested map matches any row whose value is a map at all.  None of
--        that is a promoted key, so the containment test is what carries all of
--        it; the answer must not depend on whether an index is available or on how
--        the promoted keys of the label are read.
--        A leaf-BEARING nested map and a leaf-LESS one are two different tests,
--        and only the second can be answered without descending to a leaf, so the
--        second is where the constraint can go missing entirely.  Both are checked
--        against what jsonb containment answers for the same map.
CREATE VLABEL ginnest;
CREATE (:ginnest {nm: 'map', a: {b: 1}});
CREATE (:ginnest {nm: 'deep', a: {b: 1, c: 2}});
CREATE (:ginnest {nm: 'other', c: 9});
-- the containment semantics the constraint has to reproduce
SELECT count(*) AS leaf_bearing FROM tc.ginnest WHERE properties @> '{"a": {"b": 1}}';
SELECT count(*) AS leaf_less FROM tc.ginnest WHERE properties @> '{"a": {}}';
SET enable_property_promotion = on;
MATCH (n:ginnest {a: {b: 1}}) RETURN n.nm AS nm ORDER BY nm;
MATCH (n:ginnest {a: {}}) RETURN n.nm AS nm ORDER BY nm;
-- an empty map at the TOP level constrains nothing, so every row matches
MATCH (n:ginnest {}) RETURN n.nm AS nm ORDER BY nm;
SET enable_property_promotion = off;
MATCH (n:ginnest {a: {b: 1}}) RETURN n.nm AS nm ORDER BY nm;
MATCH (n:ginnest {a: {}}) RETURN n.nm AS nm ORDER BY nm;
MATCH (n:ginnest {}) RETURN n.nm AS nm ORDER BY nm;
-- and with a GIN index on the bag, which is the only thing that can serve it
CREATE INDEX ginnest_gin ON tc.ginnest USING gin (properties);
SET enable_property_promotion = on;
MATCH (n:ginnest {a: {b: 1}}) RETURN n.nm AS nm ORDER BY nm;
MATCH (n:ginnest {a: {}}) RETURN n.nm AS nm ORDER BY nm;
DROP INDEX tc.ginnest_gin;

--        Not covered here: the same leaf-BEARING map against a label where some
--        row stores a SCALAR under that key.  Containment answers that such a row
--        simply does not match, but the per-key qual descends to the leaf on every
--        row and raises "map or list is expected but scalar value", so there is no
--        agreed answer to pin yet.

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
CREATE (:jlike {nm: 'arr', s: '[9,9]'}), (:jlike {nm: 'obj', s: '{"a":1}'}),
       (:jlike {nm: 'plain', s: 'plain'});
MATCH (n:jlike) RETURN n.s AS s ORDER BY n.nm;
SET enable_property_promotion = off;
MATCH (n:jlike) RETURN n.s AS s ORDER BY n.nm;

-- 11e2b. Every shape a JSON parse would accept, in a built-in string type.  Each
--        would be a different wrong answer if the text were reinterpreted: an
--        array or an object would stop being a string, a number would stop
--        comparing as one, a boolean likewise, a JSON null would become a missing
--        value, and a quoted string would lose its quotes.  jsonb_typeof makes the
--        distinction explicit rather than leaving it to how the value prints.
SET enable_property_promotion = on;
CREATE VLABEL jshape (s text GENERATED, vc varchar GENERATED);
CREATE (:jshape {nm: 'arr',   s: '[9,9]',   vc: '[9,9]'});
CREATE (:jshape {nm: 'obj',   s: '{"a":1}', vc: '{"a":1}'});
CREATE (:jshape {nm: 'num',   s: '123',     vc: '123'});
CREATE (:jshape {nm: 'frac',  s: '1.5e2',   vc: '1.5e2'});
CREATE (:jshape {nm: 'bool',  s: 'true',    vc: 'false'});
CREATE (:jshape {nm: 'jnull', s: 'null',    vc: 'null'});
CREATE (:jshape {nm: 'quot',  s: '"x"',     vc: '"x"'});
CREATE (:jshape {nm: 'empty', s: '[]',      vc: '{}'});
CREATE (:jshape {nm: 'plain', s: 'plain',   vc: 'plain'});
MATCH (n:jshape)
  RETURN n.nm AS nm, n.s AS s, jsonb_typeof(n.s) AS sty,
         n.vc AS vc, jsonb_typeof(n.vc) AS vcty
  ORDER BY nm;
SET enable_property_promotion = off;
MATCH (n:jshape)
  RETURN n.nm AS nm, n.s AS s, jsonb_typeof(n.s) AS sty,
         n.vc AS vc, jsonb_typeof(n.vc) AS vcty
  ORDER BY nm;

-- 11e2c. The operations a changed shape would break, on a value that looks like a
--        JSON array: string equality, string length, and taking a substring -- the
--        last of which is what an over-reaching reinterpretation turned into a
--        number.
SET enable_property_promotion = on;
MATCH (n:jshape) WHERE n.nm = 'arr'
  RETURN n.s = '[9,9]' AS eq, length(n.s) AS len, substring(n.s, 1, 3) AS sub;
SET enable_property_promotion = off;
MATCH (n:jshape) WHERE n.nm = 'arr'
  RETURN n.s = '[9,9]' AS eq, length(n.s) AS len, substring(n.s, 1, 3) AS sub;

-- 11e2d. A DOMAIN is a user-defined type, but it is not a type an extension
--        defines a representation for: it is one of the built-in types under a new
--        name, and the value it holds is whatever the base type holds.  So a
--        promoted domain column must read exactly as the same column of its base
--        type does -- which for a domain over text means the two ways of reading it
--        agree, the same as 11e2b requires of text itself.
SET enable_property_promotion = on;
CREATE DOMAIN tc_dtext AS text;
CREATE VLABEL jdom (s tc_dtext GENERATED);
CREATE (:jdom {nm: 'arr',   s: '[9,9]'});
CREATE (:jdom {nm: 'obj',   s: '{"a":1}'});
CREATE (:jdom {nm: 'num',   s: '123'});
CREATE (:jdom {nm: 'bool',  s: 'true'});
CREATE (:jdom {nm: 'jnull', s: 'null'});
CREATE (:jdom {nm: 'quot',  s: '"x"'});
CREATE (:jdom {nm: 'plain', s: 'plain'});
MATCH (n:jdom) RETURN n.nm AS nm, n.s AS s, jsonb_typeof(n.s) AS ty ORDER BY nm;
SET enable_property_promotion = off;
MATCH (n:jdom) RETURN n.nm AS nm, n.s AS s, jsonb_typeof(n.s) AS ty ORDER BY nm;

-- 11e2e. An ENUM cannot be promoted at all, whichever way the generated column is
--        spelled: reading a text value back as an enum label is not immutable, and
--        a promoted column has to be.  So an enum never reaches the conversion
--        above, and this records why -- if it ever becomes promotable, 11e2d's
--        requirement applies to it too.
SET enable_property_promotion = on;
CREATE TYPE tc_enum AS ENUM ('123', 'plain');
CREATE VLABEL jenum1 (e tc_enum GENERATED);
CREATE VLABEL jenum2 (e tc_enum GENERATED ALWAYS AS ((properties ->> 'e')::tc_enum) STORED);
DROP TYPE tc_enum;

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

-- 11h. The sentinel is matched against the element it describes, so it has to keep
--      travelling with that element through every way an element can arrive at an
--      import list.  Each of these reads a promoted property inside the body and
--      so fails outright, rather than falling back to the bag, if the sentinel is
--      left behind.
SET enable_property_promotion = on;
-- an element renamed before the CALL: the import names the new name
MATCH (n:doc) WITH n AS m CALL { WITH m RETURN m.age AS a } RETURN a ORDER BY a;
-- two elements imported at once, each with promoted properties of its own
MATCH (n:doc), (o:doc) WHERE n.name = 'n1'
  CALL { WITH n, o RETURN n.age AS na, o.age AS oa }
  RETURN na, oa ORDER BY na, oa;
-- an EDGE element, whose promoted property lives on the edge label
MATCH ()-[r:rel]->() CALL { WITH r RETURN r.weight AS w } RETURN w ORDER BY w;
-- a variable whose name is a PREFIX of another's, so matching on the owner cannot
-- be a prefix comparison
MATCH (n:doc), (n2:doc) WHERE n.name = 'n1'
  CALL { WITH n2 RETURN n2.age AS a }
  RETURN DISTINCT a ORDER BY a;
-- the body projects TWO promoted properties of one imported element, so the
-- element carries more than one sentinel through the import
MATCH (n:doc) WHERE n.age > 30
  CALL { WITH n RETURN n.age AS a, n.title AS t }
  RETURN a, t ORDER BY a;
SET enable_property_promotion = off;
MATCH (n:doc) WITH n AS m CALL { WITH m RETURN m.age AS a } RETURN a ORDER BY a;
MATCH (n:doc), (o:doc) WHERE n.name = 'n1'
  CALL { WITH n, o RETURN n.age AS na, o.age AS oa }
  RETURN na, oa ORDER BY na, oa;
MATCH ()-[r:rel]->() CALL { WITH r RETURN r.weight AS w } RETURN w ORDER BY w;
MATCH (n:doc), (n2:doc) WHERE n.name = 'n1'
  CALL { WITH n2 RETURN n2.age AS a }
  RETURN DISTINCT a ORDER BY a;
MATCH (n:doc) WHERE n.age > 30
  CALL { WITH n RETURN n.age AS a, n.title AS t }
  RETURN a, t ORDER BY a;

-- 11i. An element bound to a QUOTED identifier.  A sentinel's name is built from
--      the element's name, and the element it belongs to is recovered from that
--      name, so a name that is not a bare identifier has to survive the round
--      trip.  Outside a subquery it always has; inside one it is the import list
--      the name has to be matched against.
SET enable_property_promotion = on;
MATCH ("a b":doc) WHERE "a b".name = 'n1' RETURN "a b".age AS a;
MATCH ("a b":doc) CALL { WITH "a b" RETURN "a b".age AS a } RETURN a ORDER BY a;
SET enable_property_promotion = off;
MATCH ("a b":doc) WHERE "a b".name = 'n1' RETURN "a b".age AS a;
MATCH ("a b":doc) CALL { WITH "a b" RETURN "a b".age AS a } RETURN a ORDER BY a;

-- 11i2. The same, for a quoted name containing a COLON.  A sentinel's name joins
--       the element's name and the key with colons, so recovering the element from
--       it cannot assume the element's name has none.  As everywhere else in this
--       file, how a property is read must not decide whether the query runs.
SET enable_property_promotion = on;
MATCH ("a:b":doc) WHERE "a:b".name = 'n1' RETURN "a:b".age AS a;
MATCH ("a:b":doc) CALL { WITH "a:b" RETURN "a:b".age AS a } RETURN a ORDER BY a;
SET enable_property_promotion = off;
MATCH ("a:b":doc) WHERE "a:b".name = 'n1' RETURN "a:b".age AS a;
MATCH ("a:b":doc) CALL { WITH "a:b" RETURN "a:b".age AS a } RETURN a ORDER BY a;

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

-- 19e. The column's TYPE decides how the fault shows.  A varlena read from an
--      unassigned entry dereferences a pointer that is not one; a fixed-width type
--      copies the bytes and stores them, which is a corrupt value rather than a
--      crash and would not be noticed at all.  So the types are covered together,
--      on one label, through every write operation -- and the check is that every
--      one of them reads back null, which is only true if none of them was formed
--      from an unassigned entry.
CREATE VLABEL pctypes;
ALTER TABLE tc.pctypes ADD COLUMN o_int int;         -- by value, 4 bytes
ALTER TABLE tc.pctypes ADD COLUMN o_big bigint;      -- by value, 8 bytes
ALTER TABLE tc.pctypes ADD COLUMN o_f8 float8;       -- by value, 8 bytes
ALTER TABLE tc.pctypes ADD COLUMN o_bool bool;       -- by value, 1 byte
ALTER TABLE tc.pctypes ADD COLUMN o_ts timestamptz;  -- by value, 8 bytes
ALTER TABLE tc.pctypes ADD COLUMN o_uuid uuid;       -- by reference, fixed length
ALTER TABLE tc.pctypes ADD COLUMN o_num numeric;     -- by reference, variable
ALTER TABLE tc.pctypes ADD COLUMN o_txt text;        -- by reference, toastable

CREATE VIEW tc_pctypes_null AS
SELECT count(*) FILTER (WHERE o_int IS NULL AND o_big IS NULL AND o_f8 IS NULL
                          AND o_bool IS NULL AND o_ts IS NULL AND o_uuid IS NULL
                          AND o_num IS NULL AND o_txt IS NULL) AS all_null,
       count(*) AS rows
FROM tc.pctypes;

CREATE (:pctypes {k: 1});
SELECT * FROM tc_pctypes_null;
MATCH (n:pctypes) SET n.k = 2;
SELECT * FROM tc_pctypes_null;
MATCH (n:pctypes) SET n += {tag: 'x'};
SELECT * FROM tc_pctypes_null;
MATCH (n:pctypes) SET n = {k: 3};
SELECT * FROM tc_pctypes_null;
MATCH (n:pctypes) REMOVE n.k;
SELECT * FROM tc_pctypes_null;
MERGE (n:pctypes {k: 7}) ON CREATE SET n.who = 'created';
SELECT * FROM tc_pctypes_null;
MERGE (n:pctypes {k: 7}) ON MATCH SET n.who = 'matched';
SELECT * FROM tc_pctypes_null;
MATCH (n:pctypes) RETURN n.k AS k, n.who AS who ORDER BY k, who;
MATCH (n:pctypes) DETACH DELETE n;
SELECT * FROM tc_pctypes_null;
DROP VIEW tc_pctypes_null;

-- 19f. An edge label carrying the same mix, through MERGE as well as CREATE and
--      SET.  A MERGE reflects what it wrote back through a different helper than a
--      plain SET uses, so it fills the label tuple in a second place.
CREATE VLABEL pcend;
CREATE ELABEL pcmerge;
ALTER TABLE tc.pcmerge ADD COLUMN e_int int;
ALTER TABLE tc.pcmerge ADD COLUMN e_txt text;
CREATE (:pcend {a: 1});
CREATE (:pcend {a: 2});
MATCH (x:pcend), (y:pcend) WHERE x.a = 1 AND y.a = 2
  MERGE (x)-[r:pcmerge {w: 1}]->(y) ON CREATE SET r.made = true;
SELECT count(*) AS edge_all_null FROM tc.pcmerge
 WHERE e_int IS NULL AND e_txt IS NULL;
MATCH (x:pcend), (y:pcend) WHERE x.a = 1 AND y.a = 2
  MERGE (x)-[r:pcmerge {w: 1}]->(y) ON MATCH SET r.seen = true;
SELECT count(*) AS edge_all_null FROM tc.pcmerge
 WHERE e_int IS NULL AND e_txt IS NULL;
MATCH ()-[r:pcmerge]->() RETURN r.w AS w, r.made AS made, r.seen AS seen;

-- 19g. What a write does NOT do with such a column.  It assigns the label's
--      structural columns and nothing else, so a DEFAULT is not applied -- the
--      column is written as null, not as its default -- and a NOT NULL column
--      cannot be written at all, which is reported as the constraint violation it
--      is rather than being silently skipped.
CREATE VLABEL pcdefault;
ALTER TABLE tc.pcdefault ADD COLUMN d int DEFAULT 7;
CREATE (:pcdefault {k: 1});
SELECT (properties ->> 'k') AS k, d FROM tc.pcdefault;
CREATE VLABEL pcnotnull;
ALTER TABLE tc.pcnotnull ADD COLUMN nn int NOT NULL DEFAULT 0;
CREATE (:pcnotnull {k: 1});
SELECT count(*) AS rows FROM tc.pcnotnull;

-- ----------------------------------------------------------------------------
-- 20. A variable-length relationship over an inheritance set whose labels lay
--     their columns out differently.
--
--     Inheritance does not line a child's columns up with its parent's: a
--     column added to the parent is appended at the end of each child, so a
--     child that already had one of its own carries the two in the opposite
--     order.  Below, attnum 5 is a by-value bigint on the parent and a varlena
--     on the child, so the two labels cannot be read through one tuple
--     descriptor -- a tuple is deformed against the descriptor of the slot it
--     is read into, and one hop of the expansion reads from either label.
-- ----------------------------------------------------------------------------
CREATE VLABEL pv;
CREATE ELABEL vle_par;
CREATE ELABEL vle_kid INHERITS (vle_par);
-- the child's own column comes first, so it takes attnum 5 there
ALTER TABLE tc.vle_kid ADD COLUMN kid_only text;
-- the parent's column takes attnum 5 on the parent and attnum 6 on the child
ALTER TABLE tc.vle_par ADD COLUMN par_only bigint;

-- the layout the traversal has to cope with
SELECT c.relname, a.attnum, a.attname, a.attlen
FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
WHERE c.relnamespace = 'tc'::regnamespace
  AND c.relname IN ('vle_par', 'vle_kid')
  AND a.attnum >= 5 AND NOT a.attisdropped
ORDER BY c.relname, a.attnum;

CREATE (:pv {k: 101});
CREATE (:pv {k: 102});
CREATE (:pv {k: 103});
MATCH (a:pv), (b:pv) WHERE a.k = 101 AND b.k = 102
  CREATE (a)-[:vle_kid]->(b);
MATCH (a:pv), (b:pv) WHERE a.k = 102 AND b.k = 103
  CREATE (a)-[:vle_kid]->(b);
-- a long value in the child's attnum 5, so reading it as the parent's
-- by-value bigint would be a genuine mis-deform rather than a null
SET enable_graph_dml = on;
UPDATE tc.vle_kid SET kid_only = repeat('q', 900);
SET enable_graph_dml = off;

-- expansion driven from the parent: the descriptor differs from the child's
MATCH p = (a:pv)-[:vle_par*1..2]->(z) WHERE a.k = 101
  RETURN length(p) AS len, z.k AS reached ORDER BY len, reached;
-- and from the child itself
MATCH p = (a:pv)-[:vle_kid*1..2]->(z) WHERE a.k = 101
  RETURN length(p) AS len, z.k AS reached ORDER BY len, reached;
-- the child's own column is untouched by the traversal
SELECT count(*) AS kid_rows, min(length(kid_only)) AS kid_len FROM tc.vle_kid;

-- ----------------------------------------------------------------------------
-- 20b. The same, over THREE levels and in every direction.
--
--     With three labels in the set, no two of which agree past attnum 4, one
--     expansion reads hops from whichever of them holds the next edge -- so the
--     slot has to follow the label being scanned, not the label the expansion
--     started from.  The row sets are what is asserted: the expansion must reach
--     the same vertices whichever level it is driven from, in either direction,
--     and over a range longer than one hop.
-- ----------------------------------------------------------------------------
CREATE VLABEL v3;
CREATE ELABEL e3a;
CREATE ELABEL e3b INHERITS (e3a);
CREATE ELABEL e3c INHERITS (e3b);
-- each label declares a column of its own first, so the inherited ones pile up
-- behind it and the three descriptors disagree from attnum 5 on
ALTER TABLE tc.e3c ADD COLUMN c_own text;
ALTER TABLE tc.e3b ADD COLUMN b_own bigint;
ALTER TABLE tc.e3a ADD COLUMN a_own text;
SELECT c.relname, a.attnum, a.attname, a.attlen
FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
WHERE c.relnamespace = 'tc'::regnamespace
  AND c.relname IN ('e3a', 'e3b', 'e3c')
  AND a.attnum >= 5 AND NOT a.attisdropped
ORDER BY c.relname, a.attnum;

CREATE (:v3 {k: 1});
CREATE (:v3 {k: 2});
CREATE (:v3 {k: 3});
CREATE (:v3 {k: 4});
-- one hop of each label, so a single expansion crosses all three descriptors
MATCH (a:v3), (b:v3) WHERE a.k = 1 AND b.k = 2 CREATE (a)-[:e3a]->(b);
MATCH (a:v3), (b:v3) WHERE a.k = 2 AND b.k = 3 CREATE (a)-[:e3b]->(b);
MATCH (a:v3), (b:v3) WHERE a.k = 3 AND b.k = 4 CREATE (a)-[:e3c]->(b);
-- long values where the descriptors disagree, so reading a hop through the wrong
-- one would decode a varlena as a by-value bigint rather than yielding a null
SET enable_graph_dml = on;
UPDATE tc.e3c SET c_own = repeat('c', 900);
UPDATE ONLY tc.e3a SET a_own = repeat('a', 900);
SET enable_graph_dml = off;

-- driven from the root: reaches every vertex downstream, over every label
MATCH p = (a:v3)-[:e3a*1..3]->(z) WHERE a.k = 1
  RETURN length(p) AS len, z.k AS reached ORDER BY len, reached;
-- from the middle level: only the labels at or below it
MATCH p = (a:v3)-[:e3b*1..3]->(z) WHERE a.k = 2
  RETURN length(p) AS len, z.k AS reached ORDER BY len, reached;
-- from the leaf
MATCH p = (a:v3)-[:e3c*1..3]->(z) WHERE a.k = 3
  RETURN length(p) AS len, z.k AS reached ORDER BY len, reached;
-- reversed, so the expansion walks the end column instead of the start one
MATCH p = (z)<-[:e3a*1..3]-(a:v3) WHERE a.k = 1
  RETURN length(p) AS len, z.k AS reached ORDER BY len, reached;
-- and undirected, which expands both ways from each hop
MATCH p = (a:v3)-[:e3a*1..2]-(z) WHERE a.k = 2
  RETURN length(p) AS len, z.k AS reached ORDER BY len, reached;
-- a property constraint on the expanded edge, which reads the property bag: still
-- one of the four columns every edge has, so it is inside the shared prefix
MATCH (a:v3), (b:v3) WHERE a.k = 1 AND b.k = 3 CREATE (a)-[:e3b {tag: 'skip'}]->(b);
MATCH p = (a:v3)-[:e3a*1..2]->(z) WHERE a.k = 1
  RETURN length(p) AS len, z.k AS reached ORDER BY len, reached;
-- the columns past the prefix are untouched by any of it
SELECT 'e3a' AS lab, count(*) AS rows, max(length(a_own)) AS own_len FROM ONLY tc.e3a
UNION ALL
SELECT 'e3c', count(*), max(length(c_own)) FROM tc.e3c
ORDER BY lab;

-- ----------------------------------------------------------------------------
-- 21. The promotion catalog follows the label's columns however they change.
--
--     A label is an ordinary table underneath, so plain ALTER TABLE reaches the
--     same columns that ALTER VLABEL does.  Whatever it does to them, the
--     catalog has to keep describing what is actually there: it names a promoted
--     property's column by attnum, and a reader that trusts a stale one asks for
--     a column that is gone or that no longer derives from the property.
-- ----------------------------------------------------------------------------
CREATE VLABEL sync1 (age int GENERATED, nm text GENERATED);
CREATE (:sync1 {age: 5, nm: 'a'});

SELECT p.propname, p.attnum
FROM ag_label_property p JOIN ag_label l ON l.oid = p.laboid
WHERE l.labname = 'sync1' ORDER BY p.attnum;

-- 21a. Dropping the column with plain ALTER TABLE forgets the property, so the
--      read goes back to the bag instead of a dropped attnum.
ALTER TABLE tc.sync1 DROP COLUMN age;
SELECT p.propname, p.attnum
FROM ag_label_property p JOIN ag_label l ON l.oid = p.laboid
WHERE l.labname = 'sync1' ORDER BY p.attnum;
MATCH (n:sync1) RETURN n.age AS age, n.nm AS nm;

-- 21b. DROP EXPRESSION keeps the column but stops it deriving from the
--      property, so the mapping has to go as well.
ALTER TABLE tc.sync1 ALTER COLUMN nm DROP EXPRESSION;
SELECT p.propname, p.attnum
FROM ag_label_property p JOIN ag_label l ON l.oid = p.laboid
WHERE l.labname = 'sync1' ORDER BY p.attnum;
SELECT attname, attgenerated FROM pg_attribute
WHERE attrelid = 'tc.sync1'::regclass AND attnum >= 3 AND NOT attisdropped
ORDER BY attnum;
MATCH (n:sync1) RETURN n.age AS age, n.nm AS nm;
MATCH (n:sync1) WHERE n.nm = 'a' RETURN count(*) AS matched;
MATCH (n:sync1) SET n.nm = 'b';
MATCH (n:sync1) RETURN n.nm AS nm;

-- 21c. A generated column added by plain ALTER TABLE is recorded, and an
--      ordinary one is not.
CREATE VLABEL sync2 (age int GENERATED);
ALTER TABLE tc.sync2
  ADD COLUMN sc int GENERATED ALWAYS AS ((properties ->> 'sc')::int) STORED;
ALTER TABLE tc.sync2 ADD COLUMN plainc text;
SELECT p.propname, p.attnum
FROM ag_label_property p JOIN ag_label l ON l.oid = p.laboid
WHERE l.labname = 'sync2' ORDER BY p.attnum;
CREATE (:sync2 {age: 1, sc: 9});
MATCH (n:sync2) RETURN n.age AS age, n.sc AS sc;

-- 21d. A structural column cannot be dropped through plain ALTER TABLE either.
ALTER TABLE tc.sync2 DROP COLUMN id;
ALTER TABLE tc.sync2 DROP COLUMN properties;

-- 21e. The sync reads the whole command list, so several changes in ONE statement
--      all land: dropping one promoted column and adding another leaves the catalog
--      describing exactly what the label now has.
CREATE VLABEL sync3 (age int GENERATED, nm text GENERATED);
CREATE (:sync3 {age: 1, nm: 'a', later: 5});
ALTER TABLE tc.sync3
  DROP COLUMN age,
  ADD COLUMN later int GENERATED ALWAYS AS ((properties ->> 'later')::int) STORED;
SELECT p.propname, p.attnum
FROM ag_label_property p JOIN ag_label l ON l.oid = p.laboid
WHERE l.labname = 'sync3' ORDER BY p.attnum;
MATCH (n:sync3) RETURN n.age AS age, n.nm AS nm, n.later AS later;

-- 21f. Recursion.  A promoted column added or dropped on a PARENT reaches every
--      child, so the catalog has to follow it on each of them -- a child whose
--      entry went stale would resolve the property to whatever column now sits at
--      that attnum.
CREATE VLABEL syncpar (age int GENERATED);
CREATE VLABEL synckid (own int GENERATED) INHERITS (syncpar);
CREATE (:syncpar {age: 1});
CREATE (:synckid {age: 2, own: 9, extra: 3});
SELECT l.labname, p.propname, p.attnum
FROM ag_label_property p JOIN ag_label l ON l.oid = p.laboid
WHERE l.labname IN ('syncpar', 'synckid') ORDER BY l.labname, p.attnum;
ALTER TABLE tc.syncpar
  ADD COLUMN extra int GENERATED ALWAYS AS ((properties ->> 'extra')::int) STORED;
SELECT l.labname, p.propname, p.attnum
FROM ag_label_property p JOIN ag_label l ON l.oid = p.laboid
WHERE l.labname IN ('syncpar', 'synckid') ORDER BY l.labname, p.attnum;
MATCH (n:synckid) RETURN n.age AS age, n.own AS own, n.extra AS extra;
ALTER TABLE tc.syncpar DROP COLUMN extra;
SELECT l.labname, p.propname, p.attnum
FROM ag_label_property p JOIN ag_label l ON l.oid = p.laboid
WHERE l.labname IN ('syncpar', 'synckid') ORDER BY l.labname, p.attnum;
MATCH (n:synckid) RETURN n.age AS age, n.own AS own, n.extra AS extra;

-- 21g. DROP EXPRESSION on a column a child INHERITS.  The column survives on both,
--      so both entries have to go, and both labels then read the property from the
--      bag again.
ALTER TABLE tc.syncpar ALTER COLUMN age DROP EXPRESSION;
SELECT l.labname, p.propname, p.attnum
FROM ag_label_property p JOIN ag_label l ON l.oid = p.laboid
WHERE l.labname IN ('syncpar', 'synckid') ORDER BY l.labname, p.attnum;
MATCH (n:syncpar) RETURN n.age AS age ORDER BY age;
MATCH (n:synckid) RETURN n.age AS age, n.own AS own;

-- 21h. Dropping a promoted column that a property index binds.  The index goes
--      with the column, and the catalog entry with it, so nothing is left naming
--      either.
CREATE VLABEL syncidx (age int GENERATED);
CREATE (:syncidx {age: 1});
CREATE PROPERTY INDEX syncidx_age ON syncidx (age);
SELECT count(*) AS listed FROM ag_property_indexes WHERE indexname = 'syncidx_age';
ALTER TABLE tc.syncidx DROP COLUMN age CASCADE;
SELECT count(*) AS listed FROM ag_property_indexes WHERE indexname = 'syncidx_age';
SELECT count(*) AS cataloged
FROM ag_label_property p JOIN ag_label l ON l.oid = p.laboid
WHERE l.labname = 'syncidx';
MATCH (n:syncidx) RETURN n.age AS age;

-- ----------------------------------------------------------------------------
-- 22. Reshaping a label with plain ALTER TABLE is refused.
--
--     A label's columns are part of what makes it a label, and the graph catalog
--     describes them.  ALTER TABLE reaches them without going through the
--     label's own DDL, which is how a label ends up in a shape the graph does
--     not describe -- and one that cannot be dumped for a binary upgrade,
--     because a column ALTER TABLE added cannot be put back at the same attnum.
-- ----------------------------------------------------------------------------
RESET enable_graph_ddl;

CREATE VLABEL gate (age int GENERATED);
CREATE ELABEL gate_e;
CREATE VLABEL gate_kid INHERITS (gate);

ALTER TABLE tc.gate ADD COLUMN plainc text;
ALTER TABLE tc.gate DROP COLUMN age;
ALTER TABLE tc.gate ALTER COLUMN age TYPE bigint;
ALTER TABLE tc.gate ALTER COLUMN age DROP EXPRESSION;
ALTER TABLE tc.gate_kid NO INHERIT gate;
ALTER TABLE tc.gate_e ADD COLUMN plainc text;
ALTER TABLE tc.gate ALTER COLUMN age SET EXPRESSION AS ((properties ->> 'x')::int);
-- the columns every vertex is made of are relied on to be there
ALTER TABLE tc.gate ALTER COLUMN id DROP NOT NULL;
ALTER TABLE tc.gate ALTER COLUMN properties DROP NOT NULL;

-- 22a. The label's own DDL does these things and is unaffected.
ALTER VLABEL gate ADD COLUMN nm text GENERATED;
ALTER VLABEL gate DROP COLUMN nm;

-- 22a2. A constraint on a label is dropped by the graph's own DDL, which is
--       rewritten into an ALTER TABLE -- so what the graph itself issues has to
--       keep working while the same thing typed directly is refused.
CREATE CONSTRAINT gate_uk ON gate ASSERT age IS UNIQUE;
ALTER TABLE tc.gate DROP CONSTRAINT gate_uk;
DROP CONSTRAINT gate_uk ON gate;

-- 22b. An ALTER TABLE that reshapes nothing is still allowed, including the ones
--      a dump emits.
ALTER TABLE tc.gate ALTER COLUMN properties SET DEFAULT jsonb_build_object();
ALTER TABLE ONLY tc.gate ALTER COLUMN age SET STATISTICS 500;
ALTER TABLE tc.gate ALTER COLUMN properties SET STORAGE EXTENDED;

-- 22c. A table that is not a label is not affected at all.
CREATE TABLE tc_plain_table (a int);
ALTER TABLE tc_plain_table ADD COLUMN b text;
ALTER TABLE tc_plain_table DROP COLUMN b;
DROP TABLE tc_plain_table;

-- 22d. Renaming a label's column, or moving the label out of its graph, is
--      refused for the same reason: the graph names both.
ALTER TABLE tc.gate RENAME COLUMN age TO age2;
ALTER TABLE tc.gate RENAME COLUMN properties TO props;
CREATE SCHEMA tc_elsewhere;
ALTER TABLE tc.gate SET SCHEMA tc_elsewhere;
-- an ordinary table in the same database is unaffected by either
CREATE TABLE tc_movable (a int);
ALTER TABLE tc_movable RENAME COLUMN a TO b;
ALTER TABLE tc_movable SET SCHEMA tc_elsewhere;
DROP TABLE tc_elsewhere.tc_movable;
DROP SCHEMA tc_elsewhere;

-- 22e. An ELABEL is a label too, so every one of the refusals reaches it.  An edge
--      label's structural columns run to attnum 4, and its two endpoint columns are
--      as much relied on as its id.
CREATE ELABEL gate_e2 (weight int GENERATED);
ALTER TABLE tc.gate_e2 DROP COLUMN weight;
ALTER TABLE tc.gate_e2 ALTER COLUMN weight TYPE bigint;
ALTER TABLE tc.gate_e2 ALTER COLUMN weight DROP EXPRESSION;
ALTER TABLE tc.gate_e2 ALTER COLUMN weight SET EXPRESSION AS ((properties ->> 'w')::int);
ALTER TABLE tc.gate_e2 ALTER COLUMN start DROP NOT NULL;
ALTER TABLE tc.gate_e2 ALTER COLUMN "end" DROP NOT NULL;
ALTER TABLE tc.gate_e2 RENAME COLUMN weight TO weight2;
ALTER TABLE tc.gate_e2 RENAME COLUMN "end" TO "finish";
CREATE SCHEMA tc_elsewhere2;
ALTER TABLE tc.gate_e2 SET SCHEMA tc_elsewhere2;
DROP SCHEMA tc_elsewhere2;

-- 22f. Every refusal is lifted by enable_graph_ddl, which is what a restore and a
--      repair need.  Each one is done and then undone, so the label is left as it
--      was and the section that follows still has it.
SET enable_graph_ddl = on;
ALTER TABLE tc.gate ADD COLUMN plainc text;
ALTER TABLE tc.gate DROP COLUMN plainc;
ALTER TABLE tc.gate ALTER COLUMN age TYPE bigint;
ALTER TABLE tc.gate ALTER COLUMN age TYPE int;
ALTER TABLE tc.gate RENAME COLUMN age TO age2;
ALTER TABLE tc.gate RENAME COLUMN age2 TO age;
ALTER TABLE tc.gate_kid NO INHERIT tc.gate;
ALTER TABLE tc.gate_kid INHERIT tc.gate;
CREATE SCHEMA tc_elsewhere3;
ALTER TABLE tc.gate SET SCHEMA tc_elsewhere3;
ALTER TABLE tc_elsewhere3.gate SET SCHEMA tc;
DROP SCHEMA tc_elsewhere3;
-- the drop is not undoable, so it is done on a label of its own.  Dropping the
-- not-null of a structural column is lifted too, but PostgreSQL then refuses it
-- on its own account: a label inherits that constraint from the graph's base
-- label, and an inherited not-null is not droppable on a child.  Lifting the
-- graph's refusal does not lift PostgreSQL's.
CREATE VLABEL gate_drop (age int GENERATED);
ALTER TABLE tc.gate_drop DROP COLUMN age;
ALTER TABLE tc.gate_drop ALTER COLUMN properties DROP NOT NULL;
DROP VLABEL gate_drop;
RESET enable_graph_ddl;
-- and the label is intact, so the refusals above really did refuse rather than
-- refuse-after-doing
SELECT attname, atttypid::regtype, attnotnull FROM pg_attribute
WHERE attrelid = 'tc.gate'::regclass AND attnum > 0 AND NOT attisdropped
ORDER BY attnum;

-- 22g. Reaching ALTER TABLE from somewhere else does not get around it.  The check
--      runs where the command is executed, not where it was typed, so a function
--      body, an anonymous block and an event trigger all hit it -- and so does a
--      list of subcommands whose first one is harmless, because the whole list is
--      judged before any of it runs.
CREATE FUNCTION tc_reshape() RETURNS void LANGUAGE plpgsql AS $fn$
BEGIN
	EXECUTE 'ALTER TABLE tc.gate ADD COLUMN sneaked text';
END
$fn$;
SELECT tc_reshape();
DROP FUNCTION tc_reshape();
DO $blk$ BEGIN EXECUTE 'ALTER TABLE tc.gate ADD COLUMN sneaked text'; END $blk$;
CREATE FUNCTION tc_reshape_ev() RETURNS event_trigger LANGUAGE plpgsql AS $fn$
BEGIN
	EXECUTE 'ALTER TABLE tc.gate ADD COLUMN sneaked text';
END
$fn$;
CREATE EVENT TRIGGER tc_ev ON ddl_command_end WHEN TAG IN ('CREATE TABLE')
	EXECUTE FUNCTION tc_reshape_ev();
CREATE TABLE tc_trigger_bait (a int);
DROP EVENT TRIGGER tc_ev;
DROP FUNCTION tc_reshape_ev();
DROP TABLE IF EXISTS tc_trigger_bait;
-- a harmless subcommand first
ALTER TABLE tc.gate ALTER COLUMN properties SET STATISTICS 100, ADD COLUMN sneaked text;
-- ONLY, and IF NOT EXISTS, are the same subcommand
ALTER TABLE ONLY tc.gate ADD COLUMN sneaked text;
ALTER TABLE tc.gate ADD COLUMN IF NOT EXISTS sneaked text;
-- the base labels every graph has are labels as well
ALTER TABLE tc.ag_vertex ADD COLUMN sneaked text;
ALTER TABLE tc.ag_edge ADD COLUMN sneaked text;
-- a label cannot be given a non-label parent to be reshaped through
CREATE TABLE tc_outsider (a int);
ALTER TABLE tc.gate INHERIT tc_outsider;
DROP TABLE tc_outsider;
-- nothing above left a mark
SELECT count(*) AS sneaked_columns FROM pg_attribute
WHERE attrelid = 'tc.gate'::regclass AND attname = 'sneaked';

-- 22h. Copying a label's shape into an ordinary table is not reshaping the label,
--      so it is allowed -- and what it produces is an ordinary table, which the
--      refusals do not apply to.
CREATE TABLE tc_copy (LIKE tc.gate);
ALTER TABLE tc_copy ADD COLUMN b text;
ALTER TABLE tc_copy DROP COLUMN b;
DROP TABLE tc_copy;

-- 22i. Everything the gate does not cover on a label still works, so tuning one is
--      untouched: storage, statistics, defaults, options, clustering, ownership,
--      triggers, replica identity, a constraint added rather than dropped, and a
--      NOT NULL added rather than removed.
ALTER TABLE tc.gate ALTER COLUMN age SET STORAGE PLAIN;
ALTER TABLE tc.gate ALTER COLUMN age SET (n_distinct = 100);
ALTER TABLE tc.gate ALTER COLUMN age RESET (n_distinct);
ALTER TABLE tc.gate ALTER COLUMN properties SET STATISTICS -1;
ALTER TABLE tc.gate ADD CONSTRAINT gate_chk CHECK (true);
-- adding a not-null is allowed; taking one away is not, so putting the label back
-- as it was needs the gate lifted
ALTER TABLE tc.gate ALTER COLUMN age SET NOT NULL;
ALTER TABLE tc.gate ALTER COLUMN age DROP NOT NULL;
SET enable_graph_ddl = on;
ALTER TABLE tc.gate ALTER COLUMN age DROP NOT NULL;
RESET enable_graph_ddl;
ALTER TABLE tc.gate CLUSTER ON gate_pkey;
ALTER TABLE tc.gate SET WITHOUT CLUSTER;
ALTER TABLE tc.gate DISABLE TRIGGER ALL;
ALTER TABLE tc.gate ENABLE TRIGGER ALL;
ALTER TABLE tc.gate REPLICA IDENTITY FULL;
ALTER TABLE tc.gate REPLICA IDENTITY DEFAULT;
-- dropping that constraint, though, is refused -- and the graph's own DDL is how
-- it is done, which reaches ALTER TABLE as a subcommand and so is not judged
ALTER TABLE tc.gate DROP CONSTRAINT gate_chk;
SET enable_graph_ddl = on;
ALTER TABLE tc.gate DROP CONSTRAINT gate_chk;
RESET enable_graph_ddl;

-- 22j. A table that is not a label is not affected by any of them.
CREATE TABLE tc_ordinary (a int GENERATED ALWAYS AS (1) STORED, b int NOT NULL);
CREATE TABLE tc_ordinary_kid () INHERITS (tc_ordinary);
ALTER TABLE tc_ordinary ADD COLUMN c text;
ALTER TABLE tc_ordinary ALTER COLUMN c TYPE varchar(10);
ALTER TABLE tc_ordinary ALTER COLUMN a DROP EXPRESSION;
ALTER TABLE tc_ordinary ALTER COLUMN b DROP NOT NULL;
ALTER TABLE tc_ordinary ADD CONSTRAINT tc_ord_chk CHECK (true);
ALTER TABLE tc_ordinary DROP CONSTRAINT tc_ord_chk;
ALTER TABLE tc_ordinary DROP COLUMN c;
ALTER TABLE tc_ordinary_kid NO INHERIT tc_ordinary;
ALTER TABLE tc_ordinary_kid INHERIT tc_ordinary;
ALTER TABLE tc_ordinary RENAME COLUMN b TO b2;
CREATE SCHEMA tc_elsewhere4;
ALTER TABLE tc_ordinary_kid SET SCHEMA tc_elsewhere4;
DROP TABLE tc_elsewhere4.tc_ordinary_kid;
DROP SCHEMA tc_elsewhere4;
DROP TABLE tc_ordinary;

-- 22k. The label still works after all of that was refused.
CREATE (:gate {age: 3});
MATCH (n:gate) RETURN n.age AS age;

-- 22k2. The columns every vertex and edge is made of are refused whether or not
--       the reshaping was asked for.  Reaching them is not a repair: one
--       statement on the root of a graph would otherwise rewrite a property out
--       of every label at once.
CREATE (:gate {age: 4, keep: 'v'});
SET enable_graph_ddl = on;
ALTER TABLE tc.ag_vertex ALTER COLUMN properties TYPE jsonb USING (properties - 'age');
ALTER TABLE tc.gate ALTER COLUMN properties TYPE jsonb USING (properties - 'age');
ALTER TABLE tc.gate DROP COLUMN properties;
ALTER TABLE tc.gate ALTER COLUMN id DROP NOT NULL;
-- an ordinary column is still reshapeable, which is what restoring a dump needs
ALTER TABLE tc.gate ADD COLUMN gate_extra text;
ALTER TABLE tc.gate DROP COLUMN gate_extra;
RESET enable_graph_ddl;
ALTER TABLE tc.ag_vertex ALTER COLUMN properties TYPE jsonb USING (properties - 'age');
-- nothing was lost
MATCH (n:gate) WHERE n.keep = 'v' RETURN n.age AS age, n.keep AS keep;

-- 22l. A label can inherit from an ordinary table, and altering that table
--      descends into the label -- so the statement names a relation that is not a
--      label while reshaping one that is.  What the statement reaches decides,
--      not what it addresses, and the label it would reach is what gets named.
--      Establishing the inheritance is itself reshaping the label, so it needs
--      the same asking.
CREATE VLABEL gatekid (age int GENERATED);
CREATE TABLE tc_parent ();
SET enable_graph_ddl = on;
ALTER TABLE tc.gatekid INHERIT tc_parent;
RESET enable_graph_ddl;

ALTER TABLE tc_parent ADD COLUMN sneaky int;
-- the label did not acquire it
SELECT attname FROM pg_attribute
WHERE attrelid = 'tc.gatekid'::regclass AND attname = 'sneaky'
  AND NOT attisdropped;

-- asked for, it goes through and reaches the label
SET enable_graph_ddl = on;
ALTER TABLE tc_parent ADD COLUMN deliberate int;
RESET enable_graph_ddl;
SELECT attname FROM pg_attribute
WHERE attrelid = 'tc.gatekid'::regclass AND attname = 'deliberate'
  AND NOT attisdropped;

-- an inheritance tree with no label in it is not the gate's business
CREATE TABLE tc_op ();
CREATE TABLE tc_oc () INHERITS (tc_op);
ALTER TABLE tc_op ADD COLUMN c1 int;
SELECT attname FROM pg_attribute
WHERE attrelid = 'tc_oc'::regclass AND attname = 'c1' AND NOT attisdropped;
DROP TABLE tc_oc;
DROP TABLE tc_op;

-- ----------------------------------------------------------------------------
-- A PATTERN VARIABLE THAT NAMES A COLUMN OF AN ELEMENT ALREADY IN SCOPE
--
-- Naming a promoted column puts that name in scope wherever the label is, so a
-- later element of the same pattern -- or of a later clause -- can be written
-- with a name that already resolves to a column.  It cannot be both, and saying
-- so is the whole answer; the pattern element has no label to agree with,
-- because it is not one.
-- ----------------------------------------------------------------------------
CREATE VLABEL vardoc (t int GENERATED);
CREATE ELABEL varrel (t int GENERATED);

-- the name resolves to a column of an element matched earlier in this clause
MATCH (x:vardoc), (t:vardoc) RETURN count(*);
MATCH (a:vardoc), ()-[t:varrel]->() RETURN count(*);
MATCH (a:vardoc)-[t:varrel]->(b) RETURN count(*);

-- carried in from an earlier clause, where it never named a pattern element
MATCH (n:vardoc) WITH n AS t MATCH (t:vardoc) RETURN count(*);

-- an ordinary column of a label puts a name in scope the same way
SET enable_graph_ddl = on;
ALTER TABLE tc.vardoc ADD COLUMN plaincol int;
RESET enable_graph_ddl;
MATCH (x:vardoc), (plaincol:vardoc) RETURN count(*);

-- naming something no element in scope has a column for is still fine
MATCH (a:vardoc), (b:vardoc) RETURN count(*);

-- ----------------------------------------------------------------------------
-- ONE PROPERTY, ONE COLUMN, ALL THE WAY DOWN A LABEL'S ANCESTRY
--
-- A property is read by name, so the name has to lead to one column whichever
-- label the read goes through.  Adding a property to an existing label already
-- refuses a key an ancestor promotes; declaring a label that inherits did not,
-- and neither refused a different key promoted to a column name the ancestor
-- already uses -- which inheritance merges into one column, so both keys then
-- read the same value.
-- ----------------------------------------------------------------------------
CREATE VLABEL cohp (k int GENERATED);
-- the same key, answering from a column of another name
CREATE VLABEL cohc (kt text GENERATED (k)) INHERITS (cohp);
-- a different key, answering from a column the ancestor already uses
CREATE VLABEL cohp2 (c int GENERATED (a));
CREATE VLABEL cohc2 (c int GENERATED (b)) INHERITS (cohp2);
-- a child promoting a key of its own is fine
CREATE VLABEL cohgood (other int GENERATED) INHERITS (cohp);
CREATE (:cohgood {k: 1, other: 2});
MATCH (n:cohgood) RETURN n.k AS k, n.other AS other;
-- so is re-promoting the same key to the same column
CREATE VLABEL cohgood2 (k int GENERATED) INHERITS (cohp);
DROP VLABEL cohgood2;
DROP VLABEL cohgood CASCADE;
DROP VLABEL cohp2;
DROP VLABEL cohp;

-- ----------------------------------------------------------------------------
-- A PROMOTED COLUMN CARRYING A COLLATION OF ITS OWN
--
-- Reading a property has to mean the same thing whether the column answers or
-- the map does, and a map compares strings with the database default.  A column
-- collated otherwise orders the same values differently, which would make
-- consulting it a decision about results rather than about speed.
--
-- A collation can arrive without anyone naming one, through a domain, so this
-- is checked on the column rather than on what was written.
-- ----------------------------------------------------------------------------
CREATE DOMAIN tc.ctext AS text COLLATE "C";
CREATE VLABEL collated (s tc.ctext GENERATED);
CREATE VLABEL collated2;
ALTER VLABEL collated2 ADD COLUMN s tc.ctext GENERATED;
-- neither label was left behind
SELECT count(*) FROM pg_class WHERE relname IN ('collated', 'collated2');
-- a type that carries no collation of its own is fine
CREATE VLABEL uncollated (s text GENERATED);
CREATE (:uncollated {s: 'a'});
MATCH (n:uncollated) RETURN n.s AS s;
DROP VLABEL uncollated CASCADE;
DROP VLABEL collated2;
DROP DOMAIN tc.ctext;

-- ----------------------------------------------------------------------------
-- A VALUE WHOSE KIND IS NOT THE COLUMN'S KIND
--
-- The column holds the property's value, so it has to be a value of that kind.
-- Where PostgreSQL has a cast from jsonb to the column's type, that cast is
-- what says so, and the property is taken out of the bag as jsonb rather than
-- as its text -- otherwise "5" and 5 arrive as the same thing.
--
-- The string types have no such cast: their only route from jsonb is through
-- its text form, which would arrive still quoted.  They keep taking the text,
-- so a number written into one is still read back as its digits.
-- ----------------------------------------------------------------------------
CREATE VLABEL kinds (n int GENERATED, f float8 GENERATED,
                     b bool GENERATED, s text GENERATED);

-- how each column takes its value
SELECT a.attname, pg_get_expr(d.adbin, d.adrelid) AS expr
FROM pg_attrdef d JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
WHERE d.adrelid = 'tc.kinds'::regclass AND a.attname IN ('n','f','b','s')
ORDER BY a.attname;

-- a value of the column's kind is stored
CREATE (:kinds {n: 5, f: 1.5, b: true, s: 'five'});
MATCH (v:kinds) RETURN v.n, v.f, v.b, v.s;

-- one of another kind is refused
CREATE (:kinds {n: '5'});
CREATE (:kinds {n: true});
CREATE (:kinds {f: '1.5'});
CREATE (:kinds {b: 1});
CREATE (:kinds {b: 'true'});

-- a length-limited string type cannot hold a property either: it fits the value
-- to its length instead of reporting that it does not fit
CREATE VLABEL kindsbad (s varchar(3) GENERATED);
CREATE VLABEL kindsbad (s char(5) GENERATED);
-- unconstrained, it stores what it is given
CREATE VLABEL kindsok (s varchar GENERATED);
DROP VLABEL kindsok;

-- ----------------------------------------------------------------------------
-- A WRITE THAT WOULD REPLACE THE PROPERTY MAP WITH SOMETHING THAT IS NOT AN
-- OBJECT
--
-- Creating an element already refuses this.  A write to one that exists has to
-- refuse it for the same reason, and has more to lose: the map is where every
-- property lives, so a row left holding a scalar answers nothing for the keys it
-- used to have, and every column derived from the map goes null with them.
--
-- A Cypher string literal is a string, so casting one to jsonb gives a jsonb
-- string, not the object its text spells out.  The map forms that do work are a
-- Cypher map literal and an expression that really is a jsonb object.
-- ----------------------------------------------------------------------------
CREATE VLABEL setmap (age int GENERATED);
CREATE (:setmap {age: 7, name: 'keep'});

MATCH (n:setmap) RETURN properties(n), n.age;

MATCH (n:setmap) SET n = 'hello'::jsonb;
MATCH (n:setmap) SET n = to_jsonb(42);
MATCH (n:setmap) SET n = 'null'::jsonb;
MATCH (n:setmap) SET n = jsonb_build_array(1, 2);
MERGE (n:setmap {age: 7}) ON MATCH SET n = jsonb_build_array(1, 2);

-- none of them touched the row
MATCH (n:setmap) RETURN properties(n), n.age;

-- the forms that do name a map still work, and keep the column in step
MATCH (n:setmap) SET n = {age: 8, name: 'new'};
MATCH (n:setmap) RETURN properties(n), n.age;
MATCH (n:setmap) SET n += {extra: 1};
MATCH (n:setmap) RETURN properties(n), n.age;
MATCH (n:setmap) SET n = to_jsonb('{"age": 9}'::json);
MATCH (n:setmap) RETURN properties(n), n.age;

-- ----------------------------------------------------------------------------
-- CLEANUP
-- ----------------------------------------------------------------------------
RESET enable_graph_ddl;
RESET enable_property_promotion;
RESET enable_seqscan;
DROP GRAPH tc CASCADE;
RESET graph_path;
