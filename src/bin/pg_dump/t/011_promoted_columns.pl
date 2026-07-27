# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify that a graph label's promoted typed columns survive a pg_dump /
# restore round-trip: the STORED generated columns, the promotion catalog
# (ag_label_property), the typed property indexes (including one on an
# inherited key), the row data, and -- the point of the feature -- that a
# Cypher n.prop read still resolves to the typed column after restore.
#
# Two round-trips are exercised: a plain (logical) dump/restore, and the
# --binary-upgrade dump (the path pg_upgrade uses) re-loaded into a fresh
# binary-upgrade-mode cluster.  Both cover a label with a DROPPED promoted
# column (an attnum gap) and an inheritance hierarchy; the binary-upgrade path
# additionally preserves the dropped-column attnum gap and reconstructs a child's
# inherited promoted columns.
#
# Scalar promoted types only (int / text / numeric), so this needs no external
# extension (the vector column is exercised by the pg_regress vector suite).

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

# ---------------------------------------------------------------------------
# Build a source database with promoted labels, an inheritance hierarchy,
# typed property indexes (one on an inherited key), and data.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres', 'CREATE DATABASE agsrc');
$node->safe_psql(
	'agsrc', q{
	CREATE GRAPH gg;
	SET graph_path = gg;
	CREATE VLABEL doc (age int GENERATED, title text GENERATED, score numeric GENERATED);
	CREATE VLABEL par (age int GENERATED);
	CREATE VLABEL child (extra int GENERATED) INHERITS (par);
	-- Add promoted columns after the labels exist: one on a standalone label and
	-- one on the inherited parent (which recurses to the child).  The dump must
	-- carry these ALTER-added columns and their reads must resolve after restore.
	ALTER VLABEL doc ADD COLUMN rank int GENERATED;
	ALTER VLABEL par ADD COLUMN bonus int GENERATED;
	-- A label whose physical layout carries a DROPPED promoted column: ADD a
	-- column, DROP it (leaving a dropped-column attnum gap), then ADD another
	-- promoted column after the gap.  The dump must round-trip the SURVIVING
	-- promoted columns, their catalog and data, and native reads -- and the
	-- --binary-upgrade dump must preserve the attnum gap so pg_upgrade works.
	CREATE VLABEL dropcol (age int GENERATED);
	ALTER VLABEL dropcol ADD COLUMN tmp int GENERATED;   -- attnum 4
	ALTER VLABEL dropcol DROP COLUMN tmp;                -- dropped gap at attnum 4
	ALTER VLABEL dropcol ADD COLUMN rank int GENERATED;  -- attnum 5 (after the gap)
	-- An inheritance hierarchy that ALSO carries a dropped promoted column, so the
	-- --binary-upgrade dump must reconstruct BOTH the inherited promoted columns
	-- (in the child's CREATE VLABEL list) AND the dropped-column attnum gap.
	CREATE VLABEL bupar (pa int GENERATED);
	CREATE VLABEL buchild (ca int GENERATED) INHERITS (bupar);
	ALTER VLABEL buchild ADD COLUMN scratch int GENERATED; -- attnum gap after DROP
	ALTER VLABEL buchild DROP COLUMN scratch;
	CREATE PROPERTY INDEX ON doc (age);
	CREATE PROPERTY INDEX ON child (age);
	CREATE (:doc {name:'a', age:10, title:'x', score:1.5, rank:1}),
	       (:doc {name:'b', age:20, title:'y', score:2.5, rank:2});
	CREATE (:par {name:'p', age:5, bonus:100});
	CREATE (:child {name:'c', age:30, extra:9, bonus:200});
	CREATE (:dropcol {name:'k', age:10, rank:1});
	CREATE (:bupar {name:'bp', pa:1});
	CREATE (:buchild {name:'bc', pa:2, ca:9});
});

# ---------------------------------------------------------------------------
# Dump it.
# ---------------------------------------------------------------------------
my $dumpfile = "${PostgreSQL::Test::Utils::tmp_check}/promoted_dump.sql";
$node->command_ok(
	[ 'pg_dump', '-f', $dumpfile, '-d', $node->connstr('agsrc') ],
	'pg_dump of a graph with promoted columns succeeds');

my $dump = slurp_file($dumpfile);
like($dump, qr/GENERATED ALWAYS AS .*STORED/s,
	'dump carries the STORED generated column definitions');
like($dump, qr/CREATE INDEX \w+ ON gg\.\w+ USING btree \(age\)/,
	'dump carries the typed property index as a btree on the promoted column');
like($dump, qr/rank integer GENERATED ALWAYS AS/,
	'dump carries the ALTER-added promoted column (doc.rank)');

# ---------------------------------------------------------------------------
# Restore into a fresh database.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres', 'CREATE DATABASE agdst');
$node->command_ok(
	[ 'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-f', $dumpfile,
		'-d', $node->connstr('agdst') ],
	'restore into a fresh database succeeds');

# ---------------------------------------------------------------------------
# Assert the promoted structure and data round-tripped.
# ---------------------------------------------------------------------------
is( $node->safe_psql(
		'agdst',
		"SELECT string_agg(column_name, ',' ORDER BY column_name)
		   FROM information_schema.columns
		  WHERE table_schema='gg' AND table_name='doc' AND is_generated='ALWAYS'"),
	'age,rank,score,title',
	'the STORED generated columns (including the ALTER-added rank) are present after restore');

is( $node->safe_psql(
		'agdst',
		"SELECT string_agg(l.labname || '.' || p.propname, ',' ORDER BY l.labname, p.propname)
		   FROM pg_catalog.ag_label_property p
		   JOIN pg_catalog.ag_label l ON l.oid = p.laboid
		  WHERE l.labname IN ('doc','par','child')"),
	'child.extra,doc.age,doc.rank,doc.score,doc.title,par.age,par.bonus',
	'ag_label_property rows round-trip (including the inherited hierarchy and the ALTER-added columns)');

is( $node->safe_psql(
		'agdst',
		"SELECT indexdef FROM pg_indexes
		  WHERE schemaname='gg' AND indexname='child_age_idx'"),
	'CREATE INDEX child_age_idx ON gg.child USING btree (age)',
	'the property index on the inherited key is a typed btree after restore');

# The whole point: a Cypher read still resolves to the typed column and returns
# the typed value from the restored rows.
is( $node->safe_psql(
		'agdst',
		"SET graph_path=gg;
		 MATCH (n:doc) WHERE n.age > 15 RETURN n.title AS t ORDER BY t"),
	'"y"',
	'promoted read resolves after restore (doc.age filter -> typed column)');

is( $node->safe_psql(
		'agdst',
		"SET graph_path=gg;
		 MATCH (n:child) WHERE n.age > 15 RETURN n.name AS name, n.extra AS extra ORDER BY name"),
	'"c"|9',
	'promoted read resolves an inherited key on a child label after restore');

# The ALTER-added columns resolve after restore too: a standalone-label column
# (doc.rank) and one added on the inherited parent and read on the child
# (child.bonus).
is( $node->safe_psql(
		'agdst',
		"SET graph_path=gg;
		 MATCH (n:doc) WHERE n.rank = 2 RETURN n.title AS t"),
	'"y"',
	'ALTER-added promoted column resolves after restore (doc.rank filter -> typed column)');

is( $node->safe_psql(
		'agdst',
		"SET graph_path=gg;
		 MATCH (n:child) WHERE n.bonus = 200 RETURN n.name AS name"),
	'"c"',
	'ALTER-added inherited-parent column resolves on the child after restore (child.bonus)');

# ---------------------------------------------------------------------------
# A label with a DROPPED promoted column round-trips through a plain (logical)
# dump: the surviving promoted columns, their catalog and data, and native
# Cypher resolution all come back; the dropped column does NOT reappear.  A
# plain dump does not (and need not) preserve the dropped-column attnum gap --
# the survivors are recreated in a clean layout -- so resolution is asserted by
# value, not by attnum.
# ---------------------------------------------------------------------------
is( $node->safe_psql(
		'agdst',
		"SELECT string_agg(column_name, ',' ORDER BY column_name)
		   FROM information_schema.columns
		  WHERE table_schema='gg' AND table_name='dropcol' AND is_generated='ALWAYS'"),
	'age,rank',
	'a dropped promoted column does not reappear; the survivors round-trip (dropcol.age, dropcol.rank)');

is( $node->safe_psql(
		'agdst',
		"SELECT string_agg(p.propname, ',' ORDER BY p.propname)
		   FROM pg_catalog.ag_label_property p
		   JOIN pg_catalog.ag_label l ON l.oid = p.laboid
		  WHERE l.labname = 'dropcol'"),
	'age,rank',
	'ag_label_property records only the surviving promoted properties after restore');

is( $node->safe_psql(
		'agdst',
		"SET graph_path=gg;
		 MATCH (n:dropcol) WHERE n.rank = 1 RETURN n.age AS age"),
	'10',
	'a read of a promoted column added after a dropped-column gap resolves after restore (dropcol.rank/age)');

# ---------------------------------------------------------------------------
# The --binary-upgrade dump (the path pg_upgrade uses) must additionally
# preserve the dropped-column attnum gap so the restored layout matches the old
# cluster byte-for-byte, and it must reconstruct inherited promoted columns on a
# child label.  Unlike a plain dump, it lists each dropped column as a dummy
# placeholder INSIDE the CREATE VLABEL column list (exactly as a normal table's
# binary-upgrade dump does), so the recreation block that marks it dropped has a
# column to operate on and the dump is restorable.
# ---------------------------------------------------------------------------
my $budumpfile = "${PostgreSQL::Test::Utils::tmp_check}/promoted_binary_upgrade.sql";
$node->command_ok(
	[ 'pg_dump', '--binary-upgrade', '-f', $budumpfile, '-d', $node->connstr('agsrc') ],
	'pg_dump --binary-upgrade of a graph with promoted columns succeeds');

my $budump = slurp_file($budumpfile);
like($budump, qr/rank integer GENERATED ALWAYS AS/,
	'binary-upgrade dump carries the surviving ALTER-added promoted column (dropcol.rank)');

# The CREATE VLABEL for the dropped-column label (identified by its surviving
# "rank" promoted column) must carry the dropped column as a dummy INTEGER
# placeholder in its column list, so the subsequent recreation block has a target
# and the attnum gap is preserved through restore.
my ($dropcol_create) =
  ($budump =~ /^(CREATE VLABEL ONLY dropcol\([^\n]*)$/m);
like($dropcol_create,
	qr/"\Q........pg.dropped.\E\d+\Q........\E" INTEGER/,
	'binary-upgrade CREATE VLABEL lists the dropped column as a dummy placeholder (attnum gap preserved)');

# The CREATE VLABEL for the inheriting child must list the promoted columns it
# INHERITS from the parent (pa), alongside its own (ca), plus its dropped-column
# placeholder -- so a child of a promoted parent round-trips through pg_upgrade.
my ($buchild_create) =
  ($budump =~ /^(CREATE VLABEL ONLY buchild\([^\n]*)$/m);
like($buchild_create, qr/\bpa integer GENERATED ALWAYS AS/,
	'binary-upgrade child CREATE VLABEL carries the inherited promoted column (buchild.pa)');
like($buchild_create, qr/"\Q........pg.dropped.\E\d+\Q........\E" INTEGER/,
	'binary-upgrade child CREATE VLABEL also lists its dropped-column placeholder');

# ---------------------------------------------------------------------------
# The binary-upgrade dump must RE-LOAD without error into a fresh cluster.  A
# binary-upgrade restore requires the postmaster to be in binary-upgrade mode
# (the -b switch, as pg_upgrade uses): the OID-preserving binary_upgrade_*
# functions and the non-generated dropped-column placeholder in CREATE VLABEL are
# only accepted then.  PostgreSQL::Test::Cluster->start does not expose -b, so
# start the target node via pg_ctl directly and register its pid for teardown.
# ---------------------------------------------------------------------------
my $dst = PostgreSQL::Test::Cluster->new('bu_target');
$dst->init;
{
	local %ENV = $dst->_get_env(PGAPPNAME => undef);
	PostgreSQL::Test::Utils::system_or_bail(
		'pg_ctl', '--wait',
		'--pgdata' => $dst->data_dir,
		'--log' => $dst->logfile,
		'--options' => '--cluster-name=' . $dst->name . ' -b',
		'start');
}
$dst->_update_pid(1);

$dst->safe_psql('postgres', 'CREATE DATABASE agbu');
$dst->command_ok(
	[ 'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-f', $budumpfile,
		'-d', $dst->connstr('agbu') ],
	'binary-upgrade dump re-loads without error into a fresh -b target (no "column does not exist")');

# The dropped-column attnum gap is preserved: doc.rank stays at attnum 5 (a plain
# dump would recreate it at 4).  This is exactly what pg_upgrade needs.
is( $dst->safe_psql(
		'agbu',
		"SELECT p.attnum FROM pg_catalog.ag_label_property p
		   JOIN pg_catalog.ag_label l ON l.oid = p.laboid
		  WHERE l.labname = 'dropcol' AND p.propname = 'rank'"),
	'5',
	'binary-upgrade restore preserves the dropped-column attnum gap (dropcol.rank at attnum 5)');

is( $dst->safe_psql(
		'agbu',
		"SET graph_path=gg;
		 MATCH (n:dropcol) WHERE n.rank = 1 RETURN n.age AS age"),
	'10',
	'a promoted read resolves after binary-upgrade restore (dropcol)');

is( $dst->safe_psql(
		'agbu',
		"SET graph_path=gg;
		 MATCH (n:buchild) WHERE n.pa = 2 RETURN n.ca AS ca"),
	'9',
	'an inherited promoted read resolves on the child after binary-upgrade restore (buchild.pa -> buchild.ca)');

$dst->stop;
$node->stop;
done_testing();
