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

# A plain-text dump guards its psql meta-commands with a \restrict key that
# pg_dump appoints afresh on every run, so two dumps of the same database never
# match byte for byte.  Drop the key, keeping the marker, so that comparing two
# dumps compares what they say about the database.  A dump that carries no such
# key is left as it is.
sub without_restrict_key
{
	my ($dump) = @_;

	$dump =~ s/^\\(restrict|unrestrict) \S+$/\\$1/mg;
	return $dump;
}

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

# ---------------------------------------------------------------------------
# A label can also carry an ORDINARY column -- neither one of the columns every
# graph element has, nor a promoted property.  Label DDL cannot spell one, so
# the dump has to add it separately; if it is left out altogether the COPY that
# carries the label's data names a column the restored label does not have.  In
# a plain-text dump that COPY fails and its remaining payload is then read as
# SQL, which loses the data of every label after it -- while pg_dump and psql
# both still report success.  So what matters here is the ROW COUNTS, including
# for a label that has no ordinary column of its own.
#
# This uses its own database: --binary-upgrade cannot represent an ordinary
# column at its original attnum and refuses the dump, which is asserted below.
# ---------------------------------------------------------------------------
$node->safe_psql('postgres', 'CREATE DATABASE agplain');
$node->safe_psql(
	'agplain', q{
	CREATE GRAPH gg;
	SET graph_path = gg;
	-- an ordinary column can only be put on a label by reshaping it with plain
	-- ALTER TABLE, which is refused unless this is on
	SET enable_graph_ddl = on;
	-- carries nothing unusual: it is the label that silently lost its data
	CREATE VLABEL untouched;
	CREATE (:untouched {a:1}), (:untouched {a:2}), (:untouched {a:3});
	-- an ordinary column, added the only way one can be
	CREATE VLABEL plainv;
	ALTER TABLE gg.plainv ADD COLUMN extra text;
	CREATE (:plainv {k:1}), (:plainv {k:2});
	-- an ordinary column at a LOWER attnum than a promoted one, behind a
	-- dropped-column gap, and with a non-default collation
	CREATE VLABEL mixed;
	ALTER TABLE gg.mixed ADD COLUMN gap int;
	ALTER TABLE gg.mixed DROP COLUMN gap;
	ALTER TABLE gg.mixed ADD COLUMN plain_first text COLLATE "C";
	ALTER VLABEL mixed ADD COLUMN age int GENERATED;
	CREATE (:mixed {age:7}), (:mixed {age:8});
	-- an ordinary column inherited by a child label
	CREATE VLABEL par3;
	ALTER TABLE gg.par3 ADD COLUMN pcol text;
	CREATE VLABEL kid3 INHERITS (par3);
	CREATE (:par3 {z:1}), (:kid3 {z:2});
	-- and one on an EDGE label, whose structural columns run to attnum 4
	CREATE ELABEL erel;
	ALTER TABLE gg.erel ADD COLUMN ecol int;
	MATCH (a:untouched), (b:untouched) WHERE a.a = 1 AND b.a = 2
	  CREATE (a)-[:erel]->(b);
	-- TWO ordinary columns, each with a DEFAULT.  The ADD COLUMN that puts the
	-- column back deliberately does not carry the default; the per-column pass
	-- later in the dump emits it, which only works because that pass runs after
	-- the column exists.
	CREATE VLABEL defv;
	ALTER TABLE gg.defv ADD COLUMN d1 int DEFAULT 42;
	ALTER TABLE gg.defv ADD COLUMN d2 text DEFAULT 'dd';
	CREATE (:defv {k:1});
	-- an ordinary column of a CHILD label, beside one it inherits from its
	-- parent.  Only its own is added back, and the inherited one has to keep the
	-- attnum ahead of it -- which it only does if it arrives with the label,
	-- through INHERITS, rather than being added afterwards.
	CREATE VLABEL cpar;
	ALTER TABLE gg.cpar ADD COLUMN pcol text;
	CREATE VLABEL ckid INHERITS (cpar);
	ALTER TABLE gg.ckid ADD COLUMN kcol text;
	CREATE (:cpar {z:1}), (:ckid {z:2});
	-- a label whose ordinary column was DROPPED again.  A plain dump has nothing
	-- to add back for it, and must not try: naming a column that no longer
	-- exists would fail the restore exactly the way omitting a live one does.
	CREATE VLABEL dropord (age int GENERATED);
	ALTER TABLE gg.dropord ADD COLUMN gone text;
	ALTER TABLE gg.dropord DROP COLUMN gone;
	CREATE (:dropord {age:3});
	-- nothing unusual, created last so that it is also emitted last.  The symptom
	-- of a label's ordinary column going missing is the COPY for it failing and
	-- the rest of the dump's payload being read as SQL, which loses the data of
	-- every label AFTER it -- so a bystander is needed at each end.
	CREATE VLABEL zlast;
	CREATE (:zlast {a:1}), (:zlast {a:2});
});
$node->safe_psql(
	'agplain', q{
	SET enable_graph_dml = on;
	SET enable_graph_ddl = on;
	UPDATE gg.plainv SET extra = 'e1';
	UPDATE gg.mixed SET plain_first = 'pf';
	UPDATE gg.par3 SET pcol = 'p';
	UPDATE gg.erel SET ecol = 42;
	UPDATE ONLY gg.cpar SET pcol = 'cp';
	UPDATE gg.ckid SET pcol = 'ck-inherited', kcol = 'ck-own';
	-- now that it is populated, make it NOT NULL as well, so the dump has to
	-- carry that too.  (It could not be NOT NULL while the rows were being
	-- created: a Cypher write cannot name an ordinary column, so it leaves it
	-- null.)
	ALTER TABLE gg.mixed ALTER COLUMN plain_first SET NOT NULL;
});

my $plainfile = "${PostgreSQL::Test::Utils::tmp_check}/plain_cols.sql";
$node->command_ok(
	[ 'pg_dump', '-f', $plainfile, '-d', $node->connstr('agplain') ],
	'pg_dump of a graph with ordinary label columns succeeds');

my $plaindump = slurp_file($plainfile);
like($plaindump, qr/ALTER TABLE gg\.plainv ADD COLUMN extra text;/,
	'dump adds the ordinary column back');
like($plaindump, qr/ADD COLUMN plain_first text COLLATE pg_catalog\."C" NOT NULL;/,
	"dump keeps the ordinary column's collation and NOT NULL");
like($plaindump, qr/ALTER TABLE gg\.erel ADD COLUMN ecol integer;/,
	"dump adds an edge label's ordinary column");
# a child inherits it, so only the parent declares it
unlike($plaindump, qr/ALTER TABLE gg\.kid3 ADD COLUMN pcol/,
	'an inherited ordinary column is not re-added on the child');

# A child's OWN ordinary column is added back on the child, while the one it
# inherits still is not -- the child holds both, and only one of them is its own.
like($plaindump, qr/ALTER TABLE gg\.ckid ADD COLUMN kcol text;/,
	"a child label's own ordinary column is added back on the child");
unlike($plaindump, qr/ALTER TABLE gg\.ckid ADD COLUMN pcol/,
	'the column the same child inherits is still left to the parent');

# The ADD COLUMN does not carry the default; the per-column pass emits it
# afterwards, which is the only order that works.
like($plaindump, qr/ALTER TABLE gg\.defv ADD COLUMN d1 integer;/,
	'an ordinary column with a default is added back without it');
like($plaindump, qr/ALTER TABLE gg\.defv ALTER COLUMN d1 SET DEFAULT 42;/,
	'and the default is set afterwards, once the column exists');
like($plaindump, qr/ALTER TABLE gg\.defv ALTER COLUMN d2 SET DEFAULT 'dd'::text;/,
	'a second ordinary column on the same label keeps its default too');

# A dropped ordinary column has nothing to add back.  Naming it would fail the
# restore the same way omitting a live one does.
unlike($plaindump, qr/ALTER TABLE gg\.dropord ADD COLUMN/,
	'a dropped ordinary column is not added back');

# Restoring has to be able to reshape a label, which is otherwise refused, so the
# dump asks for it -- the ADD COLUMNs above are exactly what needs it.
like($plaindump, qr/^SET enable_graph_ddl = on;$/m,
	'the dump enables label reshaping, without which its ADD COLUMNs are refused');

$node->safe_psql('postgres', 'CREATE DATABASE agplaindst');
$node->command_ok(
	[ 'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-f', $plainfile,
		'-d', $node->connstr('agplaindst') ],
	'restore of a graph with ordinary label columns succeeds');

# The heart of it: every label's rows are back, the untouched one included.
is( $node->safe_psql(
		'agplaindst',
		q{SELECT (SELECT count(*) FROM gg.untouched) || ' '
		      || (SELECT count(*) FROM gg.plainv)    || ' '
		      || (SELECT count(*) FROM gg.mixed)     || ' '
		      || (SELECT count(*) FROM gg.par3)      || ' '
		      || (SELECT count(*) FROM gg.kid3)      || ' '
		      || (SELECT count(*) FROM gg.erel)}),
	'3 2 2 2 1 1',
	'every label restores its rows, including one with no ordinary column');

# The same for the shapes added later, ending with the label emitted last -- the
# one furthest downstream of a COPY that could have failed.
is( $node->safe_psql(
		'agplaindst',
		q{SELECT (SELECT count(*) FROM gg.defv)    || ' '
		      || (SELECT count(*) FROM gg.cpar)    || ' '
		      || (SELECT count(*) FROM gg.ckid)    || ' '
		      || (SELECT count(*) FROM gg.dropord) || ' '
		      || (SELECT count(*) FROM gg.zlast)}),
	'1 2 1 1 2',
	'the later shapes restore their rows too, the last-emitted label included');

# The defaults are back on the columns, not merely mentioned in the dump.
is( $node->safe_psql(
		'agplaindst',
		"SELECT string_agg(a.attname || '=' || pg_get_expr(d.adbin, d.adrelid), ' ' ORDER BY a.attnum)
		   FROM pg_attribute a
		   JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		  WHERE a.attrelid = 'gg.defv'::regclass AND a.attnum > 2"),
	q{d1=42 d2='dd'::text},
	"an ordinary column's default round-trips");

# The child's two ordinary columns come back in their original order: the
# inherited one first, its own after.  Adding the inherited one on the child
# instead of letting INHERITS bring it would reverse them.
is( $node->safe_psql(
		'agplaindst',
		q{SELECT string_agg(attname || '@' || attnum, ' ' ORDER BY attnum)
		    FROM pg_attribute
		   WHERE attrelid = 'gg.ckid'::regclass AND attnum > 2 AND NOT attisdropped}),
	'pcol@3 kcol@4',
	"a child label's inherited and own ordinary columns keep their attnums");

is( $node->safe_psql(
		'agplaindst',
		q{SELECT pcol || ' ' || kcol FROM gg.ckid}),
	'ck-inherited ck-own',
	"the data of a child label's inherited and own ordinary columns round-trips");

# A dropped ordinary column stays dropped, and the promoted column that came
# after it still resolves.
is( $node->safe_psql(
		'agplaindst',
		"SELECT count(*) FROM pg_attribute
		  WHERE attrelid = 'gg.dropord'::regclass AND attname = 'gone'"),
	'0',
	'a dropped ordinary column does not come back');

is( $node->safe_psql(
		'agplaindst',
		"SET graph_path=gg; MATCH (n:dropord) WHERE n.age = 3 RETURN n.age AS age"),
	'3',
	'a promoted read resolves on a label that once had an ordinary column');

is( $node->safe_psql(
		'agplaindst',
		'SELECT max(extra) FROM gg.plainv'),
	'e1',
	"an ordinary column's data round-trips");

is( $node->safe_psql(
		'agplaindst',
		q{SELECT max(plain_first) || ' ' || max(age) FROM gg.mixed}),
	'pf 8',
	'an ordinary column and a promoted column on one label both round-trip');

is( $node->safe_psql(
		'agplaindst',
		'SELECT max(ecol) FROM gg.erel'),
	'42',
	"an edge label's ordinary column round-trips");

is( $node->safe_psql(
		'agplaindst',
		"SELECT co.collname FROM pg_attribute a
		   JOIN pg_class c ON c.oid = a.attrelid
		   JOIN pg_collation co ON co.oid = a.attcollation
		  WHERE c.relnamespace = 'gg'::regnamespace
		    AND c.relname = 'mixed' AND a.attname = 'plain_first'"),
	'C',
	"an ordinary column's collation survives, so it still compares the same way");

is( $node->safe_psql(
		'agplaindst',
		"SET graph_path=gg; MATCH (n:mixed) WHERE n.age = 7 RETURN n.age AS age"),
	'7',
	'a promoted read still resolves on a label that also has an ordinary column');

# Re-dumping the restored database must reproduce the same dump.
my $redumpfile = "${PostgreSQL::Test::Utils::tmp_check}/plain_cols_redump.sql";
$node->command_ok(
	[ 'pg_dump', '-f', $redumpfile, '-d', $node->connstr('agplaindst') ],
	'the restored database dumps again');
is(without_restrict_key(slurp_file($redumpfile)),
	without_restrict_key($plaindump),
	'the re-dump is identical to the original dump');

# --binary-upgrade points the restored label at the original heap files, so an
# ordinary column that cannot be placed at its original attnum has to stop the
# dump rather than produce one that restores into a mis-aligned heap.
$node->command_fails_like(
	[
		'pg_dump', '--binary-upgrade', '-f',
		"${PostgreSQL::Test::Utils::tmp_check}/plain_cols_bu.sql",
		'-d', $node->connstr('agplain')
	],
	qr/ordinary column .* cannot be dumped for a binary upgrade/,
	'--binary-upgrade refuses a label with an ordinary column instead of corrupting it');

# The same graph through the custom archive format and pg_restore, which writes
# its own preamble: the ADD COLUMNs a label's ordinary columns need are refused
# unless that preamble asks for label reshaping too.
my $custfile = "${PostgreSQL::Test::Utils::tmp_check}/plain_cols.dump";
$node->command_ok(
	[ 'pg_dump', '-Fc', '-f', $custfile, '-d', $node->connstr('agplain') ],
	'a custom-format dump of a graph with ordinary label columns succeeds');
$node->safe_psql('postgres', 'CREATE DATABASE agplaincust');
$node->command_ok(
	[ 'pg_restore', '--exit-on-error', '-d', $node->connstr('agplaincust'),
		$custfile ],
	'pg_restore of a custom-format dump succeeds');
is( $node->safe_psql(
		'agplaincust',
		q{SELECT (SELECT count(*) FROM gg.untouched) || ' '
		      || (SELECT count(*) FROM gg.plainv)    || ' '
		      || (SELECT count(*) FROM gg.zlast)     || ' '
		      || (SELECT max(extra) FROM gg.plainv)}),
	'3 2 2 e1',
	'a custom-format restore brings back the rows and the ordinary column data');

# --binary-upgrade only has to refuse a LIVE ordinary column.  One that was
# dropped again leaves no column to place, so the dump must still succeed -- and
# it has to keep the dropped attnum, which here sits after a promoted column.
$node->safe_psql('postgres', 'CREATE DATABASE agdroponly');
$node->safe_psql(
	'agdroponly', q{
	CREATE GRAPH gg;
	SET graph_path = gg;
	SET enable_graph_ddl = on;
	-- a dropped ordinary column AFTER a promoted one
	CREATE VLABEL after_prom (age int GENERATED);
	ALTER TABLE gg.after_prom ADD COLUMN gone text;
	ALTER TABLE gg.after_prom DROP COLUMN gone;
	CREATE (:after_prom {age:3});
	-- and one BEFORE a promoted one, so the placeholder has to be emitted ahead
	-- of the promoted column rather than appended after it
	CREATE VLABEL before_prom;
	ALTER TABLE gg.before_prom ADD COLUMN gone text;
	ALTER TABLE gg.before_prom DROP COLUMN gone;
	ALTER VLABEL before_prom ADD COLUMN age int GENERATED;
	CREATE (:before_prom {age:4});
});
my $droponlyfile = "${PostgreSQL::Test::Utils::tmp_check}/droponly_bu.sql";
$node->command_ok(
	[
		'pg_dump', '--binary-upgrade', '-f', $droponlyfile,
		'-d', $node->connstr('agdroponly')
	],
	'--binary-upgrade still dumps a label whose ordinary column was dropped again');

my $droponly = slurp_file($droponlyfile);
my ($after_create) =
  ($droponly =~ /^(CREATE VLABEL ONLY after_prom\([^\n]*)$/m);
like($after_create,
	qr/age integer GENERATED ALWAYS AS .*"\Q........pg.dropped.\E\d+\Q........\E" INTEGER/,
	'the placeholder for the dropped column follows the promoted column it followed');
my ($before_create) =
  ($droponly =~ /^(CREATE VLABEL ONLY before_prom\([^\n]*)$/m);
like($before_create,
	qr/"\Q........pg.dropped.\E\d+\Q........\E" INTEGER.*age integer GENERATED ALWAYS AS/,
	'and precedes the promoted column it preceded');

$node->stop;
done_testing();
