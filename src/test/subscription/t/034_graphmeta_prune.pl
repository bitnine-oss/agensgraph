
# Copyright (c) 2024, PostgreSQL Global Development Group

# ag_graphmeta connectivity maintenance under logical replication.
#
# A subscriber that applies replicated edge-label INSERTs (the execReplication.c
# ExecSimpleRelationInsert path) must keep its ag_graphmeta catalog complete --
# otherwise graphmeta-based scan pruning on the subscriber would prune away, and
# silently drop, the replicated rows.  This exercises the apply-worker write hook
# that records connectivity for edges that arrive via replication rather than
# Cypher.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Publisher does logical decoding; the subscriber runs with connectivity
# gathering ON (set in postgresql.conf so the background apply worker sees it),
# which is what makes the apply-path hook record replicated edges.
my $pub = PostgreSQL::Test::Cluster->new('pub_graphmeta');
$pub->init(allows_streaming => 'logical');
$pub->start;

my $sub = PostgreSQL::Test::Cluster->new('sub_graphmeta');
$sub->init;
$sub->append_conf('postgresql.conf', 'auto_gather_graphmeta = on');
$sub->start;

# Logical replication does not replicate DDL, so build an identical graph schema
# on both nodes.  Creating the labels in the same order keeps their label ids
# aligned, so the graphid values in replicated edge rows mean the same thing on
# both sides.
my $ddl =
  "CREATE GRAPH g; SET graph_path = g; CREATE VLABEL v; CREATE VLABEL w; CREATE ELABEL e;";
$pub->safe_psql('postgres', $ddl);
$sub->safe_psql('postgres', $ddl);

# Publish the label tables and subscribe.  The tables are empty, so the initial
# sync copies nothing -- every edge below arrives through the streaming apply
# worker, which is the path under test.
$pub->safe_psql('postgres', "CREATE PUBLICATION p_graphmeta FOR TABLE g.v, g.w, g.e;");
my $connstr = $pub->connstr . ' dbname=postgres';
$sub->safe_psql('postgres',
	"CREATE SUBSCRIPTION s_graphmeta CONNECTION '$connstr' PUBLICATION p_graphmeta;"
);
$sub->wait_for_subscription_sync($pub, 's_graphmeta');

my $triples_sql =
  "SELECT string_agg(start || '-' || edge || '-' || \"end\", ',' ORDER BY start, edge, \"end\") "
  . "FROM ag_graphmeta_view WHERE graphname = 'g';";

# Nothing replicated yet -> subscriber connectivity is empty.
is($sub->safe_psql('postgres', $triples_sql),
	'', 'subscriber ag_graphmeta empty before any edge is applied');

# Stream an edge v -> v through the apply worker.
$pub->safe_psql('postgres', "SET graph_path = g; CREATE (:v)-[:e]->(:v);");
$pub->wait_for_catchup('s_graphmeta');

is($sub->safe_psql('postgres', $triples_sql),
	'v-e-v',
	'streaming apply records the first replicated edge connectivity on subscriber');

# Stream a second edge with different connectivity v -> w.
$pub->safe_psql('postgres', "SET graph_path = g; CREATE (:v)-[:e]->(:w);");
$pub->wait_for_catchup('s_graphmeta');

is($sub->safe_psql('postgres', $triples_sql),
	'v-e-v,v-e-w',
	'streaming apply records additional replicated edge connectivity on subscriber');

# The edges themselves actually replicated (sanity check on the row data).
is( $sub->safe_psql('postgres', "SELECT count(*)::text FROM g.e;"),
	'2', 'both edges replicated to the subscriber');

$sub->safe_psql('postgres', "DROP SUBSCRIPTION s_graphmeta;");
$pub->stop;
$sub->stop;

done_testing();
