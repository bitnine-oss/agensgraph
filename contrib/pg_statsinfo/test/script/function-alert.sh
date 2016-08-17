#!/bin/bash

. ./script/common.sh

PGCONFIG=${CONFIG_DIR}/postgresql-alert.conf

RELOAD_DELAY=3
ANALYZE_DELAY=1
SAMPLING_DELAY=6
WRITE_DELAY=1

function get_snapshot()
{
	do_snapshot ${PGUSER} ${PGPORT} ${REPOSITORY_USER} ${REPOSITORY_PORT}
}

trap stop_all_database EXIT

echo "/*---- Initialize repository DB ----*/"
setup_repository ${REPOSITORY_DATA} ${REPOSITORY_USER} ${REPOSITORY_PORT} ${REPOSITORY_CONFIG}

echo "/*---- Initialize monitored instance ----*/"
setup_dbcluster ${PGDATA} ${PGUSER} ${PGPORT} ${PGCONFIG} "" "" ""
sleep 3
if [ $(server_version) -ge 90100 ] ; then
	psql -c "CREATE EXTENSION pg_stat_statements" > /dev/null
else
	psql -f $(pg_config --sharedir)/contrib/pg_stat_statements.sql > /dev/null
fi

get_snapshot
send_query << EOF > /dev/null
UPDATE statsrepo.alert SET rollback_tps = -1;
UPDATE statsrepo.alert SET commit_tps = -1;
UPDATE statsrepo.alert SET garbage_size = -1;
UPDATE statsrepo.alert SET garbage_percent = -1;
UPDATE statsrepo.alert SET garbage_percent_table = -1;
UPDATE statsrepo.alert SET response_avg = -1;
UPDATE statsrepo.alert SET response_worst = -1;
UPDATE statsrepo.alert SET backend_max = -1;
UPDATE statsrepo.alert SET fragment_percent = -1;
UPDATE statsrepo.alert SET disk_remain_percent = -1;
UPDATE statsrepo.alert SET loadavg_1min = -1;
UPDATE statsrepo.alert SET loadavg_5min = -1;
UPDATE statsrepo.alert SET loadavg_15min = -1;
UPDATE statsrepo.alert SET swap_size = -1;
UPDATE statsrepo.alert SET rep_flush_delay = -1;
UPDATE statsrepo.alert SET rep_replay_delay = -1;
EOF

echo "/*---- Alert Function ----*/"
echo "/**--- Alert the number of rollbacks per second ---**/"
send_query -c "UPDATE statsrepo.alert SET rollback_tps = 0"
psql << EOF
BEGIN;
CREATE TABLE tbl01 (id bigint);
ROLLBACK;
ANALYZE;
EOF
sleep ${ANALYZE_DELAY}
get_snapshot
sleep ${WRITE_DELAY}
tail -n 1 ${PGDATA}/pg_log/pg_statsinfo.log |
sed "s/[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\s[0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}/xxx/g" |
sed "s#--- .\+ Rollbacks/sec #--- xxx Rollbacks/sec #"
send_query -c "UPDATE statsrepo.alert SET rollback_tps = -1" > /dev/null

echo "/**--- Alert the number of commits per second ---**/"
send_query -c "UPDATE statsrepo.alert SET commit_tps = 0"
psql << EOF
BEGIN;
CREATE TABLE tbl01 (id bigint);
COMMIT;
ANALYZE;
EOF
sleep ${ANALYZE_DELAY}
get_snapshot
sleep ${WRITE_DELAY}
tail -n 1 ${PGDATA}/pg_log/pg_statsinfo.log |
sed "s/[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\s[0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}/xxx/g" |
sed "s#--- .\+ Transactions/sec #--- xxx Transactions/sec #"
send_query -c "UPDATE statsrepo.alert SET commit_tps = -1" > /dev/null

echo "/**--- Alert the response time average of query ---**/"
send_query -c "UPDATE statsrepo.alert SET response_avg = 0"
psql -c "SELECT pg_sleep(1)" > /dev/null
get_snapshot
sleep ${WRITE_DELAY}
tail -n 1 ${PGDATA}/pg_log/pg_statsinfo.log |
sed "s/[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\s[0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}/xxx/g" |
sed "s/--- .\+ sec /--- xxx sec /"
send_query -c "UPDATE statsrepo.alert SET response_avg = -1" > /dev/null

echo "/**--- Alert the response time max of query ---**/"
send_query -c "UPDATE statsrepo.alert SET response_worst = 0"
psql -c "SELECT pg_sleep(1)" > /dev/null
get_snapshot
sleep ${WRITE_DELAY}
tail -n 1 ${PGDATA}/pg_log/pg_statsinfo.log |
sed "s/[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\s[0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}/xxx/g" |
sed "s/--- .\+ sec /--- xxx sec /"
send_query -c "UPDATE statsrepo.alert SET response_worst = -1" > /dev/null

echo "/**--- Alert the dead tuple size and ratio ---**/"
send_query << EOF
UPDATE statsrepo.alert SET garbage_size = 0;
UPDATE statsrepo.alert SET garbage_percent = 30;
UPDATE statsrepo.alert SET garbage_percent_table = 60;
EOF
psql << EOF
CREATE TABLE tbl02 (id bigint);
CREATE TABLE tbl03 (id bigint);
INSERT INTO tbl02 VALUES (generate_series(1,500000));
INSERT INTO tbl03 VALUES (generate_series(1,500000));
DELETE FROM tbl02 WHERE id <= 400000;
DELETE FROM tbl03 WHERE id <= 300000;
ANALYZE;
EOF
sleep ${ANALYZE_DELAY}
get_snapshot
sleep ${WRITE_DELAY}
tail -n 3 ${PGDATA}/pg_log/pg_statsinfo.log |
sed "s/[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\s[0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}/xxx/g" |
sed "s/--- .\+ \(MiB\|%\) /--- xxx \1 /"
send_query << EOF > /dev/null
UPDATE statsrepo.alert SET garbage_size = -1;
UPDATE statsrepo.alert SET garbage_percent = -1;
UPDATE statsrepo.alert SET garbage_percent_table = -1;
EOF

echo "/**--- Alert the fragmentation table ---**/"
send_query -c "UPDATE statsrepo.alert SET fragment_percent = 100"
psql << EOF
SET client_min_messages TO warning;
CREATE TABLE tbl04 (id bigint PRIMARY KEY);
ALTER TABLE tbl04 CLUSTER ON tbl04_pkey;
INSERT INTO tbl04 VALUES (5);
INSERT INTO tbl04 VALUES (4);
INSERT INTO tbl04 VALUES (1);
INSERT INTO tbl04 VALUES (3);
INSERT INTO tbl04 VALUES (2);
ANALYZE;
EOF
sleep ${ANALYZE_DELAY}
get_snapshot
sleep ${WRITE_DELAY}
tail -n 1 ${PGDATA}/pg_log/pg_statsinfo.log |
sed "s/[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\s[0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}/xxx/g"
send_query -c "UPDATE statsrepo.alert SET fragment_percent = -1" > /dev/null

echo "/**--- Alert the number of backend processes ---**/"
send_query -c "UPDATE statsrepo.alert SET backend_max = 0"
psql -c "SELECT pg_sleep(${SAMPLING_DELAY})" > /dev/null
get_snapshot
sleep ${WRITE_DELAY}
tail -n 1 ${PGDATA}/pg_log/pg_statsinfo.log |
sed "s/[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\s[0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}/xxx/g"
send_query -c "UPDATE statsrepo.alert SET backend_max = -1" > /dev/null

echo "/**--- Alert the condition of the OS resource ---**/"
send_query << EOF
UPDATE statsrepo.alert SET disk_remain_percent = default;
UPDATE statsrepo.alert SET (loadavg_1min, loadavg_5min, loadavg_15min) = (default, default, default);
UPDATE statsrepo.alert SET swap_size = default;
UPDATE statsrepo.tablespace SET (avail, total) = (1.9*1024^3, 10*1024^3) WHERE name = 'pg_default' AND snapid = 2;
UPDATE statsrepo.loadavg SET (loadavg1, loadavg5, loadavg15) = (7.1, 6.1, 5.1) WHERE snapid = 2;
UPDATE statsrepo.memory SET swap = 1000001 WHERE snapid = 2;
EOF
send_query -c "SELECT * FROM statsrepo.alert(2)" | 
sed "s/[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\s[0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}/xxx/g"
send_query << EOF > /dev/null
UPDATE statsrepo.alert SET disk_remain_percent = -1;
UPDATE statsrepo.alert SET (loadavg_1min, loadavg_5min, loadavg_15min) = (-1, -1, -1);
UPDATE statsrepo.alert SET swap_size = -1;
EOF

echo "/**--- Alert the replication delay ---**/"
send_query << EOF
UPDATE statsrepo.alert SET rep_flush_delay = default;
UPDATE statsrepo.alert SET rep_replay_delay = default;
INSERT INTO statsrepo.replication VALUES (
2, 2673, 10, 'postgres', 'sby', '127.0.0.1', NULL, 56442, '2013-01-01 00:00:00', '100', 'streaming',
'0/C900000 (00000001000000000000000C)', '0/6400000 (000000010000000000000006)',
'0/6400000 (000000010000000000000006)', '0/6400000 (000000010000000000000006)',
'0/0 (000000010000000000000000)', 0, 'sync');
EOF
send_query -c "SELECT * FROM statsrepo.alert(2)" | 
sed "s/[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\s[0-9]\{2\}:[0-9]\{2\}:[0-9]\{2\}/xxx/g"
send_query << EOF > /dev/null
UPDATE statsrepo.alert SET rep_flush_delay = -1;
UPDATE statsrepo.alert SET rep_replay_delay = -1;
EOF

echo "/**--- Collect alert messages ---**/"
send_query -c "SELECT snapid, substr(message, 1, 30) || '...' AS message FROM statsrepo.alert_message"
