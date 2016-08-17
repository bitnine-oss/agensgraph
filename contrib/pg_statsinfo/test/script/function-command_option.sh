#!/bin/bash

. ./script/common.sh

PGCONFIG=${CONFIG_DIR}/postgresql-report.conf

SNAPSHOT_DELAY=3

function exec_statsinfo()
{
	pg_statsinfo -U ${REPOSITORY_USER} -p ${REPOSITORY_PORT} "${@}"
}

function exec_statsinfo2()
{
	pg_statsinfo -U ${PGUSER} -p ${PGPORT} "${@}"
}

trap stop_all_database EXIT

export PGOPTIONS=' -c intervalstyle=postgres'

echo "/*---- Initialize repository DB ----*/"
setup_repository ${REPOSITORY_DATA} ${REPOSITORY_USER} ${REPOSITORY_PORT} ${REPOSITORY_CONFIG}

echo "/*---- Initialize monitored instance ----*/"
setup_dbcluster ${PGDATA} ${PGUSER} ${PGPORT} ${PGCONFIG} "" "" ""
sleep 3

echo "/*---- Show snapshot list / Show snapshot size ----*/"
echo "/**--- Number of snapshots (0) ---**/"
exec_command "exec_statsinfo -l"
exec_command "exec_statsinfo -s"

echo "/**--- Number of snapshots (1) ---**/"
send_query << EOF > /dev/null
	INSERT INTO statsrepo.instance VALUES (1, '5807946214009601530', 'statsinfo', '5432', '8.3.0');
	INSERT INTO statsrepo.snapshot VALUES (1, 1, '2012-11-01 00:00:00+09', '1st', '00:00:01', 262144);
EOF
exec_command "exec_statsinfo -l"
exec_command "exec_statsinfo -s"

echo "/**--- Number of snapshots (2) ---**/"
send_query << EOF > /dev/null
INSERT INTO statsrepo.snapshot VALUES (2, 1, '2012-11-01 00:01:00+09', '2nd', '00:00:02', 524288);
EOF
exec_command "exec_statsinfo -l"
exec_command "exec_statsinfo -s"

echo "/**--- Specify the INSTANCEID that exists in data (list) ---**/"
send_query << EOF > /dev/null
	INSERT INTO statsrepo.instance VALUES (2, '5807946214009601531', 'statsinfo', '5433', '8.4.0');
	INSERT INTO statsrepo.snapshot VALUES (3, 2, '2012-11-01 00:03:00+09', '3rd', '00:00:01', 262144);
	SELECT setval('statsrepo.instance_instid_seq', 3, false);
	SELECT setval('statsrepo.snapshot_snapid_seq', 4, false);
EOF
exec_command "exec_statsinfo -l -i 2"

echo "/**--- Specify the INSTANCEID that not exists in data (list) ---**/"
exec_command "exec_statsinfo -l -i 3"

echo "/**--- There are multiple instances data (list) ---**/"
exec_command "exec_statsinfo -s"

echo "/*---- Get a snapshot ----*/"
echo "/**--- Specify the ASCII character as a comment of the snapshot ---**/"
exec_command "exec_statsinfo2 -S \"COMMENT\""
sleep ${SNAPSHOT_DELAY}

echo "/**--- Specify the UTF-8 character as a comment of the snapshot ---**/"
exec_command "exec_statsinfo2 -S \"マルチバイト文字\""
sleep ${SNAPSHOT_DELAY}

echo "/**--- Specify the blank character as a comment of the snapshot ---**/"
exec_command "exec_statsinfo2 -S \" \""
sleep ${SNAPSHOT_DELAY}

echo "/**--- Specify the empty character as a comment of the snapshot ---**/"
exec_command "exec_statsinfo2 -S \"\""
sleep ${SNAPSHOT_DELAY}

echo "/*---- Delete a snapshot ----*/"
echo "/**--- Specify the INSTANCEID that exists in data ---**/"
exec_command "exec_statsinfo -D 3"
sleep ${SNAPSHOT_DELAY}

echo "/**--- Specify the INSTANCEID that not exists in data ---**/"
exec_command "exec_statsinfo -D 999999"
sleep ${SNAPSHOT_DELAY}

send_query << EOF
SELECT
	snapid,
	instid,
	'"' || comment || '"' AS comment
FROM
	statsrepo.snapshot
ORDER BY
	snapid;
EOF

echo "/*---- pg_statsinfo's agent start / stop ----*/"
echo "/**--- Stop pg_statsinfo's agent ---**/"
exec_command "exec_statsinfo2 --stop"

echo "/**--- Start pg_statsinfo's agent ---**/"
exec_command "exec_statsinfo2 --start"

echo "/*---- Quasi-normal pattern ----*/"
echo "/**--- Contain the snapshot that is same acquisition date ---**/"
send_query << EOF > /dev/null
DELETE FROM statsrepo.alert WHERE instid = 3;
DELETE FROM statsrepo.instance WHERE instid = 3;
UPDATE statsrepo.snapshot SET time = '2012-11-01 00:00:00';
EOF
exec_command "exec_statsinfo -l"
exec_command "exec_statsinfo -s"
