#!/bin/bash

. ./script/common.sh

PGCONFIG=${CONFIG_DIR}/postgresql-maintenance.conf

RELOAD_DELAY=3

function get_snapshot()
{
	do_snapshot ${PGUSER} ${PGPORT} ${REPOSITORY_USER} ${REPOSITORY_PORT} "${1}"
}

trap stop_all_database EXIT

echo "/*---- Initialize repository DB ----*/"
setup_repository ${REPOSITORY_DATA} ${REPOSITORY_USER} ${REPOSITORY_PORT} ${REPOSITORY_CONFIG}

echo "/*---- Initialize monitored instance ----*/"
setup_dbcluster ${PGDATA} ${PGUSER} ${PGPORT} ${PGCONFIG} "" "" ""
sleep 3

echo "/*---- Automatic maintenance function ----*/"
echo "/**--- Delete the snapshot for a certain period of time has elapsed ---**/"
get_snapshot "2 days ago"
get_snapshot "1 days ago"
get_snapshot "today"
send_query -c "SELECT snapid, comment FROM statsrepo.snapshot ORDER BY snapid"
send_query << EOF
UPDATE statsrepo.snapshot SET "time" = "time" - '2 day'::interval WHERE snapid = 1;
UPDATE statsrepo.snapshot SET "time" = "time" - '1 day'::interval WHERE snapid = 2;
EOF
maintenance_time=$(psql -Atc "SELECT (now() + '5sec')::time(0)")
update_pgconfig ${PGDATA} "<guc_prefix>.enable_maintenance" "snapshot"
update_pgconfig ${PGDATA} "<guc_prefix>.repository_keepday" "1"
update_pgconfig ${PGDATA} "<guc_prefix>.maintenance_time" "'${maintenance_time}'"
pg_ctl reload && sleep ${RELOAD_DELAY}
sleep 10
send_query -c "SELECT snapid, comment FROM statsrepo.snapshot ORDER BY snapid"

echo "/**--- Server log maintenance ---**/"
maintenance_time=$(psql -Atc "SELECT (now() + '5sec')::time(0)")
update_pgconfig ${PGDATA} "<guc_prefix>.enable_maintenance" "log"
update_pgconfig ${PGDATA} "<guc_prefix>.log_maintenance_command" "'touch %l/ok'"
update_pgconfig ${PGDATA} "<guc_prefix>.maintenance_time" "'${maintenance_time}'"
pg_ctl reload && sleep ${RELOAD_DELAY}
sleep 10
[ -f ${PGLOG_DIR}/ok ] &&
	echo "log maintenance command called"
