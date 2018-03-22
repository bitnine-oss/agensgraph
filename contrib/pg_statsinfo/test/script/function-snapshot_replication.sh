#!/bin/bash

. ./script/common.sh

PGDATA_ACT=${DBCLUSTER_DIR}/pgdata-act
PGCONFIG_ACT=${CONFIG_DIR}/postgresql-replication-act.conf
PGPORT_ACT=57401
HBACONF_REPLICATION=${CONFIG_DIR}/pg_hba-replication.conf
ARCHIVE_DIR=${PGDATA_ACT}/archivelog

PGDATA_SBY=${DBCLUSTER_DIR}/pgdata-sby
PGCONFIG_SBY=${CONFIG_DIR}/postgresql-replication-sby.conf
PGPORT_SBY=57402

PGDATA_LOGICAL_SBY=${DBCLUSTER_DIR}/pgdata-logical-sby
PGCONFIG_LOGICAL_SBY=${CONFIG_DIR}/postgresql-logical-sby.conf
PGPORT_LOGICAL_SBY=57403

function get_snapshot()
{
	do_snapshot ${PGUSER} ${1} ${REPOSITORY_USER} ${REPOSITORY_PORT}
}

trap stop_all_database EXIT

echo "/*---- Initialize repository DB ----*/"
setup_repository ${REPOSITORY_DATA} ${REPOSITORY_USER} ${REPOSITORY_PORT} ${REPOSITORY_CONFIG}

echo "/*---- Initialize monitored instance (replication configuration) ----*/"
setup_dbcluster ${PGDATA_ACT} ${PGUSER} ${PGPORT_ACT} ${PGCONFIG_ACT} ${ARCHIVE_DIR} "" ${HBACONF_REPLICATION}
sleep 3
pg_basebackup -h 127.0.0.1 -p ${PGPORT_ACT} -R -D ${PGDATA_SBY}
set_pgconfig ${PGCONFIG_SBY} ${PGDATA_SBY} ${ARCHIVE_DIR}
pg_ctl start -D ${PGDATA_SBY} -o "-p ${PGPORT_SBY}" > /dev/null

echo "/*---- Initialize logical standby instance ----*/"
setup_dbcluster ${PGDATA_LOGICAL_SBY} ${PGUSER} ${PGPORT_LOGICAL_SBY} ${PGCONFIG_LOGICAL_SBY} "" "" ${HBACONF_REPLICATION}
psql -p ${PGPORT_ACT} -U ${PGUSER} -d postgres << EOF > /dev/null
CREATE TABLE xxx (col int);
CREATE PUBLICATION pub FOR TABLE xxx;
EOF
psql -p ${PGPORT_LOGICAL_SBY} -U ${PGUSER} -d postgres << EOF > /dev/null
CREATE TABLE xxx (col int);
CREATE SUBSCRIPTION sub CONNECTION 'host=127.0.0.1 port=${PGPORT_ACT}' PUBLICATION pub;
EOF

echo "/***-- Statistics of WAL (MASTER) --***/"
get_snapshot ${PGPORT_ACT}
send_query << EOF
SELECT
	snapid,
	CASE WHEN location IS NOT NULL THEN 'xxx' END AS location,
	CASE WHEN xlogfile IS NOT NULL THEN 'xxx' END AS xlogfile
FROM
	statsrepo.xlog
WHERE
	snapid = (SELECT max(snapid) FROM statsrepo.snapshot);
EOF

echo "/***-- Statistics of archive (MASTER) --***/"
psql -p ${PGPORT_ACT} << EOF > /dev/null
SELECT pg_stat_reset_shared('archiver');
SELECT ${FUNCTION_PG_SWITCH_WAL};
SELECT pg_sleep(1);
EOF
get_snapshot ${PGPORT_ACT}
send_query << EOF
SELECT
	snapid,
	archived_count,
	CASE WHEN last_archived_wal IS NOT NULL THEN 'xxx' END AS last_archived_wal,
	CASE WHEN last_archived_time IS NOT NULL THEN 'xxx' END AS last_archived_time,
	failed_count,
	CASE WHEN last_failed_wal IS NOT NULL THEN 'xxx' END AS last_failed_wal,
	CASE WHEN last_failed_time IS NOT NULL THEN 'xxx' END AS last_failed_time,
	CASE WHEN stats_reset IS NOT NULL THEN 'xxx' END AS stats_reset
FROM
	statsrepo.archive
WHERE
	snapid = (SELECT max(snapid) FROM statsrepo.snapshot);
EOF

echo "/***-- Statistics of replication (MASTER) --***/"
send_query << EOF
SELECT
	snapid,
	CASE WHEN procpid IS NOT NULL THEN 'xxx' END AS procpid,
	CASE WHEN usesysid IS NOT NULL THEN 'xxx' END AS usesysid,
	usename,
	CASE WHEN application_name IS NOT NULL THEN 'xxx' END AS application_name,
	CASE WHEN client_addr IS NOT NULL THEN 'xxx' END AS client_addr,
	CASE WHEN client_hostname IS NOT NULL THEN 'xxx' END AS client_hostname,
	CASE WHEN client_port IS NOT NULL THEN 'xxx' END AS client_port,
	CASE WHEN backend_start IS NOT NULL THEN 'xxx' END AS backend_start,
	CASE WHEN backend_xmin IS NOT NULL THEN 'xxx' END AS backend_xmin,
	state,
	CASE WHEN current_location IS NOT NULL THEN 'xxx' END AS current_location,
	CASE WHEN sent_location IS NOT NULL THEN 'xxx' END AS sent_location,
	CASE WHEN write_location IS NOT NULL THEN 'xxx' END AS write_location,
	CASE WHEN flush_location IS NOT NULL THEN 'xxx' END AS flush_location,
	CASE WHEN replay_location IS NOT NULL THEN 'xxx' END AS replay_location,
	CASE WHEN write_lag IS NOT NULL THEN 'xxx' END AS write_lag,
	CASE WHEN flush_lag IS NOT NULL THEN 'xxx' END AS flush_lag,
	CASE WHEN replay_lag IS NOT NULL THEN 'xxx' END AS replay_lag,
	sync_priority,
	sync_state
FROM
	statsrepo.replication
WHERE
	snapid = (SELECT max(snapid) FROM statsrepo.snapshot)
EOF

echo "/***-- Statistics of replication slot (MASTER) --***/"
send_query << EOF
SELECT
	snapid,
	slot_name,
	plugin,
	slot_type,
	CASE WHEN datoid IS NOT NULL THEN 'xxx' END AS datoid,
	temporary,
	active,
	CASE WHEN active_pid IS NOT NULL THEN 'xxx' END AS active_pid,
	CASE WHEN xact_xmin IS NOT NULL THEN 'xxx' END AS xact_xmin,
	CASE WHEN catalog_xmin IS NOT NULL THEN 'xxx' END AS catalog_xmin,
	CASE WHEN restart_lsn IS NOT NULL THEN 'xxx' END AS restart_lsn,
	CASE WHEN confirmed_flush_lsn IS NOT NULL THEN 'xxx' END AS confirmed_flush_lsn
FROM
	statsrepo.replication_slots
WHERE
	snapid = (SELECT max(snapid) FROM statsrepo.snapshot)
EOF

echo "/***-- Statistics of WAL (STANDBY) --***/"
get_snapshot ${PGPORT_SBY}
send_query -c "SELECT * FROM statsrepo.xlog WHERE snapid = (SELECT max(snapid) FROM statsrepo.snapshot)"

echo "/***-- Statistics of archive (STANDBY) --***/"
send_query << EOF
SELECT
	snapid,
	archived_count,
	last_archived_wal,
	last_archived_time,
	failed_count,
	last_failed_wal,
	last_failed_time,
	CASE WHEN stats_reset IS NOT NULL THEN 'xxx' END AS stats_reset
FROM
	statsrepo.archive
WHERE
	snapid = (SELECT max(snapid) FROM statsrepo.snapshot);
EOF

echo "/***-- Statistics of replication (STANDBY) --***/"
send_query -c "SELECT * FROM statsrepo.replication WHERE snapid = (SELECT max(snapid) FROM statsrepo.snapshot)"
