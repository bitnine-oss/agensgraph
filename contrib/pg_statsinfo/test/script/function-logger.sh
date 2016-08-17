#!/bin/bash

. ./script/common.sh

PGCONFIG=${CONFIG_DIR}/postgresql-logger.conf

RELOAD_DELAY=3
WRITE_DELAY=1

trap stop_all_database EXIT

echo "/*---- Initialize repository DB ----*/"
setup_repository ${REPOSITORY_DATA} ${REPOSITORY_USER} ${REPOSITORY_PORT} ${REPOSITORY_CONFIG}

echo "/*---- Initialize monitored instance ----*/"
setup_dbcluster ${PGDATA} ${PGUSER} ${PGPORT} ${PGCONFIG} "" "" ""
sleep 3
createuser -SDRl user01
createuser -SDRl user02
[ $(server_version) -lt 90000 ] &&
	createlang plpgsql
psql << EOF
CREATE TABLE tbl01 (id bigint);
CREATE FUNCTION statsinfo.elog(text, text) RETURNS void AS
\$\$
DECLARE
BEGIN
	IF \$1 = 'DEBUG' THEN
		RAISE DEBUG '%', \$2;
	ELSIF \$1 = 'INFO' THEN
		RAISE INFO '%', \$2;
	ELSIF \$1 = 'NOTICE' THEN
		RAISE NOTICE '%', \$2;
	ELSIF \$1 = 'WARNING' THEN
		RAISE WARNING '%', \$2;
	ELSIF \$1 = 'ERROR' THEN
		RAISE EXCEPTION '%', \$2;
	ELSIF \$1 = 'LOG' THEN
		RAISE LOG '%', \$2;
	ELSIF \$1 = 'ALL' THEN
		RAISE DEBUG '%', \$2;
		RAISE INFO '%', \$2;
		RAISE NOTICE '%', \$2;
		RAISE WARNING '%', \$2;
		RAISE LOG '%', \$2;
		RAISE EXCEPTION '%', \$2;
	ELSE
		RAISE EXCEPTION 'message level % not support', \$1;
	END IF;
END;
\$\$ LANGUAGE plpgsql;
EOF

echo "/*---- Server log filter ----*/"
echo "/**--- Sets the textlog's filename and access permission ---**/"
update_pgconfig ${PGDATA} "<guc_prefix>.textlog_filename" "'postgresql-statsinfo.log'"
update_pgconfig ${PGDATA} "<guc_prefix>.textlog_permission" "0644"
pg_ctl restart -w -D ${PGDATA} -o "-p ${PGPORT}" > /dev/null
sleep 3
stat -c "postgresql-statsinfo.log %A(%a)" ${PGDATA}/pg_log/postgresql-statsinfo.log
update_pgconfig ${PGDATA} "<guc_prefix>.textlog_filename" "'pg_statsinfo.log'"
update_pgconfig ${PGDATA} "<guc_prefix>.textlog_permission" "0666"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -c "SELECT pg_rotate_logfile()" > /dev/null
sleep ${WRITE_DELAY}
stat -c "pg_statsinfo.log %A(%a)" ${PGDATA}/pg_log/pg_statsinfo.log

echo "/**--- Textlog routing (textlog_min_messages = disable) ---**/"
update_pgconfig ${PGDATA} "<guc_prefix>.textlog_min_messages" "disable"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -c "SELECT statsinfo.elog('ALL', 'textlog routing test (disable)')" > /dev/null
sleep ${WRITE_DELAY}
grep "textlog routing test (disable)" ${PGDATA}/pg_log/pg_statsinfo.log

echo "/**--- Textlog routing (textlog_min_messages = error) ---**/"
update_pgconfig ${PGDATA} "<guc_prefix>.textlog_min_messages" "error"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -c "SELECT statsinfo.elog('ALL', 'textlog routing test (error)')" > /dev/null
sleep ${WRITE_DELAY}
grep "textlog routing test (error)" ${PGDATA}/pg_log/pg_statsinfo.log

echo "/**--- Textlog routing (adjust_log_level = off) ---**/"
set_pgconfig ${PGCONFIG} ${PGDATA}
update_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_level" "off"
update_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_info" "'42P01'"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -c "SELECT * FROM xxx" > /dev/null 2>&1
sleep ${WRITE_DELAY}
tail -n 2 ${PGDATA}/pg_log/pg_statsinfo.log

echo "/**--- Adjust log level (adjust_log_info = '42P01') ---**/"
update_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_level" "on"
update_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_info" "'42P01'"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -c "SELECT * FROM xxx" > /dev/null 2>&1
sleep ${WRITE_DELAY}
tail -n 2 ${PGDATA}/pg_log/pg_statsinfo.log

echo "/**--- Adjust log level (adjust_log_notice = '42P01') ---**/"
delete_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_info"
update_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_notice" "'42P01'"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -c "SELECT * FROM xxx" > /dev/null 2>&1
sleep ${WRITE_DELAY}
tail -n 2 ${PGDATA}/pg_log/pg_statsinfo.log

echo "/**--- Adjust log level (adjust_log_warning = '42P01') ---**/"
delete_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_notice"
update_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_warning" "'42P01'"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -c "SELECT * FROM xxx" > /dev/null 2>&1
sleep ${WRITE_DELAY}
tail -n 2 ${PGDATA}/pg_log/pg_statsinfo.log

echo "/**--- Adjust log level (adjust_log_error = '00000') ---**/"
delete_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_warning"
update_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_error" "'00000'"
update_pgconfig ${PGDATA} "log_statement" "'all'"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -c "SELECT 1" > /dev/null
sleep ${WRITE_DELAY}
tail -n 1 ${PGDATA}/pg_log/pg_statsinfo.log

echo "/**--- Adjust log level (adjust_log_log = '42P01') ---**/"
delete_pgconfig ${PGDATA} "log_statement"
delete_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_error"
update_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_log" "'42P01'"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -c "SELECT * FROM xxx" > /dev/null 2>&1
sleep ${WRITE_DELAY}
tail -n 2 ${PGDATA}/pg_log/pg_statsinfo.log

echo "/**--- Adjust log level (adjust_log_fatal = '42P01') ---**/"
delete_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_log"
update_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_fatal" "'42P01'"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -c "SELECT * FROM xxx" > /dev/null 2>&1
sleep ${WRITE_DELAY}
tail -n 2 ${PGDATA}/pg_log/pg_statsinfo.log

echo "/**--- Sets the nologging filter (textlog_nologging_users = 'user01') ---**/"
set_pgconfig ${PGCONFIG} ${PGDATA}
update_pgconfig ${PGDATA} "<guc_prefix>.textlog_nologging_users" "'user01'"
update_pgconfig ${PGDATA} "log_statement" "'all'"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -U ${PGUSER} -c "SELECT 1" > /dev/null
psql -U user01 -c "SELECT 2" > /dev/null
sleep ${WRITE_DELAY}
tail -n 1 ${PGDATA}/pg_log/pg_statsinfo.log

echo "/**--- Sets the nologging filter (textlog_nologging_users = 'user01, user02') ---**/"
update_pgconfig ${PGDATA} "<guc_prefix>.textlog_nologging_users" "'user01, user02'"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -U ${PGUSER} -c "SELECT 1" > /dev/null
psql -U user01 -c "SELECT 2" > /dev/null
psql -U user02 -c "SELECT 3" > /dev/null
sleep ${WRITE_DELAY}
tail -n 1 ${PGDATA}/pg_log/pg_statsinfo.log

echo "/**--- Collect the CHECKPOINT information ---**/"
set_pgconfig ${PGCONFIG} ${PGDATA}
update_pgconfig ${PGDATA} "log_checkpoints" "on"
update_pgconfig ${PGDATA} "checkpoint_timeout" "30"
if [ $(server_version) -ge 90500 ] ; then
	update_pgconfig ${PGDATA} "max_wal_size" "2"
else
	update_pgconfig ${PGDATA} "checkpoint_segments" "1"
fi
pg_ctl restart -w -D ${PGDATA} -o "-p ${PGPORT}" > /dev/null
sleep 3
psql -c "CHECKPOINT"
psql -c "SELECT pg_switch_xlog()" > /dev/null
psql -c "INSERT INTO tbl01 VALUES (0)"
sleep 35
send_query << EOF
SELECT
	instid,
	flags,
	CASE WHEN start IS NOT NULL THEN 'xxx' END AS start,
	CASE WHEN num_buffers IS NOT NULL THEN 'xxx' END AS num_buffers,
	CASE WHEN xlog_added IS NOT NULL THEN 'xxx' END AS xlog_added,
	CASE WHEN xlog_removed IS NOT NULL THEN 'xxx' END AS xlog_removed,
	CASE WHEN xlog_recycled IS NOT NULL THEN 'xxx' END AS xlog_recycled,
	CASE WHEN write_duration IS NOT NULL THEN 'xxx' END AS write_duration,
	CASE WHEN sync_duration IS NOT NULL THEN 'xxx' END AS sync_duration,
	CASE WHEN total_duration IS NOT NULL THEN 'xxx' END AS total_duration
FROM
	statsrepo.checkpoint
ORDER BY
	flags;
EOF

echo "/**--- Collect the AUTOANALYZE information ---**/"
set_pgconfig ${PGCONFIG} ${PGDATA}
update_pgconfig ${PGDATA} "autovacuum" "on"
update_pgconfig ${PGDATA} "log_autovacuum_min_duration" "0"
update_pgconfig ${PGDATA} "autovacuum_naptime" "1"
update_pgconfig ${PGDATA} "autovacuum_analyze_threshold" "10000"
update_pgconfig ${PGDATA} "autovacuum_analyze_scale_factor" "0"
update_pgconfig ${PGDATA} "autovacuum_vacuum_threshold" "4000"
update_pgconfig ${PGDATA} "autovacuum_vacuum_scale_factor" "0"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -c "INSERT INTO tbl01 VALUES (generate_series(1,10000))"
psql -c "DELETE FROM tbl01 WHERE id <= 4000"
sleep 10
send_query << EOF
SELECT
	instid,
	database,
	schema,
	"table",
	CASE WHEN start IS NOT NULL THEN 'xxx' END AS start,
	CASE WHEN duration IS NOT NULL THEN 'xxx' END AS duration
FROM
	statsrepo.autoanalyze
ORDER BY
	database, schema, "table";
EOF

echo "/**--- Collect the AUTOVACUUM information ---**/"
if [ $(server_version) -ge 90400 ] ; then
	send_query << EOF
SELECT
	instid,
	database,
	schema,
	"table",
	CASE WHEN start IS NOT NULL THEN 'xxx' END AS start,
	CASE WHEN index_scans IS NOT NULL THEN 'xxx' END AS index_scans,
	CASE WHEN page_removed IS NOT NULL THEN 'xxx' END AS page_removed,
	CASE WHEN page_remain IS NOT NULL THEN 'xxx' END AS page_remain,
	CASE WHEN tup_removed IS NOT NULL THEN 'xxx' END AS tup_removed,
	CASE WHEN tup_remain IS NOT NULL THEN 'xxx' END AS tup_remain,
	CASE WHEN tup_dead IS NOT NULL THEN 'xxx' END AS tup_dead,
	CASE WHEN page_hit IS NOT NULL THEN 'xxx' END AS page_hit,
	CASE WHEN page_miss IS NOT NULL THEN 'xxx' END AS page_miss,
	CASE WHEN page_dirty IS NOT NULL THEN 'xxx' END AS page_dirty,
	CASE WHEN read_rate IS NOT NULL THEN 'xxx' END AS read_rate,
	CASE WHEN write_rate IS NOT NULL THEN 'xxx' END AS write_rate,
	CASE WHEN duration IS NOT NULL THEN 'xxx' END AS duration
FROM
	statsrepo.autovacuum
ORDER BY
	database, schema, "table";
EOF
elif [ $(server_version) -ge 90200 ] ; then
	send_query << EOF
SELECT
	instid,
	database,
	schema,
	"table",
	CASE WHEN start IS NOT NULL THEN 'xxx' END AS start,
	CASE WHEN index_scans IS NOT NULL THEN 'xxx' END AS index_scans,
	CASE WHEN page_removed IS NOT NULL THEN 'xxx' END AS page_removed,
	CASE WHEN page_remain IS NOT NULL THEN 'xxx' END AS page_remain,
	CASE WHEN tup_removed IS NOT NULL THEN 'xxx' END AS tup_removed,
	CASE WHEN tup_remain IS NOT NULL THEN 'xxx' END AS tup_remain,
	CASE WHEN tup_dead IS NULL THEN '(N/A)' END AS tup_dead,
	CASE WHEN page_hit IS NOT NULL THEN 'xxx' END AS page_hit,
	CASE WHEN page_miss IS NOT NULL THEN 'xxx' END AS page_miss,
	CASE WHEN page_dirty IS NOT NULL THEN 'xxx' END AS page_dirty,
	CASE WHEN read_rate IS NOT NULL THEN 'xxx' END AS read_rate,
	CASE WHEN write_rate IS NOT NULL THEN 'xxx' END AS write_rate,
	CASE WHEN duration IS NOT NULL THEN 'xxx' END AS duration
FROM
	statsrepo.autovacuum
ORDER BY
	database, schema, "table";
EOF
else
	send_query << EOF
SELECT
	instid,
	database,
	schema,
	"table",
	CASE WHEN start IS NOT NULL THEN 'xxx' END AS start,
	CASE WHEN index_scans IS NOT NULL THEN 'xxx' END AS index_scans,
	CASE WHEN page_removed IS NOT NULL THEN 'xxx' END AS page_removed,
	CASE WHEN page_remain IS NOT NULL THEN 'xxx' END AS page_remain,
	CASE WHEN tup_removed IS NOT NULL THEN 'xxx' END AS tup_removed,
	CASE WHEN tup_remain IS NOT NULL THEN 'xxx' END AS tup_remain,
	CASE WHEN tup_dead IS NULL THEN '(N/A)' END AS tup_dead,
	CASE WHEN page_hit IS NULL THEN '(N/A)' END AS page_hit,
	CASE WHEN page_miss IS NULL THEN '(N/A)' END AS page_miss,
	CASE WHEN page_dirty IS NULL THEN '(N/A)' END AS page_dirty,
	CASE WHEN read_rate IS NULL THEN '(N/A)' END AS read_rate,
	CASE WHEN write_rate IS NULL THEN '(N/A)' END AS write_rate,
	CASE WHEN duration IS NOT NULL THEN 'xxx' END AS duration
FROM
	statsrepo.autovacuum
ORDER BY
	database, schema, "table";
EOF
fi

echo "/**--- Collect the cancelled AUTOVACUUM information ---**/"
update_pgconfig ${PGDATA} "autovacuum_analyze_threshold" "2147483647"
update_pgconfig ${PGDATA} "autovacuum_vacuum_threshold" "10000"
update_pgconfig ${PGDATA} "autovacuum_vacuum_cost_delay" "100ms"
update_pgconfig ${PGDATA} "autovacuum_vacuum_cost_limit" "1"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -c "TRUNCATE tbl01"
psql -c "INSERT INTO tbl01 VALUES (generate_series(1,100000))"
psql -c "DELETE FROM tbl01"
sleep 3
psql -c "BEGIN; LOCK TABLE tbl01"
sleep 3
send_query << EOF
SELECT
	t1.instid,
	CASE WHEN t1.timestamp IS NOT NULL THEN 'xxx' END AS timestamp,
	t1.database,
	t1.schema,
	t1.table,
	CASE WHEN
		(t2.major_version = '901' AND t2.minor_version >= '19') OR
		(t2.major_version = '902' AND t2.minor_version >= '14') OR
		(t2.major_version = '903' AND t2.minor_version >= '10') OR
		(t2.major_version = '904' AND t2.minor_version >= '05') OR
		 t2.major_version >= '905'
	THEN
		'(N/A)'
	ELSE
		t1.query
	END
FROM
	statsrepo.autovacuum_cancel t1,
	(SELECT substring(current_setting('server_version_num'), 1, 3) AS major_version,
			substring(current_setting('server_version_num'), 4, 5) AS minor_version) t2
ORDER BY
	t1.database, t1.schema, t1.table;
EOF
