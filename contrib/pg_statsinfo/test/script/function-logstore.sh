#!/bin/bash

. ./script/common.sh

PGCONFIG=${CONFIG_DIR}/postgresql-logstore.conf

RELOAD_DELAY=3
STORE_DELAY=1

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

echo "/*---- Server Log Accumulation ----*/"
echo "/**--- Log Storing (repolog_min_messages = disable) ---**/"
update_pgconfig ${PGDATA} "<guc_prefix>.repolog_min_messages" "disable"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -c "SELECT statsinfo.elog('ALL', 'log storing test (disable)')" > /dev/null
sleep ${STORE_DELAY}
send_query -c "SELECT elevel, message FROM statsrepo.log WHERE message = 'log storing test (disable)'"

echo "/**--- Log Storing (repolog_min_messages = error) ---**/"
update_pgconfig ${PGDATA} "<guc_prefix>.repolog_min_messages" "error"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -c "SELECT statsinfo.elog('ALL', 'log storing test (error)')" > /dev/null
sleep ${STORE_DELAY}
send_query -c "SELECT elevel, message FROM statsrepo.log WHERE message = 'log storing test (error)'"

echo "/**--- Sets the nologging filter (repolog_nologging_users = 'user01') ---**/"
set_pgconfig ${PGCONFIG} ${PGDATA}
update_pgconfig ${PGDATA} "<guc_prefix>.repolog_nologging_users" "'user01'"
update_pgconfig ${PGDATA} "log_statement" "'all'"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -U user01 -c "SELECT 1" > /dev/null
psql -U user02 -c "SELECT 1" > /dev/null
sleep ${STORE_DELAY}
send_query -c "SELECT elevel, username, message FROM statsrepo.log WHERE username IN ('user01','user02')"

echo "/**--- Adjust log level (adjust_log_level = off) ---**/"
set_pgconfig ${PGCONFIG} ${PGDATA}
update_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_level" "off"
update_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_info" "'42P01'"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -c "SELECT * FROM xxx" > /dev/null 2>&1
sleep ${STORE_DELAY}
send_query -c "SELECT elevel, sqlstate, message FROM statsrepo.log WHERE sqlstate = '42P01' AND elevel = 'ERROR'"

echo "/**--- Adjust log level (adjust_log_info = '42P01') ---**/"
update_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_level" "on"
update_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_info" "'42P01'"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -c "SELECT * FROM xxx" > /dev/null 2>&1
sleep ${STORE_DELAY}
send_query -c "SELECT elevel, sqlstate, message FROM statsrepo.log WHERE sqlstate = '42P01' AND elevel = 'INFO'"

echo "/**--- Adjust log level (adjust_log_notice = '42P01') ---**/"
delete_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_info"
update_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_notice" "'42P01'"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -c "SELECT * FROM xxx" > /dev/null 2>&1
sleep ${STORE_DELAY}
send_query -c "SELECT elevel, sqlstate, message FROM statsrepo.log WHERE sqlstate = '42P01' AND elevel = 'NOTICE'"

echo "/**--- Adjust log level (adjust_log_warning = '42P01') ---**/"
delete_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_notice"
update_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_warning" "'42P01'"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -c "SELECT * FROM xxx" > /dev/null 2>&1
sleep ${STORE_DELAY}
send_query -c "SELECT elevel, sqlstate, message FROM statsrepo.log WHERE sqlstate = '42P01' AND elevel = 'WARNING'"

echo "/**--- Adjust log level (adjust_log_error = '00000') ---**/"
delete_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_warning"
update_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_error" "'00000'"
update_pgconfig ${PGDATA} "log_statement" "'all'"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -c "SELECT 1" > /dev/null
sleep ${STORE_DELAY}
send_query -c "SELECT elevel, sqlstate, message FROM statsrepo.log WHERE sqlstate = '00000' AND elevel = 'ERROR'"

echo "/**--- Adjust log level (adjust_log_log = '42P01') ---**/"
delete_pgconfig ${PGDATA} "log_statement"
delete_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_error"
update_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_log" "'42P01'"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -c "SELECT * FROM xxx" > /dev/null 2>&1
sleep ${STORE_DELAY}
send_query -c "SELECT elevel, sqlstate, message FROM statsrepo.log WHERE sqlstate = '42P01' AND elevel = 'LOG'"

echo "/**--- Adjust log level (adjust_log_fatal = '42P01') ---**/"
delete_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_log"
update_pgconfig ${PGDATA} "<guc_prefix>.adjust_log_fatal" "'42P01'"
pg_ctl reload && sleep ${RELOAD_DELAY}
psql -c "SELECT * FROM xxx" > /dev/null 2>&1
sleep ${STORE_DELAY}
send_query -c "SELECT elevel, sqlstate, message FROM statsrepo.log WHERE sqlstate = '42P01' AND elevel = 'FATAL'"
