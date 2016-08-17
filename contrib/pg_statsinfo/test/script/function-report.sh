#!/bin/bash

. ./script/common.sh

INPUTDATA_REPOSITORY=${INPUTDATA_DIR}/statsrepo-inputdata.sql

function exec_statsinfo()
{
	pg_statsinfo -U ${REPOSITORY_USER} -p ${REPOSITORY_PORT} "${@}"
}

trap stop_all_database EXIT

echo "/*---- Initialize repository DB ----*/"
setup_repository ${REPOSITORY_DATA} ${REPOSITORY_USER} ${REPOSITORY_PORT} ${REPOSITORY_CONFIG}

export PGOPTIONS=' -c intervalstyle=postgres'

[ $(server_version) -lt 90000 ] &&
	send_query -c "CREATE LANGUAGE plpgsql" > /dev/null

echo "/*---- Input the repository data ----*/"
send_query -qf "$(pg_config --sharedir)/contrib/pg_statsrepo.sql"
send_query -c "SELECT statsrepo.create_snapshot_partition('2012-11-01')" > /dev/null
send_query -qf ${INPUTDATA_REPOSITORY}
send_query << EOF > /dev/null
SELECT statsrepo.input_data(1, '5807946214009601530', 'statsinfo', 5432, '8.4.0', 1);
SELECT statsrepo.input_data(2, '5807946214009601531', 'statsinfo', 5433, '9.0.0', 5);
SELECT statsrepo.input_data(3, '5807946214009601532', 'statsinfo', 5434, '9.1.0', 9);
SELECT statsrepo.input_data(4, '5807946214009601533', 'statsinfo', 5435, '9.2.0', 13);
SELECT statsrepo.input_data(5, '5807946214009601534', 'statsinfo', 5436, '9.3.0', 17);
SELECT statsrepo.input_data(6, '5807946214009601535', 'statsinfo', 5437, '9.4.0', 21);
EOF

echo "/*---- Create report ----*/"
echo "/**--- REPORTID: Summary ---**/"
exec_command "exec_statsinfo -r Summary"

echo "/**--- REPORTID: DatabaseStatistics ---**/"
exec_command "exec_statsinfo -r DatabaseStatistics"

echo "/**--- REPORTID: InstanceActivity ---**/"
exec_command "exec_statsinfo -r InstanceActivity"

echo "/**--- REPORTID: OSResourceUsage ---**/"
exec_command "exec_statsinfo -r OSResourceUsage"

echo "/**--- REPORTID: DiskUsage ---**/"
exec_command "exec_statsinfo -r DiskUsage"

echo "/**--- REPORTID: LongTransactions ---**/"
exec_command "exec_statsinfo -r LongTransactions"

echo "/**--- REPORTID: NotableTables ---**/"
exec_command "exec_statsinfo -r NotableTables"

echo "/**--- REPORTID: CheckpointActivity ---**/"
exec_command "exec_statsinfo -r CheckpointActivity"

echo "/**--- REPORTID: AutovacuumActivity ---**/"
exec_command "exec_statsinfo -r AutovacuumActivity"

echo "/**--- REPORTID: QueryActivity ---**/"
exec_command "exec_statsinfo -r QueryActivity"

echo "/**--- REPORTID: LockConflicts ---**/"
exec_command "exec_statsinfo -r LockConflicts"

echo "/**--- REPORTID: ReplicationActivity ---**/"
exec_command "exec_statsinfo -r ReplicationActivity"

echo "/**--- REPORTID: SettingParameters ---**/"
exec_command "exec_statsinfo -r SettingParameters"

echo "/**--- REPORTID: SchemaInformation ---**/"
exec_command "exec_statsinfo -r SchemaInformation"

echo "/**--- REPORTID: Alert ---**/"
exec_command "exec_statsinfo -r Alert"

echo "/**--- REPORTID: Profiles ---**/"
exec_command "exec_statsinfo -r Profiles"

echo "/**--- REPORTID: All ---**/"
exec_command "exec_statsinfo -r All"

echo "/**--- Specify the INSTANCEID that exists in data ---**/"
exec_command "exec_statsinfo -r Summary -i 1"

echo "/**--- Specify the INSTANCEID that not exists in data ---**/"
exec_command "exec_statsinfo -r Summary -i 99"

echo "/**--- Specify the report scope (-e=2) ---**/"
exec_command "exec_statsinfo -r Summary -i 1 -e 2"

echo "/**--- Specify the report scope (-b=2, -e=3) ---**/"
exec_command "exec_statsinfo -r Summary -i 1 -b 2 -e 3"

echo "/**--- Specify the report scope (-b=3) ---**/"
exec_command "exec_statsinfo -r Summary -i 1 -b 3"

echo "/**--- Specify the report scope (-E=<snapid=2>) ---**/"
exec_command "exec_statsinfo -r Summary -i 1 -E '2012-11-01 00:01:00'"

echo "/**--- Specify the report scope (-B=<snapid=2>, -E=<snapid=3>) ---**/"
exec_command "exec_statsinfo -r Summary -i 1 -B '2012-11-01 00:01:00' -E '2012-11-01 00:02:00'"

echo "/**--- Specify the report scope (-B=<snapid=3>) ---**/"
exec_command "exec_statsinfo -r Summary -i 1 -B '2012-11-01 00:02:00'"

echo "/**--- Output the report to a file ---**/"
exec_command "exec_statsinfo -r Summary -i 1 -o ${REPOSITORY_DATA}/report.log"
cat ${REPOSITORY_DATA}/report.log

echo "/**--- Output the report to a file (overwrite) ---**/"
exec_command "exec_statsinfo -r Summary -i 2 -o ${REPOSITORY_DATA}/report.log"
cat ${REPOSITORY_DATA}/report.log

echo "/*---- Quasi-normal pattern ----*/"
echo "/**--- Contain the snapshot that is same acquisition date ---**/"
send_query -c "UPDATE statsrepo.snapshot SET time = '2012-11-01 00:00:00' WHERE instid = 5"
exec_command "exec_statsinfo -r All -i 5"
