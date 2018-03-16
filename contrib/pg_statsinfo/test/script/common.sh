#!/bin/bash

BASE_DIR=$(pwd)
DBCLUSTER_DIR=${BASE_DIR}/results/dbcluster
CONFIG_DIR=${BASE_DIR}/script/config
INPUTDATA_DIR=${BASE_DIR}/script/inputdata

export PGDATA=${DBCLUSTER_DIR}/pgdata
export PGPORT=57400
export PGUSER=postgres
export PGDATABASE=postgres
export PGTZ=JST-9
export PGDATESTYLE='ISO, MDY'
export LANG=C

REPOSITORY_DATA=${DBCLUSTER_DIR}/repository
REPOSITORY_CONFIG=${CONFIG_DIR}/postgresql-repository.conf
REPOSITORY_USER=postgres
REPOSITORY_PORT=57500

function server_version()
{
	local version=""
	local version_num=0
	local token_num=0
	local vmaj=0
	local vmin=0
	local vrev=0

	version=$(postgres --version | sed 's/postgres\s(PostgreSQL)\s//')
	token_num=$(echo ${version} | grep -o '\.' | wc -l)
	if [ ${token_num} -eq 2 ] ; then
		vmaj=$(echo ${version} | cut -d '.' -f 1)
		vmin=$(echo ${version} | cut -d '.' -f 2)
		vrev=$(echo ${version} | cut -d '.' -f 3)
	elif [ ${token_num} -eq 1 ] ; then
		vmaj=$(echo ${version} | cut -d '.' -f 1)
		if [ ${vmaj} -ge 10 ] ; then
			vrev=$(echo ${version} | cut -d '.' -f 2)
		else
			vmin=$(echo ${version} | cut -d '.' -f 2 | sed 's/\([0-9]\+\).*/\1/')
		fi
	else
		vmaj=$(echo ${version} | cut -d '.' -f 1 | sed 's/\([0-9]\+\).*/\1/')
	fi

	version_num=$(expr \( 100 \* ${vmaj} + ${vmin} \) \* 100 + ${vrev})
	echo ${version_num}
}

if [ $(server_version) -ge 100000 ] ; then
	PGLOG_DIR=${PGDATA}/log
	FUNCTION_PG_SWITCH_WAL="pg_switch_wal()"
else
	PGLOG_DIR=${PGDATA}/pg_log
	FUNCTION_PG_SWITCH_WAL="pg_switch_xlog()"
fi

function setup_repository()
{
	local datadir=${1}
	local superuser=${2}
	local port=${3}
	local pgconfig=${4}

	pg_ctl stop -m immediate -D ${datadir} > /dev/null 2>&1
	rm -fr ${datadir}

	initdb --no-locale -U ${superuser} -D ${datadir} > /dev/null 2>&1
	if [ ${?} -ne 0 ] ; then
		echo "ERROR: could not create database cluster of repository" 1>&2
		exit 1
	fi

	set_pgconfig ${pgconfig} ${datadir}

	pg_ctl start -w -D ${datadir} -o "-p ${port}" > /dev/null
	if [ ${?} -ne 0 ] ; then
		echo "ERROR: could not start database cluster of repository" 1>&2
		exit 1
	fi
}

function setup_dbcluster()
{
	local datadir=${1}
	local superuser=${2}
	local port=${3}
	local pgconfig=${4}
	local archivedir=${5}
	local xlogdir=${6}
	local hbaconfig=${7}
	local initdb_cmd=""

	pg_ctl stop -m immediate -D ${datadir} > /dev/null 2>&1
	rm -fr ${datadir}

	initdb_cmd="initdb --no-locale"
	[ ! -z ${datadir} ] &&
		initdb_cmd="${initdb_cmd} -D ${datadir}"
	[ ! -z ${superuser} ] &&
		initdb_cmd="${initdb_cmd} -U ${superuser}"
	[ ! -z ${xlogdir} ] &&
		initdb_cmd="${initdb_cmd} -X ${xlogdir}"

	eval ${initdb_cmd} > /dev/null 2>&1
	if [ ${?} -ne 0 ] ; then
		echo "ERROR: could not create database cluster of statsinfo" 1>&2
		exit 1
	fi

	[ ! -z ${archivedir} ] &&
		rm -fr ${archivedir} && mkdir -p ${archivedir}

	set_pgconfig ${pgconfig} ${datadir} ${archivedir}

	[ ! -z ${hbaconfig} ] &&
		cp ${hbaconfig} ${PGDATA_ACT}/pg_hba.conf

	if [ ! -z ${port} ] ; then
		pg_ctl start -w -D ${datadir} -o "-p ${port}" > /dev/null
	else
		pg_ctl start -w -D ${datadir} > /dev/null
	fi
	if [ ${?} -ne 0 ] ; then
		echo "ERROR: could not start database cluster of statsinfo" 1>&2
		exit 1
	fi
}

function set_pgconfig()
{
	local pgconfig=${1}
	local datadir=${2}
	local archivedir=${3}

	grep -q "include 'postgresql-statsinfo.conf'" ${datadir}/postgresql.conf
	[ ${?} -ne 0 ] &&
		echo "include 'postgresql-statsinfo.conf'" >> ${datadir}/postgresql.conf

	if [ -z ${pgconfig} ] ; then
		touch ${datadir}/postgresql-statsinfo.conf
	else
		local version=$(server_version)
		local guc_prefix="pg_statsinfo"
		local buffer=""

		buffer=$(cat ${pgconfig})
		if [ ${?} -ne 0 ] ; then
			echo "ERROR: could not read statsinfo's setting base file" 1>&2
			exit 1
		fi

		if [ ${version} -ge 90200 ] ; then
			buffer=$(echo "${buffer}" | grep -Pv "^\s*custom_variable_classes\s*=")
		fi

		if [ ${version} -lt 90200 ] ; then
			buffer=$(echo "${buffer}" | grep -Pv "^\s*track_io_timing\s*=")
		fi

		if [ ${version} -lt 90000 ] ; then
			buffer=$(echo "${buffer}" | grep -Pv "^\s*wal_level\s*=")
		fi

		echo "${buffer}" |
		sed "s#<archivedir>#${archivedir}#" |
		sed "s/<guc_prefix>/${guc_prefix}/" |
		sed "s/<repository_port>/${REPOSITORY_PORT}/" |
		sed "s/<repository_user>/${REPOSITORY_USER}/" > ${datadir}/postgresql-statsinfo.conf
	fi
	if [ ${?} -ne 0 ] ; then
		echo "ERROR: could not write statsinfo's setting to config file" 1>&2
		exit 1
	fi
}

function update_pgconfig()
{
	local datadir=${1}
	local param=${2}
	local value=${3}
	local buffer=""

	param=$(echo "${param}" | sed "s/<guc_prefix>/pg_statsinfo/")

	grep -Pq "^\s*${param}\s*=" ${datadir}/postgresql-statsinfo.conf
	if [ ${?} -ne 0 ] ; then
		echo "${param} = ${value}" >> ${datadir}/postgresql-statsinfo.conf
		return
	fi

	buffer=$(sed "s/^\s*${param}\s*=.\+/${param} = ${value}/" ${datadir}/postgresql-statsinfo.conf)
	echo "${buffer}" > ${datadir}/postgresql-statsinfo.conf
}

function delete_pgconfig()
{
	local datadir=${1}
	local param=${2}
	local buffer=""

	param=$(echo "${param}" | sed "s/<guc_prefix>/pg_statsinfo/")

	buffer=$(grep -Pv "^\s*${param}\s*=.\+" ${datadir}/postgresql-statsinfo.conf)
	echo "${buffer}" > ${datadir}/postgresql-statsinfo.conf
}

function do_snapshot()
{
	local user=${1}
	local port=${2}
	local repodb_user=${3}
	local repodb_port=${4}
	local comment=${5:-""}
	local prev_count=0
	local curr_count=0
	local retry=0

	prev_count=$(psql -U ${repodb_user} -p ${repodb_port} -d postgres -Atc "SELECT count(*) FROM statsrepo.snapshot")
	if [ ${?} -ne 0 ] ; then
		echo "ERROR: could not get snapid from repository" 1>&2
		exit 1
	fi

	pg_statsinfo -U ${user} -p ${port} -d postgres -S "${comment}"

	while [ ${retry} -lt 30 ]
	do
		curr_count=$(psql -U ${repodb_user} -p ${repodb_port} -d postgres -Atc "SELECT count(*) FROM statsrepo.snapshot")
		if [ ${?} -ne 0 ] ; then
			echo "ERROR: could not get snapid from repository" 1>&2
			exit 1
		fi

		[ ${curr_count} -gt ${prev_count} ] &&
			return

		retry=$(expr ${retry} + 1)
		sleep 1
	done

	echo "ERROR: snapshot has timeout" 1>&2
	exit 1
}

function send_query()
{
	psql -U ${REPOSITORY_USER} -p ${REPOSITORY_PORT} -d postgres "${@}" <&0
}

function exec_command()
{
	eval "${1}"
	printf "exit: %d\n\n" ${?}
}

function stop_all_database()
{
	if [ -e ${DBCLUSTER_DIR} ] ; then
		for datadir in $(find ${DBCLUSTER_DIR} -maxdepth 1 -mindepth 1 -type d)
		do
			pg_ctl stop -m fast -D ${datadir} > /dev/null 2>&1
		done
	fi
}
