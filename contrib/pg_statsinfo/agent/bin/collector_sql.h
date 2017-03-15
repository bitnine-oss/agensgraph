/*
 * collector_sql.h
 *
 * Copyright (c) 2009-2017, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#ifndef COLLECTOR_SQL_H
#define COLLECTOR_SQL_H

#if PG_VERSION_NUM >= 90200
#define PG_STAT_ACTIVITY_ATTNAME_PID		"pid"
#define PG_STAT_ACTIVITY_ATTNAME_QUERY		"query"
#define PG_STAT_REPLICATION_ATTNAME_PID		"pid"
#else
#define PG_STAT_ACTIVITY_ATTNAME_PID		"procpid"
#define PG_STAT_ACTIVITY_ATTNAME_QUERY		"current_query"
#define PG_STAT_REPLICATION_ATTNAME_PID		"procpid"
#endif

/*----------------------------------------------------------------------------
 * snapshots per instance
 *----------------------------------------------------------------------------
 */

/* database */
#if PG_VERSION_NUM >= 90200
#define SQL_SELECT_DATABASE "\
SELECT \
	d.oid AS dbid, \
	d.datname, \
	pg_database_size(d.oid), \
	CASE WHEN pg_is_in_recovery() THEN 0 ELSE age(d.datfrozenxid) END, \
	pg_stat_get_db_xact_commit(d.oid) AS xact_commit, \
	pg_stat_get_db_xact_rollback(d.oid) AS xact_rollback, \
	pg_stat_get_db_blocks_fetched(d.oid) - pg_stat_get_db_blocks_hit(d.oid) AS blks_read, \
	pg_stat_get_db_blocks_hit(d.oid) AS blks_hit, \
	pg_stat_get_db_tuples_returned(d.oid) AS tup_returned, \
	pg_stat_get_db_tuples_fetched(d.oid) AS tup_fetched, \
	pg_stat_get_db_tuples_inserted(d.oid) AS tup_inserted, \
	pg_stat_get_db_tuples_updated(d.oid) AS tup_updated, \
	pg_stat_get_db_tuples_deleted(d.oid) AS tup_deleted, \
	pg_stat_get_db_conflict_tablespace(d.oid) AS confl_tablespace, \
	pg_stat_get_db_conflict_lock(d.oid) AS confl_lock, \
	pg_stat_get_db_conflict_snapshot(d.oid) AS confl_snapshot, \
	pg_stat_get_db_conflict_bufferpin(d.oid) AS confl_bufferpin, \
	pg_stat_get_db_conflict_startup_deadlock(d.oid) AS confl_deadlock, \
	pg_stat_get_db_temp_files(d.oid) AS temp_files, \
	pg_stat_get_db_temp_bytes(d.oid) AS temp_bytes, \
	pg_stat_get_db_deadlocks(d.oid) AS deadlocks, \
	pg_stat_get_db_blk_read_time(d.oid) AS blk_read_time, \
	pg_stat_get_db_blk_write_time(d.oid) AS blk_write_time \
FROM \
	pg_database d \
WHERE datallowconn \
  AND datname <> ALL (('{' || $1 || '}')::text[]) \
ORDER BY dbid"
#elif PG_VERSION_NUM >= 90100
#define SQL_SELECT_DATABASE "\
SELECT \
	d.oid AS dbid, \
	d.datname, \
	pg_database_size(d.oid), \
	CASE WHEN pg_is_in_recovery() THEN 0 ELSE age(d.datfrozenxid) END, \
	pg_stat_get_db_xact_commit(d.oid) AS xact_commit, \
	pg_stat_get_db_xact_rollback(d.oid) AS xact_rollback, \
	pg_stat_get_db_blocks_fetched(d.oid) - pg_stat_get_db_blocks_hit(d.oid) AS blks_read, \
	pg_stat_get_db_blocks_hit(d.oid) AS blks_hit, \
	pg_stat_get_db_tuples_returned(d.oid) AS tup_returned, \
	pg_stat_get_db_tuples_fetched(d.oid) AS tup_fetched, \
	pg_stat_get_db_tuples_inserted(d.oid) AS tup_inserted, \
	pg_stat_get_db_tuples_updated(d.oid) AS tup_updated, \
	pg_stat_get_db_tuples_deleted(d.oid) AS tup_deleted, \
	pg_stat_get_db_conflict_tablespace(d.oid) AS confl_tablespace, \
	pg_stat_get_db_conflict_lock(d.oid) AS confl_lock, \
	pg_stat_get_db_conflict_snapshot(d.oid) AS confl_snapshot, \
	pg_stat_get_db_conflict_bufferpin(d.oid) AS confl_bufferpin, \
	pg_stat_get_db_conflict_startup_deadlock(d.oid) AS confl_deadlock, \
	NULL AS temp_files, \
	NULL AS temp_bytes, \
	NULL AS deadlocks, \
	NULL AS blk_read_time, \
	NULL AS blk_write_time \
FROM \
	pg_database d \
WHERE datallowconn \
  AND datname <> ALL (('{' || $1 || '}')::text[]) \
ORDER BY dbid"
#elif PG_VERSION_NUM >= 90000
#define SQL_SELECT_DATABASE "\
SELECT \
	d.oid AS dbid, \
	d.datname, \
	pg_database_size(d.oid), \
	CASE WHEN pg_is_in_recovery() THEN 0 ELSE age(d.datfrozenxid) END, \
	pg_stat_get_db_xact_commit(d.oid) AS xact_commit, \
	pg_stat_get_db_xact_rollback(d.oid) AS xact_rollback, \
	pg_stat_get_db_blocks_fetched(d.oid) - pg_stat_get_db_blocks_hit(d.oid) AS blks_read, \
	pg_stat_get_db_blocks_hit(d.oid) AS blks_hit, \
	pg_stat_get_db_tuples_returned(d.oid) AS tup_returned, \
	pg_stat_get_db_tuples_fetched(d.oid) AS tup_fetched, \
	pg_stat_get_db_tuples_inserted(d.oid) AS tup_inserted, \
	pg_stat_get_db_tuples_updated(d.oid) AS tup_updated, \
	pg_stat_get_db_tuples_deleted(d.oid) AS tup_deleted, \
	NULL AS confl_tablespace, \
	NULL AS confl_lock, \
	NULL AS confl_snapshot, \
	NULL AS confl_bufferpin, \
	NULL AS confl_deadlock, \
	NULL AS temp_files, \
	NULL AS temp_bytes, \
	NULL AS deadlocks, \
	NULL AS blk_read_time, \
	NULL AS blk_write_time \
FROM \
	pg_database d \
WHERE datallowconn \
  AND datname <> ALL (('{' || $1 || '}')::text[]) \
ORDER BY dbid"
#else
#define SQL_SELECT_DATABASE "\
SELECT \
	d.oid AS dbid, \
	d.datname, \
	pg_database_size(d.oid), \
	age(d.datfrozenxid), \
	pg_stat_get_db_xact_commit(d.oid) AS xact_commit, \
	pg_stat_get_db_xact_rollback(d.oid) AS xact_rollback, \
	pg_stat_get_db_blocks_fetched(d.oid) - pg_stat_get_db_blocks_hit(d.oid) AS blks_read, \
	pg_stat_get_db_blocks_hit(d.oid) AS blks_hit, \
	pg_stat_get_db_tuples_returned(d.oid) AS tup_returned, \
	pg_stat_get_db_tuples_fetched(d.oid) AS tup_fetched, \
	pg_stat_get_db_tuples_inserted(d.oid) AS tup_inserted, \
	pg_stat_get_db_tuples_updated(d.oid) AS tup_updated, \
	pg_stat_get_db_tuples_deleted(d.oid) AS tup_deleted, \
	NULL AS confl_tablespace, \
	NULL AS confl_lock, \
	NULL AS confl_snapshot, \
	NULL AS confl_bufferpin, \
	NULL AS confl_deadlock, \
	NULL AS temp_files, \
	NULL AS temp_bytes, \
	NULL AS deadlocks, \
	NULL AS blk_read_time, \
	NULL AS blk_write_time \
FROM \
	pg_database d \
WHERE datallowconn \
  AND datname <> ALL (('{' || $1 || '}')::text[]) \
ORDER BY dbid"
#endif

/* activity */
#define SQL_SELECT_ACTIVITY				"SELECT * FROM statsinfo.activity()"

/* long transaction */
#define SQL_SELECT_LONG_TRANSACTION		"SELECT * FROM statsinfo.long_xact()"

/* tablespace */
#define SQL_SELECT_TABLESPACE			"SELECT * FROM statsinfo.tablespaces"

/* setting */
#define SQL_SELECT_SETTING "\
SELECT \
	name, \
	setting, \
	unit, \
	source \
FROM \
	pg_settings \
WHERE \
	source NOT IN ('client', 'default', 'session') \
AND \
	setting <> boot_val"

/* role */
#define SQL_SELECT_ROLE "\
SELECT \
	oid, \
	rolname \
FROM \
	pg_roles"

/* statement */
#if PG_VERSION_NUM >= 90400
#define SQL_SELECT_STATEMENT "\
SELECT \
	s.dbid, \
	s.userid, \
	s.queryid, \
	s.query, \
	s.calls, \
	s.total_time / 1000, \
	s.rows, \
	s.shared_blks_hit, \
	s.shared_blks_read, \
	s.shared_blks_dirtied, \
	s.shared_blks_written, \
	s.local_blks_hit, \
	s.local_blks_read, \
	s.local_blks_dirtied, \
	s.local_blks_written, \
	s.temp_blks_read, \
	s.temp_blks_written, \
	s.blk_read_time, \
	s.blk_write_time \
FROM \
	pg_stat_statements s \
	LEFT JOIN pg_roles r ON r.oid = s.userid \
WHERE \
	r.rolname <> ALL (('{' || $1 || '}')::text[]) \
ORDER BY \
	s.total_time DESC LIMIT $2"
#elif PG_VERSION_NUM >= 90200
#define SQL_SELECT_STATEMENT "\
SELECT \
	s.dbid, \
	s.userid, \
	%s, \
	s.query, \
	s.calls, \
	s.total_time / 1000, \
	s.rows, \
	s.shared_blks_hit, \
	s.shared_blks_read, \
	s.shared_blks_dirtied, \
	s.shared_blks_written, \
	s.local_blks_hit, \
	s.local_blks_read, \
	s.local_blks_dirtied, \
	s.local_blks_written, \
	s.temp_blks_read, \
	s.temp_blks_written, \
	s.blk_read_time, \
	s.blk_write_time \
FROM \
	pg_stat_statements s \
	LEFT JOIN pg_roles r ON r.oid = s.userid \
WHERE \
	r.rolname <> ALL (('{' || $1 || '}')::text[]) \
ORDER BY \
	s.total_time DESC LIMIT $2"
#elif PG_VERSION_NUM >= 90000
#define SQL_SELECT_STATEMENT "\
SELECT \
	s.dbid, \
	s.userid, \
	%s, \
	s.query, \
	s.calls, \
	s.total_time, \
	s.rows, \
	s.shared_blks_hit, \
	s.shared_blks_read, \
	NULL AS shared_blks_dirtied, \
	s.shared_blks_written, \
	s.local_blks_hit, \
	s.local_blks_read, \
	NULL AS local_blks_dirtied, \
	s.local_blks_written, \
	s.temp_blks_read, \
	s.temp_blks_written, \
	NULL AS blk_read_time, \
	NULL AS blk_write_time \
FROM \
	pg_stat_statements s \
	LEFT JOIN pg_roles r ON r.oid = s.userid \
WHERE \
	r.rolname <> ALL (('{' || $1 || '}')::text[]) \
ORDER BY \
	s.total_time DESC LIMIT $2"
#else
#define SQL_SELECT_STATEMENT "\
SELECT \
	s.dbid, \
	s.userid, \
	%s, \
	s.query, \
	s.calls, \
	s.total_time, \
	s.rows, \
	NULL AS shared_blks_hit, \
	NULL AS shared_blks_read, \
	NULL AS shared_blks_dirtied, \
	NULL AS shared_blks_written, \
	NULL AS local_blks_hit, \
	NULL AS local_blks_read, \
	NULL AS local_blks_dirtied, \
	NULL AS local_blks_written, \
	NULL AS temp_blks_read, \
	NULL AS temp_blks_written, \
	NULL AS blk_read_time, \
	NULL AS blk_write_time \
FROM \
	pg_stat_statements s \
	LEFT JOIN pg_roles r ON r.oid = s.userid \
WHERE \
	r.rolname <> ALL (('{' || $1 || '}')::text[]) \
ORDER BY \
	s.total_time DESC LIMIT $2"
#endif

/* plan */
#if PG_VERSION_NUM >= 90400
#define SQL_SELECT_PLAN_QUERYID		"p.queryid_stat_statements"
#else
#define SQL_SELECT_PLAN_QUERYID		"p.queryid"
#endif

#define SQL_SELECT_PLAN "\
SELECT \
	p.dbid, \
	p.userid, \
	" SQL_SELECT_PLAN_QUERYID ", \
	p.planid, \
	p.plan, \
	p.calls, \
	p.total_time / 1000, \
	p.rows, \
	p.shared_blks_hit, \
	p.shared_blks_read, \
	p.shared_blks_dirtied, \
	p.shared_blks_written, \
	p.local_blks_hit, \
	p.local_blks_read, \
	p.local_blks_dirtied, \
	p.local_blks_written, \
	p.temp_blks_read, \
	p.temp_blks_written, \
	p.blk_read_time, \
	p.blk_write_time, \
	p.first_call, \
	p.last_call \
FROM \
	pg_store_plans p \
	LEFT JOIN pg_roles r ON r.oid = p.userid \
WHERE \
	r.rolname <> ALL (('{' || $1 || '}')::text[]) \
ORDER BY \
	p.total_time DESC LIMIT $2"

/* lock */
#if PG_VERSION_NUM >= 90000
#define SQL_SELECT_LOCK_APPNAME				"sa.application_name"
#else
#define SQL_SELECT_LOCK_APPNAME				"'(N/A)'"
#endif
#if PG_VERSION_NUM >= 90100
#define SQL_SELECT_LOCK_CLIENT_HOSTNAME		"sa.client_hostname"
#else
#define SQL_SELECT_LOCK_CLIENT_HOSTNAME		"'(N/A)'"
#endif

#if PG_VERSION_NUM >= 90600
#define SQL_SELECT_LOCK "\
SELECT \
	db.datname, \
	ns.nspname, \
	t.relation, \
	sa.application_name, \
	sa.client_addr, \
	sa.client_hostname, \
	sa.client_port, \
	t.blockee_pid, \
	t.blocker_pid, \
	px.gid AS blocker_gid, \
	(statement_timestamp() - sa.query_start)::interval(0), \
	sa.query, \
	CASE \
		WHEN px.gid IS NOT NULL THEN '(xact is detached from session)' \
		WHEN lx.queries IS NULL THEN '(library might not have been loaded)' \
		ELSE lx.queries \
	END \
FROM \
	(SELECT DISTINCT \
		pid AS blockee_pid, \
		unnest(pg_blocking_pids(pid)) AS blocker_pid, \
		transactionid, \
		relation \
	 FROM \
		pg_locks \
	 WHERE \
		granted = false \
	) t \
	LEFT JOIN pg_prepared_xacts px ON px.transaction = t.transactionid \
	LEFT JOIN pg_stat_activity sa ON sa.pid = t.blockee_pid \
	LEFT JOIN statsinfo.last_xact_activity() lx ON lx.pid = t.blocker_pid \
	LEFT JOIN pg_database db ON db.oid = sa.datid \
	LEFT JOIN pg_class c ON c.oid = t.relation \
	LEFT JOIN pg_namespace ns ON ns.oid = c.relnamespace \
WHERE \
	sa.query_start < statement_timestamp() - current_setting('" GUC_PREFIX ".long_lock_threshold')::interval"
#else
#define SQL_SELECT_LOCK "\
SELECT \
	db.datname, \
	nb.nspname, \
	lb.relation, \
	" SQL_SELECT_LOCK_APPNAME ", \
	sa.client_addr, \
	" SQL_SELECT_LOCK_CLIENT_HOSTNAME ", \
	sa.client_port, \
	lb.pid AS blockee_pid, \
	la.pid AS blocker_pid, \
	la.gid AS blocker_gid, \
	(statement_timestamp() - sb.query_start)::interval(0), \
	sb." PG_STAT_ACTIVITY_ATTNAME_QUERY ", \
	CASE \
		WHEN la.gid IS NOT NULL THEN '(xact is detached from session)' \
		WHEN la.queries IS NULL THEN '(library might not have been loaded)' \
		ELSE la.queries \
	END \
FROM \
	(SELECT DISTINCT l0.pid, l0.relation, transactionid, la.gid, lx.queries \
	 FROM pg_locks l0 \
		LEFT JOIN \
			(SELECT l1.virtualtransaction, pp.gid \
			 FROM pg_prepared_xacts pp \
				LEFT JOIN pg_locks l1 ON l1.transactionid = pp.transaction) la \
			ON l0.virtualtransaction = la.virtualtransaction \
		LEFT JOIN statsinfo.last_xact_activity() lx ON l0.pid = lx.pid \
	 WHERE l0.granted = true AND \
		(la.gid IS NULL OR l0.relation IS NOT NULL)) la \
	 LEFT JOIN pg_stat_activity sa ON la.pid = sa." PG_STAT_ACTIVITY_ATTNAME_PID ", \
	(SELECT DISTINCT pid, relation, transactionid \
	 FROM pg_locks \
	 WHERE granted = false) lb \
	 LEFT JOIN pg_stat_activity sb ON lb.pid = sb." PG_STAT_ACTIVITY_ATTNAME_PID " \
	 LEFT JOIN pg_database db ON sb.datid = db.oid \
	 LEFT JOIN pg_class cb ON lb.relation = cb.oid \
	 LEFT JOIN pg_namespace nb ON cb.relnamespace = nb.oid \
WHERE \
	(la.transactionid = lb.transactionid OR la.relation = lb.relation) AND \
	sb.query_start < statement_timestamp() - current_setting('" GUC_PREFIX ".long_lock_threshold')::interval"
#endif

/* replication */
#if PG_VERSION_NUM >= 90400
#define SQL_SELECT_REPLICATION_BACKEND_XMIN		"backend_xmin"
#else
#define SQL_SELECT_REPLICATION_BACKEND_XMIN		"NULL"
#endif

#define SQL_SELECT_REPLICATION "\
SELECT \
	" PG_STAT_REPLICATION_ATTNAME_PID ", \
	usesysid, \
	usename, \
	application_name, \
	client_addr, \
	client_hostname, \
	client_port, \
	backend_start, \
	" SQL_SELECT_REPLICATION_BACKEND_XMIN ", \
	state, \
	CASE WHEN pg_is_in_recovery() THEN \
		pg_last_xlog_receive_location() || ' (N/A)' ELSE \
		pg_current_xlog_location() || ' (' || pg_xlogfile_name(pg_current_xlog_location()) || ')' END, \
	CASE WHEN pg_is_in_recovery() THEN \
		sent_location || ' (N/A)' ELSE \
		sent_location || ' (' || pg_xlogfile_name(sent_location) || ')' END, \
	CASE WHEN pg_is_in_recovery() THEN \
		write_location || ' (N/A)' ELSE \
		write_location || ' (' || pg_xlogfile_name(write_location) || ')' END, \
	CASE WHEN pg_is_in_recovery() THEN \
		flush_location || ' (N/A)' ELSE \
		flush_location || ' (' || pg_xlogfile_name(flush_location) || ')' END, \
	CASE WHEN pg_is_in_recovery() THEN \
		replay_location || ' (N/A)' ELSE \
		replay_location || ' (' || pg_xlogfile_name(replay_location) || ')' END, \
	sync_priority, \
	sync_state \
FROM \
	pg_stat_replication"

/* xlog */
#if PG_VERSION_NUM >= 90000
#define SQL_SELECT_XLOG "\
SELECT \
	pg_current_xlog_location(), \
	pg_xlogfile_name(pg_current_xlog_location()) \
WHERE \
	NOT pg_is_in_recovery()"
#else
#define SQL_SELECT_XLOG "\
SELECT \
	pg_current_xlog_location(), \
	pg_xlogfile_name(pg_current_xlog_location())"
#endif

/* archive */
#define SQL_SELECT_ARCHIVE "\
SELECT * FROM pg_stat_archiver"

/* cpu */
#define SQL_SELECT_CPU "\
SELECT * FROM statsinfo.cpustats($1)"

/* device */
#define SQL_SELECT_DEVICE "\
SELECT * FROM statsinfo.devicestats()"

/* loadavg */
#define SQL_SELECT_LOADAVG "\
SELECT * FROM statsinfo.loadavg()"

/* memory */
#define SQL_SELECT_MEMORY "\
SELECT * FROM statsinfo.memory()"

/* profile */
#define SQL_SELECT_PROFILE	"SELECT * FROM statsinfo.profile()"

/* repository size */
#define SQL_SELECT_REPOSIZE "\
SELECT \
	sum(pg_relation_size(oid)) \
FROM \
	pg_class \
WHERE \
	relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'statsrepo')"

/*----------------------------------------------------------------------------
 * snapshots per database
 *----------------------------------------------------------------------------
 */

/* schema */
#define SQL_SELECT_SCHEMA "\
SELECT \
	oid AS nspid, \
	nspname \
FROM \
	pg_namespace \
WHERE \
	nspname <> ALL (('{' || $1 || '}')::text[])"

/* table */
#if PG_VERSION_NUM >= 90400
#define SQL_SELECT_TABLE_N_MOD_SINCE_ANALYZE	"pg_stat_get_mod_since_analyze(c.oid)"
#else
#define SQL_SELECT_TABLE_N_MOD_SINCE_ANALYZE	"NULL"
#endif

#define SQL_SELECT_TABLE "\
SELECT \
	c.oid AS relid, \
	c.relnamespace, \
	c.reltablespace, \
	c.relname, \
	c.reltoastrelid, \
	x.indexrelid AS reltoastidxid, \
	c.relkind, \
	c.relpages, \
	c.reltuples, \
	c.reloptions, \
	pg_relation_size(c.oid), \
	pg_stat_get_numscans(c.oid) AS seq_scan, \
	pg_stat_get_tuples_returned(c.oid) AS seq_tup_read, \
	sum(pg_stat_get_numscans(i.indexrelid))::bigint AS idx_scan, \
	sum(pg_stat_get_tuples_fetched(i.indexrelid))::bigint + \
		pg_stat_get_tuples_fetched(c.oid) AS idx_tup_fetch, \
	pg_stat_get_tuples_inserted(c.oid) AS n_tup_ins, \
	pg_stat_get_tuples_updated(c.oid) AS n_tup_upd, \
	pg_stat_get_tuples_deleted(c.oid) AS n_tup_del, \
	pg_stat_get_tuples_hot_updated(c.oid) AS n_tup_hot_upd, \
	pg_stat_get_live_tuples(c.oid) AS n_live_tup, \
	pg_stat_get_dead_tuples(c.oid) AS n_dead_tup, \
	" SQL_SELECT_TABLE_N_MOD_SINCE_ANALYZE " AS n_mod_since_analyze, \
	pg_stat_get_blocks_fetched(c.oid) - \
		pg_stat_get_blocks_hit(c.oid) AS heap_blks_read, \
	pg_stat_get_blocks_hit(c.oid) AS heap_blks_hit, \
	sum(pg_stat_get_blocks_fetched(i.indexrelid) - \
		pg_stat_get_blocks_hit(i.indexrelid))::bigint AS idx_blks_read, \
	sum(pg_stat_get_blocks_hit(i.indexrelid))::bigint AS idx_blks_hit, \
	pg_stat_get_blocks_fetched(t.oid) - \
		pg_stat_get_blocks_hit(t.oid) AS toast_blks_read, \
	pg_stat_get_blocks_hit(t.oid) AS toast_blks_hit, \
	pg_stat_get_blocks_fetched(x.indexrelid) - \
		pg_stat_get_blocks_hit(x.indexrelid) AS tidx_blks_read, \
	pg_stat_get_blocks_hit(x.indexrelid) AS tidx_blks_hit, \
	pg_stat_get_last_vacuum_time(c.oid) as last_vacuum, \
	pg_stat_get_last_autovacuum_time(c.oid) as last_autovacuum, \
	pg_stat_get_last_analyze_time(c.oid) as last_analyze, \
	pg_stat_get_last_autoanalyze_time(c.oid) as last_autoanalyze \
FROM \
	pg_class c LEFT JOIN \
	pg_index i ON c.oid = i.indrelid LEFT JOIN \
	pg_class t ON c.reltoastrelid = t.oid LEFT JOIN \
	pg_index x ON c.reltoastrelid = x.indrelid LEFT JOIN \
	pg_namespace n ON c.relnamespace = n.oid \
WHERE \
	c.relkind IN ('r', 't') AND \
	n.nspname <> ALL (('{' || $1 || '}')::text[]) \
GROUP BY \
	c.oid, \
	c.relnamespace, \
	c.reltablespace, \
	c.relname, \
	c.reltoastrelid, \
	c.relkind, \
	c.relpages, \
	c.reltuples, \
	c.reloptions, \
	t.oid, \
	x.indexrelid"

/* column */
#if PG_VERSION_NUM >= 90000
#define SQL_SELECT_COLUMN_WHERE		"AND NOT s.stainherit "
#else
#define SQL_SELECT_COLUMN_WHERE
#endif

#define SQL_SELECT_COLUMN "\
SELECT \
	a.attrelid, \
	a.attnum, \
	a.attname, \
	format_type(atttypid, atttypmod) AS type, \
	a.attstattarget, \
	a.attstorage, \
	a.attnotnull, \
	a.attisdropped, \
	s.stawidth as avg_width, \
	s.stadistinct as n_distinct, \
	CASE \
		WHEN s.stakind1 = 3 THEN s.stanumbers1[1] \
		WHEN s.stakind2 = 3 THEN s.stanumbers2[1] \
		WHEN s.stakind3 = 3 THEN s.stanumbers3[1] \
		WHEN s.stakind4 = 3 THEN s.stanumbers4[1] \
		ELSE NULL \
	END AS correlation \
FROM \
	pg_attribute a \
	LEFT JOIN pg_class c ON \
		a.attrelid = c.oid \
	LEFT JOIN pg_statistic s ON \
		a.attnum = s.staattnum \
	AND \
		a.attrelid = s.starelid " SQL_SELECT_COLUMN_WHERE "\
	LEFT JOIN pg_namespace n ON \
		c.relnamespace = n.oid \
WHERE \
	a.attnum > 0 \
AND \
	c.relkind IN ('r', 't') \
AND \
	n.nspname <> ALL (('{' || $1 || '}')::text[])"

/* index */
#define SQL_SELECT_INDEX "\
SELECT \
    i.oid AS indexrelid, \
    c.oid AS relid, \
    i.reltablespace, \
    i.relname AS indexrelname, \
	i.relam, \
    i.relpages, \
    i.reltuples, \
    i.reloptions, \
    x.indisunique, \
    x.indisprimary, \
    x.indisclustered, \
    x.indisvalid, \
	x.indkey, \
    pg_get_indexdef(i.oid), \
    pg_relation_size(i.oid), \
    pg_stat_get_numscans(i.oid) AS idx_scan, \
    pg_stat_get_tuples_returned(i.oid) AS idx_tup_read, \
    pg_stat_get_tuples_fetched(i.oid) AS idx_tup_fetch, \
    pg_stat_get_blocks_fetched(i.oid) - \
        pg_stat_get_blocks_hit(i.oid) AS idx_blks_read, \
    pg_stat_get_blocks_hit(i.oid) AS idx_blks_hit \
FROM \
    pg_class c JOIN \
    pg_index x ON c.oid = x.indrelid JOIN \
    pg_class i ON i.oid = x.indexrelid JOIN \
    pg_namespace n ON n.oid = c.relnamespace \
WHERE \
	c.relkind IN ('r', 't') AND \
	n.nspname <> ALL (('{' || $1 || '}')::text[])"

/* inherits */
#define SQL_SELECT_INHERITS "\
SELECT \
	i.inhrelid, \
	i.inhparent, \
	i.inhseqno \
FROM \
	pg_inherits i JOIN \
	pg_class c ON i.inhrelid = c.oid JOIN \
	pg_namespace n ON c.relnamespace = n.oid \
WHERE \
	n.nspname <> ALL (('{' || $1 || '}')::text[])"

/* function */
#define SQL_SELECT_FUNCTION "\
SELECT \
	s.funcid, \
	n.oid AS nspid, \
	s.funcname, \
	pg_get_function_arguments(funcid) AS argtypes, \
	s.calls, \
	s.total_time, \
	s.self_time \
FROM \
	pg_stat_user_functions s JOIN \
	pg_namespace n ON s.schemaname = n.nspname \
WHERE \
	n.nspname <> ALL (('{' || $1 || '}')::text[])"

#endif
