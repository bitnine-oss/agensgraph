/*
 * bin/pg_statsrepo_alert.sql
 *
 * Setup of an alert function.
 *
 * Copyright (c) 2009-2018, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

-- Adjust this setting to control where the objects get created.
SET search_path = public;

BEGIN;

SET LOCAL client_min_messages = WARNING;

-- table to save the alert settings
CREATE TABLE statsrepo.alert
(
	instid					bigint,
	rollback_tps			bigint	NOT NULL DEFAULT 100     CHECK (rollback_tps >= -1),
	commit_tps				bigint	NOT NULL DEFAULT 1000    CHECK (commit_tps >= -1),
	garbage_size			bigint	NOT NULL DEFAULT -1      CHECK (garbage_size >= -1),
	garbage_percent			integer	NOT NULL DEFAULT 30      CHECK (garbage_percent >= -1 AND garbage_percent <= 100),
	garbage_percent_table	integer	NOT NULL DEFAULT 30      CHECK (garbage_percent_table >= -1 AND garbage_percent_table <= 100),
	response_avg			bigint	NOT NULL DEFAULT 10      CHECK (response_avg >= -1),
	response_worst			bigint	NOT NULL DEFAULT 60      CHECK (response_worst >= -1),
	backend_max				integer	NOT NULL DEFAULT 100     CHECK (backend_max >= -1),
	fragment_percent		integer	NOT NULL DEFAULT 70      CHECK (fragment_percent >= -1 AND fragment_percent <= 100),
	disk_remain_percent		integer	NOT NULL DEFAULT 20      CHECK (disk_remain_percent >= -1 AND disk_remain_percent <= 100),
	loadavg_1min			real	NOT NULL DEFAULT 7.0     CHECK (loadavg_1min = -1 OR loadavg_1min >= 0),
	loadavg_5min			real	NOT NULL DEFAULT 6.0     CHECK (loadavg_5min = -1 OR loadavg_5min >= 0),
	loadavg_15min			real	NOT NULL DEFAULT 5.0     CHECK (loadavg_15min = -1 OR loadavg_15min >= 0),
	swap_size				integer NOT NULL DEFAULT 1000000 CHECK (swap_size >= -1),
	rep_flush_delay			integer	NOT NULL DEFAULT 100     CHECK (rep_flush_delay >= -1),
	rep_replay_delay		integer	NOT NULL DEFAULT 200     CHECK (rep_replay_delay >= -1),
	enable_alert			boolean	NOT NULL DEFAULT TRUE,
	PRIMARY KEY (instid),
	FOREIGN KEY (instid) REFERENCES statsrepo.instance (instid) ON DELETE CASCADE
);

-- add alert settings when adding a new instance
CREATE FUNCTION statsrepo.regist_alert() RETURNS TRIGGER AS
$$
DECLARE
BEGIN
	INSERT INTO statsrepo.alert VALUES (NEW.instid);
	RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- trigger registration for alert
CREATE TRIGGER regist_alert AFTER INSERT ON statsrepo.instance FOR EACH ROW
EXECUTE PROCEDURE statsrepo.regist_alert();

------------------------------------------------------------------------------
-- alert function
------------------------------------------------------------------------------

-- alert function for check the condition of transaction
CREATE FUNCTION statsrepo.alert_xact(
	statsrepo.snapshot,
	statsrepo.snapshot,
	statsrepo.alert
) RETURNS SETOF text AS
$$
DECLARE
	duration_in_sec  bigint;
	val_rollback     float8;
	val_commit       float8;
BEGIN
	-- calculate duration for the two shapshots in sec.
	duration_in_sec :=
		extract(epoch FROM $1.time::timestamp(0)) - extract(epoch FROM $2.time::timestamp(0));

	-- calculate the number of commits/rollbacks per sec.
	SELECT
		statsrepo.div((c.rollbacks - p.rollbacks), duration_in_sec),
		statsrepo.div((c.commits - p.commits), duration_in_sec)
	INTO val_rollback, val_commit
	FROM (SELECT sum(xact_rollback) AS rollbacks, sum(xact_commit) AS commits
			FROM statsrepo.database WHERE snapid = $1.snapid) AS c,
		 (SELECT sum(xact_rollback) AS rollbacks, sum(xact_commit) AS commits
			FROM statsrepo.database WHERE snapid = $2.snapid) AS p;

	-- alert if rollbacks/sec is higher than threshold.
	IF $3.rollback_tps >= 0 AND val_rollback > $3.rollback_tps THEN
		RETURN NEXT 'too many rollbacks in snapshots between ''' ||
			$2.time::timestamp(0) || ''' and ''' || $1.time::timestamp(0) ||
			''' --- ' || val_rollback || ' Rollbacks/sec (threshold = ' ||
			$3.rollback_tps || ' Rollbacks/sec)';
	END IF;

	-- alert if throughput(commit/sec) is higher than threshold.
	IF $3.commit_tps >= 0 AND val_commit > $3.commit_tps THEN
		RETURN NEXT 'too many transactions in snapshots between ''' ||
			$2.time::timestamp(0) || ''' and ''' || $1.time::timestamp(0) ||
			''' --- ' || val_commit || ' Transactions/sec (threshold = ' ||
			$3.commit_tps || ' Transactions/sec)';
	END IF;
END;
$$
LANGUAGE plpgsql;

-- alert function for check the condition of garbage space
CREATE FUNCTION statsrepo.alert_garbage(
	statsrepo.snapshot,
	statsrepo.alert
) RETURNS SETOF text AS
$$
DECLARE
	val_gb_size   float8;
	val_gb_pct    float8;
	val_gb_table  text;
BEGIN
	-- calculate the garbage size.
	SELECT
		statsrepo.div(sum(c.garbage_size), 1024 * 1024)
	INTO val_gb_size
	FROM
		(SELECT
			size * statsrepo.div(n_dead_tup, (n_live_tup + n_dead_tup)) AS garbage_size
		 FROM statsrepo.tables WHERE snapid = $1.snapid) AS c;

	-- calculate the garbage ratio.
	SELECT
		statsrepo.div((100 * sum(c.garbage_size)), sum(c.size))
	INTO val_gb_pct
	FROM
		(SELECT
			size * statsrepo.div(n_dead_tup, (n_live_tup + n_dead_tup)) AS garbage_size,
		 	size
		 FROM statsrepo.tables WHERE relpages >= 1000 AND snapid = $1.snapid) AS c;

	-- alert if garbage size is higher than threshold.
	IF $2.garbage_size >= 0 AND val_gb_size > $2.garbage_size THEN
		RETURN NEXT 'dead tuple size exceeds threshold in snapshot ''' ||
			$1.time::timestamp(0) || ''' --- ' || (val_gb_size) || ' MiB (threshold = ' ||
			$2.garbage_size || ' MiB)';
	END IF;

	-- alert if garbage ratio is higher than threshold.
	IF $2.garbage_percent >= 0 AND val_gb_pct > $2.garbage_percent THEN
		RETURN NEXT 'dead tuple ratio exceeds threshold in snapshot ''' ||
			$1.time::timestamp(0) || ''' --- ' || val_gb_pct || ' % (threshold = ' ||
			$2.garbage_percent || ' %)';
	END IF;

	-- alert if garbage ratio of each tables is higher than threshold.
	FOR val_gb_table, val_gb_pct IN
		SELECT
			database || '.' || schema || '.' || "table",
			100 * statsrepo.div(n_dead_tup, (n_live_tup + n_dead_tup))
		FROM statsrepo.tables WHERE relpages >= 1000 AND snapid = $1.snapid
	LOOP
		IF $2.garbage_percent_table >= 0 AND val_gb_pct > $2.garbage_percent_table THEN
			RETURN NEXT 'dead tuple ratio in ''' || val_gb_table ||
				''' exceeds threshold in snapshot ''' || $1.time::timestamp(0) ||
				''' --- ' || val_gb_pct || ' % (threshold = ' ||
				$2.garbage_percent_table || ' %)';
		END IF;
	END LOOP;
END;
$$
LANGUAGE plpgsql;

-- alert function for check the response time of query
CREATE FUNCTION statsrepo.alert_query(
	statsrepo.snapshot,
	statsrepo.snapshot,
	statsrepo.alert
) RETURNS SETOF text AS
$$
DECLARE
	val_res_avg  float8;
	val_res_max  float8;
BEGIN
	-- calculate the average and maximum of the query-response-time.
	SELECT
		avg((c.total_time - coalesce(p.total_time, 0)) / (c.calls - coalesce(p.calls, 0))),
		max((c.total_time - coalesce(p.total_time, 0)) / (c.calls - coalesce(p.calls, 0)))
	INTO val_res_avg, val_res_max
	FROM (SELECT dbid, userid, total_time, calls, query
			FROM statsrepo.statement WHERE snapid = $1.snapid) AS c
		 LEFT OUTER JOIN
		 (SELECT dbid, userid, total_time, calls, query
		 	FROM statsrepo.statement WHERE snapid = $2.snapid) AS p
		 ON c.dbid = p.dbid AND c.userid = p.userid AND c.query = p.query
	WHERE c.calls <> coalesce(p.calls, 0);

	-- alert if average of the query-response-time is higher than threshold.
	IF $3.response_avg >= 0 AND val_res_avg > $3.response_avg THEN
		RETURN NEXT 'Query average response time exceeds threshold in snapshots between ''' ||
			$2.time::timestamp(0) || ''' and ''' || $1.time::timestamp(0) ||
			''' --- ' || val_res_avg::numeric(10,2) || ' sec (threshold = ' ||
			$3.response_avg || ' sec)';
	END IF;

	-- alert if maximum of the query-response-time is higher than threshold.
	IF $3.response_worst >= 0 AND val_res_max > $3.response_worst THEN
		RETURN NEXT 'Query worst response time exceeds threshold in snapshots between ''' ||
			$2.time::timestamp(0) || ''' and ''' || $1.time::timestamp(0) ||
			''' --- ' || val_res_max::numeric(10,2) || ' sec (threshold = ' ||
			$3.response_worst || ' sec)';
	END IF;
END;
$$
LANGUAGE plpgsql;

-- alert function for check the condition of backend process
CREATE FUNCTION statsrepo.alert_activity(
	statsrepo.snapshot,
	statsrepo.alert
) RETURNS SETOF text AS
$$
DECLARE
	val_be_max  integer;
BEGIN
	-- alert if number of backend is higher than threshold.
	SELECT max_backends INTO val_be_max
	FROM statsrepo.activity WHERE snapid = $1.snapid;
	IF $2.backend_max >= 0 AND val_be_max > $2.backend_max THEN
		RETURN NEXT 'too many backends in snapshot ''' || $1.time::timestamp(0) ||
			''' --- ' || val_be_max || ' (threshold = ' || $2.backend_max || ')';
	END IF;
END;
$$
LANGUAGE plpgsql;

-- alert function for check the fragmentation of table
CREATE FUNCTION statsrepo.alert_fragment(
	statsrepo.snapshot,
	statsrepo.alert
) RETURNS SETOF text AS
$$
DECLARE
	val_fragment_table  text;
	val_fragment_pct    float8;
BEGIN
	-- alert if fragment ratio of the clustered index data table is higher than threshold.
	FOR val_fragment_table, val_fragment_pct IN
		SELECT
			i.database || '.' || i.schema || '.' || i.table,
			(100 * abs(c.correlation))::numeric(5,2)
		FROM
			statsrepo.indexes i,
			statsrepo.column c
		WHERE
			i.snapid = c.snapid
			AND i.tbl = c.tbl
			AND i.isclustered = true
			AND c.attnum = i.indkey[0]
			AND c.correlation IS NOT NULL
			AND c.snapid = $1.snapid
	LOOP
		IF $2.fragment_percent >= 0 AND val_fragment_pct < $2.fragment_percent THEN
			RETURN NEXT 'correlation of the clustered table fell below threshold in snapshot ''' ||
				$1.time::timestamp(0) || ''' --- ''' || val_fragment_table || ''', ' ||
				val_fragment_pct || ' % (threshold = ' || $2.fragment_percent || ' %)';
		END IF;
	END LOOP;
END;
$$
LANGUAGE plpgsql;

-- alert function for check the condition of OS resource
CREATE FUNCTION statsrepo.alert_resource(
	statsrepo.snapshot,
	statsrepo.alert
) RETURNS SETOF text AS
$$
DECLARE
	val_tablespace  text;
	val_disk_pct    float8;
	val_loadavg1    float4;
	val_loadavg5    float4;
	val_loadavg15   float4;
	val_swap_size   bigint;
BEGIN
	-- alert if free-disk-space ratio of each tablespaces is higher than threshold.
	FOR val_tablespace, val_disk_pct IN
		SELECT
			name,
			100 * statsrepo.div(avail, total)
		FROM statsrepo.tablespace WHERE snapid = $1.snapid
	LOOP
		IF $2.disk_remain_percent >= 0 AND val_disk_pct < $2.disk_remain_percent THEN
			RETURN NEXT 'free disk space ratio at ''' || val_tablespace ||
				''' fell below threshold in snapshot ''' || $1.time::timestamp(0) ||
				''' --- ' || val_disk_pct || ' % (threshold = ' || $2.disk_remain_percent || ' %)';
		END IF;
	END LOOP;

	-- alert if load average is higher than threshold.
	SELECT loadavg1, loadavg5, loadavg15 INTO val_loadavg1, val_loadavg5, val_loadavg15
	FROM statsrepo.loadavg WHERE snapid = $1.snapid;
	IF $2.loadavg_1min >= 0 AND val_loadavg1 > $2.loadavg_1min THEN
		RETURN NEXT 'load average 1min exceeds threshold in snapshot ''' || $1.time::timestamp(0) ||
			''' --- ' || val_loadavg1 || ' (threshold = ' || $2.loadavg_1min || ')';
	END IF;
	IF $2.loadavg_5min >= 0 AND val_loadavg5 > $2.loadavg_5min THEN
		RETURN NEXT 'load average 5min exceeds threshold in snapshot ''' || $1.time::timestamp(0) ||
			''' --- ' || val_loadavg5 || ' (threshold = ' || $2.loadavg_5min || ')';
	END IF;
	IF $2.loadavg_15min >= 0 AND val_loadavg15 > $2.loadavg_15min THEN
		RETURN NEXT 'load average 15min exceeds threshold in snapshot ''' || $1.time::timestamp(0) ||
			''' --- ' || val_loadavg15 || ' (threshold = ' || $2.loadavg_15min || ')';
	END IF;

	-- alert if memory swap size is higher than threshold.
	SELECT swap INTO val_swap_size FROM statsrepo.memory WHERE snapid = $1.snapid;
	IF $2.swap_size >= 0 AND val_swap_size > $2.swap_size THEN
		RETURN NEXT 'memory swap size exceeds threshold in snapshot ''' || $1.time::timestamp(0) ||
			''' --- ' || val_swap_size || ' KiB (threshold = ' || $2.swap_size || ' KiB)';
	END IF;
END;
$$
LANGUAGE plpgsql;

-- alert function for check the condition of replication
CREATE FUNCTION statsrepo.alert_replication(
	statsrepo.snapshot,
	statsrepo.alert
) RETURNS SETOF text AS
$$
DECLARE
	val_client        text;
	val_flush_delay   float8;
	val_replay_delay  float8;
BEGIN
	-- alert if replication-delay(flush or replay) is higher than threshold.
	FOR val_client, val_flush_delay, val_replay_delay IN
		SELECT
			host(r.client_addr) || ':' || r.client_port,
			statsrepo.div(
				statsrepo.xlog_location_diff(
					split_part(r.current_location, ' ', 1),
					split_part(r.flush_location, ' ', 1),
					i.xlog_file_size
				),
				1024 * 1024
			),
			statsrepo.div(
				statsrepo.xlog_location_diff(
					split_part(r.current_location, ' ', 1),
					split_part(r.replay_location, ' ', 1),
					i.xlog_file_size
				),
				1024 * 1024
			)
		FROM
			statsrepo.replication r,
			statsrepo.snapshot s,
			statsrepo.instance i
		WHERE
			r.snapid = s.snapid
			AND s.instid = i.instid
			AND r.snapid = $1.snapid
			AND r.flush_location IS NOT NULL
			AND r.replay_location IS NOT NULL
	LOOP
		IF $2.rep_flush_delay >= 0 AND val_flush_delay > $2.rep_flush_delay THEN
			RETURN NEXT 'WAL flush-delay in ''' || val_client ||
				''' exceeds threshold in snapshot ''' || $1.time::timestamp(0) ||
				''' --- ' || val_flush_delay || ' MiB (threshold = ' ||
				$2.rep_flush_delay || ' MiB)';
		END IF;
		IF $2.rep_replay_delay >= 0 AND val_replay_delay > $2.rep_replay_delay THEN
			RETURN NEXT 'replay-delay in ''' || val_client ||
				''' exceeds threshold in snapshot ''' || $1.time::timestamp(0) ||
				''' --- ' || val_replay_delay || ' MiB (threshold = ' ||
				$2.rep_replay_delay || ' MiB)';
		END IF;
	END LOOP;
END;
$$
LANGUAGE plpgsql;

-- alert function main
CREATE FUNCTION statsrepo.alert(bigint) RETURNS SETOF text AS
$$
DECLARE
	curr     statsrepo.snapshot; -- latest snapshot
	prev     statsrepo.snapshot; -- previous snapshot
	setting  statsrepo.alert;    -- alert settings
	message  text;
BEGIN
	-- exclusive control for don't run concurrently with the maintenance
	LOCK TABLE statsrepo.instance IN SHARE MODE;

	-- retrieve latest snapshot
	SELECT * INTO curr FROM statsrepo.snapshot WHERE snapid = $1;

	-- retrieve previous snapshot
	SELECT * INTO prev FROM statsrepo.snapshot
		WHERE snapid < curr.snapid AND instid = curr.instid ORDER BY snapid DESC LIMIT 1;
	IF NOT FOUND THEN
		RETURN; -- no previous snapshot
	END IF;

	-- retrieve threshold from current-settings
	SELECT * INTO setting FROM statsrepo.alert WHERE instid = curr.instid AND enable_alert = true;
	IF NOT FOUND THEN
		RETURN; -- alert is disabled
	END IF;

	-- check the frequency of occurrence of throughput and rollback
	FOR message IN SELECT * FROM statsrepo.alert_xact(curr, prev, setting)
	LOOP
		RETURN NEXT message;
	END LOOP;

	-- check the condition of the garbage space of tables
	FOR message IN SELECT * FROM statsrepo.alert_garbage(curr, setting)
	LOOP
		RETURN NEXT message;
	END LOOP;

	-- check the response time of the query
	FOR message IN SELECT * FROM statsrepo.alert_query(curr, prev, setting)
	LOOP
		RETURN NEXT message;
	END LOOP;

	-- check the condition of the backend processes
	FOR message IN SELECT * FROM statsrepo.alert_activity(curr, setting)
	LOOP
		RETURN NEXT message;
	END LOOP;

	-- check the fragmentation of tables
	FOR message IN SELECT * FROM statsrepo.alert_fragment(curr, setting)
	LOOP
		RETURN NEXT message;
	END LOOP;

	-- check the condition of OS resources
	FOR message IN SELECT * FROM statsrepo.alert_resource(curr, setting)
	LOOP
		RETURN NEXT message;
	END LOOP;

	-- check the condition of the replication
	FOR message IN SELECT * FROM statsrepo.alert_replication(curr, setting)
	LOOP
		RETURN NEXT message;
	END LOOP;
END;
$$
LANGUAGE plpgsql VOLATILE;

COMMIT;
