/*
 * writer_sql.h
 *
 * Copyright (c) 2009-2017, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#ifndef WRITER_SQL_H
#define WRITER_SQL_H

/*
 * snapshot query
 */

#define SQL_NEW_SNAPSHOT "\
INSERT INTO statsrepo.snapshot(instid, time, comment) VALUES \
($1, $2, $3) RETURNING snapid"

#define SQL_INSERT_DATABASE "\
INSERT INTO statsrepo.database VALUES \
($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)"

#define SQL_INSERT_TABLESPACE "\
INSERT INTO statsrepo.tablespace VALUES \
($1, $2, $3, $4, $5, $6, $7, $8)"

#define SQL_INSERT_ACTIVITY "\
INSERT INTO statsrepo.activity VALUES \
($1, $2, $3, $4, $5, $6)"

#define SQL_INSERT_LONG_TRANSACTION "\
INSERT INTO statsrepo.xact VALUES \
($1, $2, $3, $4, $5, $6)"

#define SQL_INSERT_STATEMENT "\
INSERT INTO statsrepo.statement \
  SELECT (($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)::statsrepo.statement).* \
    FROM statsrepo.database d \
   WHERE d.snapid = $1 AND d.dbid = $2"

#define SQL_INSERT_PLAN "\
INSERT INTO statsrepo.plan \
  SELECT (($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)::statsrepo.plan).* \
    FROM statsrepo.database d \
   WHERE d.snapid = $1 AND d.dbid = $2"

#define SQL_INSERT_LOCK "\
INSERT INTO statsrepo.lock VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)"

#define SQL_INSERT_REPLICATION "\
INSERT INTO statsrepo.replication VALUES \
($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)"

#define SQL_INSERT_XLOG "\
INSERT INTO statsrepo.xlog VALUES ($1, $2, $3)"

#define SQL_INSERT_ARCHIVE "\
INSERT INTO statsrepo.archive VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"

#define SQL_INSERT_SETTING "\
INSERT INTO statsrepo.setting VALUES ($1, $2, $3, $4, $5)"

#define SQL_INSERT_ROLE "\
INSERT INTO statsrepo.role VALUES ($1, $2, $3)"

#define SQL_INSERT_CPU "\
INSERT INTO statsrepo.cpu VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"

#define SQL_INSERT_DEVICE "\
INSERT INTO statsrepo.device VALUES \
($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)"

#define SQL_INSERT_LOADAVG "\
INSERT INTO statsrepo.loadavg VALUES ($1, $2, $3, $4)"

#define SQL_INSERT_MEMORY "\
INSERT INTO statsrepo.memory VALUES ($1, $2, $3, $4, $5, $6)"

#define SQL_INSERT_PROFILE "\
INSERT INTO statsrepo.profile VALUES ($1, $2, $3, $4)"

#define SQL_INSERT_SCHEMA "\
INSERT INTO statsrepo.schema VALUES ($1, $2, $3, $4)"

#define SQL_INSERT_TABLE "\
INSERT INTO statsrepo.table VALUES \
($1, $2, $3, $4, statsrepo.get_snap_date($1), $5, $6, $7, $8, $9, $10, \
 $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, \
 $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, \
 $31, $32, $33, $34, $35, $36)"

#define SQL_INSERT_COLUMN "\
INSERT INTO statsrepo.column VALUES \
($1, $2, $3, $4, statsrepo.get_snap_date($1), $5, $6, $7, $8, $9, $10, $11, $12, $13)"

#define SQL_INSERT_INDEX "\
INSERT INTO statsrepo.index VALUES \
($1, $2, $3, $4, statsrepo.get_snap_date($1), $5, $6, $7, $8, $9, $10, \
 $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)"

#define SQL_INSERT_INHERITS "\
INSERT INTO statsrepo.inherits VALUES ($1, $2, $3, $4, $5)"

#define SQL_INSERT_FUNCTION "\
INSERT INTO statsrepo.function VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"

#define SQL_INSERT_ALERT "\
INSERT INTO statsrepo.alert_message VALUES ($1, $2)"

#define SQL_INSERT_LOG "\
INSERT INTO statsrepo.log VALUES \
($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)"

#define SQL_UPDATE_SNAPSHOT "\
UPDATE \
	statsrepo.snapshot \
SET \
	exec_time = age($2, $3), \
	snapshot_increase_size = ((SELECT sum(pg_relation_size(oid)) FROM pg_class \
								WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'statsrepo')) - $4) \
WHERE \
	snapid = $1"

#define SQL_CREATE_SNAPSHOT_PARTITION "\
SELECT statsrepo.create_snapshot_partition($1)"

#define SQL_CREATE_REPOLOG_PARTITION "\
SELECT statsrepo.create_repolog_partition($1)"

#endif
