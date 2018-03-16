/*-------------------------------------------------------------------------
 *
 * pg_statsinfo.h
 *
 * Copyright (c) 2009-2018, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_STATSINFO_H
#define PG_STATSINFO_H

#include "postgres_fe.h"
#include "pgut/pgut.h"
#include "pgut/pgut-fe.h"
#include "pgut/pgut-list.h"

/* report ID */
#define REPORTID_SUMMARY				"Summary"
#define REPORTID_DATABASE_STATISTICS	"DatabaseStatistics"
#define REPORTID_INSTANCE_ACTIVITY		"InstanceActivity"
#define REPORTID_OS_RESOURCE_USAGE		"OSResourceUsage"
#define REPORTID_DISK_USAGE				"DiskUsage"
#define REPORTID_LONG_TRANSACTIONS		"LongTransactions"
#define REPORTID_NOTABLE_TABLES			"NotableTables"
#define REPORTID_CHECKPOINT_ACTIVITY	"CheckpointActivity"
#define REPORTID_AUTOVACUUM_ACTIVITY	"AutovacuumActivity"
#define REPORTID_QUERY_ACTIVITY			"QueryActivity"
#define REPORTID_LOCK_CONFLICTS			"LockConflicts"
#define REPORTID_REPLICATION_ACTIVITY	"ReplicationActivity"
#define REPORTID_SETTING_PARAMETERS		"SettingParameters"
#define REPORTID_SCHEMA_INFORMATION		"SchemaInformation"
#define REPORTID_ALERT					"Alert"
#define REPORTID_PROFILES				"Profiles"
#define REPORTID_ALL					"All"

/* report.c */
extern void do_report(PGconn *conn, const char *reportid, const char *instid,
				const char *beginid, const char *endid,  time_t begindate,
				time_t enddate, const char *filename);

/* snapshot.c */
extern void do_list(PGconn *conn, const char *instid);
extern void do_size(PGconn *conn);
extern void do_snapshot(PGconn *conn, const char *comment);
extern void do_delete(PGconn *conn, const char *targetid);

/* control.c */
extern void do_start(PGconn *conn);
extern void do_stop(PGconn *conn);

#endif   /* PG_STATSINFO_H */
