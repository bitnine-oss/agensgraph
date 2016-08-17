/*-------------------------------------------------------------------------
 *
 * common.h
 *
 * Copyright (c) 2009-2016, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_STATSINFO_COMMON_H
#define PG_STATSINFO_COMMON_H

#include "catalog/pg_control.h"

#ifndef WIN32
#include "linux/version.h"
#endif

#define LINUX_VERSION_AT_LEAST(major, minor, patch) \
	(LINUX_VERSION_CODE >= KERNEL_VERSION(major, minor, patch))

#define GLIBC_VERSION_AT_LEAST(major, minor) \
	(__GLIBC__ > major || (__GLIBC__ == major && __GLIBC_MINOR__ >= minor))

#ifndef HAVE_SYNC_FILE_RANGE
#if (LINUX_VERSION_AT_LEAST(2,6,17) && GLIBC_VERSION_AT_LEAST(2,6))
#define HAVE_SYNC_FILE_RANGE
#endif
#endif

/* Error level */
#define ALERT		(PANIC + 1)
#define DISABLE		(PANIC + 2)

/* guc parameter name prefix for the program */
#define GUC_PREFIX	"pg_statsinfo"

/* log message prefix for the program */
#define LOG_PREFIX	"pg_statsinfo: "

/* manual snapshot log message */
#define LOGMSG_SNAPSHOT		LOG_PREFIX "snapshot requested"
/* manual maintenance log message */
#define LOGMSG_MAINTENANCE	LOG_PREFIX "maintenance requested"
#define LOGMSG_RESTART		LOG_PREFIX "restart requested"

/* maintenance mode flag */
#define MAINTENANCE_MODE_SNAPSHOT	(1 << 0)
#define MAINTENANCE_MODE_LOG		(1 << 1)
#define MAINTENANCE_MODE_REPOLOG	(1 << 2)

/* exit code for pg_statsinfod */
#define STATSINFO_EXIT_SUCCESS		0x00
#define STATSINFO_EXIT_FAILED		0xff

/* lock file */
#define STATSINFO_LOCK_FILE		"pg_statsinfo.pid"

/* CRC calculation */
#if PG_VERSION_NUM >= 90500
#include "port/pg_crc32c.h"
#define INIT_CRC32	INIT_CRC32C
#define COMP_CRC32	COMP_CRC32C
#define FIN_CRC32	FIN_CRC32C
#define EQ_CRC32	EQ_CRC32C
typedef pg_crc32c pg_crc32;
#endif

extern bool readControlFile(ControlFileData *ctrl, const char *pgdata);

#endif   /* PG_STATSINFO_COMMON_H */
