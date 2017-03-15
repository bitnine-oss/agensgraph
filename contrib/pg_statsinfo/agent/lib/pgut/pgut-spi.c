/*-------------------------------------------------------------------------
 *
 * pgut-spi.c
 *
 * Copyright (c) 2009-2017, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "pgut-spi.h"

#define EXEC_FAILED(ret, expected) \
	(((expected) > 0 && (ret) != (expected)) || (ret) < 0)

/* simple execute */
void
execute(int expected, const char *sql)
{
	int ret = SPI_execute(sql, false, 0);
	if EXEC_FAILED(ret, expected)
		elog(ERROR, "query failed: (sql=%s, code=%d, expected=%d)", sql, ret, expected);
}
