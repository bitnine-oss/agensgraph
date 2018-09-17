/*
 * agens - the AgensGraph interactive terminal
 *
 * Copyright (c) 2018 by Bitnine Global, Inc.
 *
 * src/bin/psql/cypher_help.h
 */

#ifndef CYPHER_HELP_H
#define CYPHER_HELP_H

#include "pqexpbuffer.h"
#include "sql_help.h"

extern const struct _helpStruct CYPHER_HELP[];

#define CYPHER_HELP_COUNT	20		/* number of help items */
#define CYPHER_MAX_CMD_LEN	21		/* largest strlen(cmd) */

#endif /* CYPHER_HELP_H */
