/*
 * agens - the AgensGraph interactive terminal
 *
 * Copyright (c) 2000-2018, PostgreSQL Global Development Group
 *
 * src/bin/agens/copy.h
 */
#ifndef COPY_H
#define COPY_H

#include "libpq-fe.h"


/* handler for \copy */
extern bool do_copy(const char *args);

/* lower level processors for copy in/out streams */

extern bool handleCopyOut(PGconn *conn, FILE *copystream,
			  PGresult **res);
extern bool handleCopyIn(PGconn *conn, FILE *copystream, bool isbinary,
			 PGresult **res);

#endif
