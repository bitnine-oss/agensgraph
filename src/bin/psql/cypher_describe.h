/*
 * psql - the PostgreSQL interactive terminal
 *
 * Copyright (c) 2022 by Bitnine Global, Inc.
 *
 * src/bin/psql/cypher_describe.h
 */

#ifndef AGENSGRAPH_CYPHER_DESCRIBE_H
#define AGENSGRAPH_CYPHER_DESCRIBE_H

/* \dGi */
extern bool listGraphIndexes(const char *pattern, bool verbose);

#endif /* AGENSGRAPH_CYPHER_DESCRIBE_H */
