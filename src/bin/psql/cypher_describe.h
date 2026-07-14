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

/* \dG */
extern bool listGraphs(const char *pattern, bool verbose);

/* \dGl, \dGv, \dGe */
extern bool listLabels(const char *pattern, bool verbose, const char labkind);

#endif							/* AGENSGRAPH_CYPHER_DESCRIBE_H */
