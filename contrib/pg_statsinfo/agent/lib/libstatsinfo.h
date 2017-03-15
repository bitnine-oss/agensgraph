/*
 * lib/libstatsinfo.h
 *
 * Copyright (c) 2009-2017, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#ifndef LIBSTATSINFO_H
#define LIBSTATSINFO_H

#include "postgres.h"

#ifdef WIN32
extern ssize_t readlink(const char *path, char *target, size_t size);
extern pid_t getppid(void);
#endif

extern pid_t	forkexec(const char *cmd, int *outStdin);
extern bool		get_diskspace(const char *path, int64 *total, int64 *avail);

#endif   /* LIBSTATSINFO_H */
