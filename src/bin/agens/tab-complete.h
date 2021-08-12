/*
 * agens - the AgensGraph interactive terminal
 *
 * Copyright (c) 2000-2018, PostgreSQL Global Development Group
 *
 * src/bin/agens/tab-complete.h
 */
#ifndef TAB_COMPLETE_H
#define TAB_COMPLETE_H

#include "pqexpbuffer.h"

extern PQExpBuffer tab_completion_query_buf;

extern void initialize_readline(void);

#endif							/* TAB_COMPLETE_H */
