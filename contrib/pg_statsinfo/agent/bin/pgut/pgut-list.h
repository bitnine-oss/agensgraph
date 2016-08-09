/*-------------------------------------------------------------------------
 *
 * pgut-list.h
 *
 * Copyright (c) 2009-2016, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGUT_LIST_H
#define PGUT_LIST_H

#include "nodes/pg_list.h"

/*
 * pgut-list.c : Extended list functions
 */
extern void list_walk(List *list, void (*walker)());
extern void list_destroy(List *list, void (*walker)());

#endif /* PGUT_LIST_H */
