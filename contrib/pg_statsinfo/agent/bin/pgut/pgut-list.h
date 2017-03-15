/*-------------------------------------------------------------------------
 *
 * pgut-list.h : copied from postgres/nodes/pg_list.h
 *
 * Copyright (c) 2009-2017, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 *
 *-------------------------------------------------------------------------
 */

#ifndef PGUT_LIST_H
#define PGUT_LIST_H

#include "nodes/nodes.h"

typedef struct ListCell ListCell;

typedef struct List
{
	NodeTag		type;			/* T_List, T_IntList, or T_OidList */
	int			length;
	ListCell   *head;
	ListCell   *tail;
} List;

struct ListCell
{
	union
	{
		void	   *ptr_value;
		int			int_value;
		Oid			oid_value;
	}			data;
	ListCell   *next;
};

/*
 * The *only* valid representation of an empty list is NIL; in other
 * words, a non-NIL list is guaranteed to have length >= 1 and
 * head/tail != NULL
 */
#define NIL						((List *) NULL)

/*
 * These routines are used frequently. However, we can't implement
 * them as macros, since we want to avoid double-evaluation of macro
 * arguments. Therefore, we implement them using static inline functions
 * if supported by the compiler, or as regular functions otherwise.
 */
#ifndef __GNUC__
extern ListCell *list_head(const List *l);
extern ListCell *list_tail(List *l);
extern int list_length(const List *l);
#else
static inline ListCell *
list_head(const List *l)
{
	return l ? l->head : NULL;
}

static inline ListCell *
list_tail(List *l)
{
	return l ? l->tail : NULL;
}

static inline int
list_length(const List *l)
{
	return l ? l->length : 0;
}
#endif   /* ! __GNUC__ */

/*
 * NB: There is an unfortunate legacy from a previous incarnation of
 * the List API: the macro lfirst() was used to mean "the data in this
 * cons cell". To avoid changing every usage of lfirst(), that meaning
 * has been kept. As a result, lfirst() takes a ListCell and returns
 * the data it contains; to get the data in the first cell of a
 * List, use linitial(). Worse, lsecond() is more closely related to
 * linitial() than lfirst(): given a List, lsecond() returns the data
 * in the second cons cell.
 */
#define lnext(lc)				((lc)->next)
#define lfirst(lc)				((lc)->data.ptr_value)
#define lfirst_int(lc)			((lc)->data.int_value)
#define lfirst_oid(lc)			((lc)->data.oid_value)

#define linitial(l)				lfirst(list_head(l))
#define linitial_int(l)			lfirst_int(list_head(l))
#define linitial_oid(l)			lfirst_oid(list_head(l))

#define lsecond(l)				lfirst(lnext(list_head(l)))
#define lsecond_int(l)			lfirst_int(lnext(list_head(l)))
#define lsecond_oid(l)			lfirst_oid(lnext(list_head(l)))

#define lthird(l)				lfirst(lnext(lnext(list_head(l))))
#define lthird_int(l)			lfirst_int(lnext(lnext(list_head(l))))
#define lthird_oid(l)			lfirst_oid(lnext(lnext(list_head(l))))

#define lfourth(l)				lfirst(lnext(lnext(lnext(list_head(l)))))
#define lfourth_int(l)			lfirst_int(lnext(lnext(lnext(list_head(l)))))
#define lfourth_oid(l)			lfirst_oid(lnext(lnext(lnext(list_head(l)))))

#define llast(l)				lfirst(list_tail(l))
#define llast_int(l)			lfirst_int(list_tail(l))
#define llast_oid(l)			lfirst_oid(list_tail(l))

/*
 * foreach -
 *	  a convenience macro which loops through the list
 */
#define foreach(cell, l)	\
	for ((cell) = list_head(l); (cell) != NULL; (cell) = lnext(cell))

/*
 * for_each_cell -
 *	  a convenience macro which loops through a list starting from a
 *	  specified cell
 */
#define for_each_cell(cell, initcell)	\
	for ((cell) = (initcell); (cell) != NULL; (cell) = lnext(cell))

/*
 * forboth -
 *	  a convenience macro for advancing through two linked lists
 *	  simultaneously. This macro loops through both lists at the same
 *	  time, stopping when either list runs out of elements. Depending
 *	  on the requirements of the call site, it may also be wise to
 *	  assert that the lengths of the two lists are equal.
 */
#define forboth(cell1, list1, cell2, list2)							\
	for ((cell1) = list_head(list1), (cell2) = list_head(list2);	\
		 (cell1) != NULL && (cell2) != NULL;						\
		 (cell1) = lnext(cell1), (cell2) = lnext(cell2))

/*
 * forthree -
 *	  the same for three lists
 */
#define forthree(cell1, list1, cell2, list2, cell3, list3)			\
	for ((cell1) = list_head(list1), (cell2) = list_head(list2), (cell3) = list_head(list3); \
		 (cell1) != NULL && (cell2) != NULL && (cell3) != NULL;		\
		 (cell1) = lnext(cell1), (cell2) = lnext(cell2), (cell3) = lnext(cell3))

extern List *lappend(List *list, void *datum);
extern ListCell *lappend_cell(List *list, ListCell *prev, void *datum);
extern List *lcons(void *datum, List *list);
extern List *list_concat(List *list1, List *list2);
extern List *list_truncate(List *list, int new_size);
extern void *list_nth(const List *list, int n);
extern bool list_member_ptr(const List *list, const void *datum);
extern List *list_delete_ptr(List *list, void *datum);
extern List *list_delete_first(List *list);
extern List *list_delete_cell(List *list, ListCell *cell, ListCell *prev);
extern void list_free(List *list);
extern void list_free_deep(List *list);
extern List *list_copy(const List *list);
extern List *list_copy_tail(const List *list, int nskip);

/*
 * pgut-list.c : Extended list functions
 */
extern void list_walk(List *list, void (*walker)());
extern void list_destroy(List *list, void (*walker)());

#endif /* PGUT_LIST_H */
