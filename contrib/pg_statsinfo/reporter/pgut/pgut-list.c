/*-------------------------------------------------------------------------
 *
 * pgut-list.c : copied from postgres/nodes/list.c
 *
 * Copyright (c) 2009-2018, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"
#include "pgut.h"
#include "pgut-list.h"

/*
 * Routines to simplify writing assertions about the type of a list; a
 * NIL list is considered to be an empty list of any type.
 */
#define IsPointerList(l)		((l) == NIL || IsA((l), List))

/*
 * Return a freshly allocated List. Since empty non-NIL lists are
 * invalid, new_list() also allocates the head cell of the new list:
 * the caller should be sure to fill in that cell's data.
 */
static List *
new_list(NodeTag type)
{
	List	   *new_list;
	ListCell   *new_head;

	new_head = pgut_new(ListCell);
	new_head->next = NULL;
	/* new_head->data is left undefined! */

	new_list = pgut_new(List);
	new_list->type = type;
	new_list->length = 1;
	new_list->head = new_head;
	new_list->tail = new_head;

	return new_list;
}

/*
 * Allocate a new cell and make it the head of the specified
 * list. Assumes the list it is passed is non-NIL.
 *
 * The data in the new head cell is undefined; the caller should be
 * sure to fill it in
 */
static void
new_head_cell(List *list)
{
	ListCell   *new_head;

	new_head = pgut_new(ListCell);
	new_head->next = list->head;

	list->head = new_head;
	list->length++;
}

/*
 * Allocate a new cell and make it the tail of the specified
 * list. Assumes the list it is passed is non-NIL.
 *
 * The data in the new tail cell is undefined; the caller should be
 * sure to fill it in
 */
static void
new_tail_cell(List *list)
{
	ListCell   *new_tail;

	new_tail = pgut_new(ListCell);
	new_tail->next = NULL;

	list->tail->next = new_tail;
	list->tail = new_tail;
	list->length++;
}

/*
 * Append a pointer to the list. A pointer to the modified list is
 * returned. Note that this function may or may not destructively
 * modify the list; callers should always use this function's return
 * value, rather than continuing to use the pointer passed as the
 * first argument.
 */
List *
lappend(List *list, void *datum)
{
	Assert(IsPointerList(list));

	if (list == NIL)
		list = new_list(T_List);
	else
		new_tail_cell(list);

	lfirst(list->tail) = datum;
	return list;
}

/*
 * Add a new cell to the list, in the position after 'prev_cell'. The
 * data in the cell is left undefined, and must be filled in by the
 * caller. 'list' is assumed to be non-NIL, and 'prev_cell' is assumed
 * to be non-NULL and a member of 'list'.
 */
static ListCell *
add_new_cell(List *list, ListCell *prev_cell)
{
	ListCell   *new_cell;

	new_cell = pgut_new(ListCell);
	/* new_cell->data is left undefined! */
	new_cell->next = prev_cell->next;
	prev_cell->next = new_cell;

	if (list->tail == prev_cell)
		list->tail = new_cell;

	list->length++;

	return new_cell;
}

/*
 * Add a new cell to the specified list (which must be non-NIL);
 * it will be placed after the list cell 'prev' (which must be
 * non-NULL and a member of 'list'). The data placed in the new cell
 * is 'datum'. The newly-constructed cell is returned.
 */
ListCell *
lappend_cell(List *list, ListCell *prev, void *datum)
{
	ListCell   *new_cell;

	Assert(IsPointerList(list));

	new_cell = add_new_cell(list, prev);
	lfirst(new_cell) = datum;
	return new_cell;
}

/*
 * Prepend a new element to the list. A pointer to the modified list
 * is returned. Note that this function may or may not destructively
 * modify the list; callers should always use this function's return
 * value, rather than continuing to use the pointer passed as the
 * second argument.
 *
 * Caution: before Postgres 8.0, the original List was unmodified and
 * could be considered to retain its separate identity.  This is no longer
 * the case.
 */
List *
lcons(void *datum, List *list)
{
	Assert(IsPointerList(list));

	if (list == NIL)
		list = new_list(T_List);
	else
		new_head_cell(list);

	lfirst(list->head) = datum;
	return list;
}

/*
 * Concatenate list2 to the end of list1, and return list1. list1 is
 * destructively changed. Callers should be sure to use the return
 * value as the new pointer to the concatenated list: the 'list1'
 * input pointer may or may not be the same as the returned pointer.
 *
 * The nodes in list2 are merely appended to the end of list1 in-place
 * (i.e. they aren't copied), and the list2 handle is free-ed. This is
 * an incopatible change from backend codes.
 */
List *
list_concat(List *list1, List *list2)
{
	if (list1 == NIL)
		return list2;
	if (list2 == NIL)
		return list1;
	if (list1 == list2)
		return list1;

	Assert(list1->type == list2->type);

	list1->length += list2->length;
	list1->tail->next = list2->head;
	list1->tail = list2->tail;

	/* Note: free list2 handle but keep items in it */
	free(list2);

	return list1;
}

/*
 * Truncate 'list' to contain no more than 'new_size' elements. This
 * modifies the list in-place! Despite this, callers should use the
 * pointer returned by this function to refer to the newly truncated
 * list -- it may or may not be the same as the pointer that was
 * passed.
 *
 * Note that any cells removed by list_truncate() are NOT free'd.
 */
List *
list_truncate(List *list, int new_size)
{
	ListCell   *cell;
	int			n;

	if (new_size <= 0)
		return NIL;				/* truncate to zero length */

	/* If asked to effectively extend the list, do nothing */
	if (new_size >= list_length(list))
		return list;

	n = 1;
	foreach(cell, list)
	{
		if (n == new_size)
		{
			cell->next = NULL;
			list->tail = cell;
			list->length = new_size;
			return list;
		}
		n++;
	}

	/* keep the compiler quiet; never reached */
	Assert(false);
	return list;
}

/*
 * Locate the n'th cell (counting from 0) of the list.  It is an assertion
 * failure if there is no such cell.
 */
static ListCell *
list_nth_cell(const List *list, int n)
{
	ListCell   *match;

	Assert(list != NIL);
	Assert(n >= 0);
	Assert(n < list->length);

	/* Does the caller actually mean to fetch the tail? */
	if (n == list->length - 1)
		return list->tail;

	for (match = list->head; n-- > 0; match = match->next)
		;

	return match;
}

/*
 * Return the data value contained in the n'th element of the
 * specified list. (List elements begin at 0.)
 */
void *
list_nth(const List *list, int n)
{
	Assert(IsPointerList(list));
	return lfirst(list_nth_cell(list, n));
}

/*
 * Return true iff 'datum' is a member of the list. Equality is
 * determined by using simple pointer comparison.
 */
bool
list_member_ptr(const List *list, const void *datum)
{
	ListCell   *cell;

	Assert(IsPointerList(list));

	foreach(cell, list)
	{
		if (lfirst(cell) == datum)
			return true;
	}

	return false;
}

/*
 * Delete 'cell' from 'list'; 'prev' is the previous element to 'cell'
 * in 'list', if any (i.e. prev == NULL iff list->head == cell)
 *
 * The cell is free'd, as is the List header if this was the last member.
 */
List *
list_delete_cell(List *list, ListCell *cell, ListCell *prev)
{
	Assert(prev != NULL ? lnext(prev) == cell : list_head(list) == cell);

	/*
	 * If we're about to delete the last node from the list, free the whole
	 * list instead and return NIL, which is the only valid representation of
	 * a zero-length list.
	 */
	if (list->length == 1)
	{
		list_free(list);
		return NIL;
	}

	/*
	 * Otherwise, adjust the necessary list links, deallocate the particular
	 * node we have just removed, and return the list we were given.
	 */
	list->length--;

	if (prev)
		prev->next = cell->next;
	else
		list->head = cell->next;

	if (list->tail == cell)
		list->tail = prev;

	free(cell);
	return list;
}

/* As above, but use simple pointer equality */
List *
list_delete_ptr(List *list, void *datum)
{
	ListCell   *cell;
	ListCell   *prev;

	Assert(IsPointerList(list));

	prev = NULL;
	foreach(cell, list)
	{
		if (lfirst(cell) == datum)
			return list_delete_cell(list, cell, prev);

		prev = cell;
	}

	/* Didn't find a match: return the list unmodified */
	return list;
}

/*
 * Delete the first element of the list.
 *
 * This is useful to replace the Lisp-y code "list = lnext(list);" in cases
 * where the intent is to alter the list rather than just traverse it.
 * Beware that the removed cell is freed, whereas the lnext() coding leaves
 * the original list head intact if there's another pointer to it.
 */
List *
list_delete_first(List *list)
{
	if (list == NIL)
		return NIL;				/* would an error be better? */

	return list_delete_cell(list, list_head(list), NULL);
}

/*
 * Free all the cells of the list, as well as the list itself. Any
 * objects that are pointed-to by the cells of the list are NOT
 * free'd.
 *
 * On return, the argument to this function has been freed, so the
 * caller would be wise to set it to NIL for safety's sake.
 */
void
list_free(List *list)
{
	list_destroy(list, NULL);
}

/*
 * Free all the cells of the list, the list itself, and all the
 * objects pointed-to by the cells of the list (each element in the
 * list must contain a pointer to a malloc()'d region of memory!)
 *
 * On return, the argument to this function has been freed, so the
 * caller would be wise to set it to NIL for safety's sake.
 */
void
list_free_deep(List *list)
{
	/*
	 * A "deep" free operation only makes sense on a list of pointers.
	 */
	Assert(IsPointerList(list));
	list_destroy(list, free);
}

/*
 * Return a shallow copy of the specified list.
 */
List *
list_copy(const List *oldlist)
{
	List	   *newlist;
	ListCell   *newlist_prev;
	ListCell   *oldlist_cur;

	if (oldlist == NIL)
		return NIL;

	newlist = new_list(oldlist->type);
	newlist->length = oldlist->length;

	/*
	 * Copy over the data in the first cell; new_list() has already allocated
	 * the head cell itself
	 */
	newlist->head->data = oldlist->head->data;

	newlist_prev = newlist->head;
	oldlist_cur = oldlist->head->next;
	while (oldlist_cur)
	{
		ListCell   *newlist_cur;

		newlist_cur = pgut_new(ListCell);
		newlist_cur->data = oldlist_cur->data;
		newlist_prev->next = newlist_cur;

		newlist_prev = newlist_cur;
		oldlist_cur = oldlist_cur->next;
	}

	newlist_prev->next = NULL;
	newlist->tail = newlist_prev;

	return newlist;
}

/*
 * Return a shallow copy of the specified list, without the first N elements.
 */
List *
list_copy_tail(const List *oldlist, int nskip)
{
	List	   *newlist;
	ListCell   *newlist_prev;
	ListCell   *oldlist_cur;

	if (nskip < 0)
		nskip = 0;				/* would it be better to elog? */

	if (oldlist == NIL || nskip >= oldlist->length)
		return NIL;

	newlist = new_list(oldlist->type);
	newlist->length = oldlist->length - nskip;

	/*
	 * Skip over the unwanted elements.
	 */
	oldlist_cur = oldlist->head;
	while (nskip-- > 0)
		oldlist_cur = oldlist_cur->next;

	/*
	 * Copy over the data in the first remaining cell; new_list() has already
	 * allocated the head cell itself
	 */
	newlist->head->data = oldlist_cur->data;

	newlist_prev = newlist->head;
	oldlist_cur = oldlist_cur->next;
	while (oldlist_cur)
	{
		ListCell   *newlist_cur;

		newlist_cur = pgut_new(ListCell);
		newlist_cur->data = oldlist_cur->data;
		newlist_prev->next = newlist_cur;

		newlist_prev = newlist_cur;
		oldlist_cur = oldlist_cur->next;
	}

	newlist_prev->next = NULL;
	newlist->tail = newlist_prev;

	return newlist;
}

/*
 * When using non-GCC compilers, we can't define these as inline
 * functions, so they are defined here.
 */
#ifndef __GNUC__
ListCell *
list_head(const List *l)
{
	return l ? l->head : NULL;
}

ListCell *
list_tail(List *l)
{
	return l ? l->tail : NULL;
}

int
list_length(const List *l)
{
	return l ? l->length : 0;
}
#endif   /* ! __GNUC__ */

/* list_walk - apply walker for each item */
void
list_walk(List *list, void (*walker)())
{
	ListCell *cell;

	Assert(walker != NULL);

	foreach(cell, list)
		walker(lfirst(cell));
}

/*
 * Free all storage in a list, and optionally the pointed-to elements
 */
void
list_destroy(List *list, void (*walker)())
{
	ListCell   *cell;

	cell = list_head(list);
	while (cell != NULL)
	{
		ListCell   *tmp = cell;

		cell = lnext(cell);
		if (walker)
			walker(lfirst(tmp));
		free(tmp);
	}

	free(list);
}
