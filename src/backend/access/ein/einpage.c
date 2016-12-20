/*
 * einpage.c
 *	  BTree-specific page management code for the Edge Index.
 *
 * NOTES
 *	  This file is based on nbtpage.c. See README for more details.
 *
 * Portions Copyright (c) 2016 by Bitnine Global, Inc.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/access/ein/einpage.c
 */

#include "postgres.h"

#include "access/ein.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "miscadmin.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"
#include "storage/predicate.h"
#include "utils/snapmgr.h"

static bool _ei_mark_page_halfdead(Relation rel, Buffer buf, BTStack stack);
static bool _bt_unlink_halfdead_page(Relation rel, Buffer leafbuf,
						 bool *rightsib_empty);
static bool _ei_lock_branch_parent(Relation rel, BlockNumber child,
					   BTStack stack, Buffer *topparent, OffsetNumber *topoff,
					   BlockNumber *target, BlockNumber *rightsib);

/*
 * Returns true, if the given block has the half-dead flag set.
 */
static bool
_bt_is_page_halfdead(Relation rel, BlockNumber blk)
{
	Buffer		buf;
	Page		page;
	BTPageOpaque opaque;
	bool		result;

	buf = _bt_getbuf(rel, blk, BT_READ);
	page = BufferGetPage(buf);
	opaque = (BTPageOpaque) PageGetSpecialPointer(page);

	result = P_ISHALFDEAD(opaque);
	_bt_relbuf(rel, buf);

	return result;
}

/*
 * Subroutine to find the parent of the branch we're deleting.  This climbs
 * up the tree until it finds a page with more than one child, i.e. a page
 * that will not be totally emptied by the deletion.  The chain of pages below
 * it, with one downlink each, will form the branch that we need to delete.
 *
 * If we cannot remove the downlink from the parent, because it's the
 * rightmost entry, returns false.  On success, *topparent and *topoff are set
 * to the buffer holding the parent, and the offset of the downlink in it.
 * *topparent is write-locked, the caller is responsible for releasing it when
 * done.  *target is set to the topmost page in the branch to-be-deleted, i.e.
 * the page whose downlink *topparent / *topoff point to, and *rightsib to its
 * right sibling.
 *
 * "child" is the leaf page we wish to delete, and "stack" is a search stack
 * leading to it (approximately).  Note that we will update the stack
 * entry(s) to reflect current downlink positions --- this is harmless and
 * indeed saves later search effort in _bt_pagedel.  The caller should
 * initialize *target and *rightsib to the leaf page and its right sibling.
 *
 * Note: it's OK to release page locks on any internal pages between the leaf
 * and *topparent, because a safe deletion can't become unsafe due to
 * concurrent activity.  An internal page can only acquire an entry if the
 * child is split, but that cannot happen as long as we hold a lock on the
 * leaf.
 */
static bool
_ei_lock_branch_parent(Relation rel, BlockNumber child, BTStack stack,
					   Buffer *topparent, OffsetNumber *topoff,
					   BlockNumber *target, BlockNumber *rightsib)
{
	BlockNumber parent;
	OffsetNumber poffset,
				maxoff;
	Buffer		pbuf;
	Page		page;
	BTPageOpaque opaque;
	BlockNumber leftsib;

	/*
	 * Locate the downlink of "child" in the parent (updating the stack entry
	 * if needed)
	 */
	ItemPointerSet(&(stack->bts_btentry.t_tid), child, P_HIKEY);
	pbuf = _ei_getstackbuf(rel, stack, BT_WRITE);
	if (pbuf == InvalidBuffer)
		elog(ERROR, "failed to re-find parent key in index \"%s\" for deletion target page %u",
			 RelationGetRelationName(rel), child);
	parent = stack->bts_blkno;
	poffset = stack->bts_offset;

	page = BufferGetPage(pbuf);
	opaque = (BTPageOpaque) PageGetSpecialPointer(page);
	maxoff = PageGetMaxOffsetNumber(page);

	/*
	 * If the target is the rightmost child of its parent, then we can't
	 * delete, unless it's also the only child.
	 */
	if (poffset >= maxoff)
	{
		/* It's rightmost child... */
		if (poffset == P_FIRSTDATAKEY(opaque))
		{
			/*
			 * It's only child, so safe if parent would itself be removable.
			 * We have to check the parent itself, and then recurse to test
			 * the conditions at the parent's parent.
			 */
			if (P_RIGHTMOST(opaque) || P_ISROOT(opaque) ||
				P_INCOMPLETE_SPLIT(opaque))
			{
				_bt_relbuf(rel, pbuf);
				return false;
			}

			*target = parent;
			*rightsib = opaque->btpo_next;
			leftsib = opaque->btpo_prev;

			_bt_relbuf(rel, pbuf);

			/*
			 * Like in _bt_pagedel, check that the left sibling is not marked
			 * with INCOMPLETE_SPLIT flag.  That would mean that there is no
			 * downlink to the page to be deleted, and the page deletion
			 * algorithm isn't prepared to handle that.
			 */
			if (leftsib != P_NONE)
			{
				Buffer		lbuf;
				Page		lpage;
				BTPageOpaque lopaque;

				lbuf = _bt_getbuf(rel, leftsib, BT_READ);
				lpage = BufferGetPage(lbuf);
				lopaque = (BTPageOpaque) PageGetSpecialPointer(lpage);

				/*
				 * If the left sibling was concurrently split, so that its
				 * next-pointer doesn't point to the current page anymore, the
				 * split that created the current page must be completed. (We
				 * don't allow splitting an incompletely split page again
				 * until the previous split has been completed)
				 */
				if (lopaque->btpo_next == parent &&
					P_INCOMPLETE_SPLIT(lopaque))
				{
					_bt_relbuf(rel, lbuf);
					return false;
				}
				_bt_relbuf(rel, lbuf);
			}

			/*
			 * Perform the same check on this internal level that
			 * _bt_mark_page_halfdead performed on the leaf level.
			 */
			if (_bt_is_page_halfdead(rel, *rightsib))
			{
				elog(DEBUG1, "could not delete page %u because its right sibling %u is half-dead",
					 parent, *rightsib);
				return false;
			}

			return _ei_lock_branch_parent(rel, parent, stack->bts_parent,
										topparent, topoff, target, rightsib);
		}
		else
		{
			/* Unsafe to delete */
			_bt_relbuf(rel, pbuf);
			return false;
		}
	}
	else
	{
		/* Not rightmost child, so safe to delete */
		*topparent = pbuf;
		*topoff = poffset;
		return true;
	}
}

/*
 * _bt_pagedel() -- Delete a page from the b-tree, if legal to do so.
 *
 * This action unlinks the page from the b-tree structure, removing all
 * pointers leading to it --- but not touching its own left and right links.
 * The page cannot be physically reclaimed right away, since other processes
 * may currently be trying to follow links leading to the page; they have to
 * be allowed to use its right-link to recover.  See nbtree/README.
 *
 * On entry, the target buffer must be pinned and locked (either read or write
 * lock is OK).  This lock and pin will be dropped before exiting.
 *
 * Returns the number of pages successfully deleted (zero if page cannot
 * be deleted now; could be more than one if parent or sibling pages were
 * deleted too).
 *
 * NOTE: this leaks memory.  Rather than trying to clean up everything
 * carefully, it's better to run it in a temp context that can be reset
 * frequently.
 */
int
_ei_pagedel(Relation rel, Buffer buf)
{
	int			ndeleted = 0;
	BlockNumber rightsib;
	bool		rightsib_empty;
	Page		page;
	BTPageOpaque opaque;

	/*
	 * "stack" is a search stack leading (approximately) to the target page.
	 * It is initially NULL, but when iterating, we keep it to avoid
	 * duplicated search effort.
	 *
	 * Also, when "stack" is not NULL, we have already checked that the
	 * current page is not the right half of an incomplete split, i.e. the
	 * left sibling does not have its INCOMPLETE_SPLIT flag set.
	 */
	BTStack		stack = NULL;

	for (;;)
	{
		page = BufferGetPage(buf);
		opaque = (BTPageOpaque) PageGetSpecialPointer(page);

		/*
		 * Internal pages are never deleted directly, only as part of deleting
		 * the whole branch all the way down to leaf level.
		 */
		if (!P_ISLEAF(opaque))
		{
			/*
			 * Pre-9.4 page deletion only marked internal pages as half-dead,
			 * but now we only use that flag on leaf pages. The old algorithm
			 * was never supposed to leave half-dead pages in the tree, it was
			 * just a transient state, but it was nevertheless possible in
			 * error scenarios. We don't know how to deal with them here. They
			 * are harmless as far as searches are considered, but inserts
			 * into the deleted keyspace could add out-of-order downlinks in
			 * the upper levels. Log a notice, hopefully the admin will notice
			 * and reindex.
			 */
			if (P_ISHALFDEAD(opaque))
				ereport(LOG,
						(errcode(ERRCODE_INDEX_CORRUPTED),
					errmsg("index \"%s\" contains a half-dead internal page",
						   RelationGetRelationName(rel)),
						 errhint("This can be caused by an interrupted VACUUM in version 9.3 or older, before upgrade. Please REINDEX it.")));
			_bt_relbuf(rel, buf);
			return ndeleted;
		}

		/*
		 * We can never delete rightmost pages nor root pages.  While at it,
		 * check that page is not already deleted and is empty.
		 *
		 * To keep the algorithm simple, we also never delete an incompletely
		 * split page (they should be rare enough that this doesn't make any
		 * meaningful difference to disk usage):
		 *
		 * The INCOMPLETE_SPLIT flag on the page tells us if the page is the
		 * left half of an incomplete split, but ensuring that it's not the
		 * right half is more complicated.  For that, we have to check that
		 * the left sibling doesn't have its INCOMPLETE_SPLIT flag set.  On
		 * the first iteration, we temporarily release the lock on the current
		 * page, and check the left sibling and also construct a search stack
		 * to.  On subsequent iterations, we know we stepped right from a page
		 * that passed these tests, so it's OK.
		 */
		if (P_RIGHTMOST(opaque) || P_ISROOT(opaque) || P_ISDELETED(opaque) ||
			P_FIRSTDATAKEY(opaque) <= PageGetMaxOffsetNumber(page) ||
			P_INCOMPLETE_SPLIT(opaque))
		{
			/* Should never fail to delete a half-dead page */
			Assert(!P_ISHALFDEAD(opaque));

			_bt_relbuf(rel, buf);
			return ndeleted;
		}

		/*
		 * First, remove downlink pointing to the page (or a parent of the
		 * page, if we are going to delete a taller branch), and mark the page
		 * as half-dead.
		 */
		if (!P_ISHALFDEAD(opaque))
		{
			/*
			 * We need an approximate pointer to the page's parent page.  We
			 * use the standard search mechanism to search for the page's high
			 * key; this will give us a link to either the current parent or
			 * someplace to its left (if there are multiple equal high keys).
			 *
			 * Also check if this is the right-half of an incomplete split
			 * (see comment above).
			 */
			if (!stack)
			{
				ScanKey		itup_scankey;
				ItemId		itemid;
				IndexTuple	targetkey;
				Buffer		lbuf;
				BlockNumber leftsib;

				itemid = PageGetItemId(page, P_HIKEY);
				targetkey = CopyIndexTuple((IndexTuple) PageGetItem(page, itemid));

				leftsib = opaque->btpo_prev;

				/*
				 * To avoid deadlocks, we'd better drop the leaf page lock
				 * before going further.
				 */
				LockBuffer(buf, BUFFER_LOCK_UNLOCK);

				/*
				 * Fetch the left sibling, to check that it's not marked with
				 * INCOMPLETE_SPLIT flag.  That would mean that the page
				 * to-be-deleted doesn't have a downlink, and the page
				 * deletion algorithm isn't prepared to handle that.
				 */
				if (!P_LEFTMOST(opaque))
				{
					BTPageOpaque lopaque;
					Page		lpage;

					lbuf = _bt_getbuf(rel, leftsib, BT_READ);
					lpage = BufferGetPage(lbuf);
					lopaque = (BTPageOpaque) PageGetSpecialPointer(lpage);

					/*
					 * If the left sibling is split again by another backend,
					 * after we released the lock, we know that the first
					 * split must have finished, because we don't allow an
					 * incompletely-split page to be split again.  So we don't
					 * need to walk right here.
					 */
					if (lopaque->btpo_next == BufferGetBlockNumber(buf) &&
						P_INCOMPLETE_SPLIT(lopaque))
					{
						ReleaseBuffer(buf);
						_bt_relbuf(rel, lbuf);
						return ndeleted;
					}
					_bt_relbuf(rel, lbuf);
				}

				/* we need an insertion scan key for the search, so build one */
				itup_scankey = _bt_mkscankey(rel, targetkey);
				/* find the leftmost leaf page containing this key */
				stack = _ei_search(rel, rel->rd_rel->relnatts, itup_scankey,
								   false, &lbuf, BT_READ, NULL);
				/* don't need a pin on the page */
				_bt_relbuf(rel, lbuf);

				/*
				 * Re-lock the leaf page, and start over, to re-check that the
				 * page can still be deleted.
				 */
				LockBuffer(buf, BT_WRITE);
				continue;
			}

			if (!_ei_mark_page_halfdead(rel, buf, stack))
			{
				_bt_relbuf(rel, buf);
				return ndeleted;
			}
		}

		/*
		 * Then unlink it from its siblings.  Each call to
		 * _bt_unlink_halfdead_page unlinks the topmost page from the branch,
		 * making it shallower.  Iterate until the leaf page is gone.
		 */
		rightsib_empty = false;
		while (P_ISHALFDEAD(opaque))
		{
			if (!_bt_unlink_halfdead_page(rel, buf, &rightsib_empty))
			{
				/* _bt_unlink_halfdead_page already released buffer */
				return ndeleted;
			}
			ndeleted++;
		}

		rightsib = opaque->btpo_next;

		_bt_relbuf(rel, buf);

		/*
		 * The page has now been deleted. If its right sibling is completely
		 * empty, it's possible that the reason we haven't deleted it earlier
		 * is that it was the rightmost child of the parent. Now that we
		 * removed the downlink for this page, the right sibling might now be
		 * the only child of the parent, and could be removed. It would be
		 * picked up by the next vacuum anyway, but might as well try to
		 * remove it now, so loop back to process the right sibling.
		 */
		if (!rightsib_empty)
			break;

		buf = _bt_getbuf(rel, rightsib, BT_WRITE);
	}

	return ndeleted;
}

/*
 * First stage of page deletion.  Remove the downlink to the top of the
 * branch being deleted, and mark the leaf page as half-dead.
 */
static bool
_ei_mark_page_halfdead(Relation rel, Buffer leafbuf, BTStack stack)
{
	BlockNumber leafblkno;
	BlockNumber leafrightsib;
	BlockNumber target;
	BlockNumber rightsib;
	ItemId		itemid;
	Page		page;
	BTPageOpaque opaque;
	Buffer		topparent;
	OffsetNumber topoff;
	OffsetNumber nextoffset;
	IndexTuple	itup;
	IndexTupleData trunctuple;

	page = BufferGetPage(leafbuf);
	opaque = (BTPageOpaque) PageGetSpecialPointer(page);

	Assert(!P_RIGHTMOST(opaque) && !P_ISROOT(opaque) && !P_ISDELETED(opaque) &&
		   !P_ISHALFDEAD(opaque) && P_ISLEAF(opaque) &&
		   P_FIRSTDATAKEY(opaque) > PageGetMaxOffsetNumber(page));

	/*
	 * Save info about the leaf page.
	 */
	leafblkno = BufferGetBlockNumber(leafbuf);
	leafrightsib = opaque->btpo_next;

	/*
	 * Before attempting to lock the parent page, check that the right sibling
	 * is not in half-dead state.  A half-dead right sibling would have no
	 * downlink in the parent, which would be highly confusing later when we
	 * delete the downlink that follows the current page's downlink. (I
	 * believe the deletion would work correctly, but it would fail the
	 * cross-check we make that the following downlink points to the right
	 * sibling of the delete page.)
	 */
	if (_bt_is_page_halfdead(rel, leafrightsib))
	{
		elog(DEBUG1, "could not delete page %u because its right sibling %u is half-dead",
			 leafblkno, leafrightsib);
		return false;
	}

	/*
	 * We cannot delete a page that is the rightmost child of its immediate
	 * parent, unless it is the only child --- in which case the parent has to
	 * be deleted too, and the same condition applies recursively to it. We
	 * have to check this condition all the way up before trying to delete,
	 * and lock the final parent of the to-be-deleted branch.
	 */
	rightsib = leafrightsib;
	target = leafblkno;
	if (!_ei_lock_branch_parent(rel, leafblkno, stack,
								&topparent, &topoff, &target, &rightsib))
		return false;

	/*
	 * Check that the parent-page index items we're about to delete/overwrite
	 * contain what we expect.  This can fail if the index has become corrupt
	 * for some reason.  We want to throw any error before entering the
	 * critical section --- otherwise it'd be a PANIC.
	 *
	 * The test on the target item is just an Assert because
	 * _bt_lock_branch_parent should have guaranteed it has the expected
	 * contents.  The test on the next-child downlink is known to sometimes
	 * fail in the field, though.
	 */
	page = BufferGetPage(topparent);
	opaque = (BTPageOpaque) PageGetSpecialPointer(page);

#ifdef USE_ASSERT_CHECKING
	itemid = PageGetItemId(page, topoff);
	itup = (IndexTuple) PageGetItem(page, itemid);
	Assert(ItemPointerGetBlockNumber(&(itup->t_tid)) == target);
#endif

	nextoffset = OffsetNumberNext(topoff);
	itemid = PageGetItemId(page, nextoffset);
	itup = (IndexTuple) PageGetItem(page, itemid);
	if (ItemPointerGetBlockNumber(&(itup->t_tid)) != rightsib)
		elog(ERROR, "right sibling %u of block %u is not next child %u of block %u in index \"%s\"",
			 rightsib, target, ItemPointerGetBlockNumber(&(itup->t_tid)),
			 BufferGetBlockNumber(topparent), RelationGetRelationName(rel));

	/*
	 * Any insert which would have gone on the leaf block will now go to its
	 * right sibling.
	 */
	PredicateLockPageCombine(rel, leafblkno, leafrightsib);

	/* No ereport(ERROR) until changes are logged */
	START_CRIT_SECTION();

	/*
	 * Update parent.  The normal case is a tad tricky because we want to
	 * delete the target's downlink and the *following* key.  Easiest way is
	 * to copy the right sibling's downlink over the target downlink, and then
	 * delete the following item.
	 */
	page = BufferGetPage(topparent);
	opaque = (BTPageOpaque) PageGetSpecialPointer(page);

	itemid = PageGetItemId(page, topoff);
	itup = (IndexTuple) PageGetItem(page, itemid);
	ItemPointerSet(&(itup->t_tid), rightsib, P_HIKEY);

	nextoffset = OffsetNumberNext(topoff);
	PageIndexTupleDelete(page, nextoffset);

	/*
	 * Mark the leaf page as half-dead, and stamp it with a pointer to the
	 * highest internal page in the branch we're deleting.  We use the tid of
	 * the high key to store it.
	 */
	page = BufferGetPage(leafbuf);
	opaque = (BTPageOpaque) PageGetSpecialPointer(page);
	opaque->btpo_flags |= BTP_HALF_DEAD;

	PageIndexTupleDelete(page, P_HIKEY);
	Assert(PageGetMaxOffsetNumber(page) == 0);
	MemSet(&trunctuple, 0, sizeof(IndexTupleData));
	trunctuple.t_info = sizeof(IndexTupleData);
	if (target != leafblkno)
		ItemPointerSet(&trunctuple.t_tid, target, P_HIKEY);
	else
		ItemPointerSetInvalid(&trunctuple.t_tid);
	if (PageAddItem(page, (Item) &trunctuple, sizeof(IndexTupleData), P_HIKEY,
					false, false) == InvalidOffsetNumber)
		elog(ERROR, "could not add dummy high key to half-dead page");

	/* Must mark buffers dirty before XLogInsert */
	MarkBufferDirty(topparent);
	MarkBufferDirty(leafbuf);

	/* XLOG stuff */
	if (RelationNeedsWAL(rel))
	{
		xl_btree_mark_page_halfdead xlrec;
		XLogRecPtr	recptr;

		xlrec.poffset = topoff;
		xlrec.leafblk = leafblkno;
		if (target != leafblkno)
			xlrec.topparent = target;
		else
			xlrec.topparent = InvalidBlockNumber;

		XLogBeginInsert();
		XLogRegisterBuffer(0, leafbuf, REGBUF_WILL_INIT);
		XLogRegisterBuffer(1, topparent, REGBUF_STANDARD);

		page = BufferGetPage(leafbuf);
		opaque = (BTPageOpaque) PageGetSpecialPointer(page);
		xlrec.leftblk = opaque->btpo_prev;
		xlrec.rightblk = opaque->btpo_next;

		XLogRegisterData((char *) &xlrec, SizeOfBtreeMarkPageHalfDead);

		recptr = XLogInsert(RM_BTREE_ID, XLOG_BTREE_MARK_PAGE_HALFDEAD);

		page = BufferGetPage(topparent);
		PageSetLSN(page, recptr);
		page = BufferGetPage(leafbuf);
		PageSetLSN(page, recptr);
	}

	END_CRIT_SECTION();

	_bt_relbuf(rel, topparent);
	return true;
}

/*
 * Unlink a page in a branch of half-dead pages from its siblings.
 *
 * If the leaf page still has a downlink pointing to it, unlinks the highest
 * parent in the to-be-deleted branch instead of the leaf page.  To get rid
 * of the whole branch, including the leaf page itself, iterate until the
 * leaf page is deleted.
 *
 * Returns 'false' if the page could not be unlinked (shouldn't happen).
 * If the (new) right sibling of the page is empty, *rightsib_empty is set
 * to true.
 *
 * Must hold pin and lock on leafbuf at entry (read or write doesn't matter).
 * On success exit, we'll be holding pin and write lock.  On failure exit,
 * we'll release both pin and lock before returning (we define it that way
 * to avoid having to reacquire a lock we already released).
 */
static bool
_bt_unlink_halfdead_page(Relation rel, Buffer leafbuf, bool *rightsib_empty)
{
	BlockNumber leafblkno = BufferGetBlockNumber(leafbuf);
	BlockNumber leafleftsib;
	BlockNumber leafrightsib;
	BlockNumber target;
	BlockNumber leftsib;
	BlockNumber rightsib;
	Buffer		lbuf = InvalidBuffer;
	Buffer		buf;
	Buffer		rbuf;
	Buffer		metabuf = InvalidBuffer;
	Page		metapg = NULL;
	BTMetaPageData *metad = NULL;
	ItemId		itemid;
	Page		page;
	BTPageOpaque opaque;
	bool		rightsib_is_rightmost;
	int			targetlevel;
	ItemPointer leafhikey;
	BlockNumber nextchild;

	page = BufferGetPage(leafbuf);
	opaque = (BTPageOpaque) PageGetSpecialPointer(page);

	Assert(P_ISLEAF(opaque) && P_ISHALFDEAD(opaque));

	/*
	 * Remember some information about the leaf page.
	 */
	itemid = PageGetItemId(page, P_HIKEY);
	leafhikey = &((IndexTuple) PageGetItem(page, itemid))->t_tid;
	leafleftsib = opaque->btpo_prev;
	leafrightsib = opaque->btpo_next;

	LockBuffer(leafbuf, BUFFER_LOCK_UNLOCK);

	/*
	 * If the leaf page still has a parent pointing to it (or a chain of
	 * parents), we don't unlink the leaf page yet, but the topmost remaining
	 * parent in the branch.  Set 'target' and 'buf' to reference the page
	 * actually being unlinked.
	 */
	if (ItemPointerIsValid(leafhikey))
	{
		target = ItemPointerGetBlockNumber(leafhikey);
		Assert(target != leafblkno);

		/* fetch the block number of the topmost parent's left sibling */
		buf = _bt_getbuf(rel, target, BT_READ);
		page = BufferGetPage(buf);
		opaque = (BTPageOpaque) PageGetSpecialPointer(page);
		leftsib = opaque->btpo_prev;
		targetlevel = opaque->btpo.level;

		/*
		 * To avoid deadlocks, we'd better drop the target page lock before
		 * going further.
		 */
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	}
	else
	{
		target = leafblkno;

		buf = leafbuf;
		leftsib = leafleftsib;
		targetlevel = 0;
	}

	/*
	 * We have to lock the pages we need to modify in the standard order:
	 * moving right, then up.  Else we will deadlock against other writers.
	 *
	 * So, first lock the leaf page, if it's not the target.  Then find and
	 * write-lock the current left sibling of the target page.  The sibling
	 * that was current a moment ago could have split, so we may have to move
	 * right.  This search could fail if either the sibling or the target page
	 * was deleted by someone else meanwhile; if so, give up.  (Right now,
	 * that should never happen, since page deletion is only done in VACUUM
	 * and there shouldn't be multiple VACUUMs concurrently on the same
	 * table.)
	 */
	if (target != leafblkno)
		LockBuffer(leafbuf, BT_WRITE);
	if (leftsib != P_NONE)
	{
		lbuf = _bt_getbuf(rel, leftsib, BT_WRITE);
		page = BufferGetPage(lbuf);
		opaque = (BTPageOpaque) PageGetSpecialPointer(page);
		while (P_ISDELETED(opaque) || opaque->btpo_next != target)
		{
			/* step right one page */
			leftsib = opaque->btpo_next;
			_bt_relbuf(rel, lbuf);
			if (leftsib == P_NONE)
			{
				elog(LOG, "no left sibling (concurrent deletion?) of block %u in \"%s\"",
					 target,
					 RelationGetRelationName(rel));
				if (target != leafblkno)
				{
					/* we have only a pin on target, but pin+lock on leafbuf */
					ReleaseBuffer(buf);
					_bt_relbuf(rel, leafbuf);
				}
				else
				{
					/* we have only a pin on leafbuf */
					ReleaseBuffer(leafbuf);
				}
				return false;
			}
			lbuf = _bt_getbuf(rel, leftsib, BT_WRITE);
			page = BufferGetPage(lbuf);
			opaque = (BTPageOpaque) PageGetSpecialPointer(page);
		}
	}
	else
		lbuf = InvalidBuffer;

	/*
	 * Next write-lock the target page itself.  It should be okay to take just
	 * a write lock not a superexclusive lock, since no scans would stop on an
	 * empty page.
	 */
	LockBuffer(buf, BT_WRITE);
	page = BufferGetPage(buf);
	opaque = (BTPageOpaque) PageGetSpecialPointer(page);

	/*
	 * Check page is still empty etc, else abandon deletion.  This is just for
	 * paranoia's sake; a half-dead page cannot resurrect because there can be
	 * only one vacuum process running at a time.
	 */
	if (P_RIGHTMOST(opaque) || P_ISROOT(opaque) || P_ISDELETED(opaque))
	{
		elog(ERROR, "half-dead page changed status unexpectedly in block %u of index \"%s\"",
			 target, RelationGetRelationName(rel));
	}
	if (opaque->btpo_prev != leftsib)
		elog(ERROR, "left link changed unexpectedly in block %u of index \"%s\"",
			 target, RelationGetRelationName(rel));

	if (target == leafblkno)
	{
		if (P_FIRSTDATAKEY(opaque) <= PageGetMaxOffsetNumber(page) ||
			!P_ISLEAF(opaque) || !P_ISHALFDEAD(opaque))
			elog(ERROR, "half-dead page changed status unexpectedly in block %u of index \"%s\"",
				 target, RelationGetRelationName(rel));
		nextchild = InvalidBlockNumber;
	}
	else
	{
		if (P_FIRSTDATAKEY(opaque) != PageGetMaxOffsetNumber(page) ||
			P_ISLEAF(opaque))
			elog(ERROR, "half-dead page changed status unexpectedly in block %u of index \"%s\"",
				 target, RelationGetRelationName(rel));

		/* remember the next non-leaf child down in the branch. */
		itemid = PageGetItemId(page, P_FIRSTDATAKEY(opaque));
		nextchild = ItemPointerGetBlockNumber(&((IndexTuple) PageGetItem(page, itemid))->t_tid);
		if (nextchild == leafblkno)
			nextchild = InvalidBlockNumber;
	}

	/*
	 * And next write-lock the (current) right sibling.
	 */
	rightsib = opaque->btpo_next;
	rbuf = _bt_getbuf(rel, rightsib, BT_WRITE);
	page = BufferGetPage(rbuf);
	opaque = (BTPageOpaque) PageGetSpecialPointer(page);
	if (opaque->btpo_prev != target)
		elog(ERROR, "right sibling's left-link doesn't match: "
			 "block %u links to %u instead of expected %u in index \"%s\"",
			 rightsib, opaque->btpo_prev, target,
			 RelationGetRelationName(rel));
	rightsib_is_rightmost = P_RIGHTMOST(opaque);
	*rightsib_empty = (P_FIRSTDATAKEY(opaque) > PageGetMaxOffsetNumber(page));

	/*
	 * If we are deleting the next-to-last page on the target's level, then
	 * the rightsib is a candidate to become the new fast root. (In theory, it
	 * might be possible to push the fast root even further down, but the odds
	 * of doing so are slim, and the locking considerations daunting.)
	 *
	 * We don't support handling this in the case where the parent is becoming
	 * half-dead, even though it theoretically could occur.
	 *
	 * We can safely acquire a lock on the metapage here --- see comments for
	 * _bt_newroot().
	 */
	if (leftsib == P_NONE && rightsib_is_rightmost)
	{
		page = BufferGetPage(rbuf);
		opaque = (BTPageOpaque) PageGetSpecialPointer(page);
		if (P_RIGHTMOST(opaque))
		{
			/* rightsib will be the only one left on the level */
			metabuf = _bt_getbuf(rel, BTREE_METAPAGE, BT_WRITE);
			metapg = BufferGetPage(metabuf);
			metad = BTPageGetMeta(metapg);

			/*
			 * The expected case here is btm_fastlevel == targetlevel+1; if
			 * the fastlevel is <= targetlevel, something is wrong, and we
			 * choose to overwrite it to fix it.
			 */
			if (metad->btm_fastlevel > targetlevel + 1)
			{
				/* no update wanted */
				_bt_relbuf(rel, metabuf);
				metabuf = InvalidBuffer;
			}
		}
	}

	/*
	 * Here we begin doing the deletion.
	 */

	/* No ereport(ERROR) until changes are logged */
	START_CRIT_SECTION();

	/*
	 * Update siblings' side-links.  Note the target page's side-links will
	 * continue to point to the siblings.  Asserts here are just rechecking
	 * things we already verified above.
	 */
	if (BufferIsValid(lbuf))
	{
		page = BufferGetPage(lbuf);
		opaque = (BTPageOpaque) PageGetSpecialPointer(page);
		Assert(opaque->btpo_next == target);
		opaque->btpo_next = rightsib;
	}
	page = BufferGetPage(rbuf);
	opaque = (BTPageOpaque) PageGetSpecialPointer(page);
	Assert(opaque->btpo_prev == target);
	opaque->btpo_prev = leftsib;

	/*
	 * If we deleted a parent of the targeted leaf page, instead of the leaf
	 * itself, update the leaf to point to the next remaining child in the
	 * branch.
	 */
	if (target != leafblkno)
	{
		if (nextchild == InvalidBlockNumber)
			ItemPointerSetInvalid(leafhikey);
		else
			ItemPointerSet(leafhikey, nextchild, P_HIKEY);
	}

	/*
	 * Mark the page itself deleted.  It can be recycled when all current
	 * transactions are gone.  Storing GetTopTransactionId() would work, but
	 * we're in VACUUM and would not otherwise have an XID.  Having already
	 * updated links to the target, ReadNewTransactionId() suffices as an
	 * upper bound.  Any scan having retained a now-stale link is advertising
	 * in its PGXACT an xmin less than or equal to the value we read here.  It
	 * will continue to do so, holding back RecentGlobalXmin, for the duration
	 * of that scan.
	 */
	page = BufferGetPage(buf);
	opaque = (BTPageOpaque) PageGetSpecialPointer(page);
	opaque->btpo_flags &= ~BTP_HALF_DEAD;
	opaque->btpo_flags |= BTP_DELETED;
	opaque->btpo.xact = ReadNewTransactionId();

	/* And update the metapage, if needed */
	if (BufferIsValid(metabuf))
	{
		metad->btm_fastroot = rightsib;
		metad->btm_fastlevel = targetlevel;
		MarkBufferDirty(metabuf);
	}

	/* Must mark buffers dirty before XLogInsert */
	MarkBufferDirty(rbuf);
	MarkBufferDirty(buf);
	if (BufferIsValid(lbuf))
		MarkBufferDirty(lbuf);
	if (target != leafblkno)
		MarkBufferDirty(leafbuf);

	/* XLOG stuff */
	if (RelationNeedsWAL(rel))
	{
		xl_btree_unlink_page xlrec;
		xl_btree_metadata xlmeta;
		uint8		xlinfo;
		XLogRecPtr	recptr;

		XLogBeginInsert();

		XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);
		if (BufferIsValid(lbuf))
			XLogRegisterBuffer(1, lbuf, REGBUF_STANDARD);
		XLogRegisterBuffer(2, rbuf, REGBUF_STANDARD);
		if (target != leafblkno)
			XLogRegisterBuffer(3, leafbuf, REGBUF_WILL_INIT);

		/* information on the unlinked block */
		xlrec.leftsib = leftsib;
		xlrec.rightsib = rightsib;
		xlrec.btpo_xact = opaque->btpo.xact;

		/* information needed to recreate the leaf block (if not the target) */
		xlrec.leafleftsib = leafleftsib;
		xlrec.leafrightsib = leafrightsib;
		xlrec.topparent = nextchild;

		XLogRegisterData((char *) &xlrec, SizeOfBtreeUnlinkPage);

		if (BufferIsValid(metabuf))
		{
			XLogRegisterBuffer(4, metabuf, REGBUF_WILL_INIT);

			xlmeta.root = metad->btm_root;
			xlmeta.level = metad->btm_level;
			xlmeta.fastroot = metad->btm_fastroot;
			xlmeta.fastlevel = metad->btm_fastlevel;

			XLogRegisterBufData(4, (char *) &xlmeta, sizeof(xl_btree_metadata));
			xlinfo = XLOG_BTREE_UNLINK_PAGE_META;
		}
		else
			xlinfo = XLOG_BTREE_UNLINK_PAGE;

		recptr = XLogInsert(RM_BTREE_ID, xlinfo);

		if (BufferIsValid(metabuf))
		{
			PageSetLSN(metapg, recptr);
		}
		page = BufferGetPage(rbuf);
		PageSetLSN(page, recptr);
		page = BufferGetPage(buf);
		PageSetLSN(page, recptr);
		if (BufferIsValid(lbuf))
		{
			page = BufferGetPage(lbuf);
			PageSetLSN(page, recptr);
		}
		if (target != leafblkno)
		{
			page = BufferGetPage(leafbuf);
			PageSetLSN(page, recptr);
		}
	}

	END_CRIT_SECTION();

	/* release metapage */
	if (BufferIsValid(metabuf))
		_bt_relbuf(rel, metabuf);

	/* release siblings */
	if (BufferIsValid(lbuf))
		_bt_relbuf(rel, lbuf);
	_bt_relbuf(rel, rbuf);

	/*
	 * Release the target, if it was not the leaf block.  The leaf is always
	 * kept locked.
	 */
	if (target != leafblkno)
		_bt_relbuf(rel, buf);

	return true;
}
