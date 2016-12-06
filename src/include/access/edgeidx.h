
#ifndef EDGEIDX_H
#define	EDGEIDX_H

#include "access/nbtree.h"

// edgeidx.c
extern Datum edgeidxhandler(PG_FUNCTION_ARGS);
extern IndexBuildResult *eibuild(Relation heap, Relation index,
								 struct IndexInfo *indexInfo);
extern bool eiinsert(Relation rel, Datum *values, bool *isnull,
		 ItemPointer ht_ctid, Relation heapRel,
		 IndexUniqueCheck checkUnique);

extern bool eigettuple(IndexScanDesc scan, ScanDirection dir);
extern int64 eigetbitmap(IndexScanDesc scan, TIDBitmap *tbm);
extern IndexBulkDeleteResult *eibulkdelete(IndexVacuumInfo *info,
			IndexBulkDeleteResult *stats,
			IndexBulkDeleteCallback callback,
			void *callback_state);
extern IndexBulkDeleteResult *eivacuumcleanup(IndexVacuumInfo *info,
			IndexBulkDeleteResult *stats);

// eisearch.c
extern BTStack _ei_search(Relation rel,
			int keysz, ScanKey scankey, bool nextkey,
			Buffer *bufP, int access, Snapshot snapshot);
extern int32 _ei_compare(Relation rel, int keysz, ScanKey scankey,
		            Page page, OffsetNumber offnum);
extern bool _ei_first(IndexScanDesc scan, ScanDirection dir);
extern Buffer _ei_moveright(Relation rel, Buffer buf, int keysz,
			  ScanKey scankey, bool nextkey, bool forupdate, BTStack stack,
			  int access, Snapshot snapshot);
extern OffsetNumber _ei_binsrch(Relation rel, Buffer buf, int keysz,
			ScanKey scankey, bool nextkey);

// eiinsert.c
extern bool _ei_doinsert(Relation rel, IndexTuple itup,
		             IndexUniqueCheck checkUnique, Relation heapRel);
extern Buffer _ei_getstackbuf(Relation rel, BTStack stack, int access);
extern void _ei_finish_split(Relation rel, Buffer bbuf, BTStack stack);

// eipage.c
extern int  _ei_pagedel(Relation rel, Buffer buf); 

extern IndexTuple _ei_reformTuple (IndexTuple source, TupleDesc tupDesc);


#endif	/* EDGEIDX_H */
