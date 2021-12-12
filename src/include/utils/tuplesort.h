/*-------------------------------------------------------------------------
 *
 * tuplesort.h
 *	  Generalized tuple sorting routines.
 *
 * This module handles sorting of heap tuples, index tuples, or single
 * Datums (and could easily support other kinds of sortable objects,
 * if necessary).  It works efficiently for both small and large amounts
 * of data.  Small amounts are sorted in-memory using qsort().  Large
 * amounts are sorted using temporary files and a standard external sort
 * algorithm.  Parallel sorts use a variant of this external sort
 * algorithm, and are typically only used for large amounts of data.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/tuplesort.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TUPLESORT_H
#define TUPLESORT_H

#include "access/itup.h"
#include "executor/tuptable.h"
#include "storage/dsm.h"
#include "utils/logtape.h"
#include "utils/pg_rusage.h"
#include "utils/relcache.h"
#include "utils/sortsupport.h"


/*
 * Tuplesortstate and Sharedsort are opaque types whose details are not
 * known outside tuplesort.c.
 */
typedef struct Tuplesortstate Tuplesortstate;
typedef struct Sharedsort Sharedsort;

/*
 * Tuplesort parallel coordination state, allocated by each participant in
 * local memory.  Participant caller initializes everything.  See usage notes
 * below.
 */
typedef struct SortCoordinateData
{
	/* Worker process?  If not, must be leader. */
	bool		isWorker;

	/*
	 * Leader-process-passed number of participants known launched (workers
	 * set this to -1).  Includes state within leader needed for it to
	 * participate as a worker, if any.
	 */
	int			nParticipants;

	/* Private opaque state (points to shared memory) */
	Sharedsort *sharedsort;
}			SortCoordinateData;

typedef struct SortCoordinateData *SortCoordinate;

/*
 * Data structures for reporting sort statistics.  Note that
 * TuplesortInstrumentation can't contain any pointers because we
 * sometimes put it in shared memory.
 *
 * The parallel-sort infrastructure relies on having a zero TuplesortMethod
 * to indicate that a worker never did anything, so we assign zero to
 * SORT_TYPE_STILL_IN_PROGRESS.  The other values of this enum can be
 * OR'ed together to represent a situation where different workers used
 * different methods, so we need a separate bit for each one.  Keep the
 * NUM_TUPLESORTMETHODS constant in sync with the number of bits!
 */
typedef enum
{
	SORT_TYPE_STILL_IN_PROGRESS = 0,
	SORT_TYPE_TOP_N_HEAPSORT = 1 << 0,
	SORT_TYPE_QUICKSORT = 1 << 1,
	SORT_TYPE_EXTERNAL_SORT = 1 << 2,
	SORT_TYPE_EXTERNAL_MERGE = 1 << 3
} TuplesortMethod;

#define NUM_TUPLESORTMETHODS 4

typedef enum
{
	SORT_SPACE_TYPE_DISK,
	SORT_SPACE_TYPE_MEMORY
} TuplesortSpaceType;

typedef struct TuplesortInstrumentation
{
	TuplesortMethod sortMethod; /* sort algorithm used */
	TuplesortSpaceType spaceType;	/* type of space spaceUsed represents */
	int64		spaceUsed;		/* space consumption, in kB */
} TuplesortInstrumentation;


/*
 * We provide multiple interfaces to what is essentially the same code,
 * since different callers have different data to be sorted and want to
 * specify the sort key information differently.  There are two APIs for
 * sorting HeapTuples and two more for sorting IndexTuples.  Yet another
 * API supports sorting bare Datums.
 *
 * Serial sort callers should pass NULL for their coordinate argument.
 *
 * The "heap" API actually stores/sorts MinimalTuples, which means it doesn't
 * preserve the system columns (tuple identity and transaction visibility
 * info).  The sort keys are specified by column numbers within the tuples
 * and sort operator OIDs.  We save some cycles by passing and returning the
 * tuples in TupleTableSlots, rather than forming actual HeapTuples (which'd
 * have to be converted to MinimalTuples).  This API works well for sorts
 * executed as parts of plan trees.
 *
 * The "cluster" API stores/sorts full HeapTuples including all visibility
 * info. The sort keys are specified by reference to a btree index that is
 * defined on the relation to be sorted.  Note that putheaptuple/getheaptuple
 * go with this API, not the "begin_heap" one!
 *
 * The "index_btree" API stores/sorts IndexTuples (preserving all their
 * header fields).  The sort keys are specified by a btree index definition.
 *
 * The "index_hash" API is similar to index_btree, but the tuples are
 * actually sorted by their hash codes not the raw data.
 *
 * Parallel sort callers are required to coordinate multiple tuplesort states
 * in a leader process and one or more worker processes.  The leader process
 * must launch workers, and have each perform an independent "partial"
 * tuplesort, typically fed by the parallel heap interface.  The leader later
 * produces the final output (internally, it merges runs output by workers).
 *
 * Callers must do the following to perform a sort in parallel using multiple
 * worker processes:
 *
 * 1. Request tuplesort-private shared memory for n workers.  Use
 *    tuplesort_estimate_shared() to get the required size.
 * 2. Have leader process initialize allocated shared memory using
 *    tuplesort_initialize_shared().  Launch workers.
 * 3. Initialize a coordinate argument within both the leader process, and
 *    for each worker process.  This has a pointer to the shared
 *    tuplesort-private structure, as well as some caller-initialized fields.
 *    Leader's coordinate argument reliably indicates number of workers
 *    launched (this is unused by workers).
 * 4. Begin a tuplesort using some appropriate tuplesort_begin* routine,
 *    (passing the coordinate argument) within each worker.  The workMem
 *    arguments need not be identical.  All other arguments should match
 *    exactly, though.
 * 5. tuplesort_attach_shared() should be called by all workers.  Feed tuples
 *    to each worker, and call tuplesort_performsort() within each when input
 *    is exhausted.
 * 6. Call tuplesort_end() in each worker process.  Worker processes can shut
 *    down once tuplesort_end() returns.
 * 7. Begin a tuplesort in the leader using the same tuplesort_begin*
 *    routine, passing a leader-appropriate coordinate argument (this can
 *    happen as early as during step 3, actually, since we only need to know
 *    the number of workers successfully launched).  The leader must now wait
 *    for workers to finish.  Caller must use own mechanism for ensuring that
 *    next step isn't reached until all workers have called and returned from
 *    tuplesort_performsort().  (Note that it's okay if workers have already
 *    also called tuplesort_end() by then.)
 * 8. Call tuplesort_performsort() in leader.  Consume output using the
 *    appropriate tuplesort_get* routine.  Leader can skip this step if
 *    tuplesort turns out to be unnecessary.
 * 9. Call tuplesort_end() in leader.
 *
 * This division of labor assumes nothing about how input tuples are produced,
 * but does require that caller combine the state of multiple tuplesorts for
 * any purpose other than producing the final output.  For example, callers
 * must consider that tuplesort_get_stats() reports on only one worker's role
 * in a sort (or the leader's role), and not statistics for the sort as a
 * whole.
 *
 * Note that callers may use the leader process to sort runs as if it was an
 * independent worker process (prior to the process performing a leader sort
 * to produce the final sorted output).  Doing so only requires a second
 * "partial" tuplesort within the leader process, initialized like that of a
 * worker process.  The steps above don't touch on this directly.  The only
 * difference is that the tuplesort_attach_shared() call is never needed within
 * leader process, because the backend as a whole holds the shared fileset
 * reference.  A worker Tuplesortstate in leader is expected to do exactly the
 * same amount of total initial processing work as a worker process
 * Tuplesortstate, since the leader process has nothing else to do before
 * workers finish.
 *
 * Note that only a very small amount of memory will be allocated prior to
 * the leader state first consuming input, and that workers will free the
 * vast majority of their memory upon returning from tuplesort_performsort().
 * Callers can rely on this to arrange for memory to be used in a way that
 * respects a workMem-style budget across an entire parallel sort operation.
 *
 * Callers are responsible for parallel safety in general.  However, they
 * can at least rely on there being no parallel safety hazards within
 * tuplesort, because tuplesort thinks of the sort as several independent
 * sorts whose results are combined.  Since, in general, the behavior of
 * sort operators is immutable, caller need only worry about the parallel
 * safety of whatever the process is through which input tuples are
 * generated (typically, caller uses a parallel heap scan).
 */

/*
 * The objects we actually sort are SortTuple structs.  These contain
 * a pointer to the tuple proper (might be a MinimalTuple or IndexTuple),
 * which is a separate palloc chunk --- we assume it is just one chunk and
 * can be freed by a simple pfree() (except during merge, when we use a
 * simple slab allocator).  SortTuples also contain the tuple's first key
 * column in Datum/nullflag format, and a source/input tape number that
 * tracks which tape each heap element/slot belongs to during merging.
 *
 * Storing the first key column lets us save heap_getattr or index_getattr
 * calls during tuple comparisons.  We could extract and save all the key
 * columns not just the first, but this would increase code complexity and
 * overhead, and wouldn't actually save any comparison cycles in the common
 * case where the first key determines the comparison result.  Note that
 * for a pass-by-reference datatype, datum1 points into the "tuple" storage.
 *
 * There is one special case: when the sort support infrastructure provides an
 * "abbreviated key" representation, where the key is (typically) a pass by
 * value proxy for a pass by reference type.  In this case, the abbreviated key
 * is stored in datum1 in place of the actual first key column.
 *
 * When sorting single Datums, the data value is represented directly by
 * datum1/isnull1 for pass by value types (or null values).  If the datatype is
 * pass-by-reference and isnull1 is false, then "tuple" points to a separately
 * palloc'd data value, otherwise "tuple" is NULL.  The value of datum1 is then
 * either the same pointer as "tuple", or is an abbreviated key value as
 * described above.  Accordingly, "tuple" is always used in preference to
 * datum1 as the authoritative value for pass-by-reference cases.
 */
typedef struct
{
	void	   *tuple;			/* the tuple itself */
	Datum		datum1;			/* value of first key column */
	bool		isnull1;		/* is first key column NULL? */
	uint8		flags;			/* user-defined */
	int			srctape;		/* source tape number */
} SortTuple;

/*
 * During merge, we use a pre-allocated set of fixed-size slots to hold
 * tuples.  To avoid palloc/pfree overhead.
 *
 * Merge doesn't require a lot of memory, so we can afford to waste some,
 * by using gratuitously-sized slots.  If a tuple is larger than 1 kB, the
 * palloc() overhead is not significant anymore.
 *
 * 'nextfree' is valid when this chunk is in the free list.  When in use, the
 * slot holds a tuple.
 */
#define SLAB_SLOT_SIZE 1024

typedef union SlabSlot
{
	union SlabSlot *nextfree;
	char		buffer[SLAB_SLOT_SIZE];
} SlabSlot;

/*
 * Possible states of a Tuplesort object.  These denote the states that
 * persist between calls of Tuplesort routines.
 */
typedef enum
{
	TSS_INITIAL,				/* Loading tuples; still within memory limit */
	TSS_BOUNDED,				/* Loading tuples into bounded-size heap */
	TSS_BUILDRUNS,				/* Loading tuples; writing to tape */
	TSS_SORTEDINMEM,			/* Sort completed entirely in memory */
	TSS_SORTEDONTAPE,			/* Sort completed, final run is on tape */
	TSS_FINALMERGE				/* Performing final merge on-the-fly */
} TupSortStatus;

typedef int (*SortTupleComparator) (const SortTuple *a, const SortTuple *b,
									Tuplesortstate *state);

typedef void (*SortCopyTuple) (Tuplesortstate *state, SortTuple *stup,
							   void *tup);
typedef void (*SortWriteTuple) (Tuplesortstate *state, int tapenum,
								SortTuple *stup);
typedef void (*SortReadTuple) (Tuplesortstate *state, SortTuple *stup,
							   int tapenum, unsigned int len);

typedef void (*tuplesort_report_duplicate_type)(HeapTuple tuple, void *arg);

struct IndexInfo;
struct EState;

/*
 * Private state of a Tuplesort operation.
 */
struct Tuplesortstate
{
	TupSortStatus status;		/* enumerated value as shown above */
	int			nKeys;			/* number of columns in sort key */
	bool		randomAccess;	/* did caller request random access? */
	bool		bounded;		/* did caller specify a maximum number of
								 * tuples to return? */
	bool		boundUsed;		/* true if we made use of a bounded heap */
	int			bound;			/* if bounded, the maximum number of tuples */
	bool		tuples;			/* Can SortTuple.tuple ever be set? */
	int64		availMem;		/* remaining memory available, in bytes */
	int64		allowedMem;		/* total memory allowed, in bytes */
	int			maxTapes;		/* number of tapes (Knuth's T) */
	int			tapeRange;		/* maxTapes-1 (Knuth's P) */
	int64		maxSpace;		/* maximum amount of space occupied among sort
								 * of groups, either in-memory or on-disk */
	bool		isMaxSpaceDisk; /* true when maxSpace is value for on-disk
								 * space, false when it's value for in-memory
								 * space */
	TupSortStatus maxSpaceStatus;	/* sort status when maxSpace was reached */
	MemoryContext maincontext;	/* memory context for tuple sort metadata that
								 * persists across multiple batches */
	MemoryContext sortcontext;	/* memory context holding most sort data */
	MemoryContext tuplecontext; /* sub-context of sortcontext for tuple data */
	LogicalTapeSet *tapeset;	/* logtape.c object for tapes in a temp file */

	/*
	 * These function pointers decouple the routines that must know what kind
	 * of tuple we are sorting from the routines that don't need to know it.
	 * They are set up by the tuplesort_begin_xxx routines.
	 *
	 * Function to compare two tuples; result is per qsort() convention, ie:
	 * <0, 0, >0 according as a<b, a=b, a>b.  The API must match
	 * qsort_arg_comparator.
	 */
	SortTupleComparator comparetup;

	/*
	 * Function to copy a supplied input tuple into palloc'd space and set up
	 * its SortTuple representation (ie, set tuple/datum1/isnull1).  Also,
	 * state->availMem must be decreased by the amount of space used for the
	 * tuple copy (note the SortTuple struct itself is not counted).
	 */
	SortCopyTuple	copytup;

	/*
	 * Function to write a stored tuple onto tape.  The representation of the
	 * tuple on tape need not be the same as it is in memory; requirements on
	 * the tape representation are given below.  Unless the slab allocator is
	 * used, after writing the tuple, pfree() the out-of-line data (not the
	 * SortTuple struct!), and increase state->availMem by the amount of
	 * memory space thereby released.
	 */
	SortWriteTuple	writetup;

	/*
	 * Function to read a stored tuple from tape back into memory. 'len' is
	 * the already-read length of the stored tuple.  The tuple is allocated
	 * from the slab memory arena, or is palloc'd, see readtup_alloc().
	 */
	SortReadTuple	readtup;

	tuplesort_report_duplicate_type tuplesort_report_duplicate;
	void						   *arg;

	/*
	 * This array holds the tuples now in sort memory.  If we are in state
	 * INITIAL, the tuples are in no particular order; if we are in state
	 * SORTEDINMEM, the tuples are in final sorted order; in states BUILDRUNS
	 * and FINALMERGE, the tuples are organized in "heap" order per Algorithm
	 * H.  In state SORTEDONTAPE, the array is not used.
	 */
	SortTuple  *memtuples;		/* array of SortTuple structs */
	int			memtupcount;	/* number of tuples currently present */
	int			memtupsize;		/* allocated length of memtuples array */
	bool		growmemtuples;	/* memtuples' growth still underway? */

	/*
	 * Memory for tuples is sometimes allocated using a simple slab allocator,
	 * rather than with palloc().  Currently, we switch to slab allocation
	 * when we start merging.  Merging only needs to keep a small, fixed
	 * number of tuples in memory at any time, so we can avoid the
	 * palloc/pfree overhead by recycling a fixed number of fixed-size slots
	 * to hold the tuples.
	 *
	 * For the slab, we use one large allocation, divided into SLAB_SLOT_SIZE
	 * slots.  The allocation is sized to have one slot per tape, plus one
	 * additional slot.  We need that many slots to hold all the tuples kept
	 * in the heap during merge, plus the one we have last returned from the
	 * sort, with tuplesort_gettuple.
	 *
	 * Initially, all the slots are kept in a linked list of free slots.  When
	 * a tuple is read from a tape, it is put to the next available slot, if
	 * it fits.  If the tuple is larger than SLAB_SLOT_SIZE, it is palloc'd
	 * instead.
	 *
	 * When we're done processing a tuple, we return the slot back to the free
	 * list, or pfree() if it was palloc'd.  We know that a tuple was
	 * allocated from the slab, if its pointer value is between
	 * slabMemoryBegin and -End.
	 *
	 * When the slab allocator is used, the USEMEM/LACKMEM mechanism of
	 * tracking memory usage is not used.
	 */
	bool		slabAllocatorUsed;

	char	   *slabMemoryBegin;	/* beginning of slab memory arena */
	char	   *slabMemoryEnd;	/* end of slab memory arena */
	SlabSlot   *slabFreeHead;	/* head of free list */

	/* Buffer size to use for reading input tapes, during merge. */
	size_t		read_buffer_size;

	/*
	 * When we return a tuple to the caller in tuplesort_gettuple_XXX, that
	 * came from a tape (that is, in TSS_SORTEDONTAPE or TSS_FINALMERGE
	 * modes), we remember the tuple in 'lastReturnedTuple', so that we can
	 * recycle the memory on next gettuple call.
	 */
	void	   *lastReturnedTuple;

	/*
	 * While building initial runs, this is the current output run number.
	 * Afterwards, it is the number of initial runs we made.
	 */
	int			currentRun;

	/*
	 * Unless otherwise noted, all pointer variables below are pointers to
	 * arrays of length maxTapes, holding per-tape data.
	 */

	/*
	 * This variable is only used during merge passes.  mergeactive[i] is true
	 * if we are reading an input run from (actual) tape number i and have not
	 * yet exhausted that run.
	 */
	bool	   *mergeactive;	/* active input run source? */

	/*
	 * Variables for Algorithm D.  Note that destTape is a "logical" tape
	 * number, ie, an index into the tp_xxx[] arrays.  Be careful to keep
	 * "logical" and "actual" tape numbers straight!
	 */
	int			Level;			/* Knuth's l */
	int			destTape;		/* current output tape (Knuth's j, less 1) */
	int		   *tp_fib;			/* Target Fibonacci run counts (A[]) */
	int		   *tp_runs;		/* # of real runs on each tape */
	int		   *tp_dummy;		/* # of dummy runs for each tape (D[]) */
	int		   *tp_tapenum;		/* Actual tape numbers (TAPE[]) */
	int			activeTapes;	/* # of active input tapes in merge pass */

	/*
	 * These variables are used after completion of sorting to keep track of
	 * the next tuple to return.  (In the tape case, the tape's current read
	 * position is also critical state.)
	 */
	int			result_tape;	/* actual tape number of finished output */
	int			current;		/* array index (only used if SORTEDINMEM) */
	bool		eof_reached;	/* reached EOF (needed for cursors) */

	/* markpos_xxx holds marked position for mark and restore */
	long		markpos_block;	/* tape block# (only used if SORTEDONTAPE) */
	int			markpos_offset; /* saved "current", or offset in tape block */
	bool		markpos_eof;	/* saved "eof_reached" */

	/*
	 * These variables are used during parallel sorting.
	 *
	 * worker is our worker identifier.  Follows the general convention that
	 * -1 value relates to a leader tuplesort, and values >= 0 worker
	 * tuplesorts. (-1 can also be a serial tuplesort.)
	 *
	 * shared is mutable shared memory state, which is used to coordinate
	 * parallel sorts.
	 *
	 * nParticipants is the number of worker Tuplesortstates known by the
	 * leader to have actually been launched, which implies that they must
	 * finish a run leader can merge.  Typically includes a worker state held
	 * by the leader process itself.  Set in the leader Tuplesortstate only.
	 */
	int			worker;
	Sharedsort *shared;
	int			nParticipants;

	/*
	 * The sortKeys variable is used by every case other than the hash index
	 * case; it is set by tuplesort_begin_xxx.  tupDesc is only used by the
	 * MinimalTuple and CLUSTER routines, though.
	 */
	TupleDesc	tupDesc;
	SortSupport sortKeys;		/* array of length nKeys */

	/*
	 * This variable is shared by the single-key MinimalTuple case and the
	 * Datum case (which both use qsort_ssup()).  Otherwise it's NULL.
	 */
	SortSupport onlyKey;

	/*
	 * Additional state for managing "abbreviated key" sortsupport routines
	 * (which currently may be used by all cases except the hash index case).
	 * Tracks the intervals at which the optimization's effectiveness is
	 * tested.
	 */
	int64		abbrevNext;		/* Tuple # at which to next check
								 * applicability */

	/*
	 * These variables are specific to the CLUSTER case; they are set by
	 * tuplesort_begin_cluster.
	 */
	struct IndexInfo *indexInfo;		/* info about index being used for reference */
	struct EState *estate;			/* for evaluating index expressions */

	/*
	 * These variables are specific to the IndexTuple case; they are set by
	 * tuplesort_begin_index_xxx and used only by the IndexTuple routines.
	 */
	Relation	heapRel;		/* table the index is being built on */
	Relation	indexRel;		/* index being built */

	/* These are specific to the index_btree subcase: */
	bool		enforceUnique;	/* complain if we find duplicate tuples */

	/* These are specific to the index_hash subcase: */
	uint32		high_mask;		/* masks for sortable part of hash code */
	uint32		low_mask;
	uint32		max_buckets;

	/*
	 * These variables are specific to the Datum case; they are set by
	 * tuplesort_begin_datum and used only by the DatumTuple routines.
	 */
	Oid			datumType;
	/* we need typelen in order to know how to copy the Datums. */
	int			datumTypeLen;

	/*
	 * Resource snapshot for time of sort start.
	 */
#ifdef TRACE_SORT
	PGRUsage	ru_start;
#endif
};

/* When using this macro, beware of double evaluation of len */
#define LogicalTapeReadExact(tapeset, tapenum, ptr, len) \
	do { \
		if (LogicalTapeRead(tapeset, tapenum, ptr, len) != (size_t) (len)) \
			elog(ERROR, "unexpected end of data"); \
	} while(0)

extern void *readtup_alloc(Tuplesortstate *state, Size tuplen);
extern bool consider_abort_common(Tuplesortstate *state);
extern Tuplesortstate *tuplesort_begin_heap(TupleDesc tupDesc,
											int nkeys, AttrNumber *attNums,
											Oid *sortOperators, Oid *sortCollations,
											bool *nullsFirstFlags,
											int workMem, SortCoordinate coordinate,
											bool randomAccess);
extern Tuplesortstate *tuplesort_begin_cluster(TupleDesc tupDesc,
											   Relation indexRel, int workMem,
											   SortCoordinate coordinate, bool randomAccess);
extern Tuplesortstate *tuplesort_begin_index_btree(Relation heapRel,
												   Relation indexRel,
												   bool enforceUnique,
												   int workMem, SortCoordinate coordinate,
												   bool randomAccess);
extern Tuplesortstate *tuplesort_begin_index_hash(Relation heapRel,
												  Relation indexRel,
												  uint32 high_mask,
												  uint32 low_mask,
												  uint32 max_buckets,
												  int workMem, SortCoordinate coordinate,
												  bool randomAccess);
extern Tuplesortstate *tuplesort_begin_index_gist(Relation heapRel,
												  Relation indexRel,
												  int workMem, SortCoordinate coordinate,
												  bool randomAccess);
extern Tuplesortstate *tuplesort_begin_datum(Oid datumType,
											 Oid sortOperator, Oid sortCollation,
											 bool nullsFirstFlag,
											 int workMem, SortCoordinate coordinate,
											 bool randomAccess);

extern void tuplesort_set_bound(Tuplesortstate *state, int64 bound);
extern bool tuplesort_used_bound(Tuplesortstate *state);

extern void tuplesort_puttuple_common(Tuplesortstate *state, SortTuple *tuple);
extern void tuplesort_puttupleslot(Tuplesortstate *state,
								   TupleTableSlot *slot);
extern void tuplesort_putheaptuple(Tuplesortstate *state, HeapTuple tup);
extern void tuplesort_putindextuplevalues(Tuplesortstate *state,
										  Relation rel, ItemPointer self,
										  Datum *values, bool *isnull);
extern void tuplesort_putdatum(Tuplesortstate *state, Datum val,
							   bool isNull);

extern void tuplesort_performsort(Tuplesortstate *state);

extern bool tuplesort_gettuple_common(Tuplesortstate *state, bool forward,
									  SortTuple *stup);
extern bool tuplesort_gettupleslot(Tuplesortstate *state, bool forward,
								   bool copy, TupleTableSlot *slot, Datum *abbrev);
extern HeapTuple tuplesort_getheaptuple(Tuplesortstate *state, bool forward);
extern IndexTuple tuplesort_getindextuple(Tuplesortstate *state, bool forward);
extern bool tuplesort_getdatum(Tuplesortstate *state, bool forward,
							   Datum *val, bool *isNull, Datum *abbrev);

extern bool tuplesort_skiptuples(Tuplesortstate *state, int64 ntuples,
								 bool forward);

extern void tuplesort_end(Tuplesortstate *state);

extern void tuplesort_reset(Tuplesortstate *state);

extern void tuplesort_get_stats(Tuplesortstate *state,
								TuplesortInstrumentation *stats);
extern const char *tuplesort_method_name(TuplesortMethod m);
extern const char *tuplesort_space_type_name(TuplesortSpaceType t);

extern int	tuplesort_merge_order(int64 allowedMem);

extern Size tuplesort_estimate_shared(int nworkers);
extern void tuplesort_initialize_shared(Sharedsort *shared, int nWorkers,
										dsm_segment *seg);
extern void tuplesort_attach_shared(Sharedsort *shared, dsm_segment *seg);

/*
 * These routines may only be called if randomAccess was specified 'true'.
 * Likewise, backwards scan in gettuple/getdatum is only allowed if
 * randomAccess was specified.  Note that parallel sorts do not support
 * randomAccess.
 */

extern void tuplesort_rescan(Tuplesortstate *state);
extern void tuplesort_markpos(Tuplesortstate *state);
extern void tuplesort_restorepos(Tuplesortstate *state);

extern Tuplesortstate *tuplesort_begin_custom(TupleDesc tupDesc,
							   bool enforceUnique,
							   int nkeys, SortSupport sortKeys,
							   int workMem, SortCoordinate coordinate,
							   bool randomAccess,
							   SortTupleComparator comparetup,
							   SortCopyTuple copytup,
							   SortWriteTuple writetup,
							   SortReadTuple readtup,
							   void *arg);

#endif							/* TUPLESORT_H */
