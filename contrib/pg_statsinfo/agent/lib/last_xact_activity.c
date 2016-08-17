/*
 * lib/last_xact_activity.c
 *     Track statement execution in current/last transaction.
 *
 * Copyright (c) 2009-2016, NIPPON TELEGRAPH AND TELEPHONE CORPORATION
 */

#include "postgres.h"
#include "access/heapam.h"
#include "storage/proc.h"
#include "funcapi.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"
#include "pgstat.h"
#include "utils/memutils.h"

#if PG_VERSION_NUM >= 90300
#include "access/htup_details.h"
#endif

#include "../common.h"
#include "pgut/pgut-be.h"

#ifndef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/*
 * A struct to store the queries per backend.
 */
typedef struct statEntry
{
	Oid userid;			/* Session user id						*/
	int pid;			/* Backend PID. 0 means inactive entry	*/
	TransactionId xid;	/* Current transaction id				*/
	bool inxact;		/* If this backend is in transaction	*/
	int change_count;	/* For consistency check				*/
	char *queries;		/* Pointer to query buffer				*/
	char *current;		/* Append point for query string.		*/
	char *tail;			/* Terminal point for query storing.	*/
} statEntry;

typedef struct statBuffer
{
	int max_id;				/* Maximum entry id for this buffer This is maximum
							 *  backend id for the shared buffer, and simply
							 *  number of entries for the snapshot.
							 */
	statEntry entries[1];	/* Arrays of the entries 			*/
} statBuffer;

/* Hook storage */
static shmem_startup_hook_type	prev_shmem_startup_hook = NULL;
static ExecutorStart_hook_type	prev_ExecutorStart_hook = NULL;
static ExecutorEnd_hook_type	prev_ExecutorEnd_hook = NULL;
#if PG_VERSION_NUM >= 90000
static ProcessUtility_hook_type	prev_ProcessUtility_hook = NULL;
#endif

/* Backend local variables */
static MemoryContext pglxaContext = NULL;
static statBuffer *stat_buffer_snapshot = NULL;
static int buffer_size_per_backend = 1000;
static statBuffer *stat_buffer = NULL;
static char *query_buffer = NULL;
static int query_length_limit = 100;
static bool record_xact_commands = false;
static bool free_localdata_on_execend = false;
#if PG_VERSION_NUM >= 90000
static bool immediate_exit_xact = false;
#endif

/* Module callbacks */
void		init_last_xact_activity(void);
void		fini_last_xact_activity(void);
Datum		statsinfo_last_xact_activity(PG_FUNCTION_ARGS);
void		last_xact_activity_clear_snapshot(void);

PG_FUNCTION_INFO_V1(statsinfo_last_xact_activity);

/* Internal functions */
static void clear_snapshot(void);
static void shmem_startup(void);
static void backend_shutdown_hook(int code, Datum arg);
static void myExecutorStart(QueryDesc *queryDesc, int eflags);
static void myExecutorEnd(QueryDesc *queryDesc);
static void attatch_shmem(void);
static void append_query(statEntry *entry, const char *query_string);
static void init_entry(int beid, Oid userid);
static char* get_query_entry(int beid);
static statEntry *get_stat_entry(int beid);
static void make_status_snapshot(void);
static statEntry *get_snapshot_entry(int beid);
static Size buffer_size(int nbackends);
#if PG_VERSION_NUM >= 90000
static void myProcessUtility0(Node *parsetree, const char *queryString);
#if PG_VERSION_NUM >= 90300
static void myProcessUtility(Node *parsetree, const char *queryString,
			   ProcessUtilityContext context, ParamListInfo params,
			   DestReceiver *dest, char *completionTag);
#else
static void myProcessUtility(Node *parsetree,
			   const char *queryString, ParamListInfo params, bool isTopLevel,
			   DestReceiver *dest, char *completionTag);
#endif
#endif


// static void errout(char* format, ...) {
// 	va_list list;
// 
// 	FILE *f = fopen("/tmp/errout", "a");
// 	if (f == NULL) return;
// 	
// 	va_start(list, format);
// 	vfprintf(f, format, list);
// 	va_end(list);
// 	fclose(f);
// }

#define TAKE_HOOK2(func, replace) \
	prev_##func##_hook = func##_hook; \
	func##_hook = replace;

#define TAKE_HOOK1(func) \
	TAKE_HOOK2(func, func);

#define RESTORE_HOOK(func) \
	func##_hook = prev_##func##_hook;

/*
 * Module load callbacks
 */
void
init_last_xact_activity(void)
{
	/* Custom GUC variables */
	DefineCustomIntVariable(GUC_PREFIX ".buffer_size",
							"Sets the query buffer size per backend.",
							NULL,
							&buffer_size_per_backend,
							buffer_size_per_backend,	/* default value */
							100,						/* minimum size  */
							INT_MAX,					/* maximum size  */
							PGC_POSTMASTER,
							0,
#if PG_VERSION_NUM >= 90100
							NULL,
#endif
							NULL,
							NULL);

	DefineCustomIntVariable(GUC_PREFIX ".query_length_limit",
							"Sets the limit of the length of each query to store.",
							NULL,
							&query_length_limit,
							query_length_limit,		/* default value */
							10,						/* minimum limit */
							INT_MAX,				/* maximum limit */
							PGC_SUSET,
							0,
#if PG_VERSION_NUM >= 90100
							NULL,
#endif
							NULL,
							NULL);

	DefineCustomBoolVariable(GUC_PREFIX ".record_xact_commands",
							 "Enables to store transaction commands.",
							 NULL,
							 &record_xact_commands,
							 record_xact_commands,	/* default value */
							 PGC_SUSET,
							 0,
#if PG_VERSION_NUM >= 90100
							 NULL,
#endif
							 NULL,
							 NULL);

	RequestAddinShmemSpace(buffer_size(MaxBackends));

	TAKE_HOOK1(shmem_startup);
	TAKE_HOOK2(ExecutorStart, myExecutorStart);
	TAKE_HOOK2(ExecutorEnd, myExecutorEnd);
#if PG_VERSION_NUM >= 90000
	TAKE_HOOK2(ProcessUtility, myProcessUtility);
#endif
}

/*
 * Module unload callback
 */
void
fini_last_xact_activity(void)
{
	/* Uninstall hooks. */
	RESTORE_HOOK(shmem_startup);
	RESTORE_HOOK(ExecutorStart);
	RESTORE_HOOK(ExecutorEnd);
#if PG_VERSION_NUM >= 90000
	RESTORE_HOOK(ProcessUtility);
#endif
}

/*
 * shmem_startup() - 
 *
 * Allocate or attach shared memory, and set up a process-exit hook function
 * for the buffer.
 */
static void
shmem_startup(void)
{
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	attatch_shmem();

	/*
	 * Invalidate entry for this backend on cleanup.
	 */
	on_shmem_exit(backend_shutdown_hook, 0);
}

/*
 * backend_shutdown_hook() -
 *
 * Invalidate status entry for this backend.
 */
static void
backend_shutdown_hook(int code, Datum arg)
{
	statEntry *entry = get_stat_entry(MyBackendId);
	if (entry)
		entry->pid = 0;
}

/*
 * myExecutorStart() - 
 *
 * Collect activity of SQL execution.
 */
static void
myExecutorStart(QueryDesc *queryDesc, int eflags)
{
	statEntry *entry;

	if (prev_ExecutorStart_hook)
		prev_ExecutorStart_hook(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	entry = get_stat_entry(MyBackendId);

	entry->change_count++;

	/*
	 * Single query executed when not in transaction.
	 */
	if (!entry->inxact)
	{
		init_entry(MyBackendId, GetSessionUserId());
		/*
		 * Remember to free activity snapshot on ExecutorEnd when we're out of
		 * transaction here.
		 */
		free_localdata_on_execend = true;
	}
	else
		free_localdata_on_execend = false;

	/*
	 * Do not change data when pid is inconsistent when transaction is active.
	 */
	if (!(entry->inxact && entry->pid != MyProc->pid))
	{
#if PG_VERSION_NUM >= 90200
		entry->xid = ProcGlobal->allPgXact[MyProc->pgprocno].xid;
#else
		entry->xid = MyProc->xid;
#endif
		append_query(entry, queryDesc->sourceText);
	}
	entry->change_count++;
	Assert((entry->change_count & 1) == 0);

	return;
}

/*
 * myExecutorEnd() -
 * 
 * Hook function for finish of SQL execution.
 */
static void
myExecutorEnd(QueryDesc * queryDesc)
{
	if (prev_ExecutorEnd_hook)
		prev_ExecutorEnd_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);

	if (free_localdata_on_execend)
		clear_snapshot();
}

#if PG_VERSION_NUM >= 90000
/*
 * Erase in-transaction flag if needed.
 */
static void
exit_transaction_if_needed()
{
	if (immediate_exit_xact)
	{
		statEntry *entry = get_stat_entry(MyBackendId);
		
		entry->inxact = false;
		immediate_exit_xact = false;
	}
}

static void
myProcessUtility0(Node *parsetree, const char *queryString)
{
	statEntry *entry;
	TransactionStmt *stmt;

	entry = get_stat_entry(MyBackendId);

	/*
	 * Initialize stat entry if I find that the PID of this backend has changed
	 * unexpectedly.
	 */
	if (MyProc->pid != 0 && entry->pid != MyProc->pid)
		init_entry(MyBackendId, GetSessionUserId());

	switch (nodeTag(parsetree))
	{
		case T_TransactionStmt:
			/*
			 * Process transaction statements.
			 */
			stmt = (TransactionStmt *)parsetree;
			switch (stmt->kind)
			{
				case TRANS_STMT_BEGIN:
					entry->change_count++;
					init_entry(MyBackendId, GetSessionUserId());
					entry->inxact = true;
					break;
				case TRANS_STMT_COMMIT:
				case TRANS_STMT_ROLLBACK:
				case TRANS_STMT_PREPARE:
				case TRANS_STMT_COMMIT_PREPARED:
				case TRANS_STMT_ROLLBACK_PREPARED:
					clear_snapshot();
					entry->change_count++;
					entry->inxact = false;
					break;
				default:
					return;
			}
			if (record_xact_commands)
				append_query(entry, queryString);
			break;

		case T_LockStmt:
		case T_IndexStmt:
		case T_VacuumStmt:
		case T_AlterTableStmt:
		case T_DropStmt:  /* Drop TABLE */
		case T_TruncateStmt:
		case T_ReindexStmt:
		case T_ClusterStmt:
			/*
			 * These statements are simplly recorded.
			 */
			entry->change_count++;

			/*
			 * Single query executed when not in transaction.
			 */
			if (!entry->inxact)
			{
				immediate_exit_xact = true;
				init_entry(MyBackendId, GetSessionUserId());
				entry->inxact = true;
			}

			append_query(entry, queryString);

			break;

		default:
			return;
	}

	entry->change_count++;
	Assert((entry->change_count & 1) == 0);
}

/*
 * myProcessUtility() -
 *
 * Processing transaction state change.
 */
#if PG_VERSION_NUM >= 90300
static void
myProcessUtility(Node *parsetree, const char *queryString,
				 ProcessUtilityContext context, ParamListInfo params,
				 DestReceiver *dest, char *completionTag)
{
	/*
	 * Do my process before other hook runs.
	 */
	myProcessUtility0(parsetree, queryString);

	PG_TRY();
	{
		if (prev_ProcessUtility_hook)
			prev_ProcessUtility_hook(parsetree, queryString, context, params,
									 dest, completionTag);
		else
			standard_ProcessUtility(parsetree, queryString, context, params,
									dest, completionTag);
	}
	PG_CATCH();
	{
		exit_transaction_if_needed();
		PG_RE_THROW();
	}
	PG_END_TRY();

	exit_transaction_if_needed();
}
#else
static void
myProcessUtility(Node *parsetree, const char *queryString,
				 ParamListInfo params, bool isTopLevel,
				 DestReceiver *dest, char *completionTag)
{
	/*
	 * Do my process before other hook runs.
	 */
	myProcessUtility0(parsetree, queryString);

	PG_TRY();
	{
		if (prev_ProcessUtility_hook)
			prev_ProcessUtility_hook(parsetree, queryString, params,
									 isTopLevel, dest, completionTag);
		else
			standard_ProcessUtility(parsetree, queryString, params,
									isTopLevel, dest, completionTag);
	}
	PG_CATCH();
	{
		exit_transaction_if_needed();
		PG_RE_THROW();
	}
	PG_END_TRY();

	exit_transaction_if_needed();
}
#endif
#endif

#define LAST_XACT_ACTIVITY_COLS		4

/*
 * statsinfo_last_xact_activity() -
 *
 * Retrieve queries of last transaction.
 */
Datum
statsinfo_last_xact_activity(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;

		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(LAST_XACT_ACTIVITY_COLS, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "pid",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "xid",
						   XIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "in_xact",
						   BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "queries",
						   TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		funcctx->user_fctx = palloc0(sizeof(int));

		/* Return zero rows if module has not been loaded properly. */
		if (!stat_buffer)
		{
			MemoryContextSwitchTo(oldcontext);
			funcctx = SRF_PERCALL_SETUP();
			SRF_RETURN_DONE(funcctx);
		}

		if (PG_NARGS() == 0 || PG_ARGISNULL(0))
		{
			make_status_snapshot();
			funcctx->max_calls = stat_buffer_snapshot->max_id;
		}
		else
		{
			/*
			 * Get one backend - locate by pid
			 * Returns zero rows when not found the pid.
			 */

			int pid = PG_GETARG_INT32(0);
			int *user_fctx = (int*)(funcctx->user_fctx);
			int i;

			make_status_snapshot();

			for (i = 1 ; i <= stat_buffer_snapshot->max_id; i++)
			{
				statEntry *entry = get_snapshot_entry(i);
				if (entry && entry->pid == pid)
				{
					*user_fctx = i;
					break;
				}
			}

			if (*user_fctx == 0)
				/* If not found, return zero rows */
				funcctx->max_calls = 0;
			else
				funcctx->max_calls = 1;
		}
		
		MemoryContextSwitchTo(oldcontext);
	}
				
	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		/* for each row */
		Datum		values[LAST_XACT_ACTIVITY_COLS];
		bool        nulls[LAST_XACT_ACTIVITY_COLS];
		HeapTuple	tuple;
		statEntry  *entry;
		int *user_fctx = (int*)funcctx->user_fctx;

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		/*
		 * *user_fctx > 0 when calling last_xact_activity with parameter
		 */
		if (*user_fctx > 0)
			entry = get_snapshot_entry(*user_fctx);
		else
			entry = get_snapshot_entry(funcctx->call_cntr + 1);
		

		values[0] = Int32GetDatum(entry->pid);
		if (entry->xid != 0)
			values[1] = TransactionIdGetDatum(entry->xid);
		else
			nulls[1] = true;
		values[2] = BoolGetDatum(entry->inxact);
		values[3] = CStringGetTextDatum(entry->queries);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		SRF_RETURN_DONE(funcctx);
	}
}

void
last_xact_activity_clear_snapshot(void)
{
	clear_snapshot();
}


/*
 * Local functions
 */

static void
clear_snapshot(void)
{
	if (pglxaContext)
	{
		MemoryContextDelete(pglxaContext);
		pglxaContext = NULL;
	}
			
	stat_buffer_snapshot = NULL;
}

/*
 * Create snap shot of last_xact_activity for stable return.
 */
static void
make_status_snapshot(void)
{
	volatile statEntry *entry;
	statEntry *local_entry;
	statBuffer *tmp_stat_buffer;
	char * local_queries;
	int nentries = 0;
	int beid;

	if (stat_buffer_snapshot) return;

	if (!stat_buffer) return;

	if (!pglxaContext)
		pglxaContext =
			AllocSetContextCreate(TopMemoryContext,
								  "Last activity snapshot",
								  ALLOCSET_SMALL_MINSIZE,
								  ALLOCSET_SMALL_INITSIZE,
								  ALLOCSET_SMALL_MAXSIZE);
	tmp_stat_buffer =
		(statBuffer*)MemoryContextAllocZero(pglxaContext,
											buffer_size(stat_buffer->max_id));
	local_queries =	(char*)(&tmp_stat_buffer->entries[stat_buffer->max_id]);

	entry = stat_buffer->entries;
	local_entry = tmp_stat_buffer->entries;

	for (beid = 1 ; beid <= stat_buffer->max_id ; beid++)
	{
		while (true)
		{
			int saved_change_count = entry->change_count;
		
			if (entry->pid > 0)
			{
				memcpy(local_entry, (char*)entry, sizeof(statEntry));
				if (superuser() || entry->userid == GetSessionUserId())
				{
					/*
					 * strcpy here is safe because the tail of buffer is always
					 * '\0'
					 */
					strcpy(local_queries, entry->queries);
				}
				else
				{
					strcpy(local_queries, "<command string not enabled>");
				}
				local_entry->queries = local_queries;
			}

			if (saved_change_count == entry->change_count &&
				(saved_change_count & 1) == 0)
				break;

			/* Make sure we can break out of loop if stuck. */
			CHECK_FOR_INTERRUPTS();
		}

		entry++;

		/* Only valid entries get included in the local array */
		if (local_entry->pid > 0)
		{
			local_entry++;
			local_queries += buffer_size_per_backend;
			nentries++;
		}
	}

	/*
	 * max_id of snapshot buffer is the number of valid entries.
	 */
	tmp_stat_buffer->max_id = nentries;
	stat_buffer_snapshot = tmp_stat_buffer;
}

/*
 * get_snapshot_entry() -
 *
 * get entry of snapshot. pos is 1-based position.
 */
static statEntry *
get_snapshot_entry(int pos)
{
	if (pos < 1 || pos > stat_buffer_snapshot->max_id) return NULL;

	return &stat_buffer_snapshot->entries[pos - 1];
}

/*
 * Append string to queries buffer.
 */
static void
append_query(statEntry *entry, const char *query_string)
{
	int query_length;
	int limited_length;
	bool add_ellipsis = false;

	limited_length = entry->tail - entry->current;

	if (limited_length > query_length_limit)
		limited_length = query_length_limit;

	query_length = strlen(query_string);
	
	if (query_length > limited_length)
	{
		limited_length -= 4;
		query_length = pg_mbcliplen(query_string, query_length, limited_length);
		if (query_length == 0) return;
		add_ellipsis = true;
	}
	else 
	{
		int tail;
		tail = pg_mbcliplen(query_string, query_length, query_length - 1);
		if (tail == query_length - 1 && query_string[tail] == ';')
			query_length--;
	}
		
	memcpy(entry->current, query_string, query_length);
	entry->current += query_length;
	if (add_ellipsis) {
		*(entry->current++) = '.';
		*(entry->current++) = '.';
		*(entry->current++) = '.';
	}
	*(entry->current++) = ';';
	*entry->current = '\0';
}

static Size
buffer_size(int nbackends)
{
	/* Calculate the size of statBuffer */
	Size struct_size = (Size)&(((statBuffer*)0)->entries[nbackends]);

	/* Calculate the size of query buffers*/
	Size query_buffer_size = mul_size(buffer_size_per_backend, nbackends);

	return add_size(struct_size, query_buffer_size);
}

static char*
get_query_entry(int beid)
{
	if (beid < 1 || beid > stat_buffer->max_id) return NULL;
	return query_buffer + buffer_size_per_backend * (beid - 1);
}

static statEntry *
get_stat_entry(int beid) {
	if (beid < 1 || beid > stat_buffer->max_id) return NULL;
	return &stat_buffer->entries[beid - 1];
}

static void
init_entry(int beid, Oid userid)
{
	statEntry *entry;
	entry = get_stat_entry(beid);
	if (MyProc)
	{
		entry->pid = MyProc->pid;
#if PG_VERSION_NUM >= 90200
		entry->xid = ProcGlobal->allPgXact[MyProc->pgprocno].xid;
#else
		entry->xid = MyProc->xid;
#endif
	}
	entry->userid = userid;
	entry->inxact = false;
	entry->queries = get_query_entry(beid);
	entry->current = entry->queries;
	entry->tail = entry->current + buffer_size_per_backend - 1;
	*(entry->current) = '\0';
	*(entry->tail) = '\0';		/* Stopper on snapshot */
}

static void
attatch_shmem(void)
{
	bool	found;
	int		bufsize;
	int		max_backends = MaxBackends;

	bufsize = buffer_size(max_backends);

	/*
	 * stat_buffer is used to determine that this module is enabled or not
	 * afterwards, assuming ShmemInitStruct returns NULL when failed to acquire
	 * shared memory.
	 */
	stat_buffer = (statBuffer*)ShmemInitStruct("last_xact_activity",
											  bufsize,
											  &found);

	if (!found)
	{
		int beid;

		MemSet(stat_buffer, 0, bufsize);
		query_buffer = (char*)(&stat_buffer->entries[max_backends]);
		stat_buffer->max_id = max_backends;
		for (beid = 1 ; beid <= max_backends ; beid++)
			init_entry(beid, 0);
	}
}
