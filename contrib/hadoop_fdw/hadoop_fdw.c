/*-------------------------------------------------------------------------
 *
 * PostgreSQL Foreign Data Wrapper for Hadoop
 *
 * Copyright (c) 2014-2016, BigSQL
 * Portions Copyright (c) 2012-2015, PostgreSQL Global Development Group & Others
 *
 * IDENTIFICATION
 *		  hadoop_fdw/hadoop_fdw.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "hadoop_fdw.h"

#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <libpq/pqsignal.h>
#include "funcapi.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "optimizer/var.h"
#include "utils/guc.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "mb/pg_wchar.h"
#include "optimizer/cost.h"
#include "storage/fd.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "storage/ipc.h"

#if (PG_VERSION_NUM >= 90200)
#include "optimizer/pathnode.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/planmain.h"
#endif

#include "jni.h"

#define Str(arg) #arg
#define StrValue(arg) Str(arg)
#define STR_PKGLIBDIR StrValue(PKG_LIB_DIR)

#if PG_VERSION_NUM >= 90500
#define HADOOP_FDW_IMPORT_API
#define HADOOP_FDW_JOIN_API
#else
#undef HADOOP_FDW_IMPORT_API
#undef HADOOP_FDW_JOIN_API
#endif  /* PG_VERSION_NUM >= 90500 */


PG_MODULE_MAGIC;

static JNIEnv *env;
static JavaVM *jvm;
jobject		java_call;
static bool InterruptFlag;		/* Used for checking for SIGINT interrupt */


/*
 * Describes the valid options for objects that use this wrapper.
 */
struct hadoopFdwOption
{
	const char *optname;
	Oid			optcontext;		/* Oid of catalog in which option may appear */
};

/*
 * Valid options for hadoop_fdw.
 *
 */
static struct hadoopFdwOption valid_options[] =
{

	/* Connection options */
	{"drivername", ForeignServerRelationId},
	{"url", ForeignServerRelationId},
	{"querytimeout", ForeignServerRelationId},
	{"jarfile", ForeignServerRelationId},
	{"maxheapsize", ForeignServerRelationId},
	{"username", UserMappingRelationId},
	{"password", UserMappingRelationId},
	{"query", ForeignTableRelationId},
	{"table", ForeignTableRelationId},
	{"host", ForeignServerRelationId},
	{"port", ForeignServerRelationId},
	{"schema", ForeignTableRelationId},

	/* Sentinel */
	{NULL, InvalidOid}
};

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */

typedef struct hadoopFdwExecutionState
{
	char	   *query;
	int			NumberOfRows;
	int			NumberOfColumns;
	jobject		java_call;
	List	   *retrieved_attrs;	/* list of retrieved attribute numbers */
} hadoopFdwExecutionState;

/*
 * SQL functions
 */
extern Datum hadoop_fdw_handler(PG_FUNCTION_ARGS);
extern Datum hadoop_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(hadoop_fdw_handler);
PG_FUNCTION_INFO_V1(hadoop_fdw_validator);

/*
 * FDW callback routines
 */
#if (PG_VERSION_NUM < 90200)
static FdwPlan *hadoopPlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel);
#endif

#if (PG_VERSION_NUM >= 90200)
static void hadoopGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void hadoopGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static ForeignScan *
hadoopGetForeignPlan(
					 PlannerInfo *root,
					 RelOptInfo *baserel,
					 Oid foreigntableid,
					 ForeignPath *best_path,
					 List *tlist,
					 List *scan_clauses
#if PG_VERSION_NUM >= 90500
					 , Plan *outer_plan
#endif  /* PG_VERSION_NUM >= 90500 */
);
#endif

static void hadoopExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void hadoopBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *hadoopIterateForeignScan(ForeignScanState *node);
static void hadoopReScanForeignScan(ForeignScanState *node);
static void hadoopEndForeignScan(ForeignScanState *node);
#ifdef HADOOP_FDW_IMPORT_API
static List *hadoopImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid);
#endif  /* HADOOP_FDW_IMPORT_API */
static hadoopFdwExecutionState *hadoopGetConnection(
				char *svr_username,
				char *svr_password,
				char *svr_host,
				int svr_port,
				char *svr_schema);
#ifdef HADOOP_FDW_JOIN_API
static bool foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel,
				JoinType jointype, RelOptInfo *outerrel, RelOptInfo *innerrel,
				JoinPathExtraData *extra);
static void hadoopGetForeignJoinPaths(PlannerInfo *root,
							RelOptInfo *joinrel,
							RelOptInfo *outerrel,
							RelOptInfo *innerrel,
							JoinType jointype,
							JoinPathExtraData *extra);
#endif /* HADOOP_FDW_JOIN_API */

/*
 * Helper functions
 */
static bool hadoopIsValidOption(const char *option, Oid context);
static void
hadoopGetServerOptions(
					   Oid serveroid,
					   int *querytimeout,
					   int *maxheapsize,
					   char **username,
					   char **password,
					   char **query,
					   char **host,
					   int *port);

static void hadoopGetTableOptions(
					  Oid foreigntableid,
					  char **table,
					  char **schema);


/*
 * Uses a String object's content to create an instance of C String
 */
static char *ConvertStringToCString(jobject);

/*
 * JVM Initialization function
 */
static void JVMInitialization(Oid);

/*
 * JVM destroy function
 */
static void DestroyJVM();

/*
 * SIGINT interrupt handler
 */
static void SIGINTInterruptHandler(int);

/*
 * SIGINT interrupt check and process function
 */
static void SIGINTInterruptCheckProcess();

/*
 * SIGINTInterruptCheckProcess
 *		Checks and processes if SIGINT interrupt occurs
 */
static void
SIGINTInterruptCheckProcess()
{
	if (InterruptFlag == true)
	{
		jclass		HadoopJDBCUtilsClass;
		jmethodID	id_cancel;
		jstring		cancel_result = NULL;
		char	   *cancel_result_cstring = NULL;

		HadoopJDBCUtilsClass = (*env)->FindClass(env, "HadoopJDBCUtils");
		if (HadoopJDBCUtilsClass == NULL)
		{
			elog(ERROR, "HadoopJDBCUtilsClass is NULL");
		}

		id_cancel = (*env)->GetMethodID(env, HadoopJDBCUtilsClass, "Cancel", "()Ljava/lang/String;");
		if (id_cancel == NULL)
		{
			elog(ERROR, "id_cancel is NULL");
		}

		cancel_result = (*env)->CallObjectMethod(env, java_call, id_cancel);
		if (cancel_result != NULL)
		{
			cancel_result_cstring = ConvertStringToCString((jobject) cancel_result);
			elog(ERROR, "%s", cancel_result_cstring);
		}

		InterruptFlag = false;
		elog(ERROR, "Query has been cancelled");

		(*env)->ReleaseStringUTFChars(env, cancel_result, cancel_result_cstring);
		(*env)->DeleteLocalRef(env, cancel_result);
	}
}

/*
 * ConvertStringToCString
 *		Uses a String object passed as a jobject to the function to
 *		create an instance of C String.
 */
static char *
ConvertStringToCString(jobject java_cstring)
{
	jclass		JavaString;
	char	   *StringPointer;

	SIGINTInterruptCheckProcess();

	JavaString = (*env)->FindClass(env, "java/lang/String");
	if (!((*env)->IsInstanceOf(env, java_cstring, JavaString)))
	{
		elog(ERROR, "Object not an instance of String class");
	}

	if (java_cstring != NULL)
	{
		StringPointer = (char *) (*env)->GetStringUTFChars(env, (jstring) java_cstring, 0);
	}
	else
	{
		StringPointer = NULL;
	}

	return (StringPointer);
}

/*
 * DestroyJVM
 *		Shuts down the JVM.
 */
static void
DestroyJVM()
{
	(*jvm)->DestroyJavaVM(jvm);
}

/*
 * JVMInitialization
 *		Create the JVM which will be used for calling the Java routines
 *			that use HADOOP to connect and access the foreign database.
 *
 */
static void
JVMInitialization(Oid serveroid)
{
	jint		res = -5;		/* Initializing the value of res so that we
								 * can check it later to see whether JVM has
								 * been correctly created or not */
	JavaVMInitArgs vm_args;
	JavaVMOption *options;
	static bool FunctionCallCheck = false;		/* This flag safeguards
												 * against multiple calls of
												 * JVMInitialization(). */
	char	   *classpath;
	char	   *svr_username = NULL;
	char	   *svr_password = NULL;
	char	   *svr_query = NULL;
	char	   *svr_host = NULL;
	int			svr_port = 0;
	char	   *maxheapsizeoption = NULL;
	int			svr_querytimeout = 0;
	int			svr_maxheapsize = 0;
	char	   *var_CP = NULL;
	int			cp_len = 0;

	hadoopGetServerOptions(
						   serveroid,
						   &svr_querytimeout,
						   &svr_maxheapsize,
						   &svr_username,
						   &svr_password,
						   &svr_query,
						   &svr_host,
						   &svr_port
		);



	SIGINTInterruptCheckProcess();

	if (FunctionCallCheck == false)
	{

		var_CP = getenv("HADOOP_FDW_CLASSPATH");

		if (!var_CP)
		{
			elog(ERROR, "Please set the environment variable HADOOP_FDW_CLASSPATH");
		}

		cp_len = strlen(var_CP) + 25;
		classpath = (char *) palloc(cp_len);
		snprintf(classpath, cp_len,
#if defined(__MINGW64__) || defined(WIN32)
		         "-Djava.class.path=%s",
#else
		         "-Djava.class.path=%s",
#endif /* defined(__MINGW64__) || defined(WIN32) */
		         var_CP);

		if (svr_maxheapsize != 0)		/* If the user has given a value for
										 * setting the max heap size of the
										 * JVM */
		{
			options = (JavaVMOption *) palloc(sizeof(JavaVMOption) * 2);
			maxheapsizeoption = (char *) palloc(sizeof(int) + 6);
			snprintf(maxheapsizeoption, sizeof(int) + 6, "-Xmx%dm", svr_maxheapsize);

			options[0].optionString = classpath;
			options[1].optionString = maxheapsizeoption;
			vm_args.nOptions = 2;
		}
		else
		{
			options = (JavaVMOption *) palloc(sizeof(JavaVMOption));
			options[0].optionString = classpath;
			vm_args.nOptions = 1;
		}

		vm_args.version = 0x00010002;
		vm_args.options = options;
		vm_args.ignoreUnrecognized = JNI_FALSE;

		/* Create the Java VM */
		res = JNI_CreateJavaVM(&jvm, (void **) &env, &vm_args);
		if (res < 0)
		{
			ereport(ERROR,
					(errmsg("Failed to create Java VM")
					 ));
		}

		InterruptFlag = false;
		/* Register an on_proc_exit handler that shuts down the JVM. */
		on_proc_exit(DestroyJVM, 0);
		FunctionCallCheck = true;
	}
}

/*
 * SIGINTInterruptHandler
 *		Handles SIGINT interrupt
 */
static void
SIGINTInterruptHandler(int sig)
{
	InterruptFlag = true;
}

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
hadoop_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

#if (PG_VERSION_NUM < 90200)
	fdwroutine->PlanForeignScan = hadoopPlanForeignScan;
#endif

#if (PG_VERSION_NUM >= 90200)
	fdwroutine->GetForeignRelSize = hadoopGetForeignRelSize;
	fdwroutine->GetForeignPaths = hadoopGetForeignPaths;
	fdwroutine->GetForeignPlan = hadoopGetForeignPlan;
#endif

	fdwroutine->ExplainForeignScan = hadoopExplainForeignScan;
	fdwroutine->BeginForeignScan = hadoopBeginForeignScan;
	fdwroutine->IterateForeignScan = hadoopIterateForeignScan;
	fdwroutine->ReScanForeignScan = hadoopReScanForeignScan;
	fdwroutine->EndForeignScan = hadoopEndForeignScan;
#ifdef HADOOP_FDW_IMPORT_API
	fdwroutine->ImportForeignSchema = hadoopImportForeignSchema;
#endif  /* HADOOP_FDW_IMPORT_API */
#ifdef HADOOP_FDW_JOIN_API
	/* Support functions for join push-down */
	fdwroutine->GetForeignJoinPaths = hadoopGetForeignJoinPaths;
#endif  /* HADOOP_FDW_JOIN_API */
	pqsignal(SIGINT, SIGINTInterruptHandler);

	PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses hadoop_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum
hadoop_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	char	   *svr_drivername = NULL;
	char	   *svr_url = NULL;
	char	   *svr_username = NULL;
	char	   *svr_password = NULL;
	char	   *svr_query = NULL;
	char	   *svr_table = NULL;
	char	   *svr_jarfile = NULL;
	char	   *svr_schema = NULL;
	int			svr_querytimeout = 0;
	int			svr_maxheapsize = 0;
	ListCell   *cell;
	char	   *svr_host = NULL;
	int			svr_port = 0;

	/*
	 * Check that only options supported by hadoop_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (!hadoopIsValidOption(def->defname, catalog))
		{
			struct hadoopFdwOption *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
									 opt->optname);
			}

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname),
					 errhint("Valid options in this context are: %s", buf.len ? buf.data : "<none>")
					 ));
		}
		if (strcmp(def->defname, "host") == 0)
		{
			if (svr_host)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options: jarfile (%s)", defGetString(def))
								));

			svr_host = defGetString(def);
		}

		if (strcmp(def->defname, "port") == 0)
		{
			if (svr_port)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options: maxheapsize (%s)", defGetString(def))
								));

			svr_port = atoi(defGetString(def));
		}

		if (strcmp(def->defname, "drivername") == 0)
		{
			if (svr_drivername)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options: drivername (%s)", defGetString(def))
								));

			svr_drivername = defGetString(def);
		}

		if (strcmp(def->defname, "url") == 0)
		{
			if (svr_url)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: url (%s)", defGetString(def))
						 ));

			svr_url = defGetString(def);
		}

		if (strcmp(def->defname, "querytimeout") == 0)
		{
			if (svr_querytimeout)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options: querytimeout (%s)", defGetString(def))
								));

			svr_querytimeout = atoi(defGetString(def));
		}

		if (strcmp(def->defname, "jarfile") == 0)
		{
			if (svr_jarfile)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options: jarfile (%s)", defGetString(def))
								));

			svr_jarfile = defGetString(def);
		}

		if (strcmp(def->defname, "maxheapsize") == 0)
		{
			if (svr_maxheapsize)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options: maxheapsize (%s)", defGetString(def))
								));

			svr_maxheapsize = atoi(defGetString(def));
		}

		if (strcmp(def->defname, "username") == 0)
		{
			if (svr_username)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options: username (%s)", defGetString(def))
								));

			svr_username = defGetString(def);
		}

		if (strcmp(def->defname, "password") == 0)
		{
			if (svr_password)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("conflicting or redundant options: password (%s)", defGetString(def))
								));

			svr_password = defGetString(def);
		}
		else if (strcmp(def->defname, "query") == 0)
		{
			if (svr_table)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("conflicting options: query cannot be used with table")
						 ));

			if (svr_query)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: query (%s)", defGetString(def))
						 ));

			svr_query = defGetString(def);
		}
		else if (strcmp(def->defname, "table") == 0)
		{
			if (svr_query)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("conflicting options: table cannot be used with query")
						 ));

			if (svr_table)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options: table (%s)", defGetString(def))
						 ));

			svr_table = defGetString(def);
		}
		else if (strcmp(def->defname, "schema") == 0)
		{
			if (svr_schema)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			svr_schema = defGetString(def);
		}

	}

	if (catalog == ForeignServerRelationId && svr_host == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("HOST name  must be specified")
				 ));
	}

	if (catalog == ForeignServerRelationId && svr_port == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PORT number must be specified")
				 ));
	}

	if (catalog == ForeignTableRelationId && svr_query == NULL && svr_table == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("either a table or a query must be specified")
				 ));
	}

	PG_RETURN_VOID();
}

/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
hadoopIsValidOption(const char *option, Oid context)
{
	struct hadoopFdwOption *opt;

	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}
	return false;
}

/*
 * Fetch the options for a hadoop_fdw foreign table.
 */
static void
hadoopGetTableOptions(Oid foreigntableid, char **table, char **schema)
{
	ForeignTable *f_table;
	List	   *options;
	ListCell   *lc;

	f_table = GetForeignTable(foreigntableid);

	options = NIL;
	options = list_concat(options, f_table->options);

	/* Loop through the options, and get the server/port */
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "table") == 0)
		{
			*table = defGetString(def);
		}
		if (strcmp(def->defname, "schema") == 0)
		{
			*schema = defGetString(def);
		}
	}
}

/*
 * Fetch the options for the hadoop_fdw foreign server.
 */
static void
hadoopGetServerOptions(Oid serveroid, int *querytimeout, int *maxheapsize, char **username, char **password, char **query, char **host, int *port)
{
	ForeignServer *f_server;
	UserMapping *f_mapping;
	List	   *options;
	ListCell   *lc;

	/*
	 * Extract options from FDW objects.
	 */
	f_server = GetForeignServer(serveroid);
	f_mapping = GetUserMapping(GetUserId(), serveroid);

	options = NIL;
	options = list_concat(options, f_server->options);
	options = list_concat(options, f_mapping->options);

	/* Loop through the options, and get the server/port */
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "host") == 0)
		{
			*host = defGetString(def);
		}
		if (strcmp(def->defname, "port") == 0)
		{
			*port = atoi(defGetString(def));
		}
		if (strcmp(def->defname, "username") == 0)
		{
			*username = defGetString(def);
		}
		if (strcmp(def->defname, "querytimeout") == 0)
		{
			*querytimeout = atoi(defGetString(def));
		}
		if (strcmp(def->defname, "maxheapsize") == 0)
		{
			*maxheapsize = atoi(defGetString(def));
		}
		if (strcmp(def->defname, "password") == 0)
		{
			*password = defGetString(def);
		}
		if (strcmp(def->defname, "query") == 0)
		{
			*query = defGetString(def);
		}
	}
}
#if (PG_VERSION_NUM < 90200)
/*
 * hadoopPlanForeignScan
 *		Create a FdwPlan for a scan on the foreign table
 */
static FdwPlan *
hadoopPlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel)
{
	FdwPlan    *fdwplan = NULL;
	char	   *svr_drivername = NULL;
	char	   *svr_username = NULL;
	char	   *svr_password = NULL;
	char	   *svr_query = NULL;
	char	   *svr_table = NULL;
	char	   *svr_url = NULL;
	char	   *svr_jarfile = NULL;
	char	   *svr_schema = NULL;
	int			svr_querytimeout = 0;
	int			svr_maxheapsize = 0;
	char	   *query;
	char	   *svr_host = NULL;
	int			svr_port = 0;
	ForeignTable *f_table = NULL;

	SIGINTInterruptCheckProcess();

	f_table = GetForeignTable(foreigntableid);
	JVMInitialization(f_table->serverid);

	fdwplan = makeNode(FdwPlan);

	/* Fetch options */
	hadoopGetServerOptions(
						   f_table->serverid,
						   &svr_querytimeout,
						   &svr_maxheapsize,
						   &svr_username,
						   &svr_password,
						   &svr_query,
						   &svr_host,
						   &svr_port
		);
	hadoopGetTableOptions(foreigntableid, &svr_table, &svr_schema);
	/* Build the query */
	if (svr_query)
	{
		size_t		len = strlen(svr_query) + 9;

		query = (char *) palloc(len);
		snprintf(query, len, "EXPLAIN %s", svr_query);
	}
	else
	{
		size_t		len = strlen(svr_table) + 23;

		query = (char *) palloc(len);
		snprintf(query, len, "EXPLAIN SELECT * FROM %s", svr_table);
	}

	return (fdwplan);
}
#endif

/*
 * hadoopExplainForeignScan
 *		Produce extra output for EXPLAIN
 */
static void
hadoopExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	char	   *svr_username = NULL;
	char	   *svr_password = NULL;
	char	   *svr_query = NULL;
	int			svr_querytimeout = 0;
	int			svr_maxheapsize = 0;
	char	   *svr_host = NULL;
	int			svr_port = 0;
	ForeignTable *f_table = NULL;

	f_table = GetForeignTable(RelationGetRelid(node->ss.ss_currentRelation));

	elog(DEBUG3, HADOOP_FDW_NAME ": explain foreign scan for relation ID %d",
		 RelationGetRelid(node->ss.ss_currentRelation));

	/* Fetch options  */
	hadoopGetServerOptions(
						   f_table->serverid,
						   &svr_querytimeout,
						   &svr_maxheapsize,
						   &svr_username,
						   &svr_password,
						   &svr_query,
						   &svr_host,
						   &svr_port
		);

	SIGINTInterruptCheckProcess();
}

/*
 * hadoopBeginForeignScan
 *		Initiate access to the database
 */
static void
hadoopBeginForeignScan(ForeignScanState *node, int eflags)
{
	char	   *svr_username = NULL;
	char	   *svr_password = NULL;
	char	   *svr_query = NULL;
	char	   *svr_table = NULL;
	char	   *svr_schema = NULL;
	int			svr_querytimeout = 0;
	int			svr_maxheapsize = 0;
	hadoopFdwExecutionState *festate;
	char	   *query;
	jclass		HadoopJDBCUtilsClass;
	jstring		initialize_result = NULL;
	jmethodID	id_initialize;
	jfieldID	id_numberofcolumns;
	char	   *initialize_result_cstring = NULL;
	char	   *svr_host = NULL;
	int			svr_port = 0;
	Oid			foreigntableid;
	jstring		name;

	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	Oid serverid;
#ifndef HADOOP_FDW_JOIN_API
	ForeignTable *f_table = NULL;
#endif /* HADOOP_FDW_JOIN_API */
	SIGINTInterruptCheckProcess();

#ifdef HADOOP_FDW_JOIN_API
	serverid = intVal(list_nth(fsplan->fdw_private, 2));
	if (fsplan->scan.scanrelid > 0)
	{
		elog(DEBUG3, HADOOP_FDW_NAME ": begin foreign scan for relation ID %d",
			 RelationGetRelid(node->ss.ss_currentRelation));

		/* Fetch options  */
		foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);

	}
	else
		foreigntableid = intVal(list_nth(fsplan->fdw_private, 3));
#else
	foreigntableid = RelationGetRelid(node->ss.ss_currentRelation);
	f_table = GetForeignTable(foreigntableid);
	serverid = f_table->serverid;
#endif /* HADOOP_FDW_JOIN_API */
	hadoopGetTableOptions(foreigntableid, &svr_table, &svr_schema);
	hadoopGetServerOptions(serverid,
						   &svr_querytimeout,
						   &svr_maxheapsize,
						   &svr_username,
						   &svr_password,
						   &svr_query,
						   &svr_host,
						   &svr_port
		);

	festate = hadoopGetConnection(svr_username, svr_password, svr_host, svr_port, svr_schema);

	query = strVal(list_nth(fsplan->fdw_private, 0));

	elog(DEBUG1, "hadoop_fdw: Starting Query: %s", query);

	node->fdw_state = (void *) festate;
/*	festate->result = NULL; */
	festate->query = query;
	festate->retrieved_attrs = (List *) list_nth(fsplan->fdw_private, 1);
	festate->NumberOfColumns = 0;
	festate->NumberOfRows = 0;

	/* Connect to the server and execute the query */
	HadoopJDBCUtilsClass = (*env)->FindClass(env, "HadoopJDBCUtils");
	if (HadoopJDBCUtilsClass == NULL)
	{
		elog(ERROR, "HadoopJDBCUtilsClass is NULL");
	}

	id_initialize = (*env)->GetMethodID(env, HadoopJDBCUtilsClass, "Execute_Query", "(Ljava/lang/String;)Ljava/lang/String;");
	if (id_initialize == NULL)
	{
		elog(ERROR, "id_initialize is NULL");
	}

	id_numberofcolumns = (*env)->GetFieldID(env, HadoopJDBCUtilsClass, "NumberOfColumns", "I");
	if (id_numberofcolumns == NULL)
	{
		elog(ERROR, "id_numberofcolumns is NULL");
	}

	if (java_call == NULL)
	{
		elog(ERROR, "java_call is NULL");
	}

	name = (*env)->NewStringUTF(env, query);
	initialize_result = (*env)->CallObjectMethod(env, java_call, id_initialize, name);
	if (initialize_result != NULL)
	{
		initialize_result_cstring = ConvertStringToCString((jobject) initialize_result);
		elog(ERROR, "%s", initialize_result_cstring);
	}

	node->fdw_state = (void *) festate;
	festate->NumberOfColumns = (*env)->GetIntField(env, java_call, id_numberofcolumns);
}

/*
 * hadoopIterateForeignScan
 *		Read next record from the data file and store it into the
 *		ScanTupleSlot as a virtual tuple
 */
static TupleTableSlot *
hadoopIterateForeignScan(ForeignScanState *node)
{
	char	  **values;
	HeapTuple	tuple;
	jmethodID	id_returnresultset;
	jclass		HadoopJDBCUtilsClass;
	jobjectArray java_rowarray;
	int			i = 0;
	int			j = 0;
	jstring		tempString;
	hadoopFdwExecutionState *festate = (hadoopFdwExecutionState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	jobject		java_call = festate->java_call;
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;

	/* Cleanup */
	ExecClearTuple(slot);

	SIGINTInterruptCheckProcess();

	if ((*env)->PushLocalFrame(env, (festate->NumberOfColumns + 10)) < 0)
	{
		/* frame not pushed, no PopLocalFrame needed */
		elog(ERROR, "Error");
	}


	HadoopJDBCUtilsClass = (*env)->FindClass(env, "HadoopJDBCUtils");
	if (HadoopJDBCUtilsClass == NULL)
	{
		elog(ERROR, "HadoopJDBCUtilsClass is NULL");
	}

	id_returnresultset = (*env)->GetMethodID(env, HadoopJDBCUtilsClass, "ReturnResultSet", "()[Ljava/lang/String;");
	if (id_returnresultset == NULL)
	{
		elog(ERROR, "id_returnresultset is NULL");
	}

	values = (char **) palloc(sizeof(char *) * (festate->NumberOfColumns));

	java_rowarray = (*env)->CallObjectMethod(env, java_call, id_returnresultset);

	if (java_rowarray != NULL)
	{

		for (i = 0; i < (festate->NumberOfColumns); i++)
		{
			values[i] = ConvertStringToCString((jobject) (*env)->GetObjectArrayElement(env, java_rowarray, i));
		}

		if (fsplan->scan.scanrelid > 0)
			tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(node->ss.ss_currentRelation->rd_att), values);
		else
			tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(node->ss.ss_ScanTupleSlot->tts_tupleDescriptor), values);

		ExecStoreTuple(tuple, slot, InvalidBuffer, false);
		++(festate->NumberOfRows);

		for (j = 0; j < festate->NumberOfColumns; j++)
		{
			tempString = (jstring) (*env)->GetObjectArrayElement(env, java_rowarray, j);
			(*env)->ReleaseStringUTFChars(env, tempString, values[j]);
			(*env)->DeleteLocalRef(env, tempString);
		}

		(*env)->DeleteLocalRef(env, java_rowarray);
	}

	(*env)->PopLocalFrame(env, NULL);

	return (slot);
}

/*
 * hadoopEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
hadoopEndForeignScan(ForeignScanState *node)
{
	jmethodID	id_close;
	jclass		HadoopJDBCUtilsClass;
	jstring		close_result = NULL;
	char	   *close_result_cstring = NULL;
	hadoopFdwExecutionState *festate = (hadoopFdwExecutionState *) node->fdw_state;
	jobject		java_call = festate->java_call;
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;

	SIGINTInterruptCheckProcess();

	if (fsplan->scan.scanrelid > 0)
	{
		elog(DEBUG3, HADOOP_FDW_NAME ": end foreign scan for relation ID %d",
			 RelationGetRelid(node->ss.ss_currentRelation));
	}


	HadoopJDBCUtilsClass = (*env)->FindClass(env, "HadoopJDBCUtils");
	if (HadoopJDBCUtilsClass == NULL)
	{
		elog(ERROR, "HadoopJDBCUtilsClass is NULL");
	}

	id_close = (*env)->GetMethodID(env, HadoopJDBCUtilsClass, "Close", "()Ljava/lang/String;");
	if (id_close == NULL)
	{
		elog(ERROR, "id_close is NULL");
	}

	close_result = (*env)->CallObjectMethod(env, java_call, id_close);
	if (close_result != NULL)
	{
		close_result_cstring = ConvertStringToCString((jobject) close_result);
		elog(ERROR, "%s", close_result_cstring);
	}
	if (festate->query)
	{
		pfree(festate->query);
		festate->query = 0;
	}

	(*env)->ReleaseStringUTFChars(env, close_result, close_result_cstring);
	(*env)->DeleteLocalRef(env, close_result);
	(*env)->DeleteGlobalRef(env, java_call);
}

/*
 * hadoopReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
hadoopReScanForeignScan(ForeignScanState *node)
{
	SIGINTInterruptCheckProcess();
}

#if (PG_VERSION_NUM >= 90200)
/*
 * hadoopGetForeignPaths
 *		(9.2+) Get the foreign paths
 */
static void
hadoopGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	Cost		startup_cost = 0;
	Cost		total_cost = 0;

	SIGINTInterruptCheckProcess();

	elog(DEBUG3, HADOOP_FDW_NAME
		 ": get foreign paths for relation ID %d", foreigntableid);

	/* Create a ForeignPath node and add it as only possible path */
#if PG_VERSION_NUM < 90500
	add_path(baserel, (Path *) create_foreignscan_path(root, baserel, baserel->rows, startup_cost, total_cost, NIL, NULL, NIL));
#elif PG_VERSION_NUM < 90600
	add_path(baserel, (Path *) create_foreignscan_path(root, baserel, baserel->rows, startup_cost, total_cost, NIL, NULL, NULL, NIL));
#else
	add_path(baserel, (Path *) create_foreignscan_path(root, baserel, NULL, baserel->rows, startup_cost, total_cost, NIL, NULL, NULL, NIL));
#endif /* PG_VERSION_NUM < 90500 */
}

/*
 * hadoopGetForeignPlan
 *		(9.2+) Get a foreign scan plan node
 */
static ForeignScan *
hadoopGetForeignPlan(PlannerInfo *root,
					 RelOptInfo *baserel,
					 Oid foreigntableid,
					 ForeignPath *best_path,
					 List *tlist,
					 List *scan_clauses
#if PG_VERSION_NUM >= 90500
					 ,Plan *outer_plan
#endif   /* PG_VERSION_NUM >= 90500 */
)
{
	/*
	 * Create a ForeignScan plan node from the selected foreign access path.
	 * This is called at the end of query planning. The parameters are as for
	 * GetForeignRelSize, plus the selected ForeignPath (previously produced
	 * by GetForeignPaths), the target list to be emitted by the plan node,
	 * and the restriction clauses to be enforced by the plan node.
	 *
	 * This function must create and return a ForeignScan plan node; it's
	 * recommended to use make_foreignscan to build the ForeignScan node.
	 *
	 */
	List	   *fdw_private;
	List	   *remote_conds = NIL;
	List	   *local_exprs = NIL;
	List	   *params_list = NIL;
	List	   *retrieved_attrs;
	StringInfoData sql;
	ListCell   *lc;
	List	   *fdw_scan_tlist = NIL;

	Index		scan_relid = baserel->relid;
#ifndef HADOOP_FDW_JOIN_API
	ForeignTable *f_table;
#endif /* HADOOP_FDW_JOIN_API */
	hadoopFdwRelationInfo *fpinfo = (hadoopFdwRelationInfo *) baserel->fdw_private;

	elog(DEBUG3, HADOOP_FDW_NAME
		 ": get foreign plan for relation ID %d", foreigntableid);

	/*
	 * For base relations, set scan_relid as the relid of the relation. For
	 * other kinds of relations set it to 0.
	 */
	if (baserel->reloptkind == RELOPT_BASEREL ||
		baserel->reloptkind == RELOPT_OTHER_MEMBER_REL)
		scan_relid = baserel->relid;
	else
	{
		scan_relid = 0;

		/*
		 * create_scan_plan() and create_foreignscan_plan() pass
		 * rel->baserestrictinfo + parameterization clauses through
		 * scan_clauses. For a join rel->baserestrictinfo is NIL and we are
		 * not considering parameterization right now, so there should be no
		 * scan_clauses for a joinrel.
		 */
		Assert(!scan_clauses);
	}

#ifdef HADOOP_FDW_JOIN_API
	JVMInitialization(baserel->serverid);
#else
	Assert(foreigntableid);
	f_table = GetForeignTable(foreigntableid);
	JVMInitialization(f_table->serverid);
#endif /* HADOOP_FDW_JOIN_API */
	foreach(lc, scan_clauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		Assert(IsA(rinfo, RestrictInfo));

		/* Ignore any pseudoconstants, they're dealt with elsewhere */
		if (rinfo->pseudoconstant)
			continue;

		if (list_member_ptr(fpinfo->remote_conds, rinfo))
		{
			remote_conds = lappend(remote_conds, rinfo);
		}
		else if (list_member_ptr(fpinfo->local_conds, rinfo))
			local_exprs = lappend(local_exprs, rinfo->clause);
		else
		{
			Assert(is_foreign_expr(root, baserel, rinfo->clause));
			remote_conds = lappend(remote_conds, rinfo);
		}
	}

	if (baserel->reloptkind == RELOPT_JOINREL)
	{
		/* For a join relation, get the conditions from fdw_private structure */
		remote_conds = fpinfo->remote_conds;
		local_exprs = fpinfo->local_conds;

		/* Build the list of columns to be fetched from the foreign server. */
		fdw_scan_tlist = build_tlist_to_deparse(baserel);

		/*
		 * Ensure that the outer plan produces a tuple whose descriptor
		 * matches our scan tuple slot. This is safe because all scans and
		 * joins support projection, so we never need to insert a Result node.
		 * Also, remove the local conditions from outer plan's quals, lest
		 * they will be evaluated twice, once by the local plan and once by
		 * the scan.
		 */
	}
	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */
	initStringInfo(&sql);
	deparseSelectStmtForRel(&sql, root, baserel, remote_conds, &retrieved_attrs, &params_list,
							fpinfo, fdw_scan_tlist);

	elog(DEBUG1, HADOOP_FDW_NAME ": built HiveQL:\n\n%s\n", sql.data);

#ifdef HADOOP_FDW_JOIN_API
	/*
	 * When it is a join relation the foreigntableid passed to hadoopGetForeignPlan
	 * is zero. We cannot obtain the serverid from this relation so we add the serverid
	 * to the fdw_private here so we can use this in hadoopGetForeignPaln. Likewise
	 * we also need to know the schema to use when we obtain the Hive connection
	 * we add the foreigntableid as well so that we can get the table options
	 * from this foreigntableid. Since it is assumed that all the relations
	 * involved in the joins belong to the same server and to the same schema it is
	 * irrelevant which of the foreign tables makes it in the fdw_private list.
	 */
	fdw_private = list_make4(makeString(sql.data),
							 retrieved_attrs,
							 makeInteger(baserel->serverid),
							 makeInteger(fpinfo->foreigntableid));
#else
	fdw_private = list_make2(makeString(sql.data),
							 retrieved_attrs);
#endif /* HADOOP_FDW_JOIN_API */
	/* Create the ForeignScan node */
#if PG_VERSION_NUM >= 90500
	return make_foreignscan(tlist, local_exprs, scan_relid, params_list, fdw_private, fdw_scan_tlist, NIL, (Plan *) NIL);
#else
	return make_foreignscan(tlist, local_exprs, scan_relid, params_list, fdw_private);
#endif
}

/*
 * hadoopGetForeignRelSize
 *		(9.2+) Get a foreign scan plan node
 */
static void
hadoopGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	hadoopFdwRelationInfo *fpinfo;
	ListCell   *lc;

	SIGINTInterruptCheckProcess();

	elog(DEBUG3, HADOOP_FDW_NAME
		 ": get foreign rel size for relation ID %d", foreigntableid);

	baserel->rows = 1000;

	fpinfo = (hadoopFdwRelationInfo *) palloc0(sizeof(hadoopFdwRelationInfo));
	baserel->fdw_private = (void *) fpinfo;

	/* Base foreign tables need to be push down always. */
	fpinfo->pushdown_safe = true;

	/* Look up foreign-table catalog info. */
	fpinfo->table = GetForeignTable(foreigntableid);
	fpinfo->server = GetForeignServer(fpinfo->table->serverid);

	foreach(lc, baserel->baserestrictinfo)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		if (is_foreign_expr(root, baserel, ri->clause))
			fpinfo->remote_conds = lappend(fpinfo->remote_conds, ri);
		else
			fpinfo->local_conds = lappend(fpinfo->local_conds, ri);
	}

	/*
	 * Identify which attributes will need to be retrieved from the remote
	 * server.  These include all attrs needed for joins or final output, plus
	 * all attrs used in the local_conds.
	 *
	 * In cases where there is a small amount of data, and we don't send
	 * hadoop a WHERE clause, we should also do a SELECT *. And that happens
	 * when attrs_used remains NULL for the local execution case below
	 */
	fpinfo->attrs_used = NULL;

#if (PG_VERSION_NUM < 90600)
	pull_varattnos((Node *) baserel->reltargetlist, baserel->relid,
				   &fpinfo->attrs_used);
#else
	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
				   &fpinfo->attrs_used);
#endif /* PG_VERSION_NUM < 90600 */
	foreach(lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		pull_varattnos((Node *) rinfo->clause, baserel->relid,
					   &fpinfo->attrs_used);
	}

}
#endif

#ifdef HADOOP_FDW_IMPORT_API
/*
 ** hadoopImportForeignSchema
 ** Generates CREATE FOREIGN TABLE statements for each of the tables
 ** in the source schema and returns the list of these statements
 ** to the caller.
 **/

static List *
hadoopImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serveroid)
{
	ForeignServer *server;
	List	   *result = NIL;
	char	   *svr_username = NULL;
	char	   *svr_password = NULL;
	char	   *svr_query = NULL;
	int			svr_querytimeout = 0;
	int			svr_maxheapsize = 0;
	jclass		HadoopJDBCUtilsClass;
	jstring		initialize_result = NULL;
	jmethodID	id_initialize;
	jfieldID	id_numberofrows;
	char	   *initialize_result_cstring = NULL;
	char	   *svr_host = NULL;
	int			svr_port = 0;
	char	  **values;
	jmethodID	id_returnresultset;
	jobjectArray java_rowarray;
	int			i = 0;
	int			j = 0;
	jstring		tempString;
	int			NumberOfRows = 4;
	jstring		schemaname;
	jstring		servername;


	StringInfoData buf;

	initStringInfo(&buf);

	SIGINTInterruptCheckProcess();


	JVMInitialization(serveroid);

	hadoopGetServerOptions(
						   serveroid,
						   &svr_querytimeout,
						   &svr_maxheapsize,
						   &svr_username,
						   &svr_password,
						   &svr_query,
						   &svr_host,
						   &svr_port
		);

	server = GetForeignServer(serveroid);

	hadoopGetConnection(svr_username, svr_password, svr_host, svr_port, stmt->remote_schema);

	HadoopJDBCUtilsClass = (*env)->FindClass(env, "HadoopJDBCUtils");
	if (HadoopJDBCUtilsClass == NULL)
	{
		elog(ERROR, "HadoopJDBCUtilsClass is NULL");
	}

	id_initialize = (*env)->GetMethodID(env, HadoopJDBCUtilsClass, "PrepareDDLStmtList", "(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;");

	if (id_initialize == NULL)
	{
		elog(ERROR, "id_initialize is NULL");
	}

	id_numberofrows = (*env)->GetFieldID(env, HadoopJDBCUtilsClass, "NumberOfRows", "I");
	if (id_numberofrows == NULL)
	{
		elog(ERROR, "id_numberofrows is NULL");
	}

	schemaname = (*env)->NewStringUTF(env, stmt->remote_schema);
	servername = (*env)->NewStringUTF(env, server->servername);
	initialize_result = (*env)->CallObjectMethod(env, java_call, id_initialize, schemaname, servername);


	if (initialize_result != NULL)
	{
		initialize_result_cstring = ConvertStringToCString((jobject) initialize_result);
		elog(ERROR, "%s", initialize_result_cstring);
	}

	NumberOfRows = (*env)->GetIntField(env, java_call, id_numberofrows);

	id_returnresultset = (*env)->GetMethodID(env, HadoopJDBCUtilsClass, "ReturnDDLStmtList", "()[Ljava/lang/String;");
	if (id_returnresultset == NULL)
	{
		elog(ERROR, "id_returnresultset is NULL");
	}

	values = (char **) palloc(sizeof(char *) * NumberOfRows);

	java_rowarray = (*env)->CallObjectMethod(env, java_call, id_returnresultset);

	if (java_rowarray != NULL)
	{

		for (i = 0; i < NumberOfRows; i++)
		{
			resetStringInfo(&buf);
			values[i] = ConvertStringToCString((jobject) (*env)->GetObjectArrayElement(env, java_rowarray, i));
			appendStringInfo(&buf, "%s", values[i]);
			result = lappend(result, pstrdup(buf.data));

			elog(DEBUG1, HADOOP_FDW_NAME "DDL: %.*s\n", (int) buf.len, buf.data);

		}

		for (j = 0; j < NumberOfRows; j++)
		{
			tempString = (jstring) (*env)->GetObjectArrayElement(env, java_rowarray, j);
			(*env)->ReleaseStringUTFChars(env, tempString, values[j]);
			(*env)->DeleteLocalRef(env, tempString);
		}

		(*env)->DeleteLocalRef(env, java_rowarray);
	}


	(*env)->ReleaseStringUTFChars(env, initialize_result, initialize_result_cstring);
	(*env)->DeleteLocalRef(env, initialize_result);

	(*env)->PopLocalFrame(env, NULL);
	return result;
}
#endif  /* HADOOP_FDW_IMPORT_API */

/*
 * hadoopGetConnection
 *		Initiate access to the database
 */
static hadoopFdwExecutionState *
hadoopGetConnection(char *svr_username, char *svr_password, char *svr_host, int svr_port, char *svr_schema)
{
	char	   *svr_url = NULL;
	hadoopFdwExecutionState *festate = NULL;
	jclass		HadoopJDBCUtilsClass;
	jclass		JavaString;
	jstring		StringArray[7];
	jstring		initialize_result = NULL;
	jmethodID	id_initialize;
	jobjectArray arg_array;
	int			counter = 0;
	int			referencedeletecounter = 0;
	char	   *jar_classpath;
	char	   *initialize_result_cstring = NULL;
	char	   *var_CP = NULL;
	int			cp_len = 0;
	char	   *portstr = NULL;

	SIGINTInterruptCheckProcess();

	/* Set the options for JNI */
	var_CP = getenv("HADOOP_FDW_CLASSPATH");
	if (!var_CP)
	{
		elog(ERROR, "Please set the environment variable HADOOP_FDW_CLASSPATH");
	}
	cp_len = strlen(var_CP) + 2;
	jar_classpath = (char *) palloc(cp_len);
	snprintf(jar_classpath, (cp_len + 1), "%s", var_CP);

	portstr = (char *) palloc(sizeof(int));
	snprintf(portstr, sizeof(int), "%d", svr_port);

	if (svr_schema)
	{
		cp_len = strlen(svr_schema) + strlen(svr_host) + sizeof(int) + 30;
		svr_url = (char *) palloc(cp_len);
		snprintf(svr_url, cp_len, "jdbc:hive2://%s:%d/%s", svr_host, svr_port, svr_schema);
	}
	else
	{
		cp_len = strlen(svr_host) + sizeof(int) + 30;
		svr_url = (char *) palloc(cp_len);
		snprintf(svr_url, cp_len, "jdbc:hive2://%s:%d/default", svr_host, svr_port);
	}

	/* Stash away the state info we have already */
	festate = (hadoopFdwExecutionState *) palloc(sizeof(hadoopFdwExecutionState));

	/* Connect to the server and execute the query */
	HadoopJDBCUtilsClass = (*env)->FindClass(env, "HadoopJDBCUtils");
	if (HadoopJDBCUtilsClass == NULL)
	{
		elog(ERROR, "HadoopJDBCUtilsClass is NULL");
	}

	id_initialize = (*env)->GetMethodID(env, HadoopJDBCUtilsClass, "ConnInitialize", "([Ljava/lang/String;)Ljava/lang/String;");
	if (id_initialize == NULL)
	{
		elog(ERROR, "id_ConnInitialize is NULL");
	}

	if (svr_username == NULL)
	{
		svr_username = "";
	}

	if (svr_password == NULL)
	{
		svr_password = "";
	}

	StringArray[0] = (*env)->NewStringUTF(env, "org.apache.hive.jdbc.HiveDriver");
	StringArray[1] = (*env)->NewStringUTF(env, svr_url);
	StringArray[2] = (*env)->NewStringUTF(env, svr_username);
	StringArray[3] = (*env)->NewStringUTF(env, svr_password);
	StringArray[4] = (*env)->NewStringUTF(env, jar_classpath);

	JavaString = (*env)->FindClass(env, "java/lang/String");

	arg_array = (*env)->NewObjectArray(env, 5, JavaString, StringArray[0]);
	if (arg_array == NULL)
	{
		elog(ERROR, "arg_array is NULL");
	}

	for (counter = 1; counter < 5; counter++)
	{
		(*env)->SetObjectArrayElement(env, arg_array, counter, StringArray[counter]);
	}

	java_call = (*env)->AllocObject(env, HadoopJDBCUtilsClass);
	if (java_call == NULL)
	{
		elog(ERROR, "java_call is NULL");
	}

	festate->java_call = java_call;

	initialize_result = (*env)->CallObjectMethod(env, java_call, id_initialize, arg_array);
	if (initialize_result != NULL)
	{
		initialize_result_cstring = ConvertStringToCString((jobject) initialize_result);
		elog(ERROR, "%s", initialize_result_cstring);
	}

	for (referencedeletecounter = 0; referencedeletecounter < 5; referencedeletecounter++)
	{
		(*env)->DeleteLocalRef(env, StringArray[referencedeletecounter]);
	}

	(*env)->DeleteLocalRef(env, arg_array);
	(*env)->ReleaseStringUTFChars(env, initialize_result, initialize_result_cstring);
	(*env)->DeleteLocalRef(env, initialize_result);
	return festate;
}

#ifdef HADOOP_FDW_JOIN_API
/*
 * hadoopGetForeignJoinPaths
 *		Add possible ForeignPath to joinrel, if join is safe to push down.
 */
static void
hadoopGetForeignJoinPaths(PlannerInfo *root,
							RelOptInfo *joinrel,
							RelOptInfo *outerrel,
							RelOptInfo *innerrel,
							JoinType jointype,
							JoinPathExtraData *extra)
{
	hadoopFdwRelationInfo *fpinfo;
	Cost		startup_cost = 0;
	Cost		total_cost = 0;
	Path	   *epq_path;		/* Path to create plan to be executed when
								 * EvalPlanQual gets triggered. */

	elog(DEBUG3, HADOOP_FDW_NAME
		 ": get foreign join paths");

	/*
	 * Skip if this join combination has been considered already.
	 */
	if (joinrel->fdw_private)
		return;

	/*
	 * Create unfinished hadoopFdwRelationInfo entry which is used to indicate
	 * that the join relation is already considered, so that we won't waste
	 * time in judging safety of join pushdown and adding the same paths again
	 * if found safe. Once we know that this join can be pushed down, we fill
	 * the entry.
	 */
	fpinfo = (hadoopFdwRelationInfo *) palloc0(sizeof(hadoopFdwRelationInfo));
	fpinfo->pushdown_safe = false;
	joinrel->fdw_private = fpinfo;
	/* attrs_used is only for base relations. */
	fpinfo->attrs_used = NULL;

	epq_path = NULL;

	if (!foreign_join_ok(root, joinrel, jointype, outerrel, innerrel, extra))
	{
		/* Free path required for EPQ if we copied one; we don't need it now */
		if (epq_path)
			pfree(epq_path);
		return;
	}

	fpinfo->server = GetForeignServer(joinrel->serverid);

	/* Now update this information in the joinrel */
	joinrel->rows = 1000;

	/*
	 * Create a new join path and add it to the joinrel which represents a
	 * join between foreign tables.
	 */
#if PG_VERSION_NUM < 90500
	add_path(joinrel, (Path *) create_foreignscan_path(root, joinrel, joinrel->rows, startup_cost, total_cost, NIL, NULL, NIL));
#elif PG_VERSION_NUM < 90600
	add_path(joinrel, (Path *) create_foreignscan_path(root, joinrel, joinrel->rows, startup_cost, total_cost, NIL, NULL, NULL, NIL));
#else
	add_path(joinrel, (Path *) create_foreignscan_path(root, joinrel, NULL, joinrel->rows, startup_cost, total_cost, NIL, NULL, NULL, NIL));
#endif /* PG_VERSION_NUM < 90500 */

}

/*
 * Assess whether the join between inner and outer relations can be pushed down
 * to the foreign server. As a side effect, save information we obtain in this
 * function to hadoopFdwRelationInfo passed in.
 *
 * Joins that satisfy conditions below are safe to push down.
 *
 * 1) Join type is INNER or OUTER (one of LEFT/RIGHT/FULL)
 * 2) Both outer and inner portions are safe to push-down
 * 3) All join conditions are safe to push down
 * 4) No relation has local filter (this can be relaxed for INNER JOIN, if we
 *	  can move unpushable clauses upwards in the join tree).
 */
static bool
foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel, JoinType jointype,
				RelOptInfo *outerrel, RelOptInfo *innerrel,
				JoinPathExtraData *extra)
{
	hadoopFdwRelationInfo *fpinfo;
	hadoopFdwRelationInfo *fpinfo_o;
	hadoopFdwRelationInfo *fpinfo_i;
	ListCell   *lc;
	List	   *joinclauses;
	List	   *otherclauses;

	/*
	 * We support pushing down INNER, LEFT, RIGHT and FULL OUTER joins.
	 * Constructing queries representing SEMI and ANTI joins is hard, hence
	 * not considered right now.
	 */
	if (jointype != JOIN_INNER && jointype != JOIN_LEFT &&
		jointype != JOIN_RIGHT && jointype != JOIN_FULL)
		return false;

	/*
	 * If either of the joining relations is marked as unsafe to pushdown, the
	 * join can not be pushed down.
	 */
	fpinfo = (hadoopFdwRelationInfo *) joinrel->fdw_private;
	fpinfo_o = (hadoopFdwRelationInfo *) outerrel->fdw_private;
	fpinfo_i = (hadoopFdwRelationInfo *) innerrel->fdw_private;
	fpinfo->foreigntableid = fpinfo_o->table->relid;
	if (!fpinfo_o || !fpinfo_o->pushdown_safe ||
		!fpinfo_i || !fpinfo_i->pushdown_safe)
		return false;

	/*
	 * If joining relations have local conditions, those conditions are
	 * required to be applied before joining the relations. Hence the join can
	 * not be pushed down.
	 */
	if (fpinfo_o->local_conds || fpinfo_i->local_conds)
		return false;

	/* Separate restrict list into join quals and quals on join relation */
	if (IS_OUTER_JOIN(jointype))
		extract_actual_join_clauses(extra->restrictlist, &joinclauses, &otherclauses);
	else
	{
		/*
		 * Unlike an outer join, for inner join, the join result contains only
		 * the rows which satisfy join clauses, similar to the other clause.
		 * Hence all clauses can be treated as other quals. This helps to push
		 * a join down to the foreign server even if some of its join quals
		 * are not safe to pushdown.
		 */
		otherclauses = extract_actual_clauses(extra->restrictlist, false);
		joinclauses = NIL;
	}

	/* Save the join clauses, for later use. */
	fpinfo->joinclauses = joinclauses;

	/* Join quals must be safe to push down. */
	foreach(lc, joinclauses)
	{
		Expr	   *expr = (Expr *) lfirst(lc);

		if (!is_foreign_expr(root, joinrel, expr))
			return false;
	}

	/* Other clauses are applied after the join has been performed and thus
	 * need not be all pushable. We will push those which can be pushed to
	 * reduce the number of rows fetched from the foreign server. Rest of them
	 * will be applied locally after fetching join result. Add them to fpinfo
	 * so that other joins involving this joinrel will know that this joinrel
	 * has local clauses.
	 */
	foreach(lc, otherclauses)
	{
		Expr	   *expr = (Expr *) lfirst(lc);

		if (!is_foreign_expr(root, joinrel, expr))
			fpinfo->local_conds = lappend(fpinfo->local_conds, expr);
		else
			fpinfo->remote_conds = lappend(fpinfo->remote_conds, expr);
	}

	fpinfo->outerrel = outerrel;
	fpinfo->innerrel = innerrel;
	fpinfo->jointype = jointype;

	/*
	 * Pull the other remote conditions from the joining relations into join
	 * clauses or other remote clauses (remote_conds) of this relation wherever
	 * possible. This avoids building subqueries at every join step, which is
	 * not currently supported by the deparser logic.
	 *
	 * For an inner join, clauses from both the relations are added to the
	 * other remote clauses. For LEFT and RIGHT OUTER join, the clauses from the
	 * outer side are added to remote_conds since those can be evaluated after
	 * the join is evaluated. The clauses from inner side are added to the
	 * joinclauses, since they need to evaluated while constructing the join.
	 *
	 * For a FULL OUTER JOIN, the other clauses from either relation can not be
	 * added to the joinclauses or remote_conds, since each relation acts as an
	 * outer relation for the other. Consider such full outer join as
	 * unshippable because of the reasons mentioned above in this comment.
	 *
	 * The joining sides can not have local conditions, thus no need to test
	 * shippability of the clauses being pulled up.
	 */
	switch (jointype)
	{
		case JOIN_INNER:
			fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
										  list_copy(fpinfo_i->remote_conds));
			fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
										  list_copy(fpinfo_o->remote_conds));
			break;

		case JOIN_LEFT:
			fpinfo->joinclauses = list_concat(fpinfo->joinclauses,
										  list_copy(fpinfo_i->remote_conds));
			fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
										  list_copy(fpinfo_o->remote_conds));
			break;

		case JOIN_RIGHT:
			fpinfo->joinclauses = list_concat(fpinfo->joinclauses,
										  list_copy(fpinfo_o->remote_conds));
			fpinfo->remote_conds = list_concat(fpinfo->remote_conds,
										  list_copy(fpinfo_i->remote_conds));
			break;

		case JOIN_FULL:
			if (fpinfo_i->remote_conds || fpinfo_o->remote_conds)
				return false;
			break;

		default:
			/* Should not happen, we have just check this above */
			elog(ERROR, "unsupported join type %d", jointype);
	}

	/*
	 * For an inner join, as explained above all restrictions can be treated
	 * alike. Treating the pushed down conditions as join conditions allows a
	 * top level full outer join to be deparsed without requiring subqueries.
	 */
	if (jointype == JOIN_INNER)
	{
		Assert(!fpinfo->joinclauses);
		fpinfo->joinclauses = fpinfo->remote_conds;
		fpinfo->remote_conds = NIL;
	}

	/* Mark that this join can be pushed down safely */
	fpinfo->pushdown_safe = true;

	return true;
}
#endif /* HADOOP_FDW_JOIN_API */
