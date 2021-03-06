/*
 * pg_timelord
 *
 * Enable time traveling.
 *
 * TODO:
 *   * work harder on the satisfies function
 *   * handle multiple db for vacuum preventing?
 *   * verify it works
 *
 * This program is open source, licensed under the PostgreSQL license.
 * For license terms, see the LICENSE file.
 *
 */
#include "postgres.h"

#include <unistd.h>

#include "access/commit_ts.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "datatype/timestamp.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "optimizer/cost.h"
#include "optimizer/planner.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/procarray.h"
#include "storage/shmem.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/tqual.h"


PG_MODULE_MAGIC;

#if PG_VERSION_NUM >= 90300
#define PGTL_DUMP_FILE		"pg_stat/pg_timelord.stat"
#else
#define PGTL_DUMP_FILE		"global/pg_timelord.stat"
#endif

static const uint32 PGTL_FILE_HEADER = 0x20161025;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

/*
 * Global shared state
 */
typedef struct pgtlSharedState
{
	LWLockId		lock;			/* protects counter modification */
	TransactionId	oldestSafeXid;	/* oldest safe transactionid reachable */
	int8			datname_len;
	char			datname[NAMEDATALEN];
} pgtlSharedState;

/* Links to shared memory state */
static pgtlSharedState *pgtl = NULL;

/* flag set by bgworker signal handler */
static volatile sig_atomic_t got_sigterm = false;

/*--- Functions --- */

void	_PG_init(void);
void	_PG_fini(void);
bool	check_pgtl_ts(char **newval, void **extra, GucSource source);
void	assign_pgtl_ts(const char *newval, void *extra);

PG_FUNCTION_INFO_V1(pg_timelord_oldest_xact);
Datum	pg_timelord_oldest_xact(PG_FUNCTION_ARGS);

static void pgtl_main(Datum main_arg);
static void pgtl_sigterm(SIGNAL_ARGS);
static void pgtl_shmem_startup(void);
static void pgtl_shmem_shutdown(int code, Datum arg);
static PlannedStmt *pgtl_planner(Query *parse,
								 int cursorOptions,
								 ParamListInfo boundParams);
static void pgtl_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pgtl_ProcessUtility(Node *parsetree,
		const char *queryString, ProcessUtilityContext context,
		ParamListInfo params,
		DestReceiver *dest, char *completionTag);

static void pgtl_initOldestSafeXid(void);
static void pgtl_preventWrite(void);
static void pgtl_restoreWrite(void);
static bool pgtl_saveShmemState(bool need_lock);
static void pgtl_setoldestSafeXid(TransactionId newOldest);
static TransactionId pgtl_getOldestSafeXid(void);
static void pgtl_setdatname(int8 len, char *datname);
static bool HeapTupleSatisfiesTimeLord(HeapTuple htup, Snapshot snapshot,
								Buffer buffer);

static char *pgtl_ts_char;
static TimestampTz pgtl_ts = 0;
static char *pgtl_database;
static int  pgtl_keep_xact;
static bool old_XactReadOnly;
static bool old_XactReadOnly_changed = false;

void
_PG_init(void)
{
	BackgroundWorker worker;

	if (!process_shared_preload_libraries_in_progress)
	{
		elog(ERROR, "This module can only be loaded via shared_preload_libraries");
		return;
	}

	if (!track_commit_timestamp)
	{
		elog(ERROR, "I need track_commit_timestamp enabled as a timelord need his tardis!");
	}

	RequestAddinShmemSpace(MAXALIGN(sizeof(pgtlSharedState)));
#if PG_VERSION_NUM >= 90600
	RequestNamedLWLockTranche("pg_timelord", 1);
#else
	RequestAddinLWLocks(1);
#endif

	/* Install hook */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgtl_shmem_startup;
	prev_planner_hook = planner_hook;
	planner_hook = pgtl_planner;
	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pgtl_ExecutorStart;
	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = pgtl_ProcessUtility;

	DefineCustomStringVariable("pg_timelord.ts",
							 "Timestamp with time zone destination",
							 NULL,
							 &pgtl_ts_char,
							 "",
							 PGC_USERSET,
							 0,
							 check_pgtl_ts,
							 assign_pgtl_ts,
							 NULL);

	DefineCustomStringVariable("pg_timelord.database",
							   "Defines the database on which prevent VACUUM",
							   NULL,
							   &pgtl_database,
							   "postgres",
							   PGC_POSTMASTER,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomIntVariable("pg_timelord.keep_xact",
							"Defines the number of transaction to prevent from cleaning",
							NULL,
							&pgtl_keep_xact,
							200000000,
							-1,
							1000000000,
							PGC_SUSET,
							0,
							NULL,
							NULL,
							NULL);

	/* Register the worker processes */
	worker.bgw_flags =
		BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;		/* Must write to the
																 * database */
	worker.bgw_main = pgtl_main;
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_timelord");
	worker.bgw_restart_time = 3;
	worker.bgw_main_arg = (Datum) 0;
#if (PG_VERSION_NUM >= 90400)
	worker.bgw_notify_pid = 0;
#endif
	RegisterBackgroundWorker(&worker);
}

void
_PG_fini(void)
{
	/* uninstall hook */
	shmem_startup_hook = prev_shmem_startup_hook;
	planner_hook = prev_planner_hook;
	ExecutorStart_hook = prev_ExecutorStart;
	ProcessUtility_hook = prev_ProcessUtility;
}

Datum
pg_timelord_oldest_xact(PG_FUNCTION_ARGS)
{
	return TransactionIdGetDatum(pgtl_getOldestSafeXid());
}

static void
pgtl_main(Datum main_arg)
{
	Snapshot		pgtl_snap;
	char			msg[100];

	/* Establish signal handler before unblocking signals. */
	pqsignal(SIGTERM, pgtl_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Connect to postgres database */
	BackgroundWorkerInitializeConnection(pgtl_database, NULL);

	elog(LOG, "pg_timelord VACUUM preventer© connected on database \"%s\"", pgtl_database);

	set_ps_display("init", false);
	StartTransactionCommand();
	SPI_connect();

	/* There doesn't seem to a nice API to set these */
	XactIsoLevel = XACT_REPEATABLE_READ;
	XactReadOnly = true;

	pgtl_snap = GetTransactionSnapshot();

	/* detect if new safe xid is needed */
	pgtl_initOldestSafeXid();
	pgtl_snap->xmin = pgtl_getOldestSafeXid();
	Assert(TransactionIdIsNormal(pgtl_getOldestSafeXid()));

	/* start by setting up previous kept xid */
	PushActiveSnapshot(pgtl_snap);
	pgstat_report_activity(STATE_IDLEINTRANSACTION, "Setting up pg_timelord...");

	while (!got_sigterm)
	{
		/*
		 * this has to be declared as static (see the hack in GetSnapshotData
		 * to avoid repeated malloc/free).
		 */
		static SnapshotData snap_tmp;
		TransactionId new_xmin;

		new_xmin = ReadNewTransactionId() - pgtl_keep_xact;

		/* make sure we get a normal xid */
		if (!TransactionIdIsNormal(new_xmin))
			new_xmin = FirstNormalTransactionId;

		/* make sure this xid is older than any active xid */
		/* TODO: make sure we don't get the bgworker xmin... */
		GetSnapshotData(&snap_tmp);
		if (TransactionIdPrecedes(snap_tmp.xmin, new_xmin))
		{
			new_xmin = snap_tmp.xmin;
			TransactionIdRetreat(new_xmin);
		}

		/*
		 * get a committed xid. This loop could take a long time if your
		 * application sucks (ie. does a lot of rollback), but you'd deserve it
		 */
		while(TransactionIdPrecedes(pgtl_snap->xmin, new_xmin) &&
			  !TransactionIdDidCommit(new_xmin))
			TransactionIdRetreat(new_xmin);

		/* don't go further in the past that what we can */
		if (TransactionIdPrecedes(new_xmin, pgtl_snap->xmin))
				new_xmin = pgtl_snap->xmin;

		if (pgtl_snap->xmin != new_xmin)
		{
			pgtl_snap->xmin = new_xmin;
			/* these should be unneded, XXX check */
			//PopActiveSnapshot();
			//PushActiveSnapshot(pgtl_snap);
			pgtl_setoldestSafeXid(new_xmin);
			if (!pgtl_saveShmemState(true))
				/* XXX should try again or check why it failed? */
				elog(WARNING, "could not save pg_timelord shmstate");

			if (!ProcArrayInstallImportedXmin(new_xmin, GetTopTransactionId()))
			{
				/* this shouldn't happen unless xmin goes forward */
				elog(PANIC, "error during ProcArrayInstallImportedXmin,"
						" new_xmin: %d", new_xmin);
			}

			sprintf(msg, "oldest unvacuumed xid: %u", new_xmin);
			pgstat_report_activity(STATE_IDLEINTRANSACTION, msg);
		}

		/* sleep 5 minutes */
		WaitLatch(MyLatch,
				WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
				300000
#if PG_VERSION_NUM >= 100000
				,PG_WAIT_EXTENSION
#endif
		);
		ResetLatch(MyLatch);
	}

	proc_exit(1);
}

static void
pgtl_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

static void
pgtl_shmem_startup(void)
{
	bool			found;
	FILE		   *file;
	uint32			header;
	TransactionId	temp_ts;
	int8			temp_len;
	char			temp_datname[NAMEDATALEN];

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* reset in case this is a restart within the postmaster */
	pgtl = NULL;

	/* Create or attach to the shared memory state */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	/* global access lock */
	pgtl = ShmemInitStruct("pg_timelord",
					sizeof(pgtlSharedState),
					&found);

	if (!found)
	{
		/* First time through ... */
#if PG_VERSION_NUM >= 90600
		pgtl->lock = &(GetNamedLWLockTranche("pg_timelord"))->lock;
#else
		pgtl->lock = LWLockAssign();
#endif
	}

	LWLockRelease(AddinShmemInitLock);

	if (!IsUnderPostmaster)
		on_shmem_exit(pgtl_shmem_shutdown, (Datum) 0);

	/*
	 * Done if some other process already completed our initialization.
	 */
	if (found)
		return;

	/* Load stat file, don't care about locking */
	file = AllocateFile(PGTL_DUMP_FILE, PG_BINARY_R);
	if (file == NULL)
	{
		if (errno == ENOENT)
		{
			/*
			 * no stat file, probably initialization. A safe xid will be set
			 * at first query execution, save configured database as reference
			 * database.
			 */
			pgtl_setoldestSafeXid(BootstrapTransactionId);
			pgtl_setdatname(strlen(pgtl_database), pgtl_database);
			return;			/* ignore not-found error */
		}
		goto error;
	}

	/* check is header is valid */
	if (fread(&header, sizeof(uint32), 1, file) != 1 ||
		header != PGTL_FILE_HEADER)
		goto error;

	/* read saved oldestSafeXid and datname */
	if ((fread(&temp_ts, sizeof(TransactionId), 1, file) != 1) ||
		(fread(&temp_len, sizeof(int8), 1, file) != 1) ||
		(fread(&temp_datname, 1, temp_len + 1, file) != temp_len + 1))
		goto error;

	Assert(temp_len <= NAMEDATALEN);

	/* detect if database changed */
	if (strcmp(pgtl_database, temp_datname) != 0)
	{
		/* database changed, force new safe xid */
		pgtl_setoldestSafeXid(BootstrapTransactionId);
	}
	else
	{
		/* restore the oldest safe transaction id */
		pgtl_setoldestSafeXid(temp_ts);
	}
	/* save reference database in shmem */
	pgtl_setdatname(temp_len, temp_datname);

	FreeFile(file);

	/*
	 * Don't remove the file so it's included in backups/replication slaves,
	 * etc. This file will be rewritten as needed by the background worker or
	 * during shutdown
	 */

	return;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read pg_timelord file \"%s\": %m",
					PGTL_DUMP_FILE)));
	if (file)
		FreeFile(file);
	/* delete bogus file, don't care of errors in this case */
	unlink(PGTL_DUMP_FILE);
}

static void
pgtl_shmem_shutdown(int code, Datum arg)
{
	/* Don't try to dump during a crash. */
	if (code)
		return;

	if (!pgtl)
		return;

	/* no lock here */
	pgtl_saveShmemState(false);
}

bool
check_pgtl_ts(char **newval, void **extra, GucSource source)
{
	TimestampTz newTs = 0;
	TransactionId oldest;
	TimestampTz oldestTs = 0;

	if (!track_commit_timestamp)
	{
		elog(ERROR, "I need track_commit_timestamp enabled as a timelord need his tardis!");
		return false;
	}

	if (!pgtl)
	{
		/* don't allow non empty default */
		if (strcmp(*newval, "") != 0)
			return false;
	}
	else
	{
		/* shmem avail and asked for ts, let's check a safe xid exists first */

		oldest = pgtl_getOldestSafeXid();

		if (!TransactionIdIsNormal(oldest))
		{
			elog(ERROR, "No safe point is the past available yet");
			return false;
		}
	}

	if (IsTransactionBlock())
	{
		/*
		 * no time travel if in a transaction
		 * XXX allow if transaction began while time traveling and changing
		 * date?
		 */
		elog(ERROR, "No time travel inside a transaction");
		return false;
	}

	if (strcmp(*newval, "") != 0)
	{
		/* new char is needed, don't know why *newval doesn't work */
		char *str = pstrdup(*newval);

		newTs = DatumGetTimestampTz(DirectFunctionCall3(timestamptz_in,
					CStringGetDatum(str),
					ObjectIdGetDatum(InvalidOid),
					Int32GetDatum(-1)));
		pfree(str);

		/* check if ts is recent enough */
		if (!TransactionIdGetCommitTsData(oldest, &oldestTs, NULL))
		{
			if (TransactionIdEquals(oldest, ReadNewTransactionId()))
				elog(ERROR, "There is no previous commit stored");
			else
				elog(ERROR, "Could not check data availability for ts %s", *newval);
			return false;
		}

		if (timestamp_cmp_internal(newTs, oldestTs) < 0)
		{
			ereport(ERROR,
					(errmsg("Requested ts %s is too old", *newval),
					errdetail("Oldest ts available: %s",
							  timestamptz_to_str(oldestTs))));
			return false;
		}
	}

	*extra = malloc(sizeof(TimestampTz));
	if (!*extra)
		return false;
	*((TimestampTz *) *extra) = newTs;

	return true;
}

void
assign_pgtl_ts(const char *newval, void *extra)
{
	pgtl_ts = *((TimestampTz *) extra);
}

static PlannedStmt *pgtl_planner(Query *parse,
								 int cursorOptions,
								 ParamListInfo boundParams)
{
	PlannedStmt *result;
	bool prev_enable_bitmapscan = enable_bitmapscan;
	bool prev_enable_indexonlyscan = enable_indexonlyscan;

	if (pgtl_ts != 0)
	{
		/*
		 * bitmap scan does not support non-core MVCC snapshot, see Assert in
		 * ExecInitBitmapHeapScan() and comment at top of nodeBitmapHeapscan.c
		 */
		enable_bitmapscan = false;
		/* index-only scan does not support non-core MVCC snapshot, see comment
		 * above the elog(ERROR) in IndexOnlyNext()
		 */
		enable_indexonlyscan = false;
	}

	if (prev_planner_hook)
		result = (*prev_planner_hook) (parse, cursorOptions, boundParams);
	else
		result = standard_planner(parse, cursorOptions, boundParams);

	if (pgtl_ts != 0)
	{
		/* restore planner parameters */
		enable_bitmapscan = prev_enable_bitmapscan;
		enable_indexonlyscan = prev_enable_indexonlyscan;
	}

	return result;
}

static void
pgtl_ExecutorStart (QueryDesc *queryDesc, int eflags)
{
	pgtl_preventWrite();

	/* do not mess with non MVCC snapshots, like index build :) */
	if (pgtl_ts != 0 && IsMVCCSnapshot(queryDesc->snapshot))
		queryDesc->snapshot->satisfies = HeapTupleSatisfiesTimeLord;

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

	pgtl_restoreWrite();
}

static void
pgtl_ProcessUtility(Node *parsetree,
		const char *queryString, ProcessUtilityContext context,
		ParamListInfo params,
		DestReceiver *dest, char *completionTag)
{
	pgtl_preventWrite();

	/* forbid most of utility command in the past! */
	if (pgtl_ts != 0)
	{
		switch (nodeTag(parsetree))
		{
			case T_TransactionStmt:
				{
					TransactionStmt *stmt = (TransactionStmt *) parsetree;

					/* error out if explicitely asked rw transaction */
					if (stmt->kind == TRANS_STMT_BEGIN ||
						stmt->kind == TRANS_STMT_START)
					{
						ListCell *lc;

						foreach(lc, stmt->options)
						{
							DefElem    *item = (DefElem *) lfirst(lc);

							if (strcmp(item->defname, "transaction_read_only") == 0)
							{
								A_Const *con = (A_Const *) item->arg;

								Assert(IsA(item->arg, A_Const) && IsA(&con->val, Integer));
								if (!intVal(&con->val))
								{
									elog(ERROR, "You cannot ask for read-write transaction in the past!");
								}
							}
						}
					}
					/*
					 * I keep the code above just in case I want to allow it
					 * again sometime in the future, but for now just forbid
					 * any transaction in the past
					 */
					elog(ERROR, "You cannot use TCL in the past!");
				}
			case T_VariableSetStmt:
			case T_VariableShowStmt:
			case T_ExplainStmt:
			case T_NotifyStmt:
			case T_ListenStmt:
			case T_UnlistenStmt:
			case T_CopyStmt:
			case T_PrepareStmt:
			case T_DeallocateStmt:
			case T_PlannedStmt:
			case T_ClosePortalStmt:
			case T_FetchStmt:
			case T_DoStmt:
				/* whitelist of utility command */
				break;
			default:
				elog(ERROR, "You cannot run utility command \"%s\" in the past!",
						CreateCommandTag(parsetree));
		}
	}

	if (IsA(parsetree, VacuumStmt))
	{
		VacuumStmt *vacstmt = (VacuumStmt *) parsetree;

		if (vacstmt->options & VACOPT_VERBOSE)
			elog(NOTICE, "pg_timelord is blocking VACUUM for xid >= %u",
					pgtl_getOldestSafeXid());
	}

	if (prev_ProcessUtility)
	(*prev_ProcessUtility) (parsetree, queryString,
								context, params,
								dest, completionTag);
	else
		standard_ProcessUtility(parsetree, queryString,
								context, params,
								dest, completionTag);

	pgtl_restoreWrite();
}

static void
pgtl_initOldestSafeXid(void)
{
	/* test without lock first */
	if (TransactionIdEquals(pgtl->oldestSafeXid, BootstrapTransactionId))
	{
		/* new test with the lock, and set value if needed */
		LWLockAcquire(pgtl->lock, LW_EXCLUSIVE);
		if (pgtl->oldestSafeXid == BootstrapTransactionId)
		{
			pgtl->oldestSafeXid = ReadNewTransactionId();
			elog(WARNING, "new safe xid set: %u", pgtl->oldestSafeXid);
		}
		LWLockRelease(pgtl->lock);
	}
}

static void
pgtl_preventWrite(void)
{
	if (pgtl_ts != 0)
	{
		old_XactReadOnly = XactReadOnly;
		XactReadOnly = true;
		old_XactReadOnly_changed = true;
	}
	else
		/* bgworker should have done it already */
		pgtl_initOldestSafeXid();
}

static void
pgtl_restoreWrite(void)
{
	if (old_XactReadOnly_changed)
	{
		XactReadOnly = old_XactReadOnly;
		old_XactReadOnly_changed = false;
	}
}

/* callse must take care of taking the lock */
static bool
pgtl_saveShmemState(bool need_lock)
{
	FILE   *file;
	bool	lock_taken = false;

	if (!pgtl)
		return false;

	if (need_lock)
	{
		LWLockAcquire(pgtl->lock, LW_SHARED);
		lock_taken = true;
	}

	file = AllocateFile(PGTL_DUMP_FILE ".tmp", PG_BINARY_W);
	if (file == NULL)
		goto error;

	if ((fwrite(&PGTL_FILE_HEADER, sizeof(uint32), 1, file) != 1) ||
		(fwrite(&pgtl->oldestSafeXid, sizeof(TransactionId), 1, file) != 1) ||
		(fwrite(&pgtl->datname_len, sizeof(int8), 1, file) != 1) ||
		(fwrite(&pgtl->datname, 1, pgtl->datname_len + 1, file) != pgtl->datname_len + 1))
		goto error;

	if (FreeFile(file))
	{
		file = NULL;
		goto error;
	}

	/*
	 * Remove old file and rename file inplace
	 */
	unlink(PGTL_DUMP_FILE);
	/* if crash happens here, you're really not lucky */
	if (rename(PGTL_DUMP_FILE ".tmp", PGTL_DUMP_FILE) != 0)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not rename pg_timelord file \"%s\": %m",
						PGTL_DUMP_FILE ".tmp")));

		goto error;
	}

	if (need_lock && lock_taken)
		LWLockRelease(pgtl->lock);

	return true;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not write pg_timelord file \"%s\": %m",
					PGTL_DUMP_FILE)));

	if (file)
		FreeFile(file);
	unlink(PGTL_DUMP_FILE);

	if (lock_taken)
		LWLockRelease(pgtl->lock);
	return false;
}

static void
pgtl_setoldestSafeXid(TransactionId newOldest)
{
	Assert(TransactionIdIsValid(newOldest));
	Assert(TransactionIdPrecedesOrEquals(pgtl->oldestSafeXid, newOldest));

	if (TransactionIdEquals(newOldest, BootstrapTransactionId))
		elog(WARNING, "Forcing new safe xid at next transaction");

	LWLockAcquire(pgtl->lock, LW_EXCLUSIVE);
	pgtl->oldestSafeXid = newOldest;
	LWLockRelease(pgtl->lock);
}

static TransactionId
pgtl_getOldestSafeXid(void)
{
	TransactionId xmin;

	LWLockAcquire(pgtl->lock, LW_SHARED);
	xmin = pgtl->oldestSafeXid;
	LWLockRelease(pgtl->lock);

	return xmin;
}

static void
pgtl_setdatname(int8 len, char *datname)
{
	LWLockAcquire(pgtl->lock, LW_EXCLUSIVE);
	pgtl->datname_len = len;
	if (len > 0)
		strncpy(pgtl->datname, datname, len + 1);
	LWLockRelease(pgtl->lock);
}

static bool
HeapTupleSatisfiesTimeLord(HeapTuple htup, Snapshot snapshot,
					   Buffer buffer)
{
	/* naive way FTW */
	HeapTupleHeader tuple = htup->t_data;
	TransactionId xmin = HeapTupleHeaderGetXmin(tuple);
	TransactionId xmax = HeapTupleHeaderGetRawXmax(tuple);

	Assert(TransactionIdIsValid(xmin));

	/* frozen and bootstrap xid are always visible */
	if (TransactionIdIsNormal(xmin) && (!HeapTupleHeaderXminFrozen(tuple)))
	{
		/* really inserted ? */
		if (HeapTupleHeaderXminCommitted(tuple) && TransactionIdDidCommit(xmin))
		{
			TimestampTz insert_ts;

			if (!TransactionIdGetCommitTsData(xmin, &insert_ts, NULL))
				/* too old, too bad */
				elog(ERROR, "You requested too old snapshot");

			if (timestamp_cmp_internal(insert_ts, pgtl_ts) > 0)
				/* line created after requested ts */
				return false;
		}
		else /* line was never inserted */
			return false;
	}

	/* at this point, xmin has committed and is visible, what about xmax ? */

	/*
	 * quick exit if no xmax, probably not needed since HEAP_XMAX_COMMITTED
	 * should not be true
	 */
	if (!TransactionIdIsValid(xmax))
			return true;

	/* check hint bit first */
	if ((tuple->t_infomask & HEAP_XMAX_COMMITTED) ||
		/* check clog */
		TransactionIdDidCommit(xmax))
	{
		TimestampTz delete_ts;

		/* I think this test is stupid */
		if (xmax != FrozenTransactionId)
		{
			if (!TransactionIdGetCommitTsData(xmax, &delete_ts, NULL))
				/* too old, too bad */
				elog(ERROR, "You requested too old snapshot");

			if (timestamp_cmp_internal(delete_ts, pgtl_ts) > 0)
				/* line deleted after requested ts */
				return true;
			else
				return false;
		}
	}

	return true;
}
