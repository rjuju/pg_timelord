/*
 * pg_timelord
 *
 * Enable time traveling.
 *
 * TODO:
 *   * add a GUC to prevent VACUUM from removing recent enough data
 *   * work harder on the satisfies function
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
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/snapshot.h"
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
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

/*
 * Global shared state
 */
typedef struct pgtlSharedState
{
	LWLockId		lock;			/* protects counter modification */
	TransactionId	oldestSafeTs;		/* oldest safe transactionid reachable */
} pgtlSharedState;

/* Links to shared memory state */
static pgtlSharedState *pgtl = NULL;

/*--- Functions --- */

void	_PG_init(void);
void	_PG_fini(void);
bool	check_pgtl_ts(char **newval, void **extra, GucSource source);
void	assign_pgtl_ts(const char *newval, void *extra);

static void pgtl_shmem_startup(void);
static void pgtl_shmem_shutdown(int code, Datum arg);
static void pgtl_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pgtl_ProcessUtility(Node *parsetree,
		const char *queryString, ProcessUtilityContext context,
		ParamListInfo params,
		DestReceiver *dest, char *completionTag);

void pgtl_preventWrite(void);
void pgtl_restoreWrite(void);
void pgtl_setOldestSafeTs(TransactionId newOldest);
bool HeapTupleSatisfiesTimeLord(HeapTuple htup, Snapshot snapshot,
								Buffer buffer);

static char *pgtl_ts_char;
static TimestampTz pgtl_ts = 0;
static bool old_XactReadOnly;
static bool old_XactReadOnly_changed = false;

void
_PG_init(void)
{
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
}

void
_PG_fini(void)
{
	/* uninstall hook */
	shmem_startup_hook = prev_shmem_startup_hook;
	ExecutorStart_hook = prev_ExecutorStart;
	ProcessUtility_hook = prev_ProcessUtility;
}

static void
pgtl_shmem_startup(void)
{
	bool		found;
	FILE		*file;
	uint32		header;
	TransactionId temp_ts;

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
			 * at first query execution
			 */
			pgtl_setOldestSafeTs(BootstrapTransactionId);
			return;			/* ignore not-found error */
		}
		goto error;
	}

	/* check is header is valid */
	if (fread(&header, sizeof(uint32), 1, file) != 1 ||
		header != PGTL_FILE_HEADER)
		goto error;

	/* read saved oldestSafeTs */
	if (fread(&temp_ts, sizeof(TransactionId), 1, file) != 1)
		goto error;

	/* restore the oldest safe transaction id */
	pgtl_setOldestSafeTs(temp_ts);

	FreeFile(file);
	/*
	 * Remove the file so it's not included in backups/replication slaves,
	 * etc. A new file will be written on next shutdown.
	 */
	unlink(PGTL_DUMP_FILE);

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
	FILE	*file;

	/* Don't try to dump during a crash. */
	if (code)
		return;

	if (!pgtl)
		return;

	file = AllocateFile(PGTL_DUMP_FILE ".tmp", PG_BINARY_W);
	if (file == NULL)
		goto error;

	if (fwrite(&PGTL_FILE_HEADER, sizeof(uint32), 1, file) != 1)
		goto error;

	if (fwrite(&pgtl->oldestSafeTs, sizeof(TransactionId), 1, file) != 1)
		goto error;

	if (FreeFile(file))
	{
		file = NULL;
		goto error;
	}

	/*
	 * Rename file inplace
	 */
	if (rename(PGTL_DUMP_FILE ".tmp", PGTL_DUMP_FILE) != 0)
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not rename pg_timelord file \"%s\": %m",
						PGTL_DUMP_FILE ".tmp")));

	return;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read pg_timelord file \"%s\": %m",
					PGTL_DUMP_FILE)));

	if (file)
		FreeFile(file);
	unlink(PGTL_DUMP_FILE);
}

bool
check_pgtl_ts(char **newval, void **extra, GucSource source)
{
	TimestampTz newTs = 0;
	TransactionId oldest;
	TimestampTz oldestTs = 0;
	bool	ok = false;

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

		LWLockAcquire(pgtl->lock, LW_SHARED);
		ok = TransactionIdIsNormal(pgtl->oldestSafeTs);
		LWLockRelease(pgtl->lock);

		if (!ok)
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
		LWLockAcquire(CommitTsLock, LW_SHARED);
		oldest = ShmemVariableCache->oldestCommitTsXid;
		LWLockRelease(CommitTsLock);

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

	/* forbid explicit VACUUM FREEZE, there's no way to modify oldest xact */
	if (IsA(parsetree, VacuumStmt))
	{
		VacuumStmt *vacstmt = (VacuumStmt *) parsetree;

		if (vacstmt->options & VACOPT_FREEZE)
			elog(ERROR, "Explicit VACUUM FREEZE is disabled!");

		/* XXX disble CLUSTER and VACUUM FULL too? */
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

void pgtl_preventWrite(void)
{
	if (pgtl_ts != 0)
	{
		old_XactReadOnly = XactReadOnly;
		XactReadOnly = true;
		old_XactReadOnly_changed = true;
	}
	/* test without lock first */
	else if (TransactionIdEquals(pgtl->oldestSafeTs, BootstrapTransactionId))
	{
		/* new test with the lock, and set value if needed */
		LWLockAcquire(pgtl->lock, LW_EXCLUSIVE);
		if (pgtl->oldestSafeTs == BootstrapTransactionId)
			pgtl->oldestSafeTs = ReadNewTransactionId();
		LWLockRelease(pgtl->lock);
	}
}

void pgtl_restoreWrite(void)
{
	if (old_XactReadOnly_changed)
	{
		XactReadOnly = old_XactReadOnly;
		old_XactReadOnly_changed = false;
	}
}

void
pgtl_setOldestSafeTs(TransactionId newOldest)
{
	elog(WARNING, "set %d", newOldest);
	Assert(TransactionIdIsValid(newOldest));
	Assert(TransactionIdPrecedesOrEquals(pgtl->oldestSafeTs, newOldest));

	LWLockAcquire(pgtl->lock, LW_EXCLUSIVE);
	pgtl->oldestSafeTs = newOldest;
	LWLockRelease(pgtl->lock);
}

bool
HeapTupleSatisfiesTimeLord(HeapTuple htup, Snapshot snapshot,
					   Buffer buffer)
{
	/* naive way FTW */
	HeapTupleHeader tuple = htup->t_data;
	TransactionId xmin = HeapTupleHeaderGetXmin(tuple);
	TransactionId xmax = HeapTupleHeaderGetRawXmax(tuple);

	Assert(TransactionIdIsValid(xmin));

	if (!TransactionIdIsNormal(xmin)) /* frozen or bootstrap */
		return true;

	/* really inserted ? */
	if (HeapTupleHeaderXminCommitted(tuple) && TransactionIdDidCommit(xmin))
	{
		TimestampTz insert_ts;

		if (xmin != FrozenTransactionId)
		{
			if (!TransactionIdGetCommitTsData(xmin, &insert_ts, NULL))
				/* too old, too bad */
				elog(ERROR, "You requested too old snapshot");

			if (timestamp_cmp_internal(pgtl_ts, insert_ts) <= 0)
				/* not visible yet */
				return false;
		}
	}

	/* at this point, xmin has committed, what about xmax ? */
	/* check hint bit first */
	if ((tuple->t_infomask & HEAP_XMAX_COMMITTED) ||
		/* check clog */
		TransactionIdDidCommit(xmax))
	{
		TimestampTz delete_ts;

		if (xmax != FrozenTransactionId)
		{
			if (!TransactionIdGetCommitTsData(xmax, &delete_ts, NULL))
				/* too old, too bad */
				elog(ERROR, "You requested too old snapshot");

			if (timestamp_cmp_internal(pgtl_ts, delete_ts) <= 0)
				/* not visible yet */
				return true;
		}
	}
	return true;
}
