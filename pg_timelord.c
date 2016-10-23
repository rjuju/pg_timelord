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

#include "access/commit_ts.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "datatype/timestamp.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/snapshot.h"
#include "utils/timestamp.h"
#include "utils/tqual.h"
#include "tcop/utility.h"


PG_MODULE_MAGIC;


static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ProcessUtility_hook_type prev_ProcessUtility = NULL;

/*--- Functions --- */

void	_PG_init(void);
void	_PG_fini(void);
bool	check_pgtl_ts(char **newval, void **extra, GucSource source);
void	assign_pgtl_ts(const char *newval, void *extra);

static void pgtl_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pgtl_ProcessUtility(Node *parsetree,
		const char *queryString, ProcessUtilityContext context,
		ParamListInfo params,
		DestReceiver *dest, char *completionTag);

void pgtl_preventWrite(void);
void pgtl_restoreWrite(void);
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

	/* Install hook */
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
	ExecutorStart_hook = prev_ExecutorStart;
	ProcessUtility_hook = prev_ProcessUtility;
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
				elog(ERROR, "You cannot run utility statement in the past!");
		}
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
}

void pgtl_restoreWrite(void)
{
	if (old_XactReadOnly_changed)
	{
		XactReadOnly = old_XactReadOnly;
		old_XactReadOnly_changed = false;
	}
}

bool
HeapTupleSatisfiesTimeLord(HeapTuple htup, Snapshot snapshot,
					   Buffer buffer)
{
	/* naive way FTW */
	HeapTupleHeader tuple = htup->t_data;
	TransactionId xmin = HeapTupleHeaderGetXmin(tuple);
	TransactionId xmax = HeapTupleHeaderGetRawXmax(tuple);

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
