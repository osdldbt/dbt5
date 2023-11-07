/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * Based on TPC-E Standard Specification Revision 1.10.0.
 */

#include <sys/types.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h> /* this should include most necessary APIs */
#include <executor/executor.h>  /* for GetAttributeByName() */
#include <funcapi.h> /* for returning set of rows in order_status */
#include <utils/datetime.h>
#include <utils/numeric.h>
#include <utils/builtins.h>
#include <catalog/pg_type.h>

#include "frame.h"
#include "dbt5common.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#ifdef DEBUG
#define SQLTCF1_1 \
		"SELECT tr_t_id\n" \
		"FROM trade_request\n" \
		"ORDER BY tr_t_id"

#define SQLTCF1_2 \
		"INSERT INTO trade_history(th_t_id, th_dts, th_st_id)\n" \
		"VALUES (%s, now(), '%s')"

#define SQLTCF1_3 \
		"DELETE FROM trade_request"

#define SQLTCF1_4 \
		"SELECT t_id\n" \
		"FROM trade\n" \
		"WHERE t_id >= %ld\n" \
		"  AND t_st_id = '%s'"

#define SQLTCF1_5 \
		"UPDATE trade\n" \
		"SET t_st_id = '%s',\n" \
		"    t_dts = now()\n" \
		"WHERE t_id = %s\n" \
		"RETURNING t_dts"

#define SQLTCF1_6 \
		"INSERT INTO trade_history(th_t_id, th_dts, th_st_id)\n" \
		"VALUES (%s, now(), '%s')"
#endif /* End DEBUG */

#define TCF1_1 TCF1_statements[0].plan
#define TCF1_2 TCF1_statements[1].plan
#define TCF1_3 TCF1_statements[2].plan
#define TCF1_4 TCF1_statements[3].plan
#define TCF1_5 TCF1_statements[4].plan
#define TCF1_6 TCF1_statements[5].plan

static cached_statement TCF1_statements[] = {

	/* TCF1_1 */
	{
	"SELECT tr_t_id\n" \
	"FROM trade_request\n" \
	"ORDER BY tr_t_id"
	},

	/* TCF1_2 */
	{
	"INSERT INTO trade_history(th_t_id, th_dts, th_st_id)\n" \
	"VALUES ($1, now(), $2)",
	2,
	{ INT8OID, TEXTOID }
	},

	/* TCF1_3 */
	{
	"DELETE FROM trade_request"
	},

	/* TCF1_4 */
	{
	"SELECT t_id\n" \
	"FROM trade\n" \
	"WHERE t_id >= $1\n" \
	"  AND t_st_id = $2",
	2,
	{ INT8OID, TEXTOID }
	},

	/* TCF1_5 */
	{
	"UPDATE trade\n" \
	"SET t_st_id = $1,\n" \
	"    t_dts = now()\n" \
	"WHERE t_id = $2\n" \
	"RETURNING t_dts",
	2,
	{ TEXTOID, INT8OID }
	},

	/* TCF1_6 */
	{
	"INSERT INTO trade_history(th_t_id, th_dts, th_st_id)\n" \
	"VALUES ($1, now(), $2)",
	2,
	{ INT8OID, TEXTOID }
	},

	{ NULL }
}; /* End TCF1_statements */

/* Prototypes. */
void dump_tcf1_inputs(char *, char *, char *, long);

Datum TradeCleanupFrame1(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(TradeCleanupFrame1);

void dump_tcf1_inputs(char *st_canceled_id, char * st_pending_id,
		char *st_submitted_id, long trade_id)
{
	elog(NOTICE, "TCF1: INPUTS START");
	elog(NOTICE, "TCF1: st_canceled_id %s", st_canceled_id);
	elog(NOTICE, "TCF1: st_pending_id %s", st_pending_id);
	elog(NOTICE, "TCF1: st_submitted_id %s", st_submitted_id);
	elog(NOTICE, "TCF1: trade_id %ld", trade_id);
	elog(NOTICE, "TCF1: INPUTS END");
}

/* Clause 3.3.12.3 */
Datum TradeCleanupFrame1(PG_FUNCTION_ARGS)
{
	char *st_canceled_id_p = (char *) PG_GETARG_TEXT_P(0);
	char *st_pending_id_p = (char *) PG_GETARG_TEXT_P(1);
	char *st_submitted_id_p = (char *) PG_GETARG_TEXT_P(2);
	long trade_id = PG_GETARG_INT64(3);

	char st_canceled_id[ST_ID_LEN + 1];
	char st_pending_id[ST_ID_LEN + 1];
	char st_submitted_id[ST_ID_LEN + 1];

	int ret;
	TupleDesc tupdesc = NULL;
	SPITupleTable *tuptable = NULL;
	HeapTuple tuple = NULL;

#ifdef DEBUG
	char sql[2048];
#endif
	int status = 0;
	int i, n;
	char *tr_t_id = NULL;
	Datum args[2];
	char nulls[] = { ' ', ' ' };

	strcpy(st_canceled_id, DatumGetCString(DirectFunctionCall1(textout,
			PointerGetDatum(st_canceled_id_p))));
	strcpy(st_pending_id, DatumGetCString(DirectFunctionCall1(textout,
			PointerGetDatum(st_pending_id_p))));
	strcpy(st_submitted_id, DatumGetCString(DirectFunctionCall1(textout,
			PointerGetDatum(st_submitted_id_p))));

#ifdef DEBUG
	dump_tcf1_inputs(st_canceled_id, st_pending_id, st_submitted_id, trade_id);
#endif

	SPI_connect();
	plan_queries(TCF1_statements);
#ifdef DEBUG
	elog(NOTICE, "SQL\n%s", SQLTCF1_1);
#endif /* DEBUG */
	ret = SPI_execute_plan(TCF1_1, NULL, nulls, false, 0);
	if (ret == SPI_OK_SELECT) {
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
	} else {
		dump_tcf1_inputs(st_canceled_id, st_pending_id, st_submitted_id,
				trade_id);
		FAIL_FRAME(TCF1_statements[0].sql);
	}

	n = SPI_processed;
	for (i = 0; i < n; i++) {
		tuple = tuptable->vals[i];
		tr_t_id = SPI_getvalue(tuple, tupdesc, 1);
#ifdef DEBUG
		sprintf(sql, SQLTCF1_2, tr_t_id, st_submitted_id);
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		args[0] = Int64GetDatum(atoll(tr_t_id));
		args[1] = CStringGetTextDatum(st_submitted_id);
		ret = SPI_execute_plan(TCF1_2, args, nulls, false, 0);
		if (ret != SPI_OK_INSERT) {
			dump_tcf1_inputs(st_canceled_id, st_pending_id, st_submitted_id,
					trade_id);
			FAIL_FRAME(TCF1_statements[1].sql);
		}
	}

#ifdef DEBUG
	sprintf(sql, SQLTCF1_4, trade_id, st_submitted_id);
	elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
	args[0] = Int64GetDatum(trade_id);
	args[1] = CStringGetTextDatum(st_submitted_id);
	ret = SPI_execute_plan(TCF1_4, args, nulls, true, 0);
	if (ret == SPI_OK_SELECT) {
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
	} else {
		dump_tcf1_inputs(st_canceled_id, st_pending_id, st_submitted_id,
				trade_id);
		FAIL_FRAME(TCF1_statements[3].sql);
	}

	n = SPI_processed;
	for (i = 0; i < n; i++) {
		char *t_id;

		tuple = tuptable->vals[i];
		t_id = SPI_getvalue(tuple, tupdesc, 1);
#ifdef DEBUG
		sprintf(sql, SQLTCF1_5, st_canceled_id, t_id);
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		args[0] = CStringGetTextDatum(st_canceled_id);
		args[1] = Int64GetDatum(t_id);
		ret = SPI_execute_plan(TCF1_5, args, nulls, false, 0);
		if (ret != SPI_OK_UPDATE_RETURNING) {
			dump_tcf1_inputs(st_canceled_id, st_pending_id, st_submitted_id,
					trade_id);
			FAIL_FRAME(TCF1_statements[4].sql);
		}
#ifdef DEBUG
		sprintf(sql, SQLTCF1_6, t_id, st_canceled_id);
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		args[0] = Int64GetDatum(atoll(tr_t_id));
		args[1] = CStringGetTextDatum(st_canceled_id);
		ret = SPI_execute_plan(TCF1_6, args, nulls, false, 0);
		if (ret != SPI_OK_INSERT) {
			dump_tcf1_inputs(st_canceled_id, st_pending_id, st_submitted_id,
					trade_id);
			FAIL_FRAME(TCF1_statements[5].sql);
		}
	}

#ifdef DEBUG
	elog(NOTICE, "SQL\n%s", SQLTCF1_3);
#endif /* DEBUG */
	ret = SPI_execute_plan(TCF1_3, NULL, nulls, false, 0);
	if (ret != SPI_OK_DELETE) {
		dump_tcf1_inputs(st_canceled_id, st_pending_id, st_submitted_id,
				trade_id);
		FAIL_FRAME(TCF1_statements[2].sql);
	}

	SPI_finish();
	PG_RETURN_INT32(status);
}
