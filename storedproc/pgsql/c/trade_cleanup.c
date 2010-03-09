/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2007-2010 Mark Wong
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

#include "frame.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define TCF1_1 \
		"SELECT tr_t_id\n" \
		"FROM trade_request\n" \
		"ORDER BY tr_t_id"

#define TCF1_2 \
		"INSERT INTO trade_history(th_t_id, th_dts, th_st_id)\n" \
		"VALUES (%s, now(), '%s')"

#define TCF1_3 \
		"DELETE FROM trade_request"

#define TCF1_4 \
		"SELECT t_id\n" \
		"FROM trade\n" \
		"WHERE t_id >= %ld\n" \
		"  AND t_st_id = '%s'"

#define TCF1_5 \
		"UPDATE trade\n" \
		"SET t_st_id = '%s',\n" \
		"    t_dts = now()\n" \
		"WHERE t_id = %s\n" \
		"RETURNING t_dts"

#define TCF1_6 \
		"INSERT INTO trade_history(th_t_id, th_dts, th_st_id)\n" \
		"VALUES (%s, now(), '%s')"

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

	char sql[2048];
	int status = 0;

	int i;
	char *tr_t_id;

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

#ifdef DEBUG
	elog(NOTICE, "SQL\n%s", TCF1_1);
#endif /* DEBUG */
	ret = SPI_exec(TCF1_1, 0);
	if (ret == SPI_OK_SELECT) {
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
	} else {
		dump_tcf1_inputs(st_canceled_id, st_pending_id, st_submitted_id,
				trade_id);
		FAIL_FRAME2(status, TCF1_1);
	}

	for (i = 0; i < SPI_processed; i++) {
		tuple = tuptable->vals[i];
		tr_t_id = SPI_getvalue(tuple, tupdesc, 1);

		sprintf(sql, TCF1_2, tr_t_id, st_submitted_id);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret != SPI_OK_INSERT) {
			dump_tcf1_inputs(st_canceled_id, st_pending_id, st_submitted_id,
					trade_id);
			FAIL_FRAME2(status, sql);
		}

#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", TCF1_3);
#endif /* DEBUG */
		ret = SPI_exec(TCF1_3, 0);
		if (ret != SPI_OK_DELETE) {
			dump_tcf1_inputs(st_canceled_id, st_pending_id, st_submitted_id,
					trade_id);
			FAIL_FRAME2(status, sql);
		}
	}

	sprintf(sql, TCF1_4, trade_id, st_submitted_id);
#ifdef DEBUG
	elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
	ret = SPI_exec(sql, 0);
	if (ret == SPI_OK_SELECT) {
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
	} else {
		dump_tcf1_inputs(st_canceled_id, st_pending_id, st_submitted_id,
				trade_id);
		FAIL_FRAME2(status, sql);
	}

	for (i = 0; i < SPI_processed; i++) {
		char *t_id;

		tuple = tuptable->vals[i];
		t_id = SPI_getvalue(tuple, tupdesc, 1);

		sprintf(sql, TCF1_5, st_canceled_id, t_id);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret != SPI_OK_UPDATE) {
			dump_tcf1_inputs(st_canceled_id, st_pending_id, st_submitted_id,
					trade_id);
			FAIL_FRAME2(status, sql);
		}

		sprintf(sql, TCF1_6, t_id, st_canceled_id);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret != SPI_OK_INSERT) {
			dump_tcf1_inputs(st_canceled_id, st_pending_id, st_submitted_id,
					trade_id);
			FAIL_FRAME2(status, sql);
		}
	}

	SPI_finish();
	PG_RETURN_INT32(status);
}
