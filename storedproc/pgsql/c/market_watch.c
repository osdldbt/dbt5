/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2007-2010 Mark Wong
 *
 * Based on TPC-E Standard Specification Revision 1.10.0.
 */

#include <sys/types.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h> /* this should include most necessary APIs */
#include <executor/executor.h>  /* for GetAttributeByName() */
#include <funcapi.h> /* for returning set of rows in order_status */
#include <miscadmin.h>
#include <utils/date.h>
#include <utils/builtins.h>
#include <utils/datetime.h>

#include "frame.h"

#define BAD_INPUT_DATA 1

#define MWF1_1 \
		"SELECT wi_s_symb\n" \
		"FROM   watch_item,\n" \
		"       watch_list\n" \
		"WHERE  wi_wl_id = wl_id\n" \
		"       AND wl_c_id = %" PRIu64

#define MWF1_2 \
		"SELECT s_symb\n" \
		"FROM   industry,\n" \
		"       company,\n" \
		"       security\n" \
		"WHERE  in_name = '%s'\n" \
		"       AND co_in_id = in_id\n" \
		"       AND co_id BETWEEN %" PRIu64 "\n" \
		"                         AND %" PRIu64 "\n" \
		"       AND s_co_id = co_id"

#define MWF1_3 \
		"SELECT hs_s_symb\n" \
		"FROM   holding_summary\n" \
		"WHERE  hs_ca_id = %" PRIu64

#define MWF1_4 \
		"SELECT lt_price\n" \
		"FROM   last_trade\n" \
		"WHERE  lt_s_symb = '%s'"

#define MWF1_5 \
		"SELECT s_num_out\n" \
		"FROM   security\n" \
		"WHERE  s_symb = '%s'"

#define MWF1_6 \
		"SELECT dm_close\n" \
		"FROM   daily_market\n" \
		"WHERE  dm_s_symb = '%s'\n" \
		"       AND dm_date = '%s'"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* Prototypes to prevent potential gcc warnings. */
Datum MarketWatchFrame1(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(MarketWatchFrame1);

void dump_mwf1_inputs(long, long, long, char *, char *, long);

void dump_mwf1_inputs(long acct_id, long cust_id, long ending_co_id,
		char *industry_name, char *start_date, long starting_co_id) {
	elog(NOTICE, "MWF1: INPUTS START");
	elog(NOTICE, "MWF1: %ld", acct_id);
	elog(NOTICE, "MWF1: %ld", cust_id);
	elog(NOTICE, "MWF1: %ld", ending_co_id);
	elog(NOTICE, "MWF1: '%s'", industry_name);
	elog(NOTICE, "MWF1: %s", pstrdup(start_date));
	elog(NOTICE, "MWF1: %ld", starting_co_id);
	elog(NOTICE, "MWF1: INPUTS END");
}

/* Clause 3.3.1.3 */
Datum MarketWatchFrame1(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

	int i;

	char **values = NULL;

	int status = 0;
	double old_mkt_cap = 0.0;
	double new_mkt_cap = 0.0;
	double pct_change = 0.0;

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		struct pg_tm tt, *tm = &tt;
		int64 acct_id = PG_GETARG_INT64(0);
		int64 cust_id = PG_GETARG_INT64(1);
		int64 ending_co_id = PG_GETARG_INT64(2);
		char *industry_name_p = (char *) PG_GETARG_TEXT_P(3);
		DateADT start_date_p = PG_GETARG_DATEADT(4);
		int64 starting_co_id = PG_GETARG_INT64(5);

		enum mwf1 { i_pct_change=0, i_status };

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;

		char buf[MAXDATELEN + 1];
		char industry_name[IN_NAME_LEN + 1];

		char sql[2048] = "";

		j2date(start_date_p + POSTGRES_EPOCH_JDATE,
		   &(tm->tm_year), &(tm->tm_mon), &(tm->tm_mday));
		EncodeDateOnly(tm, DateStyle, buf);

		strcpy(industry_name, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(industry_name_p))));

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 2);
		values[i_pct_change] = (char *) palloc(64 * sizeof(char));
		values[i_status] = (char *) palloc((STATUS_LEN + 1) * sizeof(char));

#ifdef DEBUG
		dump_mwf1_inputs(acct_id, cust_id, ending_co_id, industry_name,
				buf, starting_co_id);
#endif

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;


		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();

		if (cust_id != 0) {
			sprintf(sql, MWF1_1, cust_id);
		} else if (industry_name[0] != '\0') {
			sprintf(sql, MWF1_2, industry_name, starting_co_id, ending_co_id);
		} else if (acct_id != 0) {
			sprintf(sql, MWF1_3, acct_id);
		} else {
			status = BAD_INPUT_DATA;
		}
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret != SPI_OK_SELECT) {
			dump_mwf1_inputs(acct_id, cust_id, ending_co_id, industry_name,
					buf, starting_co_id);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
		}

		if (status != BAD_INPUT_DATA) {
			int count = SPI_processed;

			TupleDesc tupdesc4;
			SPITupleTable *tuptable4 = NULL;
			HeapTuple tuple4 = NULL;

			char *symbol;
			char *new_price;
			char *old_price;
			char *s_num_out;

			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			for (i = 0; i < count; i++) {
				tuple = tuptable->vals[i];
				symbol = SPI_getvalue(tuple, tupdesc, 1);
#ifdef DEBUG
				elog(NOTICE, "  symbol = '%s'", symbol);
#endif /* DEBUG */

				sprintf(sql, MWF1_4, symbol);
#ifdef DEBUG
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				ret = SPI_exec(sql, 0);
				if (ret != SPI_OK_SELECT) {
					dump_mwf1_inputs(acct_id, cust_id, ending_co_id,
							industry_name, buf, starting_co_id);
					FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
				}
				tupdesc4 = SPI_tuptable->tupdesc;
				tuptable4 = SPI_tuptable;
				tuple4 = tuptable4->vals[0];
				new_price = SPI_getvalue(tuple4, tupdesc4, 1);
#ifdef DEBUG
				elog(NOTICE, "  new_price  = %s", new_price);
				elog(NOTICE, "  new_price  = %f", atof(new_price));
#endif /* DEBUG */

				sprintf(sql, MWF1_5, symbol);
#ifdef DEBUG
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				ret = SPI_exec(sql, 0);
				if (ret != SPI_OK_SELECT) {
					dump_mwf1_inputs(acct_id, cust_id, ending_co_id,
							industry_name, buf, starting_co_id);
					elog(NOTICE, "ERROR: sql not ok = %d", ret);
				}
				tupdesc4 = SPI_tuptable->tupdesc;
				tuptable4 = SPI_tuptable;
				tuple4 = tuptable4->vals[0];
				s_num_out = SPI_getvalue(tuple4, tupdesc4, 1);
#ifdef DEBUG
				elog(NOTICE, "  s_num_out  = %s", s_num_out);
#endif /* DEBUG */

				sprintf(sql, MWF1_6, symbol, pstrdup(buf));
#ifdef DEBUG
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				ret = SPI_exec(sql, 0);
				if (ret != SPI_OK_SELECT) {
					dump_mwf1_inputs(acct_id, cust_id, ending_co_id,
							industry_name, buf, starting_co_id);
					FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
				}

				if (SPI_processed == 0) {
					elog(NOTICE, "No rows returned for getting old_price.");
				} else {
					tupdesc4 = SPI_tuptable->tupdesc;
					tuptable4 = SPI_tuptable;
					tuple4 = tuptable4->vals[0];
					old_price = SPI_getvalue(tuple4, tupdesc4, 1);
#ifdef DEBUG
					elog(NOTICE, "  old_price  = %s", old_price);
					elog(NOTICE, "  old_price  = %f", atof(old_price));
#endif /* DEBUG */
					old_mkt_cap += atof(s_num_out) * atof(old_price);
				}
				new_mkt_cap += atof(s_num_out) * atof(new_price);

#ifdef DEBUG
				elog(NOTICE, "old_mkt_cap = %f", old_mkt_cap);
				elog(NOTICE, "new_mkt_cap = %f", new_mkt_cap);
#endif /* DEBUG */
			}
			pct_change = 100.0 * (new_mkt_cap / old_mkt_cap - 1.0);
#ifdef DEBUG
			elog(NOTICE, "pct_change = %f", pct_change);
#endif /* DEBUG */
		}
		sprintf(values[i_pct_change], "%f", pct_change);
		sprintf(values[i_status], "%d", status);

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) !=
				TYPEFUNC_COMPOSITE) {
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("function returning record called in context "
							"that cannot accept type record")));
		}

		/*
		 * generate attribute metadata needed later to produce tuples from raw
		 * C strings
		 */
		attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->attinmeta = attinmeta;

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	attinmeta = funcctx->attinmeta;

	if (call_cntr < max_calls) {
		/* do when there is more left to send */
		HeapTuple tuple;
		Datum result;

#ifdef DEBUG                                                                    
		for (i = 0; i < 2; i++) {
			elog(NOTICE, "MWF1 OUT: %d %s", i, values[i]);
		}
#endif /* DEBUG */

		/* Build a tuple. */
		tuple = BuildTupleFromCStrings(attinmeta, values);

		/* Make the tuple into a datum. */
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	} else {
		/* Do when there is no more left. */
		SPI_finish();
		SRF_RETURN_DONE(funcctx);
	}
}
