/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2007 Mark Wong
 *
 * Based on TPC-E Standard Specification Revision 1.3.
 */

#include <sys/types.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h> /* this should include most necessary APIs */
#include <executor/executor.h>  /* for GetAttributeByName() */
#include <funcapi.h> /* for returning set of rows in order_status */
#include <utils/lsyscache.h>
#include <utils/datetime.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define USE_ISO_DATES 1
#define MAXDATEFIELDS 25
#define MAXDATELEN 63

#define TLF1_1 \
		"SELECT t_bid_price, t_exec_name, t_is_cash, tt_is_mrkt,\n" \
		"       t_trade_price\n" \
		"FROM trade, trade_type\n" \
		"WHERE t_id = %ld\n" \
		"  AND t_tt_id = tt_id"

#define TLF1_2 \
		"SELECT se_amt, se_cash_due_date, se_cash_type\n" \
		"FROM settlement\n" \
		"WHERE se_t_id = %ld"

#define TLF1_3 \
		"SELECT ct_amt, ct_dts, ct_name\n" \
		"FROM cash_transaction\n" \
		"WHERE ct_t_id = %ld"

#define TLF1_4 \
		"SELECT th_dts, th_st_id\n" \
		"FROM trade_history\n" \
		"WHERE th_t_id = %ld\n" \
		"ORDER BY th_dts\n" \
		"LIMIT 3"

#define TLF2_1 \
		"SELECT t_bid_price, t_exec_name, t_is_cash, t_id, t_trade_price\n" \
		"FROM trade\n" \
		"WHERE t_ca_id = %d\n" \
		"  AND t_dts >= '%s'\n" \
		"  AND t_dts <= '%s'\n" \
		"ORDER BY t_dts\n" \
		"LIMIT %d"

#define TLF2_2 \
		"SELECT se_amt, se_cash_due_date, se_cash_type\n" \
		"FROM settlement\n" \
		"WHERE se_t_id = %s"

#define TLF2_3 \
		"SELECT ct_amt, ct_dts, ct_name\n" \
		"FROM cash_transaction\n" \
		"WHERE ct_t_id = %s"

#define TLF2_4 \
		"SELECT th_dts, th_st_id\n" \
		"FROM trade_history\n" \
		"WHERE th_t_id = %s\n" \
		"ORDER BY th_dts\n" \
		"LIMIT 3"

#define TLF3_1 \
		"SELECT t_ca_id, t_exec_name, t_is_cash, t_trade_price, t_qty,\n" \
		"       t_dts, t_id, t_tt_id\n" \
		"FROM trade\n" \
		"WHERE t_s_symb = '%s'\n" \
		"  AND t_dts >= '%s'\n" \
		"  AND t_dts <= '%s'\n" \
		"ORDER BY t_dts ASC\n" \
		"LIMIT %d"

#define TLF3_2 \
		"SELECT se_amt, se_cash_due_date, se_cash_type\n" \
		"FROM settlement\n" \
		"WHERE se_t_id = %s"

#define TLF3_3 \
		"SELECT ct_amt, ct_dts, ct_name\n" \
		"FROM cash_transaction\n" \
		"WHERE ct_t_id = %s\n" \
		"ORDER BY th_dts ASC" \

#define TLF3_4 \
		"SELECT th_dts, th_st_id\n" \
		"FROM trade_history\n" \
		"WHERE th_t_id = %s\n" \
		"ORDER BY th_dts ASC\n" \
		"LIMIT 3"

#define TLF4_1 \
		"SELECT t_id\n" \
		"FROM trade\n" \
		"WHERE t_ca_id = %d\n" \
		"  AND t_dts >= '%s'\n" \
		"ORDER BY t_dts ASC\n" \
		"LIMIT 1"

#define TLF4_2 \
		"SELECT hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty\n" \
		"FROM holding_history\n" \
		"WHERE hh_h_t_id IN (\n" \
		"      SELECT hh_h_t_id\n" \
		"      FROM holding_history\n" \
		"      WHERE hh_t_id = %s)"

/* Prototypes to prevent potential gcc warnings. */
Datum TradeLookupFrame1(PG_FUNCTION_ARGS);
Datum TradeLookupFrame2(PG_FUNCTION_ARGS);
Datum TradeLookupFrame3(PG_FUNCTION_ARGS);
Datum TradeLookupFrame4(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(TradeLookupFrame1);
PG_FUNCTION_INFO_V1(TradeLookupFrame2);
PG_FUNCTION_INFO_V1(TradeLookupFrame3);
PG_FUNCTION_INFO_V1(TradeLookupFrame4);

/* Clause 3.3.6.3 */
Datum TradeLookupFrame1(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

	int i;

	int ndim, nitems;
	int *dim;
	char *p;	

	char **values = NULL;
	enum tlf1 { bid_price=0, cash_transaction_amount, cash_transaction_dts,
			cash_transaction_name, exec_name, is_cash, is_market, num_found,
			settlement_amount, settlement_cash_due_date, settlement_cash_type,
			status, trade_history_dts, trade_history_status_id, trade_price };

	int num_found_count = 0;

	/* Helper counters to determine when to add commas. */
	int num_cash_txn = 0;
	int num_settlement = 0;
	int num_history = 0;

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		int max_trades = PG_GETARG_INT32(0);
		ArrayType *trade_id_p = PG_GETARG_ARRAYTYPE_P(1);

		int16 typlen;
		bool typbyval;
		char typalign;

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;

		char sql[2048];

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 15);
		
		values[bid_price] =
				(char *) palloc((12 * max_trades + 2) * sizeof(char));
		values[cash_transaction_amount] =
				(char *) palloc((14 * max_trades + 2) * sizeof(char));
		values[cash_transaction_dts] =
				(char *) palloc((27 * max_trades + 2) * sizeof(char));
		values[cash_transaction_name] =
				(char *) palloc((101 * max_trades + 2) * sizeof(char));
		values[exec_name] =
				(char *) palloc((65 * max_trades + 2) * sizeof(char));
		values[is_cash] = (char *) palloc((2 * max_trades + 2) * sizeof(char));
		values[is_market] =
				(char *) palloc((2 * max_trades + 2) * sizeof(char));
		values[num_found] = (char *) palloc(5 * sizeof(char));
		values[settlement_amount] =
				(char *) palloc((14 * max_trades + 2) * sizeof(char));
		values[settlement_cash_due_date] =
				(char *) palloc((27 * max_trades + 2) * sizeof(char));
		values[settlement_cash_type] =
				(char *) palloc((41 * max_trades + 2) * sizeof(char));
		values[status] = (char *) palloc(2 * sizeof(char));
		values[trade_history_dts] =
				(char *) palloc((27 * max_trades * 3 + 2) * sizeof(char));
		values[trade_history_status_id] =
				(char *) palloc((5 * max_trades * 3 + 2) * sizeof(char));
		values[trade_price] =
				(char *) palloc((12 * max_trades + 2) * sizeof(char));

		strcpy(values[status], "0");

		/*
		 * This might be overkill since we always expect single dimensions
		 * arrays.  This is not necessary if we trust the input.
		 */
		ndim = ARR_NDIM(trade_id_p);
		dim = ARR_DIMS(trade_id_p);
		nitems = ArrayGetNItems(ndim, dim);

		get_typlenbyvalalign(ARR_ELEMTYPE(trade_id_p), &typlen, &typbyval,
				&typalign);
		p = ARR_DATA_PTR(trade_id_p);

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();

#ifdef DEBUG
		elog(NOTICE, "IN");
		elog(NOTICE, "[0] %d", max_trades);
		for (i = 0; i < nitems; i++) {
			elog(NOTICE, "[1.%d] %ld", i, (long) (((long *) p)[i]));
		}
#endif
		strcpy(values[bid_price], "{");
		strcpy(values[cash_transaction_amount], "{");
		strcpy(values[cash_transaction_dts], "{");
		strcpy(values[cash_transaction_name], "{");
		strcpy(values[exec_name], "{");
		strcpy(values[is_cash], "{");
		strcpy(values[is_market], "{");
		strcpy(values[settlement_amount], "{");
		strcpy(values[settlement_cash_due_date], "{");
		strcpy(values[settlement_cash_type], "{");
		strcpy(values[trade_history_dts], "{");
		strcpy(values[trade_history_status_id], "{");
		strcpy(values[trade_price], "{");
		for (i = 0; i < nitems; i++) {
			char *is_cash_str;

			sprintf(sql, TLF1_1, (long) (((long *) p)[i]));
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret == SPI_OK_SELECT) {
				if (SPI_processed > 0) {
					tupdesc = SPI_tuptable->tupdesc;
					tuptable = SPI_tuptable;
					tuple = tuptable->vals[0];
					++num_found_count;
				} else {
					strcpy(values[status], "1");
					continue;
				}
			} else {
				strcpy(values[status], "1");
				elog(NOTICE, "ERROR: sql not ok");
				continue;
			}

			if (num_found_count > 1) {
				strcat(values[bid_price], ",");
				strcat(values[exec_name], ",");
				strcat(values[is_cash], ",");
				strcat(values[is_market], ",");
				strcat(values[trade_price], ",");
			}
			strcat(values[bid_price], SPI_getvalue(tuple, tupdesc, 1));
			strcat(values[exec_name], SPI_getvalue(tuple, tupdesc, 2));
			/* Use the is_cash point more easier reference later. */
			is_cash_str = SPI_getvalue(tuple, tupdesc, 3);
			strcat(values[is_cash], is_cash_str);
			strcat(values[is_market], SPI_getvalue(tuple, tupdesc, 4));
			strcat(values[trade_price], SPI_getvalue(tuple, tupdesc, 5));
#ifdef DEBUG
			elog(NOTICE, "t_is_cash = %s", is_cash_str);
#endif /* DEBUG */

			sprintf(sql, TLF1_2, (long) (((long *) p)[i]));
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret == SPI_OK_SELECT) {
				if (SPI_processed > 0) {
					tupdesc = SPI_tuptable->tupdesc;
					tuptable = SPI_tuptable;
					tuple = tuptable->vals[0];
					if (num_settlement > 0) {
						strcat(values[settlement_amount], ",");
						strcat(values[settlement_cash_due_date], ",");
						strcat(values[settlement_cash_type], ",");
					}
					strcat(values[settlement_amount],
							SPI_getvalue(tuple, tupdesc, 1));
					strcat(values[settlement_cash_due_date],
							SPI_getvalue(tuple, tupdesc, 2));
					strcat(values[settlement_cash_type],
							SPI_getvalue(tuple, tupdesc, 3));
					++num_settlement;
				}
			} else {
				strcpy(values[status], "1");
				elog(NOTICE, "ERROR: sql not ok");
			}

			if (is_cash_str[0] == '1') {
				sprintf(sql, TLF1_3, (long) (((long *) p)[i]));
#ifdef DEBUG
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				ret = SPI_exec(sql, 0);
				if (ret == SPI_OK_SELECT) {
					if (SPI_processed > 0) {
						tupdesc = SPI_tuptable->tupdesc;
						tuptable = SPI_tuptable;
						tuple = tuptable->vals[0];
						if (num_cash_txn > 0) {
							strcat(values[cash_transaction_amount], ",");
							strcat(values[cash_transaction_dts], ",");
							strcat(values[cash_transaction_name], ",");
						}
						strcat(values[cash_transaction_amount],
								SPI_getvalue(tuple, tupdesc, 1));
						strcat(values[cash_transaction_dts],
								SPI_getvalue(tuple, tupdesc, 2));
						strcat(values[cash_transaction_name],
								SPI_getvalue(tuple, tupdesc, 3));
						++num_cash_txn;
					}
				} else {
					strcpy(values[status], "1");
					elog(NOTICE, "ERROR: sql not ok");
				}
			}

			sprintf(sql, TLF1_4, (long) (((long *) p)[i]));
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret == SPI_OK_SELECT) {
				if (SPI_processed > 0) {
					tupdesc = SPI_tuptable->tupdesc;
					tuptable = SPI_tuptable;
					tuple = tuptable->vals[0];
					if (num_history > 0) {
						strcat(values[trade_history_dts], ",");
						strcat(values[trade_history_status_id], ",");
					}
					strcat(values[trade_history_dts],
							SPI_getvalue(tuple, tupdesc, 1));
					strcat(values[trade_history_status_id],
							 SPI_getvalue(tuple, tupdesc, 2));
					++num_history;
				}
			} else {
				strcpy(values[status], "1");
				elog(NOTICE, "ERROR: sql not ok");
			}
		}
		strcat(values[bid_price], "}");
		strcat(values[cash_transaction_amount], "}");
		strcat(values[cash_transaction_dts], "}");
		strcat(values[cash_transaction_name], "}");
		strcat(values[exec_name], "}");
		strcat(values[is_cash], "}");
		strcat(values[is_market], "}");
		strcat(values[settlement_amount], "}");
		strcat(values[settlement_cash_due_date], "}");
		strcat(values[settlement_cash_type], "}");
		strcat(values[trade_history_dts], "}");
		strcat(values[trade_history_status_id], "}");
		strcat(values[trade_price], "}");

		sprintf(values[num_found], "%d", num_found_count);

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
		funcctx->max_calls = 1;
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
		for (i = 0; i < 15; i++) {
			elog(NOTICE, "%d %s", i, values[i]);
		}
#endif /* DEBUG */

		/* Build a tuple. */
		tuple = BuildTupleFromCStrings(attinmeta, values);

		/* Make the tuple into a datum. */
		result = HeapTupleGetDatum(tuple);

		/* Clean up. */
		for (i = 0; i < 15; i++) {
			pfree(values[i]);
		} 
		pfree(values);

		SRF_RETURN_NEXT(funcctx, result);
	} else {
		/* Do when there is no more left. */
		SPI_finish();
		SRF_RETURN_DONE(funcctx);
	}
}

/* Clause 3.3.6.3 */
Datum TradeLookupFrame2(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

	int i;

	char **values = NULL;
	enum tlf2 { bid_price=0, cash_transaction_amount, cash_transaction_dts,
			cash_transaction_name, exec_name, is_cash, num_found,
			settlement_amount, settlement_cash_due_date, settlement_cash_type,
			status, trade_history_dts, trade_history_status_id, trade_list,
			trade_price };

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		int acct_id = PG_GETARG_INT64(0);
		Timestamp end_trade_dts_ts = PG_GETARG_TIMESTAMP(1);
		int max_trades = PG_GETARG_INT32(2);
		Timestamp start_trade_dts_ts = PG_GETARG_TIMESTAMP(3);

		struct pg_tm tt, *tm = &tt;
		fsec_t fsec;
		char *tzn = NULL;
		char end_trade_dts[MAXDATELEN + 1];
		char start_trade_dts[MAXDATELEN + 1];

		char sql[2048];

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;

		int num_found_count = 0;
		int num_settlements = 0;
		int num_cash_txn = 0;
		int num_trade_history = 0;

		if (timestamp2tm(end_trade_dts_ts, NULL, tm, &fsec, NULL, NULL) == 0) {
			EncodeDateTime(tm, fsec, NULL, &tzn, USE_ISO_DATES, end_trade_dts);
		}
		if (timestamp2tm(start_trade_dts_ts, NULL, tm, &fsec, NULL,
				NULL) == 0) {
			EncodeDateTime(tm, fsec, NULL, &tzn, USE_ISO_DATES,
					start_trade_dts);
		}

#ifdef DEBUG
		elog(NOTICE, "acct_id = %d", acct_id);
		elog(NOTICE, "end_trade_dts = %s", end_trade_dts);
		elog(NOTICE, "max_trades = %d", max_trades);
		elog(NOTICE, "start_trade_dts = %s", start_trade_dts);
#endif /* DEBUG */

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 15);
		values[bid_price] =
				(char *) palloc((12 * max_trades + 2) * sizeof(char));
		values[cash_transaction_amount] =
				(char *) palloc((14 * max_trades + 2) * sizeof(char));
		values[cash_transaction_dts] =
				(char *) palloc((27 * max_trades + 2) * sizeof(char));
		values[cash_transaction_name] =
				(char *) palloc((101 * max_trades + 2) * sizeof(char));
		values[exec_name] =
				(char *) palloc((65 * max_trades + 2) * sizeof(char));
		values[is_cash] = (char *) palloc((2 * max_trades + 2) * sizeof(char));
		values[num_found] = (char *) palloc(5 * sizeof(char));
		values[settlement_amount] =
				(char *) palloc((14 * max_trades + 2) * sizeof(char));
		values[settlement_cash_due_date] =
				(char *) palloc((27 * max_trades + 2) * sizeof(char));
		values[settlement_cash_type] =
				(char *) palloc((41 * max_trades + 2) * sizeof(char));
		values[status] = (char *) palloc(2 * sizeof(char));
		values[trade_history_dts] =
				(char *) palloc((27 * max_trades * 3 + 2) * sizeof(char));
		values[trade_history_status_id] =
				(char *) palloc((5 * max_trades * 3 + 2) * sizeof(char));
		values[trade_list] =
				(char *) palloc((11 * max_trades + 2) * sizeof(char));
		values[trade_price] =
				(char *) palloc((12 * max_trades + 2) * sizeof(char));

		strcpy(values[status], "0");

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();

		sprintf(sql, TLF2_1, acct_id, start_trade_dts, end_trade_dts,
				max_trades);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			num_found_count = SPI_processed;
			sprintf(values[num_found], "%d", num_found_count);
		} else {
			elog(NOTICE, "ERROR: sql not ok");
		}

#ifdef DEBUG
		elog(NOTICE, "num_found = %d", num_found_count);
#endif /* DEBUG */
		strcpy(values[bid_price], "{");
		strcpy(values[cash_transaction_amount], "{");
		strcpy(values[cash_transaction_dts], "{");
		strcpy(values[cash_transaction_name], "{");
		strcpy(values[exec_name], "{");
		strcpy(values[is_cash], "{");
		strcpy(values[settlement_amount], "{");
		strcpy(values[settlement_cash_due_date], "{");
		strcpy(values[settlement_cash_type], "{");
		strcpy(values[trade_history_dts], "{");
		strcpy(values[trade_history_status_id], "{");
		strcpy(values[trade_list], "{");
		strcpy(values[trade_price], "{");
		for (i = 0; i < num_found_count; i++) {
			TupleDesc tupdesc2;
			SPITupleTable *tuptable2 = NULL;
			HeapTuple tuple2 = NULL;

			char *trade_list_str;

			tuple = tuptable->vals[i];
			if (i > 0) {
				strcat(values[bid_price], ",");
				strcat(values[exec_name], ",");
				strcat(values[is_cash], ",");
				strcat(values[trade_list], ",");
				strcat(values[trade_price], ",");
			}
			strcat(values[bid_price], SPI_getvalue(tuple, tupdesc, 1));
			strcat(values[exec_name], SPI_getvalue(tuple, tupdesc, 2));
			strcat(values[is_cash], SPI_getvalue(tuple, tupdesc, 3));
			trade_list_str = SPI_getvalue(tuple, tupdesc, 4);
			strcat(values[trade_list], trade_list_str);
			strcat(values[trade_price], SPI_getvalue(tuple, tupdesc, 5));

			sprintf(sql, TLF2_2, trade_list_str);
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret == SPI_OK_SELECT) {
				if (SPI_processed > 0) {
					tupdesc2 = SPI_tuptable->tupdesc;
					tuptable2 = SPI_tuptable;
					tuple2 = tuptable2->vals[0];
					++num_settlements;
				} else {
					strcpy(values[status], "1");
					continue;
				}
			} else {
				strcpy(values[status], "1");
				elog(NOTICE, "ERROR: sql not ok");
				continue;
			}

			if (num_settlements > 1) {
				strcat(values[settlement_amount], ",");
				strcat(values[settlement_cash_due_date], ",");
				strcat(values[settlement_cash_type], ",");
			}
			strcat(values[settlement_amount],
					SPI_getvalue(tuple2, tupdesc2, 1));
			strcat(values[settlement_cash_due_date],
					SPI_getvalue(tuple2, tupdesc2, 2));
			strcat(values[settlement_cash_type],
					SPI_getvalue(tuple2, tupdesc2, 3));

			sprintf(sql, TLF2_3, trade_list_str);
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret == SPI_OK_SELECT) {
				if (SPI_processed > 0) {
					tupdesc2 = SPI_tuptable->tupdesc;
					tuptable2 = SPI_tuptable;
					tuple2 = tuptable2->vals[0];
					++num_cash_txn;
				} else {
					strcpy(values[status], "1");
					continue;
				}
			} else {
				elog(NOTICE, "ERROR: sql not ok");
				strcpy(values[status], "1");
				continue;
			}

			if (num_cash_txn > 1) {
				strcat(values[cash_transaction_amount], ",");
				strcat(values[cash_transaction_dts], ",");
				strcat(values[cash_transaction_name], ",");
			}
			strcat(values[cash_transaction_amount],
					SPI_getvalue(tuple2, tupdesc2, 1));
			strcat(values[cash_transaction_dts],
					SPI_getvalue(tuple2, tupdesc2, 2));
			strcat(values[cash_transaction_name],
					SPI_getvalue(tuple2, tupdesc2, 3));

			sprintf(sql, TLF2_4, trade_list_str);
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret == SPI_OK_SELECT) {
				if (SPI_processed > 0) {
					tupdesc2 = SPI_tuptable->tupdesc;
					tuptable2 = SPI_tuptable;
					tuple2 = tuptable2->vals[0];
					++num_trade_history;
				} else {
					strcpy(values[status], "1");
					continue;
				}
			} else {
				elog(NOTICE, "ERROR: sql not ok");
				strcpy(values[status], "1");
				continue;
			}

			if (num_trade_history > 1) {
				strcat(values[trade_history_dts], ",");
				strcat(values[trade_history_status_id], ",");
			}
			strcat(values[trade_history_dts],
					SPI_getvalue(tuple2, tupdesc2, 1));
			strcat(values[trade_history_status_id],
					SPI_getvalue(tuple2, tupdesc2, 2));
		}
		strcat(values[bid_price], "}");
		strcat(values[cash_transaction_amount], "}");
		strcat(values[cash_transaction_dts], "}");
		strcat(values[cash_transaction_name], "}");
		strcat(values[exec_name], "}");
		strcat(values[is_cash], "}");
		strcat(values[settlement_amount], "}");
		strcat(values[settlement_cash_due_date], "}");
		strcat(values[settlement_cash_type], "}");
		strcat(values[trade_history_dts], "}");
		strcat(values[trade_history_status_id], "}");
		strcat(values[trade_list], "}");
		strcat(values[trade_price], "}");

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
		funcctx->max_calls = 1;
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
		for (i = 0; i < 15; i++) {
			elog(NOTICE, "%d %s", i, values[i]);
		}
#endif /* DEBUG */

		/* Build a tuple. */
		tuple = BuildTupleFromCStrings(attinmeta, values);

		/* Make the tuple into a datum. */
		result = HeapTupleGetDatum(tuple);

		/* Clean up. */
		for (i = 0; i < 15; i++) {
			pfree(values[i]);
		} 
		pfree(values);

		SRF_RETURN_NEXT(funcctx, result);
	} else {
		/* Do when there is no more left. */
		SPI_finish();
		SRF_RETURN_DONE(funcctx);
	}
}

Datum TradeLookupFrame3(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

	int i;

	char **values = NULL;
	enum tlf3 { acct_id=0, cash_transaction_amount, cash_transaction_dts,
			cash_transaction_name, exec_name, is_cash, num_found,
			price, quantity, settlement_amount, settlement_cash_due_date,
			settlement_cash_type, status, trade_dts, trade_history_dts,
			trade_history_status_id, trade_list, trade_type };

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		Timestamp end_trade_dts_ts = PG_GETARG_TIMESTAMP(0);
/* Used only for engineering purposes...
		int max_acct_id = PG_GETARG_INT64(1);
*/
		int max_trades = PG_GETARG_INT32(2);
		Timestamp start_trade_dts_ts = PG_GETARG_TIMESTAMP(3);
		char *symbol_p = (char *) PG_GETARG_TEXT_P(4);
		char symbol[16];

		struct pg_tm tt, *tm = &tt;
		fsec_t fsec;
		char *tzn = NULL;
		char end_trade_dts[MAXDATELEN + 1];
		char start_trade_dts[MAXDATELEN + 1];

		char sql[2048];

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;

		int num_found_count = 0;
		int num_settlements = 0;
		int num_cash_txn = 0;
		int num_trade_history = 0;

		if (timestamp2tm(end_trade_dts_ts, NULL, tm, &fsec, NULL, NULL) == 0) {
			EncodeDateTime(tm, fsec, NULL, &tzn, USE_ISO_DATES, end_trade_dts);
		}
		if (timestamp2tm(start_trade_dts_ts, NULL, tm, &fsec, NULL,
				NULL) == 0) {
			EncodeDateTime(tm, fsec, NULL, &tzn, USE_ISO_DATES,
					start_trade_dts);
		}
		strcpy(symbol, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(symbol_p))));

#ifdef DEBUG
/*
		elog(NOTICE, "max_acct_id = %d", acct_id);
*/
		elog(NOTICE, "end_trade_dts = %s", end_trade_dts);
		elog(NOTICE, "max_trades = %d", max_trades);
		elog(NOTICE, "start_trade_dts = %s", start_trade_dts);
		elog(NOTICE, "symbol = %s", symbol);
#endif /* DEBUG */

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 18);
		values[acct_id] =
				(char *) palloc((11 * max_trades + 2) * sizeof(char));
		values[cash_transaction_amount] =
				(char *) palloc((14 * max_trades + 2) * sizeof(char));
		values[cash_transaction_dts] =
				(char *) palloc((27 * max_trades + 2) * sizeof(char));
		values[cash_transaction_name] =
				(char *) palloc((101 * max_trades + 2) * sizeof(char));
		values[exec_name] =
				(char *) palloc((65 * max_trades + 2) * sizeof(char));
		values[is_cash] = (char *) palloc((2 * max_trades + 2) * sizeof(char));
		values[num_found] = (char *) palloc(5 * sizeof(char));
		values[price] = (char *) palloc((12 * max_trades + 2) * sizeof(char));
		values[quantity] = (char *) palloc((6 * max_trades + 2) * sizeof(char));
		values[settlement_amount] =
				(char *) palloc((14 * max_trades + 2) * sizeof(char));
		values[settlement_cash_due_date] =
				(char *) palloc((27 * max_trades + 2) * sizeof(char));
		values[settlement_cash_type] =
				(char *) palloc((41 * max_trades + 2) * sizeof(char));
		values[status] = (char *) palloc(2 * sizeof(char));
		values[trade_dts] =
				(char *) palloc((27 * max_trades * 3 + 2) * sizeof(char));
		values[trade_history_dts] =
				(char *) palloc((27 * max_trades * 3 + 2) * sizeof(char));
		values[trade_history_status_id] =
				(char *) palloc((5 * max_trades * 3 + 2) * sizeof(char));
		values[trade_list] =
				(char *) palloc((11 * max_trades + 2) * sizeof(char));
		values[trade_type] =
				(char *) palloc((4 * max_trades + 2) * sizeof(char));

		strcpy(values[status], "0");

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();

		sprintf(sql, TLF3_1, symbol, start_trade_dts, end_trade_dts,
				max_trades);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			num_found_count = SPI_processed;
			sprintf(values[num_found], "%d", num_found_count);
		} else {
			elog(NOTICE, "ERROR: sql not ok");
		}

#ifdef DEBUG
		elog(NOTICE, "num_found = %d", num_found_count);
#endif /* DEBUG */
		strcpy(values[acct_id], "{");
		strcpy(values[cash_transaction_amount], "{");
		strcpy(values[cash_transaction_dts], "{");
		strcpy(values[cash_transaction_name], "{");
		strcpy(values[exec_name], "{");
		strcpy(values[is_cash], "{");
		strcpy(values[price], "{");
		strcpy(values[quantity], "{");
		strcpy(values[settlement_amount], "{");
		strcpy(values[settlement_cash_due_date], "{");
		strcpy(values[settlement_cash_type], "{");
		strcpy(values[trade_dts], "{");
		strcpy(values[trade_history_dts], "{");
		strcpy(values[trade_history_status_id], "{");
		strcpy(values[trade_list], "{");
		strcpy(values[trade_type], "{");
		for (i = 0; i < num_found_count; i++) {
			TupleDesc tupdesc2;
			SPITupleTable *tuptable2 = NULL;
			HeapTuple tuple2 = NULL;

			char *trade_list_str;

			tuple = tuptable->vals[i];
			if (i > 0) {
				strcat(values[acct_id], ",");
				strcat(values[exec_name], ",");
				strcat(values[is_cash], ",");
				strcat(values[price], ",");
				strcat(values[trade_dts], ",");
				strcat(values[trade_list], ",");
				strcat(values[trade_type], ",");
			}
			strcat(values[acct_id], SPI_getvalue(tuple, tupdesc, 1));
			strcat(values[exec_name], SPI_getvalue(tuple, tupdesc, 2));
			strcat(values[is_cash], SPI_getvalue(tuple, tupdesc, 3));
			strcat(values[price], SPI_getvalue(tuple, tupdesc, 4));
			strcat(values[quantity], SPI_getvalue(tuple, tupdesc, 5));
			strcat(values[trade_dts], SPI_getvalue(tuple, tupdesc, 6));
			trade_list_str = SPI_getvalue(tuple, tupdesc, 4);
			strcat(values[trade_list], trade_list_str);
			strcat(values[trade_type], SPI_getvalue(tuple, tupdesc, 5));

			sprintf(sql, TLF3_2, trade_list_str);
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret == SPI_OK_SELECT) {
				if (SPI_processed > 0) {
					tupdesc2 = SPI_tuptable->tupdesc;
					tuptable2 = SPI_tuptable;
					tuple2 = tuptable2->vals[0];
					++num_settlements;
				} else {
					strcpy(values[status], "1");
					continue;
				}
			} else {
				strcpy(values[status], "1");
				elog(NOTICE, "ERROR: sql not ok");
				continue;
			}

			if (num_settlements > 1) {
				strcat(values[settlement_amount], ",");
				strcat(values[settlement_cash_due_date], ",");
				strcat(values[settlement_cash_type], ",");
			}
			strcat(values[settlement_amount],
					SPI_getvalue(tuple2, tupdesc2, 1));
			strcat(values[settlement_cash_due_date],
					SPI_getvalue(tuple2, tupdesc2, 2));
			strcat(values[settlement_cash_type],
					SPI_getvalue(tuple2, tupdesc2, 3));

			sprintf(sql, TLF3_3, trade_list_str);
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret == SPI_OK_SELECT) {
				if (SPI_processed > 0) {
					tupdesc2 = SPI_tuptable->tupdesc;
					tuptable2 = SPI_tuptable;
					tuple2 = tuptable2->vals[0];
					++num_cash_txn;
				} else {
					strcpy(values[status], "1");
					continue;
				}
			} else {
				elog(NOTICE, "ERROR: sql not ok");
				strcpy(values[status], "1");
				continue;
			}

			if (num_cash_txn > 1) {
				strcat(values[cash_transaction_amount], ",");
				strcat(values[cash_transaction_dts], ",");
				strcat(values[cash_transaction_name], ",");
			}
			strcat(values[cash_transaction_amount],
					SPI_getvalue(tuple2, tupdesc2, 1));
			strcat(values[cash_transaction_dts],
					SPI_getvalue(tuple2, tupdesc2, 2));
			strcat(values[cash_transaction_name],
					SPI_getvalue(tuple2, tupdesc2, 3));

			sprintf(sql, TLF3_4, trade_list_str);
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret == SPI_OK_SELECT) {
				if (SPI_processed > 0) {
					tupdesc2 = SPI_tuptable->tupdesc;
					tuptable2 = SPI_tuptable;
					tuple2 = tuptable2->vals[0];
					++num_trade_history;
				} else {
					strcpy(values[status], "1");
					continue;
				}
			} else {
				elog(NOTICE, "ERROR: sql not ok");
				strcpy(values[status], "1");
				continue;
			}

			if (num_trade_history > 1) {
				strcat(values[trade_history_dts], ",");
				strcat(values[trade_history_status_id], ",");
			}
			strcat(values[trade_history_dts],
					SPI_getvalue(tuple2, tupdesc2, 1));
			strcat(values[trade_history_status_id],
					SPI_getvalue(tuple2, tupdesc2, 2));
		}
		strcat(values[acct_id], "}");
		strcat(values[cash_transaction_amount], "}");
		strcat(values[cash_transaction_dts], "}");
		strcat(values[cash_transaction_name], "}");
		strcat(values[exec_name], "}");
		strcat(values[is_cash], "}");
		strcat(values[price], "}");
		strcat(values[quantity], "}");
		strcat(values[settlement_amount], "}");
		strcat(values[settlement_cash_due_date], "}");
		strcat(values[settlement_cash_type], "}");
		strcat(values[trade_dts], "}");
		strcat(values[trade_history_dts], "}");
		strcat(values[trade_history_status_id], "}");
		strcat(values[trade_list], "}");
		strcat(values[trade_type], "}");

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
		funcctx->max_calls = 1;
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
		for (i = 0; i < 18; i++) {
			elog(NOTICE, "%d %s", i, values[i]);
		}
#endif /* DEBUG */

		/* Build a tuple. */
		tuple = BuildTupleFromCStrings(attinmeta, values);

		/* Make the tuple into a datum. */
		result = HeapTupleGetDatum(tuple);

		/* Clean up. */
		for (i = 0; i < 18; i++) {
			pfree(values[i]);
		} 
		pfree(values);

		SRF_RETURN_NEXT(funcctx, result);
	} else {
		/* Do when there is no more left. */
		SPI_finish();
		SRF_RETURN_DONE(funcctx);
	}
}

/* Clause 3.3.6.6 */
Datum TradeLookupFrame4(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

	int i;

	char **values = NULL;
	enum tlf4 { holding_history_id=0, holding_history_trade_id,
			num_found, quantity_after, quantity_before, status, trade_id };

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		int acct_id = PG_GETARG_INT64(0);
		Timestamp start_trade_dts_ts = PG_GETARG_TIMESTAMP(1);

		struct pg_tm tt, *tm = &tt;
		fsec_t fsec;
		char *tzn = NULL;
		char start_trade_dts[MAXDATELEN + 1];

		char sql[2048];

		int num_found_count = 0;

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;

		if (timestamp2tm(start_trade_dts_ts, NULL, tm, &fsec, NULL,
				NULL) == 0) {
			EncodeDateTime(tm, fsec, NULL, &tzn, USE_ISO_DATES,
					start_trade_dts);
		}

#ifdef DEBUG
		elog(NOTICE, "acct_id = %d", acct_id);
		elog(NOTICE, "start_trade_dts = %s", start_trade_dts);
#endif /* DEBUG */

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 7);
		values[holding_history_id] =
				(char *) palloc((11 * 20 + 2) * sizeof(char));
		values[holding_history_trade_id] =
				(char *) palloc((11 * 20 + 2) * sizeof(char));
		values[num_found] = (char *) palloc(5 * sizeof(char));
		values[quantity_after] = (char *) palloc((6 * 20 + 2) * sizeof(char));
		values[quantity_before] = (char *) palloc((6 * 20 + 2) * sizeof(char));
		values[status] = (char *) palloc(2 * sizeof(char));
		values[trade_id] = (char *) palloc(11 * sizeof(char));

		strcpy(values[status], "0");

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();

		sprintf(sql, TLF4_1, acct_id, start_trade_dts);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			if (SPI_processed > 0) {
				tuple = tuptable->vals[0];
				sprintf(values[trade_id], "%s",
						SPI_getvalue(tuple, tupdesc, 1));
			}
		} else {
			strcpy(values[status], "1");
			elog(NOTICE, "ERROR: sql not ok");
		}

		sprintf(sql, TLF4_2, values[trade_id]);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
		} else {
			elog(NOTICE, "ERROR: sql not ok");
		}

		num_found_count = SPI_processed;
		sprintf(values[num_found], "%d", num_found_count);
#ifdef DEBUG
		elog(NOTICE, "num_found = %d", num_found_count);
#endif /* DEBUG */

		strcpy(values[holding_history_id], "{");
		strcpy(values[holding_history_trade_id], "{");
		strcpy(values[quantity_after], "{");
		strcpy(values[quantity_before], "{");
		for (i = 0; i < num_found_count; i++) {
			tuple = tuptable->vals[i];
			if (i > 0) {
				strcat(values[holding_history_id], ",");
				strcat(values[holding_history_trade_id], ",");
				strcat(values[quantity_after], ",");
				strcat(values[quantity_before], ",");
			}
			strcat(values[holding_history_id], SPI_getvalue(tuple, tupdesc, 1));
			strcat(values[holding_history_trade_id],
					SPI_getvalue(tuple, tupdesc, 2));
			strcat(values[quantity_after], SPI_getvalue(tuple, tupdesc, 3));
			strcat(values[quantity_before], SPI_getvalue(tuple, tupdesc, 4));
		}
		strcat(values[holding_history_id], "}");
		strcat(values[holding_history_trade_id], "}");
		strcat(values[quantity_after], "}");
		strcat(values[quantity_before], "}");

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
		funcctx->max_calls = 1;
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
		for (i = 0; i < 7; i++) {
			elog(NOTICE, "%d %s", i, values[i]);
		}
#endif /* DEBUG */

		/* Build a tuple. */
		tuple = BuildTupleFromCStrings(attinmeta, values);

		/* Make the tuple into a datum. */
		result = HeapTupleGetDatum(tuple);

		/* Clean up. */
		for (i = 0; i < 7; i++) {
			pfree(values[i]);
		} 
		pfree(values);

		SRF_RETURN_NEXT(funcctx, result);
	} else {
		/* Do when there is no more left. */
		SPI_finish();
		SRF_RETURN_DONE(funcctx);
	}
}
