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
#include <utils/lsyscache.h>
#include <utils/datetime.h>
#include <utils/array.h>
#include <utils/builtins.h>

#include "frame.h"

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
		"WHERE t_ca_id = %ld\n" \
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
		"WHERE ct_t_id = %s"

#define TLF3_4 \
		"SELECT th_dts, th_st_id\n" \
		"FROM trade_history\n" \
		"WHERE th_t_id = %s\n" \
		"ORDER BY th_dts ASC\n" \
		"LIMIT 3"

#define TLF4_1 \
		"SELECT t_id\n" \
		"FROM trade\n" \
		"WHERE t_ca_id = %ld\n" \
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

void dump_tlf1_inputs(int, ArrayType *);
void dump_tlf2_inputs(long, char *, int, char *);
void dump_tlf3_inputs(char *, long, int, char *, char *);
void dump_tlf4_inputs(long, char *);

void dump_tlf1_inputs(int max_trades, ArrayType *trade_id_p)
{
	int ndim = ARR_NDIM(trade_id_p);
	int *dim = ARR_DIMS(trade_id_p);
	int nitems = ArrayGetNItems(ndim, dim);
	long *trade_id;

	int16 typlen;
	bool typbyval;
	char typalign;

	int i;

	get_typlenbyvalalign(ARR_ELEMTYPE(trade_id_p), &typlen, &typbyval,
			&typalign);
	trade_id = (long *) ARR_DATA_PTR(trade_id_p);

	elog(NOTICE, "TLF1: INPUTS START");
	elog(NOTICE, "TLF1: max_trades %d", max_trades);
	for (i = 0; i < nitems; i++) {
		elog(NOTICE, "TLF1: trade_id[%d] %ld", i, trade_id[i]);
	}
	elog(NOTICE, "TLF1: INPUTS END");
}

void dump_tlf2_inputs(long acct_id, char *end_trade_dts, int max_trades,
		char *start_trade_dts)
{
	elog(NOTICE, "TLF2: INPUTS START");
	elog(NOTICE, "TLF2: acct_id = %ld", acct_id);
	elog(NOTICE, "TLF2: end_trade_dts = %s", end_trade_dts);
	elog(NOTICE, "TLF2: max_trades = %d", max_trades);
	elog(NOTICE, "TLF2: start_trade_dts = %s", start_trade_dts);
	elog(NOTICE, "TLF2: INPUTS END");
}

void dump_tlf3_inputs(char *end_trade_dts, long max_acct_id, int max_trades,
		char *start_trade_dts, char *symbol)
{
	elog(NOTICE, "TLF3: INPUTS START");
	elog(NOTICE, "TLF3: end_trade_dts = %s", end_trade_dts);
	elog(NOTICE, "TLF3: max_acct_id = %ld", max_acct_id);
	elog(NOTICE, "TLF3: max_trades = %d", max_trades);
	elog(NOTICE, "TLF3: start_trade_dts = %s", start_trade_dts);
	elog(NOTICE, "TLF3: symbol = %s", symbol);
	elog(NOTICE, "TLF3: INPUTS END");
}

void dump_tlf4_inputs(long acct_id, char *start_trade_dts)
{
	elog(NOTICE, "TLF4: INPUTS START");
	elog(NOTICE, "TLF4: acct_id = %ld", acct_id);
	elog(NOTICE, "TLF4: start_trade_dts = %s", start_trade_dts);
	elog(NOTICE, "TLF4: INPUTS END");
}

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
	long *trade_id;	

	char **values = NULL;
	enum tlf1 {
			i_bid_price=0, i_cash_transaction_amount,
			i_cash_transaction_dts, i_cash_transaction_name, i_exec_name,
			i_is_cash, i_is_market, i_num_found, i_settlement_amount,
			i_settlement_cash_due_date, i_settlement_cash_type,
			i_status, i_trade_history_dts, i_trade_history_status_id,
			i_trade_price
	};

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
		
		values[i_bid_price] = (char *) palloc(((S_PRICE_T_LEN + 1) *
				max_trades + 2) * sizeof(char));
		values[i_cash_transaction_amount] = (char *) palloc(((VALUE_T_LEN +
				1) * max_trades + 2) * sizeof(char));
		values[i_cash_transaction_dts] = (char *) palloc(((MAXDATELEN + 1) *
				max_trades + 2) * sizeof(char));
		values[i_cash_transaction_name] = (char *) palloc(((CT_NAME_LEN + 3) *
				max_trades + 2) * sizeof(char));
		values[i_exec_name] = (char *) palloc(((T_EXEC_NAME_LEN + 3) *
				max_trades + 2) * sizeof(char));
		values[i_is_cash] = (char *) palloc(((BOOLEAN_LEN + 1) * max_trades +
				2) * sizeof(char));
		values[i_is_market] = (char *) palloc(((BOOLEAN_LEN + 1) * max_trades +
				2) * sizeof(char));
		values[i_num_found] = (char *) palloc((INTEGER_LEN + 1) * sizeof(char));
		values[i_settlement_amount] = (char *) palloc(((VALUE_T_LEN + 1) *
				max_trades + 2) * sizeof(char));
		values[i_settlement_cash_due_date] = (char *) palloc(((MAXDATELEN +
				1) * max_trades + 2) * sizeof(char));
		values[i_settlement_cash_type] = (char *) palloc(((SE_CASH_TYPE_LEN +
				1) * max_trades + 2) * sizeof(char));
		values[i_status] = (char *) palloc((STATUS_LEN + 1) * sizeof(char));
		values[i_trade_history_dts] = (char *) palloc((((MAXDATELEN + 1) *
				max_trades + 3) * 3 + 2) * sizeof(char));
		values[i_trade_history_status_id] = (char *) palloc((((ST_ID_LEN + 3) *
				max_trades + 3) * 3 + 2) * sizeof(char));
		values[i_trade_price] = (char *) palloc(((S_PRICE_T_LEN + 1) *
				max_trades + 2) * sizeof(char));

		strcpy(values[i_status], "0");

		/*
		 * This might be overkill since we always expect single dimensions
		 * arrays.  This is not necessary if we trust the input.
		 */
		ndim = ARR_NDIM(trade_id_p);
		dim = ARR_DIMS(trade_id_p);
		nitems = ArrayGetNItems(ndim, dim);

		get_typlenbyvalalign(ARR_ELEMTYPE(trade_id_p), &typlen, &typbyval,
				&typalign);
		trade_id = (long *) ARR_DATA_PTR(trade_id_p);

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();

#ifdef DEBUG
		dump_tlf1_inputs(max_trades, trade_id_p);
#endif
		strcpy(values[i_bid_price], "{");
		strcpy(values[i_cash_transaction_amount], "{");
		strcpy(values[i_cash_transaction_dts], "{");
		strcpy(values[i_cash_transaction_name], "{");
		strcpy(values[i_exec_name], "{");
		strcpy(values[i_is_cash], "{");
		strcpy(values[i_is_market], "{");
		strcpy(values[i_settlement_amount], "{");
		strcpy(values[i_settlement_cash_due_date], "{");
		strcpy(values[i_settlement_cash_type], "{");
		strcpy(values[i_trade_history_dts], "{");
		strcpy(values[i_trade_history_status_id], "{");
		strcpy(values[i_trade_price], "{");
		for (i = 0; i < nitems; i++) {
			char *is_cash_str;
			char *is_market_str;

			sprintf(sql, TLF1_1,  trade_id[i]);
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret == SPI_OK_SELECT) {
				if (SPI_processed > 0) {
					tupdesc = SPI_tuptable->tupdesc;
					tuptable = SPI_tuptable;
					tuple = tuptable->vals[0];
					num_found_count += SPI_processed;
				} else {
					dump_tlf1_inputs(max_trades, trade_id_p);
					FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
					continue;
				}
			} else {
				dump_tlf1_inputs(max_trades, trade_id_p);
				FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
				continue;
			}

			if (num_found_count > 1) {
				strcat(values[i_bid_price], ",");
				strcat(values[i_exec_name], ",");
				strcat(values[i_is_cash], ",");
				strcat(values[i_is_market], ",");
				strcat(values[i_trade_price], ",");
			}
			strcat(values[i_bid_price], SPI_getvalue(tuple, tupdesc, 1));
			strcat(values[i_exec_name], "\"");
			strcat(values[i_exec_name], SPI_getvalue(tuple, tupdesc, 2));
			strcat(values[i_exec_name], "\"");
			/* Use the is_cash pointer, easier to reference later. */
			is_cash_str = SPI_getvalue(tuple, tupdesc, 3);
			strcat(values[i_is_cash], (is_cash_str[0] == 't' ? "0" : "1"));
			is_market_str = SPI_getvalue(tuple, tupdesc, 4);
			strcat(values[i_is_market], (is_market_str[0] == 't' ? "0" : "1"));
			strcat(values[i_trade_price], SPI_getvalue(tuple, tupdesc, 5));
#ifdef DEBUG
			elog(NOTICE, "t_is_cash = %s", is_cash_str);
#endif /* DEBUG */

			sprintf(sql, TLF1_2, trade_id[i]);
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
						strcat(values[i_settlement_amount], ",");
						strcat(values[i_settlement_cash_due_date], ",");
						strcat(values[i_settlement_cash_type], ",");
					}
					strcat(values[i_settlement_amount],
							SPI_getvalue(tuple, tupdesc, 1));
					strcat(values[i_settlement_cash_due_date],
							SPI_getvalue(tuple, tupdesc, 2));
					strcat(values[i_settlement_cash_type],
							SPI_getvalue(tuple, tupdesc, 3));
					++num_settlement;
				}
			} else {
				dump_tlf1_inputs(max_trades, trade_id_p);
				FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
			}

			if (is_cash_str[0] == 't') {
				sprintf(sql, TLF1_3, trade_id[i]);
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
							strcat(values[i_cash_transaction_amount], ",");
							strcat(values[i_cash_transaction_dts], ",");
							strcat(values[i_cash_transaction_name], ",");
						}
						strcat(values[i_cash_transaction_amount],
								SPI_getvalue(tuple, tupdesc, 1));
						strcat(values[i_cash_transaction_dts],
								SPI_getvalue(tuple, tupdesc, 2));
						strcat(values[i_cash_transaction_name], "\"");
						strcat(values[i_cash_transaction_name],
								SPI_getvalue(tuple, tupdesc, 3));
						strcat(values[i_cash_transaction_name], "\"");
						++num_cash_txn;
					}
				} else {
					dump_tlf1_inputs(max_trades, trade_id_p);
					FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
				}
			}

			sprintf(sql, TLF1_4,  trade_id[i]);
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret == SPI_OK_SELECT) {
				if (SPI_processed > 0) {
					int j;
					tupdesc = SPI_tuptable->tupdesc;
					tuptable = SPI_tuptable;
					tuple = tuptable->vals[0];
					if (num_history > 0) {
						strcat(values[i_trade_history_dts], ",");
						strcat(values[i_trade_history_status_id], ",");
					}
					strcat(values[i_trade_history_dts], "{");
					strcat(values[i_trade_history_status_id], "{");
					/*
					 * FIXME: Can't have varying size multi-dimensial array.
					 * Since the spec says no more than three items here,
					 * pad the array up to 3 all the time.
					 */
					for (j = 0; j < SPI_processed; j++) {
						if (j > 0) {
							strcat(values[i_trade_history_dts], ",");
							strcat(values[i_trade_history_status_id], ",");
						}
						strcat(values[i_trade_history_dts], "\"");
						strcat(values[i_trade_history_dts],
								SPI_getvalue(tuple, tupdesc, 1));
						strcat(values[i_trade_history_dts], "\"");
						strcat(values[i_trade_history_status_id], "\"");
						strcat(values[i_trade_history_status_id],
								SPI_getvalue(tuple, tupdesc, 2));
						strcat(values[i_trade_history_status_id], "\"");
					}
					for (j = SPI_processed; j < 3; j++) {
						if (j > 0) {
							strcat(values[i_trade_history_dts], ",");
							strcat(values[i_trade_history_status_id], ",");
						}
						strcat(values[i_trade_history_dts], "NULL");
						strcat(values[i_trade_history_status_id], "\"\"");
					}
					strcat(values[i_trade_history_dts], "}");
					strcat(values[i_trade_history_status_id], "}");
					++num_history;
				}
			} else {
				dump_tlf1_inputs(max_trades, trade_id_p);
				FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
			}
		}
		strcat(values[i_bid_price], "}");
		strcat(values[i_cash_transaction_amount], "}");
		strcat(values[i_cash_transaction_dts], "}");
		strcat(values[i_cash_transaction_name], "}");
		strcat(values[i_exec_name], "}");
		strcat(values[i_is_cash], "}");
		strcat(values[i_is_market], "}");
		strcat(values[i_settlement_amount], "}");
		strcat(values[i_settlement_cash_due_date], "}");
		strcat(values[i_settlement_cash_type], "}");
		strcat(values[i_trade_history_dts], "}");
		strcat(values[i_trade_history_status_id], "}");
		strcat(values[i_trade_price], "}");

		sprintf(values[i_num_found], "%d", num_found_count);

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
		for (i = 0; i < 15; i++) {
			elog(NOTICE, "TLF1 OUT: %d %s", i, values[i]);
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

/* Clause 3.3.6.3 */
Datum TradeLookupFrame2(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

	int i;

	char **values = NULL;
	enum tlf2 {
			i_bid_price=0, i_cash_transaction_amount, i_cash_transaction_dts,
			i_cash_transaction_name, i_exec_name, i_is_cash, i_num_found,
			i_settlement_amount, i_settlement_cash_due_date,
			i_settlement_cash_type, i_status, i_trade_history_dts,
			i_trade_history_status_id, i_trade_list, i_trade_price
	};

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		long acct_id = PG_GETARG_INT64(0);
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

		char *is_cash_str;

		if (timestamp2tm(end_trade_dts_ts, NULL, tm, &fsec, NULL, NULL) == 0) {
			EncodeDateTime(tm, fsec, NULL, &tzn, USE_ISO_DATES, end_trade_dts);
		}
		if (timestamp2tm(start_trade_dts_ts, NULL, tm, &fsec, NULL,
				NULL) == 0) {
			EncodeDateTime(tm, fsec, NULL, &tzn, USE_ISO_DATES,
					start_trade_dts);
		}

#ifdef DEBUG
		dump_tlf2_inputs(acct_id, end_trade_dts, max_trades, start_trade_dts);
#endif /* DEBUG */

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 15);
		values[i_bid_price] = (char *) palloc(((S_PRICE_T_LEN + 1) *
				max_trades + 2) * sizeof(char));
		values[i_cash_transaction_amount] = (char *) palloc(((VALUE_T_LEN +
				1) * max_trades + 2) * sizeof(char));
		values[i_cash_transaction_dts] = (char *) palloc(((MAXDATELEN + 1) *
				max_trades + 2) * sizeof(char));
		values[i_cash_transaction_name] = (char *) palloc(((CT_NAME_LEN +
				3) * max_trades + 2) * sizeof(char));
		values[i_exec_name] = (char *) palloc(((T_EXEC_NAME_LEN + 1) *
				max_trades + 3) * sizeof(char));
		values[i_is_cash] = (char *) palloc(((BOOLEAN_LEN + 1) * max_trades +
				2) * sizeof(char));
		values[i_num_found] = (char *) palloc((INTEGER_LEN + 1) * sizeof(char));
		values[i_settlement_amount] = (char *) palloc(((VALUE_T_LEN + 1) *
				max_trades + 2) * sizeof(char));
		values[i_settlement_cash_due_date] = (char *) palloc(((MAXDATELEN +
				1) * max_trades + 2) * sizeof(char));
		values[i_settlement_cash_type] = (char *) palloc(((SE_CASH_TYPE_LEN +
				1) * max_trades + 2) * sizeof(char));
		values[i_status] = (char *) palloc((STATUS_LEN + 1) * sizeof(char));
		values[i_trade_history_dts] = (char *) palloc((((MAXDATELEN + 1) *
				max_trades + 3) * 3 + 2) * sizeof(char));
		values[i_trade_history_status_id] = (char *) palloc((((ST_ID_LEN +
				3) * max_trades + 3) * 3 + 2) * sizeof(char));
		values[i_trade_list] = (char *) palloc(((TRADE_T_LEN + 1) *
				max_trades + 2) * sizeof(char));
		values[i_trade_price] = (char *) palloc(((S_PRICE_T_LEN + 1) *
				max_trades + 2) * sizeof(char));

		strcpy(values[i_status], "0");

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;

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
			sprintf(values[i_num_found], "%d", num_found_count);
		} else {
			dump_tlf2_inputs(acct_id, end_trade_dts, max_trades,
					start_trade_dts);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
		}

#ifdef DEBUG
		elog(NOTICE, "num_found = %d", num_found_count);
#endif /* DEBUG */
		strcpy(values[i_bid_price], "{");
		strcpy(values[i_cash_transaction_amount], "{");
		strcpy(values[i_cash_transaction_dts], "{");
		strcpy(values[i_cash_transaction_name], "{");
		strcpy(values[i_exec_name], "{");
		strcpy(values[i_is_cash], "{");
		strcpy(values[i_settlement_amount], "{");
		strcpy(values[i_settlement_cash_due_date], "{");
		strcpy(values[i_settlement_cash_type], "{");
		strcpy(values[i_trade_history_dts], "{");
		strcpy(values[i_trade_history_status_id], "{");
		strcpy(values[i_trade_list], "{");
		strcpy(values[i_trade_price], "{");
		for (i = 0; i < num_found_count; i++) {
			TupleDesc tupdesc2;
			SPITupleTable *tuptable2 = NULL;
			HeapTuple tuple2 = NULL;

			char *trade_list_str;

			tuple = tuptable->vals[i];
			if (i > 0) {
				strcat(values[i_bid_price], ",");
				strcat(values[i_exec_name], ",");
				strcat(values[i_is_cash], ",");
				strcat(values[i_trade_list], ",");
				strcat(values[i_trade_price], ",");
			}
			strcat(values[i_bid_price], SPI_getvalue(tuple, tupdesc, 1));
			strcat(values[i_exec_name], "\"");
			strcat(values[i_exec_name], SPI_getvalue(tuple, tupdesc, 2));
			strcat(values[i_exec_name], "\"");
			is_cash_str = SPI_getvalue(tuple, tupdesc, 3);
			strcat(values[i_is_cash], (is_cash_str[0] == 't' ? "0" : "1"));
			trade_list_str = SPI_getvalue(tuple, tupdesc, 4);
			strcat(values[i_trade_list], trade_list_str);
			strcat(values[i_trade_price], SPI_getvalue(tuple, tupdesc, 5));

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
					dump_tlf2_inputs(acct_id, end_trade_dts, max_trades,
							start_trade_dts);
					FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
					continue;
				}
			} else {
				dump_tlf2_inputs(acct_id, end_trade_dts, max_trades,
						start_trade_dts);
				FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
				continue;
			}

			if (num_settlements > 1) {
				strcat(values[i_settlement_amount], ",");
				strcat(values[i_settlement_cash_due_date], ",");
				strcat(values[i_settlement_cash_type], ",");
			}
			strcat(values[i_settlement_amount],
					SPI_getvalue(tuple2, tupdesc2, 1));
			strcat(values[i_settlement_cash_due_date],
					SPI_getvalue(tuple2, tupdesc2, 2));
			strcat(values[i_settlement_cash_type],
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

					if (num_cash_txn > 1) {
						strcat(values[i_cash_transaction_amount], ",");
						strcat(values[i_cash_transaction_dts], ",");
						strcat(values[i_cash_transaction_name], ",");
					}
					strcat(values[i_cash_transaction_amount],
							SPI_getvalue(tuple2, tupdesc2, 1));
					strcat(values[i_cash_transaction_dts],
							SPI_getvalue(tuple2, tupdesc2, 2));
					strcat(values[i_cash_transaction_name], "\"");
					strcat(values[i_cash_transaction_name],
							SPI_getvalue(tuple2, tupdesc2, 3));
					strcat(values[i_cash_transaction_name], "\"");
				}
			} else {
				dump_tlf2_inputs(acct_id, end_trade_dts, max_trades,
						start_trade_dts);
				FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
				continue;
			}

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
					dump_tlf2_inputs(acct_id, end_trade_dts, max_trades,
							start_trade_dts);
					FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
					continue;
				}
			} else {
				dump_tlf2_inputs(acct_id, end_trade_dts, max_trades,
						start_trade_dts);
				FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
				continue;
			}

			if (num_trade_history > 1) {
				strcat(values[i_trade_history_dts], ",");
				strcat(values[i_trade_history_status_id], ",");
			}
			strcat(values[i_trade_history_dts],
					SPI_getvalue(tuple2, tupdesc2, 1));
			strcat(values[i_trade_history_status_id], "\"");
			strcat(values[i_trade_history_status_id],
					SPI_getvalue(tuple2, tupdesc2, 2));
			strcat(values[i_trade_history_status_id], "\"");
		}
		strcat(values[i_bid_price], "}");
		strcat(values[i_cash_transaction_amount], "}");
		strcat(values[i_cash_transaction_dts], "}");
		strcat(values[i_cash_transaction_name], "}");
		strcat(values[i_exec_name], "}");
		strcat(values[i_is_cash], "}");
		strcat(values[i_settlement_amount], "}");
		strcat(values[i_settlement_cash_due_date], "}");
		strcat(values[i_settlement_cash_type], "}");
		strcat(values[i_trade_history_dts], "}");
		strcat(values[i_trade_history_status_id], "}");
		strcat(values[i_trade_list], "}");
		strcat(values[i_trade_price], "}");

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
		for (i = 0; i < 15; i++) {
			elog(NOTICE, "TLF2 OUT: %d %s", i, values[i]);
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

Datum TradeLookupFrame3(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

	int i;

	char **values = NULL;
	enum tlf3 {
			i_acct_id=0, i_cash_transaction_amount, i_cash_transaction_dts,
			i_cash_transaction_name, i_exec_name, i_is_cash, i_num_found,
			i_price, i_quantity, i_settlement_amount,
			i_settlement_cash_due_date, i_settlement_cash_type, i_status,
			i_trade_dts, i_trade_history_dts, i_trade_history_status_id,
			i_trade_list, i_trade_type
	};

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		Timestamp end_trade_dts_ts = PG_GETARG_TIMESTAMP(0);
		/* max_acct_id used only for engineering purposes... */
		long max_acct_id = PG_GETARG_INT64(1);
		int max_trades = PG_GETARG_INT32(2);
		Timestamp start_trade_dts_ts = PG_GETARG_TIMESTAMP(3);
		char *symbol_p = (char *) PG_GETARG_TEXT_P(4);

		char symbol[S_SYMB_LEN + 1];

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

		char *is_cash_str;

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
		 dump_tlf3_inputs(end_trade_dts, max_acct_id, max_trades,
				start_trade_dts, symbol);
#endif /* DEBUG */

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 18);
		values[i_acct_id] = (char *) palloc(((IDENT_T_LEN + 1) * max_trades +
				2) * sizeof(char));
		values[i_cash_transaction_amount] = (char *) palloc(((VALUE_T_LEN +
				1) * max_trades + 2) * sizeof(char));
		values[i_cash_transaction_dts] = (char *) palloc(((MAXDATELEN + 1) *
				max_trades + 2) * sizeof(char));
		values[i_cash_transaction_name] = (char *) palloc(((CT_NAME_LEN + 3) *
				max_trades + 2) * sizeof(char));
		values[i_exec_name] = (char *) palloc(((T_EXEC_NAME_LEN + 3) *
				max_trades + 2) * sizeof(char));
		values[i_is_cash] = (char *) palloc(((BOOLEAN_LEN + 1) * max_trades +
					2) * sizeof(char));
		values[i_num_found] = (char *) palloc((INTEGER_LEN + 1) * sizeof(char));
		values[i_price] = (char *) palloc(((S_PRICE_T_LEN + 1) * max_trades +
				2) * sizeof(char));
		values[i_quantity] = (char *) palloc(((INTEGER_LEN + 1) * max_trades +
				2) * sizeof(char));
		values[i_settlement_amount] = (char *) palloc(((VALUE_T_LEN + 1) *
				max_trades + 2) * sizeof(char));
		values[i_settlement_cash_due_date] = (char *) palloc(((MAXDATELEN +
				1) * max_trades + 2) * sizeof(char));
		values[i_settlement_cash_type] = (char *) palloc(((SE_CASH_TYPE_LEN +
				3) * max_trades + 2) * sizeof(char));
		values[i_status] = (char *) palloc((STATUS_LEN + 1) * sizeof(char));
		values[i_trade_dts] = (char *) palloc(((MAXDATELEN * 3 + 4) *
				max_trades + max_trades + 2) * sizeof(char));
		values[i_trade_history_dts] = (char *) palloc(((MAXDATELEN * 3 + 4) *
				max_trades + max_trades + 2) * sizeof(char));
		values[i_trade_history_status_id] = (char *) palloc(((ST_ID_LEN + 3) *
				max_trades * 3 + 2) * sizeof(char));
		values[i_trade_list] = (char *) palloc(((TRADE_T_LEN + 1) *
				max_trades + 2) * sizeof(char));
		values[i_trade_type] = (char *) palloc(((TT_ID_LEN + 3) * max_trades +
				2) * sizeof(char));

		strcpy(values[i_status], "0");

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;

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
			sprintf(values[i_num_found], "%d", num_found_count);
		} else {
			dump_tlf3_inputs(end_trade_dts, max_acct_id, max_trades,
					start_trade_dts, symbol);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
		}

#ifdef DEBUG
		elog(NOTICE, "num_found = %d", num_found_count);
#endif /* DEBUG */
		strcpy(values[i_acct_id], "{");
		strcpy(values[i_cash_transaction_amount], "{");
		strcpy(values[i_cash_transaction_dts], "{");
		strcpy(values[i_cash_transaction_name], "{");
		strcpy(values[i_exec_name], "{");
		strcpy(values[i_is_cash], "{");
		strcpy(values[i_price], "{");
		strcpy(values[i_quantity], "{");
		strcpy(values[i_settlement_amount], "{");
		strcpy(values[i_settlement_cash_due_date], "{");
		strcpy(values[i_settlement_cash_type], "{");
		strcpy(values[i_trade_dts], "{");
		strcpy(values[i_trade_history_dts], "{");
		strcpy(values[i_trade_history_status_id], "{");
		strcpy(values[i_trade_list], "{");
		strcpy(values[i_trade_type], "{");
		for (i = 0; i < num_found_count; i++) {
			TupleDesc tupdesc2;
			SPITupleTable *tuptable2 = NULL;
			HeapTuple tuple2 = NULL;

			char *trade_list_str;

			tuple = tuptable->vals[i];
			if (i > 0) {
				strcat(values[i_acct_id], ",");
				strcat(values[i_exec_name], ",");
				strcat(values[i_is_cash], ",");
				strcat(values[i_price], ",");
				strcat(values[i_quantity], ",");
				strcat(values[i_trade_dts], ",");
				strcat(values[i_trade_list], ",");
				strcat(values[i_trade_type], ",");
			}
			strcat(values[i_acct_id], SPI_getvalue(tuple, tupdesc, 1));
			strcat(values[i_exec_name], "\"");
			strcat(values[i_exec_name], SPI_getvalue(tuple, tupdesc, 2));
			strcat(values[i_exec_name], "\"");
			is_cash_str = SPI_getvalue(tuple, tupdesc, 3);
			strcat(values[i_is_cash], (is_cash_str[0] == 't' ? "0" : "1"));
			strcat(values[i_price], SPI_getvalue(tuple, tupdesc, 4));
			strcat(values[i_quantity], SPI_getvalue(tuple, tupdesc, 5));
			strcat(values[i_trade_dts], SPI_getvalue(tuple, tupdesc, 6));
			trade_list_str = SPI_getvalue(tuple, tupdesc, 7);
			strcat(values[i_trade_list], trade_list_str);
			strcat(values[i_trade_type], "\"");
			strcat(values[i_trade_type], SPI_getvalue(tuple, tupdesc, 8));
			strcat(values[i_trade_type], "\"");

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

					if (num_settlements > 1) {
						strcat(values[i_settlement_amount], ",");
						strcat(values[i_settlement_cash_due_date], ",");
						strcat(values[i_settlement_cash_type], ",");
					}
					strcat(values[i_settlement_amount],
							SPI_getvalue(tuple2, tupdesc2, 1));
					strcat(values[i_settlement_cash_due_date],
							SPI_getvalue(tuple2, tupdesc2, 2));
					strcat(values[i_settlement_cash_type], "\"");
					strcat(values[i_settlement_cash_type],
							SPI_getvalue(tuple2, tupdesc2, 3));
					strcat(values[i_settlement_cash_type], "\"");
				}
			} else {
				dump_tlf3_inputs(end_trade_dts, max_acct_id, max_trades,
						start_trade_dts, symbol);
				FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
				continue;
			}

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

					if (num_cash_txn > 0) {
						strcat(values[i_cash_transaction_amount], ",");
						strcat(values[i_cash_transaction_dts], ",");
						strcat(values[i_cash_transaction_name], ",");
					}
					strcat(values[i_cash_transaction_amount],
							SPI_getvalue(tuple2, tupdesc2, 1));
					strcat(values[i_cash_transaction_dts],
							SPI_getvalue(tuple2, tupdesc2, 2));
					strcat(values[i_cash_transaction_name], "\"");
					strcat(values[i_cash_transaction_name],
							SPI_getvalue(tuple2, tupdesc2, 3));
					strcat(values[i_cash_transaction_name], "\"");
					++num_cash_txn;
				}
			} else {
				dump_tlf3_inputs(end_trade_dts, max_acct_id, max_trades,
						start_trade_dts, symbol);
				FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
				continue;
			}

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

					if (num_trade_history > 1) {
						strcat(values[i_trade_history_dts], ",");
						strcat(values[i_trade_history_status_id], ",");
					}
					strcat(values[i_trade_history_dts],
							SPI_getvalue(tuple2, tupdesc2, 1));
					strcat(values[i_trade_history_status_id], "\"");
					strcat(values[i_trade_history_status_id],
							SPI_getvalue(tuple2, tupdesc2, 2));
					strcat(values[i_trade_history_status_id], "\"");
				}
			} else {
				dump_tlf3_inputs(end_trade_dts, max_acct_id, max_trades,
						start_trade_dts, symbol);
				FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
				continue;
			}
		}
		strcat(values[i_acct_id], "}");
		strcat(values[i_cash_transaction_amount], "}");
		strcat(values[i_cash_transaction_dts], "}");
		strcat(values[i_cash_transaction_name], "}");
		strcat(values[i_exec_name], "}");
		strcat(values[i_is_cash], "}");
		strcat(values[i_price], "}");
		strcat(values[i_quantity], "}");
		strcat(values[i_settlement_amount], "}");
		strcat(values[i_settlement_cash_due_date], "}");
		strcat(values[i_settlement_cash_type], "}");
		strcat(values[i_trade_dts], "}");
		strcat(values[i_trade_history_dts], "}");
		strcat(values[i_trade_history_status_id], "}");
		strcat(values[i_trade_list], "}");
		strcat(values[i_trade_type], "}");

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
		for (i = 0; i < 18; i++) {
			elog(NOTICE, "TLF3 OUT: %d %s", i, values[i]);
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

/* Clause 3.3.6.6 */
Datum TradeLookupFrame4(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

	int i;

	char **values = NULL;
	enum tlf4 {
			i_holding_history_id=0, i_holding_history_trade_id, i_num_found,
			i_quantity_after, i_quantity_before, i_status, i_trade_id
	};

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		long acct_id = PG_GETARG_INT64(0);
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
		 dump_tlf4_inputs(acct_id, start_trade_dts);
#endif /* DEBUG */

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 7);
		values[i_holding_history_id] =
				(char *) palloc(((TRADE_T_LEN + 1) * 20 + 2) * sizeof(char));
		values[i_holding_history_trade_id] =
				(char *) palloc(((TRADE_T_LEN + 1) * 20 + 2) * sizeof(char));
		values[i_num_found] = (char *) palloc((INTEGER_LEN + 1) * sizeof(char));
		values[i_quantity_after] = (char *) palloc(((S_QTY_T_LEN + 1) * 20 +
				2) * sizeof(char));
		values[i_quantity_before] = (char *) palloc(((S_QTY_T_LEN + 1) * 20 +
				2) * sizeof(char));
		values[i_status] = (char *) palloc((STATUS_LEN + 1) * sizeof(char));

		strcpy(values[i_status], "0");

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;

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
				values[i_trade_id] = SPI_getvalue(tuple, tupdesc, 1);
			}
		} else {
			dump_tlf4_inputs(acct_id, start_trade_dts);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
		}

		sprintf(sql, TLF4_2, values[i_trade_id]);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
		} else {
			dump_tlf4_inputs(acct_id, start_trade_dts);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
		}

		num_found_count = SPI_processed;
		sprintf(values[i_num_found], "%d", num_found_count);
#ifdef DEBUG
		elog(NOTICE, "num_found = %d", num_found_count);
#endif /* DEBUG */

		strcpy(values[i_holding_history_id], "{");
		strcpy(values[i_holding_history_trade_id], "{");
		strcpy(values[i_quantity_after], "{");
		strcpy(values[i_quantity_before], "{");
		for (i = 0; i < num_found_count; i++) {
			tuple = tuptable->vals[i];
			if (i > 0) {
				strcat(values[i_holding_history_id], ",");
				strcat(values[i_holding_history_trade_id], ",");
				strcat(values[i_quantity_after], ",");
				strcat(values[i_quantity_before], ",");
			}
			strcat(values[i_holding_history_id],
					SPI_getvalue(tuple, tupdesc, 1));
			strcat(values[i_holding_history_trade_id],
					SPI_getvalue(tuple, tupdesc, 2));
			strcat(values[i_quantity_after], SPI_getvalue(tuple, tupdesc, 3));
			strcat(values[i_quantity_before], SPI_getvalue(tuple, tupdesc, 4));
		}
		strcat(values[i_holding_history_id], "}");
		strcat(values[i_holding_history_trade_id], "}");
		strcat(values[i_quantity_after], "}");
		strcat(values[i_quantity_before], "}");

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
		for (i = 0; i < 7; i++) {
			elog(NOTICE, "TLF4 OUT: %d %s", i, values[i]);
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
