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
#include <utils/numeric.h>
#include <utils/builtins.h>

#include "frame.h"

#define TOF1_1 \
		"SELECT ca_name, ca_b_id, ca_c_id, ca_tax_st\n" \
		"FROM customer_account\n" \
		"WHERE ca_id = %ld"

#define TOF1_2 \
		"SELECT c_f_name, c_l_name, c_tier, c_tax_id\n" \
		"FROM customer\n" \
		"WHERE c_id = %s"

#define TOF1_3 \
		"SELECT b_name\n" \
		"FROM Broker\n" \
		"WHERE b_id = %s"

#define TOF2_1 \
		"SELECT ap_acl\n" \
		"FROM account_permission\n" \
		"WHERE ap_ca_id = %ld\n" \
		"  AND ap_f_name = '%s'\n" \
		"  AND ap_l_name = '%s'\n" \
		"  AND ap_tax_id = '%s'"

#define TOF3_1a \
		"SELECT co_id\n" \
		"FROM company\n" \
		"WHERE co_name = e'%s'"

#define TOF3_2a \
		"SELECT s_ex_id, s_name, s_symb\n" \
		"FROM security\n" \
		"WHERE s_co_id = %s\n" \
		"  AND s_issue = '%s'"

#define TOF3_1b \
		"SELECT s_co_id, s_ex_id, s_name\n" \
		"FROM security\n" \
		"WHERE s_symb = '%s'\n"

#define TOF3_2b \
		"SELECT co_name\n" \
		"FROM company\n" \
		"WHERE co_id = %s"

#define TOF3_3 \
		"SELECT lt_price\n" \
		"FROM last_trade\n" \
		"WHERE lt_s_symb = '%s'"

#define TOF3_4 \
		"SELECT tt_is_mrkt, tt_is_sell\n" \
		"FROM trade_type\n" \
		"WHERE tt_id = '%s'"

#define TOF3_5 \
		"SELECT hs_qty\n" \
		"FROM holding_summary\n" \
		"WHERE hs_ca_id = %ld\n" \
		"  AND hs_s_symb = '%s'"

#define TOF3_6a \
		"SELECT h_qty, h_price\n" \
		"FROM holding\n" \
		"WHERE h_ca_id = %ld\n" \
		"  AND h_s_symb = '%s'\n" \
		"ORDER BY h_dts DESC"

#define TOF3_6b \
		"SELECT h_qty, h_price\n" \
		"FROM holding\n" \
		"WHERE h_ca_id = %ld\n" \
		"  AND h_s_symb = '%s'\n" \
		"ORDER BY h_dts ASC"

#define TOF3_7 \
		"SELECT sum(tx_rate)\n" \
		"FROM taxrate\n" \
		"WHERE tx_id in (\n" \
		"                SELECT cx_tx_id\n" \
		"                FROM customer_taxrate\n" \
		"                WHERE cx_c_id = %ld)\n"

#define TOF3_8 \
		"SELECT cr_rate\n" \
		"FROM commission_rate\n" \
		"WHERE cr_c_tier = %d\n" \
		"  AND cr_tt_id = '%s'\n" \
		"  AND cr_ex_id = '%s' \n" \
		"  AND cr_from_qty <= %d\n" \
		"  AND cr_to_qty >= %d"

#define TOF3_9 \
		"SELECT ch_chrg\n" \
		"FROM charge\n" \
		"WHERE ch_c_tier = %d\n" \
		"  AND ch_tt_id = '%s'\n"

#define TOF3_10 \
		"SELECT ca_bal\n" \
		"FROM customer_account\n" \
		"WHERE ca_id = %ld"

#define TOF3_11 \
		"SELECT sum(hs_qty * lt_price)\n" \
		"FROM holding_summary, last_trade\n" \
		"WHERE hs_ca_id = %ld\n" \
		"  AND lt_s_symb = hs_s_symb"

#define TOF4_1 \
		"INSERT INTO trade(t_id, t_dts, t_st_id, t_tt_id, t_is_cash,\n" \
		"                  t_s_symb, t_qty, t_bid_price, t_ca_id,\n" \
		"                  t_exec_name, t_trade_price, t_chrg, t_comm, \n" \
		"                  t_tax, t_lifo)\n" \
		"VALUES (NEXTVAL('seq_trade_id'), now(), '%s', '%s', %s, '%s',\n" \
		"        %d, %8.2f, %ld, '%s', NULL, %10.2f, %10.2f, 0, %s)\n" \
		"RETURNING t_id, t_dts"

#define TOF4_2 \
		"INSERT INTO trade_request(tr_t_id, tr_tt_id, tr_s_symb, tr_qty,\n" \
		"                          tr_bid_price, tr_b_id)\n" \
		"VALUES (%s, '%s', '%s', %d, %8.2f, %ld)"

#define TOF4_3 \
		"INSERT INTO trade_history(th_t_id, th_dts, th_st_id)\n" \
		"VALUES(%s, now(), '%s')"


#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* Prototypes. */
void dump_tof1_inputs(long);
void dump_tof2_inputs(long, char *, char *, char *);
void dump_tof3_inputs(long, long, int, int, char *, char *, char *, int, int,
		char *, int, char *, double, char *);
void dump_tof4_inputs(long, long, double, char *, int, int, double, char *,
		char *, int, char *, int);

Datum TradeOrderFrame1(PG_FUNCTION_ARGS);
Datum TradeOrderFrame2(PG_FUNCTION_ARGS);
Datum TradeOrderFrame3(PG_FUNCTION_ARGS);
Datum TradeOrderFrame4(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(TradeOrderFrame1);
PG_FUNCTION_INFO_V1(TradeOrderFrame2);
PG_FUNCTION_INFO_V1(TradeOrderFrame3);
PG_FUNCTION_INFO_V1(TradeOrderFrame4);

void dump_tof1_inputs(long acct_id)
{
	elog(NOTICE, "TOF1: INPUTS START");
	elog(NOTICE, "TOF1: acct_id %ld", acct_id);
	elog(NOTICE, "TOF1: INPUTS END");
}

void dump_tof2_inputs(long acct_id, char *exec_f_name, char *exec_l_name,
		char *exec_tax_id)
{
	elog(NOTICE, "TOF2: INPUTS START");
	elog(NOTICE, "TOF2: acct_id %ld", acct_id);
	elog(NOTICE, "TOF2: exec_f_name %s", exec_f_name);
	elog(NOTICE, "TOF2: exec_l_name %s", exec_l_name);
	elog(NOTICE, "TOF2: exec_tax_id %s", exec_tax_id);
	elog(NOTICE, "TOF2: INPUTS END");
}

void dump_tof3_inputs(long acct_id, long cust_id, int cust_tier, int is_lifo,
		char *issue, char *st_pending_id, char *st_submitted_id,
		int tax_status, int trade_qty, char *trade_type_id, int type_is_margin,
		char *co_name, double requested_price, char *symbol)
{
	elog(NOTICE, "TOF3: INPUTS START");
	elog(NOTICE, "TOF3: acct_id %ld", acct_id);
	elog(NOTICE, "TOF3: cust_id %ld", cust_id);
	elog(NOTICE, "TOF3: cust_tier %d", cust_tier);
	elog(NOTICE, "TOF3: is_lifo %d", is_lifo);
	elog(NOTICE, "TOF3: issue %s", issue);
	elog(NOTICE, "TOF3: st_pending_id %s", st_pending_id);
	elog(NOTICE, "TOF3: st_submitted_id %s", st_submitted_id);
	elog(NOTICE, "TOF3: tax_status %d", tax_status);
	elog(NOTICE, "TOF3: trade_qty %d", trade_qty);
	elog(NOTICE, "TOF3: trade_type_id %s", trade_type_id);
	elog(NOTICE, "TOF3: type_is_margin %d", type_is_margin);
	elog(NOTICE, "TOF3: co_name %s", co_name);
	elog(NOTICE, "TOF3: requested_price %8.2f", requested_price);
	elog(NOTICE, "TOF3: symbol %s", symbol);
	elog(NOTICE, "TOF3: INPUTS END");
}

void dump_tof4_inputs(long acct_id, long broker_id, double charge_amount,
		char *exec_name, int is_cash, int is_lifo, double requested_price,
		char *status_id, char *symbol, int trade_qty, char *trade_type_id,
		int type_is_market) {
	elog(NOTICE, "TOF4: INPUTS START");
	elog(NOTICE, "TOF4: acct_id %ld", acct_id);
	elog(NOTICE, "TOF4: broker_id %ld", broker_id);
	elog(NOTICE, "TOF4: charge_amount %8.2f", charge_amount);
	elog(NOTICE, "TOF4: exec_name %s", exec_name);
	elog(NOTICE, "TOF4: is_cash %d", is_cash);
	elog(NOTICE, "TOF4: is_lifo %d", is_lifo);
	elog(NOTICE, "TOF4: requested_price %8.2f", requested_price);
	elog(NOTICE, "TOF4: status_id %s", status_id);
	elog(NOTICE, "TOF4: symbol %s", symbol);
	elog(NOTICE, "TOF4: trade_qty %d", trade_qty);
	elog(NOTICE, "TOF4: trade_type_id %s", trade_type_id);
	elog(NOTICE, "TOF4: type_is_market %d", type_is_market);
	elog(NOTICE, "TOF4: INPUTS END");
}

/* Clause 3.3.7.3 */
Datum TradeOrderFrame1(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

#ifdef DEBUG
	int i;
#endif /* DEBUG */

	char **values = NULL;

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		enum tof1 {
				i_acct_name=0, i_broker_id, i_broker_name, i_cust_f_name,
				i_cust_id, i_cust_l_name, i_cust_tier, i_status, i_tax_id,
				i_tax_status
		};

		long acct_id = PG_GETARG_INT64(0);

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
		values = (char **) palloc(sizeof(char *) * 10);
		values[i_status] = (char *) palloc((STATUS_LEN + 1) * sizeof(char));

#ifdef DEBUG
		dump_tof1_inputs(acct_id);
#endif

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		strcpy(values[i_status], "0");
		funcctx->max_calls = 1;

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();

		sprintf(sql, TOF1_1, acct_id);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			values[i_acct_name] = SPI_getvalue(tuple, tupdesc, 1);
			values[i_broker_id] = SPI_getvalue(tuple, tupdesc, 2);
			values[i_cust_id] = SPI_getvalue(tuple, tupdesc, 3);
			values[i_tax_status] = SPI_getvalue(tuple, tupdesc, 4);
		} else {
			dump_tof1_inputs(acct_id);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);

			SPI_finish();
			SRF_RETURN_DONE(funcctx);
		}

		sprintf(sql, TOF1_2, values[i_cust_id]);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			values[i_cust_f_name] = SPI_getvalue(tuple, tupdesc, 1);
			values[i_cust_l_name] = SPI_getvalue(tuple, tupdesc, 2);
			values[i_cust_tier] = SPI_getvalue(tuple, tupdesc, 3);
			values[i_tax_id] = SPI_getvalue(tuple, tupdesc, 4);
		} else {
			dump_tof1_inputs(acct_id);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);

			SPI_finish();
			SRF_RETURN_DONE(funcctx);
		}

		sprintf(sql, TOF1_3, values[i_broker_id]);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			values[i_broker_name] = SPI_getvalue(tuple, tupdesc, 1);
		} else {
			dump_tof1_inputs(acct_id);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
		}

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
		for (i = 0; i < 10; i++) {
			elog(NOTICE, "TOF1 OUT: %d %s", i, values[i]);
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

/* Clause 3.3.7.4 */
Datum TradeOrderFrame2(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

#ifdef DEBUG
	int i;
#endif /* DEBUG */

	char **values = NULL;

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		enum tof2 { i_ap_acl=0, i_status };

		long acct_id = PG_GETARG_INT64(0);
		char *exec_f_name_p = (char *) PG_GETARG_TEXT_P(1);
		char *exec_l_name_p = (char *) PG_GETARG_TEXT_P(2);
		char *exec_tax_id_p = (char *) PG_GETARG_TEXT_P(3);

		char exec_f_name[AP_F_NAME_LEN + 1];
		char exec_l_name[AP_L_NAME_LEN + 1];
		char exec_tax_id[AP_TAX_ID_LEN + 1];

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
		values = (char **) palloc(sizeof(char *) * 2);
		values[i_ap_acl] = NULL;
		values[i_status] = (char *) palloc((STATUS_LEN + 1) * sizeof(char));

		strcpy(exec_f_name, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(exec_f_name_p))));
		strcpy(exec_l_name, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(exec_l_name_p))));
		strcpy(exec_tax_id, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(exec_tax_id_p))));
#ifdef DEBUG
		dump_tof2_inputs(acct_id, exec_f_name, exec_l_name, exec_tax_id);
#endif

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		strcpy(values[i_status], "0");
		funcctx->max_calls = 1;

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();

		sprintf(sql, TOF2_1, acct_id, exec_f_name, exec_l_name, exec_tax_id);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			if (SPI_processed > 0) {
				tuple = tuptable->vals[0];
				values[i_ap_acl] = SPI_getvalue(tuple, tupdesc, 1);
			}
		} else {
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
			dump_tof2_inputs(acct_id, exec_f_name, exec_l_name, exec_tax_id);
		}

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
			elog(NOTICE, "TOF2 OUT: %d %s", i, values[i]);
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

/* Clause 3.3.7.5 */
Datum TradeOrderFrame3(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

	int i;

	enum tof3 {
			i_co_name=0, i_requested_price, i_symbol, i_buy_value,
			i_charge_amount, i_comm_rate, i_cust_assets, i_market_price,
			i_s_name, i_sell_value, i_status, i_status_id, i_tax_amount,
			i_type_is_market, i_type_is_sell
	};

	char **values = NULL;

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		long acct_id = PG_GETARG_INT64(0);
		long cust_id = PG_GETARG_INT64(1);
		int cust_tier = PG_GETARG_INT16(2);
		int is_lifo = PG_GETARG_INT16(3);
		char *issue_p = (char *) PG_GETARG_TEXT_P(4);
		char *st_pending_id_p = (char *) PG_GETARG_TEXT_P(5);
		char *st_submitted_id_p = (char *) PG_GETARG_TEXT_P(6);
		int tax_status = PG_GETARG_INT16(7);
		int trade_qty = PG_GETARG_INT32(8);
		char *trade_type_id_p = (char *) PG_GETARG_TEXT_P(9);
		int type_is_margin = PG_GETARG_INT16(10);
		char *co_name_p = (char *) PG_GETARG_TEXT_P(11);
		Numeric requested_price_num = PG_GETARG_NUMERIC(12);
		char *symbol_p = (char *) PG_GETARG_TEXT_P(13);

		char co_name[CO_NAME_LEN + 1];
		char issue[7];
		char st_pending_id[10];
		char st_submitted_id[10];
		char trade_type_id[TT_ID_LEN + 1];
		char symbol[S_SYMB_LEN + 1];
		double requested_price;
		int hs_qty = 0;
		int needed_qty;
		double buy_value = 0;
		double sell_value = 0;
		char *exch_id = NULL;

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;

		char sql[2048];
		char *co_id = NULL;
		double tax_amount = 0;

		char co_name_esc[CO_NAME_LEN * 2 + 1];

		strcpy(co_name, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(co_name_p))));
		strcpy(issue, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(issue_p))));
		strcpy(st_pending_id, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(st_pending_id_p))));
		strcpy(st_submitted_id, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(st_submitted_id_p))));
		strcpy(trade_type_id, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(trade_type_id_p))));
		strcpy(symbol, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(symbol_p))));

		requested_price = DatumGetFloat8(DirectFunctionCall1(
				numeric_float8_no_overflow,
				PointerGetDatum(requested_price_num)));

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 15);
		values[i_requested_price] = (char *) palloc((S_PRICE_T_LEN + 1) *
				sizeof(char));
		values[i_buy_value] = (char *) palloc((S_PRICE_T_LEN + 1) *
				sizeof(char));
		values[i_cust_assets] = (char *) palloc((S_PRICE_T_LEN + 1) *
				sizeof(char));
		values[i_sell_value] = (char *) palloc((S_PRICE_T_LEN + 1) *
				sizeof(char));
		values[i_status] = (char *) palloc((STATUS_LEN + 1) * sizeof(char));
		values[i_tax_amount] = (char *) palloc((S_PRICE_T_LEN + 1) *
				sizeof(char));

		values[i_symbol] = NULL;

#ifdef DEBUG
		dump_tof3_inputs(acct_id, cust_id, cust_tier, is_lifo, issue,
				st_pending_id, st_submitted_id, tax_status, trade_qty,
				trade_type_id, type_is_margin, co_name, requested_price,
				symbol);
#endif

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;
		strcpy(values[i_status], "0");
		sprintf(values[i_requested_price], "%8.2f", requested_price);

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();

		if (strlen(symbol) == 0) {
			escape_me(co_name, co_name_esc);
			sprintf(sql, TOF3_1a, co_name_esc);
			values[i_co_name] = co_name;
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret == SPI_OK_SELECT && SPI_processed > 0) {
				tupdesc = SPI_tuptable->tupdesc;
				tuptable = SPI_tuptable;
				tuple = tuptable->vals[0];
				co_id = SPI_getvalue(tuple, tupdesc, 1);
			} else {
				dump_tof3_inputs(acct_id, cust_id, cust_tier, is_lifo, issue,
						st_pending_id, st_submitted_id, tax_status, trade_qty,
						trade_type_id, type_is_margin, co_name, requested_price,
						symbol);
				FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
			}

			sprintf(sql, TOF3_2a, co_id, issue);
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret == SPI_OK_SELECT && SPI_processed > 0) {
				tupdesc = SPI_tuptable->tupdesc;
				tuptable = SPI_tuptable;
				tuple = tuptable->vals[0];
				exch_id = SPI_getvalue(tuple, tupdesc, 1);
				values[i_s_name] = SPI_getvalue(tuple, tupdesc, 2);
				values[i_symbol] = SPI_getvalue(tuple, tupdesc, 3);
			} else {
				dump_tof3_inputs(acct_id, cust_id, cust_tier, is_lifo, issue,
						st_pending_id, st_submitted_id, tax_status, trade_qty,
						trade_type_id, type_is_margin, co_name, requested_price,
						symbol);
				FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
			}
		} else {
			sprintf(sql, TOF3_1b, symbol);
			values[i_symbol] = symbol;
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret == SPI_OK_SELECT && SPI_processed > 0) {
				tupdesc = SPI_tuptable->tupdesc;
				tuptable = SPI_tuptable;
				tuple = tuptable->vals[0];
				co_id = SPI_getvalue(tuple, tupdesc, 1);
				exch_id = SPI_getvalue(tuple, tupdesc, 2);
				values[i_s_name] = SPI_getvalue(tuple, tupdesc, 3);
			} else {
				dump_tof3_inputs(acct_id, cust_id, cust_tier, is_lifo, issue,
						st_pending_id, st_submitted_id, tax_status, trade_qty,
						trade_type_id, type_is_margin, co_name, requested_price,
						symbol);
				FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
			}

			sprintf(sql, TOF3_2b, co_id);
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret == SPI_OK_SELECT && SPI_processed > 0) {
				tupdesc = SPI_tuptable->tupdesc;
				tuptable = SPI_tuptable;
				tuple = tuptable->vals[0];
				values[i_co_name] = SPI_getvalue(tuple, tupdesc, 1);
			} else {
				dump_tof3_inputs(acct_id, cust_id, cust_tier, is_lifo, issue,
						st_pending_id, st_submitted_id, tax_status, trade_qty,
						trade_type_id, type_is_margin, co_name, requested_price,
						symbol);
				FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
			}
		}

		sprintf(sql, TOF3_3, values[i_symbol]);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			values[i_market_price] = SPI_getvalue(tuple, tupdesc, 1);
		} else {
			dump_tof3_inputs(acct_id, cust_id, cust_tier, is_lifo, issue,
					st_pending_id, st_submitted_id, tax_status, trade_qty,
					trade_type_id, type_is_margin, co_name, requested_price,
					symbol);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
		}

		sprintf(sql, TOF3_4, trade_type_id);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			values[i_type_is_market] = SPI_getvalue(tuple, tupdesc, 1);
			values[i_type_is_sell] = SPI_getvalue(tuple, tupdesc, 2);
		} else {
			dump_tof3_inputs(acct_id, cust_id, cust_tier, is_lifo, issue,
					st_pending_id, st_submitted_id, tax_status, trade_qty,
					trade_type_id, type_is_margin, co_name, requested_price,
					symbol);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
		}

		if (values[i_type_is_market][0] == '1') {
			values[i_requested_price] = values[i_market_price];
		}

		needed_qty = trade_qty;

		sprintf(sql, TOF3_5, acct_id, values[i_symbol]);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			hs_qty = atoi(SPI_getvalue(tuple, tupdesc, 1));
		} else {
			hs_qty = 0;
		}

		if (values[i_type_is_sell][0] == '1') {
			int rows = 0;

			if (hs_qty > 0) {
				sprintf(sql, TOF3_6a, acct_id, values[i_symbol]);
			} else {
				sprintf(sql, TOF3_6b, acct_id, values[i_symbol]);
			}
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret == SPI_OK_SELECT) {
				tupdesc = SPI_tuptable->tupdesc;
				tuptable = SPI_tuptable;
			} else {
				dump_tof3_inputs(acct_id, cust_id, cust_tier, is_lifo, issue,
						st_pending_id, st_submitted_id, tax_status, trade_qty,
						trade_type_id, type_is_margin, co_name, requested_price,
						symbol);
				FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
			}

			i = 0;
			while (needed_qty > 0 && i < rows) {
				int hold_qty;
				double hold_price;

				tuple = tuptable->vals[i];
				hold_qty = atoi(SPI_getvalue(tuple, tupdesc, 1));
				hold_price = atof(SPI_getvalue(tuple, tupdesc, 2));
				if (hold_qty > needed_qty) {
					buy_value += needed_qty * hold_price;
					sell_value += needed_qty * requested_price;
				} else {
					buy_value += hold_qty * hold_price;
					sell_value += hold_qty * requested_price;
					needed_qty -= hold_qty;
				}
				i++;
			}
		} else {
			int rows = 0;

			if (hs_qty < 0) {
				if (is_lifo == 1) {
					sprintf(sql, TOF3_6a, acct_id, values[i_symbol]);
				} else {
					sprintf(sql, TOF3_6b, acct_id, values[i_symbol]);
				}
#ifdef DEBUG
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				ret = SPI_exec(sql, 0);
				if (ret == SPI_OK_SELECT) {
					tupdesc = SPI_tuptable->tupdesc;
					tuptable = SPI_tuptable;
					rows = SPI_processed;
				} else {
					dump_tof3_inputs(acct_id, cust_id, cust_tier, is_lifo,
							issue, st_pending_id, st_submitted_id, tax_status,
							trade_qty, trade_type_id, type_is_margin, co_name,
							requested_price, symbol);
					FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
				}
			}

			i = 0;
			while (needed_qty > 0 && i < rows) {
				int hold_qty;
				double hold_price;

				tuple = tuptable->vals[i];
				hold_qty = atoi(SPI_getvalue(tuple, tupdesc, 1));
				hold_price = atof(SPI_getvalue(tuple, tupdesc, 2));
				if (hold_qty + needed_qty < 0) {
					sell_value += needed_qty * requested_price;
					buy_value += needed_qty * hold_price;
				} else {
					hold_qty *= -1;
					sell_value += hold_qty * hold_price;
					buy_value += hold_qty * requested_price;
					needed_qty -= hold_qty;
				}
				i++;
			}
		}

		sprintf(values[i_buy_value], "%8.2f", buy_value);
		sprintf(values[i_sell_value], "%8.2f", sell_value);

		if (sell_value > buy_value && (tax_status == 1 || tax_status == 2)) {
			sprintf(sql, TOF3_7, cust_id);
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret == SPI_OK_SELECT && SPI_processed > 0) {
				tupdesc = SPI_tuptable->tupdesc;
				tuptable = SPI_tuptable;
				tuple = tuptable->vals[0];
				tax_amount = (sell_value - buy_value) *
						atof(SPI_getvalue(tuple, tupdesc, 1));
			} else {
				dump_tof3_inputs(acct_id, cust_id, cust_tier, is_lifo, issue,
						st_pending_id, st_submitted_id, tax_status, trade_qty,
						trade_type_id, type_is_margin, co_name, requested_price,
						symbol);
				FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
			}
		}
		sprintf(values[i_tax_amount], "%8.2f", tax_amount);

		sprintf(sql, TOF3_8, cust_tier, trade_type_id, exch_id, trade_qty,
				trade_qty);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			values[i_comm_rate] = SPI_getvalue(tuple, tupdesc, 1);
		} else {
			dump_tof3_inputs(acct_id, cust_id, cust_tier, is_lifo, issue,
					st_pending_id, st_submitted_id, tax_status, trade_qty,
					trade_type_id, type_is_margin, co_name, requested_price,
					symbol);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
		}

		sprintf(sql, TOF3_9, cust_tier, trade_type_id);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			values[i_charge_amount] = SPI_getvalue(tuple, tupdesc, 1);
		} else {
			dump_tof3_inputs(acct_id, cust_id, cust_tier, is_lifo, issue,
					st_pending_id, st_submitted_id, tax_status, trade_qty,
					trade_type_id, type_is_margin, co_name, requested_price,
					symbol);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
		}

		strcpy(values[i_cust_assets], "0.00");
		if (type_is_margin == 1) {
			double acct_bal = 0;

			sprintf(sql, TOF3_10, acct_id);
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret == SPI_OK_SELECT && SPI_processed > 0) {
				tupdesc = SPI_tuptable->tupdesc;
				tuptable = SPI_tuptable;
				tuple = tuptable->vals[0];
				acct_bal = atof(SPI_getvalue(tuple, tupdesc, 1));
			} else {
				dump_tof3_inputs(acct_id, cust_id, cust_tier, is_lifo, issue,
						st_pending_id, st_submitted_id, tax_status, trade_qty,
						trade_type_id, type_is_margin, co_name, requested_price,
						symbol);
				FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
			}

			sprintf(sql, TOF3_11, acct_id);
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret == SPI_OK_SELECT) {
				tupdesc = SPI_tuptable->tupdesc;
				tuptable = SPI_tuptable;
				if (SPI_processed > 0) {
					tuple = tuptable->vals[0];
					sprintf(values[i_cust_assets], "%8.2f",
							atof(SPI_getvalue(tuple, tupdesc, 1)) * acct_bal);
				} else {
					sprintf(values[i_cust_assets], "%8.2f", acct_bal);
				}
			} else {
				dump_tof3_inputs(acct_id, cust_id, cust_tier, is_lifo, issue,
						st_pending_id, st_submitted_id, tax_status, trade_qty,
						trade_type_id, type_is_margin, co_name, requested_price,
						symbol);
				FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
			}
		}

		if (values[i_type_is_market][0] == '1') {
			values[i_status_id] = st_submitted_id;
		} else {
			values[i_status_id] = st_pending_id;
		}

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
			elog(NOTICE, "TOF3 OUT: %d %s", i, values[i]);
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

/* Clause 3.3.7.6 */
Datum TradeOrderFrame4(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

#ifdef DEBUG
	int i;
#endif /* DEBUG */

	enum tof4 { i_status=0, i_trade_id };

	char **values = NULL;

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		long acct_id = PG_GETARG_INT64(0);
		long broker_id = PG_GETARG_INT64(1);
		Numeric charge_amount_num = PG_GETARG_NUMERIC(3);
		Numeric comm_amount_num = PG_GETARG_NUMERIC(3);
		char *exec_name_p = (char *) PG_GETARG_TEXT_P(4);
		int is_cash = PG_GETARG_INT16(5);
		int is_lifo = PG_GETARG_INT16(6);
		Numeric requested_price_num = PG_GETARG_NUMERIC(7);
		char *status_id_p = (char *) PG_GETARG_TEXT_P(8);
		char *symbol_p = (char *) PG_GETARG_TEXT_P(9);
		int trade_qty = PG_GETARG_INT32(10);
		char *trade_type_id_p = (char *) PG_GETARG_TEXT_P(11);
		int type_is_market = PG_GETARG_INT16(12);

		char exec_name[T_EXEC_NAME_LEN + 1];
		char status_id[ST_ID_LEN + 1];
		char symbol[S_SYMB_LEN + 1];
		char trade_type_id[TT_ID_LEN + 1];
		double charge_amount = 0;
		double comm_amount;
		double requested_price;

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;

		char sql[2048];

		strcpy(exec_name, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(exec_name_p))));
		strcpy(status_id, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(status_id_p))));
		strcpy(symbol, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(symbol_p))));
		strcpy(trade_type_id, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(trade_type_id_p))));

		charge_amount = DatumGetFloat8(DirectFunctionCall1(
				numeric_float8_no_overflow,
				PointerGetDatum(charge_amount_num)));
		comm_amount = DatumGetFloat8(DirectFunctionCall1(
				numeric_float8_no_overflow,
				PointerGetDatum(comm_amount_num)));
		requested_price = DatumGetFloat8(DirectFunctionCall1(
				numeric_float8_no_overflow,
				PointerGetDatum(requested_price_num)));

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 2);
		values[i_status] = (char *) palloc((STATUS_LEN + 1) * sizeof(char));

		strcpy(values[i_status], "0");
		values[i_trade_id] = NULL;
#ifdef DEBUG
		dump_tof4_inputs(acct_id, broker_id, charge_amount, exec_name,
				is_cash, is_lifo, requested_price, status_id, symbol,
				trade_qty, trade_type_id, type_is_market);
#endif

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		funcctx->max_calls = 1;
		SPI_connect();

		sprintf(sql, TOF4_1, status_id, trade_type_id,
				(is_cash == 1 ? "true": "false"), symbol, trade_qty,
				requested_price, acct_id, exec_name, charge_amount,
				comm_amount, (is_lifo  == 1 ? "true": "false"));
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_INSERT_RETURNING && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			values[i_trade_id] = SPI_getvalue(tuple, tupdesc, 1);
		} else {
			dump_tof4_inputs(acct_id, broker_id, charge_amount, exec_name,
					is_cash, is_lifo, requested_price, status_id, symbol,
					trade_qty, trade_type_id, type_is_market);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
		}

		if (type_is_market == 0) {
			sprintf(sql, TOF4_2, values[i_trade_id], trade_type_id, symbol,
					trade_qty, requested_price, broker_id);
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret != SPI_OK_INSERT) {
				dump_tof4_inputs(acct_id, broker_id, charge_amount, exec_name,
						is_cash, is_lifo, requested_price, status_id, symbol,
						trade_qty, trade_type_id, type_is_market);
				FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
			}
		}

		sprintf(sql, TOF4_3, values[i_trade_id], status_id);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret != SPI_OK_INSERT) {
			dump_tof4_inputs(acct_id, broker_id, charge_amount, exec_name,
					is_cash, is_lifo, requested_price, status_id, symbol,
					trade_qty, trade_type_id, type_is_market);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
		}

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
			elog(NOTICE, "TOF4 OUT:%d %s", i, values[i]);
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
