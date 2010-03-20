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

#include "frame.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define TSF1_1 \
		"SELECT t_id, t_dts, st_name, tt_name, t_s_symb, t_qty, \n" \
		"       t_exec_name, t_chrg, s_name, ex_name\n" \
		"FROM trade, status_type, trade_type, security, exchange\n" \
		"WHERE t_ca_id = %ld\n" \
		"  AND st_id = t_st_id\n" \
		"  AND tt_id = t_tt_id\n" \
		"  AND s_symb = t_s_symb\n" \
		"  AND ex_id = s_ex_id\n" \
		"ORDER BY t_dts DESC\n" \
		"LIMIT 50"

#define TSF1_2 \
		"SELECT c_l_name, c_f_name, b_name\n" \
		"FROM customer_account, customer, broker\n" \
		"WHERE ca_id = %ld\n" \
		"  AND c_id = ca_c_id\n" \
		"  AND b_id = ca_b_id"

/* Prototypes. */
void dump_tsf1_inputs(long);

Datum TradeStatusFrame1(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(TradeStatusFrame1);

void dump_tsf1_inputs(long acct_id)
{
	elog(NOTICE, "TSF1: INPUTS START");
	elog(NOTICE, "TSF1: acct_id %ld", acct_id);
	elog(NOTICE, "TSF1: INPUTS END");
}

/* Clause 3.3.9.3 */
Datum TradeStatusFrame1(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

	int i;

	char **values = NULL;

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		enum tsf1 {
				i_broker_name=0, i_charge, i_cust_f_name, i_cust_l_name,
				i_ex_name, i_exec_name, i_s_name, i_status, i_status_name,
				i_symbol, i_trade_dts, i_trade_id, i_trade_qty, i_type_name
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
		values = (char **) palloc(sizeof(char *) * 14);
		values[i_charge] =
				(char *) palloc((VALUE_T_LEN + 1) * sizeof(char) * 50);
		values[i_ex_name] =
				(char *) palloc((EX_NAME_LEN + 3) * sizeof(char) * 50);
		values[i_exec_name] =
				(char *) palloc((T_EXEC_NAME_LEN + 3) * sizeof(char) * 50);
		values[i_s_name] =
				(char *) palloc((S_NAME_LEN + 3) * sizeof(char) * 50);
		values[i_status] = (char *) palloc((STATUS_LEN + 1) * sizeof(char));
		values[i_status_name] =
				(char *) palloc((ST_NAME_LEN + 3) * sizeof(char) * 50);
		values[i_symbol] =
				(char *) palloc((S_SYMB_LEN + 3) * sizeof(char) * 50);
		values[i_trade_dts] =
				(char *) palloc((MAXDATELEN + 1) * sizeof(char) * 50);
		values[i_trade_id] =
				(char *) palloc((BIGINT_LEN + 1) * sizeof(char) * 50);
		values[i_trade_qty] =
				(char *) palloc((INTEGER_LEN + 1) * sizeof(char) * 50);
		values[i_type_name] =
				(char *) palloc((TT_NAME_LEN + 3) * sizeof(char) * 50);

		values[i_cust_l_name] = NULL;
		values[i_cust_f_name] = NULL;
		values[i_broker_name] = NULL;

#ifdef DEBUG
		dump_tsf1_inputs(acct_id);
#endif

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		strcpy(values[i_status], "0");
		funcctx->max_calls = 1;

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();

		sprintf(sql, TSF1_1, acct_id);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			if (SPI_processed != 50)
				strcpy(values[i_status], "-911");
		} else {
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
			dump_tsf1_inputs(acct_id);
		}

		strcpy(values[i_trade_id], "{");
		strcpy(values[i_trade_dts], "{");
		strcpy(values[i_status_name], "{");
		strcpy(values[i_type_name], "{");
		strcpy(values[i_symbol], "{");
		strcpy(values[i_trade_qty], "{");
		strcpy(values[i_exec_name], "{");
		strcpy(values[i_charge], "{");
		strcpy(values[i_s_name], "{");
		strcpy(values[i_ex_name], "{");
		for (i = 0; i < SPI_processed; i++) {
			tuple = tuptable->vals[i];
			if (i > 0) {
				strcat(values[i_trade_id], ",");
				strcat(values[i_trade_dts], ",");
				strcat(values[i_status_name], ",");
				strcat(values[i_type_name], ",");
				strcat(values[i_symbol], ",");
				strcat(values[i_trade_qty], ",");
				strcat(values[i_exec_name], ",");
				strcat(values[i_charge], ",");
				strcat(values[i_s_name], ",");
				strcat(values[i_ex_name], ",");
			}
			strcat(values[i_trade_id], SPI_getvalue(tuple, tupdesc, 1));
			strcat(values[i_trade_dts], SPI_getvalue(tuple, tupdesc, 2));
			strcat(values[i_status_name], "\"");
			strcat(values[i_status_name], SPI_getvalue(tuple, tupdesc, 3));
			strcat(values[i_status_name], "\"");
			strcat(values[i_type_name], "\"");
			strcat(values[i_type_name], SPI_getvalue(tuple, tupdesc, 4));
			strcat(values[i_type_name], "\"");
			strcat(values[i_symbol], "\"");
			strcat(values[i_symbol], SPI_getvalue(tuple, tupdesc, 5));
			strcat(values[i_symbol], "\"");
			strcat(values[i_trade_qty], SPI_getvalue(tuple, tupdesc, 6));
			strcat(values[i_exec_name], "\"");
			strcat(values[i_exec_name], SPI_getvalue(tuple, tupdesc, 7));
			strcat(values[i_exec_name], "\"");
			strcat(values[i_charge], SPI_getvalue(tuple, tupdesc, 8));
			strcat(values[i_s_name], "\"");
			strcat(values[i_s_name], SPI_getvalue(tuple, tupdesc, 9));
			strcat(values[i_s_name], "\"");
			strcat(values[i_ex_name], "\"");
			strcat(values[i_ex_name], SPI_getvalue(tuple, tupdesc, 10));
			strcat(values[i_ex_name], "\"");
		}
		strcat(values[i_trade_id], "}");
		strcat(values[i_trade_dts], "}");
		strcat(values[i_status_name], "}");
		strcat(values[i_type_name], "}");
		strcat(values[i_symbol], "}");
		strcat(values[i_trade_qty], "}");
		strcat(values[i_exec_name], "}");
		strcat(values[i_charge], "}");
		strcat(values[i_s_name], "}");
		strcat(values[i_ex_name], "}");

		sprintf(sql, TSF1_2, acct_id);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			if (SPI_processed > 0) {
				tuple = tuptable->vals[0];
				values[i_cust_l_name] = SPI_getvalue(tuple, tupdesc, 1);
				values[i_cust_f_name] = SPI_getvalue(tuple, tupdesc, 2);
				values[i_broker_name] = SPI_getvalue(tuple, tupdesc, 3);
			}
		} else {
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
			dump_tsf1_inputs(acct_id);
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
		for (i = 0; i < 14; i++) {
			elog(NOTICE, "TSF1 OUT: %d %s", i, values[i]);
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
