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
#include <inttypes.h>
#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h> /* this should include most necessary APIs */
#include <executor/executor.h>  /* for GetAttributeByName() */
#include <funcapi.h> /* for returning set of rows in order_status */
#include <utils/datetime.h>
#include <utils/builtins.h>

#include "frame.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define CPF1_1 \
		"SELECT c_id\n" \
		"FROM   customer\n" \
		"WHERE  c_tax_id = '%s'"

#define CPF1_2 \
		"SELECT c_st_id,\n" \
		"       c_l_name,\n" \
		"       c_f_name,\n" \
		"       c_m_name,\n" \
		"       c_gndr,\n" \
		"       c_tier,\n" \
		"       c_dob,\n" \
		"       c_ad_id,\n" \
		"       c_ctry_1,\n" \
		"       c_area_1,\n" \
		"       c_local_1,\n" \
		"       c_ext_1,\n" \
		"       c_ctry_2,\n" \
		"       c_area_2,\n" \
		"       c_local_2,\n" \
		"       c_ext_2,\n" \
		"       c_ctry_3,\n" \
		"       c_area_3,\n" \
		"       c_local_3,\n" \
		"       c_ext_3,\n" \
		"       c_email_1,\n" \
		"       c_email_2\n" \
		"FROM   customer\n" \
		"WHERE  c_id = %" PRId64

#define CPF1_3 \
		"SELECT   ca_id,\n" \
		"         ca_bal,\n" \
		"         COALESCE(SUM(hs_qty * lt_price), 0) AS soma\n" \
		"FROM     customer_account\n" \
		"         LEFT OUTER JOIN holding_summary\n" \
		"                      ON hs_ca_id = ca_id,\n" \
		"         last_trade\n" \
		"WHERE    ca_c_id = %" PRId64 "\n" \
		"         AND lt_s_symb = hs_s_symb\n" \
		"GROUP BY ca_id, ca_bal\n" \
		"ORDER BY 3 ASC\n" \
		"LIMIT 10"

#define CPF2_1 \
		"SELECT   t_id,\n" \
		"         t_s_symb,\n" \
		"         t_qty,\n" \
		"         st_name,\n" \
		"         th_dts\n" \
		"FROM     (SELECT   t_id AS id\n" \
		"          FROM     trade\n" \
		"          WHERE    t_ca_id = %" PRId64 "\n" \
		"          ORDER BY t_dts DESC\n" \
		"          LIMIT 10) AS t,\n" \
		"         trade,\n" \
		"         trade_history,\n" \
		"         status_type\n" \
		"WHERE    t_id = id\n" \
		"         AND th_t_id = t_id\n" \
		"         AND st_id = th_st_id\n" \
		"ORDER BY th_dts DESC\n" \
		"LIMIT 30"

/* Prototypes to prevent potential gcc warnings. */
Datum CustomerPositionFrame1(PG_FUNCTION_ARGS);
Datum CustomerPositionFrame2(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(CustomerPositionFrame1);
PG_FUNCTION_INFO_V1(CustomerPositionFrame2);

void dump_cpf1_inputs(long, text *);
void dump_cpf2_inputs(long);

void dump_cpf1_inputs(long cust_id, text *tax_id_p) {
	elog(NOTICE, "CPF1: INPUTS START");
	elog(NOTICE, "CPF1: cust_id %ld", cust_id);
	elog(NOTICE, "CPF1: tax_id %s", DatumGetCString(DirectFunctionCall1(textout,
			PointerGetDatum(tax_id_p))));
	elog(NOTICE, "CPF1: INPUTS END"); 
}

void dump_cpf2_inputs(long acct_id) {
	elog(NOTICE, "CPF2: INPUTS START");
	elog(NOTICE, "CPF2: acct_id %ld", acct_id);
	elog(NOTICE, "CPF2: INPUTS END"); 
}

/* Clause 3.3.2.3 */
Datum CustomerPositionFrame1(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	int i;
	char **values = NULL;

	int64 cust_id;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		text *tax_id_p;

		enum cpf1 {
				i_cust_id=0, i_acct_id, i_acct_len, i_asset_total, i_c_ad_id,
				i_c_area_1, i_c_area_2, i_c_area_3, i_c_ctry_1, i_c_ctry_2,
				i_c_ctry_3, i_c_dob, i_c_email_1, i_c_email_2, i_c_ext_1,
				i_c_ext_2, i_c_ext_3, i_c_f_name, i_c_gndr, i_c_l_name,
				i_c_local_1, i_c_local_2, i_c_local_3, i_c_m_name, i_c_st_id,
				i_c_tier, i_cash_bal, i_status
		};

		int ret;
		char sql[1024];

		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple;

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 28);
		values[i_cust_id] = (char *) palloc((IDENT_T_LEN + 1) * sizeof(char));
		values[i_acct_len] = (char *) palloc((INTEGER_LEN + 1) * sizeof(char));
		values[i_status] = (char *) palloc((STATUS_LEN + 1) * sizeof(char));

		/* Create a function context for cross-call persistence. */
		funcctx = SRF_FIRSTCALL_INIT();
		strcpy(values[i_status], "0");

		/* Switch to memory context appropriate for multiple function calls. */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		cust_id = PG_GETARG_INT64(0);
		tax_id_p= PG_GETARG_TEXT_P(1);
#ifdef DEBUG
		dump_cpf1_inputs(cust_id, tax_id_p);
#endif /* DEBUG */

		SPI_connect();
		if (cust_id == 0) {
			sprintf(sql, CPF1_1, DatumGetCString(DirectFunctionCall1(textout,
					PointerGetDatum(tax_id_p))));
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret == SPI_OK_SELECT && SPI_processed > 0) {
				tupdesc = SPI_tuptable->tupdesc;
				tuptable = SPI_tuptable;
				tuple = tuptable->vals[0];
				cust_id = atoll(SPI_getvalue(tuple, tupdesc, 1));
#ifdef DEBUG
				elog(NOTICE, "Got cust_id ok: %ld", cust_id);
#endif /* DEBUG */
			} else {
				dump_cpf1_inputs(cust_id, tax_id_p);
				FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
			}
		}

		sprintf(sql, CPF1_2, cust_id);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
#ifdef DEBUG
		elog(NOTICE, "%d row(s) returned from CPF1_2.", SPI_processed);
#endif /* DEBUG */
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];

			values[i_c_st_id] = SPI_getvalue(tuple, tupdesc, 1);
			values[i_c_l_name] = SPI_getvalue(tuple, tupdesc, 2);
			values[i_c_f_name] = SPI_getvalue(tuple, tupdesc, 3);
			values[i_c_m_name] = SPI_getvalue(tuple, tupdesc, 4);
			values[i_c_gndr] = SPI_getvalue(tuple, tupdesc, 5);
			values[i_c_tier] = SPI_getvalue(tuple, tupdesc, 6);
			values[i_c_dob] = SPI_getvalue(tuple, tupdesc, 7);
			values[i_c_ad_id] = SPI_getvalue(tuple, tupdesc, 8);
			values[i_c_ctry_1] = SPI_getvalue(tuple, tupdesc, 9);
			values[i_c_area_1] = SPI_getvalue(tuple, tupdesc, 10);
			values[i_c_local_1] = SPI_getvalue(tuple, tupdesc, 11);
			values[i_c_ext_1] = SPI_getvalue(tuple, tupdesc, 12);
			values[i_c_ctry_2] = SPI_getvalue(tuple, tupdesc, 13);
			values[i_c_area_2] = SPI_getvalue(tuple, tupdesc, 14);
			values[i_c_local_2] = SPI_getvalue(tuple, tupdesc, 15);
			values[i_c_ext_2] = SPI_getvalue(tuple, tupdesc, 16);
			values[i_c_ctry_3] = SPI_getvalue(tuple, tupdesc, 17);
			values[i_c_area_3] = SPI_getvalue(tuple, tupdesc, 18);
			values[i_c_local_3] = SPI_getvalue(tuple, tupdesc, 19);
			values[i_c_ext_3] = SPI_getvalue(tuple, tupdesc, 20);
			values[i_c_email_1] = SPI_getvalue(tuple, tupdesc, 21);
			values[i_c_email_2] = SPI_getvalue(tuple, tupdesc, 22);
		}

		sprintf(sql, CPF1_3, cust_id);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		sprintf(values[i_acct_len], "%d", SPI_processed);
#ifdef DEBUG
		elog(NOTICE, "%d row(s) returned from CPF1_3.", SPI_processed);
#endif /* DEBUG */
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			/* Total number of tuples to be returned. */
			funcctx->max_calls = 1;

			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];

			values[i_acct_id] = (char *) palloc(((BIGINT_LEN + 1) *
					(SPI_processed + 1) + 2) * sizeof(char));
			values[i_cash_bal] = (char *) palloc(((BALANCE_T_LEN + 1) *
					(SPI_processed + 1) +2) * sizeof(char));
			values[i_asset_total] = (char *) palloc(((S_PRICE_T_LEN + 1) *
					(SPI_processed + 1) + 2) * sizeof(char));

			strcpy(values[i_acct_id], "{");
			strcpy(values[i_cash_bal], "{");
			strcpy(values[i_asset_total], "{");

			if (SPI_processed > 0) {
				strcat(values[i_acct_id], SPI_getvalue(tuple, tupdesc, 1));
				strcat(values[i_cash_bal], SPI_getvalue(tuple, tupdesc, 2));
				strcat(values[i_asset_total], SPI_getvalue(tuple, tupdesc, 3));
			}
			for (i = 1; i < SPI_processed; i++) {
				char *sum;

				tuple = tuptable->vals[i];
				strcat(values[i_acct_id], ",");
				strcat(values[i_acct_id], SPI_getvalue(tuple, tupdesc, 1));

				strcat(values[i_cash_bal], ",");
				strcat(values[i_cash_bal], SPI_getvalue(tuple, tupdesc, 2));

				strcat(values[i_asset_total], ",");
				sum = SPI_getvalue(tuple, tupdesc, 3);
				if (sum != NULL) {
					strcat(values[i_asset_total], sum);
				} else {
					strcat(values[i_asset_total], "0.00");
				}
			}
			strcat(values[i_acct_id], "}");
			strcat(values[i_cash_bal], "}");
			strcat(values[i_asset_total], "}");
		} else {
			dump_cpf1_inputs(cust_id, tax_id_p);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
		}
		snprintf(values[i_cust_id], 12, "%" PRId64, cust_id);

		/* Build a tuple descriptor for our result type. */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) !=
				TYPEFUNC_COMPOSITE) {
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("function returning record called in context "
							"that cannot accept type record")));
		}

		/*
		 * Generate attribute metadata needed later to produce tuples from raw
		 * C strings.
		 */
		funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);

		/* save SPI data for use across calls */
		funcctx->user_fctx = tuptable;

		MemoryContextSwitchTo(oldcontext);
	}

	/* Stuff done on every call of the function. */
	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls) {
		Datum result;
		HeapTuple tuple;

#ifdef DEBUG
		for (i = 0; i < 28; i++) {
			elog(NOTICE, "CPF1 OUT: %d '%s'", i, values[i]);
		}
#endif /* DEBUG */

		/* Build a tuple. */
		tuple = BuildTupleFromCStrings(funcctx->attinmeta, values);

		/* Make the tuple into a datum. */
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	} else {
		SPI_finish();
		SRF_RETURN_DONE(funcctx);
	}
}

/* Clause 3.3.2.4 */
Datum CustomerPositionFrame2(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	char **values = NULL;
	int i;

	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple;

		int64 acct_id = PG_GETARG_INT64(0);

		enum cpf2 {
				i_hist_dts=0, i_hist_len, i_qty, i_status, i_symbol,
				i_trade_id, i_trade_status
		};

		int ret;
		char sql[1024];

#ifdef DEBUG
		dump_cpf2_inputs(acct_id);
#endif /* DEBUG */

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 7);
		values[i_hist_len] = (char *) palloc((INTEGER_LEN + 1) * sizeof(char));
		values[i_status] = (char *) palloc((STATUS_LEN + 1) * sizeof(char));

		strcpy(values[i_status], "0");

		/* Create a function context for cross-call persistence. */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;

		/* Switch to memory context appropriate for multiple function calls. */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();

		sprintf(sql, CPF2_1, acct_id);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		sprintf(values[i_hist_len], "%d", SPI_processed);
#ifdef DEBUG
		elog(NOTICE, "%d row(s) returned.", SPI_processed);
#endif /* DEBUG */
		/* Should return 1 to rows. */
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			/* Total number of tuples to be returned. */
			funcctx->max_calls = 1;

			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];

			values[i_hist_dts] = (char *) palloc(((MAXDATELEN + 1) *
					SPI_processed + 3) * sizeof(char));
			values[i_qty] = (char *) palloc(((INTEGER_LEN + 1) *
					SPI_processed + 3) * sizeof(char));
			values[i_symbol] = (char *) palloc(((S_SYMB_LEN + 3) *
					SPI_processed + 3) * sizeof(char));
			values[i_trade_id] = (char *) palloc(((IDENT_T_LEN + 1) *
					SPI_processed + 3) * sizeof(char));
			values[i_trade_status] = (char *) palloc(((ST_NAME_LEN + 3) *
					SPI_processed + 3) * sizeof(char));

			strcpy(values[i_hist_dts], "{");
			strcpy(values[i_qty], "{");
			strcpy(values[i_symbol], "{");
			strcpy(values[i_trade_id], "{");
			strcpy(values[i_trade_status], "{");

			strcat(values[i_hist_dts], SPI_getvalue(tuple, tupdesc, 5));
			strcat(values[i_qty], SPI_getvalue(tuple, tupdesc, 3));
			strcat(values[i_symbol], "\"");
			strcat(values[i_symbol], SPI_getvalue(tuple, tupdesc, 2));
			strcat(values[i_symbol], "\"");
			strcat(values[i_trade_id], SPI_getvalue(tuple, tupdesc, 1));
			strcat(values[i_trade_status], "\"");
			strcat(values[i_trade_status], SPI_getvalue(tuple, tupdesc, 4));
			strcat(values[i_trade_status], "\"");

			for (i = 1; i < SPI_processed; i++) {
				tuple = tuptable->vals[i];

				strcat(values[i_hist_dts], ",");
				strcat(values[i_hist_dts], SPI_getvalue(tuple, tupdesc, 5));

				strcat(values[i_qty], ",");
				strcat(values[i_qty], SPI_getvalue(tuple, tupdesc, 3));

				strcat(values[i_symbol], ",");
				strcat(values[i_symbol], "\"");
				strcat(values[i_symbol], SPI_getvalue(tuple, tupdesc, 2));
				strcat(values[i_symbol], "\"");

				strcat(values[i_trade_id], ",");
				strcat(values[i_trade_id], SPI_getvalue(tuple, tupdesc, 1));

				strcat(values[i_trade_status], ",");
				strcat(values[i_trade_status], "\"");
				strcat(values[i_trade_status], SPI_getvalue(tuple, tupdesc, 4));
				strcat(values[i_trade_status], "\"");
			}
			strcat(values[i_hist_dts], "}");
			strcat(values[i_qty], "}");
			strcat(values[i_symbol], "}");
			strcat(values[i_trade_id], "}");
			strcat(values[i_trade_status], "}");
		} else {
			if (ret == SPI_OK_SELECT && SPI_processed == 0) {
				elog(WARNING, "Query CPF2_1 should return 10-30 rows.");
			}
			dump_cpf2_inputs(acct_id);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);

			/*
			 * FIXME: Probably don't need to do this if we're not going to
			 * return any rows, but if we do then we don't need to figure
			 * out what pieces of memory we need to free later in the function.
			 */
			values[i_hist_dts] = (char *) palloc(3 * sizeof(char));
			values[i_qty] = (char *) palloc(3 * sizeof(char));
			values[i_symbol] = (char *) palloc(3 * sizeof(char));
			values[i_trade_id] = (char *) palloc(3 * sizeof(char));
			values[i_trade_status] = (char *) palloc(3 * sizeof(char));
			strcpy(values[i_hist_dts], "{}");
			strcpy(values[i_qty], "{}");
			strcpy(values[i_symbol], "{}");
			strcpy(values[i_trade_id], "{}");
			strcpy(values[i_trade_status], "{}");
		}

		/* Build a tuple descriptor for our result type. */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) !=
				TYPEFUNC_COMPOSITE) {
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("function returning record called in context "
							"that cannot accept type record")));
		}

		/*
		 * Generate attribute metadata needed later to produce tuples from raw
		 * C strings.
		 */
		funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);

		/* save SPI data for use across calls */
		funcctx->user_fctx = tuptable;

		MemoryContextSwitchTo(oldcontext);
	}

	/* Stuff done on every call of the function. */
	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls) {
		Datum result;
		HeapTuple tuple;

#ifdef DEBUG
		for (i = 0; i < 7; i++) {
			elog(NOTICE, "CPF2 OUT: %d '%s'", i, values[i]);
		}
#endif /* DEBUG */

		/* Build a tuple. */
		tuple = BuildTupleFromCStrings(funcctx->attinmeta, values);

		/* Make the tuple into a datum. */
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	} else {
		SPI_finish();
		SRF_RETURN_DONE(funcctx);
	}
}
