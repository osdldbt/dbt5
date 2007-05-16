/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2007 Mark Wong
 *
 * Based on TPC-E Standard Specification Revision 1.1.0
 */

#include <sys/types.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h> /* this should include most necessary APIs */
#include <executor/executor.h>  /* for GetAttributeByName() */
#include <funcapi.h> /* for returning set of rows in order_status */
#include <miscadmin.h>
#include <utils/date.h>
#include <utils/datetime.h>

/* FIXME: These values should be inherited from the egen headers. */
#define MAX_COMP_LEN 3
#define MAX_FIN_LEN 20
#define MAX_NEWS_LEN 2

#define SDF1_1 \
		"SELECT s_name,\n" \
		"       co_id,\n" \
		"       co_name,\n" \
		"       co_sp_rate,\n" \
		"       co_ceo,\n" \
		"       co_desc,\n" \
		"       co_open_date,\n" \
		"       co_st_id,\n" \
		"       ca.ad_line1,\n" \
		"       ca.ad_line2,\n" \
		"       zca.zc_town,\n" \
		"       zca.zc_div,\n" \
		"       ca.ad_zc_code,\n" \
		"       ca.ad_ctry,\n" \
		"       s_num_out,\n" \
		"       s_start_date,\n" \
		"       s_exch_date,\n" \
		"       s_pe,\n" \
		"       s_52wk_high,\n" \
		"       s_52wk_high_date,\n" \
		"       s_52wk_low,\n" \
		"       s_52wk_low_date,\n" \
		"       s_dividend,\n" \
		"       s_yield,\n" \
		"       zea.zc_div,\n" \
		"       ea.ad_ctry,\n" \
		"       ea.ad_line1,\n" \
		"       ea.ad_line2,\n" \
		"       zea.zc_town,\n" \
		"       ea.ad_zc_code,\n" \
		"       ex_close,\n" \
		"       ex_desc,\n" \
		"       ex_name,\n" \
		"       ex_num_symb,\n" \
		"       ex_open\n" \
		"FROM   security,\n" \
		"       company,\n" \
		"       address ca,\n" \
		"       address ea,\n" \
		"       zip_code zca,\n" \
		"       zip_code zea,\n" \
		"       exchange\n" \
		"WHERE  s_symb = '%s'\n" \
		"       AND co_id = s_co_id\n" \
		"       AND ca.ad_id = co_ad_id\n" \
		"       AND ea.ad_id = ex_ad_id\n" \
		"       AND ex_id = s_ex_id\n" \
		"       AND ca.ad_zc_code = zca.zc_code\n" \
		"       AND ea.ad_zc_code = zea.zc_code;"

#define SDF1_2 \
		"SELECT co_name,\n" \
		"       in_name\n" \
		"FROM   company_competitor,\n" \
		"       company,\n" \
		"       industry\n" \
		"WHERE  cp_co_id = %s\n" \
		"       AND co_id = cp_comp_co_id\n" \
		"       AND in_id = cp_in_id\n" \
		"LIMIT %d;"

#define SDF1_3 \
		"SELECT   fi_year,\n" \
		"         fi_qtr,\n" \
		"         fi_qtr_start_date,\n" \
		"         fi_revenue,\n" \
		"         fi_net_earn,\n" \
		"         fi_basic_eps,\n" \
		"         fi_dilut_eps,\n" \
		"         fi_margin,\n" \
		"         fi_inventory,\n" \
		"         fi_assets,\n" \
		"         fi_liability,\n" \
		"         fi_out_basic,\n" \
		"         fi_out_dilut\n" \
		"FROM     financial\n" \
		"WHERE    fi_co_id = %s\n" \
		"ORDER BY fi_year ASC,\n" \
		"         fi_qtr\n" \
		"LIMIT %d;"

#define SDF1_4 \
		"SELECT   dm_date,\n" \
		"         dm_close,\n" \
		"         dm_high,\n" \
		"         dm_low,\n" \
		"         dm_vol\n" \
		"FROM     daily_market\n" \
		"WHERE    dm_s_symb = '%s'\n" \
		"         AND dm_date >= '%s'\n" \
		"ORDER BY dm_date ASC\n" \
		"LIMIT %d;"

#define SDF1_5 \
		"SELECT lt_price,\n" \
		"       lt_open_price,\n" \
		"       lt_vol\n" \
		"FROM   last_trade\n" \
		"WHERE  lt_s_symb = '%s';"

#define SDF1_6 \
		"SELECT ni_item,\n" \
		"       ni_dts,\n" \
		"       ni_source,\n" \
		"       ni_author,\n" \
		"      '',\n" \
		"      ''\n" \
		"FROM   news_xref,\n" \
		"       news_item\n" \
		"WHERE  ni_id = nx_ni_id\n" \
		"       AND nx_co_id = %s\n" \
		"LIMIT %d;"

#define SDF1_7 \
		"SELECT '',\n" \
		"       ni_dts,\n" \
		"       ni_source,\n" \
		"       ni_author,\n" \
		"       ni_headline,\n" \
		"       ni_summary\n" \
		"FROM   news_xref,\n" \
		"       news_item\n" \
		"WHERE  ni_id = nx_ni_id\n" \
		"       AND nx_co_id = %s\n" \
		"LIMIT %d;"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* Prototypes to prevent potential gcc warnings. */
Datum SecurityDetailFrame1(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(SecurityDetailFrame1);

/* Clause 3.3.5.3 */
Datum SecurityDetailFrame1(PG_FUNCTION_ARGS)
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

		bool access_log_flag = PG_GETARG_BOOL(0);
		int max_rows_to_return = PG_GETARG_INT32(1);
		DateADT start_date_p = PG_GETARG_DATEADT(2);
		char *symbol_p = (char *) PG_GETARG_TEXT_P(3);
		char symbol[16];
		struct pg_tm tt, *tm = &tt;
		char buf[MAXDATELEN + 1];

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;

		char sql[2048];

		char *co_id = NULL;

		strcpy(symbol, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(symbol_p))));
		j2date(start_date_p + POSTGRES_EPOCH_JDATE,
				&(tm->tm_year), &(tm->tm_mon), &(tm->tm_mday));
		EncodeDateOnly(tm, DateStyle, buf);
#ifdef DEBUG
		elog(NOTICE, "[0] %d", access_log_flag);
		elog(NOTICE, "[1] %d", max_rows_to_return);
		elog(NOTICE, "[2] %s", pstrdup(buf));
		elog(NOTICE, "[3] %s", symbol);
#endif

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 46);
		values[14] = (char *) palloc(sizeof(char *) * (MAX_COMP_LEN * 63 + 2));
		values[15] = (char *) palloc(sizeof(char *) * (MAX_COMP_LEN * 53 + 2));
		/*
		 * FIXME:
		 * I don't understand what to do here with 'day', setting an empty
		 * array.
		 */
		values[16] = (char *) palloc(sizeof(char *) * 3);
		strcpy(values[16], "{}");
		values[17] = (char *) palloc(sizeof(char *) * 11);
		/*
		 * FIXME:
		 * I don't understand what to do here with 'fin', setting an empty
		 * array.
		 */
		values[31] = (char *) palloc(sizeof(char *) * 3);
		strcpy(values[31], "{}");
		values[32] = (char *) palloc(sizeof(char *) * 11);
		/*
		 * FIXME:
		 * I don't understand what to do here with 'news', setting an empty
		 * array.
		 */
		values[36] = (char *) palloc(sizeof(char *) * 3);
		strcpy(values[36], "{}");
		values[37] = (char *) palloc(sizeof(char *) * 11);
		values[44] = (char *) palloc(sizeof(char *) * 2);
		strcpy(values[44], "0");

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();

		sprintf(sql, SDF1_1, symbol);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];

			co_id = SPI_getvalue(tuple, tupdesc, 2);
			values[0] = SPI_getvalue(tuple, tupdesc, 19);
			values[1] = SPI_getvalue(tuple, tupdesc, 20);
			values[2] = SPI_getvalue(tuple, tupdesc, 21);
			values[3] = SPI_getvalue(tuple, tupdesc, 22);
			values[4] = SPI_getvalue(tuple, tupdesc, 5);
			values[5] = SPI_getvalue(tuple, tupdesc, 14);
			values[6] = SPI_getvalue(tuple, tupdesc, 12);
			values[7] = SPI_getvalue(tuple, tupdesc, 9);
			values[8] = SPI_getvalue(tuple, tupdesc, 10);
			values[9] = SPI_getvalue(tuple, tupdesc, 11);
			values[10] = SPI_getvalue(tuple, tupdesc, 13);
			values[11] = SPI_getvalue(tuple, tupdesc, 6);
			values[12] = SPI_getvalue(tuple, tupdesc, 3);
			values[13] = SPI_getvalue(tuple, tupdesc, 8);
			values[18] = SPI_getvalue(tuple, tupdesc, 23);
			values[19] = SPI_getvalue(tuple, tupdesc, 26);
			values[20] = SPI_getvalue(tuple, tupdesc, 25);
			values[21] = SPI_getvalue(tuple, tupdesc, 27);
			values[22] = SPI_getvalue(tuple, tupdesc, 28);
			values[23] = SPI_getvalue(tuple, tupdesc, 29);
			values[24] = SPI_getvalue(tuple, tupdesc, 30);
			values[25] = SPI_getvalue(tuple, tupdesc, 31);
			values[26] = SPI_getvalue(tuple, tupdesc, 17);
			values[27] = SPI_getvalue(tuple, tupdesc, 32);
			values[28] = SPI_getvalue(tuple, tupdesc, 33);
			values[29] = SPI_getvalue(tuple, tupdesc, 34);
			values[30] = SPI_getvalue(tuple, tupdesc, 35);
			values[38] = SPI_getvalue(tuple, tupdesc, 15);
			values[39] = SPI_getvalue(tuple, tupdesc, 7);
			values[40] = SPI_getvalue(tuple, tupdesc, 18);
			values[41] = SPI_getvalue(tuple, tupdesc, 1);
			values[42] = SPI_getvalue(tuple, tupdesc, 4);
			values[43] = SPI_getvalue(tuple, tupdesc, 16);
			values[45] = SPI_getvalue(tuple, tupdesc, 24);
		} else {
			elog(NOTICE, "ERROR: sql not ok = %d", ret);
			strcpy(values[44], "1");
		}


		sprintf(sql, SDF1_2, co_id, MAX_COMP_LEN);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;

			strcpy(values[14], "{");
			strcpy(values[15], "{");

			if (SPI_processed > 0) {
				tuple = tuptable->vals[0];

				strcat(values[14], "\"");
				strcat(values[14], SPI_getvalue(tuple, tupdesc, 1));
				strcat(values[14], "\"");

				strcat(values[15], "\"");
				strcat(values[15], SPI_getvalue(tuple, tupdesc, 2));
				strcat(values[15], "\"");
			}
			for (i = 1; i < SPI_processed; i++) {
				tuple = tuptable->vals[i];

				strcat(values[14], ",\"");
				strcat(values[14], SPI_getvalue(tuple, tupdesc, 1));
				strcat(values[14], "\"");

				strcat(values[15], ",\"");
				strcat(values[15], SPI_getvalue(tuple, tupdesc, 2));
				strcat(values[15], "\"");
			}
			strcat(values[14], "}");
			strcat(values[15], "}");
		} else {
			elog(NOTICE, "ERROR: sql not ok = %d", ret);
			strcpy(values[14], "{}");
			strcpy(values[44], "1");
		}

		sprintf(sql, SDF1_3, co_id, MAX_FIN_LEN);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			sprintf(values[32], "%d", SPI_processed);
		} else {
			elog(NOTICE, "ERROR: sql not ok = %d", ret);
			strcpy(values[32], "0");
			strcpy(values[44], "1");
		}

		sprintf(sql, SDF1_4, symbol, pstrdup(buf), max_rows_to_return);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			sprintf(values[17], "%d", SPI_processed);
		} else {
			elog(NOTICE, "ERROR: sql not ok = %d", ret);
			strcpy(values[17], "0");
			strcpy(values[44], "1");
		}

		sprintf(sql, SDF1_5, symbol);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			values[33] = SPI_getvalue(tuple, tupdesc, 2);
			values[34] = SPI_getvalue(tuple, tupdesc, 1);
			values[35] = SPI_getvalue(tuple, tupdesc, 3);
		} else {
			elog(NOTICE, "ERROR: sql not ok = %d", ret);
			values[33] = NULL;
			values[34] = NULL;
			values[35] = NULL;
			strcpy(values[44], "1");
		}

		if (access_log_flag == true) {
			sprintf(sql, SDF1_6, co_id, MAX_NEWS_LEN);
		} else {
			sprintf(sql, SDF1_7, co_id, MAX_NEWS_LEN);
		}
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			sprintf(values[37], "%d", SPI_processed);
		} else {
			elog(NOTICE, "ERROR: sql not ok = %d", ret);
			strcpy(values[37], "0");
			strcpy(values[44], "1");
		}

		funcctx->max_calls = 1;

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
		for (i = 0; i < 46; i++) {
			elog(NOTICE, "%d %s", i, values[i]);
		}
#endif /* DEBUG */

		/* Build a tuple. */
		tuple = BuildTupleFromCStrings(attinmeta, values);

		/* Make the tuple into a datum. */
		result = HeapTupleGetDatum(tuple);

		/* Clean up. */
		pfree(values[14]);                                                   
		pfree(values[15]);                                                   
		pfree(values[16]);                                                   
		pfree(values[17]);                                                   
		pfree(values[31]);                                                   
		pfree(values[32]);                                                   
		pfree(values[36]);                                                   
		pfree(values[37]);                                                   
		pfree(values[44]);                                                   
		pfree(values);                                                   

		SRF_RETURN_NEXT(funcctx, result);
	} else {
		/* Do when there is no more left. */
		SPI_finish();
		SRF_RETURN_DONE(funcctx);
	}
}
