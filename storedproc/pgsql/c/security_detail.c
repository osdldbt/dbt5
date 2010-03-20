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
#include <miscadmin.h>
#include <utils/date.h>
#include <utils/builtins.h>
#include <utils/datetime.h>

#include "frame.h"

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
		"       AND ea.ad_zc_code = zea.zc_code"

#define SDF1_2 \
		"SELECT co_name,\n" \
		"       in_name\n" \
		"FROM   company_competitor,\n" \
		"       company,\n" \
		"       industry\n" \
		"WHERE  cp_co_id = %s\n" \
		"       AND co_id = cp_comp_co_id\n" \
		"       AND in_id = cp_in_id\n" \
		"LIMIT %d"

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
		"LIMIT %d"

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
		"LIMIT %d"

#define SDF1_5 \
		"SELECT lt_price,\n" \
		"       lt_open_price,\n" \
		"       lt_vol\n" \
		"FROM   last_trade\n" \
		"WHERE  lt_s_symb = '%s'"

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
		"LIMIT %d"

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
		"LIMIT %d"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* Prototypes to prevent potential gcc warnings. */
Datum SecurityDetailFrame1(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(SecurityDetailFrame1);

void dump_sdf1_inputs(bool, int, char *, char *);

void dump_sdf1_inputs(bool access_lob_flag, int max_rows_to_return,
		char *start_date, char *symbol) {
	elog(NOTICE, "SDF1: INPUTS START");
	elog(NOTICE, "SDF1: access_lob_flag %d", access_lob_flag);
	elog(NOTICE, "SDF1: max_rows_to_return %d", max_rows_to_return);
	elog(NOTICE, "SDF1: start_date %s", pstrdup(start_date));
	elog(NOTICE, "SDF1: symbol '%s'", symbol);
	elog(NOTICE, "SDF1: INPUTS END");
}

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

		bool access_lob_flag = PG_GETARG_BOOL(0);
		int max_rows_to_return = PG_GETARG_INT32(1);
		DateADT start_date_p = PG_GETARG_DATEADT(2);
		char *symbol_p = (char *) PG_GETARG_TEXT_P(3);
		char symbol[S_SYMB_LEN + 1];
		struct pg_tm tt, *tm = &tt;
		char buf[MAXDATELEN + 1];

		enum sdf1 {
			i_x52_wk_high=0, i_x52_wk_high_date, i_x52_wk_low,
			i_x52_wk_low_date, i_ceo_name, i_co_ad_ctry, i_co_ad_div,
			i_co_ad_line1, i_co_ad_line2, i_co_ad_town, i_co_ad_zip,
			i_co_desc, i_co_name, i_co_st_id, i_cp_co_name, i_cp_in_name,
			i_day, i_day_len, i_divid, i_ex_ad_ctry, i_ex_ad_div,
			i_ex_ad_line1, i_ex_ad_line2, i_ex_ad_town, i_ex_ad_zip,
			i_ex_close, i_ex_date, i_ex_desc, i_ex_name, i_ex_num_symb,
			i_ex_open, i_fin, i_fin_len, i_last_open, i_last_price,
			i_last_vol, i_news, i_news_len, i_num_out, i_open_date,
			i_pe_ratio, i_s_name, i_sp_rate, i_start_date, i_status, i_yield
		};

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;

		char sql[2048];

		char *co_id = NULL;

		strncpy(symbol, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(symbol_p))), S_SYMB_LEN);
		j2date(start_date_p + POSTGRES_EPOCH_JDATE,
				&(tm->tm_year), &(tm->tm_mon), &(tm->tm_mday));
		EncodeDateOnly(tm, DateStyle, buf);
#ifdef DEBUG
		dump_sdf1_inputs(access_lob_flag, max_rows_to_return, buf, symbol);
#endif

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 46);
		values[i_cp_co_name] = (char *) palloc(sizeof(char) *
				(MAX_COMP_LEN * (CO_NAME_LEN + 3) + 3));
		values[i_cp_in_name] = (char *) palloc(sizeof(char) *
				(MAX_COMP_LEN * (IN_NAME_LEN + 3) + 3));
		values[i_day] = (char *) palloc(sizeof(char) * ((MAXDATELEN +
				DM_CLOSE_LEN + DM_HIGH_LEN + DM_LOW_LEN + DM_VOL_LEN + 7) *
				max_rows_to_return + 3));
		values[i_day_len] = (char *) palloc(sizeof(char) * (INTEGER_LEN + 1));
		values[i_fin] = (char *) palloc(((FI_YEAR_LEN + FI_QTR_LEN +
				MAXDATELEN + FI_REVENUE_LEN + FI_NET_EARN_LEN +
				FI_BASIC_EPS_LEN + FI_MARGIN_LEN + FI_INVENTORY_LEN +
				FI_ASSETS_LEN + FI_LIABILITY_LEN + FI_OUT_BASIC_LEN +
				FI_OUT_DILUT_LEN + 14) * MAX_FIN_LEN + 3) * sizeof(char));
		values[i_fin_len] = (char *) palloc(sizeof(char) * (INTEGER_LEN + 1));
		values[i_news] = (char *) palloc(sizeof(char) * ((NI_ITEM_LEN +
				MAXDATELEN + NI_SOURCE_LEN + NI_AUTHOR_LEN + NI_SUMMARY_LEN +
				NI_HEADLINE_LEN + 19) * MAX_NEWS_LEN + 3));
		values[i_news_len] = (char *) palloc(sizeof(char) * (INTEGER_LEN + 1));
		values[i_status] = (char *) palloc(sizeof(char) * (STATUS_LEN + 1));
		strcpy(values[i_status], "0");

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;

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

			values[i_s_name] = SPI_getvalue(tuple, tupdesc, 1);
			co_id = SPI_getvalue(tuple, tupdesc, 2);
			values[i_co_name] = SPI_getvalue(tuple, tupdesc, 3);
			values[i_sp_rate] = SPI_getvalue(tuple, tupdesc, 4);
			values[i_ceo_name] = SPI_getvalue(tuple, tupdesc, 5);
			values[i_co_desc] = SPI_getvalue(tuple, tupdesc, 6);
			values[i_open_date] = SPI_getvalue(tuple, tupdesc, 7);
			values[i_co_st_id] = SPI_getvalue(tuple, tupdesc, 8);
			values[i_co_ad_line1] = SPI_getvalue(tuple, tupdesc, 9);
			values[i_co_ad_line2] = SPI_getvalue(tuple, tupdesc, 10);
			values[i_co_ad_town] = SPI_getvalue(tuple, tupdesc, 11);
			values[i_co_ad_div] = SPI_getvalue(tuple, tupdesc, 12);
			values[i_co_ad_zip] = SPI_getvalue(tuple, tupdesc, 13);
			values[i_co_ad_ctry] = SPI_getvalue(tuple, tupdesc, 14);
			values[i_num_out] = SPI_getvalue(tuple, tupdesc, 15);
			values[i_start_date] = SPI_getvalue(tuple, tupdesc, 16);
			values[i_ex_date] = SPI_getvalue(tuple, tupdesc, 17);
			values[i_pe_ratio] = SPI_getvalue(tuple, tupdesc, 18);
			values[i_x52_wk_high] = SPI_getvalue(tuple, tupdesc, 19);
			values[i_x52_wk_high_date] = SPI_getvalue(tuple, tupdesc, 20);
			values[i_x52_wk_low] = SPI_getvalue(tuple, tupdesc, 21);
			values[i_x52_wk_low_date] = SPI_getvalue(tuple, tupdesc, 22);
			values[i_divid] = SPI_getvalue(tuple, tupdesc, 23);
			values[i_yield] = SPI_getvalue(tuple, tupdesc, 24);
			values[i_ex_ad_div] = SPI_getvalue(tuple, tupdesc, 25);
			values[i_ex_ad_ctry] = SPI_getvalue(tuple, tupdesc, 26);
			values[i_ex_ad_line1] = SPI_getvalue(tuple, tupdesc, 27);
			values[i_ex_ad_line2] = SPI_getvalue(tuple, tupdesc, 28);
			values[i_ex_ad_town] = SPI_getvalue(tuple, tupdesc, 29);
			values[i_ex_ad_zip] = SPI_getvalue(tuple, tupdesc, 30);
			values[i_ex_close] = SPI_getvalue(tuple, tupdesc, 31);
			values[i_ex_desc] = SPI_getvalue(tuple, tupdesc, 32);
			values[i_ex_name] = SPI_getvalue(tuple, tupdesc, 33);
			values[i_ex_num_symb] = SPI_getvalue(tuple, tupdesc, 34);
			values[i_ex_open] = SPI_getvalue(tuple, tupdesc, 35);
		} else {
			dump_sdf1_inputs(access_lob_flag, max_rows_to_return, buf, symbol);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
		}

		sprintf(sql, SDF1_2, co_id, MAX_COMP_LEN);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;

			strcpy(values[i_cp_co_name], "{");
			strcpy(values[i_cp_in_name], "{");

			if (SPI_processed > 0) {
				tuple = tuptable->vals[0];

				strcat(values[i_cp_co_name], "\"");
				strcat(values[i_cp_co_name], SPI_getvalue(tuple, tupdesc, 1));
				strcat(values[i_cp_co_name], "\"");

				strcat(values[i_cp_in_name], "\"");
				strcat(values[i_cp_in_name], SPI_getvalue(tuple, tupdesc, 2));
				strcat(values[i_cp_in_name], "\"");
			}
			for (i = 1; i < SPI_processed; i++) {
				tuple = tuptable->vals[i];

				strcat(values[i_cp_co_name], ",\"");
				strcat(values[i_cp_co_name], SPI_getvalue(tuple, tupdesc, 1));
				strcat(values[i_cp_co_name], "\"");

				strcat(values[i_cp_in_name], ",\"");
				strcat(values[i_cp_in_name], SPI_getvalue(tuple, tupdesc, 2));
				strcat(values[i_cp_in_name], "\"");
			}
			strcat(values[i_cp_co_name], "}");
			strcat(values[i_cp_in_name], "}");
		} else {
			dump_sdf1_inputs(access_lob_flag, max_rows_to_return, buf, symbol);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
			strcpy(values[i_cp_co_name], "{}");
			strcpy(values[i_cp_in_name], "{}");
		}

		sprintf(sql, SDF1_3, co_id, MAX_FIN_LEN);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
		} else {
			dump_sdf1_inputs(access_lob_flag, max_rows_to_return, buf, symbol);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
		}
		sprintf(values[i_fin_len], "%d", SPI_processed);
		strcpy(values[i_fin], "{");
		for (i = 0; i < SPI_processed; i++) {
			tuple = tuptable->vals[i];
			if (i > 0) {
				strcat(values[i_fin], ",");
			}
			strcat(values[i_fin], "{");
			strcat(values[i_fin], SPI_getvalue(tuple, tupdesc, 1));
			strcat(values[i_fin], ",");
			strcat(values[i_fin], SPI_getvalue(tuple, tupdesc, 2));
			strcat(values[i_fin], ",");
			strcat(values[i_fin], SPI_getvalue(tuple, tupdesc, 3));
			strcat(values[i_fin], ",");
			strcat(values[i_fin], SPI_getvalue(tuple, tupdesc, 4));
			strcat(values[i_fin], ",");
			strcat(values[i_fin], SPI_getvalue(tuple, tupdesc, 5));
			strcat(values[i_fin], ",");
			strcat(values[i_fin], SPI_getvalue(tuple, tupdesc, 6));
			strcat(values[i_fin], ",");
			strcat(values[i_fin], SPI_getvalue(tuple, tupdesc, 7));
			strcat(values[i_fin], ",");
			strcat(values[i_fin], SPI_getvalue(tuple, tupdesc, 8));
			strcat(values[i_fin], ",");
			strcat(values[i_fin], SPI_getvalue(tuple, tupdesc, 9));
			strcat(values[i_fin], ",");
			strcat(values[i_fin], SPI_getvalue(tuple, tupdesc, 10));
			strcat(values[i_fin], ",");
			strcat(values[i_fin], SPI_getvalue(tuple, tupdesc, 11));
			strcat(values[i_fin], ",");
			strcat(values[i_fin], SPI_getvalue(tuple, tupdesc, 12));
			strcat(values[i_fin], ",");
			strcat(values[i_fin], SPI_getvalue(tuple, tupdesc, 13));
			strcat(values[i_fin], "}");
		}
		strcat(values[i_fin], "}");

		sprintf(sql, SDF1_4, symbol, pstrdup(buf), max_rows_to_return);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
		} else {
			dump_sdf1_inputs(access_lob_flag, max_rows_to_return, buf, symbol);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
		}
		sprintf(values[i_day_len], "%d", SPI_processed);
		strcpy(values[i_day], "{");
		for (i = 0; i < SPI_processed; i++) {
			tuple = tuptable->vals[i];
			if (i > 0) {
				strcat(values[i_day], ",");
			}
			strcat(values[i_day], "{");
			strcat(values[i_day], SPI_getvalue(tuple, tupdesc, 1));
			strcat(values[i_day], ",");
			strcat(values[i_day], SPI_getvalue(tuple, tupdesc, 2));
			strcat(values[i_day], ",");
			strcat(values[i_day], SPI_getvalue(tuple, tupdesc, 3));
			strcat(values[i_day], ",");
			strcat(values[i_day], SPI_getvalue(tuple, tupdesc, 4));
			strcat(values[i_day], ",");
			strcat(values[i_day], SPI_getvalue(tuple, tupdesc, 5));
			strcat(values[i_day], "}");
		}
		strcat(values[i_day], "}");

		sprintf(sql, SDF1_5, symbol);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			values[i_last_price] = SPI_getvalue(tuple, tupdesc, 1);
			values[i_last_open] = SPI_getvalue(tuple, tupdesc, 2);
			values[i_last_vol] = SPI_getvalue(tuple, tupdesc, 3);
		} else {
			dump_sdf1_inputs(access_lob_flag, max_rows_to_return, buf, symbol);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
			values[i_last_open] = NULL;
			values[i_last_price] = NULL;
			values[i_last_vol] = NULL;
		}

		if (access_lob_flag == true) {
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
		} else {
			dump_sdf1_inputs(access_lob_flag, max_rows_to_return, buf, symbol);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
		}
		sprintf(values[i_news_len], "%d", SPI_processed);
		strcpy(values[i_news], "{");
		for (i = 0; i < SPI_processed; i++) {
			tuple = tuptable->vals[i];
			if (i > 0) {
				strcat(values[i_news], ",");
			}
			strcat(values[i_news], "{");
			strcat(values[i_news], "\"");
			strcat(values[i_news], SPI_getvalue(tuple, tupdesc, 1));
			strcat(values[i_news], "\",\"");
			strcat(values[i_news], SPI_getvalue(tuple, tupdesc, 2));
			strcat(values[i_news], "\",\"");
			strcat(values[i_news], SPI_getvalue(tuple, tupdesc, 3));
			strcat(values[i_news], "\",\"");
			strcat(values[i_news], SPI_getvalue(tuple, tupdesc, 4));
			strcat(values[i_news], "\",\"");
			strcat(values[i_news], SPI_getvalue(tuple, tupdesc, 5));
			strcat(values[i_news], "\",\"");
			strcat(values[i_news], SPI_getvalue(tuple, tupdesc, 6));
			strcat(values[i_news], "\"");
			strcat(values[i_news], "}");
		}
		strcat(values[i_news], "}");

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
			elog(NOTICE, "SDF1 OUT: %d %s", i, values[i]);
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
