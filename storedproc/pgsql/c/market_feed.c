/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2007 Mark Wong
 *
 * Based on TPC-E Standard Specification Revision 1.0
 */

#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h> /* this should include most necessary APIs */
#include <executor/executor.h>  /* for GetAttributeByName() */
#include <funcapi.h> /* for returning set of rows in order_status */
#include <utils/array.h>
#include <utils/lsyscache.h>
#include <utils/numeric.h>

typedef int16 NumericDigit;
#define NUMERIC_NDIGITS(num) \
		((VARSIZE(num) - NUMERIC_HDRSZ) / sizeof(NumericDigit))

/* Does the timestamp need to be more accurate? */
#define MFF1_1 \
		"UPDATE LAST_TRADE\n" \
		"SET    LT_PRICE = %s,\n" \
		"       LT_VOL = LT_VOL + %d,\n" \
		"       LT_DTS = '%04d-%02d-%02d %02d:%02d:%02d'\n" \
		"WHERE  LT_S_SYMB = '%s';"

#define MFF1_2 \
		"SELECT tr_t_id,\n" \
		"       tr_bid_price,\n" \
		"       tr_tt_id,\n" \
		"       tr_qty\n" \
		"FROM   trade_request\n" \
		"WHERE  tr_s_symb = '%s'\n" \
		"       AND ((tr_tt_id = '%s'\n" \
		"             AND tr_bid_price >= %s)\n" \
		"             OR (tr_tt_id = '%s'\n" \
		"                 AND tr_bid_price <= %s)\n" \
		"             OR (tr_tt_id = '%s'\n" \
		"                 AND tr_bid_price >= %s));"

#define MFF1_3 \
		"UPDATE trade\n" \
		"SET    t_dts = '%04d-%02d-%02d %02d:%02d:%02d',\n" \
		"       t_st_id = '%s'\n" \
		"WHERE  t_id = %s;"

#define MFF1_4 \
		"DELETE FROM trade_request\n" \
		"WHERE  tr_t_id = %s;"

#define MFF1_5 \
		"INSERT INTO trade_history\n" \
		"           (th_t_id,\n" \
		"            th_dts,\n" \
		"            th_st_id)\n" \
		"VALUES     (th_t_id = %s,\n" \
		"            th_dts = '%04d-%02d-%02d %02d:%02d:%02d',\n" \
		"            th_st_id = '%s');"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* Prototypes to prevent potential gcc warnings. */
Datum MarketFeedFrame1(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(MarketFeedFrame1);

/* Clause 3.3.1.3 */
Datum MarketFeedFrame1(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

	int i, j, k;

	int nitems_pq;
	char *p_tq;
	char *p_s;

	char **values = NULL;

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		ArrayType *price_quote_p = PG_GETARG_ARRAYTYPE_P(0);
		char *status_submitted_p = (char *) PG_GETARG_TEXT_P(1);
		ArrayType *symbol_p = PG_GETARG_ARRAYTYPE_P(2);
		ArrayType *trade_qty = PG_GETARG_ARRAYTYPE_P(3);
		char *type_limit_buy_p = (char *) PG_GETARG_TEXT_P(4);
        char *type_limit_sell_p = (char *) PG_GETARG_TEXT_P(5);
		char *type_stop_loss_p = (char *) PG_GETARG_TEXT_P(6);

		Datum *transdatums_pq;

		int16 typlen_s;
		bool typbyval_s;
		char typalign_s;

		int16 typlen_tq;
		bool typbyval_tq;
		char typalign_tq;

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;

		time_t t1;
		struct tm *date;

		char sql[2048];
		char price_quote[18];
		char status_submitted[5];
		char symbol[16];
		char type_limit_buy[4];
        char type_limit_sell[4];
		char type_stop_loss[4];
		char *trade_id;

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 2);
		values[0] = (char *) palloc(8 * sizeof(char));
		values[1] = (char *) palloc(2 * sizeof(char));

		/* FIXME: How do se we status properly? */
		sprintf(values[1], "%d", 1);

		/*
		 * This might be overkill since we always expect single dimensions
		 * arrays.
		 * Should probably check the count of all the arrays to make sure
		 * they are the same...
		 */
		get_typlenbyvalalign(ARR_ELEMTYPE(symbol_p), &typlen_s, &typbyval_s,
				&typalign_s);
		p_s = ARR_DATA_PTR(symbol_p);

		get_typlenbyvalalign(ARR_ELEMTYPE(trade_qty), &typlen_tq, &typbyval_tq,
				&typalign_tq);
		p_tq = ARR_DATA_PTR(trade_qty);

		deconstruct_array(price_quote_p, NUMERICOID, -1, false, 'i',
				&transdatums_pq, NULL, &nitems_pq);

		strcpy(status_submitted, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(status_submitted_p))));
		strcpy(type_limit_buy, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(type_limit_buy_p))));
        strcpy(type_limit_sell, DatumGetCString(DirectFunctionCall1(textout,
                PointerGetDatum(type_limit_sell_p))));
		strcpy(type_stop_loss, DatumGetCString(DirectFunctionCall1(textout,
                PointerGetDatum(type_stop_loss_p))));

		sprintf(values[1], "%d", 1);
#ifdef DEBUG
		for (i = 0; i < nitems_pq; i++) {
			elog(NOTICE, "[0.%d] %s", i,
					DatumGetCString(DirectFunctionCall1(numeric_out,
							transdatums_pq[i])));
			elog(NOTICE, "[2.%d] %s", i,
					DatumGetCString(DirectFunctionCall1(textout,
							PointerGetDatum(p_s))));
			elog(NOTICE, "[3.%d] %d", i, (int) (((int *)p_tq)[i]));

			p_s = att_addlength(p_s, typlen_s, PointerGetDatum(p_s));
			p_s = (char *) att_align(p_s, typalign_s);
		}
		p_s = ARR_DATA_PTR(symbol_p);
		elog(NOTICE, "[1] %s", DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(status_submitted_p))));
		elog(NOTICE, "[4] %s", DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(type_limit_buy_p))));
        elog(NOTICE, "[5] %s", DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(type_limit_sell_p))));
		elog(NOTICE, "[6] %s", DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(type_stop_loss_p))));
#endif

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		time(&t1);
		date = localtime(&t1);

		SPI_connect();

		k = 0;
		for (i = 0; i < nitems_pq; i++) {
			strcpy(price_quote,
					DatumGetCString(DirectFunctionCall1(numeric_out,
							transdatums_pq[i])));
			strcpy(symbol, DatumGetCString(DirectFunctionCall1(textout,
					PointerGetDatum(p_s))));
			/* BEGIN/COMMIT statements not supported with SPI. */
/*
			ret = SPI_exec("BEGIN;", 0);
			if (ret == SPI_OK_SELECT) {
			} else {
				elog(NOTICE, "ERROR: BEGIN not ok = %d", ret);
			}
*/

			sprintf(sql, MFF1_1,
					DatumGetCString(DirectFunctionCall1(numeric_out,            
							transdatums_pq[i])),
					(int) (((int *)p_tq)[i]),
					date->tm_year + 1900, date->tm_mon + 1, date->tm_mday,
					date->tm_hour, date->tm_min, date->tm_sec,
					symbol);
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret == SPI_OK_UPDATE) {
			} else {
				elog(NOTICE, "ERROR: sql not ok = %d", ret);
			}

			sprintf(sql, MFF1_2, symbol, type_stop_loss, price_quote,
					type_limit_sell, price_quote, type_limit_buy, price_quote);
#ifdef DEBUG
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_exec(sql, 0);
			if (ret != SPI_OK_SELECT) {
				elog(NOTICE, "ERROR: sql not ok = %d", ret);
				continue;
			}
#ifdef DEBUG
			elog(NOTICE, "%d row(s) returned", SPI_processed);
#endif /* DEBUG */

			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			for (j = 0; j < SPI_processed; j++, k++) {
				tuple = tuptable->vals[j];
				trade_id = SPI_getvalue(tuple, tupdesc, 1);
#ifdef DEBUG
				elog(NOTICE, "trade_id = %s", trade_id);
#endif /* DEBUG */

				sprintf(sql, MFF1_3,
					date->tm_year + 1900, date->tm_mon + 1, date->tm_mday,
					date->tm_hour, date->tm_min, date->tm_sec,
					status_submitted,
					trade_id);
#ifdef DEBUG
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				ret = SPI_exec(sql, 0);
				if (ret != SPI_OK_UPDATE) {
					elog(NOTICE, "ERROR: sql not ok = %d", ret);
				}

				sprintf(sql, MFF1_4, trade_id);
#ifdef DEBUG
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				ret = SPI_exec(sql, 0);
				if (ret != SPI_OK_DELETE) {
					elog(NOTICE, "ERROR: sql not ok = %d", ret);
				}

				sprintf(sql, MFF1_5,
					trade_id,
					date->tm_year + 1900, date->tm_mon + 1, date->tm_mday,
					date->tm_hour, date->tm_min, date->tm_sec,
					status_submitted);
#ifdef DEBUG
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			}

			/* BEGIN/COMMIT statements not supported with SPI. */
/*
			ret = SPI_exec("COMMIT;", 0);
			if (ret == SPI_OK_SELECT) {
			} else {
				elog(NOTICE, "ERROR: COMMIT not ok = %d", ret);
			}
*/
			p_s = att_addlength(p_s, typlen_s, PointerGetDatum(p_s));
			p_s = (char *) att_align(p_s, typalign_s);
		}
		sprintf(values[0], "%d", k);
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
		for (i = 0; i < 2; i++) {
			elog(NOTICE, "%d %s", i, values[i]);
		}
#endif /* DEBUG */

		/* Build a tuple. */
		tuple = BuildTupleFromCStrings(attinmeta, values);

		/* Make the tuple into a datum. */
		result = HeapTupleGetDatum(tuple);

		/* Clean up. */
		for (i = 0; i < 2; i++) {                                               
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
