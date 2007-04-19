/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2007 Mark Wong
 *
 * Based on TPC-E Standard Specification Revision 1.0
 */

#include <sys/types.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h> /* this should include most necessary APIs */
#include <executor/executor.h>  /* for GetAttributeByName() */
#include <funcapi.h> /* for returning set of rows in order_status */

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define CPF1_1 \
		"SELECT c_id\n" \
		"FROM   customer\n" \
		"WHERE  c_tax_id = '%s';"

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
		"WHERE  c_id = %ld;"

#define CPF1_3 \
		"SELECT   ca_id,\n" \
		"         ca_bal,\n" \
		"         SUM(hs_qty * lt_price) AS soma\n" \
		"FROM     customer_account\n" \
		"         LEFT OUTER JOIN holding_summary\n" \
		"           ON hs_ca_id = ca_id,\n" \
		"         last_trade\n" \
		"WHERE    ca_c_id = %ld\n" \
		"         AND lt_s_symb = hs_s_symb\n" \
		"GROUP BY ca_id,ca_bal\n" \
		"ORDER BY 3 ASC\n" \
		"LIMIT 10;"

#define CPF2_1 \
		"SELECT   t_id,\n" \
		"         t_s_symb,\n" \
		"         t_qty,\n" \
		"         st_name,\n" \
		"         th_dts\n" \
		"FROM     (SELECT   t_id AS id\n" \
		"          FROM     trade\n" \
		"          WHERE    t_ca_id = %ld\n" \
		"          ORDER BY t_dts DESC\n" \
		"          LIMIT 10) AS t,\n" \
		"         trade,\n" \
		"         trade_history,\n" \
		"         status_type\n" \
		"WHERE    t_id = id\n" \
		"         AND th_t_id = t_id\n" \
		"         AND st_id = th_st_id\n" \
		"ORDER BY th_dts DESC\n" \
		"LIMIT 30;"

/* Prototypes to prevent potential gcc warnings. */
Datum CustomerPositionFrame1(PG_FUNCTION_ARGS);
Datum CustomerPositionFrame2(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(CustomerPositionFrame1);

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

		text *tax_id;

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
		values = (char **) palloc(sizeof(char *) * 23);
		values[0] = (char *) palloc(12 * sizeof(char));
		values[2] = (char *) palloc(3 * sizeof(char));
		values[4] = (char *) palloc(12 * sizeof(char));
		values[5] = (char *) palloc(4 * sizeof(char));
		values[6] = (char *) palloc(4 * sizeof(char));
		values[7] = (char *) palloc(4 * sizeof(char));
		values[8] = (char *) palloc(4 * sizeof(char));
		values[9] = (char *) palloc(4 * sizeof(char));
		values[10] = (char *) palloc(4 * sizeof(char));
		values[11] = (char *) palloc(20 * sizeof(char));
		values[12] = (char *) palloc(51 * sizeof(char));
		values[13] = (char *) palloc(51 * sizeof(char));
		values[14] = (char *) palloc(6 * sizeof(char));
		values[15] = (char *) palloc(6 * sizeof(char));
		values[16] = (char *) palloc(6 * sizeof(char));
		values[17] = (char *) palloc(31 * sizeof(char));
		values[18] = (char *) palloc(2 * sizeof(char));
		values[19] = (char *) palloc(31 * sizeof(char));
		values[20] = (char *) palloc(11 * sizeof(char));
		values[21] = (char *) palloc(11 * sizeof(char));
		values[22] = (char *) palloc(11 * sizeof(char));
		values[23] = (char *) palloc(2 * sizeof(char));

		/* Create a function context for cross-call persistence. */
		funcctx = SRF_FIRSTCALL_INIT();

		/* Switch to memory context appropriate for multiple function calls. */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		cust_id = PG_GETARG_INT64(0);
		tax_id= PG_GETARG_TEXT_P(1);
#ifdef DEBUG
		elog(NOTICE, "[1] %ld", cust_id);
		elog(NOTICE, "[2] %s", DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(tax_id))));
#endif /* DEBUG */

		SPI_connect();
		if (cust_id == 0) {
			sprintf(sql, CPF1_1, DatumGetCString(DirectFunctionCall1(textout,
					PointerGetDatum(tax_id))));
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
				/* Total number of tuples to be returned. */
				funcctx->max_calls = 0;

				elog(NOTICE, "ERROR: did not get cust_id.");
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
			char *tmp;

			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];

			strcpy(values[4], SPI_getvalue(tuple, tupdesc, 8));

			tmp = SPI_getvalue(tuple, tupdesc, 10);
			if (tmp != NULL)
				strcpy(values[5], tmp);
			else
				values[5][0] = '\0';

			tmp = SPI_getvalue(tuple, tupdesc, 14);
			if (tmp != NULL)
				strcpy(values[6], tmp);
			else
				values[6][0] = '\0';

			tmp = SPI_getvalue(tuple, tupdesc, 18);
			if (tmp != NULL)
				strcpy(values[7], tmp);
			else
				values[7][0] = '\0';

			tmp = SPI_getvalue(tuple, tupdesc, 9);
			if (tmp != NULL)
				strcpy(values[8], tmp);
			else
				values[8][0] = '\0';

			tmp = SPI_getvalue(tuple, tupdesc, 13);
			if (tmp != NULL)
				strcpy(values[9], tmp);
			else
				values[9][0] = '\0';

			tmp = SPI_getvalue(tuple, tupdesc, 17);
			if (tmp != NULL)
				strcpy(values[10], tmp);
			else
				values[10][0] = '\0';

			strcpy(values[11], SPI_getvalue(tuple, tupdesc, 7));

			tmp = SPI_getvalue(tuple, tupdesc, 21);
			if (tmp != NULL)
				strcpy(values[12], tmp);
			else
				values[12][0] = '\0';

			tmp = SPI_getvalue(tuple, tupdesc, 22);
			if (tmp != NULL)
				strcpy(values[13], tmp);
			else
				values[13][0] = '\0';

			tmp = SPI_getvalue(tuple, tupdesc, 12);
			if (tmp != NULL)
				strcpy(values[14], tmp);
			else
				values[14][0] = '\0';

			tmp = SPI_getvalue(tuple, tupdesc, 16);
			if (tmp != NULL)
				strcpy(values[15], tmp);
			else
				values[15][0] = '\0';

			tmp = SPI_getvalue(tuple, tupdesc, 20);
			if (tmp != NULL)
				strcpy(values[16], tmp);
			else
				values[16][0] = '\0';

			strcpy(values[17], SPI_getvalue(tuple, tupdesc, 3));

			tmp = SPI_getvalue(tuple, tupdesc, 5);
			if (tmp != NULL)
				strcpy(values[18], tmp);
			else
				values[18][0] = '\0';

			strcpy(values[19], SPI_getvalue(tuple, tupdesc, 2));

			tmp = SPI_getvalue(tuple, tupdesc, 11);
			if (tmp != NULL)
				strcpy(values[20], tmp);
			else
				values[20][0] = '\0';

			tmp = SPI_getvalue(tuple, tupdesc, 15);
			if (tmp != NULL)
				strcpy(values[21], tmp);
			else
				values[21][0] = '\0';

			tmp = SPI_getvalue(tuple, tupdesc, 19);
			if (tmp != NULL)
				strcpy(values[22], tmp);
			else
				values[22][0] = '\0';

			tmp = SPI_getvalue(tuple, tupdesc, 4);
			if (tmp != NULL)
				strcpy(values[23], tmp);
			else
				values[23][0] = '\0';
		} else {
/*
			values[4][0] = '\0';
			values[5][0] = '\0';
			values[6][0] = '\0';
			values[7][0] = '\0';
			values[8][0] = '\0';
			values[9][0] = '\0';
			values[10][0] = '\0';
			values[11][0] = '\0';
			values[12][0] = '\0';
			values[13][0] = '\0';
			values[14][0] = '\0';
			values[15][0] = '\0';
			values[16][0] = '\0';
			values[17][0] = '\0';
			values[18][0] = '\0';
			values[19][0] = '\0';
			values[20][0] = '\0';
			values[21][0] = '\0';
			values[22][0] = '\0';
			values[23][0] = '\0';
*/
		}

		sprintf(sql, CPF1_3, cust_id);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		sprintf(values[2], "%d", SPI_processed);
#ifdef DEBUG
		elog(NOTICE, "%d row(s) returned from CPF1_3.", SPI_processed);
#endif /* DEBUG */
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			/* Total number of tuples to be returned. */
			funcctx->max_calls = 1;

			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];

			values[1] = (char *) palloc((12 * SPI_processed + 2) *
					sizeof(char));
			values[3] = (char *) palloc((18 * SPI_processed + 2) *
					sizeof(char));

			strcpy(values[1], "{");
			strcpy(values[3], "{");

			if (SPI_processed > 0) {
				strcat(values[1], SPI_getvalue(tuple, tupdesc, 1));
				strcat(values[3], SPI_getvalue(tuple, tupdesc, 3));
			}
			for (i = 1; i < SPI_processed; i++) {
				tuple = tuptable->vals[i];
				strcat(values[1], ",");
				strcat(values[1], SPI_getvalue(tuple, tupdesc, 1));

				strcat(values[3], ",");
				strcat(values[3], SPI_getvalue(tuple, tupdesc, 3));
			}
			strcat(values[1], "}");
			strcat(values[3], "}");
		} else {
			/* Total number of tuples to be returned. */
			funcctx->max_calls = 0;

/*
			values[1] = (char *) palloc(3 * sizeof(char));
			values[3] = (char *) palloc(3 * sizeof(char));
			strcpy(values[1], "{}");
			strcpy(values[3], "{}");
*/
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

		snprintf(values[0], 12, "%ld", cust_id);

#ifdef DEBUG
		for (i = 0; i < 24; i++) {
			elog(NOTICE, "%d '%s'", i, values[i]);
		}
#endif /* DEBUG */

		/* Build a tuple. */
		tuple = BuildTupleFromCStrings(funcctx->attinmeta, values);

		/* Make the tuple into a datum. */
		result = HeapTupleGetDatum(tuple);

		/* Free memory. */
		for (i = 0; i < 24; i++) {
			pfree(values[i]);
		}
		pfree(values);

		SRF_RETURN_NEXT(funcctx, result);
	} else {
		SPI_finish();
		SRF_RETURN_DONE(funcctx);
	}
}

PG_FUNCTION_INFO_V1(CustomerPositionFrame2);

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

		int64 acct_id;

		int ret;
		char sql[1024];

		acct_id = PG_GETARG_INT64(0);
#ifdef DEBUG
		elog(NOTICE, "[1] %ld", acct_id);
#endif /* DEBUG */

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 7);
		values[1] = (char *) palloc(3 * sizeof(char));
		values[3] = (char *) palloc(2 * sizeof(char));

		/* Create a function context for cross-call persistence. */
		funcctx = SRF_FIRSTCALL_INIT();

		/* Switch to memory context appropriate for multiple function calls. */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();

		sprintf(sql, CPF2_1, acct_id);
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		sprintf(values[1], "%d", SPI_processed);
#ifdef DEBUG
		elog(NOTICE, "%d row(s) returned.", SPI_processed);
#endif /* DEBUG */
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			/* Total number of tuples to be returned. */
			funcctx->max_calls = 1;

			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];

			values[0] = (char *) palloc((27 * SPI_processed + 2) *
					sizeof(char));
			values[2] = (char *) palloc((7 * SPI_processed + 2) * sizeof(char));
			values[4] = (char *) palloc((16 * SPI_processed + 2) *
					sizeof(char));
			values[5] = (char *) palloc((16 * SPI_processed + 2) *
					sizeof(char));
			values[6] = (char *) palloc((11 * SPI_processed + 2) *
					sizeof(char));

			strcpy(values[0], "{");
			strcpy(values[2], "{");
			strcpy(values[4], "{");
			strcpy(values[5], "{");
			strcpy(values[6], "{");
			if (SPI_processed > 0) {
				strcat(values[0], SPI_getvalue(tuple, tupdesc, 5));
				strcat(values[2], SPI_getvalue(tuple, tupdesc, 3));
				strcat(values[4], SPI_getvalue(tuple, tupdesc, 2));
				strcat(values[5], SPI_getvalue(tuple, tupdesc, 1));
				strcat(values[6], SPI_getvalue(tuple, tupdesc, 4));
			}
			for (i = 1; i < SPI_processed; i++) {
				tuple = tuptable->vals[i];

				strcat(values[0], ",");
				strcat(values[0], SPI_getvalue(tuple, tupdesc, 5));

				strcat(values[2], ",");
				strcat(values[2], SPI_getvalue(tuple, tupdesc, 3));

				strcat(values[4], ",");
				strcat(values[4], SPI_getvalue(tuple, tupdesc, 2));

				strcat(values[5], ",");
				strcat(values[5], SPI_getvalue(tuple, tupdesc, 1));

				strcat(values[6], ",");
				strcat(values[6], SPI_getvalue(tuple, tupdesc, 4));
			}
			strcat(values[0], "}");
			strcat(values[2], "}");
			strcat(values[4], "}");
			strcat(values[5], "}");
			strcat(values[6], "}");
		} else {
			/* Total number of tuples to be returned. */
			funcctx->max_calls = 0;

			/*
			 * FIXME: Probably don't need to do this if we're not going to
			 * return any rows, but if we do then we don't need to figure
			 * out what pieces of memory we need to free later in the function.
			 */
			values[0] = (char *) palloc(3 * sizeof(char));
			values[2] = (char *) palloc(3 * sizeof(char));
			values[4] = (char *) palloc(3 * sizeof(char));
			values[5] = (char *) palloc(3 * sizeof(char));
			values[6] = (char *) palloc(3 * sizeof(char));
			strcpy(values[0], "{}");
			strcpy(values[2], "{}");
			strcpy(values[4], "{}");
			strcpy(values[5], "{}");
			strcpy(values[6], "{}");
		}

		/* FIXME: How is 'status' supposed to be set? */
		strcpy(values[3], "1");

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
			elog(NOTICE, "%d '%s'", i, values[i]);
		}
#endif /* DEBUG */

		/* Build a tuple. */
		tuple = BuildTupleFromCStrings(funcctx->attinmeta, values);

		/* Make the tuple into a datum. */
		result = HeapTupleGetDatum(tuple);

		/* Free memory. */
		for (i = 0; i < 7; i++) {
			pfree(values[i]);
		}
		pfree(values);
		SRF_RETURN_NEXT(funcctx, result);
	} else {
		SPI_finish();
		SRF_RETURN_DONE(funcctx);
	}
}
