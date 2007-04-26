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
#include <utils/lsyscache.h>

/*
 * FIXME
 * This is part of the FROM clause and the WHERE clause from the 1.0.0 spec.
 * Why doesn't it match the database?
...
		"         SECURITY,\n" \
		"         CUSTOMER_ACCOUNT\n" \
...
		"WHERE    TR_B_ID = B_ID\n" \
		"         AND TR_S_SYMB = S_SYMB\n" \
		"         AND S_CO_ID = CO_ID\n" \
		"         AND CO_IN_ID = IN_ID\n" \
		"         AND SC_ID = IN_SC_ID\n" \
		"         AND B_NAME IN (%s)\n" \
		"         AND SC_NAME = %s\n" \
...
*/
#define BVF1_1 \
		"SELECT   B_NAME,\n" \
		"         SUM(TR_QTY * TR_BID_PRICE)\n" \
		"FROM     TRADE_REQUEST,\n" \
		"         SECTOR,\n" \
		"         INDUSTRY,\n" \
		"         COMPANY,\n" \
		"         BROKER,\n" \
		"         SECURITY,\n" \
		"         CUSTOMER_ACCOUNT\n" \
		"WHERE    TR_CA_ID = CA_ID\n" \
		"         AND CA_B_ID = B_ID\n" \
		"         AND TR_S_SYMB = S_SYMB\n" \
		"         AND S_CO_ID = CO_ID\n" \
		"         AND CO_IN_ID = IN_ID\n" \
		"         AND SC_ID = IN_SC_ID\n" \
		"         AND B_NAME IN (%s)\n" \
		"         AND SC_NAME = '%s'\n" \
		"GROUP BY B_NAME\n" \
		"ORDER BY 2 DESC;"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* Prototypes to prevent potential gcc warnings. */
Datum BrokerVolumeFrame1(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(BrokerVolumeFrame1);

/* Clause 3.3.1.3 */
Datum BrokerVolumeFrame1(PG_FUNCTION_ARGS)
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

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		ArrayType *broker_list = PG_GETARG_ARRAYTYPE_P(0);
		char *sector_name = (char *) PG_GETARG_TEXT_P(1);

		int16 typlen;
		bool typbyval;
		char typalign;

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;

		char sql[2048];
		char bl[2048];

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 4);
		values[1] = (char *) palloc(3 * sizeof(char));
		values[2] = (char *) palloc(2 * sizeof(char));

		/*
		 * This might be overkill since we always expect single dimensions
		 * arrays.
		 */
		ndim = ARR_NDIM(broker_list);
		dim = ARR_DIMS(broker_list);
		nitems = ArrayGetNItems(ndim, dim);
		get_typlenbyvalalign(ARR_ELEMTYPE(broker_list), &typlen, &typbyval,
				&typalign);
		p = ARR_DATA_PTR(broker_list);
		if (nitems > 0) {
#ifdef DEBUG
			elog(NOTICE, "[0.0] %s",
					DatumGetCString(DirectFunctionCall1(textout,
					PointerGetDatum(p))));
#endif
			strcat(bl, "'");
			strcat(bl, DatumGetCString(DirectFunctionCall1(textout,
					PointerGetDatum(p))));
			strcat(bl, "'");
			p = att_addlength(p, typlen, PointerGetDatum(p));                   
			p = (char *) att_align(p, typalign);
		}
		for (i = 1; i < nitems; i++) {
#ifdef DEBUG
			elog(NOTICE, "[0.%d] %s", i,
					DatumGetCString(DirectFunctionCall1(textout,
					PointerGetDatum(p))));
#endif
			strcat(bl, ",'");
			strcat(bl, DatumGetCString(DirectFunctionCall1(textout,
					PointerGetDatum(p))));
			strcat(bl, "'");
			p = att_addlength(p, typlen, PointerGetDatum(p));                   
			p = (char *) att_align(p, typalign);
		}
#ifdef DEBUG
		elog(NOTICE, "[1] %s", DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(sector_name))));
#endif

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();

		sprintf(sql, BVF1_1, bl, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(sector_name))));
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
		} else {
			elog(NOTICE, "ERROR: sql not ok");
		}

		sprintf(values[1], "%d", SPI_processed);
		/* FIXME: How do se we status properly? */
		sprintf(values[2], "%d", 1);

		if (SPI_processed == 0) {
			/* Total number of tuples to be returned. */
			funcctx->max_calls = 0;

			values[0] = (char *) palloc(3 * sizeof(char));
			values[3] = (char *) palloc(3 * sizeof(char));
			strcpy(values[0], "{}");
			strcpy(values[3], "{}");
		} else {
			/* Total number of tuples to be returned. */
			funcctx->max_calls = 1;

			values[0] = (char *) palloc((101 * SPI_processed + 2) *
					sizeof(char));
			values[3] = (char *) palloc(18 * SPI_processed + 2 *
					sizeof(char));

			strcpy(values[0], "{");
			strcpy(values[3], "{");

			if (SPI_processed > 0) {
				strcat(values[0], SPI_getvalue(tuple, tupdesc, 1));
				strcat(values[3], SPI_getvalue(tuple, tupdesc, 2));
			}
			for (i = 1; i < SPI_processed; i++) {
				tuple = tuptable->vals[i];
				strcat(values[0], ",");
				strcat(values[0], SPI_getvalue(tuple, tupdesc, 1));

				strcat(values[3], ",");
				strcat(values[3], SPI_getvalue(tuple, tupdesc, 2));
			}
			strcat(values[0], "}");
			strcat(values[3], "}");
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
		for (i = 0; i < 4; i++) {
			elog(NOTICE, "%d %s", i, values[i]);
		}
#endif /* DEBUG */

		/* Build a tuple. */
		tuple = BuildTupleFromCStrings(attinmeta, values);

		/* Make the tuple into a datum. */
		result = HeapTupleGetDatum(tuple);

		/* Clean up. */
		for (i = 0; i < 4; i++) {                                               
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
