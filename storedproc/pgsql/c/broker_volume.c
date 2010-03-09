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
#include <utils/array.h>
#include <utils/builtins.h>
#include <access/tupmacs.h>

#include "frame.h"

#define BVF1_1 \
		"SELECT b_name, SUM(tr_qty * tr_bid_price)\n" \
		"FROM trade_request, sector, industry, company, broker,\n" \
		"     security\n" \
		"WHERE tr_b_id = b_id\n" \
		"  AND tr_s_symb = s_symb\n" \
		"  AND s_co_id = co_id\n" \
		"  AND co_in_id = in_id\n" \
		"  AND sc_id = in_sc_id\n" \
		"  AND b_name = ANY (%s)\n" \
		"  AND sc_name = '%s'\n" \
		"GROUP BY b_name\n" \
		"ORDER BY 2 DESC"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* Prototypes to prevent potential gcc warnings. */
Datum BrokerVolumeFrame1(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(BrokerVolumeFrame1);

void dump_bvf1_inputs(ArrayType *, text *);

void dump_bvf1_inputs(ArrayType *broker_list_p, text *sector_name_p) {
	int ndim, nitems;
	int *dim;

	int16 typlen;
	bool typbyval;
	char typalign;

	int i;

	char *broker_list;

	ndim = ARR_NDIM(broker_list_p);
	dim = ARR_DIMS(broker_list_p);
	nitems = ArrayGetNItems(ndim, dim);
	get_typlenbyvalalign(ARR_ELEMTYPE(broker_list_p), &typlen, &typbyval,
			&typalign);

	broker_list = ARR_DATA_PTR(broker_list_p);
	elog(NOTICE, "BVF1: INPUTS START");
	for (i = 0; i < nitems; i++) {
		elog(NOTICE, "BVF1: broker_list[%d] %s", i,
				DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(broker_list))));
		broker_list = att_addlength_pointer(broker_list, typlen,
				broker_list);
		broker_list = (char *) att_align_nominal(broker_list, typalign);
	}
	elog(NOTICE, "BVF1: sector_name %s",
			DatumGetCString(DirectFunctionCall1(textout,
			PointerGetDatum(sector_name_p))));
	elog(NOTICE, "BVF1: INPUTS END");
}

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
	char *broker_list;	

	char **values = NULL;

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		ArrayType *broker_list_p = PG_GETARG_ARRAYTYPE_P(0);
		text *sector_name_p = PG_GETARG_TEXT_P(1);

		enum bvf1 { i_broker_name=0, i_list_len, i_status, i_volume };

		int16 typlen;
		bool typbyval;
		char typalign;

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;

		char sql[2048];
		char broker_list_array[(B_NAME_LEN + 3) * 40 + 5] = "'{";

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 4);
		values[i_list_len] = (char *) palloc((SMALLINT_LEN + 1) * sizeof(char));
		values[i_status] = (char *) palloc((STATUS_LEN + 1) * sizeof(char));

		strcpy(values[i_status], "0");

		/*
		 * This might be overkill since we always expect single dimensions
		 * arrays.
		 */
		ndim = ARR_NDIM(broker_list_p);
		dim = ARR_DIMS(broker_list_p);
		nitems = ArrayGetNItems(ndim, dim);
		get_typlenbyvalalign(ARR_ELEMTYPE(broker_list_p), &typlen, &typbyval,
				&typalign);
		broker_list = ARR_DATA_PTR(broker_list_p);
		/* Turn the broker_list input into an array format. */
		if (nitems > 0) {
			strcat(broker_list_array, "\"");
			strcat(broker_list_array,
					DatumGetCString(DirectFunctionCall1(textout,
					PointerGetDatum(broker_list))));
			broker_list = att_addlength_pointer(broker_list, typlen,
					broker_list);
			broker_list = (char *) att_align_nominal(broker_list,
					typalign);
			strcat(broker_list_array, "\"");
		}
		for (i = 1; i < nitems; i++) {
			strcat(broker_list_array, ",\"");
			strcat(broker_list_array,
					DatumGetCString(DirectFunctionCall1(textout,
					PointerGetDatum(broker_list))));
			broker_list = att_addlength_pointer(broker_list, typlen,
					broker_list);
			broker_list = (char *) att_align_nominal(broker_list,
					typalign);
			strcat(broker_list_array, "\"");
		}
		strcat(broker_list_array, "}'");
#ifdef DEBUG
		dump_bvf1_inputs(broker_list_p, sector_name_p);
#endif

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();

		sprintf(sql, BVF1_1, broker_list_array,
				DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(sector_name_p))));
#ifdef DEBUG
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
		} else {
			dump_bvf1_inputs(broker_list_p, sector_name_p);
			FAIL_FRAME(&funcctx->max_calls, values[i_status], sql);
		}

		sprintf(values[i_list_len], "%d", SPI_processed);
		values[i_broker_name] = (char *) palloc(((B_NAME_LEN + 2) *
				(SPI_processed + 1) + 3) * sizeof(char));
		values[i_volume] = (char *) palloc((INTEGER_LEN *
				(SPI_processed + 1) + 3) * sizeof(char));

		if (SPI_processed == 0) {
			strcpy(values[i_broker_name], "{}");
			strcpy(values[i_volume], "{}");
		} else {
			strcpy(values[i_broker_name], "{");
			strcpy(values[i_volume], "{");

			if (SPI_processed > 0) {
				strcat(values[i_broker_name], "\"");
				strcat(values[i_broker_name], SPI_getvalue(tuple, tupdesc, 1));
				strcat(values[i_broker_name], "\"");
				strcat(values[i_volume], SPI_getvalue(tuple, tupdesc, 2));
			}
			for (i = 1; i < SPI_processed; i++) {
				tuple = tuptable->vals[i];
				strcat(values[i_broker_name], ",");
				strcat(values[i_broker_name], "\"");
				strcat(values[i_broker_name], SPI_getvalue(tuple, tupdesc, 1));
				strcat(values[i_broker_name], "\"");

				strcat(values[i_volume], ",");
				strcat(values[i_volume], SPI_getvalue(tuple, tupdesc, 2));
			}
			strcat(values[i_broker_name], "}");
			strcat(values[i_volume], "}");
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
			elog(NOTICE, "BVF1 OUT: %d %s", i, values[i]);
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
