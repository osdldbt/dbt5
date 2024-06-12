/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * Based on TPC-E Standard Specification Revision 1.14.0.
 */

#include <sys/types.h>
#include <unistd.h>
#include <inttypes.h>
#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h> /* this should include most necessary APIs */
#include <executor/executor.h> /* for GetAttributeByName() */
#include <funcapi.h> /* for returning set of rows */
#include <utils/lsyscache.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <access/tupmacs.h>
#include <catalog/pg_type.h>

#include "frame.h"
#include "dbt5common.h"

#define BROKER_LIST_ARRAY_LEN ((B_NAME_LEN + 3) * 40 + 4)

#define SQLBVF1_1                                                             \
	"SELECT b_name\n"                                                         \
	"     , sum(tr_qty * tr_bid_price)\n"                                     \
	"FROM trade_request\n"                                                    \
	"   , sector\n"                                                           \
	"   , industry\n"                                                         \
	"   , company\n"                                                          \
	"   , broker\n"                                                           \
	"   , security\n"                                                         \
	"WHERE tr_b_id = b_id\n"                                                  \
	"  AND tr_s_symb = s_symb\n"                                              \
	"  AND s_co_id = co_id\n"                                                 \
	"  AND co_in_id = in_id\n"                                                \
	"  AND sc_id = in_sc_id\n"                                                \
	"  AND b_name = ANY ($1)\n"                                               \
	"  AND sc_name = $2\n"                                                    \
	"GROUP BY b_name\n"                                                       \
	"ORDER BY 2 DESC"

#define BVF1_1 BVF1_statements[0].plan

static cached_statement BVF1_statements[] = {

	{ SQLBVF1_1, 2, { TEXTARRAYOID, TEXTOID } },

	{ NULL }
};

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* Prototypes to prevent potential gcc warnings. */
Datum BrokerVolumeFrame1(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(BrokerVolumeFrame1);

#ifdef DEBUG
void dump_bvf1_inputs(ArrayType *, text *);

void
dump_bvf1_inputs(ArrayType *broker_list_p, text *sector_name_p)
{
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
	get_typlenbyvalalign(
			ARR_ELEMTYPE(broker_list_p), &typlen, &typbyval, &typalign);

	broker_list = ARR_DATA_PTR(broker_list_p);
	elog(DEBUG1, "BVF1: INPUTS START");
	for (i = 0; i < nitems; i++) {
		elog(DEBUG1, "BVF1: broker_list[%d] %s", i,
				DatumGetCString(DirectFunctionCall1(
						textout, PointerGetDatum(broker_list))));
		broker_list = att_addlength_pointer(broker_list, typlen, broker_list);
		broker_list = (char *) att_align_nominal(broker_list, typalign);
	}
	elog(DEBUG1, "BVF1: sector_name %s",
			DatumGetCString(DirectFunctionCall1(
					textout, PointerGetDatum(sector_name_p))));
	elog(DEBUG1, "BVF1: INPUTS END");
}
#endif /* DEBUG */

/* Clause 3.3.1.3 */
Datum
BrokerVolumeFrame1(PG_FUNCTION_ARGS)
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

		enum bvf1
		{
			i_broker_name = 0,
			i_list_len,
			i_volume
		};

		int16 typlen;
		bool typbyval;
		char typalign;

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;

		char broker_list_array[BROKER_LIST_ARRAY_LEN + 1] = "'{";
		int length_bl = BROKER_LIST_ARRAY_LEN - 2;
		Datum args[2];
		char nulls[2] = { ' ', ' ' };

		char *tmp;

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 3);
		values[i_list_len]
				= (char *) palloc((SMALLINT_LEN + 1) * sizeof(char));

		/*
		 * This might be overkill since we always expect single dimensions
		 * arrays.
		 */
		ndim = ARR_NDIM(broker_list_p);
		dim = ARR_DIMS(broker_list_p);
		nitems = ArrayGetNItems(ndim, dim);
		get_typlenbyvalalign(
				ARR_ELEMTYPE(broker_list_p), &typlen, &typbyval, &typalign);
		broker_list = ARR_DATA_PTR(broker_list_p);
		/* Turn the broker_list input into an array format. */
		if (nitems > 0) {
			strncat(broker_list_array, "\"", length_bl--);
			if (length_bl < 0) {
				FAIL_FRAME("broker_list_array needs to be increased");
			}

			tmp = DatumGetCString(DirectFunctionCall1(
					textout, PointerGetDatum(broker_list)));
			strncat(broker_list_array,
					DatumGetCString(DirectFunctionCall1(
							textout, PointerGetDatum(broker_list))),
					length_bl);
			length_bl -= strlen(tmp);
			if (length_bl < 0) {
				FAIL_FRAME("broker_list_array needs to be increased");
			}

			broker_list
					= att_addlength_pointer(broker_list, typlen, broker_list);
			broker_list = (char *) att_align_nominal(broker_list, typalign);
			strncat(broker_list_array, "\"", length_bl--);
			if (length_bl < 0) {
				FAIL_FRAME("broker_list_array needs to be increased");
			}
		}
		for (i = 1; i < nitems; i++) {
			strncat(broker_list_array, ",\"", length_bl--);
			if (length_bl < 0) {
				FAIL_FRAME("broker_list_array needs to be increased");
			}

			tmp = DatumGetCString(DirectFunctionCall1(
					textout, PointerGetDatum(broker_list)));
			strncat(broker_list_array, tmp, length_bl);
			length_bl -= strlen(tmp);
			if (length_bl < 0) {
				FAIL_FRAME("broker_list_array needs to be increased");
			}

			broker_list
					= att_addlength_pointer(broker_list, typlen, broker_list);
			broker_list = (char *) att_align_nominal(broker_list, typalign);
			strncat(broker_list_array, "\"", length_bl--);
			if (length_bl < 0) {
				FAIL_FRAME("broker_list_array needs to be increased");
			}
		}
		strncat(broker_list_array, "}'", length_bl);
		length_bl -= 2;
		if (length_bl < 0) {
			FAIL_FRAME("broker_list_array needs to be increased");
		}
#ifdef DEBUG
		dump_bvf1_inputs(broker_list_p, sector_name_p);
#endif

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();
		plan_queries(BVF1_statements);
#ifdef DEBUG
		elog(DEBUG1, "%s", SQLBVF1_1);
#endif /* DEBUG */
		args[0] = PointerGetDatum(broker_list_p);
		args[1] = PointerGetDatum(sector_name_p);
		ret = SPI_execute_plan(BVF1_1, args, nulls, true, 0);
		if (ret != SPI_OK_SELECT) {
			FAIL_FRAME_SET(&funcctx->max_calls, BVF1_statements[0].sql);
		}
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		tuple = tuptable->vals[0];

		snprintf(values[i_list_len], BIGINT_LEN, "%" PRId64, SPI_processed);

		if (SPI_processed == 0) {
			values[i_broker_name] = (char *) palloc(3 * sizeof(char));
			values[i_volume] = (char *) palloc(3 * sizeof(char));

			strncpy(values[i_broker_name], "{}", 3);
			strncpy(values[i_volume], "{}", 3);
		} else {
			int length_bn, length_v;

			length_bn = (B_NAME_LEN + 2) * (SPI_processed + 1) + 3;
			values[i_broker_name]
					= (char *) palloc(length_bn-- * sizeof(char));

			length_v = INTEGER_LEN * (SPI_processed + 1) + 3;
			values[i_volume] = (char *) palloc(length_v-- * sizeof(char));

			values[i_broker_name][0] = '{';
			values[i_broker_name][1] = '\0';

			values[i_volume][0] = '{';
			values[i_volume][1] = '\0';

			if (SPI_processed > 0) {
				strncat(values[i_broker_name], "\"", length_bn--);
				if (length_bn < 0) {
					FAIL_FRAME("broker_name values needs to be increased");
				}

				tmp = SPI_getvalue(tuple, tupdesc, 1);
				strncat(values[i_broker_name], tmp, length_bn);
				length_bn -= strlen(tmp);
				if (length_bn < 0) {
					FAIL_FRAME("broker_name values needs to be increased");
				}

				strncat(values[i_broker_name], "\"", length_bn--);
				if (length_bn < 0) {
					FAIL_FRAME("broker_name values needs to be increased");
				}

				tmp = SPI_getvalue(tuple, tupdesc, 2);
				strncat(values[i_volume], tmp, length_v);
				length_v -= strlen(tmp);
				if (length_v < 0) {
					FAIL_FRAME("volume values needs to be increased");
				}
			}
			for (i = 1; i < SPI_processed; i++) {
				tuple = tuptable->vals[i];

				strncat(values[i_broker_name], ",\"", length_bn);
				length_bn -= 2;
				if (length_bn < 0) {
					FAIL_FRAME("broker_name values needs to be increased");
				}

				tmp = SPI_getvalue(tuple, tupdesc, 1);
				strncat(values[i_broker_name], tmp, length_bn);
				length_bn -= strlen(tmp);
				if (length_bn < 0) {
					FAIL_FRAME("broker_name values needs to be increased");
				}

				strncat(values[i_broker_name], "\"", length_bn--);
				if (length_bn < 0) {
					FAIL_FRAME("broker_name values needs to be increased");
				}

				strncat(values[i_volume], ",", length_v--);
				if (length_v < 0) {
					FAIL_FRAME("volume values needs to be increased");
				}

				tmp = SPI_getvalue(tuple, tupdesc, 2);
				strncat(values[i_volume], tmp, length_v);
				length_v -= strlen(tmp);
				if (length_v < 0) {
					FAIL_FRAME("volume values needs to be increased");
				}
			}
			strncat(values[i_broker_name], "}", length_bn--);
			if (length_bn < 0) {
				FAIL_FRAME("broker_name values needs to be increased");
			}

			strncat(values[i_volume], "}", length_v--);
			if (length_v < 0) {
				FAIL_FRAME("volume values needs to be increased");
			}
		}

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc)
				!= TYPEFUNC_COMPOSITE) {
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								   errmsg("function returning record called "
										  "in context "
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
		for (i = 0; i < 3; i++) {
			elog(DEBUG1, "BVF1 OUT: %d %s", i, values[i]);
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
