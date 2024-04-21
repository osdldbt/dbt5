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
#include <funcapi.h> /* for returning set of rows in order_status */
#include <utils/datetime.h>
#include <utils/numeric.h>
#include <catalog/pg_type.h>

#include "frame.h"
#include "dbt5common.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define SQLTSF1_1                                                             \
	"SELECT t_id\n"                                                           \
	"     , t_dts\n"                                                          \
	"     , st_name\n"                                                        \
	"     , tt_name\n"                                                        \
	"     , t_s_symb\n"                                                       \
	"     , t_qty\n"                                                          \
	"     , t_exec_name\n"                                                    \
	"     , t_chrg\n"                                                         \
	"     , s_name\n"                                                         \
	"     , ex_name\n"                                                        \
	"FROM trade\n"                                                            \
	"   , status_type\n"                                                      \
	"   , trade_type\n"                                                       \
	"   , security\n"                                                         \
	"   , exchange\n"                                                         \
	"WHERE t_ca_id = $1\n"                                                    \
	"  AND st_id = t_st_id\n"                                                 \
	"  AND tt_id = t_tt_id\n"                                                 \
	"  AND s_symb = t_s_symb\n"                                               \
	"  AND ex_id = s_ex_id\n"                                                 \
	"ORDER BY t_dts DESC\n"                                                   \
	"LIMIT 50"

#define SQLTSF1_2                                                             \
	"SELECT c_l_name\n"                                                       \
	"     , c_f_name\n"                                                       \
	"     , b_name\n"                                                         \
	"FROM customer_account\n"                                                 \
	"   , customer\n"                                                         \
	"   , broker\n"                                                           \
	"WHERE ca_id = $1\n"                                                      \
	"  AND c_id = ca_c_id\n"                                                  \
	"  AND b_id = ca_b_id"

#define TSF1_1 TSF1_statements[0].plan
#define TSF1_2 TSF1_statements[1].plan

static cached_statement TSF1_statements[] = {

	{ SQLTSF1_1, 1, { INT8OID } },

	{ SQLTSF1_2, 1, { INT8OID } },

	{ NULL }
};

/* Prototypes. */
#ifdef DEBUG
void dump_tsf1_inputs(long);
#endif /* DEBUG */

Datum TradeStatusFrame1(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(TradeStatusFrame1);

#ifdef DEBUG
void
dump_tsf1_inputs(long acct_id)
{
	elog(DEBUG1, "TSF1: INPUTS START");
	elog(DEBUG1, "TSF1: acct_id %ld", acct_id);
	elog(DEBUG1, "TSF1: INPUTS END");
}
#endif /* DEBUG */

/* Clause 3.3.9.3 */
Datum
TradeStatusFrame1(PG_FUNCTION_ARGS)
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

		enum tsf1
		{
			i_broker_name = 0,
			i_charge,
			i_cust_f_name,
			i_cust_l_name,
			i_ex_name,
			i_exec_name,
			i_num_found,
			i_s_name,
			i_status_name,
			i_symbol,
			i_trade_dts,
			i_trade_id,
			i_trade_qty,
			i_type_name
		};

		long acct_id = PG_GETARG_INT64(0);

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;
		Datum args[1];
		char nulls[1] = { ' ' };

		char *tmp;
		int length_c, length_en, length_en2, length_s, length_sn, length_sn2,
				length_td, length_ti, length_tn, length_tq;

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 14);

		length_c = (VALUE_T_LEN + 1) * 50;
		values[i_charge] = (char *) palloc(length_c-- * sizeof(char));

		length_en2 = (EX_NAME_LEN + 3) * 50;
		values[i_ex_name] = (char *) palloc(length_en2-- * sizeof(char));

		length_en = (T_EXEC_NAME_LEN + 3) * 50;
		values[i_exec_name] = (char *) palloc(length_en-- * sizeof(char));

		values[i_num_found] = (char *) palloc((BIGINT_LEN + 1) * sizeof(char));

		length_sn2 = (S_NAME_LEN + 3) * 50;
		values[i_s_name] = (char *) palloc(length_sn2-- * sizeof(char));

		length_sn = (ST_NAME_LEN + 3) * 50;
		values[i_status_name] = (char *) palloc(length_sn-- * sizeof(char));

		length_s = (S_SYMB_LEN + 3) * 50;
		values[i_symbol] = (char *) palloc(length_s-- * sizeof(char));

		length_td = (MAXDATELEN + 1) * 50;
		values[i_trade_dts] = (char *) palloc(length_td-- * sizeof(char));

		length_ti = (BIGINT_LEN + 1) * 50;
		values[i_trade_id] = (char *) palloc(length_ti-- * sizeof(char));

		length_tq = (INTEGER_LEN + 1) * 50;
		values[i_trade_qty] = (char *) palloc(length_tq-- * sizeof(char));

		length_tn = (TT_NAME_LEN + 3) * 50;
		values[i_type_name] = (char *) palloc(length_tn-- * sizeof(char));

		values[i_cust_l_name] = NULL;
		values[i_cust_f_name] = NULL;
		values[i_broker_name] = NULL;

#ifdef DEBUG
		dump_tsf1_inputs(acct_id);
#endif

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();
		plan_queries(TSF1_statements);
#ifdef DEBUG
		elog(DEBUG1, "%s", SQLTSF1_1);
#endif /* DEBUG */
		args[0] = Int64GetDatum(acct_id);
		ret = SPI_execute_plan(TSF1_1, args, nulls, true, 0);
		if (ret != SPI_OK_SELECT) {
			FAIL_FRAME_SET(&funcctx->max_calls, TSF1_statements[0].sql);
#ifdef DEBUG
			dump_tsf1_inputs(acct_id);
#endif /* DEBUG */
		}
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		snprintf(values[i_num_found], BIGINT_LEN, "%" PRId64, SPI_processed);

		values[i_trade_id][0] = '{';
		values[i_trade_id][1] = '\0';

		values[i_trade_dts][0] = '{';
		values[i_trade_dts][1] = '\0';

		values[i_status_name][0] = '{';
		values[i_status_name][1] = '\0';

		values[i_type_name][0] = '{';
		values[i_type_name][1] = '\0';

		values[i_symbol][0] = '{';
		values[i_symbol][1] = '\0';

		values[i_trade_qty][0] = '{';
		values[i_trade_qty][1] = '\0';

		values[i_exec_name][0] = '{';
		values[i_exec_name][1] = '\0';

		values[i_charge][0] = '{';
		values[i_charge][1] = '\0';

		values[i_s_name][0] = '{';
		values[i_s_name][1] = '\0';

		values[i_ex_name][0] = '{';
		values[i_ex_name][1] = '\0';

		for (i = 0; i < SPI_processed; i++) {
			tuple = tuptable->vals[i];
			if (i > 0) {
				strncat(values[i_trade_id], ",", length_ti--);
				if (length_ti < 0) {
					FAIL_FRAME("trade_id values needs to be increased");
				}

				strncat(values[i_trade_dts], ",", length_td--);
				if (length_td < 0) {
					FAIL_FRAME("trade_dts values needs to be increased");
				}

				strncat(values[i_status_name], ",", length_sn--);
				if (length_sn < 0) {
					FAIL_FRAME("status_name values needs to be increased");
				}

				strncat(values[i_type_name], ",", length_tn--);
				if (length_tn < 0) {
					FAIL_FRAME("type_name values needs to be increased");
				}

				strncat(values[i_symbol], ",", length_s--);
				if (length_s < 0) {
					FAIL_FRAME("symbol values needs to be increased");
				}

				strncat(values[i_trade_qty], ",", length_tq--);
				if (length_tq < 0) {
					FAIL_FRAME("trade_qty values needs to be increased");
				}

				strncat(values[i_exec_name], ",", length_en--);
				if (length_en < 0) {
					FAIL_FRAME("exec_name values needs to be increased");
				}

				strncat(values[i_charge], ",", length_c--);
				if (length_c < 0) {
					FAIL_FRAME("charge values needs to be increased");
				}

				strncat(values[i_s_name], ",", length_sn2--);
				if (length_sn2 < 0) {
					FAIL_FRAME("s_name values needs to be increased");
				}

				strncat(values[i_ex_name], ",", length_en2--);
				if (length_en2 < 0) {
					FAIL_FRAME("ex_name values needs to be increased");
				}
			}

			tmp = SPI_getvalue(tuple, tupdesc, 1);
			strncat(values[i_trade_id], tmp, length_ti);
			length_ti -= strlen(tmp);
			if (length_ti < 0) {
				FAIL_FRAME("trade_id values needs to be increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 2);
			strncat(values[i_trade_dts], tmp, length_td);
			length_td -= strlen(tmp);
			if (length_td < 0) {
				FAIL_FRAME("trade_dts values needs to be increased");
			}

			strncat(values[i_status_name], "\"", length_sn--);
			if (length_sn < 0) {
				FAIL_FRAME("status_name values needs to be increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 3);
			strncat(values[i_status_name], tmp, length_sn);
			length_sn -= strlen(tmp);
			if (length_sn < 0) {
				FAIL_FRAME("status_name values needs to be increased");
			}

			strncat(values[i_status_name], "\"", length_sn--);
			if (length_sn < 0) {
				FAIL_FRAME("status_name values needs to be increased");
			}

			strncat(values[i_type_name], "\"", length_tn--);
			if (length_tn < 0) {
				FAIL_FRAME("type_name values needs to be increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 4);
			strncat(values[i_type_name], tmp, length_tn);
			length_tn -= strlen(tmp);
			if (length_tn < 0) {
				FAIL_FRAME("type_name values needs to be increased");
			}

			strncat(values[i_type_name], "\"", length_tn--);
			if (length_tn < 0) {
				FAIL_FRAME("type_name values needs to be increased");
			}

			strncat(values[i_symbol], "\"", length_s--);
			if (length_s < 0) {
				FAIL_FRAME("symbol values needs to be increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 5);
			strncat(values[i_symbol], tmp, length_s);
			length_s -= strlen(tmp);
			if (length_s < 0) {
				FAIL_FRAME("symbol values needs to be increased");
			}

			strncat(values[i_symbol], "\"", length_s--);
			if (length_s < 0) {
				FAIL_FRAME("symbol values needs to be increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 6);
			strncat(values[i_trade_qty], tmp, length_tq);
			length_tq -= strlen(tmp);
			if (length_tq < 0) {
				FAIL_FRAME("trade_qty values needs to be increased");
			}

			strncat(values[i_exec_name], "\"", length_en--);
			if (length_en < 0) {
				FAIL_FRAME("exec_name values needs to be increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 7);
			strncat(values[i_exec_name], tmp, length_en);
			length_en -= strlen(tmp);
			if (length_en < 0) {
				FAIL_FRAME("exec_name values needs to be increased");
			}

			strncat(values[i_exec_name], "\"", length_en--);
			if (length_en < 0) {
				FAIL_FRAME("exec_name values needs to be increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 8);
			strncat(values[i_charge], tmp, length_c);
			length_c -= strlen(tmp);
			if (length_c < 0) {
				FAIL_FRAME("charge values needs to be increased");
			}

			strncat(values[i_s_name], "\"", length_sn2--);
			if (length_sn2 < 0) {
				FAIL_FRAME("s_name values needs to be increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 9);
			strncat(values[i_s_name], tmp, length_sn2);
			length_sn2 -= strlen(tmp);
			if (length_sn2 < 0) {
				FAIL_FRAME("s_name values needs to be increased");
			}

			strncat(values[i_s_name], "\"", length_sn2--);
			if (length_sn2 < 0) {
				FAIL_FRAME("s_name values needs to be increased");
			}

			strncat(values[i_ex_name], "\"", length_en2--);
			if (length_en2 < 0) {
				FAIL_FRAME("ex_name values needs to be increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 10);
			strncat(values[i_ex_name], tmp, length_en2);
			length_en2 -= strlen(tmp);
			if (length_en2 < 0) {
				FAIL_FRAME("ex_name values needs to be increased");
			}

			strncat(values[i_ex_name], "\"", length_en2--);
			if (length_en2 < 0) {
				FAIL_FRAME("ex_name values needs to be increased");
			}
		}

		strncat(values[i_trade_id], "}", length_ti--);
		if (length_ti < 0) {
			FAIL_FRAME("trade_id values needs to be increased");
		}

		strncat(values[i_trade_dts], "}", length_td--);
		if (length_td < 0) {
			FAIL_FRAME("trade_dts values needs to be increased");
		}

		strncat(values[i_status_name], "}", length_sn--);
		if (length_sn < 0) {
			FAIL_FRAME("status_name values needs to be increased");
		}

		strncat(values[i_type_name], "}", length_tn--);
		if (length_tn < 0) {
			FAIL_FRAME("type_name values needs to be increased");
		}

		strncat(values[i_symbol], "}", length_s--);
		if (length_s < 0) {
			FAIL_FRAME("symbol values needs to be increased");
		}

		strncat(values[i_trade_qty], "}", length_tq--);
		if (length_tq < 0) {
			FAIL_FRAME("trade_qty values needs to be increased");
		}

		strncat(values[i_exec_name], "}", length_en--);
		if (length_en < 0) {
			FAIL_FRAME("exec_name values needs to be increased");
		}

		strncat(values[i_charge], "}", length_c--);
		if (length_c < 0) {
			FAIL_FRAME("charge values needs to be increased");
		}

		strncat(values[i_s_name], "}", length_sn2--);
		if (length_sn2 < 0) {
			FAIL_FRAME("s_name values needs to be increased");
		}

		strncat(values[i_ex_name], "}", length_en2--);
		if (length_en2 < 0) {
			FAIL_FRAME("ex_name values needs to be increased");
		}

#ifdef DEBUG
		elog(DEBUG1, "%s", SQLTSF1_2);
#endif /* DEBUG */
		ret = SPI_execute_plan(TSF1_2, args, nulls, true, 0);
		if (ret != SPI_OK_SELECT) {
			FAIL_FRAME_SET(&funcctx->max_calls, TSF1_statements[1].sql);
#ifdef DEBUG
			dump_tsf1_inputs(acct_id);
#endif /* DEBUG */
		}
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		if (SPI_processed > 0) {
			tuple = tuptable->vals[0];
			values[i_cust_l_name] = SPI_getvalue(tuple, tupdesc, 1);
			values[i_cust_f_name] = SPI_getvalue(tuple, tupdesc, 2);
			values[i_broker_name] = SPI_getvalue(tuple, tupdesc, 3);
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
		for (i = 0; i < 14; i++) {
			elog(DEBUG1, "TSF1 OUT: %d %s", i, values[i]);
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
