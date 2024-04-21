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
#include <postgres.h>
#include <fmgr.h>
#include <executor/spi.h> /* this should include most necessary APIs */
#include <executor/executor.h> /* for GetAttributeByName() */
#include <funcapi.h> /* for returning set of rows in order_status */
#include <utils/datetime.h>
#include <utils/lsyscache.h>
#include <utils/numeric.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <catalog/pg_type.h>

#include "frame.h"
#include "dbt5common.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define SQLTUF1_1                                                             \
	"SELECT t_exec_name\n"                                                    \
	"FROM trade\n"                                                            \
	"WHERE t_id = $1"

#define SQLTUF1_2a "SELECT replace($1, ' X ', ' ')"

#define SQLTUF1_2b "SELECT replace($1, ' ', ' X ')"

#define SQLTUF1_3                                                             \
	"UPDATE trade\n"                                                          \
	"SET t_exec_name = $1\n"                                                  \
	"WHERE t_id = $2"

#define SQLTUF1_4                                                             \
	"SELECT t_bid_price\n"                                                    \
	"     , t_exec_name\n"                                                    \
	"     , t_is_cash\n"                                                      \
	"     , tt_is_mrkt\n"                                                     \
	"     , t_trade_price\n"                                                  \
	"FROM trade\n"                                                            \
	"   , trade_type\n"                                                       \
	"WHERE t_id = $1\n"                                                       \
	"  AND t_tt_id = tt_id"

#define SQLTUF1_5                                                             \
	"SELECT se_amt\n"                                                         \
	"     , se_cash_due_date\n"                                               \
	"     , se_cash_type\n"                                                   \
	"FROM settlement\n"                                                       \
	"WHERE se_t_id = $1"

#define SQLTUF1_6                                                             \
	"SELECT ct_amt\n"                                                         \
	"     , ct_dts\n"                                                         \
	"     , ct_name\n"                                                        \
	"FROM cash_transaction\n"                                                 \
	"WHERE ct_t_id = $1"

#define SQLTUF1_7                                                             \
	"SELECT th_dts\n"                                                         \
	"     , th_st_id\n"                                                       \
	"FROM trade_history\n"                                                    \
	"WHERE th_t_id = $1\n"                                                    \
	"ORDER BY th_dts\n"                                                       \
	"LIMIT 3"

#define SQLTUF2_1                                                             \
	"SELECT t_bid_price\n"                                                    \
	"     , t_exec_name\n"                                                    \
	"     , t_is_cash\n"                                                      \
	"     , t_id\n"                                                           \
	"     , t_trade_price\n"                                                  \
	"FROM trade\n"                                                            \
	"WHERE t_ca_id = $1\n"                                                    \
	"  AND t_dts >= $2\n"                                                     \
	"  AND t_dts <= $3\n"                                                     \
	"ORDER BY t_dts ASC\n"                                                    \
	"LIMIT $4"

#define SQLTUF2_2                                                             \
	"SELECT se_cash_type\n"                                                   \
	"FROM settlement\n"                                                       \
	"WHERE se_t_id = $1"

#define SQLTUF2_3                                                             \
	"UPDATE settlement\n"                                                     \
	"SET se_cash_type = $1\n"                                                 \
	"WHERE se_t_id = $2"

#define SQLTUF2_4                                                             \
	"SELECT se_amt\n"                                                         \
	"     , se_cash_due_date\n"                                               \
	"     , se_cash_type\n"                                                   \
	"FROM settlement\n"                                                       \
	"WHERE se_t_id = $1"

#define SQLTUF2_5                                                             \
	"SELECT ct_amt\n"                                                         \
	"     , ct_dts\n"                                                         \
	"     , ct_name\n"                                                        \
	"FROM cash_transaction\n"                                                 \
	"WHERE ct_t_id = $1"

#define SQLTUF2_6                                                             \
	"SELECT th_dts\n"                                                         \
	"     , th_st_id\n"                                                       \
	"FROM trade_history\n"                                                    \
	"WHERE th_t_id = $1\n"                                                    \
	"ORDER BY th_dts\n"                                                       \
	"LIMIT 3"

#define SQLTUF3_1                                                             \
	"SELECT t_ca_id\n"                                                        \
	"     , t_exec_name\n"                                                    \
	"     , t_is_cash\n"                                                      \
	"     , t_trade_price\n"                                                  \
	"     , t_qty\n"                                                          \
	"     , s_name\n"                                                         \
	"     , t_dts\n"                                                          \
	"     , t_id\n"                                                           \
	"     , t_tt_id\n"                                                        \
	"     , tt_name\n"                                                        \
	"FROM trade\n"                                                            \
	"   , trade_type\n"                                                       \
	"   , security\n"                                                         \
	"WHERE t_s_symb = $1\n"                                                   \
	"  AND t_dts >= $2\n"                                                     \
	"  AND t_dts <= $3\n"                                                     \
	"  AND tt_id = t_tt_id\n"                                                 \
	"  AND s_symb = t_s_symb\n"                                               \
	"ORDER BY t_dts ASC\n"                                                    \
	"LIMIT $4"

#define SQLTUF3_2 SQLTUF2_4

#define SQLTUF3_3                                                             \
	"SELECT ct_name\n"                                                        \
	"FROM cash_transaction\n"                                                 \
	"WHERE ct_t_id = $1"

#define SQLTUF3_4                                                             \
	"UPDATE cash_transaction\n"                                               \
	"SET ct_name = $1\n"                                                      \
	"WHERE ct_t_id = $2"

#define SQLTUF3_5 SQLTUF2_5

#define SQLTUF3_6 SQLTUF2_6

#define TUF1_1 TUF1_statements[0].plan
#define TUF1_2a TUF1_statements[1].plan
#define TUF1_2b TUF1_statements[2].plan
#define TUF1_3 TUF1_statements[3].plan
#define TUF1_4 TUF1_statements[4].plan
#define TUF1_5 TUF1_statements[5].plan
#define TUF1_6 TUF1_statements[6].plan
#define TUF1_7 TUF1_statements[7].plan

#define TUF2_1 TUF2_statements[0].plan
#define TUF2_2 TUF2_statements[1].plan
#define TUF2_3 TUF2_statements[2].plan
#define TUF2_4 TUF2_statements[3].plan
#define TUF2_5 TUF2_statements[4].plan
#define TUF2_6 TUF2_statements[5].plan

#define TUF3_1 TUF3_statements[0].plan
#define TUF3_2 TUF3_statements[1].plan
#define TUF3_3 TUF3_statements[2].plan
#define TUF3_4 TUF3_statements[3].plan
#define TUF3_5 TUF3_statements[4].plan
#define TUF3_6 TUF3_statements[5].plan

static cached_statement TUF1_statements[] = {

	{ SQLTUF1_1, 1, { INT8OID } },

	{ SQLTUF1_2a, 1, { TEXTOID } },

	{ SQLTUF1_2b, 1, { TEXTOID } },

	{ SQLTUF1_3, 2, { TEXTOID, INT8OID } },

	{ SQLTUF1_4, 1, { INT8OID } },

	{ SQLTUF1_5, 1, { INT8OID } },

	{ SQLTUF1_6, 1, { INT8OID } },

	{ SQLTUF1_7, 1, { INT8OID } },

	{ NULL }
};

static cached_statement TUF2_statements[] = {

	{ SQLTUF2_1, 4, { INT8OID, TIMESTAMPOID, TIMESTAMPOID, INT4OID } },

	{ SQLTUF2_2, 1, { INT8OID } },

	{ SQLTUF2_3, 2, { TEXTOID, INT8OID } },

	{ SQLTUF2_4, 1, { INT8OID } },

	{ SQLTUF2_5, 1, { INT8OID } },

	{ SQLTUF2_6, 1, { INT8OID } },

	{ NULL }
};

static cached_statement TUF3_statements[] = {

	{ SQLTUF3_1, 4, { TEXTOID, TIMESTAMPOID, TIMESTAMPOID, INT4OID } },

	{ SQLTUF3_2, 1, { INT8OID } },

	{ SQLTUF3_3, 1, { INT8OID } },

	{ SQLTUF3_4, 2, { TEXTOID, INT8OID } },

	{ SQLTUF3_5, 1, { INT8OID } },

	{ SQLTUF3_6, 1, { INT8OID } },

	{ NULL }
};

/* Prototypes. */
#ifdef DEBUG
void dump_tuf1_inputs(int, int, long *);
void dump_tuf2_inputs(long, char *, int, int, char *);
void dump_tuf3_inputs(char *, long, int, int, char *, char *);
#endif /* DEBUG */

Datum TradeUpdateFrame1(PG_FUNCTION_ARGS);
Datum TradeUpdateFrame2(PG_FUNCTION_ARGS);
Datum TradeUpdateFrame3(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(TradeUpdateFrame1);
PG_FUNCTION_INFO_V1(TradeUpdateFrame2);
PG_FUNCTION_INFO_V1(TradeUpdateFrame3);

#ifdef DEBUG
void
dump_tuf1_inputs(int max_trades, int max_updates, long *trade_id)
{
	int i;

	elog(DEBUG1, "TUF1: INPUTS START");
	elog(DEBUG1, "TUF1: max_trades %d", max_trades);
	elog(DEBUG1, "TUF1: max_updates %d", max_updates);
	for (i = 0; i < max_trades; i++) {
		elog(DEBUG1, "TUF1: trade_id[%d] %ld", i, trade_id[i]);
	}
	elog(DEBUG1, "TUF1: INPUTS END");
}

void
dump_tuf2_inputs(long acct_id, char *end_trade_dts, int max_trades,
		int max_updates, char *start_trade_dts)
{
	elog(DEBUG1, "TUF2: INPUTS START");
	elog(DEBUG1, "TUF2: acct_id %ld", acct_id);
	elog(DEBUG1, "TUF2: end_trade_dts %s", end_trade_dts);
	elog(DEBUG1, "TUF2: max_trades %d", max_trades);
	elog(DEBUG1, "TUF2: max_updates %d", max_updates);
	elog(DEBUG1, "TUF2: start_trade_dts %s", start_trade_dts);
	elog(DEBUG1, "TUF2: INPUTS END");
}

void
dump_tuf3_inputs(char *end_trade_dts, long max_acct_id, int max_trades,
		int max_updates, char *start_trade_dts, char *symbol)
{
	elog(DEBUG1, "TUF3: INPUTS START");
	elog(DEBUG1, "TUF3: end_trade_dts %s", end_trade_dts);
	elog(DEBUG1, "TUF3: max_acct_id %ld", max_acct_id);
	elog(DEBUG1, "TUF3: max_trades %d", max_trades);
	elog(DEBUG1, "TUF3: max_updates %d", max_updates);
	elog(DEBUG1, "TUF3: start_trade_dts %s", start_trade_dts);
	elog(DEBUG1, "TUF3: symbol %s", symbol);
	elog(DEBUG1, "TUF3: INPUTS END");
}
#endif /* DEBUG */

/* Clause 3.3.9.3 */
Datum
TradeUpdateFrame1(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

	int i;
	int j;

	char **values = NULL;

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		enum tuf1
		{
			i_bid_price = 0,
			i_cash_transaction_amount,
			i_cash_transaction_dts,
			i_cash_transaction_name,
			i_exec_name,
			i_is_cash,
			i_is_market,
			i_num_found,
			i_num_updated,
			i_settlement_amount,
			i_settlement_cash_due_date,
			i_settlement_cash_type,
			i_trade_history_dts,
			i_trade_history_status_id,
			i_trade_price
		};

		int max_trades = PG_GETARG_INT32(0);
		int max_updates = PG_GETARG_INT32(1);
		ArrayType *trade_id_p = PG_GETARG_ARRAYTYPE_P(2);

		int16 typlen;
		bool typbyval;
		char typalign;

		long *trade_id;

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;
		Datum args[2];
		char nulls[2] = { ' ', ' ' };

		int num_found = max_trades;
		int num_updated = 0;
		int num_updated4 = 0;
		int num_updated5 = 0;
		int num_updated7 = 0;
		int num_cash = 0;

		char *tmp;
		int length_bp, length_cta, length_ctd, length_ctn, length_en,
				length_ic, length_im, length_sa, length_scdd, length_sct,
				length_thd, length_thsi, length_tp;

		get_typlenbyvalalign(
				ARR_ELEMTYPE(trade_id_p), &typlen, &typbyval, &typalign);
		trade_id = (long *) ARR_DATA_PTR(trade_id_p);

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 * Don't forget to factor in commas (,) and braces ({}) for the arrays.
		 */
		values = (char **) palloc(sizeof(char *) * 15);

		length_bp = (S_PRICE_T_LEN + 1) * 20 + 2;
		values[i_bid_price] = (char *) palloc(length_bp-- * sizeof(char));

		length_cta = ((VALUE_T_LEN + 1) * 20 + 2);
		values[i_cash_transaction_amount]
				= (char *) palloc(length_cta-- * sizeof(char));

		length_ctd = (MAXDATELEN + 1) * 20 + 2;
		values[i_cash_transaction_dts]
				= (char *) palloc(length_ctd-- * sizeof(char));

		length_ctn = (CT_NAME_LEN + 3) * 20 + 2;
		values[i_cash_transaction_name]
				= (char *) palloc(length_ctn-- * sizeof(char));

		length_en = (T_EXEC_NAME_LEN + 3) * 20 + 2;
		values[i_exec_name] = (char *) palloc(length_en-- * sizeof(char));

		length_ic = (SMALLINT_LEN + 1) * 20 + 2;
		values[i_is_cash] = (char *) palloc(length_ic-- * sizeof(char));

		length_im = (SMALLINT_LEN + 1) * 20 + 2;
		values[i_is_market] = (char *) palloc(length_im-- * sizeof(char));
		values[i_num_found]
				= (char *) palloc((INTEGER_LEN + 1) * sizeof(char));
		values[i_num_updated]
				= (char *) palloc((INTEGER_LEN + 1) * sizeof(char));

		length_sa = (VALUE_T_LEN + 1) * 20 + 2;
		values[i_settlement_amount]
				= (char *) palloc(length_sa-- * sizeof(char));

		length_scdd = (MAXDATELEN + 1) * 20 + 2;
		values[i_settlement_cash_due_date]
				= (char *) palloc(length_scdd-- * sizeof(char));

		length_sct = (SE_CASH_TYPE_LEN + 3) * 20 + 2;
		values[i_settlement_cash_type]
				= (char *) palloc(length_sct-- * sizeof(char));

		length_thd = (MAXDATELEN * 3 + 4) * 20 + 22;
		values[i_trade_history_dts]
				= (char *) palloc(length_thd-- * sizeof(char));

		length_thsi = ((ST_ID_LEN + 2) * 3 + 4) * 20 + 22;
		values[i_trade_history_status_id]
				= (char *) palloc(length_thsi-- * sizeof(char));

		length_tp = (S_PRICE_T_LEN + 1) * 20 + 2;
		values[i_trade_price] = (char *) palloc(length_tp-- * sizeof(char));

#ifdef DEBUG
		dump_tuf1_inputs(max_trades, max_updates, trade_id);
#endif

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();
		plan_queries(TUF1_statements);

		values[i_bid_price][0] = '{';
		values[i_bid_price][1] = '\0';

		values[i_exec_name][0] = '{';
		values[i_exec_name][1] = '\0';

		values[i_is_cash][0] = '{';
		values[i_is_cash][1] = '\0';

		values[i_is_market][0] = '{';
		values[i_is_market][1] = '\0';

		values[i_trade_price][0] = '{';
		values[i_trade_price][1] = '\0';

		values[i_settlement_amount][0] = '{';
		values[i_settlement_amount][1] = '\0';

		values[i_settlement_cash_due_date][0] = '{';
		values[i_settlement_cash_due_date][1] = '\0';

		values[i_settlement_cash_type][0] = '{';
		values[i_settlement_cash_type][1] = '\0';

		values[i_cash_transaction_amount][0] = '{';
		values[i_cash_transaction_amount][1] = '\0';

		values[i_cash_transaction_dts][0] = '{';
		values[i_cash_transaction_dts][1] = '\0';

		values[i_cash_transaction_name][0] = '{';
		values[i_cash_transaction_name][1] = '\0';

		values[i_trade_history_dts][0] = '{';
		values[i_trade_history_dts][1] = '\0';

		values[i_trade_history_status_id][0] = '{';
		values[i_trade_history_status_id][1] = '\0';

		for (i = 0; i < max_trades; i++) {
			char *is_cash_str = NULL;
			char *is_market_str;

			if (num_updated < max_updates) {
				char *ex_name;

#ifdef DEBUG
				elog(DEBUG1, "%s", SQLTUF1_1);
#endif /* DEBUG */
				args[0] = Int64GetDatum(trade_id[i]);
				ret = SPI_execute_plan(TUF1_1, args, nulls, true, 0);
				if (ret == SPI_OK_SELECT && SPI_processed > 0) {
					tupdesc = SPI_tuptable->tupdesc;
					tuptable = SPI_tuptable;
					tuple = tuptable->vals[0];
					ex_name = SPI_getvalue(tuple, tupdesc, 1);
				} else {
					continue;
				}

				args[0] = CStringGetTextDatum(ex_name);
				if (strstr(ex_name, " X ")) {
#ifdef DEBUG
					elog(DEBUG1, "%s", SQLTUF1_2a);
					elog(DEBUG1, "ex_name = %s", ex_name);
#endif /* DEBUG */
					ret = SPI_execute_plan(TUF1_2a, args, nulls, true, 0);
				} else {
#ifdef DEBUG
					elog(DEBUG1, "%s", SQLTUF1_2b);
					elog(DEBUG1, "ex_name = %s", ex_name);
#endif /* DEBUG */
					ret = SPI_execute_plan(TUF1_2b, args, nulls, true, 0);
				}
				if (ret == SPI_OK_SELECT && SPI_processed > 0) {
					tupdesc = SPI_tuptable->tupdesc;
					tuptable = SPI_tuptable;
					tuple = tuptable->vals[0];
					ex_name = SPI_getvalue(tuple, tupdesc, 1);
				} else {
					FAIL_FRAME_SET(&funcctx->max_calls,
							strstr(ex_name, " X ") ? TUF1_statements[1].sql
												   : TUF1_statements[2].sql);
#ifdef DEBUG
					dump_tuf1_inputs(max_trades, max_updates, trade_id);
#endif /* DEBUG */
					continue;
				}

#ifdef DEBUG
				elog(DEBUG1, "%s", SQLTUF1_3);
#endif /* DEBUG */
				args[1] = Int64GetDatum(trade_id[i]);
				ret = SPI_execute_plan(TUF1_3, args, nulls, false, 0);
				if (ret != SPI_OK_UPDATE) {
					FAIL_FRAME_SET(
							&funcctx->max_calls, TUF1_statements[4].sql);
#ifdef DEBUG
					dump_tuf1_inputs(max_trades, max_updates, trade_id);
#endif /* DEBUG */
					continue;
				}

				num_updated += SPI_processed;
			}

#ifdef DEBUG
			elog(DEBUG1, "%s", SQLTUF1_4);
#endif /* DEBUG */
			args[0] = Int64GetDatum(trade_id[i]);
			ret = SPI_execute_plan(TUF1_4, args, nulls, true, 0);
			if (ret != SPI_OK_SELECT) {
				FAIL_FRAME_SET(&funcctx->max_calls, TUF1_statements[5].sql);
#ifdef DEBUG
				dump_tuf1_inputs(max_trades, max_updates, trade_id);
#endif /* DEBUG */
				continue;
			}

			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			for (j = 0; j < SPI_processed; j++) {
				char *trade_price;
				if (num_updated4 > 0) {
					strncat(values[i_bid_price], ",", length_bp--);
					if (length_bp < 0) {
						FAIL_FRAME("bid_price values needs to be increased");
					}

					strncat(values[i_exec_name], ",", length_en--);
					if (length_en < 0) {
						FAIL_FRAME("exec_name values needs to be increased");
					}

					strncat(values[i_is_cash], ",", length_ic--);
					if (length_ic < 0) {
						FAIL_FRAME("is_cash values needs to be increased");
					}

					strncat(values[i_is_market], ",", length_im--);
					if (length_im < 0) {
						FAIL_FRAME("is_market values needs to be increased");
					}

					strncat(values[i_trade_price], ",", length_tp--);
					if (length_tp < 0) {
						FAIL_FRAME("trade_price values needs to be increased");
					}
				}

				tuple = tuptable->vals[j];

				tmp = SPI_getvalue(tuple, tupdesc, 1);
				strncat(values[i_bid_price], tmp, length_bp);
				length_bp -= strlen(tmp);
				if (length_bp < 0) {
					FAIL_FRAME("bid_price values needs to be increased");
				}

				strncat(values[i_exec_name], "\"", length_en--);
				if (length_en < 0) {
					FAIL_FRAME("exec_name values needs to be increased");
				}

				tmp = SPI_getvalue(tuple, tupdesc, 2);
				strncat(values[i_exec_name], tmp, length_en);
				length_en -= strlen(tmp);
				if (length_en < 0) {
					FAIL_FRAME("exec_name values needs to be increased");
				}

				strncat(values[i_exec_name], "\"", length_en--);
				if (length_en < 0) {
					FAIL_FRAME("exec_name values needs to be increased");
				}

				is_cash_str = SPI_getvalue(tuple, tupdesc, 3);
				if (is_cash_str[0] == 't') {
					values[i_is_cash][0] = '1';
				} else {
					values[i_is_cash][0] = '0';
				}
				values[i_is_cash][1] = '\0';
				if (--length_ic < 0) {
					FAIL_FRAME("is_cash values needs to be increased");
				}

				is_market_str = SPI_getvalue(tuple, tupdesc, 4);
				if (is_market_str[0] == 't') {
					values[i_is_market][0] = '1';
				} else {
					values[i_is_market][0] = '0';
				}
				values[i_is_market][1] = '\0';
				if (--length_im < 0) {
					FAIL_FRAME("is_market values needs to be increased");
				}

				trade_price = SPI_getvalue(tuple, tupdesc, 5);
				if (trade_price != NULL) {
					tmp = SPI_getvalue(tuple, tupdesc, 5);
					strncat(values[i_trade_price], tmp, length_tp);
					length_tp -= strlen(tmp);
					if (length_tp < 0) {
						FAIL_FRAME("trade_price values needs to be increased");
					}
				} else {
					strncat(values[i_trade_price], "NULL", length_tp);
					length_tp -= 4;
					if (length_tp < 0) {
						FAIL_FRAME("trade_price values needs to be increased");
					}
				}

				num_updated4++;
			}

#ifdef DEBUG
			elog(DEBUG1, "%s", SQLTUF1_5);
#endif /* DEBUG */
			ret = SPI_execute_plan(TUF1_5, args, nulls, true, 0);
			if (ret != SPI_OK_SELECT) {
				FAIL_FRAME_SET(&funcctx->max_calls, TUF1_statements[6].sql);
#ifdef DEBUG
				dump_tuf1_inputs(max_trades, max_updates, trade_id);
#endif /* DEBUG */
				continue;
			}

			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			for (j = 0; j < SPI_processed; j++) {
				if (num_updated5 > 0) {
					strncat(values[i_settlement_amount], ",", length_sa--);
					if (length_sa < 0) {
						FAIL_FRAME("settlement_amount values needs to be "
								   "increased");
					}

					strncat(values[i_settlement_cash_due_date], ",",
							length_scdd--);
					if (length_scdd < 0) {
						FAIL_FRAME("settlement_cash_due_date values needs to "
								   "be increased");
					}

					strncat(values[i_settlement_cash_type], ",", length_sct--);
					if (length_sct < 0) {
						FAIL_FRAME("settlement_cash_type values needs to be "
								   "increased");
					}
				}

				tuple = tuptable->vals[j];

				tmp = SPI_getvalue(tuple, tupdesc, 1);
				strncat(values[i_settlement_amount], tmp, length_sa);
				length_sa -= strlen(tmp);
				if (length_sa < 0) {
					FAIL_FRAME(
							"settlement_amount values needs to be increased");
				}

				tmp = SPI_getvalue(tuple, tupdesc, 2);
				strncat(values[i_settlement_cash_due_date], tmp, length_scdd);
				length_scdd -= strlen(tmp);
				if (length_scdd < 0) {
					FAIL_FRAME("settlement_cash_due_date values needs to be "
							   "increased");
				}

				strncat(values[i_settlement_cash_type], "\"", length_sct--);
				if (length_sct < 0) {
					FAIL_FRAME("settlement_cash_type values needs to be "
							   "increased");
				}

				tmp = SPI_getvalue(tuple, tupdesc, 3);
				strncat(values[i_settlement_cash_type], tmp, length_sct);
				length_sct -= strlen(tmp);
				if (length_sct < 0) {
					FAIL_FRAME("settlement_cash_type values needs to be "
							   "increased");
				}

				strncat(values[i_settlement_cash_type], "\"", length_sct--);
				if (length_sct < 0) {
					FAIL_FRAME("settlement_cash_type values needs to be "
							   "increased");
				}

				num_updated5++;
			}

			if (is_cash_str[0] == 't') {
				if (num_cash > 0) {
					strncat(values[i_cash_transaction_amount], ",",
							length_cta--);
					if (length_cta < 0) {
						FAIL_FRAME("cash_transaction_amount values needs to "
								   "be increased");
					}

					strncat(values[i_cash_transaction_dts], ",", length_ctd--);
					if (length_ctd < 0) {
						FAIL_FRAME("cash_transaction_dts values needs to be "
								   "increased");
					}

					strncat(values[i_cash_transaction_name], ",",
							length_ctn--);
					if (length_ctn < 0) {
						FAIL_FRAME("cash_transaction_name values needs to be "
								   "increased");
					}
				}

#ifdef DEBUG
				elog(DEBUG1, "%s", SQLTUF1_6);
#endif /* DEBUG */
				ret = SPI_execute_plan(TUF1_6, args, nulls, true, 0);
				if (ret == SPI_OK_SELECT && SPI_processed > 0) {
					tupdesc = SPI_tuptable->tupdesc;
					tuptable = SPI_tuptable;
					tuple = tuptable->vals[0];

					tmp = SPI_getvalue(tuple, tupdesc, 1);
					strncat(values[i_cash_transaction_amount], tmp,
							length_cta);
					length_cta -= strlen(tmp);
					if (length_cta < 0) {
						FAIL_FRAME("cash_transaction_amount values needs to "
								   "be increased");
					}

					tmp = SPI_getvalue(tuple, tupdesc, 2);
					strncat(values[i_cash_transaction_dts], tmp, length_ctd);
					length_ctd -= strlen(tmp);
					if (length_ctd < 0) {
						FAIL_FRAME("cash_transaction_dts values needs to be "
								   "increased");
					}

					strncat(values[i_cash_transaction_name], "\"",
							length_ctn--);
					if (length_ctn < 0) {
						FAIL_FRAME("cash_transaction_name values needs to be "
								   "increased");
					}

					tmp = SPI_getvalue(tuple, tupdesc, 3);
					strncat(values[i_cash_transaction_name], tmp, length_ctn);
					length_ctn -= strlen(tmp);
					if (length_ctn < 0) {
						FAIL_FRAME("cash_transaction_name values needs to be "
								   "increased");
					}

					strncat(values[i_cash_transaction_name], "\"",
							length_ctn--);
					if (length_ctn < 0) {
						FAIL_FRAME("cash_transaction_name values needs to be "
								   "increased");
					}

					++num_cash;
				} else {
					FAIL_FRAME_SET(
							&funcctx->max_calls, TUF1_statements[7].sql);
#ifdef DEBUG
					dump_tuf1_inputs(max_trades, max_updates, trade_id);
#endif /* DEBUG */
					continue;
				}
			}

#ifdef DEBUG
			elog(DEBUG1, "%s", SQLTUF1_7);
#endif /* DEBUG */
			ret = SPI_execute_plan(TUF1_7, args, nulls, true, 0);
			if (ret != SPI_OK_SELECT) {
				FAIL_FRAME_SET(&funcctx->max_calls, TUF1_statements[8].sql);
#ifdef DEBUG
				dump_tuf1_inputs(max_trades, max_updates, trade_id);
#endif /* DEBUG */
				continue;
			}
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;

			if (num_updated7 > 0) {
				strncat(values[i_trade_history_dts], ",", length_thd--);
				if (length_thd < 0) {
					FAIL_FRAME(
							"trade_history_dts values needs to be increased");
				}

				strncat(values[i_trade_history_status_id], ",", length_thsi--);
				if (length_thsi < 0) {
					FAIL_FRAME("trade_history_status_id values needs to be "
							   "increased");
				}
			}

			strncat(values[i_trade_history_dts], "{", length_thd--);
			if (length_thd < 0) {
				FAIL_FRAME("trade_history_dts values needs to be increased");
			}

			strncat(values[i_trade_history_status_id], "{", length_thsi--);
			if (length_thsi < 0) {
				FAIL_FRAME("trade_history_status_id values needs to be "
						   "increased");
			}

			for (j = 0; j < SPI_processed; j++) {
				if (j > 0) {
					strncat(values[i_trade_history_dts], ",", length_thd--);
					if (length_thd < 0) {
						FAIL_FRAME("trade_history_dts values needs to be "
								   "increased");
					}

					strncat(values[i_trade_history_status_id], ",",
							length_thsi--);
					if (length_thsi < 0) {
						FAIL_FRAME("trade_history_status_id values needs to "
								   "be increased");
					}
				}

				tuple = tuptable->vals[j];

				tmp = SPI_getvalue(tuple, tupdesc, 1);
				strncat(values[i_trade_history_dts], tmp, length_thd);
				length_thd -= strlen(tmp);
				if (length_thd < 0) {
					FAIL_FRAME(
							"trade_history_dts values needs to be increased");
				}

				strncat(values[i_trade_history_status_id], "\"",
						length_thsi--);
				if (length_thsi < 0) {
					FAIL_FRAME("trade_history_status_id values needs to be "
							   "increased");
				}

				tmp = SPI_getvalue(tuple, tupdesc, 2);
				strncat(values[i_trade_history_status_id], tmp, length_thsi);
				length_thsi -= strlen(tmp);
				if (length_thsi < 0) {
					FAIL_FRAME("trade_history_status_id values needs to be "
							   "increased");
				}

				strncat(values[i_trade_history_status_id], "\"",
						length_thsi--);
				if (length_thsi < 0) {
					FAIL_FRAME("trade_history_status_id values needs to be "
							   "increased");
				}

				num_updated7++;
			}
			for (j = SPI_processed; j < 3; j++) {
				if (j > 0) {
					strncat(values[i_trade_history_dts], ",", length_thd--);
					if (length_thd < 0) {
						FAIL_FRAME("trade_history_dts values needs to be "
								   "increased");
					}

					strncat(values[i_trade_history_status_id], ",",
							length_thsi--);
					if (length_thsi < 0) {
						FAIL_FRAME("trade_history_status_id values needs to "
								   "be increased");
					}
				}

				strncat(values[i_trade_history_dts], "NULL", length_thd);
				length_thd -= 4;
				if (length_thd < 0) {
					FAIL_FRAME(
							"trade_history_dts values needs to be increased");
				}

				strncat(values[i_trade_history_status_id], "\"\"",
						length_thsi);
				length_thsi -= 2;
				if (length_thsi < 0) {
					FAIL_FRAME("trade_history_status_id values needs to be "
							   "increased");
				}

				num_updated7++;
			}

			strncat(values[i_trade_history_dts], "}", length_thd--);
			if (length_thd < 0) {
				FAIL_FRAME("trade_history_dts values needs to be increased");
			}

			strncat(values[i_trade_history_status_id], "}", length_thsi--);
			if (length_thsi < 0) {
				FAIL_FRAME("trade_history_status_id values needs to be "
						   "increased");
			}
		}

		strncat(values[i_bid_price], "}", length_bp--);
		if (length_bp < 0) {
			FAIL_FRAME("bid_price values needs to be increased");
		}

		strncat(values[i_exec_name], "}", length_en--);
		if (length_en < 0) {
			FAIL_FRAME("exec_name values needs to be increased");
		}

		strncat(values[i_is_cash], "}", length_ic--);
		if (length_ic < 0) {
			FAIL_FRAME("is_cash values needs to be increased");
		}

		strncat(values[i_is_market], "}", length_im--);
		if (length_im < 0) {
			FAIL_FRAME("is_market values needs to be increased");
		}

		strncat(values[i_trade_price], "}", length_tp--);
		if (length_tp < 0) {
			FAIL_FRAME("trade_price values needs to be increased");
		}

		strncat(values[i_settlement_amount], "}", length_sa--);
		if (length_sa < 0) {
			FAIL_FRAME("settlement_amount values needs to be increased");
		}

		strncat(values[i_settlement_cash_due_date], "}", length_scdd--);
		if (length_scdd < 0) {
			FAIL_FRAME(
					"settlement_cash_due_date values needs to be increased");
		}

		strncat(values[i_settlement_cash_type], "}", length_sct--);
		if (length_sct < 0) {
			FAIL_FRAME("settlement_cash_type values needs to be increased");
		}

		strncat(values[i_cash_transaction_amount], "}", length_cta--);
		if (length_cta < 0) {
			FAIL_FRAME("cash_transaction_amount values needs to be increased");
		}

		strncat(values[i_cash_transaction_dts], "}", length_ctd--);
		if (length_ctd < 0) {
			FAIL_FRAME("cash_transaction_dts values needs to be increased");
		}

		strncat(values[i_cash_transaction_name], "}", length_ctn--);
		if (length_ctn < 0) {
			FAIL_FRAME("cash_transaction_name values needs to be increased");
		}

		strncat(values[i_trade_history_dts], "}", length_thd--);
		if (length_thd < 0) {
			FAIL_FRAME("trade_history_dts values needs to be increased");
		}

		strncat(values[i_trade_history_status_id], "}", length_thsi--);
		if (length_thsi < 0) {
			FAIL_FRAME("trade_history_status_id values needs to be increased");
		}

		snprintf(values[i_num_found], INTEGER_LEN, "%d", num_found);
		snprintf(values[i_num_updated], INTEGER_LEN, "%d", num_updated);

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
		for (i = 0; i < 15; i++) {
			elog(DEBUG1, "TUF1 OUT: %d %s", i, values[i]);
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

/* Clause 3.3.9.4 */
Datum
TradeUpdateFrame2(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

	int i;
	int j;

	char **values = NULL;

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		enum tuf2
		{
			i_bid_price = 0,
			i_cash_transaction_amount,
			i_cash_transaction_dts,
			i_cash_transaction_name,
			i_exec_name,
			i_is_cash,
			i_num_found,
			i_num_updated,
			i_settlement_amount,
			i_settlement_cash_due_date,
			i_settlement_cash_type,
			i_trade_history_dts,
			i_trade_history_status_id,
			i_trade_list,
			i_trade_price
		};

		long acct_id = PG_GETARG_INT64(0);
		Timestamp end_trade_dts_ts = PG_GETARG_TIMESTAMP(1);
		int max_trades = PG_GETARG_INT32(2);
		int max_updates = PG_GETARG_INT32(3);
		Timestamp start_trade_dts_ts = PG_GETARG_TIMESTAMP(4);

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;

		struct pg_tm tt, *tm = &tt;
		fsec_t fsec;
		char *tzn = NULL;
		Datum args[4];
		char nulls[4] = { ' ', ' ', ' ', ' ' };

		char end_trade_dts[MAXDATELEN + 1];
		char start_trade_dts[MAXDATELEN + 1];

		int num_found;
		int num_updated = 0;
		int num_cash = 0;

		char *tmp;
		int length_bp, length_cta, length_ctd, length_ctn, length_en,
				length_ic, length_sa, length_scdd, length_sct, length_thd,
				length_thsi, length_tl, length_tp;

		if (timestamp2tm(end_trade_dts_ts, NULL, tm, &fsec, NULL, NULL) == 0) {
			EncodeDateTimeM(tm, fsec, tzn, end_trade_dts);
		}
		if (timestamp2tm(start_trade_dts_ts, NULL, tm, &fsec, NULL, NULL)
				== 0) {
			EncodeDateTimeM(tm, fsec, tzn, start_trade_dts);
		}

#ifdef DEBUG
		dump_tuf2_inputs(acct_id, end_trade_dts, max_trades, max_updates,
				start_trade_dts);
#endif

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 * Don't forget to factor in commas (,) and braces ({}) for the arrays.
		 */
		values = (char **) palloc(sizeof(char *) * 15);

		length_bp = (S_PRICE_T_LEN + 1) * 20 + 2;
		values[i_bid_price] = (char *) palloc(length_bp-- * sizeof(char));

		length_cta = (VALUE_T_LEN + 1) * 20 + 2;
		values[i_cash_transaction_amount]
				= (char *) palloc(length_cta-- * sizeof(char));

		length_ctd = (MAXDATELEN + 1) * 20 + 2;
		values[i_cash_transaction_dts]
				= (char *) palloc(length_ctd-- * sizeof(char));

		length_ctn = (CT_NAME_LEN + 3) * 20 + 2;
		values[i_cash_transaction_name]
				= (char *) palloc(length_ctn-- * sizeof(char));

		length_en = (T_EXEC_NAME_LEN + 3) * 20 + 2;
		values[i_exec_name] = (char *) palloc(length_en-- * sizeof(char));

		length_ic = (SMALLINT_LEN + 1) * 20 + 2;
		values[i_is_cash] = (char *) palloc(length_ic-- * sizeof(char));

		values[i_num_found]
				= (char *) palloc((INTEGER_LEN + 1) * sizeof(char));
		values[i_num_updated]
				= (char *) palloc((INTEGER_LEN + 1) * sizeof(char));

		length_sa = (VALUE_T_LEN + 1) * 20 + 2;
		values[i_settlement_amount]
				= (char *) palloc(length_sa-- * sizeof(char));

		length_scdd = (MAXDATELEN + 1) * 20 + 2;
		values[i_settlement_cash_due_date]
				= (char *) palloc(length_scdd-- * sizeof(char));

		length_sct = (SE_CASH_TYPE_LEN + 3) * 20 + 2;
		values[i_settlement_cash_type]
				= (char *) palloc(length_sct-- * sizeof(char));

		length_thd = ((MAXDATELEN + 2) * 3 + 2) * 20 + 5;
		values[i_trade_history_dts]
				= (char *) palloc(length_thd-- * sizeof(char));

		length_thsi = ((ST_ID_LEN + 2) * 3 + 2) * 20 + 5;
		values[i_trade_history_status_id]
				= (char *) palloc(length_thsi-- * sizeof(char));

		length_tl = (BIGINT_LEN + 1) * 20 + 2;
		values[i_trade_list] = (char *) palloc(length_tl-- * sizeof(char));

		length_tp = (S_PRICE_T_LEN + 1) * 20 + 2;
		values[i_trade_price] = (char *) palloc(length_tp-- * sizeof(char));

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();
		plan_queries(TUF2_statements);
#ifdef DEBUG
		elog(DEBUG1, "%s", SQLTUF2_1);
#endif /* DEBUG */
		args[0] = Int64GetDatum(acct_id);
		args[1] = TimestampGetDatum(start_trade_dts_ts);
		args[2] = TimestampGetDatum(end_trade_dts_ts);
		args[3] = Int32GetDatum(max_trades);
		ret = SPI_execute_plan(TUF2_1, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
		}

		num_found = SPI_processed;

		strncpy(values[i_bid_price], "{", length_bp--);
		if (length_bp < 0) {
			FAIL_FRAME("bid_price values needs to be increased");
		}

		values[i_exec_name][0] = '{';
		values[i_exec_name][1] = '\0';

		values[i_is_cash][0] = '{';
		values[i_is_cash][1] = '\0';

		values[i_trade_list][0] = '{';
		values[i_trade_list][1] = '\0';

		values[i_trade_price][0] = '{';
		values[i_trade_price][1] = '\0';

		values[i_settlement_amount][0] = '{';
		values[i_settlement_amount][1] = '\0';

		values[i_settlement_cash_due_date][0] = '{';
		values[i_settlement_cash_due_date][1] = '\0';

		values[i_settlement_cash_type][0] = '{';
		values[i_settlement_cash_type][1] = '\0';

		values[i_cash_transaction_amount][0] = '{';
		values[i_cash_transaction_amount][1] = '\0';

		values[i_cash_transaction_dts][0] = '{';
		values[i_cash_transaction_dts][1] = '\0';

		values[i_cash_transaction_name][0] = '{';
		values[i_cash_transaction_name][1] = '\0';

		values[i_trade_history_dts][0] = '{';
		values[i_trade_history_dts][1] = '\0';

		values[i_trade_history_status_id][0] = '{';
		values[i_trade_history_status_id][1] = '\0';

		for (i = 0; i < num_found; i++) {
			TupleDesc l_tupdesc;
			SPITupleTable *l_tuptable = NULL;
			HeapTuple l_tuple = NULL;

			char *is_cash_str;
			char *trade_list;
			char cash_type[41];

			tuple = tuptable->vals[i];

			if (i > 0) {
				strncat(values[i_bid_price], ",", length_bp--);
				if (length_bp < 0) {
					FAIL_FRAME("bid_price values needs to be increased");
				}

				strncat(values[i_exec_name], ",", length_en--);
				if (length_en < 0) {
					FAIL_FRAME("exec_name values needs to be increased");
				}

				strncat(values[i_is_cash], ",", length_ic--);
				if (length_ic < 0) {
					FAIL_FRAME("is_cash values needs to be increased");
				}

				strncat(values[i_trade_list], ",", length_tl--);
				if (length_tl < 0) {
					FAIL_FRAME("trade_list values needs to be increased");
				}

				strncat(values[i_trade_price], ",", length_tp--);
				if (length_tp < 0) {
					FAIL_FRAME("trade_price values needs to be increased");
				}

				strncat(values[i_settlement_amount], ",", length_sa--);
				if (length_sa < 0) {
					FAIL_FRAME(
							"settlement_amount values needs to be increased");
				}

				strncat(values[i_settlement_cash_due_date], ",",
						length_scdd--);
				if (length_scdd < 0) {
					FAIL_FRAME("settlement_cash_due_date values needs to be "
							   "increased");
				}

				strncat(values[i_settlement_cash_type], ",", length_sct--);
				if (length_sct < 0) {
					FAIL_FRAME("settlement_cash_type values needs to be "
							   "increased");
				}

				strncat(values[i_trade_history_dts], ",", length_thd);
				if (length_thd < 0) {
					FAIL_FRAME(
							"trade_history_dts values needs to be increased");
				}

				strncat(values[i_trade_history_status_id], ",", length_thsi--);
				if (length_thsi < 0) {
					FAIL_FRAME("trade_history_status_id values needs to be "
							   "increased");
				}
			}

			tmp = SPI_getvalue(tuple, tupdesc, 1);
			strncat(values[i_bid_price], tmp, length_bp);
			length_bp -= strlen(tmp);
			if (length_bp < 0) {
				FAIL_FRAME("bid_price values needs to be increased");
			}

			strncat(values[i_exec_name], "\"", length_en--);
			if (length_en < 0) {
				FAIL_FRAME("exec_name values needs to be increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 2);
			strncat(values[i_exec_name], tmp, length_en);
			length_en -= strlen(tmp);
			if (length_en < 0) {
				FAIL_FRAME("exec_name values needs to be increased");
			}

			strncat(values[i_exec_name], "\"", length_en--);
			if (length_en < 0) {
				FAIL_FRAME("exec_name values needs to be increased");
			}

			is_cash_str = SPI_getvalue(tuple, tupdesc, 3);
			if (is_cash_str[0] == 't') {
				values[i_is_cash][0] = '1';
			} else {
				values[i_is_cash][0] = '0';
			}
			values[i_is_cash][1] = '\0';
			if (--length_ic < 0) {
				FAIL_FRAME("is_cash values needs to be increased");
			}

			trade_list = SPI_getvalue(tuple, tupdesc, 4);
			strncat(values[i_trade_list], trade_list, length_tl);
			length_tl -= strlen(trade_list);
			if (length_tl < 0) {
				FAIL_FRAME("trade_list values needs to be increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 5);
			strncat(values[i_trade_price], tmp, length_tp);
			length_tp -= strlen(tmp);
			if (length_tp < 0) {
				FAIL_FRAME("trade_price values needs to be increased");
			}

			if (num_updated < max_updates) {
				char *old_cash_type;

#ifdef DEBUG
				elog(DEBUG1, "%s", SQLTUF2_2);
#endif /* DEBUG */
				args[0] = Int64GetDatum(atoll(trade_list));
				ret = SPI_execute_plan(TUF2_2, args, nulls, true, 0);
				if (ret == SPI_OK_SELECT && SPI_processed > 0) {
					l_tupdesc = SPI_tuptable->tupdesc;
					l_tuptable = SPI_tuptable;
					l_tuple = l_tuptable->vals[0];
					old_cash_type = SPI_getvalue(l_tuple, l_tupdesc, 1);
				} else {
					FAIL_FRAME_SET(
							&funcctx->max_calls, TUF2_statements[1].sql);
#ifdef DEBUG
					dump_tuf2_inputs(acct_id, end_trade_dts, max_trades,
							max_updates, start_trade_dts);
#endif /* DEBUG */
					continue;
				}

#ifdef DEBUG
				elog(DEBUG1, "cash_type = '%s'", old_cash_type);
#endif /* DEBUG */

				if (is_cash_str[0] == 't') {
					if (strcmp(old_cash_type, "Cash Account") == 0) {
						strncpy(cash_type, "Cash", sizeof(cash_type));
					} else {
						strncpy(cash_type, "Cash Account", sizeof(cash_type));
					}
				} else {
					if (strcmp(old_cash_type, "Margin Account") == 0) {
						strncpy(cash_type, "Margin", sizeof(cash_type));
					} else {
						strncpy(cash_type, "Margin Account",
								sizeof(cash_type));
					}
				}

#ifdef DEBUG
				elog(DEBUG1, "%s", SQLTUF2_3);
#endif /* DEBUG */
				args[0] = CStringGetTextDatum(cash_type);
				args[1] = Int64GetDatum(atoll(trade_list));
				ret = SPI_execute_plan(TUF2_3, args, nulls, false, 0);
				if (ret != SPI_OK_UPDATE) {
					FAIL_FRAME_SET(
							&funcctx->max_calls, TUF2_statements[2].sql);
#ifdef DEBUG
					dump_tuf2_inputs(acct_id, end_trade_dts, max_trades,
							max_updates, start_trade_dts);
#endif /* DEBUG */
					continue;
				}
				num_updated += SPI_processed;
			}

#ifdef DEBUG
			elog(DEBUG1, "%s", SQLTUF2_4);
#endif /* DEBUG */
			args[0] = Int64GetDatum(atoll(trade_list));
			ret = SPI_execute_plan(TUF2_4, args, nulls, true, 0);
			if (ret == SPI_OK_SELECT && SPI_processed > 0) {
				l_tupdesc = SPI_tuptable->tupdesc;
				l_tuptable = SPI_tuptable;
				l_tuple = l_tuptable->vals[0];

				tmp = SPI_getvalue(l_tuple, l_tupdesc, 1);
				strncat(values[i_settlement_amount], tmp, length_sa);
				length_sa -= strlen(tmp);
				if (length_sa < 0) {
					FAIL_FRAME(
							"settlement_amount values needs to be increased");
				}

				tmp = SPI_getvalue(l_tuple, l_tupdesc, 2);
				strncat(values[i_settlement_cash_due_date], tmp, length_scdd);
				length_scdd -= strlen(tmp);
				if (length_scdd < 0) {
					FAIL_FRAME("settlement_cash_due_date values needs to be "
							   "increased");
				}

				strncat(values[i_settlement_cash_type], "\"", length_sct--);
				if (length_sct < 0) {
					FAIL_FRAME("settlement_cash_type values needs to be "
							   "increased");
				}

				tmp = SPI_getvalue(l_tuple, l_tupdesc, 3);
				strncat(values[i_settlement_cash_type], tmp, length_sct);
				length_sct -= strlen(tmp);
				if (length_sct < 0) {
					FAIL_FRAME("settlement_cash_type values needs to be "
							   "increased");
				}

				strncat(values[i_settlement_cash_type], "\"", length_sct--);
				if (length_sct < 0) {
					FAIL_FRAME("settlement_cash_type values needs to be "
							   "increased");
				}
			} else {
				FAIL_FRAME_SET(&funcctx->max_calls, TUF2_statements[3].sql);
#ifdef DEBUG
				dump_tuf2_inputs(acct_id, end_trade_dts, max_trades,
						max_updates, start_trade_dts);
#endif /* DEBUG */
				continue;
			}

			if (is_cash_str[0] == 't') {
#ifdef DEBUG
				elog(DEBUG1, "%s", SQLTUF2_5);
#endif /* DEBUG */
				ret = SPI_execute_plan(TUF2_5, args, nulls, true, 0);
				if (ret == SPI_OK_SELECT && SPI_processed > 0) {
					l_tupdesc = SPI_tuptable->tupdesc;
					l_tuptable = SPI_tuptable;
					l_tuple = l_tuptable->vals[0];
					if (num_cash > 0) {
						strncat(values[i_cash_transaction_amount], ",",
								length_cta--);
						if (length_cta < 0) {
							FAIL_FRAME("cash_transaction_amount values needs "
									   "to be increased");
						}

						strncat(values[i_cash_transaction_dts], ",",
								length_ctd--);
						if (length_ctd < 0) {
							FAIL_FRAME("cash_transaction_dts values needs to "
									   "be increased");
						}

						strncat(values[i_cash_transaction_name], ",",
								length_ctn--);
						if (length_ctn < 0) {
							FAIL_FRAME("cash_transaction_name values needs to "
									   "be increased");
						}
					}

					tmp = SPI_getvalue(l_tuple, l_tupdesc, 1);
					strncat(values[i_cash_transaction_amount], tmp,
							length_cta);
					length_cta -= strlen(tmp);
					if (length_cta < 0) {
						FAIL_FRAME("cash_transaction_amount values needs to "
								   "be increased");
					}

					tmp = SPI_getvalue(l_tuple, l_tupdesc, 2);
					strncat(values[i_cash_transaction_dts], tmp, length_ctd);
					length_ctd -= strlen(tmp);
					if (length_ctd < 0) {
						FAIL_FRAME("cash_transaction_dts values needs to be "
								   "increased");
					}

					strncat(values[i_cash_transaction_name], "\"",
							length_ctn--);
					if (length_ctn < 0) {
						FAIL_FRAME("cash_transaction_name values needs to be "
								   "increased");
					}

					tmp = SPI_getvalue(l_tuple, l_tupdesc, 3);
					strncat(values[i_cash_transaction_name], tmp, length_ctn);
					length_ctn -= strlen(tmp);
					if (length_ctn < 0) {
						FAIL_FRAME("cash_transaction_name values needs to be "
								   "increased");
					}

					strncat(values[i_cash_transaction_name], "\"",
							length_ctn--);
					if (length_ctn < 0) {
						FAIL_FRAME("cash_transaction_name values needs to be "
								   "increased");
					}

					++num_cash;
				} else {
					FAIL_FRAME_SET(
							&funcctx->max_calls, TUF2_statements[4].sql);
#ifdef DEBUG
					dump_tuf2_inputs(acct_id, end_trade_dts, max_trades,
							max_updates, start_trade_dts);
#endif /* DEBUG */
					continue;
				}
			} else {
				if (num_cash > 0) {
					strncat(values[i_cash_transaction_amount], ",",
							length_cta--);
					if (length_cta < 0) {
						FAIL_FRAME("cash_transaction_amount values needs to "
								   "be increased");
					}

					strncat(values[i_cash_transaction_dts], ",", length_ctd--);
					if (length_ctd < 0) {
						FAIL_FRAME("cash_transaction_dts values needs to be "
								   "increased");
					}

					strncat(values[i_cash_transaction_name], ",",
							length_ctn--);
					if (length_ctn < 0) {
						FAIL_FRAME("cash_transaction_name values needs to be "
								   "increased");
					}
				}
				strncat(values[i_cash_transaction_amount], "0", length_cta--);
				if (length_cta < 0) {
					FAIL_FRAME("cash_transaction_amount values needs to be "
							   "increased");
				}

				strncat(values[i_cash_transaction_dts], "1970-1-1 0:0:0",
						length_ctd);
				length_ctd -= 14;
				if (length_ctd < 0) {
					FAIL_FRAME("cash_transaction_dts values needs to be "
							   "increased");
				}

				strncat(values[i_cash_transaction_name], "\"\"", length_ctn);
				length_ctn -= 2;
				if (length_ctn < 0) {
					FAIL_FRAME("cash_transaction_name values needs to be "
							   "increased");
				}

				++num_cash;
			}

#ifdef DEBUG
			elog(DEBUG1, "%s", SQLTUF2_6);
#endif /* DEBUG */

			ret = SPI_execute_plan(TUF2_6, args, nulls, true, 0);
			if (ret == SPI_OK_SELECT && SPI_processed > 0) {
				l_tupdesc = SPI_tuptable->tupdesc;
				l_tuptable = SPI_tuptable;
			} else {
				FAIL_FRAME_SET(&funcctx->max_calls, TUF2_statements[5].sql);
#ifdef DEBUG
				dump_tuf2_inputs(acct_id, end_trade_dts, max_trades,
						max_updates, start_trade_dts);
#endif /* DEBUG */
				continue;
			}

			strncat(values[i_trade_history_dts], "{", length_thd--);
			if (length_thd < 0) {
				FAIL_FRAME("trade_history_dts values needs to be increased");
			}

			strncat(values[i_trade_history_status_id], "{", length_thsi--);
			if (length_thsi < 0) {
				FAIL_FRAME("trade_history_status_id values needs to be "
						   "increased");
			}

			for (j = 0; j < SPI_processed; j++) {
				if (j > 0) {
					strncat(values[i_trade_history_dts], ",", length_thd--);
					if (length_thd < 0) {
						FAIL_FRAME("trade_history_dts values needs to be "
								   "increased");
					}

					strncat(values[i_trade_history_status_id], ",",
							length_thsi--);
					if (length_thsi < 0) {
						FAIL_FRAME("trade_history_status_id values needs to "
								   "be increased");
					}
				}
				l_tuple = l_tuptable->vals[j];

				tmp = SPI_getvalue(l_tuple, l_tupdesc, 1);
				strncat(values[i_trade_history_dts], tmp, length_thd);
				length_thd -= strlen(tmp);
				if (length_thd < 0) {
					FAIL_FRAME(
							"trade_history_dts values needs to be increased");
				}

				strncat(values[i_trade_history_status_id], "\"",
						length_thsi--);
				if (length_thsi < 0) {
					FAIL_FRAME("trade_history_status_id values needs to be "
							   "increased");
				}

				tmp = SPI_getvalue(l_tuple, l_tupdesc, 2);
				strncat(values[i_trade_history_status_id], tmp, length_thsi);
				length_thsi -= strlen(tmp);
				if (length_thsi < 0) {
					FAIL_FRAME("trade_history_status_id values needs to be "
							   "increased");
				}

				strncat(values[i_trade_history_status_id], "\"",
						length_thsi--);
				if (length_thsi < 0) {
					FAIL_FRAME("trade_history_status_id values needs to be "
							   "increased");
				}
			}
			for (j = SPI_processed; j < 3; j++) {
				if (j > 0) {
					strncat(values[i_trade_history_dts], ",", length_thd--);
					if (length_thd < 0) {
						FAIL_FRAME("trade_history_dts values needs to be "
								   "increased");
					}

					strncat(values[i_trade_history_status_id], ",",
							length_thsi--);
					if (length_thsi < 0) {
						FAIL_FRAME("trade_history_status_id values needs to "
								   "be increased");
					}
				}
				strncat(values[i_trade_history_dts], "NULL", length_thd);
				length_thd -= 4;
				if (length_thd < 0) {
					FAIL_FRAME(
							"trade_history_dts values needs to be increased");
				}

				strncat(values[i_trade_history_status_id], "\"\"",
						length_thsi);
				length_thsi -= 2;
				if (length_thsi < 0) {
					FAIL_FRAME("trade_history_status_id values needs to be "
							   "increased");
				}
			}
			strncat(values[i_trade_history_dts], "}", length_thd--);
			if (length_thd < 0) {
				FAIL_FRAME("trade_history_dts values needs to be increased");
			}

			strncat(values[i_trade_history_status_id], "}", length_thsi--);
			if (length_thsi < 0) {
				FAIL_FRAME("trade_history_status_id values needs to be "
						   "increased");
			}
		}

		strncat(values[i_bid_price], "}", length_bp--);
		if (length_bp < 0) {
			FAIL_FRAME("bid_price values needs to be increased");
		}

		strncat(values[i_exec_name], "}", length_en--);
		if (length_en < 0) {
			FAIL_FRAME("exec_name values needs to be increased");
		}

		strncat(values[i_is_cash], "}", length_ic--);
		if (length_ic < 0) {
			FAIL_FRAME("is_cash values needs to be increased");
		}

		strncat(values[i_trade_list], "}", length_tl--);
		if (length_tl < 0) {
			FAIL_FRAME("trade_list values needs to be increased");
		}

		strncat(values[i_trade_price], "}", length_tp--);
		if (length_tp < 0) {
			FAIL_FRAME("trade_price values needs to be increased");
		}

		strncat(values[i_settlement_amount], "}", length_sa--);
		if (length_sa < 0) {
			FAIL_FRAME("settlement_amount values needs to be increased");
		}

		strncat(values[i_settlement_cash_due_date], "}", length_scdd--);
		if (length_scdd < 0) {
			FAIL_FRAME("settlement_cash_due_date values needs to be "
					   "increased");
		}

		strncat(values[i_settlement_cash_type], "}", length_sct--);
		if (length_sct < 0) {
			FAIL_FRAME("settlement_cash_type values needs to be increased");
		}

		strncat(values[i_cash_transaction_amount], "}", length_cta--);
		if (length_cta < 0) {
			FAIL_FRAME("cash_transaction_amount values needs to be increased");
		}

		strncat(values[i_cash_transaction_dts], "}", length_ctd--);
		if (length_ctd < 0) {
			FAIL_FRAME("cash_transaction_dts values needs to be increased");
		}

		strncat(values[i_cash_transaction_name], "}", length_ctn--);
		if (length_ctn < 0) {
			FAIL_FRAME("cash_transaction_name values needs to be increased");
		}

		strncat(values[i_trade_history_dts], "}", length_thd--);
		if (length_thd < 0) {
			FAIL_FRAME("trade_history_dts values needs to be increased");
		}

		strncat(values[i_trade_history_status_id], "}", length_thsi--);
		if (length_thsi < 0) {
			FAIL_FRAME("trade_history_status_id values needs to be increased");
		}

		snprintf(values[i_num_found], INTEGER_LEN, "%d", num_found);
		snprintf(values[i_num_updated], INTEGER_LEN, "%d", num_updated);

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
		for (i = 0; i < 15; i++) {
			elog(DEBUG1, "TUF2 OUT: %d %s", i, values[i]);
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

/* Clause 3.3.9.5 */
Datum
TradeUpdateFrame3(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

	int i;
	int j;
	int k;

	char **values = NULL;

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		enum tuf3
		{
			i_acct_id = 0,
			i_cash_transaction_amount,
			i_cash_transaction_dts,
			i_cash_transaction_name,
			i_exec_name,
			i_is_cash,
			i_num_found,
			i_num_updated,
			i_price,
			i_quantity,
			i_s_name,
			i_settlement_amount,
			i_settlement_cash_due_date,
			i_settlement_cash_type,
			i_trade_dts,
			i_trade_history_dts,
			i_trade_history_status_id,
			i_trade_list,
			i_type_name,
			i_trade_type
		};

		Timestamp end_trade_dts_ts = PG_GETARG_TIMESTAMP(0);
#ifdef DEBUG
		long max_acct_id = PG_GETARG_INT64(1);
#endif /* DEBUG */
		int max_trades = PG_GETARG_INT32(2);
		int max_updates = PG_GETARG_INT32(3);
		Timestamp start_trade_dts_ts = PG_GETARG_TIMESTAMP(4);
		char *symbol_p = (char *) PG_GETARG_TEXT_P(5);

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;

		struct pg_tm tt, *tm = &tt;
		fsec_t fsec;
		char *tzn = NULL;
		Datum args[4];
		char nulls[4] = { ' ', ' ', ' ', ' ' };

		char symbol[S_SYMB_LEN + 1];
		char end_trade_dts[MAXDATELEN + 1];
		char start_trade_dts[MAXDATELEN + 1];

		int num_found;
		int num_updated = 0;
		int num_cash = 0;

		char *tmp;
		int length_ai, length_ic, length_cta, length_ctd, length_ctn,
				length_en, length_p, length_q, length_sa, length_scdd,
				length_sct, length_sn, length_td, length_thd, length_thsi,
				length_tl, length_tn, length_tt;

		strncpy(symbol,
				DatumGetCString(DirectFunctionCall1(
						textout, PointerGetDatum(symbol_p))),
				sizeof(symbol));
		if (timestamp2tm(end_trade_dts_ts, NULL, tm, &fsec, NULL, NULL) == 0) {
			EncodeDateTimeM(tm, fsec, tzn, end_trade_dts);
		}
		if (timestamp2tm(start_trade_dts_ts, NULL, tm, &fsec, NULL, NULL)
				== 0) {
			EncodeDateTimeM(tm, fsec, tzn, start_trade_dts);
		}

#ifdef DEBUG
		dump_tuf3_inputs(end_trade_dts, max_acct_id, max_trades, max_updates,
				start_trade_dts, symbol);
#endif

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 * Don't forget to factor in commas (,) and braces ({}) for the arrays.
		 */
		values = (char **) palloc(sizeof(char *) * 20);

		length_ai = BIGINT_LEN * 20 + 22;
		values[i_acct_id] = (char *) palloc(length_ai-- * sizeof(char));

		length_cta = VALUE_T_LEN * 20 + 22;
		values[i_cash_transaction_amount]
				= (char *) palloc(length_cta-- * sizeof(char));

		length_ctd = MAXDATELEN * 20 + 22;
		values[i_cash_transaction_dts]
				= (char *) palloc(length_ctd-- * sizeof(char));

		length_ctn = (CT_NAME_LEN + 2) * 20 + 22;
		values[i_cash_transaction_name]
				= (char *) palloc(length_ctn-- * sizeof(char));

		length_en = (T_EXEC_NAME_LEN + 2) * 20 + 22;
		values[i_exec_name] = (char *) palloc(length_en-- * sizeof(char));

		length_ic = SMALLINT_LEN * 20 + 22;
		values[i_is_cash] = (char *) palloc(length_ic-- * sizeof(char));

		values[i_num_found]
				= (char *) palloc((INTEGER_LEN + 1) * sizeof(char));
		values[i_num_updated]
				= (char *) palloc((INTEGER_LEN + 1) * sizeof(char));

		length_p = S_PRICE_T_LEN * 20 + 22;
		values[i_price] = (char *) palloc(length_p-- * sizeof(char));

		length_q = INTEGER_LEN * 20 + 22;
		values[i_quantity] = (char *) palloc(length_q-- * sizeof(char));

		length_sn = (S_NAME_LEN + 2) * 20 + 22;
		values[i_s_name] = (char *) palloc(length_sn-- * sizeof(char));

		length_sa = VALUE_T_LEN * 20 + 22;
		values[i_settlement_amount]
				= (char *) palloc(length_sa-- * sizeof(char));

		length_scdd = MAXDATELEN * 20 + 22;
		values[i_settlement_cash_due_date]
				= (char *) palloc(length_scdd-- * sizeof(char));

		length_sct = (SE_CASH_TYPE_LEN + 2) * 20 + 22;
		values[i_settlement_cash_type]
				= (char *) palloc(length_sct-- * sizeof(char));

		length_td = (MAXDATELEN + 1) * 20 + 2;
		values[i_trade_dts] = (char *) palloc(length_td-- * sizeof(char));

		length_thd = (MAXDATELEN * 3 + 4) * 20 + 23;
		values[i_trade_history_dts]
				= (char *) palloc(length_thd-- * sizeof(char));

		length_thsi = ((ST_ID_LEN + 2) * 3 + 4) * 20 + 23;
		values[i_trade_history_status_id]
				= (char *) palloc(length_thsi-- * sizeof(char));

		length_tl = BIGINT_LEN * 20 + 22;
		values[i_trade_list] = (char *) palloc(length_tl-- * sizeof(char));

		length_tn = (TT_NAME_LEN + 2) * 20 + 2;
		values[i_type_name] = (char *) palloc(length_tn-- * sizeof(char));

		length_tt = (TT_ID_LEN + 2) * 20 + 22;
		values[i_trade_type] = (char *) palloc(length_tt-- * sizeof(char));

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();
		plan_queries(TUF3_statements);
#ifdef DEBUG
		elog(DEBUG1, "%s", SQLTUF3_1);
#endif /* DEBUG */
		args[0] = CStringGetTextDatum(symbol);
		args[1] = TimestampGetDatum(start_trade_dts_ts);
		args[2] = TimestampGetDatum(end_trade_dts_ts);
		args[3] = Int32GetDatum(max_trades);
		ret = SPI_execute_plan(TUF3_1, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
		}

		num_found = SPI_processed;

		values[i_acct_id][0] = '{';
		values[i_acct_id][1] = '\0';

		values[i_cash_transaction_amount][0] = '{';
		values[i_cash_transaction_amount][1] = '\0';

		values[i_cash_transaction_dts][0] = '{';
		values[i_cash_transaction_dts][1] = '\0';

		values[i_cash_transaction_name][0] = '{';
		values[i_cash_transaction_name][1] = '\0';

		values[i_exec_name][0] = '{';
		values[i_exec_name][1] = '\0';

		values[i_is_cash][0] = '{';
		values[i_is_cash][1] = '\0';

		values[i_price][0] = '{';
		values[i_price][1] = '\0';

		values[i_quantity][0] = '{';
		values[i_quantity][1] = '\0';

		values[i_s_name][0] = '{';
		values[i_s_name][1] = '\0';

		values[i_settlement_amount][0] = '{';
		values[i_settlement_amount][1] = '\0';

		values[i_settlement_cash_due_date][0] = '{';
		values[i_settlement_cash_due_date][1] = '\0';

		values[i_settlement_cash_type][0] = '{';
		values[i_settlement_cash_type][1] = '\0';

		values[i_trade_dts][0] = '{';
		values[i_trade_dts][1] = '\0';

		values[i_trade_history_dts][0] = '{';
		values[i_trade_history_dts][1] = '\0';

		values[i_trade_history_status_id][0] = '{';
		values[i_trade_history_status_id][1] = '\0';

		values[i_trade_list][0] = '{';
		values[i_trade_list][1] = '\0';

		values[i_type_name][0] = '{';
		values[i_type_name][1] = '\0';

		values[i_trade_type][0] = '{';
		values[i_trade_type][1] = '\0';

		for (i = 0; i < num_found; i++) {
			TupleDesc l_tupdesc;
			SPITupleTable *l_tuptable = NULL;
			HeapTuple l_tuple = NULL;

			char *is_cash_str;
			char *trade_list;
			char *old_ct_name;
			char *quantity;
			char *s_name;
			char *type_name;
			char ct_name[CT_NAME_LEN + 1];
			char ct_name_esc[2 * CT_NAME_LEN + 1];
			char *price;

			tuple = tuptable->vals[i];

			if (i > 0) {
				strncat(values[i_acct_id], ",", length_ai--);
				if (length_ai < 0) {
					FAIL_FRAME("acct_id values needs to be increased");
				}

				strncat(values[i_exec_name], ",", length_en--);
				if (length_en < 0) {
					FAIL_FRAME("exec_name values needs to be increased");
				}

				strncat(values[i_is_cash], ",", length_ic--);
				if (length_ic < 0) {
					FAIL_FRAME("is_cash values needs to be increased");
				}

				strncat(values[i_price], ",", length_p--);
				if (length_p < 0) {
					FAIL_FRAME("price values needs to be increased");
				}

				strncat(values[i_quantity], ",", length_q--);
				if (length_q < 0) {
					FAIL_FRAME("quantity values needs to be increased");
				}

				strncat(values[i_s_name], ",", length_sn--);
				if (length_sn < 0) {
					FAIL_FRAME("s_name values needs to be increased");
				}

				strncat(values[i_trade_dts], ",", length_td--);
				if (length_td < 0) {
					FAIL_FRAME("trade_dts values needs to be increased");
				}

				strncat(values[i_trade_list], ",", length_tl--);
				if (length_tl < 0) {
					FAIL_FRAME("trade_list values needs to be increased");
				}

				strncat(values[i_trade_type], ",", length_tt--);
				if (length_tt < 0) {
					FAIL_FRAME("trade_type values needs to be increased");
				}

				strncat(values[i_type_name], ",", length_tn--);
				if (length_tn < 0) {
					FAIL_FRAME("type_name values needs to be increased");
				}

				strncat(values[i_settlement_amount], ",", length_sa--);
				if (length_sa < 0) {
					FAIL_FRAME(
							"settlement_amount values needs to be increased");
				}

				strncat(values[i_settlement_cash_due_date], ",",
						length_scdd--);
				if (length_scdd < 0) {
					FAIL_FRAME("settlement_cash_due_date values needs to be "
							   "increased");
				}

				strncat(values[i_settlement_cash_type], ",", length_sct--);
				if (length_sct < 0) {
					FAIL_FRAME("settlement_cash_type values needs to be "
							   "increased");
				}

				strncat(values[i_trade_history_dts], ",", length_thd--);
				if (length_thd < 0) {
					FAIL_FRAME(
							"trade_history_dts values needs to be increased");
				}

				strncat(values[i_trade_history_status_id], ",", length_thsi--);
				if (length_thsi < 0) {
					FAIL_FRAME("trade_history_status_id values needs to be "
							   "increased");
				}
			}

			tmp = SPI_getvalue(tuple, tupdesc, 1);
			strncat(values[i_acct_id], tmp, length_ai);
			length_ai -= strlen(tmp);
			if (length_ai < 0) {
				FAIL_FRAME("acct_id values needs to be increased");
			}

			strncat(values[i_exec_name], "\"", length_en--);
			if (length_en < 0) {
				FAIL_FRAME("exec_name values needs to be increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 2);
			strncat(values[i_exec_name], tmp, length_en);
			length_en -= strlen(tmp);
			if (length_en < 0) {
				FAIL_FRAME("exec_name values needs to be increased");
			}

			strncat(values[i_exec_name], "\"", length_en--);
			if (length_en < 0) {
				FAIL_FRAME("exec_name values needs to be increased");
			}

			is_cash_str = SPI_getvalue(tuple, tupdesc, 3);
			if (is_cash_str[0] == 't') {
				values[i_is_cash][0] = '1';
			} else {
				values[i_is_cash][0] = '0';
			}
			values[i_is_cash][1] = '\0';
			if (--length_ic < 0) {
				FAIL_FRAME("is_cash values needs to be increased");
			}

			price = SPI_getvalue(tuple, tupdesc, 4);
			if (price == NULL) {
				strncat(values[i_price], "NULL", length_p);
				length_p -= 4;
			} else {
				strncat(values[i_price], price, length_p);
				length_p -= strlen(price);
			}
			if (length_p < 0) {
				FAIL_FRAME("price values needs to be increased");
			}

			quantity = SPI_getvalue(tuple, tupdesc, 5);
			strncat(values[i_quantity], quantity, length_q);
			length_q -= strlen(quantity);
			if (length_q < 0) {
				FAIL_FRAME("quantity values needs to be increased");
			}

			strncat(values[i_s_name], "\"", length_sn--);
			if (length_sn < 0) {
				FAIL_FRAME("s_name values needs to be increased");
			}

			s_name = SPI_getvalue(tuple, tupdesc, 6);
			strncat(values[i_s_name], s_name, length_sn);
			length_sn -= strlen(s_name);
			if (length_sn < 0) {
				FAIL_FRAME("s_name values needs to be increased");
			}

			strncat(values[i_s_name], "\"", length_sn--);
			if (length_sn < 0) {
				FAIL_FRAME("s_name values needs to be increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 7);
			strncat(values[i_trade_dts], tmp, length_td);
			length_td -= strlen(tmp);
			if (length_td < 0) {
				FAIL_FRAME("trade_dts values needs to be increased");
			}

			trade_list = SPI_getvalue(tuple, tupdesc, 8);
			strncat(values[i_trade_list], trade_list, length_tl);
			length_tl -= strlen(trade_list);
			if (length_tl < 0) {
				FAIL_FRAME("trade_list values needs to be increased");
			}

			strncat(values[i_trade_type], "\"", length_tt--);
			if (length_tt < 0) {
				FAIL_FRAME("trade_type values needs to be increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 9);
			strncat(values[i_trade_type], tmp, length_tt);
			length_tt -= strlen(tmp);
			if (length_tt < 0) {
				FAIL_FRAME("trade_type values needs to be increased");
			}

			strncat(values[i_trade_type], "\"", length_tt--);
			if (length_tt < 0) {
				FAIL_FRAME("trade_type values needs to be increased");
			}

			strncat(values[i_type_name], "\"", length_tn--);
			if (length_tn < 0) {
				FAIL_FRAME("type_name values needs to be increased");
			}

			type_name = SPI_getvalue(tuple, tupdesc, 10);
			strncat(values[i_type_name], type_name, length_tn);
			length_tn -= strlen(type_name);
			if (length_tn < 0) {
				FAIL_FRAME("type_name values needs to be increased");
			}

			strncat(values[i_type_name], "\"", length_tn--);
			if (length_tn < 0) {
				FAIL_FRAME("type_name values needs to be increased");
			}

#ifdef DEBUG
			elog(DEBUG1, "%s", SQLTUF3_2);
#endif /* DEBUG */
			args[0] = Int64GetDatum(atoll(trade_list));
			ret = SPI_execute_plan(TUF3_2, args, nulls, true, 0);
			if (ret == SPI_OK_SELECT && SPI_processed > 0) {
				l_tupdesc = SPI_tuptable->tupdesc;
				l_tuptable = SPI_tuptable;
				l_tuple = l_tuptable->vals[0];

				tmp = SPI_getvalue(l_tuple, l_tupdesc, 1);
				strncat(values[i_settlement_amount], tmp, length_sa);
				length_sa -= strlen(tmp);
				if (length_sa < 0) {
					FAIL_FRAME(
							"settlement_amount values needs to be increased");
				}

				tmp = SPI_getvalue(l_tuple, l_tupdesc, 2);
				strncat(values[i_settlement_cash_due_date], tmp, length_scdd);
				length_scdd -= strlen(tmp);
				if (length_scdd < 0) {
					FAIL_FRAME("settlement_cash_due_date values needs to be "
							   "increased");
				}

				strncat(values[i_settlement_cash_type], "\"", length_sct--);
				if (length_sct < 0) {
					FAIL_FRAME("settlement_cash_type values needs to be "
							   "increased");
				}

				tmp = SPI_getvalue(l_tuple, l_tupdesc, 3);
				strncat(values[i_settlement_cash_type], tmp, length_sct);
				length_sct -= strlen(tmp);
				if (length_sct < 0) {
					FAIL_FRAME("settlement_cash_type values needs to be "
							   "increased");
				}

				strncat(values[i_settlement_cash_type], "\"", length_sct--);
				if (length_sct < 0) {
					FAIL_FRAME("settlement_cash_type values needs to be "
							   "increased");
				}
			} else {
				FAIL_FRAME_SET(&funcctx->max_calls, TUF3_statements[1].sql);
#ifdef DEBUG
				dump_tuf3_inputs(end_trade_dts, max_acct_id, max_trades,
						max_updates, start_trade_dts, symbol);
#endif /* DEBUG */
				continue;
			}

			if (is_cash_str[0] == 't') {
				if (num_updated < max_updates) {
#ifdef DEBUG
					elog(DEBUG1, "%s", SQLTUF3_3);
#endif /* DEBUG */
					ret = SPI_execute_plan(TUF3_3, args, nulls, true, 0);
					if (ret == SPI_OK_SELECT && SPI_processed > 0) {
						l_tupdesc = SPI_tuptable->tupdesc;
						l_tuptable = SPI_tuptable;
						l_tuple = l_tuptable->vals[0];
						old_ct_name = SPI_getvalue(l_tuple, l_tupdesc, 1);
					} else {
						FAIL_FRAME_SET(
								&funcctx->max_calls, TUF3_statements[2].sql);
#ifdef DEBUG
						dump_tuf3_inputs(end_trade_dts, max_acct_id,
								max_trades, max_updates, start_trade_dts,
								symbol);
#endif /* DEBUG */
						continue;
					}

					if (strstr(old_ct_name, " shares of ")) {
						snprintf(ct_name, sizeof(ct_name),
								"%s %s Shares of %s", type_name, quantity,
								s_name);
					} else {
						snprintf(ct_name, sizeof(ct_name),
								"%s %s shares of %s", type_name, quantity,
								s_name);
					}
					for (j = 0, k = 0; j < CT_NAME_LEN && ct_name[j] != '\0';
							j++, k++) {
						if (ct_name[j] == '\'')
							ct_name_esc[k++] = '\\';
						ct_name_esc[k] = ct_name[j];
					}
					ct_name_esc[k] = '\0';

#ifdef DEBUG
					elog(DEBUG1, "%s", SQLTUF3_4);
#endif /* DEBUG */
					args[0] = CStringGetTextDatum(ct_name_esc);
					args[1] = Int64GetDatum(atoll(trade_list));
					ret = SPI_execute_plan(TUF3_4, args, nulls, false, 0);
					if (ret != SPI_OK_UPDATE) {
						FAIL_FRAME_SET(
								&funcctx->max_calls, TUF3_statements[3].sql);
#ifdef DEBUG
						dump_tuf3_inputs(end_trade_dts, max_acct_id,
								max_trades, max_updates, start_trade_dts,
								symbol);
#endif /* DEBUG */
						continue;
					}
					num_updated += SPI_processed;
				}

				if (num_cash > 0) {
					strncat(values[i_cash_transaction_amount], ",",
							length_cta--);
					if (length_cta < 0) {
						FAIL_FRAME("cash_transaction_amount values needs to "
								   "be increased");
					}

					strncat(values[i_cash_transaction_dts], ",", length_ctd--);
					if (length_ctd < 0) {
						FAIL_FRAME("cash_transaction_dts values needs to be "
								   "increased");
					}

					strncat(values[i_cash_transaction_name], ",",
							length_ctn--);
					if (length_ctn < 0) {
						FAIL_FRAME("cash_transaction_name values needs to be "
								   "increased");
					}
				}
#ifdef DEBUG
				elog(DEBUG1, "%s", SQLTUF3_5);
#endif /* DEBUG */
				args[0] = Int64GetDatum(atoll(trade_list));
				ret = SPI_execute_plan(TUF3_5, args, nulls, true, 0);
				if (ret == SPI_OK_SELECT && SPI_processed > 0) {
					l_tupdesc = SPI_tuptable->tupdesc;
					l_tuptable = SPI_tuptable;
					l_tuple = l_tuptable->vals[0];

					tmp = SPI_getvalue(l_tuple, l_tupdesc, 1);
					strncat(values[i_cash_transaction_amount], tmp,
							length_cta--);
					length_cta -= strlen(tmp);
					if (length_cta < 0) {
						FAIL_FRAME("cash_transaction_amount values needs to "
								   "be increased");
					}

					tmp = SPI_getvalue(l_tuple, l_tupdesc, 2);
					strncat(values[i_cash_transaction_dts], tmp, length_ctd);
					length_ctd -= strlen(tmp);
					if (length_ctd < 0) {
						FAIL_FRAME("cash_transaction_dts values needs to be "
								   "increased");
					}

					strncat(values[i_cash_transaction_name], "\"",
							length_ctn--);
					if (length_ctn < 0) {
						FAIL_FRAME("cash_transaction_name values needs to be "
								   "increased");
					}

					tmp = SPI_getvalue(l_tuple, l_tupdesc, 3);
					strncat(values[i_cash_transaction_name], tmp, length_ctn);
					length_ctn -= strlen(tmp);
					if (length_ctn < 0) {
						FAIL_FRAME("cash_transaction_name values needs to be "
								   "increased");
					}

					strncat(values[i_cash_transaction_name], "\"",
							length_ctn--);
					if (length_ctn < 0) {
						FAIL_FRAME("cash_transaction_name values needs to be "
								   "increased");
					}

					++num_cash;
				} else {
					FAIL_FRAME_SET(
							&funcctx->max_calls, TUF3_statements[4].sql);
#ifdef DEBUG
					dump_tuf3_inputs(end_trade_dts, max_acct_id, max_trades,
							max_updates, start_trade_dts, symbol);
#endif /* DEBUG */
					continue;
				}
			}

#ifdef DEBUG
			elog(DEBUG1, "%s", SQLTUF3_6);
#endif /* DEBUG */
			args[0] = Int64GetDatum(atoll(trade_list));
			ret = SPI_execute_plan(TUF3_6, args, nulls, true, 0);
			if (ret == SPI_OK_SELECT && SPI_processed > 0) {
				l_tupdesc = SPI_tuptable->tupdesc;
				l_tuptable = SPI_tuptable;
			} else {
				FAIL_FRAME_SET(&funcctx->max_calls, TUF3_statements[5].sql);
#ifdef DEBUG
				dump_tuf3_inputs(end_trade_dts, max_acct_id, max_trades,
						max_updates, start_trade_dts, symbol);
#endif /* DEBUG */
				continue;
			}

			strncat(values[i_trade_history_dts], "{", length_thd--);
			if (length_thd < 0) {
				FAIL_FRAME("trade_history_dts values needs to be increased");
			}

			strncat(values[i_trade_history_status_id], "{", length_thsi--);
			if (length_thsi < 0) {
				FAIL_FRAME("trade_history_status_id values needs to be "
						   "increased");
			}

			for (j = 0; j < 3; j++) {
				if (j > 0) {
					strncat(values[i_trade_history_dts], ",", length_thd--);
					if (length_thd < 0) {
						FAIL_FRAME("trade_history_dts values needs to be "
								   "increased");
					}

					strncat(values[i_trade_history_status_id], ",",
							length_thsi--);
					if (length_thsi < 0) {
						FAIL_FRAME("trade_history_status_id values needs to "
								   "be increased");
					}
				}
				if (j < SPI_processed) {
					l_tuple = l_tuptable->vals[j];

					tmp = SPI_getvalue(l_tuple, l_tupdesc, 1);
					strncat(values[i_trade_history_dts], tmp, length_thd);
					length_thd -= strlen(tmp);
					if (length_thd < 0) {
						FAIL_FRAME("trade_history_dts values needs to be "
								   "increased");
					}

					strncat(values[i_trade_history_status_id], "\"",
							length_thsi--);
					if (length_thsi < 0) {
						FAIL_FRAME("trade_history_status_id values needs to "
								   "be increased");
					}

					tmp = SPI_getvalue(l_tuple, l_tupdesc, 2);
					strncat(values[i_trade_history_status_id], tmp,
							length_thsi);
					length_thsi -= strlen(tmp);
					if (length_thsi < 0) {
						FAIL_FRAME("trade_history_status_id values needs to "
								   "be increased");
					}

					strncat(values[i_trade_history_status_id], "\"",
							length_thsi--);
					if (length_thsi < 0) {
						FAIL_FRAME("trade_history_status_id values needs to "
								   "be increased");
					}
				} else {
					strncat(values[i_trade_history_dts], "NULL", length_thd);
					length_thd -= 4;
					if (length_thd < 0) {
						FAIL_FRAME("trade_history_dts values needs to be "
								   "increased");
					}

					strncat(values[i_trade_history_status_id], "NULL",
							length_thsi);
					length_thsi -= 4;
					if (length_thsi < 0) {
						FAIL_FRAME("trade_history_status_id values needs to "
								   "be increased");
					}
				}
			}

			strncat(values[i_trade_history_dts], "}", length_thd--);
			if (length_thd < 0) {
				FAIL_FRAME("trade_history_dts values needs to be increased");
			}

			strncat(values[i_trade_history_status_id], "}", length_thsi--);
			if (length_thsi < 0) {
				FAIL_FRAME("trade_history_status_id values needs to be "
						   "increased");
			}
		}

		strncat(values[i_acct_id], "}", length_ai--);
		if (length_ai < 0) {
			FAIL_FRAME("acct_id values needs to be increased");
		}

		strncat(values[i_cash_transaction_amount], "}", length_cta--);
		if (length_cta < 0) {
			FAIL_FRAME("cash_transaction_amount values needs to be increased");
		}

		strncat(values[i_cash_transaction_dts], "}", length_ctd--);
		if (length_ctd < 0) {
			FAIL_FRAME("cash_transaction_dts values needs to be increased");
		}

		strncat(values[i_cash_transaction_name], "}", length_ctn--);
		if (length_ctn < 0) {
			FAIL_FRAME("cash_transaction_name values needs to be increased");
		}

		strncat(values[i_exec_name], "}", length_en--);
		if (length_en < 0) {
			FAIL_FRAME("exec_name values needs to be increased");
		}

		strncat(values[i_is_cash], "}", length_ic--);
		if (length_ic < 0) {
			FAIL_FRAME("is_cash values needs to be increased");
		}

		strncat(values[i_price], "}", length_p--);
		if (length_p < 0) {
			FAIL_FRAME("price values needs to be increased");
		}

		strncat(values[i_quantity], "}", length_q--);
		if (length_q < 0) {
			FAIL_FRAME("quantity values needs to be increased");
		}

		strncat(values[i_s_name], "}", length_sn--);
		if (length_sn < 0) {
			FAIL_FRAME("s_name values needs to be increased");
		}

		strncat(values[i_settlement_amount], "}", length_sa--);
		if (length_sa < 0) {
			FAIL_FRAME("settlement_amount values needs to be increased");
		}

		strncat(values[i_settlement_cash_due_date], "}", length_scdd--);
		if (length_scdd < 0) {
			FAIL_FRAME(
					"settlement_cash_due_date values needs to be increased");
		}

		strncat(values[i_settlement_cash_type], "}", length_sct--);
		if (length_sct < 0) {
			FAIL_FRAME("settlement_cash_type values needs to be "
					   "increased");
		}

		strncat(values[i_trade_dts], "}", length_td);
		if (length_td < 0) {
			FAIL_FRAME("trade_dts values needs to be increased");
		}

		strncat(values[i_trade_history_dts], "}", length_thd--);
		if (length_thd < 0) {
			FAIL_FRAME("trade_history_dts values needs to be increased");
		}

		strncat(values[i_trade_history_status_id], "}", length_thsi--);
		if (length_thsi < 0) {
			FAIL_FRAME("trade_history_status_id values needs to be increased");
		}

		strncat(values[i_trade_list], "}", length_tl--);
		if (length_tl < 0) {
			FAIL_FRAME("trade_list values needs to be increased");
		}

		strncat(values[i_type_name], "}", length_tn--);
		if (length_tn < 0) {
			FAIL_FRAME("type_name values needs to be increased");
		}

		strncat(values[i_trade_type], "}", length_tt--);
		if (length_tt < 0) {
			FAIL_FRAME("trade_type values needs to be increased");
		}

		snprintf(values[i_num_found], INTEGER_LEN, "%d", num_found);
		snprintf(values[i_num_updated], INTEGER_LEN, "%d", num_updated);

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
		for (i = 0; i < 20; i++) {
			elog(DEBUG1, "TUF3 OUT: %d %s", i, values[i]);
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
