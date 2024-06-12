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
#include <utils/lsyscache.h>
#include <utils/datetime.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/timestamp.h>
#include <catalog/pg_type.h>

#include "frame.h"
#include "dbt5common.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define USE_ISO_DATES 1
#define MAXDATEFIELDS 25
#define MYMAXDATELEN 63

#define SQLTLF1_1                                                             \
	"SELECT t_bid_price\n"                                                    \
	"     , t_exec_name\n"                                                    \
	"     , t_is_cash\n"                                                      \
	"     , tt_is_mrkt\n"                                                     \
	"     , t_trade_price\n"                                                  \
	"FROM trade\n"                                                            \
	"   , trade_type\n"                                                       \
	"WHERE t_id = $1\n"                                                       \
	"  AND t_tt_id = tt_id"

#define SQLTLF1_2                                                             \
	"SELECT se_amt\n"                                                         \
	"     , se_cash_due_date\n"                                               \
	"     , se_cash_type\n"                                                   \
	"FROM settlement\n"                                                       \
	"WHERE se_t_id = $1"

#define SQLTLF1_3                                                             \
	"SELECT ct_amt\n"                                                         \
	"     , ct_dts\n"                                                         \
	"     , ct_name\n"                                                        \
	"FROM cash_transaction\n"                                                 \
	"WHERE ct_t_id = $1"

#define SQLTLF1_4                                                             \
	"SELECT th_dts\n"                                                         \
	"     , th_st_id\n"                                                       \
	"FROM trade_history\n"                                                    \
	"WHERE th_t_id = $1\n"                                                    \
	"ORDER BY th_dts\n"                                                       \
	"LIMIT 3"

#define SQLTLF2_1                                                             \
	"SELECT t_bid_price\n"                                                    \
	"     , t_exec_name\n"                                                    \
	"     , t_is_cash\n"                                                      \
	"     , t_id\n"                                                           \
	"     , t_trade_price\n"                                                  \
	"FROM trade\n"                                                            \
	"WHERE t_ca_id = $1\n"                                                    \
	"  AND t_dts >= $2\n"                                                     \
	"  AND t_dts <= $3\n"                                                     \
	"ORDER BY t_dts\n"                                                        \
	"LIMIT $4"

#define SQLTLF2_2                                                             \
	"SELECT se_amt\n"                                                         \
	"     , se_cash_due_date\n"                                               \
	"     , se_cash_type\n"                                                   \
	"FROM settlement\n"                                                       \
	"WHERE se_t_id = $1"

#define SQLTLF2_3                                                             \
	"SELECT ct_amt\n"                                                         \
	"     , ct_dts\n"                                                         \
	"     , ct_name\n"                                                        \
	"FROM cash_transaction\n"                                                 \
	"WHERE ct_t_id = $1"

#define SQLTLF2_4                                                             \
	"SELECT th_dts\n"                                                         \
	"     , th_st_id\n"                                                       \
	"FROM trade_history\n"                                                    \
	"WHERE th_t_id = $1\n"                                                    \
	"ORDER BY th_dts\n"                                                       \
	"LIMIT 3"

#define SQLTLF3_1                                                             \
	"SELECT t_ca_id\n"                                                        \
	"     , t_exec_name\n"                                                    \
	"     , t_is_cash\n"                                                      \
	"     , t_trade_price\n"                                                  \
	"     , t_qty\n"                                                          \
	"     , t_dts\n"                                                          \
	"     , t_id\n"                                                           \
	"     , t_tt_id\n"                                                        \
	"FROM trade\n"                                                            \
	"WHERE t_s_symb = $1\n"                                                   \
	"  AND t_dts >= $2\n"                                                     \
	"  AND t_dts <= $3\n"                                                     \
	"ORDER BY t_dts ASC\n"                                                    \
	"LIMIT $4"

#define SQLTLF3_2                                                             \
	"SELECT se_amt\n"                                                         \
	"     , se_cash_due_date\n"                                               \
	"     , se_cash_type\n"                                                   \
	"FROM settlement\n"                                                       \
	"WHERE se_t_id = $1"

#define SQLTLF3_3                                                             \
	"SELECT ct_amt\n"                                                         \
	"     , ct_dts\n"                                                         \
	"     , ct_name\n"                                                        \
	"FROM cash_transaction\n"                                                 \
	"WHERE ct_t_id = $1"

#define SQLTLF3_4                                                             \
	"SELECT th_dts\n"                                                         \
	"     , th_st_id\n"                                                       \
	"FROM trade_history\n"                                                    \
	"WHERE th_t_id = $1\n"                                                    \
	"ORDER BY th_dts ASC\n"                                                   \
	"LIMIT 3"

#define SQLTLF4_1                                                             \
	"SELECT t_id\n"                                                           \
	"FROM trade\n"                                                            \
	"WHERE t_ca_id = $1\n"                                                    \
	"  AND t_dts >= $2\n"                                                     \
	"ORDER BY t_dts ASC\n"                                                    \
	"LIMIT 1"

#define SQLTLF4_2                                                             \
	"SELECT hh_h_t_id\n"                                                      \
	"     , hh_t_id\n"                                                        \
	"     , hh_before_qty\n"                                                  \
	"     , hh_after_qty\n"                                                   \
	"FROM holding_history\n"                                                  \
	"WHERE hh_h_t_id IN (\n"                                                  \
	"                       SELECT hh_h_t_id\n"                               \
	"                       FROM holding_history\n"                           \
	"                       WHERE hh_t_id = $1\n"                             \
	"                   )"

#define TLF1_1 TLF1_statements[0].plan
#define TLF1_2 TLF1_statements[1].plan
#define TLF1_3 TLF1_statements[2].plan
#define TLF1_4 TLF1_statements[3].plan

#define TLF2_1 TLF2_statements[0].plan
#define TLF2_2 TLF2_statements[1].plan
#define TLF2_3 TLF2_statements[2].plan
#define TLF2_4 TLF2_statements[3].plan

#define TLF3_1 TLF3_statements[0].plan
#define TLF3_2 TLF3_statements[1].plan
#define TLF3_3 TLF3_statements[2].plan
#define TLF3_4 TLF3_statements[3].plan

#define TLF4_1 TLF4_statements[0].plan
#define TLF4_2 TLF4_statements[1].plan

static cached_statement TLF1_statements[] = {

	{ SQLTLF1_1, 1, { INT8OID } },

	{ SQLTLF1_2, 1, { INT8OID } },

	{ SQLTLF1_3, 1, { INT8OID } },

	{ SQLTLF1_4, 1, { INT8OID } },

	{ NULL }
};

static cached_statement TLF2_statements[] = {

	{ SQLTLF2_1, 4, { INT8OID, TIMESTAMPOID, TIMESTAMPOID, INT4OID } },

	{ SQLTLF2_2, 1, { INT8OID } },

	{ SQLTLF2_3, 1, { INT8OID } },

	{ SQLTLF2_4, 1, { INT8OID } },

	{ NULL }
};

static cached_statement TLF3_statements[] = {

	{ SQLTLF3_1, 4, { TEXTOID, TIMESTAMPOID, TIMESTAMPOID, INT4OID } },

	{ SQLTLF3_2, 1, { INT8OID } },

	{ SQLTLF3_3, 1, { INT8OID } },

	{ SQLTLF3_4, 1, { INT8OID } },

	{ NULL }
};

static cached_statement TLF4_statements[] = {

	{ SQLTLF4_1, 2, { INT8OID, TIMESTAMPOID } },

	{ SQLTLF4_2, 1, { INT8OID } },

	{ NULL }
};

/* Prototypes to prevent potential gcc warnings. */
Datum TradeLookupFrame1(PG_FUNCTION_ARGS);
Datum TradeLookupFrame2(PG_FUNCTION_ARGS);
Datum TradeLookupFrame3(PG_FUNCTION_ARGS);
Datum TradeLookupFrame4(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(TradeLookupFrame1);
PG_FUNCTION_INFO_V1(TradeLookupFrame2);
PG_FUNCTION_INFO_V1(TradeLookupFrame3);
PG_FUNCTION_INFO_V1(TradeLookupFrame4);

#ifdef DEBUG
void dump_tlf1_inputs(int, ArrayType *);
void dump_tlf2_inputs(long, char *, int, char *);
void dump_tlf3_inputs(char *, long, int, char *, char *);
void dump_tlf4_inputs(long, char *);

void
dump_tlf1_inputs(int max_trades, ArrayType *trade_id_p)
{
	int ndim = ARR_NDIM(trade_id_p);
	int *dim = ARR_DIMS(trade_id_p);
	int nitems = ArrayGetNItems(ndim, dim);
	long *trade_id;

	int16 typlen;
	bool typbyval;
	char typalign;

	int i;

	get_typlenbyvalalign(
			ARR_ELEMTYPE(trade_id_p), &typlen, &typbyval, &typalign);
	trade_id = (long *) ARR_DATA_PTR(trade_id_p);

	elog(DEBUG1, "TLF1: INPUTS START");
	elog(DEBUG1, "TLF1: max_trades %d", max_trades);
	for (i = 0; i < nitems; i++) {
		elog(DEBUG1, "TLF1: trade_id[%d] %ld", i, trade_id[i]);
	}
	elog(DEBUG1, "TLF1: INPUTS END");
}

void
dump_tlf2_inputs(long acct_id, char *end_trade_dts, int max_trades,
		char *start_trade_dts)
{
	elog(DEBUG1, "TLF2: INPUTS START");
	elog(DEBUG1, "TLF2: acct_id = %ld", acct_id);
	elog(DEBUG1, "TLF2: end_trade_dts = %s", end_trade_dts);
	elog(DEBUG1, "TLF2: max_trades = %d", max_trades);
	elog(DEBUG1, "TLF2: start_trade_dts = %s", start_trade_dts);
	elog(DEBUG1, "TLF2: INPUTS END");
}

void
dump_tlf3_inputs(char *end_trade_dts, long max_acct_id, int max_trades,
		char *start_trade_dts, char *symbol)
{
	elog(DEBUG1, "TLF3: INPUTS START");
	elog(DEBUG1, "TLF3: end_trade_dts = %s", end_trade_dts);
	elog(DEBUG1, "TLF3: max_acct_id = %ld", max_acct_id);
	elog(DEBUG1, "TLF3: max_trades = %d", max_trades);
	elog(DEBUG1, "TLF3: start_trade_dts = %s", start_trade_dts);
	elog(DEBUG1, "TLF3: symbol = %s", symbol);
	elog(DEBUG1, "TLF3: INPUTS END");
}

void
dump_tlf4_inputs(long acct_id, char *start_trade_dts)
{
	elog(DEBUG1, "TLF4: INPUTS START");
	elog(DEBUG1, "TLF4: acct_id = %ld", acct_id);
	elog(DEBUG1, "TLF4: start_trade_dts = %s", start_trade_dts);
	elog(DEBUG1, "TLF4: INPUTS END");
}
#endif /* DEBUG */

/* Clause 3.3.6.3 */
Datum
TradeLookupFrame1(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

	int i, j;

	long *trade_id;

	char **values = NULL;

	enum tlf1
	{
		i_bid_price = 0,
		i_cash_transaction_amount,
		i_cash_transaction_dts,
		i_cash_transaction_name,
		i_exec_name,
		i_is_cash,
		i_is_market,
		i_num_found,
		i_settlement_amount,
		i_settlement_cash_due_date,
		i_settlement_cash_type,
		i_trade_history_dts,
		i_trade_history_status_id,
		i_trade_price
	};

	int num_found_count = 0;

	/* Helper counters to determine when to add commas. */
	int num_cash_txn = 0;
	int num_settlement = 0;
	int num_history = 0;

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		int max_trades = PG_GETARG_INT32(0);
		ArrayType *trade_id_p = PG_GETARG_ARRAYTYPE_P(1);

		int16 typlen;
		bool typbyval;
		char typalign;

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;
		Datum args[1];
		char nulls[1] = { ' ' };

		char *tmp;
		int length_bp, length_cta, length_ctd, length_ctn, length_en,
				length_ic, length_im, length_sa, length_scdd, length_sct,
				length_thd, length_thsi, length_tp;

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 14);

		length_bp = (S_PRICE_T_LEN + 1) * max_trades + 2;
		values[i_bid_price] = (char *) palloc(length_bp-- * sizeof(char));

		length_cta = (VALUE_T_LEN + 1) * max_trades + 2;
		values[i_cash_transaction_amount]
				= (char *) palloc(length_cta-- * sizeof(char));

		length_ctd = (MYMAXDATELEN + 3) * max_trades + 2;
		values[i_cash_transaction_dts]
				= (char *) palloc(length_ctd-- * sizeof(char));

		length_ctn = (CT_NAME_LEN + 3) * max_trades + 2;
		values[i_cash_transaction_name]
				= (char *) palloc(length_ctn-- * sizeof(char));

		length_en = (T_EXEC_NAME_LEN + 3) * max_trades + 2;
		values[i_exec_name] = (char *) palloc(length_en-- * sizeof(char));

		length_ic = (BOOLEAN_LEN + 1) * max_trades + 2;
		values[i_is_cash] = (char *) palloc(length_ic-- * sizeof(char));

		length_im = (BOOLEAN_LEN + 1) * max_trades + 3;
		values[i_is_market] = (char *) palloc(length_im-- * sizeof(char));

		values[i_num_found]
				= (char *) palloc((INTEGER_LEN + 1) * sizeof(char));

		length_sa = (VALUE_T_LEN + 1) * max_trades + 2;
		values[i_settlement_amount]
				= (char *) palloc(length_sa-- * sizeof(char));

		length_scdd = (MYMAXDATELEN + 3) * max_trades + 2;
		values[i_settlement_cash_due_date]
				= (char *) palloc(length_scdd-- * sizeof(char));

		length_sct = (SE_CASH_TYPE_LEN + 3) * max_trades + 2;
		values[i_settlement_cash_type]
				= (char *) palloc(length_sct-- * sizeof(char));

		length_thd = ((MYMAXDATELEN + 1) * max_trades + 3) * 3 + 2;
		values[i_trade_history_dts]
				= (char *) palloc(length_thd-- * sizeof(char));

		length_thsi = ((ST_ID_LEN + 3) * max_trades + 3) * 3 + 2;
		values[i_trade_history_status_id]
				= (char *) palloc(length_thsi-- * sizeof(char));

		length_tp = (S_PRICE_T_LEN + 1) * max_trades + 2;
		values[i_trade_price] = (char *) palloc(length_tp-- * sizeof(char));

		get_typlenbyvalalign(
				ARR_ELEMTYPE(trade_id_p), &typlen, &typbyval, &typalign);
		trade_id = (long *) ARR_DATA_PTR(trade_id_p);

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();
		plan_queries(TLF1_statements);
#ifdef DEBUG
		dump_tlf1_inputs(max_trades, trade_id_p);
#endif

		values[i_bid_price][0] = '{';
		values[i_bid_price][1] = '\0';

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

		values[i_is_market][0] = '{';
		values[i_is_market][1] = '\0';

		values[i_settlement_amount][0] = '{';
		values[i_settlement_amount][1] = '\0';

		values[i_settlement_cash_due_date][0] = '{';
		values[i_settlement_cash_due_date][1] = '\0';

		values[i_settlement_cash_type][0] = '{';
		values[i_settlement_cash_type][1] = '\0';

		values[i_trade_history_dts][0] = '{';
		values[i_trade_history_dts][1] = '\0';

		values[i_trade_history_status_id][0] = '{';
		values[i_trade_history_status_id][1] = '\0';

		values[i_trade_price][0] = '{';
		values[i_trade_price][1] = '\0';

		for (i = 0; i < max_trades; i++) {
			char *is_cash_str = NULL;

#ifdef DEBUG
			elog(DEBUG1, "%s", SQLTLF1_1);
#endif /* DEBUG */
			args[0] = Int64GetDatum(trade_id[i]);
			ret = SPI_execute_plan(TLF1_1, args, nulls, true, 0);
			if (ret != SPI_OK_SELECT) {
				FAIL_FRAME_SET(&funcctx->max_calls, TLF1_statements[0].sql);
				continue;
			}

			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			for (j = 0; j < SPI_processed; j++) {
				char *trade_price;
				if (num_found_count > 0) {
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
				length_bp -= strlen(tmp);
				strncat(values[i_bid_price], tmp, length_bp);
				if (length_bp < 0) {
					FAIL_FRAME("bid_price values needs to be increased");
				}

				strncat(values[i_exec_name], "\"", length_en--);
				if (length_en < 0) {
					FAIL_FRAME("exec_name values needs to be increased");
				}

				tmp = SPI_getvalue(tuple, tupdesc, 2);
				strncat(values[i_exec_name], tmp, length_en--);
				length_en -= strlen(tmp);
				if (length_en < 0) {
					FAIL_FRAME("exec_name values needs to be increased");
				}

				strncat(values[i_exec_name], "\"", length_en--);
				if (length_en < 0) {
					FAIL_FRAME("exec_name values needs to be increased");
				}

				/* Use the is_cash pointer, easier to reference later. */
				is_cash_str = SPI_getvalue(tuple, tupdesc, 3);
				if (is_cash_str[0] == 't') {
					strncat(values[i_is_cash], "1", length_ic--);
				} else {
					strncat(values[i_is_cash], "0", length_ic--);
				}
				if (length_ic < 0) {
					FAIL_FRAME("is_cash values needs to be increased");
				}

				tmp = SPI_getvalue(tuple, tupdesc, 4);
				if (tmp[0] == 't') {
					strncat(values[i_is_market], "1", length_im--);
				} else {
					strncat(values[i_is_market], "0", length_im--);
				}
				if (length_im < 0) {
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
#ifdef DEBUG
				elog(DEBUG1, "t_is_cash = %s", is_cash_str);
#endif /* DEBUG */

				++num_found_count;
			}

#ifdef DEBUG
			elog(DEBUG1, "%s", SQLTLF1_2);
#endif /* DEBUG */
			ret = SPI_execute_plan(TLF1_2, args, nulls, true, 0);
			if (ret != SPI_OK_SELECT) {
				FAIL_FRAME_SET(&funcctx->max_calls, TLF1_statements[1].sql);
			}

			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			for (j = 0; j < SPI_processed; j++) {
				if (num_settlement > 0) {
					strncat(values[i_settlement_amount], ",", length_sa--);
					if (length_sa < 0) {
						FAIL_FRAME("settlement_amount values needs to be "
								   "increased");
					}

					strncat(values[i_settlement_cash_due_date], ",",
							length_scdd--);
					if (length_scdd < 0) {
						FAIL_FRAME("settlement_cash_due_date values needs "
								   "to be increased");
					}

					strncat(values[i_settlement_cash_type], ",", length_sct--);
					if (length_sct < 0) {
						FAIL_FRAME("settlement_cash_type values needs to "
								   "be increased");
					}
				}

				tuple = tuptable->vals[j];

				tmp = SPI_getvalue(tuple, tupdesc, 1);
				strncat(values[i_settlement_amount], tmp, length_sa);
				length_sa -= strlen(tmp);
				if (length_sa < 0) {
					FAIL_FRAME("settlement_amount values needs to be "
							   "increased");
				}

				strncat(values[i_settlement_cash_due_date], "\"",
						length_scdd--);
				if (length_scdd < 0) {
					FAIL_FRAME("settlement_cash_due_date values needs to be "
							   "increased");
				}

				tmp = SPI_getvalue(tuple, tupdesc, 2);
				strncat(values[i_settlement_cash_due_date], tmp, length_scdd);
				length_scdd -= strlen(tmp);
				if (length_scdd < 0) {
					FAIL_FRAME("settlement_cash_due_date values needs to "
							   "be increased");
				}

				strncat(values[i_settlement_cash_due_date], "\"",
						length_scdd--);
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

				++num_settlement;
			}

			if (is_cash_str[0] == 't') {
#ifdef DEBUG
				elog(DEBUG1, "%s", SQLTLF1_3);
#endif /* DEBUG */
				ret = SPI_execute_plan(TLF1_3, args, nulls, true, 0);
				if (ret != SPI_OK_SELECT) {
					FAIL_FRAME_SET(
							&funcctx->max_calls, TLF1_statements[2].sql);
				}
				tupdesc = SPI_tuptable->tupdesc;
				tuptable = SPI_tuptable;

				for (j = 0; j < SPI_processed; j++) {
					if (num_cash_txn > 0) {
						strncat(values[i_cash_transaction_amount], ",",
								length_cta--);
						if (length_cta < 0) {
							FAIL_FRAME("cash_transaction_amount values "
									   "needs to be increased");
						}

						strncat(values[i_cash_transaction_dts], ",",
								length_ctd--);
						if (length_ctd < 0) {
							FAIL_FRAME("cash_transaction_dts values needs "
									   "to be increased");
						}

						strncat(values[i_cash_transaction_name], ",",
								length_ctn--);
						if (length_ctn < 0) {
							FAIL_FRAME("cash_transaction_name values "
									   "needs to be increased");
						}
					}
					tuple = tuptable->vals[j];

					strncat(values[i_cash_transaction_amount], "\"",
							length_cta--);
					if (length_ctd < 0) {
						FAIL_FRAME("cash_transaction_amount values needs to "
								   "be increased");
					}

					tmp = SPI_getvalue(tuple, tupdesc, 1);
					strncat(values[i_cash_transaction_amount], tmp,
							length_cta);
					length_cta -= strlen(tmp);
					if (length_cta < 0) {
						FAIL_FRAME("cash_transaction_amount values needs "
								   "to be increased");
					}

					strncat(values[i_cash_transaction_amount], "\"",
							length_cta--);
					if (length_ctd < 0) {
						FAIL_FRAME("cash_transaction_amount values needs to "
								   "be increased");
					}

					strncat(values[i_cash_transaction_dts], "\"",
							length_ctd--);
					if (length_ctd < 0) {
						FAIL_FRAME("cash_transaction_dts values needs to "
								   "be increased");
					}

					tmp = SPI_getvalue(tuple, tupdesc, 2);
					strncat(values[i_cash_transaction_dts], tmp, length_ctd);
					length_ctd -= strlen(tmp);
					if (length_ctd < 0) {
						FAIL_FRAME("cash_transaction_dts values needs to "
								   "be increased");
					}

					strncat(values[i_cash_transaction_dts], "\"",
							length_ctd--);
					if (length_ctd < 0) {
						FAIL_FRAME("cash_transaction_dts values needs to "
								   "be increased");
					}

					strncat(values[i_cash_transaction_name], "\"",
							length_ctn--);
					if (length_ctn < 0) {
						FAIL_FRAME("cash_transaction_name values needs to "
								   "be increased");
					}

					tmp = SPI_getvalue(tuple, tupdesc, 3);
					strncat(values[i_cash_transaction_name], tmp, length_ctn);
					length_ctn -= strlen(tmp);
					if (length_ctn < 0) {
						FAIL_FRAME("cash_transaction_name values needs to "
								   "be increased");
					}

					strncat(values[i_cash_transaction_name], "\"",
							length_ctn--);
					if (length_ctn < 0) {
						FAIL_FRAME("cash_transaction_name values needs to "
								   "be increased");
					}

					++num_cash_txn;
				}
			}

#ifdef DEBUG
			elog(DEBUG1, "%s", SQLTLF1_4);
#endif /* DEBUG */
			ret = SPI_execute_plan(TLF1_4, args, nulls, true, 0);
			if (ret != SPI_OK_SELECT) {
				FAIL_FRAME_SET(&funcctx->max_calls, TLF1_statements[3].sql);
			}
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;

			if (SPI_processed > 0) {
				if (num_history > 0) {
					strncat(values[i_trade_history_dts], ",", length_thd--);
					if (length_thd < 0) {
						FAIL_FRAME("trade_history_dts values needs to be "
								   "increased");
					}

					strncat(values[i_trade_history_status_id], ",",
							length_thsi--);
					if (length_thsi < 0) {
						FAIL_FRAME("trade_history_status_id values needs "
								   "to be increased");
					}
				}
			}

			for (j = 0; j < SPI_processed; j++) {
				tuple = tuptable->vals[j];

				if (j > 0) {
					strncat(values[i_trade_history_dts], ",", length_thd--);
					if (length_thd < 0) {
						FAIL_FRAME("trade_history_dts values needs to be "
								   "increased");
					}

					strncat(values[i_trade_history_status_id], ",",
							length_thsi--);
					if (length_thsi < 0) {
						FAIL_FRAME("trade_history_status_id values needs "
								   "to be increased");
					}
				}

				strncat(values[i_trade_history_dts], "\"", length_thd--);
				if (length_thd < 0) {
					FAIL_FRAME("trade_history_dts values needs to be "
							   "increased");
				}

				tmp = SPI_getvalue(tuple, tupdesc, 1);
				strncat(values[i_trade_history_dts], tmp, length_thd);
				length_thd -= strlen(tmp);
				if (length_thd < 0) {
					FAIL_FRAME("trade_history_dts values needs to be "
							   "increased");
				}

				strncat(values[i_trade_history_dts], "\"", length_thd--);
				if (length_thd < 0) {
					FAIL_FRAME("trade_history_dts values needs to be "
							   "increased");
				}

				strncat(values[i_trade_history_status_id], "\"", length_thsi);
				if (length_thsi < 0) {
					FAIL_FRAME("trade_history_status_id values needs "
							   "to be increased");
				}

				tmp = SPI_getvalue(tuple, tupdesc, 2);
				strncat(values[i_trade_history_status_id], tmp, length_thsi);
				length_thsi -= strlen(tmp);
				if (length_thsi < 0) {
					FAIL_FRAME("trade_history_status_id values needs "
							   "to be increased");
				}

				strncat(values[i_trade_history_status_id], "\"",
						length_thsi--);
				if (length_thsi < 0) {
					FAIL_FRAME("trade_history_status_id values needs "
							   "to be increased");
				}
			}

			/*
			 * FIXME: Can't have varying size multi-dimensional array.
			 * Since the spec says no more than three items here, pad
			 * the array up to 3 all the time.
			 */
			for (j = SPI_processed; j < 3; j++) {
				if (j > 0) {
					strncat(values[i_trade_history_dts], ",", length_thd--);
					if (length_thd < 0) {
						FAIL_FRAME("trade_history_dts values needs to "
								   "be increased");
					}

					strncat(values[i_trade_history_status_id], ",",
							length_thsi--);
					if (length_thsi < 0) {
						FAIL_FRAME("trade_history_status_id values "
								   "needs to be increased");
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
					FAIL_FRAME("trade_history_status_id values needs "
							   "to be increased");
				}
			}

			++num_history;
		}

		strncat(values[i_bid_price], "}", length_bp--);
		if (length_bp < 0) {
			FAIL_FRAME("bid_price values needs to be increased");
		}

		strncat(values[i_cash_transaction_amount], "}", length_cta--);
		if (length_cta < 0) {
			FAIL_FRAME("cash_transaction_amount values needs to be "
					   "increased");
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

		strncat(values[i_is_market], "}", length_im--);
		if (length_im < 0) {
			FAIL_FRAME("is_market values needs to be increased");
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

		strncat(values[i_trade_history_dts], "}", length_thd--);
		if (length_thd < 0) {
			FAIL_FRAME("trade_history_dts values needs to be increased");
		}

		strncat(values[i_trade_history_status_id], "}", length_thsi--);
		if (length_thsi < 0) {
			FAIL_FRAME("trade_history_status_id values needs to be "
					   "increased");
		}

		strncat(values[i_trade_price], "}", length_tp--);
		if (length_tp < 0) {
			FAIL_FRAME("trade_price values needs to be increased");
		}

		snprintf(values[i_num_found], INTEGER_LEN, "%d", num_found_count);

		/* Build a tuple descriptor for our result type */
		if (get_call_result_type(fcinfo, NULL, &tupdesc)
				!= TYPEFUNC_COMPOSITE) {
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								   errmsg("function returning record called "
										  "in context "
										  "that cannot accept type record")));
		}

		/*
		 * generate attribute metadata needed later to produce tuples from
		 * raw C strings
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
			elog(DEBUG1, "TLF1 OUT: %d %s", i, values[i]);
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

/* Clause 3.3.6.3 */
Datum
TradeLookupFrame2(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

	int i;

	char **values = NULL;

	enum tlf2
	{
		i_bid_price = 0,
		i_cash_transaction_amount,
		i_cash_transaction_dts,
		i_cash_transaction_name,
		i_exec_name,
		i_is_cash,
		i_num_found,
		i_settlement_amount,
		i_settlement_cash_due_date,
		i_settlement_cash_type,
		i_trade_history_dts,
		i_trade_history_status_id,
		i_trade_list,
		i_trade_price
	};

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		long acct_id = PG_GETARG_INT64(0);
		Timestamp end_trade_dts_ts = PG_GETARG_TIMESTAMP(1);
		int max_trades = PG_GETARG_INT32(2);
		Timestamp start_trade_dts_ts = PG_GETARG_TIMESTAMP(3);

		struct pg_tm tt, *tm = &tt;
		fsec_t fsec;
		char *tzn = NULL;
		char end_trade_dts[MYMAXDATELEN + 1];
		char start_trade_dts[MYMAXDATELEN + 1];
		Datum args[4];
		char nulls[4] = { ' ', ' ', ' ', ' ' };
		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;

		int num_found_count = 0;
		int num_settlements = 0;
		int num_cash_txn = 0;
		int num_trade_history = 0;

		char *is_cash_str;

		int j;
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
		dump_tlf2_inputs(acct_id, end_trade_dts, max_trades, start_trade_dts);
#endif /* DEBUG */

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 14);

		length_bp = (S_PRICE_T_LEN + 1) * 20 + 2;
		values[i_bid_price] = (char *) palloc(length_bp-- * sizeof(char));

		length_cta = (VALUE_T_LEN + 1) * 20 + 2;
		values[i_cash_transaction_amount]
				= (char *) palloc(length_cta-- * sizeof(char));

		length_ctd = (MYMAXDATELEN + 1) * 20 + 2;
		values[i_cash_transaction_dts]
				= (char *) palloc(length_ctd-- * sizeof(char));

		length_ctn = (CT_NAME_LEN + 3) * 20 + 2;
		values[i_cash_transaction_name]
				= (char *) palloc(length_ctn-- * sizeof(char));

		length_en = (T_EXEC_NAME_LEN + 1) * 20 + 3;
		values[i_exec_name] = (char *) palloc(length_en-- * sizeof(char));

		length_ic = (BOOLEAN_LEN + 1) * 20 + 2;
		values[i_is_cash] = (char *) palloc(length_ic-- * sizeof(char));

		values[i_num_found]
				= (char *) palloc((INTEGER_LEN + 1) * sizeof(char));

		length_sa = (VALUE_T_LEN + 1) * 20 + 2;
		values[i_settlement_amount]
				= (char *) palloc(length_sa-- * sizeof(char));

		length_scdd = (MYMAXDATELEN + 1) * 20 + 2;
		values[i_settlement_cash_due_date]
				= (char *) palloc(length_scdd-- * sizeof(char));

		length_sct = (SE_CASH_TYPE_LEN + 1) * 20 + 2;
		values[i_settlement_cash_type]
				= (char *) palloc(length_sct-- * sizeof(char));

		length_thd = (MYMAXDATELEN + 1) * 60 + 2;
		values[i_trade_history_dts]
				= (char *) palloc(length_thd-- * sizeof(char));

		length_thsi = (ST_ID_LEN + 1) * 60 + 2;
		values[i_trade_history_status_id]
				= (char *) palloc(length_thsi-- * sizeof(char));

		length_tl = (TRADE_T_LEN + 1) * 20 + 2;
		values[i_trade_list] = (char *) palloc(length_tl-- * sizeof(char));

		length_tp = (S_PRICE_T_LEN + 1) * 20 + 2;
		values[i_trade_price] = (char *) palloc(length_tp-- * sizeof(char));

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;

		/* switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();
		plan_queries(TLF2_statements);

#ifdef DEBUG
		elog(DEBUG1, "%s", SQLTLF2_1);
#endif /* DEBUG */
		args[0] = Int64GetDatum(acct_id);
		args[1] = TimestampGetDatum(start_trade_dts_ts);
		args[2] = TimestampGetDatum(end_trade_dts_ts);
		args[3] = Int32GetDatum(max_trades);
		ret = SPI_execute_plan(TLF2_1, args, nulls, true, 0);
		if (ret != SPI_OK_SELECT) {
#ifdef DEBUG
			dump_tlf2_inputs(
					acct_id, end_trade_dts, max_trades, start_trade_dts);
#endif /* DEBUG */
			FAIL_FRAME_SET(&funcctx->max_calls, TLF2_statements[0].sql);
		}
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		num_found_count = SPI_processed;
		snprintf(values[i_num_found], INTEGER_LEN, "%d", num_found_count);

#ifdef DEBUG
		elog(DEBUG1, "num_found = %d", num_found_count);
#endif /* DEBUG */

		values[i_bid_price][0] = '{';
		values[i_bid_price][1] = '\0';

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

		values[i_settlement_amount][0] = '{';
		values[i_settlement_amount][1] = '\0';

		values[i_settlement_cash_due_date][0] = '{';
		values[i_settlement_cash_due_date][1] = '\0';

		values[i_settlement_cash_type][0] = '{';
		values[i_settlement_cash_type][1] = '\0';

		values[i_trade_history_dts][0] = '{';
		values[i_trade_history_dts][1] = '\0';

		values[i_trade_history_status_id][0] = '{';
		values[i_trade_history_status_id][1] = '\0';

		values[i_trade_list][0] = '{';
		values[i_trade_list][1] = '\0';

		values[i_trade_price][0] = '{';
		values[i_trade_price][1] = '\0';

		for (i = 0; i < num_found_count; i++) {
			char *tmp;
			TupleDesc tupdesc2;
			SPITupleTable *tuptable2 = NULL;
			HeapTuple tuple2 = NULL;

			char *trade_list_str;

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
			strncat(values[i_exec_name], tmp, length_en--);
			length_en -= strlen(tmp);
			if (length_en < 0) {
				FAIL_FRAME("exec_name values needs to be increased");
			}

			strncat(values[i_exec_name], "\"", length_en--);
			if (length_en < 0) {
				FAIL_FRAME("exec_name values needs to be increased");
			}

			is_cash_str = SPI_getvalue(tuple, tupdesc, 3);
			strncat(values[i_is_cash], (is_cash_str[0] == 't' ? "0" : "1"),
					length_ic--);
			if (length_ic < 0) {
				FAIL_FRAME("is_cash values needs to be increased");
			}

			trade_list_str = SPI_getvalue(tuple, tupdesc, 4);
			strncat(values[i_trade_list], trade_list_str, length_tl);
			length_tl -= strlen(trade_list_str);
			if (length_tl < 0) {
				FAIL_FRAME("trade_list values needs to be increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 5);
			strncat(values[i_trade_price], tmp, length_tp);
			length_tp -= strlen(tmp);
			if (length_tp < 0) {
				FAIL_FRAME("trade_price values needs to be increased");
			}

#ifdef DEBUG
			elog(DEBUG1, "%s", SQLTLF2_2);
#endif /* DEBUG */
			args[0] = Int64GetDatum(atoll(trade_list_str));
			ret = SPI_execute_plan(TLF2_2, args, nulls, true, 0);
			if (ret != SPI_OK_SELECT) {
#ifdef DEBUG
				dump_tlf2_inputs(
						acct_id, end_trade_dts, max_trades, start_trade_dts);
#endif /* DEBUG */
				FAIL_FRAME_SET(&funcctx->max_calls, TLF2_statements[1].sql);
				continue;
			}
			for (j = 0; j < SPI_processed; j++) {
				tupdesc2 = SPI_tuptable->tupdesc;
				tuptable2 = SPI_tuptable;
				++num_settlements;

				if (num_settlements > 1) {
					strncat(values[i_settlement_amount], ",", length_sa--);
					if (length_sa < 0) {
						FAIL_FRAME("settlement_amount values needs to be "
								   "increased");
					}

					strncat(values[i_settlement_cash_due_date], ",",
							length_scdd--);
					if (length_scdd < 0) {
						FAIL_FRAME(
								"settlement_cash_due_date values needs to be "
								"increased");
					}

					strncat(values[i_settlement_cash_type], ",", length_sct--);
					if (length_sct < 0) {
						FAIL_FRAME("settlement_cash_type values needs to be "
								   "increased");
					}
				}

				tuple2 = tuptable2->vals[j];

				tmp = SPI_getvalue(tuple2, tupdesc2, 1);
				strncat(values[i_settlement_amount], tmp, length_sa);
				length_sa -= strlen(tmp);
				if (length_sa < 0) {
					FAIL_FRAME(
							"settlement_amount values needs to be increased");
				}

				tmp = SPI_getvalue(tuple2, tupdesc2, 2);
				strncat(values[i_settlement_cash_due_date], tmp, length_scdd);
				length_scdd -= strlen(tmp);
				if (length_scdd < 0) {
					FAIL_FRAME("settlement_cash_due_date values needs to be "
							   "increased");
				}

				tmp = SPI_getvalue(tuple2, tupdesc2, 3);
				strncat(values[i_settlement_cash_type], tmp, length_sct);
				length_sct -= strlen(tmp);
				if (length_sct < 0) {
					FAIL_FRAME("settlement_cash_type values needs to be "
							   "increased");
				}
			}

			if (is_cash_str[0] == 't') {
#ifdef DEBUG
				elog(DEBUG1, "%s", SQLTLF2_3);
#endif /* DEBUG */
				ret = SPI_execute_plan(TLF2_3, args, nulls, true, 0);
				if (ret != SPI_OK_SELECT) {
#ifdef DEBUG
					dump_tlf2_inputs(acct_id, end_trade_dts, max_trades,
							start_trade_dts);
#endif /* DEBUG */
					FAIL_FRAME_SET(
							&funcctx->max_calls, TLF2_statements[2].sql);
					continue;
				}
				tupdesc2 = SPI_tuptable->tupdesc;
				tuptable2 = SPI_tuptable;
				for (j = 0; j < SPI_processed; j++) {
					tuple2 = tuptable2->vals[j];
					++num_cash_txn;

					if (num_cash_txn > 1) {
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

					tmp = SPI_getvalue(tuple2, tupdesc2, 1);
					strncat(values[i_cash_transaction_amount], tmp,
							length_cta);
					length_cta -= strlen(tmp);
					if (length_cta < 0) {
						FAIL_FRAME("cash_transaction_amount values needs to "
								   "be increased");
					}

					strncat(values[i_cash_transaction_dts], "\"",
							length_ctd--);
					if (length_ctd < 0) {
						FAIL_FRAME("cash_transaction_dts values needs to be "
								   "increased");
					}

					tmp = SPI_getvalue(tuple2, tupdesc2, 2);
					strncat(values[i_cash_transaction_dts], tmp, length_ctd);
					length_ctd -= strlen(tmp);
					if (length_ctd < 0) {
						FAIL_FRAME("cash_transaction_dts values needs to be "
								   "increased");
					}

					strncat(values[i_cash_transaction_dts], "\"",
							length_ctd--);
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

					tmp = SPI_getvalue(tuple2, tupdesc2, 3);
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
				}
			}

#ifdef DEBUG
			elog(DEBUG1, "%s", SQLTLF2_4);
#endif /* DEBUG */
			ret = SPI_execute_plan(TLF2_4, args, nulls, true, 0);
			if (ret != SPI_OK_SELECT) {
#ifdef DEBUG
				dump_tlf2_inputs(
						acct_id, end_trade_dts, max_trades, start_trade_dts);
#endif /* DEBUG */
				FAIL_FRAME_SET(&funcctx->max_calls, TLF2_statements[3].sql);
				continue;
			}

			tupdesc2 = SPI_tuptable->tupdesc;
			tuptable2 = SPI_tuptable;

			for (j = 0; j < SPI_processed; j++) {
				tuple2 = tuptable2->vals[j];

				++num_trade_history;

				if (num_trade_history > 1) {
					strncat(values[i_trade_history_dts], ",", length_thd--);
					if (length_thd < 0) {
						FAIL_FRAME("trade_history_dts values needs to be "
								   "increased");
					}

					strncat(values[i_trade_history_status_id], ",",
							length_thsi--);
					if (length_thsi < 0) {
						FAIL_FRAME(
								"trade_history_status_id values needs to be "
								"increased");
					}
				}

				tmp = SPI_getvalue(tuple2, tupdesc2, 1);
				strncat(values[i_trade_history_dts], tmp, length_thd);
				length_thd -= strlen(tmp);
				if (length_thd < 0) {
					FAIL_FRAME(
							"trade_history_dts values needs to be increased");
				}

				tmp = SPI_getvalue(tuple2, tupdesc2, 2);
				strncat(values[i_trade_history_status_id], tmp, length_thsi);
				length_thsi -= strlen(tmp);
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
						elog(DEBUG1,
								"TUF2 trade_history_status_id out of space: "
								"%s",
								values[i_trade_history_status_id]);
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

				strncat(values[i_trade_history_status_id], "NULL",
						length_thsi);
				length_thsi -= 4;
				if (length_thsi < 0) {
					FAIL_FRAME("trade_history_status_id values needs to be "
							   "increased");
				}
#ifdef DEBUG
				elog(DEBUG1, "TUF2 trade_history_dts[%d][%d] %s", i, j,
						values[i_trade_history_dts]);
				elog(DEBUG1, "TUF2 trade_history_status_id[%d][%d] %s", i, j,
						values[i_trade_history_status_id]);
#endif /* DEBUG */
			}
		}

		strncat(values[i_bid_price], "}", length_bp--);
		if (length_bp < 0) {
			FAIL_FRAME("bid_price values needs to be increased");
		}

		strncat(values[i_cash_transaction_amount], "}", length_cta--);
		if (length_cta < 0) {
			FAIL_FRAME("cash_transaction_amount values needs to be "
					   "increased");
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

		strncat(values[i_trade_history_dts], "}", length_thd--);
		if (length_thd < 0) {
			FAIL_FRAME("trade_history_dts values needs to be increased");
		}

		strncat(values[i_trade_history_status_id], "}", length_thsi--);
		if (length_thsi < 0) {
			FAIL_FRAME("trade_history_status_id values needs to be "
					   "increased");
		}

		strncat(values[i_trade_list], "}", length_tl--);
		if (length_tl < 0) {
			FAIL_FRAME("trade_list values needs to be increased");
		}

		strncat(values[i_trade_price], "}", length_tp--);
		if (length_tp < 0) {
			FAIL_FRAME("trade_price values needs to be increased");
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
		 * generate attribute metadata needed later to produce tuples from
		 * raw C strings
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
			elog(DEBUG1, "TLF2 OUT: %d %s", i, values[i]);
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

Datum
TradeLookupFrame3(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

	int i;

	char **values = NULL;

	enum tlf3
	{
		i_acct_id = 0,
		i_cash_transaction_amount,
		i_cash_transaction_dts,
		i_cash_transaction_name,
		i_exec_name,
		i_is_cash,
		i_num_found,
		i_price,
		i_quantity,
		i_settlement_amount,
		i_settlement_cash_due_date,
		i_settlement_cash_type,
		i_trade_dts,
		i_trade_history_dts,
		i_trade_history_status_id,
		i_trade_list,
		i_trade_type
	};

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		Timestamp end_trade_dts_ts = PG_GETARG_TIMESTAMP(0);
		/* max_acct_id used only for engineering purposes... */
#ifdef DEBUG
		long max_acct_id = PG_GETARG_INT64(1);
#endif /* DEBUG */
		int max_trades = PG_GETARG_INT32(2);
		Timestamp start_trade_dts_ts = PG_GETARG_TIMESTAMP(3);
		char *symbol_p = (char *) PG_GETARG_TEXT_P(4);

		char symbol[S_SYMB_LEN + 1];

		struct pg_tm tt, *tm = &tt;
		fsec_t fsec;
		char *tzn = NULL;
		char end_trade_dts[MYMAXDATELEN + 1];
		char start_trade_dts[MYMAXDATELEN + 1];
		Datum args[4];
		char nulls[4] = { ' ', ' ', ' ', ' ' };
		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;

		int num_found_count = 0;
		int num_settlements = 0;
		int num_cash_txn = 0;
		int num_trade_history = 0;

		char *is_cash_str;

		int j;
		char *tmp;
		int length_ai, length_cta, length_ctd, length_ctn, length_en,
				length_ic, length_p, length_q, length_sa, length_scdd,
				length_sct, length_td, length_tl, length_thd, length_thsi,
				length_tt;

		if (timestamp2tm(end_trade_dts_ts, NULL, tm, &fsec, NULL, NULL) == 0) {
			EncodeDateTimeM(tm, fsec, tzn, end_trade_dts);
		}
		if (timestamp2tm(start_trade_dts_ts, NULL, tm, &fsec, NULL, NULL)
				== 0) {
			EncodeDateTimeM(tm, fsec, tzn, start_trade_dts);
		}
		strncpy(symbol,
				DatumGetCString(DirectFunctionCall1(
						textout, PointerGetDatum(symbol_p))),
				sizeof(symbol));

#ifdef DEBUG
		dump_tlf3_inputs(end_trade_dts, max_acct_id, max_trades,
				start_trade_dts, symbol);
#endif /* DEBUG */

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 17);

		length_ai = (IDENT_T_LEN + 1) * max_trades + 2;
		values[i_acct_id] = (char *) palloc(length_ai-- * sizeof(char));

		length_cta = (VALUE_T_LEN + 1) * max_trades + 2;
		values[i_cash_transaction_amount]
				= (char *) palloc(length_cta-- * sizeof(char));

		length_ctd = (MYMAXDATELEN + 1) * max_trades + 2;

		values[i_cash_transaction_dts]
				= (char *) palloc(length_ctd-- * sizeof(char));

		length_ctn = (CT_NAME_LEN + 3) * max_trades + 2;
		values[i_cash_transaction_name]
				= (char *) palloc(length_ctn-- * sizeof(char));

		length_en = (T_EXEC_NAME_LEN + 3) * max_trades + 2;
		values[i_exec_name] = (char *) palloc(length_en-- * sizeof(char));

		length_ic = (BOOLEAN_LEN + 1) * max_trades + 2;
		values[i_is_cash] = (char *) palloc(length_ic-- * sizeof(char));

		values[i_num_found]
				= (char *) palloc((INTEGER_LEN + 1) * sizeof(char));

		length_p = (S_PRICE_T_LEN + 1) * max_trades + 2;
		values[i_price] = (char *) palloc(length_p-- * sizeof(char));

		length_q = (INTEGER_LEN + 1) * max_trades + 2;
		values[i_quantity] = (char *) palloc(length_q-- * sizeof(char));

		length_sa = (VALUE_T_LEN + 1) * max_trades + 2;
		values[i_settlement_amount]
				= (char *) palloc(length_sa-- * sizeof(char));

		length_scdd = (MYMAXDATELEN + 1) * max_trades + 2;
		values[i_settlement_cash_due_date]
				= (char *) palloc(length_scdd-- * sizeof(char));

		length_sct = (SE_CASH_TYPE_LEN + 3) * max_trades + 2;
		values[i_settlement_cash_type]
				= (char *) palloc(length_sct-- * sizeof(char));

		length_td = (MYMAXDATELEN * 3 + 4) * max_trades + max_trades + 2;
		values[i_trade_dts] = (char *) palloc(length_td-- * sizeof(char));

		length_thd = (MYMAXDATELEN * 3 + 4) * max_trades + max_trades + 2;
		values[i_trade_history_dts]
				= (char *) palloc(length_thd-- * sizeof(char));

		length_thsi = (ST_ID_LEN + 3) * max_trades * 3 + 2;
		values[i_trade_history_status_id]
				= (char *) palloc(length_thsi-- * sizeof(char));

		length_tl = (TRADE_T_LEN + 1) * max_trades + 2;
		values[i_trade_list] = (char *) palloc(length_tl-- * sizeof(char));

		length_tt = (TT_ID_LEN + 3) * max_trades + 2;
		values[i_trade_type] = (char *) palloc(length_tt-- * sizeof(char));

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;

		/* switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();
		plan_queries(TLF3_statements);
#ifdef DEBUG
		elog(DEBUG1, "SQLTLF3_1\n%s", SQLTLF3_1);
		elog(DEBUG1, "$1 %s", symbol);
		elog(DEBUG1, "$2 %s", start_trade_dts);
		elog(DEBUG1, "$3 %s", end_trade_dts);
		elog(DEBUG1, "$4 %d", max_trades);
#endif /* DEBUG */
		args[0] = CStringGetTextDatum(symbol);
		args[1] = TimestampGetDatum(start_trade_dts_ts);
		args[2] = TimestampGetDatum(end_trade_dts_ts);
		args[3] = Int32GetDatum(max_trades);
		ret = SPI_execute_plan(TLF3_1, args, nulls, true, 0);
		if (ret != SPI_OK_SELECT) {
#ifdef DEBUG
			dump_tlf3_inputs(end_trade_dts, max_acct_id, max_trades,
					start_trade_dts, symbol);
#endif /* DEBUG */
			FAIL_FRAME_SET(&funcctx->max_calls, TLF3_statements[0].sql);
		}
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		num_found_count = SPI_processed;
		snprintf(values[i_num_found], INTEGER_LEN, "%d", num_found_count);

#ifdef DEBUG
		elog(DEBUG1, "num_found = %d", num_found_count);
#endif /* DEBUG */

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

		values[i_trade_type][0] = '{';
		values[i_trade_type][1] = '\0';

		for (i = 0; i < num_found_count; i++) {
			TupleDesc tupdesc2;
			SPITupleTable *tuptable2 = NULL;
			HeapTuple tuple2 = NULL;

			char *trade_list_str;

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
			strncat(values[i_exec_name], tmp, length_en--);
			length_en -= strlen(tmp);
			if (length_en < 0) {
				FAIL_FRAME("exec_name values needs to be increased");
			}

			strncat(values[i_exec_name], "\"", length_en--);
			if (length_en < 0) {
				FAIL_FRAME("exec_name values needs to be increased");
			}

			is_cash_str = SPI_getvalue(tuple, tupdesc, 3);
			strncat(values[i_is_cash], (is_cash_str[0] == 't' ? "0" : "1"),
					length_ic--);
			if (length_ic < 0) {
				FAIL_FRAME("is_cash values needs to be increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 4);
			strncat(values[i_price], tmp, length_p);
			length_p -= strlen(tmp);
			if (length_p < 0) {
				FAIL_FRAME("price values needs to be increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 5);
			strncat(values[i_quantity], tmp, length_q);
			length_q -= strlen(tmp);
			if (length_q < 0) {
				FAIL_FRAME("quantity values needs to be increased");
			}

			strncat(values[i_trade_dts], "\"", length_td--);
			if (length_td < 0) {
				FAIL_FRAME("trade_dts values needs to be increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 6);
			strncat(values[i_trade_dts], tmp, length_td);
			length_td -= strlen(tmp);
			if (length_td < 0) {
				FAIL_FRAME("trade_dts values needs to be increased");
			}

			strncat(values[i_trade_dts], "\"", length_td--);
			if (length_td < 0) {
				FAIL_FRAME("trade_dts values needs to be increased");
			}

			trade_list_str = SPI_getvalue(tuple, tupdesc, 7);
			strncat(values[i_trade_list], trade_list_str, length_tl);
			length_tl -= strlen(trade_list_str);
			if (length_tl < 0) {
				FAIL_FRAME("trade_list values needs to be increased");
			}

			strncat(values[i_trade_type], "\"", length_tt--);
			if (length_tt < 0) {
				FAIL_FRAME("trade_type values needs to be increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 8);
			strncat(values[i_trade_type], tmp, length_tt);
			length_tt -= strlen(tmp);
			if (length_tt < 0) {
				FAIL_FRAME("trade_type values needs to be increased");
			}

			strncat(values[i_trade_type], "\"", length_tt--);
			if (length_tt < 0) {
				FAIL_FRAME("trade_type values needs to be increased");
			}

#ifdef DEBUG
			elog(DEBUG1, "SQLTLF3_2\n%s", SQLTLF3_2);
			elog(DEBUG1, "$1 %s", trade_list_str);
#endif /* DEBUG */
			args[0] = Int64GetDatum(atoll(trade_list_str));
			ret = SPI_execute_plan(TLF3_2, args, nulls, true, 0);
			if (ret != SPI_OK_SELECT) {
#ifdef DEBUG
				dump_tlf3_inputs(end_trade_dts, max_acct_id, max_trades,
						start_trade_dts, symbol);
#endif /* DEBUG */
				FAIL_FRAME_SET(&funcctx->max_calls, TLF3_statements[1].sql);
				continue;
			}
			for (j = 0; j < SPI_processed; j++) {
				tupdesc2 = SPI_tuptable->tupdesc;
				tuptable2 = SPI_tuptable;
				tuple2 = tuptable2->vals[j];
				++num_settlements;

				if (num_settlements > 1) {
					strncat(values[i_settlement_amount], ",", length_sa--);
					if (length_sa < 0) {
						FAIL_FRAME("settlement_amount values needs to be "
								   "increased");
					}

					strncat(values[i_settlement_cash_due_date], ",",
							length_scdd--);
					if (length_scdd < 0) {
						FAIL_FRAME("settlement_cash_due_date values needs "
								   "to be increased");
					}

					strncat(values[i_settlement_cash_type], ",", length_sct--);
					if (length_sct < 0) {
						FAIL_FRAME("settlement_cash_type values needs to "
								   "be increased");
					}
				}

				tmp = SPI_getvalue(tuple2, tupdesc2, 1);
				strncat(values[i_settlement_amount], tmp, length_sa--);
				if (length_sa < 0) {
					FAIL_FRAME("settlement_amount values needs to be "
							   "increased");
				}

				strncat(values[i_settlement_cash_due_date], "\"",
						length_scdd--);
				if (length_scdd < 0) {
					FAIL_FRAME("settlement_cash_due_date values needs to "
							   "be increased");
				}

				tmp = SPI_getvalue(tuple2, tupdesc2, 2);
				strncat(values[i_settlement_cash_due_date], tmp, length_scdd);
				length_scdd -= strlen(tmp);
				if (length_scdd < 0) {
					FAIL_FRAME("settlement_cash_due_date values needs to "
							   "be increased");
				}

				strncat(values[i_settlement_cash_due_date], "\"",
						length_scdd--);
				if (length_scdd < 0) {
					FAIL_FRAME("settlement_cash_due_date values needs to "
							   "be increased");
				}

				strncat(values[i_settlement_cash_type], "\"", length_sct--);
				if (length_sct < 0) {
					FAIL_FRAME("settlement_cash_type values needs to be "
							   "increased");
				}

				tmp = SPI_getvalue(tuple2, tupdesc2, 3);
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
			}

#ifdef DEBUG
			elog(DEBUG1, "SQLTLF3_3\n%s", SQLTLF3_3);
			elog(DEBUG1, "$1 %s", trade_list_str);
#endif /* DEBUG */
			ret = SPI_execute_plan(TLF3_3, args, nulls, true, 0);
			if (ret != SPI_OK_SELECT) {
#ifdef DEBUG
				dump_tlf3_inputs(end_trade_dts, max_acct_id, max_trades,
						start_trade_dts, symbol);
#endif /* DEBUG */
				FAIL_FRAME_SET(&funcctx->max_calls, TLF3_statements[2].sql);
				continue;
			}
			for (j = 0; j < SPI_processed; j++) {
				tupdesc2 = SPI_tuptable->tupdesc;
				tuptable2 = SPI_tuptable;
				tuple2 = tuptable2->vals[j];

				if (num_cash_txn > 0) {
					strncat(values[i_cash_transaction_amount], ",",
							length_cta--);
					if (length_cta < 0) {
						FAIL_FRAME("cash_transaction_amount values needs "
								   "to be increased");
					}

					strncat(values[i_cash_transaction_dts], ",", length_ctd--);
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

				tmp = SPI_getvalue(tuple2, tupdesc2, 1);
				strncat(values[i_cash_transaction_amount], tmp, length_cta);
				length_cta -= strlen(tmp);
				if (length_cta < 0) {
					FAIL_FRAME("cash_transaction_amount values needs to "
							   "be increased");
				}

				strncat(values[i_cash_transaction_dts], "\"", length_ctd--);
				if (length_ctd < 0) {
					FAIL_FRAME("cash_transaction_dts values needs to be "
							   "increased");
				}

				tmp = SPI_getvalue(tuple2, tupdesc2, 2);
				strncat(values[i_cash_transaction_dts], tmp, length_ctd--);
				length_ctd -= strlen(tmp);
				if (length_ctd < 0) {
					FAIL_FRAME("cash_transaction_dts values needs to be "
							   "increased");
				}

				strncat(values[i_cash_transaction_dts], "\"", length_ctd--);
				if (length_ctd < 0) {
					FAIL_FRAME("cash_transaction_dts values needs to be "
							   "increased");
				}

				strncat(values[i_cash_transaction_name], "\"", length_ctn--);
				if (length_ctn < 0) {
					FAIL_FRAME("cash_transaction_name values needs to be "
							   "increased");
				}

				tmp = SPI_getvalue(tuple2, tupdesc2, 3);
				strncat(values[i_cash_transaction_name], tmp, length_ctn);
				length_ctn -= strlen(tmp);
				if (length_ctn < 0) {
					FAIL_FRAME("cash_transaction_name values needs to be "
							   "increased");
				}

				strncat(values[i_cash_transaction_name], "\"", length_ctn--);
				if (length_ctn < 0) {
					FAIL_FRAME("cash_transaction_name values needs to be "
							   "increased");
				}

				++num_cash_txn;
			}

#ifdef DEBUG
			elog(DEBUG1, "SQLTLF3_4\n%s", SQLTLF3_4);
			elog(DEBUG1, "$1 %s", trade_list_str);
#endif /* DEBUG */
			ret = SPI_execute_plan(TLF3_4, args, nulls, true, 0);
			if (ret != SPI_OK_SELECT) {
#ifdef DEBUG
				dump_tlf3_inputs(end_trade_dts, max_acct_id, max_trades,
						start_trade_dts, symbol);
#endif /* DEBUG */
				FAIL_FRAME_SET(&funcctx->max_calls, TLF3_statements[3].sql);
				continue;
			}
			tupdesc2 = SPI_tuptable->tupdesc;
			tuptable2 = SPI_tuptable;
			tuple2 = tuptable2->vals[j];

			for (j = 0; j < SPI_processed; j++) {
				++num_trade_history;
				if (num_trade_history > 1) {
					strncat(values[i_trade_history_dts], ",", length_thd--);
					if (length_thd < 0) {
						FAIL_FRAME("trade_history_dts values needs to be "
								   "increased");
					}

					strncat(values[i_trade_history_status_id], ",",
							length_thsi--);
					if (length_thsi < 0) {
						FAIL_FRAME("trade_history_status_id values needs "
								   "to be increased");
					}
				}

				strncat(values[i_trade_history_dts], "\"", length_thd--);
				if (length_thd < 0) {
					FAIL_FRAME("trade_history_dts values needs to be "
							   "increased");
				}

				tmp = SPI_getvalue(tuple2, tupdesc2, 1);
				strncat(values[i_trade_history_dts], tmp, length_thd);
				length_thd -= strlen(tmp);
				if (length_thd < 0) {
					FAIL_FRAME("trade_history_dts values needs to be "
							   "increased");
				}

				strncat(values[i_trade_history_dts], "\"", length_thd--);
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

				tmp = SPI_getvalue(tuple2, tupdesc2, 2);
				strncat(values[i_trade_history_status_id], tmp, length_thsi);
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
			}

			/*
			 * FIXME: Can't have varying size multi-dimensional array.
			 * Since the spec says no more than three items here, pad
			 * the array up to 3 all the time.
			 */
			for (j = SPI_processed; j < 3; j++) {
				if (j > 0) {
					strncat(values[i_trade_history_dts], ",", length_thd--);
					if (length_thd < 0) {
						FAIL_FRAME("trade_history_dts values needs to "
								   "be increased");
					}

					strncat(values[i_trade_history_status_id], ",",
							length_thsi--);
					if (length_thsi < 0) {
						FAIL_FRAME("trade_history_status_id values "
								   "needs to be increased");
					}
				}
				strncat(values[i_trade_history_dts], "NULL", length_thd);
				length_thd -= 4;
				if (length_thd < 0) {
					FAIL_FRAME(
							"trade_history_dts values needs to be increased");
				}

				strncat(values[i_trade_history_status_id], "NULL",
						length_thsi);
				length_thsi -= 4;
				if (length_thsi < 0) {
					FAIL_FRAME("trade_history_status_id values needs "
							   "to be increased");
				}
			}
		}

		strncat(values[i_acct_id], "}", length_ai--);
		if (length_ai < 0) {
			FAIL_FRAME("acct_id values needs to be increased");
		}

		strncat(values[i_cash_transaction_amount], "}", length_cta--);
		if (length_cta < 0) {
			FAIL_FRAME("cash_transaction_amount values needs to be "
					   "increased");
		}

		strncat(values[i_cash_transaction_dts], "}", length_ctd--);
		if (length_ctd < 0) {
			FAIL_FRAME("cash_transaction_dts values needs to be increased");
		}

		strncat(values[i_cash_transaction_name], "}", length_ctn--);
		if (length_ctn < 0) {
			FAIL_FRAME("cash_transaction_name values needs to be "
					   "increased");
		}

		strncat(values[i_exec_name], "}", length_en--);
		if (length_en < 0) {
			FAIL_FRAME("exec_name values needs to be increased");
		}

		strncat(values[i_is_cash], "}", length_ic);
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

		strncat(values[i_trade_dts], "}", length_td--);
		if (length_td < 0) {
			FAIL_FRAME("trade_dts values needs to be increased");
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

		strncat(values[i_trade_list], "}", length_tl--);
		if (length_tl < 0) {
			FAIL_FRAME("trade_list values needs to be increased");
		}

		strncat(values[i_trade_type], "}", length_tt--);
		if (length_tt < 0) {
			FAIL_FRAME("trade_type values needs to be increased");
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
		 * generate attribute metadata needed later to produce tuples from
		 * raw C strings
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
		for (i = 0; i < 17; i++) {
			elog(DEBUG1, "TLF3 OUT: %d %s", i, values[i]);
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

/* Clause 3.3.6.6 */
Datum
TradeLookupFrame4(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

	int i;

	char **values = NULL;

	enum tlf4
	{
		i_holding_history_id = 0,
		i_holding_history_trade_id,
		i_num_found,
		i_num_trades_found,
		i_quantity_after,
		i_quantity_before,
		i_trade_id
	};

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		long acct_id = PG_GETARG_INT64(0);
		Timestamp start_trade_dts_ts = PG_GETARG_TIMESTAMP(1);

		struct pg_tm tt, *tm = &tt;
		fsec_t fsec;
		char *tzn = NULL;
		char start_trade_dts[MYMAXDATELEN + 1];
		int num_found_count = 0;
		int num_trades_found_count = 0;
		Datum args[2];
		char nulls[2] = { ' ', ' ' };

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;

		char *tmp;
		int length_hhi, length_hhti, length_qa, length_qb;

		if (timestamp2tm(start_trade_dts_ts, NULL, tm, &fsec, NULL, NULL)
				== 0) {
			EncodeDateTimeM(tm, fsec, tzn, start_trade_dts);
		}

#ifdef DEBUG
		dump_tlf4_inputs(acct_id, start_trade_dts);
#endif /* DEBUG */

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 7);

		length_hhi = (TRADE_T_LEN + 1) * 20 + 2;
		values[i_holding_history_id]
				= (char *) palloc(length_hhi-- * sizeof(char));

		length_hhti = (TRADE_T_LEN + 1) * 20 + 2;
		values[i_holding_history_trade_id]
				= (char *) palloc(length_hhti-- * sizeof(char));

		values[i_num_found]
				= (char *) palloc((INTEGER_LEN + 1) * sizeof(char));
		values[i_num_trades_found]
				= (char *) palloc((INTEGER_LEN + 1) * sizeof(char));

		length_qa = (S_QTY_T_LEN + 1) * 20 + 2;
		values[i_quantity_after] = (char *) palloc(length_qa-- * sizeof(char));

		length_qb = (S_QTY_T_LEN + 1) * 20 + 2;
		values[i_quantity_before]
				= (char *) palloc(length_qb-- * sizeof(char));

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;

		/* switch to memory context appropriate for multiple function calls
		 */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();
		plan_queries(TLF4_statements);
#ifdef DEBUG
		elog(DEBUG1, "%s", SQLTLF4_1);
#endif /* DEBUG */
		args[0] = Int64GetDatum(acct_id);
		args[1] = TimestampGetDatum(start_trade_dts_ts);
		ret = SPI_execute_plan(TLF4_1, args, nulls, true, 0);
		if (ret != SPI_OK_SELECT) {
			values[i_trade_id] = NULL;
#ifdef DEBUG
			dump_tlf4_inputs(acct_id, start_trade_dts);
#endif /* DEBUG */
			FAIL_FRAME_SET(&funcctx->max_calls, TLF4_statements[0].sql);
		}
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		if (SPI_processed > 0) {
			tuple = tuptable->vals[0];
			values[i_trade_id] = SPI_getvalue(tuple, tupdesc, 1);
		} else
			values[i_trade_id] = NULL;
		num_trades_found_count = SPI_processed;

		if (values[i_trade_id] != NULL) {
#ifdef DEBUG
			elog(DEBUG1, "%s", SQLTLF4_2);
#endif /* DEBUG */
			args[0] = Int64GetDatum(atoll(values[i_trade_id]));
			ret = SPI_execute_plan(TLF4_2, args, nulls, true, 0);
			if (ret != SPI_OK_SELECT) {
#ifdef DEBUG
				dump_tlf4_inputs(acct_id, start_trade_dts);
#endif /* DEBUG */
				FAIL_FRAME_SET(&funcctx->max_calls, TLF4_statements[1].sql);
			}
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			num_found_count = SPI_processed;
		} else
			num_found_count = 0;

		snprintf(values[i_num_found], INTEGER_LEN, "%d", num_found_count);
		snprintf(values[i_num_trades_found], INTEGER_LEN, "%d",
				num_trades_found_count);
#ifdef DEBUG
		elog(DEBUG1, "num_found = %d", num_found_count);
		elog(DEBUG1, "num_trades_found = %d", num_trades_found_count);
#endif /* DEBUG */

		values[i_holding_history_id][0] = '{';
		values[i_holding_history_id][1] = '\0';

		values[i_holding_history_trade_id][0] = '{';
		values[i_holding_history_trade_id][1] = '\0';

		values[i_quantity_after][0] = '{';
		values[i_quantity_after][1] = '\0';

		values[i_quantity_before][0] = '{';
		values[i_quantity_before][1] = '\0';

		for (i = 0; i < num_found_count; i++) {
			tuple = tuptable->vals[i];
			if (i > 0) {
				strncat(values[i_holding_history_id], ",", length_hhi--);
				if (length_hhi < 0) {
					FAIL_FRAME("holding_history_id values needs to be "
							   "increased");
				}

				strncat(values[i_holding_history_trade_id], ",",
						length_hhti--);
				if (length_hhti < 0) {
					FAIL_FRAME("holding_history_trade_id values needs to be "
							   "increased");
				}

				strncat(values[i_quantity_after], ",", length_qa--);
				if (length_qa < 0) {
					FAIL_FRAME("quantify_after values needs to be increased");
				}

				strncat(values[i_quantity_before], ",", length_qb--);
				if (length_qb < 0) {
					FAIL_FRAME("quantify_before values needs to be "
							   "increased");
				}
			}

			tmp = SPI_getvalue(tuple, tupdesc, 1);
			strncat(values[i_holding_history_id], tmp, length_hhi);
			length_hhi -= strlen(tmp);
			if (length_hhi < 0) {
				FAIL_FRAME("holding_history_id values needs to be increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 2);
			strncat(values[i_holding_history_trade_id], tmp, length_hhti);
			length_hhti -= strlen(tmp);
			if (length_hhti < 0) {
				FAIL_FRAME("holding_history_trade_id values needs to be "
						   "increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 3);
			strncat(values[i_quantity_after], tmp, length_qa--);
			length_qa -= strlen(tmp);
			if (length_qa < 0) {
				FAIL_FRAME("quantify_after values needs to be increased");
			}

			tmp = SPI_getvalue(tuple, tupdesc, 4);
			strncat(values[i_quantity_before], tmp, length_qb);
			length_qb -= strlen(tmp);
			if (length_qb < 0) {
				FAIL_FRAME("quantify_before values needs to be increased");
			}
		}

		strncat(values[i_holding_history_id], "}", length_hhi--);
		if (length_hhi < 0) {
			FAIL_FRAME("holding_history_id values needs to be increased");
		}

		strncat(values[i_holding_history_trade_id], "}", length_hhti--);
		if (length_hhti < 0) {
			FAIL_FRAME("holding_history_trade_id values needs to be "
					   "increased");
		}

		strncat(values[i_quantity_after], "}", length_qa--);
		if (length_qa < 0) {
			FAIL_FRAME("quantify_after values needs to be increased");
		}

		strncat(values[i_quantity_before], "}", length_qb--);
		if (length_qb < 0) {
			FAIL_FRAME("quantify_before values needs to be increased");
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
		 * generate attribute metadata needed later to produce tuples from
		 * raw C strings
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
		for (i = 0; i < 7; i++) {
			elog(DEBUG1, "TLF4 OUT: %d %s", i, values[i]);
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
