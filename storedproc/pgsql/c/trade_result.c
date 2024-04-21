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
#include <time.h>
#include <executor/spi.h> /* this should include most necessary APIs */
#include <executor/executor.h> /* for GetAttributeByName() */
#include <funcapi.h> /* for returning set of rows in order_status */
#include <utils/datetime.h>
#include <utils/numeric.h>
#include <utils/builtins.h>
#include <utils/date.h>
#include <utils/timestamp.h>
#include <catalog/pg_type.h>

#include "frame.h"
#include "dbt5common.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define SQLTRF1_1                                                             \
	"SELECT t_ca_id\n"                                                        \
	"     , t_tt_id\n"                                                        \
	"     , t_s_symb\n"                                                       \
	"     , t_qty\n"                                                          \
	"     , t_chrg,\n"                                                        \
	"       CASE WHEN t_lifo = true\n"                                        \
	"            THEN 1\n"                                                    \
	"            ELSE 0 END\n"                                                \
	"     , CASE WHEN t_is_cash = true\n"                                     \
	"            THEN 1\n"                                                    \
	"            ELSE 0 END\n"                                                \
	"FROM trade\n"                                                            \
	"WHERE t_id = $1"

#define SQLTRF1_2                                                             \
	"SELECT tt_name\n"                                                        \
	"     , CASE WHEN tt_is_sell = true\n"                                    \
	"            THEN 1\n"                                                    \
	"            ELSE 0 END\n"                                                \
	"     , CASE WHEN tt_is_mrkt = true\n"                                    \
	"            THEN 1\n"                                                    \
	"            ELSE 0 END\n"                                                \
	"FROM trade_type\n"                                                       \
	"WHERE tt_id = $1"

#define SQLTRF1_3                                                             \
	"SELECT hs_qty\n"                                                         \
	"FROM holding_summary\n"                                                  \
	"WHERE hs_ca_id = $1\n"                                                   \
	"  AND hs_s_symb = $2"

#define SQLTRF2_1                                                             \
	"SELECT ca_b_id\n"                                                        \
	"     , ca_c_id\n"                                                        \
	"     , ca_tax_st\n"                                                      \
	"FROM customer_account\n"                                                 \
	"WHERE ca_id = $1\n"                                                      \
	"FOR UPDATE"

#define SQLTRF2_2a                                                            \
	"INSERT INTO holding_summary(\n"                                          \
	"    hs_ca_id\n"                                                          \
	"  , hs_s_symb\n"                                                         \
	"  , hs_qty\n"                                                            \
	")\n"                                                                     \
	"VALUES(\n"                                                               \
	"    $1\n"                                                                \
	"  , $2\n"                                                                \
	"  , $3\n"                                                                \
	")"

#define SQLTRF2_2b                                                            \
	"UPDATE holding_summary\n"                                                \
	"SET hs_qty = $1\n"                                                       \
	"WHERE hs_ca_id = $2\n "                                                  \
	"  AND hs_s_symb = $3"

#define SQLTRF2_3a                                                            \
	"SELECT h_t_id\n"                                                         \
	"     , h_qty\n"                                                          \
	"     , h_price\n"                                                        \
	"FROM holding\n"                                                          \
	"WHERE h_ca_id = $1\n"                                                    \
	"  AND h_s_symb = $2\n"                                                   \
	"ORDER BY h_dts DESC\n"                                                   \
	"FOR UPDATE"

#define SQLTRF2_3b                                                            \
	"SELECT h_t_id\n"                                                         \
	"     , h_qty\n"                                                          \
	"     , h_price\n"                                                        \
	"FROM holding\n"                                                          \
	"WHERE h_ca_id = $1\n"                                                    \
	"  AND h_s_symb = $2\n"                                                   \
	"ORDER BY h_dts ASC\n"                                                    \
	"FOR UPDATE"

#define SQLTRF2_4a                                                            \
	"INSERT INTO holding_history(\n"                                          \
	"    hh_h_t_id\n"                                                         \
	"  , hh_t_id\n"                                                           \
	"  , hh_before_qty\n"                                                     \
	"  , hh_after_qty\n"                                                      \
	")\n"                                                                     \
	"VALUES(\n"                                                               \
	"    $1\n"                                                                \
	"  , $2\n"                                                                \
	"  , $3\n"                                                                \
	"  , $4\n"                                                                \
	")"

#define SQLTRF2_5a                                                            \
	"UPDATE holding\n"                                                        \
	"SET h_qty = $1\n"                                                        \
	"WHERE h_t_id = $2"

#define SQLTRF2_5b                                                            \
	"DELETE FROM holding\n"                                                   \
	"WHERE h_t_id = $1"

#define SQLTRF2_7a                                                            \
	"INSERT INTO holding(\n"                                                  \
	"    h_t_id\n"                                                            \
	"  , h_ca_id\n"                                                           \
	"  , h_s_symb\n"                                                          \
	"  , h_dts\n"                                                             \
	"  , h_price\n"                                                           \
	"  , h_qty)\n"                                                            \
	"VALUES (\n"                                                              \
	"    $1\n"                                                                \
	"  , $2\n"                                                                \
	"  , $3\n"                                                                \
	"  , $4\n"                                                                \
	"  , $5\n"                                                                \
	"  , $6\n"                                                                \
	")"

#define SQLTRF2_7b                                                            \
	"DELETE FROM holding_summary\n"                                           \
	"WHERE hs_ca_id = $1\n"                                                   \
	"  AND hs_s_symb = $2"

#define SQLTRF2_8a                                                            \
	"INSERT INTO holding_summary(\n"                                          \
	"    hs_ca_id\n"                                                          \
	"  , hs_s_symb\n"                                                         \
	"  , hs_qty\n"                                                            \
	")\n"                                                                     \
	"VALUES (\n"                                                              \
	"    $1\n"                                                                \
	"  , $2\n"                                                                \
	"  , $3\n"                                                                \
	")"

#define SQLTRF2_8b                                                            \
	"UPDATE holding_summary\n"                                                \
	"SET hs_qty = $1\n"                                                       \
	"WHERE hs_ca_id = $2\n"                                                   \
	"  AND hs_s_symb = $3"

#define SQLTRF3_1                                                             \
	"SELECT sum(tx_rate)\n"                                                   \
	"FROM taxrate\n"                                                          \
	"WHERE tx_id IN (\n"                                                      \
	"                   SELECT cx_tx_id\n"                                    \
	"                   FROM customer_taxrate\n"                              \
	"                   WHERE cx_c_id = $1\n"                                 \
	"               )\n"

#define SQLTRF3_2                                                             \
	"UPDATE trade\n"                                                          \
	"SET t_tax = $1\n"                                                        \
	"WHERE t_id = $2"

#define SQLTRF4_1                                                             \
	"SELECT s_ex_id\n"                                                        \
	"     , s_name\n"                                                         \
	"FROM security\n"                                                         \
	"WHERE s_symb = $1"

#define SQLTRF4_2                                                             \
	"SELECT c_tier\n"                                                         \
	"FROM customer\n"                                                         \
	"WHERE c_id = $1"

#define SQLTRF4_3                                                             \
	"SELECT cr_rate\n"                                                        \
	"FROM commission_rate\n"                                                  \
	"WHERE cr_c_tier = $1\n"                                                  \
	"  AND cr_tt_id = $2\n"                                                   \
	"  AND cr_ex_id = $3\n"                                                   \
	"  AND cr_from_qty <= $4\n"                                               \
	"  AND cr_to_qty >= $5\n"                                                 \
	"LIMIT 1"

#define SQLTRF5_1                                                             \
	"UPDATE trade\n"                                                          \
	"SET t_comm = $1\n"                                                       \
	"  , t_dts = $2\n"                                                        \
	"  , t_st_id = $3\n"                                                      \
	"  , t_trade_price = $4\n"                                                \
	"WHERE t_id = $5"

#define SQLTRF5_2                                                             \
	"INSERT INTO trade_history(\n"                                            \
	"    th_t_id\n"                                                           \
	"  , th_dts\n"                                                            \
	"  , th_st_id\n"                                                          \
	")\n"                                                                     \
	"VALUES (\n"                                                              \
	"    $1\n"                                                                \
	"  , $2\n"                                                                \
	"  , $3\n"                                                                \
	")"

#define SQLTRF5_3                                                             \
	"UPDATE broker\n"                                                         \
	"SET b_comm_total = b_comm_total + $1\n"                                  \
	"  , b_num_trades = b_num_trades + 1\n"                                   \
	"WHERE b_id = $2"

#define SQLTRF6_1                                                             \
	"INSERT INTO settlement(\n"                                               \
	"    se_t_id\n"                                                           \
	"  , se_cash_type\n"                                                      \
	"  , se_cash_due_date\n"                                                  \
	"  , se_amt)\n"                                                           \
	"VALUES (\n"                                                              \
	"    $1\n"                                                                \
	"  , $2\n"                                                                \
	"  , $3\n"                                                                \
	"  , $4\n"                                                                \
	")"

#define SQLTRF6_2                                                             \
	"UPDATE customer_account\n"                                               \
	"SET ca_bal = ca_bal + $1\n"                                              \
	"WHERE ca_id = $2"

#define SQLTRF6_3                                                             \
	"INSERT INTO cash_transaction(\n"                                         \
	"    ct_dts\n"                                                            \
	"  , ct_t_id\n"                                                           \
	"  , ct_amt\n"                                                            \
	"  , ct_name\n"                                                           \
	")\n"                                                                     \
	"VALUES (\n"                                                              \
	"    $1\n"                                                                \
	"  , $2\n"                                                                \
	"  , $3\n"                                                                \
	"  , e'$4 $5 shared of $6'\n"                                             \
	")"

#define SQLTRF6_4                                                             \
	"SELECT ca_bal\n"                                                         \
	"FROM customer_account\n"                                                 \
	"WHERE ca_id = $1"

#define TRF1_1 TRF1_statements[0].plan
#define TRF1_2 TRF1_statements[1].plan
#define TRF1_3 TRF1_statements[2].plan

#define TRF2_1 TRF2_statements[0].plan
#define TRF2_2a TRF2_statements[1].plan
#define TRF2_2b TRF2_statements[2].plan
#define TRF2_3a TRF2_statements[3].plan
#define TRF2_3b TRF2_statements[4].plan
#define TRF2_4a TRF2_statements[5].plan
#define TRF2_5a TRF2_statements[6].plan
#define TRF2_5b TRF2_statements[7].plan
#define TRF2_7a TRF2_statements[8].plan
#define TRF2_7b TRF2_statements[9].plan
#define TRF2_8a TRF2_statements[10].plan
#define TRF2_8b TRF2_statements[11].plan

#define TRF3_1 TRF3_statements[0].plan
#define TRF3_2 TRF3_statements[1].plan

#define TRF4_1 TRF4_statements[0].plan
#define TRF4_2 TRF4_statements[1].plan
#define TRF4_3 TRF4_statements[2].plan

#define TRF5_1 TRF5_statements[0].plan
#define TRF5_2 TRF5_statements[1].plan
#define TRF5_3 TRF5_statements[2].plan

#define TRF6_1 TRF6_statements[0].plan
#define TRF6_2 TRF6_statements[1].plan
#define TRF6_3 TRF6_statements[2].plan
#define TRF6_4 TRF6_statements[3].plan

static cached_statement TRF1_statements[] = {

	{ SQLTRF1_1, 1, { INT8OID } },

	{ SQLTRF1_2, 1, { TEXTOID } },

	{ SQLTRF1_3, 2, { INT8OID, TEXTOID } },

	{ NULL }
};

static cached_statement TRF2_statements[] = {

	{ SQLTRF2_1, 1, { INT8OID } },

	{ SQLTRF2_2a, 3, { INT8OID, TEXTOID, INT4OID } },

	{ SQLTRF2_2b, 3, { INT4OID, INT8OID, TEXTOID } },

	{ SQLTRF2_3a, 2, { INT8OID, TEXTOID } },

	{ SQLTRF2_3b, 2, { INT8OID, TEXTOID } },

	{ SQLTRF2_4a, 4, { INT8OID, INT8OID, INT4OID, INT4OID } },

	{ SQLTRF2_5a, 2, { INT4OID, INT8OID } },

	{ SQLTRF2_5b, 1, { INT8OID } },

	{ SQLTRF2_7a, 6,
			{ INT8OID, INT8OID, TEXTOID, TIMESTAMPOID, FLOAT8OID, INT4OID } },

	{ SQLTRF2_7b, 2, { INT8OID, TEXTOID } },

	{ SQLTRF2_8a, 3, { INT8OID, TEXTOID, INT4OID } },

	{ SQLTRF2_8b, 3, { INT4OID, INT8OID, TEXTOID } },

	{ NULL }
};

static cached_statement TRF3_statements[] = {

	{ SQLTRF3_1, 1, { INT8OID } },

	{ SQLTRF3_2, 2, { FLOAT8OID, INT8OID } },

	{ NULL }
};

static cached_statement TRF4_statements[] = {

	{ SQLTRF4_1, 1, { TEXTOID } },

	{ SQLTRF4_2, 1, { INT8OID } },

	{ SQLTRF4_3, 5, { INT2OID, TEXTOID, TEXTOID, INT4OID, INT4OID } },

	{ NULL }
};

static cached_statement TRF5_statements[] = {

	{ SQLTRF5_1, 5, { FLOAT4OID, TIMESTAMPOID, TEXTOID, FLOAT8OID, INT8OID } },

	{ SQLTRF5_2, 3, { INT8OID, TIMESTAMPOID, TEXTOID } },

	{ SQLTRF5_3, 2, { FLOAT8OID, INT8OID } },

	{ NULL }
};

static cached_statement TRF6_statements[] = {

	{ SQLTRF6_1, 4, { INT8OID, TEXTOID, DATEOID, FLOAT8OID } },

	{ SQLTRF6_2, 2, { FLOAT8OID, INT8OID } },

	{ SQLTRF6_3, 6,
			{ TIMESTAMPOID, INT8OID, FLOAT8OID, TEXTOID, INT4OID, TEXTOID } },

	{ SQLTRF6_4, 1, { INT8OID } },

	{ NULL }
};

/* Prototypes. */
#ifdef DEBUG
void dump_trf1_inputs(long);
void dump_trf2_inputs(long, int, int, char *, long, double, int, int);
void dump_trf3_inputs(double, long, double, long);
void dump_trf4_inputs(long, char *, int, char *);
void dump_trf5_inputs(long, double, char *, char *, long, double);
void dump_trf6_inputs(
		long, char *, char *, double, char *, long, int, int, char *);
#endif /* DEBUG */

Datum TradeResultFrame1(PG_FUNCTION_ARGS);
Datum TradeResultFrame2(PG_FUNCTION_ARGS);
Datum TradeResultFrame3(PG_FUNCTION_ARGS);
Datum TradeResultFrame4(PG_FUNCTION_ARGS);
Datum TradeResultFrame5(PG_FUNCTION_ARGS);
Datum TradeResultFrame6(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(TradeResultFrame1);
PG_FUNCTION_INFO_V1(TradeResultFrame2);
PG_FUNCTION_INFO_V1(TradeResultFrame3);
PG_FUNCTION_INFO_V1(TradeResultFrame4);
PG_FUNCTION_INFO_V1(TradeResultFrame5);
PG_FUNCTION_INFO_V1(TradeResultFrame6);

#ifdef DEBUG
void
dump_trf1_inputs(long trade_id)
{
	elog(DEBUG1, "TRF1: INPUTS START");
	elog(DEBUG1, "TRF1: trade_id %ld", trade_id);
	elog(DEBUG1, "TRF1: INPUTS END");
}

void
dump_trf2_inputs(long acct_id, int hs_qty, int is_lifo, char *symbol,
		long trade_id, double trade_price, int trade_qty, int type_is_sell)
{
	elog(DEBUG1, "TRF2: INPUTS START");
	elog(DEBUG1, "TRF2: acct_id %ld", acct_id);
	elog(DEBUG1, "TRF2: hs_qty %d", hs_qty);
	elog(DEBUG1, "TRF2: is_lifo %d", is_lifo);
	elog(DEBUG1, "TRF2: symbol %s", symbol);
	elog(DEBUG1, "TRF2: trade_id %ld", trade_id);
	elog(DEBUG1, "TRF2: trade_price %f", trade_price);
	elog(DEBUG1, "TRF2: trade_qty %d", trade_qty);
	elog(DEBUG1, "TRF2: type_is_sell %d", type_is_sell);
	elog(DEBUG1, "TRF2: INPUTS END");
}

void
dump_trf3_inputs(
		double buy_value, long cust_id, double sell_value, long trade_id)
{
	elog(DEBUG1, "TRF3: INPUTS START");
	elog(DEBUG1, "TRF3: buy_value %f", buy_value);
	elog(DEBUG1, "TRF3: cust_id %ld", cust_id);
	elog(DEBUG1, "TRF3: sell_value %f", sell_value);
	elog(DEBUG1, "TRF3: trade_id %ld", trade_id);
	elog(DEBUG1, "TRF3: INPUTS END");
}

void
dump_trf4_inputs(long cust_id, char *symbol, int trade_qty, char *type_id)
{
	elog(DEBUG1, "TRF4: INPUTS START");
	elog(DEBUG1, "TRF4: cust_id %ld", cust_id);
	elog(DEBUG1, "TRF4: symbol %s", symbol);
	elog(DEBUG1, "TRF4: trade_qty %d", trade_qty);
	elog(DEBUG1, "TRF4: type_id %s", type_id);
	elog(DEBUG1, "TRF4: INPUTS END");
}

void
dump_trf5_inputs(long broker_id, double comm_amount, char *st_completed_id,
		char *trade_dts, long trade_id, double trade_price)
{
	elog(DEBUG1, "TRF5: INPUTS START");
	elog(DEBUG1, "TRF5: broker_id %ld", broker_id);
	elog(DEBUG1, "TRF5: comm_amount %f", comm_amount);
	elog(DEBUG1, "TRF5: st_completed_id %s", st_completed_id);
	elog(DEBUG1, "TRF5: trade_dts %s", trade_dts);
	elog(DEBUG1, "TRF5: trade_id %ld", trade_id);
	elog(DEBUG1, "TRF5: trade_price %f", trade_price);
	elog(DEBUG1, "TRF5: INPUTS END");
}

void
dump_trf6_inputs(long acct_id, char *due_date, char *s_name, double se_amount,
		char *trade_dts, long trade_id, int trade_is_cash, int trade_qty,
		char *type_name)
{
	elog(DEBUG1, "TRF6: INPUTS START");
	elog(DEBUG1, "TRF6: acct_id %ld", acct_id);
	elog(DEBUG1, "TRF6: due_date %s", due_date);
	elog(DEBUG1, "TRF6: s_name %s", s_name);
	elog(DEBUG1, "TRF6: se_amount %f", se_amount);
	elog(DEBUG1, "TRF6: trade_dts %s", trade_dts);
	elog(DEBUG1, "TRF6: trade_id %ld", trade_id);
	elog(DEBUG1, "TRF6: trade_is_cash %d", trade_is_cash);
	elog(DEBUG1, "TRF6: trade_qty %d", trade_qty);
	elog(DEBUG1, "TRF6: type_name %s", type_name);
	elog(DEBUG1, "TRF6: INPUTS END");
}
#endif /* DEBUG */

/* Clause 3.3.8.3 */
Datum
TradeResultFrame1(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;
	int num_found = 0;

#ifdef DEBUG
	int i;
#endif /* DEBUG */

	char **values = NULL;

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		enum trf1
		{
			i_acct_id = 0,
			i_charge,
			i_hs_qty,
			i_is_lifo,
			i_num_found,
			i_symbol,
			i_trade_is_cash,
			i_trade_qty,
			i_type_id,
			i_type_is_market,
			i_type_is_sell,
			i_type_name
		};

		long trade_id = PG_GETARG_INT64(0);

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;
		Datum args[2];
		char nulls[2] = { ' ', ' ' };
		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 12);
		values[i_num_found]
				= (char *) palloc((INTEGER_LEN + 1) * sizeof(char));

#ifdef DEBUG
		dump_trf1_inputs(trade_id);
#endif

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();
		plan_queries(TRF1_statements);
#ifdef DEBUG
		elog(DEBUG1, "%s", SQLTRF1_1);
#endif /* DEBUG */
		args[0] = Int64GetDatum(trade_id);
		ret = SPI_execute_plan(TRF1_1, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			num_found = SPI_processed;
			if (SPI_processed > 0) {
				tuple = tuptable->vals[0];
				values[i_acct_id] = SPI_getvalue(tuple, tupdesc, 1);
				values[i_type_id] = SPI_getvalue(tuple, tupdesc, 2);
				values[i_symbol] = SPI_getvalue(tuple, tupdesc, 3);
				values[i_trade_qty] = SPI_getvalue(tuple, tupdesc, 4);
				values[i_charge] = SPI_getvalue(tuple, tupdesc, 5);
				values[i_is_lifo] = SPI_getvalue(tuple, tupdesc, 6);
				values[i_trade_is_cash] = SPI_getvalue(tuple, tupdesc, 7);

#ifdef DEBUG
				elog(DEBUG1, "%s", SQLTRF1_2);
#endif /* DEBUG */
				args[0] = CStringGetTextDatum(values[i_type_id]);
				ret = SPI_execute_plan(TRF1_2, args, nulls, true, 0);
				if (ret == SPI_OK_SELECT) {
					tupdesc = SPI_tuptable->tupdesc;
					tuptable = SPI_tuptable;
					if (SPI_processed > 0) {
						tuple = tuptable->vals[0];
						values[i_type_name] = SPI_getvalue(tuple, tupdesc, 1);
						values[i_type_is_sell]
								= SPI_getvalue(tuple, tupdesc, 2);
						values[i_type_is_market]
								= SPI_getvalue(tuple, tupdesc, 3);
					}
				} else {
					FAIL_FRAME_SET(
							&funcctx->max_calls, TRF1_statements[1].sql);
#ifdef DEBUG
					dump_trf1_inputs(trade_id);
#endif /* DEBUG */
				}

#ifdef DEBUG
				elog(DEBUG1, "%s", SQLTRF1_3);
#endif /* DEBUG */
				args[0] = Int64GetDatum(atoll(values[i_acct_id]));
				args[1] = CStringGetTextDatum(values[i_symbol]);
				ret = SPI_execute_plan(TRF1_3, args, nulls, true, 0);
				if (ret == SPI_OK_SELECT) {
					tupdesc = SPI_tuptable->tupdesc;
					tuptable = SPI_tuptable;
					if (SPI_processed > 0) {
						tuple = tuptable->vals[0];
						values[i_hs_qty] = SPI_getvalue(tuple, tupdesc, 1);
					} else {
						values[i_hs_qty] = (char *) palloc(sizeof(char) * 2);
						strncpy(values[i_hs_qty], "0", 2);
					}
				} else {
					FAIL_FRAME_SET(
							&funcctx->max_calls, TRF1_statements[2].sql);
#ifdef DEBUG
					dump_trf1_inputs(trade_id);
#endif /* DEBUG */
				}
			} else if (SPI_processed == 0) {
				values[i_acct_id] = (char *) palloc(2 * sizeof(char));
				strncpy(values[i_acct_id], "0", 2);
				values[i_type_id] = (char *) palloc(sizeof(char));
				values[i_type_id][0] = '\0';
				values[i_symbol] = (char *) palloc(sizeof(char));
				values[i_symbol][0] = '\0';
				values[i_trade_qty] = (char *) palloc(2 * sizeof(char));
				strncpy(values[i_trade_qty], "0", 2);
				values[i_charge] = (char *) palloc(2 * sizeof(char));
				strncpy(values[i_charge], "0", 2);
				values[i_is_lifo] = (char *) palloc(2 * sizeof(char));
				strncpy(values[i_is_lifo], "0", 2);
				values[i_trade_is_cash] = (char *) palloc(2 * sizeof(char));
				strncpy(values[i_trade_is_cash], "0", 2);

				values[i_type_name] = (char *) palloc(sizeof(char));
				values[i_type_name][0] = '\0';
				values[i_type_is_sell] = (char *) palloc(2 * sizeof(char));
				strncpy(values[i_type_is_sell], "0", 2);
				values[i_type_is_market] = (char *) palloc(2 * sizeof(char));
				strncpy(values[i_type_is_market], "0", 2);

				values[i_hs_qty] = (char *) palloc(sizeof(char) * 2);
				strncpy(values[i_hs_qty], "0", 2);
			}
		} else {
#ifdef DEBUG
			dump_trf1_inputs(trade_id);
#endif /* DEBUG */
			FAIL_FRAME_SET(&funcctx->max_calls, TRF1_statements[0].sql);
		}

		snprintf(values[i_num_found], INTEGER_LEN, "%d", num_found);

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
		for (i = 0; i < 12; i++) {
			elog(DEBUG1, "TRF1 OUT: %d %s", i, values[i]);
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

/* Clause 3.3.8.4 */
Datum
TradeResultFrame2(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

	int i, n;

	char **values = NULL;

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		enum trf2
		{
			i_broker_id = 0,
			i_buy_value,
			i_cust_id,
			i_sell_value,
			i_tax_status,
			i_trade_dts
		};

		long acct_id = PG_GETARG_INT64(0);
		int hs_qty = PG_GETARG_INT32(1);
		int is_lifo = PG_GETARG_INT16(2);
		char *symbol_p = (char *) PG_GETARG_TEXT_P(3);
		long trade_id = PG_GETARG_INT64(4);
		Numeric trade_price_num = PG_GETARG_NUMERIC(5);
		int trade_qty = PG_GETARG_INT32(6);
		int type_is_sell = PG_GETARG_INT16(7);

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;

		char sql[2048];
		Datum args[6];
		char nulls[6] = { ' ', ' ', ' ', ' ', ' ', ' ' };

		char symbol[S_SYMB_LEN + 1];
		double trade_price;
		int needed_qty = trade_qty;

		double buy_value = 0;
		double sell_value = 0;

		strncpy(symbol,
				DatumGetCString(DirectFunctionCall1(
						textout, PointerGetDatum(symbol_p))),
				S_SYMB_LEN);
		symbol[S_SYMB_LEN] = '\0';
		trade_price = DatumGetFloat8(DirectFunctionCall1(
				numeric_float8_no_overflow, PointerGetDatum(trade_price_num)));

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 6);
		values[i_buy_value]
				= (char *) palloc((S_PRICE_T_LEN + 1) * sizeof(char));
		values[i_sell_value]
				= (char *) palloc((S_PRICE_T_LEN + 1) * sizeof(char));

#ifdef DEBUG
		dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol, trade_id,
				trade_price, trade_qty, type_is_sell);
#endif

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();
		plan_queries(TRF2_statements);

		strncpy(sql, "SELECT now()::timestamp(0)", 27);
#ifdef DEBUG
		elog(DEBUG1, "%s", sql);
#endif /* DEBUG */
		ret = SPI_exec(sql, 0);
		if (ret == SPI_OK_SELECT && SPI_processed == 1) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			values[i_trade_dts] = SPI_getvalue(tuple, tupdesc, 1);
		} else {
			FAIL_FRAME_SET(&funcctx->max_calls, sql);
		}

#ifdef DEBUG
		elog(DEBUG1, "%s", SQLTRF2_1);
#endif /* DEBUG */
		args[0] = Int64GetDatum(acct_id);
		ret = SPI_execute_plan(TRF2_1, args, nulls, false, 0);
		if (ret == SPI_OK_SELECT) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			if (SPI_processed > 0) {
				tuple = tuptable->vals[0];
				values[i_broker_id] = SPI_getvalue(tuple, tupdesc, 1);
				values[i_cust_id] = SPI_getvalue(tuple, tupdesc, 2);
				values[i_tax_status] = SPI_getvalue(tuple, tupdesc, 3);
			}
		} else {
#ifdef DEBUG
			dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol, trade_id,
					trade_price, trade_qty, type_is_sell);
#endif /* DEBUG */
			FAIL_FRAME_SET(&funcctx->max_calls, TRF2_statements[0].sql);
		}

		/* Determine if sell or buy order */
		if (type_is_sell == 1) {
			if (hs_qty == 0) {
				/* no prior holdings exist, but one will be inserted */
#ifdef DEBUG
				elog(DEBUG1, "%s", SQLTRF2_2a);
#endif /* DEBUG */
				args[0] = Int64GetDatum(acct_id);
				args[1] = CStringGetTextDatum(symbol);
				args[2] = Int32GetDatum(-1 * trade_qty);
				ret = SPI_execute_plan(TRF2_2a, args, nulls, false, 0);
				if (ret != SPI_OK_INSERT) {
					FAIL_FRAME_SET(
							&funcctx->max_calls, TRF2_statements[1].sql);
#ifdef DEBUG
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
#endif /* DEBUG */
				}
			} else if (hs_qty != trade_qty) {
#ifdef DEBUG
				elog(DEBUG1, "%s", SQLTRF2_2b);
#endif /* DEBUG */
				args[0] = Int32GetDatum(hs_qty - trade_qty);
				args[1] = Int64GetDatum(acct_id);
				args[2] = CStringGetTextDatum(symbol);
				ret = SPI_execute_plan(TRF2_2b, args, nulls, false, 0);
				if (ret != SPI_OK_UPDATE) {
					FAIL_FRAME_SET(
							&funcctx->max_calls, TRF2_statements[2].sql);
#ifdef DEBUG
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
#endif /* DEBUG */
				}
			}

			/* Sell Trade: */

			/* First look for existing holdings */
			if (hs_qty > 0) {
				args[0] = Int64GetDatum(acct_id);
				args[1] = CStringGetTextDatum(symbol);
				if (is_lifo == 1) {
#ifdef DEBUG
					elog(DEBUG1, "%s", SQLTRF2_3a);
#endif /* DEBUG */
					ret = SPI_execute_plan(TRF2_3a, args, nulls, false, 0);
				} else {
#ifdef DEBUG
					elog(DEBUG1, "%s", SQLTRF2_3b);
#endif /* DEBUG */
					ret = SPI_execute_plan(TRF2_3b, args, nulls, false, 0);
				}
				if (ret != SPI_OK_SELECT) {
					FAIL_FRAME_SET(&funcctx->max_calls,
							is_lifo == 1 ? TRF2_statements[3].sql
										 : TRF2_statements[4].sql);
#ifdef DEBUG
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
#endif /* DEBUG */
				}

				/*
				 * Liquidate existing holdings.  Note that more than
				 * 1 HOLDING record acn be deleted here since customer
				 * may have the same security with differeing prices.
				 */

				i = 0;
				tupdesc = SPI_tuptable->tupdesc;
				tuptable = SPI_tuptable;
				n = SPI_processed;
				while (needed_qty > 0 && i < n) {
					long hold_id;
					int hold_qty;
					double hold_price;

					tuple = tuptable->vals[i++];
					hold_id = atol(SPI_getvalue(tuple, tupdesc, 1));
					hold_qty = atoi(SPI_getvalue(tuple, tupdesc, 2));
					hold_price = atof(SPI_getvalue(tuple, tupdesc, 3));

					if (hold_qty > needed_qty) {
						/* Selling some of the holdings */
#ifdef DEBUG
						elog(DEBUG1, "%s", SQLTRF2_4a);
#endif /* DEBUG */
						args[0] = Int64GetDatum(hold_id);
						args[1] = Int64GetDatum(trade_id);
						args[2] = Int32GetDatum(hold_qty);
						args[3] = Int32GetDatum(hold_qty - needed_qty);
						ret = SPI_execute_plan(TRF2_4a, args, nulls, false, 0);
						if (ret != SPI_OK_INSERT) {
							FAIL_FRAME_SET(&funcctx->max_calls,
									TRF2_statements[5].sql);
#ifdef DEBUG
							dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
									trade_id, trade_price, trade_qty,
									type_is_sell);
#endif /* DEBUG */
						}

#ifdef DEBUG
						elog(DEBUG1, "%s", SQLTRF2_5a);
#endif /* DEBUG */
						args[0] = Int32GetDatum(hold_qty - needed_qty);
						args[1] = Int64GetDatum(hold_id);
						ret = SPI_execute_plan(TRF2_5a, args, nulls, false, 0);
						if (ret != SPI_OK_UPDATE) {
							FAIL_FRAME_SET(&funcctx->max_calls,
									TRF2_statements[6].sql);
#ifdef DEBUG
							dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
									trade_id, trade_price, trade_qty,
									type_is_sell);
#endif /* DEBUG */
						}

						buy_value += (double) needed_qty * hold_price;
						sell_value += (double) needed_qty * trade_price;
						needed_qty = 0;
					} else {
						/* Selling all holdings */
#ifdef DEBUG
						elog(DEBUG1, "%s", SQLTRF2_4a);
#endif /* DEBUG */
						args[0] = Int64GetDatum(hold_id);
						args[1] = Int64GetDatum(trade_id);
						args[2] = Int32GetDatum(hold_qty);
						args[3] = Int32GetDatum(0);
						ret = SPI_execute_plan(TRF2_4a, args, nulls, false, 0);
						if (ret != SPI_OK_INSERT) {
							FAIL_FRAME_SET(&funcctx->max_calls,
									TRF2_statements[5].sql);
#ifdef DEBUG
							dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
									trade_id, trade_price, trade_qty,
									type_is_sell);
#endif /* DEBUG */
						}

#ifdef DEBUG
						elog(DEBUG1, "%s", SQLTRF2_5b);
#endif /* DEBUG */
						args[0] = Int64GetDatum(hold_id);
						ret = SPI_execute_plan(TRF2_5b, args, nulls, false, 0);
						if (ret != SPI_OK_DELETE) {
							FAIL_FRAME_SET(&funcctx->max_calls,
									TRF2_statements[7].sql);
#ifdef DEBUG
							dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
									trade_id, trade_price, trade_qty,
									type_is_sell);
#endif /* DEBUG */
						}
						sell_value += (double) hold_qty * hold_price;
						buy_value += (double) hold_qty * trade_price;
						needed_qty -= hold_qty;
					}
				}
			}

			/*
			 * Sell short:
			 * If needed_qty > 0 then customer has sold all existing
			 * holdings and customer is selling short.  A new HOLDING
			 * record will be created with H_QTY set to the negative
			 * number of needed shares.
			 */

			if (needed_qty > 0) {
				struct tm tm;
				struct pg_tm tt;
				Timestamp dt;

#ifdef DEBUG
				elog(DEBUG1, "%s", SQLTRF2_4a);
#endif /* DEBUG */
				args[0] = Int64GetDatum(trade_id);
				args[1] = Int64GetDatum(trade_id);
				args[2] = Int32GetDatum(0);
				args[3] = Int32GetDatum(-1 * needed_qty);
				ret = SPI_execute_plan(TRF2_4a, args, nulls, false, 0);
				if (ret != SPI_OK_INSERT) {
#ifdef DEBUG
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
#endif /* DEBUG */
					FAIL_FRAME_SET(
							&funcctx->max_calls, TRF2_statements[5].sql);
				}
#ifdef DEBUG
				elog(DEBUG1, "%s", SQLTRF2_7a);
#endif /* DEBUG */
				args[0] = Int64GetDatum(trade_id);
				args[1] = Int64GetDatum(acct_id);
				args[2] = CStringGetTextDatum(symbol);

				strptime(values[i_trade_dts], "%Y-%m-%d %H:%M:%S", &tm);
				tt.tm_year = tm.tm_year - 1900;
				tt.tm_mon = tm.tm_mon - 1;
				tt.tm_mday = tm.tm_mday;
				tt.tm_hour = tm.tm_hour;
				tt.tm_min = tm.tm_min;
				tt.tm_sec = tm.tm_sec;
				tt.tm_isdst = tm.tm_isdst;
				tt.tm_gmtoff = tm.tm_gmtoff;
				tt.tm_zone = tm.tm_zone;
				tm2timestamp(&tt, 0, NULL, &dt);
				args[3] = TimestampGetDatum(dt);

				args[4] = Float8GetDatum(trade_price);
				args[5] = Int32GetDatum(-1 * needed_qty);
				ret = SPI_execute_plan(TRF2_7a, args, nulls, false, 0);
				if (ret != SPI_OK_INSERT) {
#ifdef DEBUG
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
#endif /* DEBUG */
					FAIL_FRAME_SET(
							&funcctx->max_calls, TRF2_statements[7].sql);
				}
			} else if (hs_qty == trade_qty) {
#ifdef DEBUG
				elog(DEBUG1, "%s", SQLTRF2_7b);
#endif /* DEBUG */
				args[0] = Int64GetDatum(acct_id);
				args[1] = CStringGetTextDatum(symbol);
				ret = SPI_execute_plan(TRF2_7b, args, nulls, false, 0);
				if (ret != SPI_OK_DELETE) {
#ifdef DEBUG
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
#endif /* DEBUG */
					FAIL_FRAME_SET(
							&funcctx->max_calls, TRF2_statements[9].sql);
				}
			}
		} else {
			/* The trade is a BUY */
			if (hs_qty == 0) {
				/* no prior holdings exists, but one will be inserted */
#ifdef DEBUG
				elog(DEBUG1, "%s", SQLTRF2_8a);
#endif /* DEBUG */
				args[0] = Int64GetDatum(acct_id);
				args[1] = CStringGetTextDatum(symbol);
				args[2] = Int32GetDatum(trade_qty);
				ret = SPI_execute_plan(TRF2_8a, args, nulls, false, 0);
				if (ret != SPI_OK_INSERT) {
					FAIL_FRAME_SET(
							&funcctx->max_calls, TRF2_statements[10].sql);
#ifdef DEBUG
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
#endif /* DEBUG */
				}
			} else if ((-1 * hs_qty) != trade_qty) {
#ifdef DEBUG
				elog(DEBUG1, "%s", SQLTRF2_8b);
#endif /* DEBUG */
				args[0] = Int32GetDatum(hs_qty + trade_qty);
				args[1] = Int64GetDatum(acct_id);
				args[2] = CStringGetTextDatum(symbol);
				ret = SPI_execute_plan(TRF2_8b, args, nulls, false, 0);
				if (ret != SPI_OK_UPDATE) {
					FAIL_FRAME_SET(
							&funcctx->max_calls, TRF2_statements[11].sql);
#ifdef DEBUG
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
#endif /* DEBUG */
				}
			}

			/*
			 * Short Cover:
			 * First look for existing negative holdings, H_QTY < 0,
			 * which indicates a previous short sell.  The buy trade
			 * will cover the short sell.
			 */

			if (hs_qty < 0) {
				/* Could return 0, 1 or many rows */
				args[0] = Int64GetDatum(acct_id);
				args[1] = CStringGetTextDatum(symbol);
				if (is_lifo == 1) {
#ifdef DEBUG
					elog(DEBUG1, "%s", SQLTRF2_3a);
#endif /* DEBUG */
					ret = SPI_execute_plan(TRF2_3a, args, nulls, false, 0);
				} else {
#ifdef DEBUG
					elog(DEBUG1, "%s", SQLTRF2_3b);
#endif /* DEBUG */
					ret = SPI_execute_plan(TRF2_3b, args, nulls, false, 0);
				}
				if (ret != SPI_OK_SELECT) {
					FAIL_FRAME_SET(&funcctx->max_calls,
							is_lifo == 1 ? TRF2_statements[3].sql
										 : TRF2_statements[4].sql);
#ifdef DEBUG
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
#endif /* DEBUG */
				}

				/* Buy back securities to cover a short position. */

				tupdesc = SPI_tuptable->tupdesc;
				tuptable = SPI_tuptable;
				i = 0;
				n = SPI_processed;
				while (needed_qty > 0 && i < n) {
					long hold_id;
					int hold_qty;
					double hold_price;

					tuple = tuptable->vals[i++];
					hold_id = atol(SPI_getvalue(tuple, tupdesc, 1));
					hold_qty = atoi(SPI_getvalue(tuple, tupdesc, 2));
					hold_price = atof(SPI_getvalue(tuple, tupdesc, 3));

					if (hold_qty + needed_qty < 0) {
						/* Buying back some of the Short Sell */
#ifdef DEBUG
						elog(DEBUG1, "%s", SQLTRF2_4a);
#endif /* DEBUG */
						args[0] = Int64GetDatum(hold_id);
						args[1] = Int64GetDatum(trade_id);
						args[2] = Int32GetDatum(hold_qty);
						args[3] = Int32GetDatum(hold_qty + needed_qty);
						ret = SPI_execute_plan(TRF2_4a, args, nulls, false, 0);
						if (ret != SPI_OK_INSERT) {
							FAIL_FRAME_SET(&funcctx->max_calls,
									TRF2_statements[5].sql);
#ifdef DEBUG
							dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
									trade_id, trade_price, trade_qty,
									type_is_sell);
#endif /* DEBUG */
						}

#ifdef DEBUG
						elog(DEBUG1, "%s", SQLTRF2_5a);
#endif /* DEBUG */
						args[0] = Int32GetDatum(hold_qty + needed_qty);
						args[1] = Int64GetDatum(hold_id);
						ret = SPI_execute_plan(TRF2_5a, args, nulls, false, 0);
						if (ret != SPI_OK_UPDATE) {
							FAIL_FRAME_SET(&funcctx->max_calls,
									TRF2_statements[6].sql);
#ifdef DEBUG
							dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
									trade_id, trade_price, trade_qty,
									type_is_sell);
#endif /* DEBUG */
						}

						buy_value += (double) needed_qty * hold_price;
						sell_value += (double) needed_qty * trade_price;
						needed_qty = 0;
					} else {
						/* Buying back all of the Short Sell */
#ifdef DEBUG
						elog(DEBUG1, "%s", SQLTRF2_4a);
#endif /* DEBUG */
						args[0] = Int64GetDatum(hold_id);
						args[1] = Int64GetDatum(trade_id);
						args[2] = Int32GetDatum(hold_qty);
						args[3] = Int32GetDatum(0);
						ret = SPI_execute_plan(TRF2_4a, args, nulls, false, 0);
						if (ret != SPI_OK_INSERT) {
							FAIL_FRAME_SET(&funcctx->max_calls,
									TRF2_statements[5].sql);
#ifdef DEBUG
							dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
									trade_id, trade_price, trade_qty,
									type_is_sell);
#endif /* DEBUG */
						}

#ifdef DEBUG
						elog(DEBUG1, "%s", SQLTRF2_5b);
#endif /* DEBUG */
						args[0] = Int64GetDatum(hold_id);
						ret = SPI_execute_plan(TRF2_5b, args, nulls, false, 0);
						if (ret != SPI_OK_DELETE) {
							FAIL_FRAME_SET(&funcctx->max_calls,
									TRF2_statements[7].sql);
#ifdef DEBUG
							dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
									trade_id, trade_price, trade_qty,
									type_is_sell);
#endif /* DEBUG */
						}
						hold_qty *= -1;
						sell_value += (double) hold_qty * hold_price;
						buy_value += (double) hold_qty * trade_price;
						needed_qty = needed_qty - hold_qty;
					}
				}
			}

			/*
			 * Buy Trade:
			 * If needed_qty > 0, then the customer has covered all
			 * previous Short Sells and the customer is buying new
			 * holdings.  A new HOLDING record will be created with
			 * H_QTY set to the number of needed shares.
			 */

			if (needed_qty > 0) {
				struct tm tm;
				struct pg_tm tt;
				Timestamp dt;

#ifdef DEBUG
				elog(DEBUG1, "%s", SQLTRF2_4a);
#endif /* DEBUG */
				args[0] = Int64GetDatum(trade_id);
				args[1] = Int64GetDatum(trade_id);
				args[2] = Int32GetDatum(0);
				args[3] = Int32GetDatum(needed_qty);
				ret = SPI_execute_plan(TRF2_4a, args, nulls, false, 0);
				if (ret != SPI_OK_INSERT) {
					FAIL_FRAME_SET(
							&funcctx->max_calls, TRF2_statements[5].sql);
#ifdef DEBUG
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
#endif /* DEBUG */
				}
#ifdef DEBUG
				elog(DEBUG1, "%s", SQLTRF2_7a);
#endif /* DEBUG */
				args[0] = Int64GetDatum(trade_id);
				args[1] = Int64GetDatum(acct_id);
				args[2] = CStringGetTextDatum(symbol);

				strptime(values[i_trade_dts], "%Y-%m-%d %H:%M:%S", &tm);
				tt.tm_year = tm.tm_year - 1900;
				tt.tm_mon = tm.tm_mon - 1;
				tt.tm_mday = tm.tm_mday;
				tt.tm_hour = tm.tm_hour;
				tt.tm_min = tm.tm_min;
				tt.tm_sec = tm.tm_sec;
				tt.tm_isdst = tm.tm_isdst;
				tt.tm_gmtoff = tm.tm_gmtoff;
				tt.tm_zone = tm.tm_zone;
				tm2timestamp(&tt, 0, NULL, &dt);
				args[3] = TimestampGetDatum(dt);

				args[4] = Float8GetDatum(trade_price);
				args[5] = Int32GetDatum(needed_qty);
				ret = SPI_execute_plan(TRF2_7a, args, nulls, false, 0);
				if (ret != SPI_OK_INSERT) {
#ifdef DEBUG
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
#endif /* DEBUG */
					FAIL_FRAME_SET(
							&funcctx->max_calls, TRF2_statements[7].sql);
				}
			} else if ((-1 * hs_qty) == trade_qty) {
#ifdef DEBUG
				elog(DEBUG1, "%s", SQLTRF2_7b);
#endif /* DEBUG */
				args[0] = Int64GetDatum(acct_id);
				args[1] = CStringGetTextDatum(symbol);
				ret = SPI_execute_plan(TRF2_7b, args, nulls, false, 0);
				if (ret != SPI_OK_DELETE) {
#ifdef DEBUG
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
#endif /* DEBUG */
					FAIL_FRAME_SET(
							&funcctx->max_calls, TRF2_statements[9].sql);
				}
			}
		}

		snprintf(values[i_buy_value], S_PRICE_T_LEN, "%.2f", buy_value);
		snprintf(values[i_sell_value], S_PRICE_T_LEN, "%.2f", sell_value);

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
		for (i = 0; i < 6; i++) {
			elog(DEBUG1, "TRF2 OUT: %d %s", i, values[i]);
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

/* Clause 3.3.8.5 */
Datum
TradeResultFrame3(PG_FUNCTION_ARGS)
{
	Numeric buy_value_num = PG_GETARG_NUMERIC(0);
	long cust_id = PG_GETARG_INT64(1);
	Numeric sell_value_num = PG_GETARG_NUMERIC(2);
	long trade_id = PG_GETARG_INT64(3);

	int ret;
	TupleDesc tupdesc;
	SPITupleTable *tuptable = NULL;
	HeapTuple tuple = NULL;

	Numeric result;
	double buy_value;
	double sell_value;

	double tax_amount = 0;
	Datum args[2];
	char nulls[2] = { ' ', ' ' };

	buy_value = DatumGetFloat8(DirectFunctionCall1(
			numeric_float8_no_overflow, PointerGetDatum(buy_value_num)));
	sell_value = DatumGetFloat8(DirectFunctionCall1(
			numeric_float8_no_overflow, PointerGetDatum(sell_value_num)));

#ifdef DEBUG
	dump_trf3_inputs(buy_value, cust_id, sell_value, trade_id);
#endif

	SPI_connect();
	plan_queries(TRF3_statements);
#ifdef DEBUG
	elog(DEBUG1, "%s", SQLTRF3_1);
#endif /* DEBUG */
	args[0] = Int64GetDatum(cust_id);
	ret = SPI_execute_plan(TRF3_1, args, nulls, true, 0);
	if (ret == SPI_OK_SELECT) {
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		if (SPI_processed > 0) {
			double tax_rates;
			tuple = tuptable->vals[0];
			tax_rates = atof(SPI_getvalue(tuple, tupdesc, 1));
			tax_amount = (sell_value - buy_value) * tax_rates;
		}
	} else {
		FAIL_FRAME(TRF3_statements[0].sql);
#ifdef DEBUG
		dump_trf3_inputs(buy_value, cust_id, sell_value, trade_id);
#endif /* DEBUG */
	}

#ifdef DEBUG
	elog(DEBUG1, "%s", SQLTRF3_2);
#endif /* DEBUG */
	args[0] = Float8GetDatum(tax_amount);
	args[1] = Int64GetDatum(trade_id);
	ret = SPI_execute_plan(TRF3_2, args, nulls, false, 0);
	if (ret != SPI_OK_UPDATE) {
		FAIL_FRAME(TRF3_statements[1].sql);
#ifdef DEBUG
		dump_trf3_inputs(buy_value, cust_id, sell_value, trade_id);
#endif /* DEBUG */
	}

#ifdef DEBUG
	elog(DEBUG1, "TRF3 OUT: 1 %f", tax_amount);
#endif /* DEBUG */

	SPI_finish();
	result = DatumGetNumeric(
			DirectFunctionCall1(float8_numeric, Float8GetDatum(tax_amount)));
	PG_RETURN_NUMERIC(result);
}

/* Clause 3.3.8.6 */
Datum
TradeResultFrame4(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	AttInMetadata *attinmeta;
	int call_cntr;
	int max_calls;

#ifdef DEBUG
	int i;
#endif /* DEBUG */

	char **values = NULL;

	/* Stuff done only on the first call of the function. */
	if (SRF_IS_FIRSTCALL()) {
		MemoryContext oldcontext;

		enum trf4
		{
			i_comm_rate = 0,
			i_s_name
		};

		long cust_id = PG_GETARG_INT64(0);
		char *symbol_p = (char *) PG_GETARG_TEXT_P(1);
		int trade_qty = PG_GETARG_INT32(2);
		char *type_id_p = (char *) PG_GETARG_TEXT_P(3);

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;
		Datum args[5];
		char nulls[5] = { ' ', ' ', ' ', ' ', ' ' };

		char symbol[S_SYMB_LEN + 1];
		char type_id[TT_ID_LEN + 1];

		char *s_ex_id = NULL;
		char *c_tier = NULL;

		strncpy(symbol,
				DatumGetCString(DirectFunctionCall1(
						textout, PointerGetDatum(symbol_p))),
				S_SYMB_LEN);
		symbol[S_SYMB_LEN] = '\0';
		strncpy(type_id,
				DatumGetCString(DirectFunctionCall1(
						textout, PointerGetDatum(type_id_p))),
				TT_ID_LEN);
		type_id[TT_ID_LEN] = '\0';

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 2);

#ifdef DEBUG
		dump_trf4_inputs(cust_id, symbol, trade_qty, type_id);
#endif

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();
		plan_queries(TRF4_statements);
#ifdef DEBUG
		elog(DEBUG1, "%s", SQLTRF4_1);
#endif /* DEBUG */
		args[0] = CStringGetTextDatum(symbol);
		ret = SPI_execute_plan(TRF4_1, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			s_ex_id = SPI_getvalue(tuple, tupdesc, 1);
			values[i_s_name] = SPI_getvalue(tuple, tupdesc, 2);
		} else {
			FAIL_FRAME_SET(&funcctx->max_calls, TRF4_statements[0].sql);
#ifdef DEBUG
			dump_trf4_inputs(cust_id, symbol, trade_qty, type_id);
#endif /* DEBUG */
		}

#ifdef DEBUG
		elog(DEBUG1, "%s", SQLTRF4_2);
#endif /* DEBUG */
		args[0] = Int64GetDatum(cust_id);
		ret = SPI_execute_plan(TRF4_2, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			c_tier = SPI_getvalue(tuple, tupdesc, 1);
		} else {
#ifdef DEBUG
			dump_trf4_inputs(cust_id, symbol, trade_qty, type_id);
#endif /* DEBUG */
			FAIL_FRAME_SET(&funcctx->max_calls, TRF4_statements[1].sql);
		}

#ifdef DEBUG
		elog(DEBUG1, "%s", SQLTRF4_3);
#endif /* DEBUG */
		args[0] = Int16GetDatum(atoi(c_tier));
		args[1] = CStringGetTextDatum(type_id);
		args[2] = CStringGetTextDatum(s_ex_id);
		args[3] = Int32GetDatum(trade_qty);
		args[4] = Int32GetDatum(trade_qty);
		ret = SPI_execute_plan(TRF4_3, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			values[i_comm_rate] = SPI_getvalue(tuple, tupdesc, 1);
		} else {
#ifdef DEBUG
			dump_trf4_inputs(cust_id, symbol, trade_qty, type_id);
#endif /* DEBUG */
			FAIL_FRAME_SET(&funcctx->max_calls, TRF4_statements[2].sql);
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
		for (i = 0; i < 2; i++) {
			elog(DEBUG1, "TRF4 OUT: %d %s", i, values[i]);
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

/* Clause 3.3.8.7 */
Datum
TradeResultFrame5(PG_FUNCTION_ARGS)
{
	long broker_id = PG_GETARG_INT64(0);
	Numeric comm_amount_num = PG_GETARG_NUMERIC(1);
	char *st_completed_id_p = (char *) PG_GETARG_TEXT_P(2);
	Timestamp trade_dts_ts = PG_GETARG_TIMESTAMP(3);
	long trade_id = PG_GETARG_INT64(4);
	Numeric trade_price_num = PG_GETARG_NUMERIC(5);

	int ret;

	struct pg_tm tt, *tm = &tt;
	fsec_t fsec;
	char *tzn = NULL;
	Datum args[5];
	char nulls[5] = { ' ', ' ', ' ', ' ', ' ' };

	double comm_amount;
#ifdef DEBUG
	double trade_price;
#endif
	char trade_dts[MAXDATELEN + 1];
	char st_completed_id[ST_ID_LEN + 1];

	strncpy(st_completed_id,
			DatumGetCString(DirectFunctionCall1(
					textout, PointerGetDatum(st_completed_id_p))),
			ST_ID_LEN);
	st_completed_id[ST_ID_LEN] = '\0';

	comm_amount = DatumGetFloat8(DirectFunctionCall1(
			numeric_float8_no_overflow, PointerGetDatum(comm_amount_num)));
#ifdef DEBUG
	trade_price = DatumGetFloat8(DirectFunctionCall1(
			numeric_float8_no_overflow, PointerGetDatum(trade_price_num)));
#endif

	if (timestamp2tm(trade_dts_ts, NULL, tm, &fsec, NULL, NULL) == 0) {
		EncodeDateTimeM(tm, fsec, tzn, trade_dts);
	}

#ifdef DEBUG
	dump_trf5_inputs(broker_id, comm_amount, st_completed_id, trade_dts,
			trade_id, trade_price);
#endif

	SPI_connect();
	plan_queries(TRF5_statements);
#ifdef DEBUG
	elog(DEBUG1, "%s", SQLTRF5_1);
#endif /* DEBUG */
	args[0] = PointerGetDatum(comm_amount_num);
	args[1] = TimestampGetDatum(trade_dts_ts);
	args[2] = CStringGetTextDatum(st_completed_id);
	args[3] = PointerGetDatum(trade_price_num);
	args[4] = Int64GetDatum(trade_id);
	ret = SPI_execute_plan(TRF5_1, args, nulls, false, 0);
	if (ret != SPI_OK_UPDATE) {
		FAIL_FRAME(TRF5_statements[0].sql);
#ifdef DEBUG
		dump_trf5_inputs(broker_id, comm_amount, st_completed_id, trade_dts,
				trade_id, trade_price);
#endif /* DEBUG */
	}

#ifdef DEBUG
	elog(DEBUG1, "%s", SQLTRF5_2);
#endif /* DEBUG */
	args[0] = Int64GetDatum(trade_id);
	args[1] = TimestampGetDatum(trade_dts_ts);
	args[2] = CStringGetTextDatum(st_completed_id);
	ret = SPI_execute_plan(TRF5_2, args, nulls, false, 0);
	if (ret != SPI_OK_INSERT) {
		FAIL_FRAME(TRF5_statements[1].sql);
#ifdef DEBUG
		dump_trf5_inputs(broker_id, comm_amount, st_completed_id, trade_dts,
				trade_id, trade_price);
#endif /* DEBUG */
	}

#ifdef DEBUG
	elog(DEBUG1, "%s", SQLTRF5_3);
#endif /* DEBUG */
	args[0] = Float8GetDatum(comm_amount);
	args[1] = Int64GetDatum(broker_id);
	ret = SPI_execute_plan(TRF5_3, args, nulls, false, 0);
	if (ret != SPI_OK_UPDATE) {
		FAIL_FRAME(TRF5_statements[2].sql);
#ifdef DEBUG
		dump_trf5_inputs(broker_id, comm_amount, st_completed_id, trade_dts,
				trade_id, trade_price);
#endif /* DEBUG */
	}

	SPI_finish();
	PG_RETURN_INT32(0);
}

/* Clause 3.3.8.8 */
Datum
TradeResultFrame6(PG_FUNCTION_ARGS)
{
	long acct_id = PG_GETARG_INT64(0);
	Timestamp due_date_ts = PG_GETARG_TIMESTAMP(1);
	char *s_name_p = (char *) PG_GETARG_TEXT_P(2);
	Numeric se_amount_num = PG_GETARG_NUMERIC(3);
	Timestamp trade_dts_ts = PG_GETARG_TIMESTAMP(4);
	long trade_id = PG_GETARG_INT64(5);
	int trade_is_cash = PG_GETARG_INT16(6);
	int trade_qty = PG_GETARG_INT32(7);
	char *type_name_p = (char *) PG_GETARG_TEXT_P(8);

	struct pg_tm tt, *tm = &tt;
	fsec_t fsec;
	char *tzn = NULL;

	int ret;
	TupleDesc tupdesc;
	SPITupleTable *tuptable = NULL;
	HeapTuple tuple = NULL;

	Numeric result;
	Datum args[6];
	char nulls[6] = { ' ', ' ', ' ', ' ', ' ', ' ' };

	char s_name[2 * S_NAME_LEN + 1];
	char *s_name_tmp;
	char type_name[TT_NAME_LEN + 1];
	double se_amount;

	char due_date[MAXDATELEN + 1];
	char trade_dts[MAXDATELEN + 1];

	char cash_type[41];

	double acct_bal = 0;

	int i;
	int k = 0;

	se_amount = DatumGetFloat8(DirectFunctionCall1(
			numeric_float8_no_overflow, PointerGetDatum(se_amount_num)));

	s_name_tmp = DatumGetCString(
			DirectFunctionCall1(textout, PointerGetDatum(s_name_p)));

	for (i = 0; i < S_NAME_LEN && s_name_tmp[i] != '\0'; i++) {
		if (s_name_tmp[i] == '\'')
			s_name[k++] = '\\';
		s_name[k++] = s_name_tmp[i];
	}
	s_name[k] = '\0';
	s_name[S_NAME_LEN] = '\0';

	strncpy(type_name,
			DatumGetCString(DirectFunctionCall1(
					textout, PointerGetDatum(type_name_p))),
			TT_NAME_LEN);
	type_name[TT_NAME_LEN] = '\0';

	if (timestamp2tm(due_date_ts, NULL, tm, &fsec, NULL, NULL) == 0) {
		EncodeDateTimeM(tm, fsec, tzn, due_date);
	}
	if (timestamp2tm(trade_dts_ts, NULL, tm, &fsec, NULL, NULL) == 0) {
		EncodeDateTimeM(tm, fsec, tzn, trade_dts);
	}

#ifdef DEBUG
	dump_trf6_inputs(acct_id, due_date, s_name, se_amount, trade_dts, trade_id,
			trade_is_cash, trade_qty, type_name);
#endif

	SPI_connect();
	plan_queries(TRF6_statements);

	if (trade_is_cash == 1) {
		strncpy(cash_type, "Cash Account", sizeof(cash_type));
	} else {
		strncpy(cash_type, "Margin", sizeof(cash_type));
	}

#ifdef DEBUG
	elog(DEBUG1, "%s", SQLTRF6_1);
	elog(DEBUG1, "$1 %ld", trade_id);
	elog(DEBUG1, "$2 %s", cash_type);
	elog(DEBUG1, "$3 %s", due_date);
	elog(DEBUG1, "$4 %f", se_amount);
#endif /* DEBUG */
	args[0] = Int64GetDatum(trade_id);
	args[1] = CStringGetTextDatum(cash_type);
	args[2] = DirectFunctionCall1(date_in, CStringGetDatum(due_date));
	args[3] = Float8GetDatum(se_amount);
	ret = SPI_execute_plan(TRF6_1, args, nulls, false, 0);
	if (ret != SPI_OK_INSERT) {
		FAIL_FRAME(TRF6_statements[0].sql);
#ifdef DEBUG
		dump_trf6_inputs(acct_id, due_date, s_name, se_amount, trade_dts,
				trade_id, trade_is_cash, trade_qty, type_name);
#endif /* DEBUG */
	}

	if (trade_is_cash == 1) {
#ifdef DEBUG
		elog(DEBUG1, "%s", SQLTRF6_2);
		elog(DEBUG1, "$1 %f", se_amount);
		elog(DEBUG1, "$2 %ld", acct_id);
#endif /* DEBUG */
		args[0] = Float8GetDatum(se_amount);
		args[1] = Int64GetDatum(acct_id);
		ret = SPI_execute_plan(TRF6_2, args, nulls, false, 0);
		if (ret != SPI_OK_UPDATE) {
			FAIL_FRAME(TRF6_statements[1].sql);
#ifdef DEBUG
			dump_trf6_inputs(acct_id, due_date, s_name, se_amount, trade_dts,
					trade_id, trade_is_cash, trade_qty, type_name);
#endif /* DEBUG */
		}
#ifdef DEBUG
		elog(DEBUG1, "%s", SQLTRF6_3);
		elog(DEBUG1, "$1 %ld", trade_dts_ts);
		elog(DEBUG1, "$2 %ld", trade_id);
		elog(DEBUG1, "$3 %f", se_amount);
		elog(DEBUG1, "$4 %s", type_name);
		elog(DEBUG1, "$5 %d", trade_qty);
		elog(DEBUG1, "$6 %s", s_name);
#endif /* DEBUG */
		args[0] = TimestampGetDatum(trade_dts_ts);
		args[1] = Int64GetDatum(trade_id);
		args[2] = Float8GetDatum(se_amount);
		args[3] = CStringGetTextDatum(type_name);
		args[4] = Int32GetDatum(trade_qty);
		args[5] = CStringGetTextDatum(s_name);
		ret = SPI_execute_plan(TRF6_3, args, nulls, false, 0);
		if (ret != SPI_OK_INSERT) {
			FAIL_FRAME(TRF6_statements[2].sql);
#ifdef DEBUG
			dump_trf6_inputs(acct_id, due_date, s_name, se_amount, trade_dts,
					trade_id, trade_is_cash, trade_qty, type_name);
#endif /* DEBUG */
		}
	}

#ifdef DEBUG
	elog(DEBUG1, "%s", SQLTRF6_4);
	elog(DEBUG1, "$1 %ld", acct_id);
#endif /* DEBUG */
	args[0] = Int64GetDatum(acct_id);
	ret = SPI_execute_plan(TRF6_4, args, nulls, true, 0);
	if (ret == SPI_OK_SELECT && SPI_processed > 0) {
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		tuple = tuptable->vals[0];
		acct_bal = atof(SPI_getvalue(tuple, tupdesc, 1));
	} else {
#ifdef DEBUG
		dump_trf6_inputs(acct_id, due_date, s_name, se_amount, trade_dts,
				trade_id, trade_is_cash, trade_qty, type_name);
#endif /* DEBUG */
		FAIL_FRAME(TRF6_statements[3].sql);
	}

#ifdef DEBUG
	elog(DEBUG1, "TRF6 OUT: 1 %f", acct_bal);
#endif /* DEBUG */

	SPI_finish();
	result = DatumGetNumeric(
			DirectFunctionCall1(float8_numeric, Float8GetDatum(acct_bal)));
	PG_RETURN_NUMERIC(result);
}
