/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
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

#ifdef DEBUG
#define SQLTUF1_1 \
		"SELECT t_exec_name\n" \
		"FROM trade\n" \
		"WHERE t_id = %ld"

#define SQLTUF1_2a \
		"SELECT REPLACE('%s', ' X ', ' ')"

#define SQLTUF1_2b \
		"SELECT REPLACE('%s', ' ', ' X ')"

#define SQLTUF1_3 \
		"UPDATE trade\n" \
		"SET t_exec_name = '%s'\n" \
		"WHERE t_id = %ld"

#define SQLTUF1_4 \
		"SELECT t_bid_price, t_exec_name, t_is_cash, tt_is_mrkt,\n" \
		"       t_trade_price\n" \
		"FROM trade, trade_type\n" \
		"WHERE t_id = %ld\n" \
		"  AND t_tt_id = tt_id"

#define SQLTUF1_5 \
		"SELECT se_amt, se_cash_due_date, se_cash_type\n" \
		"FROM settlement\n" \
		"WHERE se_t_id = %ld"

#define SQLTUF1_6 \
		"SELECT ct_amt, ct_dts, ct_name\n" \
		"FROM cash_transaction\n" \
		"WHERE ct_t_id = %ld"

#define SQLTUF1_7 \
		"SELECT th_dts, th_st_id\n" \
		"FROM trade_history\n" \
		"WHERE th_t_id = %ld\n" \
		"ORDER BY th_dts\n" \
		"LIMIT 3"

#define SQLTUF2_1 \
		"SELECT t_bid_price, t_exec_name, t_is_cash, t_id, t_trade_price\n" \
		"FROM trade\n" \
		"WHERE t_ca_id = %ld\n" \
		"  AND t_dts >= '%s'\n" \
		"  AND t_dts <= '%s'\n" \
		"ORDER BY t_dts ASC\n" \
		"LIMIT %d"

#define SQLTUF2_2 \
		"SELECT se_cash_type\n" \
		"FROM settlement\n" \
		"WHERE se_t_id = %s"

#define SQLTUF2_3 \
		"UPDATE settlement\n" \
		"SET se_cash_type = '%s'\n" \
		"WHERE se_t_id = %s"

#define SQLTUF2_4 \
		"SELECT se_amt, se_cash_due_date, se_cash_type\n" \
		"FROM settlement\n" \
		"WHERE se_t_id = %s"

#define SQLTUF2_5 \
		"SELECT ct_amt, ct_dts, ct_name\n" \
		"FROM cash_transaction\n" \
		"WHERE ct_t_id = %s"

#define SQLTUF2_6 \
		"SELECT th_dts, th_st_id\n" \
		"FROM trade_history\n" \
		"WHERE th_t_id = %s\n" \
		"ORDER BY th_dts\n" \
		"LIMIT 3"

#define SQLTUF3_1 \
		"SELECT t_ca_id, t_exec_name, t_is_cash, t_trade_price, t_qty,\n" \
		"       s_name, t_dts, t_id, t_tt_id, tt_name\n" \
		"FROM trade, trade_type, security\n" \
		"WHERE t_s_symb = '%s'\n" \
		"  AND t_dts >= '%s'\n" \
		"  AND t_dts <= '%s'\n" \
		"  AND tt_id = t_tt_id\n" \
		"  AND s_symb = t_s_symb\n" \
		"ORDER BY t_dts ASC\n" \
		"LIMIT %d"

#define SQLTUF3_2 SQLTUF2_4

#define SQLTUF3_3 \
		"SELECT ct_name\n" \
		"FROM cash_transaction\n" \
		"WHERE ct_t_id = %s"

#define SQLTUF3_4 \
		"UPDATE cash_transaction\n" \
		"SET ct_name = e'%s'\n" \
		"WHERE ct_t_id = %s"

#define SQLTUF3_5 SQLTUF2_5

#define SQLTUF3_6 SQLTUF2_6
#endif /* End DEBUG */

#define TUF1_1  TUF1_statements[0].plan
#define TUF1_2a TUF1_statements[1].plan
#define TUF1_2b TUF1_statements[2].plan
#define TUF1_3  TUF1_statements[3].plan
#define TUF1_4  TUF1_statements[4].plan
#define TUF1_5  TUF1_statements[5].plan
#define TUF1_6  TUF1_statements[6].plan
#define TUF1_7  TUF1_statements[7].plan

#define TUF2_1  TUF2_statements[0].plan
#define TUF2_2  TUF2_statements[1].plan
#define TUF2_3  TUF2_statements[2].plan
#define TUF2_4  TUF2_statements[3].plan
#define TUF2_5  TUF2_statements[4].plan
#define TUF2_6  TUF2_statements[5].plan

#define TUF3_1  TUF3_statements[0].plan
#define TUF3_2  TUF3_statements[1].plan
#define TUF3_3  TUF3_statements[2].plan
#define TUF3_4  TUF3_statements[3].plan
#define TUF3_5  TUF3_statements[4].plan
#define TUF3_6  TUF3_statements[5].plan

static cached_statement TUF1_statements[] = {

	/* TUF1_1 */
	{
	"SELECT t_exec_name\n" \
	"FROM trade\n" \
	"WHERE t_id = $1",
	1,
	{ INT8OID }
	},

	/* TUF1_2a */
	{
	"SELECT REPLACE($1, ' X ', ' ')",
	1,
	{ TEXTOID }
	},

	/* TUF1_2b */
	{
	"SELECT REPLACE($1, ' ', ' X ')",
	1,
	{ TEXTOID }
	},

	/* TUF1_3 */
	{
	"UPDATE trade\n" \
	"SET t_exec_name = $1\n" \
	"WHERE t_id = $2",
	2,
	{ TEXTOID, INT8OID }
	},

	/* TUF1_4 */
	{
	"SELECT t_bid_price, t_exec_name, t_is_cash, tt_is_mrkt,\n" \
	"       t_trade_price\n" \
	"FROM trade, trade_type\n" \
	"WHERE t_id = $1\n" \
	"  AND t_tt_id = tt_id",
	1,
	{ INT8OID }
	},

	/* TUF1_5 */
	{
	"SELECT se_amt, se_cash_due_date, se_cash_type\n" \
	"FROM settlement\n" \
	"WHERE se_t_id = $1",
	1,
	{ INT8OID }
	},

	/* TUF1_6 */
	{
	"SELECT ct_amt, ct_dts, ct_name\n" \
	"FROM cash_transaction\n" \
	"WHERE ct_t_id = $1",
	1,
	{ INT8OID }
	},

	/* TUF1_7 */
	{
	"SELECT th_dts, th_st_id\n" \
	"FROM trade_history\n" \
	"WHERE th_t_id = $1\n" \
	"ORDER BY th_dts\n" \
	"LIMIT 3",
	1,
	{ INT8OID }
	},

	{ NULL }
}; /* End TUF1_statments */

static cached_statement TUF2_statements[] = {
	/* TUF2_1 */
	{
	"SELECT t_bid_price, t_exec_name, t_is_cash, t_id, t_trade_price\n" \
	"FROM trade\n" \
	"WHERE t_ca_id = $1\n" \
	"  AND t_dts >= $2\n" \
	"  AND t_dts <= $3\n" \
	"ORDER BY t_dts ASC\n" \
	"LIMIT $4",
	4,
	{ INT8OID, TIMESTAMPOID, TIMESTAMPOID, INT4OID }
	},

	/* TUF2_2 */
	{
	"SELECT se_cash_type\n" \
	"FROM settlement\n" \
	"WHERE se_t_id = $1",
	1,
	{ INT8OID }
	},

	/* TUF2_3 */
	{
	"UPDATE settlement\n" \
	"SET se_cash_type = $1\n" \
	"WHERE se_t_id = $2",
	2,
	{ TEXTOID, INT8OID }
	},

	/* TUF2_4 */
	{
	"SELECT se_amt, se_cash_due_date, se_cash_type\n" \
	"FROM settlement\n" \
	"WHERE se_t_id = $1",
	1,
	{ INT8OID }
	},

	/* TUF2_5 */
	{
	"SELECT ct_amt, ct_dts, ct_name\n" \
	"FROM cash_transaction\n" \
	"WHERE ct_t_id = $1",
	1,
	{ INT8OID }
	},

	/* TUF2_6 */
	{
	"SELECT th_dts, th_st_id\n" \
	"FROM trade_history\n" \
	"WHERE th_t_id = $1\n" \
	"ORDER BY th_dts\n" \
	"LIMIT 3",
	1,
	{ INT8OID }
	},

	{ NULL }
}; /* End TUF2_statements */

static cached_statement TUF3_statements[] = {
	/* TUF3_1 */
	{
	"SELECT t_ca_id, t_exec_name, t_is_cash, t_trade_price, t_qty,\n" \
	"       s_name, t_dts, t_id, t_tt_id, tt_name\n" \
	"FROM trade, trade_type, security\n" \
	"WHERE t_s_symb = $1\n" \
	"  AND t_dts >= $2\n" \
	"  AND t_dts <= $3\n" \
	"  AND tt_id = t_tt_id\n" \
	"  AND s_symb = t_s_symb\n" \
	"ORDER BY t_dts ASC\n" \
	"LIMIT $4",
	4,
	{ TEXTOID, TIMESTAMPOID, TIMESTAMPOID, INT4OID }
	},

	/* TUF3_2 TUF2_4 */
	{
	"SELECT se_amt, se_cash_due_date, se_cash_type\n" \
	"FROM settlement\n" \
	"WHERE se_t_id = $1",
	1,
	{ INT8OID }
	},

	/* TUF3_3 */
	{
	"SELECT ct_name\n" \
	"FROM cash_transaction\n" \
	"WHERE ct_t_id = $1",
	1,
	{ INT8OID }
	},

	/* TUF3_4 */
	{
	"UPDATE cash_transaction\n" \
	"SET ct_name = $1\n" \
	"WHERE ct_t_id = $2",
	2,
	{ TEXTOID, INT8OID }
	},

	/* TUF3_5 TUF2_5 */
	{
	"SELECT ct_amt, ct_dts, ct_name\n" \
	"FROM cash_transaction\n" \
	"WHERE ct_t_id = $1",
	1,
	{ INT8OID }
	},

	/* TUF3_6 TUF2_6 */
	{
	"SELECT th_dts, th_st_id\n" \
	"FROM trade_history\n" \
	"WHERE th_t_id = $1\n" \
	"ORDER BY th_dts\n" \
	"LIMIT 3",
	1,
	{ INT8OID }
	},

	{ NULL }
}; /* End TUF3_statements */

/* Prototypes. */
void dump_tuf1_inputs(int, int, long *);
void dump_tuf2_inputs(long, char *, int, int, char *);
void dump_tuf3_inputs(char *, long, int, int, char *, char *);

Datum TradeUpdateFrame1(PG_FUNCTION_ARGS);
Datum TradeUpdateFrame2(PG_FUNCTION_ARGS);
Datum TradeUpdateFrame3(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(TradeUpdateFrame1);
PG_FUNCTION_INFO_V1(TradeUpdateFrame2);
PG_FUNCTION_INFO_V1(TradeUpdateFrame3);

void dump_tuf1_inputs(int max_trades, int max_updates, long *trade_id)
{
	int i;

	elog(NOTICE, "TUF1: INPUTS START");
	elog(NOTICE, "TUF1: max_trades %d", max_trades);
	elog(NOTICE, "TUF1: max_updates %d", max_updates);
	for (i = 0; i < max_trades; i++) {
		elog(NOTICE, "TUF1: trade_id[%d] %ld", i, trade_id[i]);
	}
	elog(NOTICE, "TUF1: INPUTS END");
}

void dump_tuf2_inputs(long acct_id, char *end_trade_dts, int max_trades,
		int max_updates, char *start_trade_dts)
{
	elog(NOTICE, "TUF2: INPUTS START");
	elog(NOTICE, "TUF2: acct_id %ld", acct_id);
	elog(NOTICE, "TUF2: end_trade_dts %s", end_trade_dts);
	elog(NOTICE, "TUF2: max_trades %d", max_trades);
	elog(NOTICE, "TUF2: max_updates %d", max_updates);
	elog(NOTICE, "TUF2: start_trade_dts %s", start_trade_dts);
	elog(NOTICE, "TUF2: INPUTS END");
}

void dump_tuf3_inputs(char *end_trade_dts, long max_acct_id, int max_trades,
		int max_updates, char *start_trade_dts, char *symbol)
{
	elog(NOTICE, "TUF3: INPUTS START");
	elog(NOTICE, "TUF3: end_trade_dts %s", end_trade_dts);
	elog(NOTICE, "TUF3: max_acct_id %ld", max_acct_id);
	elog(NOTICE, "TUF3: max_trades %d", max_trades);
	elog(NOTICE, "TUF3: max_updates %d", max_updates);
	elog(NOTICE, "TUF3: start_trade_dts %s", start_trade_dts);
	elog(NOTICE, "TUF3: symbol %s", symbol);
	elog(NOTICE, "TUF3: INPUTS END");
}

/* Clause 3.3.9.3 */
Datum TradeUpdateFrame1(PG_FUNCTION_ARGS)
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

		enum tuf1 {
				i_bid_price=0, i_cash_transaction_amount,
				i_cash_transaction_dts, i_cash_transaction_name, i_exec_name,
				i_is_cash, i_is_market, i_num_found, i_num_updated,
				i_settlement_amount, i_settlement_cash_due_date,
				i_settlement_cash_type, i_trade_history_dts,
				i_trade_history_status_id, i_trade_price
		};

		int max_trades = PG_GETARG_INT32(0);
		int max_updates = PG_GETARG_INT32(1);
		ArrayType *trade_id_p = PG_GETARG_ARRAYTYPE_P(2);

		int16 typlen;
		bool typbyval;
		char typalign;

		int ndim, nitems;
		int *dim;
		long *trade_id;

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;
#ifdef DEBUG
		char sql[2048];
#endif
		Datum args[2];
		char nulls[2] = { ' ', ' ' };

		int num_found = max_trades;
		int num_updated = 0;
		int num_updated4 = 0;
		int num_updated5 = 0;
		int num_updated7 = 0;
		int num_cash = 0;

		ndim = ARR_NDIM(trade_id_p);
		dim = ARR_DIMS(trade_id_p);
		nitems = ArrayGetNItems(ndim, dim);

		get_typlenbyvalalign(ARR_ELEMTYPE(trade_id_p), &typlen, &typbyval,
				&typalign);
		trade_id = (long *) ARR_DATA_PTR(trade_id_p);

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 * Don't forget to factor in commas (,) and braces ({}) for the arrays.
		 */
		values = (char **) palloc(sizeof(char *) * 15);
		values[i_bid_price] =
				(char *) palloc(((S_PRICE_T_LEN + 1) * 20 + 2) * sizeof(char));
		values[i_cash_transaction_amount] =
				(char *) palloc(((VALUE_T_LEN + 1) * 20 + 2) * sizeof(char));
		values[i_cash_transaction_dts] =
				(char *) palloc(((MAXDATELEN + 1) * 20 + 2) * sizeof(char));
		values[i_cash_transaction_name] =
				(char *) palloc(((CT_NAME_LEN + 3) * 20 + 2) * sizeof(char));
		values[i_exec_name] = (char *) palloc(((T_EXEC_NAME_LEN + 3) * 20 +
				2) * sizeof(char));
		values[i_is_cash] =
				(char *) palloc(((SMALLINT_LEN + 1) * 20 + 2) * sizeof(char));
		values[i_is_market] =
				(char *) palloc(((SMALLINT_LEN + 1) * 20 + 2) * sizeof(char));
		values[i_num_found] = (char *) palloc((INTEGER_LEN + 1) * sizeof(char));
		values[i_num_updated] =
				(char *) palloc((INTEGER_LEN + 1) * sizeof(char));
		values[i_settlement_amount] =
				(char *) palloc(((VALUE_T_LEN + 1) * 20 + 2) * sizeof(char));
		values[i_settlement_cash_due_date] =
				(char *) palloc(((MAXDATELEN + 1) * 20 + 2) * sizeof(char));
		values[i_settlement_cash_type] = (char *) palloc(((SE_CASH_TYPE_LEN +
				3) * 20 + 2) * sizeof(char));
		values[i_trade_history_dts] = (char *) palloc(((MAXDATELEN * 3 + 4) *
				20 + 22) * sizeof(char));
		values[i_trade_history_status_id] = (char *) palloc((((ST_ID_LEN +
				2) * 3 + 4) * 20 + 22) * sizeof(char));
		values[i_trade_price] =
				(char *) palloc(((S_PRICE_T_LEN + 1) * 20 + 2) * sizeof(char));

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

		strcpy(values[i_bid_price], "{");
		strcpy(values[i_exec_name], "{");
		strcpy(values[i_is_cash], "{");
		strcpy(values[i_is_market], "{");
		strcpy(values[i_trade_price], "{");
		strcpy(values[i_settlement_amount], "{");
		strcpy(values[i_settlement_cash_due_date], "{");
		strcpy(values[i_settlement_cash_type], "{");
		strcpy(values[i_cash_transaction_amount], "{");
		strcpy(values[i_cash_transaction_dts], "{");
		strcpy(values[i_cash_transaction_name], "{");
		strcpy(values[i_trade_history_dts], "{");
		strcpy(values[i_trade_history_status_id], "{");
		for (i = 0; i < max_trades; i++) {
			char *is_cash_str = NULL;
			char *is_market_str;

			if (num_updated < max_updates) {
				char *ex_name;

#ifdef DEBUG
				sprintf(sql, SQLTUF1_1, trade_id[i]);
				elog(NOTICE, "SQL\n%s", sql);
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

#ifdef DEBUG
				elog(NOTICE, "ex_name = %s", ex_name);

				if (strstr(ex_name, " X ")) {
					sprintf(sql, SQLTUF1_2a, ex_name);
				} else {
					sprintf(sql, SQLTUF1_2b, ex_name);
				}
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				args[0] = CStringGetTextDatum(ex_name);
				if (strstr(ex_name, " X ")) {
					ret = SPI_execute_plan(TUF1_2a,
							args, nulls, true, 0);
				} else {
					ret = SPI_execute_plan(TUF1_2b,
							args, nulls, true, 0);
				}
				if (ret == SPI_OK_SELECT && SPI_processed > 0) {
					tupdesc = SPI_tuptable->tupdesc;
					tuptable = SPI_tuptable;
					tuple = tuptable->vals[0];
					ex_name = SPI_getvalue(tuple, tupdesc, 1);
				} else {
					FAIL_FRAME_SET(&funcctx->max_calls,
					strstr(ex_name, " X ")? TUF1_statements[1].sql:
								TUF1_statements[2].sql);
					dump_tuf1_inputs(max_trades, max_updates, trade_id);
					continue;
				}

#ifdef DEBUG
				elog(NOTICE, "ex_name = %s", ex_name);
				sprintf(sql, SQLTUF1_3, ex_name, trade_id[i]);
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				args[1] = Int64GetDatum(trade_id[i]);
				ret = SPI_execute_plan(TUF1_3, args, nulls, false, 0);
				if (ret != SPI_OK_UPDATE) {
					FAIL_FRAME_SET(&funcctx->max_calls,
								TUF1_statements[4].sql);
					dump_tuf1_inputs(max_trades, max_updates, trade_id);
					continue;
				}

				num_updated += SPI_processed;
			}

#ifdef DEBUG
			sprintf(sql, SQLTUF1_4, trade_id[i]);
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			args[0] = Int64GetDatum(trade_id[i]);
			ret = SPI_execute_plan(TUF1_4, args, nulls, true, 0);
			if (ret != SPI_OK_SELECT) {
				FAIL_FRAME_SET(&funcctx->max_calls,
							TUF1_statements[5].sql);
				dump_tuf1_inputs(max_trades, max_updates, trade_id);
				continue;
			}

			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			for (j = 0; j < SPI_processed; j++) {
				char *trade_price;
				if (num_updated4 > 0) {
					strcat(values[i_bid_price], ",");
					strcat(values[i_exec_name], ",");
					strcat(values[i_is_cash], ",");
					strcat(values[i_is_market], ",");
					strcat(values[i_trade_price], ",");
				}

				tuple = tuptable->vals[j];
				strcat(values[i_bid_price], SPI_getvalue(tuple, tupdesc, 1));
				strcat(values[i_exec_name], "\"");
				strcat(values[i_exec_name], SPI_getvalue(tuple, tupdesc, 2));
				strcat(values[i_exec_name], "\"");
				is_cash_str = SPI_getvalue(tuple, tupdesc, 3);
				strcat(values[i_is_cash], (is_cash_str[0] == 't' ? "1": "0"));
				is_market_str = SPI_getvalue(tuple, tupdesc, 4);
				strcat(values[i_is_market],
						(is_market_str[0] == 't' ? "0" : "1"));
				trade_price = SPI_getvalue(tuple, tupdesc, 5);
				if (trade_price != NULL)
					strcat(values[i_trade_price],
							SPI_getvalue(tuple, tupdesc, 5));
				else
					strcat(values[i_trade_price], "NULL");

				num_updated4++;
			}

#ifdef DEBUG
			sprintf(sql, SQLTUF1_5, trade_id[i]);
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_execute_plan(TUF1_5, args, nulls, true, 0);
			if (ret != SPI_OK_SELECT) {
				FAIL_FRAME_SET(&funcctx->max_calls,
							TUF1_statements[6].sql);
				dump_tuf1_inputs(max_trades, max_updates, trade_id);
				continue;
			}

			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			for (j = 0; j < SPI_processed; j++) {
				if (num_updated5 > 0) {
					strcat(values[i_settlement_amount], ",");
					strcat(values[i_settlement_cash_due_date], ",");
					strcat(values[i_settlement_cash_type], ",");
				}

				tuple = tuptable->vals[j];
				strcat(values[i_settlement_amount],
						SPI_getvalue(tuple, tupdesc, 1));
				strcat(values[i_settlement_cash_due_date],
						SPI_getvalue(tuple, tupdesc, 2));
				strcat(values[i_settlement_cash_type], "\"");
				strcat(values[i_settlement_cash_type],
						SPI_getvalue(tuple, tupdesc, 3));
				strcat(values[i_settlement_cash_type], "\"");

				num_updated5++;
			}

			if (is_cash_str[0] == 't') {
				if (num_cash > 0) {
					strcat(values[i_cash_transaction_amount], ",");
					strcat(values[i_cash_transaction_dts], ",");
					strcat(values[i_cash_transaction_name], ",");
				}

#ifdef DEBUG
				sprintf(sql, SQLTUF1_6, trade_id[i]);
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				ret = SPI_execute_plan(TUF1_6, args, nulls, true, 0);
				if (ret == SPI_OK_SELECT && SPI_processed > 0) {
					tupdesc = SPI_tuptable->tupdesc;
					tuptable = SPI_tuptable;
					tuple = tuptable->vals[0];
					strcat(values[i_cash_transaction_amount],
							SPI_getvalue(tuple, tupdesc, 1));
					strcat(values[i_cash_transaction_dts],
							SPI_getvalue(tuple, tupdesc, 2));
					strcat(values[i_cash_transaction_name], "\"");
					strcat(values[i_cash_transaction_name],
							SPI_getvalue(tuple, tupdesc, 3));
					strcat(values[i_cash_transaction_name], "\"");
					++num_cash;
				} else {
					FAIL_FRAME_SET(&funcctx->max_calls,
								TUF1_statements[7].sql);
					dump_tuf1_inputs(max_trades, max_updates, trade_id);
					continue;
				}
			}

#ifdef DEBUG
			sprintf(sql, SQLTUF1_7, trade_id[i]);
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			ret = SPI_execute_plan(TUF1_7, args, nulls, true, 0);
			if (ret == SPI_OK_SELECT) {
				tupdesc = SPI_tuptable->tupdesc;
				tuptable = SPI_tuptable;
			} else {
				FAIL_FRAME_SET(&funcctx->max_calls,
							TUF1_statements[8].sql);
				dump_tuf1_inputs(max_trades, max_updates, trade_id);
				continue;
			}

			if (num_updated7 > 0) {
				strcat(values[i_trade_history_dts], ",");
				strcat(values[i_trade_history_status_id], ",");
			}
			strcat(values[i_trade_history_dts], "{");
			strcat(values[i_trade_history_status_id], "{");
			for (j = 0; j < SPI_processed; j++) {
				if (j > 0) {
					strcat(values[i_trade_history_dts], ",");
					strcat(values[i_trade_history_status_id], ",");
				}

				tuple = tuptable->vals[j];
				strcat(values[i_trade_history_dts],
							SPI_getvalue(tuple, tupdesc, 1));
				strcat(values[i_trade_history_status_id], "\"");
				strcat(values[i_trade_history_status_id],
							SPI_getvalue(tuple, tupdesc, 2));
				strcat(values[i_trade_history_status_id], "\"");

				num_updated7++;
			}
			for (j = SPI_processed; j < 3; j++) {
				if (j > 0) {
					strcat(values[i_trade_history_dts], ",");
					strcat(values[i_trade_history_status_id], ",");
				}
				strcat(values[i_trade_history_dts], "NULL");
				strcat(values[i_trade_history_status_id], "\"\"");

				num_updated7++;
			}
			strcat(values[i_trade_history_dts], "}");
			strcat(values[i_trade_history_status_id], "}");
		}
		strcat(values[i_bid_price], "}");
		strcat(values[i_exec_name], "}");
		strcat(values[i_is_cash], "}");
		strcat(values[i_is_market], "}");
		strcat(values[i_trade_price], "}");
		strcat(values[i_settlement_amount], "}");
		strcat(values[i_settlement_cash_due_date], "}");
		strcat(values[i_settlement_cash_type], "}");
		strcat(values[i_cash_transaction_amount], "}");
		strcat(values[i_cash_transaction_dts], "}");
		strcat(values[i_cash_transaction_name], "}");
		strcat(values[i_trade_history_dts], "}");
		strcat(values[i_trade_history_status_id], "}");

		sprintf(values[i_num_found], "%d", num_found);
		sprintf(values[i_num_updated], "%d", num_updated);

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
		for (i = 0; i < 15; i++) {
			elog(NOTICE, "TUF1 OUT: %d %s", i, values[i]);
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
Datum TradeUpdateFrame2(PG_FUNCTION_ARGS)
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

		enum tuf2 {
				i_bid_price=0, i_cash_transaction_amount,
				i_cash_transaction_dts, i_cash_transaction_name, i_exec_name,
				i_is_cash, i_num_found, i_num_updated, i_settlement_amount,
				i_settlement_cash_due_date, i_settlement_cash_type,
				i_trade_history_dts, i_trade_history_status_id, i_trade_list,
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
#ifdef DEBUG
		char sql[2048];
#endif
		Datum args[4];
		char nulls[4] = { ' ', ' ', ' ', ' ' };

		char end_trade_dts[MAXDATELEN + 1];
		char start_trade_dts[MAXDATELEN + 1];

		int num_found;
		int num_updated = 0;
		int num_cash = 0;

		if (timestamp2tm(end_trade_dts_ts, NULL, tm, &fsec, NULL, NULL) == 0) {
			EncodeDateTimeM(tm, fsec, tzn, end_trade_dts);
		}
		if (timestamp2tm(start_trade_dts_ts, NULL, tm, &fsec, NULL, NULL) ==
				0) {
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
		values[i_bid_price] =
				(char *) palloc(((S_PRICE_T_LEN + 1) * 20 + 2) * sizeof(char));
		values[i_cash_transaction_amount] =
				(char *) palloc(((VALUE_T_LEN + 1) * 20 + 2) * sizeof(char));
		values[i_cash_transaction_dts] =
				(char *) palloc(((MAXDATELEN + 1) * 20 + 2) * sizeof(char));
		values[i_cash_transaction_name] =
				(char *) palloc(((CT_NAME_LEN + 3) * 20 + 2) * sizeof(char));
		values[i_exec_name] = (char *) palloc(((T_EXEC_NAME_LEN + 3) * 20 +
				2) * sizeof(char));
		values[i_is_cash] =
				(char *) palloc(((SMALLINT_LEN + 1) * 20 + 2) * sizeof(char));
		values[i_num_found] = (char *) palloc((INTEGER_LEN + 1) * sizeof(char));
		values[i_num_updated] =
				(char *) palloc((INTEGER_LEN + 1) * sizeof(char));
		values[i_settlement_amount] =
				(char *) palloc(((VALUE_T_LEN + 1) * 20 + 2) * sizeof(char));
		values[i_settlement_cash_due_date] =
				(char *) palloc(((MAXDATELEN + 1) * 20 + 2) * sizeof(char));
		values[i_settlement_cash_type] = (char *) palloc(((SE_CASH_TYPE_LEN +
				3) * 20 + 2) * sizeof(char));
		values[i_trade_history_dts] = (char *) palloc((((MAXDATELEN + 2) * 3
				+ 2) * 20 + 5) * sizeof(char));
		values[i_trade_history_status_id] = (char *) palloc((((ST_ID_LEN +
				2) * 3 + 2) * 20 + 5) * sizeof(char));
		values[i_trade_list] =
				(char *) palloc(((BIGINT_LEN + 1) * 20 + 2) * sizeof(char));
		values[i_trade_price] =
				(char *) palloc(((S_PRICE_T_LEN + 1) * 20 + 2) * sizeof(char));

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();
		plan_queries(TUF2_statements);
#ifdef DEBUG
		sprintf(sql, SQLTUF2_1, acct_id, start_trade_dts, end_trade_dts,
				max_trades);
		elog(NOTICE, "SQL\n%s", sql);
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

		strcpy(values[i_bid_price], "{");
		strcpy(values[i_exec_name], "{");
		strcpy(values[i_is_cash], "{");
		strcpy(values[i_trade_list], "{");
		strcpy(values[i_trade_price], "{");
		strcpy(values[i_settlement_amount], "{");
		strcpy(values[i_settlement_cash_due_date], "{");
		strcpy(values[i_settlement_cash_type], "{");
		strcpy(values[i_cash_transaction_amount], "{");
		strcpy(values[i_cash_transaction_dts], "{");
		strcpy(values[i_cash_transaction_name], "{");
		strcpy(values[i_trade_history_dts], "{");
		strcpy(values[i_trade_history_status_id], "{");
		for (i = 0; i < num_found; i++) {
			TupleDesc l_tupdesc;
			SPITupleTable *l_tuptable = NULL;
			HeapTuple l_tuple = NULL;

			char *is_cash_str;
			char *trade_list;
			char cash_type[41];

			tuple = tuptable->vals[i];

			if (i > 0) {
				strcat(values[i_bid_price], ",");
				strcat(values[i_exec_name], ",");
				strcat(values[i_is_cash], ",");
				strcat(values[i_trade_list], ",");
				strcat(values[i_trade_price], ",");
				strcat(values[i_settlement_amount], ",");
				strcat(values[i_settlement_cash_due_date], ",");
				strcat(values[i_settlement_cash_type], ",");
				strcat(values[i_trade_history_dts], ",");
				strcat(values[i_trade_history_status_id], ",");
			}

			strcat(values[i_bid_price], SPI_getvalue(tuple, tupdesc, 1));
			strcat(values[i_exec_name], "\"");
			strcat(values[i_exec_name], SPI_getvalue(tuple, tupdesc, 2));
			strcat(values[i_exec_name], "\"");
			is_cash_str = SPI_getvalue(tuple, tupdesc, 3);
			strcat(values[i_is_cash], (is_cash_str[0] == 't' ? "0" : "1"));
			trade_list = SPI_getvalue(tuple, tupdesc, 4);
			strcat(values[i_trade_list], trade_list);
			strcat(values[i_trade_price], SPI_getvalue(tuple, tupdesc, 5));

			if (num_updated < max_updates) {
				char *old_cash_type;

#ifdef DEBUG
				sprintf(sql, SQLTUF2_2, trade_list);
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				args[0] = Int64GetDatum(atoll(trade_list));
				ret = SPI_execute_plan(TUF2_2, args, nulls, true, 0);
				if (ret == SPI_OK_SELECT && SPI_processed > 0) {
					l_tupdesc = SPI_tuptable->tupdesc;
					l_tuptable = SPI_tuptable;
					l_tuple = l_tuptable->vals[0];
					old_cash_type = SPI_getvalue(l_tuple, l_tupdesc, 1);
				} else {
					FAIL_FRAME_SET(&funcctx->max_calls,
								TUF2_statements[1].sql);
					dump_tuf2_inputs(acct_id, end_trade_dts, max_trades,
							max_updates, start_trade_dts);
					continue;
				}

#ifdef DEBUG
				elog(NOTICE, "cash_type = '%s'", old_cash_type);
#endif /* DEBUG */

				if (is_cash_str[0] == 't') {
					if (strcmp(old_cash_type, "Cash Account") == 0) {
						strcpy(cash_type, "Cash");
					} else {
						strcpy(cash_type, "Cash Account");
					}
				} else {
					if (strcmp(old_cash_type, "Margin Account") == 0) {
						strcpy(cash_type, "Margin");
					} else {
						strcpy(cash_type, "Margin Account");
					}
				}

#ifdef DEBUG
				sprintf(sql, SQLTUF2_3, cash_type, trade_list);
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				args[0] = CStringGetTextDatum(cash_type);
				args[1] = Int64GetDatum(atoll(trade_list));
				ret = SPI_execute_plan(TUF2_3, args, nulls, false, 0);
				if (ret != SPI_OK_UPDATE) {
					FAIL_FRAME_SET(&funcctx->max_calls,
								TUF2_statements[2].sql);
					dump_tuf2_inputs(acct_id, end_trade_dts, max_trades,
							max_updates, start_trade_dts);
					continue;
				}
				num_updated += SPI_processed;
			}

#ifdef DEBUG
			sprintf(sql, SQLTUF2_4, trade_list);
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			args[0] = Int64GetDatum(atoll(trade_list));
			ret = SPI_execute_plan(TUF2_4, args, nulls, true, 0);
			if (ret == SPI_OK_SELECT && SPI_processed > 0) {
				l_tupdesc = SPI_tuptable->tupdesc;
				l_tuptable = SPI_tuptable;
				l_tuple = l_tuptable->vals[0];
				strcat(values[i_settlement_amount],
						SPI_getvalue(l_tuple, l_tupdesc, 1));
				strcat(values[i_settlement_cash_due_date],
						SPI_getvalue(l_tuple, l_tupdesc, 2));
				strcat(values[i_settlement_cash_type], "\"");
				strcat(values[i_settlement_cash_type],
						SPI_getvalue(l_tuple, l_tupdesc, 3));
				strcat(values[i_settlement_cash_type], "\"");
			} else {
				FAIL_FRAME_SET(&funcctx->max_calls,
							TUF2_statements[3].sql);
				dump_tuf2_inputs(acct_id, end_trade_dts, max_trades,
						max_updates, start_trade_dts);
				continue;
			}

			if (is_cash_str[0] == 't') {
#ifdef DEBUG
				sprintf(sql, SQLTUF2_5, trade_list);
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				ret = SPI_execute_plan(TUF2_5, args, nulls, true, 0);
				if (ret == SPI_OK_SELECT && SPI_processed > 0) {
					l_tupdesc = SPI_tuptable->tupdesc;
					l_tuptable = SPI_tuptable;
					l_tuple = l_tuptable->vals[0];
					if (num_cash > 0) {
						strcat(values[i_cash_transaction_amount], ",");
						strcat(values[i_cash_transaction_dts], ",");
						strcat(values[i_cash_transaction_name], ",");
					}
					strcat(values[i_cash_transaction_amount],
							SPI_getvalue(l_tuple, l_tupdesc, 1));
					strcat(values[i_cash_transaction_dts],
							SPI_getvalue(l_tuple, l_tupdesc, 2));
					strcat(values[i_cash_transaction_name], "\"");
					strcat(values[i_cash_transaction_name],
							SPI_getvalue(l_tuple, l_tupdesc, 3));
					strcat(values[i_cash_transaction_name], "\"");
					++num_cash;
				} else {
					FAIL_FRAME_SET(&funcctx->max_calls,
								TUF2_statements[4].sql);
					dump_tuf2_inputs(acct_id, end_trade_dts, max_trades,
							max_updates, start_trade_dts);
					continue;
				}
			} else {
					if (num_cash > 0) {
						strcat(values[i_cash_transaction_amount], ",");
						strcat(values[i_cash_transaction_dts], ",");
						strcat(values[i_cash_transaction_name], ",");
					}
					strcat(values[i_cash_transaction_amount], "0");
					strcat(values[i_cash_transaction_dts], "1970-1-1 0:0:0");
					strcat(values[i_cash_transaction_name], "\"\"");
					++num_cash;
			}

#ifdef DEBUG
			sprintf(sql, SQLTUF2_6, trade_list);
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */

			ret = SPI_execute_plan(TUF2_6, args, nulls, true, 0);
			if (ret == SPI_OK_SELECT && SPI_processed > 0) {
				l_tupdesc = SPI_tuptable->tupdesc;
				l_tuptable = SPI_tuptable;
			} else {
				FAIL_FRAME_SET(&funcctx->max_calls, TUF2_statements[5].sql);
				dump_tuf2_inputs(acct_id, end_trade_dts, max_trades,
						max_updates, start_trade_dts);
				continue;
			}
			strcat(values[i_trade_history_dts], "{");
			strcat(values[i_trade_history_status_id], "{");
			for (j = 0; j < SPI_processed; j ++) {
				if (j > 0) {
					strcat(values[i_trade_history_dts], ",");
					strcat(values[i_trade_history_status_id], ",");
				}
				l_tuple = l_tuptable->vals[j];
				strcat(values[i_trade_history_dts],
							SPI_getvalue(l_tuple, l_tupdesc, 1));
				strcat(values[i_trade_history_status_id], "\"");
				strcat(values[i_trade_history_status_id],
							SPI_getvalue(l_tuple, l_tupdesc, 2));
				strcat(values[i_trade_history_status_id], "\"");
			}
                        for (j = SPI_processed; j < 3; j++) {
                                if (j > 0) {
                                        strcat(values[i_trade_history_dts], ",");
                                        strcat(values[i_trade_history_status_id], ",");
                                }
                                strcat(values[i_trade_history_dts], "NULL");
                                strcat(values[i_trade_history_status_id], "\"\"");
                        }
			strcat(values[i_trade_history_dts], "}");
			strcat(values[i_trade_history_status_id], "}");
		}
		strcat(values[i_bid_price], "}");
		strcat(values[i_exec_name], "}");
		strcat(values[i_is_cash], "}");
		strcat(values[i_trade_list], "}");
		strcat(values[i_trade_price], "}");
		strcat(values[i_settlement_amount], "}");
		strcat(values[i_settlement_cash_due_date], "}");
		strcat(values[i_settlement_cash_type], "}");
		strcat(values[i_cash_transaction_amount], "}");
		strcat(values[i_cash_transaction_dts], "}");
		strcat(values[i_cash_transaction_name], "}");
		strcat(values[i_trade_history_dts], "}");
		strcat(values[i_trade_history_status_id], "}");

		sprintf(values[i_num_found], "%d", num_found);
		sprintf(values[i_num_updated], "%d", num_updated);

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
		for (i = 0; i < 15; i++) {
			elog(NOTICE, "TUF2 OUT: %d %s", i, values[i]);
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
Datum TradeUpdateFrame3(PG_FUNCTION_ARGS)
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

		enum tuf3 {
				i_acct_id=0, i_cash_transaction_amount,
				i_cash_transaction_dts, i_cash_transaction_name, i_exec_name,
				i_is_cash, i_num_found, i_num_updated, i_price, i_quantity,
				i_s_name, i_settlement_amount, i_settlement_cash_due_date,
				i_settlement_cash_type, i_trade_dts,
				i_trade_history_dts, i_trade_history_status_id, i_trade_list,
				i_type_name, i_trade_type
		};

		Timestamp end_trade_dts_ts = PG_GETARG_TIMESTAMP(0);
		long max_acct_id = PG_GETARG_INT64(1);
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
#ifdef DEBUG
		char sql[2048];
#endif
		Datum args[4];
		char nulls[4] = { ' ', ' ', ' ', ' ' };

		char symbol[S_SYMB_LEN + 1];
		char end_trade_dts[MAXDATELEN + 1];
		char start_trade_dts[MAXDATELEN + 1];

		int num_found;
		int num_updated = 0;
		int num_cash = 0;

		strcpy(symbol, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(symbol_p))));
		if (timestamp2tm(end_trade_dts_ts, NULL, tm, &fsec, NULL, NULL) == 0) {
			EncodeDateTimeM(tm, fsec, tzn, end_trade_dts);
		}
		if (timestamp2tm(start_trade_dts_ts, NULL, tm, &fsec, NULL, NULL) ==
				0) {
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
		values[i_acct_id] =
				(char *) palloc((BIGINT_LEN * 20 + 22) * sizeof(char));
		values[i_cash_transaction_amount] =
				(char *) palloc((VALUE_T_LEN * 20 + 22) * sizeof(char));
		values[i_cash_transaction_dts] =
				(char *) palloc((MAXDATELEN * 20 + 22) * sizeof(char));
		values[i_cash_transaction_name] =
				(char *) palloc(((CT_NAME_LEN + 2) * 20 + 22) * sizeof(char));
		values[i_exec_name] = (char *) palloc(((T_EXEC_NAME_LEN + 2) * 20 +
				22) * sizeof(char));
		values[i_is_cash] =
				(char *) palloc((SMALLINT_LEN * 20 + 22) * sizeof(char));
		values[i_num_found] = (char *) palloc((INTEGER_LEN + 1) * sizeof(char));
		values[i_num_updated] =
				(char *) palloc((INTEGER_LEN + 1) * sizeof(char));
		values[i_price] =
				(char *) palloc((S_PRICE_T_LEN * 20 + 22) * sizeof(char));
		values[i_quantity] =
				(char *) palloc((INTEGER_LEN * 20 + 22) * sizeof(char));
		values[i_s_name] =
				(char *) palloc(((S_NAME_LEN + 2) * 20 + 22) * sizeof(char));
		values[i_settlement_amount] =
				(char *) palloc((VALUE_T_LEN * 20 + 22) * sizeof(char));
		values[i_settlement_cash_due_date] =
				(char *) palloc((MAXDATELEN * 20 + 22) * sizeof(char));
		values[i_settlement_cash_type] = (char *) palloc(((SE_CASH_TYPE_LEN +
				2) * 20 + 22) * sizeof(char));
		values[i_trade_dts] = (char *) palloc(((MAXDATELEN + 1) * 20 + 2) *
				sizeof(char));
		values[i_trade_history_dts] = (char *) palloc(((MAXDATELEN * 3 + 4) *
				20 + 23) * sizeof(char));
		values[i_trade_history_status_id] = (char *) palloc((((ST_ID_LEN +
				2) * 3 + 4) * 20 + 23) * sizeof(char));
		values[i_trade_list] =
				(char *) palloc((BIGINT_LEN * 20 + 22) * sizeof(char));
		values[i_type_name] =
				(char *) palloc(((TT_NAME_LEN + 2) * 20 + 2) * sizeof(char));
		values[i_trade_type] =
				(char *) palloc(((TT_ID_LEN + 2) * 20 + 22) * sizeof(char));

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->max_calls = 1;

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		SPI_connect();
		plan_queries(TUF3_statements);
#ifdef DEBUG
		sprintf(sql, SQLTUF3_1, symbol, start_trade_dts, end_trade_dts,
				max_trades);
		elog(NOTICE, "SQL\n%s", sql);
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

		strcpy(values[i_acct_id], "{");
		strcpy(values[i_cash_transaction_amount], "{");
		strcpy(values[i_cash_transaction_dts], "{");
		strcpy(values[i_cash_transaction_name], "{");
		strcpy(values[i_exec_name], "{");
		strcpy(values[i_is_cash], "{");
		strcpy(values[i_price], "{");
		strcpy(values[i_quantity], "{");
		strcpy(values[i_s_name], "{");
		strcpy(values[i_settlement_amount], "{");
		strcpy(values[i_settlement_cash_due_date], "{");
		strcpy(values[i_settlement_cash_type], "{");
		strcpy(values[i_trade_dts], "{");
		strcpy(values[i_trade_history_dts], "{");
		strcpy(values[i_trade_history_status_id], "{");
		strcpy(values[i_trade_list], "{");
		strcpy(values[i_type_name], "{");
		strcpy(values[i_trade_type], "{");
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
				strcat(values[i_acct_id], ",");
				strcat(values[i_exec_name], ",");
				strcat(values[i_is_cash], ",");
				strcat(values[i_price], ",");
				strcat(values[i_quantity], ",");
				strcat(values[i_s_name], ",");
				strcat(values[i_trade_dts], ",");
				strcat(values[i_trade_list], ",");
				strcat(values[i_trade_type], ",");
				strcat(values[i_type_name], ",");
				strcat(values[i_settlement_amount], ",");
				strcat(values[i_settlement_cash_due_date], ",");
				strcat(values[i_settlement_cash_type], ",");
				strcat(values[i_trade_history_dts], ",");
				strcat(values[i_trade_history_status_id], ",");
			}

			strcat(values[i_acct_id], SPI_getvalue(tuple, tupdesc, 1));
			strcat(values[i_exec_name], "\"");
			strcat(values[i_exec_name], SPI_getvalue(tuple, tupdesc, 2));
			strcat(values[i_exec_name], "\"");
			is_cash_str = SPI_getvalue(tuple, tupdesc, 3);
			strcat(values[i_is_cash], (is_cash_str[0] == 't' ? "0" : "1"));
			price = SPI_getvalue(tuple, tupdesc, 4);
			if (price == NULL) {
				strcat(values[i_price], "NULL");
			} else {
				strcat(values[i_price], SPI_getvalue(tuple, tupdesc, 4));
			}
			quantity = SPI_getvalue(tuple, tupdesc, 5);
			strcat(values[i_quantity], quantity);
			s_name = SPI_getvalue(tuple, tupdesc, 6);
			strcat(values[i_s_name], "\"");
			strcat(values[i_s_name], s_name);
			strcat(values[i_s_name], "\"");
			strcat(values[i_trade_dts], SPI_getvalue(tuple, tupdesc, 7));
			trade_list = SPI_getvalue(tuple, tupdesc, 8);
			strcat(values[i_trade_list], trade_list);
			strcat(values[i_trade_type], "\"");
			strcat(values[i_trade_type], SPI_getvalue(tuple, tupdesc, 9));
			strcat(values[i_trade_type], "\"");
			type_name = SPI_getvalue(tuple, tupdesc, 10);
			strcat(values[i_type_name], "\"");
			strcat(values[i_type_name], type_name);
			strcat(values[i_type_name], "\"");

#ifdef DEBUG
			sprintf(sql, SQLTUF3_2, trade_list);
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			args[0] = Int64GetDatum(atoll(trade_list));
			ret = SPI_execute_plan(TUF3_2, args, nulls, true, 0);
			if (ret == SPI_OK_SELECT && SPI_processed > 0) {
				l_tupdesc = SPI_tuptable->tupdesc;
				l_tuptable = SPI_tuptable;
				l_tuple = l_tuptable->vals[0];
				strcat(values[i_settlement_amount],
						SPI_getvalue(l_tuple, l_tupdesc, 1));
				strcat(values[i_settlement_cash_due_date],
						SPI_getvalue(l_tuple, l_tupdesc, 2));
				strcat(values[i_settlement_cash_type], "\"");
				strcat(values[i_settlement_cash_type],
						SPI_getvalue(l_tuple, l_tupdesc, 3));
				strcat(values[i_settlement_cash_type], "\"");
			} else {
				FAIL_FRAME_SET(&funcctx->max_calls,
							TUF3_statements[1].sql);
				dump_tuf3_inputs(end_trade_dts, max_acct_id, max_trades,
						max_updates, start_trade_dts, symbol);
				continue;
			}

			if (is_cash_str[0] == 't') {
				if (num_updated < max_updates) {
#ifdef DEBUG
					sprintf(sql, SQLTUF3_3, trade_list);
					elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
					ret = SPI_execute_plan(TUF3_3, args, nulls, true, 0);
					if (ret == SPI_OK_SELECT && SPI_processed > 0) {
						l_tupdesc = SPI_tuptable->tupdesc;
						l_tuptable = SPI_tuptable;
						l_tuple = l_tuptable->vals[0];
						old_ct_name = SPI_getvalue(l_tuple, l_tupdesc, 1);
					} else {
						FAIL_FRAME_SET(&funcctx->max_calls,
									TUF3_statements[2].sql);
						dump_tuf3_inputs(end_trade_dts, max_acct_id, max_trades,
								max_updates, start_trade_dts, symbol);
						continue;
					}

					if (strstr(old_ct_name, " shares of ")) {
						sprintf(ct_name, "%s %s Shares of %s", type_name,
								quantity, s_name);
					} else {
						sprintf(ct_name, "%s %s shares of %s", type_name,
								quantity, s_name);
					}
					for (j = 0, k = 0; j < CT_NAME_LEN && ct_name[j] != '\0'; j++, k++) {
						if (ct_name[j] == '\'')
							ct_name_esc[k++] = '\\';
						ct_name_esc[k] = ct_name[j];
					}
					ct_name_esc[k] = '\0';

#ifdef DEBUG
					sprintf(sql, SQLTUF3_4, ct_name_esc, trade_list);
					elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
					args[0] = CStringGetTextDatum(ct_name_esc);
					args[1] = Int64GetDatum(atoll(trade_list));
					ret = SPI_execute_plan(TUF3_4, args, nulls, false, 0);
					if (ret != SPI_OK_UPDATE) {
						FAIL_FRAME_SET(&funcctx->max_calls,
									TUF3_statements[3].sql);
						dump_tuf3_inputs(end_trade_dts, max_acct_id, max_trades,
								max_updates, start_trade_dts, symbol);
						continue;
					}
					num_updated += SPI_processed;
				}

				if (num_cash > 0) {
					strcat(values[i_cash_transaction_amount], ",");
					strcat(values[i_cash_transaction_dts], ",");
					strcat(values[i_cash_transaction_name], ",");
				}
#ifdef DEBUG
				sprintf(sql, SQLTUF3_5, trade_list);
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				args[0] = Int64GetDatum(atoll(trade_list));
				ret = SPI_execute_plan(TUF3_5, args, nulls, true, 0);
				if (ret == SPI_OK_SELECT && SPI_processed > 0) {
					l_tupdesc = SPI_tuptable->tupdesc;
					l_tuptable = SPI_tuptable;
					l_tuple = l_tuptable->vals[0];
					strcat(values[i_cash_transaction_amount],
							SPI_getvalue(l_tuple, l_tupdesc, 1));
					strcat(values[i_cash_transaction_dts],
							SPI_getvalue(l_tuple, l_tupdesc, 2));
					strcat(values[i_cash_transaction_name], "\"");
					strcat(values[i_cash_transaction_name],
							SPI_getvalue(l_tuple, l_tupdesc, 3));
					strcat(values[i_cash_transaction_name], "\"");
					++num_cash;
				} else {
					FAIL_FRAME_SET(&funcctx->max_calls,
								TUF3_statements[4].sql);
					dump_tuf3_inputs(end_trade_dts, max_acct_id, max_trades,
							max_updates, start_trade_dts, symbol);
					continue;
				}
			}

#ifdef DEBUG
			sprintf(sql, SQLTUF3_6, trade_list);
			elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
			args[0] = Int64GetDatum(atoll(trade_list));
			ret = SPI_execute_plan(TUF3_6, args, nulls, true, 0);
			if (ret == SPI_OK_SELECT && SPI_processed > 0) {
				l_tupdesc = SPI_tuptable->tupdesc;
				l_tuptable = SPI_tuptable;
			} else {
				FAIL_FRAME_SET(&funcctx->max_calls,
							TUF3_statements[5].sql);
				dump_tuf3_inputs(end_trade_dts, max_acct_id, max_trades,
						max_updates, start_trade_dts, symbol);
				continue;
			}
			strcat(values[i_trade_history_dts], "{");
			strcat(values[i_trade_history_status_id], "{");
			for (j = 0; j < 3; j ++) {
				if (j > 0) {
					strcat(values[i_trade_history_dts], ",");
					strcat(values[i_trade_history_status_id], ",");
				}
				if (j < SPI_processed) {
					l_tuple = l_tuptable->vals[j];
					strcat(values[i_trade_history_dts],
								SPI_getvalue(l_tuple, l_tupdesc, 1));
					strcat(values[i_trade_history_status_id], "\"");
					strcat(values[i_trade_history_status_id],
								SPI_getvalue(l_tuple, l_tupdesc, 2));
					strcat(values[i_trade_history_status_id], "\"");
				} else {
					strcat(values[i_trade_history_dts], "NULL");
					strcat(values[i_trade_history_status_id], "NULL");
				}
			}
			strcat(values[i_trade_history_dts], "}");
			strcat(values[i_trade_history_status_id], "}");
		}
		strcat(values[i_acct_id], "}");
		strcat(values[i_cash_transaction_amount], "}");
		strcat(values[i_cash_transaction_dts], "}");
		strcat(values[i_cash_transaction_name], "}");
		strcat(values[i_exec_name], "}");
		strcat(values[i_is_cash], "}");
		strcat(values[i_price], "}");
		strcat(values[i_quantity], "}");
		strcat(values[i_s_name], "}");
		strcat(values[i_settlement_amount], "}");
		strcat(values[i_settlement_cash_due_date], "}");
		strcat(values[i_settlement_cash_type], "}");
		strcat(values[i_trade_dts], "}");
		strcat(values[i_trade_history_dts], "}");
		strcat(values[i_trade_history_status_id], "}");
		strcat(values[i_trade_list], "}");
		strcat(values[i_type_name], "}");
		strcat(values[i_trade_type], "}");

		sprintf(values[i_num_found], "%d", num_found);
		sprintf(values[i_num_updated], "%d", num_updated);

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
		for (i = 0; i < 20; i++) {
			elog(NOTICE, "TUF3 OUT: %d %s", i, values[i]);
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
