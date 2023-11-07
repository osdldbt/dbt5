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
#include <utils/numeric.h>
#include <utils/builtins.h>
#include <utils/date.h>
#include <catalog/pg_type.h>

#include "frame.h"
#include "dbt5common.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#ifdef DEBUG
#define SQLTRF1_1 \
		"SELECT t_ca_id, t_tt_id, t_s_symb, t_qty, t_chrg,\n" \
		"       CASE WHEN t_lifo = true\n" \
		"            THEN 1\n" \
		"            ELSE 0 END,\n" \
		"       CASE WHEN t_is_cash = true\n" \
		"            THEN 1\n" \
		"            ELSE 0 END\n" \
		"FROM trade\n" \
		"WHERE t_id = %ld"

#define SQLTRF1_2 \
		"SELECT tt_name,\n" \
		"       CASE WHEN tt_is_sell = true\n" \
		"            THEN 1\n" \
		"            ELSE 0 END,\n" \
		"       CASE WHEN tt_is_mrkt = true\n" \
		"            THEN 1\n" \
		"            ELSE 0 END\n" \
		"FROM trade_type\n" \
		"WHERE tt_id = '%s'"

#define SQLTRF1_3 \
		"SELECT hs_qty\n" \
		"FROM holding_summary\n" \
		"WHERE hs_ca_id = %s\n" \
		"  AND hs_s_symb = '%s'"

#define SQLTRF2_1 \
		"SELECT ca_b_id, ca_c_id, ca_tax_st\n" \
		"FROM customer_account\n" \
		"WHERE ca_id = %ld\n" \
		"FOR UPDATE"

#define SQLTRF2_2a \
		"INSERT INTO holding_summary(hs_ca_id, hs_s_symb, hs_qty)\n" \
		"VALUES(%ld, '%s', %d)"

#define SQLTRF2_2b \
		"UPDATE holding_summary\n" \
		"SET hs_qty = %d\n" \
		"WHERE hs_ca_id = %ld\n " \
		"  AND hs_s_symb = '%s'"

#define SQLTRF2_3a \
		"SELECT h_t_id, h_qty, h_price\n" \
		"FROM holding\n" \
		"WHERE h_ca_id = %ld\n" \
		"  AND h_s_symb = '%s'\n" \
		"ORDER BY h_dts DESC\n" \
		"FOR UPDATE"

#define SQLTRF2_3b \
		"SELECT h_t_id, h_qty, h_price\n" \
		"FROM holding\n" \
		"WHERE h_ca_id = %ld\n" \
		"  AND h_s_symb = '%s'\n" \
		"ORDER BY h_dts ASC\n" \
		"FOR UPDATE"

#define SQLTRF2_4a \
		"INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty,\n" \
		"                            hh_after_qty)\n" \
		"VALUES(%ld, %ld, %d, %d)"

#define SQLTRF2_5a \
		"UPDATE holding\n" \
		"SET h_qty = %d\n" \
		"WHERE h_t_id = %ld"

#define SQLTRF2_5b \
		"DELETE FROM holding\n" \
		"WHERE h_t_id = %ld"

#define SQLTRF2_7a \
		"INSERT INTO holding(h_t_id, h_ca_id, h_s_symb, h_dts, h_price,\n" \
		"                    h_qty)\n" \
		"VALUES (%ld, %ld, '%s', '%s', %f, %d)"

#define SQLTRF2_7b \
		"DELETE FROM holding_summary\n" \
		"WHERE hs_ca_id = %ld\n" \
		"  AND hs_s_symb = '%s'"

#define SQLTRF2_8a \
		"INSERT INTO holding_summary(hs_ca_id, hs_s_symb, hs_qty)\n" \
		"VALUES (%ld, '%s', %d)"

#define SQLTRF2_8b \
		"UPDATE holding_summary\n" \
		"SET hs_qty = %d\n" \
		"WHERE hs_ca_id = %ld\n" \
		"  AND hs_s_symb = '%s'"

#define SQLTRF3_1 \
		"SELECT SUM(tx_rate)\n" \
		"FROM taxrate\n" \
		"WHERE tx_id IN (SELECT cx_tx_id\n" \
		"                FROM customer_taxrate\n" \
		"                WHERE cx_c_id = %ld)\n"

#define SQLTRF3_2 \
		"UPDATE trade\n" \
		"SET t_tax = %f\n" \
		"WHERE t_id = %ld"

#define SQLTRF4_1 \
		"SELECT s_ex_id, s_name\n" \
		"FROM security\n" \
		"WHERE s_symb = '%s'"

#define SQLTRF4_2 \
		"SELECT c_tier\n" \
		"FROM customer\n" \
		"WHERE c_id = %ld"

#define SQLTRF4_3 \
		"SELECT cr_rate\n" \
		"FROM commission_rate\n" \
		"WHERE cr_c_tier = %s\n" \
		"  AND cr_tt_id = '%s'\n" \
		"  AND cr_ex_id = '%s'\n" \
		"  AND cr_from_qty <= %d\n" \
		"  AND cr_to_qty >= %d\n" \
		"LIMIT 1"

#define SQLTRF5_1 \
		"UPDATE trade\n" \
		"SET t_comm = %f,\n" \
		"    t_dts = '%s',\n" \
		"    t_st_id = '%s',\n" \
		"    t_trade_price = %f\n" \
		"WHERE t_id = %ld"

#define SQLTRF5_2 \
		"INSERT INTO trade_history(th_t_id, th_dts, th_st_id)\n" \
		"VALUES (%ld, '%s', '%s')"

#define SQLTRF5_3 \
		"UPDATE broker\n" \
		"SET b_comm_total = b_comm_total + %f,\n" \
		"    b_num_trades = b_num_trades + 1\n" \
		"WHERE b_id = %ld"

#define SQLTRF6_1 \
		"INSERT INTO settlement(se_t_id, se_cash_type, se_cash_due_date,\n " \
		"                       se_amt)\n" \
		"VALUES (%ld, '%s', '%s', %f)"

#define SQLTRF6_2 \
		"UPDATE customer_account\n" \
		"SET ca_bal = ca_bal + %f\n" \
		"WHERE ca_id = %ld"

#define SQLTRF6_3 \
		"INSERT INTO cash_transaction(ct_dts, ct_t_id, ct_amt, ct_name)\n" \
		"VALUES ('%s', %ld, %f, e'%s %d shared of %s')"

#define SQLTRF6_4 \
		"SELECT ca_bal\n" \
		"FROM customer_account\n" \
		"WHERE ca_id = %ld"
#endif /* End DEBUG */

#define TRF1_1  TRF1_statements[0].plan
#define TRF1_2  TRF1_statements[1].plan
#define TRF1_3  TRF1_statements[2].plan

#define TRF2_1  TRF2_statements[0].plan
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

	/* TRF1_1 */
	{
	"SELECT t_ca_id, t_tt_id, t_s_symb, t_qty, t_chrg,\n" \
	"       CASE WHEN t_lifo = true\n" \
	"            THEN 1\n" \
	"            ELSE 0 END,\n" \
	"       CASE WHEN t_is_cash = true\n" \
	"            THEN 1\n" \
	"            ELSE 0 END\n" \
	"FROM trade\n" \
	"WHERE t_id = $1",
	1,
	{ INT8OID }
	},

	/* TRF1_2 */
	{
	"SELECT tt_name,\n" \
	"       CASE WHEN tt_is_sell = true\n" \
	"            THEN 1\n" \
	"            ELSE 0 END,\n" \
	"       CASE WHEN tt_is_mrkt = true\n" \
	"            THEN 1\n" \
	"            ELSE 0 END\n" \
	"FROM trade_type\n" \
	"WHERE tt_id = $1",
	1,
	{ TEXTOID }
	},

	/* TRF1_3 */
	{
	"SELECT hs_qty\n" \
	"FROM holding_summary\n" \
	"WHERE hs_ca_id = $1\n" \
	"  AND hs_s_symb = $2",
	2,
	{ INT8OID, TEXTOID }
	},

	{ NULL }
}; /* End TRF1_statements */

static cached_statement TRF2_statements[] = {

	/* TRF2_1 */
	{
	"SELECT ca_b_id, ca_c_id, ca_tax_st\n" \
	"FROM customer_account\n" \
	"WHERE ca_id = $1\n" \
	"FOR UPDATE",
	1,
	{ INT8OID }
	},

	/* TRF2_2a */
	{
	"INSERT INTO holding_summary(hs_ca_id, hs_s_symb, hs_qty)\n" \
	"VALUES($1, $2, $3)",
	3,
	{ INT8OID, TEXTOID, INT4OID }
	},

	/* TRF2_2b */
	{
	"UPDATE holding_summary\n" \
	"SET hs_qty = $1\n" \
	"WHERE hs_ca_id = $2\n " \
	"  AND hs_s_symb = $3",
	3,
	{ INT4OID, INT8OID, TEXTOID }
	},

	/* TRF2_3a */
	{
	"SELECT h_t_id, h_qty, h_price\n" \
	"FROM holding\n" \
	"WHERE h_ca_id = $1\n" \
	"  AND h_s_symb = $2\n" \
	"ORDER BY h_dts DESC\n" \
	"FOR UPDATE",
	2,
	{ INT8OID, TEXTOID }
	},

	/* TRF2_3b */
	{
	"SELECT h_t_id, h_qty, h_price\n" \
	"FROM holding\n" \
	"WHERE h_ca_id = $1\n" \
	"  AND h_s_symb = $2\n" \
	"ORDER BY h_dts ASC\n" \
	"FOR UPDATE",
	2,
	{ INT8OID, TEXTOID }
	},

	/* TRF2_4a */
	{
	"INSERT INTO holding_history(hh_h_t_id, hh_t_id, hh_before_qty,\n" \
	"                            hh_after_qty)\n" \
	"VALUES($1, $2, $3, $4)",
	4,
	{ INT8OID, INT8OID, INT4OID, INT4OID }
	},

	/* TRF2_5a */
	{
	"UPDATE holding\n" \
	"SET h_qty = $1\n" \
	"WHERE h_t_id = $2",
	2,
	{ INT4OID, INT8OID }
	},

	/* TRF2_5b */
	{
	"DELETE FROM holding\n" \
	"WHERE h_t_id = $1",
	1,
	{ INT8OID }
	},

	/* TRF2_7a */
	{
	"INSERT INTO holding(h_t_id, h_ca_id, h_s_symb, h_dts, h_price,\n" \
	"                    h_qty)\n" \
	"VALUES ($1, $2, $3, $4, $5, $6)",
	6,
	{ INT8OID, INT8OID, TEXTOID, TIMESTAMPOID, FLOAT8OID, INT4OID }
	},

	/* TRF2_7b */
	{
	"DELETE FROM holding_summary\n" \
	"WHERE hs_ca_id = $1\n" \
	"  AND hs_s_symb = $2",
	2,
	{ INT8OID, TEXTOID }
	},

	/* TRF2_8a */
	{
	"INSERT INTO holding_summary(hs_ca_id, hs_s_symb, hs_qty)\n" \
	"VALUES ($1, $2, $3)",
	3,
	{ INT8OID, TEXTOID, INT4OID }
	},

	/* TRF2_8b */
	{
	"UPDATE holding_summary\n" \
	"SET hs_qty = $1\n" \
	"WHERE hs_ca_id = $2\n" \
	"  AND hs_s_symb = $3",
	3,
	{ INT4OID, INT8OID, TEXTOID }
	},

	{ NULL }
}; /* END TRF2_statements */

static cached_statement TRF3_statements[] = {

	/* TRF3_1 */
	{
	"SELECT SUM(tx_rate)\n" \
	"FROM taxrate\n" \
	"WHERE tx_id IN (SELECT cx_tx_id\n" \
	"                FROM customer_taxrate\n" \
	"                WHERE cx_c_id = $1)\n",
	1,
	{ INT8OID }
	},

	/* TRF3_2 */
	{
	"UPDATE trade\n" \
	"SET t_tax = $1\n" \
	"WHERE t_id = $2",
	2,
	{ FLOAT8OID, INT8OID }
	},

	{ NULL }
}; /* END TRF3_statements */

static cached_statement TRF4_statements[] = {

	/* TRF4_1 */
	{
	"SELECT s_ex_id, s_name\n" \
	"FROM security\n" \
	"WHERE s_symb = $1",
	1,
	{ TEXTOID }
	},

	/* TRF4_2 */
	{
	"SELECT c_tier\n" \
	"FROM customer\n" \
	"WHERE c_id = $1",
	1,
	{ INT8OID }
	},

	/* TRF4_3 */
	{
	"SELECT cr_rate\n" \
	"FROM commission_rate\n" \
	"WHERE cr_c_tier = $1\n" \
	"  AND cr_tt_id = $2\n" \
	"  AND cr_ex_id = $3\n" \
	"  AND cr_from_qty <= $4\n" \
	"  AND cr_to_qty >= $5\n" \
	"LIMIT 1",
	5,
	{ INT2OID, TEXTOID, TEXTOID, INT4OID, INT4OID }
	},

	{ NULL }
}; /* END TRF4_statements */

static cached_statement TRF5_statements[] = {

	/* TRF5_1 */
	{
	"UPDATE trade\n" \
	"SET t_comm = $1,\n" \
	"    t_dts = $2,\n" \
	"    t_st_id = $3,\n" \
	"    t_trade_price = $4\n" \
	"WHERE t_id = $5",
	5,
	{ FLOAT4OID, TIMESTAMPOID, TEXTOID, FLOAT8OID, INT8OID }
	},

	/* TRF5_2 */
	{
	"INSERT INTO trade_history(th_t_id, th_dts, th_st_id)\n" \
	"VALUES ($1, $2, $3)",
	3,
	{ INT8OID, TIMESTAMPOID, TEXTOID }
	},

	/* TRF5_3 */
	{
	"UPDATE broker\n" \
	"SET b_comm_total = b_comm_total + $1,\n" \
	"    b_num_trades = b_num_trades + 1\n" \
	"WHERE b_id = $2",
	2,
	{FLOAT8OID, INT8OID }
	},

	{ NULL }
}; /* TRF5_statements */

static cached_statement TRF6_statements[] = {

	/* TRF6_1 */
	{
	"INSERT INTO settlement(se_t_id, se_cash_type, se_cash_due_date,\n " \
	"                       se_amt)\n" \
	"VALUES ($1, $2, $3, $4)",
	4,
	{ INT8OID, TEXTOID, DATEOID, FLOAT8OID }
	},

	/* TRF6_2 */
	{
	"UPDATE customer_account\n" \
	"SET ca_bal = ca_bal + $1\n" \
	"WHERE ca_id = $2",
	2,
	{ FLOAT8OID, INT8OID }
	},

	/* TRF6_3 */
	{
	"INSERT INTO cash_transaction(ct_dts, ct_t_id, ct_amt, ct_name)\n" \
	"VALUES ($1, $2, $3, e'$4 $5 shared of $6')",
	6,
	{ TIMESTAMPOID, INT8OID, FLOAT8OID, TEXTOID, INT4OID, TEXTOID }
	},

	/* TRF6_4 */
	{
	"SELECT ca_bal\n" \
	"FROM customer_account\n" \
	"WHERE ca_id = $1",
	1,
	{ INT8OID }
	},

	{ NULL }
}; /* End TRF6_statements */

/* Prototypes. */
void dump_trf1_inputs(long);
void dump_trf2_inputs(long, int, int, char *, long, double, int, int);
void dump_trf3_inputs(double, long, double, long);
void dump_trf4_inputs(long, char *, int, char *);
void dump_trf5_inputs(long, double, char *, char *, long, double);
void dump_trf6_inputs(long, char *, char *, double, char *, long, int, int,
		char *);

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

void dump_trf1_inputs(long trade_id)
{
	elog(NOTICE, "TRF1: INPUTS START");
	elog(NOTICE, "TRF1: trade_id %ld", trade_id);
	elog(NOTICE, "TRF1: INPUTS END");
}

void dump_trf2_inputs(long acct_id, int hs_qty, int is_lifo, char *symbol,
		long trade_id, double trade_price, int trade_qty, int type_is_sell)
{
	elog(NOTICE, "TRF2: INPUTS START");
	elog(NOTICE, "TRF2: acct_id %ld", acct_id);
	elog(NOTICE, "TRF2: hs_qty %d", hs_qty);
	elog(NOTICE, "TRF2: is_lifo %d", is_lifo);
	elog(NOTICE, "TRF2: symbol %s", symbol);
	elog(NOTICE, "TRF2: trade_id %ld", trade_id);
	elog(NOTICE, "TRF2: trade_price %f", trade_price);
	elog(NOTICE, "TRF2: trade_qty %d", trade_qty);
	elog(NOTICE, "TRF2: type_is_sell %d", type_is_sell);
	elog(NOTICE, "TRF2: INPUTS END");
}

void dump_trf3_inputs(double buy_value, long cust_id, double sell_value,
		long trade_id)
{
	elog(NOTICE, "TRF3: INPUTS START");
	elog(NOTICE, "TRF3: buy_value %f", buy_value);
	elog(NOTICE, "TRF3: cust_id %ld", cust_id);
	elog(NOTICE, "TRF3: sell_value %f", sell_value);
	elog(NOTICE, "TRF3: trade_id %ld", trade_id);
	elog(NOTICE, "TRF3: INPUTS END");
}

void dump_trf4_inputs(long cust_id, char *symbol, int trade_qty, char *type_id)
{
	elog(NOTICE, "TRF4: INPUTS START");
	elog(NOTICE, "TRF4: cust_id %ld", cust_id);
	elog(NOTICE, "TRF4: symbol %s", symbol);
	elog(NOTICE, "TRF4: trade_qty %d", trade_qty);
	elog(NOTICE, "TRF4: type_id %s", type_id);
	elog(NOTICE, "TRF4: INPUTS END");
}

void dump_trf5_inputs(long broker_id, double comm_amount, char *st_completed_id,
		char *trade_dts, long trade_id, double trade_price)
{
	elog(NOTICE, "TRF5: INPUTS START");
	elog(NOTICE, "TRF5: broker_id %ld", broker_id);
	elog(NOTICE, "TRF5: comm_amount %f", comm_amount);
	elog(NOTICE, "TRF5: st_completed_id %s", st_completed_id);
	elog(NOTICE, "TRF5: trade_dts %s", trade_dts);
	elog(NOTICE, "TRF5: trade_id %ld", trade_id);
	elog(NOTICE, "TRF5: trade_price %f", trade_price);
	elog(NOTICE, "TRF5: INPUTS END");
}

void dump_trf6_inputs(long acct_id, char *due_date, char *s_name,
		double se_amount, char *trade_dts, long trade_id, int trade_is_cash,
		int trade_qty, char *type_name)
{
	elog(NOTICE, "TRF6: INPUTS START");
	elog(NOTICE, "TRF6: acct_id %ld", acct_id);
	elog(NOTICE, "TRF6: due_date %s", due_date);
	elog(NOTICE, "TRF6: s_name %s", s_name);
	elog(NOTICE, "TRF6: se_amount %f", se_amount);
	elog(NOTICE, "TRF6: trade_dts %s", trade_dts);
	elog(NOTICE, "TRF6: trade_id %ld", trade_id);
	elog(NOTICE, "TRF6: trade_is_cash %d", trade_is_cash);
	elog(NOTICE, "TRF6: trade_qty %d", trade_qty);
	elog(NOTICE, "TRF6: type_name %s", type_name);
	elog(NOTICE, "TRF6: INPUTS END");
}

/* Clause 3.3.8.3 */
Datum TradeResultFrame1(PG_FUNCTION_ARGS)
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

		enum trf1 {
				i_acct_id=0, i_charge, i_hs_qty, i_is_lifo, i_num_found,
				i_symbol, i_trade_is_cash, i_trade_qty, i_type_id,
				i_type_is_market, i_type_is_sell, i_type_name
		};

		long trade_id = PG_GETARG_INT64(0);

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;
#ifdef DEBUG
		char sql[2048];
#endif /* DEBUG */
		Datum args[2];
		char nulls[2] = {' ', ' ' };
		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 12);
		values[i_num_found] = (char *) palloc((INTEGER_LEN + 1) * sizeof(char));

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
		sprintf(sql, SQLTRF1_1, trade_id);
		elog(NOTICE, "SQL\n%s", sql);
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
				sprintf(sql, SQLTRF1_2, values[i_type_id]);
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				args[0] = CStringGetTextDatum(values[i_type_id]);
				ret = SPI_execute_plan(TRF1_2, args, nulls, true, 0);
				if (ret == SPI_OK_SELECT) {
					tupdesc = SPI_tuptable->tupdesc;
					tuptable = SPI_tuptable;
					if (SPI_processed > 0) {
						tuple = tuptable->vals[0];
						values[i_type_name] = SPI_getvalue(tuple, tupdesc, 1);
						values[i_type_is_sell] =
								SPI_getvalue(tuple, tupdesc, 2);
						values[i_type_is_market] =
								SPI_getvalue(tuple, tupdesc, 3);
					}
				} else {
					FAIL_FRAME_SET(&funcctx->max_calls, TRF1_statements[1].sql);
					dump_trf1_inputs(trade_id);
				}

#ifdef DEBUG
				sprintf(sql, SQLTRF1_3, values[i_acct_id], values[i_symbol]);
				elog(NOTICE, "SQL\n%s", sql);
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
					FAIL_FRAME_SET(&funcctx->max_calls, TRF1_statements[2].sql);
					dump_trf1_inputs(trade_id);
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
			dump_trf1_inputs(trade_id);
			FAIL_FRAME_SET(&funcctx->max_calls, TRF1_statements[0].sql);
		}

		sprintf(values[i_num_found], "%d", num_found);

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
		for (i = 0; i < 12; i++) {
			elog(NOTICE, "TRF1 OUT: %d %s", i, values[i]);
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
Datum TradeResultFrame2(PG_FUNCTION_ARGS)
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

		enum trf2 {
				i_broker_id=0, i_buy_value, i_cust_id, i_sell_value,
				i_tax_status, i_trade_dts
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

		strncpy(symbol, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(symbol_p))), S_SYMB_LEN);
		symbol[S_SYMB_LEN] = '\0';
		trade_price = DatumGetFloat8(DirectFunctionCall1(
				numeric_float8_no_overflow, PointerGetDatum(trade_price_num)));

		/*
		 * Prepare a values array for building the returned tuple.
		 * This should be an array of C strings, which will
		 * be processed later by the type input functions.
		 */
		values = (char **) palloc(sizeof(char *) * 6);
		values[i_buy_value] =
				(char *) palloc((S_PRICE_T_LEN + 1) * sizeof(char));
		values[i_sell_value] =
				(char *) palloc((S_PRICE_T_LEN + 1) * sizeof(char));

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
		elog(NOTICE, "SQL\n%s", sql);
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
		sprintf(sql, SQLTRF2_1, acct_id);
		elog(NOTICE, "SQL\n%s", sql);
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
			dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol, trade_id,
				trade_price, trade_qty, type_is_sell);
			FAIL_FRAME_SET(&funcctx->max_calls, TRF2_statements[0].sql);
		}

		/* Determine if sell or buy order */
		if (type_is_sell == 1) {
			if (hs_qty == 0) {
				/* no prior holdings exist, but one will be inserted */
#ifdef DEBUG
				sprintf(sql, SQLTRF2_2a, acct_id, symbol, -1 * trade_qty);
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				args[0] = Int64GetDatum(acct_id);
				args[1] = CStringGetTextDatum(symbol);
				args[2] = Int32GetDatum(-1 * trade_qty);
				ret = SPI_execute_plan(TRF2_2a, args, nulls, false, 0);
				if (ret != SPI_OK_INSERT) {
					FAIL_FRAME_SET(&funcctx->max_calls,  TRF2_statements[1].sql);
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
				}
			} else if (hs_qty != trade_qty) {
#ifdef DEBUG
				sprintf(sql, SQLTRF2_2b, hs_qty - trade_qty, acct_id, symbol);
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				args[0] = Int32GetDatum(hs_qty - trade_qty);
				args[1] = Int64GetDatum(acct_id);
				args[2] = CStringGetTextDatum(symbol);
				ret = SPI_execute_plan(TRF2_2b, args, nulls, false, 0);
				if (ret != SPI_OK_UPDATE) {
					FAIL_FRAME_SET(&funcctx->max_calls,  TRF2_statements[2].sql);
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
				}
			}

			/* Sell Trade: */

			/* First look for existing holdings */
			if (hs_qty > 0) {
#ifdef DEBUG
				/* Could return 0, 1 or many rows */
				if (is_lifo == 1) {
					sprintf(sql, SQLTRF2_3a, acct_id, symbol);
				} else {
					sprintf(sql, SQLTRF2_3b, acct_id, symbol);
				}
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				args[0] = Int64GetDatum(acct_id);
				args[1] = CStringGetTextDatum(symbol);
				if (is_lifo == 1) {
					ret = SPI_execute_plan(TRF2_3a, args, nulls, false, 0);
				} else {
					ret = SPI_execute_plan(TRF2_3b, args, nulls, false, 0);
				}
				if (ret != SPI_OK_SELECT) {
					FAIL_FRAME_SET(&funcctx->max_calls, is_lifo == 1?
							TRF2_statements[3].sql: TRF2_statements[4].sql);
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
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

					if (hold_qty > needed_qty ) {
						/* Selling some of the holdings */
#ifdef DEBUG
						sprintf(sql, SQLTRF2_4a, hold_id, trade_id, hold_qty,
								hold_qty - needed_qty);
						elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
						args[0] = Int64GetDatum(hold_id);
						args[1] = Int64GetDatum(trade_id);
						args[2] = Int32GetDatum(hold_qty);
						args[3] = Int32GetDatum(hold_qty - needed_qty);
						ret = SPI_execute_plan(TRF2_4a, args, nulls, false, 0);
						if (ret != SPI_OK_INSERT) {
							FAIL_FRAME_SET(&funcctx->max_calls,
										TRF2_statements[5].sql);
							dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
									trade_id, trade_price, trade_qty,
									type_is_sell);
						}

#ifdef DEBUG
						sprintf(sql, SQLTRF2_5a, hold_qty - needed_qty, hold_id);
						elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
						args[0] = Int32GetDatum(hold_qty - needed_qty);
						args[1] = Int64GetDatum(hold_id);
						ret = SPI_execute_plan(TRF2_5a, args, nulls, false, 0);
						if (ret != SPI_OK_UPDATE) {
							FAIL_FRAME_SET(&funcctx->max_calls,
										TRF2_statements[6].sql);
							dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
									trade_id, trade_price, trade_qty,
									type_is_sell);
						}

						buy_value += (double) needed_qty * hold_price;
						sell_value += (double) needed_qty * trade_price;
						needed_qty = 0;
					} else {
						/* Selling all holdings */
#ifdef DEBUG
						sprintf(sql, SQLTRF2_4a, hold_id, trade_id, hold_qty, 0);
						elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
						args[0] = Int64GetDatum(hold_id);
						args[1] = Int64GetDatum(trade_id);
						args[2] = Int32GetDatum(hold_qty);
						args[3] = Int32GetDatum(0);
						ret = SPI_execute_plan(TRF2_4a, args, nulls, false, 0);
						if (ret != SPI_OK_INSERT) {
							FAIL_FRAME_SET(&funcctx->max_calls,
										TRF2_statements[5].sql);
							dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
									trade_id, trade_price, trade_qty,
									type_is_sell);
						}

#ifdef DEBUG
						sprintf(sql, SQLTRF2_5b, hold_id);
						elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
						args[0] = Int64GetDatum(hold_id);
						ret = SPI_execute_plan(TRF2_5b, args, nulls, false, 0);
						if (ret != SPI_OK_DELETE) {
							FAIL_FRAME_SET(&funcctx->max_calls,
										TRF2_statements[7].sql);
							dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
									trade_id, trade_price, trade_qty,
									type_is_sell);
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
#ifdef DEBUG
				sprintf(sql, SQLTRF2_4a, trade_id, trade_id, 0, -1 * needed_qty);
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				args[0] = Int64GetDatum(trade_id);
				args[1] = Int64GetDatum(trade_id);
				args[2] = Int32GetDatum(0);
				args[3] = Int32GetDatum(-1 * needed_qty);
				ret = SPI_execute_plan(TRF2_4a, args, nulls, false, 0);
				if (ret != SPI_OK_INSERT) {
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
					FAIL_FRAME_SET(&funcctx->max_calls, TRF2_statements[5].sql);
				}
#ifdef DEBUG
				sprintf(sql, SQLTRF2_7a, trade_id, acct_id, symbol,
						values[i_trade_dts], trade_price, -1 * needed_qty);
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				args[0] = Int64GetDatum(trade_id);
				args[1] = Int64GetDatum(acct_id);
				args[2] = CStringGetTextDatum(symbol);
				args[3] = TimestampGetDatum(values[i_trade_dts]);
				args[4] = Float8GetDatum(trade_price);
				args[5] = Int32GetDatum(-1 * needed_qty);
				ret = SPI_execute_plan(TRF2_7a, args, nulls, false, 0);
				if (ret != SPI_OK_INSERT) {
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
					FAIL_FRAME_SET(&funcctx->max_calls, TRF2_statements[7].sql);
				}
			} else if (hs_qty == trade_qty) {
#ifdef DEBUG
				sprintf(sql, SQLTRF2_7b, acct_id, symbol);
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				args[0] = Int64GetDatum(acct_id);
				args[1] = CStringGetTextDatum(symbol);
				ret = SPI_execute_plan(TRF2_7b, args, nulls, false, 0);
				if (ret != SPI_OK_DELETE) {
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
					FAIL_FRAME_SET(&funcctx->max_calls, TRF2_statements[9].sql);
				}
			}
		} else {
			/* The trade is a BUY */
			if (hs_qty == 0) {
				/* no prior holdings exists, but one will be inserted */
#ifdef DEBUG
				sprintf(sql, SQLTRF2_8a, acct_id, symbol, trade_qty);
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				args[0] = Int64GetDatum(acct_id);
				args[1] = CStringGetTextDatum(symbol);
				args[2] = Int32GetDatum(trade_qty);
				ret = SPI_execute_plan(TRF2_8a, args, nulls, false, 0);
				if (ret != SPI_OK_INSERT) {
					FAIL_FRAME_SET(&funcctx->max_calls, TRF2_statements[10].sql);
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
				}
			} else if ((-1 * hs_qty) != trade_qty) {
#ifdef DEBUG
				sprintf(sql, SQLTRF2_8b, hs_qty + trade_qty, acct_id, symbol);
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				args[0] = Int32GetDatum(hs_qty + trade_qty);
				args[1] = Int64GetDatum(acct_id);
				args[2] = CStringGetTextDatum(symbol);
				ret = SPI_execute_plan(TRF2_8b, args, nulls, false, 0);
				if (ret != SPI_OK_UPDATE) {
					FAIL_FRAME_SET(&funcctx->max_calls, TRF2_statements[11].sql);
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
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
#ifdef DEBUG
				if (is_lifo == 1) {
					sprintf(sql, SQLTRF2_3a, acct_id, symbol);
				} else {
					sprintf(sql, SQLTRF2_3b, acct_id, symbol);
				}
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				args[0] = Int64GetDatum(acct_id);
				args[1] = CStringGetTextDatum(symbol);
				if (is_lifo == 1) {
					ret = SPI_execute_plan(TRF2_3a, args, nulls, false, 0);
				} else {
					ret = SPI_execute_plan(TRF2_3b, args, nulls, false, 0);
				}
				if (ret != SPI_OK_SELECT) {
					FAIL_FRAME_SET(&funcctx->max_calls, is_lifo == 1?
							TRF2_statements[3].sql: TRF2_statements[4].sql);
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
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
						sprintf(sql, SQLTRF2_4a, hold_id, trade_id, hold_qty,
								hold_qty + needed_qty);
						elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
						args[0] = Int64GetDatum(hold_id);
						args[1] = Int64GetDatum(trade_id);
						args[2] = Int32GetDatum(hold_qty);
						args[3] = Int32GetDatum(hold_qty + needed_qty);
						ret = SPI_execute_plan(TRF2_4a, args, nulls, false, 0);
						if (ret != SPI_OK_INSERT) {
							FAIL_FRAME_SET(&funcctx->max_calls,
										TRF2_statements[5].sql);
							dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
									trade_id, trade_price, trade_qty,
									type_is_sell);
						}

#ifdef DEBUG
						sprintf(sql, SQLTRF2_5a, hold_qty + needed_qty, hold_id);
						elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
						args[0] = Int32GetDatum(hold_qty + needed_qty);
						args[1] = Int64GetDatum(hold_id);
						ret = SPI_execute_plan(TRF2_5a, args, nulls, false, 0);
						if (ret != SPI_OK_UPDATE) {
							FAIL_FRAME_SET(&funcctx->max_calls,
										TRF2_statements[6].sql);
							dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
									trade_id, trade_price, trade_qty,
									type_is_sell);
						}

						buy_value += (double) needed_qty * hold_price;
						sell_value += (double) needed_qty * trade_price;
						needed_qty = 0;
					} else {
						/* Buying back all of the Short Sell */
#ifdef DEBUG
						sprintf(sql, SQLTRF2_4a, hold_id, trade_id, hold_qty, 0);
						elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
						args[0] = Int64GetDatum(hold_id);
						args[1] = Int64GetDatum(trade_id);
						args[2] = Int32GetDatum(hold_qty);
						args[3] = Int32GetDatum(0);
						ret = SPI_execute_plan(TRF2_4a, args, nulls, false, 0);
						if (ret != SPI_OK_INSERT) {
							FAIL_FRAME_SET(&funcctx->max_calls,
										TRF2_statements[5].sql);
							dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
									trade_id, trade_price, trade_qty,
									type_is_sell);
						}

#ifdef DEBUG
						sprintf(sql, SQLTRF2_5b, hold_id);
						elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
						args[0] = Int64GetDatum(hold_id);
						ret = SPI_execute_plan(TRF2_5b, args, nulls, false, 0);
						if (ret != SPI_OK_DELETE) {
							FAIL_FRAME_SET(&funcctx->max_calls, TRF2_statements[7].sql);
							dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
									trade_id, trade_price, trade_qty,
									type_is_sell);
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
#ifdef DEBUG
				sprintf(sql, SQLTRF2_4a, trade_id, trade_id, 0, needed_qty);
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				args[0] = Int64GetDatum(trade_id);
				args[1] = Int64GetDatum(trade_id);
				args[2] = Int32GetDatum(0);
				args[3] = Int32GetDatum(needed_qty);
				ret = SPI_execute_plan(TRF2_4a, args, nulls, false, 0);
				if (ret != SPI_OK_INSERT) {
					FAIL_FRAME_SET(&funcctx->max_calls, TRF2_statements[5].sql);
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
				}
#ifdef DEBUG
				sprintf(sql, SQLTRF2_7a, trade_id, acct_id, symbol,
						values[i_trade_dts], trade_price, needed_qty);
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				args[0] = Int64GetDatum(trade_id);
				args[1] = Int64GetDatum(acct_id);
				args[2] = CStringGetTextDatum(symbol);
				args[3] = TimestampGetDatum(values[i_trade_dts]);
				args[4] = Float8GetDatum(trade_price);
				args[5] = Int32GetDatum(needed_qty);
				ret = SPI_execute_plan(TRF2_7a, args, nulls, false, 0);
				if (ret != SPI_OK_INSERT) {
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
					FAIL_FRAME_SET(&funcctx->max_calls, TRF2_statements[7].sql);
				}
			} else if ((-1 * hs_qty) == trade_qty) {
#ifdef DEBUG
				sprintf(sql, SQLTRF2_7b, acct_id, symbol);
				elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
				args[0] = Int64GetDatum(acct_id);
				args[1] = CStringGetTextDatum(symbol);
				ret = SPI_execute_plan(TRF2_7b, args, nulls, false, 0);
				if (ret != SPI_OK_DELETE) {
					dump_trf2_inputs(acct_id, hs_qty, is_lifo, symbol,
							trade_id, trade_price, trade_qty, type_is_sell);
					FAIL_FRAME_SET(&funcctx->max_calls, TRF2_statements[9].sql);
				}
			}
		}

		sprintf(values[i_buy_value], "%.2f", buy_value);
		sprintf(values[i_sell_value], "%.2f", sell_value);

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
		for (i = 0; i < 6; i++) {
			elog(NOTICE, "TRF2 OUT: %d %s", i, values[i]);
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
Datum TradeResultFrame3(PG_FUNCTION_ARGS)
{
	Numeric buy_value_num = PG_GETARG_NUMERIC(0);
	long cust_id = PG_GETARG_INT64(1);
	Numeric sell_value_num = PG_GETARG_NUMERIC(2);
	long trade_id = PG_GETARG_INT64(3);

	int ret;
	TupleDesc tupdesc;
	SPITupleTable *tuptable = NULL;
	HeapTuple tuple = NULL;

	Datum result;
#ifdef DEBUG
	char sql[2048];
#endif
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
	sprintf(sql, SQLTRF3_1, cust_id);
	elog(NOTICE, "SQL\n%s", sql);
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
		dump_trf3_inputs(buy_value, cust_id, sell_value, trade_id);
	}

#ifdef DEBUG
	sprintf(sql, SQLTRF3_2, tax_amount, trade_id);
	elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
	args[0] = Float8GetDatum(tax_amount);
	args[1] = Int64GetDatum(trade_id);
	ret = SPI_execute_plan(TRF3_2, args, nulls, false, 0);
	if (ret != SPI_OK_UPDATE) {
		FAIL_FRAME(TRF3_statements[1].sql);
		dump_trf3_inputs(buy_value, cust_id, sell_value, trade_id);
	}

#ifdef DEBUG
	elog(NOTICE, "TRF4 OUT: 1 %f", tax_amount);
#endif /* DEBUG */

	SPI_finish();
	result = DirectFunctionCall1(float8_numeric, Float8GetDatum(tax_amount));
	PG_RETURN_NUMERIC(result);
}

/* Clause 3.3.8.6 */
Datum TradeResultFrame4(PG_FUNCTION_ARGS)
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

		enum trf4 { i_comm_rate=0, i_s_name };

		long cust_id = PG_GETARG_INT64(0);
		char *symbol_p = (char *) PG_GETARG_TEXT_P(1);
		int trade_qty = PG_GETARG_INT32(2);
		char *type_id_p = (char *) PG_GETARG_TEXT_P(3);

		int ret;
		TupleDesc tupdesc;
		SPITupleTable *tuptable = NULL;
		HeapTuple tuple = NULL;
#ifdef DEBUG
		char sql[2048];
#endif
		Datum args[5];
		char nulls[5] = { ' ', ' ', ' ', ' ', ' ' };

		char symbol[S_SYMB_LEN + 1];
		char type_id[TT_ID_LEN + 1];

		char *s_ex_id = NULL;
		char *c_tier = NULL;

		strncpy(symbol, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(symbol_p))), S_SYMB_LEN);
		symbol[S_SYMB_LEN] = '\0';
		strncpy(type_id, DatumGetCString(DirectFunctionCall1(textout,
				PointerGetDatum(type_id_p))), TT_ID_LEN);
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
		sprintf(sql, SQLTRF4_1, symbol);
		elog(NOTICE, "SQL\n%s", sql);
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
			dump_trf4_inputs(cust_id, symbol, trade_qty, type_id);
		}

#ifdef DEBUG
		sprintf(sql, SQLTRF4_2, cust_id);
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		args[0] = Int64GetDatum(cust_id);
		ret = SPI_execute_plan(TRF4_2, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			c_tier = SPI_getvalue(tuple, tupdesc, 1);
		} else {
			dump_trf4_inputs(cust_id, symbol, trade_qty, type_id);
			FAIL_FRAME_SET(&funcctx->max_calls, TRF4_statements[1].sql);
		}

#ifdef DEBUG
		sprintf(sql, SQLTRF4_3, c_tier, type_id, s_ex_id, trade_qty, trade_qty);
		elog(NOTICE, "SQL\n%s", sql);
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
			dump_trf4_inputs(cust_id, symbol, trade_qty, type_id);
			FAIL_FRAME_SET(&funcctx->max_calls, TRF4_statements[2].sql);
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
		for (i = 0; i < 2; i++) {
			elog(NOTICE, "TRF4 OUT: %d %s", i, values[i]);
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
Datum TradeResultFrame5(PG_FUNCTION_ARGS)
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
#ifdef DEBUG
	char sql[2048];
#endif
	Datum args[5];
	char nulls[5] = { ' ', ' ', ' ', ' ', ' '};

	double comm_amount;
	double trade_price;
	char trade_dts[MAXDATELEN + 1];
	char st_completed_id[ST_ID_LEN + 1];

	strncpy(st_completed_id, DatumGetCString(DirectFunctionCall1(textout,
			PointerGetDatum(st_completed_id_p))), ST_ID_LEN);
	st_completed_id[ST_ID_LEN] = '\0';

	comm_amount = DatumGetFloat8(DirectFunctionCall1(
			numeric_float8_no_overflow, PointerGetDatum(comm_amount_num)));
	trade_price = DatumGetFloat8(DirectFunctionCall1(
			numeric_float8_no_overflow, PointerGetDatum(trade_price_num)));

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
	sprintf(sql, SQLTRF5_1, comm_amount, trade_dts, st_completed_id, trade_price,
			trade_id);
	elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
	args[0] = PointerGetDatum(comm_amount_num);
	args[1] = TimestampGetDatum(trade_dts_ts);
	args[2] = CStringGetTextDatum(st_completed_id);
	args[3] = PointerGetDatum(trade_price_num);
	args[4] = Int64GetDatum(trade_id);
	ret = SPI_execute_plan(TRF5_1, args, nulls, false, 0);
	if (ret != SPI_OK_UPDATE) {
		FAIL_FRAME(TRF5_statements[0].sql);
		dump_trf5_inputs(broker_id, comm_amount, st_completed_id, trade_dts,
				trade_id, trade_price);
	}

#ifdef DEBUG
	sprintf(sql, SQLTRF5_2, trade_id, trade_dts, st_completed_id);
	elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
	args[0] = Int64GetDatum(trade_id);
	args[1] = TimestampGetDatum(trade_dts_ts);
	args[2] = CStringGetTextDatum(st_completed_id);
	ret = SPI_execute_plan(TRF5_2, args, nulls, false, 0);
	if (ret != SPI_OK_INSERT) {
		FAIL_FRAME(TRF5_statements[1].sql);
		dump_trf5_inputs(broker_id, comm_amount, st_completed_id, trade_dts,
				trade_id, trade_price);
	}

#ifdef DEBUG
	sprintf(sql, SQLTRF5_3, comm_amount, broker_id);
	elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
	args[0] = Float8GetDatum(comm_amount);
	args[1] = Int64GetDatum(broker_id);
	ret = SPI_execute_plan(TRF5_3, args, nulls, false, 0);
	if (ret != SPI_OK_UPDATE) {
		FAIL_FRAME(TRF5_statements[2].sql);
		dump_trf5_inputs(broker_id, comm_amount, st_completed_id, trade_dts,
				trade_id, trade_price);
	}

	SPI_finish();
	PG_RETURN_INT32(0);
}

/* Clause 3.3.8.8 */
Datum TradeResultFrame6(PG_FUNCTION_ARGS)
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

	Datum result;
#ifdef DEBUG
	char sql[2048];
#endif
	Datum args[6];
	char nulls[6] = { ' ', ' ', ' ', ' ', ' ', ' '};

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

	s_name_tmp =  DatumGetCString(DirectFunctionCall1(textout,
               PointerGetDatum(s_name_p)));

	for (i = 0; i < S_NAME_LEN && s_name_tmp[i] != '\0'; i++) {
		if (s_name_tmp[i] == '\'')
			s_name[k++] = '\\';
		s_name[k++] = s_name_tmp[i];
	}
	s_name[k] = '\0';
	s_name[S_NAME_LEN] = '\0';

	strncpy(type_name, DatumGetCString(DirectFunctionCall1(textout,
			PointerGetDatum(type_name_p))), TT_NAME_LEN);
	type_name[TT_NAME_LEN] = '\0';

	if (timestamp2tm(due_date_ts, NULL, tm, &fsec, NULL, NULL) == 0) {
		EncodeDateTimeM(tm, fsec, tzn, due_date);
	}
	if (timestamp2tm(trade_dts_ts, NULL, tm, &fsec, NULL, NULL) == 0) {
		EncodeDateTimeM(tm, fsec, tzn, trade_dts);
	}

#ifdef DEBUG
	dump_trf6_inputs(acct_id, due_date, s_name, se_amount, trade_dts,
			trade_id, trade_is_cash, trade_qty, type_name);
#endif

	SPI_connect();
	plan_queries(TRF6_statements);

	if (trade_is_cash == 1) {
		strcpy(cash_type, "Cash Account");
	} else {
		strcpy(cash_type, "Margin");
	}

#ifdef DEBUG
	sprintf(sql, SQLTRF6_1, trade_id, cash_type, due_date, se_amount);
	elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
	args[0] = Int64GetDatum(trade_id);
	args[1] = CStringGetTextDatum(cash_type);
	args[2] = DirectFunctionCall1(date_in, CStringGetDatum(due_date));
	args[4] = Float8GetDatum(se_amount);
	ret = SPI_execute_plan(TRF6_1, args, nulls, false, 0);
	if (ret != SPI_OK_INSERT) {
		FAIL_FRAME(TRF6_statements[0].sql);
		dump_trf6_inputs(acct_id, due_date, s_name, se_amount, trade_dts,
				trade_id, trade_is_cash, trade_qty, type_name);
	}

	if (trade_is_cash == 1) {
#ifdef DEBUG
		sprintf(sql, SQLTRF6_2, se_amount, acct_id);
		elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
		args[0] = Float8GetDatum(se_amount);
		args[1] = Int64GetDatum(acct_id);
		ret = SPI_execute_plan(TRF6_2, args, nulls, false, 0);
		if (ret != SPI_OK_UPDATE) {
			FAIL_FRAME(TRF6_statements[1].sql);
			dump_trf6_inputs(acct_id, due_date, s_name, se_amount, trade_dts,
					trade_id, trade_is_cash, trade_qty, type_name);
		}
#ifdef DEBUG
		sprintf(sql, SQLTRF6_3, trade_dts, trade_id, se_amount, type_name,
				trade_qty, s_name);
		elog(NOTICE, "SQL\n%s", sql);
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
			dump_trf6_inputs(acct_id, due_date, s_name, se_amount,
					trade_dts, trade_id, trade_is_cash, trade_qty, type_name);
		}
	}

#ifdef DEBUG
	sprintf(sql, SQLTRF6_4, acct_id);
	elog(NOTICE, "SQL\n%s", sql);
#endif /* DEBUG */
	args[0] = Int64GetDatum(acct_id);
	ret = SPI_execute_plan(TRF6_4, args, nulls, true, 0);
	if (ret == SPI_OK_SELECT && SPI_processed > 0) {
		tupdesc = SPI_tuptable->tupdesc;
		tuptable = SPI_tuptable;
		tuple = tuptable->vals[0];
		acct_bal = atof(SPI_getvalue(tuple, tupdesc, 1));
	} else {
		dump_trf6_inputs(acct_id, due_date, s_name, se_amount, trade_dts,
				trade_id, trade_is_cash, trade_qty, type_name);
		FAIL_FRAME(TRF6_statements[3].sql);
	}

#ifdef DEBUG
		elog(NOTICE, "TRF5 OUT: 1 %f", acct_bal);
#endif /* DEBUG */

	SPI_finish();
	result = DirectFunctionCall1(float8_numeric, Float8GetDatum(acct_bal));
	PG_RETURN_NUMERIC(result);
}
