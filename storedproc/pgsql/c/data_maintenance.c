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
#include <executor/executor.h> /* for GetAttributeByName() */
#include <funcapi.h> /* for returning set of rows in order_status */
#include <utils/datetime.h>
#include <utils/numeric.h>
#include <utils/builtins.h>
#include <catalog/pg_type.h>

#include "frame.h"
#include "dbt5common.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define TABLE_NAME_LEN 18

#define SQLDMF1_1                                                             \
	"SELECT ap_acl\n"                                                         \
	"FROM account_permission\n"                                               \
	"WHERE ap_ca_id = $1\n"                                                   \
	"ORDER BY ap_acl DESC\n"                                                  \
	"LIMIT 1"

#define SQLDMF1_2                                                             \
	"UPDATE account_permission\n"                                             \
	"SET ap_acl = $1\n"                                                       \
	"WHERE ap_ca_id = $2\n"                                                   \
	"  AND ap_acl = $3"

#define SQLDMF1_3a                                                            \
	"SELECT ad_line2, ad_id\n"                                                \
	"FROM address, customer\n"                                                \
	"WHERE ad_id = c_ad_id\n"                                                 \
	"  AND c_id = $1"

#define SQLDMF1_3b                                                            \
	"SELECT ad_line2, ad_id\n"                                                \
	"FROM address, company\n"                                                 \
	"WHERE ad_id = co_ad_id\n"                                                \
	"  AND co_id = $1"

#define SQLDMF1_4                                                             \
	"UPDATE address\n"                                                        \
	"SET ad_line2 = $1 \n"                                                    \
	"WHERE ad_id = $2"

#define SQLDMF1_5                                                             \
	"SELECT co_sp_rate\n"                                                     \
	"FROM company\n"                                                          \
	"WHERE co_id = $1"

#define SQLDMF1_6                                                             \
	"UPDATE company\n"                                                        \
	"SET co_sp_rate = $1\n"                                                   \
	"WHERE co_id = $2"

#define SQLDMF1_7                                                             \
	"SELECT c_email_2\n"                                                      \
	"FROM customer\n"                                                         \
	"WHERE c_id = $1"

#define SQLDMF1_8                                                             \
	"UPDATE customer\n"                                                       \
	"SET c_email_2 = SUBSTRING(c_email_2\n"                                   \
	"                          FROM '#\"%%@#\"%%'\n"                          \
	"                          FOR '#') || $1\n"                              \
	"WHERE c_id = $2"

#define SQLDMF1_9                                                             \
	"SELECT cx_tx_id\n"                                                       \
	"FROM customer_taxrate\n"                                                 \
	"WHERE cx_c_id = $1\n"                                                    \
	"  AND (cx_tx_id LIKE 'US%%' OR cx_tx_id LIKE 'CN%%')"

#define SQLDMF1_10                                                            \
	"UPDATE customer_taxrate\n"                                               \
	"SET cx_tx_id = $1\n"                                                     \
	"WHERE cx_c_id = $2\n"                                                    \
	"  AND cx_tx_id = $3"

#define SQLDMF1_11                                                            \
	"UPDATE daily_market\n"                                                   \
	"SET dm_vol = dm_vol + $1\n"                                              \
	"WHERE dm_s_symb = $2\n"                                                  \
	"  AND EXTRACT(DAY FROM dm_date) = $3"

#define SQLDMF1_12                                                            \
	"SELECT COUNT(*)\n"                                                       \
	"FROM exchange\n"                                                         \
	"WHERE ex_desc LIKE '%%LAST UPDATED%%'"

#define SQLDMF1_13a                                                           \
	"UPDATE exchange\n"                                                       \
	"SET ex_desc = ex_desc || ' LAST UPDATED ' || now()\n"

#define SQLDMF1_13b                                                           \
	"UPDATE exchange\n"                                                       \
	"SET ex_desc = SUBSTRING(ex_desc || ' LAST UPDATED ' || NOW()\n"          \
	"                        FROM 1 FOR (CHAR_LENGTH(ex_desc) -\n"            \
	"                                    CHAR_LENGTH(NOW()::TEXT))) || NOW()"

#define SQLDMF1_14                                                            \
	"SELECT COUNT(*)\n"                                                       \
	"FROM financial\n"                                                        \
	"WHERE fi_co_id = $1\n"                                                   \
	"  AND EXTRACT(DAY FROM fi_qtr_start_date) = 1"

#define SQLDMF1_15a                                                           \
	"UPDATE financial\n"                                                      \
	"SET fi_qtr_start_date = fi_qtr_start_date + INTERVAL '1 day'\n"          \
	"WHERE fi_co_id = $1"

#define SQLDMF1_15b                                                           \
	"UPDATE financial\n"                                                      \
	"SET fi_qtr_start_date = fi_qtr_start_date - INTERVAL '1 day'\n"          \
	"WHERE fi_co_id = $1"

#define SQLDMF1_16                                                            \
	"UPDATE news_item\n"                                                      \
	"SET ni_dts = ni_dts + INTERVAL '1 day'\n"                                \
	"WHERE ni_id IN (SELECT nx_ni_id\n"                                       \
	"               FROM news_xref\n"                                         \
	"               WHERE nx_co_id = $1)"

#define SQLDMF1_17                                                            \
	"UPDATE security\n"                                                       \
	"SET s_exch_date = s_exch_date + INTERVAL '1 day'\n"                      \
	"WHERE s_symb = $1"

#define SQLDMF1_18                                                            \
	"SELECT tx_name\n"                                                        \
	"FROM taxrate\n"                                                          \
	"WHERE tx_id = $1"

#define SQLDMF1_19                                                            \
	"UPDATE taxrate\n"                                                        \
	"SET tx_name = $1\n"                                                      \
	"WHERE tx_id = $2"

#define SQLDMF1_20                                                            \
	"SELECT count(*)\n"                                                       \
	"FROM watch_item, watch_list\n"                                           \
	"WHERE wl_c_id = $1\n"                                                    \
	"  AND wi_wl_id = wl_id"

#define SQLDMF1_21                                                            \
	"SELECT wi_s_symb\n"                                                      \
	"FROM (SELECT wi_s_symb\n"                                                \
	"      FROM watch_item, watch_list\n"                                     \
	"      WHERE wl_c_id = $1\n"                                              \
	"        AND wi_wl_id = wl_id\n"                                          \
	"      ORDER BY wi_s_symb ASC) AS foo\n"                                  \
	"OFFSET $2\n"                                                             \
	"LIMIT 1"

#define SQLDMF1_22                                                            \
	"SELECT s_symb\n"                                                         \
	"FROM security\n"                                                         \
	"WHERE s_symb > $1\n"                                                     \
	"  AND s_symb NOT IN (SELECT wi_s_symb\n"                                 \
	"                     FROM watch_item, watch_list\n"                      \
	"                     WHERE wl_c_id = $2\n"                               \
	"                       AND wi_wl_id = wl_id)\n"                          \
	"ORDER BY s_symb ASC\n"                                                   \
	"LIMIT 1"

#define SQLDMF1_23                                                            \
	"UPDATE watch_item\n"                                                     \
	"SET wi_s_symb = $1\n"                                                    \
	"FROM watch_list\n"                                                       \
	"WHERE wl_c_id = $2\n"                                                    \
	"  AND wi_wl_id = wl_id\n"                                                \
	"  AND wi_s_symb = $3"

#define DMF1_1 DMF1_statements[0].plan
#define DMF1_2 DMF1_statements[1].plan
#define DMF1_3a DMF1_statements[2].plan
#define DMF1_3b DMF1_statements[3].plan
#define DMF1_4 DMF1_statements[4].plan
#define DMF1_5 DMF1_statements[5].plan
#define DMF1_6 DMF1_statements[6].plan
#define DMF1_7 DMF1_statements[7].plan
#define DMF1_8 DMF1_statements[8].plan
#define DMF1_9 DMF1_statements[9].plan
#define DMF1_10 DMF1_statements[10].plan
#define DMF1_11 DMF1_statements[11].plan
#define DMF1_12 DMF1_statements[12].plan
#define DMF1_13a DMF1_statements[13].plan
#define DMF1_13b DMF1_statements[14].plan
#define DMF1_14 DMF1_statements[15].plan
#define DMF1_15a DMF1_statements[16].plan
#define DMF1_15b DMF1_statements[17].plan
#define DMF1_16 DMF1_statements[18].plan
#define DMF1_17 DMF1_statements[19].plan
#define DMF1_18 DMF1_statements[20].plan
#define DMF1_19 DMF1_statements[21].plan
#define DMF1_20 DMF1_statements[22].plan
#define DMF1_21 DMF1_statements[23].plan
#define DMF1_22 DMF1_statements[24].plan
#define DMF1_23 DMF1_statements[25].plan

static cached_statement DMF1_statements[] = {

	{ SQLDMF1_1, 1, { INT8OID } },

	{ SQLDMF1_2, 3, { TEXTOID, INT8OID, TEXTOID } },

	{ SQLDMF1_3a, 1, { INT8OID } }, { SQLDMF1_3b, 1, { INT8OID } },

	{ SQLDMF1_4, 2, { TEXTOID, INT8OID } }, { SQLDMF1_5, 1, { INT8OID } },

	{ SQLDMF1_6, 2, { TEXTOID, INT8OID } }, { SQLDMF1_7, 1, { INT8OID } },

	{ SQLDMF1_8, 2, { TEXTOID, INT8OID } }, { SQLDMF1_9, 1, { INT8OID } },

	{ SQLDMF1_10, 3, { TEXTOID, INT8OID, TEXTOID } },

	{ SQLDMF1_11, 3, { INT8OID, TEXTOID, INT2OID } },

	{
			SQLDMF1_12,
	},

	{
			SQLDMF1_13a,
	},

	{
			SQLDMF1_13b,
	},

	{ SQLDMF1_14, 1, { INT8OID } }, { SQLDMF1_15a, 1, { INT8OID } },

	{ SQLDMF1_15b, 1, { INT8OID } }, { SQLDMF1_16, 1, { INT8OID } },

	{ SQLDMF1_17, 1, { TEXTOID } }, { SQLDMF1_18, 1, { TEXTOID } },

	{ SQLDMF1_19, 2, { TEXTOID, TEXTOID } }, { SQLDMF1_20, 1, { INT8OID } },

	{ SQLDMF1_21, 2, { INT8OID, INT2OID } },

	{ SQLDMF1_22, 2, { TEXTOID, INT8OID } },

	{ SQLDMF1_23, 3, { TEXTOID, INT8OID, TEXTOID } },

	{ NULL }
};

/* Prototypes. */
#ifdef DEBUG
void dump_dmf1_inputs(long, long, long, int, char *, char *, char *, int);
#endif /* DEBUG */

Datum DataMaintenanceFrame1(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(DataMaintenanceFrame1);

#ifdef DEBUG
void
dump_dmf1_inputs(long acct_id, long c_id, long co_id, int day_of_month,
		char *symbol, char *table_name, char *tx_id, int vol_incr)
{
	elog(DEBUG1, "DMF1: INPUTS START");
	elog(DEBUG1, "DMF1: acct_id %ld", acct_id);
	elog(DEBUG1, "DMF1: c_id %ld", c_id);
	elog(DEBUG1, "DMF1: co_id %ld", co_id);
	elog(DEBUG1, "DMF1: day_of_month %d", day_of_month);
	elog(DEBUG1, "DMF1: symbol %s", symbol);
	elog(DEBUG1, "DMF1: table_name %s", table_name);
	elog(DEBUG1, "DMF1: tx_id %s", tx_id);
	elog(DEBUG1, "DMF1: vol_incr %d", vol_incr);
	elog(DEBUG1, "DMF1: INPUTS END");
}
#endif /* DEBUG */

/* Clause 3.3.9.3 */
Datum
DataMaintenanceFrame1(PG_FUNCTION_ARGS)
{
	long acct_id = PG_GETARG_INT64(0);
	long c_id = PG_GETARG_INT64(1);
	long co_id = PG_GETARG_INT64(2);
	int day_of_month = PG_GETARG_INT32(3);
	char *symbol_p = (char *) PG_GETARG_TEXT_P(4);
	char *table_name_p = (char *) PG_GETARG_TEXT_P(5);
	char *tx_id_p = (char *) PG_GETARG_TEXT_P(6);
	int vol_incr = PG_GETARG_INT32(7);

	char symbol[S_SYMB_LEN + 1];
	char table_name[TABLE_NAME_LEN + 1];
	char tx_id[TX_ID_LEN + 1];

	int ret;
	TupleDesc tupdesc;
	SPITupleTable *tuptable = NULL;
	HeapTuple tuple = NULL;

	int status = 0;
	Datum args[3];
	char nulls[3] = { ' ', ' ', ' ' };

	strncpy(symbol,
			DatumGetCString(
					DirectFunctionCall1(textout, PointerGetDatum(symbol_p))),
			S_SYMB_LEN);
	symbol[S_SYMB_LEN] = '\0';
	strncpy(table_name,
			DatumGetCString(DirectFunctionCall1(
					textout, PointerGetDatum(table_name_p))),
			TABLE_NAME_LEN);
	table_name[TABLE_NAME_LEN] = '\0';
	strncpy(tx_id,
			DatumGetCString(
					DirectFunctionCall1(textout, PointerGetDatum(tx_id_p))),
			TX_ID_LEN);
	tx_id[TX_ID_LEN] = '\0';

#ifdef DEBUG
	dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol, table_name,
			tx_id, vol_incr);
#endif

	SPI_connect();
	plan_queries(DMF1_statements);

	if (strcmp(table_name, "ACCOUNT_PERMISSION") == 0) {
		char *acl = NULL;

#ifdef DEBUG
		elog(DEBUG1, "%s", SQLDMF1_1);
#endif /* DEBUG */
		args[0] = Int64GetDatum(acct_id);
		ret = SPI_execute_plan(DMF1_1, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			acl = SPI_getvalue(tuple, tupdesc, 1);
		} else {
			FAIL_FRAME(DMF1_statements[0].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}

#ifdef DEBUG
		elog(DEBUG1, "acl = '%s'", acl);
#endif /* DEBUG */
		if (strcmp(acl, "1111") != 0) {
			args[0] = CStringGetTextDatum("1111");
		} else {
			args[0] = CStringGetTextDatum("0011");
		}
		args[1] = Int64GetDatum(acct_id);
		args[2] = CStringGetTextDatum(acl);
		ret = SPI_execute_plan(DMF1_2, args, nulls, false, 0);
		if (ret != SPI_OK_UPDATE) {
			FAIL_FRAME(DMF1_statements[1].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}
	} else if (strcmp(table_name, "ADDRESS") == 0) {
		long ad_id = 0;
		char *line2 = NULL;
		if (c_id != 0) {
#ifdef DEBUG
			elog(DEBUG1, "%s", SQLDMF1_3a);
#endif /* DEBUG */
			args[0] = Int64GetDatum(c_id);
			ret = SPI_execute_plan(DMF1_3a, args, nulls, true, 0);
		} else {
#ifdef DEBUG
			elog(DEBUG1, "%s", SQLDMF1_3b);
#endif /* DEBUG */
			args[0] = Int64GetDatum(co_id);
			ret = SPI_execute_plan(DMF1_3b, args, nulls, true, 0);
		}
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			line2 = SPI_getvalue(tuple, tupdesc, 1);
			ad_id = atol(SPI_getvalue(tuple, tupdesc, 2));
		} else {
			FAIL_FRAME(c_id != 0 ? DMF1_statements[2].sql
								 : DMF1_statements[3].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}

		/*
		 * FIXME:
		 * Verify that line2 can even be NULL from the previoius query.
		 */
		if (line2 != NULL && strcmp(line2, "Apt. 10C") != 0) {
			args[0] = CStringGetTextDatum("Apt. 10C");
		} else {
			args[0] = CStringGetTextDatum("Apt. 22");
		}
#ifdef DEBUG
		elog(DEBUG1, "%s", SQLDMF1_4);
#endif /* DEBUG */
		args[1] = Int64GetDatum(ad_id);
		ret = SPI_execute_plan(DMF1_4, args, nulls, false, 0);
		if (ret != SPI_OK_UPDATE) {
			FAIL_FRAME(DMF1_statements[4].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}
	} else if (strcmp(table_name, "COMPANY") == 0) {
		char *sprate = NULL;
#ifdef DEBUG
		elog(DEBUG1, "%s", SQLDMF1_5);
#endif /* DEBUG */
		args[0] = Int64GetDatum(co_id);
		ret = SPI_execute_plan(DMF1_5, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			sprate = SPI_getvalue(tuple, tupdesc, 1);
		} else {
			FAIL_FRAME(DMF1_statements[5].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}

#ifdef DEBUG
		elog(DEBUG1, "%s", SQLDMF1_6);
		elog(DEBUG1, "sprate = '%s'", sprate);
#endif /* DEBUG */
		if (strcmp(sprate, "ABA") != 0) {
			args[0] = CStringGetTextDatum("ABA");
		} else {
			args[0] = CStringGetTextDatum("AAA");
		}
		args[1] = Int64GetDatum(co_id);
		ret = SPI_execute_plan(DMF1_6, args, nulls, false, 0);
		if (ret != SPI_OK_UPDATE) {
			FAIL_FRAME(DMF1_statements[6].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}
	} else if (strcmp(table_name, "CUSTOMER") == 0) {
		char *email2 = NULL;
		int len = 0;
		int lenMindspring = strlen("@mindspring.com");
#ifdef DEBUG
		elog(DEBUG1, "%s", SQLDMF1_7);
#endif /* DEBUG */
		args[0] = Int64GetDatum(c_id);
		ret = SPI_execute_plan(DMF1_7, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			email2 = SPI_getvalue(tuple, tupdesc, 1);
		} else {
			FAIL_FRAME(DMF1_statements[7].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}

		len = strlen(email2);
#ifdef DEBUG
		elog(DEBUG1, "%s", SQLDMF1_8);
#endif /* DEBUG */
		if ((len - lenMindspring) > 0 && strstr(email2, "@mindspring.com")) {
			args[0] = CStringGetTextDatum("earthlink.com");
		} else {
			args[0] = CStringGetTextDatum("mindspring.com");
		}
		args[1] = Int64GetDatum(co_id);
		ret = SPI_execute_plan(DMF1_8, args, nulls, false, 0);
		if (ret != SPI_OK_UPDATE) {
			FAIL_FRAME(DMF1_statements[8].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}
	} else if (strcmp(table_name, "CUSTOMER_TAXRATE") == 0) {
		char new_tax_rate[TX_ID_LEN + 1];
		char *old_tax_rate = NULL;
#ifdef DEBUG
		elog(DEBUG1, "%s", SQLDMF1_9);
#endif /* DEBUG */
		args[0] = Int64GetDatum(c_id);
		ret = SPI_execute_plan(DMF1_9, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			old_tax_rate = SPI_getvalue(tuple, tupdesc, 1);
		} else {
			FAIL_FRAME(DMF1_statements[9].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}

#ifdef DEBUG
		elog(DEBUG1, "old_tax_rate = '%s'", old_tax_rate);
#endif /* DEBUG */

		if (strncmp(old_tax_rate, "US", 2) == 0) {
			if (old_tax_rate[2] == '5') {
				strcpy(new_tax_rate, "US1");
			} else {
				sprintf(new_tax_rate, "US%c", old_tax_rate[2] + 1);
			}
		} else {
			if (old_tax_rate[2] == '4') {
				strcpy(new_tax_rate, "CN1");
			} else {
				sprintf(new_tax_rate, "CN%c", old_tax_rate[2] + 1);
			}
		}

#ifdef DEBUG
		elog(DEBUG1, "%s", SQLDMF1_10);
		elog(DEBUG1, "new_tax_rate = '%s'", new_tax_rate);
#endif /* DEBUG */
		args[0] = CStringGetTextDatum(new_tax_rate);
		args[1] = Int64GetDatum(c_id);
		args[2] = CStringGetTextDatum(old_tax_rate);
		ret = SPI_execute_plan(DMF1_10, args, nulls, false, 0);
		if (ret != SPI_OK_UPDATE) {
			FAIL_FRAME(DMF1_statements[10].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}
	} else if (strcmp(table_name, "DAILY_MARKET") == 0) {
#ifdef DEBUG
		elog(DEBUG1, "%s", SQLDMF1_11);
#endif /* DEBUG */
		args[0] = Int64GetDatum(vol_incr);
		args[1] = CStringGetTextDatum(symbol);
		args[2] = Int16GetDatum(day_of_month);
		ret = SPI_execute_plan(DMF1_11, args, nulls, false, 0);
		if (ret != SPI_OK_UPDATE) {
			FAIL_FRAME(DMF1_statements[11].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}
	} else if (strcmp(table_name, "EXCHANGE") == 0) {
		int rowcount = 0;

#ifdef DEBUG
		elog(DEBUG1, "%s", SQLDMF1_12);
#endif /* DEBUG */
		ret = SPI_execute_plan(DMF1_12, NULL, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			rowcount = atof(SPI_getvalue(tuple, tupdesc, 1));
		} else {
			FAIL_FRAME(DMF1_statements[12].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}

#ifdef DEBUG
		elog(DEBUG1, "rowcount = %d", rowcount);
#endif /* DEBUG */

		if (rowcount == 0) {
#ifdef DEBUG
			elog(DEBUG1, "%s", SQLDMF1_13a);
#endif /* DEBUG */
			ret = SPI_execute_plan(DMF1_13a, NULL, nulls, false, 0);
		} else {
#ifdef DEBUG
			elog(DEBUG1, "%s", SQLDMF1_13b);
#endif /* DEBUG */
			ret = SPI_execute_plan(DMF1_13b, NULL, nulls, false, 0);
		}
		if (ret != SPI_OK_UPDATE) {
			FAIL_FRAME(rowcount ? DMF1_statements[14].sql
								: DMF1_statements[13].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}
	} else if (strcmp(table_name, "FINANCIAL") == 0) {
		int rowcount = 0;
#ifdef DEBUG
		elog(DEBUG1, "%s", SQLDMF1_14);
#endif /* DEBUG */
		args[0] = Int64GetDatum(co_id);
		ret = SPI_execute_plan(DMF1_14, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			rowcount = atof(SPI_getvalue(tuple, tupdesc, 1));
		} else {
			FAIL_FRAME(DMF1_statements[15].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}

#ifdef DEBUG
		elog(DEBUG1, "rowcount = %d", rowcount);
#endif /* DEBUG */
		args[0] = Int64GetDatum(co_id);
		if (rowcount > 0) {
#ifdef DEBUG
			elog(DEBUG1, "%s", SQLDMF1_15a);
#endif /* DEBUG */
			ret = SPI_execute_plan(DMF1_15a, args, nulls, false, 0);
		} else {
#ifdef DEBUG
			elog(DEBUG1, "%s", SQLDMF1_15b);
#endif /* DEBUG */
			ret = SPI_execute_plan(DMF1_15b, args, nulls, false, 0);
		}
		if (ret != SPI_OK_UPDATE) {
			FAIL_FRAME(rowcount > 0 ? DMF1_statements[16].sql
									: DMF1_statements[17].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}
	} else if (strcmp(table_name, "NEWS_ITEM") == 0) {
#ifdef DEBUG
		elog(DEBUG1, "%s", SQLDMF1_16);
#endif /* DEBUG */
		args[0] = Int64GetDatum(co_id);
		ret = SPI_execute_plan(DMF1_16, args, nulls, false, 0);
		if (ret != SPI_OK_UPDATE) {
			FAIL_FRAME(DMF1_statements[18].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}
	} else if (strcmp(table_name, "SECURITY") == 0) {
#ifdef DEBUG
		elog(DEBUG1, "%s", SQLDMF1_17);
#endif /* DEBUG */
		args[0] = CStringGetTextDatum(symbol);
		ret = SPI_execute_plan(DMF1_17, args, nulls, false, 0);
		if (ret != SPI_OK_UPDATE) {
			FAIL_FRAME(DMF1_statements[19].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}
	} else if (strcmp(table_name, "TAXRATE") == 0) {
		char tx_name[TX_NAME_LEN + 1];
		char *p;

#ifdef DEBUG
		elog(DEBUG1, "%s", SQLDMF1_18);
#endif /* DEBUG */
		args[0] = CStringGetTextDatum(tx_id);
		ret = SPI_execute_plan(DMF1_18, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			strcpy(tx_name, SPI_getvalue(tuple, tupdesc, 1));
		} else {
			FAIL_FRAME(DMF1_statements[20].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}

#ifdef DEBUG
		elog(DEBUG1, "tx_name = '%s'", tx_name);
#endif /* DEBUG */

		if ((p = strstr(tx_name, " Tax ")) > 0) {
			p[1] = 't';
		} else if ((p = strstr(tx_name, " tax ")) > 0) {
			p[1] = 'T';
		} else {
			FAIL_FRAME(DMF1_statements[20].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}

#ifdef DEBUG
		elog(DEBUG1, "tx_name = '%s'", tx_name);
		elog(DEBUG1, "%s", SQLDMF1_19);
#endif /* DEBUG */
		args[0] = CStringGetTextDatum(tx_name);
		args[1] = CStringGetTextDatum(tx_id);
		ret = SPI_execute_plan(DMF1_19, args, nulls, false, 0);
		if (ret != SPI_OK_UPDATE) {
			FAIL_FRAME(DMF1_statements[21].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}
	} else if (strcmp(table_name, "WATCH_ITEM") == 0) {
		int cnt;
		char *old_symbol = NULL;
		char *new_symbol = NULL;

#ifdef DEBUG
		elog(DEBUG1, "%s", SQLDMF1_20);
#endif /* DEBUG */
		args[0] = Int64GetDatum(c_id);
		ret = SPI_execute_plan(DMF1_20, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			cnt = atoi(SPI_getvalue(tuple, tupdesc, 1));
		} else {
			FAIL_FRAME(DMF1_statements[22].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}

#ifdef DEBUG
		elog(DEBUG1, "cnt = %d", cnt);
#endif /* DEBUG */

		cnt = (cnt + 1) / 2;

#ifdef DEBUG
		elog(DEBUG1, "%s", SQLDMF1_21);
#endif /* DEBUG */
		args[0] = Int64GetDatum(c_id);
		args[1] = Int16GetDatum(cnt);
		ret = SPI_execute_plan(DMF1_21, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			old_symbol = SPI_getvalue(tuple, tupdesc, 1);
		} else {
			FAIL_FRAME(DMF1_statements[23].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}

#ifdef DEBUG
		elog(DEBUG1, "%s", SQLDMF1_22);
		elog(DEBUG1, "old_symbol = '%s'", old_symbol);
#endif /* DEBUG */
		args[0] = CStringGetTextDatum(old_symbol);
		args[1] = Int64GetDatum(c_id);
		ret = SPI_execute_plan(DMF1_22, args, nulls, true, 0);
		if (ret == SPI_OK_SELECT && SPI_processed > 0) {
			tupdesc = SPI_tuptable->tupdesc;
			tuptable = SPI_tuptable;
			tuple = tuptable->vals[0];
			new_symbol = SPI_getvalue(tuple, tupdesc, 1);
		} else {
			FAIL_FRAME(DMF1_statements[24].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}

#ifdef DEBUG
		elog(DEBUG1, "%s", SQLDMF1_23);
#endif /* DEBUG */
		args[0] = CStringGetTextDatum(new_symbol);
		args[1] = Int64GetDatum(c_id);
		args[2] = CStringGetTextDatum(old_symbol);
		ret = SPI_execute_plan(DMF1_23, args, nulls, false, 0);
		if (ret != SPI_OK_UPDATE) {
			FAIL_FRAME(DMF1_statements[25].sql);
#ifdef DEBUG
			dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
					table_name, tx_id, vol_incr);
#endif /* DEBUG */
			SPI_finish();
			PG_RETURN_INT32(1);
		}
	} else {
		elog(ERROR, "unknown table for data maintenance: %s", table_name);
		FAIL_FRAME("DMF1_statements");
#ifdef DEBUG
		dump_dmf1_inputs(acct_id, c_id, co_id, day_of_month, symbol,
				table_name, tx_id, vol_incr);
#endif /* DEBUG */
		SPI_finish();
		PG_RETURN_INT32(1);
	}
	SPI_finish();
	PG_RETURN_INT32(status);
}
