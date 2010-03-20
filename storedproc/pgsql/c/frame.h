/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2007-2010 Mark Wong
 *
 * Based on TPC-E Standard Specification Revision 1.10.0.
 */

#ifndef _FRAME_H_
#define _FRAME_H_

#define FAIL_FRAME(rows, status, sql) \
		elog(WARNING, "UNEXPECTED EXECUTION RESULT: %s %d\n%s", \
				__FILE__, __LINE__, sql); \
		strncpy(status, "1", STATUS_LEN); \
		*rows = 0;

#define FAIL_FRAME2(status, sql) \
		elog(WARNING, "UNEXPECTED EXECUTION RESULT: %s %d\n%s", \
				__FILE__, __LINE__, sql); \
		status = 1;

#define FAIL_FRAME3(rows, status, sql, codestr) \
		elog(WARNING, "UNEXPECTED EXECUTION RESULT: %s %d\n%s", \
				__FILE__, __LINE__, sql); \
		strncpy(status, codestr, STATUS_LEN); \
		*rows = 0;

/* PostgreSQL types */
#define BIGINT_LEN 20
#define BOOLEAN_LEN 1
#define INTEGER_LEN 10
#define SMALLINT_LEN 5

/* Domains */
#define BALANCE_T_LEN 14
#define FIN_AGG_T_LEN 16
#define IDENT_T_LEN BIGINT_LEN
#define S_COUNT_T_LEN BIGINT_LEN
#define S_PRICE_T_LEN 10
#define S_QTY_T_LEN INTEGER_LEN
#define TRADE_T_LEN BIGINT_LEN
#define VALUE_T_LEN 12

/* Table column sizes */

#define AD_LINE2_LEN 80

#define AP_ACL_LEN 4
#define AP_F_NAME_LEN  30
#define AP_L_NAME_LEN  30
#define AP_TAX_ID_LEN  20

#define B_NAME_LEN 100

#define C_AREA_1_LEN 3
#define C_AREA_2_LEN 3
#define C_AREA_3_LEN 3
#define C_CTRY_1_LEN 3
#define C_CTRY_2_LEN 3
#define C_CTRY_3_LEN 3
#define C_EMAIL_1_LEN 50
#define C_EMAIL_2_LEN 50
#define C_EXT_1_LEN 5
#define C_EXT_2_LEN 5
#define C_EXT_3_LEN 5
#define C_F_NAME_LEN 30
#define C_GNDR_LEN 1
#define C_L_NAME_LEN 30
#define C_LOCAL_1_LEN 10
#define C_LOCAL_2_LEN 10
#define C_LOCAL_3_LEN 10
#define C_M_NAME_LEN 1
#define C_TIER_LEN SMALLINT_LEN

#define CO_NAME_LEN 60
#define CO_SP_RATE_LEN 5

#define CR_RATE_LEN 7

#define CT_NAME_LEN 100

#define DM_CLOSE_LEN S_PRICE_T_LEN
#define DM_HIGH_LEN S_PRICE_T_LEN
#define DM_LOW_LEN S_PRICE_T_LEN
#define DM_VOL_LEN S_COUNT_T_LEN

#define EX_NAME_LEN 100

#define FI_CO_ID_LEN IDENT_T_LEN
#define FI_YEAR_LEN INTEGER_LEN
#define FI_QTR_LEN SMALLINT_LEN
#define FI_REVENUE_LEN FIN_AGG_T_LEN
#define FI_NET_EARN_LEN FIN_AGG_T_LEN
#define FI_BASIC_EPS_LEN VALUE_T_LEN
#define FI_DILUT_EPS_LEN VALUE_T_LEN
#define FI_MARGIN_LEN VALUE_T_LEN
#define FI_INVENTORY_LEN FIN_AGG_T_LEN
#define FI_ASSETS_LEN FIN_AGG_T_LEN
#define FI_LIABILITY_LEN FIN_AGG_T_LEN
#define FI_OUT_BASIC_LEN S_COUNT_T_LEN
#define FI_OUT_DILUT_LEN S_COUNT_T_LEN

#define IN_NAME_LEN 50

#define NI_AUTHOR_LEN 30
/* PostgreSQL bytea can blow this up by 5. */
#define NI_ITEM_LEN 100000 * 5
#define NI_SOURCE_LEN 30
#define NI_HEADLINE_LEN 80
#define NI_SUMMARY_LEN 255

#define S_EX_ID_LEN 6
#define S_NAME_LEN 70
#define S_SYMB_LEN 15

#define SE_CASH_TYPE_LEN 40

#define ST_ID_LEN 4
#define ST_NAME_LEN 10

#define T_EXEC_NAME_LEN 64

#define TT_ID_LEN 3
#define TT_NAME_LEN 12

#define TX_ID_LEN 4
#define TX_NAME_LEN 50

#define STATUS_LEN 5

#define USE_ISO_DATES 1

/*
 * FIXME: This escaping function probably isn't comprehensive and what a 
 * terrible thing to do to define this inline function in a header file!
 */
void inline escape_me(char *s, char *t);

void inline escape_me(char *s, char *t) {
	int length = strlen(s);
	int i;
	int k = 0;
	for (i = 0; i < length; i++) {
		if (s[i] == '\'')
			t[k++] = '\\';
		t[k++] = s[i];
	}
	t[k] = '\0';
}

#endif /* _FRAME_H_ */
