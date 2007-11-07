/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2007 Mark Wong
 *
 * Based on TPC-E Standard Specification Revision 1.3.
 */

#ifndef _FRAME_H_
#define _FRAME_H_

#define FAIL_FRAME(rows, status, sql) \
		elog(WARNING, "UNEXPECTED EXECUTION RESULT: %s %d\n%s", \
				__FILE__, __LINE__, sql); \
		strcpy(status, "1"); \
		*rows = 0;

#define FAIL_FRAME2(status, sql) \
		elog(WARNING, "UNEXPECTED EXECUTION RESULT: %s %d\n%s", \
				__FILE__, __LINE__, sql); \
		status = 1;

/* PostgreSQL types */
#define BIGINT_LEN 10
#define INTEGER_LEN 5
#define SMALLINT_LEN 3

/* Domains */
#define BALANCE_T_LEN 14
#define IDENT_T_LEN BIGINT_LEN
#define S_PRICE_T_LEN 10
#define VALUE_T_LEN 12

/* Table column sizes */

#define AD_LINE2_LEN 80

#define AP_ACL_LEN 4

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

#define CO_SP_RATE_LEN 5

#define CR_RATE_LEN 7

#define CT_NAME_LEN 100

#define EX_NAME_LEN 100

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

#define STATUS_LEN 1

#define USE_ISO_DATES 1

#endif /* _FRAME_H_ */
