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
		elog(WARNING, "UNEXPECTED EXECUTION RESULT:\n%s", sql); \
		strcpy(status, "1"); \
		*rows = 0;

#define FAIL_FRAME2(status, sql) \
		elog(WARNING, "UNEXPECTED EXECUTION RESULT:\n%s", sql); \
		status = 1;

/* PostgreSQL types */
#define BIGINT_LEN 10
#define INTEGER_LEN 5
#define SMALLINT_LEN 3

/* Domains */
#define BALANCE_T_LEN 14
#define S_PRICE_T_LEN 10
#define VALUE_T_LEN 12

/* Table column sizes */

#define CR_RATE_LEN 7

#define S_EX_ID_LEN 6
#define S_NAME_LEN 70
#define S_SYMB_LEN 15

#define T_ST_ID_LEN 4

#define TT_ID_LEN 3
#define TT_NAME_LEN 12

#define STATUS_LEN 1

#define USE_ISO_DATES 1

#endif /* _FRAME_H_ */
