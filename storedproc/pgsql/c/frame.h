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

#endif /* _FRAME_H_ */
