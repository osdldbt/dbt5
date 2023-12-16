/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors
 * - 2006 Rilson Nascimento
 * - 2010 Mark Wong <markwkm@postgresql.org>
 */

//
// Database loader class for TRADE table.
//

#ifndef PGSQL_TRADE_LOAD_H
#define PGSQL_TRADE_LOAD_H

namespace TPCE
{

class CPGSQLTradeLoad: public CPGSQLLoader<TRADE_ROW>
{
private:
	CDateTime t_dts;

public:
	CPGSQLTradeLoad(const char *szConnectStr, const char *szTable = "trade")
	: CPGSQLLoader<TRADE_ROW>(szConnectStr, szTable){};

	void
	WriteNextRecord(const TRADE_ROW &next_record)
	{
		t_dts = next_record.T_DTS;

		fprintf(p,
				"%" PRId64 "|%s|%s|%s|%d|%s|%d|%.2f|%" PRId64
				"|%s|%.2f|%.2f|%.2f|%.2f|%d\n",
				next_record.T_ID, t_dts.ToStr(iDateTimeFmt),
				next_record.T_ST_ID, next_record.T_TT_ID,
				next_record.T_IS_CASH, next_record.T_S_SYMB, next_record.T_QTY,
				next_record.T_BID_PRICE, next_record.T_CA_ID,
				next_record.T_EXEC_NAME, next_record.T_TRADE_PRICE,
				next_record.T_CHRG, next_record.T_COMM, next_record.T_TAX,
				next_record.T_LIFO);
		// FIXME: Have blind faith that this row of data was built correctly.
		while (fgetc(p) != EOF)
			;
	}

	void
	FinishLoad()
	{
		// FIXME: Can't we call the parent class's FinishLoad to do the COMMIT?
		// End of the COPY.
		fprintf(p, "\\.\n");
		// FIXME: Have blind faith that COPY was successful.
		while (fgetc(p) != EOF)
			;

		// COMMIT the COPY.
		fprintf(p, "COMMIT;\n");
		// FIXME: Have blind faith that COMMIT was successful.
		while (fgetc(p) != EOF)
			;

		fprintf(p, "SELECT SETVAL('seq_trade_id', (SELECT MAX(t_id) FROM "
				   "trade));\n");
		// FIXME: Have blind faith that this row of data was built correctly.
		while (fgetc(p) != EOF)
			;
	}
};

} // namespace TPCE

#endif // PGSQL_TRADE_LOAD_H
