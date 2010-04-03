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

/*
 * Database loader class for FINANCIAL table.
 */

#ifndef PGSQL_FINANCIAL_LOAD_H
#define PGSQL_FINANCIAL_LOAD_H

namespace TPCE
{

class CPGSQLFinancialLoad : public CPGSQLLoader<FINANCIAL_ROW>
{
private:
	CDateTime fi_qtr_start_date;

public:
	CPGSQLFinancialLoad(char *szConnectStr, char *szTable = "financial")
			: CPGSQLLoader<FINANCIAL_ROW>(szConnectStr, szTable) { };

	// copy to the bound location inside this class first
	virtual void WriteNextRecord(PT next_record) {
		fi_qtr_start_date = next_record->FI_QTR_START_DATE;

		fprintf(p,
				"%ld%c%d%c%d%c%s%c%.2f%c%.2f%c%.2f%c%.2f%c%.2f%c%.2f%c%.2f%c%.2f%c%.0f%c%.0f\n",
				next_record->FI_CO_ID, delimiter,
				next_record->FI_YEAR, delimiter,
				next_record->FI_QTR, delimiter,
				fi_qtr_start_date.ToStr(iDateTimeFmt), delimiter,
				next_record->FI_REVENUE, delimiter,
				next_record->FI_NET_EARN, delimiter,
				next_record->FI_BASIC_EPS, delimiter,
				next_record->FI_DILUT_EPS, delimiter,
				next_record->FI_MARGIN, delimiter,
				next_record->FI_INVENTORY, delimiter,
				next_record->FI_ASSETS, delimiter,
				next_record->FI_LIABILITY, delimiter,
				next_record->FI_OUT_BASIC, delimiter,
				next_record->FI_OUT_DILUT);
		// FIXME: Have blind faith that this row of data was built correctly.
		while (fgetc(p) != EOF) ;
	}
};

} // namespace TPCE

#endif // PGSQL_FINANCIAL_LOAD_H
