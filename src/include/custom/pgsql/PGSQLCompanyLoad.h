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
 * Database loader class for COMPANY table.
 */

#ifndef PGSQL_COMPANY_LOAD_H
#define PGSQL_COMPANY_LOAD_H

namespace TPCE
{

class CPGSQLCompanyLoad: public CPGSQLLoader<COMPANY_ROW>
{
private:
	CDateTime co_open_date;

public:
	CPGSQLCompanyLoad(
			const char *szConnectStr, const char *szTable = "company")
	: CPGSQLLoader<COMPANY_ROW>(szConnectStr, szTable){};

	void
	WriteNextRecord(const COMPANY_ROW &next_record)
	{
		co_open_date = next_record.CO_OPEN_DATE;

		fprintf(p, "%" PRId64 "|%s|%s|%s|%s|%s|%" PRId64 "|%s|%s\n",
				next_record.CO_ID, next_record.CO_ST_ID, next_record.CO_NAME,
				next_record.CO_IN_ID, next_record.CO_SP_RATE,
				next_record.CO_CEO, next_record.CO_AD_ID, next_record.CO_DESC,
				co_open_date.ToStr(iDateTimeFmt));
		// FIXME: Have blind faith that this row of data was built correctly.
		while (fgetc(p) != EOF)
			;
	}
};

} // namespace TPCE

#endif // PGSQL_COMPANY_LOAD_H
