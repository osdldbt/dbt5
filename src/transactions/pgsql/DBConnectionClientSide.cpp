/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 */

#include "DBConnection.h"
#include "DBConnectionClientSide.h"

CDBConnectionClientSide::CDBConnectionClientSide(const char *szHost,
		const char *szDBName, const char *szDBPort, bool bVerbose)
: CDBConnection(szHost, szDBName, szDBPort, bVerbose)
{
}

CDBConnectionClientSide::~CDBConnectionClientSide() {}

void
CDBConnectionClientSide::execute(
		const TBrokerVolumeFrame1Input *pIn, TBrokerVolumeFrame1Output *pOut)
{
	ostringstream osBrokers;
	int i = 0;
	osBrokers << pIn->broker_list[i];
	for (i = 1; pIn->broker_list[i][0] != '\0' && i < max_broker_list_len;
			i++) {
		osBrokers << ", " << pIn->broker_list[i];
	}

	ostringstream osSQL;
	osSQL << "SELECT b_name" << endl
		  << "     , sum(tr_qty * tr_bid_price) AS vol" << endl
		  << "FROM trade_request" << endl
		  << "   , sector" << endl
		  << "   , industry" << endl
		  << "   , company" << endl
		  << "   , broker" << endl
		  << "   , security" << endl
		  << "WHERE tr_b_id = b_id" << endl
		  << "  AND tr_s_symb = s_symb" << endl
		  << "  AND s_co_id = co_id" << endl
		  << "  AND co_in_id = in_id" << endl
		  << "  AND sc_id = in_sc_id" << endl
		  << "  AND b_name = ANY ('{" << osBrokers.str() << "}')" << endl
		  << "  AND sc_name = '" << pIn->sector_name << "'" << endl
		  << "GROUP BY b_name" << endl
		  << "ORDER BY 2 DESC";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	PGresult *res = exec(osSQL.str().c_str());

	pOut->list_len = PQntuples(res);
	if (pOut->list_len == 0) {
		return;
	}

	for (i = 0; i < pOut->list_len; i++) {
		strncpy(pOut->broker_name[i], PQgetvalue(res, i, 0), cB_NAME_len);
		pOut->volume[i] = atof(PQgetvalue(res, i, 1));
	}
	PQclear(res);

	if (m_bVerbose) {
		cout << "list_len = " << pOut->list_len << endl;
		for (i = 0; i < pOut->list_len; i++) {
			cout << "broker_name[" << i << "] = " << pOut->broker_name[i]
				 << endl;
			cout << "volume[" << i << "] = " << pOut->volume[i] << endl;
		}
	}
}

void
CDBConnectionClientSide::execute(const TCustomerPositionFrame1Input *pIn,
		TCustomerPositionFrame1Output *pOut)
{
}

void
CDBConnectionClientSide::execute(const TCustomerPositionFrame2Input *pIn,
		TCustomerPositionFrame2Output *pOut)
{
}

void
CDBConnectionClientSide::execute(const TDataMaintenanceFrame1Input *pIn)
{
}

void
CDBConnectionClientSide::execute(
		const TMarketWatchFrame1Input *pIn, TMarketWatchFrame1Output *pOut)
{
}

void
CDBConnectionClientSide::execute(const TSecurityDetailFrame1Input *pIn,
		TSecurityDetailFrame1Output *pOut)
{
}

void
CDBConnectionClientSide::execute(const TTradeCleanupFrame1Input *pIn)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeLookupFrame1Input *pIn, TTradeLookupFrame1Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeLookupFrame2Input *pIn, TTradeLookupFrame2Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeLookupFrame3Input *pIn, TTradeLookupFrame3Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeLookupFrame4Input *pIn, TTradeLookupFrame4Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeOrderFrame1Input *pIn, TTradeOrderFrame1Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeOrderFrame2Input *pIn, TTradeOrderFrame2Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeOrderFrame3Input *pIn, TTradeOrderFrame3Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeOrderFrame4Input *pIn, TTradeOrderFrame4Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeResultFrame1Input *pIn, TTradeResultFrame1Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeResultFrame2Input *pIn, TTradeResultFrame2Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeResultFrame3Input *pIn, TTradeResultFrame3Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeResultFrame4Input *pIn, TTradeResultFrame4Output *pOut)
{
}

void
CDBConnectionClientSide::execute(const TTradeResultFrame5Input *pIn)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeResultFrame6Input *pIn, TTradeResultFrame6Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeStatusFrame1Input *pIn, TTradeStatusFrame1Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeUpdateFrame1Input *pIn, TTradeUpdateFrame1Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeUpdateFrame2Input *pIn, TTradeUpdateFrame2Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeUpdateFrame3Input *pIn, TTradeUpdateFrame3Output *pOut)
{
}
