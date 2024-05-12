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
	ostringstream osSQL;
	PGresult *res = NULL;

	pOut->cust_id = pIn->cust_id;

	if (pOut->cust_id == 0) {
		osSQL << "SELECT c_id" << endl
			  << "FROM   customer" << endl
			  << "WHERE  c_tax_id = '" << pIn->tax_id << "'";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		if (PQntuples(res) == 0) {
			return;
		}

		pOut->cust_id = atoll(PQgetvalue(res, 0, 0));
		PQclear(res);

		if (m_bVerbose) {
			cout << "c_id = " << pOut->cust_id << endl;
		}
	}

	osSQL.clear();
	osSQL.str("");
	osSQL << "SELECT c_st_id" << endl
		  << "     , c_l_name" << endl
		  << "     , c_f_name" << endl
		  << "     , c_m_name" << endl
		  << "     , c_gndr" << endl
		  << "     , c_tier" << endl
		  << "     , c_dob" << endl
		  << "     , c_ad_id" << endl
		  << "     , c_ctry_1" << endl
		  << "     , c_area_1" << endl
		  << "     , c_local_1" << endl
		  << "     , c_ext_1" << endl
		  << "     , c_ctry_2" << endl
		  << "     , c_area_2" << endl
		  << "     , c_local_2" << endl
		  << "     , c_ext_2" << endl
		  << "     , c_ctry_3" << endl
		  << "     , c_area_3" << endl
		  << "     , c_local_3" << endl
		  << "     , c_ext_3" << endl
		  << "     , c_email_1" << endl
		  << "     , c_email_2" << endl
		  << "FROM customer" << endl
		  << "WHERE c_id = " << pOut->cust_id;
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	if (PQntuples(res) == 0) {
		return;
	}

	strncpy(pOut->c_st_id, PQgetvalue(res, 0, 0), cST_ID_len);
	strncpy(pOut->c_l_name, PQgetvalue(res, 0, 1), cL_NAME_len);
	strncpy(pOut->c_f_name, PQgetvalue(res, 0, 2), cF_NAME_len);
	strncpy(pOut->c_m_name, PQgetvalue(res, 0, 3), cM_NAME_len);
	strncpy(pOut->c_gndr, PQgetvalue(res, 0, 4), cGNDR_len);
	pOut->c_tier = PQgetvalue(res, 0, 5)[0];
	sscanf(PQgetvalue(res, 0, 6), "%hd-%hd-%hd", &pOut->c_dob.year,
			&pOut->c_dob.month, &pOut->c_dob.day);
	pOut->c_ad_id = atoll(PQgetvalue(res, 0, 7));
	strncpy(pOut->c_ctry_1, PQgetvalue(res, 0, 8), cCTRY_len);
	strncpy(pOut->c_area_1, PQgetvalue(res, 0, 9), cAREA_len);
	strncpy(pOut->c_local_1, PQgetvalue(res, 0, 10), cLOCAL_len);
	strncpy(pOut->c_ext_1, PQgetvalue(res, 0, 11), cEXT_len);
	strncpy(pOut->c_ctry_2, PQgetvalue(res, 0, 12), cCTRY_len);
	strncpy(pOut->c_area_2, PQgetvalue(res, 0, 13), cAREA_len);
	strncpy(pOut->c_local_2, PQgetvalue(res, 0, 14), cLOCAL_len);
	strncpy(pOut->c_ext_2, PQgetvalue(res, 0, 15), cEXT_len);
	strncpy(pOut->c_ctry_3, PQgetvalue(res, 0, 16), cCTRY_len);
	strncpy(pOut->c_area_3, PQgetvalue(res, 0, 17), cAREA_len);
	strncpy(pOut->c_local_3, PQgetvalue(res, 0, 18), cLOCAL_len);
	strncpy(pOut->c_ext_3, PQgetvalue(res, 0, 19), cEXT_len);
	strncpy(pOut->c_email_1, PQgetvalue(res, 0, 20), cEMAIL_len);
	strncpy(pOut->c_email_2, PQgetvalue(res, 0, 21), cEMAIL_len);
	PQclear(res);

	if (m_bVerbose) {
		cout << "c_st_id = " << pOut->c_st_id << endl;
		cout << "c_l_name = " << pOut->c_l_name << endl;
		cout << "c_f_name = " << pOut->c_f_name << endl;
		cout << "c_m_name = " << pOut->c_m_name << endl;
		cout << "c_gndr = " << pOut->c_gndr << endl;
		cout << "c_tier = " << pOut->c_tier << endl;
		cout << "c_dob = " << pOut->c_dob.year << "-" << pOut->c_dob.month
			 << "-" << pOut->c_dob.day << endl;
		cout << "c_ad_id = " << pOut->c_ad_id << endl;
		cout << "c_ctry_1 = " << pOut->c_ctry_1 << endl;
		cout << "c_area_1 = " << pOut->c_area_1 << endl;
		cout << "c_local_1 = " << pOut->c_local_1 << endl;
		cout << "c_ext_1 = " << pOut->c_ext_1 << endl;
		cout << "c_ctry_2 = " << pOut->c_ctry_2 << endl;
		cout << "c_area_2 = " << pOut->c_area_2 << endl;
		cout << "c_local_2 = " << pOut->c_local_2 << endl;
		cout << "c_ext_2 = " << pOut->c_ext_2 << endl;
		cout << "c_ctry_3 = " << pOut->c_ctry_3 << endl;
		cout << "c_area_3 = " << pOut->c_area_3 << endl;
		cout << "c_local_3 = " << pOut->c_local_3 << endl;
		cout << "c_ext_3 = " << pOut->c_ext_3 << endl;
		cout << "c_email_1 = " << pOut->c_email_1 << endl;
		cout << "c_email_2 = " << pOut->c_email_2 << endl;
	}

	osSQL.clear();
	osSQL.str("");
	osSQL << "SELECT ca_id" << endl
		  << "    ,  ca_bal" << endl
		  << "    ,  coalesce(sum(hs_qty * lt_price), 0) AS soma" << endl
		  << "FROM customer_account" << endl
		  << "     LEFT OUTER JOIN holding_summary" << endl
		  << "                  ON hs_ca_id = ca_id," << endl
		  << "     last_trade" << endl
		  << "WHERE ca_c_id = " << pOut->cust_id << endl
		  << " AND lt_s_symb = hs_s_symb" << endl
		  << "GROUP BY ca_id, ca_bal" << endl
		  << "ORDER BY 3 ASC" << endl
		  << "LIMIT 10";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	pOut->acct_len = PQntuples(res);
	for (int i = 0; i < pOut->acct_len; i++) {
		pOut->acct_id[i] = atoll(PQgetvalue(res, i, 0));
		pOut->cash_bal[i] = atof(PQgetvalue(res, i, 1));
		pOut->asset_total[i] = atof(PQgetvalue(res, i, 2));
	}
	PQclear(res);

	if (m_bVerbose) {
		cout << "acct_len = " << pOut->acct_len << endl;
		for (int i = 0; i < pOut->acct_len; i++) {
			cout << "acct_id[" << i << "] = " << pOut->acct_id[i] << endl;
			cout << "cash_bal[" << i << "] = " << pOut->cash_bal[i] << endl;
			cout << "asset_total[" << i << "] = " << pOut->asset_total[i]
				 << endl;
		}
	}
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
