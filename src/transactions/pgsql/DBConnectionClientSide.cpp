/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 */

#include <endian.h>

#include <catalog/pg_type_d.h>

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
	osBrokers << "{" << pIn->broker_list[i];
	for (i = 1; pIn->broker_list[i][0] != '\0' && i < max_broker_list_len;
			i++) {
		osBrokers << ", " << pIn->broker_list[i];
	}
	osBrokers << "}";

	char brokers[osBrokers.str().length() + 1];
	strncpy(brokers, osBrokers.str().c_str(), osBrokers.str().length());
	brokers[osBrokers.str().length()] = '\0';

#define BVF1Q1                                                                \
	"SELECT b_name\n"                                                         \
	"     , sum(tr_qty * tr_bid_price) AS vol\n"                              \
	"FROM trade_request\n"                                                    \
	"   , sector\n"                                                           \
	"   , industry\n"                                                         \
	"   , company\n"                                                          \
	"   , broker\n"                                                           \
	"   , security\n"                                                         \
	"WHERE tr_b_id = b_id\n"                                                  \
	"  AND tr_s_symb = s_symb\n"                                              \
	"  AND s_co_id = co_id\n"                                                 \
	"  AND co_in_id = in_id\n"                                                \
	"  AND sc_id = in_sc_id\n"                                                \
	"  AND b_name = ANY ($1)\n"                                               \
	"  AND sc_name = $2\n"                                                    \
	"GROUP BY b_name\n"                                                       \
	"ORDER BY 2 DESC"

	const char *paramValues[2]
			= { (char *) brokers, (char *) pIn->sector_name };
	const int paramLengths[2]
			= { (int) sizeof(char) * ((int) osBrokers.str().length() + 1),
				  sizeof(char) * (cSC_NAME_len + 1) };
	const int paramFormats[2] = { 0, 0 };

	PGresult *res = exec(
			BVF1Q1, 2, NULL, paramValues, paramLengths, paramFormats, 0);

	pOut->list_len = PQntuples(res);
	for (i = 0; i < pOut->list_len; i++) {
		strncpy(pOut->broker_name[i], PQgetvalue(res, i, 0), cB_NAME_len);
		pOut->volume[i] = atof(PQgetvalue(res, i, 1));
	}
	PQclear(res);

	if (m_bVerbose) {
		cout << BVF1Q1 << endl;
		cout << "$1 = " << paramValues[0] << endl;
		cout << "$2 = " << paramValues[1] << endl;
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
	PGresult *res = NULL;

	const char *paramValues[1];

	pOut->cust_id = pIn->cust_id;

	if (pOut->cust_id == 0) {
#define CPF1Q1                                                                \
	"SELECT c_id\n"                                                           \
	"FROM customer\n"                                                         \
	"WHERE c_tax_id = $1"

		paramValues[0] = (char *) pIn->tax_id;
		const int paramLengths[1] = { sizeof(char) * (cTAX_ID_len + 1) };
		const int paramFormats[1] = { 0 };

		if (m_bVerbose) {
			cout << CPF1Q1 << endl;
			cout << "$1 = " << paramValues[0] << endl;
		}

		res = exec(
				CPF1Q1, 1, NULL, paramValues, paramLengths, paramFormats, 0);

		if (PQntuples(res) == 0) {
			return;
		}

		pOut->cust_id = atoll(PQgetvalue(res, 0, 0));
		PQclear(res);

		if (m_bVerbose) {
			cout << "c_id = " << pOut->cust_id << endl;
		}
	}

	uint64_t cust_id = htobe64((uint64_t) pOut->cust_id);

	paramValues[0] = (char *) &cust_id;
	const int paramLengths[1] = { sizeof(uint64_t) };
	const int paramFormats[1] = { 1 };

#define CPF1Q2                                                                \
	"SELECT c_st_id\n"                                                        \
	"     , c_l_name\n"                                                       \
	"     , c_f_name\n"                                                       \
	"     , c_m_name\n"                                                       \
	"     , c_gndr\n"                                                         \
	"     , c_tier\n"                                                         \
	"     , c_dob\n"                                                          \
	"     , c_ad_id\n"                                                        \
	"     , c_ctry_1\n"                                                       \
	"     , c_area_1\n"                                                       \
	"     , c_local_1\n"                                                      \
	"     , c_ext_1\n"                                                        \
	"     , c_ctry_2\n"                                                       \
	"     , c_area_2\n"                                                       \
	"     , c_local_2\n"                                                      \
	"     , c_ext_2\n"                                                        \
	"     , c_ctry_3\n"                                                       \
	"     , c_area_3\n"                                                       \
	"     , c_local_3\n"                                                      \
	"     , c_ext_3\n"                                                        \
	"     , c_email_1\n"                                                      \
	"     , c_email_2\n"                                                      \
	"FROM customer\n"                                                         \
	"WHERE c_id = $1"

	if (m_bVerbose) {
		cout << CPF1Q2 << endl;
		cout << "$1 = " << be64toh(cust_id) << endl;
	}

	res = exec(CPF1Q2, 1, NULL, paramValues, paramLengths, paramFormats, 0);

	if (PQntuples(res) == 0) {
		PQclear(res);
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

#define CPF1Q3                                                                \
	"SELECT ca_id\n"                                                          \
	"    ,  ca_bal\n"                                                         \
	"    ,  coalesce(sum(hs_qty * lt_price), 0) AS soma\n"                    \
	"FROM customer_account\n"                                                 \
	"     LEFT OUTER JOIN holding_summary\n"                                  \
	"                  ON hs_ca_id = ca_id,\n"                                \
	"     last_trade\n"                                                       \
	"WHERE ca_c_id = $1\n"                                                    \
	" AND lt_s_symb = hs_s_symb\n"                                            \
	"GROUP BY ca_id, ca_bal\n"                                                \
	"ORDER BY 3 ASC\n"                                                        \
	"LIMIT 10"

	if (m_bVerbose) {
		cout << CPF1Q3 << endl;
		cout << "$1 = " << be64toh(cust_id) << endl;
	}

	res = exec(CPF1Q3, 1, NULL, paramValues, paramLengths, paramFormats, 0);

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
	uint64_t acct_id = htobe64((uint64_t) pIn->acct_id);

	const char *paramValues[1] = { (char *) &acct_id };
	const int paramLengths[1] = { sizeof(uint64_t) };
	const int paramFormats[1] = { 1 };

#define CPF2Q1                                                                \
	"SELECT t_id\n"                                                           \
	"     , t_s_symb\n"                                                       \
	"     , t_qty\n"                                                          \
	"     , st_name\n"                                                        \
	"     , th_dts\n"                                                         \
	"FROM (\n"                                                                \
	"         SELECT t_id AS id\n"                                            \
	"         FROM trade\n"                                                   \
	"         WHERE t_ca_id = $1\n"                                           \
	"         ORDER BY t_dts DESC\n"                                          \
	"         LIMIT 10\n"                                                     \
	"     ) AS t\n"                                                           \
	"   , trade\n"                                                            \
	"   , trade_history\n"                                                    \
	"   , status_type\n"                                                      \
	"WHERE t_id = id\n"                                                       \
	"  AND th_t_id = t_id\n"                                                  \
	"  AND st_id = th_st_id\n"                                                \
	"ORDER BY th_dts DESC"

	if (m_bVerbose) {
		cout << CPF2Q1 << endl;
	}

	PGresult *res = exec(
			CPF2Q1, 1, NULL, paramValues, paramLengths, paramFormats, 0);

	pOut->hist_len = PQntuples(res);
	for (int i = 0; i < pOut->hist_len; i++) {
		pOut->trade_id[i] = atoll(PQgetvalue(res, i, 0));
		strncpy(pOut->symbol[i], PQgetvalue(res, i, 1), cSYMBOL_len);
		pOut->qty[i] = atoi(PQgetvalue(res, i, 2));
		strncpy(pOut->trade_status[i], PQgetvalue(res, i, 3), cST_NAME_len);
		sscanf(PQgetvalue(res, i, 4), "%hd-%hd-%hd %hd:%hd:%hd.%d",
				&pOut->hist_dts[i].year, &pOut->hist_dts[i].month,
				&pOut->hist_dts[i].day, &pOut->hist_dts[i].hour,
				&pOut->hist_dts[i].minute, &pOut->hist_dts[i].second,
				&pOut->hist_dts[i].fraction);
	}
	PQclear(res);

	if (m_bVerbose) {
		cout << " = " << pOut->hist_len << endl;
		for (int i = 0; i < pOut->hist_len; i++) {
			cout << "trade_id[" << i << "] = " << pOut->trade_id[i] << endl;
			cout << "symbol[" << i << "] = " << pOut->symbol[i] << endl;
			cout << "qty[" << i << "] = " << pOut->qty[i] << endl;
			cout << "trade_status[" << i << "] = " << pOut->trade_status[i]
				 << endl;
			cout << "hist_dts[" << i << "] = " << pOut->hist_dts[i].year << "-"
				 << pOut->hist_dts[i].month << "-" << pOut->hist_dts[i].day
				 << " " << pOut->hist_dts[i].hour << ":"
				 << pOut->hist_dts[i].minute << ":" << pOut->hist_dts[i].second
				 << "." << pOut->hist_dts[i].fraction << endl;
		}
	}
}

void
CDBConnectionClientSide::execute(const TDataMaintenanceFrame1Input *pIn)
{
	ostringstream osSQL;
	PGresult *res = NULL;

	if (strncmp(pIn->table_name, "ACCOUNT_PERMISSION", max_table_name) == 0) {
		osSQL << "SELECT ap_acl" << endl
			  << "FROM account_permission" << endl
			  << "WHERE ap_ca_id = " << pIn->acct_id << endl
			  << "ORDER BY ap_acl DESC" << endl
			  << "LIMIT 1";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		char *ap_acl = PQgetvalue(res, 0, 0);

		if (m_bVerbose) {
			cout << "ap_acl = " << ap_acl << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "UPDATE account_permission" << endl << "SET ap_acl = '";
		if (strncmp(ap_acl, "1111", 4) != 0) {
			osSQL << "1111";
		} else {
			osSQL << "0011";
		}
		osSQL << "'" << endl
			  << "WHERE ap_ca_id = " << pIn->acct_id << endl
			  << "  AND ap_acl = '" << ap_acl << "'";

		PQclear(res);
	} else if (strncmp(pIn->table_name, "ADDRESS", max_table_name) == 0) {
		if (pIn->c_id != 0) {
			osSQL << "SELECT ad_line2" << endl
				  << "     , ad_id" << endl
				  << "FROM address" << endl
				  << "   , customer" << endl
				  << "WHERE ad_id = c_ad_id" << endl
				  << "  AND c_id = " << pIn->c_id;
		} else {
			osSQL << "SELECT ad_line2" << endl
				  << "     , ad_id" << endl
				  << "FROM address" << endl
				  << "   , company" << endl
				  << "WHERE ad_id = co_ad_id" << endl
				  << "  AND co_id = " << pIn->co_id;
		}
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		if (m_bVerbose) {
			cout << "ad_line2 = " << PQgetvalue(res, 0, 0) << endl;
			cout << "ad_id = " << PQgetvalue(res, 0, 1) << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "UPDATE address" << endl << "SET ad_line2 = '";
		if (strncmp(PQgetvalue(res, 0, 0), "Apt. 10C", 8) != 0) {
			osSQL << "Apt. 10C" << endl;
		} else {
			osSQL << "Apt. 22" << endl;
		}
		osSQL << "'" << endl << "WHERE ad_id = " << PQgetvalue(res, 0, 1);

		PQclear(res);
	} else if (strncmp(pIn->table_name, "COMPANY", max_table_name) == 0) {
		osSQL << "SELECT co_sp_rate" << endl
			  << "FROM company" << endl
			  << "WHERE co_id = " << pIn->co_id;
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		if (m_bVerbose) {
			cout << "co_sp_rate = " << PQgetvalue(res, 0, 0) << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "UPDATE company" << endl << "SET co_sp_rate = '";
		if (strncmp(PQgetvalue(res, 0, 0), "ABA", 3) != 0) {
			osSQL << "ABA";
		} else {
			osSQL << "AAA";
		}
		osSQL << "'" << endl << "WHERE co_id = " << pIn->co_id;

		PQclear(res);
	} else if (strncmp(pIn->table_name, "CUSTOMER", max_table_name) == 0) {
		int lenMindspring = strlen("@mindspring.com");

		osSQL << "SELECT c_email_2" << endl
			  << "FROM customer" << endl
			  << "WHERE c_id = " << pIn->c_id;
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		char *c_email_2 = PQgetvalue(res, 0, 0);
		int len = strlen(c_email_2);

		if (m_bVerbose) {
			cout << "c_email_2 = " << c_email_2 << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "UPDATE customer" << endl
			  << "SET c_email_2 = substring(c_email_2" << endl
			  << "                          FROM '#\"%@#\"%'" << endl
			  << "                          FOR '#') || '";
		if (((len - lenMindspring) > 0)
				&& (strstr(c_email_2, "@mindspring.com") != NULL)) {
			osSQL << "earthlink.com";
		} else {
			osSQL << "mindspring.com";
		}
		osSQL << "'" << endl << "WHERE c_id = " << pIn->c_id;

		PQclear(res);
	} else if (strncmp(pIn->table_name, "CUSTOMER_TAXRATE", max_table_name)
			   == 0) {
		osSQL << "SELECT cx_tx_id" << endl
			  << "FROM customer_taxrate" << endl
			  << "WHERE cx_c_id = " << pIn->c_id << endl
			  << "  AND (cx_tx_id LIKE 'US%' OR cx_tx_id LIKE 'CN%')";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		char *cx_tx_id = PQgetvalue(res, 0, 0);

		if (m_bVerbose) {
			cout << "cx_tx_id = " << cx_tx_id << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "UPDATE customer_taxrate" << endl << "SET cx_tx_id = '";
		if (strncmp(cx_tx_id, "US", 2) == 0) {
			if (cx_tx_id[2] == '5') {
				osSQL << "US1";
			} else if (cx_tx_id[2] == '4') {
				osSQL << "US5";
			} else if (cx_tx_id[2] == '3') {
				osSQL << "US4";
			} else if (cx_tx_id[2] == '2') {
				osSQL << "US3";
			} else if (cx_tx_id[2] == '1') {
				osSQL << "US2";
			}
		} else {
			if (cx_tx_id[2] == '4') {
				osSQL << "CN1";
			} else if (cx_tx_id[2] == '3') {
				osSQL << "CN4";
			} else if (cx_tx_id[2] == '2') {
				osSQL << "CN3";
			} else if (cx_tx_id[2] == '1') {
				osSQL << "CN2";
			}
		}
		osSQL << "'" << endl
			  << "WHERE cx_c_id = " << pIn->c_id << endl
			  << " AND cx_tx_id = '" << cx_tx_id << "'";

		PQclear(res);
	} else if (strncmp(pIn->table_name, "DAILY_MARKET", max_table_name) == 0) {
		osSQL << "UPDATE daily_market" << endl
			  << "SET dm_vol = dm_vol + " << pIn->vol_incr << endl
			  << "WHERE dm_s_symb = '" << pIn->symbol << "'" << endl
			  << "  AND extract(DAY FROM dm_date) = " << pIn->day_of_month;
	} else if (strncmp(pIn->table_name, "EXCHANGE", max_table_name) == 0) {
		osSQL << "SELECT count(*)" << endl
			  << "FROM exchange" << endl
			  << "WHERE ex_desc LIKE '%LAST UPDATED%'";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		if (m_bVerbose) {
			cout << "count(*) = " << PQgetvalue(res, 0, 0) << endl;
		}

		osSQL.clear();
		osSQL.str("");
		if (PQgetvalue(res, 0, 0)[0] == '0') {
			osSQL << "UPDATE exchange" << endl
				  << "SET ex_desc = ex_desc || ' LAST UPDATED ' || "
					 "CURRENT_TIMESTAMP";
		} else {
			osSQL << "UPDATE exchange" << endl
				  << "SET ex_desc = substring(ex_desc || ' LAST UPDATED ' || "
					 "now()"
				  << endl
				  << "                        FROM 1 FOR "
					 "(char_length(ex_desc) -"
				  << endl
				  << "                                    "
					 "char_length(now()::TEXT))) || CURRENT_TIMESTAMP";
		}

		PQclear(res);
	} else if (strncmp(pIn->table_name, "FINANCIAL", max_table_name) == 0) {
		osSQL << "SELECT count(*)" << endl
			  << "FROM financial" << endl
			  << "WHERE fi_co_id = " << pIn->co_id << endl
			  << "  AND extract(DAY FROM fi_qtr_start_date) = 1";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		osSQL.clear();
		osSQL.str("");
		if (atoi(PQgetvalue(res, 0, 0)) > 0) {
			osSQL << "UPDATE financial" << endl
				  << "SET fi_qtr_start_date = fi_qtr_start_date + INTERVAL '1 "
					 "DAY'"
				  << endl
				  << "WHERE fi_co_id = " << pIn->co_id;
		} else {
			osSQL << "UPDATE financial" << endl
				  << "SET fi_qtr_start_date = fi_qtr_start_date - INTERVAL '1 "
					 "DAY'"
				  << endl
				  << "WHERE fi_co_id = " << pIn->co_id;
		}

		PQclear(res);
	} else if (strncmp(pIn->table_name, "NEWS_ITEM", max_table_name) == 0) {
		osSQL << "UPDATE news_item" << endl
			  << "SET ni_dts = ni_dts + INTERVAL '1 day'" << endl
			  << "WHERE ni_id IN (" << endl
			  << "                   SELECT nx_ni_id" << endl
			  << "                   FROM news_xref" << endl
			  << "                   WHERE nx_co_id = " << pIn->co_id << endl
			  << "               )";
	} else if (strncmp(pIn->table_name, "SECURITY", max_table_name) == 0) {
		osSQL << "UPDATE security" << endl
			  << "SET s_exch_date = s_exch_date + INTERVAL '1 DAY'" << endl
			  << "WHERE s_symb = '" << pIn->symbol << "'";
	} else if (strncmp(pIn->table_name, "TAXRATE", max_table_name) == 0) {
		osSQL << "SELECT tx_name" << endl
			  << "FROM taxrate" << endl
			  << "WHERE tx_id = '" << pIn->tx_id << "'";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		char *tx_name = PQgetvalue(res, 0, 0);
		char *p;

		if (m_bVerbose) {
			cout << "tx_name = " << tx_name << endl;
		}

		if ((p = strstr(tx_name, " Tax ")) != NULL) {
			p[1] = 't';
		} else if ((p = strstr(tx_name, " tax ")) != NULL) {
			p[1] = 'T';
		} else {
			cerr << "could not find 'tax' or 'Tax' in taxrate data maintenance"
				 << endl;
			return;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "UPDATE taxrate" << endl
			  << "SET tx_name = '" << tx_name << "'" << endl
			  << "WHERE tx_id = '" << pIn->tx_id << "'";

		PQclear(res);
	} else if (strncmp(pIn->table_name, "WATCH_ITEM", max_table_name) == 0) {
		osSQL << "SELECT count(*)" << endl
			  << "FROM watch_item" << endl
			  << "   , watch_list" << endl
			  << "WHERE wl_c_id = " << pIn->c_id << endl
			  << "  AND wi_wl_id = wl_id";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		int cnt = (atoi(PQgetvalue(res, 0, 0)) + 1) / 2;
		PQclear(res);

		if (m_bVerbose) {
			cout << "count(*) = " << PQgetvalue(res, 0, 0) << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT wi_s_symb" << endl
			  << "FROM (" << endl
			  << "         SELECT wi_s_symb" << endl
			  << "         FROM watch_item, watch_list" << endl
			  << "         WHERE wl_c_id = " << pIn->c_id << endl
			  << "           AND wi_wl_id = wl_id" << endl
			  << "         ORDER BY wi_s_symb ASC" << endl
			  << "     ) AS foo" << endl
			  << "OFFSET " << cnt << endl
			  << "LIMIT 1";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		char old_symbol[cSYMBOL_len + 1];
		strncpy(old_symbol, PQgetvalue(res, 0, 0), cSYMBOL_len);
		PQclear(res);

		if (m_bVerbose) {
			cout << "wi_s_symb = " << old_symbol << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT s_symb" << endl
			  << "FROM security" << endl
			  << "WHERE s_symb > '" << old_symbol << "'" << endl
			  << "  AND s_symb NOT IN (" << endl
			  << "                        SELECT wi_s_symb" << endl
			  << "                        FROM watch_item, watch_list" << endl
			  << "                        WHERE wl_c_id = " << pIn->c_id
			  << endl
			  << "                          AND wi_wl_id = wl_id" << endl
			  << "                    )" << endl
			  << "ORDER BY s_symb ASC" << endl
			  << "LIMIT 1";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		if (m_bVerbose) {
			cout << "s_symb = " << PQgetvalue(res, 0, 0) << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "UPDATE watch_item" << endl
			  << "SET wi_s_symb = '" << old_symbol << "'" << endl
			  << "FROM watch_list" << endl
			  << "WHERE wl_c_id = " << pIn->c_id << endl
			  << "  AND wi_wl_id = wl_id" << endl
			  << "  AND wi_s_symb = '" << PQgetvalue(res, 0, 0) << "'";

		PQclear(res);
	}

	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());
	PQclear(res);
}

void
CDBConnectionClientSide::execute(
		const TMarketWatchFrame1Input *pIn, TMarketWatchFrame1Output *pOut)
{
	ostringstream osSQL;
	PGresult *res = NULL;

	double old_mkt_cap = 0.0;
	double new_mkt_cap = 0.0;

	if (pIn->c_id != 0) {
		osSQL << "SELECT wi_s_symb" << endl
			  << "FROM watch_item" << endl
			  << "   , watch_list" << endl
			  << "WHERE wi_wl_id = wl_id" << endl
			  << "  AND wl_c_id = " << pIn->c_id;
	} else if (pIn->industry_name[0] != '\0') {
		osSQL << "SELECT s_symb" << endl
			  << "FROM industry" << endl
			  << "   , company" << endl
			  << "   , security" << endl
			  << "WHERE in_name = '" << pIn->industry_name << "'" << endl
			  << "  AND co_in_id = in_id" << endl
			  << "  AND co_id BETWEEN " << pIn->starting_co_id << " AND "
			  << pIn->ending_co_id << endl
			  << "  AND s_co_id = co_id";
	} else if (pIn->acct_id != 0) {
		osSQL << "SELECT hs_s_symb" << endl
			  << "FROM holding_summary" << endl
			  << "WHERE  hs_ca_id = " << pIn->acct_id;
	} else {
		cerr << "MarketWatchFrame1 error figuring out what to do" << endl;
		return;
	}
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	int count = PQntuples(res);
	for (int i = 0; i < count; i++) {
		PGresult *res2 = NULL;

		char *s_symb = PQgetvalue(res, i, 0);

		if (m_bVerbose) {
			cout << "s_symb[" << i << "] = " << s_symb << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT lt_price" << endl
			  << "FROM last_trade" << endl
			  << "WHERE lt_s_symb = '" << s_symb << "'";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res2 = exec(osSQL.str().c_str());

		if (PQntuples(res2) == 0) {
			return;
		}

		double new_price = atof(PQgetvalue(res2, 0, 0));
		PQclear(res2);

		if (m_bVerbose) {
			cout << "new_price[" << i << "] = " << new_price << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT s_num_out" << endl
			  << "FROM security" << endl
			  << "WHERE s_symb = '" << s_symb << "'";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res2 = exec(osSQL.str().c_str());

		if (PQntuples(res2) == 0) {
			return;
		}

		double s_num_out = atof(PQgetvalue(res2, 0, 0));
		PQclear(res2);

		if (m_bVerbose) {
			cout << "s_num_out[" << i << "] = " << s_num_out << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT dm_close" << endl
			  << "FROM daily_market" << endl
			  << "WHERE dm_s_symb = '" << s_symb << "'" << endl
			  << "  AND dm_date = '" << pIn->start_day.year << "-"
			  << pIn->start_day.month << "-" << pIn->start_day.day << "'";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res2 = exec(osSQL.str().c_str());

		if (PQntuples(res2) == 0) {
			return;
		}

		double old_price = atof(PQgetvalue(res2, 0, 0));
		PQclear(res2);

		if (m_bVerbose) {
			cout << "old_price[" << i << "] = " << old_price << endl;
		}

		old_mkt_cap += s_num_out * old_price;
		new_mkt_cap += s_num_out * new_price;
	}
	PQclear(res);

	pOut->pct_change = 100.0 * (new_mkt_cap / old_mkt_cap - 1.0);

	if (m_bVerbose) {
		cout << "pct_change = " << pOut->pct_change << endl;
	}
}

void
CDBConnectionClientSide::execute(const TSecurityDetailFrame1Input *pIn,
		TSecurityDetailFrame1Output *pOut)
{
	ostringstream osSQL;
	PGresult *res = NULL;

	osSQL << "SELECT s_name" << endl
		  << "     , co_id" << endl
		  << "     , co_name" << endl
		  << "     , co_sp_rate" << endl
		  << "     , co_ceo" << endl
		  << "     , co_desc" << endl
		  << "     , co_open_date" << endl
		  << "     , co_st_id" << endl
		  << "     , ca.ad_line1 AS ca_ad_line1" << endl
		  << "     , ca.ad_line2 AS ca_ad_line2" << endl
		  << "     , zca.zc_town AS zca_zc_town" << endl
		  << "     , zca.zc_div AS zca_zc_div" << endl
		  << "     , ca.ad_zc_code AS ca_ad_zc_code" << endl
		  << "     , ca.ad_ctry AS ca_ad_ctry" << endl
		  << "     , s_num_out" << endl
		  << "     , s_start_date" << endl
		  << "     , s_exch_date" << endl
		  << "     , s_pe" << endl
		  << "     , s_52wk_high" << endl
		  << "     , s_52wk_high_date" << endl
		  << "     , s_52wk_low" << endl
		  << "     , s_52wk_low_date" << endl
		  << "     , s_dividend" << endl
		  << "     , s_yield" << endl
		  << "     , zea.zc_div AS zea_zc_div" << endl
		  << "     , ea.ad_ctry AS ea_ad_ctry" << endl
		  << "     , ea.ad_line1 AS ea_ad_line1" << endl
		  << "     , ea.ad_line2 AS ea_ad_line2" << endl
		  << "     , zea.zc_town AS zea_zc_town" << endl
		  << "     , ea.ad_zc_code AS ea_ad_zc_code" << endl
		  << "     , ex_close" << endl
		  << "     , ex_desc" << endl
		  << "     , ex_name" << endl
		  << "     , ex_num_symb" << endl
		  << "     , ex_open" << endl
		  << "FROM security" << endl
		  << "   , company" << endl
		  << "   , address ca" << endl
		  << "   , address ea" << endl
		  << "   , zip_code zca" << endl
		  << "   , zip_code zea" << endl
		  << "   , exchange" << endl
		  << "WHERE s_symb = '" << pIn->symbol << "'" << endl
		  << "  AND co_id = s_co_id" << endl
		  << "  AND ca.ad_id = co_ad_id" << endl
		  << "  AND ea.ad_id = ex_ad_id" << endl
		  << "  AND ex_id = s_ex_id" << endl
		  << "  AND ca.ad_zc_code = zca.zc_code" << endl
		  << "  AND ea.ad_zc_code = zea.zc_code";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	if (PQntuples(res) == 0) {
		return;
	}

	strncpy(pOut->s_name, PQgetvalue(res, 0, 0), cS_NAME_len);
	INT64 co_id = atoll(PQgetvalue(res, 0, 1));
	strncpy(pOut->co_name, PQgetvalue(res, 0, 2), cCO_NAME_len);
	strncpy(pOut->sp_rate, PQgetvalue(res, 0, 3), cSP_RATE_len);
	strncpy(pOut->ceo_name, PQgetvalue(res, 0, 4), cCEO_NAME_len);
	strncpy(pOut->co_desc, PQgetvalue(res, 0, 5), cCO_DESC_len);
	sscanf(PQgetvalue(res, 0, 6), "%hd-%hd-%hd %hd:%hd:%hd.%d",
			&pOut->open_date.year, &pOut->open_date.month,
			&pOut->open_date.day, &pOut->open_date.hour,
			&pOut->open_date.minute, &pOut->open_date.second,
			&pOut->open_date.fraction);
	strncpy(pOut->co_st_id, PQgetvalue(res, 0, 7), cST_ID_len);
	strncpy(pOut->co_ad_line1, PQgetvalue(res, 0, 8), cAD_LINE_len);
	strncpy(pOut->co_ad_line2, PQgetvalue(res, 0, 9), cAD_LINE_len);
	strncpy(pOut->co_ad_town, PQgetvalue(res, 0, 10), cAD_TOWN_len);
	strncpy(pOut->co_ad_div, PQgetvalue(res, 0, 11), cAD_DIV_len);
	strncpy(pOut->co_ad_zip, PQgetvalue(res, 0, 12), cAD_ZIP_len);
	strncpy(pOut->co_ad_cty, PQgetvalue(res, 0, 13), cAD_CTRY_len);
	pOut->num_out = atoll(PQgetvalue(res, 0, 14));
	sscanf(PQgetvalue(res, 0, 15), "%hd-%hd-%hd %hd:%hd:%hd.%d",
			&pOut->start_date.year, &pOut->start_date.month,
			&pOut->start_date.day, &pOut->start_date.hour,
			&pOut->start_date.minute, &pOut->start_date.second,
			&pOut->start_date.fraction);
	sscanf(PQgetvalue(res, 0, 16), "%hd-%hd-%hd %hd:%hd:%hd.%d",
			&pOut->ex_date.year, &pOut->ex_date.month, &pOut->ex_date.day,
			&pOut->ex_date.hour, &pOut->ex_date.minute, &pOut->ex_date.second,
			&pOut->ex_date.fraction);
	pOut->pe_ratio = atof(PQgetvalue(res, 0, 17));
	pOut->s52_wk_high = atof(PQgetvalue(res, 0, 18));
	sscanf(PQgetvalue(res, 0, 19), "%hd-%hd-%hd %hd:%hd:%hd.%d",
			&pOut->s52_wk_high_date.year, &pOut->s52_wk_high_date.month,
			&pOut->s52_wk_high_date.day, &pOut->s52_wk_high_date.hour,
			&pOut->s52_wk_high_date.minute, &pOut->s52_wk_high_date.second,
			&pOut->s52_wk_high_date.fraction);
	pOut->s52_wk_low = atof(PQgetvalue(res, 0, 20));
	sscanf(PQgetvalue(res, 0, 21), "%hd-%hd-%hd %hd:%hd:%hd.%d",
			&pOut->s52_wk_low_date.year, &pOut->s52_wk_low_date.month,
			&pOut->s52_wk_low_date.day, &pOut->s52_wk_low_date.hour,
			&pOut->s52_wk_low_date.minute, &pOut->s52_wk_low_date.second,
			&pOut->s52_wk_low_date.fraction);
	pOut->divid = atof(PQgetvalue(res, 0, 22));
	pOut->yield = atof(PQgetvalue(res, 0, 23));
	strncpy(pOut->ex_ad_div, PQgetvalue(res, 0, 24), cAD_DIV_len);
	strncpy(pOut->ex_ad_cty, PQgetvalue(res, 0, 25), cAD_CTRY_len);
	strncpy(pOut->ex_ad_line1, PQgetvalue(res, 0, 26), cAD_LINE_len);
	strncpy(pOut->ex_ad_line2, PQgetvalue(res, 0, 27), cAD_LINE_len);
	strncpy(pOut->ex_ad_town, PQgetvalue(res, 0, 28), cAD_TOWN_len);
	strncpy(pOut->ex_ad_zip, PQgetvalue(res, 0, 29), cAD_ZIP_len);
	pOut->ex_close = atoi(PQgetvalue(res, 0, 30));
	strncpy(pOut->ex_desc, PQgetvalue(res, 0, 31), cEX_DESC_len);
	strncpy(pOut->ex_name, PQgetvalue(res, 0, 32), cEX_NAME_len);
	pOut->ex_num_symb = atoi(PQgetvalue(res, 0, 33));
	pOut->ex_open = atoi(PQgetvalue(res, 0, 34));
	PQclear(res);

	if (m_bVerbose) {
		cout << "s_name = " << pOut->s_name << endl;
		cout << "co_id = " << co_id << endl;
		cout << "co_name = " << pOut->co_name << endl;
		cout << "sp_rate = " << pOut->sp_rate << endl;
		cout << "ceo_name = " << pOut->ceo_name << endl;
		cout << "co_desc = " << pOut->co_desc << endl;
		cout << "open_date = " << pOut->open_date.year << "-"
			 << pOut->open_date.month << "-" << pOut->open_date.day << " "
			 << pOut->open_date.hour << ":" << pOut->open_date.minute << ":"
			 << pOut->open_date.second << "." << pOut->open_date.fraction
			 << endl;
		cout << "co_st_id = " << pOut->co_st_id << endl;
		cout << "co_ad_line1 = " << pOut->co_ad_line1 << endl;
		cout << "co_ad_line2 = " << pOut->co_ad_line2 << endl;
		cout << "co_ad_town = " << pOut->co_ad_town << endl;
		cout << "co_ad_div = " << pOut->co_ad_div << endl;
		cout << "co_ad_zip = " << pOut->co_ad_zip << endl;
		cout << "co_ad_cty = " << pOut->co_ad_cty << endl;
		cout << "num_out = " << pOut->num_out << endl;
		cout << "start_date = " << pOut->start_date.year << "-"
			 << pOut->start_date.month << "-" << pOut->start_date.day << " "
			 << pOut->start_date.hour << ":" << pOut->start_date.minute << ":"
			 << pOut->start_date.second << "." << pOut->start_date.fraction
			 << endl;
		cout << "ex_date = " << pOut->ex_date.year << "-"
			 << pOut->ex_date.month << "-" << pOut->ex_date.day << " "
			 << pOut->ex_date.hour << " " << pOut->ex_date.minute << ":"
			 << pOut->ex_date.second << "." << pOut->ex_date.fraction << endl;
		cout << "pe_ratio = " << pOut->pe_ratio << endl;
		cout << "s52_wk_high = " << pOut->s52_wk_high << endl;
		cout << "s52_wk_high_date = " << pOut->s52_wk_high_date.year << "-"
			 << pOut->s52_wk_high_date.month << "-"
			 << pOut->s52_wk_high_date.day << " "
			 << pOut->s52_wk_high_date.hour << ":"
			 << pOut->s52_wk_high_date.minute << ":"
			 << pOut->s52_wk_high_date.second << "."
			 << pOut->s52_wk_high_date.fraction << endl;
		cout << "s52_wk_low = " << pOut->s52_wk_low << endl;
		cout << "s52_wk_low_date = " << pOut->s52_wk_low_date.year << "-"
			 << pOut->s52_wk_low_date.month << "-" << pOut->s52_wk_low_date.day
			 << " " << pOut->s52_wk_low_date.hour << ":"
			 << pOut->s52_wk_low_date.minute << ":"
			 << pOut->s52_wk_low_date.second << "."
			 << pOut->s52_wk_low_date.fraction << endl;
		cout << "divid = " << pOut->divid << endl;
		cout << "yield = " << pOut->yield << endl;
		cout << "ex_ad_div = " << pOut->ex_ad_div << endl;
		cout << "ex_ad_cty = " << pOut->ex_ad_cty << endl;
		cout << "ex_ad_line1 = " << pOut->ex_ad_line1 << endl;
		cout << "ex_ad_line2 = " << pOut->ex_ad_line2 << endl;
		cout << "ex_ad_town = " << pOut->ex_ad_town << endl;
		cout << "ex_ad_zip = " << pOut->ex_ad_zip << endl;
		cout << "ex_close = " << pOut->ex_close << endl;
		cout << "ex_desc = " << pOut->ex_desc << endl;
		cout << "ex_name = " << pOut->ex_name << endl;
		cout << "ex_num_symb = " << pOut->ex_num_symb << endl;
		cout << "ex_open = " << pOut->ex_open << endl;
	}

	osSQL.clear();
	osSQL.str("");
	osSQL << "SELECT co_name" << endl
		  << "     , in_name" << endl
		  << "FROM company_competitor" << endl
		  << "   , company" << endl
		  << "   , industry" << endl
		  << "WHERE cp_co_id = " << co_id << endl
		  << "  AND co_id = cp_comp_co_id" << endl
		  << "  AND in_id = cp_in_id" << endl
		  << "LIMIT " << max_comp_len;
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	int count = PQntuples(res);
	for (int i = 0; i < count; i++) {
		strncpy(pOut->cp_co_name[i], PQgetvalue(res, i, 0), cCO_NAME_len);
		strncpy(pOut->cp_in_name[i], PQgetvalue(res, i, 1), cIN_NAME_len);
	}
	PQclear(res);

	if (m_bVerbose) {
		for (int i = 0; i < count; i++) {
			cout << "cp_co_name[" << i << "] = " << pOut->cp_co_name[i]
				 << endl;
			cout << "cp_in_name[" << i << "] = " << pOut->cp_in_name[i]
				 << endl;
		}
	}

	osSQL.clear();
	osSQL.str("");
	osSQL << "SELECT fi_year" << endl
		  << "     , fi_qtr" << endl
		  << "     , fi_qtr_start_date" << endl
		  << "     , fi_revenue" << endl
		  << "     , fi_net_earn" << endl
		  << "     , fi_basic_eps" << endl
		  << "     , fi_dilut_eps" << endl
		  << "     , fi_margin" << endl
		  << "     , fi_inventory" << endl
		  << "     , fi_assets" << endl
		  << "     , fi_liability" << endl
		  << "     , fi_out_basic" << endl
		  << "     , fi_out_dilut" << endl
		  << "FROM financial" << endl
		  << "WHERE fi_co_id = " << co_id << endl
		  << "ORDER BY fi_year ASC" << endl
		  << "       , fi_qtr" << endl
		  << "LIMIT " << max_fin_len;
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	pOut->fin_len = PQntuples(res);
	for (int i = 0; i < pOut->fin_len; i++) {
		pOut->fin[i].year = atoi(PQgetvalue(res, i, 0));
		pOut->fin[i].qtr = atoi(PQgetvalue(res, i, 1));
		sscanf(PQgetvalue(res, i, 2), "%hd-%hd-%hd",
				&pOut->fin[i].start_date.year, &pOut->fin[i].start_date.month,
				&pOut->fin[i].start_date.day);
		pOut->fin[i].rev = atof(PQgetvalue(res, i, 3));
		pOut->fin[i].net_earn = atof(PQgetvalue(res, i, 4));
		pOut->fin[i].basic_eps = atof(PQgetvalue(res, i, 5));
		pOut->fin[i].dilut_eps = atof(PQgetvalue(res, i, 6));
		pOut->fin[i].margin = atof(PQgetvalue(res, i, 7));
		pOut->fin[i].invent = atof(PQgetvalue(res, i, 8));
		pOut->fin[i].assets = atof(PQgetvalue(res, i, 9));
		pOut->fin[i].liab = atof(PQgetvalue(res, i, 10));
		pOut->fin[i].out_basic = atof(PQgetvalue(res, i, 11));
		pOut->fin[i].out_dilut = atof(PQgetvalue(res, i, 12));
	}
	PQclear(res);

	if (m_bVerbose) {
		for (int i = 0; i < pOut->fin_len; i++) {
			cout << "year[" << i << "] = " << pOut->fin[i].year << endl;
			cout << "qtr[" << i << "] = " << pOut->fin[i].qtr << endl;
			cout << "start_date[" << i
				 << "] = " << pOut->fin[i].start_date.year << "-"
				 << pOut->fin[i].start_date.month << "-"
				 << pOut->fin[i].start_date.day << endl;
			cout << "rev[" << i << "] = " << pOut->fin[i].rev << endl;
			cout << "net_earn[" << i << "] = " << pOut->fin[i].net_earn
				 << endl;
			cout << "basic_eps[" << i << "] = " << pOut->fin[i].basic_eps
				 << endl;
			cout << "dilut_eps[" << i << "] = " << pOut->fin[i].dilut_eps
				 << endl;
			cout << "margin[" << i << "] = " << pOut->fin[i].margin << endl;
			cout << "invent[" << i << "] = " << pOut->fin[i].invent << endl;
			cout << "assets[" << i << "] = " << pOut->fin[i].assets << endl;
			cout << "liab[" << i << "] = " << pOut->fin[i].liab << endl;
			cout << "out_basic[" << i << "] = " << pOut->fin[i].out_basic
				 << endl;
			cout << "out_dilut[" << i << "] = " << pOut->fin[i].out_dilut
				 << endl;
		}
	}

	osSQL.clear();
	osSQL.str("");
	osSQL << "SELECT dm_date" << endl
		  << "     , dm_close" << endl
		  << "     , dm_high" << endl
		  << "     , dm_low" << endl
		  << "     , dm_vol" << endl
		  << "FROM daily_market" << endl
		  << "WHERE dm_s_symb = '" << pIn->symbol << "'" << endl
		  << "  AND dm_date >= '" << pIn->start_day.year << "-"
		  << pIn->start_day.month << "-" << pIn->start_day.day << "'" << endl
		  << "ORDER BY dm_date ASC" << endl
		  << "LIMIT " << pIn->max_rows_to_return;
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	pOut->day_len = PQntuples(res);
	for (int i = 0; i < pOut->day_len; i++) {
		sscanf(PQgetvalue(res, i, 0), "%hd-%hd-%hd", &pOut->day[i].date.year,
				&pOut->day[i].date.month, &pOut->day[i].date.day);
		pOut->day[i].close = atof(PQgetvalue(res, i, 1));
		pOut->day[i].high = atof(PQgetvalue(res, i, 2));
		pOut->day[i].low = atof(PQgetvalue(res, i, 3));
		pOut->day[i].vol = atoll(PQgetvalue(res, i, 4));
	}
	PQclear(res);

	if (m_bVerbose) {
		for (int i = 0; i < pOut->day_len; i++) {
			cout << "date[" << i << "] = " << pOut->day[i].date.year << "-"
				 << pOut->day[i].date.month << "-" << pOut->day[i].date.day
				 << endl;
			cout << "close[" << i << "] = " << pOut->day[i].close << endl;
			cout << "high[" << i << "] = " << pOut->day[i].high << endl;
			cout << "low[" << i << "] = " << pOut->day[i].low << endl;
			cout << "vol[" << i << "] = " << pOut->day[i].vol << endl;
		}
	}

	osSQL.clear();
	osSQL.str("");
	osSQL << "SELECT lt_price" << endl
		  << "     , lt_open_price" << endl
		  << "     , lt_vol" << endl
		  << "FROM last_trade" << endl
		  << "WHERE lt_s_symb = '" << pIn->symbol << "'";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	if (PQntuples(res) == 0) {
		return;
	}

	pOut->last_price = atof(PQgetvalue(res, 0, 0));
	pOut->last_open = atof(PQgetvalue(res, 0, 1));
	pOut->last_vol = atoll(PQgetvalue(res, 0, 2));
	PQclear(res);

	if (m_bVerbose) {
		cout << "last_price = " << pOut->last_price << endl;
		cout << "last_open = " << pOut->last_open << endl;
		cout << "last_vol = " << pOut->last_vol << endl;
	}

	osSQL.clear();
	osSQL.str("");
	if (pIn->access_lob_flag == 1) {
		osSQL << "SELECT ni_item" << endl
			  << "     , ni_dts" << endl
			  << "     , ni_source" << endl
			  << "     , ni_author" << endl
			  << "     ,'' AS ni_headline" << endl
			  << "     ,'' AS ni_summary" << endl
			  << "FROM news_xref" << endl
			  << "   ,  news_item" << endl
			  << "WHERE ni_id = nx_ni_id" << endl
			  << "  AND nx_co_id = " << co_id << endl
			  << "LIMIT " << max_news_len;
	} else {
		osSQL << "SELECT '' AS ni_item" << endl
			  << "     , ni_dts" << endl
			  << "     , ni_source" << endl
			  << "     , ni_author" << endl
			  << "     , ni_headline" << endl
			  << "     , ni_summary" << endl
			  << "FROM news_xref" << endl
			  << "   , news_item" << endl
			  << "WHERE ni_id = nx_ni_id" << endl
			  << "  AND nx_co_id = " << co_id << endl
			  << "LIMIT " << max_news_len;
	}
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	pOut->news_len = PQntuples(res);
	for (int i = 0; i < pOut->news_len; i++) {
		strncpy(pOut->news[i].item, PQgetvalue(res, i, 0), cNI_ITEM_len);
		sscanf(PQgetvalue(res, i, 1), "%hd-%hd-%hd %hd:%hd:%hd.%d",
				&pOut->news[i].dts.year, &pOut->news[i].dts.month,
				&pOut->news[i].dts.day, &pOut->news[i].dts.hour,
				&pOut->news[i].dts.minute, &pOut->news[i].dts.second,
				&pOut->news[i].dts.fraction);
		strncpy(pOut->news[i].src, PQgetvalue(res, i, 2), cNI_SOURCE_len);
		strncpy(pOut->news[i].auth, PQgetvalue(res, i, 3), cNI_AUTHOR_len);
		strncpy(pOut->news[i].headline, PQgetvalue(res, i, 4),
				cNI_HEADLINE_len);
		strncpy(pOut->news[i].summary, PQgetvalue(res, i, 5), cNI_SUMMARY_len);
	}
	PQclear(res);

	if (m_bVerbose) {
		for (int i = 0; i < pOut->news_len; i++) {
			cout << "item[" << i << "] = " << pOut->news[i].item << endl;
			cout << "news[" << i << "] = " << pOut->news[i].dts.year << "-"
				 << pOut->news[i].dts.month << "-" << pOut->news[i].dts.day
				 << " " << pOut->news[i].dts.hour << ":"
				 << pOut->news[i].dts.minute << ":"
				 << &pOut->news[i].dts.second << "."
				 << pOut->news[i].dts.fraction << endl;
			cout << "src[" << i << "] = " << pOut->news[i].src << endl;
			cout << "auth[" << i << "] = " << pOut->news[i].auth << endl;
			cout << "headline[" << i << "] = " << pOut->news[i].headline
				 << endl;
			cout << "summary[" << i << "] = " << pOut->news[i].summary << endl;
		}
	}
}

void
CDBConnectionClientSide::execute(
		const TTradeLookupFrame1Input *pIn, TTradeLookupFrame1Output *pOut)
{
	PGresult *res = NULL;

	pOut->num_found = 0;

	for (int i = 0; i < pIn->max_trades; i++) {
		ostringstream osSQL;
		osSQL << "SELECT t_bid_price" << endl
			  << "     , t_exec_name" << endl
			  << "     , t_is_cash" << endl
			  << "     , tt_is_mrkt" << endl
			  << "     , t_trade_price" << endl
			  << "FROM trade" << endl
			  << "   , trade_type" << endl
			  << "WHERE t_id = " << pIn->trade_id[i] << endl
			  << "  AND t_tt_id = tt_id";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		if (PQntuples(res) > 0) {
			++pOut->num_found;

			pOut->trade_info[i].bid_price = atof(PQgetvalue(res, 0, 0));
			strncpy(pOut->trade_info[i].exec_name, PQgetvalue(res, 0, 1),
					cEXEC_NAME_len);
			pOut->trade_info[i].is_cash
					= PQgetvalue(res, 0, 2)[0] == 't' ? true : false;
			pOut->trade_info[i].is_market
					= PQgetvalue(res, 0, 3)[0] == 't' ? true : false;
			pOut->trade_info[i].trade_price = atof(PQgetvalue(res, 0, 4));
		}
		PQclear(res);

		if (m_bVerbose) {
			cout << "bid_price[" << i
				 << "] = " << pOut->trade_info[i].bid_price << endl;
			cout << "exec_name[" << i
				 << "] = " << pOut->trade_info[i].exec_name << endl;
			cout << "is_cash[" << i << "] = " << pOut->trade_info[i].is_cash
				 << endl;
			cout << "is_market[" << i
				 << "] = " << pOut->trade_info[i].is_market << endl;
			cout << "trade_price[" << i
				 << "] = " << pOut->trade_info[i].trade_price << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT se_amt" << endl
			  << "     , se_cash_due_date" << endl
			  << "     , se_cash_type" << endl
			  << "FROM settlement" << endl
			  << "WHERE se_t_id = " << pIn->trade_id[i];
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		if (PQntuples(res) > 0) {
			pOut->trade_info[i].settlement_amount
					= atof(PQgetvalue(res, 0, 0));
			sscanf(PQgetvalue(res, 0, 1), "%hd-%hd-%hd",
					&pOut->trade_info[i].settlement_cash_due_date.year,
					&pOut->trade_info[i].settlement_cash_due_date.month,
					&pOut->trade_info[i].settlement_cash_due_date.day);
			strncpy(pOut->trade_info[i].settlement_cash_type,
					PQgetvalue(res, 0, 2), cSE_CASH_TYPE_len);
		}
		PQclear(res);

		if (m_bVerbose) {
			cout << "settlement_amount[" << i
				 << "] = " << pOut->trade_info[i].settlement_amount << endl;
			cout << "settlement_cash_due_date[" << i
				 << "] = " << pOut->trade_info[i].settlement_cash_due_date.year
				 << "-" << pOut->trade_info[i].settlement_cash_due_date.month
				 << "-" << pOut->trade_info[i].settlement_cash_due_date.day
				 << endl;
			cout << "settlement_cash_type[" << i
				 << "] = " << pOut->trade_info[i].settlement_cash_type << endl;
		}

		if (pOut->trade_info[i].is_cash) {
			osSQL.clear();
			osSQL.str("");
			osSQL << "SELECT ct_amt" << endl
				  << "     , ct_dts" << endl
				  << "     , ct_name" << endl
				  << "FROM cash_transaction" << endl
				  << "WHERE ct_t_id = " << pIn->trade_id[i];
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res = exec(osSQL.str().c_str());

			if (PQntuples(res) > 0) {
				pOut->trade_info[i].cash_transaction_amount
						= atof(PQgetvalue(res, 0, 0));
				sscanf(PQgetvalue(res, 0, 1), "%hd-%hd-%hd %hd:%hd:%hd.%d",
						&pOut->trade_info[i].cash_transaction_dts.year,
						&pOut->trade_info[i].cash_transaction_dts.month,
						&pOut->trade_info[i].cash_transaction_dts.day,
						&pOut->trade_info[i].cash_transaction_dts.hour,
						&pOut->trade_info[i].cash_transaction_dts.minute,
						&pOut->trade_info[i].cash_transaction_dts.second,
						&pOut->trade_info[i].cash_transaction_dts.fraction);
				strncpy(pOut->trade_info[i].cash_transaction_name,
						PQgetvalue(res, 0, 2), cCT_NAME_len);
			}

			PQclear(res);

			if (m_bVerbose) {
				cout << "cash_transaction_amount[" << i
					 << "] = " << pOut->trade_info[i].cash_transaction_amount
					 << endl;
				cout << "cash_transaction_dts[" << i
					 << "] = " << pOut->trade_info[i].cash_transaction_dts.year
					 << "-" << pOut->trade_info[i].cash_transaction_dts.month
					 << "-" << pOut->trade_info[i].cash_transaction_dts.day
					 << " " << pOut->trade_info[i].cash_transaction_dts.hour
					 << ":" << pOut->trade_info[i].cash_transaction_dts.minute
					 << ":" << pOut->trade_info[i].cash_transaction_dts.second
					 << "."
					 << pOut->trade_info[i].cash_transaction_dts.fraction
					 << endl;
				cout << "cash_transaction_name[" << i
					 << "] = " << pOut->trade_info[i].cash_transaction_name
					 << endl;
			}

			osSQL.clear();
			osSQL.str("");
			osSQL << "SELECT th_dts" << endl
				  << "     , th_st_id" << endl
				  << "FROM trade_history" << endl
				  << "WHERE th_t_id = " << pIn->trade_id[i] << endl
				  << "ORDER BY th_dts" << endl
				  << "LIMIT 3";
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res = exec(osSQL.str().c_str());

			int count = PQntuples(res);
			for (int k = 0; k < count; k++) {
				sscanf(PQgetvalue(res, k, 0), "%hd-%hd-%hd %hd:%hd:%hd.%d",
						&pOut->trade_info[i].trade_history_dts[k].year,
						&pOut->trade_info[i].trade_history_dts[k].month,
						&pOut->trade_info[i].trade_history_dts[k].day,
						&pOut->trade_info[i].trade_history_dts[k].hour,
						&pOut->trade_info[i].trade_history_dts[k].minute,
						&pOut->trade_info[i].trade_history_dts[k].second,
						&pOut->trade_info[i].trade_history_dts[k].fraction);
				strncpy(pOut->trade_info[i].trade_history_status_id[k],
						PQgetvalue(res, k, 1), cTH_ST_ID_len);
			}
			PQclear(res);

			if (m_bVerbose) {
				for (int k = 0; k < count; k++) {
					cout << "trade_history_dts[" << k << "] = "
						 << pOut->trade_info[i].trade_history_dts[k].year
						 << "-"
						 << pOut->trade_info[i].trade_history_dts[k].month
						 << "-" << pOut->trade_info[i].trade_history_dts[k].day
						 << " "
						 << pOut->trade_info[i].trade_history_dts[k].hour
						 << ":"
						 << pOut->trade_info[i].trade_history_dts[k].minute
						 << ":"
						 << pOut->trade_info[i].trade_history_dts[k].second
						 << "."
						 << pOut->trade_info[i].trade_history_dts[k].fraction
						 << endl;
					cout << "trade_history_status_id[" << k << "] = "
						 << pOut->trade_info[i].trade_history_status_id[k]
						 << endl;
				}
			}
		}
	}
}

void
CDBConnectionClientSide::execute(
		const TTradeLookupFrame2Input *pIn, TTradeLookupFrame2Output *pOut)
{
	PGresult *res = NULL;
	ostringstream osSQL;

	osSQL << "SELECT t_bid_price" << endl
		  << "     , t_exec_name" << endl
		  << "     , t_is_cash" << endl
		  << "     , t_id" << endl
		  << "     , t_trade_price" << endl
		  << "FROM trade" << endl
		  << "WHERE t_ca_id = " << pIn->acct_id << endl
		  << "  AND t_dts >= '" << pIn->start_trade_dts.year << "-"
		  << pIn->start_trade_dts.month << "-" << pIn->start_trade_dts.day
		  << " " << pIn->start_trade_dts.hour << ":"
		  << pIn->start_trade_dts.minute << ":" << pIn->start_trade_dts.second
		  << "." << pIn->start_trade_dts.fraction << "'" << endl
		  << "  AND t_dts <= '" << pIn->end_trade_dts.year << "-"
		  << pIn->end_trade_dts.month << "-" << pIn->end_trade_dts.day << " "
		  << pIn->end_trade_dts.hour << ":" << pIn->end_trade_dts.minute << ":"
		  << pIn->end_trade_dts.second << "." << pIn->end_trade_dts.fraction
		  << "'" << endl
		  << "ORDER BY t_dts" << endl
		  << "LIMIT " << pIn->max_trades;
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	pOut->num_found = PQntuples(res);
	for (int i = 0; i < pOut->num_found; i++) {
		PGresult *res2 = NULL;

		pOut->trade_info[i].bid_price = atof(PQgetvalue(res, i, 0));
		strncpy(pOut->trade_info[i].exec_name, PQgetvalue(res, i, 1),
				cEXEC_NAME_len);
		pOut->trade_info[i].is_cash
				= PQgetvalue(res, i, 2)[0] == 't' ? true : false;
		pOut->trade_info[i].trade_id = atoll(PQgetvalue(res, i, 3));
		pOut->trade_info[i].trade_price = atof(PQgetvalue(res, i, 4));

		if (m_bVerbose) {
			cout << "bid_price[" << i
				 << "] = " << pOut->trade_info[i].bid_price << endl;
			cout << "exec_name[" << i
				 << "] = " << pOut->trade_info[i].exec_name << endl;
			cout << "is_cash[" << i << "] = " << pOut->trade_info[i].is_cash
				 << endl;
			cout << "trade_id[" << i << "] = " << pOut->trade_info[i].trade_id
				 << endl;
			cout << "trade_price[" << i
				 << "] = " << pOut->trade_info[i].trade_price << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT se_amt" << endl
			  << "     , se_cash_due_date" << endl
			  << "     , se_cash_type" << endl
			  << "FROM settlement" << endl
			  << "WHERE se_t_id = " << pOut->trade_info[i].trade_id;
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res2 = exec(osSQL.str().c_str());

		if (PQntuples(res2) > 0) {
			pOut->trade_info[i].settlement_amount
					= atof(PQgetvalue(res2, 0, 0));
			sscanf(PQgetvalue(res2, 0, 1), "%hd-%hd-%hd",
					&pOut->trade_info[i].settlement_cash_due_date.year,
					&pOut->trade_info[i].settlement_cash_due_date.month,
					&pOut->trade_info[i].settlement_cash_due_date.day);
			strncpy(pOut->trade_info[i].settlement_cash_type,
					PQgetvalue(res2, 0, 2), cSE_CASH_TYPE_len);
		}
		PQclear(res2);

		if (m_bVerbose) {
			cout << "settlement_amount[" << i
				 << "] = " << pOut->trade_info[i].settlement_amount << endl;
			cout << "settlement_cash_due_date[" << i
				 << "] = " << pOut->trade_info[i].settlement_cash_due_date.year
				 << "-" << pOut->trade_info[i].settlement_cash_due_date.month
				 << "-" << pOut->trade_info[i].settlement_cash_due_date.day
				 << endl;
			cout << "settlement_cash_type[" << i
				 << "] = " << pOut->trade_info[i].settlement_cash_type << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT ct_amt" << endl
			  << "     , ct_dts" << endl
			  << "     , ct_name" << endl
			  << "FROM cash_transaction" << endl
			  << "WHERE ct_t_id = " << pOut->trade_info[i].trade_id;
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res2 = exec(osSQL.str().c_str());

		if (PQntuples(res) > 0) {
			pOut->trade_info[i].cash_transaction_amount
					= atof(PQgetvalue(res, 0, 0));
			sscanf(PQgetvalue(res, 0, 1), "%hd-%hd-%hd %hd:%hd:%hd.%d",
					&pOut->trade_info[i].cash_transaction_dts.year,
					&pOut->trade_info[i].cash_transaction_dts.month,
					&pOut->trade_info[i].cash_transaction_dts.day,
					&pOut->trade_info[i].cash_transaction_dts.hour,
					&pOut->trade_info[i].cash_transaction_dts.minute,
					&pOut->trade_info[i].cash_transaction_dts.second,
					&pOut->trade_info[i].cash_transaction_dts.fraction);
			strncpy(pOut->trade_info[i].cash_transaction_name,
					PQgetvalue(res, 0, 2), cCT_NAME_len);
		}
		PQclear(res2);

		if (m_bVerbose) {
			cout << "cash_transaction_amount[" << i
				 << "] = " << pOut->trade_info[i].cash_transaction_amount
				 << endl;
			cout << "cash_transaction_dts[" << i
				 << "] = " << pOut->trade_info[i].cash_transaction_dts.year
				 << "-" << pOut->trade_info[i].cash_transaction_dts.month
				 << "-" << pOut->trade_info[i].cash_transaction_dts.day << " "
				 << pOut->trade_info[i].cash_transaction_dts.hour << ":"
				 << pOut->trade_info[i].cash_transaction_dts.minute << ":"
				 << pOut->trade_info[i].cash_transaction_dts.second << "."
				 << pOut->trade_info[i].cash_transaction_dts.fraction << endl;
			cout << "cash_transaction_name[" << i
				 << "] = " << pOut->trade_info[i].cash_transaction_name
				 << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT th_dts" << endl
			  << "     , th_st_id" << endl
			  << "FROM trade_history" << endl
			  << "WHERE th_t_id = " << pOut->trade_info[i].trade_id << endl
			  << "ORDER BY th_dts" << endl
			  << "LIMIT 3";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res2 = exec(osSQL.str().c_str());

		int count = PQntuples(res);
		for (int j = 0; j < count; j++) {
			sscanf(PQgetvalue(res, j, 0), "%hd-%hd-%hd %hd:%hd:%hd.%d",
					&pOut->trade_info[i].trade_history_dts[j].year,
					&pOut->trade_info[i].trade_history_dts[j].month,
					&pOut->trade_info[i].trade_history_dts[j].day,
					&pOut->trade_info[i].trade_history_dts[j].hour,
					&pOut->trade_info[i].trade_history_dts[j].minute,
					&pOut->trade_info[i].trade_history_dts[j].second,
					&pOut->trade_info[i].trade_history_dts[j].fraction);
			strncpy(pOut->trade_info[i].trade_history_status_id[j],
					PQgetvalue(res, j, 1), cTH_ST_ID_len);
		}
		PQclear(res2);

		if (m_bVerbose) {
			for (int j = 0; j < count; j++) {
				cout << "trade_history_dts[" << j
					 << "] = " << pOut->trade_info[i].trade_history_dts[j].year
					 << "-" << pOut->trade_info[i].trade_history_dts[j].month
					 << "-" << pOut->trade_info[i].trade_history_dts[j].day
					 << " " << pOut->trade_info[i].trade_history_dts[j].hour
					 << ":" << pOut->trade_info[i].trade_history_dts[j].minute
					 << ":" << pOut->trade_info[i].trade_history_dts[j].second
					 << "."
					 << pOut->trade_info[i].trade_history_dts[j].fraction
					 << endl;
				cout << "trade_history_status_id[" << j << "] = "
					 << pOut->trade_info[i].trade_history_status_id[j] << endl;
			}
		}
	}
	PQclear(res);
}

void
CDBConnectionClientSide::execute(
		const TTradeLookupFrame3Input *pIn, TTradeLookupFrame3Output *pOut)
{
	PGresult *res = NULL;
	ostringstream osSQL;

	osSQL << "SELECT t_ca_id" << endl
		  << "     , t_exec_name" << endl
		  << "     , t_is_cash" << endl
		  << "     , t_trade_price" << endl
		  << "     , t_qty" << endl
		  << "     , t_dts" << endl
		  << "     , t_id" << endl
		  << "     , t_tt_id" << endl
		  << "FROM trade" << endl
		  << "WHERE t_s_symb = '" << pIn->symbol << "'" << endl
		  << "  AND t_dts >= '" << pIn->start_trade_dts.year << "-"
		  << pIn->start_trade_dts.month << "-" << pIn->start_trade_dts.day
		  << " " << pIn->start_trade_dts.hour << ":"
		  << pIn->start_trade_dts.minute << ":" << pIn->start_trade_dts.second
		  << "." << pIn->start_trade_dts.fraction << "'" << endl
		  << "  AND t_dts <= '" << pIn->end_trade_dts.year << "-"
		  << pIn->end_trade_dts.month << "-" << pIn->end_trade_dts.day << " "
		  << pIn->end_trade_dts.hour << ":" << pIn->end_trade_dts.minute << ":"
		  << pIn->end_trade_dts.second << "." << pIn->end_trade_dts.fraction
		  << "'" << endl
		  << "ORDER BY t_dts ASC" << endl
		  << "LIMIT " << pIn->max_trades;
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	pOut->num_found = PQntuples(res);
	for (int i = 0; i < pOut->num_found; i++) {
		PGresult *res2 = NULL;

		pOut->trade_info[i].acct_id = atoll(PQgetvalue(res, i, 0));
		strncpy(pOut->trade_info[i].exec_name, PQgetvalue(res, i, 1),
				cEXEC_NAME_len);
		pOut->trade_info[i].is_cash
				= PQgetvalue(res, 0, 2)[0] == 't' ? true : false;
		pOut->trade_info[i].price = atof(PQgetvalue(res, i, 3));
		pOut->trade_info[i].quantity = atoi(PQgetvalue(res, i, 4));
		sscanf(PQgetvalue(res, i, 5), "%hd-%hd-%hd %hd:%hd:%hd.%d",
				&pOut->trade_info[i].trade_dts.year,
				&pOut->trade_info[i].trade_dts.month,
				&pOut->trade_info[i].trade_dts.day,
				&pOut->trade_info[i].trade_dts.hour,
				&pOut->trade_info[i].trade_dts.minute,
				&pOut->trade_info[i].trade_dts.second,
				&pOut->trade_info[i].trade_dts.fraction);
		pOut->trade_info[i].trade_id = atoll(PQgetvalue(res, i, 6));
		strncpy(pOut->trade_info[i].trade_type, PQgetvalue(res, i, 7),
				cTT_ID_len);

		if (m_bVerbose) {
			cout << "acct_id[" << i << "] = " << pOut->trade_info[i].acct_id
				 << endl;
			cout << "exec_name[" << i
				 << "] = " << pOut->trade_info[i].exec_name << endl;
			cout << "is_cash[" << i << "] = " << pOut->trade_info[i].is_cash
				 << endl;
			cout << "price[" << i << "] = " << pOut->trade_info[i].price
				 << endl;
			cout << "quantity[" << i << "] = " << pOut->trade_info[i].quantity
				 << endl;
			cout << "trade_dts[" << i
				 << "] = " << pOut->trade_info[i].trade_dts.year << "-"
				 << pOut->trade_info[i].trade_dts.month << "-"
				 << pOut->trade_info[i].trade_dts.day << " "
				 << pOut->trade_info[i].trade_dts.hour << ":"
				 << pOut->trade_info[i].trade_dts.minute << ":"
				 << pOut->trade_info[i].trade_dts.second << "."
				 << pOut->trade_info[i].trade_dts.fraction << endl;
			cout << "trade_id[" << i << "] = " << pOut->trade_info[i].trade_id
				 << endl;
			cout << "trade_type[" << i
				 << "] = " << pOut->trade_info[i].trade_type << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT se_amt" << endl
			  << "     , se_cash_due_date" << endl
			  << "     , se_cash_type" << endl
			  << "FROM settlement" << endl
			  << "WHERE se_t_id = " << pOut->trade_info[i].trade_id;
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res2 = exec(osSQL.str().c_str());

		if (PQntuples(res2) > 0) {
			pOut->trade_info[i].settlement_amount
					= atof(PQgetvalue(res2, 0, 0));
			sscanf(PQgetvalue(res2, 0, 1), "%hd-%hd-%hd",
					&pOut->trade_info[i].settlement_cash_due_date.year,
					&pOut->trade_info[i].settlement_cash_due_date.month,
					&pOut->trade_info[i].settlement_cash_due_date.day);
			strncpy(pOut->trade_info[i].settlement_cash_type,
					PQgetvalue(res2, 0, 2), cSE_CASH_TYPE_len);
		}
		PQclear(res2);

		if (m_bVerbose) {
			cout << "settlement_amount[" << i
				 << "] = " << pOut->trade_info[i].settlement_amount << endl;
			cout << "settlement_cash_due_date[" << i
				 << "] = " << pOut->trade_info[i].settlement_cash_due_date.year
				 << "-" << pOut->trade_info[i].settlement_cash_due_date.month
				 << "-" << pOut->trade_info[i].settlement_cash_due_date.day
				 << endl;
			cout << "settlement_cash_type[" << i
				 << "] = " << pOut->trade_info[i].settlement_cash_type << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT ct_amt" << endl
			  << "     , ct_dts" << endl
			  << "     , ct_name" << endl
			  << "FROM cash_transaction" << endl
			  << "WHERE ct_t_id = " << pOut->trade_info[i].trade_id;
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res2 = exec(osSQL.str().c_str());

		if (PQntuples(res2) > 0) {
			pOut->trade_info[i].cash_transaction_amount
					= atof(PQgetvalue(res2, 0, 0));
			sscanf(PQgetvalue(res2, 0, 1), "%hd-%hd-%hd %hd:%hd:%hd.%d",
					&pOut->trade_info[i].cash_transaction_dts.year,
					&pOut->trade_info[i].cash_transaction_dts.month,
					&pOut->trade_info[i].cash_transaction_dts.day,
					&pOut->trade_info[i].cash_transaction_dts.hour,
					&pOut->trade_info[i].cash_transaction_dts.minute,
					&pOut->trade_info[i].cash_transaction_dts.second,
					&pOut->trade_info[i].cash_transaction_dts.fraction);
			strncpy(pOut->trade_info[i].cash_transaction_name,
					PQgetvalue(res2, 0, 2), cCT_NAME_len);
		}
		PQclear(res2);

		if (m_bVerbose) {
			cout << "cash_transaction_amount[" << i
				 << "] = " << pOut->trade_info[i].cash_transaction_amount
				 << endl;
			cout << "cash_transaction_dts[" << i
				 << "] = " << pOut->trade_info[i].cash_transaction_dts.year
				 << "-" << pOut->trade_info[i].cash_transaction_dts.month
				 << "-" << pOut->trade_info[i].cash_transaction_dts.day << " "
				 << pOut->trade_info[i].cash_transaction_dts.hour << ":"
				 << pOut->trade_info[i].cash_transaction_dts.minute << ":"
				 << pOut->trade_info[i].cash_transaction_dts.second << "."
				 << pOut->trade_info[i].cash_transaction_dts.fraction << endl;
			cout << "cash_transaction_name[" << i
				 << "] = " << pOut->trade_info[i].cash_transaction_name
				 << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT th_dts" << endl
			  << "     , th_st_id" << endl
			  << "FROM trade_history" << endl
			  << "WHERE th_t_id = " << pOut->trade_info[i].trade_id << endl
			  << "ORDER BY th_dts ASC" << endl
			  << "LIMIT 3";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res2 = exec(osSQL.str().c_str());

		int count = PQntuples(res2);
		for (int j = 0; j < count; j++) {
			sscanf(PQgetvalue(res2, j, 0), "%hd-%hd-%hd %hd:%hd:%hd.%d",
					&pOut->trade_info[i].trade_history_dts[j].year,
					&pOut->trade_info[i].trade_history_dts[j].month,
					&pOut->trade_info[i].trade_history_dts[j].day,
					&pOut->trade_info[i].trade_history_dts[j].hour,
					&pOut->trade_info[i].trade_history_dts[j].minute,
					&pOut->trade_info[i].trade_history_dts[j].second,
					&pOut->trade_info[i].trade_history_dts[j].fraction);
			strncpy(pOut->trade_info[i].trade_history_status_id[j],
					PQgetvalue(res2, j, 1), cTH_ST_ID_len);
		}
		PQclear(res2);

		if (m_bVerbose) {
			for (int j = 0; j < count; j++) {
				cout << "trade_history_dts[" << j
					 << "] = " << pOut->trade_info[i].trade_history_dts[j].year
					 << "-" << pOut->trade_info[i].trade_history_dts[j].month
					 << "-" << pOut->trade_info[i].trade_history_dts[j].day
					 << " " << pOut->trade_info[i].trade_history_dts[j].hour
					 << ":" << pOut->trade_info[i].trade_history_dts[j].minute
					 << ":" << pOut->trade_info[i].trade_history_dts[j].second
					 << "."
					 << pOut->trade_info[i].trade_history_dts[j].fraction
					 << endl;
				cout << "trade_history_status_id[" << j << "] = "
					 << pOut->trade_info[i].trade_history_status_id[j] << endl;
			}
		}
	}
	PQclear(res);
}

void
CDBConnectionClientSide::execute(
		const TTradeLookupFrame4Input *pIn, TTradeLookupFrame4Output *pOut)
{
	PGresult *res = NULL;
	ostringstream osSQL;

	osSQL << "SELECT t_id" << endl
		  << "FROM trade" << endl
		  << "WHERE t_ca_id = " << pIn->acct_id << endl
		  << "  AND t_dts >= '" << pIn->trade_dts.year << "-"
		  << pIn->trade_dts.month << "-" << pIn->trade_dts.day << " "
		  << pIn->trade_dts.hour << ":" << pIn->trade_dts.minute << ":"
		  << pIn->trade_dts.second << "." << pIn->trade_dts.fraction << "'"
		  << endl
		  << "ORDER BY t_dts ASC" << endl
		  << "LIMIT 1";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	pOut->num_trades_found = PQntuples(res);
	if (pOut->num_trades_found == 0) {
		PQclear(res);
		return;
	}

	pOut->trade_id = atoll(PQgetvalue(res, 0, 0));
	PQclear(res);

	if (m_bVerbose) {
		cout << "t_id = " << pOut->trade_id << endl;
	}

	osSQL.clear();
	osSQL.str("");
	osSQL << "SELECT hh_h_t_id" << endl
		  << "     , hh_t_id" << endl
		  << "     , hh_before_qty" << endl
		  << "     , hh_after_qty" << endl
		  << "FROM holding_history" << endl
		  << "WHERE hh_h_t_id IN (" << endl
		  << "                       SELECT hh_h_t_id" << endl
		  << "                       FROM holding_history" << endl
		  << "                       WHERE hh_t_id = " << pOut->trade_id
		  << endl
		  << "                   )";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	pOut->num_found = PQntuples(res);
	for (int i = 0; i < pOut->num_found; i++) {
		pOut->trade_info[i].holding_history_id = atoll(PQgetvalue(res, i, 0));
		pOut->trade_info[i].holding_history_trade_id
				= atoll(PQgetvalue(res, i, 1));
		pOut->trade_info[i].quantity_before = atoi(PQgetvalue(res, i, 2));
		pOut->trade_info[i].quantity_after = atoi(PQgetvalue(res, i, 3));
	}
	PQclear(res);

	if (m_bVerbose) {
		for (int i = 0; i < pOut->num_found; i++) {
			cout << "holding_history_id[" << i
				 << "] = " << pOut->trade_info[i].holding_history_id << endl;
			cout << "holding_history_trade_id[" << i
				 << "] = " << pOut->trade_info[i].holding_history_trade_id
				 << endl;
			cout << "quantity_before[" << i
				 << "] = " << pOut->trade_info[i].quantity_before << endl;
			cout << "quantity_after[" << i
				 << "] = " << pOut->trade_info[i].quantity_after << endl;
		}
	}
}

void
CDBConnectionClientSide::execute(
		const TTradeOrderFrame1Input *pIn, TTradeOrderFrame1Output *pOut)
{
	PGresult *res = NULL;
	ostringstream osSQL;

	osSQL << "SELECT ca_name" << endl
		  << "     , ca_b_id" << endl
		  << "     , ca_c_id" << endl
		  << "     , ca_tax_st" << endl
		  << "FROM customer_account" << endl
		  << "WHERE ca_id = " << pIn->acct_id;
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	pOut->num_found = PQntuples(res);
	if (pOut->num_found == 0) {
		PQclear(res);
		return;
	}

	strncpy(pOut->acct_name, PQgetvalue(res, 0, 0), cCA_NAME_len);
	pOut->broker_id = atoll(PQgetvalue(res, 0, 1));
	pOut->cust_id = atoll(PQgetvalue(res, 0, 2));
	pOut->tax_status = atoi(PQgetvalue(res, 0, 3));

	PQclear(res);

	if (m_bVerbose) {
		cout << "acct_name = " << pOut->acct_name << endl;
		cout << "broker_id = " << pOut->broker_id << endl;
		cout << "cust_id = " << pOut->cust_id << endl;
		cout << "tax_status = " << pOut->tax_status << endl;
	}

	osSQL.clear();
	osSQL.str("");
	osSQL << "SELECT c_f_name" << endl
		  << "     , c_l_name" << endl
		  << "     , c_tier" << endl
		  << "     , c_tax_id" << endl
		  << "FROM customer" << endl
		  << "WHERE c_id = " << pOut->broker_id;
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	if (PQntuples(res) != 0) {
		strncpy(pOut->cust_f_name, PQgetvalue(res, 0, 0), cF_NAME_len);
		strncpy(pOut->cust_l_name, PQgetvalue(res, 0, 1), cL_NAME_len);
		pOut->cust_tier = atoi(PQgetvalue(res, 0, 2));
		strncpy(pOut->tax_id, PQgetvalue(res, 0, 3), cTAX_ID_len);
	}
	PQclear(res);

	if (m_bVerbose) {
		cout << "cust_f_name = " << pOut->cust_f_name << endl;
		cout << "cust_l_name = " << pOut->cust_l_name << endl;
		cout << "cust_tier = " << pOut->cust_tier << endl;
		cout << "tax_id = " << pOut->tax_id << endl;
	}

	osSQL.clear();
	osSQL.str("");
	osSQL << "SELECT b_name" << endl
		  << "FROM Broker" << endl
		  << "WHERE b_id = " << pOut->broker_id;
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	if (PQntuples(res) != 0) {
		strncpy(pOut->broker_name, PQgetvalue(res, 0, 0), cB_NAME_len);
	}
	PQclear(res);

	if (m_bVerbose) {
		cout << "broker_name = " << pOut->broker_name << endl;
	}
}

void
CDBConnectionClientSide::execute(
		const TTradeOrderFrame2Input *pIn, TTradeOrderFrame2Output *pOut)
{
	PGresult *res = NULL;
	ostringstream osSQL;

	osSQL << "SELECT ap_acl" << endl
		  << "FROM account_permission" << endl
		  << "WHERE ap_ca_id = " << pIn->acct_id << endl
		  << "  AND ap_f_name = '" << pIn->exec_f_name << "'" << endl
		  << "  AND ap_l_name = '" << pIn->exec_l_name << "'" << endl
		  << "  AND ap_tax_id = '" << pIn->exec_tax_id << "'";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	if (PQntuples(res) == 0) {
		PQclear(res);
		return;
	}

	strncpy(pOut->ap_acl, PQgetvalue(res, 0, 0), cACL_len);
	PQclear(res);

	if (m_bVerbose) {
		cout << "ap_acl = " << pOut->ap_acl << endl;
	}
}

void
CDBConnectionClientSide::execute(
		const TTradeOrderFrame3Input *pIn, TTradeOrderFrame3Output *pOut)
{
	PGresult *res = NULL;
	ostringstream osSQL;

	INT64 co_id = 0;
	char ex_id[cEX_ID_len + 1];

	if (pIn->symbol[0] == '\0') {
		char *co_name
				= PQescapeLiteral(m_Conn, pIn->co_name, strlen(pIn->co_name));
		osSQL << "SELECT co_id" << endl
			  << "FROM company" << endl
			  << "WHERE co_name = e" << co_name;
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		if (PQntuples(res) == 0) {
			PQclear(res);
			return;
		}

		co_id = atoll(PQgetvalue(res, 0, 0));
		PQclear(res);

		if (m_bVerbose) {
			cout << "co_id = " << co_id << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT s_ex_id" << endl
			  << "     , s_name" << endl
			  << "     , s_symb" << endl
			  << "FROM security" << endl
			  << "WHERE s_co_id = " << co_id << endl
			  << "  AND s_issue = '" << pIn->issue << "'";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		if (PQntuples(res) == 0) {
			PQclear(res);
			return;
		}

		strncpy(ex_id, PQgetvalue(res, 0, 0), cEX_ID_len);
		strncpy(pOut->s_name, PQgetvalue(res, 0, 1), cS_NAME_len);
		strncpy(pOut->symbol, PQgetvalue(res, 0, 2), cSYMBOL_len);
		PQclear(res);

		if (m_bVerbose) {
			cout << "ex_id = " << ex_id << endl;
			cout << "s_name = " << pOut->s_name << endl;
			cout << "symbol = " << pOut->symbol << endl;
		}
	} else {
		strncpy(pOut->symbol, pIn->symbol, cSYMBOL_len);

		osSQL << "SELECT s_co_id" << endl
			  << "     , s_ex_id" << endl
			  << "     , s_name" << endl
			  << "FROM security" << endl
			  << "WHERE s_symb = '" << pIn->symbol << "'";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		if (PQntuples(res) == 0) {
			PQclear(res);
			return;
		}

		co_id = atoll(PQgetvalue(res, 0, 0));
		strncpy(ex_id, PQgetvalue(res, 0, 1), cEX_ID_len);
		strncpy(pOut->s_name, PQgetvalue(res, 0, 2), cS_NAME_len);
		PQclear(res);

		if (m_bVerbose) {
			cout << "co_id = " << co_id << endl;
			cout << "ex_id = " << ex_id << endl;
			cout << "s_name = " << pOut->s_name << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT co_name" << endl
			  << "FROM company" << endl
			  << "WHERE co_id = " << co_id;
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		if (PQntuples(res) == 0) {
			PQclear(res);
			return;
		}

		strncpy(pOut->co_name, PQgetvalue(res, 0, 0), cCO_NAME_len);
		PQclear(res);

		if (m_bVerbose) {
			cout << "co_name = " << pOut->co_name << endl;
		}
	}

	osSQL.clear();
	osSQL.str("");
	osSQL << "SELECT lt_price" << endl
		  << "FROM last_trade" << endl
		  << "WHERE lt_s_symb = '" << pOut->symbol << "'";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	if (PQntuples(res) == 0) {
		PQclear(res);
		return;
	}

	pOut->market_price = atof(PQgetvalue(res, 0, 0));
	PQclear(res);

	if (m_bVerbose) {
		cout << "market_price = " << pOut->market_price << endl;
	}

	osSQL.clear();
	osSQL.str("");
	osSQL << "SELECT tt_is_mrkt" << endl
		  << "     , tt_is_sell" << endl
		  << "FROM trade_type" << endl
		  << "WHERE tt_id = '" << pIn->trade_type_id << "'";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	if (PQntuples(res) == 0) {
		PQclear(res);
		return;
	}

	pOut->type_is_market = PQgetvalue(res, 0, 0)[0] == 't' ? 1 : 0;
	pOut->type_is_sell = PQgetvalue(res, 0, 1)[0] == 't' ? 1 : 0;
	PQclear(res);

	if (m_bVerbose) {
		cout << "type_is_market = " << pOut->type_is_market << endl;
		cout << "type_is_sell = " << pOut->type_is_sell << endl;
	}

	if (pOut->type_is_market == 1) {
		pOut->requested_price = pOut->market_price;
	} else {
		pOut->requested_price = pIn->requested_price;
	}

	pOut->buy_value = pOut->sell_value = 0;
	INT32 needed_qty = pIn->trade_qty;

	osSQL.clear();
	osSQL.str("");
	osSQL << "SELECT hs_qty" << endl
		  << "FROM holding_summary" << endl
		  << "WHERE hs_ca_id = " << pIn->acct_id << endl
		  << "  AND hs_s_symb = '" << pOut->symbol << "'";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	int hs_qty = 0;

	if (PQntuples(res) != 0) {
		hs_qty = atoi(PQgetvalue(res, 0, 0));
	}
	PQclear(res);

	if (m_bVerbose) {
		cout << "hs_qty = " << hs_qty << endl;
	}

	if (pOut->type_is_sell == 1) {
		if (hs_qty > 0) {
			osSQL.clear();
			osSQL.str("");
			osSQL << "SELECT h_qty" << endl
				  << "     , h_price" << endl
				  << "FROM holding" << endl
				  << "WHERE h_ca_id = " << pIn->acct_id << endl
				  << "  AND h_s_symb = '" << pOut->symbol << "'" << endl;
			if (pIn->is_lifo) {
				osSQL << "ORDER BY h_dts DESC";
			} else {
				osSQL << "ORDER BY h_dts ASC";
			}
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res = exec(osSQL.str().c_str());

			INT32 hold_qty;
			double hold_price;
			int count = PQntuples(res);
			for (int i = 0; i < count && needed_qty != 0; i++) {
				hold_qty = atoll(PQgetvalue(res, i, 0));
				hold_price = atof(PQgetvalue(res, i, 1));
				if (hold_qty > needed_qty) {
					pOut->buy_value += (double) needed_qty * hold_price;
					pOut->sell_value
							+= (double) needed_qty * pOut->requested_price;
					needed_qty = 0;
				} else {
					pOut->buy_value += (double) hold_qty * hold_price;
					pOut->sell_value
							+= (double) hold_qty * pOut->requested_price;
					needed_qty -= hold_qty;
				}

				if (m_bVerbose) {
					cout << "hold_qty[" << i << "] = " << hold_qty << endl;
					cout << "hold_price[" << i << "] = " << hold_price << endl;
				}
			}
			PQclear(res);
		}
	} else {
		if (hs_qty < 0) {
			osSQL.clear();
			osSQL.str("");
			osSQL << "SELECT h_qty" << endl
				  << "     , h_price" << endl
				  << "FROM holding" << endl
				  << "WHERE h_ca_id = " << pIn->acct_id << endl
				  << "  AND h_s_symb = '" << pOut->symbol << "'" << endl;
			if (pIn->is_lifo) {
				osSQL << "ORDER BY h_dts DESC";
			} else {
				osSQL << "ORDER BY h_dts ASC";
			}
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res = exec(osSQL.str().c_str());

			INT32 hold_qty;
			double hold_price;
			int count = PQntuples(res);
			for (int i = 0; i < count && needed_qty != 0; i++) {
				hold_qty = atoll(PQgetvalue(res, i, 0));
				hold_price = atof(PQgetvalue(res, i, 1));
				if (hold_qty + needed_qty < 0) {
					pOut->sell_value += (double) needed_qty * hold_price;
					pOut->buy_value
							+= (double) needed_qty * pOut->requested_price;
					needed_qty = 0;
				} else {
					hold_qty *= -1;
					pOut->sell_value += (double) hold_qty * hold_price;
					pOut->buy_value
							+= (double) hold_qty * pOut->requested_price;
					needed_qty -= hold_qty;
				}

				if (m_bVerbose) {
					cout << "hold_qty[" << i << "] = " << hold_qty << endl;
					cout << "hold_price[" << i << "] = " << hold_price << endl;
				}
			}
			PQclear(res);
		}
	}

	if (m_bVerbose) {
		cout << "sell_value = " << pOut->sell_value << endl;
		cout << "buy_value = " << pOut->buy_value << endl;
	}

	pOut->tax_amount = 0;

	if ((pOut->sell_value > pOut->buy_value)
			&& ((pIn->tax_status == 1) || (pIn->tax_status == 2))) {
		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT sum(tx_rate)" << endl
			  << "FROM taxrate" << endl
			  << "WHERE tx_id in (" << endl
			  << "                   SELECT cx_tx_id" << endl
			  << "                   FROM customer_taxrate" << endl
			  << "                   WHERE cx_c_id = " << pIn->cust_id << endl
			  << "               )";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		if (PQntuples(res) == 0) {
			PQclear(res);
			return;
		}

		pOut->tax_amount = (pOut->sell_value - pOut->buy_value)
						   * atof(PQgetvalue(res, 0, 0));
		PQclear(res);
	}

	if (m_bVerbose) {
		cout << "tax_amount = " << pOut->tax_amount << endl;
	}

	osSQL.clear();
	osSQL.str("");
	osSQL << "SELECT cr_rate" << endl
		  << "FROM commission_rate" << endl
		  << "WHERE cr_c_tier = " << pIn->cust_tier << endl
		  << "  AND cr_tt_id = '" << pIn->trade_type_id << "'" << endl
		  << "  AND cr_ex_id = '" << ex_id << "'" << endl
		  << "  AND cr_from_qty <= " << pIn->trade_qty << endl
		  << "  AND cr_to_qty >= " << pIn->trade_qty;
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	if (PQntuples(res) == 0) {
		PQclear(res);
		return;
	}

	pOut->comm_rate = atof(PQgetvalue(res, 0, 0));
	PQclear(res);

	if (m_bVerbose) {
		cout << "comm_rate = " << pOut->comm_rate << endl;
	}

	osSQL.clear();
	osSQL.str("");
	osSQL << "SELECT ch_chrg" << endl
		  << "FROM charge" << endl
		  << "WHERE ch_c_tier = " << pIn->cust_tier << endl
		  << "  AND ch_tt_id = '" << pIn->trade_type_id << "'";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	if (PQntuples(res) == 0) {
		PQclear(res);
		return;
	}

	pOut->charge_amount = atof(PQgetvalue(res, 0, 0));
	PQclear(res);

	if (m_bVerbose) {
		cout << "charge_amount = " << pOut->charge_amount << endl;
	}

	pOut->acct_assets = 0;

	if (pIn->type_is_margin == 1) {
		double acct_bal;

		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT ca_bal" << endl
			  << "FROM customer_account" << endl
			  << "WHERE ca_id = " << pIn->acct_id;
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		if (PQntuples(res) == 0) {
			PQclear(res);
			return;
		}

		acct_bal = atof(PQgetvalue(res, 0, 0));
		PQclear(res);

		if (m_bVerbose) {
			cout << "acct_bal = " << acct_bal << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT sum(hs_qty * lt_price)" << endl
			  << "FROM holding_summary" << endl
			  << "   , last_trade" << endl
			  << "WHERE hs_ca_id = " << pIn->acct_id << endl
			  << "  AND lt_s_symb = hs_s_symb";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		if (PQntuples(res) == 0) {
			pOut->acct_assets = acct_bal;
		}

		pOut->acct_assets = atof(PQgetvalue(res, 0, 0)) + acct_bal;
		PQclear(res);

		if (m_bVerbose) {
			cout << "acct_assets = " << pOut->acct_assets << endl;
		}
	}

	if (pOut->type_is_market == 1) {
		strncpy(pOut->status_id, pIn->st_submitted_id, cST_ID_len);
	} else {
		strncpy(pOut->status_id, pIn->st_pending_id, cST_ID_len);
	}

	if (m_bVerbose) {
		cout << "status_id = " << pOut->status_id << endl;
	}
}

void
CDBConnectionClientSide::execute(
		const TTradeOrderFrame4Input *pIn, TTradeOrderFrame4Output *pOut)
{
	PGresult *res = NULL;
	PGresult *res2 = NULL;
	ostringstream osSQL;

	osSQL << "INSERT INTO trade(" << endl
		  << "    t_id" << endl
		  << "  , t_dts" << endl
		  << "  , t_st_id" << endl
		  << "  , t_tt_id" << endl
		  << "  , t_is_cash" << endl
		  << "  , t_s_symb" << endl
		  << "  , t_qty" << endl
		  << "  , t_bid_price" << endl
		  << "  , t_ca_id" << endl
		  << "  , t_exec_name" << endl
		  << "  , t_trade_price" << endl
		  << "  , t_chrg" << endl
		  << "  , t_comm" << endl
		  << "  , t_tax" << endl
		  << "  , t_lifo" << endl
		  << ")" << endl
		  << "VALUES (" << endl
		  << "    nextval('seq_trade_id')" << endl
		  << "  , CURRENT_TIMESTAMP" << endl
		  << "  , '" << pIn->status_id << "'" << endl
		  << "  , '" << pIn->trade_type_id << "'" << endl
		  << "  , " << (pIn->is_cash ? "true" : "false") << endl
		  << "  , '" << pIn->symbol << "'" << endl
		  << "  , " << pIn->trade_qty << endl
		  << "  , " << pIn->requested_price << endl
		  << "  , " << pIn->acct_id << endl
		  << "  , '" << pIn->exec_name << "'" << endl
		  << "  , NULL" << endl
		  << "  , " << pIn->charge_amount << endl
		  << "  , " << pIn->comm_amount << endl
		  << "  , 0" << endl
		  << "  , " << (pIn->is_lifo ? "true" : "false") << endl
		  << ")" << endl
		  << "RETURNING t_id" << endl
		  << "        , t_dts";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	pOut->trade_id = atoll(PQgetvalue(res, 0, 0));

	if (!pIn->type_is_market) {
		osSQL.clear();
		osSQL.str("");
		osSQL << "INSERT INTO trade_request(" << endl
			  << "    tr_t_id" << endl
			  << "  , tr_tt_id" << endl
			  << "  , tr_s_symb" << endl
			  << "  , tr_qty" << endl
			  << "  , tr_bid_price" << endl
			  << "  , tr_b_id" << endl
			  << ")" << endl
			  << "VALUES (" << endl
			  << "    " << pOut->trade_id << endl
			  << "  , '" << pIn->trade_type_id << "'" << endl
			  << "  , '" << pIn->symbol << "'" << endl
			  << "  , " << pIn->trade_qty << endl
			  << "  , " << pIn->requested_price << endl
			  << "  , " << pIn->broker_id << endl
			  << "  )";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res2 = exec(osSQL.str().c_str());
		PQclear(res2);
	}

	osSQL.clear();
	osSQL.str("");
	osSQL << "INSERT INTO trade_history(" << endl
		  << "    th_t_id" << endl
		  << "  , th_dts" << endl
		  << "  , th_st_id" << endl
		  << ")" << endl
		  << "VALUES(" << endl
		  << "    " << pOut->trade_id << endl
		  << "  , CURRENT_TIMESTAMP" << endl
		  << "  , '" << pIn->status_id << "'" << endl
		  << ")";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res2 = exec(osSQL.str().c_str());

	PQclear(res2);
	PQclear(res);
}

void
CDBConnectionClientSide::execute(
		const TTradeResultFrame1Input *pIn, TTradeResultFrame1Output *pOut)
{
	PGresult *res = NULL;
	ostringstream osSQL;

	osSQL << "SELECT t_ca_id" << endl
		  << "     , t_tt_id" << endl
		  << "     , t_s_symb" << endl
		  << "     , t_qty" << endl
		  << "     , t_chrg," << endl
		  << "       CASE WHEN t_lifo = true" << endl
		  << "            THEN 1" << endl
		  << "            ELSE 0 END" << endl
		  << "     , CASE WHEN t_is_cash = true" << endl
		  << "            THEN 1" << endl
		  << "            ELSE 0 END" << endl
		  << "FROM trade" << endl
		  << "WHERE t_id = " << pIn->trade_id;
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	if (PQntuples(res) == 0) {
		PQclear(res);
		return;
	}

	pOut->num_found = PQntuples(res);
	pOut->acct_id = atoll(PQgetvalue(res, 0, 0));
	strncpy(pOut->type_id, PQgetvalue(res, 0, 1), cTT_ID_len);
	strncpy(pOut->symbol, PQgetvalue(res, 0, 2), cSYMBOL_len);
	pOut->trade_qty = atoi(PQgetvalue(res, 0, 3));
	pOut->charge = atof(PQgetvalue(res, 0, 4));
	pOut->is_lifo = atoi(PQgetvalue(res, 0, 5));
	pOut->trade_is_cash = atoi(PQgetvalue(res, 0, 6));
	PQclear(res);

	if (m_bVerbose) {
		cout << "num_found = " << pOut->num_found << endl;
		cout << "acct_id = " << pOut->acct_id << endl;
		cout << "type_id = " << pOut->type_id << endl;
		cout << "symbol = " << pOut->symbol << endl;
		cout << "trade_qty = " << pOut->trade_qty << endl;
		cout << "charge = " << pOut->charge << endl;
		cout << "is_lifo = " << pOut->is_lifo << endl;
		cout << "trade_is_cash = " << pOut->trade_is_cash << endl;
	}

	osSQL.clear();
	osSQL.str("");
	osSQL << "SELECT tt_name" << endl
		  << "     , CASE WHEN tt_is_sell IS TRUE" << endl
		  << "            THEN 1" << endl
		  << "            ELSE 0 END" << endl
		  << "     , CASE WHEN tt_is_mrkt IS TRUE" << endl
		  << "            THEN 1" << endl
		  << "            ELSE 0 END" << endl
		  << "FROM trade_type" << endl
		  << "WHERE tt_id = '" << pOut->type_id << "'";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	if (PQntuples(res) == 0) {
		PQclear(res);
		return;
	}

	strncpy(pOut->type_name, PQgetvalue(res, 0, 0), cTT_NAME_len);
	pOut->type_is_sell = atoi(PQgetvalue(res, 0, 1));
	pOut->type_is_market = atoi(PQgetvalue(res, 0, 2));
	PQclear(res);

	if (m_bVerbose) {
		cout << "type_name = " << pOut->type_name << endl;
		cout << "type_is_sell = " << pOut->type_is_sell << endl;
		cout << "type_is_market = " << pOut->type_is_market << endl;
	}

	osSQL.clear();
	osSQL.str("");
	osSQL << "SELECT hs_qty" << endl
		  << "FROM holding_summary" << endl
		  << "WHERE hs_ca_id = " << pOut->acct_id << endl
		  << "  AND hs_s_symb = '" << pOut->symbol << "'";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	if (PQntuples(res) == 0) {
		PQclear(res);
		return;
	}

	pOut->hs_qty = atoi(PQgetvalue(res, 0, 0));
	PQclear(res);

	if (m_bVerbose) {
		cout << "hs_qty = " << pOut->hs_qty << endl;
	}
}

void
CDBConnectionClientSide::execute(
		const TTradeResultFrame2Input *pIn, TTradeResultFrame2Output *pOut)
{
	PGresult *res = NULL;
	ostringstream osSQL;

	pOut->buy_value = pOut->sell_value = 0;
	INT32 needed_qty = pIn->trade_qty;

	res = exec("SELECT CURRENT_TIMESTAMP");
	sscanf(PQgetvalue(res, 0, 0), "%hd-%hd-%hd %hd:%hd:%hd.%d",
			&pOut->trade_dts.year, &pOut->trade_dts.month,
			&pOut->trade_dts.day, &pOut->trade_dts.hour,
			&pOut->trade_dts.minute, &pOut->trade_dts.second,
			&pOut->trade_dts.fraction);
	PQclear(res);

	osSQL << "SELECT ca_b_id" << endl
		  << "     , ca_c_id" << endl
		  << "     , ca_tax_st" << endl
		  << "FROM customer_account" << endl
		  << "WHERE ca_id = " << pIn->acct_id << endl
		  << "FOR UPDATE";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	if (PQntuples(res) == 0) {
		PQclear(res);
		return;
	}

	pOut->broker_id = atoll(PQgetvalue(res, 0, 0));
	pOut->cust_id = atoll(PQgetvalue(res, 0, 1));
	pOut->tax_status = atoi(PQgetvalue(res, 0, 2));
	PQclear(res);

	if (m_bVerbose) {
		cout << "broker_id = " << pOut->broker_id << endl;
		cout << "cust_id = " << pOut->cust_id << endl;
		cout << "tax_status = " << pOut->tax_status << endl;
	}

	if (pIn->type_is_sell) {
		if (pIn->hs_qty == 0) {
			osSQL.clear();
			osSQL.str("");
			osSQL << "INSERT INTO holding_summary(" << endl
				  << "    hs_ca_id" << endl
				  << "  , hs_s_symb" << endl
				  << "  , hs_qty" << endl
				  << ")" << endl
				  << "VALUES(" << endl
				  << "    " << pIn->acct_id << endl
				  << "  , '" << pIn->symbol << "'" << endl
				  << "  , -" << pIn->trade_qty << endl
				  << ")";
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res = exec(osSQL.str().c_str());
			PQclear(res);
		} else if (pIn->hs_qty != pIn->trade_qty) {
			osSQL.clear();
			osSQL.str("");
			osSQL << "UPDATE holding_summary" << endl
				  << "SET hs_qty = " << pIn->hs_qty << " - " << pIn->trade_qty
				  << endl
				  << "WHERE hs_ca_id = " << pIn->acct_id << endl
				  << "  AND hs_s_symb = '" << pIn->symbol << "'";
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res = exec(osSQL.str().c_str());
			PQclear(res);
		} else if (pIn->hs_qty > 0) {
			osSQL.clear();
			osSQL.str("");
			osSQL << "SELECT h_t_id" << endl
				  << "     , h_qty" << endl
				  << "     , h_price" << endl
				  << "FROM holding" << endl
				  << "WHERE h_ca_id = " << pIn->acct_id << endl
				  << "  AND h_s_symb = '" << pIn->symbol << "'" << endl;
			if (pIn->is_lifo) {
				osSQL << "ORDER BY h_dts DESC" << endl;
			} else {
				osSQL << "ORDER BY h_dts ASC" << endl;
			}
			osSQL << "FOR UPDATE";
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res = exec(osSQL.str().c_str());

			int count = PQntuples(res);
			PGresult *res2 = NULL;
			for (int i = 0; i < count; i++) {
				if (needed_qty == 0)
					break;

				char *hold_id = PQgetvalue(res, i, 0);
				INT32 hold_qty = atoi(PQgetvalue(res, i, 1));
				double hold_price = atof(PQgetvalue(res, i, 2));

				if (m_bVerbose) {
					cout << "hold_id[" << i << "] = " << hold_id << endl;
					cout << "hold_qty[" << i << "] = " << hold_qty << endl;
					cout << "hold_price[" << i << "] = " << hold_price << endl;
				}

				if (hold_qty > needed_qty) {
					osSQL.clear();
					osSQL.str("");
					osSQL << "INSERT INTO holding_history(" << endl
						  << "    hh_h_t_id" << endl
						  << "  , hh_t_id" << endl
						  << "  , hh_before_qty" << endl
						  << "  , hh_after_qty" << endl
						  << ")" << endl
						  << "VALUES(" << endl
						  << "    " << hold_id << endl
						  << "  , " << pIn->trade_id << endl
						  << "  , " << hold_qty << endl
						  << "  , " << hold_qty - needed_qty << endl
						  << ")";
					if (m_bVerbose) {
						cout << osSQL.str() << endl;
					}
					res2 = exec(osSQL.str().c_str());
					PQclear(res2);

					osSQL.clear();
					osSQL.str("");
					osSQL << "UPDATE holding" << endl
						  << "SET h_qty = " << hold_qty - needed_qty << endl
						  << "WHERE h_t_id = " << hold_id;
					if (m_bVerbose) {
						cout << osSQL.str() << endl;
					}
					res2 = exec(osSQL.str().c_str());
					PQclear(res2);

					pOut->buy_value += (double) needed_qty * hold_price;
					pOut->sell_value += (double) needed_qty * pIn->trade_price;
					needed_qty = 0;
				} else {
					osSQL.clear();
					osSQL.str("");
					osSQL << "INSERT INTO holding_history(" << endl
						  << "    hh_h_t_id" << endl
						  << "  , hh_t_id" << endl
						  << "  , hh_before_qty" << endl
						  << "  , hh_after_qty" << endl
						  << ")" << endl
						  << "VALUES(" << endl
						  << "    " << hold_id << endl
						  << "  , " << pIn->trade_id << endl
						  << "  , " << hold_qty << endl
						  << "  , 0" << endl
						  << ")";
					if (m_bVerbose) {
						cout << osSQL.str() << endl;
					}
					res2 = exec(osSQL.str().c_str());
					PQclear(res2);

					osSQL.clear();
					osSQL.str("");
					osSQL << "DELETE FROM holding" << endl
						  << "WHERE h_t_id = " << hold_id;
					res2 = exec(osSQL.str().c_str());
					PQclear(res2);

					pOut->buy_value += (double) hold_qty * hold_price;
					pOut->sell_value += (double) hold_qty * pIn->trade_price;
					needed_qty -= hold_qty;
				}
			}
			PQclear(res);
		}

		if (needed_qty > 0) {
			PGresult *res2 = NULL;

			osSQL.clear();
			osSQL.str("");
			osSQL << "INSERT INTO holding_history(" << endl
				  << "    hh_h_t_id" << endl
				  << "  , hh_t_id" << endl
				  << "  , hh_before_qty" << endl
				  << "  , hh_after_qty" << endl
				  << ")" << endl
				  << "VALUES(" << endl
				  << "    " << pIn->trade_id << endl
				  << "  , " << pIn->trade_id << endl
				  << "  , 0" << endl
				  << "  , -1 * " << needed_qty << endl
				  << ")";
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res2 = exec(osSQL.str().c_str());
			PQclear(res2);

			osSQL.clear();
			osSQL.str("");
			osSQL << "INSERT INTO holding(" << endl
				  << "    h_t_id" << endl
				  << "  , h_ca_id" << endl
				  << "  , h_s_symb" << endl
				  << "  , h_dts" << endl
				  << "  , h_price" << endl
				  << "  , h_qty)" << endl
				  << "VALUES (" << endl
				  << "    " << pIn->trade_id << endl
				  << "  , " << pIn->acct_id << endl
				  << "  , '" << pIn->symbol << "'" << endl
				  << "  , CURRENT_TIMESTAMP" << endl
				  << "  , " << pIn->trade_price << endl
				  << "  , -1 * " << needed_qty << endl
				  << ")";
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res2 = exec(osSQL.str().c_str());
			PQclear(res2);
		} else if (pIn->hs_qty == pIn->trade_qty) {
			osSQL.clear();
			osSQL.str("");
			osSQL << "DELETE FROM holding_summary" << endl
				  << "WHERE hs_ca_id = " << pIn->acct_id << endl
				  << "  AND hs_s_symb = '" << pIn->symbol << "'";
			PGresult *res2 = exec(osSQL.str().c_str());
			PQclear(res2);
		}
	} else {
		PGresult *res2 = NULL;
		if (pIn->hs_qty == 0) {
			osSQL.clear();
			osSQL.str("");
			osSQL << "INSERT INTO holding_summary(" << endl
				  << "    hs_ca_id" << endl
				  << "  , hs_s_symb" << endl
				  << "  , hs_qty" << endl
				  << ")" << endl
				  << "VALUES (" << endl
				  << "    " << pIn->acct_id << endl
				  << "  , '" << pIn->symbol << "'" << endl
				  << "  , " << pIn->trade_qty << endl
				  << ")";
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res2 = exec(osSQL.str().c_str());
			PQclear(res2);
		} else if ((-1 * pIn->hs_qty) != pIn->trade_qty) {
			osSQL.clear();
			osSQL.str("");
			osSQL << "UPDATE holding_summary" << endl
				  << "SET hs_qty = " << pIn->hs_qty + pIn->trade_qty << endl
				  << "WHERE hs_ca_id = " << pIn->acct_id << endl
				  << "  AND hs_s_symb = '" << pIn->symbol << "'";
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res2 = exec(osSQL.str().c_str());
			PQclear(res2);
		}

		if (pIn->hs_qty < 0) {
			osSQL.clear();
			osSQL.str("");
			osSQL << "SELECT h_t_id" << endl
				  << "     , h_qty" << endl
				  << "     , h_price" << endl
				  << "FROM holding" << endl
				  << "WHERE h_ca_id = " << pIn->acct_id << endl
				  << "  AND h_s_symb = '" << pIn->symbol << "'" << endl;
			if (pIn->is_lifo) {
				osSQL << "ORDER BY h_dts DESC" << endl;
			} else {
				osSQL << "ORDER BY h_dts ASC" << endl;
			}
			osSQL << "FOR UPDATE";
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res = exec(osSQL.str().c_str());

			int count = PQntuples(res);
			PGresult *res2 = NULL;
			for (int i = 0; i < count; i++) {
				if (needed_qty == 0)
					break;

				char *hold_id = PQgetvalue(res, 0, 0);
				INT32 hold_qty = atoi(PQgetvalue(res, 0, 1));
				double hold_price = atof(PQgetvalue(res, 0, 2));

				if (m_bVerbose) {
					cout << "hold_id[" << i << "] = " << hold_id << endl;
					cout << "hold_qty[" << i << "] = " << hold_qty << endl;
					cout << "hold_price[" << i << "] = " << hold_price << endl;
				}

				if (hold_qty + needed_qty < 0) {
					osSQL.clear();
					osSQL.str("");
					osSQL << "INSERT INTO holding_history(" << endl
						  << "    hh_h_t_id" << endl
						  << "  , hh_t_id" << endl
						  << "  , hh_before_qty" << endl
						  << "  , hh_after_qty" << endl
						  << ")" << endl
						  << "VALUES(" << endl
						  << "    " << hold_id << endl
						  << "  , " << pIn->trade_id << endl
						  << "  , " << hold_qty << endl
						  << "  , " << hold_qty + needed_qty << endl
						  << ")";
					if (m_bVerbose) {
						cout << osSQL.str() << endl;
					}
					res2 = exec(osSQL.str().c_str());
					PQclear(res2);

					osSQL.clear();
					osSQL.str("");
					osSQL << "UPDATE holding" << endl
						  << "SET h_qty = " << hold_qty + needed_qty << endl
						  << "WHERE h_t_id = " << hold_id;
					if (m_bVerbose) {
						cout << osSQL.str() << endl;
					}
					res2 = exec(osSQL.str().c_str());
					PQclear(res2);

					pOut->sell_value += (double) needed_qty * hold_price;
					pOut->buy_value += (double) needed_qty * pIn->trade_price;
					needed_qty = 0;
				} else {
					osSQL.clear();
					osSQL.str("");
					osSQL << "INSERT INTO holding_history(" << endl
						  << "    hh_h_t_id" << endl
						  << "  , hh_t_id" << endl
						  << "  , hh_before_qty" << endl
						  << "  , hh_after_qty" << endl
						  << ")" << endl
						  << "VALUES(" << endl
						  << "    " << hold_id << endl
						  << "  , " << pIn->trade_id << endl
						  << "  , " << hold_qty << endl
						  << "  , 0" << endl
						  << ")";
					if (m_bVerbose) {
						cout << osSQL.str() << endl;
					}
					res2 = exec(osSQL.str().c_str());
					PQclear(res2);

					osSQL.clear();
					osSQL.str("");
					osSQL << "DELETE FROM holding" << endl
						  << "WHERE h_t_id = " << hold_id;
					if (m_bVerbose) {
						cout << osSQL.str() << endl;
					}
					res2 = exec(osSQL.str().c_str());
					PQclear(res2);

					hold_qty *= -1;
					pOut->sell_value += (double) hold_qty * hold_price;
					pOut->buy_value += (double) hold_qty * pIn->trade_price;
					needed_qty -= hold_qty;
				}
			}
			PQclear(res);
		}

		if (needed_qty > 0) {
			osSQL.clear();
			osSQL.str("");
			osSQL << "INSERT INTO holding_history(" << endl
				  << "    hh_h_t_id" << endl
				  << "  , hh_t_id" << endl
				  << "  , hh_before_qty" << endl
				  << "  , hh_after_qty" << endl
				  << ")" << endl
				  << "VALUES(" << endl
				  << "    " << pIn->trade_id << endl
				  << "  , " << pIn->trade_id << endl
				  << "  , 0" << endl
				  << "  , " << needed_qty << endl
				  << ")";
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res2 = exec(osSQL.str().c_str());
			PQclear(res2);

			osSQL.clear();
			osSQL.str("");
			osSQL << "INSERT INTO holding(" << endl
				  << "    h_t_id" << endl
				  << "  , h_ca_id" << endl
				  << "  , h_s_symb" << endl
				  << "  , h_dts" << endl
				  << "  , h_price" << endl
				  << "  , h_qty)" << endl
				  << "VALUES (" << endl
				  << "    " << pIn->trade_id << endl
				  << "  , " << pIn->acct_id << endl
				  << "  , '" << pIn->symbol << "'" << endl
				  << "  , CURRENT_TIMESTAMP" << endl
				  << "  , " << pIn->trade_price << endl
				  << "  , " << needed_qty << endl
				  << ")";
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res2 = exec(osSQL.str().c_str());
			PQclear(res2);
		} else if ((-1 * pIn->hs_qty) == pIn->trade_qty) {
			osSQL.clear();
			osSQL.str("");
			osSQL << "DELETE FROM holding_summary" << endl
				  << "WHERE hs_ca_id = " << pIn->acct_id << endl
				  << "hs_s_symb = '" << pIn->symbol << "'";
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res2 = exec(osSQL.str().c_str());
			PQclear(res2);
		}
	}
}

void
CDBConnectionClientSide::execute(
		const TTradeResultFrame3Input *pIn, TTradeResultFrame3Output *pOut)
{
	PGresult *res = NULL;
	ostringstream osSQL;

	osSQL << "SELECT sum(tx_rate)" << endl
		  << "FROM taxrate" << endl
		  << "WHERE tx_id IN (" << endl
		  << "                   SELECT cx_tx_id" << endl
		  << "                   FROM customer_taxrate" << endl
		  << "                   WHERE cx_c_id = " << pIn->cust_id << endl
		  << "               )";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	if (PQntuples(res) == 0) {
		PQclear(res);
		return;
	}

	if (m_bVerbose) {
		cout << "sum = " << PQgetvalue(res, 0, 0) << endl;
	}

	pOut->tax_amount
			= (pIn->sell_value - pIn->buy_value) * atof(PQgetvalue(res, 0, 0));
	PQclear(res);

	osSQL.clear();
	osSQL.str("");
	osSQL << "UPDATE trade" << endl
		  << "SET t_tax = " << pOut->tax_amount << endl
		  << "WHERE t_id = " << pIn->trade_id;
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());
	PQclear(res);
}

void
CDBConnectionClientSide::execute(
		const TTradeResultFrame4Input *pIn, TTradeResultFrame4Output *pOut)
{
	PGresult *res = NULL;
	PGresult *res2 = NULL;
	ostringstream osSQL;

	osSQL << "SELECT s_ex_id" << endl
		  << "     , s_name" << endl
		  << "FROM security" << endl
		  << "WHERE s_symb = '" << pIn->symbol << "'";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	if (PQntuples(res) == 0) {
		PQclear(res);
		return;
	}

	strncpy(pOut->s_name, PQgetvalue(res, 0, 1), cS_NAME_len);

	if (m_bVerbose) {
		cout << "ex_id = " << PQgetvalue(res, 0, 0) << endl;
		;
		cout << "s_name = " << pOut->s_name << endl;
	}

	osSQL.clear();
	osSQL.str("");
	osSQL << "SELECT c_tier" << endl
		  << "FROM customer" << endl
		  << "WHERE c_id = " << pIn->cust_id;
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res2 = exec(osSQL.str().c_str());

	if (PQntuples(res2) == 0) {
		PQclear(res2);
		return;
	}

	osSQL.clear();
	osSQL.str("");
	osSQL << "SELECT cr_rate" << endl
		  << "FROM commission_rate" << endl
		  << "WHERE cr_c_tier = '" << PQgetvalue(res2, 0, 0) << "'" << endl
		  << "  AND cr_tt_id = '" << pIn->type_id << "'" << endl
		  << "  AND cr_ex_id = '" << PQgetvalue(res, 0, 0) << "'" << endl
		  << "  AND cr_from_qty <= " << pIn->trade_qty << endl
		  << "  AND cr_to_qty >= " << pIn->trade_qty << endl
		  << "LIMIT 1";
	PQclear(res2);
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res2 = exec(osSQL.str().c_str());

	if (PQntuples(res2) == 0) {
		PQclear(res2);
		return;
	}

	pOut->comm_rate = atof(PQgetvalue(res2, 0, 0));
	PQclear(res2);
	PQclear(res);

	if (m_bVerbose) {
		cout << "comm_rate = " << pOut->comm_rate << endl;
	}
}

void
CDBConnectionClientSide::execute(const TTradeResultFrame5Input *pIn)
{
	PGresult *res = NULL;
	ostringstream osSQL;

	osSQL << "UPDATE trade" << endl
		  << "SET t_comm = " << pIn->comm_amount << endl
		  << "  , t_dts = '" << pIn->trade_dts.year << "-"
		  << pIn->trade_dts.month << "-" << pIn->trade_dts.day << " "
		  << pIn->trade_dts.hour << ":" << pIn->trade_dts.minute << ":"
		  << pIn->trade_dts.second << "." << pIn->trade_dts.fraction << "'"
		  << endl
		  << "  , t_st_id = '" << pIn->st_completed_id << "'" << endl
		  << "  , t_trade_price = " << pIn->trade_price << endl
		  << "WHERE t_id = " << pIn->trade_id;
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());
	PQclear(res);

	osSQL.clear();
	osSQL.str("");
	osSQL << "INSERT INTO trade_history(" << endl
		  << "    th_t_id" << endl
		  << "  , th_dts" << endl
		  << "  , th_st_id" << endl
		  << ")" << endl
		  << "VALUES (" << endl
		  << "    " << pIn->trade_id << endl
		  << "  , '" << pIn->trade_dts.year << "-" << pIn->trade_dts.month
		  << "-" << pIn->trade_dts.day << " " << pIn->trade_dts.hour << ":"
		  << pIn->trade_dts.minute << ":" << pIn->trade_dts.second << "."
		  << pIn->trade_dts.fraction << "'" << endl
		  << "  , '" << pIn->st_completed_id << "'" << endl
		  << ")";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());
	PQclear(res);

	osSQL.clear();
	osSQL.str("");
	osSQL << "UPDATE broker" << endl
		  << "SET b_comm_total = b_comm_total + " << pIn->comm_amount << endl
		  << "  , b_num_trades = b_num_trades + 1" << endl
		  << "WHERE b_id = " << pIn->broker_id;
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());
	PQclear(res);
}

void
CDBConnectionClientSide::execute(
		const TTradeResultFrame6Input *pIn, TTradeResultFrame6Output *pOut)
{
	PGresult *res = NULL;
	ostringstream osSQL;

	osSQL << "INSERT INTO settlement(" << endl
		  << "    se_t_id" << endl
		  << "  , se_cash_type" << endl
		  << "  , se_cash_due_date" << endl
		  << "  , se_amt)" << endl
		  << "VALUES (" << endl
		  << "    " << pIn->trade_id << endl
		  << "  , '";
	if (pIn->trade_is_cash) {
		osSQL << "Cash Account" << endl;
	} else {
		osSQL << "Margin" << endl;
	}
	osSQL << "'" << endl
		  << "  , '" << pIn->due_date.year << "-" << pIn->due_date.month << "-"
		  << pIn->due_date.day << "'"
		  << "  , " << pIn->se_amount << endl
		  << ")";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());
	PQclear(res);

	osSQL.clear();
	osSQL.str("");
	osSQL << "UPDATE customer_account" << endl
		  << "SET ca_bal = ca_bal + " << pIn->se_amount << endl
		  << "WHERE ca_id = " << pIn->acct_id;
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());
	PQclear(res);

	ostringstream os_ct_name;
	os_ct_name << pIn->type_name << " " << pIn->trade_qty << " shares of "
			   << pIn->s_name;
	char *ct_name = escape(os_ct_name.str());
	osSQL.clear();
	osSQL.str("");
	osSQL << "INSERT INTO cash_transaction(" << endl
		  << "    ct_dts" << endl
		  << "  , ct_t_id" << endl
		  << "  , ct_amt" << endl
		  << "  , ct_name" << endl
		  << ")" << endl
		  << "VALUES (" << endl
		  << "    '" << pIn->trade_dts.year << "-" << pIn->trade_dts.month
		  << "-" << pIn->trade_dts.day << " " << pIn->trade_dts.hour << ":"
		  << pIn->trade_dts.minute << ":" << pIn->trade_dts.second << "."
		  << pIn->trade_dts.fraction << "'" << endl
		  << "  , " << pIn->trade_id << endl
		  << "  , " << pIn->se_amount << endl
		  << "  , e" << ct_name << endl
		  << ")";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());
	PQclear(res);
	PQfreemem(ct_name);

	osSQL.clear();
	osSQL.str("");
	osSQL << "SELECT ca_bal" << endl
		  << "FROM customer_account" << endl
		  << "WHERE ca_id = " << pIn->acct_id;
	res = exec(osSQL.str().c_str());

	if (PQntuples(res) == 0) {
		PQclear(res);
		return;
	}

	pOut->acct_bal = atof(PQgetvalue(res, 0, 0));
	PQclear(res);

	if (m_bVerbose) {
		cout << "acct_bal = " << pOut->acct_bal << endl;
	}
}

void
CDBConnectionClientSide::execute(
		const TTradeStatusFrame1Input *pIn, TTradeStatusFrame1Output *pOut)
{
	PGresult *res = NULL;
	ostringstream osSQL;

	osSQL << "SELECT t_id" << endl
		  << "     , t_dts" << endl
		  << "     , st_name" << endl
		  << "     , tt_name" << endl
		  << "     , t_s_symb" << endl
		  << "     , t_qty" << endl
		  << "     , t_exec_name" << endl
		  << "     , t_chrg" << endl
		  << "     , s_name" << endl
		  << "     , ex_name" << endl
		  << "FROM trade" << endl
		  << "   , status_type" << endl
		  << "   , trade_type" << endl
		  << "   , security" << endl
		  << "   , exchange" << endl
		  << "WHERE t_ca_id = " << pIn->acct_id << endl
		  << "  AND st_id = t_st_id" << endl
		  << "  AND tt_id = t_tt_id" << endl
		  << "  AND s_symb = t_s_symb" << endl
		  << "  AND ex_id = s_ex_id" << endl
		  << "ORDER BY t_dts DESC" << endl
		  << "LIMIT 50";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	if (PQntuples(res) == 0) {
		PQclear(res);
		return;
	}

	pOut->num_found = PQntuples(res);
	for (int i = 0; i < pOut->num_found; i++) {
		pOut->trade_id[i] = atoll(PQgetvalue(res, i, 0));
		sscanf(PQgetvalue(res, i, 1), "%hd-%hd-%hd %hd:%hd:%hd.%d",
				&pOut->trade_dts[i].year, &pOut->trade_dts[i].month,
				&pOut->trade_dts[i].day, &pOut->trade_dts[i].hour,
				&pOut->trade_dts[i].minute, &pOut->trade_dts[i].second,
				&pOut->trade_dts[i].fraction);
		strncpy(pOut->status_name[i], PQgetvalue(res, i, 2), cST_NAME_len);
		strncpy(pOut->type_name[i], PQgetvalue(res, i, 3), cTT_NAME_len);
		strncpy(pOut->symbol[i], PQgetvalue(res, i, 4), cSYMBOL_len);
		pOut->trade_qty[i] = atoll(PQgetvalue(res, i, 5));
		strncpy(pOut->exec_name[i], PQgetvalue(res, i, 6), cEXEC_NAME_len);
		pOut->charge[i] = atof(PQgetvalue(res, i, 7));
		strncpy(pOut->s_name[i], PQgetvalue(res, i, 8), cS_NAME_len);
		strncpy(pOut->ex_name[i], PQgetvalue(res, i, 9), cEX_NAME_len);
	}
	PQclear(res);

	if (m_bVerbose) {
		for (int i = 0; i < pOut->num_found; i++) {
			cout << "trade_id[" << i << "] = " << pOut->trade_id[i] << endl;
			cout << "trade_dts[" << i << "] = " << pOut->trade_dts[i].year
				 << "-" << pOut->trade_dts[i].month << "-"
				 << pOut->trade_dts[i].day << " " << pOut->trade_dts[i].hour
				 << ":" << pOut->trade_dts[i].minute << ":"
				 << pOut->trade_dts[i].second << "."
				 << pOut->trade_dts[i].fraction << endl;
			cout << "status_name[" << i << "] = " << pOut->status_name[i]
				 << endl;
			cout << "type_name[" << i << "] = " << pOut->type_name[i] << endl;
			cout << "symbol[" << i << "] = " << pOut->symbol[i] << endl;
			cout << "trade_qty[" << i << "] = " << pOut->trade_qty[i] << endl;
			cout << "exec_name[" << i << "] = " << pOut->exec_name[i] << endl;
			cout << "charge[" << i << "] = " << pOut->charge[i] << endl;
			cout << "s_name[" << i << "] = " << pOut->s_name[i] << endl;
			cout << "ex_name[" << i << "] = " << pOut->ex_name[i] << endl;
		}
	}

	osSQL.clear();
	osSQL.str("");
	osSQL << "SELECT c_l_name" << endl
		  << "     , c_f_name" << endl
		  << "     , b_name" << endl
		  << "FROM customer_account" << endl
		  << "   , customer" << endl
		  << "   , broker" << endl
		  << "WHERE ca_id = " << pIn->acct_id << endl
		  << "  AND c_id = ca_c_id" << endl
		  << "  AND b_id = ca_b_id";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	if (PQntuples(res) == 0) {
		PQclear(res);
		return;
	}

	strncpy(pOut->cust_l_name, PQgetvalue(res, 0, 0), cL_NAME_len);
	strncpy(pOut->cust_f_name, PQgetvalue(res, 0, 1), cF_NAME_len);
	strncpy(pOut->broker_name, PQgetvalue(res, 0, 2), cB_NAME_len);
	PQclear(res);

	if (m_bVerbose) {
		cout << "cust_l_name = " << pOut->cust_l_name << endl;
		cout << "cust_f_name = " << pOut->cust_f_name << endl;
		cout << "broker_name = " << pOut->broker_name << endl;
	}
}

void
CDBConnectionClientSide::execute(
		const TTradeUpdateFrame1Input *pIn, TTradeUpdateFrame1Output *pOut)
{
	PGresult *res = NULL;

	pOut->num_found = pOut->num_updated = 0;

	for (int i = 0; i < pIn->max_trades; i++) {
		ostringstream osSQL;

		if (pOut->num_updated < pIn->max_updates) {
			osSQL << "SELECT t_exec_name" << endl
				  << "FROM trade" << endl
				  << "WHERE t_id = " << pIn->trade_id[i];
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res = exec(osSQL.str().c_str());

			if (PQntuples(res) == 0) {
				PQclear(res);
				return;
			}

			pOut->num_found += PQntuples(res);
			char *ex_name = PQgetvalue(res, 0, 0);
			PQclear(res);

			if (m_bVerbose) {
				cout << "ex_name = " << ex_name << endl;
			}

			osSQL.clear();
			osSQL.str("");
			osSQL << "UPDATE trade" << endl;
			if (strstr(ex_name, " X ")) {
				osSQL << "SET t_exec_name = replace(t_exec_name, ' X ', ' ')"
					  << endl;
			} else {
				osSQL << "SET t_exec_name = replace(t_exec_name, ' ', ' X ')"
					  << endl;
			}
			osSQL << "WHERE t_id = " << pIn->trade_id[i];
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res = exec(osSQL.str().c_str());

			if (m_bVerbose) {
				cout << "PQcmdTuples = " << PQcmdTuples(res) << endl;
			}
			pOut->num_updated += atoi(PQcmdTuples(res));
			PQclear(res);
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT t_bid_price" << endl
			  << "     , t_exec_name" << endl
			  << "     , t_is_cash" << endl
			  << "     , tt_is_mrkt" << endl
			  << "     , t_trade_price" << endl
			  << "FROM trade" << endl
			  << "   , trade_type" << endl
			  << "WHERE t_id = " << pIn->trade_id[i] << endl
			  << "  AND t_tt_id = tt_id";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		if (PQntuples(res) == 0) {
			PQclear(res);
			return;
		}

		pOut->trade_info[i].bid_price = atof(PQgetvalue(res, 0, 0));
		strncpy(pOut->trade_info[i].exec_name, PQgetvalue(res, 0, 1),
				cEXEC_NAME_len);
		if (PQgetvalue(res, 0, 2)[0] == 't') {
			pOut->trade_info[i].is_cash = true;
		} else {
			pOut->trade_info[i].is_cash = false;
		}
		if (PQgetvalue(res, 0, 3)[0] == 't') {
			pOut->trade_info[i].is_market = true;
		} else {
			pOut->trade_info[i].is_market = false;
		}
		pOut->trade_info[i].trade_price = atof(PQgetvalue(res, 0, 4));
		PQclear(res);

		if (m_bVerbose) {
			cout << "bid_price[" << i
				 << "] = " << pOut->trade_info[i].bid_price << endl;
			cout << "exec_name[" << i
				 << "] = " << pOut->trade_info[i].exec_name << endl;
			cout << "is_cash[" << i << "] = " << pOut->trade_info[i].is_cash
				 << endl;
			cout << "is_market[" << i
				 << "] = " << pOut->trade_info[i].is_market << endl;
			cout << "trade_price[" << i
				 << "] = " << pOut->trade_info[i].trade_price << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT se_amt" << endl
			  << "     , se_cash_due_date" << endl
			  << "     , se_cash_type" << endl
			  << "FROM settlement" << endl
			  << "WHERE se_t_id = " << pIn->trade_id[i];
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		if (PQntuples(res) == 0) {
			PQclear(res);
			return;
		}

		pOut->trade_info[i].settlement_amount = atof(PQgetvalue(res, 0, 0));
		sscanf(PQgetvalue(res, 0, 1), "%hd-%hd-%hd",
				&pOut->trade_info[i].settlement_cash_due_date.year,
				&pOut->trade_info[i].settlement_cash_due_date.month,
				&pOut->trade_info[i].settlement_cash_due_date.day);
		strncpy(pOut->trade_info[i].settlement_cash_type,
				PQgetvalue(res, 0, 2), cSE_CASH_TYPE_len);
		PQclear(res);

		if (m_bVerbose) {
			cout << "settlement_amount[" << i
				 << "] = " << pOut->trade_info[i].settlement_amount << endl;
			cout << "settlement_cash_due_date[" << i
				 << "] = " << pOut->trade_info[i].settlement_cash_due_date.year
				 << "-" << pOut->trade_info[i].settlement_cash_due_date.month
				 << "-" << pOut->trade_info[i].settlement_cash_due_date.day
				 << endl;
			cout << "settlement_cash_type[" << i
				 << "] = " << pOut->trade_info[i].settlement_cash_type << endl;
		}

		if (pOut->trade_info[i].is_cash) {
			osSQL.clear();
			osSQL.str("");
			osSQL << "SELECT ct_amt" << endl
				  << "     , ct_dts" << endl
				  << "     , ct_name" << endl
				  << "FROM cash_transaction" << endl
				  << "WHERE ct_t_id = " << pIn->trade_id[i];
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res = exec(osSQL.str().c_str());

			if (PQntuples(res) == 0) {
				PQclear(res);
				return;
			}

			pOut->trade_info[i].cash_transaction_amount
					= atof(PQgetvalue(res, 0, 0));
			sscanf(PQgetvalue(res, 0, 1), "%hd-%hd-%hd %hd:%hd:%hd.%d",
					&pOut->trade_info[i].cash_transaction_dts.year,
					&pOut->trade_info[i].cash_transaction_dts.month,
					&pOut->trade_info[i].cash_transaction_dts.day,
					&pOut->trade_info[i].cash_transaction_dts.hour,
					&pOut->trade_info[i].cash_transaction_dts.minute,
					&pOut->trade_info[i].cash_transaction_dts.second,
					&pOut->trade_info[i].cash_transaction_dts.fraction);
			strncpy(pOut->trade_info[i].cash_transaction_name,
					PQgetvalue(res, 0, 2), cCT_NAME_len);
			PQclear(res);
		}

		if (m_bVerbose) {
			cout << "cash_transaction_amount[" << i
				 << "] = " << pOut->trade_info[i].cash_transaction_amount
				 << endl;
			cout << "cash_transaction_dts[" << i
				 << "] = " << pOut->trade_info[i].cash_transaction_dts.year
				 << "-" << pOut->trade_info[i].cash_transaction_dts.month
				 << "-" << pOut->trade_info[i].cash_transaction_dts.day << " "
				 << pOut->trade_info[i].cash_transaction_dts.hour << ":"
				 << pOut->trade_info[i].cash_transaction_dts.minute << ":"
				 << pOut->trade_info[i].cash_transaction_dts.second << "."
				 << pOut->trade_info[i].cash_transaction_dts.fraction << endl;
			cout << "cash_transaction_name[" << i
				 << "] = " << pOut->trade_info[i].cash_transaction_name
				 << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT th_dts" << endl
			  << "     , th_st_id" << endl
			  << "FROM trade_history" << endl
			  << "WHERE th_t_id = " << pIn->trade_id[i] << endl
			  << "ORDER BY th_dts" << endl
			  << "LIMIT 3";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res = exec(osSQL.str().c_str());

		int count = PQntuples(res);
		for (int j = 0; j < count; j++) {
			sscanf(PQgetvalue(res, j, 0), "%hd-%hd-%hd %hd:%hd:%hd.%d",
					&pOut->trade_info[i].trade_history_dts[j].year,
					&pOut->trade_info[i].trade_history_dts[j].month,
					&pOut->trade_info[i].trade_history_dts[j].day,
					&pOut->trade_info[i].trade_history_dts[j].hour,
					&pOut->trade_info[i].trade_history_dts[j].minute,
					&pOut->trade_info[i].trade_history_dts[j].second,
					&pOut->trade_info[i].trade_history_dts[j].fraction);
			strncpy(pOut->trade_info[i].trade_history_status_id[j],
					PQgetvalue(res, j, 1), cTH_ST_ID_len);
		}
		PQclear(res);

		if (m_bVerbose) {
			for (int j = 0; j < count; j++) {
				cout << "trade_history_dts[" << j
					 << "] = " << pOut->trade_info[i].trade_history_dts[j].year
					 << "-" << pOut->trade_info[i].trade_history_dts[j].month
					 << "-" << pOut->trade_info[i].trade_history_dts[j].day
					 << " " << pOut->trade_info[i].trade_history_dts[j].hour
					 << ":" << pOut->trade_info[i].trade_history_dts[j].minute
					 << ":" << pOut->trade_info[i].trade_history_dts[j].second
					 << "."
					 << pOut->trade_info[i].trade_history_dts[j].fraction
					 << endl;
				cout << "trade_history_status_id[" << j << "] = "
					 << pOut->trade_info[i].trade_history_status_id[j] << endl;
			}
		}
	}
}

void
CDBConnectionClientSide::execute(
		const TTradeUpdateFrame2Input *pIn, TTradeUpdateFrame2Output *pOut)
{
	PGresult *res = NULL;
	PGresult *res2 = NULL;
	ostringstream osSQL;

	osSQL << "SELECT t_bid_price" << endl
		  << "     , t_exec_name" << endl
		  << "     , t_is_cash" << endl
		  << "     , t_id" << endl
		  << "     , t_trade_price" << endl
		  << "FROM trade" << endl
		  << "WHERE t_ca_id = " << pIn->acct_id << endl
		  << "  AND t_dts >= '" << pIn->start_trade_dts.year << "-"
		  << pIn->start_trade_dts.month << "-" << pIn->start_trade_dts.day
		  << " " << pIn->start_trade_dts.hour << ":"
		  << pIn->start_trade_dts.minute << ":" << pIn->start_trade_dts.second
		  << "." << pIn->start_trade_dts.fraction << "'" << endl
		  << "  AND t_dts <= '" << pIn->end_trade_dts.year << "-"
		  << pIn->end_trade_dts.month << "-" << pIn->end_trade_dts.day << " "
		  << pIn->end_trade_dts.hour << ":" << pIn->end_trade_dts.minute << ":"
		  << pIn->end_trade_dts.second << "." << pIn->end_trade_dts.fraction
		  << "'" << endl
		  << "ORDER BY t_dts ASC" << endl
		  << "LIMIT " << pIn->max_trades;
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	pOut->num_updated = 0;
	pOut->num_found = PQntuples(res);
	for (int i = 0; i < pOut->num_found; i++) {
		pOut->trade_info[i].bid_price = atof(PQgetvalue(res, i, 0));
		strncpy(pOut->trade_info[i].exec_name, PQgetvalue(res, i, 1),
				cEXEC_NAME_len);
		if (PQgetvalue(res, 0, 2)[0] == 't') {
			pOut->trade_info[i].is_cash = true;
		} else {
			pOut->trade_info[i].is_cash = false;
		}
		pOut->trade_info[i].trade_id = atoll(PQgetvalue(res, i, 3));
		pOut->trade_info[i].trade_price = atof(PQgetvalue(res, i, 4));

		if (m_bVerbose) {
			cout << "bid_price[" << i
				 << "] = " << pOut->trade_info[i].bid_price << endl;
			cout << "exec_name[" << i
				 << "] = " << pOut->trade_info[i].exec_name << endl;
			cout << "is_cash[" << i << "] = " << pOut->trade_info[i].is_cash
				 << endl;
			cout << "trade_id[" << i << "] = " << pOut->trade_info[i].trade_id
				 << endl;
			cout << "trade_price[" << i
				 << "] = " << pOut->trade_info[i].trade_price << endl;
		}

		if (pOut->num_updated < pIn->max_updates) {
			char cash_type[cSE_CASH_TYPE_len + 1];

			osSQL.clear();
			osSQL.str("");
			osSQL << "SELECT se_cash_type" << endl
				  << "FROM settlement" << endl
				  << "WHERE se_t_id = " << pOut->trade_info[i].trade_id;
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res2 = exec(osSQL.str().c_str());

			strncpy(cash_type, PQgetvalue(res2, 0, 0), cSE_CASH_TYPE_len);
			PQclear(res2);

			if (m_bVerbose) {
				cout << "cash_type[" << i << "] = " << cash_type << endl;
			}

			osSQL.clear();
			osSQL.str("");
			osSQL << "UPDATE settlement" << endl;
			if (pOut->trade_info[i].is_cash) {
				if (strncmp(cash_type, "Cash Account", cSE_CASH_TYPE_len)
						== 0) {
					osSQL << "SET se_cash_type = 'Cash'" << endl;
				} else {
					osSQL << "SET se_cash_type = 'Cash Account'" << endl;
				}
			} else {
				if (strncmp(cash_type, "Margin Account", cSE_CASH_TYPE_len)
						== 0) {
					osSQL << "SET se_cash_type = 'Margin'" << endl;
				} else {
					osSQL << "SET se_cash_type = 'Margin Account'" << endl;
				}
			}
			osSQL << "WHERE se_t_id = " << pOut->trade_info[i].trade_id;
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res2 = exec(osSQL.str().c_str());

			if (m_bVerbose) {
				cout << "PQcmdTuples = " << PQcmdTuples(res) << endl;
			}
			pOut->num_updated += atoi(PQcmdTuples(res2));
			PQclear(res2);
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT se_amt" << endl
			  << "     , se_cash_due_date" << endl
			  << "     , se_cash_type" << endl
			  << "FROM settlement" << endl
			  << "WHERE se_t_id = " << pOut->trade_info[i].trade_id;
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res2 = exec(osSQL.str().c_str());

		if (PQntuples(res2) == 0) {
			PQclear(res2);
			return;
		}

		pOut->trade_info[i].settlement_amount = atof(PQgetvalue(res2, 0, 0));
		sscanf(PQgetvalue(res2, 0, 1), "%hd-%hd-%hd",
				&pOut->trade_info[i].settlement_cash_due_date.year,
				&pOut->trade_info[i].settlement_cash_due_date.month,
				&pOut->trade_info[i].settlement_cash_due_date.day);
		strncpy(pOut->trade_info[i].settlement_cash_type,
				PQgetvalue(res2, 0, 2), cSE_CASH_TYPE_len);
		PQclear(res2);

		if (m_bVerbose) {
			cout << "settlement_amount[" << i
				 << "] = " << pOut->trade_info[i].settlement_amount << endl;
			cout << "settlement_cash_due_date[" << i
				 << "] = " << pOut->trade_info[i].settlement_cash_due_date.year
				 << "-" << pOut->trade_info[i].settlement_cash_due_date.month
				 << "-" << pOut->trade_info[i].settlement_cash_due_date.day
				 << endl;
			cout << "settlement_cash_type[" << i
				 << "] = " << pOut->trade_info[i].settlement_cash_type << endl;
		}

		if (pOut->trade_info[i].is_cash) {
			osSQL.clear();
			osSQL.str("");
			osSQL << "SELECT ct_amt" << endl
				  << "     , ct_dts" << endl
				  << "     , ct_name" << endl
				  << "FROM cash_transaction" << endl
				  << "WHERE ct_t_id = " << pOut->trade_info[i].trade_id;
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res2 = exec(osSQL.str().c_str());

			if (PQntuples(res2) > 0) {
				pOut->trade_info[i].cash_transaction_amount
						= atof(PQgetvalue(res2, 0, 0));
				sscanf(PQgetvalue(res2, 0, 1), "%hd-%hd-%hd %hd:%hd:%hd.%d",
						&pOut->trade_info[i].cash_transaction_dts.year,
						&pOut->trade_info[i].cash_transaction_dts.month,
						&pOut->trade_info[i].cash_transaction_dts.day,
						&pOut->trade_info[i].cash_transaction_dts.hour,
						&pOut->trade_info[i].cash_transaction_dts.minute,
						&pOut->trade_info[i].cash_transaction_dts.second,
						&pOut->trade_info[i].cash_transaction_dts.fraction);
				strncpy(pOut->trade_info[i].cash_transaction_name,
						PQgetvalue(res2, 0, 2), cCT_NAME_len);
			}
			PQclear(res2);

			if (m_bVerbose) {
				cout << "cash_transaction_amount[" << i
					 << "] = " << pOut->trade_info[i].cash_transaction_amount
					 << endl;
				cout << "cash_transaction_dts[" << i
					 << "] = " << pOut->trade_info[i].cash_transaction_dts.year
					 << "-" << pOut->trade_info[i].cash_transaction_dts.month
					 << "-" << pOut->trade_info[i].cash_transaction_dts.day
					 << " " << pOut->trade_info[i].cash_transaction_dts.hour
					 << ":" << pOut->trade_info[i].cash_transaction_dts.minute
					 << ":" << pOut->trade_info[i].cash_transaction_dts.second
					 << "."
					 << pOut->trade_info[i].cash_transaction_dts.fraction
					 << endl;
				cout << "cash_transaction_name[" << i
					 << "] = " << pOut->trade_info[i].cash_transaction_name
					 << endl;
			}

			osSQL.clear();
			osSQL.str("");
			osSQL << "SELECT th_dts" << endl
				  << "     , th_st_id" << endl
				  << "FROM trade_history" << endl
				  << "WHERE th_t_id = " << pOut->trade_info[i].trade_id << endl
				  << "ORDER BY th_dts" << endl
				  << "LIMIT 3";
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res2 = exec(osSQL.str().c_str());

			int count = PQntuples(res2);
			for (int j = 0; j < count; j++) {
				sscanf(PQgetvalue(res2, j, 0), "%hd-%hd-%hd %hd:%hd:%hd.%d",
						&pOut->trade_info[i].trade_history_dts[j].year,
						&pOut->trade_info[i].trade_history_dts[j].month,
						&pOut->trade_info[i].trade_history_dts[j].day,
						&pOut->trade_info[i].trade_history_dts[j].hour,
						&pOut->trade_info[i].trade_history_dts[j].minute,
						&pOut->trade_info[i].trade_history_dts[j].second,
						&pOut->trade_info[i].trade_history_dts[j].fraction);
				strncpy(pOut->trade_info[i].trade_history_status_id[j],
						PQgetvalue(res2, j, 1), cTH_ST_ID_len);
			}
			PQclear(res2);

			if (m_bVerbose) {
				for (int j = 0; j < count; j++) {
					cout << "trade_history_dts[" << j << "] = "
						 << pOut->trade_info[i].trade_history_dts[j].year
						 << "-"
						 << pOut->trade_info[i].trade_history_dts[j].month
						 << "-" << pOut->trade_info[i].trade_history_dts[j].day
						 << " "
						 << pOut->trade_info[i].trade_history_dts[j].hour
						 << ":"
						 << pOut->trade_info[i].trade_history_dts[j].minute
						 << ":"
						 << pOut->trade_info[i].trade_history_dts[j].second
						 << "."
						 << pOut->trade_info[i].trade_history_dts[j].fraction
						 << endl;
					cout << "trade_history_status_id[" << j << "] = "
						 << pOut->trade_info[i].trade_history_status_id[j]
						 << endl;
				}
			}
		}
	}
	PQclear(res);
}

void
CDBConnectionClientSide::execute(
		const TTradeUpdateFrame3Input *pIn, TTradeUpdateFrame3Output *pOut)
{
	PGresult *res = NULL;
	PGresult *res2 = NULL;
	ostringstream osSQL;

	osSQL << "SELECT t_ca_id" << endl
		  << "     , t_exec_name" << endl
		  << "     , t_is_cash" << endl
		  << "     , t_trade_price" << endl
		  << "     , t_qty" << endl
		  << "     , s_name" << endl
		  << "     , t_dts" << endl
		  << "     , t_id" << endl
		  << "     , t_tt_id" << endl
		  << "     , tt_name" << endl
		  << "FROM trade" << endl
		  << "   , trade_type" << endl
		  << "   , security" << endl
		  << "WHERE t_s_symb = '" << pIn->symbol << "'" << endl
		  << "  AND t_dts >= '" << pIn->start_trade_dts.year << "-"
		  << pIn->start_trade_dts.month << "-" << pIn->start_trade_dts.day
		  << " " << pIn->start_trade_dts.hour << ":"
		  << pIn->start_trade_dts.minute << ":" << pIn->start_trade_dts.second
		  << "." << pIn->start_trade_dts.fraction << "'" << endl
		  << "  AND t_dts <= '" << pIn->end_trade_dts.year << "-"
		  << pIn->end_trade_dts.month << "-" << pIn->end_trade_dts.day << " "
		  << pIn->end_trade_dts.hour << ":" << pIn->end_trade_dts.minute << ":"
		  << pIn->end_trade_dts.second << "." << pIn->end_trade_dts.fraction
		  << "'" << endl
		  << "  AND tt_id = t_tt_id" << endl
		  << "  AND s_symb = t_s_symb" << endl
		  << "ORDER BY t_dts ASC" << endl
		  << "LIMIT " << pIn->max_trades;
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	res = exec(osSQL.str().c_str());

	pOut->num_updated = 0;
	pOut->num_found = PQntuples(res);
	for (int i = 0; i < pOut->num_found; i++) {
		pOut->trade_info[i].acct_id = atoll(PQgetvalue(res, i, 0));
		strncpy(pOut->trade_info[i].exec_name, PQgetvalue(res, i, 1),
				cEXEC_NAME_len);
		pOut->trade_info[i].is_cash
				= PQgetvalue(res, i, 2)[0] == 't' ? true : false;
		pOut->trade_info[i].price = atof(PQgetvalue(res, i, 3));
		pOut->trade_info[i].quantity = atoi(PQgetvalue(res, i, 4));
		strncpy(pOut->trade_info[i].s_name, PQgetvalue(res, i, 5),
				cS_NAME_len);
		sscanf(PQgetvalue(res, i, 6), "%hd-%hd-%hd %hd:%hd:%hd.%d",
				&pOut->trade_info[i].trade_dts.year,
				&pOut->trade_info[i].trade_dts.month,
				&pOut->trade_info[i].trade_dts.day,
				&pOut->trade_info[i].trade_dts.hour,
				&pOut->trade_info[i].trade_dts.minute,
				&pOut->trade_info[i].trade_dts.second,
				&pOut->trade_info[i].trade_dts.fraction);
		pOut->trade_info[i].trade_id = atoll(PQgetvalue(res, i, 7));
		strncpy(pOut->trade_info[i].trade_type, PQgetvalue(res, i, 8),
				cTT_ID_len);
		strncpy(pOut->trade_info[i].type_name, PQgetvalue(res, i, 9),
				cTT_NAME_len);

		if (m_bVerbose) {
			cout << "acct_id[" << i << "] = " << pOut->trade_info[i].acct_id
				 << endl;
			cout << "exec_name[" << i
				 << "] = " << pOut->trade_info[i].exec_name << endl;
			cout << "is_cash[" << i << "] = " << pOut->trade_info[i].is_cash
				 << endl;
			cout << "price[" << i << "] = " << pOut->trade_info[i].price
				 << endl;
			cout << "quantity[" << i << "] = " << pOut->trade_info[i].quantity
				 << endl;
			cout << "s_name[" << i << "] = " << pOut->trade_info[i].s_name
				 << endl;
			cout << "trade_dts[" << i << "] = " << cout << "[" << i
				 << "] = " << &pOut->trade_info[i].trade_dts.year << "-"
				 << cout << "[" << i
				 << "] = " << &pOut->trade_info[i].trade_dts.month << "-"
				 << cout << "[" << i
				 << "] = " << &pOut->trade_info[i].trade_dts.day << " " << cout
				 << "[" << i << "] = " << &pOut->trade_info[i].trade_dts.hour
				 << ":" << cout << "[" << i
				 << "] = " << &pOut->trade_info[i].trade_dts.minute << ":"
				 << cout << "[" << i
				 << "] = " << &pOut->trade_info[i].trade_dts.second << "."
				 << cout << "[" << i
				 << "] = " << &pOut->trade_info[i].trade_dts.fraction << endl;
			cout << "trade_id[" << i << "] = " << pOut->trade_info[i].trade_id
				 << endl;
			cout << "trade_type[" << i
				 << "] = " << pOut->trade_info[i].trade_type << endl;
			cout << "type_name[" << i
				 << "] = " << pOut->trade_info[i].type_name << endl;
		}

		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT se_amt" << endl
			  << "     , se_cash_due_date" << endl
			  << "     , se_cash_type" << endl
			  << "FROM settlement" << endl
			  << "WHERE se_t_id = " << pOut->trade_info[i].trade_id;
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res2 = exec(osSQL.str().c_str());

		if (PQntuples(res2) == 0) {
			PQclear(res2);
			return;
		}

		pOut->trade_info[i].settlement_amount = atof(PQgetvalue(res2, 0, 0));
		sscanf(PQgetvalue(res2, 0, 1), "%hd-%hd-%hd",
				&pOut->trade_info[i].settlement_cash_due_date.year,
				&pOut->trade_info[i].settlement_cash_due_date.month,
				&pOut->trade_info[i].settlement_cash_due_date.day);
		strncpy(pOut->trade_info[i].settlement_cash_type,
				PQgetvalue(res2, 0, 2), cSE_CASH_TYPE_len);
		PQclear(res2);

		if (m_bVerbose) {
			cout << "settlement_amount[" << i
				 << "] = " << pOut->trade_info[i].settlement_amount << endl;
			cout << "settlement_cash_due_date[" << i
				 << "] = " << pOut->trade_info[i].settlement_cash_due_date.year
				 << "-" << pOut->trade_info[i].settlement_cash_due_date.month
				 << "-" << pOut->trade_info[i].settlement_cash_due_date.day
				 << endl;
			cout << "settlement_cash_type[" << i
				 << "] = " << pOut->trade_info[i].settlement_cash_type << endl;
		}

		if (pOut->trade_info[i].is_cash) {
			if (pOut->num_updated < pIn->max_updates) {
				char *ct_name;

				osSQL.clear();
				osSQL.str("");
				osSQL << "SELECT ct_name" << endl
					  << "FROM cash_transaction" << endl
					  << "WHERE ct_t_id = " << pOut->trade_info[i].trade_id;
				if (m_bVerbose) {
					cout << osSQL.str() << endl;
				}
				res2 = exec(osSQL.str().c_str());

				ct_name = PQgetvalue(res2, 0, 0);
				if (m_bVerbose) {
					cout << "ct_name[" << i << "] = " << ct_name << endl;
				}

				ostringstream os_ct_name;
				if (strstr(ct_name, " shares of ") != NULL) {
					os_ct_name << pOut->trade_info[i].type_name << " "
							   << pOut->trade_info[i].quantity << " Shares of "
							   << pOut->trade_info[i].s_name;
				} else {
					os_ct_name << pOut->trade_info[i].type_name << " "
							   << pOut->trade_info[i].quantity << " shares of "
							   << pOut->trade_info[i].s_name;
				}
				char *new_ct_name = escape(os_ct_name.str());

				osSQL.clear();
				osSQL.str("");
				osSQL << "UPDATE cash_transaction" << endl
					  << "SET ct_name = e" << new_ct_name << endl
					  << "WHERE ct_t_id = " << pOut->trade_info[i].trade_id;
				PQfreemem(new_ct_name);
				PQclear(res2);
				if (m_bVerbose) {
					cout << osSQL.str() << endl;
				}
				res2 = exec(osSQL.str().c_str());

				if (m_bVerbose) {
					cout << "PQcmdTuples = " << PQcmdTuples(res) << endl;
				}
				pOut->num_updated += atoi(PQcmdTuples(res2));
				PQclear(res2);
			}
			osSQL.clear();
			osSQL.str("");
			osSQL << "SELECT ct_amt" << endl
				  << "     , ct_dts" << endl
				  << "     , ct_name" << endl
				  << "FROM cash_transaction" << endl
				  << "WHERE ct_t_id = " << pOut->trade_info[i].trade_id;
			if (m_bVerbose) {
				cout << osSQL.str() << endl;
			}
			res2 = exec(osSQL.str().c_str());

			if (PQntuples(res2) > 0) {
				pOut->trade_info[i].cash_transaction_amount
						= atof(PQgetvalue(res2, 0, 0));
				sscanf(PQgetvalue(res2, 0, 1), "%hd-%hd-%hd %hd:%hd:%hd.%d",
						&pOut->trade_info[i].cash_transaction_dts.year,
						&pOut->trade_info[i].cash_transaction_dts.month,
						&pOut->trade_info[i].cash_transaction_dts.day,
						&pOut->trade_info[i].cash_transaction_dts.hour,
						&pOut->trade_info[i].cash_transaction_dts.minute,
						&pOut->trade_info[i].cash_transaction_dts.second,
						&pOut->trade_info[i].cash_transaction_dts.fraction);
				strncpy(pOut->trade_info[i].cash_transaction_name,
						PQgetvalue(res2, 0, 2), cCT_NAME_len);
			}
			PQclear(res2);

			if (m_bVerbose) {
				cout << "cash_transaction_amount[" << i
					 << "] = " << pOut->trade_info[i].cash_transaction_amount
					 << endl;
				cout << "cash_transaction_dts[" << i
					 << "] = " << pOut->trade_info[i].cash_transaction_dts.year
					 << "-" << pOut->trade_info[i].cash_transaction_dts.month
					 << "-" << pOut->trade_info[i].cash_transaction_dts.day
					 << " " << pOut->trade_info[i].cash_transaction_dts.hour
					 << ":" << pOut->trade_info[i].cash_transaction_dts.minute
					 << ":" << pOut->trade_info[i].cash_transaction_dts.second
					 << "."
					 << pOut->trade_info[i].cash_transaction_dts.fraction
					 << endl;
				cout << "cash_transaction_name[" << i
					 << "] = " << pOut->trade_info[i].cash_transaction_name
					 << endl;
			}
		}
		osSQL.clear();
		osSQL.str("");
		osSQL << "SELECT th_dts" << endl
			  << "     , th_st_id" << endl
			  << "FROM trade_history" << endl
			  << "WHERE th_t_id = " << pOut->trade_info[i].trade_id << endl
			  << "ORDER BY th_dts" << endl
			  << "LIMIT 3";
		if (m_bVerbose) {
			cout << osSQL.str() << endl;
		}
		res2 = exec(osSQL.str().c_str());

		int count = PQntuples(res2);
		for (int j = 0; j < count; j++) {
			sscanf(PQgetvalue(res2, j, 0), "%hd-%hd-%hd %hd:%hd:%hd.%d",
					&pOut->trade_info[i].trade_history_dts[j].year,
					&pOut->trade_info[i].trade_history_dts[j].month,
					&pOut->trade_info[i].trade_history_dts[j].day,
					&pOut->trade_info[i].trade_history_dts[j].hour,
					&pOut->trade_info[i].trade_history_dts[j].minute,
					&pOut->trade_info[i].trade_history_dts[j].second,
					&pOut->trade_info[i].trade_history_dts[j].fraction);
			strncpy(pOut->trade_info[i].trade_history_status_id[j],
					PQgetvalue(res2, j, 1), cTH_ST_ID_len);
		}
		PQclear(res2);

		if (m_bVerbose) {
			for (int j = 0; j < count; j++) {
				cout << "trade_history_dts[" << j
					 << "] = " << pOut->trade_info[i].trade_history_dts[j].year
					 << "-" << pOut->trade_info[i].trade_history_dts[j].month
					 << "-" << pOut->trade_info[i].trade_history_dts[j].day
					 << " " << pOut->trade_info[i].trade_history_dts[j].hour
					 << ":" << pOut->trade_info[i].trade_history_dts[j].minute
					 << ":" << pOut->trade_info[i].trade_history_dts[j].second
					 << "."
					 << pOut->trade_info[i].trade_history_dts[j].fraction
					 << endl;
				cout << "trade_history_status_id[" << j << "] = "
					 << pOut->trade_info[i].trade_history_status_id[j] << endl;
			}
		}
	}
	PQclear(res);
}
