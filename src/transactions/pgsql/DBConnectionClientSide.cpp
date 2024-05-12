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
	ostringstream osSQL;
	osSQL << "SELECT t_id" << endl
		  << "     , t_s_symb" << endl
		  << "     , t_qty" << endl
		  << "     , st_name" << endl
		  << "     , th_dts" << endl
		  << "FROM (" << endl
		  << "         SELECT t_id AS id" << endl
		  << "         FROM trade" << endl
		  << "         WHERE t_ca_id = " << pIn->acct_id << endl
		  << "         ORDER BY t_dts DESC" << endl
		  << "         LIMIT 10" << endl
		  << "     ) AS t" << endl
		  << "   , trade" << endl
		  << "   , trade_history" << endl
		  << "   , status_type" << endl
		  << "WHERE t_id = id" << endl
		  << "  AND th_t_id = t_id" << endl
		  << "  AND st_id = th_st_id" << endl
		  << "ORDER BY th_dts DESC";
	if (m_bVerbose) {
		cout << osSQL.str() << endl;
	}
	PGresult *res = exec(osSQL.str().c_str());

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
