/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * The purpose of this class object is allow other classes to share the
 * same database connection.  The database connection is managed by this
 * class.
 *
 * 13 June 2006
 */

#include "DBConnection.h"

// These are inlined function that should only be used here.

bool inline check_count(int should, int is, const char *file, int line) {
	if (should != is) {
		cout << "*** array length (" << is <<
				") does not match expections (" << should << "): " << file <<
				":" << line << endl;
		return false;
	}
	return true;
}


// Array Tokenizer
void inline TokenizeArray(const string& str2, vector<string>& tokens)
{
	// This is essentially an empty array. i.e. '{}'
	if (str2.size() < 3)
		return;

	// We only call this function because we need to chop up arrays that
	// are in the format '{{1,2,3},{a,b,c}}', so trim off the braces.
	string str = str2.substr(1, str2.size() - 2);

	// Skip delimiters at beginning.
	string::size_type lastPos = str.find_first_of("{", 0);
	// Find first "non-delimiter".
	string::size_type pos = str.find_first_of("}", lastPos);

	while (string::npos != pos || string::npos != lastPos)
	{
		// Found a token, add it to the vector.
		tokens.push_back(str.substr(lastPos, pos - lastPos + 1));

		lastPos = str.find_first_of("{", pos);
		pos = str.find_first_of("}", lastPos);
	}
}

// String Tokenizer
// FIXME: This token doesn't handle strings with escaped characters.
void inline TokenizeSmart(const string& str, vector<string>& tokens)
{
	// This is essentially an empty array. i.e. '{}'
	if (str.size() < 3)
		return;

	string::size_type lastPos = 1;
	string::size_type pos = 1;
	bool end = false;
	while (end == false)
	{
		if (str[lastPos] == '"') {
			pos = str.find_first_of("\"", lastPos + 1);
			if (pos == string::npos) {
				pos = str.find_first_of("}", lastPos);
				end = true;
			}
			tokens.push_back(str.substr(lastPos + 1, pos - lastPos - 1));
			lastPos = pos + 2;
		} else if (str[lastPos] == '\0') {
			return;
		} else {
			pos = str.find_first_of(",", lastPos);
			if (pos == string::npos) {
				pos = str.find_first_of("}", lastPos);
				end = true;
			}
			tokens.push_back(str.substr(lastPos, pos - lastPos));
			lastPos = pos + 1;
		}
	}
}

// Constructor: Creates PgSQL connection
CDBConnection::CDBConnection(const char *szHost, const char *szDBName,
		const char *szDBPort)
{
	szConnectStr[0] = '\0';

	// Just pad everything with spaces so we don't have to figure out if it's
	// needed or not.
	if (strlen(szHost) > 0) {
		strcat(szConnectStr, " host=");
		strcat(szConnectStr, szHost);
	}
	if (strlen(szDBName) > 0) {
		strcat(szConnectStr, " dbname=");
		strcat(szConnectStr, szDBName);
	}
	if (strlen(szDBPort) > 0) {
		strcat(szConnectStr, " port=");
		strcat(szConnectStr, szDBPort);
	}

	sprintf(name, "%d", (int) pthread_self());
	m_Conn = PQconnectdb(szConnectStr);
}

// Destructor: Disconnect from server
CDBConnection::~CDBConnection()
{
	PQfinish(m_Conn);
}

void CDBConnection::begin()
{
	PQexec(m_Conn, "BEGIN;");
}

void CDBConnection::connect()
{
	m_Conn = PQconnectdb(szConnectStr);
}

void CDBConnection::commit()
{
	PQexec(m_Conn, "COMMIT;");
}

char *CDBConnection::escape(string s)
{
	char *esc = PQescapeLiteral(m_Conn, s.c_str(), s.length());
	if (esc == NULL)
		cerr << "ERROR: could not escape '" << s << "'" << endl;
	return esc;
}

void CDBConnection::disconnect()
{
	PQfinish(m_Conn);
}

PGresult *CDBConnection::exec(const char *sql)
{
	// FIXME: Handle serialization errors.
	// For PostgreSQL, see comment in the Concurrency Control chapter, under
	// the Transaction Isolation section for dealing with serialization
	// failures.  These serialization failures can occur with REPEATABLE READS
	// or SERIALIZABLE.

	PGresult *res = PQexec(m_Conn, sql);
	ExecStatusType status = PQresultStatus(res);

	ostringstream msg;
	switch (status) {
	case PGRES_FATAL_ERROR:
		msg << time(NULL) << " " << pthread_self() << endl <<
				"SQL: " << sql << endl <<
				PQresultErrorMessage(res) << endl;
		rollback();
		throw msg.str().c_str();
		break;
	case PGRES_TUPLES_OK:
		if (PQntuples(res) == 0) {
			msg << time(NULL) << " " << pthread_self() << endl <<
					"SQL: " << sql << endl <<
					"NO RESULTS" << endl;
			rollback();
			throw msg.str().c_str();
		}
		break;
	case PGRES_EMPTY_QUERY:
	case PGRES_COMMAND_OK:
	case PGRES_COPY_OUT:
	case PGRES_COPY_IN:
	case PGRES_BAD_RESPONSE:
	case PGRES_NONFATAL_ERROR:
	default:
		cout << "*** " << PQresStatus(PQresultStatus(res)) << endl;
		break;
	}

	return res;
}

void CDBConnection::execute(const TBrokerVolumeFrame1Input *pIn,
		TBrokerVolumeFrame1Output *pOut)
{
	int i_broker_name;
	int i_list_len;
	int i_volume;

	ostringstream osBrokers;
	int i = 0;
	osBrokers << pIn->broker_list[i];
	for (i = 1; pIn->broker_list[i][0] != '\0' &&
			i < max_broker_list_len; i++) {
		osBrokers << ", " << pIn->broker_list[i];
	}

	ostringstream osSQL;
	osSQL << "SELECT * FROM BrokerVolumeFrame1('{" <<
			osBrokers.str() << "}','" <<
			pIn->sector_name << "')";

	PGresult *res = exec(osSQL.str().c_str());
	i_broker_name = PQfnumber(res, "broker_name");
	i_list_len = PQfnumber(res, "list_len");
	i_volume = PQfnumber(res, "volume");

	pOut->list_len = atoi(PQgetvalue(res, 0, i_list_len));

	vector<string> vAux;
	vector<string>::iterator p;

	TokenizeSmart(PQgetvalue(res, 0, i_broker_name), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->broker_name[i], (*p).c_str(), cB_NAME_len);
		pOut->broker_name[i][cB_NAME_len] = '\0';
		++i;
	}
	check_count(pOut->list_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_volume), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->volume[i] = atof((*p).c_str());
		++i;
	}
	check_count(pOut->list_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();
}

void CDBConnection::execute(const TCustomerPositionFrame1Input *pIn,
		TCustomerPositionFrame1Output *pOut)
{
	int i_cust_id;
	int i_acct_id;
	int i_acct_len;
	int i_asset_total;
	int i_c_ad_id;
	int i_c_area_1;
	int i_c_area_2;
	int i_c_area_3;
	int i_c_ctry_1;
	int i_c_ctry_2;
	int i_c_ctry_3;
	int i_c_dob;
	int i_c_email_1;
	int i_c_email_2;
	int i_c_ext_1;
	int i_c_ext_2;
	int i_c_ext_3;
	int i_c_f_name;
	int i_c_gndr;
	int i_c_l_name;
	int i_c_local_1;
	int i_c_local_2;
	int i_c_local_3;
	int i_c_m_name;
	int i_c_st_id;
	int i_c_tier;
	int i_cash_bal;

	ostringstream osSQL;
	osSQL << "SELECT * FROM CustomerPositionFrame1(" <<
			pIn->cust_id << ",'" <<
			pIn->tax_id << "')";

	PGresult *res = exec(osSQL.str().c_str());
	i_cust_id = PQfnumber(res, "cust_id");
	i_acct_id = PQfnumber(res, "acct_id");
	i_acct_len = PQfnumber(res, "acct_len");
	i_asset_total = PQfnumber(res, "asset_total");
	i_c_ad_id = PQfnumber(res, "c_ad_id");
	i_c_area_1 = PQfnumber(res, "c_area_1");
	i_c_area_2 = PQfnumber(res, "c_area_2");
	i_c_area_3 = PQfnumber(res, "c_area_3");
	i_c_ctry_1 = PQfnumber(res, "c_ctry_1");
	i_c_ctry_2 = PQfnumber(res, "c_ctry_2");
	i_c_ctry_3 = PQfnumber(res, "c_ctry_3");
	i_c_dob = PQfnumber(res, "c_dob");
	i_c_email_1 = PQfnumber(res, "c_email_1");
	i_c_email_2 = PQfnumber(res, "c_email_2");
	i_c_ext_1 = PQfnumber(res, "c_ext_1");
	i_c_ext_2 = PQfnumber(res, "c_ext_2");
	i_c_ext_3 = PQfnumber(res, "c_ext_3");
	i_c_f_name = PQfnumber(res, "c_f_name");
	i_c_gndr = PQfnumber(res, "c_gndr");
	i_c_l_name = PQfnumber(res, "c_l_name");
	i_c_local_1 = PQfnumber(res, "c_local_1");
	i_c_local_2 = PQfnumber(res, "c_local_2");
	i_c_local_3 = PQfnumber(res, "c_local_3");
	i_c_m_name = PQfnumber(res, "c_m_name");
	i_c_st_id = PQfnumber(res, "c_st_id");
	i_c_tier = PQfnumber(res, "c_tier");
	i_cash_bal = PQfnumber(res, "cash_bal");

	pOut->acct_len = atoi(PQgetvalue(res, 0, i_acct_len));
	pOut->cust_id = atol(PQgetvalue(res, 0, i_cust_id));

	vector<string> vAux;
	vector<string>::iterator p;

	TokenizeSmart(PQgetvalue(res, 0, i_acct_id), vAux);
	int i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->acct_id[i] = atol( (*p).c_str() );
		++i;
	}
	check_count(pOut->acct_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_asset_total), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
			pOut->asset_total[i] = atof( (*p).c_str() );
			++i;
	}
	check_count(pOut->acct_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->c_ad_id = atol(PQgetvalue(res, 0, i_c_ad_id));


	strncpy(pOut->c_area_1, PQgetvalue(res, 0, i_c_area_1), cAREA_len);
	pOut->c_area_1[cAREA_len] = '\0';
	strncpy(pOut->c_area_2, PQgetvalue(res, 0, i_c_area_2), cAREA_len);
	pOut->c_area_2[cAREA_len] = '\0';
	strncpy(pOut->c_area_3, PQgetvalue(res, 0, i_c_area_3), cAREA_len);
	pOut->c_area_3[cAREA_len] = '\0';

	strncpy(pOut->c_ctry_1, PQgetvalue(res, 0, i_c_ctry_1), cCTRY_len);
	pOut->c_ctry_1[cCTRY_len] = '\0';
	strncpy(pOut->c_ctry_2, PQgetvalue(res, 0, i_c_ctry_2), cCTRY_len);
	pOut->c_ctry_2[cCTRY_len] = '\0';
	strncpy(pOut->c_ctry_3, PQgetvalue(res, 0, i_c_ctry_3), cCTRY_len);
	pOut->c_ctry_3[cCTRY_len] = '\0';

	sscanf(PQgetvalue(res, 0, i_c_dob), "%hd-%hd-%hd", &pOut->c_dob.year,
			&pOut->c_dob.month, &pOut->c_dob.day);

	strncpy(pOut->c_email_1, PQgetvalue(res, 0, i_c_email_1), cEMAIL_len);
	pOut->c_email_1[cEMAIL_len] = '\0';
	strncpy(pOut->c_email_2, PQgetvalue(res, 0, i_c_email_2), cEMAIL_len);
	pOut->c_email_2[cEMAIL_len] = '\0';

	strncpy(pOut->c_ext_1, PQgetvalue(res, 0, i_c_ext_1), cEXT_len);
	pOut->c_ext_1[cEXT_len] = '\0';
	strncpy(pOut->c_ext_2, PQgetvalue(res, 0, i_c_ext_2), cEXT_len);
	pOut->c_ext_2[cEXT_len] = '\0';
	strncpy(pOut->c_ext_3, PQgetvalue(res, 0, i_c_ext_3), cEXT_len);
	pOut->c_ext_3[cEXT_len] = '\0';

	strncpy(pOut->c_f_name, PQgetvalue(res, 0, i_c_f_name), cF_NAME_len);
	pOut->c_f_name[cF_NAME_len] = '\0';
	strncpy(pOut->c_gndr, PQgetvalue(res, 0, i_c_gndr), cGNDR_len);
	pOut->c_gndr[cGNDR_len] = '\0';
	strncpy(pOut->c_l_name, PQgetvalue(res, 0, i_c_l_name), cL_NAME_len);
	pOut->c_l_name[cL_NAME_len] = '\0';

	strncpy(pOut->c_local_1, PQgetvalue(res, 0, i_c_local_1), cLOCAL_len);
	pOut->c_local_1[cLOCAL_len] = '\0';
	strncpy(pOut->c_local_2, PQgetvalue(res, 0, i_c_local_2), cLOCAL_len);
	pOut->c_local_2[cLOCAL_len] = '\0';
	strncpy(pOut->c_local_3, PQgetvalue(res, 0, i_c_local_3), cLOCAL_len);
	pOut->c_local_3[cLOCAL_len] = '\0';

	strncpy(pOut->c_m_name, PQgetvalue(res, 0, i_c_m_name), cM_NAME_len);
	pOut->c_m_name[cM_NAME_len] = '\0';
	strncpy(pOut->c_st_id, PQgetvalue(res, 0, i_c_st_id), cST_ID_len);
	pOut->c_st_id[cST_ID_len] = '\0';
	strncpy(&pOut->c_tier, PQgetvalue(res, 0, i_c_tier), 1);

	TokenizeSmart(PQgetvalue(res, 0, i_cash_bal), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->cash_bal[i] = atof( (*p).c_str() );
		++i;
	}
	check_count(pOut->acct_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();
}

void CDBConnection::execute(const TCustomerPositionFrame2Input *pIn,
		TCustomerPositionFrame2Output *pOut)
{
	int i_hist_dts;
	int i_hist_len;
	int i_qty;
	int i_symbol;
	int i_trade_id;
	int i_trade_status;

	ostringstream osSQL;
	osSQL << "SELECT * FROM CustomerPositionFrame2(" << pIn->acct_id << ")";

	PGresult *res = exec(osSQL.str().c_str());
	i_hist_dts = PQfnumber(res, "hist_dts");
	i_hist_len = PQfnumber(res, "hist_len");
	i_qty = PQfnumber(res, "qty");
	i_symbol = PQfnumber(res, "symbol");
	i_trade_id = PQfnumber(res, "trade_id");
	i_trade_status = PQfnumber(res, "trade_status");

	pOut->hist_len = atoi(PQgetvalue(res, 0, i_hist_len));

	vector<string> vAux;
	vector<string>::iterator p;
	int i;

	TokenizeSmart(PQgetvalue(res, 0, i_hist_dts), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->hist_dts[i].year,
				&pOut->hist_dts[i].month,
				&pOut->hist_dts[i].day,
				&pOut->hist_dts[i].hour,
				&pOut->hist_dts[i].minute,
				&pOut->hist_dts[i].second);
		++i;
	}
	check_count(pOut->hist_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_qty), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->qty[i] = atoi((*p).c_str());
		++i;
	}
	check_count(pOut->hist_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_symbol), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->symbol[i], (*p).c_str(), cSYMBOL_len);
		pOut->symbol[i][cSYMBOL_len] = '\0';
		++i;
	}
	check_count(pOut->hist_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_trade_id), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_id[i] = atol((*p).c_str());
		++i;
	}
	check_count(pOut->hist_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_trade_status), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_status[i], (*p).c_str(), cST_NAME_len);
		pOut->trade_status[i][cST_NAME_len] = '\0';
		++i;
	}
	check_count(pOut->hist_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();
}

void CDBConnection::execute(const TDataMaintenanceFrame1Input *pIn)
{
	ostringstream osSQL;
	osSQL << "SELECT * FROM DataMaintenanceFrame1(" <<
			pIn->acct_id << ", " <<
			pIn->c_id << ", " <<
			pIn->co_id << ", " <<
			pIn->day_of_month << ", '" <<
			pIn->symbol << "', '" <<
			pIn->table_name << "', '" <<
			pIn->tx_id << "', " <<
			pIn->vol_incr << ")";

	exec(osSQL.str().c_str());
}


void CDBConnection::execute(const TMarketFeedFrame1Input *pIn,
		TMarketFeedFrame1Output *pOut, CSendToMarketInterface *pMarketExchange)
{
	int i_num_updated;
	int i_send_len;
	int i_symbol;
	int i_trade_id;
	int i_price_quote;
	int i_trade_qty;
	int i_trade_type;

	ostringstream osSymbol, osPrice, osQty;

	for (unsigned int i = 0;
			i < (sizeof(pIn->Entries) / sizeof(pIn->Entries[0])); ++i) {
		if (i == 0) {
			osSymbol << "\"" << pIn->Entries[i].symbol;
			osPrice << pIn->Entries[i].price_quote;
			osQty << pIn->Entries[i].trade_qty;
		} else {
			osSymbol << "\",\"" << pIn->Entries[i].symbol;
			osPrice << "," << pIn->Entries[i].price_quote;
			osQty << "," << pIn->Entries[i].trade_qty;
		}
	}
	osSymbol << "\"";

	ostringstream osSQL;
	osSQL << "SELECT * FROM MarketFeedFrame1('{" <<
			osPrice.str() << "}','" <<
			pIn->StatusAndTradeType.status_submitted << "','{" <<
			osSymbol.str() << "}', '{" <<
			osQty.str() << "}','" <<
			pIn->StatusAndTradeType.type_limit_buy << "','" <<
			pIn->StatusAndTradeType.type_limit_sell << "','" <<
			pIn->StatusAndTradeType.type_stop_loss << "')";

	PGresult *res = exec(osSQL.str().c_str());
	i_num_updated = PQfnumber(res, "num_updated");
	i_send_len = PQfnumber(res, "send_len");
	i_symbol = PQfnumber(res, "symbol");
	i_trade_id = PQfnumber(res, "trade_id");
	i_price_quote = PQfnumber(res, "price_quote");
	i_trade_qty = PQfnumber(res, "trade_qty");
	i_trade_type = PQfnumber(res, "trade_type");

	pOut->send_len = atoi(PQgetvalue(res, 0, i_send_len));

	vector<string> v1;
	vector<string>::iterator p1;
	vector<string> v2;
	vector<string>::iterator p2;
	vector<string> v3;
	vector<string>::iterator p3;
	vector<string> v4;
	vector<string>::iterator p4;
	vector<string> v5;
	vector<string>::iterator p5;

	TokenizeSmart(PQgetvalue(res, 0, i_symbol), v1);
	TokenizeSmart(PQgetvalue(res, 0, i_trade_id), v2);
	TokenizeSmart(PQgetvalue(res, 0, i_price_quote), v3);
	TokenizeSmart(PQgetvalue(res, 0, i_trade_qty), v4);
	TokenizeSmart(PQgetvalue(res, 0, i_trade_type), v5);

	// FIXME: Consider altering to match spec.  Since PostgreSQL cannot
	// control transaction from within a stored function and because we're
	// making the call to the Market Exchange Emulator from outside
	// the transaction, consider altering the code to call a stored
	// function once per symbol to match the transaction rules in
	// the spec.
	int i = 0;
	bool bSent;
	for (p1 = v1.begin(), p2 = v2.begin(), p3 = v3.begin(), p4 = v4.begin(),
			p5 = v5.begin(); p1 != v1.end(); ++p1, ++p2, ++p3, ++p4) {
		strncpy(m_TriggeredLimitOrders.symbol, (*p1).c_str(), cSYMBOL_len);
		m_TriggeredLimitOrders.symbol[cSYMBOL_len] = '\0';
		m_TriggeredLimitOrders.trade_id = atol((*p2).c_str());
		m_TriggeredLimitOrders.price_quote = atof((*p3).c_str());
		m_TriggeredLimitOrders.trade_qty = atoi((*p4).c_str());
		strncpy(m_TriggeredLimitOrders.trade_type_id, (*p5).c_str(),
				cTT_ID_len);
		m_TriggeredLimitOrders.trade_type_id[cTT_ID_len] = '\0';

		bSent = pMarketExchange->SendToMarketFromFrame(
				m_TriggeredLimitOrders);
		++i;
	}
	check_count(pOut->send_len, i, __FILE__, __LINE__);
}

void CDBConnection::execute(const TMarketWatchFrame1Input *pIn,
		TMarketWatchFrame1Output *pOut)
{
	ostringstream osSQL;
	osSQL << "SELECT * FROM MarketWatchFrame1(" <<
			pIn->acct_id << "," <<
			pIn->c_id << "," <<
			pIn->ending_co_id << ",'" <<
			pIn->industry_name << "','" <<
			pIn->start_day.year << "-" <<
			pIn->start_day.month << "-" <<
			pIn->start_day.day << "'," <<
			pIn->starting_co_id << ")";

	PGresult *res = exec(osSQL.str().c_str());

	pOut->pct_change = atof(PQgetvalue(res, 0, 0));
}

void CDBConnection::execute(const TSecurityDetailFrame1Input *pIn,
		TSecurityDetailFrame1Output *pOut)
{
	int i_s52_wk_high;
	int i_s52_wk_high_date;
	int i_s52_wk_low;
	int i_s52_wk_low_date;
	int i_ceo_name;
	int i_co_ad_cty;
	int i_co_ad_div;
	int i_co_ad_line1;
	int i_co_ad_line2;
	int i_co_ad_town;
	int i_co_ad_zip;
	int i_co_desc;
	int i_co_name;
	int i_co_st_id;
	int i_cp_co_name;
	int i_cp_in_name;
	int i_day;
	int i_day_len;
	int i_divid;
	int i_ex_ad_cty;
	int i_ex_ad_div;
	int i_ex_ad_line1;
	int i_ex_ad_line2;
	int i_ex_ad_town;
	int i_ex_ad_zip;
	int i_ex_close;
	int i_ex_date;
	int i_ex_desc;
	int i_ex_name;
	int i_ex_num_symb;
	int i_ex_open;
	int i_fin;
	int i_fin_len;
	int i_last_open;
	int i_last_price;
	int i_last_vol;
	int i_news;
	int i_news_len;
	int i_num_out;
	int i_open_date;
	int i_pe_ratio;
	int i_s_name;
	int i_sp_rate;
	int i_start_date;
	int i_yield;

	ostringstream osSQL;
	osSQL << "SELECT * FROM SecurityDetailFrame1(" <<
			(pIn->access_lob_flag == 0 ? "false" : "true") << "," <<
			pIn->max_rows_to_return << ",'" <<
			pIn->start_day.year << "-" <<
			pIn->start_day.month << "-" <<
			pIn->start_day.day << "','" <<
			pIn->symbol << "')";

	PGresult *res = exec(osSQL.str().c_str());
	i_s52_wk_high = PQfnumber(res, "x52_wk_high");
	i_s52_wk_high_date = PQfnumber(res, "x52_wk_high_date");
	i_s52_wk_low = PQfnumber(res, "x52_wk_low");
	i_s52_wk_low_date = PQfnumber(res, "x52_wk_low_date");
	i_ceo_name = PQfnumber(res, "ceo_name");
	i_co_ad_cty = PQfnumber(res, "co_ad_ctry");
	i_co_ad_div = PQfnumber(res, "co_ad_div");
	i_co_ad_line1 = PQfnumber(res, "co_ad_line1");
	i_co_ad_line2 = PQfnumber(res, "co_ad_line2");
	i_co_ad_town = PQfnumber(res, "co_ad_town");
	i_co_ad_zip = PQfnumber(res, "co_ad_zip");
	i_co_desc = PQfnumber(res, "co_desc");
	i_co_name = PQfnumber(res, "co_name");
	i_co_st_id = PQfnumber(res, "co_st_id");
	i_cp_co_name = PQfnumber(res, "cp_co_name");
	i_cp_in_name = PQfnumber(res, "cp_in_name");
	i_day = PQfnumber(res, "day");
	i_day_len = PQfnumber(res, "day_len");
	i_divid = PQfnumber(res, "divid");
	i_ex_ad_cty = PQfnumber(res, "ex_ad_ctry");
	i_ex_ad_div = PQfnumber(res, "ex_ad_div");
	i_ex_ad_line1 = PQfnumber(res, "ex_ad_line1");
	i_ex_ad_line2 = PQfnumber(res, "ex_ad_line2");
	i_ex_ad_town = PQfnumber(res, "ex_ad_town");
	i_ex_ad_zip = PQfnumber(res, "ex_ad_zip");
	i_ex_close = PQfnumber(res, "ex_close");
	i_ex_date = PQfnumber(res, "ex_date");
	i_ex_desc = PQfnumber(res, "ex_desc");
	i_ex_name = PQfnumber(res, "ex_name");
	i_ex_num_symb = PQfnumber(res, "ex_num_symb");
	i_ex_open = PQfnumber(res, "ex_open");
	i_fin = PQfnumber(res, "fin");
	i_fin_len = PQfnumber(res, "fin_len");
	i_last_open = PQfnumber(res, "last_open");
	i_last_price = PQfnumber(res, "last_price");
	i_last_vol = PQfnumber(res, "last_vol");
	i_news = PQfnumber(res, "news");
	i_news_len = PQfnumber(res, "news_len");
	i_num_out = PQfnumber(res, "num_out");
	i_open_date = PQfnumber(res, "open_date");
	i_pe_ratio = PQfnumber(res, "pe_ratio");
	i_s_name = PQfnumber(res, "s_name");
	i_sp_rate = PQfnumber(res, "sp_rate");
	i_start_date = PQfnumber(res, "start_date");
	i_yield = PQfnumber(res, "yield");

	pOut->fin_len = atoi(PQgetvalue(res, 0, i_fin_len));
	pOut->day_len = atoi(PQgetvalue(res, 0, i_day_len));
	pOut->news_len = atoi(PQgetvalue(res, 0, i_news_len));

	pOut->s52_wk_high = atof(PQgetvalue(res, 0, i_s52_wk_high));
	sscanf(PQgetvalue(res, 0, i_s52_wk_high_date), "%hd-%hd-%hd",
			&pOut->s52_wk_high_date.year,
			&pOut->s52_wk_high_date.month,
			&pOut->s52_wk_high_date.day);
	pOut->s52_wk_low = atof(PQgetvalue(res, 0, i_s52_wk_low));
	sscanf(PQgetvalue(res, 0, i_s52_wk_low_date), "%hd-%hd-%hd",
			&pOut->s52_wk_low_date.year,
			&pOut->s52_wk_low_date.month,
			&pOut->s52_wk_low_date.day);

	strncpy(pOut->ceo_name, PQgetvalue(res, 0, i_ceo_name), cCEO_NAME_len);
	pOut->ceo_name[cCEO_NAME_len] = '\0';
	strncpy(pOut->co_ad_cty, PQgetvalue(res, 0, i_co_ad_cty), cAD_CTRY_len);
	pOut->co_ad_cty[cAD_CTRY_len] = '\0';
	strncpy(pOut->co_ad_div, PQgetvalue(res, 0, i_co_ad_div), cAD_DIV_len);
	pOut->co_ad_div[cAD_DIV_len] = '\0';
	strncpy(pOut->co_ad_line1, PQgetvalue(res, 0, i_co_ad_line1), cAD_LINE_len);
	pOut->co_ad_line1[cAD_LINE_len] = '\0';
	strncpy(pOut->co_ad_line2, PQgetvalue(res, 0, i_co_ad_line2), cAD_LINE_len);
	pOut->co_ad_line2[cAD_LINE_len] = '\0';
	strncpy(pOut->co_ad_town, PQgetvalue(res, 0, i_co_ad_town), cAD_TOWN_len);
	pOut->co_ad_town[cAD_TOWN_len] = '\0';
	strncpy(pOut->co_ad_zip, PQgetvalue(res, 0, i_co_ad_zip), cAD_ZIP_len);
	pOut->co_ad_zip[cAD_ZIP_len] = '\0';
	strncpy(pOut->co_desc, PQgetvalue(res, 0, i_co_desc), cCO_DESC_len);
	pOut->co_desc[cCO_DESC_len] = '\0';
	strncpy(pOut->co_name, PQgetvalue(res, 0, i_co_name), cCO_NAME_len);
	pOut->co_name[cCO_NAME_len] = '\0';
	strncpy(pOut->co_st_id, PQgetvalue(res, 0, i_co_st_id), cST_ID_len);
	pOut->co_st_id[cST_ID_len] = '\0';

	vector<string> vAux;
	vector<string>::iterator p;
	TokenizeSmart(PQgetvalue(res, 0, i_cp_co_name), vAux);
	int i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->cp_co_name[i], (*p).c_str(), cCO_NAME_len);
		pOut->cp_co_name[i][cCO_NAME_len] = '\0';
		++i;
	}
	// FIXME: The stored functions for PostgreSQL are designed to return 3
	// items in the array, even though it's not required.
	check_count(3, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_cp_in_name), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->cp_in_name[i], (*p).c_str(), cIN_NAME_len);
		pOut->cp_in_name[i][cIN_NAME_len] = '\0';
		++i;
	}

	// FIXME: The stored functions for PostgreSQL are designed to return 3
	// items in the array, even though it's not required.
	check_count(3, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeArray(PQgetvalue(res, 0, i_day), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		vector<string> v2;
		vector<string>::iterator p2;

		TokenizeSmart((*p).c_str(), v2);

		p2 = v2.begin();
		sscanf((*p2++).c_str(), "%hd-%hd-%hd",
				&pOut->day[i].date.year,
				&pOut->day[i].date.month,
				&pOut->day[i].date.day);
		pOut->day[i].close = atof((*p2++).c_str());
		pOut->day[i].high = atof((*p2++).c_str());
		pOut->day[i].low = atof((*p2++).c_str());
		pOut->day[i].vol = atoi((*p2++).c_str());
		++i;
		v2.clear();
	}
	check_count(pOut->day_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->divid = atof(PQgetvalue(res, 0, i_divid));

	strncpy(pOut->ex_ad_cty, PQgetvalue(res, 0, i_ex_ad_cty), cAD_CTRY_len);
	pOut->ex_ad_cty[cAD_CTRY_len] = '\0';
	strncpy(pOut->ex_ad_div, PQgetvalue(res, 0, i_ex_ad_div), cAD_DIV_len);
	pOut->ex_ad_div[cAD_DIV_len] = '\0';
	strncpy(pOut->ex_ad_line1, PQgetvalue(res, 0, i_ex_ad_line1), cAD_LINE_len);
	pOut->ex_ad_line1[cAD_LINE_len] = '\0';
	strncpy(pOut->ex_ad_line2, PQgetvalue(res, 0, i_ex_ad_line2), cAD_LINE_len);
	pOut->ex_ad_line2[cAD_LINE_len] = '\0';
	strncpy(pOut->ex_ad_town, PQgetvalue(res, 0, i_ex_ad_town), cAD_TOWN_len);
	pOut->ex_ad_town[cAD_TOWN_len]  = '\0';
	strncpy(pOut->ex_ad_zip, PQgetvalue(res, 0, i_ex_ad_zip), cAD_ZIP_len);
	pOut->ex_ad_zip[cAD_ZIP_len] = '\0';
	pOut->ex_close = atoi(PQgetvalue(res, 0, i_ex_close));
	sscanf(PQgetvalue(res, 0, i_ex_date), "%hd-%hd-%hd",
			&pOut->ex_date.year,
			&pOut->ex_date.month,
			&pOut->ex_date.day);
	strncpy(pOut->ex_desc, PQgetvalue(res, 0, i_ex_desc), cEX_DESC_len);
	pOut->ex_desc[cEX_DESC_len] = '\0';
	strncpy(pOut->ex_name, PQgetvalue(res, 0, i_ex_name), cEX_NAME_len);
	pOut->ex_name[cEX_NAME_len] = '\0';
	pOut->ex_num_symb = atoi(PQgetvalue(res, 0, i_ex_num_symb));
	pOut->ex_open = atoi(PQgetvalue(res, 0, i_ex_open));

	TokenizeArray(PQgetvalue(res, 0, i_fin), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		vector<string> v2;
		vector<string>::iterator p2;

		TokenizeSmart((*p).c_str(), v2);

		p2 = v2.begin();
		pOut->fin[i].year = atoi((*p2++).c_str());
		pOut->fin[i].qtr = atoi((*p2++).c_str());
		sscanf((*p2++).c_str(), "%hd-%hd-%hd",
				&pOut->fin[i].start_date.year,
				&pOut->fin[i].start_date.month,
				&pOut->fin[i].start_date.day);
		pOut->fin[i].rev = atof((*p2++).c_str());
		pOut->fin[i].net_earn = atof((*p2++).c_str());
		pOut->fin[i].basic_eps = atof((*p2++).c_str());
		pOut->fin[i].dilut_eps = atof((*p2++).c_str());
		pOut->fin[i].margin = atof((*p2++).c_str());
		pOut->fin[i].invent = atof((*p2++).c_str());
		pOut->fin[i].assets = atof((*p2++).c_str());
		pOut->fin[i].liab = atof((*p2++).c_str());
		pOut->fin[i].out_basic = atof((*p2++).c_str());
		pOut->fin[i].out_dilut = atof((*p2++).c_str());
		++i;
		v2.clear();
	}
	check_count(pOut->fin_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->last_open = atof(PQgetvalue(res, 0, i_last_open));
	pOut->last_price = atof(PQgetvalue(res, 0, i_last_price));
	pOut->last_vol = atoi(PQgetvalue(res, 0, i_last_vol));

	TokenizeArray(PQgetvalue(res, 0, i_news), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		vector<string> v2;
		vector<string>::iterator p2;

		TokenizeSmart((*p).c_str(), v2);

		p2 = v2.begin();
		// FIXME: Postgresql can actually return 5 times the amount of data due
		// to escaped characters.  Cap the data at the length that EGen defines
		// it and hope it isn't a problem for continuing the test correctly.
		strncpy(pOut->news[i].item, (*p2++).c_str(), cNI_ITEM_len);
		pOut->news[i].item[cNI_ITEM_len] = '\0';
		sscanf((*p2++).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->news[i].dts.year,
				&pOut->news[i].dts.month,
				&pOut->news[i].dts.day,
				&pOut->news[i].dts.hour,
				&pOut->news[i].dts.minute,
				&pOut->news[i].dts.second);
		strncpy(pOut->news[i].src, (*p2++).c_str(), cNI_SOURCE_len);
		pOut->news[i].src[cNI_SOURCE_len] = '\0';
		strncpy(pOut->news[i].auth, (*p2++).c_str(), cNI_AUTHOR_len);
		pOut->news[i].auth[cNI_AUTHOR_len] = '\0';
		strncpy(pOut->news[i].headline, (*p2++).c_str(), cNI_HEADLINE_len);
		pOut->news[i].headline[cNI_HEADLINE_len] = '\0';
		strncpy(pOut->news[i].summary, (*p2++).c_str(), cNI_SUMMARY_len);
		pOut->news[i].summary[cNI_SUMMARY_len] = '\0';
		++i;
		v2.clear();
	}
	check_count(pOut->news_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	sscanf(PQgetvalue(res, 0, i_open_date), "%hd-%hd-%hd",
			&pOut->open_date.year,
			&pOut->open_date.month,
			&pOut->open_date.day);
	pOut->pe_ratio = atof(PQgetvalue(res, 0, i_pe_ratio));
	strncpy(pOut->s_name, PQgetvalue(res, 0, i_s_name), cS_NAME_len);
	pOut->s_name[cS_NAME_len] = '\0';
	pOut->num_out = atol(PQgetvalue(res, 0, i_num_out));
	strncpy(pOut->sp_rate, PQgetvalue(res, 0, i_sp_rate), cSP_RATE_len);
	pOut->sp_rate[cSP_RATE_len] = '\0';
	sscanf(PQgetvalue(res, 0, i_start_date), "%hd-%hd-%hd",
			&pOut->start_date.year,
			&pOut->start_date.month,
			&pOut->start_date.day);
	pOut->yield = atof(PQgetvalue(res, 0, i_yield));
}

void CDBConnection::execute(const TTradeCleanupFrame1Input *pIn)
{
	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeCleanupFrame1('" <<
			pIn->st_canceled_id << "','" <<
			pIn->st_pending_id << "','" <<
			pIn->st_submitted_id << "'," <<
			pIn->start_trade_id << ")";

	exec(osSQL.str().c_str());
}

void CDBConnection::execute(const TTradeLookupFrame1Input *pIn,
		TTradeLookupFrame1Output *pOut)
{
	int i_bid_price;
	int i_cash_transaction_amount;
	int i_cash_transaction_dts;
	int i_cash_transaction_name;
	int i_exec_name;
	int i_is_cash;
	int i_is_market;
	int i_num_found;
	int i_settlement_amount;
	int i_settlement_cash_due_date;
	int i_settlement_cash_type;
	int i_trade_history_dts;
	int i_trade_history_status_id;
	int i_trade_price;

	ostringstream osTrades;
	int i = 0;
	osTrades << pIn->trade_id[i];
	for ( i = 1; i < pIn->max_trades; i++) {
		osTrades << "," << pIn->trade_id[i];
	}

	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeLookupFrame1(" <<
			pIn->max_trades << ",'{" <<
			osTrades.str() << "}')";

	PGresult *res = exec(osSQL.str().c_str());
	i_bid_price = PQfnumber(res, "bid_price");
	i_cash_transaction_amount = PQfnumber(res, "cash_transaction_amount");
	i_cash_transaction_dts = PQfnumber(res, "cash_transaction_dts");
	i_cash_transaction_name = PQfnumber(res, "cash_transaction_name");
	i_exec_name = PQfnumber(res, "exec_name");
	i_is_cash = PQfnumber(res, "is_cash");
	i_is_market = PQfnumber(res, "is_market");
	i_num_found = PQfnumber(res, "num_found");
	i_settlement_amount = PQfnumber(res, "settlement_amount");
	i_settlement_cash_due_date = PQfnumber(res, "settlement_cash_due_date");
	i_settlement_cash_type = PQfnumber(res, "settlement_cash_type");
	i_trade_history_dts = PQfnumber(res, "trade_history_dts");
	i_trade_history_status_id = PQfnumber(res, "trade_history_status_id");
	i_trade_price = PQfnumber(res, "trade_price");

	pOut->num_found = atoi(PQgetvalue(res, 0, i_num_found));

	vector<string> vAux;
	vector<string>::iterator p;

	TokenizeSmart(PQgetvalue(res, 0, i_bid_price), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].bid_price = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_cash_transaction_amount), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].cash_transaction_amount = atof((*p).c_str());
		++i;
	}
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_cash_transaction_dts), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->trade_info[i].cash_transaction_dts.year,
				&pOut->trade_info[i].cash_transaction_dts.month,
				&pOut->trade_info[i].cash_transaction_dts.day,
				&pOut->trade_info[i].cash_transaction_dts.hour,
				&pOut->trade_info[i].cash_transaction_dts.minute,
				&pOut->trade_info[i].cash_transaction_dts.second);
		++i;
	}
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_cash_transaction_name), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].cash_transaction_name, (*p).c_str(),
				cCT_NAME_len);
		pOut->trade_info[i].cash_transaction_name[cCT_NAME_len] = '\0';
		++i;
	}
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_exec_name), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].exec_name, (*p).c_str(), cEXEC_NAME_len);
		pOut->trade_info[i].exec_name[cEXEC_NAME_len] = '\0';
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_is_cash), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].is_cash = atoi((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_is_market), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].is_market = atoi((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_settlement_amount), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].settlement_amount = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_settlement_cash_due_date), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->trade_info[i].settlement_cash_due_date.year,
				&pOut->trade_info[i].settlement_cash_due_date.month,
				&pOut->trade_info[i].settlement_cash_due_date.day,
				&pOut->trade_info[i].settlement_cash_due_date.hour,
				&pOut->trade_info[i].settlement_cash_due_date.minute,
				&pOut->trade_info[i].settlement_cash_due_date.second);
		++i;
	}
	if (!check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__)) {
		cout << "*** settlement_cash_due_date = " <<
				PQgetvalue(res, 0, i_settlement_cash_due_date) << endl;
	}
	vAux.clear();
	TokenizeSmart(PQgetvalue(res, 0, i_settlement_cash_type), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].settlement_cash_type, (*p).c_str(),
				cSE_CASH_TYPE_len);
		pOut->trade_info[i].settlement_cash_type[cSE_CASH_TYPE_len] = '\0';
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeArray(PQgetvalue(res, 0, i_trade_history_dts), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		vector<string> v2;
		vector<string>::iterator p2;
		TokenizeSmart((*p).c_str(), v2);
		int j = 0;
		for (p2 = v2.begin(); p2 != v2.end(); ++p2) {
			sscanf((*p2).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
					&pOut->trade_info[i].trade_history_dts[j].year,
					&pOut->trade_info[i].trade_history_dts[j].month,
					&pOut->trade_info[i].trade_history_dts[j].day,
					&pOut->trade_info[i].trade_history_dts[j].hour,
					&pOut->trade_info[i].trade_history_dts[j].minute,
					&pOut->trade_info[i].trade_history_dts[j].second);
			++j;
		}
		++i;
	}
	vAux.clear();

	TokenizeArray(PQgetvalue(res, 0, i_trade_history_status_id), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		vector<string> v2;
		vector<string>::iterator p2;
		TokenizeSmart((*p).c_str(), v2);
		int j = 0;
		for (p2 = v2.begin(); p2 != v2.end(); ++p2) {
			strncpy(pOut->trade_info[i].trade_history_status_id[j],
					(*p2).c_str(), cTH_ST_ID_len);
			pOut->trade_info[i].trade_history_status_id[j][cTH_ST_ID_len] =
					'\0';
			++j;
		}
		++i;
	}
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_trade_price), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].trade_price = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();
}

void CDBConnection::execute(const TTradeLookupFrame2Input *pIn,
		TTradeLookupFrame2Output *pOut)
{
	int i_bid_price;
	int i_cash_transaction_amount;
	int i_cash_transaction_dts;
	int i_cash_transaction_name;
	int i_exec_name;
	int i_is_cash;
	int i_num_found;
	int i_settlement_amount;
	int i_settlement_cash_due_date;
	int i_settlement_cash_type;
	int i_trade_history_dts;
	int i_trade_history_status_id;
	int i_trade_list;
	int i_trade_price;

	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeLookupFrame2(" <<
			pIn->acct_id << ",'" <<
			pIn->end_trade_dts.year << "-" <<
			pIn->end_trade_dts.month << "-" <<
			pIn->end_trade_dts.day << " " <<
			pIn->end_trade_dts.hour << ":" <<
			pIn->end_trade_dts.minute << ":" <<
			pIn->end_trade_dts.second << "'," <<
			pIn->max_trades << ",'" <<
			pIn->start_trade_dts.year << "-" <<
			pIn->start_trade_dts.month << "-" <<
			pIn->start_trade_dts.day << " " <<
			pIn->start_trade_dts.hour << ":" <<
			pIn->start_trade_dts.minute << ":" <<
			pIn->start_trade_dts.second << "')";

	PGresult *res = exec(osSQL.str().c_str());

	i_bid_price = PQfnumber(res, "bid_price");
	i_cash_transaction_amount = PQfnumber(res, "cash_transaction_amount");
	i_cash_transaction_dts = PQfnumber(res, "cash_transaction_dts");
	i_cash_transaction_name = PQfnumber(res, "cash_transaction_name");
	i_exec_name = PQfnumber(res, "exec_name");
	i_is_cash = PQfnumber(res, "is_cash");
	i_num_found = PQfnumber(res, "num_found");
	i_settlement_amount = PQfnumber(res, "settlement_amount");
	i_settlement_cash_due_date = PQfnumber(res, "settlement_cash_due_date");
	i_settlement_cash_type = PQfnumber(res, "settlement_cash_type");
	i_trade_history_dts = PQfnumber(res, "trade_history_dts");
	i_trade_history_status_id = PQfnumber(res, "trade_history_status_id");
	i_trade_list = PQfnumber(res, "trade_list");
	i_trade_price = PQfnumber(res, "trade_price");

	pOut->num_found = atoi(PQgetvalue(res, 0, i_num_found));

	vector<string> vAux;
	vector<string>::iterator p;

	TokenizeSmart(PQgetvalue(res, 0, i_bid_price), vAux);
	int i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].bid_price = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_cash_transaction_amount), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].cash_transaction_amount = atof((*p).c_str());
		++i;
	}
	// FIXME: According to spec, this may not match the returned number found?
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_cash_transaction_dts), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->trade_info[i].cash_transaction_dts.year,
				&pOut->trade_info[i].cash_transaction_dts.month,
				&pOut->trade_info[i].cash_transaction_dts.day,
				&pOut->trade_info[i].cash_transaction_dts.hour,
				&pOut->trade_info[i].cash_transaction_dts.minute,
				&pOut->trade_info[i].cash_transaction_dts.second);
		++i;
	}
	// FIXME: According to spec, this may not match the returned number found?
   vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_cash_transaction_name), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].cash_transaction_name, (*p).c_str(),
				cCT_NAME_len);
		pOut->trade_info[i].cash_transaction_name[cCT_NAME_len] = '\0';
		++i;
	}
	// FIXME: According to spec, this may not match the returned number found?
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_exec_name), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].exec_name, (*p).c_str(), cEXEC_NAME_len);
		pOut->trade_info[i].exec_name[cEXEC_NAME_len] = '\0';
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_is_cash), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].is_cash = atoi((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_settlement_amount), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].settlement_amount = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_settlement_cash_due_date), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->trade_info[i].settlement_cash_due_date.year,
				&pOut->trade_info[i].settlement_cash_due_date.month,
				&pOut->trade_info[i].settlement_cash_due_date.day,
				&pOut->trade_info[i].settlement_cash_due_date.hour,
				&pOut->trade_info[i].settlement_cash_due_date.minute,
				&pOut->trade_info[i].settlement_cash_due_date.second);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_settlement_cash_type), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].settlement_cash_type, (*p).c_str(),
				cSE_CASH_TYPE_len);
		pOut->trade_info[i].settlement_cash_type[cSE_CASH_TYPE_len] = '\0';
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeArray(PQgetvalue(res, 0, i_trade_history_dts), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		vector<string> v2;
		vector<string>::iterator p2;
		TokenizeSmart((*p).c_str(), v2);
		int j = 0;
		for (p2 = v2.begin(); p2 != v2.end(); ++p2) {
			sscanf((*p2).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
						&pOut->trade_info[i].trade_history_dts[j].year,
						&pOut->trade_info[i].trade_history_dts[j].month,
						&pOut->trade_info[i].trade_history_dts[j].day,
						&pOut->trade_info[i].trade_history_dts[j].hour,
						&pOut->trade_info[i].trade_history_dts[j].minute,
						&pOut->trade_info[i].trade_history_dts[j].second);
			++j;
		}
		++i;
	}
	vAux.clear();

	TokenizeArray(PQgetvalue(res, 0, i_trade_history_status_id), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		vector<string> v2;
		vector<string>::iterator p2;
		TokenizeSmart((*p).c_str(), v2);
		int j = 0;
		for (p2 = v2.begin(); p2 != v2.end(); ++p2) {
			strncpy(pOut->trade_info[i].trade_history_status_id[j],
						(*p2).c_str(), cTH_ST_ID_len);
			pOut->trade_info[i].trade_history_status_id[j][cTH_ST_ID_len] =
					'\0';
			++j;
		}
		++i;
	}
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_trade_list), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].trade_id = atol((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_trade_price), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].trade_price = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();
}

void CDBConnection::execute(const TTradeLookupFrame3Input *pIn,
		TTradeLookupFrame3Output *pOut)
{
	int i_acct_id;
	int i_cash_transaction_amount;
	int i_cash_transaction_dts;
	int i_cash_transaction_name;
	int i_exec_name;
	int i_is_cash;
	int i_num_found;
	int i_price;
	int i_quantity;
	int i_settlement_amount;
	int i_settlement_cash_due_date;
	int i_settlement_cash_type;
	int i_trade_dts;
	int i_trade_history_dts;
	int i_trade_history_status_id;
	int i_trade_list;
	int i_trade_type;

	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeLookupFrame3('" <<
			pIn->end_trade_dts.year << "-" <<
			pIn->end_trade_dts.month << "-" <<
			pIn->end_trade_dts.day << " " <<
			pIn->end_trade_dts.hour << ":" <<
			pIn->end_trade_dts.minute << ":" <<
			pIn->end_trade_dts.second << "'," <<
			pIn->max_acct_id << "," <<
			pIn->max_trades << ",'" <<
			pIn->start_trade_dts.year << "-" <<
			pIn->start_trade_dts.month << "-" <<
			pIn->start_trade_dts.day << " " <<
			pIn->start_trade_dts.hour << ":" <<
			pIn->start_trade_dts.minute << ":" <<
			pIn->start_trade_dts.second << "','" <<
			pIn->symbol << "')";

	PGresult *res = exec(osSQL.str().c_str());
	i_acct_id = PQfnumber(res, "acct_id");
	i_cash_transaction_amount = PQfnumber(res, "cash_transaction_amount");
	i_cash_transaction_dts = PQfnumber(res, "cash_transaction_dts");
	i_cash_transaction_name = PQfnumber(res, "cash_transaction_name");
	i_exec_name = PQfnumber(res, "exec_name");
	i_is_cash = PQfnumber(res, "is_cash");
	i_num_found = PQfnumber(res, "num_found");
	i_price = PQfnumber(res, "price");
	i_quantity = PQfnumber(res, "quantity");
	i_settlement_amount = PQfnumber(res, "settlement_amount");
	i_settlement_cash_due_date = PQfnumber(res, "settlement_cash_due_date");
	i_settlement_cash_type = PQfnumber(res, "settlement_cash_type");
	i_trade_dts = PQfnumber(res, "trade_dts");
	i_trade_history_dts = PQfnumber(res, "trade_history_dts");
	i_trade_history_status_id = PQfnumber(res, "trade_history_status_id");
	i_trade_list = PQfnumber(res, "trade_list");
	i_trade_type = PQfnumber(res, "trade_type");

	pOut->num_found = atoi(PQgetvalue(res, 0, i_num_found));

	vector<string> vAux;
	vector<string>::iterator p;

	TokenizeSmart(PQgetvalue(res, 0, i_acct_id), vAux);
	int i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].acct_id = atol((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_cash_transaction_amount), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].cash_transaction_amount = atof((*p).c_str());
		++i;
	}
	// FIXME: According to spec, this may not match the returned number found?
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_cash_transaction_dts), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->trade_info[i].cash_transaction_dts.year,
				&pOut->trade_info[i].cash_transaction_dts.month,
				&pOut->trade_info[i].cash_transaction_dts.day,
				&pOut->trade_info[i].cash_transaction_dts.hour,
				&pOut->trade_info[i].cash_transaction_dts.minute,
				&pOut->trade_info[i].cash_transaction_dts.second);
		++i;
	}
	// FIXME: According to spec, this may not match the returned number found?
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_cash_transaction_name), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].cash_transaction_name, (*p).c_str(),
				cCT_NAME_len);
		pOut->trade_info[i].cash_transaction_name[cCT_NAME_len] = '\0';
		++i;
	}
	// FIXME: According to spec, this may not match the returned number found?
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_exec_name), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].exec_name, (*p).c_str(),
				cEXEC_NAME_len);
		pOut->trade_info[i].exec_name[cEXEC_NAME_len] = '\0';
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_is_cash), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].is_cash = atoi((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_price), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].price = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_quantity), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].quantity = atoi((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_settlement_amount), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].settlement_amount = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_settlement_cash_due_date), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->trade_info[i].settlement_cash_due_date.year,
				&pOut->trade_info[i].settlement_cash_due_date.month,
				&pOut->trade_info[i].settlement_cash_due_date.day,
				&pOut->trade_info[i].settlement_cash_due_date.hour,
				&pOut->trade_info[i].settlement_cash_due_date.minute,
				&pOut->trade_info[i].settlement_cash_due_date.second);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_settlement_cash_type), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].settlement_cash_type, (*p).c_str(),
				cSE_CASH_TYPE_len);
		pOut->trade_info[i].settlement_cash_type[cSE_CASH_TYPE_len] = '\0';
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_trade_dts), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->trade_info[i].trade_dts.year,
				&pOut->trade_info[i].trade_dts.month,
				&pOut->trade_info[i].trade_dts.day,
				&pOut->trade_info[i].trade_dts.hour,
				&pOut->trade_info[i].trade_dts.minute,
				&pOut->trade_info[i].trade_dts.second);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeArray(PQgetvalue(res, 0, i_trade_history_dts), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		vector<string> v2;
		vector<string>::iterator p2;
		TokenizeSmart((*p).c_str(), v2);
		int j = 0;
		for (p2 = v2.begin(); p2 != v2.end(); ++p2) {
			sscanf((*p2).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
					&pOut->trade_info[i].trade_history_dts[j].year,
					&pOut->trade_info[i].trade_history_dts[j].month,
					&pOut->trade_info[i].trade_history_dts[j].day,
					&pOut->trade_info[i].trade_history_dts[j].hour,
					&pOut->trade_info[i].trade_history_dts[j].minute,
					&pOut->trade_info[i].trade_history_dts[j].second);
			++j;
		}
		++i;
	}
	vAux.clear();

	TokenizeArray(PQgetvalue(res, 0, i_trade_history_status_id), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		vector<string> v2;
		vector<string>::iterator p2;
		TokenizeSmart((*p).c_str(), v2);
		int j = 0;
		for (p2 = v2.begin(); p2 != v2.end(); ++p2) {
			strncpy(pOut->trade_info[i].trade_history_status_id[j],
					(*p2).c_str(), cTH_ST_ID_len);
			pOut->trade_info[i].trade_history_status_id[j][cTH_ST_ID_len] =
					'\0';
			++j;
		}
		++i;
	}
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_trade_list), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].trade_id = atol((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_trade_type), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].trade_type, (*p).c_str(), cTT_ID_len);
		pOut->trade_info[i].trade_type[cTT_ID_len] = '\0';
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();
}

void CDBConnection::execute(const TTradeLookupFrame4Input *pIn,
		TTradeLookupFrame4Output *pOut)
{
	int i_holding_history_id;
	int i_holding_history_trade_id;
	int i_num_found;
	int i_quantity_after;
	int i_quantity_before;
	int i_trade_id;

	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeLookupFrame4(" <<
			pIn->acct_id << ",'" <<
			pIn->trade_dts.year << "-" <<
			pIn->trade_dts.month << "-" <<
			pIn->trade_dts.day << " " <<
			pIn->trade_dts.hour << ":" <<
			pIn->trade_dts.minute << ":" <<
			pIn->trade_dts.second << "')";

	PGresult *res = exec(osSQL.str().c_str());
	i_holding_history_id = PQfnumber(res, "holding_history_id");
	i_holding_history_trade_id = PQfnumber(res, "holding_history_trade_id");
	i_num_found = PQfnumber(res, "num_found");
	i_quantity_after = PQfnumber(res, "quantity_after");
	i_quantity_before = PQfnumber(res, "quantity_before");
	i_trade_id = PQfnumber(res, "trade_id");

	pOut->num_found = atoi(PQgetvalue(res, 0, i_num_found));

	vector<string> vAux;
	vector<string>::iterator p;

	TokenizeSmart(PQgetvalue(res, 0, i_holding_history_id), vAux);
	int i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].holding_history_id = atol((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_holding_history_trade_id), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].holding_history_trade_id = atol((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_quantity_after), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].quantity_after = atol((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_quantity_before), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].quantity_before = atol((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->trade_id = atol(PQgetvalue(res, 0, i_trade_id));
}

void CDBConnection::execute(const TTradeOrderFrame1Input *pIn,
		TTradeOrderFrame1Output *pOut)
{
	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeOrderFrame1(" << pIn->acct_id << ")";

	PGresult *res = exec(osSQL.str().c_str());

	strncpy(pOut->acct_name, PQgetvalue(res, 0, 0), cCA_NAME_len);
	pOut->acct_name[cCA_NAME_len] ='\0';
	pOut->broker_id = atol(PQgetvalue(res, 0, 1));
	strncpy(pOut->broker_name, PQgetvalue(res, 0, 2), cB_NAME_len);
	pOut->broker_name[cB_NAME_len]  ='\0';
	strncpy(pOut->cust_f_name, PQgetvalue(res, 0, 3), cF_NAME_len);
	pOut->cust_f_name[cF_NAME_len] = '\0';
	pOut->cust_id = atol(PQgetvalue(res, 0, 4));
	strncpy(pOut->cust_l_name, PQgetvalue(res, 0, 5), cL_NAME_len);
	pOut->cust_l_name[cL_NAME_len] = '\0';
	pOut->cust_tier = atoi(PQgetvalue(res, 0, 6));
	pOut->num_found = atoi(PQgetvalue(res, 0, 7));
	strncpy(pOut->tax_id, PQgetvalue(res, 0, 8), cTAX_ID_len);
	pOut->tax_id[cTAX_ID_len] = '\0';
	pOut->tax_status = atoi(PQgetvalue(res, 0, 9));
}

void CDBConnection::execute(const TTradeOrderFrame2Input *pIn,
		TTradeOrderFrame2Output *pOut)
{
	ostringstream osSQL;
	char *tmpstr;
	osSQL << "SELECT * FROM TradeOrderFrame2(" <<
			pIn->acct_id << ",";
	tmpstr = escape(pIn->exec_f_name);
	osSQL << tmpstr;
	PQfreemem(tmpstr);
	osSQL << ",";
	tmpstr = escape(pIn->exec_l_name);
	osSQL << tmpstr;
	PQfreemem(tmpstr);
	osSQL << ",'" << pIn->exec_tax_id<<"')";

	PGresult *res = exec(osSQL.str().c_str());

	if (PQgetvalue(res, 0, 0) != NULL) {
		strncpy(pOut->ap_acl, PQgetvalue(res, 0, 0), cACL_len);
		pOut->ap_acl[cACL_len] = '\0';
	} else {
		pOut->ap_acl[0] = '\0';
	}
}

void CDBConnection::execute(const TTradeOrderFrame3Input *pIn,
		TTradeOrderFrame3Output *pOut)
{
	ostringstream osSQL;
	char *tmpstr;
	osSQL << "SELECT * FROM TradeOrderFrame3(" <<
			pIn->acct_id << "," <<
			pIn->cust_id << "," <<
			pIn->cust_tier << "::SMALLINT," <<
			pIn->is_lifo << "::SMALLINT,'" <<
			pIn->issue << "','" <<
			pIn->st_pending_id << "','" <<
			pIn->st_submitted_id << "'," <<
			pIn->tax_status << "::SMALLINT," <<
			pIn->trade_qty << ",'" <<
			pIn->trade_type_id << "'," <<
			pIn->type_is_margin << "::SMALLINT,";
	tmpstr = escape(pIn->co_name);
	osSQL << tmpstr;
	PQfreemem(tmpstr);
	osSQL << "," <<
			pIn->requested_price << ",'" <<
			pIn->symbol << "')";

	PGresult *res = exec(osSQL.str().c_str());

	strncpy(pOut->co_name, PQgetvalue(res, 0, 0), cCO_NAME_len);
	pOut->requested_price = atof(PQgetvalue(res, 0, 1));
	strncpy(pOut->symbol, PQgetvalue(res, 0, 2), cSYMBOL_len);
	pOut->symbol[cSYMBOL_len] = '\0';
	pOut->buy_value = atof(PQgetvalue(res, 0, 3));
	pOut->charge_amount = atof(PQgetvalue(res, 0, 4));
	pOut->comm_rate = atof(PQgetvalue(res, 0, 5));
	pOut->acct_assets = atof(PQgetvalue(res, 0, 6));
	pOut->market_price = atof(PQgetvalue(res, 0, 7));
	strncpy(pOut->s_name, PQgetvalue(res, 0, 8), cS_NAME_len);
	pOut->s_name[cS_NAME_len] = '\0';
	pOut->sell_value = atof(PQgetvalue(res, 0, 9));
	strncpy(pOut->status_id, PQgetvalue(res, 0, 10), cTH_ST_ID_len);
	pOut->status_id[cTH_ST_ID_len] = '\0';
	pOut->tax_amount = atof(PQgetvalue(res, 0, 11));
	pOut->type_is_market = (PQgetvalue(res, 0, 12)[0] == 't' ? 1 : 0);
	pOut->type_is_sell = (PQgetvalue(res, 0, 13)[0] == 't' ? 1 : 0);
}

void CDBConnection::execute(const TTradeOrderFrame4Input *pIn,
		TTradeOrderFrame4Output *pOut)
{
	ostringstream osSQL;
	char *tmpstr;
	osSQL << "SELECT * FROM TradeOrderFrame4(" <<
			pIn->acct_id << "," <<
			pIn->broker_id << "," <<
			pIn->charge_amount << "," <<
			pIn->comm_amount << ",";
	tmpstr = escape(pIn->exec_name);
	osSQL << tmpstr;
	PQfreemem(tmpstr);
	osSQL << "," <<
			pIn->is_cash << "::SMALLINT," <<
			pIn->is_lifo << "::SMALLINT," <<
			pIn->requested_price << ",'" <<
			pIn->status_id << "','" <<
			pIn->symbol << "'," <<
			pIn->trade_qty << ",'" <<
			pIn->trade_type_id << "'," <<
			pIn->type_is_market << "::SMALLINT)";

	PGresult *res = exec(osSQL.str().c_str());

	pOut->trade_id = atol(PQgetvalue(res, 0, 0));
}

void CDBConnection::execute(const TTradeResultFrame1Input *pIn,
		TTradeResultFrame1Output *pOut)
{
	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeResultFrame1(" << pIn->trade_id << ")";

	PGresult *res = exec(osSQL.str().c_str());

	pOut->acct_id = atol(PQgetvalue(res, 0, 0));
	pOut->charge = atof(PQgetvalue(res, 0, 1));
	pOut->hs_qty = atoi(PQgetvalue(res, 0, 2));
	pOut->is_lifo = atoi(PQgetvalue(res, 0, 3));
	pOut->num_found = atoi(PQgetvalue(res, 0, 4));
	strncpy(pOut->symbol, PQgetvalue(res, 0, 5), cSYMBOL_len);
	pOut->symbol[cSYMBOL_len] = '\0';
	pOut->trade_is_cash = atoi(PQgetvalue(res, 0, 6));
	pOut->trade_qty = atoi(PQgetvalue(res, 0, 7));
	strncpy(pOut->type_id, PQgetvalue(res, 0, 8), cTT_ID_len);
	pOut->type_id[cTT_ID_len] = '\0';
	pOut->type_is_market = atoi(PQgetvalue(res, 0, 9));
	pOut->type_is_sell = atoi(PQgetvalue(res, 0, 10));
	strncpy(pOut->type_name, PQgetvalue(res, 0, 11), cTT_NAME_len);
	pOut->type_name[cTT_NAME_len] = '\0';
}

void CDBConnection::execute(const TTradeResultFrame2Input *pIn,
		TTradeResultFrame2Output *pOut)
{
	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeResultFrame2(" <<
			pIn->acct_id << "," <<
			pIn->hs_qty << "," <<
			pIn->is_lifo << "::SMALLINT,'" <<
			pIn->symbol << "'," <<
			pIn->trade_id << "," <<
			pIn->trade_price << "," <<
			pIn->trade_qty << "," <<
			pIn->type_is_sell << "::SMALLINT)";

	PGresult *res = exec(osSQL.str().c_str());

	pOut->broker_id = atol(PQgetvalue(res, 0, 0));
	pOut->buy_value = atof(PQgetvalue(res, 0, 1));
	pOut->cust_id = atol(PQgetvalue(res, 0, 2));
	pOut->sell_value = atof(PQgetvalue(res, 0, 3));
	pOut->tax_status = atoi(PQgetvalue(res, 0, 4));
	sscanf(PQgetvalue(res, 0, 5), "%hd-%hd-%hd %hd:%hd:%hd.%*d",
			&pOut->trade_dts.year,
			&pOut->trade_dts.month,
			&pOut->trade_dts.day,
			&pOut->trade_dts.hour,
			&pOut->trade_dts.minute,
			&pOut->trade_dts.second);
}

void CDBConnection::execute(const TTradeResultFrame3Input *pIn,
		TTradeResultFrame3Output *pOut)
{
	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeResultFrame3(" <<
			pIn->buy_value << "," <<
			pIn->cust_id << "," <<
			pIn->sell_value << "," <<
			pIn->trade_id << ")";

	PGresult *res = exec(osSQL.str().c_str());

	pOut->tax_amount = atof(PQgetvalue(res, 0, 0));
}

void CDBConnection::execute(const TTradeResultFrame4Input *pIn,
		TTradeResultFrame4Output *pOut)
{
	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeResultFrame4(" <<
			pIn->cust_id << ",'" <<
			pIn->symbol << "'," <<
			pIn->trade_qty << ",'" <<
			pIn->type_id << "')";

	PGresult *res = exec(osSQL.str().c_str());

	pOut->comm_rate = atof(PQgetvalue(res, 0, 0));
	strncpy(pOut->s_name, PQgetvalue(res, 0, 1), cS_NAME_len);
	pOut->s_name[cS_NAME_len] = '\0';
}

void CDBConnection::execute(const TTradeResultFrame5Input *pIn)
{
	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeResultFrame5(" <<
			pIn->broker_id << "," <<
			pIn->comm_amount << ",'" <<
			pIn->st_completed_id << "','" <<
			pIn->trade_dts.year << "-" <<
			pIn->trade_dts.month << "-" <<
			pIn->trade_dts.day << " " <<
			pIn->trade_dts.hour << ":" <<
			pIn->trade_dts.minute << ":" <<
			pIn->trade_dts.second << "'," <<
			pIn->trade_id << "," <<
			pIn->trade_price << ")";

	// For PostgreSQL, see comment in the Concurrency Control chapter, under
	// the Transaction Isolation section for dealing with serialization
	// failures.  These serialization failures can occur with REPEATABLE READS
	// or SERIALIZABLE.
	exec(osSQL.str().c_str());
}

void CDBConnection::execute(const TTradeResultFrame6Input *pIn,
		TTradeResultFrame6Output *pOut)
{
	ostringstream osSQL;
	char *tmpstr;
	osSQL << "SELECT * FROM TradeResultFrame6(" <<
			pIn->acct_id << ",'" <<
			pIn->due_date.year << "-"<<
			pIn->due_date.month << "-" <<
			pIn->due_date.day << " " <<
			pIn->due_date.hour << ":" <<
			pIn->due_date.minute << ":" <<
			pIn->due_date.second << "',";
	tmpstr = escape(pIn->s_name);
	osSQL << tmpstr;
	PQfreemem(tmpstr);
	osSQL << ", " <<
			pIn->se_amount << ",'" <<
			pIn->trade_dts.year << "-" <<
			pIn->trade_dts.month << "-" <<
			pIn->trade_dts.day << " " <<
			pIn->trade_dts.hour << ":" <<
			pIn->trade_dts.minute << ":" <<
			pIn->trade_dts.second << "'," <<
			pIn->trade_id << "," <<
			pIn->trade_is_cash << "::SMALLINT," <<
			pIn->trade_qty << ",'" <<
			pIn->type_name << "')";

	PGresult *res = exec(osSQL.str().c_str());

	pOut->acct_bal = atof(PQgetvalue(res, 0, 0));
}

void CDBConnection::execute(const TTradeStatusFrame1Input *pIn,
		TTradeStatusFrame1Output *pOut)
{
	int i_broker_name;
	int i_charge;
	int i_cust_f_name;
	int i_cust_l_name;
	int i_ex_name;
	int i_exec_name;
	int i_num_found;
	int i_s_name;
	int i_status_name;
	int i_symbol;
	int i_trade_dts;
	int i_trade_id;
	int i_trade_qty;
	int i_type_name;

	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeStatusFrame1(" << pIn->acct_id << ")";

	PGresult *res = exec(osSQL.str().c_str());
	i_broker_name = PQfnumber(res, "broker_name");
	i_charge = PQfnumber(res, "charge");
	i_cust_f_name = PQfnumber(res, "cust_f_name");
	i_cust_l_name = PQfnumber(res, "cust_l_name");
	i_ex_name = PQfnumber(res, "ex_name");
	i_exec_name = PQfnumber(res, "exec_name");
	i_num_found = PQfnumber(res, "num_found");
	i_s_name = PQfnumber(res, "s_name");
	i_status_name = PQfnumber(res, "st_name");
	i_symbol = PQfnumber(res, "symbol");
	i_trade_dts = PQfnumber(res, "trade_dts");
	i_trade_id = PQfnumber(res, "trade_id");
	i_trade_qty = PQfnumber(res, "trade_qty");
	i_type_name = PQfnumber(res, "type_name");

	vector<string> vAux;
	vector<string>::iterator p;

	pOut->num_found = atoi(PQgetvalue(res, 0, i_num_found));

	strncpy(pOut->broker_name, PQgetvalue(res, 0, i_broker_name), cB_NAME_len);
	pOut->broker_name[cB_NAME_len] = '\0';

	TokenizeSmart(PQgetvalue(res, 0, i_charge), vAux);
	int i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->charge[i] = atof((*p).c_str());
		++i;
	}
	vAux.clear();

	strncpy(pOut->cust_f_name, PQgetvalue(res, 0, i_cust_f_name), cF_NAME_len);
	pOut->cust_f_name[cF_NAME_len] = '\0';
	strncpy(pOut->cust_l_name, PQgetvalue(res, 0, i_cust_l_name), cL_NAME_len);
	pOut->cust_l_name[cL_NAME_len] = '\0';

	int len = i;

	TokenizeSmart(PQgetvalue(res, 0, i_ex_name), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->ex_name[i], (*p).c_str(), cEX_NAME_len);
		pOut->ex_name[i][cEX_NAME_len] = '\0';
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_exec_name), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->exec_name[i], (*p).c_str(), cEXEC_NAME_len);
		pOut->exec_name[i][cEXEC_NAME_len] = '\0';
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_s_name), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->s_name[i], (*p).c_str(), cS_NAME_len);
		pOut->s_name[i][cS_NAME_len] = '\0';
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_status_name), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->status_name[i], (*p).c_str(), cST_NAME_len);
		pOut->status_name[i][cST_NAME_len] = '\0';
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();
	TokenizeSmart(PQgetvalue(res, 0, i_symbol), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->symbol[i], (*p).c_str(), cSYMBOL_len);
		pOut->symbol[i][cSYMBOL_len] = '\0';
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_trade_dts), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->trade_dts[i].year,
				&pOut->trade_dts[i].month,
				&pOut->trade_dts[i].day,
				&pOut->trade_dts[i].hour,
				&pOut->trade_dts[i].minute,
				&pOut->trade_dts[i].second);
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_trade_id), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_id[i] = atol((*p).c_str());
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_trade_qty), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_qty[i] = atoi((*p).c_str());
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_type_name), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->type_name[i], (*p).c_str(), cTT_NAME_len);
		pOut->type_name[i][cTT_NAME_len] = '\0';
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();
}

void CDBConnection::execute(const TTradeUpdateFrame1Input *pIn,
		TTradeUpdateFrame1Output *pOut)
{
	int i_bid_price;
	int i_cash_transaction_amount;
	int i_cash_transaction_dts;
	int i_cash_transaction_name;
	int i_exec_name;
	int i_is_cash;
	int i_is_market;
	int i_num_found;
	int i_num_updated;
	int i_settlement_amount;
	int i_settlement_cash_due_date;
	int i_settlement_cash_type;
	int i_trade_history_dts;
	int i_trade_history_status_id;
	int i_trade_price;

	ostringstream osTrades;
	int i = 0;
	osTrades << pIn->trade_id[i];
	for (i = 1; i < pIn->max_trades; i++) {
		osTrades << "," << pIn->trade_id[i];
	}

	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeUpdateFrame1(" <<
			pIn->max_trades << "," <<
			pIn->max_updates << ",'{" <<
			osTrades.str() << "}')";

	PGresult *res = exec(osSQL.str().c_str());
	i_bid_price = PQfnumber(res, "bid_price");
	i_cash_transaction_amount = PQfnumber(res, "cash_transaction_amount");
	i_cash_transaction_dts = PQfnumber(res, "cash_transaction_dts");
	i_cash_transaction_name = PQfnumber(res, "cash_transaction_name");
	i_exec_name = PQfnumber(res, "exec_name");
	i_is_cash = PQfnumber(res, "is_cash");
	i_is_market = PQfnumber(res, "is_market");
	i_num_found = PQfnumber(res, "num_found");
	i_num_updated = PQfnumber(res, "num_updated");
	i_settlement_amount = PQfnumber(res, "settlement_amount");
	i_settlement_cash_due_date = PQfnumber(res, "settlement_cash_due_date");
	i_settlement_cash_type = PQfnumber(res, "settlement_cash_type");
	i_trade_history_dts = PQfnumber(res, "trade_history_dts");
	i_trade_history_status_id = PQfnumber(res, "trade_history_status_id");
	i_trade_price = PQfnumber(res, "trade_price");

	pOut->num_found = atoi(PQgetvalue(res, 0, i_num_found));

	vector<string> vAux;
	vector<string>::iterator p;

	TokenizeSmart(PQgetvalue(res, 0, i_bid_price), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].bid_price = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_cash_transaction_amount), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].cash_transaction_amount = atof((*p).c_str());
		++i;
	}
	// FIXME: According to spec, this may not match the returned number found?
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_cash_transaction_dts), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->trade_info[i].cash_transaction_dts.year,
				&pOut->trade_info[i].cash_transaction_dts.month,
				&pOut->trade_info[i].cash_transaction_dts.day,
				&pOut->trade_info[i].cash_transaction_dts.hour,
				&pOut->trade_info[i].cash_transaction_dts.minute,
				&pOut->trade_info[i].cash_transaction_dts.second);
		++i;
	}
	// FIXME: According to spec, this may not match the returned number found?
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_cash_transaction_name), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].cash_transaction_name, (*p).c_str(),
				cCT_NAME_len);
		pOut->trade_info[i].cash_transaction_name[cCT_NAME_len] = '\0';
		++i;
	}
	// FIXME: According to spec, this may not match the returned number found?
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_exec_name), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].exec_name, (*p).c_str(), cEXEC_NAME_len);
		pOut->trade_info[i].exec_name[cEXEC_NAME_len] = '\0';
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();
	TokenizeSmart(PQgetvalue(res, 0, i_is_cash), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].is_cash = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_is_market), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].is_market = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_settlement_amount), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].settlement_amount = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_settlement_cash_due_date), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->trade_info[i].settlement_cash_due_date.year,
				&pOut->trade_info[i].settlement_cash_due_date.month,
				&pOut->trade_info[i].settlement_cash_due_date.day,
				&pOut->trade_info[i].settlement_cash_due_date.hour,
				&pOut->trade_info[i].settlement_cash_due_date.minute,
				&pOut->trade_info[i].settlement_cash_due_date.second);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_settlement_cash_type), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].settlement_cash_type, (*p).c_str(),
				cSE_CASH_TYPE_len);
		pOut->trade_info[i].settlement_cash_type[cSE_CASH_TYPE_len] = '\0';
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->num_updated = atoi(PQgetvalue(res, 0, i_num_updated));

	TokenizeArray(PQgetvalue(res, 0, i_trade_history_dts), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		vector<string> v2;
		vector<string>::iterator p2;
		TokenizeSmart((*p).c_str(), v2);
		p2 = v2.begin();
		sscanf((*p2++).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->trade_info[i].trade_history_dts[0].year,
				&pOut->trade_info[i].trade_history_dts[0].month,
				&pOut->trade_info[i].trade_history_dts[0].day,
				&pOut->trade_info[i].trade_history_dts[0].hour,
				&pOut->trade_info[i].trade_history_dts[0].minute,
				&pOut->trade_info[i].trade_history_dts[0].second);
		sscanf((*p2++).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->trade_info[i].trade_history_dts[1].year,
				&pOut->trade_info[i].trade_history_dts[1].month,
				&pOut->trade_info[i].trade_history_dts[1].day,
				&pOut->trade_info[i].trade_history_dts[1].hour,
				&pOut->trade_info[i].trade_history_dts[1].minute,
				&pOut->trade_info[i].trade_history_dts[1].second);
		sscanf((*p2).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->trade_info[i].trade_history_dts[2].year,
				&pOut->trade_info[i].trade_history_dts[2].month,
				&pOut->trade_info[i].trade_history_dts[2].day,
				&pOut->trade_info[i].trade_history_dts[2].hour,
				&pOut->trade_info[i].trade_history_dts[2].minute,
				&pOut->trade_info[i].trade_history_dts[2].second);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeArray(PQgetvalue(res, 0, i_trade_history_status_id), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		vector<string> v2;
		vector<string>::iterator p2;
		TokenizeSmart((*p).c_str(), v2);
		p2 = v2.begin();
		strncpy(pOut->trade_info[i].trade_history_status_id[0],
				(*p2++).c_str(), cTH_ST_ID_len);
		pOut->trade_info[i].trade_history_status_id[0][cTH_ST_ID_len] = '\0';
		strncpy(pOut->trade_info[i].trade_history_status_id[1],
				(*p2++).c_str(), cTH_ST_ID_len);
		pOut->trade_info[i].trade_history_status_id[1][cTH_ST_ID_len] = '\0';
		strncpy(pOut->trade_info[i].trade_history_status_id[3],
				(*p2).c_str(), cTH_ST_ID_len);
		pOut->trade_info[i].trade_history_status_id[3][cTH_ST_ID_len] = '\0';
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_trade_price), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].trade_price = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();
}

void CDBConnection::execute(const TTradeUpdateFrame2Input *pIn,
		TTradeUpdateFrame2Output *pOut)
{
	int i_bid_price;
	int i_cash_transaction_amount;
	int i_cash_transaction_dts;
	int i_cash_transaction_name;
	int i_exec_name;
	int i_is_cash;
	int i_num_found;
	int i_num_updated;
	int i_settlement_amount;
	int i_settlement_cash_due_date;
	int i_settlement_cash_type;
	int i_trade_history_dts;
	int i_trade_history_status_id;
	int i_trade_list;
	int i_trade_price;

	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeUpdateFrame2(" <<
			pIn->acct_id << ",'" <<
			pIn->end_trade_dts.year << "-" <<
			pIn->end_trade_dts.month << "-" <<
			pIn->end_trade_dts.day << " " <<
			pIn->end_trade_dts.hour << ":" <<
			pIn->end_trade_dts.minute << ":" <<
			pIn->end_trade_dts.second << "'," <<
			pIn->max_trades << "," <<
			pIn->max_updates << ",'" <<
			pIn->end_trade_dts.year << "-" <<
			pIn->end_trade_dts.month << "-" <<
			pIn->end_trade_dts.day << " " <<
			pIn->end_trade_dts.hour << ":" <<
			pIn->end_trade_dts.minute << ":" <<
			pIn->end_trade_dts.second << "')";

	PGresult *res = exec(osSQL.str().c_str());
	i_bid_price = PQfnumber(res, "bid_price");
	i_cash_transaction_amount = PQfnumber(res, "cash_transaction_amount");
	i_cash_transaction_dts = PQfnumber(res, "cash_transaction_dts");
	i_cash_transaction_name = PQfnumber(res, "cash_transaction_name");
	i_exec_name = PQfnumber(res, "exec_name");
	i_is_cash = PQfnumber(res, "is_cash");
	i_num_found = PQfnumber(res, "num_found");
	i_num_updated = PQfnumber(res, "num_updated");
	i_settlement_amount = PQfnumber(res, "settlement_amount");
	i_settlement_cash_due_date = PQfnumber(res, "settlement_cash_due_date");
	i_settlement_cash_type = PQfnumber(res, "settlement_cash_type");
	i_trade_history_dts = PQfnumber(res, "trade_history_dts");
	i_trade_history_status_id = PQfnumber(res, "trade_history_status_id");
	i_trade_list = PQfnumber(res, "trade_list");
	i_trade_price = PQfnumber(res, "trade_price");

	pOut->num_found = atoi(PQgetvalue(res, 0, i_num_found));

	vector<string> vAux;
	vector<string>::iterator p;

	TokenizeSmart(PQgetvalue(res, 0, i_bid_price), vAux);
	int i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].bid_price = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_cash_transaction_amount), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].cash_transaction_amount = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_cash_transaction_dts), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->trade_info[i].cash_transaction_dts.year,
				&pOut->trade_info[i].cash_transaction_dts.month,
				&pOut->trade_info[i].cash_transaction_dts.day,
				&pOut->trade_info[i].cash_transaction_dts.hour,
				&pOut->trade_info[i].cash_transaction_dts.minute,
				&pOut->trade_info[i].cash_transaction_dts.second);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_cash_transaction_name), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].cash_transaction_name, (*p).c_str(),
				cCT_NAME_len);
		pOut->trade_info[i].cash_transaction_name[cCT_NAME_len] = '\0';
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_exec_name), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].exec_name, (*p).c_str(), cEXEC_NAME_len);
		pOut->trade_info[i].exec_name[cEXEC_NAME_len] = '\0';
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_is_cash), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].is_cash = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->num_updated = atoi(PQgetvalue(res, 0, i_num_updated));

	TokenizeSmart(PQgetvalue(res, 0, i_settlement_amount), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].settlement_amount = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_settlement_cash_due_date), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->trade_info[i].settlement_cash_due_date.year,
				&pOut->trade_info[i].settlement_cash_due_date.month,
				&pOut->trade_info[i].settlement_cash_due_date.day,
				&pOut->trade_info[i].settlement_cash_due_date.hour,
				&pOut->trade_info[i].settlement_cash_due_date.minute,
				&pOut->trade_info[i].settlement_cash_due_date.second);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_settlement_cash_type), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].settlement_cash_type, (*p).c_str(),
				cSE_CASH_TYPE_len);
		pOut->trade_info[i].settlement_cash_type[cSE_CASH_TYPE_len] = '\0';
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeArray(PQgetvalue(res, 0, i_trade_history_dts), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		vector<string> v2;
		vector<string>::iterator p2;
		TokenizeSmart((*p).c_str(), v2);
		p2 = v2.begin();
		sscanf((*p2++).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->trade_info[i].trade_history_dts[0].year,
				&pOut->trade_info[i].trade_history_dts[0].month,
				&pOut->trade_info[i].trade_history_dts[0].day,
				&pOut->trade_info[i].trade_history_dts[0].hour,
				&pOut->trade_info[i].trade_history_dts[0].minute,
				&pOut->trade_info[i].trade_history_dts[0].second);
		sscanf((*p2++).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->trade_info[i].trade_history_dts[1].year,
				&pOut->trade_info[i].trade_history_dts[1].month,
				&pOut->trade_info[i].trade_history_dts[1].day,
				&pOut->trade_info[i].trade_history_dts[1].hour,
				&pOut->trade_info[i].trade_history_dts[1].minute,
				&pOut->trade_info[i].trade_history_dts[1].second);
		sscanf((*p2).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->trade_info[i].trade_history_dts[2].year,
				&pOut->trade_info[i].trade_history_dts[2].month,
				&pOut->trade_info[i].trade_history_dts[2].day,
				&pOut->trade_info[i].trade_history_dts[2].hour,
				&pOut->trade_info[i].trade_history_dts[2].minute,
				&pOut->trade_info[i].trade_history_dts[2].second);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeArray(PQgetvalue(res, 0, i_trade_history_status_id), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		vector<string> v2;
		vector<string>::iterator p2;
		TokenizeSmart((*p).c_str(), v2);
		p2 = v2.begin();
		strncpy(pOut->trade_info[i].trade_history_status_id[0],
				(*p2++).c_str(), cTH_ST_ID_len);
		pOut->trade_info[i].trade_history_status_id[0][cTH_ST_ID_len] = '\0';
		strncpy(pOut->trade_info[i].trade_history_status_id[1],
				(*p2++).c_str(), cTH_ST_ID_len);
		pOut->trade_info[i].trade_history_status_id[1][cTH_ST_ID_len] = '\0';
		strncpy(pOut->trade_info[i].trade_history_status_id[3],
				(*p2).c_str(), cTH_ST_ID_len);
		pOut->trade_info[i].trade_history_status_id[3][cTH_ST_ID_len] = '\0';
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_trade_list), vAux);
	this->bh = bh;
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].trade_id = atol((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_trade_price), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].trade_price = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();
}

void CDBConnection::execute(const TTradeUpdateFrame3Input *pIn,
		TTradeUpdateFrame3Output *pOut)
{
	int i_acct_id;
	int i_cash_transaction_amount;
	int i_cash_transaction_dts;
	int i_cash_transaction_name;
	int i_exec_name;
	int i_is_cash;
	int i_num_found;
	int i_num_updated;
	int i_price;
	int i_quantity;
	int i_s_name;
	int i_settlement_amount;
	int i_settlement_cash_due_date;
	int i_settlement_cash_type;
	int i_trade_dts;
	int i_trade_history_dts;
	int i_trade_history_status_id;
	int i_trade_list;
	int i_type_name;
	int i_trade_type;

	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeUpdateFrame3('" <<
			pIn->end_trade_dts.year << "-" <<
			pIn->end_trade_dts.month << "-" <<
			pIn->end_trade_dts.day << " " <<
			pIn->end_trade_dts.hour << ":" <<
			pIn->end_trade_dts.minute << ":" <<
			pIn->end_trade_dts.second << "'," <<
			pIn->max_acct_id << "," <<
			pIn->max_trades << "," <<
			pIn->max_updates << ",'" <<
			pIn->start_trade_dts.year << "-" <<
			pIn->start_trade_dts.month << "-" <<
			pIn->start_trade_dts.day << " " <<
			pIn->start_trade_dts.hour << ":" <<
			pIn->start_trade_dts.minute << ":" <<
			pIn->start_trade_dts.second << "','" <<
			pIn->symbol << "')";

	PGresult *res = exec(osSQL.str().c_str());
	i_acct_id = PQfnumber(res, "acct_id");
	i_cash_transaction_amount = PQfnumber(res, "cash_transaction_amount");
	i_cash_transaction_dts = PQfnumber(res, "cash_transaction_dts");
	i_cash_transaction_name = PQfnumber(res, "cash_transaction_name");
	i_exec_name = PQfnumber(res, "exec_name");
	i_is_cash = PQfnumber(res, "is_cash");
	i_num_found = PQfnumber(res, "num_found");
	i_num_updated = PQfnumber(res, "num_updated");
	i_price = PQfnumber(res, "price");
	i_quantity = PQfnumber(res, "quantity");
	i_s_name = PQfnumber(res, "s_name");
	i_settlement_amount = PQfnumber(res, "settlement_amount");
	i_settlement_cash_due_date = PQfnumber(res, "settlement_cash_due_date");
	i_settlement_cash_type = PQfnumber(res, "settlement_cash_type");
	i_trade_dts = PQfnumber(res, "trade_dts");
	i_trade_history_dts = PQfnumber(res, "trade_history_dts");
	i_trade_history_status_id = PQfnumber(res, "trade_history_status_id");
	i_trade_list = PQfnumber(res, "trade_list");
	i_type_name = PQfnumber(res, "type_name");
	i_trade_type = PQfnumber(res, "trade_type");

	pOut->num_found = atoi(PQgetvalue(res, 0, i_num_found));

	vector<string> vAux;
	vector<string>::iterator p;

	TokenizeSmart(PQgetvalue(res, 0, i_acct_id), vAux);
	int i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].acct_id = atol((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_cash_transaction_amount), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].cash_transaction_amount = atof((*p).c_str());
		++i;
	}
	// FIXME: According to spec, this may not match the returned number found?
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_cash_transaction_dts), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->trade_info[i].cash_transaction_dts.year,
				&pOut->trade_info[i].cash_transaction_dts.month,
				&pOut->trade_info[i].cash_transaction_dts.day,
				&pOut->trade_info[i].cash_transaction_dts.hour,
				&pOut->trade_info[i].cash_transaction_dts.minute,
				&pOut->trade_info[i].cash_transaction_dts.second);
		++i;
	}
	// FIXME: According to spec, this may not match the returned number found?
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_cash_transaction_name), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].cash_transaction_name, (*p).c_str(),
				cCT_NAME_len);
		pOut->trade_info[i].cash_transaction_name[cCT_NAME_len] = '\0';
		++i;
	}
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_exec_name), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].exec_name, (*p).c_str(), cEXEC_NAME_len);
		pOut->trade_info[i].exec_name[cEXEC_NAME_len] = '\0';
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_is_cash), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].is_cash = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->num_updated = atoi(PQgetvalue(res, 0, i_num_updated));

	TokenizeSmart(PQgetvalue(res, 0, i_price), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].price = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_quantity), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].quantity = atoi((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_s_name), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].s_name, (*p).c_str(), cS_NAME_len);
		pOut->trade_info[i].s_name[cS_NAME_len] = '\0';
		++i;
	}
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_settlement_amount), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].settlement_amount = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_settlement_cash_due_date), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->trade_info[i].settlement_cash_due_date.year,
				&pOut->trade_info[i].settlement_cash_due_date.month,
				&pOut->trade_info[i].settlement_cash_due_date.day,
				&pOut->trade_info[i].settlement_cash_due_date.hour,
				&pOut->trade_info[i].settlement_cash_due_date.minute,
				&pOut->trade_info[i].settlement_cash_due_date.second);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_settlement_cash_type), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].settlement_cash_type, (*p).c_str(),
				cSE_CASH_TYPE_len);
		pOut->trade_info[i].settlement_cash_type[cSE_CASH_TYPE_len] = '\0';
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_trade_dts), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->trade_info[i].trade_dts.year,
				&pOut->trade_info[i].trade_dts.month,
				&pOut->trade_info[i].trade_dts.day,
				&pOut->trade_info[i].trade_dts.hour,
				&pOut->trade_info[i].trade_dts.minute,
				&pOut->trade_info[i].trade_dts.second);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeArray(PQgetvalue(res, 0, i_trade_history_dts), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		vector<string> v2;
		vector<string>::iterator p2;
		TokenizeSmart((*p).c_str(), v2);
		int j = 0;
		for (p2 = v2.begin(); p2 != v2.end(); ++p2) {
			sscanf((*p2).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
					&pOut->trade_info[i].trade_history_dts[j].year,
					&pOut->trade_info[i].trade_history_dts[j].month,
					&pOut->trade_info[i].trade_history_dts[j].day,
					&pOut->trade_info[i].trade_history_dts[j].hour,
					&pOut->trade_info[i].trade_history_dts[j].minute,
					&pOut->trade_info[i].trade_history_dts[j].second);
			++j;
		}
		++i;
	}
	vAux.clear();

	TokenizeArray(PQgetvalue(res, 0, i_trade_history_status_id), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		vector<string> v2;
		vector<string>::iterator p2;
		TokenizeSmart((*p).c_str(), v2);
		int j = 0;
		for (p2 = v2.begin(); p2 != v2.end(); ++p2) {
			strncpy(pOut->trade_info[i].trade_history_status_id[j],
					(*p2).c_str(), cTH_ST_ID_len);
			pOut->trade_info[i].trade_history_status_id[j][cTH_ST_ID_len] =
					'\0';
			++j;
		}
		++i;
	}
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_trade_list), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].trade_id = atol((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_type_name), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].type_name, (*p).c_str(), cTT_NAME_len);
		pOut->trade_info[i].type_name[cTT_NAME_len] = '\0';
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(PQgetvalue(res, 0, i_trade_type), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].trade_type, (*p).c_str(), cTT_ID_len);
		pOut->trade_info[i].trade_type[cTT_ID_len] = '\0';
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();
}

void CDBConnection::reconnect()
{
	disconnect();
	connect();
}

void CDBConnection::rollback()
{
	PQexec(m_Conn, "ROLLBACK;");
}

void CDBConnection::setBrokerageHouse(CBrokerageHouse *bh)
{
	this->bh = bh;
}

void CDBConnection::setReadCommitted()
{
	PQexec(m_Conn, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
}

void CDBConnection::setReadUncommitted()
{
	PQexec(m_Conn, "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;");
}

void CDBConnection::setRepeatableRead()
{
	PQexec(m_Conn, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
}

void CDBConnection::setSerializable()
{
	PQexec(m_Conn, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
}
