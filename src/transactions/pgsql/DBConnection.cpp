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

bool inline check_count(int should, int is, char *file, int line) {
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
	m_Conn = new connection(szConnectStr);
	m_Txn = new nontransaction(*m_Conn, name);
}

// Destructor: Disconnect from server
CDBConnection::~CDBConnection()
{
	m_Conn->disconnect();

	delete m_Txn;
	delete m_Conn;
}

void CDBConnection::begin()
{
	m_Txn->exec("BEGIN;");
}

void CDBConnection::connect()
{
	m_Conn = new connection(szConnectStr);
	m_Txn = new nontransaction(*m_Conn, name);
}

void CDBConnection::commit()
{
	m_Txn->exec("COMMIT;");
}

string CDBConnection::escape(string s)
{
	return m_Txn->esc(s);
}

void CDBConnection::disconnect()
{
	delete m_Txn;
	delete m_Conn;
}

void CDBConnection::execute(const TBrokerVolumeFrame1Input *pIn,
		TBrokerVolumeFrame1Output *pOut)
{
	enum bvf1 {i_broker_name=0, i_list_len, i_status, i_volume};

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

	result R(m_Txn->exec(osSQL.str()));
	if (R.empty()) {
		pOut->status = CBaseTxnErr::ROLLBACK;
		ostringstream msg;
		msg << time(NULL) << " " << pthread_self() << endl <<
				"NO RESULTS" << endl <<
				osSQL.str() << endl;
		bh->logErrorMessage(msg.str(), false);
		rollback();
		throw;
	}

	result::const_iterator c = R.begin();

	pOut->list_len = c[i_list_len].as(int());;

	vector<string> vAux;
	vector<string>::iterator p;

	TokenizeSmart(c[i_broker_name].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->broker_name[i], (*p).c_str(), cB_NAME_len);
		++i;
	}
	check_count(pOut->list_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_volume].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->volume[i] = atof((*p).c_str());
		++i;
	}
	check_count(pOut->list_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->status = c[i_status].as(int());
}

void CDBConnection::execute(const TCustomerPositionFrame1Input *pIn,
		TCustomerPositionFrame1Output *pOut)
{
	ostringstream osSQL;
	osSQL << "SELECT * FROM CustomerPositionFrame1(" <<
			pIn->cust_id << ",'" <<
			pIn->tax_id << "')";

	result R(m_Txn->exec(osSQL.str()));
	if (R.empty()) {
		pOut->status = CBaseTxnErr::ROLLBACK;
		ostringstream msg;
		msg << time(NULL) << " " << pthread_self() << endl <<
				"NO RESULTS" << endl <<
				osSQL.str() << endl;
		bh->logErrorMessage(msg.str(), false);
		rollback();
		throw;
	}

	result::const_iterator c = R.begin();

	enum cpf1 {
			i_cust_id=0, i_acct_id, i_acct_len, i_asset_total, i_c_ad_id,
			i_c_area_1, i_c_area_2, i_c_area_3, i_c_ctry_1, i_c_ctry_2,
			i_c_ctry_3, i_c_dob, i_c_email_1, i_c_email_2, i_c_ext_1,
			i_c_ext_2, i_c_ext_3, i_c_f_name, i_c_gndr, i_c_l_name,
			i_c_local_1, i_c_local_2, i_c_local_3, i_c_m_name, i_c_st_id,
			i_c_tier, i_cash_bal, i_status
	};

	pOut->acct_len = c[i_acct_len].as(int());

	vector<string> vAux;
	vector<string>::iterator p;

	pOut->cust_id = c[i_cust_id].as(long());

	TokenizeSmart(c[i_acct_id].c_str(), vAux);
	int i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->acct_id[i] = atol( (*p).c_str() );
		++i;
	}
	check_count(pOut->acct_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_asset_total].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
			pOut->asset_total[i] = atof( (*p).c_str() );
			++i;
	}
	check_count(pOut->acct_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->c_ad_id = c[i_c_ad_id].as(long());

	strncpy(pOut->c_area_1, c[i_c_area_1].c_str(), cAREA_len);
	strncpy(pOut->c_area_2, c[i_c_area_2].c_str(), cAREA_len);
	strncpy(pOut->c_area_3, c[i_c_area_3].c_str(), cAREA_len);

	strncpy(pOut->c_ctry_1, c[i_c_ctry_1].c_str(), cCTRY_len);
	strncpy(pOut->c_ctry_2, c[i_c_ctry_2].c_str(), cCTRY_len);
	strncpy(pOut->c_ctry_3, c[i_c_ctry_3].c_str(), cCTRY_len);

	sscanf(c[i_c_dob].c_str(), "%hd-%hd-%hd", &pOut->c_dob.year,
			&pOut->c_dob.month, &pOut->c_dob.day);

	strncpy(pOut->c_email_1, c[i_c_email_1].c_str(), cEMAIL_len);
	strncpy(pOut->c_email_2, c[i_c_email_2].c_str(), cEMAIL_len);

	strncpy(pOut->c_ext_1, c[i_c_ext_1].c_str(), cEXT_len);
	strncpy(pOut->c_ext_2, c[i_c_ext_2].c_str(), cEXT_len);
	strncpy(pOut->c_ext_3, c[i_c_ext_3].c_str(), cEXT_len);

	strncpy(pOut->c_f_name, c[i_c_f_name].c_str(), cF_NAME_len);
	strncpy(pOut->c_gndr, c[i_c_gndr].c_str(), cGNDR_len);
	strncpy(pOut->c_l_name, c[i_c_l_name].c_str(), cL_NAME_len);

	strncpy(pOut->c_local_1, c[i_c_local_1].c_str(), cLOCAL_len);
	strncpy(pOut->c_local_2, c[i_c_local_2].c_str(), cLOCAL_len);
	strncpy(pOut->c_local_3, c[i_c_local_3].c_str(), cLOCAL_len);

	strncpy(pOut->c_m_name, c[i_c_m_name].c_str(), cM_NAME_len);
	strncpy(pOut->c_st_id, c[i_c_st_id].c_str(), cST_ID_len);
	strncpy(&pOut->c_tier, c[i_c_tier].c_str(), 1);

	TokenizeSmart(c[i_cash_bal].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->cash_bal[i] = atof( (*p).c_str() );
		++i;
	}
	check_count(pOut->acct_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->status = c[i_status].as(int());
}

void CDBConnection::execute(const TCustomerPositionFrame2Input *pIn,
		TCustomerPositionFrame2Output *pOut)
{
	enum cpf2 {
		i_hist_dts=0, i_hist_len, i_qty, i_status, i_symbol,
		i_trade_id, i_trade_status
	};

	ostringstream osSQL;
	osSQL << "SELECT * FROM CustomerPositionFrame2(" << pIn->acct_id << ")";

	result R(m_Txn->exec(osSQL.str()));
	if (R.empty()) {
		pOut->status = CBaseTxnErr::ROLLBACK;
		ostringstream msg;
		msg << time(NULL) << " " << pthread_self() << endl <<
				"NO RESULTS" << endl <<
				osSQL.str() << endl;
		bh->logErrorMessage(msg.str(), false);
		rollback();
		throw;
	}

	result::const_iterator c = R.begin();

	pOut->hist_len = c[i_hist_len].as(int());;

	vector<string> vAux;
	vector<string>::iterator p;
	int i;

	TokenizeSmart(c[i_hist_dts].c_str(), vAux);
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

	TokenizeSmart(c[i_qty].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->qty[i] = atoi((*p).c_str());
		++i;
	}
	check_count(pOut->hist_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->status = c[i_status].as(int());

	TokenizeSmart(c[i_symbol].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->symbol[i], (*p).c_str(), cSYMBOL_len);
		++i;
	}
	check_count(pOut->hist_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_trade_id].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_id[i] = atol((*p).c_str());
		++i;
	}
	check_count(pOut->hist_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_trade_status].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_status[i], (*p).c_str(), cST_NAME_len);
		++i;
	}
	check_count(pOut->hist_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();
}

void CDBConnection::execute(const TDataMaintenanceFrame1Input *pIn,
		TDataMaintenanceFrame1Output *pOut)
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

	result R(m_Txn->exec(osSQL.str()));
	if (R.empty()) {
		pOut->status = CBaseTxnErr::ROLLBACK;
		ostringstream msg;
		msg << time(NULL) << " " << pthread_self() << endl <<
				"NO RESULTS" << endl <<
				osSQL.str() << endl;
		bh->logErrorMessage(msg.str(), false);
		rollback();
		throw;
	}

	result::const_iterator c = R.begin();
	pOut->status = c[0].as(int());
}


void CDBConnection::execute(const TMarketFeedFrame1Input *pIn,
		TMarketFeedFrame1Output *pOut, CSendToMarketInterface *pMarketExchange)
{
	enum mff1 {i_send_len=0, i_status, i_rows_updated, i_symbol,
			i_trade_id, i_price_quote, i_trade_qty, i_trade_type};

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

	// For PostgreSQL, see comment in the Concurrency Control chapter, under
	// the Transaction Isolation section for dealing with serialization
	// failures.  These serialization failures can occur with REPEATABLE READS
	// or SERIALIZABLE.
	int iNumRetry = 1;
	result R;
	while (true) {
		try {
			R = m_Txn->exec(osSQL.str());
			if (R.empty()) {
				pOut->send_len = 0;
				pOut->status = CBaseTxnErr::ROLLBACK;
				ostringstream msg;
				msg << time(NULL) << " " << pthread_self() << endl <<
						"NO RESULTS" << endl <<
						osSQL.str() << endl;
				bh->logErrorMessage(msg.str(), false);
				rollback();
				throw;
			}
			break;
		} catch (const pqxx::sql_error &e) {
			if (PGSQL_SERIALIZE_ERROR.compare(e.what()) == 0) {
					ostringstream msg;
					msg << time(NULL) << " " << pthread_self() <<
							"MARKET_FEED" << endl;
					msg << "attempt: " << iNumRetry << endl;
					msg << "what: " << e.what();
					msg << "query: " << e.query() << endl;

					if (iNumRetry <= iMaxRetries) {
						// Wait 1 second to give the other transaction time to
						// finish.
						bh->logErrorMessage(msg.str(), false);
						iNumRetry++;
						sleep(1);
					} else {
						// Couldn't resubmit successfully.
						msg << "giving up" << endl;
						bh->logErrorMessage(msg.str(), false);
						pOut->status = CBaseTxnErr::ROLLBACK;
						rollback();
						throw;
					}
			} else {
				pOut->status = CBaseTxnErr::ROLLBACK;
				rollback();
				throw;
			}
		}
	}

	result::const_iterator c = R.begin();

	pOut->send_len = c[i_send_len].as(int());;

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

	TokenizeSmart(c[i_symbol].c_str(), v1);
	TokenizeSmart(c[i_trade_id].c_str(), v2);
	TokenizeSmart(c[i_price_quote].c_str(), v3);
	TokenizeSmart(c[i_trade_qty].c_str(), v4);
	TokenizeSmart(c[i_trade_type].c_str(), v5);

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
		m_TriggeredLimitOrders.trade_id = atol((*p2).c_str());
		m_TriggeredLimitOrders.price_quote = atof((*p3).c_str());
		m_TriggeredLimitOrders.trade_qty = atoi((*p4).c_str());
		strncpy(m_TriggeredLimitOrders.trade_type_id, (*p5).c_str(),
				cTT_ID_len);

		bSent = pMarketExchange->SendToMarketFromFrame(
				m_TriggeredLimitOrders);
		++i;
	}
	check_count(pOut->send_len, i, __FILE__, __LINE__);

	if (atoi(c[i_rows_updated].c_str()) != max_feed_len)
		pOut->status = -311;
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

	result R(m_Txn->exec(osSQL.str()));
	if (R.empty()) {
		pOut->status = CBaseTxnErr::ROLLBACK;
		ostringstream msg;
		msg << time(NULL) << " " << pthread_self() << endl <<
				"NO RESULTS" << endl <<
				osSQL.str() << endl;
		bh->logErrorMessage(msg.str(), false);
		rollback();
		throw;
	}

	result::const_iterator c = R.begin();

	pOut->pct_change = c[0].as(double());
	pOut->status = c[1].as(int());
}

void CDBConnection::execute(const TSecurityDetailFrame1Input *pIn,
		TSecurityDetailFrame1Output *pOut)
{
	enum sdf1 {
		i_s52_wk_high=0, i_s52_wk_high_date, i_s52_wk_low,
		i_s52_wk_low_date, i_ceo_name, i_co_ad_cty, i_co_ad_div,
		i_co_ad_line1, i_co_ad_line2, i_co_ad_town, i_co_ad_zip,
		i_co_desc, i_co_name, i_co_st_id, i_cp_co_name, i_cp_in_name,
		i_day, i_day_len, i_divid, i_ex_ad_cty, i_ex_ad_div,
		i_ex_ad_line1, i_ex_ad_line2, i_ex_ad_town, i_ex_ad_zip,
		i_ex_close, i_ex_date, i_ex_desc, i_ex_name, i_ex_num_symb,
		i_ex_open, i_fin, i_fin_len, i_last_open, i_last_price,
		i_last_vol, i_news, i_news_len, i_num_out, i_open_date,
		i_pe_ratio, i_s_name, i_sp_rate, i_start_date, i_status, i_yield
	};

	ostringstream osSQL;
	osSQL << "SELECT * FROM SecurityDetailFrame1(" <<
			(pIn->access_lob_flag == 0 ? "false" : "true") << "," <<
			pIn->max_rows_to_return << ",'" <<
			pIn->start_day.year << "-" <<
			pIn->start_day.month << "-" <<
			pIn->start_day.day << "','" <<
			pIn->symbol << "')";

	result R(m_Txn->exec(osSQL.str()));
	if (R.empty()) {
		pOut->status = CBaseTxnErr::ROLLBACK;
		ostringstream msg;
		msg << time(NULL) << " " << pthread_self() << endl <<
				"NO RESULTS" << endl <<
				osSQL.str() << endl;
		bh->logErrorMessage(msg.str(), false);
		rollback();
		throw;
	}

	result::const_iterator c = R.begin();

	pOut->fin_len = c[i_fin_len].as(int());
	pOut->day_len = c[i_day_len].as(int());
	pOut->news_len = c[i_news_len].as(int());

	pOut->s52_wk_high = c[i_s52_wk_high].as(double());
	sscanf(c[i_s52_wk_high_date].c_str(), "%hd-%hd-%hd",
			&pOut->s52_wk_high_date.year,
			&pOut->s52_wk_high_date.month,
			&pOut->s52_wk_high_date.day);
	pOut->s52_wk_low = c[i_s52_wk_low].as(double());
	sscanf(c[i_s52_wk_low_date].c_str(), "%hd-%hd-%hd",
			&pOut->s52_wk_low_date.year,
			&pOut->s52_wk_low_date.month,
			&pOut->s52_wk_low_date.day);

	strncpy(pOut->ceo_name, c[i_ceo_name].c_str(), cCEO_NAME_len);
	strncpy(pOut->co_ad_cty, c[i_co_ad_cty].c_str(), cAD_CTRY_len);
	strncpy(pOut->co_ad_div, c[i_co_ad_div].c_str(), cAD_DIV_len);
	strncpy(pOut->co_ad_line1, c[i_co_ad_line1].c_str(), cAD_LINE_len);
	strncpy(pOut->co_ad_line2, c[i_co_ad_line2].c_str(), cAD_LINE_len);
	strncpy(pOut->co_ad_town, c[i_co_ad_town].c_str(), cAD_TOWN_len);
	strncpy(pOut->co_ad_zip, c[i_co_ad_zip].c_str(), cAD_ZIP_len);
	strncpy(pOut->co_desc, c[i_co_desc].c_str(), cCO_DESC_len);
	strncpy(pOut->co_name, c[i_co_name].c_str(), cCO_NAME_len);
	strncpy(pOut->co_st_id, c[i_co_st_id].c_str(), cST_ID_len);

	vector<string> vAux;
	vector<string>::iterator p;
	TokenizeSmart(c[i_cp_co_name].c_str(), vAux);
	int i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->cp_co_name[i], (*p).c_str(), cCO_NAME_len);
		++i;
	}
	// FIXME: The stored functions for PostgreSQL are designed to return 3
	// items in the array, even though it's not required.
	check_count(3, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_cp_in_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->cp_in_name[i], (*p).c_str(), cIN_NAME_len);
		++i;
	}

	// FIXME: The stored functions for PostgreSQL are designed to return 3
	// items in the array, even though it's not required.
	check_count(3, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeArray(c[i_day].c_str(), vAux);
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

	pOut->divid = c[i_divid].as(double());

	strncpy(pOut->ex_ad_cty, c[i_ex_ad_cty].c_str(), cAD_CTRY_len);
	strncpy(pOut->ex_ad_div, c[i_ex_ad_div].c_str(), cAD_DIV_len);
	strncpy(pOut->ex_ad_line1, c[i_ex_ad_line1].c_str(), cAD_LINE_len);
	strncpy(pOut->ex_ad_line2, c[i_ex_ad_line2].c_str(), cAD_LINE_len);
	strncpy(pOut->ex_ad_town, c[i_ex_ad_town].c_str(), cAD_TOWN_len);
	strncpy(pOut->ex_ad_zip, c[i_ex_ad_zip].c_str(), cAD_ZIP_len);
	pOut->ex_close = c[i_ex_close].as(int());
	sscanf(c[i_ex_date].c_str(), "%hd-%hd-%hd",
			&pOut->ex_date.year,
			&pOut->ex_date.month,
			&pOut->ex_date.day);
	strncpy(pOut->ex_desc, c[i_ex_desc].c_str(), cEX_DESC_len);
	strncpy(pOut->ex_name, c[i_ex_name].c_str(), cEX_NAME_len);
	pOut->ex_num_symb = c[i_ex_num_symb].as(int());
	pOut->ex_open = c[i_ex_open].as(int());

	TokenizeArray(c[i_fin].c_str(), vAux);
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

	pOut->last_open = c[i_last_open].as(double());
	pOut->last_price = c[i_last_price].as(double());
	pOut->last_vol = c[i_last_vol].as(int());

	TokenizeArray(c[i_news].c_str(), vAux);
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
		sscanf((*p2++).c_str(), "%hd-%hd-%hd %hd:%hd:%hd",
				&pOut->news[i].dts.year,
				&pOut->news[i].dts.month,
				&pOut->news[i].dts.day,
				&pOut->news[i].dts.hour,
				&pOut->news[i].dts.minute,
				&pOut->news[i].dts.second);
		strncpy(pOut->news[i].src, (*p2++).c_str(), cNI_SOURCE_len);
		strncpy(pOut->news[i].auth, (*p2++).c_str(), cNI_AUTHOR_len);
		strncpy(pOut->news[i].headline, (*p2++).c_str(), cNI_HEADLINE_len);
		strncpy(pOut->news[i].summary, (*p2++).c_str(), cNI_SUMMARY_len);
		++i;
		v2.clear();
	}
	check_count(pOut->news_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	sscanf(c[i_open_date].c_str(), "%hd-%hd-%hd",
			&pOut->open_date.year,
			&pOut->open_date.month,
			&pOut->open_date.day);
	pOut->pe_ratio = c[i_pe_ratio].as(double());
	strncpy(pOut->s_name, c[i_s_name].c_str(), cS_NAME_len);
	pOut->num_out = c[i_num_out].as(int());
	strncpy(pOut->sp_rate, c[i_sp_rate].c_str(), cSP_RATE_len);
	sscanf(c[i_start_date].c_str(), "%hd-%hd-%hd",
			&pOut->start_date.year,
			&pOut->start_date.month,
			&pOut->start_date.day);
	pOut->status = c[i_status].as(int());
	pOut->yield = c[i_yield].as(double());
}

void CDBConnection::execute(const TTradeCleanupFrame1Input *pIn,
		TTradeCleanupFrame1Output *pOut)
{
	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeCleanupFrame1('" <<
			pIn->st_canceled_id << "','" <<
			pIn->st_pending_id << "','" <<
			pIn->st_submitted_id << "'," <<
			pIn->start_trade_id << ")";

	result R(m_Txn->exec(osSQL.str()));
	if (R.empty()) {
		pOut->status = CBaseTxnErr::ROLLBACK;
		ostringstream msg;
		msg << time(NULL) << " " << pthread_self() << endl <<
				"NO RESULTS" << endl <<
				osSQL.str() << endl;
		bh->logErrorMessage(msg.str(), false);
		rollback();
		throw;
	}

	result::const_iterator c = R.begin();

	pOut->status = c[0].as(int());
}

void CDBConnection::execute(const TTradeLookupFrame1Input *pIn,
		TTradeLookupFrame1Output *pOut)
{
	enum tlf1 {
			i_bid_price=0, i_cash_transaction_amount,
			i_cash_transaction_dts, i_cash_transaction_name, i_exec_name,
			i_is_cash, i_is_market, i_num_found, i_settlement_amount,
			i_settlement_cash_due_date, i_settlement_cash_type,
			i_status, i_trade_history_dts, i_trade_history_status_id,
			i_trade_price
	};

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

	result R(m_Txn->exec(osSQL.str()));
	if (R.empty()) {
		pOut->status = CBaseTxnErr::ROLLBACK;
		ostringstream msg;
		msg << time(NULL) << " " << pthread_self() << endl <<
				"NO RESULTS" << endl <<
				osSQL.str() << endl;
		bh->logErrorMessage(msg.str(), false);
		rollback();
		throw;
	}

	result::const_iterator c = R.begin();

	pOut->num_found = c[i_num_found].as(int());

	vector<string> vAux;
	vector<string>::iterator p;

	TokenizeSmart(c[i_bid_price].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].bid_price = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_cash_transaction_amount].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].cash_transaction_amount = atof((*p).c_str());
		++i;
	}
	vAux.clear();

	TokenizeSmart(c[i_cash_transaction_dts].c_str(), vAux);
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

	TokenizeSmart(c[i_cash_transaction_name].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].cash_transaction_name, (*p).c_str(),
				cCT_NAME_len);
		++i;
	}
	vAux.clear();

	TokenizeSmart(c[i_exec_name].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].exec_name, (*p).c_str(), cEXEC_NAME_len);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_is_cash].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].is_cash = atoi((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_is_market].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].is_market = atoi((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_settlement_amount].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].settlement_amount = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_settlement_cash_due_date].c_str(), vAux);
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
				c[i_settlement_cash_due_date].c_str() << endl;
	}
	vAux.clear();
	TokenizeSmart(c[i_settlement_cash_type].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].settlement_cash_type, (*p).c_str(),
				cSE_CASH_TYPE_len);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->status = c[i_status].as(int());

	TokenizeArray(c[i_trade_history_dts].c_str(), vAux);
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

	TokenizeArray(c[i_trade_history_status_id].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		vector<string> v2;
		vector<string>::iterator p2;
		TokenizeSmart((*p).c_str(), v2);
		int j = 0;
		for (p2 = v2.begin(); p2 != v2.end(); ++p2) {
			strncpy(pOut->trade_info[i].trade_history_status_id[j],
					(*p2).c_str(), cTH_ST_ID_len);
			++j;
		}
		++i;
	}
	vAux.clear();

	TokenizeSmart(c[i_trade_price].c_str(), vAux);
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
	enum tlf2 {
			i_bid_price=0, i_cash_transaction_amount, i_cash_transaction_dts,
			i_cash_transaction_name, i_exec_name, i_is_cash, i_num_found,
			i_settlement_amount, i_settlement_cash_due_date,
			i_settlement_cash_type, i_status, i_trade_history_dts,
			i_trade_history_status_id, i_trade_list, i_trade_price
	};

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

	result R(m_Txn->exec(osSQL.str()));
	if (R.empty()) {
		pOut->status = CBaseTxnErr::ROLLBACK;
		ostringstream msg;
		msg << time(NULL) << " " << pthread_self() << endl <<
				"NO RESULTS" << endl <<
				osSQL.str() << endl;
		bh->logErrorMessage(msg.str(), false);
		rollback();
		throw;
	}

	result::const_iterator c = R.begin();

	pOut->num_found = c[i_num_found].as(int());

	vector<string> vAux;
	vector<string>::iterator p;

	TokenizeSmart(c[i_bid_price].c_str(), vAux);
	int i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].bid_price = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_cash_transaction_amount].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].cash_transaction_amount = atof((*p).c_str());
		++i;
	}
	// FIXME: According to spec, this may not match the returned number found?
	vAux.clear();

	TokenizeSmart(c[i_cash_transaction_dts].c_str(), vAux);
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

	TokenizeSmart(c[i_cash_transaction_name].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].cash_transaction_name, (*p).c_str(),
				cCT_NAME_len);
		++i;
	}
	// FIXME: According to spec, this may not match the returned number found?
	vAux.clear();

	TokenizeSmart(c[i_exec_name].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].exec_name, (*p).c_str(), cEXEC_NAME_len);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_is_cash].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].is_cash = atoi((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_settlement_amount].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].settlement_amount = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_settlement_cash_due_date].c_str(), vAux);
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

	TokenizeSmart(c[i_settlement_cash_type].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].settlement_cash_type, (*p).c_str(),
				cSE_CASH_TYPE_len);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->status = c[i_status].as(int());

	TokenizeArray(c[i_trade_history_dts].c_str(), vAux);
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

	TokenizeArray(c[i_trade_history_status_id].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		vector<string> v2;
		vector<string>::iterator p2;
		TokenizeSmart((*p).c_str(), v2);
		int j = 0;
		for (p2 = v2.begin(); p2 != v2.end(); ++p2) {
			strncpy(pOut->trade_info[i].trade_history_status_id[j],
						(*p2).c_str(), cTH_ST_ID_len);
			++j;
		}
		++i;
	}
	vAux.clear();

	TokenizeSmart(c[i_trade_list].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].trade_id = atol((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_trade_price].c_str(), vAux);
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
	enum tlf3 {
			i_acct_id=0, i_cash_transaction_amount, i_cash_transaction_dts,
			i_cash_transaction_name, i_exec_name, i_is_cash, i_num_found,
			i_price, i_quantity, i_settlement_amount,
			i_settlement_cash_due_date, i_settlement_cash_type, i_status,
			i_trade_dts, i_trade_history_dts, i_trade_history_status_id,
			i_trade_list, i_trade_type
	};

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

	result R(m_Txn->exec(osSQL.str()));
	if (R.empty()) {
		pOut->status = CBaseTxnErr::ROLLBACK;
		ostringstream msg;
		msg << time(NULL) << " " << pthread_self() << endl <<
				"NO RESULTS" << endl <<
				osSQL.str() << endl;
		bh->logErrorMessage(msg.str(), false);
		rollback();
		throw;
	}

	result::const_iterator c = R.begin();

	pOut->num_found = c[i_num_found].as(int());

	vector<string> vAux;
	vector<string>::iterator p;

	TokenizeSmart(c[i_acct_id].c_str(), vAux);
	int i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].acct_id = atol((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_cash_transaction_amount].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].cash_transaction_amount = atof((*p).c_str());
		++i;
	}
	// FIXME: According to spec, this may not match the returned number found?
	vAux.clear();

	TokenizeSmart(c[i_cash_transaction_dts].c_str(), vAux);
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

	TokenizeSmart(c[i_cash_transaction_name].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].cash_transaction_name, (*p).c_str(),
				cCT_NAME_len);
		++i;
	}
	// FIXME: According to spec, this may not match the returned number found?
	vAux.clear();

	TokenizeSmart(c[i_exec_name].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].exec_name, (*p).c_str(),
				cEXEC_NAME_len);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_is_cash].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].is_cash = atoi((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_price].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].price = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_quantity].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].quantity = atoi((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_settlement_amount].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].settlement_amount = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_settlement_cash_due_date].c_str(), vAux);
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

	TokenizeSmart(c[i_settlement_cash_type].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].settlement_cash_type, (*p).c_str(),
				cSE_CASH_TYPE_len);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->status = c[i_status].as(int());

	TokenizeSmart(c[i_trade_dts].c_str(), vAux);
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

	TokenizeArray(c[i_trade_history_dts].c_str(), vAux);
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

	TokenizeArray(c[i_trade_history_status_id].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		vector<string> v2;
		vector<string>::iterator p2;
		TokenizeSmart((*p).c_str(), v2);
		int j = 0;
		for (p2 = v2.begin(); p2 != v2.end(); ++p2) {
			strncpy(pOut->trade_info[i].trade_history_status_id[j],
					(*p2).c_str(), cTH_ST_ID_len);
			++j;
		}
		++i;
	}
	vAux.clear();

	TokenizeSmart(c[i_trade_list].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].trade_id = atol((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_trade_type].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].trade_type, (*p).c_str(), cTT_ID_len);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();
}

void CDBConnection::execute(const TTradeLookupFrame4Input *pIn,
		TTradeLookupFrame4Output *pOut)
{
	enum tlf4 {
			i_holding_history_id=0, i_holding_history_trade_id, i_num_found,
			i_quantity_after, i_quantity_before, i_status, i_trade_id
	};

	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeLookupFrame4(" <<
			pIn->acct_id << ",'" <<
			pIn->trade_dts.year << "-" <<
			pIn->trade_dts.month << "-" <<
			pIn->trade_dts.day << " " <<
			pIn->trade_dts.hour << ":" <<
			pIn->trade_dts.minute << ":" <<
			pIn->trade_dts.second << "')";

	result R(m_Txn->exec(osSQL.str()));
	if (R.empty()) {
		pOut->status = CBaseTxnErr::ROLLBACK;
		ostringstream msg;
		msg << time(NULL) << " " << pthread_self() << endl <<
				"NO RESULTS" << endl <<
				osSQL.str() << endl;
		bh->logErrorMessage(msg.str(), false);
		rollback();
		throw;
	}

	result::const_iterator c = R.begin();

	pOut->num_found = c[i_num_found].as(int());

	vector<string> vAux;
	vector<string>::iterator p;

	TokenizeSmart(c[i_holding_history_id].c_str(), vAux);
	int i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].holding_history_id = atol((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_holding_history_trade_id].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].holding_history_trade_id = atol((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_quantity_after].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].quantity_after = atol((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_quantity_before].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].quantity_before = atol((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->status = c[i_status].as(int());
	pOut->trade_id = c[i_trade_id].as(long());
}

void CDBConnection::execute(const TTradeOrderFrame1Input *pIn,
		TTradeOrderFrame1Output *pOut)
{
	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeOrderFrame1(" << pIn->acct_id << ")";

	// For PostgreSQL, see comment in the Concurrency Control chapter, under
	// the Transaction Isolation section for dealing with serialization
	// failures.  These serialization failures can occur with REPEATABLE READS
	// or SERIALIZABLE.
	int iNumRetry = 1;
	result R;
	while (true) {
		try {
			R = m_Txn->exec(osSQL.str());
			if (R.empty()) {
				pOut->status = CBaseTxnErr::ROLLBACK;
				ostringstream msg;
				msg << time(NULL) << " " << pthread_self() << endl <<
						"NO RESULTS" << endl <<
						osSQL.str() << endl;
				bh->logErrorMessage(msg.str(), false);
				rollback();
				throw;
			}
			break;
		} catch (const pqxx::sql_error &e) {
			if (PGSQL_SERIALIZE_ERROR.compare(e.what()) == 0) {
					ostringstream msg;
					msg << time(NULL) << " " << pthread_self() <<
							"TRADE_ORDER" << endl;
					msg << "attempt: " << iNumRetry << endl;
					msg << "what: " << e.what();
					msg << "query: " << e.query() << endl;

					if (iNumRetry <= iMaxRetries) {
						// Wait 1 second to give the other transaction time to
						// finish.
						bh->logErrorMessage(msg.str(), false);
						iNumRetry++;
						sleep(1);
					} else {
						// Couldn't resubmit successfully.
						msg << "giving up" << endl;
						bh->logErrorMessage(msg.str(), false);
						pOut->status = CBaseTxnErr::ROLLBACK;
						rollback();
						throw;
					}
			} else {
				pOut->status = CBaseTxnErr::ROLLBACK;
				rollback();
				throw;
			}
		}
	}

	result::const_iterator c = R.begin();

	strncpy(pOut->acct_name, c[0].c_str(), cCA_NAME_len);
	pOut->broker_id = c[1].as(long());
	strncpy(pOut->broker_name, c[2].c_str(), cB_NAME_len);
	strncpy(pOut->cust_f_name, c[3].c_str(), cF_NAME_len);
	pOut->cust_id = c[4].as(long());
	strncpy(pOut->cust_l_name, c[5].c_str(), cL_NAME_len);
	pOut->cust_tier = c[6].as(int());
	pOut->status = c[7].as(int());
	strncpy(pOut->tax_id, c[8].c_str(), cTAX_ID_len);
	pOut->tax_status = c[9].as(int());
}

void CDBConnection::execute(const TTradeOrderFrame2Input *pIn,
		TTradeOrderFrame2Output *pOut)
{
	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeOrderFrame2(" <<
			pIn->acct_id << ",'" <<
			escape(pIn->exec_f_name) << "','" <<
			escape(pIn->exec_l_name) << "','" <<
			pIn->exec_tax_id<<"')";

	// For PostgreSQL, see comment in the Concurrency Control chapter, under
	// the Transaction Isolation section for dealing with serialization
	// failures.  These serialization failures can occur with REPEATABLE READS
	// or SERIALIZABLE.
	int iNumRetry = 1;
	result R;
	while (true) {
		try {
			R = m_Txn->exec(osSQL.str());
			if (R.empty()) {
				pOut->status = CBaseTxnErr::ROLLBACK;
				ostringstream msg;
				msg << time(NULL) << " " << pthread_self() << endl <<
						"NO RESULTS" << endl <<
						osSQL.str() << endl;
				bh->logErrorMessage(msg.str(), false);
				rollback();
				throw;
			}
			break;
		} catch (const pqxx::sql_error &e) {
			if (PGSQL_SERIALIZE_ERROR.compare(e.what()) == 0) {
					ostringstream msg;
					msg << time(NULL) << " " << pthread_self() <<
							"TRADE_ORDER" << endl;
					msg << "attempt: " << iNumRetry << endl;
					msg << "what: " << e.what();
					msg << "query: " << e.query() << endl;

					if (iNumRetry <= iMaxRetries) {
						// Wait 1 second to give the other transaction time to
						// finish.
						bh->logErrorMessage(msg.str(), false);
						iNumRetry++;
						sleep(1);
					} else {
						// Couldn't resubmit successfully.
						msg << "giving up" << endl;
						bh->logErrorMessage(msg.str(), false);
						pOut->status = CBaseTxnErr::ROLLBACK;
						rollback();
						throw;
					}
			} else {
				pOut->status = CBaseTxnErr::ROLLBACK;
				rollback();
				throw;
			}
		}
	}

	result::const_iterator c = R.begin();

	if (c[0].is_null() == false) {
		strncpy(pOut->ap_acl, c[0].c_str(), cACL_len);
	} else {
		pOut->ap_acl[0] = '\0';
	}
	pOut->status = c[1].as(int());
}

void CDBConnection::execute(const TTradeOrderFrame3Input *pIn,
		TTradeOrderFrame3Output *pOut)
{
	ostringstream osSQL;
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
			pIn->type_is_margin << "::SMALLINT,'" <<
			escape(pIn->co_name) << "'," <<
			pIn->requested_price << ",'" <<
			pIn->symbol << "')";

	// For PostgreSQL, see comment in the Concurrency Control chapter, under
	// the Transaction Isolation section for dealing with serialization
	// failures.  These serialization failures can occur with REPEATABLE READS
	// or SERIALIZABLE.
	int iNumRetry = 1;
	result R;
	while (true) {
		try {
			R = m_Txn->exec(osSQL.str());
			if (R.empty()) {
				pOut->status = CBaseTxnErr::ROLLBACK;
				ostringstream msg;
				msg << time(NULL) << " " << pthread_self() << endl <<
						"NO RESULTS" << endl <<
						osSQL.str() << endl;
				bh->logErrorMessage(msg.str(), false);
				rollback();
				throw;
			}
			break;
		} catch (const pqxx::sql_error &e) {
			if (PGSQL_SERIALIZE_ERROR.compare(e.what()) == 0) {
					ostringstream msg;
					msg << time(NULL) << " " << pthread_self() <<
							"TRADE_ORDER" << endl;
					msg << "attempt: " << iNumRetry << endl;
					msg << "what: " << e.what();
					msg << "query: " << e.query() << endl;

					if (iNumRetry <= iMaxRetries) {
						// Wait 1 second to give the other transaction time to
						// finish.
						bh->logErrorMessage(msg.str(), false);
						iNumRetry++;
						sleep(1);
					} else {
						// Couldn't resubmit successfully.
						msg << "giving up" << endl;
						bh->logErrorMessage(msg.str(), false);
						pOut->status = CBaseTxnErr::ROLLBACK;
						rollback();
						throw;
					}
			} else {
				pOut->status = CBaseTxnErr::ROLLBACK;
				rollback();
				throw;
			}
		}
	}

	result::const_iterator c = R.begin();

	strncpy(pOut->co_name, c[0].c_str(), cCO_NAME_len);
	pOut->requested_price = c[1].as(double());
	strncpy(pOut->symbol, c[2].c_str(), cSYMBOL_len);
	pOut->buy_value = c[3].as(double());
	pOut->charge_amount = c[4].as(double());
	pOut->comm_rate = c[5].as(double());
	pOut->cust_assets = c[6].as(double());
	pOut->market_price = c[7].as(double());
	strncpy(pOut->s_name, c[8].c_str(), cS_NAME_len);
	pOut->sell_value = c[9].as(double());
	pOut->status = c[10].as(int());
	strncpy(pOut->status_id, c[11].c_str(), cTH_ST_ID_len);
	pOut->tax_amount = c[12].as(double());
	pOut->type_is_market = (c[13].c_str()[0] == 't' ? 1 : 0);
	pOut->type_is_sell = (c[14].c_str()[0] == 't' ? 1 : 0);
}

void CDBConnection::execute(const TTradeOrderFrame4Input *pIn,
		TTradeOrderFrame4Output *pOut)
{
	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeOrderFrame4(" <<
			pIn->acct_id << "," <<
			pIn->broker_id << "," <<
			pIn->charge_amount << "," <<
			pIn->comm_amount << ",'" <<
			escape(pIn->exec_name) << "'," <<
			pIn->is_cash << "::SMALLINT," <<
			pIn->is_lifo << "::SMALLINT," <<
			pIn->requested_price << ",'" <<
			pIn->status_id << "','" <<
			pIn->symbol << "'," <<
			pIn->trade_qty << ",'" <<
			pIn->trade_type_id << "'," <<
			pIn->type_is_market << "::SMALLINT)";

	// For PostgreSQL, see comment in the Concurrency Control chapter, under
	// the Transaction Isolation section for dealing with serialization
	// failures.  These serialization failures can occur with REPEATABLE READS
	// or SERIALIZABLE.
	int iNumRetry = 1;
	result R;
	while (true) {
		try {
			R = m_Txn->exec(osSQL.str());
			if (R.empty()) {
				pOut->status = CBaseTxnErr::ROLLBACK;
				ostringstream msg;
				msg << time(NULL) << " " << pthread_self() << endl <<
						"NO RESULTS" << endl <<
						osSQL.str() << endl;
				bh->logErrorMessage(msg.str(), false);
				rollback();
				throw;
			}
			break;
		} catch (const pqxx::sql_error &e) {
			if (PGSQL_SERIALIZE_ERROR.compare(e.what()) == 0) {
					ostringstream msg;
					msg << time(NULL) << " " << pthread_self() <<
							"TRADE_ORDER" << endl;
					msg << "attempt: " << iNumRetry << endl;
					msg << "what: " << e.what();
					msg << "query: " << e.query() << endl;

					if (iNumRetry <= iMaxRetries) {
						// Wait 1 second to give the other transaction time to
						// finish.
						bh->logErrorMessage(msg.str(), false);
						iNumRetry++;
						sleep(1);
					} else {
						// Couldn't resubmit successfully.
						msg << "giving up" << endl;
						bh->logErrorMessage(msg.str(), false);
						pOut->status = CBaseTxnErr::ROLLBACK;
						rollback();
						throw;
					}
			} else {
				pOut->status = CBaseTxnErr::ROLLBACK;
				rollback();
				throw;
			}
		}
	}

	result::const_iterator c = R.begin();

	pOut->status = c[0].as(int());
	pOut->trade_id = c[1].as(long());
}

void CDBConnection::execute(const TTradeResultFrame1Input *pIn,
		TTradeResultFrame1Output *pOut)
{
	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeResultFrame1(" << pIn->trade_id << ")";

	// For PostgreSQL, see comment in the Concurrency Control chapter, under
	// the Transaction Isolation section for dealing with serialization
	// failures.  These serialization failures can occur with REPEATABLE READS
	// or SERIALIZABLE.
	int iNumRetry = 1;
	result R;
	while (true) {
		try {
			R = m_Txn->exec(osSQL.str());
			if (R.empty()) {
				pOut->status = CBaseTxnErr::ROLLBACK;
				ostringstream msg;
				msg << time(NULL) << " " << pthread_self() << endl <<
						"NO RESULTS" << endl <<
						osSQL.str() << endl;
				bh->logErrorMessage(msg.str(), false);
				rollback();
				throw;
			}
			break;
		} catch (const pqxx::sql_error &e) {
			if (PGSQL_SERIALIZE_ERROR.compare(e.what()) == 0) {
					ostringstream msg;
					msg << time(NULL) << " " << pthread_self() <<
							"TRADE_RESULT" << endl;
					msg << "attempt: " << iNumRetry << endl;
					msg << "what: " << e.what();
					msg << "query: " << e.query() << endl;

					if (iNumRetry <= iMaxRetries) {
						// Wait 1 second to give the other transaction time to
						// finish.
						bh->logErrorMessage(msg.str(), false);
						iNumRetry++;
						sleep(1);
					} else {
						// Couldn't resubmit successfully.
						msg << "giving up" << endl;
						bh->logErrorMessage(msg.str(), false);
						pOut->status = CBaseTxnErr::ROLLBACK;
						rollback();
						throw;
					}
			} else {
				pOut->status = CBaseTxnErr::ROLLBACK;
				rollback();
				throw;
			}
		}
	}

	result::const_iterator c = R.begin();

	pOut->acct_id = c[0].as(long());
	pOut->charge = c[1].as(double());
	pOut->hs_qty = c[2].as(int());
	pOut->is_lifo = c[3].as(int());
	pOut->status = c[4].as(int());
	strncpy(pOut->symbol, c[5].c_str(), cSYMBOL_len);
	pOut->trade_is_cash = c[6].as(int());
	pOut->trade_qty = c[7].as(int());
	strncpy(pOut->type_id, c[8].c_str(), cTT_ID_len);
	pOut->type_is_market = c[9].as(int());
	pOut->type_is_sell = c[10].as(int());
	strncpy(pOut->type_name, c[11].c_str(), cTT_NAME_len);
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

	// For PostgreSQL, see comment in the Concurrency Control chapter, under
	// the Transaction Isolation section for dealing with serialization
	// failures.  These serialization failures can occur with REPEATABLE READS
	// or SERIALIZABLE.
	int iNumRetry = 1;
	result R;
	while (true) {
		try {
			R = m_Txn->exec(osSQL.str());
			if (R.empty()) {
				pOut->status = CBaseTxnErr::ROLLBACK;
				ostringstream msg;
				msg << time(NULL) << " " << pthread_self() << endl <<
						"NO RESULTS" << endl <<
						osSQL.str() << endl;
				bh->logErrorMessage(msg.str(), false);
				rollback();
				throw;
			}
			break;
		} catch (const pqxx::sql_error &e) {
			if (PGSQL_SERIALIZE_ERROR.compare(e.what()) == 0) {
					ostringstream msg;
					msg << time(NULL) << " " << pthread_self() <<
							"TRADE_RESULT" << endl;
					msg << "attempt: " << iNumRetry << endl;
					msg << "what: " << e.what();
					msg << "query: " << e.query() << endl;

					if (iNumRetry <= iMaxRetries) {
						// Wait 1 second to give the other transaction time to
						// finish.
						bh->logErrorMessage(msg.str(), false);
						iNumRetry++;
						sleep(1);
					} else {
						// Couldn't resubmit successfully.
						msg << "giving up" << endl;
						bh->logErrorMessage(msg.str(), false);
						pOut->status = CBaseTxnErr::ROLLBACK;
						rollback();
						throw;
					}
			} else {
				pOut->status = CBaseTxnErr::ROLLBACK;
				rollback();
				throw;
			}
		}
	}

	result::const_iterator c = R.begin();

	pOut->broker_id = c[0].as(long());
	pOut->buy_value = c[1].as(double());
	pOut->cust_id = c[2].as(long());
	pOut->sell_value = c[3].as(double());
	pOut->status = c[4].as(int());
	pOut->tax_status = c[5].as(int());
	sscanf(c[6].c_str(), "%hd-%hd-%hd %hd:%hd:%hd.%*d",
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

	// For PostgreSQL, see comment in the Concurrency Control chapter, under
	// the Transaction Isolation section for dealing with serialization
	// failures.  These serialization failures can occur with REPEATABLE READS
	// or SERIALIZABLE.
	int iNumRetry = 1;
	result R;
	while (true) {
		try {
			R = m_Txn->exec(osSQL.str());
			if (R.empty()) {
				pOut->status = CBaseTxnErr::ROLLBACK;
				ostringstream msg;
				msg << time(NULL) << " " << pthread_self() << endl <<
						"NO RESULTS" << endl <<
						osSQL.str() << endl;
				bh->logErrorMessage(msg.str(), false);
				rollback();
				throw;
			}
			break;
		} catch (const pqxx::sql_error &e) {
			if (PGSQL_SERIALIZE_ERROR.compare(e.what()) == 0) {
					ostringstream msg;
					msg << time(NULL) << " " << pthread_self() <<
							"TRADE_RESULT" << endl;
					msg << "attempt: " << iNumRetry << endl;
					msg << "what: " << e.what();
					msg << "query: " << e.query() << endl;

					if (iNumRetry <= iMaxRetries) {
						// Wait 1 second to give the other transaction time to
						// finish.
						bh->logErrorMessage(msg.str(), false);
						iNumRetry++;
						sleep(1);
					} else {
						// Couldn't resubmit successfully.
						msg << "giving up" << endl;
						bh->logErrorMessage(msg.str(), false);
						pOut->status = CBaseTxnErr::ROLLBACK;
						rollback();
						throw;
					}
			} else {
				pOut->status = CBaseTxnErr::ROLLBACK;
				rollback();
				throw;
			}
		}
	}

	result::const_iterator c = R.begin();

	pOut->status = c[0].as(int());
	pOut->tax_amount = c[1].as(double());
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

	// For PostgreSQL, see comment in the Concurrency Control chapter, under
	// the Transaction Isolation section for dealing with serialization
	// failures.  These serialization failures can occur with REPEATABLE READS
	// or SERIALIZABLE.
	int iNumRetry = 1;
	result R;
	while (true) {
		try {
			R = m_Txn->exec(osSQL.str());
			if (R.empty()) {
				pOut->status = CBaseTxnErr::ROLLBACK;
				ostringstream msg;
				msg << time(NULL) << " " << pthread_self() << endl <<
						"NO RESULTS" << endl <<
						osSQL.str() << endl;
				bh->logErrorMessage(msg.str(), false);
				rollback();
				throw;
			}
			break;
		} catch (const pqxx::sql_error &e) {
			if (PGSQL_SERIALIZE_ERROR.compare(e.what()) == 0) {
					ostringstream msg;
					msg << time(NULL) << " " << pthread_self() <<
							"TRADE_RESULT" << endl;
					msg << "attempt: " << iNumRetry << endl;
					msg << "what: " << e.what();
					msg << "query: " << e.query() << endl;

					if (iNumRetry <= iMaxRetries) {
						// Wait 1 second to give the other transaction time to
						// finish.
						bh->logErrorMessage(msg.str(), false);
						iNumRetry++;
						sleep(1);
					} else {
						// Couldn't resubmit successfully.
						msg << "giving up" << endl;
						bh->logErrorMessage(msg.str(), false);
						pOut->status = CBaseTxnErr::ROLLBACK;
						rollback();
						throw;
					}
			} else {
				pOut->status = CBaseTxnErr::ROLLBACK;
				rollback();
				throw;
			}
		}
	}

	result::const_iterator c = R.begin();

	pOut->comm_rate = c[0].as(double());
	strncpy(pOut->s_name, c[1].c_str(), cS_NAME_len);
	pOut->status = c[2].as(int());
}

void CDBConnection::execute(const TTradeResultFrame5Input *pIn,
TTradeResultFrame5Output *pOut)
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
	int iNumRetry = 1;
	result R;
	while (true) {
		try {
			R = m_Txn->exec(osSQL.str());
			if (R.empty()) {
				pOut->status = CBaseTxnErr::ROLLBACK;
				ostringstream msg;
				msg << time(NULL) << " " << pthread_self() << endl <<
						"NO RESULTS" << endl <<
						osSQL.str() << endl;
				bh->logErrorMessage(msg.str(), false);
				rollback();
				throw;
			}
			break;
		} catch (const pqxx::sql_error &e) {
			if (PGSQL_SERIALIZE_ERROR.compare(e.what()) == 0) {
					ostringstream msg;
					msg << time(NULL) << " " << pthread_self() <<
							"TRADE_RESULT" << endl;
					msg << "attempt: " << iNumRetry << endl;
					msg << "what: " << e.what();
					msg << "query: " << e.query() << endl;

					if (iNumRetry <= iMaxRetries) {
						// Wait 1 second to give the other transaction time to
						// finish.
						bh->logErrorMessage(msg.str(), false);
						iNumRetry++;
						sleep(1);
					} else {
						// Couldn't resubmit successfully.
						msg << "giving up" << endl;
						bh->logErrorMessage(msg.str(), false);
						pOut->status = CBaseTxnErr::ROLLBACK;
						rollback();
						throw;
					}
			} else {
				pOut->status = CBaseTxnErr::ROLLBACK;
				rollback();
				throw;
			}
		}
	}

	result::const_iterator c = R.begin();
	pOut->status = c[0].as(int());
}

void CDBConnection::execute(const TTradeResultFrame6Input *pIn,
		TTradeResultFrame6Output *pOut)
{
	ostringstream osSQL;
	osSQL << "SELECT * FROM TradeResultFrame6(" <<
			pIn->acct_id << ",'" <<
			pIn->due_date.year << "-"<<
			pIn->due_date.month << "-" <<
			pIn->due_date.day << " " <<
			pIn->due_date.hour << ":" <<
			pIn->due_date.minute << ":" <<
			pIn->due_date.second << "','" <<
			escape(pIn->s_name) << "', " <<
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

	// For PostgreSQL, see comment in the Concurrency Control chapter, under
	// the Transaction Isolation section for dealing with serialization
	// failures.  These serialization failures can occur with REPEATABLE READS
	// or SERIALIZABLE.
	int iNumRetry = 1;
	result R;
	while (true) {
		try {
			R = m_Txn->exec(osSQL.str());
			if (R.empty()) {
				pOut->status = CBaseTxnErr::ROLLBACK;
				ostringstream msg;
				msg << time(NULL) << " " << pthread_self() << endl <<
						"NO RESULTS" << endl <<
						osSQL.str() << endl;
				bh->logErrorMessage(msg.str(), false);
				rollback();
				throw;
			}
			break;
		} catch (const pqxx::sql_error &e) {
			if (PGSQL_SERIALIZE_ERROR.compare(e.what()) == 0) {
					ostringstream msg;
					msg << time(NULL) << " " << pthread_self() <<
							"TRADE_RESULT" << endl;
					msg << "attempt: " << iNumRetry << endl;
					msg << "what: " << e.what();
					msg << "query: " << e.query() << endl;

					if (iNumRetry <= iMaxRetries) {
						// Wait 1 second to give the other transaction time to
						// finish.
						bh->logErrorMessage(msg.str(), false);
						iNumRetry++;
						sleep(1);
					} else {
						// Couldn't resubmit successfully.
						msg << "giving up" << endl;
						bh->logErrorMessage(msg.str(), false);
						pOut->status = CBaseTxnErr::ROLLBACK;
						rollback();
						throw;
					}
			} else {
				pOut->status = CBaseTxnErr::ROLLBACK;
				rollback();
				throw;
			}
		}
	}

	result::const_iterator c = R.begin();

	pOut->acct_bal = c[0].as(double());
	pOut->status = c[1].as(int());
}

void CDBConnection::execute(const TTradeStatusFrame1Input *pIn,
		TTradeStatusFrame1Output *pOut)
{
	enum tsf1 {
			i_broker_name=0, i_charge, i_cust_f_name, i_cust_l_name,
			i_ex_name, i_exec_name, i_s_name, i_status, i_status_name,
			i_symbol, i_trade_dts, i_trade_id, i_trade_qty, i_type_name
	};

	ostringstream osSQL;
	osSQL << "SELECT * from TradeStatusFrame1(" << pIn->acct_id << ")";

	result R(m_Txn->exec(osSQL.str()));
	if (R.empty()) {
		pOut->status = CBaseTxnErr::ROLLBACK;
		ostringstream msg;
		msg << time(NULL) << " " << pthread_self() << endl <<
				"NO RESULTS" << endl <<
				osSQL.str() << endl;
		bh->logErrorMessage(msg.str(), false);
		rollback();
		throw;
	}

	result::const_iterator c = R.begin();

	vector<string> vAux;
	vector<string>::iterator p;

	strncpy(pOut->broker_name, c[i_broker_name].c_str(), cB_NAME_len);

	TokenizeSmart(c[i_charge].c_str(), vAux);
	int i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->charge[i] = atof((*p).c_str());
		++i;
	}
	vAux.clear();

	strncpy(pOut->cust_f_name, c[i_cust_f_name].c_str(), cF_NAME_len);
	strncpy(pOut->cust_l_name, c[i_cust_l_name].c_str(), cL_NAME_len);

	int len = i;

	TokenizeSmart(c[i_ex_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->ex_name[i], (*p).c_str(), cEX_NAME_len);
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_exec_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->exec_name[i], (*p).c_str(), cEXEC_NAME_len);
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_s_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->s_name[i], (*p).c_str(), cS_NAME_len);
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->status = c[i_status].as(int());

	TokenizeSmart(c[i_status_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->status_name[i], (*p).c_str(), cST_NAME_len);
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();
	TokenizeSmart(c[i_symbol].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->symbol[i], (*p).c_str(), cSYMBOL_len);
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_trade_dts].c_str(), vAux);
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

	TokenizeSmart(c[i_trade_id].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_id[i] = atol((*p).c_str());
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_trade_qty].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_qty[i] = atoi((*p).c_str());
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_type_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->type_name[i], (*p).c_str(), cTT_NAME_len);
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();
}

void CDBConnection::execute(const TTradeUpdateFrame1Input *pIn,
		TTradeUpdateFrame1Output *pOut)
{
	enum tuf1 {
			i_bid_price=0, i_cash_transaction_amount,
			i_cash_transaction_dts, i_cash_transaction_name, i_exec_name,
			i_is_cash, i_is_market, i_num_found, i_num_updated,
			i_settlement_amount, i_settlement_cash_due_date,
			i_settlement_cash_type, i_status, i_trade_history_dts,
			i_trade_history_status_id, i_trade_price
	};

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

	// For PostgreSQL, see comment in the Concurrency Control chapter, under
	// the Transaction Isolation section for dealing with serialization
	// failures.  These serialization failures can occur with REPEATABLE READS
	// or SERIALIZABLE.
	int iNumRetry = 1;
	result R;
	while (true) {
		try {
			R = m_Txn->exec(osSQL.str());
			if (R.empty()) {
				pOut->status = CBaseTxnErr::ROLLBACK;
				ostringstream msg;
				msg << time(NULL) << " " << pthread_self() << endl <<
						"NO RESULTS" << endl <<
						osSQL.str() << endl;
				bh->logErrorMessage(msg.str(), false);
				rollback();
				throw;
			}
			break;
		} catch (const pqxx::sql_error &e) {
			if (PGSQL_SERIALIZE_ERROR.compare(e.what()) == 0) {
					ostringstream msg;
					msg << time(NULL) << " " << pthread_self() <<
							"TRADE_UPDATE" << endl;
					msg << "attempt: " << iNumRetry << endl;
					msg << "what: " << e.what();
					msg << "query: " << e.query() << endl;

					if (iNumRetry <= iMaxRetries) {
						// Wait 1 second to give the other transaction time to
						// finish.
						bh->logErrorMessage(msg.str(), false);
						iNumRetry++;
						sleep(1);
					} else {
						// Couldn't resubmit successfully.
						msg << "giving up" << endl;
						bh->logErrorMessage(msg.str(), false);
						pOut->status = CBaseTxnErr::ROLLBACK;
						rollback();
						throw;
					}
			} else {
				pOut->status = CBaseTxnErr::ROLLBACK;
				rollback();
				throw;
			}
		}
	}

	result::const_iterator c = R.begin();

	pOut->num_found = c[i_num_found].as(int());

	vector<string> vAux;
	vector<string>::iterator p;

	TokenizeSmart(c[i_bid_price].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].bid_price = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_cash_transaction_amount].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].cash_transaction_amount = atof((*p).c_str());
		++i;
	}
	// FIXME: According to spec, this may not match the returned number found?
	vAux.clear();

	TokenizeSmart(c[i_cash_transaction_dts].c_str(), vAux);
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

	TokenizeSmart(c[i_cash_transaction_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].cash_transaction_name, (*p).c_str(),
				cCT_NAME_len);
		++i;
	}
	// FIXME: According to spec, this may not match the returned number found?
	vAux.clear();

	TokenizeSmart(c[i_exec_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].exec_name, (*p).c_str(), cEXEC_NAME_len);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();
	TokenizeSmart(c[i_is_cash].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].is_cash = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_is_market].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].is_market = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_settlement_amount].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].settlement_amount = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_settlement_cash_due_date].c_str(), vAux);
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

	TokenizeSmart(c[i_settlement_cash_type].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].settlement_cash_type, (*p).c_str(),
				cSE_CASH_TYPE_len);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->status = c[i_status].as(int());
	pOut->num_updated = c[i_num_updated].as(int());

	TokenizeArray(c[i_trade_history_dts].c_str(), vAux);
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

	TokenizeArray(c[i_trade_history_status_id].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		vector<string> v2;
		vector<string>::iterator p2;
		TokenizeSmart((*p).c_str(), v2);
		p2 = v2.begin();
		strncpy(pOut->trade_info[i].trade_history_status_id[0],
				(*p2++).c_str(), cTH_ST_ID_len);
		strncpy(pOut->trade_info[i].trade_history_status_id[1],
				(*p2++).c_str(), cTH_ST_ID_len);
		strncpy(pOut->trade_info[i].trade_history_status_id[3],
				(*p2).c_str(), cTH_ST_ID_len);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_trade_price].c_str(), vAux);
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
	enum tuf2 {
			i_bid_price=0, i_cash_transaction_amount,
			i_cash_transaction_dts, i_cash_transaction_name, i_exec_name,
			i_is_cash, i_num_found, i_num_updated, i_settlement_amount,
			i_settlement_cash_due_date, i_settlement_cash_type, i_status,
			i_trade_history_dts, i_trade_history_status_id, i_trade_list,
			i_trade_price
	};

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

	// For PostgreSQL, see comment in the Concurrency Control chapter, under
	// the Transaction Isolation section for dealing with serialization
	// failures.  These serialization failures can occur with REPEATABLE READS
	// or SERIALIZABLE.
	int iNumRetry = 1;
	result R;
	while (true) {
		try {
			R = m_Txn->exec(osSQL.str());
			if (R.empty()) {
				pOut->status = CBaseTxnErr::ROLLBACK;
				ostringstream msg;
				msg << time(NULL) << " " << pthread_self() << endl <<
						"NO RESULTS" << endl <<
						osSQL.str() << endl;
				bh->logErrorMessage(msg.str(), false);
				rollback();
				throw;
			}
			break;
		} catch (const pqxx::sql_error &e) {
			if (PGSQL_SERIALIZE_ERROR.compare(e.what()) == 0) {
					ostringstream msg;
					msg << time(NULL) << " " << pthread_self() <<
							"TRADE_UPDATE" << endl;
					msg << "attempt: " << iNumRetry << endl;
					msg << "what: " << e.what();
					msg << "query: " << e.query() << endl;

					if (iNumRetry <= iMaxRetries) {
						// Wait 1 second to give the other transaction time to
						// finish.
						bh->logErrorMessage(msg.str(), false);
						iNumRetry++;
						sleep(1);
					} else {
						// Couldn't resubmit successfully.
						msg << "giving up" << endl;
						bh->logErrorMessage(msg.str(), false);
						pOut->status = CBaseTxnErr::ROLLBACK;
						rollback();
						throw;
					}
			} else {
				pOut->status = CBaseTxnErr::ROLLBACK;
				rollback();
				throw;
			}
		}
	}

	result::const_iterator c = R.begin();

	pOut->num_found = c[i_num_found].as(int());

	vector<string> vAux;
	vector<string>::iterator p;

	TokenizeSmart(c[i_bid_price].c_str(), vAux);
	int i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].bid_price = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_cash_transaction_amount].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].cash_transaction_amount = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_cash_transaction_dts].c_str(), vAux);
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

	TokenizeSmart(c[i_cash_transaction_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].cash_transaction_name, (*p).c_str(),
				cCT_NAME_len);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_exec_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].exec_name, (*p).c_str(), cEXEC_NAME_len);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_is_cash].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].is_cash = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->num_updated = c[i_num_updated].as(int());

	TokenizeSmart(c[i_settlement_amount].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].settlement_amount = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_settlement_cash_due_date].c_str(), vAux);
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

	TokenizeSmart(c[i_settlement_cash_type].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].settlement_cash_type, (*p).c_str(),
				cSE_CASH_TYPE_len);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->status = c[i_status].as(int());

	TokenizeArray(c[i_trade_history_dts].c_str(), vAux);
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

	TokenizeArray(c[i_trade_history_status_id].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		vector<string> v2;
		vector<string>::iterator p2;
		TokenizeSmart((*p).c_str(), v2);
		p2 = v2.begin();
		strncpy(pOut->trade_info[i].trade_history_status_id[0],
				(*p2++).c_str(), cTH_ST_ID_len);
		strncpy(pOut->trade_info[i].trade_history_status_id[1],
				(*p2++).c_str(), cTH_ST_ID_len);
		strncpy(pOut->trade_info[i].trade_history_status_id[3],
				(*p2).c_str(), cTH_ST_ID_len);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_trade_list].c_str(), vAux);
	this->bh = bh;
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].trade_id = atol((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_trade_price].c_str(), vAux);
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
	enum tuf3 {
			i_acct_id=0, i_cash_transaction_amount,
			i_cash_transaction_dts, i_cash_transaction_name, i_exec_name,
			i_is_cash, i_num_found, i_num_updated, i_price, i_quantity,
			i_s_name, i_settlement_amount, i_settlement_cash_due_date,
			i_settlement_cash_type, i_status, i_trade_dts,
			i_trade_history_dts, i_trade_history_status_id, i_trade_list,
			i_type_name, i_trade_type
	};

	ostringstream osSQL;
	osSQL << "SELECT * from TradeUpdateFrame3('" <<
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

	// For PostgreSQL, see comment in the Concurrency Control chapter, under
	// the Transaction Isolation section for dealing with serialization
	// failures.  These serialization failures can occur with REPEATABLE READS
	// or SERIALIZABLE.
	int iNumRetry = 1;
	result R;
	while (true) {
		try {
			R = m_Txn->exec(osSQL.str());
			if (R.empty()) {
				pOut->status = CBaseTxnErr::ROLLBACK;
				ostringstream msg;
				msg << time(NULL) << " " << pthread_self() << endl <<
						"NO RESULTS" << endl <<
						osSQL.str() << endl;
				bh->logErrorMessage(msg.str(), false);
				rollback();
				throw;
			}
			break;
		} catch (const pqxx::sql_error &e) {
			if (PGSQL_SERIALIZE_ERROR.compare(e.what()) == 0) {
					ostringstream msg;
					msg << time(NULL) << " " << pthread_self() <<
							"TRADE_UPDATE" << endl;
					msg << "attempt: " << iNumRetry << endl;
					msg << "what: " << e.what();
					msg << "query: " << e.query() << endl;

					if (iNumRetry <= iMaxRetries) {
						// Wait 1 second to give the other transaction time to
						// finish.
						bh->logErrorMessage(msg.str(), false);
						iNumRetry++;
						sleep(1);
					} else {
						// Couldn't resubmit successfully.
						msg << "giving up" << endl;
						bh->logErrorMessage(msg.str(), false);
						pOut->status = CBaseTxnErr::ROLLBACK;
						rollback();
						throw;
					}
			} else {
				pOut->status = CBaseTxnErr::ROLLBACK;
				rollback();
				throw;
			}
		}
	}

	result::const_iterator c = R.begin();

	pOut->num_found = c[i_num_found].as(int());

	vector<string> vAux;
	vector<string>::iterator p;

	TokenizeSmart(c[i_acct_id].c_str(), vAux);
	int i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].acct_id = atol((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_cash_transaction_amount].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].cash_transaction_amount = atof((*p).c_str());
		++i;
	}
	// FIXME: According to spec, this may not match the returned number found?
	vAux.clear();

	TokenizeSmart(c[i_cash_transaction_dts].c_str(), vAux);
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

	TokenizeSmart(c[i_cash_transaction_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].cash_transaction_name, (*p).c_str(),
				cCT_NAME_len);
		++i;
	}
	vAux.clear();

	TokenizeSmart(c[i_exec_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].exec_name, (*p).c_str(), cEXEC_NAME_len);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_is_cash].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].is_cash = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->num_updated = c[i_num_updated].as(int());

	TokenizeSmart(c[i_price].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].price = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_quantity].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].quantity = atoi((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_s_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].s_name, (*p).c_str(), cS_NAME_len);
		++i;
	}
	vAux.clear();

	TokenizeSmart(c[i_settlement_amount].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].settlement_amount = atof((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_settlement_cash_due_date].c_str(), vAux);
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

	TokenizeSmart(c[i_settlement_cash_type].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].settlement_cash_type, (*p).c_str(),
				cSE_CASH_TYPE_len);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->status = c[i_status].as(int());

	TokenizeSmart(c[i_trade_dts].c_str(), vAux);
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

	TokenizeArray(c[i_trade_history_dts].c_str(), vAux);
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

	TokenizeArray(c[i_trade_history_status_id].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		vector<string> v2;
		vector<string>::iterator p2;
		TokenizeSmart((*p).c_str(), v2);
		int j = 0;
		for (p2 = v2.begin(); p2 != v2.end(); ++p2) {
			strncpy(pOut->trade_info[i].trade_history_status_id[j],
					(*p2).c_str(), cTH_ST_ID_len);
			++j;
		}
		++i;
	}
	vAux.clear();

	TokenizeSmart(c[i_trade_list].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].trade_id = atol((*p).c_str());
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_type_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].type_name, (*p).c_str(), cTT_NAME_len);
		++i;
	}
	check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_trade_type].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].trade_type, (*p).c_str(), cTT_ID_len);
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
	m_Txn->exec("ROLLBACK;");
}

void CDBConnection::setBrokerageHouse(CBrokerageHouse *bh)
{
	this->bh = bh;
}

void CDBConnection::setReadCommitted()
{
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
}

void CDBConnection::setReadUncommitted()
{
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;");
}

void CDBConnection::setRepeatableRead()
{
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
}

void CDBConnection::setSerializable()
{
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
}
