/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * 11 July 2006
 */

#include <transactions.h>

// Call Trade Lookup Frame 1
void CTradeUpdateDB::DoTradeUpdateFrame1(const TTradeUpdateFrame1Input *pIn,
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
	for ( i = 1; i < pIn->max_trades; i++)
	{
		osTrades << "," << pIn->trade_id[i];
	}

	ostringstream osCall;
	osCall << "SELECT * FROM TradeUpdateFrame1(" <<
			pIn->max_trades << "," <<
			pIn->max_updates << ",'{" <<
			osTrades.str() << "}')";
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TUF1" << endl;
	cout << "*** " << osCall.str() << endl;
	cout << "- Trade Update Frame 1 (input)" << endl <<
			"-- max_trades: " << pIn->max_trades << endl <<
			"-- max_updates: " << pIn->max_updates << endl <<
			"-- Trades: {" << osTrades.str() << "}" << endl;
	m_coutLock.unlock();
#endif // DEBUG

	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	if (R.empty()) 
	{
		cout << "warning: empty result set at DoTradeUpdateFrame1" << endl <<
				osCall.str() << endl;
		pOut->status = CBaseTxnErr::ROLLBACK;
		return;
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
	//check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_cash_transaction_dts].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%d-%d-%d %d:%d:%d",
				&pOut->trade_info[i].cash_transaction_dts.year,
				&pOut->trade_info[i].cash_transaction_dts.month,
				&pOut->trade_info[i].cash_transaction_dts.day,
				&pOut->trade_info[i].cash_transaction_dts.hour,
				&pOut->trade_info[i].cash_transaction_dts.minute,
				&pOut->trade_info[i].cash_transaction_dts.second);
		++i;
	}
	// FIXME: According to spec, this may not match the returned number found?
	//check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_cash_transaction_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_info[i].cash_transaction_name, (*p).c_str(),
				cCT_NAME_len);
		++i;
	}
	// FIXME: According to spec, this may not match the returned number found?
	//check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
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
		sscanf((*p).c_str(), "%d-%d-%d %d:%d:%d",
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
		sscanf((*p2++).c_str(), "%d-%d-%d %d:%d:%d",
				&pOut->trade_info[i].trade_history_dts[0].year,
				&pOut->trade_info[i].trade_history_dts[0].month,
				&pOut->trade_info[i].trade_history_dts[0].day,
				&pOut->trade_info[i].trade_history_dts[0].hour,
				&pOut->trade_info[i].trade_history_dts[0].minute,
				&pOut->trade_info[i].trade_history_dts[0].second);
		sscanf((*p2++).c_str(), "%d-%d-%d %d:%d:%d",
				&pOut->trade_info[i].trade_history_dts[1].year,
				&pOut->trade_info[i].trade_history_dts[1].month,
				&pOut->trade_info[i].trade_history_dts[1].day,
				&pOut->trade_info[i].trade_history_dts[1].hour,
				&pOut->trade_info[i].trade_history_dts[1].minute,
				&pOut->trade_info[i].trade_history_dts[1].second);
		sscanf((*p2).c_str(), "%d-%d-%d %d:%d:%d",
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

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Update Frame 1 (output)" << endl <<
			"-- num_found: " << pOut->num_found << endl <<
			"-- num_updated: " << pOut->num_updated << endl;
	for (i = 0; i < pOut->num_found; i++) {
		cout << "-- bid_price[" << i << "]: " <<
						pOut->trade_info[i].bid_price << endl <<
				"-- exec_name[" << i << "]: " <<
						pOut->trade_info[i].exec_name << endl <<
				"-- is_cash[" << i << "]: " << pOut->trade_info[i].is_cash <<
						endl <<
				"-- is_market[" << i << "]: " <<
						pOut->trade_info[i].is_market << endl <<
				"-- trade_price[" << i << "]: " <<
						pOut->trade_info[i].trade_price << endl <<
				"-- settlement_amount[" << i << "]: "<<
						pOut->trade_info[i].settlement_amount << endl <<
				"-- settlement_cash_due_date[" << i << "]: " <<
						pOut->trade_info[i].settlement_cash_due_date.year <<
						"-" <<
						pOut->trade_info[i].settlement_cash_due_date.month <<
						"-" <<
						pOut->trade_info[i].settlement_cash_due_date.day <<
						" " <<
						pOut->trade_info[i].settlement_cash_due_date.hour <<
						":"<<
						pOut->trade_info[i].settlement_cash_due_date.minute <<
						":" <<
						pOut->trade_info[i].settlement_cash_due_date.second <<
						endl <<
				"-- settlement_cash_type[" << i << "]: " <<
						pOut->trade_info[i].settlement_cash_type << endl <<	
				"-- cash_transaction_amount[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_amount << endl <<
				"-- cash_transaction_dts[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_dts.year << "-" <<
						pOut->trade_info[i].cash_transaction_dts.month << "-" <<
						pOut->trade_info[i].cash_transaction_dts.day << " " <<
						pOut->trade_info[i].cash_transaction_dts.hour << ":" <<
						pOut->trade_info[i].cash_transaction_dts.minute <<
						":" <<
						pOut->trade_info[i].cash_transaction_dts.second <<
						endl <<
				"-- cash_transaction_name[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_name << endl <<
				"-- trade_history_dts[" << i << "][0]: " <<
						pOut->trade_info[i].trade_history_dts[0].year << "-" <<
						pOut->trade_info[i].trade_history_dts[0].month << "-" <<
						pOut->trade_info[i].trade_history_dts[0].day << " " <<
						pOut->trade_info[i].trade_history_dts[0].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[0].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[0].second <<
						endl <<
				"-- trade_history_status_id[" << i << "][0]: " <<
						pOut->trade_info[i].trade_history_status_id[0] <<
						endl <<
				"-- trade_history_dts[" << i << "][1]: " <<
						pOut->trade_info[i].trade_history_dts[1].year << "-" <<
						pOut->trade_info[i].trade_history_dts[1].month << "-" <<
						pOut->trade_info[i].trade_history_dts[1].day << " " <<
						pOut->trade_info[i].trade_history_dts[1].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[1].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[1].second <<
						endl <<
				"-- trade_history_status_id[" << i << "][1]: " <<
						pOut->trade_info[i].trade_history_status_id[1] <<
						endl <<
				"-- trade_history_dts[" << i << "][2]: " <<
						pOut->trade_info[i].trade_history_dts[2].year << "-" <<
						pOut->trade_info[i].trade_history_dts[2].month << "-" <<
						pOut->trade_info[i].trade_history_dts[2].day << " " <<
						pOut->trade_info[i].trade_history_dts[2].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[2].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[2].second <<
						endl <<
				"-- trade_history_status_id[" << i << "][2]: " <<
						pOut->trade_info[i].trade_history_status_id[2] << endl;
	}
	cout << ">>> TUF1" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}

// Call Trade Lookup Frame 2
void CTradeUpdateDB::DoTradeUpdateFrame2(const TTradeUpdateFrame2Input *pIn,
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

	ostringstream osCall;
	osCall << "SELECT * FROM TradeUpdateFrame2(" <<
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
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TUF2" << endl;
	cout << "*** " << osCall.str() << endl;
	cout << "- Trade Update Frame 2 (input)" << endl <<
			"-- acct_id: " << pIn->acct_id << endl <<
			"-- max_trades: " << pIn->max_trades << endl <<
			"-- max_updates: " << pIn->max_updates << endl <<
			"-- trade_dts: " << pIn->end_trade_dts.year << "-" <<
					pIn->end_trade_dts.month << "-" <<
					pIn->end_trade_dts.day << " " <<
					pIn->end_trade_dts.hour << ":" <<
					pIn->end_trade_dts.minute << ":" <<
					pIn->end_trade_dts.second << endl;
	m_coutLock.unlock();
#endif // DEBUG

	BeginTxn();
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	if (R.empty()) 
	{
		cout << "warning: empty result set at DoTradeUpdateFrame2" << endl <<
				osCall.str() << endl;
		pOut->num_found = 0;
		pOut->status = CBaseTxnErr::ROLLBACK;
		return;
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
		sscanf((*p).c_str(), "%d-%d-%d %d:%d:%d",
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
		sscanf((*p).c_str(), "%d-%d-%d %d:%d:%d",
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
		sscanf((*p2++).c_str(), "%d-%d-%d %d:%d:%d",
				&pOut->trade_info[i].trade_history_dts[0].year,
				&pOut->trade_info[i].trade_history_dts[0].month,
				&pOut->trade_info[i].trade_history_dts[0].day,
				&pOut->trade_info[i].trade_history_dts[0].hour,
				&pOut->trade_info[i].trade_history_dts[0].minute,
				&pOut->trade_info[i].trade_history_dts[0].second);
		sscanf((*p2++).c_str(), "%d-%d-%d %d:%d:%d",
				&pOut->trade_info[i].trade_history_dts[1].year,
				&pOut->trade_info[i].trade_history_dts[1].month,
				&pOut->trade_info[i].trade_history_dts[1].day,
				&pOut->trade_info[i].trade_history_dts[1].hour,
				&pOut->trade_info[i].trade_history_dts[1].minute,
				&pOut->trade_info[i].trade_history_dts[1].second);
		sscanf((*p2).c_str(), "%d-%d-%d %d:%d:%d",
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

#ifdef DEBUG
	m_coutLock.lock();
	cout << "-- Trade Update Frame 2 (output)" << endl <<
			"-- num_found: " << pOut->num_found << endl <<
			"-- num_updated: " << pOut->num_updated << endl;
	for (i = 0; i < pOut->num_found; i++) {
		cout << "-- bid_price[" << i << "]: " <<
						pOut->trade_info[i].bid_price << endl <<
				"-- exec_name[" << i << "]: " <<
						pOut->trade_info[i].exec_name << endl <<
				"-- is_cash[" << i << "]: " << pOut->trade_info[i].is_cash <<
						endl <<
				"-- trade_price[" << i << "]: " <<
						pOut->trade_info[i].trade_price << endl <<
				"-- trade_id[" << i << "]: " <<
						pOut->trade_info[i].trade_id << endl <<
				"-- settlement_amount[" << i << "]: " <<
						pOut->trade_info[i].settlement_amount << endl <<
				"-- settlement_cash_due_date[" << i << "]: " <<
						pOut->trade_info[i].settlement_cash_due_date.year <<
						"-" <<
						pOut->trade_info[i].settlement_cash_due_date.month <<
						"-" <<
						pOut->trade_info[i].settlement_cash_due_date.day <<
						" " <<
						pOut->trade_info[i].settlement_cash_due_date.hour <<
						":" <<
						pOut->trade_info[i].settlement_cash_due_date.minute <<
						":" <<
						pOut->trade_info[i].settlement_cash_due_date.second <<
						endl <<
				"-- settlement_cash_type[" << i << "]: " <<
						pOut->trade_info[i].settlement_cash_type << endl <<
				"-- cash_transaction_amount[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_amount << endl <<
				"-- cash_transaction_dts[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_dts.year << "-" <<
						pOut->trade_info[i].cash_transaction_dts.month << "-" <<
						pOut->trade_info[i].cash_transaction_dts.day << " " <<
						pOut->trade_info[i].cash_transaction_dts.hour << ":" <<
						pOut->trade_info[i].cash_transaction_dts.minute <<":" <<
						pOut->trade_info[i].cash_transaction_dts.second <<
						endl <<
				"-- cash_transaction_name[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_name << endl <<
				"-- trade_history_dts[" << i << "][0]: " <<
						pOut->trade_info[i].trade_history_dts[0].year << "-" <<
						pOut->trade_info[i].trade_history_dts[0].month << "-" <<
						pOut->trade_info[i].trade_history_dts[0].day << " " <<
						pOut->trade_info[i].trade_history_dts[0].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[0].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[0].second <<
						endl <<
				"-- trade_history_status_id[" << i << "][0]: " <<
						pOut->trade_info[i].trade_history_status_id[0] <<
						endl <<
				"-- trade_history_dts[" << i << "][1]: " <<
						pOut->trade_info[i].trade_history_dts[1].year << "-" <<
						pOut->trade_info[i].trade_history_dts[1].month << "-" <<
						pOut->trade_info[i].trade_history_dts[1].day << " " <<
						pOut->trade_info[i].trade_history_dts[1].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[1].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[1].second <<
						endl <<
				"-- trade_history_status_id[" << i << "][1]: " <<
						pOut->trade_info[i].trade_history_status_id[1] <<
						endl <<
				"-- trade_history_dts[" << i << "][2]: " <<
						pOut->trade_info[i].trade_history_dts[2].year << "-" <<
						pOut->trade_info[i].trade_history_dts[2].month << "-" <<
						pOut->trade_info[i].trade_history_dts[2].day << " " <<
						pOut->trade_info[i].trade_history_dts[2].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[2].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[2].second <<
						endl <<
				"-- trade_history_status_id[" << i << "][2]: " <<
						pOut->trade_info[i].trade_history_status_id[2] << endl;
	}
	cout << ">>> TUF2" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}

// Call Trade Lookup Frame 3
void CTradeUpdateDB::DoTradeUpdateFrame3(const TTradeUpdateFrame3Input *pIn,
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

	ostringstream osCall;
	osCall << "SELECT * from TradeUpdateFrame3('" <<
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
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TUF3" << endl;
	cout << "*** " << osCall.str() << endl;
	cout << "- Trade Update Frame 3 (input)" << endl <<
			"-- max_acct_id: " << pIn->max_acct_id << endl <<
			"-- max_trades: " << pIn->max_trades << endl <<
			"-- max_updates: " << pIn->max_updates << endl <<
			"-- trade_dts: " << pIn->start_trade_dts.year << "-" <<
					pIn->start_trade_dts.month << "-" <<
					pIn->start_trade_dts.day << " " <<
					pIn->start_trade_dts.hour << ":" <<
					pIn->start_trade_dts.minute << ":" <<
					pIn->start_trade_dts.second << endl <<
			"-- symbol: " << pIn->symbol << endl;
	m_coutLock.unlock();
#endif //DEBUG

	BeginTxn();
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	if (R.empty()) 
	{
		cout << "warning: empty result set at DoTradeUpdateFrame3" << endl <<
				osCall.str() << endl;
		pOut->num_found = 0;
		pOut->status = CBaseTxnErr::ROLLBACK;
		return;
	}
	result::const_iterator c = R.begin();
	result::const_iterator e = R.end();
	--e;

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
	//check_count(pOut->num_found, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_cash_transaction_dts].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%d-%d-%d %d:%d:%d",
				&pOut->trade_info[i].cash_transaction_dts.year,
				&pOut->trade_info[i].cash_transaction_dts.month,
				&pOut->trade_info[i].cash_transaction_dts.day,
				&pOut->trade_info[i].cash_transaction_dts.hour,
				&pOut->trade_info[i].cash_transaction_dts.minute,
				&pOut->trade_info[i].cash_transaction_dts.second);
		++i;
	}
	// FIXME: According to spec, this may not match the returned number found?
	//check_count(pout->num_found, vaux.size(), __file__, __line__);
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
		sscanf((*p).c_str(), "%d-%d-%d %d:%d:%d",
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
		sscanf((*p).c_str(), "%d-%d-%d %d:%d:%d",
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
			sscanf((*p2).c_str(), "%d-%d-%d %d:%d:%d",
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

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Update Frame 3 (output)" << endl <<
			"-- num_found: " << pOut->num_found << endl <<
			"-- num_updated: " << pOut->num_updated << endl;
	for (i = 0; i < pOut->num_found; i++) {
		cout << "-- acct_id[" << i << "]: " << pOut->trade_info[i].acct_id <<
						endl <<
				"-- cash_transaction_amount[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_amount << endl <<
				"-- cash_transaction_dts[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_dts.year << "-" <<
						pOut->trade_info[i].cash_transaction_dts.month << "-" <<
						pOut->trade_info[i].cash_transaction_dts.day << " " <<
						pOut->trade_info[i].cash_transaction_dts.hour << ":" <<
						pOut->trade_info[i].cash_transaction_dts.minute <<
						":" <<
						pOut->trade_info[i].cash_transaction_dts.second <<
						endl <<
				"-- cash_transaction_name[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_name << endl <<
				"-- exec_name[" << i << "]: " <<
						pOut->trade_info[i].exec_name << endl <<
				"-- is_cash[" << i << "]: " << pOut->trade_info[i].is_cash <<
						endl <<
				"-- price[" << i << "]: " << pOut->trade_info[i].price <<
						endl <<
				"-- quantity[" << i << "]: " << pOut->trade_info[i].quantity <<
						endl <<
				"-- settlement_amount[" << i << "]: " <<
						pOut->trade_info[i].settlement_amount << endl <<
				"-- settlement_cash_due_date[" << i << "]: " <<
						pOut->trade_info[i].settlement_cash_due_date.year <<
						"-" <<
						pOut->trade_info[i].settlement_cash_due_date.month <<
						"-" <<
						pOut->trade_info[i].settlement_cash_due_date.day <<
						" " <<
						pOut->trade_info[i].settlement_cash_due_date.hour <<
						":" <<
						pOut->trade_info[i].settlement_cash_due_date.minute <<
						":" <<
						pOut->trade_info[i].settlement_cash_due_date.second <<
						endl <<
				"-- settlement_cash_type[" << i << "]: " <<
						pOut->trade_info[i].settlement_cash_type << endl <<
				"-- trade_dts[" << i << "]: " <<
						pOut->trade_info[i].trade_dts.year << "-" <<
						pOut->trade_info[i].trade_dts.month << "-" <<
						pOut->trade_info[i].trade_dts.day << " " <<
						pOut->trade_info[i].trade_dts.hour << ":" <<
						pOut->trade_info[i].trade_dts.minute << ":" <<
						pOut->trade_info[i].trade_dts.second << endl <<
				"-- cash_transaction_dts[" << i << "]: " <<
						pOut->trade_info[i].trade_dts.year << "-" <<
						pOut->trade_info[i].trade_dts.month << "-" <<
						pOut->trade_info[i].trade_dts.day << " " <<
						pOut->trade_info[i].trade_dts.hour << ":" <<
						pOut->trade_info[i].trade_dts.minute << ":" <<
						pOut->trade_info[i].trade_dts.second << endl <<
				"-- trade_history_dts[" << i << "][0]: " <<
						pOut->trade_info[i].trade_history_dts[0].year << "-" <<
						pOut->trade_info[i].trade_history_dts[0].month << "-" <<
						pOut->trade_info[i].trade_history_dts[0].day << " " <<
						pOut->trade_info[i].trade_history_dts[0].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[0].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[0].second <<
						endl <<
				"-- trade_history_status_id[" << i << "][0]: " <<
						pOut->trade_info[i].trade_history_status_id[0] <<
						endl <<
				"-- trade_history_dts[" << i << "][1]: " <<
						pOut->trade_info[i].trade_history_dts[1].year << "-" <<
						pOut->trade_info[i].trade_history_dts[1].month << "-" <<
						pOut->trade_info[i].trade_history_dts[1].day << " " <<
						pOut->trade_info[i].trade_history_dts[1].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[1].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[1].second <<
						endl <<
				"-- trade_history_status_id[" << i << "][1]: " <<
						pOut->trade_info[i].trade_history_status_id[1] <<
						endl <<
				"-- trade_history_dts[" << i << "][2]: " <<
						pOut->trade_info[i].trade_history_dts[2].year << "-" <<
						pOut->trade_info[i].trade_history_dts[2].month << "-" <<
						pOut->trade_info[i].trade_history_dts[2].day << " " <<
						pOut->trade_info[i].trade_history_dts[2].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[2].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[2].second <<
						endl <<
				"-- trade_history_status_id[" << i << "][2]: " <<
						pOut->trade_info[i].trade_history_status_id[2] <<
						endl <<
				"-- trade_id[" << i << "]: " << pOut->trade_info[i].trade_id <<
						endl <<
				"-- trade_type[" << i << "]: " <<
						pOut->trade_info[i].trade_type << endl;
	}
	cout << ">>> TUF3" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}
