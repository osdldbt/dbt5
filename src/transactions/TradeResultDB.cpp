/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * 07 July 2006
 */

#include <transactions.h>

using namespace TPCE;

// Call Trade Result Frame 1
void CTradeResultDB::DoTradeResultFrame1(
		const TTradeResultFrame1Input *pIn,
		TTradeResultFrame1Output *pOut)
{
	ostringstream osCall;
	osCall << "SELECT * FROM TradeResultFrame1(" << pIn->trade_id << ")";
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TRF1" << endl;
	cout << "*** " << osCall.str() << endl;
	cout << "- Trade Result Frame 1 (input)" << endl <<
			"-- trade_id: " << pIn->trade_id << endl;
	m_coutLock.unlock();
#endif // DEBUG

	// start transaction but not commit in this frame
	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		cerr << "warning: empty result set at DoTradeResultFrame1" << endl <<
				osCall.str() << endl;
		pOut->status = CBaseTxnErr::ROLLBACK;
		return;
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

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Result Frame 1 (output)" << endl <<
			"-- acct_id: " << pOut->acct_id << endl <<
			"-- charge: " << pOut->charge << endl <<
			"-- hs_qty: " << pOut->hs_qty << endl <<
			"-- is_lifo: " << pOut->is_lifo << endl <<
			"-- symbol: " << pOut->symbol << endl <<
			"-- trade_is_cash: " << pOut->trade_is_cash << endl <<
			"-- trade_qty: " << pOut->trade_qty << endl <<
			"-- type_id: " << pOut->type_id << endl <<
			"-- type_is_market: " << pOut->type_is_market << endl <<
			"-- type_is_sell: " << pOut->type_is_sell << endl <<
			"-- type_name: " << pOut->type_name << endl;
	cout << ">>> TRF1" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}

// Call Trade Result Frame 2
void CTradeResultDB::DoTradeResultFrame2(
		const TTradeResultFrame2Input *pIn,
		TTradeResultFrame2Output *pOut)
{
	ostringstream osCall;
	osCall << "SELECT * FROM TradeResultFrame2(" <<
			pIn->acct_id << "," <<
			pIn->hs_qty << "," <<
			pIn->is_lifo << "::SMALLINT,'" <<
			pIn->symbol << "'," <<
			pIn->trade_id << "," <<
			pIn->trade_price << "," <<
			pIn->trade_qty << "," <<
			pIn->type_is_sell << "::SMALLINT)";
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TRF2" << endl;
	cout << "*** " << osCall.str() << endl;
	cout << "- Trade Result Frame 2 (input)" << endl <<
			"-- acct_id: " << pIn->acct_id << endl <<
			"-- hs_qty: " << pIn->hs_qty << endl <<
			"-- is_lifo: " << pIn->is_lifo << endl <<
			"-- symbol: " << pIn->symbol << endl <<
			"-- trade_id: " << pIn->trade_id << endl <<
			"-- trade_price: " << pIn->trade_price << endl <<
			"-- trade_qty: " << pIn->trade_qty << endl <<
			"-- type_is_sell: " << pIn->type_is_sell << endl;
	m_coutLock.unlock();
#endif // DEBUG

	// we are inside a transaction
	try {
		result R(m_Txn->exec(osCall.str()));

		if (R.empty()) {
			cerr << "warning: empty result set at DoTradeResultFrame2" <<
					endl << osCall.str() << endl;
			pOut->status = CBaseTxnErr::ROLLBACK;
			return;
		}
		result::const_iterator c = R.begin();

		pOut->broker_id = c[0].as(long());
		pOut->buy_value = c[1].as(double());
		pOut->cust_id = c[2].as(long());
		pOut->sell_value = c[3].as(double());
		pOut->status = c[4].as(int());
		pOut->tax_status = c[5].as(int());
		sscanf(c[6].c_str(), "%d-%d-%d %d:%d:%d.%*d",
				&pOut->trade_dts.year,
				&pOut->trade_dts.month,
				&pOut->trade_dts.day,
				&pOut->trade_dts.hour,
				&pOut->trade_dts.minute,
				&pOut->trade_dts.second);
	} catch (const pqxx::sql_error &e) {
        cout << "SQL Error: " << e.what() << endl <<
				"Query: '" << e.query() << "'" << endl;
		pOut->status = CBaseTxnErr::ROLLBACK;
		return;
	}

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Result Frame 2 (output)" << endl <<
			"-- broker_id: " << pOut->broker_id << endl <<
			"-- buy_value: " << pOut->buy_value << endl <<
			"-- cust_id: " << pOut->cust_id << endl <<
			"-- sell_value: " << pOut->sell_value << endl <<
			"-- status: " << pOut->status << endl <<
			"-- tax_status: " << pOut->tax_status << endl <<
			"-- trade_dts: " << pOut->trade_dts.year << "-" <<
					pOut->trade_dts.month << "-" <<
					pOut->trade_dts.day << " " <<
					pOut->trade_dts.hour << ":" <<
					pOut->trade_dts.minute << ":" <<
					pOut->trade_dts.second << endl;
	cout << ">>> TRF2" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}

// Call Trade Result Frame 3
void CTradeResultDB::DoTradeResultFrame3(
		const TTradeResultFrame3Input *pIn,
		TTradeResultFrame3Output *pOut)
{
	ostringstream osCall;
	osCall << "SELECT * FROM TradeResultFrame3(" <<
			pIn->buy_value << "," <<
			pIn->cust_id << "," <<
			pIn->sell_value << "," <<
			pIn->trade_id << ")";
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TRF3" << endl;
	cout << "*** " << osCall.str() << endl;
	cout << "--Trade Result Frame 3 (input)" << endl <<
			"-- buy_value: " << pIn->buy_value << endl <<
			"-- cust_id: " << pIn->cust_id << endl <<
			"-- sell_value: " << pIn->sell_value << endl <<
			"-- trade_id: " << pIn->trade_id << endl;
	m_coutLock.unlock();
#endif //DEBUG

	// we are inside a transaction
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		cerr << "warning: empty result set at DoTradeResultFrame3" << endl <<
				osCall.str() << endl;
		pOut->status = CBaseTxnErr::ROLLBACK;
		return;
	}
	result::const_iterator c = R.begin();

	pOut->status = c[0].as(int());
	pOut->tax_amount = c[1].as(double());

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Result Frame 3 (output)" << endl <<
			"-- status:" << pOut->status << endl <<
			"-- tax_amount:" << pOut->tax_amount << endl;
	cout << ">>> TRF3" << endl;
	m_coutLock.unlock();
#endif //DEBUG
}

// Call Trade Result Frame 4
void CTradeResultDB::DoTradeResultFrame4(
		const TTradeResultFrame4Input *pIn,
		TTradeResultFrame4Output *pOut)
{
	ostringstream osCall;
	osCall << "SELECT * FROM TradeResultFrame4(" <<
			pIn->cust_id << ",'" <<
			pIn->symbol << "'," <<
			pIn->trade_qty << ",'" <<
			pIn->type_id << "')";
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TRF4" << endl;
	cout << "*** " << osCall.str() << endl;
	cout << "- Trade Result Frame 4 (input)" << endl <<
			"-- cust_id: " << pIn->cust_id << endl <<
			"-- symbol: " << pIn->symbol << endl <<
			"-- trade_qty: " << pIn->trade_qty << endl <<
			"-- type_id: " << pIn->type_id << endl;
	m_coutLock.unlock();
#endif //DEBUG

	// we are inside a transaction
	try {
		result R(m_Txn->exec(osCall.str()));

		if (R.empty()) {
			cerr << "warning: empty result set at DoTradeResultFrame4" <<
					endl << osCall.str() << endl;
			pOut->status = CBaseTxnErr::ROLLBACK;
			return;
		}
		result::const_iterator c = R.begin();

		pOut->comm_rate = c[0].as(double());
		strncpy(pOut->s_name, c[1].c_str(), cS_NAME_len);
		pOut->status = c[2].as(int());
	} catch (const pqxx::sql_error &e) {
        cout << "SQL Error: " << e.what() << endl <<
				"Query: '" << e.query() << "'" << endl;
		pOut->status = CBaseTxnErr::ROLLBACK;
		return;
	}

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Result Frame 4 (output)" << endl <<
			"-- comm_rate:" << pOut->comm_rate << endl <<
			"-- s_name:" << pOut->s_name << endl <<
			"-- status:" << pOut->status << endl;
	cout << ">>> TRF4" << endl;
	m_coutLock.unlock();
#endif //DEBUG
}

// Call Trade Result Frame 5
void CTradeResultDB::DoTradeResultFrame5(
		const TTradeResultFrame5Input *pIn,
		TTradeResultFrame5Output *pOut)
{
	ostringstream osCall;
	osCall << "SELECT * FROM TradeResultFrame5(" <<
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
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TRF5" << endl;
	cout << "*** " << osCall.str() <<endl;
	cout << "- Trade Result Frame 5 (input)" << endl <<
			"-- broker_id: " << pIn->broker_id << endl <<
			"-- comm_amount: " << pIn->comm_amount << endl <<
			"-- st_completed_id: " << pIn->st_completed_id << endl <<
			"-- trade_dts: " << pIn->trade_dts.year << "-" <<
					pIn->trade_dts.month << "-" <<
					pIn->trade_dts.day << " " <<
					pIn->trade_dts.hour << ":" <<
					pIn->trade_dts.minute << ":" <<
					pIn->trade_dts.second << endl <<
			"-- trade_id: " << pIn->trade_id << endl <<
			"-- trade_price: " << pIn->trade_price << endl;
	m_coutLock.unlock();
#endif //DEBUG

	// we are inside a transaction
	try {
		result R( m_Txn->exec(osCall.str()));
		if (R.empty()) {
			//throw logic_error("TradeResultFrame5: empty result set");
			cerr << "warning: empty result set at DoTradeResultFrame5" << endl;
		}

		result::const_iterator c = R.begin();

		pOut->status = c[0].as(int());
	} catch (const pqxx::sql_error &e) {
        cout << "SQL Error: " << e.what() << endl <<
				"Query: '" << e.query() << "'" << endl;
		pOut->status = CBaseTxnErr::ROLLBACK;
		return;
	}

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Result Frame 5 (output)" << endl <<
			"-- stats:" << pOut->status << endl;
	cout << ">>> TRF5" << endl;
	m_coutLock.unlock();
#endif //DEBUG
}

// Call Trade Result Frame 6
void CTradeResultDB::DoTradeResultFrame6(
		const TTradeResultFrame6Input *pIn,
		TTradeResultFrame6Output *pOut)
{
	ostringstream osCall;
	osCall << "SELECT * FROM TradeResultFrame6(" <<
			pIn->acct_id << ",'" <<
			pIn->due_date.year << "-"<<
			pIn->due_date.month << "-" <<
			pIn->due_date.day << " " <<
			pIn->due_date.hour << ":" <<
			pIn->due_date.minute << ":" <<
			pIn->due_date.second << "','" <<
			m_Txn->esc(pIn->s_name) << "', " <<
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
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TRF6" << endl;
	cout << "*** " << osCall.str() << endl;
	cout << "- Trade Result Frame 6 (input)" << endl <<
			"-- acct_id: " << pIn->acct_id << endl <<
			"-- due_date: " << pIn->due_date.year << "-" <<
					pIn->due_date.month << "-" << pIn->due_date.day <<
					" " << pIn->due_date.hour << ":" <<
					pIn->due_date.minute << ":" <<
					pIn->due_date.second << endl <<
			"-- s_name: " << pIn->s_name << endl <<
			"-- se_amount: " << pIn->se_amount << endl <<
			"-- trade_dts: " << pIn->trade_dts.year << "-" <<
					pIn->trade_dts.month << "-" <<
					pIn->trade_dts.day << " " <<
					pIn->trade_dts.hour << ":" <<
					pIn->trade_dts.minute << ":" <<
					pIn->trade_dts.second << endl <<
			"-- trade_id: " << pIn->trade_id << endl <<
			"-- trade_is_cash: " << pIn->trade_is_cash << endl <<
			"-- trade_qty: " << pIn->trade_qty << endl <<
			"-- type_name: " << pIn->type_name << endl;
	m_coutLock.unlock();
#endif //DEBUG

	// we are inside a transaction
	try {
		result R(m_Txn->exec(osCall.str()));
		CommitTxn();
		if (R.empty()) {
			cerr << "warning: empty result set at DoTradeResultFrame6" <<
					endl << osCall.str() << endl;
			pOut->status = CBaseTxnErr::ROLLBACK;
			return;
		}
		result::const_iterator c = R.begin();

		pOut->acct_bal = c[0].as(double());
		pOut->status = c[1].as(int());
	} catch (const pqxx::sql_error &e) {
        cout << "SQL Error: " << e.what() << endl <<
				"Query: '" << e.query() << "'" << endl;
		pOut->status = CBaseTxnErr::ROLLBACK;
		return;
	}

#ifdef DEBUG
	m_coutLock.lock();
	cout << "Trade Result Frame 6 (output)" << endl <<
			"- acct_bal:" << pOut->acct_bal << endl <<
			"- status:" << pOut->status << endl;
	cout << ">>> TRF6" << endl;
	m_coutLock.unlock();
#endif //DEBUG
}
