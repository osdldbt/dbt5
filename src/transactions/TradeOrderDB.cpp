/*
 * TradeOrderDB.cpp
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *
 * 03 July 2006
 */

#include <transactions.h>

using namespace TPCE;

// Call Trade Order Frame 1
void CTradeOrderDB::DoTradeOrderFrame1(const TTradeOrderFrame1Input *pIn,
		TTradeOrderFrame1Output *pOut)
{
#ifdef DEBUG
	cout << "TOF1" << endl;
#endif

	ostringstream osCall;
	osCall << "SELECT * FROM TradeOrderFrame1(" << pIn->acct_id << ")";

	// start transaction but not commit in this frame
	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		cout << "warning: empty result set at DoTradeOrderFrame1" << endl <<
				osCall.str() << endl;
		pOut->status = CBaseTxnErr::ROLLBACK;
		return;
	}
	result::const_iterator c = R.begin();
	
	strcpy(pOut->acct_name, c[0].c_str());
	pOut->broker_id = c[1].as(long());
	strcpy(pOut->broker_name, c[2].c_str());
	strcpy(pOut->cust_f_name, c[3].c_str());
	pOut->cust_id = c[4].as(long());
	strcpy(pOut->cust_l_name, c[5].c_str());
	pOut->cust_tier = c[6].as(int());
	pOut->status = c[7].as(int());
	strcpy(pOut->tax_id, c[8].c_str());
	pOut->tax_status = c[9].as(int());

#ifdef DEBUG
	m_coutLock.lock();
	cout<<"Trade Order Frame 1 (input)"<<endl
	    <<"- acct_id: "<<pIn->acct_id<<endl;
	cout<<"Trade Order Frame 1 (output)"<<endl
	    <<"- acct_name: "<<pOut->acct_name<<endl
	    <<"- broker_name: "<<pOut->broker_name<<endl
	    <<"- cust_f_name: "<<pOut->cust_f_name<<endl
	    <<"- cust_id: "<<pOut->cust_id<<endl
	    <<"- cust_l_name: "<<pOut->cust_l_name<<endl
	    <<"- cust_tier: "<<pOut->cust_tier<<endl
	    <<"- tax_id: "<<pOut->tax_id<<endl
	    <<"- tax_status: "<<pOut->tax_status<<endl;
	m_coutLock.unlock();
#endif // DEBUG
}

// Call Trade Order Frame 2
void CTradeOrderDB::DoTradeOrderFrame2(const TTradeOrderFrame2Input *pIn,
		TTradeOrderFrame2Output *pOut)
{
#ifdef DEBUG
	cout << "TOF2" << endl;
#endif

	ostringstream osCall;
	osCall << "SELECT * FROM TradeOrderFrame2(" <<
			pIn->acct_id << ",'" <<
			m_Txn->esc(pIn->exec_f_name) << "','" <<
			m_Txn->esc(pIn->exec_l_name) << "','" <<
			pIn->exec_tax_id<<"')";

	// we are inside a transaction
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		cout << "warning: empty result set at DoTradeOrderFrame2" << endl <<
				osCall.str() << endl;
		pOut->status = CBaseTxnErr::ROLLBACK;
		RollbackTxn();
		return;
	}
	result::const_iterator c = R.begin();
	
	if (c[0].is_null() == false)
		strcpy(pOut->ap_acl, c[0].c_str());
	pOut->status = c[1].as(int());
}

// Call Trade Order Frame 3
void CTradeOrderDB::DoTradeOrderFrame3(const TTradeOrderFrame3Input *pIn,
		TTradeOrderFrame3Output *pOut)
{
#ifdef DEBUG
	cout << "TOF3" << endl;
#endif

	ostringstream osCall;
	osCall << "SELECT * FROM TradeOrderFrame3(" <<
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
			m_Txn->esc(pIn->co_name) << "'," <<
			pIn->requested_price << ",'" <<
			pIn->symbol << "')";

	// we are inside a transaction
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		cout << "warning: empty result set at DoTradeOrderFrame3" << endl <<
				osCall.str() << endl;
		pOut->status = CBaseTxnErr::ROLLBACK;
		return;
	}
	result::const_iterator c = R.begin();
	
	strcpy(pOut->co_name, c[0].c_str());
	pOut->requested_price = c[1].as(double());
	strcpy(pOut->symbol, c[2].c_str());
	pOut->buy_value = c[3].as(double());
	pOut->charge_amount = c[4].as(double());
	pOut->comm_rate = c[5].as(double());
	pOut->cust_assets = c[6].as(double());
	pOut->market_price = c[7].as(double());
	strcpy(pOut->s_name, c[8].c_str());
	pOut->sell_value = c[9].as(double());
	pOut->status = c[10].as(int());
	strcpy(pOut->status_id, c[11].c_str());
	pOut->tax_amount = c[12].as(double());
	pOut->type_is_market = (c[13].c_str()[0] == 't' ? 1 : 0);
	pOut->type_is_sell = (c[14].c_str()[0] == 't' ? 1 : 0);

#ifdef DEBUG
	m_coutLock.lock();
	cout<<"Trade Order Frame 3 (input)"<<endl
	    <<"- acct_id: "<<pIn->acct_id<<endl
	    <<"- cust_id: "<<pIn->cust_id<<endl
	    <<"- cust_tier: "<<pIn->cust_tier<<endl
	    <<"- is_lifo: "<<pIn->is_lifo<<endl
	    <<"- issue: "<<pIn->issue<<endl
	    <<"- st_pending_id: "<<pIn->st_pending_id<<endl
	    <<"- st_submitted_id: "<<pIn->st_submitted_id<<endl
	    <<"- tax_status: "<<pIn->tax_status<<endl
	    <<"- trade_qty: "<<pIn->trade_qty<<endl
	    <<"- trade_type_id: "<<pIn->trade_type_id<<endl
	    <<"- type_is_margin: "<<pIn->type_is_margin<<endl
	    <<"- co_name: "<<pIn->co_name<<endl
	    <<"- requested_price: "<<pIn->requested_price<<endl
	    <<"- symbol: "<<pIn->symbol<<endl;
	cout<<"Trade Order Frame 3 (output)"<<endl
	    <<"- co_name: "<<pOut->co_name<<endl
	    <<"- requested_price: "<<pOut->requested_price<<endl
	    <<"- symbol: "<<pOut->symbol<<endl
	    <<"- buy_value: "<<pOut->buy_value<<endl
	    <<"- charge_amount: "<<pOut->charge_amount<<endl
	    <<"- comm_rate: "<<pOut->comm_rate<<endl
	    <<"- cust_assets: "<<pOut->cust_assets<<endl
	    <<"- market_price: "<<pOut->market_price<<endl
	    <<"- s_name: "<<pOut->s_name<<endl
	    <<"- sell_value: "<<pOut->sell_value<<endl
	    <<"- status_id: "<<pOut->status_id<<endl
	    <<"- tax_amount: "<<pOut->tax_amount<<endl
	    <<"- type_is_market: "<<pOut->type_is_market<<endl
	    <<"- type_is_sell: "<<pOut->type_is_sell<<endl;
	m_coutLock.unlock();
#endif //DEBUG
}

// Call Trade Order Frame 4
void CTradeOrderDB::DoTradeOrderFrame4(const TTradeOrderFrame4Input *pIn,
		TTradeOrderFrame4Output *pOut)
{
#ifdef DEBUG
	cout << "TOF4" << endl;
#endif

	ostringstream osCall;
	osCall << "SELECT * FROM TradeOrderFrame4(" <<
			pIn->acct_id << "," <<
			pIn->broker_id << "," <<
			pIn->charge_amount << "," <<
			pIn->comm_amount << ",'" <<
			m_Txn->esc(pIn->exec_name) << "'," <<
			pIn->is_cash << "::SMALLINT," <<
			pIn->is_lifo << "::SMALLINT," <<
			pIn->requested_price << ",'" <<
			pIn->status_id << "','" <<
			pIn->symbol << "'," <<
			pIn->trade_qty << ",'" <<
			pIn->trade_type_id << "'," <<
			pIn->type_is_market << "::SMALLINT)";

	// we are inside a transaction
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		cout << "warning: empty result set at DoTradeOrderFrame4" << endl <<
				osCall.str() << endl;
		pOut->status = CBaseTxnErr::ROLLBACK;
		return;
	}
	result::const_iterator c = R.begin();

	pOut->status = c[0].as(int());
	pOut->trade_id = c[1].as(long());

#ifdef DEBUG
	m_coutLock.lock();
	cout<<"Trade Order Frame 4 (input)"<<endl
	    <<"- acct_id: "<<pIn->acct_id<<endl
	    <<"- charge_amount: "<<pIn->charge_amount<<endl
	    <<"- comm_amount: "<<pIn->comm_amount<<endl
	    <<"- exec_name: "<<pIn->exec_name<<endl
	    <<"- is_cash: "<<pIn->is_cash<<endl
	    <<"- is_lifo: "<<pIn->is_lifo<<endl
	    <<"- requested_price: "<<pIn->requested_price<<endl
	    <<"- status_id: "<<pIn->status_id<<endl
	    <<"- symbol: "<<pIn->symbol<<endl
	    <<"- trade_qty: "<<pIn->trade_qty<<endl
	    <<"- trade_type_id: "<<pIn->trade_type_id<<endl
	    <<"- type_is_market: "<<pIn->type_is_market<<endl;
	cout<<"Trade Order Frame 4 (output)"<<endl
	    <<"- trade_id: "<<pOut->trade_id<<endl;
	m_coutLock.unlock();
#endif //DEBUG
}

// Call Trade Order Frame 5
void CTradeOrderDB::DoTradeOrderFrame5(TTradeOrderFrame5Output *pOut)
{
#ifdef DEBUG
	cout << "TOF5" << endl;
#endif

	// rollback the transaction we are inside
	RollbackTxn();
	pOut->status = CBaseTxnErr::ROLLBACK;
}

// Call Trade Order Frame 6
void CTradeOrderDB::DoTradeOrderFrame6(TTradeOrderFrame6Output *pOut)
{
#ifdef DEBUG
	cout << "TOF6" << endl;
#endif

	// commit the transaction we are inside
	CommitTxn();
	pOut->status = CBaseTxnErr::SUCCESS;
}
