/*
 * MarketWatchDB.cpp
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *
 * 13 July 2006
 */

#include <transactions.h>

using namespace TPCE;

// Call Market Watch Frame 1
void CMarketWatchDB::DoMarketWatchFrame1(const TMarketWatchFrame1Input *pIn,
		TMarketWatchFrame1Output *pOut)
{
	ostringstream osCall;
	osCall << "SELECT * FROM MarketWatchFrame1(" <<
			pIn->acct_id << "," <<
			pIn->c_id << "," <<
			pIn->ending_co_id << ",'" <<
			pIn->industry_name << "','" <<
			pIn->start_day.year << "-" <<
			pIn->start_day.month << "-" <<
			pIn->start_day.day << "'," <<
			pIn->starting_co_id << ")";
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< MWF1" << endl;
	cout << "*** " << osCall.str() << endl;
	cout << "- Market Watch Frame 1 (input)" << endl <<
			"-- acct_id: " << pIn->acct_id << endl <<
			"-- cust_id: " << pIn->c_id << endl <<
			"-- ending_co_id: " << pIn->ending_co_id << endl <<
			"-- industry_name: " << pIn->industry_name << " (5% used)" <<
					endl <<
			"-- starting_co_id: " << pIn->starting_co_id <<
					" (used only when industry_name is used)" << endl;
	m_coutLock.unlock();
#endif // DEBUG

	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		//throw logic_error("TradeLookupFrame1: empty result set");
		cout<<"warning: empty result set at DoMarketWatchFrame1"<<endl;
		pOut->status = 1;
	}
	else
	{
		result::const_iterator c = R.begin();

		pOut->pct_change = c[0].as(double());
		pOut->status = c[1].as(int());
	}

	if (pOut->status == 0)	// status ok
	{
		CommitTxn();
	}
	else
	{
		RollbackTxn();
	}

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Market Watch Frame 1 (output)" << endl <<
			"-- status: " << pOut->status << endl <<
			"-- pct_change: " << pOut->pct_change << endl;
	cout << ">>> MWF1" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}
