/*
 * TradeCleanupDB.cpp
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *
 * 18 July 2006
 */

#include <transactions.h>

using namespace TPCE;

// Call Trade Cleanup Frame 1
void CTradeCleanupDB::DoTradeCleanupFrame1(
		const TTradeCleanupFrame1Input *pIn,
		TTradeCleanupFrame1Output *pOut)
{
	ostringstream osCall;
	osCall << "SELECT * FROM TradeCleanupFrame1('" <<
			pIn->st_canceled_id << "','" <<
			pIn->st_pending_id << "','" <<
			pIn->st_submitted_id << "'," <<
			pIn->start_trade_id << ")";

	BeginTxn();
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	if (R.empty()) 
	{
		cerr << "warning: empty result set at DoTradeCleanupFrame1" << endl <<
				osCall.str() << endl;
 		pOut->status = CBaseTxnErr::ROLLBACK;
		return;
	}
	result::const_iterator c = R.begin();
 	pOut->status = c[0].as(int());

#ifdef DEBUG
	m_coutLock.lock();
	cout << ">>> TCF1" << endl;
	cout << "*** " << osCall.str() << endl;
	cout << "- Trade Cleanup Frame 1 (input)" << endl <<
			"-- st_canceled_id: " << pIn->st_canceled_id << endl <<
			"-- st_pending_id: " << pIn->st_pending_id << endl <<
			"-- st_submitted_id: " << pIn->st_submitted_id << endl <<
			"-- trade_id: " << pIn->start_trade_id << endl;
	cout << "- Trade Cleanup Frame 1 (output)" << endl <<
			"-- status: " << pOut->status << endl;
	m_coutLock.unlock();
#endif // DEBUG
}
