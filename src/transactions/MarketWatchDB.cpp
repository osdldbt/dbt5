/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * 13 July 2006
 */

#include "MarketWatchDB.h"

// Call Market Watch Frame 1
void CMarketWatchDB::DoMarketWatchFrame1(const TMarketWatchFrame1Input *pIn,
		TMarketWatchFrame1Output *pOut)
{
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< MWF1" << endl;
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

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);

	if (pOut->status == CBaseTxnErr::SUCCESS) {
	// status ok
		commitTransaction();
	} else {
		rollbackTransaction();
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
