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
	pthread_t pid = pthread_self();
	cout << pid << " <<< MWF1" << endl;
	cout << pid << " - Market Watch Frame 1 (input)" << endl <<
			pid << " -- acct_id: " << pIn->acct_id << endl <<
			pid << " -- cust_id: " << pIn->c_id << endl <<
			pid << " -- ending_co_id: " << pIn->ending_co_id << endl <<
			pid << " -- industry_name: " << pIn->industry_name <<
					" (5% used)" << endl <<
			pid << " -- starting_co_id: " << pIn->starting_co_id <<
					" (used only when industry_name is used)" << endl;
#endif // DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);

	commitTransaction();

#ifdef DEBUG
	cout << pid << " - Market Watch Frame 1 (output)" << endl <<
			pid << " -- pct_change: " << pOut->pct_change << endl;
	cout << pid << " >>> MWF1" << endl;
#endif // DEBUG
}
