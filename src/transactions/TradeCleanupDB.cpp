/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 18 July 2006
 */

#include "TradeCleanupDB.h"

// Call Trade Cleanup Frame 1
void CTradeCleanupDB::DoTradeCleanupFrame1(const TTradeCleanupFrame1Input *pIn)
{
#ifdef DEBUG
	pthread_t pid = pthread_self();
	cout << pid << " - Trade Cleanup Frame 1 (input)" << endl <<
			pid << " -- st_canceled_id: " << pIn->st_canceled_id << endl <<
			pid << " -- st_pending_id: " << pIn->st_pending_id << endl <<
			pid << " -- st_submitted_id: " << pIn->st_submitted_id << endl <<
			pid << " -- trade_id: " << pIn->start_trade_id << endl;
#endif // DEBUG

	startTransaction();
	execute(pIn);
	commitTransaction();

#ifdef DEBUG
	cout << pid << " - Trade Cleanup Frame 1 (output)" << endl <<
			pid << " >>> TCF1" << endl;
#endif // DEBUG
}
