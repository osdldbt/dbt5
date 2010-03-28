/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * 18 July 2006
 */

#include "TradeCleanupDB.h"

// Call Trade Cleanup Frame 1
void CTradeCleanupDB::DoTradeCleanupFrame1(
		const TTradeCleanupFrame1Input *pIn,
		TTradeCleanupFrame1Output *pOut)
{
#ifdef DEBUG
	m_coutLock.lock();
	cout << "*** " << osCall.str() << endl;
	cout << "- Trade Cleanup Frame 1 (input)" << endl <<
			"-- st_canceled_id: " << pIn->st_canceled_id << endl <<
			"-- st_pending_id: " << pIn->st_pending_id << endl <<
			"-- st_submitted_id: " << pIn->st_submitted_id << endl <<
			"-- trade_id: " << pIn->start_trade_id << endl;
	m_coutLock.unlock();
#endif // DEBUG

	startTransaction();
	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Cleanup Frame 1 (output)" << endl <<
			"-- status: " << pOut->status << endl;
	cout << ">>> TCF1" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}
