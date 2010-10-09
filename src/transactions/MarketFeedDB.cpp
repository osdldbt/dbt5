/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * 21 July 2006
 */


#include "MarketFeedDB.h"

// Call Market Feed Frame 1
void CMarketFeedDB::DoMarketFeedFrame1(
		const TMarketFeedFrame1Input *pIn, TMarketFeedFrame1Output *pOut,
		CSendToMarketInterface *pMarketExchange)
{
#ifdef DEBUG
	pthread_t pid = pthread_self();
	cout << pid << " <<< MFF1" << endl;
	cout << pid << " - Market Feed Frame 1 (input)" << endl <<
			pid << " -- max_feed_len: " << max_feed_len << endl <<
			pid << " -- status_submitted: " <<
					pIn->StatusAndTradeType.status_submitted << endl <<
			pid << " -- type_limit_buy: " <<
					pIn->StatusAndTradeType.type_limit_buy << endl <<
			pid << " -- type_limit_sell: " <<
					pIn->StatusAndTradeType.type_limit_sell << endl <<
			pid << " -- type_stop_loss: " <<
					pIn->StatusAndTradeType.type_stop_loss << endl;
#endif // DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setRepeatableRead();
	execute(pIn, pOut, pMarketExchange);
	commitTransaction();
	
#ifdef DEBUG
	cout << pid << " - Market Feed Frame 1 (output)" << endl <<
			pid << " -- send_len: " << pOut->send_len << endl <<
	cout << pid << " >>> MFF1" << endl;
#endif // DEBUG
}
