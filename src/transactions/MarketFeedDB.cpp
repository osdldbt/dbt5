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
	m_coutLock.lock();
	cout << "<<< MFF1" << endl;
	cout << "- Market Feed Frame 1 (input)" << endl <<
			"-- max_feed_len: " << max_feed_len << endl <<
			"-- price_quote: " << "{" << osPrice.str() << "}"<< endl <<
			"-- status_submitted: " <<
					pIn->StatusAndTradeType.status_submitted << endl <<
			"-- symbol: " << "{" << osSymbol.str() << "}" << endl <<
			"-- trade_qty: " << "{" << osQty.str() << "}" << endl <<
			"-- type_limit_buy: " <<
					pIn->StatusAndTradeType.type_limit_buy << endl <<
			"-- type_limit_sell: " <<
					pIn->StatusAndTradeType.type_limit_sell << endl <<
			"-- type_stop_loss: " <<
					pIn->StatusAndTradeType.type_stop_loss << endl;
	m_coutLock.unlock();
#endif // DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setRepeatableRead();
	execute(pIn, pOut, pMarketExchange);
	commitTransaction();
	
#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Market Feed Frame 1 (output)" << endl <<
			"-- send_len: " << pOut->send_len << endl <<
			"-- status: " << pOut->status << endl;
	cout << ">>> MFF1" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}
