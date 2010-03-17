/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * 21 July 2006
 */

#include "transactions.h"

#include "TxnBaseDB.h"

// Call Market Feed Frame 1
void CMarketFeedDB::DoMarketFeedFrame1(
		const TMarketFeedFrame1Input *pIn, TMarketFeedFrame1Output *pOut,
		CSendToMarketInterface *pMarketExchange)
{
	ostringstream osSymbol, osPrice, osQty;
	enum mff1 {i_send_len=0, i_status, i_rows_updated, i_symbol,
			i_trade_id, i_price_quote, i_trade_qty, i_trade_type};

	for (unsigned int i = 0;
			i < (sizeof(pIn->Entries)/sizeof(pIn->Entries[0])); ++i) {
		if (i == 0) {
			osSymbol << "\"" << pIn->Entries[i].symbol;
			osPrice << pIn->Entries[i].price_quote;
			osQty << pIn->Entries[i].trade_qty;
		} else {
			osSymbol << "\",\"" << pIn->Entries[i].symbol;
			osPrice << "," << pIn->Entries[i].price_quote;
			osQty << "," << pIn->Entries[i].trade_qty;
		}
	}
	osSymbol << "\"";

	ostringstream osCall;
	osCall << "SELECT * FROM MarketFeedFrame1('{" <<
			osPrice.str() << "}','" <<
			pIn->StatusAndTradeType.status_submitted << "','{" <<
			osSymbol.str() << "}', '{" <<
			osQty.str() << "}','" <<
			pIn->StatusAndTradeType.type_limit_buy << "','" <<
			pIn->StatusAndTradeType.type_limit_sell << "','" <<
			pIn->StatusAndTradeType.type_stop_loss << "')";

#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< MFF1" << endl;
	cout << "*** " << osCall.str() << endl;
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
	execute(osCall.str(), pOut, pMarketExchange);
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
