/*
 * MarketFeedDB.cpp
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *
 * 21 July 2006
 */

#include <transactions.h>

using namespace TPCE;

// Call Market Feed Frame 1
void CMarketFeedDB::DoMarketFeedFrame1(
		const TMarketFeedFrame1Input *pIn, TMarketFeedFrame1Output *pOut,
		CSendToMarketInterface *pSendToMarket)
{
#ifdef DEBUG
	cout << "MFF1" << endl;
#endif

	ostringstream osSymbol, osPrice, osQty;

	for (int i = 0; i < (sizeof(pIn->Entries)/sizeof(pIn->Entries[0])); ++i)
	{
		if (i == 0)
		{
			osSymbol << "\"" << pIn->Entries[i].symbol;
			osPrice << pIn->Entries[i].price_quote;
			osQty << pIn->Entries[i].trade_qty;
		}
		else
		{
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

	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
	result R( m_Txn->exec( osCall.str() ) );
	// Sponsors should consider committing the database changes
	// before calling the send_to_market interface (we're doing that)
	CommitTxn();

	if (!R.empty()) 
	{
		result::const_iterator c = R.begin();
	
		int i = 0;
		bool bSent;	
		for ( c; c != R.end(); ++c )
		{
			strcpy(m_TriggeredLimitOrders.symbol,  pIn->Entries[i].symbol);
			m_TriggeredLimitOrders.price_quote = pIn->Entries[i].price_quote;
			m_TriggeredLimitOrders.trade_qty = pIn->Entries[i].trade_qty;

			bSent = pSendToMarket->SendToMarketFromFrame(
					m_TriggeredLimitOrders);
			++i;
		}
		pOut->send_len = c[0].as(int());;
		pOut->status = c[1].as(int());
	}
	else
	{
		pOut->send_len = 0;
	}
	
#ifdef DEBUG
	m_coutLock.lock(); cout << "Market Feed Frame 1 (input)" << endl << "- max_feed_len: " << max_feed_len << endl <<
			"- price_quote: " << "{" << osPrice.str() << "}"<< endl <<
			"- status_submitted: " <<
			pIn->StatusAndTradeType.status_submitted << endl <<
			"- symbol: " << "{" << osSymbol.str() << "}" << endl <<
			"- trade_qty: " << "{" << osQty.str() << "}" << endl <<
			"- type_limit_buy: " <<
			pIn->StatusAndTradeType.type_limit_buy << endl <<
			"- type_limit_sell: " <<
			pIn->StatusAndTradeType.type_limit_sell << endl <<
			"- type_stop_loss: " <<
			pIn->StatusAndTradeType.type_stop_loss << endl;
	cout<<"Market Feed Frame 1 (output)"<<endl
	    <<"- send_len: "<<pOut->send_len<<endl;
	m_coutLock.unlock();
#endif // DEBUG
}
