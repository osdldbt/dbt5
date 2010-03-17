/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * 21 July 2006
 */

#include <transactions.h>

// Call Market Feed Frame 1
void CMarketFeedDB::DoMarketFeedFrame1(
		const TMarketFeedFrame1Input *pIn, TMarketFeedFrame1Output *pOut,
		CSendToMarketInterface *pSendToMarket)
{
	ostringstream osSymbol, osPrice, osQty;
	enum mff1 {i_send_len=0, i_status, i_rows_updated, i_symbol,
			i_trade_id, i_price_quote, i_trade_qty, i_trade_type};

	for (unsigned int i = 0;
			i < (sizeof(pIn->Entries)/sizeof(pIn->Entries[0])); ++i)
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

	begin();
	// Isolation level required by Clause 7.4.1.3
	execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
	result R(execute(osCall.str()));
	commit();

	if (!R.empty()) 
	{
		result::const_iterator c = R.begin();
	
		pOut->send_len = c[i_send_len].as(int());;

		vector<string> v1;
		vector<string>::iterator p1;
		vector<string> v2;
		vector<string>::iterator p2;
		vector<string> v3;
		vector<string>::iterator p3;
		vector<string> v4;
		vector<string>::iterator p4;
		vector<string> v5;
		vector<string>::iterator p5;

		TokenizeSmart(c[i_symbol].c_str(), v1);
		TokenizeSmart(c[i_trade_id].c_str(), v2);
		TokenizeSmart(c[i_price_quote].c_str(), v3);
		TokenizeSmart(c[i_trade_qty].c_str(), v4);
		TokenizeSmart(c[i_trade_type].c_str(), v5);

		// FIXME: Consider altering to match spec.  Since PostgreSQL cannot 
		// control transaction from within a stored function and because we're
		// making the call to the Market Emulator from outside the transaction,
		// consider altering the code to call a stored function once per symbol
		// to match the transaction rules in the spec.
		int i = 0;
		bool bSent;	
		for (p1 = v1.begin(), p2 = v2.begin(), p3 = v3.begin(), p4 = v4.begin(),
				p5 = v5.begin(); p1 != v1.end(); ++p1, ++p2, ++p3, ++p4) {
			strncpy(m_TriggeredLimitOrders.symbol, (*p1).c_str(), cSYMBOL_len);
			m_TriggeredLimitOrders.trade_id = atol((*p2).c_str());
			m_TriggeredLimitOrders.price_quote = atof((*p3).c_str());
			m_TriggeredLimitOrders.trade_qty = atoi((*p4).c_str());
			strncpy(m_TriggeredLimitOrders.trade_type_id, (*p5).c_str(),
					cTT_ID_len);

			bSent = pSendToMarket->SendToMarketFromFrame(
					m_TriggeredLimitOrders);
			++i;
		}
		check_count(pOut->send_len, i, __FILE__, __LINE__);

		if (atoi(c[i_rows_updated].c_str()) != max_feed_len)
			pOut->status = -311;
	} else {
		pOut->send_len = 0;
		pOut->status = CBaseTxnErr::ROLLBACK;
	}
	
#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Market Feed Frame 1 (output)" << endl <<
			"-- send_len: " << pOut->send_len << endl <<
			"-- status: " << pOut->status << endl;
	cout << ">>> MFF1" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}
