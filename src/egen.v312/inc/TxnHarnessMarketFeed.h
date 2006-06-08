/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
#ifndef TXN_HARNESS_MARKET_FEED_H
#define TXN_HARNESS_MARKET_FEED_H

namespace TPCE
{

class CMarketFeed
{
	CMarketFeedDB			m_db;
	CSendToMarketInterface*	m_pSendToMarket;

public:
	CMarketFeed(const char *szServer, const char *szDatabase, CSendToMarketInterface *pSendToMarket)
	: m_db(szServer, szDatabase)
	, m_pSendToMarket(pSendToMarket)
	{		
	};

	void DoTxn( PMarketFeedTxnInput pTxnInput, PMarketFeedTxnOutput pTxnOutput )
	{
		int							j;
		TMarketFeedFrame1Output		Frame1Output;
		memset(&Frame1Output, 0, sizeof( Frame1Output ));

		// Execute Frame1
		m_db.DoMarketFeedFrame1(pTxnInput, &Frame1Output);		

		// Process the returned array and send it to the Market Emulator
		for (j=0; j<Frame1Output.send_len; ++j)
		{
			//Send to Market Emulator
			Frame1Output.trade_req[j].eAction = eMEEProcessOrder;
			m_pSendToMarket->SendToMarket(Frame1Output.trade_req[j]);// maybe should check the return code here

			//Send trade_id back to driver
			pTxnOutput->trade_id[j] = Frame1Output.trade_req[j].trade_id;
		}

		pTxnOutput->send_len = Frame1Output.send_len;
		pTxnOutput->status = Frame1Output.status;
	}

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException)
	{
		m_db.Cleanup(pException);
	}
};

}	// namespace TPCE

#endif //TXN_HARNESS_MARKET_FEED_H
