/*
 * MarketFeedDB.h
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *
 * 21 July 2006
 */

#ifndef MARKET_FEED_DB_H
#define MARKET_FEED_DB_H

#include <TxnHarnessDBInterface.h> 

namespace TPCE
{

class CMarketFeedDB : public CTxnBaseDB, public CMarketFeedDBInterface
{
	// member used to send triggered orders to MEE
	TTradeRequest		m_TriggeredLimitOrders;
public:
	CMarketFeedDB(CDBConnection *pDBConn);
	~CMarketFeedDB();

	virtual void DoMarketFeedFrame1(const TMarketFeedFrame1Input *pIn, 
			TMarketFeedFrame1Output *pOut,
			CSendToMarketInterface *pSendToMarket);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException);

};

}	// namespace TPCE

#endif	// MARKET_FEED_DB_H
