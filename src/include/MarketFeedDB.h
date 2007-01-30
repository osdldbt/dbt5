/*
 * MarketFeedDB.h
 *
 * 2006 Rilson Nascimento
 *
 * 21 July 2006
 */

#ifndef MARKET_FEED_DB_H
#define MARKET_FEED_DB_H

namespace TPCE
{

class CMarketFeedDB : public CTxnBaseDB
{
	TMarketFeedFrame1Input	Frame1Input;
	TMarketFeedFrame1Output	Frame1Output;

	// member used to send triggered orders to MEE
	TTradeRequest		m_TriggeredLimitOrders;
public:
	CMarketFeedDB(CDBConnection *pDBConn);
	~CMarketFeedDB();

	void DoMarketFeedFrame1(PMarketFeedFrame1Input pFrame1Input, 
					PMarketFeedFrame1Output pFrame1Output,
					CSendToMarketInterface*	pSendToMarket);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException);

};

}	// namespace TPCE

#endif	// MARKET_FEED_DB_H
