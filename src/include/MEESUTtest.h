/*
 * MEESUTtest.h
 * MEE (Market Exchange Emulator) - SUT Interface test class 
 *
 * 2006 Rilson Nascimento
 *
 * 23 July 2006
 */

#ifndef MEE_SUT_TEST_H
#define MEE_SUT_TEST_H

#include "MEESUTInterface.h"

namespace TPCE
{
class CMEESUTtest : public CMEESUTInterface
{
private:
	CDBConnection*		m_pDBConnection;
	TTradeResultTxnInput	m_TradeResultTxnInput;
	TMarketFeedTxnInput	m_MarketFeedTxnInput;

public:
	CMEESUTtest(CDBConnection *pDBConn);
	~CMEESUTtest();

	// return whether it was successful
	virtual bool TradeResult( PTradeResultTxnInput pTxnInput );
	// return whether it was successful
	virtual bool MarketFeed( PMarketFeedTxnInput pTxnInput );
	
	friend void* TPCE::TradeResultAsync(void* data);
	friend bool TPCE::RunTradeResultAsync( CMEESUTtest* );

	friend void* TPCE::MarketFeedAsync(void* data);
	friend bool TPCE::RunMarketFeedAsync( CMEESUTtest* );
};

}	// namespace TPCE

#endif	// MEE_SUT_TEST_H
