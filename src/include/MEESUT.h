/*
 * MEESUT.h
 * MEE (Market Exchange Emulator) - SUT Interface class
 *
 * 2006 Rilson Nascimento
 *
 * 23 July 2006
 */

#ifndef MEE_SUT_H
#define MEE_SUT_H

#include "MEESUTInterface.h"

namespace TPCE
{
class CMEESUT : public CMEESUTInterface
{
private:
	CDBConnection*		m_pDBConnection;
	TTradeResultTxnInput	m_TradeResultTxnInput;
	TMarketFeedTxnInput	m_MarketFeedTxnInput;

public:
	CMEESUT(CDBConnection *pDBConn);
	~CMEESUT();

	virtual bool TradeResult( PTradeResultTxnInput pTxnInput );	// return whether it was successful
	virtual bool MarketFeed( PMarketFeedTxnInput pTxnInput );	// return whether it was successful
	
	friend void* TPCE::TradeResultAsync(void* data);
	friend bool TPCE::RunTradeResultAsync( CMEESUT* );

	friend void* TPCE::MarketFeedAsync(void* data);
	friend bool TPCE::RunMarketFeedAsync( CMEESUT* );
};

}	// namespace TPCE

#endif	// DM_SUT_H
