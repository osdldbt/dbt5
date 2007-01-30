/*
 * MEESUT.h
 * MEE (Market Exchange Emulator) - SUT Interface class 
 *
 * 2006 Rilson Nascimento
 *
 * 30 July 2006
 */

#ifndef MEE_SUT_H
#define MEE_SUT_H

#include "MEESUTInterface.h"

namespace TPCE
{
class CMEESUT : public CMEESUTInterface, public CBaseInterface
{
private:
	TTradeResultTxnInput	m_TradeResultTxnInput;
	TMarketFeedTxnInput	m_MarketFeedTxnInput;
	
public:
	CMEESUT(char* addr, const int iListenPort, ofstream* pflog, ofstream* pfmix,
			CSyncLock* pLogLock, CSyncLock* pMixLock);
	~CMEESUT();

	// return whether it was successful
	virtual bool TradeResult( PTradeResultTxnInput pTxnInput );
	// return whether it was successful
	virtual bool MarketFeed( PMarketFeedTxnInput pTxnInput );
	
	friend void* TPCE::TradeResultAsync(void* data);
	friend bool TPCE::RunTradeResultAsync(void* data);

	friend void* TPCE::MarketFeedAsync(void* data);
	friend bool TPCE::RunMarketFeedAsync(void* data);

};

//parameter structure for the threads
typedef struct TMEESUTThreadParam
{
	CMEESUT*		pCMEESUT;
	union
	{
		TTradeResultTxnInput	m_TradeResultTxnInput;
		TMarketFeedTxnInput	m_MarketFeedTxnInput;
	} TxnInput;
} *PMEESUTThreadParam;


}	// namespace TPCE

#endif	// MEE_SUT_H
