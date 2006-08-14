/*
 * CESUT.h
 * CE (Customer Emulator) - SUT (Brokerage House) connection class
 *
 * 2006 Rilson Nascimento
 *
 * 28 July 2006
 */

#ifndef CE_SUT_H
#define CE_SUT_H

#include "CESUTInterface.h"

namespace TPCE
{

class CCESUT : public CCESUTInterface, public CBaseInterface
{
	
public:

	CCESUT(char* addr, const int iListenPort, ofstream* pflog, ofstream* pfmix, 
			CSyncLock* pLogLock, CSyncLock* pMixLock);
	~CCESUT(void);

	virtual bool BrokerVolume( PBrokerVolumeTxnInput pTxnInput );			// return whether it was successful
	virtual bool CustomerPosition( PCustomerPositionTxnInput pTxnInput );		// return whether it was successful
	virtual bool MarketWatch( PMarketWatchTxnInput pTxnInput );			// return whether it was successful
	virtual bool SecurityDetail( PSecurityDetailTxnInput pTxnInput );		// return whether it was successful
	virtual bool TradeLookup( PTradeLookupTxnInput pTxnInput );			// return whether it was successful
	virtual bool TradeOrder( PTradeOrderTxnInput pTxnInput, INT32 iTradeType, bool bExecutorIsAccountOwner );
											// return whether it was successful
	virtual bool TradeStatus( PTradeStatusTxnInput pTxnInput );			// return whether it was successful
	virtual bool TradeUpdate( PTradeUpdateTxnInput pTxnInput );			// return whether it was successful

};

}	// namespace TPCE

#endif	// CE_SUT_H
