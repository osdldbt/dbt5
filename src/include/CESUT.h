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
#include "locking.h"

using namespace TPCE;

class CCESUT : public CCESUTInterface, public CBaseInterface
{
	
public:

	CCESUT(char* addr, const int iListenPort, ofstream* pflog, ofstream* pfmix, 
			CMutex* pLogLock, CMutex* pMixLock);
	~CCESUT(void);

	// return whether it was successful
	virtual bool BrokerVolume( PBrokerVolumeTxnInput pTxnInput );
	// return whether it was successful
	virtual bool CustomerPosition( PCustomerPositionTxnInput pTxnInput );
	// return whether it was successful
	virtual bool MarketWatch( PMarketWatchTxnInput pTxnInput );
	// return whether it was successful
	virtual bool SecurityDetail( PSecurityDetailTxnInput pTxnInput );
	// return whether it was successful
	virtual bool TradeLookup( PTradeLookupTxnInput pTxnInput );
	// return whether it was successful
	virtual bool TradeOrder( PTradeOrderTxnInput pTxnInput, INT32 iTradeType,
			bool bExecutorIsAccountOwner );
	// return whether it was successful
	virtual bool TradeStatus( PTradeStatusTxnInput pTxnInput );
	// return whether it was successful
	virtual bool TradeUpdate( PTradeUpdateTxnInput pTxnInput );

};

#endif	// CE_SUT_H
