/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2010 Rilson Nascimento
 *
 * CE (Customer Emulator) - SUT (Brokerage House) connection class
 * 28 July 2006
 */

#ifndef CE_SUT_H
#define CE_SUT_H

#include "CE.h"
#include "CESUTInterface.h"
#include "locking.h"

#include "BaseInterface.h"
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

private:

	struct TMsgDriverBrokerage request;
};

#endif	// CE_SUT_H
