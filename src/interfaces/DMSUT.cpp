/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *
 * 12 August 2006
 */

#include <transactions.h>

using namespace TPCE;

// constructor
CDMSUT::CDMSUT(char* addr, const int iListenPort, ofstream* pflog,
		ofstream* pfmix, CMutex* pLogLock, CMutex* pMixLock)
: CBaseInterface(addr, iListenPort, pflog, pfmix, pLogLock, pMixLock)
{
}

// destructor
CDMSUT::~CDMSUT()
{
}

// Data Maintenance
bool CDMSUT::DataMaintenance( PDataMaintenanceTxnInput pTxnInput )
{
	cout<<"Data Maintenance requested"<<endl;

	PMsgDriverBrokerage pRequest = new TMsgDriverBrokerage;
	memset(pRequest, 0, sizeof(TMsgDriverBrokerage));

	pRequest->TxnType = DATA_MAINTENANCE;
	memcpy( &(pRequest->TxnInput.DataMaintenanceTxnInput), pTxnInput,
			sizeof( TDataMaintenanceTxnInput ));
	
	TalkToSUT(pRequest);
	return true;
}

// Trade Cleanup
bool CDMSUT::TradeCleanup( PTradeCleanupTxnInput pTxnInput )
{
	PMsgDriverBrokerage pRequest = new TMsgDriverBrokerage;
	memset(pRequest, 0, sizeof(TMsgDriverBrokerage));

	pRequest->TxnType = TRADE_CLEANUP;
	memcpy( &(pRequest->TxnInput.TradeCleanupTxnInput), pTxnInput,
			sizeof( TTradeCleanupTxnInput ));
	
	TalkToSUT(pRequest);
	return true;
}
