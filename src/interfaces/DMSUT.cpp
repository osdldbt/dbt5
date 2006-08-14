/*
 * DMSUT.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 12 August 2006
 */

#include <transactions.h>

using namespace TPCE;

// constructor
CDMSUT::CDMSUT(char* addr, const int iListenPort, ofstream* pflog, ofstream* pfmix, CSyncLock* pLogLock, CSyncLock* pMixLock)
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

	TMsgDriverBrokerage Request;
	memset(&Request, 0, sizeof(TMsgDriverBrokerage));

	Request.TxnType = DATA_MAINTENANCE;
	memcpy( &(Request.TxnInput.DataMaintenanceTxnInput), pTxnInput, sizeof( TDataMaintenanceTxnInput ));
	
	TalkToSUT(&Request);
	return true;
}

// Trade Cleanup
bool CDMSUT::TradeCleanup( PTradeCleanupTxnInput pTxnInput )
{
	cout<<"Trade Cleanup requested"<<endl;

	TMsgDriverBrokerage Request;
	memset(&Request, 0, sizeof(TMsgDriverBrokerage));

	Request.TxnType = TRADE_CLEANUP;
	memcpy( &(Request.TxnInput.TradeCleanupTxnInput), pTxnInput, sizeof( TTradeCleanupTxnInput ));
	
	TalkToSUT(&Request);
	return true;
}
