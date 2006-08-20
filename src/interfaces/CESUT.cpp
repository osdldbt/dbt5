/*
 * CESUT.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 13 August 2006
 */

#include <transactions.h>

using namespace TPCE;

// Constructor
CCESUT::CCESUT(char* addr, const int iListenPort, ofstream* pflog, ofstream* pfmix, CSyncLock* pLogLock, CSyncLock* pMixLock)
: CBaseInterface(addr, iListenPort, pflog, pfmix, pLogLock, pMixLock)
{
}

// Destructor
CCESUT::~CCESUT()
{
}

// Broker Volume
bool CCESUT::BrokerVolume( PBrokerVolumeTxnInput pTxnInput )
{
	//cout<<"Broker Volume requested"<<endl;

	PMsgDriverBrokerage pRequest = new TMsgDriverBrokerage;
	memset(pRequest, 0, sizeof(*pRequest));

	pRequest->TxnType = BROKER_VOLUME;
	memcpy( &(pRequest->TxnInput.BrokerVolumeTxnInput), pTxnInput, sizeof( pRequest->TxnInput.BrokerVolumeTxnInput ));
	
	TalkToSUT(pRequest);
	return true;
}

// Customer Position
bool CCESUT::CustomerPosition( PCustomerPositionTxnInput pTxnInput )
{
	//cout<<"Customer Position requested"<<endl;

	PMsgDriverBrokerage pRequest = new TMsgDriverBrokerage;
	memset(pRequest, 0, sizeof(*pRequest));

	pRequest->TxnType = CUSTOMER_POSITION;
	memcpy( &(pRequest->TxnInput.CustomerPositionTxnInput), pTxnInput, sizeof( pRequest->TxnInput.CustomerPositionTxnInput ));
	
	TalkToSUT(pRequest);
	return true;	
}

// Market Watch
bool CCESUT::MarketWatch( PMarketWatchTxnInput pTxnInput )
{
	//cout<<"Market Watch requested"<<endl;

	PMsgDriverBrokerage pRequest = new TMsgDriverBrokerage;
	memset(pRequest, 0, sizeof(*pRequest));

	pRequest->TxnType = MARKET_WATCH;
	memcpy( &(pRequest->TxnInput.MarketWatchTxnInput), pTxnInput, sizeof( pRequest->TxnInput.MarketWatchTxnInput ));
	
	TalkToSUT(pRequest);
	return true;
}

// Security Detail
bool CCESUT::SecurityDetail( PSecurityDetailTxnInput pTxnInput )
{
	//cout<<"Security Detail requested"<<endl;

	PMsgDriverBrokerage pRequest = new TMsgDriverBrokerage;
	memset(pRequest, 0, sizeof(*pRequest));

	pRequest->TxnType = SECURITY_DETAIL;
	memcpy( &(pRequest->TxnInput.SecurityDetailTxnInput), pTxnInput, sizeof( pRequest->TxnInput.SecurityDetailTxnInput ));
	
	TalkToSUT(pRequest);
	return true;
}

// Trade Lookup
bool CCESUT::TradeLookup( PTradeLookupTxnInput pTxnInput )
{
	//cout<<"Trade Lookup requested"<<endl;

	PMsgDriverBrokerage pRequest = new TMsgDriverBrokerage;
	memset(pRequest, 0, sizeof(*pRequest));

	pRequest->TxnType = TRADE_LOOKUP;
	memcpy( &(pRequest->TxnInput.TradeLookupTxnInput), pTxnInput, sizeof( pRequest->TxnInput.TradeLookupTxnInput ));
	
	TalkToSUT(pRequest);
	return true;
}

// Trade Status
bool CCESUT::TradeStatus( PTradeStatusTxnInput pTxnInput )
{
	//cout<<"Trade Status requested"<<endl;

	PMsgDriverBrokerage pRequest = new TMsgDriverBrokerage;
	memset(pRequest, 0, sizeof(*pRequest));

	pRequest->TxnType = TRADE_STATUS;
	memcpy( &(pRequest->TxnInput.TradeStatusTxnInput), pTxnInput, sizeof( pRequest->TxnInput.TradeStatusTxnInput ));
	
	TalkToSUT(pRequest);
	return true;
}

// Trade Order
bool CCESUT::TradeOrder( PTradeOrderTxnInput pTxnInput, INT32 iTradeType, bool bExecutorIsAccountOwner )
{
	//cout<<"Trade Order requested"<<endl;

	PMsgDriverBrokerage pRequest = new TMsgDriverBrokerage;
	memset(pRequest, 0, sizeof(*pRequest));

	pRequest->TxnType = TRADE_ORDER;
	memcpy( &(pRequest->TxnInput.TradeOrderTxnInput), pTxnInput, sizeof( pRequest->TxnInput.TradeOrderTxnInput ));
	
	TalkToSUT(pRequest);
	return true;
}

// Trade Update
bool CCESUT::TradeUpdate( PTradeUpdateTxnInput pTxnInput )
{
	//cout<<"Trade Update requested"<<endl;

	PMsgDriverBrokerage pRequest = new TMsgDriverBrokerage;
	memset(pRequest, 0, sizeof(*pRequest));

	pRequest->TxnType = TRADE_UPDATE;
	memcpy( &(pRequest->TxnInput.TradeUpdateTxnInput), pTxnInput, sizeof( pRequest->TxnInput.TradeUpdateTxnInput ));
	
	TalkToSUT(pRequest);
	return true;
}

