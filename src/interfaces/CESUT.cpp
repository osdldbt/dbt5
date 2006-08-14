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

	TMsgDriverBrokerage Request;
	memset(&Request, 0, sizeof(TMsgDriverBrokerage));

	Request.TxnType = BROKER_VOLUME;
	memcpy( &(Request.TxnInput.BrokerVolumeTxnInput), pTxnInput, sizeof( TBrokerVolumeTxnInput ));
	
	TalkToSUT(&Request);
	return true;
}

// Customer Position
bool CCESUT::CustomerPosition( PCustomerPositionTxnInput pTxnInput )
{
	//cout<<"Customer Position requested"<<endl;

	TMsgDriverBrokerage Request;
	memset(&Request, 0, sizeof(TMsgDriverBrokerage));

	Request.TxnType = CUSTOMER_POSITION;
	memcpy( &(Request.TxnInput.CustomerPositionTxnInput), pTxnInput, sizeof( TCustomerPositionTxnInput ));
	
	TalkToSUT(&Request);
	return true;	
}

// Market Watch
bool CCESUT::MarketWatch( PMarketWatchTxnInput pTxnInput )
{
	//cout<<"Market Watch requested"<<endl;

	TMsgDriverBrokerage Request;
	memset(&Request, 0, sizeof(TMsgDriverBrokerage));

	Request.TxnType = MARKET_WATCH;
	memcpy( &(Request.TxnInput.MarketWatchTxnInput), pTxnInput, sizeof( TMarketWatchTxnInput ));
	
	TalkToSUT(&Request);
	return true;
}

// Security Detail
bool CCESUT::SecurityDetail( PSecurityDetailTxnInput pTxnInput )
{
	//cout<<"Security Detail requested"<<endl;

	TMsgDriverBrokerage Request;
	memset(&Request, 0, sizeof(TMsgDriverBrokerage));

	Request.TxnType = SECURITY_DETAIL;
	memcpy( &(Request.TxnInput.SecurityDetailTxnInput), pTxnInput, sizeof( TSecurityDetailTxnInput ));
	
	TalkToSUT(&Request);
	return true;
}

// Trade Lookup
bool CCESUT::TradeLookup( PTradeLookupTxnInput pTxnInput )
{
	//cout<<"Trade Lookup requested"<<endl;

	TMsgDriverBrokerage Request;
	memset(&Request, 0, sizeof(TMsgDriverBrokerage));

	Request.TxnType = TRADE_LOOKUP;
	memcpy( &(Request.TxnInput.TradeLookupTxnInput), pTxnInput, sizeof( TTradeLookupTxnInput ));
	
	TalkToSUT(&Request);
	return true;
}

// Trade Status
bool CCESUT::TradeStatus( PTradeStatusTxnInput pTxnInput )
{
	//cout<<"Trade Status requested"<<endl;

	TMsgDriverBrokerage Request;
	memset(&Request, 0, sizeof(TMsgDriverBrokerage));

	Request.TxnType = TRADE_STATUS;
	memcpy( &(Request.TxnInput.TradeStatusTxnInput), pTxnInput, sizeof( TTradeStatusTxnInput ));
	
	TalkToSUT(&Request);
	return true;
}

// Trade Order
bool CCESUT::TradeOrder( PTradeOrderTxnInput pTxnInput, INT32 iTradeType, bool bExecutorIsAccountOwner )
{
	//cout<<"Trade Order requested"<<endl;

	TMsgDriverBrokerage Request;
	memset(&Request, 0, sizeof(TMsgDriverBrokerage));

	Request.TxnType = TRADE_ORDER;
	memcpy( &(Request.TxnInput.TradeOrderTxnInput), pTxnInput, sizeof( TTradeOrderTxnInput ));
	
	TalkToSUT(&Request);
	return true;
}

// Trade Update
bool CCESUT::TradeUpdate( PTradeUpdateTxnInput pTxnInput )
{
	//cout<<"Trade Update requested"<<endl;

	TMsgDriverBrokerage Request;
	memset(&Request, 0, sizeof(TMsgDriverBrokerage));

	Request.TxnType = TRADE_UPDATE;
	memcpy( &(Request.TxnInput.TradeUpdateTxnInput), pTxnInput, sizeof( TTradeUpdateTxnInput ));
	
	TalkToSUT(&Request);
	return true;
}

