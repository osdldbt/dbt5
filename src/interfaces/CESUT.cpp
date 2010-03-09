/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *
 * 13 August 2006
 */

#include <transactions.h>

// Constructor
CCESUT::CCESUT(char* addr, const int iListenPort, ofstream* pflog,
		ofstream* pfmix, CMutex* pLogLock, CMutex* pMixLock)
: CBaseInterface(addr, iListenPort, pflog, pfmix, pLogLock, pMixLock)
{
}

// Destructor
CCESUT::~CCESUT()
{
}

// Broker Volume
bool CCESUT::BrokerVolume(PBrokerVolumeTxnInput pTxnInput)
{
	memset(&request, 0, sizeof(struct TMsgDriverBrokerage));

	request.TxnType = BROKER_VOLUME;
	memcpy(&(request.TxnInput.BrokerVolumeTxnInput), pTxnInput,
			sizeof(request.TxnInput.BrokerVolumeTxnInput));
	
	TalkToSUT(&request);
	return true;
}

// Customer Position
bool CCESUT::CustomerPosition(PCustomerPositionTxnInput pTxnInput)
{
	memset(&request, 0, sizeof(struct TMsgDriverBrokerage));

	request.TxnType = CUSTOMER_POSITION;
	memcpy(&(request.TxnInput.CustomerPositionTxnInput), pTxnInput,
			sizeof(request.TxnInput.CustomerPositionTxnInput));
	
	TalkToSUT(&request);
	return true;	
}

// Market Watch
bool CCESUT::MarketWatch(PMarketWatchTxnInput pTxnInput)
{
	memset(&request, 0, sizeof(struct TMsgDriverBrokerage));

	request.TxnType = MARKET_WATCH;
	memcpy(&(request.TxnInput.MarketWatchTxnInput), pTxnInput,
			sizeof(request.TxnInput.MarketWatchTxnInput));
	
	TalkToSUT(&request);
	return true;
}

// Security Detail
bool CCESUT::SecurityDetail(PSecurityDetailTxnInput pTxnInput)
{
	memset(&request, 0, sizeof(struct TMsgDriverBrokerage));

	request.TxnType = SECURITY_DETAIL;
	memcpy(&(request.TxnInput.SecurityDetailTxnInput), pTxnInput,
			sizeof(request.TxnInput.SecurityDetailTxnInput));
	
	TalkToSUT(&request);
	return true;
}

// Trade Lookup
bool CCESUT::TradeLookup(PTradeLookupTxnInput pTxnInput)
{
	memset(&request, 0, sizeof(struct TMsgDriverBrokerage));

	request.TxnType = TRADE_LOOKUP;
	memcpy(&(request.TxnInput.TradeLookupTxnInput), pTxnInput,
			sizeof(request.TxnInput.TradeLookupTxnInput));
	
	TalkToSUT(&request);
	return true;
}

// Trade Status
bool CCESUT::TradeStatus(PTradeStatusTxnInput pTxnInput)
{
	memset(&request, 0, sizeof(struct TMsgDriverBrokerage));

	request.TxnType = TRADE_STATUS;
	memcpy(&(request.TxnInput.TradeStatusTxnInput), pTxnInput,
			sizeof(request.TxnInput.TradeStatusTxnInput));
	
	TalkToSUT(&request);
	return true;
}

// Trade Order
bool CCESUT::TradeOrder(PTradeOrderTxnInput pTxnInput, INT32 iTradeType,
		bool bExecutorIsAccountOwner)
{
	memset(&request, 0, sizeof(struct TMsgDriverBrokerage));

	request.TxnType = TRADE_ORDER;
	memcpy(&(request.TxnInput.TradeOrderTxnInput), pTxnInput,
			sizeof(request.TxnInput.TradeOrderTxnInput));
	
	TalkToSUT(&request);
	return true;
}

// Trade Update
bool CCESUT::TradeUpdate(PTradeUpdateTxnInput pTxnInput)
{
	memset(&request, 0, sizeof(struct TMsgDriverBrokerage));

	request.TxnType = TRADE_UPDATE;
	memcpy(&(request.TxnInput.TradeUpdateTxnInput), pTxnInput,
			sizeof(request.TxnInput.TradeUpdateTxnInput));
	
	TalkToSUT(&request);
	return true;
}

