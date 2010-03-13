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
	
	return TalkToSUT(&request);
}

// Customer Position
bool CCESUT::CustomerPosition(PCustomerPositionTxnInput pTxnInput)
{
	memset(&request, 0, sizeof(struct TMsgDriverBrokerage));

	request.TxnType = CUSTOMER_POSITION;
	memcpy(&(request.TxnInput.CustomerPositionTxnInput), pTxnInput,
			sizeof(request.TxnInput.CustomerPositionTxnInput));
	
	return TalkToSUT(&request);
}

// Market Watch
bool CCESUT::MarketWatch(PMarketWatchTxnInput pTxnInput)
{
	memset(&request, 0, sizeof(struct TMsgDriverBrokerage));

	request.TxnType = MARKET_WATCH;
	memcpy(&(request.TxnInput.MarketWatchTxnInput), pTxnInput,
			sizeof(request.TxnInput.MarketWatchTxnInput));
	
	return TalkToSUT(&request);
}

// Security Detail
bool CCESUT::SecurityDetail(PSecurityDetailTxnInput pTxnInput)
{
	memset(&request, 0, sizeof(struct TMsgDriverBrokerage));

	request.TxnType = SECURITY_DETAIL;
	memcpy(&(request.TxnInput.SecurityDetailTxnInput), pTxnInput,
			sizeof(request.TxnInput.SecurityDetailTxnInput));
	
	return TalkToSUT(&request);
}

// Trade Lookup
bool CCESUT::TradeLookup(PTradeLookupTxnInput pTxnInput)
{
	memset(&request, 0, sizeof(struct TMsgDriverBrokerage));

	request.TxnType = TRADE_LOOKUP;
	memcpy(&(request.TxnInput.TradeLookupTxnInput), pTxnInput,
			sizeof(request.TxnInput.TradeLookupTxnInput));
	
	return TalkToSUT(&request);
}

// Trade Status
bool CCESUT::TradeStatus(PTradeStatusTxnInput pTxnInput)
{
	memset(&request, 0, sizeof(struct TMsgDriverBrokerage));

	request.TxnType = TRADE_STATUS;
	memcpy(&(request.TxnInput.TradeStatusTxnInput), pTxnInput,
			sizeof(request.TxnInput.TradeStatusTxnInput));
	
	return TalkToSUT(&request);
}

// Trade Order
bool CCESUT::TradeOrder(PTradeOrderTxnInput pTxnInput, INT32 iTradeType,
		bool bExecutorIsAccountOwner)
{
	memset(&request, 0, sizeof(struct TMsgDriverBrokerage));

	request.TxnType = TRADE_ORDER;
	memcpy(&(request.TxnInput.TradeOrderTxnInput), pTxnInput,
			sizeof(request.TxnInput.TradeOrderTxnInput));
	
	return TalkToSUT(&request);
}

// Trade Update
bool CCESUT::TradeUpdate(PTradeUpdateTxnInput pTxnInput)
{
	memset(&request, 0, sizeof(struct TMsgDriverBrokerage));

	request.TxnType = TRADE_UPDATE;
	memcpy(&(request.TxnInput.TradeUpdateTxnInput), pTxnInput,
			sizeof(request.TxnInput.TradeUpdateTxnInput));
	
	return TalkToSUT(&request);
}
