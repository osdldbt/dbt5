/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *               2010 Mark Wong
 *
 * 29 July 2006
 */

#ifndef COMMON_STRUCTS_H
#define COMMON_STRUCTS_H

#include "CE.h"
using namespace TPCE;

// Transaction types
enum eTxnType
{
	NULL_TXN = -1,
	TRADE_ORDER,
	TRADE_RESULT,
	TRADE_LOOKUP,
	TRADE_UPDATE,
	TRADE_STATUS,
	CUSTOMER_POSITION,
	BROKER_VOLUME,
	SECURITY_DETAIL,
	MARKET_FEED,
	MARKET_WATCH,
	DATA_MAINTENANCE,
	TRADE_CLEANUP
};

// structure of the message Driver --> Brokerage House
typedef struct TMsgDriverBrokerage
{
	eTxnType	TxnType;
	union
	{
		TBrokerVolumeTxnInput		BrokerVolumeTxnInput;
		TCustomerPositionTxnInput	CustomerPositionTxnInput;
		TMarketFeedTxnInput		MarketFeedTxnInput;
		TMarketWatchTxnInput		MarketWatchTxnInput;
		TTradeLookupTxnInput		TradeLookupTxnInput;
		TSecurityDetailTxnInput		SecurityDetailTxnInput;
		TTradeOrderTxnInput		TradeOrderTxnInput;
		TTradeResultTxnInput		TradeResultTxnInput;
		TTradeStatusTxnInput		TradeStatusTxnInput;
		TTradeUpdateTxnInput		TradeUpdateTxnInput;
		TDataMaintenanceTxnInput	DataMaintenanceTxnInput;
		TTradeCleanupTxnInput		TradeCleanupTxnInput;
	} TxnInput;
} *PMsgDriverBrokerage;

// structure of the message Brokerage House --> Driver
typedef struct TMsgBrokerageDriver
{
	int			iStatus;
} *PMsgBrokerageDriver;

#endif	//COMMON_STRUCTS_H
