/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
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
	SECURITY_DETAIL,
	BROKER_VOLUME,
	CUSTOMER_POSITION,
	MARKET_WATCH,
	TRADE_STATUS,
	TRADE_LOOKUP,
	TRADE_ORDER,
	TRADE_UPDATE,
	MARKET_FEED,
	TRADE_RESULT,
	DATA_MAINTENANCE,
	TRADE_CLEANUP
};

// structure of the message Driver --> Brokerage House
typedef struct TMsgDriverBrokerage
{
	eTxnType TxnType;

	union
	{
		TBrokerVolumeTxnInput BrokerVolumeTxnInput;
		TCustomerPositionTxnInput CustomerPositionTxnInput;
		TMarketFeedTxnInput MarketFeedTxnInput;
		TMarketWatchTxnInput MarketWatchTxnInput;
		TTradeLookupTxnInput TradeLookupTxnInput;
		TSecurityDetailTxnInput SecurityDetailTxnInput;
		TTradeOrderTxnInput TradeOrderTxnInput;
		TTradeResultTxnInput TradeResultTxnInput;
		TTradeStatusTxnInput TradeStatusTxnInput;
		TTradeUpdateTxnInput TradeUpdateTxnInput;
		TDataMaintenanceTxnInput DataMaintenanceTxnInput;
		TTradeCleanupTxnInput TradeCleanupTxnInput;
	} TxnInput;
} *PMsgDriverBrokerage;

// structure of the message Brokerage House --> Driver
typedef struct TMsgBrokerageDriver
{
	int iStatus;
} *PMsgBrokerageDriver;

#endif // COMMON_STRUCTS_H
