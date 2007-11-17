/*
 *  Shared structs and containers
 *  2006 Rilson Nascimento
 *
 * 29 July 2006
 */

#ifndef COMMON_STRUCTS_H
#define COMMON_STRUCTS_H

namespace TPCE
{

// Transaction types
enum eTxnType
{
	TRADE_ORDER = 0,
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

} // namespace TPCE

#endif	//COMMON_STRUCTS_H
