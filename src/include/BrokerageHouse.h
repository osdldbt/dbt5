/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *               20010 Mark Wong
 *
 * This class represents the Brokerage House
 * 25 July 2006
 */

#ifndef BROKERAGE_HOUSE_H
#define BROKERAGE_HOUSE_H

#include "DBT5Consts.h"
#include "locking.h"

class CBrokerageHouse
{
	int m_iListenPort;
	CSocket m_Socket;
	CMutex m_LogLock;
	ofstream m_fLog;

	char m_szHost[iMaxHostname + 1]; // host name
	char m_szDBName[iMaxDBName + 1]; // database name
	char m_szDBPort[iMaxPort + 1]; // PostgreSQL postmaster port

private:
	void logErrorMessage(const string sErr, bool bScreen = true);

	friend void *workerThread(void *);
	friend void entryWorkerThread(void *); // entry point for worker thread

	TBrokerVolumeTxnOutput bvOutput;
	TCustomerPositionTxnOutput cpOutput;
	TDataMaintenanceTxnOutput dmOutput;
	TTradeCleanupTxnOutput tcOutput;
	TMarketFeedTxnOutput mfOutput;
	TMarketWatchTxnOutput mwOutput;
	TSecurityDetailTxnOutput sdOutput;
	TTradeLookupTxnOutput tlOutput;
	TTradeOrderTxnOutput toOutput;
	TTradeResultTxnOutput trOutput;
	TTradeStatusTxnOutput tsOutput;
	TTradeUpdateTxnOutput tuOutput;

	INT32 RunBrokerVolume(PBrokerVolumeTxnInput pTxnInput,
			CBrokerVolume &BrokerVolume);
	INT32 RunCustomerPosition(PCustomerPositionTxnInput pTxnInput,
			CCustomerPosition &CustomerPosition);
	INT32 RunDataMaintenance(PDataMaintenanceTxnInput pTxnInput,
			CDataMaintenance &DataMaintenance);
	INT32 RunTradeCleanup(PTradeCleanupTxnInput pTxnInput,
			CTradeCleanup &TradeCleanup);
	INT32 RunMarketWatch(PMarketWatchTxnInput pTxnInput,
			CMarketWatch &MarketWatch);
	INT32 RunMarketFeed(PMarketFeedTxnInput pTxnInput,
			CMarketFeed &MarketFeed);
	INT32 RunSecurityDetail(PSecurityDetailTxnInput pTxnInput,
			CSecurityDetail &SecurityDetail);
	INT32 RunTradeStatus(PTradeStatusTxnInput pTxnInput,
			CTradeStatus &TradeStatus);
	INT32 RunTradeLookup(PTradeLookupTxnInput pTxnInput,
			CTradeLookup &TradeLookup);
	INT32 RunTradeOrder(PTradeOrderTxnInput pTxnInput,
			CTradeOrder &TradeOrder);
	INT32 RunTradeResult(PTradeResultTxnInput pTxnInput,
			CTradeResult &TradeResult);
	INT32 RunTradeUpdate(PTradeUpdateTxnInput pTxnInput,
			CTradeUpdate &TradeUpdate);

public:
	CBrokerageHouse(const char *, const char *, const char *, const int,
				char *);
	~CBrokerageHouse();

	void startListener(void);

};

//parameter structure for the threads
typedef struct TThreadParameter
{
	CBrokerageHouse* pBrokerageHouse;
	int iSockfd;
} *PThreadParameter;

#endif // BROKERAGE_HOUSE_H
