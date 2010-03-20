/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *               2010 Mark Wong
 *
 * This class represents the Brokerage House
 * 25 July 2006
 */

#ifndef BROKERAGE_HOUSE_H
#define BROKERAGE_HOUSE_H

#include <fstream>
using namespace std;

#include "locking.h"
#include "TxnHarnessStructs.h"
#include "TxnHarnessBrokerVolume.h"
#include "TxnHarnessCustomerPosition.h"
#include "TxnHarnessDataMaintenance.h"
#include "TxnHarnessMarketFeed.h"
#include "TxnHarnessMarketWatch.h"
#include "TxnHarnessSecurityDetail.h"
#include "TxnHarnessTradeCleanup.h"
#include "TxnHarnessTradeLookup.h"
#include "TxnHarnessTradeOrder.h"
#include "TxnHarnessTradeResult.h"
#include "TxnHarnessTradeStatus.h"
#include "TxnHarnessTradeUpdate.h"

#include "DBT5Consts.h"
#include "CSocket.h"
using namespace TPCE;

class CBrokerageHouse
{
private:
	int m_iListenPort;
	CSocket m_Socket;
	CMutex m_LogLock;
	ofstream m_fLog;

	char m_szHost[iMaxHostname + 1]; // host name
	char m_szDBName[iMaxDBName + 1]; // database name
	char m_szDBPort[iMaxPort + 1]; // PostgreSQL postmaster port

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

	friend void entryWorkerThread(void *); // entry point for worker thread

	void dumpInputData(PBrokerVolumeTxnInput);
	void dumpInputData(PCustomerPositionTxnInput);
	void dumpInputData(PDataMaintenanceTxnInput);
	void dumpInputData(PTradeCleanupTxnInput);
	void dumpInputData(PMarketWatchTxnInput);
	void dumpInputData(PMarketFeedTxnInput);
	void dumpInputData(PSecurityDetailTxnInput);
	void dumpInputData(PTradeStatusTxnInput);
	void dumpInputData(PTradeLookupTxnInput);
	void dumpInputData(PTradeOrderTxnInput);
	void dumpInputData(PTradeResultTxnInput);
	void dumpInputData(PTradeUpdateTxnInput);

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

	friend void *workerThread(void *);

public:
	CBrokerageHouse(const char *, const char *, const char *, const int,
				char *);
	~CBrokerageHouse();

	void logErrorMessage(const string sErr, bool bScreen = true);

	void startListener(void);
};

//parameter structure for the threads
typedef struct TThreadParameter
{
	CBrokerageHouse* pBrokerageHouse;
	int iSockfd;
} *PThreadParameter;

#endif // BROKERAGE_HOUSE_H
