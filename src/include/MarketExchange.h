/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * This class represents the Market Exchange driver
 * 30 July 2006
 */

#ifndef MARKET_EXCHANGE_H
#define MARKET_EXCHANGE_H

#include "EGenLogFormatterTab.h"
#include "EGenLogger.h"
#include "locking.h"
#include "condition.h"

#include "CSocket.h"
#include "MEESUT.h"
using namespace TPCE;

class CMarketExchange
{
private:
	UINT32 m_UniqueId;
	int m_iListenPort;
	CSocket m_Socket;
	CLogFormatTab m_fmt;
	CEGenLogger *m_pLog;
	CMEESUT *m_pCMEESUT;
	CSecurityFile *m_pSecurities;
	bool m_Verbose;

	// Fire pending MEE timers (deferred Trade-Result and triggered
	// Market-Feed processing) when they expire without another trade
	// request arriving.
	CMutex m_TimerLock;
	CCondition m_TimerCond;
	INT32 m_NextTimerDelay; // ms until the next MEE timer, < 0 if none
	unsigned int m_TimerGeneration;
	bool m_TimerShutdown;
	pthread_t m_TimerThreadId;

	void updateNextTimer(INT32 delay);

	friend void *MarketWorkerThread(void *);
	// entry point for driver worker thread
	friend void EntryMarketWorkerThread(void *);
	friend void *MarketTimerThread(void *);

public:
	CMEE *m_pCMEE;

	CMarketExchange(const DataFileManager &, char *, UINT32, TIdent, TIdent,
			int, char *, int, char *, bool);
	~CMarketExchange();

	void startListener(void);
	bool verbose();
};

// parameter structure for the threads
typedef struct TMarketThreadParam
{
	CMarketExchange *pMarketExchange;
	int iSockfd;
} *PMarketThreadParam;

#endif // MARKET_EXCHANGE_H
