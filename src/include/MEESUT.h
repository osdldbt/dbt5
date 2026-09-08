/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * MEE (Market Exchange Emulator) - SUT Interface class
 * 30 July 2006
 */

#ifndef MEE_SUT_H
#define MEE_SUT_H

#include "MEESUTInterface.h"
#include "locking.h"
#include "condition.h"
#include "MEE.h"

#include <vector>

#include "BaseInterface.h"
using namespace TPCE;

// Connections from the Market Exchange to the Brokerage House.  Every
// Trade Result and Market Feed is a synchronous round trip on one of
// them that lasts as long as the Brokerage House's database transaction
// does, so a connection delivers at most one per that latency, and on
// a fast system one connection cannot keep up with the rate Trade
// Orders arrive at.  The Brokerage House serves each connection with a
// thread and a database connection of its own, which is why the number
// is fixed here rather than a connection opened per delivery.
const int iMEESUTConnections = 16;

class CMEESUT: public CMEESUTInterface, public CBaseInterface
{
private:
	TTradeResultTxnInput m_TradeResultTxnInput;
	TMarketFeedTxnInput m_MarketFeedTxnInput;

	// Number of detached transaction threads still running, so the
	// destructor can wait for them instead of freeing this object out
	// from under them.
	CMutex m_ThreadCountLock;
	CCondition m_ThreadCountCond;
	int m_OutstandingThreads;

	// The idle connections.  A delivery takes one from here and puts
	// it back when it is done, and a delivery that finds none waits
	// here until one comes back.
	CMutex m_PoolLock;
	CCondition m_PoolCond;
	std::vector<CSocket *> m_FreeSockets;
	// The connections opened here beyond the base class's own, for the
	// destructor to close.
	std::vector<CSocket *> m_ExtraSockets;

	void threadStarted();
	void threadFinished();
	CSocket *acquireSocket();
	void releaseSocket(CSocket *);

public:
	CMEESUT(char *, char *, const int, int = iMEESUTConnections);
	~CMEESUT();

	bool TradeResult(PTradeResultTxnInput);
	bool MarketFeed(PMarketFeedTxnInput);

	friend void *TradeResultAsync(void *);
	friend bool RunTradeResultAsync(void *);

	friend void *MarketFeedAsync(void *);
	friend bool RunMarketFeedAsync(void *);
};

// parameter structure for the threads
typedef struct TMEESUTThreadParam
{
	CMEESUT *pCMEESUT;

	union
	{
		TTradeResultTxnInput m_TradeResultTxnInput;
		TMarketFeedTxnInput m_MarketFeedTxnInput;
	} TxnInput;
} *PMEESUTThreadParam;

#endif // MEE_SUT_H
