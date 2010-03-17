/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2010 Rilson Nascimento
 *
 * MEE (Market Exchange Emulator) - SUT Interface class
 * 30 July 2006
 */

#ifndef MEE_SUT_H
#define MEE_SUT_H

#include "MEESUTInterface.h"
#include "locking.h"
#include "MEE.h"

#include "BaseInterface.h"
using namespace TPCE;

class CMEESUT : public CMEESUTInterface, public CBaseInterface
{
private:
	TTradeResultTxnInput m_TradeResultTxnInput;
	TMarketFeedTxnInput m_MarketFeedTxnInput;

public:
	CMEESUT(char *addr, const int iListenPort, ofstream *pflog, ofstream *pfmix,
			CMutex *pLogLock, CMutex *pMixLock) :
			CBaseInterface(addr, iListenPort, pflog, pfmix, pLogLock,
			pMixLock) {};
	~CMEESUT() {};

	// return whether it was successful
	virtual bool TradeResult(PTradeResultTxnInput);
	// return whether it was successful
	virtual bool MarketFeed(PMarketFeedTxnInput);

	friend void *TradeResultAsync(void *);
	friend bool RunTradeResultAsync(void *);

	friend void *MarketFeedAsync(void *);
	friend bool RunMarketFeedAsync(void *);
};

//parameter structure for the threads
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
