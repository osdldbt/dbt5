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

	friend void *marketWorkerThread(void *);
	// entry point for driver worker thread
	friend void entryMarketWorkerThread(void *);

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
