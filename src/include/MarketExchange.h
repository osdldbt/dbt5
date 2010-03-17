/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *               2010 Mark Wong
 *
 * This class represents the Market Exchange driver
 * 30 July 2006
 */

#ifndef MARKET_EXCHANGE_H
#define MARKET_EXCHANGE_H

#include "EGenLogFormatterTab.h"
#include "EGenLogger.h"
#include "SecurityFile.h"
#include "locking.h"

#include "CSocket.h"
#include "MEESUT.h"
using namespace TPCE;

class CMarketExchange
{
private:
	int m_iListenPort;
	CSocket m_Socket;
	CLogFormatTab m_fmt;
	CEGenLogger *m_pLog;
	CMEESUT *m_pCMEESUT;
	CSecurityFile *m_pSecurities;
	CMutex m_LogLock;
	CMutex m_MixLock;
	ofstream m_fLog; // error log file
	ofstream m_fMix; // mix log file

	friend void *marketWorkerThread(void *);
	// entry point for driver worker thread
	friend void entryMarketWorkerThread(void *);

public:
	CMEE *m_pCMEE;

	CMarketExchange(char *, TIdent, TIdent, int, char *, int, char *);
	~CMarketExchange();

	void logErrorMessage(const string);
	void startListener(void);
};

//parameter structure for the threads
typedef struct TMarketThreadParam
{
	CMarketExchange *pMarketExchange;
	int iSockfd;
} *PMarketThreadParam;

#endif // MARKET_EXCHANGE_H
