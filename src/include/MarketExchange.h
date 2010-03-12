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

#include "locking.h"

class CMarketExchange
{
private:
	int m_iListenPort;
	CSocket m_Socket;
	CLogFormatTab m_fmt;
	CEGenLogger *m_pLog;
	CMEESUT *m_pCMEESUT;
	CSecurityFile *m_pSecurities;
	CMEE *m_pCMEE;
	CMutex m_LogLock;
	CMutex m_MixLock;
	ofstream m_fLog; // error log file
	ofstream m_fMix; // mix log file

	void LogErrorMessage(const string sErr);

	friend void *MarketWorkerThread(void* data);
	// entry point for driver worker thread
	friend void EntryMarketWorkerThread(void* data);

public:
	CMarketExchange(char *, TIdent, TIdent, int, char *, int, char *);
	~CMarketExchange();

	void Listener(void);
};

//parameter structure for the threads
typedef struct TMarketThreadParam
{
	CMarketExchange *pMarketExchange;
	int iSockfd;
} *PMarketThreadParam;

#endif // MARKET_EXCHANGE_H
