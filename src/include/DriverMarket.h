/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *
 * This class represents the Market Exchange driver
 * 30 July 2006
 */

#ifndef DRIVER_MARKET_H
#define DRIVER_MARKET_H

#include "locking.h"

using namespace TPCE;

class CDriverMarket
{
private:
	int			m_iListenPort;
	CSocket			m_Socket;
	CLogFormatTab		m_fmt;
	CEGenLogger*		m_pLog;
	CMEESUT*		m_pCMEESUT;
	CSecurityFile*		m_pSecurities;
	CMEE*			m_pCMEE;
	CMutex		m_LogLock;
	CMutex		m_MixLock;
	ofstream		m_fLog;		// error log file
	ofstream		m_fMix;		// mix log file

	void LogErrorMessage(const string sErr);

	friend void *MarketWorkerThread(void* data);
	// entry point for driver worker thread
	friend void EntryMarketWorkerThread(void* data);

public:
	CDriverMarket(char* szFileLoc, TIdent iConfiguredCustomerCount,
			TIdent iActiveCustomerCount, int iListenPort, char* szBHaddr,
			int iBHlistenPort, char *outputDirectory);
	~CDriverMarket();

	void Listener(void);
};

//parameter structure for the threads
typedef struct TMarketThreadParam
{
	CDriverMarket*		pDriverMarket;
	int			iSockfd;
} *PMarketThreadParam;

#endif	// DRIVER_MARKET_H
