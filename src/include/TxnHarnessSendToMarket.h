/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2010 Rilson Nascimento
 *
 * 06 July 2006
 */

#ifndef TXN_HARNESS_SENDTOMARKET_H
#define TXN_HARNESS_SENDTOMARKET_H

#include "TxnHarnessSendToMarketInterface.h"
#include "locking.h"

#include "DBT5Consts.h"
#include "CSocket.h"

class CSendToMarket : public CSendToMarketInterface
{
	ofstream *m_pfLog;
	int m_MEport;
	CSocket *m_Socket;
	CMutex m_LogLock;

public:
	void LogErrorMessage(const string);

	CSendToMarket(ofstream* pfile, int MEport = iMarketExchangePort);
	~CSendToMarket();

	virtual bool SendToMarket(TTradeRequest &);
};

#endif	// TXN_HARNESS_SENDTOMARKET_H
