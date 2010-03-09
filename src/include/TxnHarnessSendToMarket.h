/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *
 * 06 July 2006
 */

#ifndef TXN_HARNESS_SENDTOMARKET_H
#define TXN_HARNESS_SENDTOMARKET_H

#include "locking.h"

class CSendToMarket : public CSendToMarketInterface
{
	ofstream*	m_pfLog;
	int		m_MEport;
	CSocket		*m_Socket;
	CMutex	m_LogLock;

public:
	void LogErrorMessage(const string sErr);

	CSendToMarket(ofstream* pfile, int MEport = MarketExchangePort);
	~CSendToMarket();

	virtual bool SendToMarket(TTradeRequest &trade_mes);
};

#endif	// TXN_HARNESS_SENDTOMARKET_H
