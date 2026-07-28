/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 30 July 2006
 */

#include "TxnHarnessSendToMarket.h"

CSendToMarket::CSendToMarket(
		ofstream *pfile, char *addr, int MEport = iMarketExchangePort)
: m_pfLog(pfile), m_MEport(MEport)
{
	if (addr != NULL)
		m_Socket = new CSocket(addr, m_MEport);
	else
		m_Socket = new CSocket((char *) "localhost", m_MEport);
	m_Socket->dbt5Connect();
}

CSendToMarket::~CSendToMarket()
{
	m_Socket->dbt5Disconnect();
	delete m_Socket;
}

bool
CSendToMarket::SendToMarket(TTradeRequest &trade_mes)
{
	try {
		// send Trade Request to MEE
		m_Socket->dbt5Send(
				reinterpret_cast<void *>(&trade_mes), sizeof(TTradeRequest));
	} catch (CSocketErr *pErr) {
		ostringstream osErr;
		osErr << "Cannot send to market" << endl
			  << "Error: " << pErr->ErrorText()
			  << " at CSendToMarket::SendToMarket" << endl;
		delete pErr;
		LogErrorMessage(osErr.str());

		// Reconnect and retry once so a single failure does not lose
		// the market connection for the rest of the run.  If this also
		// fails, the closed socket sends the next call back through
		// this reconnect path.
		try {
			m_Socket->dbt5Reconnect();
			m_Socket->dbt5Send(reinterpret_cast<void *>(&trade_mes),
					sizeof(TTradeRequest));
		} catch (CSocketErr *pErr2) {
			m_Socket->dbt5Disconnect(); // close connection

			ostringstream osErr2;
			osErr2 << "Cannot send to market after reconnecting" << endl
				   << "Error: " << pErr2->ErrorText()
				   << " at CSendToMarket::SendToMarket" << endl;
			delete pErr2;
			LogErrorMessage(osErr2.str());
			return false;
		}
	}

	return true;
}

// LogErrorMessage
void
CSendToMarket::LogErrorMessage(const string sErr)
{
	m_LogLock.lock();
	cout << sErr;
	*(m_pfLog) << sErr;
	m_pfLog->flush();
	m_LogLock.unlock();
}
