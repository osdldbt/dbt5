/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *               2010 Mark Wong
 *
 * 30 July 2006
 */

#include "transactions.h"

CSendToMarket::CSendToMarket(ofstream* pfile, int MEport)
: m_pfLog(pfile), m_MEport(MEport)
{
	// FIXME: This addr needs to be configurable.
	strncpy(addr, "localhost", iMaxHostname);
	m_Socket = new CSocket(addr, m_MEport);
	m_Socket->dbt5Connect();
}

CSendToMarket::~CSendToMarket()
{
	m_Socket->closeAccSocket();
	delete m_Socket;
}

bool CSendToMarket::SendToMarket(TTradeRequest &trade_mes)
{
	try
	{
		// send Trade Request to MEE
		m_Socket->dbt5Send(reinterpret_cast<void *>(&trade_mes),
				sizeof(TTradeRequest));
	}
	catch(CSocketErr *pErr)
	{
		m_Socket->closeAccSocket();	// close connection

		ostringstream osErr;
		osErr<<endl<<"Cannot send to market"<<endl
		     <<"Error: "<<pErr->ErrorText()
		     <<" at "<<"CSendToMarket::SendToMarket"<<endl;
		delete pErr;
		LogErrorMessage(osErr.str());
		return false;
	}	

	return true;
}

// LogErrorMessage
void CSendToMarket::LogErrorMessage( const string sErr )
{
	m_LogLock.lock();
	cout<<sErr;
	*(m_pfLog)<<sErr;
	m_pfLog->flush();
	m_LogLock.unlock();
}
