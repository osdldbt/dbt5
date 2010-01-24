/*
 * TxnHarnessSendToMarket.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 30 July 2006
 */

#include <transactions.h>

using namespace TPCE;

char* addr = "localhost";

CSendToMarket::CSendToMarket(ofstream* pfile, int MEport)
: m_pfLog(pfile), m_MEport(MEport)
{
	m_Socket = new CSocket(addr, m_MEport);
	m_Socket->Connect();
}

CSendToMarket::~CSendToMarket()
{
	m_Socket->CloseAccSocket();
	delete m_Socket;
}

bool CSendToMarket::SendToMarket(TTradeRequest &trade_mes)
{
	try
	{
		// send Trade Request to MEE
		m_Socket->Send(reinterpret_cast<void*>(&trade_mes),
				sizeof(TTradeRequest));
	}
	catch(CSocketErr *pErr)
	{
		m_Socket->CloseAccSocket();	// close connection

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
