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
: m_pfLog(pfile),
  m_MEport(MEport)
{
}

CSendToMarket::~CSendToMarket()
{
}

bool CSendToMarket::SendToMarket(TTradeRequest &trade_mes)
{
	try
	{
		// connect to the Market Exchange
		m_Socket.Connect(addr, m_MEport);
	
		// send Trade Request to MEE
		m_Socket.Send(reinterpret_cast<void*>(&trade_mes), sizeof(TTradeRequest));
	
		// close connection
		m_Socket.CloseAccSocket();
	}
	catch(CSocketErr *pErr)
	{
		m_Socket.CloseAccSocket();	// close connection

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
	m_LogLock.ClaimLock();
	cout<<sErr;
	*(m_pfLog)<<sErr;
	m_pfLog->flush();
	m_LogLock.ReleaseLock();
}
