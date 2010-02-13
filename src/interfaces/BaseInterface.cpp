/*
 * BaseInterface.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 13 August 2006
 */

#include <transactions.h>

using namespace TPCE;

// constructor
CBaseInterface::CBaseInterface(char* addr, const int iListenPort,
		ofstream* pflog, ofstream* pfmix, CMutex* pLogLock,
		CMutex* pMixLock)
: m_szBHAddress(addr),
  m_iBHlistenPort(iListenPort),
  m_pLogLock(pLogLock),
  m_pMixLock(pMixLock),
  m_pfLog(pflog),
  m_pfMix(pfmix)
{
	sock = new CSocket(m_szBHAddress, m_iBHlistenPort);
	Connect();
}

// destructor
CBaseInterface::~CBaseInterface()
{
	Disconnect();
	delete sock;
}

// connect to BrokerageHouse
void CBaseInterface::Connect()
{
	try
	{
		sock->Connect();
	}
	catch(CSocketErr *pErr)
	{
		ostringstream osErr;
		osErr << "Error: " << pErr->ErrorText() << " at " <<
				"CBaseInterface::TalkToSUT " << __FILE__ << ":" << __LINE__ <<
				endl;
		LogErrorMessage(osErr.str());
	}
}

// close connection to BrokerageHouse
void CBaseInterface::Disconnect()
{
	try
	{
		sock->CloseAccSocket();
	}
	catch(CSocketErr *pErr)
	{
		ostringstream osErr;
		osErr << "Error: " << pErr->ErrorText() << " at " <<
				"CBaseInterface::TalkToSUT " << __FILE__ << ":" << __LINE__ <<
				endl;
		LogErrorMessage(osErr.str());
	}
}

// Connect to BrokerageHouse, send request, receive reply, and calculate RT
void CBaseInterface::TalkToSUT(PMsgDriverBrokerage pRequest)
{
	int length;
	TMsgBrokerageDriver Reply;	// reply message from BrokerageHouse
	memset(&Reply, 0, sizeof(Reply)); 

	CDateTime	StartTime, EndTime, TxnTime;	// to time the transaction

	// record txn start time -- please, see TPC-E specification clause
	// 6.2.1.3
	StartTime.SetToCurrent();

	// send and wait for response
	try
	{
		length = sock->Send(reinterpret_cast<void*>(pRequest),
				sizeof(*pRequest));
	}
	catch(CSocketErr *pErr)
	{
		sock->CloseAccSocket(); // close connection
		LogResponseTime(-1, 0, 0);

		ostringstream osErr;
		osErr << "Error sending ("<< length << ") txn " <<
				pRequest->TxnType << ": " << pErr->ErrorText() << " at " <<
				"CBaseInterface::TalkToSUT " << __FILE__ << ":" << __LINE__ <<
				endl;
		LogErrorMessage(osErr.str());
		delete pErr;
	}
	try
	{
		length = sock->Receive(reinterpret_cast<void*>(&Reply), sizeof(Reply));
	}
	catch(CSocketErr *pErr)
	{
		sock->CloseAccSocket(); // close connection
		LogResponseTime(-1, 0, 0);

		ostringstream osErr;
		osErr << "Error sending ("<< length << ") txn " <<
				pRequest->TxnType << ": " << pErr->ErrorText() << " at " <<
				"CBaseInterface::TalkToSUT " << __FILE__ << ":" << __LINE__ <<
				endl;
		LogErrorMessage(osErr.str());
		delete pErr;
	}

	// record txn end time
	EndTime.SetToCurrent();

	// calculate txn response time
	TxnTime.Set(0);	// clear time
	TxnTime.Add(0, (int)((EndTime - StartTime) * MsPerSecond));	// add ms

	//log response time
	LogResponseTime(Reply.iStatus, pRequest->TxnType, (TxnTime.MSec()/1000.0));
	delete pRequest;
}

// Log Response Time
void CBaseInterface::LogResponseTime(int iStatus, int iTxnType, double dRT)
{
	// Errors:
	// CBaseTxnErr::SUCCESS
	// CBaseTxnErr::ROLLBACK (trade-order)
	// CBaseTxnErr::UNAUTHORIZED_EXECUTOR (trade-order)
	// ERR_TYPE_PQXX
	// ERR_TYPE_WRONGTXN

	// logging
	m_pMixLock->lock();
	if (iStatus == CBaseTxnErr::SUCCESS)
	{
		*(m_pfMix) << (int) time(NULL) << "," << iTxnType << "," << dRT <<
				"," << (int) pthread_self() << endl;
	}
	else if (iStatus == CBaseTxnErr::ROLLBACK)
	{
		*(m_pfMix) << (int) time(NULL) << "," << iTxnType << "R" << "," <<
				dRT << "," << (int) pthread_self() << endl;
	}
	else
	{
		*(m_pfMix) << (int) time(NULL) << "," << "E" << "," << dRT << "," <<
				(int) pthread_self() << endl;
	}
	m_pfMix->flush();
	m_pMixLock->unlock();
}

// LogErrorMessage
void CBaseInterface::LogErrorMessage( const string sErr )
{
	m_pLogLock->lock();
	cout<<sErr;
	*(m_pfLog)<<sErr;
	m_pfLog->flush();
	m_pLogLock->unlock();
}
