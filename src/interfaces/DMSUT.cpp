/*
 * DMSUT.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 12 August 2006
 */

#include <transactions.h>

char* addr2 = "localhost";

using namespace TPCE;

// constructor
CDMSUT::CDMSUT(const int iListenPort, ofstream* pflog, ofstream* pfmix, CSyncLock* pLogLock, CSyncLock* pMixLock)
: m_iBHlistenPort(iListenPort),
  m_pLogLock(pLogLock),
  m_pMixLock(pMixLock),
  m_pfLog(pflog),
  m_pfMix(pfmix)
{
}

// destructor
CDMSUT::~CDMSUT()
{
}

// Connect, send to, and receive reply from, Brokerage House & logging
void CDMSUT::ConnectRunTxnAndLog(PMsgDriverBrokerage pRequest)
{
	TMsgBrokerageDriver Reply;	// reply message from BrokerageHouse
	memset(&Reply, 0, sizeof(Reply)); 

	CDateTime	StartTime, EndTime, TxnTime;	// to time the transaction
	CSocket		sock;

	try
	{
		// connect to BrokerageHouse
		sock.Connect(addr2, m_iBHlistenPort);
	
		// record txn start time -- please, see TPC-E specification clause 6.2.1.3
		StartTime.SetToCurrent();
	
		// send and wait for response
		sock.Send(reinterpret_cast<void*>(pRequest), sizeof(TMsgDriverBrokerage));
		sock.Receive( reinterpret_cast<void*>(&Reply), sizeof(Reply));

		// record txn end time
		EndTime.SetToCurrent();

		// close connection
		sock.CloseAccSocket();
	
		// calculate txn response time
		TxnTime.Set(0);	// clear time
		TxnTime.Add(0, (int)((EndTime - StartTime) * MsPerSecond));	// add ms

		// Errors:
		// CBaseTxnErr::SUCCESS
		// CBaseTxnErr::ROLLBACK (trade-order)
		// CBaseTxnErr::UNAUTHORIZED_EXECUTOR (trade-order)
		// ERR_TYPE_PQXX
		// ERR_TYPE_WRONGTXN

		// logging
		m_pMixLock->ClaimLock();
		if (Reply.iStatus == CBaseTxnErr::SUCCESS)
		{
			*(m_pfMix)<<(int)time(NULL)<<","<<pRequest->TxnType<< ","<<(TxnTime.MSec()/1000.0)<<","<<(int)pthread_self()<<endl;
		}
		else if (Reply.iStatus == CBaseTxnErr::ROLLBACK)
		{
			*(m_pfMix)<<(int)time(NULL)<<","<<pRequest->TxnType<<"R"<<","<<(TxnTime.MSec()/1000.0)<<","<<(int)pthread_self()<<endl;
		}
		else
		{
			*(m_pfMix)<<(int)time(NULL)<<","<<"E"<<","<<(TxnTime.MSec()/1000.0)<<","<<(int)pthread_self()<<endl;
		}
		m_pfMix->flush();
		m_pMixLock->ReleaseLock();
		
	}
	catch(CSocketErr *pErr)
	{
		sock.CloseAccSocket(); // close connection
		*(m_pfMix)<<(int)time(NULL)<<","<<"E"<<","<<"000"<<","<<(int)pthread_self()<<endl;

		ostringstream osErr;
		osErr<<endl<<"Error: "<<pErr->ErrorText()
		     <<" at "<<"CCESUT::ConnectRunTxnAndLog"<<endl;
		LogErrorMessage(osErr.str());
		delete pErr;
	}
}

// Data Maintenance
bool CDMSUT::DataMaintenance( PDataMaintenanceTxnInput pTxnInput )
{
	cout<<"Data Maintenance requested"<<endl;

	TMsgDriverBrokerage Request;
	memset(&Request, 0, sizeof(TMsgDriverBrokerage));

	Request.TxnType = DATA_MAINTENANCE;
	memcpy( &(Request.TxnInput.DataMaintenanceTxnInput), pTxnInput, sizeof( TDataMaintenanceTxnInput ));
	
	ConnectRunTxnAndLog(&Request);
	return true;
}

// Trade Cleanup
bool CDMSUT::TradeCleanup( PTradeCleanupTxnInput pTxnInput )
{
	cout<<"Trade Cleanup requested"<<endl;

	TMsgDriverBrokerage Request;
	memset(&Request, 0, sizeof(TMsgDriverBrokerage));

	Request.TxnType = TRADE_CLEANUP;
	memcpy( &(Request.TxnInput.TradeCleanupTxnInput), pTxnInput, sizeof( TTradeCleanupTxnInput ));
	
	ConnectRunTxnAndLog(&Request);
	return true;
}

// LogErrorMessage
void CDMSUT::LogErrorMessage( const string sErr )
{
	m_pLogLock->ClaimLock();
	cout<<sErr;
	*(m_pfLog)<<sErr;
	m_pfLog->flush();
	m_pLogLock->ReleaseLock();
}
