/*
 * CESUT.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 29 July 2006
 */

#include <transactions.h>

char* addr = "localhost";

using namespace TPCE;

// Constructor
CCESUT::CCESUT(const int iListenPort, ofstream* pflog, ofstream* pfmix)
: m_iBHlistenPort(iListenPort),
  m_pfLog(pflog),
  m_pfMix(pfmix)
{
}

// Destructor
CCESUT::~CCESUT()
{
}

// Connect and Send to Brokerage House
void CCESUT::ConnectRunTxnAndLog()
{
	TMsgBrokerageDriver Reply;	// reply message from BrokerageHouse
	memset(&Reply, 0, sizeof(Reply)); 

	CDateTime	StartTime, EndTime, TxnTime;	// to time the transaction

	try
	{
		// connect to BrokerageHouse
		m_Socket.Connect(addr, m_iBHlistenPort);
	
		// record txn start time -- please, see TPC-E specification clause 6.2.1.3
		StartTime.SetToCurrent();
	
		// send and wait for response
		m_Socket.Send(reinterpret_cast<void*>(&m_Request), sizeof(TMsgDriverBrokerage));
		m_Socket.Receive( reinterpret_cast<void*>(&Reply), sizeof(Reply));

		// record txn end time
		EndTime.SetToCurrent();

		// close connection
		m_Socket.CloseAccSocket();
	
		// calculate txn response time
		TxnTime.Set(0);	// clear time
		TxnTime.Add(0, (int)((EndTime - StartTime) * MsPerSecond));	// add ms

		//cout<<"TxnType = "<<m_Request.TxnType<<"\tTxn RT = "<<TxnTime.ToStr(02)<<endl;

		//	CBaseTxnErr::SUCCESS
		//	CBaseTxnErr::ROLLBACK (trade-order)
		//	CBaseTxnErr::UNAUTHORIZED_EXECUTOR (trade-order)
		// 	PQXX ERROR, WRONGTXN
		// see CBrokerageHouse::WorkerThread

		// logging
		m_MixLock.ClaimLock();
		if (Reply.iStatus == CBaseTxnErr::SUCCESS)
		{
			*(m_pfMix)<<(int)time(NULL)<<","<<m_Request.TxnType<< ","<<(TxnTime.MSec()/1000.0)<<","<<(int)pthread_self()<<endl;
		}
		else if (Reply.iStatus == CBaseTxnErr::ROLLBACK)
		{
			*(m_pfMix)<<(int)time(NULL)<<","<<m_Request.TxnType<<"R"<<","<<(TxnTime.MSec()/1000.0)<<","<<(int)pthread_self()<<endl;
		}
		else
		{
			*(m_pfMix)<<(int)time(NULL)<<","<<"E"<<","<<(TxnTime.MSec()/1000.0)<<","<<(int)pthread_self()<<endl;
		}
		m_pfMix->flush();
		m_MixLock.ReleaseLock();
		
	}
	catch(CSocketErr *pErr)
	{
		m_Socket.CloseAccSocket(); // close connection
		*(m_pfMix)<<(int)time(NULL)<<","<<"E"<<","<<"000"<<","<<(int)pthread_self()<<endl;

		ostringstream osErr;
		osErr<<endl<<"Error: "<<pErr->ErrorText()
		     <<" at "<<"CCESUT::ConnectRunTxnAndLog"<<endl;
		LogErrorMessage(osErr.str());
		delete pErr;
	}
}

// Broker Volume
bool CCESUT::BrokerVolume( PBrokerVolumeTxnInput pTxnInput )
{
	cout<<"Broker Volume requested"<<endl;
	return ( RunTxn(BROKER_VOLUME, &m_Request.TxnInput.BrokerVolumeTxnInput, pTxnInput) );
}

// Customer Position
bool CCESUT::CustomerPosition( PCustomerPositionTxnInput pTxnInput )
{
	cout<<"Customer Position requested"<<endl;
	return ( RunTxn(CUSTOMER_POSITION, &m_Request.TxnInput.CustomerPositionTxnInput, pTxnInput) );
}

// Market Watch
bool CCESUT::MarketWatch( PMarketWatchTxnInput pTxnInput )
{
	cout<<"Market Watch requested"<<endl;
	return ( RunTxn(MARKET_WATCH, &m_Request.TxnInput.MarketWatchTxnInput, pTxnInput) );
}

// Security Detail
bool CCESUT::SecurityDetail( PSecurityDetailTxnInput pTxnInput )
{
	cout<<"Security Detail requested"<<endl;
	return ( RunTxn(SECURITY_DETAIL, &m_Request.TxnInput.SecurityDetailTxnInput, pTxnInput) );
}

// Trade Lookup
bool CCESUT::TradeLookup( PTradeLookupTxnInput pTxnInput )
{
	cout<<"Trade Lookup requested"<<endl;
	return ( RunTxn(TRADE_LOOKUP, &m_Request.TxnInput.TradeLookupTxnInput, pTxnInput) );
}

// Trade Status
bool CCESUT::TradeStatus( PTradeStatusTxnInput pTxnInput )
{
	cout<<"Trade Status requested"<<endl;
	return ( RunTxn(TRADE_STATUS, &m_Request.TxnInput.TradeStatusTxnInput, pTxnInput) );
}

// Trade Order
bool CCESUT::TradeOrder( PTradeOrderTxnInput pTxnInput, INT32 iTradeType, bool bExecutorIsAccountOwner )
{
	cout<<"Trade Order requested"<<endl;
	return ( RunTxn(TRADE_ORDER, &m_Request.TxnInput.TradeOrderTxnInput, pTxnInput) );
}

// Trade Update
bool CCESUT::TradeUpdate( PTradeUpdateTxnInput pTxnInput )
{
	cout<<"Trade Update requested"<<endl;
	return ( RunTxn(TRADE_UPDATE, &m_Request.TxnInput.TradeUpdateTxnInput, pTxnInput) );
}

// LogErrorMessage
void CCESUT::LogErrorMessage( const string sErr )
{
	m_LogLock.ClaimLock();
	cout<<sErr;
	*(m_pfLog)<<sErr;
	m_pfLog->flush();
	m_LogLock.ReleaseLock();
}
