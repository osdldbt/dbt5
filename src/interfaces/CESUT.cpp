/*
 * CESUT.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 12 August 2006
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

// Connect, send to, and receive reply from, Brokerage House & logging
void CCESUT::ConnectRunTxnAndLog(PMsgDriverBrokerage pRequest)
{
	TMsgBrokerageDriver Reply;	// reply message from BrokerageHouse
	memset(&Reply, 0, sizeof(Reply)); 

	CDateTime	StartTime, EndTime, TxnTime;	// to time the transaction
	CSocket		sock;

	try
	{
		// connect to BrokerageHouse
		sock.Connect(addr, m_iBHlistenPort);
	
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
		m_MixLock.ClaimLock();
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
		m_MixLock.ReleaseLock();
		
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

// Broker Volume
bool CCESUT::BrokerVolume( PBrokerVolumeTxnInput pTxnInput )
{
	cout<<"Broker Volume requested"<<endl;

	TMsgDriverBrokerage Request;
	memset(&Request, 0, sizeof(TMsgDriverBrokerage));

	Request.TxnType = BROKER_VOLUME;
	memcpy( &(Request.TxnInput.BrokerVolumeTxnInput), pTxnInput, sizeof( TBrokerVolumeTxnInput ));
	
	ConnectRunTxnAndLog(&Request);
	return true;
}

// Customer Position
bool CCESUT::CustomerPosition( PCustomerPositionTxnInput pTxnInput )
{
	cout<<"Customer Position requested"<<endl;

	TMsgDriverBrokerage Request;
	memset(&Request, 0, sizeof(TMsgDriverBrokerage));

	Request.TxnType = CUSTOMER_POSITION;
	memcpy( &(Request.TxnInput.CustomerPositionTxnInput), pTxnInput, sizeof( TCustomerPositionTxnInput ));
	
	ConnectRunTxnAndLog(&Request);
	return true;	
}

// Market Watch
bool CCESUT::MarketWatch( PMarketWatchTxnInput pTxnInput )
{
	cout<<"Market Watch requested"<<endl;

	TMsgDriverBrokerage Request;
	memset(&Request, 0, sizeof(TMsgDriverBrokerage));

	Request.TxnType = MARKET_WATCH;
	memcpy( &(Request.TxnInput.MarketWatchTxnInput), pTxnInput, sizeof( TMarketWatchTxnInput ));
	
	ConnectRunTxnAndLog(&Request);
	return true;
}

// Security Detail
bool CCESUT::SecurityDetail( PSecurityDetailTxnInput pTxnInput )
{
	cout<<"Security Detail requested"<<endl;

	TMsgDriverBrokerage Request;
	memset(&Request, 0, sizeof(TMsgDriverBrokerage));

	Request.TxnType = SECURITY_DETAIL;
	memcpy( &(Request.TxnInput.SecurityDetailTxnInput), pTxnInput, sizeof( TSecurityDetailTxnInput ));
	
	ConnectRunTxnAndLog(&Request);
	return true;
}

// Trade Lookup
bool CCESUT::TradeLookup( PTradeLookupTxnInput pTxnInput )
{
	cout<<"Trade Lookup requested"<<endl;

	TMsgDriverBrokerage Request;
	memset(&Request, 0, sizeof(TMsgDriverBrokerage));

	Request.TxnType = TRADE_LOOKUP;
	memcpy( &(Request.TxnInput.TradeLookupTxnInput), pTxnInput, sizeof( TTradeLookupTxnInput ));
	
	ConnectRunTxnAndLog(&Request);
	return true;
}

// Trade Status
bool CCESUT::TradeStatus( PTradeStatusTxnInput pTxnInput )
{
	cout<<"Trade Status requested"<<endl;

	TMsgDriverBrokerage Request;
	memset(&Request, 0, sizeof(TMsgDriverBrokerage));

	Request.TxnType = TRADE_STATUS;
	memcpy( &(Request.TxnInput.TradeStatusTxnInput), pTxnInput, sizeof( TTradeStatusTxnInput ));
	
	ConnectRunTxnAndLog(&Request);
	return true;
}

// Trade Order
bool CCESUT::TradeOrder( PTradeOrderTxnInput pTxnInput, INT32 iTradeType, bool bExecutorIsAccountOwner )
{
	cout<<"Trade Order requested"<<endl;

	TMsgDriverBrokerage Request;
	memset(&Request, 0, sizeof(TMsgDriverBrokerage));

	Request.TxnType = TRADE_ORDER;
	memcpy( &(Request.TxnInput.TradeOrderTxnInput), pTxnInput, sizeof( TTradeOrderTxnInput ));
	
	ConnectRunTxnAndLog(&Request);
	return true;
}

// Trade Update
bool CCESUT::TradeUpdate( PTradeUpdateTxnInput pTxnInput )
{
	cout<<"Trade Update requested"<<endl;

	TMsgDriverBrokerage Request;
	memset(&Request, 0, sizeof(TMsgDriverBrokerage));

	Request.TxnType = TRADE_UPDATE;
	memcpy( &(Request.TxnInput.TradeUpdateTxnInput), pTxnInput, sizeof( TTradeUpdateTxnInput ));
	
	ConnectRunTxnAndLog(&Request);
	return true;
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
