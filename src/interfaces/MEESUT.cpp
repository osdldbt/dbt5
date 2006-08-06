/*
 * MEESUT.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 30 July 2006
 */

#include <transactions.h>

char* addr = "localhost";

using namespace TPCE;

CMEESUT::CMEESUT(const int iBHlistenPort)
: m_iBHlistenPort(iBHlistenPort)
{
}

CMEESUT::~CMEESUT()
{
}

// Trade Result
//
void* TPCE::TradeResultAsync(void* data)
{
	CMEESUT* pCMEESUT = reinterpret_cast<CMEESUT*>(data);

	cout<<"Trade Result requested"<<endl;

	PMsgDriverBrokerage pRequest =  new TMsgDriverBrokerage;
	memset(pRequest, 0, sizeof(TMsgDriverBrokerage));

	pRequest->TxnType = TRADE_RESULT;
	memcpy( &(pRequest->TxnInput.TradeResultTxnInput), &(pCMEESUT->m_TradeResultTxnInput), 
								sizeof( pRequest->TxnInput.TradeResultTxnInput ));

	TMsgBrokerageDriver Reply;	// reply message from BrokerageHouse
	memset(&Reply, 0, sizeof(Reply)); 

	CDateTime	StartTime, EndTime, TxnTime;	// to time the transaction

	// connect to BrokerageHouse
	CSocket sockdrv;
	sockdrv.Connect(addr, pCMEESUT->m_iBHlistenPort);

	// record txn start time -- please, see TPC-E specification clause 6.2.1.3
	StartTime.SetToCurrent();

	// send and wait for response
	sockdrv.Send(reinterpret_cast<void*>(pRequest), sizeof(TMsgDriverBrokerage));
	sockdrv.Receive( reinterpret_cast<void*>(&Reply), sizeof(Reply));

	// record txn end time
	EndTime.SetToCurrent();

	// calculate txn response time
	TxnTime.Set(0);	// clear time
	TxnTime.Add(0, (int)((EndTime - StartTime) * MsPerSecond));	// add ms

	// close connection
	sockdrv.CloseAccSocket();

	cout<<"TxnType = "<<TRADE_RESULT<<"\tTxn RT = "<<TxnTime.ToStr(02)<<endl;

	delete pRequest;
	return NULL;
}

bool TPCE::RunTradeResultAsync( CMEESUT* pCMEESUT )
{
	pthread_t threadID; // thread ID
	int status; // error code
	pthread_attr_t threadAttribute; // thread attribute

	status = pthread_attr_init(&threadAttribute); // initialize the attribute object
	if (status != 0)
	{
		cout<<"pthread_attr_init failed, status = "<<status<<endl;
		return false;
	}

	// set the detachstate attribute to detached
	status = pthread_attr_setdetachstate(&threadAttribute, PTHREAD_CREATE_DETACHED);
	if (status != 0)
	{
		cout<<"pthread_attr_setdetachstate failed, status = "<<status<<endl;
		return false;
	}

	// create the thread in the detached state - Call Trade Result asyncronously
	status = pthread_create(&threadID, &threadAttribute, &TradeResultAsync,
						reinterpret_cast<void*>( pCMEESUT ));
	cout<<"thread id="<<threadID<<endl;
	if (status != 0)
	{
		cout<<"pthread_create failed, status = "<<status<<endl;
		return false;
	}

	// return immediatelly
	return true;	

}

bool CMEESUT::TradeResult( PTradeResultTxnInput pTxnInput )
{
	memcpy(&m_TradeResultTxnInput, pTxnInput, sizeof(m_TradeResultTxnInput));
	
	return ( RunTradeResultAsync( this ) );
}

// Market Feed
//
void* TPCE::MarketFeedAsync(void* data)
{
	CMEESUT* pCMEESUT = reinterpret_cast<CMEESUT*>(data);

	cout<<"Market Feed requested"<<endl;

	PMsgDriverBrokerage pRequest =  new TMsgDriverBrokerage;
	memset(pRequest, 0, sizeof(TMsgDriverBrokerage));

	pRequest->TxnType = MARKET_FEED;
	memcpy( &(pRequest->TxnInput.MarketFeedTxnInput), &(pCMEESUT->m_MarketFeedTxnInput), 
								sizeof( pRequest->TxnInput.MarketFeedTxnInput ));

	TMsgBrokerageDriver Reply;	// reply message from BrokerageHouse
	memset(&Reply, 0, sizeof(Reply)); 

	CDateTime	StartTime, EndTime, TxnTime;	// to time the transaction

	// connect to BrokerageHouse
	CSocket sockdrv;
	sockdrv.Connect(addr, BrokerageHousePort);

	// record txn start time -- please, see TPC-E specification clause 6.2.1.3
	StartTime.SetToCurrent();

	// send and wait for response
	sockdrv.Send(reinterpret_cast<void*>(pRequest), sizeof(TMsgDriverBrokerage));
	sockdrv.Receive( reinterpret_cast<void*>(&Reply), sizeof(Reply));

	// record txn end time
	EndTime.SetToCurrent();

	// calculate txn response time
	TxnTime.Set(0);	// clear time
	TxnTime.Add(0, (int)((EndTime - StartTime) * MsPerSecond));	// add ms

	// close connection
	sockdrv.CloseAccSocket();

	cout<<"TxnType = "<<MARKET_FEED<<"\tTxn RT = "<<TxnTime.ToStr(02)<<endl;

	delete pRequest;
	return NULL;
}

bool TPCE::RunMarketFeedAsync( CMEESUT* pCMEESUT )
{
	pthread_t threadID; // thread ID
	int status; // error code
	pthread_attr_t threadAttribute; // thread attribute

	status = pthread_attr_init(&threadAttribute); // initialize the attribute object
	if (status != 0)
	{
		cout<<"pthread_attr_init failed, status = "<<status<<endl;
		return false;
	}

	// set the detachstate attribute to detached
	status = pthread_attr_setdetachstate(&threadAttribute, PTHREAD_CREATE_DETACHED);
	if (status != 0)
	{
		cout<<"pthread_attr_setdetachstate failed, status = "<<status<<endl;
		return false;
	}

	// create the thread in the detached state - Call Trade Result asyncronously
	status = pthread_create(&threadID, &threadAttribute, &MarketFeedAsync,
						reinterpret_cast<void*>( pCMEESUT ));
	cout<<"thread id="<<threadID<<endl;
	if (status != 0)
	{
		cout<<"pthread_create failed, status = "<<status<<endl;
		return false;
	}

	// return immediatelly
	return true;	

}

bool CMEESUT::MarketFeed( PMarketFeedTxnInput pTxnInput )
{
	memcpy(&m_MarketFeedTxnInput, pTxnInput, sizeof(m_MarketFeedTxnInput));
	
	return ( RunMarketFeedAsync( this ) );
}
