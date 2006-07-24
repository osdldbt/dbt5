/*
 * MEESUT.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 23 July 2006
 */

#include <transactions.h>

using namespace TPCE;

CMEESUT::CMEESUT(CDBConnection *pDBConn)
: m_pDBConnection(pDBConn)
{
}

CMEESUT::~CMEESUT()
{
}

// Trade Result
//
//
void* TPCE::TradeResultAsync(void* data)
{
	CMEESUT* pCMEESUT = reinterpret_cast<CMEESUT*>(data);

	// creating separated connection for Market Feed (just for testing)
	const char *server = "localhost";
	const char *db = "dbt5";
	const char *port = "5432";
	CDBConnection*	m_pConn = new CDBConnection( server, db, port );

	// trade result harness code (TPC provided)
	// this class uses our implementation of CTradeResultDB class
	CTradeResult		m_TradeResult( m_pConn );

	// Market-Feed output parameters
	TTradeResultTxnOutput	m_TradeResultTxnOutput;
	
	m_TradeResult.DoTxn( &(pCMEESUT->m_TradeResultTxnInput), &m_TradeResultTxnOutput); // Perform Trade Result

	delete m_pConn;
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
//
void* TPCE::MarketFeedAsync(void* data)
{
	CMEESUT* pCMEESUT = reinterpret_cast<CMEESUT*>(data);
	CSendToMarketTest	m_SendToMarket;

	// creating separated connection for Market Feed (just for testing)
	const char *server = "localhost";
	const char *db = "dbt5";
	const char *port = "5432";
	CDBConnection*	m_pConn = new CDBConnection( server, db, port );

	// trade result harness code (TPC provided)
	// this class uses our implementation of CMarketFeedDB class
	CMarketFeed		m_MarketFeed( m_pConn, &m_SendToMarket );

	// Market-Feed output parameters
	TMarketFeedTxnOutput	m_MarketFeedTxnOutput;
	
	m_MarketFeed.DoTxn( &(pCMEESUT->m_MarketFeedTxnInput), &m_MarketFeedTxnOutput); // Perform Market Feed

	delete m_pConn;
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
