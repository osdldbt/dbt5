/*
 * MEESUTtest.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 23 July 2006
 */

#include <transactions.h>
#include <MEESUTtest.h>
#include <TxnHarnessSendToMarketTest.h>

using namespace TPCE;

CMEESUTtest::CMEESUTtest(CDBConnection *pDBConn)
: m_pDBConnection(pDBConn)
{
}

CMEESUTtest::~CMEESUTtest()
{
}

// Trade Result
//
//
void* TPCE::TradeResultAsync(void* data)
{
	CMEESUTtest* pCMEESUTtest = reinterpret_cast<CMEESUTtest*>(data);

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
	
	m_TradeResult.DoTxn( &(pCMEESUTtest->m_TradeResultTxnInput), &m_TradeResultTxnOutput); // Perform Trade Result

	delete m_pConn;
	return NULL;
}

bool TPCE::RunTradeResultAsync( CMEESUTtest* pCMEESUTtest )
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
						reinterpret_cast<void*>( pCMEESUTtest ));
	cout<<"thread id="<<threadID<<endl;
	if (status != 0)
	{
		cout<<"pthread_create failed, status = "<<status<<endl;
		return false;
	}

	// return immediatelly
	return true;	

}

bool CMEESUTtest::TradeResult( PTradeResultTxnInput pTxnInput )
{
	memcpy(&m_TradeResultTxnInput, pTxnInput, sizeof(m_TradeResultTxnInput));
	
	return ( RunTradeResultAsync( this ) );
}

// Market Feed
//
//
void* TPCE::MarketFeedAsync(void* data)
{
	CMEESUTtest* pCMEESUTtest = reinterpret_cast<CMEESUTtest*>(data);
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
	
	m_MarketFeed.DoTxn( &(pCMEESUTtest->m_MarketFeedTxnInput), &m_MarketFeedTxnOutput); // Perform Market Feed

	delete m_pConn;
	return NULL;

}

bool TPCE::RunMarketFeedAsync( CMEESUTtest* pCMEESUTtest )
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
						reinterpret_cast<void*>( pCMEESUTtest ));
	cout<<"thread id="<<threadID<<endl;
	if (status != 0)
	{
		cout<<"pthread_create failed, status = "<<status<<endl;
		return false;
	}

	// return immediatelly
	return true;	

}

bool CMEESUTtest::MarketFeed( PMarketFeedTxnInput pTxnInput )
{
	memcpy(&m_MarketFeedTxnInput, pTxnInput, sizeof(m_MarketFeedTxnInput));
	
	return ( RunMarketFeedAsync( this ) );
}
