/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2010 Rilson Nascimento
 *
 * 23 July 2006
 */

#include "MEESUTtest.h"
#include "TxnHarnessSendToMarketTest.h"

#include "TxnHarnessMarketFeed.h"
#include "TxnHarnessTradeResult.h"

#include "MarketFeedDB.h"
#include "TradeResultDB.h"
using namespace TPCE;

void *TradeResultAsync(void* data)
{
	CMEESUTtest* pCMEESUTtest = reinterpret_cast<CMEESUTtest*>(data);

	// creating separated connection for Market Feed (just for testing)
	const char *server = "localhost";
	const char *db = "dbt5";
	const char *port = "5432";
	CDBConnection*	m_pConn = new CDBConnection( server, db, port );

	// trade result harness code (TPC provided)
	// this class uses our implementation of CTradeResultDB class
	CTradeResultDB		m_TradeResultDB( m_pConn );
	CTradeResult		m_TradeResult( &m_TradeResultDB );

	// Market-Feed output parameters
	TTradeResultTxnOutput	m_TradeResultTxnOutput;
	
	// Perform Trade Result
	m_TradeResult.DoTxn(&(pCMEESUTtest->m_TradeResultTxnInput),
			&m_TradeResultTxnOutput);

	delete m_pConn;
	return NULL;
}

bool RunTradeResultAsync( CMEESUTtest* pCMEESUTtest )
{
	pthread_t threadID; // thread ID
	int status; // error code
	pthread_attr_t threadAttribute; // thread attribute

	// initialize the attribute object
	status = pthread_attr_init(&threadAttribute);
	if (status != 0)
	{
		cout<<"pthread_attr_init failed, status = "<<status<<endl;
		return false;
	}

	// set the detachstate attribute to detached
	status = pthread_attr_setdetachstate(&threadAttribute,
			PTHREAD_CREATE_DETACHED);
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

//
// Market Feed
//
void *MarketFeedAsync(void* data)
{
	CMEESUTtest *pCMEESUTtest = reinterpret_cast<CMEESUTtest*>(data);
	CSendToMarketTest m_SendToMarket(pCMEESUTtest->iConfiguredCustomerCount,
			pCMEESUTtest->iActiveCustomerCount, pCMEESUTtest->szInDir);

	// creating separated connection for Market Feed (just for testing)
	const char *server = "localhost";
	const char *db = "dbt5";
	const char *port = "5432";
	CDBConnection*	m_pConn = new CDBConnection( server, db, port );

	// trade result harness code (TPC provided)
	// this class uses our implementation of CMarketFeedDB class
	CMarketFeedDB m_MarketFeedDB( m_pConn );
	CMarketFeed		m_MarketFeed( &m_MarketFeedDB, &m_SendToMarket );

	// Market-Feed output parameters
	TMarketFeedTxnOutput	m_MarketFeedTxnOutput;
	
	// Perform Market Feed
	m_MarketFeed.DoTxn( &(pCMEESUTtest->m_MarketFeedTxnInput),
			&m_MarketFeedTxnOutput);

	delete m_pConn;
	return NULL;

}

bool RunMarketFeedAsync( CMEESUTtest* pCMEESUTtest )
{
	pthread_t threadID; // thread ID
	int status; // error code
	pthread_attr_t threadAttribute; // thread attribute

	// initialize the attribute object
	status = pthread_attr_init(&threadAttribute);
	if (status != 0)
	{
		cout<<"pthread_attr_init failed, status = "<<status<<endl;
		return false;
	}

	// set the detachstate attribute to detached
	status = pthread_attr_setdetachstate(&threadAttribute,
			PTHREAD_CREATE_DETACHED);
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
