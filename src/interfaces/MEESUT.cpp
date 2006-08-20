/*
 * MEESUT.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 30 July 2006
 */

#include <transactions.h>

using namespace TPCE;

CMEESUT::CMEESUT(char* addr, const int iListenPort, ofstream* pflog, ofstream* pfmix, 
			CSyncLock* pLogLock, CSyncLock* pMixLock)
: CBaseInterface(addr, iListenPort, pflog, pfmix, pLogLock, pMixLock)
{
}

CMEESUT::~CMEESUT()
{
}

// Trade Result
//
void* TPCE::TradeResultAsync(void* data)
{
	PMEESUTThreadParam pThrParam = reinterpret_cast<PMEESUTThreadParam>(data);

	cout<<"Trade Result requested"<<endl;

	PMsgDriverBrokerage pRequest =  new TMsgDriverBrokerage;
	memset(pRequest, 0, sizeof(TMsgDriverBrokerage));

	pRequest->TxnType = TRADE_RESULT;
	memcpy( &(pRequest->TxnInput.TradeResultTxnInput), &(pThrParam->TxnInput.m_TradeResultTxnInput), 
								sizeof( pRequest->TxnInput.TradeResultTxnInput ));

	// communicate with the SUT and log response time
	pThrParam->pCMEESUT->TalkToSUT(pRequest);

	delete pThrParam;
	return NULL;
}

bool TPCE::RunTradeResultAsync( void* data )
{
	PMEESUTThreadParam pThrParam = reinterpret_cast<PMEESUTThreadParam>(data);

	pthread_t threadID; // thread ID
	pthread_attr_t threadAttribute; // thread attribute

	try
	{
		int status = pthread_attr_init(&threadAttribute); // initialize the attribute object
		if (status != 0)
		{
			throw new CThreadErr( CThreadErr::ERR_THREAD_ATTR_INIT );
		}
	
		// set the detachstate attribute to detached
		status = pthread_attr_setdetachstate(&threadAttribute, PTHREAD_CREATE_DETACHED);
		if (status != 0)
		{
			throw new CThreadErr( CThreadErr::ERR_THREAD_ATTR_DETACH );
		}
	
		// create the thread in the detached state - Call Trade Result asyncronously
		status = pthread_create(&threadID, &threadAttribute, &TradeResultAsync, data);

		if (status != 0)
		{
			throw new CThreadErr( CThreadErr::ERR_THREAD_CREATE );
		}
	}
	catch(CThreadErr *pErr)
	{
		ostringstream osErr;
		osErr<<endl<<"Error: "<<pErr->ErrorText()
		     <<" at "<<"MEESUT::RunTradeResultAsync"<<endl;
		pThrParam->pCMEESUT->LogErrorMessage(osErr.str());
		delete pErr;
		return false;
	}

	// return immediatelly
	return true;	
}

bool CMEESUT::TradeResult( PTradeResultTxnInput pTxnInput )
{
	PMEESUTThreadParam pThrParam = new TMEESUTThreadParam;
	memset(pThrParam, 0, sizeof(TMEESUTThreadParam));

	pThrParam->pCMEESUT = this;
	memcpy(&(pThrParam->TxnInput.m_TradeResultTxnInput), pTxnInput, sizeof(TTradeResultTxnInput));

	return ( RunTradeResultAsync( reinterpret_cast<void*>(pThrParam) ) );
}

// Market Feed
//
void* TPCE::MarketFeedAsync(void* data)
{
	PMEESUTThreadParam pThrParam = reinterpret_cast<PMEESUTThreadParam>(data);

	cout<<"Market Feed requested"<<endl;

	PMsgDriverBrokerage pRequest =  new TMsgDriverBrokerage;
	memset(pRequest, 0, sizeof(TMsgDriverBrokerage));

	pRequest->TxnType = MARKET_FEED;
	memcpy( &(pRequest->TxnInput.MarketFeedTxnInput), &(pThrParam->TxnInput.m_MarketFeedTxnInput), 
								sizeof( pRequest->TxnInput.MarketFeedTxnInput ));

	// communicate with the SUT and log response time
	pThrParam->pCMEESUT->TalkToSUT(pRequest);

	delete pThrParam;
	return NULL;
}

bool TPCE::RunMarketFeedAsync(void* data)
{
	PMEESUTThreadParam pThrParam = reinterpret_cast<PMEESUTThreadParam>(data);

	pthread_t threadID; // thread ID
	pthread_attr_t threadAttribute; // thread attribute

	try
	{
		int status = pthread_attr_init(&threadAttribute); // initialize the attribute object
		if (status != 0)
		{
			throw new CThreadErr( CThreadErr::ERR_THREAD_ATTR_INIT );
		}
	
		// set the detachstate attribute to detached
		status = pthread_attr_setdetachstate(&threadAttribute, PTHREAD_CREATE_DETACHED);
		if (status != 0)
		{
			throw new CThreadErr( CThreadErr::ERR_THREAD_ATTR_DETACH );
		}
	
		// create the thread in the detached state - Call Trade Result asyncronously
		status = pthread_create(&threadID, &threadAttribute, &MarketFeedAsync, data);

		if (status != 0)
		{
			throw new CThreadErr( CThreadErr::ERR_THREAD_CREATE );
		}
	}
	catch(CThreadErr *pErr)
	{
		ostringstream osErr;
		osErr<<endl<<"Error: "<<pErr->ErrorText()
		     <<" at "<<"MEESUT::RunMarketFeedAsync"<<endl;
		pThrParam->pCMEESUT->LogErrorMessage(osErr.str());
		delete pErr;
		return false;
	}

	// return immediatelly
	return true;
}

bool CMEESUT::MarketFeed( PMarketFeedTxnInput pTxnInput )
{
	PMEESUTThreadParam pThrParam = new TMEESUTThreadParam;
	memset(pThrParam, 0, sizeof(TMEESUTThreadParam));

	pThrParam->pCMEESUT = this;
	memcpy(&(pThrParam->TxnInput.m_MarketFeedTxnInput), pTxnInput, sizeof(TMarketFeedTxnInput));

	return ( RunMarketFeedAsync( reinterpret_cast<void*>(pThrParam) ) );
}
