/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 30 July 2006
 */

#include "MEESUT.h"

CMEESUT::~CMEESUT()
{
	// Wait for detached Trade-Result and Market-Feed threads so they
	// are not left dereferencing a destroyed object.
	m_ThreadCountCond.lock();
	while (m_OutstandingThreads > 0) {
		m_ThreadCountCond.wait();
	}
	m_ThreadCountCond.unlock();
}

void
CMEESUT::threadStarted()
{
	Locker<CMutex> locker(m_ThreadCountLock);
	++m_OutstandingThreads;
}

void
CMEESUT::threadFinished()
{
	Locker<CMutex> locker(m_ThreadCountLock);
	if (--m_OutstandingThreads == 0) {
		m_ThreadCountCond.broadcast();
	}
}

void *
TradeResultAsync(void *data)
{
	PMEESUTThreadParam pThrParam = reinterpret_cast<PMEESUTThreadParam>(data);
	struct TMsgDriverBrokerage request;

	memset(&request, 0, sizeof(TMsgDriverBrokerage));

	request.TxnType = TRADE_RESULT;
	memcpy(&(request.TxnInput.TradeResultTxnInput),
			&(pThrParam->TxnInput.m_TradeResultTxnInput),
			sizeof(request.TxnInput.TradeResultTxnInput));

	// communicate with the SUT and log response time
	{
		Locker<CMutex> locker(pThrParam->pCMEESUT->m_SocketLock);
		try {
			pThrParam->pCMEESUT->talkToSUT(&request);
		} catch (CSocketErr *pErr) {
			ostringstream osErr;
			osErr << "Error: " << pErr->ErrorText()
				  << " at MEESUT::TradeResultAsync" << endl;
			pThrParam->pCMEESUT->logErrorMessage(osErr.str());
			delete pErr;
		}
	}

	pThrParam->pCMEESUT->threadFinished();
	delete pThrParam;
	return NULL;
}

bool
RunTradeResultAsync(void *data)
{
	PMEESUTThreadParam pThrParam = reinterpret_cast<PMEESUTThreadParam>(data);

	pthread_t threadID; // thread ID
	pthread_attr_t threadAttribute; // thread attribute

	try {
		// initialize the attribute object
		int status = pthread_attr_init(&threadAttribute);
		if (status != 0) {
			throw CThreadErr(CThreadErr::ERR_THREAD_ATTR_INIT);
		}

		// set the detachstate attribute to detached
		status = pthread_attr_setdetachstate(
				&threadAttribute, PTHREAD_CREATE_DETACHED);
		if (status != 0) {
			throw CThreadErr(CThreadErr::ERR_THREAD_ATTR_DETACH);
		}

		// create the thread in the detached state - Call Trade Result
		// asyncronously
		pThrParam->pCMEESUT->threadStarted();
		status = pthread_create(
				&threadID, &threadAttribute, &TradeResultAsync, data);
		pthread_attr_destroy(&threadAttribute);

		if (status != 0) {
			pThrParam->pCMEESUT->threadFinished();
			throw CThreadErr(CThreadErr::ERR_THREAD_CREATE);
		}
	} catch (const CThreadErr &pErr) {
		ostringstream osErr;
		osErr << "Error: " << pErr.ErrorText()
			  << " at MEESUT::RunTradeResultAsync" << endl;
		pThrParam->pCMEESUT->logErrorMessage(osErr.str());
		delete pThrParam;
		return false;
	}

	// return immediatelly
	return true;
}

bool
CMEESUT::TradeResult(PTradeResultTxnInput pTxnInput)
{
	PMEESUTThreadParam pThrParam = new TMEESUTThreadParam;
	memset(pThrParam, 0, sizeof(TMEESUTThreadParam));

	pThrParam->pCMEESUT = this;
	memcpy(&(pThrParam->TxnInput.m_TradeResultTxnInput), pTxnInput,
			sizeof(TTradeResultTxnInput));

	return (RunTradeResultAsync(reinterpret_cast<void *>(pThrParam)));
}

// Market Feed
//
void *
MarketFeedAsync(void *data)
{
	PMEESUTThreadParam pThrParam = reinterpret_cast<PMEESUTThreadParam>(data);
	struct TMsgDriverBrokerage request;

	memset(&request, 0, sizeof(TMsgDriverBrokerage));

	request.TxnType = MARKET_FEED;
	memcpy(&(request.TxnInput.MarketFeedTxnInput),
			&(pThrParam->TxnInput.m_MarketFeedTxnInput),
			sizeof(request.TxnInput.MarketFeedTxnInput));

	// communicate with the SUT and log response time
	{
		Locker<CMutex> locker(pThrParam->pCMEESUT->m_SocketLock);
		try {
			pThrParam->pCMEESUT->talkToSUT(&request);
		} catch (CSocketErr *pErr) {
			ostringstream osErr;
			osErr << "Error: " << pErr->ErrorText()
				  << " at MEESUT::MarketFeedAsync" << endl;
			pThrParam->pCMEESUT->logErrorMessage(osErr.str());
			delete pErr;
		}
	}

	pThrParam->pCMEESUT->threadFinished();
	delete pThrParam;
	return NULL;
}

bool
RunMarketFeedAsync(void *data)
{
	PMEESUTThreadParam pThrParam = reinterpret_cast<PMEESUTThreadParam>(data);

	pthread_t threadID; // thread ID
	pthread_attr_t threadAttribute; // thread attribute

	try {
		// initialize the attribute object
		int status = pthread_attr_init(&threadAttribute);
		if (status != 0) {
			throw CThreadErr(CThreadErr::ERR_THREAD_ATTR_INIT);
		}

		// set the detachstate attribute to detached
		status = pthread_attr_setdetachstate(
				&threadAttribute, PTHREAD_CREATE_DETACHED);
		if (status != 0) {
			throw CThreadErr(CThreadErr::ERR_THREAD_ATTR_DETACH);
		}

		// create the thread in the detached state - Call Market Feed
		// asyncronously
		pThrParam->pCMEESUT->threadStarted();
		status = pthread_create(
				&threadID, &threadAttribute, &MarketFeedAsync, data);
		pthread_attr_destroy(&threadAttribute);

		if (status != 0) {
			pThrParam->pCMEESUT->threadFinished();
			throw CThreadErr(CThreadErr::ERR_THREAD_CREATE);
		}
	} catch (const CThreadErr &pErr) {
		ostringstream osErr;
		osErr << "Error: " << pErr.ErrorText()
			  << " at MEESUT::RunMarketFeedAsync" << endl;
		pThrParam->pCMEESUT->logErrorMessage(osErr.str());
		delete pThrParam;
		return false;
	}

	// return immediatelly
	return true;
}

bool
CMEESUT::MarketFeed(PMarketFeedTxnInput pTxnInput)
{
	PMEESUTThreadParam pThrParam = new TMEESUTThreadParam;
	memset(pThrParam, 0, sizeof(TMEESUTThreadParam));

	pThrParam->pCMEESUT = this;
	memcpy(&(pThrParam->TxnInput.m_MarketFeedTxnInput), pTxnInput,
			sizeof(TMarketFeedTxnInput));

	return (RunMarketFeedAsync(reinterpret_cast<void *>(pThrParam)));
}
