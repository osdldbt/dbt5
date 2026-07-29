/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 30 July 2006
 */

#include "MarketExchange.h"

// Fire expired MEE timers.  SubmitTradeRequest and GenerateTradeResult
// return the number of milliseconds until the next pending timer, and
// GenerateTradeResult must be called when that time elapses or pending
// Trade-Result and triggered Market-Feed transactions are never sent.
void *
MarketTimerThread(void *data)
{
	CMarketExchange *pMarketExchange
			= reinterpret_cast<CMarketExchange *>(data);

	pMarketExchange->m_TimerCond.lock();
	while (!pMarketExchange->m_TimerShutdown) {
		if (pMarketExchange->m_NextTimerDelay < 0) {
			// No outstanding timers; wait for a trade request to start
			// one.
			pMarketExchange->m_TimerCond.wait();
			continue;
		}

		unsigned int generation = pMarketExchange->m_TimerGeneration;
		pMarketExchange->m_TimerCond.timedwait(
				(long) pMarketExchange->m_NextTimerDelay * 1000);
		if (pMarketExchange->m_TimerShutdown) {
			break;
		}
		if (generation != pMarketExchange->m_TimerGeneration) {
			// A trade request rescheduled the next timer; reevaluate.
			continue;
		}

		pMarketExchange->m_TimerCond.unlock();
		INT32 next = pMarketExchange->m_pCMEE->GenerateTradeResult();
		pMarketExchange->m_TimerCond.lock();
		if (generation == pMarketExchange->m_TimerGeneration) {
			pMarketExchange->m_NextTimerDelay = next;
		}
	}
	pMarketExchange->m_TimerCond.unlock();

	return NULL;
}

void
CMarketExchange::updateNextTimer(INT32 delay)
{
	m_TimerCond.lock();
	m_NextTimerDelay = delay;
	++m_TimerGeneration;
	m_TimerCond.signal();
	m_TimerCond.unlock();
}

// worker thread
void *
MarketWorkerThread(void *data)
{
	PMarketThreadParam pThrParam = reinterpret_cast<PMarketThreadParam>(data);

	CSocket sockDrv;
	sockDrv.setSocketFd(pThrParam->iSockfd); // client socket

	PTradeRequest pMessage = new TTradeRequest;
	memset(pMessage, 0, sizeof(TTradeRequest)); // zero the structure

	do {
		try {
			sockDrv.dbt5Receive(
					reinterpret_cast<void *>(pMessage), sizeof(TTradeRequest));

			if (pThrParam->pMarketExchange->verbose()) {
				cout << "TTradeRequest" << endl
					 << "  price_quote: " << pMessage->price_quote << endl
					 << "  trade_id: " << pMessage->trade_id << endl
					 << "  trade_qty: " << pMessage->trade_qty << endl
					 << "  eAction: " << pMessage->eAction << endl
					 << "  symbol: " << pMessage->symbol << endl
					 << "  trade_type_id: " << pMessage->trade_type_id << endl;
			}

			// submit trade request
			pThrParam->pMarketExchange->updateNextTimer(
					pThrParam->pMarketExchange->m_pCMEE->SubmitTradeRequest(
							pMessage));
		} catch (CSocketErr *pErr) {
			sockDrv.dbt5Disconnect(); // close connection

			if (pErr->getAction() == CSocketErr::ERR_SOCKET_CLOSED) {
				delete pErr;
				break;
			}

			cerr << time(NULL)
				 << " Trade Request not submitted to Market Exchange" << endl
				 << "Error: " << pErr->ErrorText() << endl;
			delete pErr;

			// The socket is closed, break and let this thread die.
			break;
		}
	} while (true);

	delete pMessage;
	delete pThrParam;
	return NULL;
}

// entry point for worker thread
void
EntryMarketWorkerThread(void *data)
{
	PMarketThreadParam pThrParam = reinterpret_cast<PMarketThreadParam>(data);

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

		// create the thread in the detached state
		status = pthread_create(
				&threadID, &threadAttribute, &MarketWorkerThread, data);

		if (status != 0) {
			throw CThreadErr(CThreadErr::ERR_THREAD_CREATE);
		}
	} catch (const CThreadErr &pErr) {
		// close recently accepted connection, to release threads
		close(pThrParam->iSockfd);

		cerr << "Error: " << pErr.ErrorText()
			 << " at MarketExchange::entryMarketWorkerThread" << endl
			 << "accepted socket connection closed" << endl;
	}
}

// Constructor
CMarketExchange::CMarketExchange(const DataFileManager &inputFiles,
		char *szFileLoc, UINT32 UniqueId, TIdent iConfiguredCustomerCount,
		TIdent iActiveCustomerCount, int iListenPort, char *szBHaddr,
		int iBHlistenPort, char *outputDirectory, bool verbose = false)
: m_UniqueId(UniqueId), m_iListenPort(iListenPort), m_Verbose(verbose),
  m_TimerCond(m_TimerLock), m_NextTimerDelay(-1), m_TimerGeneration(0),
  m_TimerShutdown(false)
{
	char filename[iMaxPath + 1];
	snprintf(filename, iMaxPath, "%s/MarketExchange.log", outputDirectory);
	m_pLog = new CEGenLogger(eDriverEGenLoader, 0, filename, &m_fmt);

	// Initialize MEESUT
	m_pCMEESUT = new CMEESUT(outputDirectory, szBHaddr, iBHlistenPort);

	// Initialize MEE
	m_pCMEE = new CMEE(0, m_pCMEESUT, m_pLog, inputFiles, UniqueId);
	m_pCMEE->SetBaseTime();

	// Fire deferred trade processing when its timers expire.
	if (pthread_create(&m_TimerThreadId, NULL, &MarketTimerThread, this)
			!= 0) {
		throw CThreadErr(
				CThreadErr::ERR_THREAD_CREATE, "CMarketExchange::ctor");
	}
}

// Destructor
CMarketExchange::~CMarketExchange()
{
	// Stop the timer thread before tearing down the MEE it uses.
	m_TimerCond.lock();
	m_TimerShutdown = true;
	m_TimerCond.broadcast();
	m_TimerCond.unlock();
	pthread_join(m_TimerThreadId, NULL);

	delete m_pCMEE;
	delete m_pCMEESUT;
	delete m_pLog;
}

void
CMarketExchange::startListener(void)
{
	int acc_socket;
	PMarketThreadParam pThrParam;

	m_Socket.dbt5Listen(m_iListenPort);

	while (true) {
		acc_socket = 0;
		try {
			acc_socket = m_Socket.dbt5Accept();

			// create new parameter structure
			pThrParam = new TMarketThreadParam;
			// zero the structure
			memset(pThrParam, 0, sizeof(TMarketThreadParam));

			pThrParam->iSockfd = acc_socket;
			pThrParam->pMarketExchange = this;

			// call entry point
			EntryMarketWorkerThread(reinterpret_cast<void *>(pThrParam));
		} catch (CSocketErr *pErr) {
			cerr << "Problem to accept socket connection" << endl
				 << "Error: " << pErr->ErrorText() << " at "
				 << "MarketExchange::startListener" << endl;
			delete pErr;
		}
	}
}

bool
CMarketExchange::verbose()
{
	return m_Verbose;
}
