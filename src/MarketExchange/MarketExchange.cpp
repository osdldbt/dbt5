/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 30 July 2006
 */

#include "MarketExchange.h"

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
			pThrParam->pMarketExchange->m_pCMEE->SubmitTradeRequest(pMessage);
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
			throw new CThreadErr(CThreadErr::ERR_THREAD_ATTR_INIT);
		}

		// set the detachstate attribute to detached
		status = pthread_attr_setdetachstate(
				&threadAttribute, PTHREAD_CREATE_DETACHED);
		if (status != 0) {
			throw new CThreadErr(CThreadErr::ERR_THREAD_ATTR_DETACH);
		}

		// create the thread in the detached state
		status = pthread_create(
				&threadID, &threadAttribute, &MarketWorkerThread, data);

		if (status != 0) {
			throw new CThreadErr(CThreadErr::ERR_THREAD_CREATE);
		}
	} catch (CThreadErr *pErr) {
		// close recently accepted connection, to release threads
		close(pThrParam->iSockfd);

		cerr << "Error: " << pErr->ErrorText()
			 << " at MarketExchange::entryMarketWorkerThread" << endl
			 << "accepted socket connection closed" << endl;
		delete pErr;
	}
}

// Constructor
CMarketExchange::CMarketExchange(const DataFileManager &inputFiles,
		char *szFileLoc, UINT32 UniqueId, TIdent iConfiguredCustomerCount,
		TIdent iActiveCustomerCount, int iListenPort, char *szBHaddr,
		int iBHlistenPort, char *outputDirectory, bool verbose = false)
: m_UniqueId(UniqueId), m_iListenPort(iListenPort), m_Verbose(verbose)
{
	char filename[iMaxPath + 1];
	snprintf(filename, iMaxPath, "%s/MarketExchange.log", outputDirectory);
	m_pLog = new CEGenLogger(eDriverEGenLoader, 0, filename, &m_fmt);

	// Initialize MEESUT
	m_pCMEESUT = new CMEESUT(outputDirectory, szBHaddr, iBHlistenPort);

	// Initialize MEE
	m_pCMEE = new CMEE(0, m_pCMEESUT, m_pLog, inputFiles, UniqueId);
	m_pCMEE->SetBaseTime();
}

// Destructor
CMarketExchange::~CMarketExchange()
{
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
