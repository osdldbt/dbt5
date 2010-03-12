/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *               2010 Mark Wong
 *
 * 25 July 2006
 */

#include "transactions.h"

void *WorkerThread(void *data)
{
	PThreadParameter pThrParam = reinterpret_cast<PThreadParameter>(data);

	CSocket sockDrv;
	sockDrv.SetSocketfd(pThrParam->iSockfd);	// client socket

	PMsgDriverBrokerage pMessage = new TMsgDriverBrokerage;
	memset(pMessage, 0, sizeof(TMsgDriverBrokerage));   // zero the structure

	TMsgBrokerageDriver Reply; // return message
	INT32 iRet = 0; // return error
	CDBConnection* pDBConnection = NULL;

	int iNumRetry;

	// new connection
	pDBConnection = new CDBConnection(
			pThrParam->pBrokerageHouse->m_szHost,
			pThrParam->pBrokerageHouse->m_szDBName,
			pThrParam->pBrokerageHouse->m_szPostmasterPort);
	CSendToMarket sendToMarket = CSendToMarket(
			&(pThrParam->pBrokerageHouse->m_fLog));
	CMarketFeedDB marketFeedDB(pDBConnection);
	CMarketFeed marketFeed = CMarketFeed(&marketFeedDB, &sendToMarket);
	CTradeOrderDB tradeOrderDB(pDBConnection);
	CTradeOrder tradeOrder = CTradeOrder(&tradeOrderDB, &sendToMarket);

	// Initialize all classes that will be used to execute transactions.
	CBrokerVolumeDB brokerVolumeDB(pDBConnection);
	CBrokerVolume brokerVolume = CBrokerVolume(&brokerVolumeDB);
	CCustomerPositionDB customerPositionDB(pDBConnection);
	CCustomerPosition customerPosition = CCustomerPosition(&customerPositionDB);
	CMarketWatchDB marketWatchDB(pDBConnection);
	CMarketWatch marketWatch = CMarketWatch(&marketWatchDB);
	CSecurityDetailDB securityDetailDB = CSecurityDetailDB(pDBConnection);
	CSecurityDetail securityDetail = CSecurityDetail(&securityDetailDB);
	CTradeLookupDB tradeLookupDB(pDBConnection);
	CTradeLookup tradeLookup = CTradeLookup(&tradeLookupDB);
	CTradeStatusDB tradeStatusDB(pDBConnection);
	CTradeStatus tradeStatus = CTradeStatus(&tradeStatusDB);
	CTradeUpdateDB tradeUpdateDB(pDBConnection);
	CTradeUpdate tradeUpdate = CTradeUpdate(&tradeUpdateDB);
	CDataMaintenanceDB dataMaintenanceDB(pDBConnection);
	CDataMaintenance dataMaintenance = CDataMaintenance(&dataMaintenanceDB);
	CTradeCleanupDB tradeCleanupDB(pDBConnection);
	CTradeCleanup tradeCleanup = CTradeCleanup(&tradeCleanupDB);
	CTradeResultDB tradeResultDB(pDBConnection);
	CTradeResult tradeResult = CTradeResult(&tradeResultDB);

	do {
		try {
			sockDrv.Receive(reinterpret_cast<void *>(pMessage),
					sizeof(TMsgDriverBrokerage));
		} catch(CSocketErr *pErr) {
			sockDrv.CloseAccSocket();

			ostringstream osErr;
			osErr << "Error on Receive: " << pErr->ErrorText() <<
					" at BrokerageHouse::WorkerThread" << endl;
			pThrParam->pBrokerageHouse->LogErrorMessage(osErr.str());
			delete pErr;

			// The socket has been closed, break and let this thread die.
			break;
		}

		try {
			//  Parse Txn type
			switch (pMessage->TxnType) {
			case BROKER_VOLUME:
				iRet = pThrParam->pBrokerageHouse->RunBrokerVolume(
						&(pMessage->TxnInput.BrokerVolumeTxnInput),
						brokerVolume);
				break;
			case CUSTOMER_POSITION:
				iRet = pThrParam->pBrokerageHouse->RunCustomerPosition(
						&(pMessage->TxnInput.CustomerPositionTxnInput),
						customerPosition);
				break;
			case MARKET_FEED:
				iRet = pThrParam->pBrokerageHouse->RunMarketFeed(
						&(pMessage->TxnInput.MarketFeedTxnInput),
						marketFeed);
				break;
			case MARKET_WATCH:
				iRet = pThrParam->pBrokerageHouse->RunMarketWatch(
						&(pMessage->TxnInput.MarketWatchTxnInput),
						marketWatch);
				break;
			case SECURITY_DETAIL:
				iRet = pThrParam->pBrokerageHouse->RunSecurityDetail(
						&(pMessage->TxnInput.SecurityDetailTxnInput),
						securityDetail);
				break;
			case TRADE_LOOKUP:
				iRet = pThrParam->pBrokerageHouse->RunTradeLookup(
						&(pMessage->TxnInput.TradeLookupTxnInput),
						tradeLookup);
				break;
			case TRADE_ORDER:
				iRet = pThrParam->pBrokerageHouse->RunTradeOrder(
						&(pMessage->TxnInput.TradeOrderTxnInput),
						tradeOrder);
				break;
			case TRADE_RESULT:
				iNumRetry = 1;
				while (true) {
					try {
						iRet = pThrParam->pBrokerageHouse->RunTradeResult(
								&(pMessage->TxnInput.TradeResultTxnInput),
								tradeResult);
						break;
					} catch (const pqxx::sql_error &e) {
						// catch only serialization failure errors
						if (PGSQL_SERIALIZE_ERROR.compare(e.what())) {
							ostringstream osErr;
							osErr << "*** TRADE RESULT serialization failure : thread = " <<
									pthread_self() << " Retry #"
									<< iNumRetry << endl <<
									"*** query = " << e.query() << endl;
							pThrParam->pBrokerageHouse->LogErrorMessage(
									osErr.str(), false);

							if (iNumRetry <= 3) {
								iNumRetry++;
								// rollback the current trade result
								pDBConnection->RollbackTxn();
							} else {
								// it couldn't resubmit successfully,
								osErr << "*** TRADE RESULT could not resubmit transaction successfully : thread = " <<
										pthread_self() << " Retry #"
										<< iNumRetry << endl <<
										"*** query = " << e.query() << endl;
								throw;
							}
							// error should be caught by the next pqxx
							// catch in this function
						}
						// other pqxx errors should be caught by the next
						// pqxx catch in this function
						else throw;
					}
				}

				if (iNumRetry > 1) {
					ostringstream osErr;
					osErr << "*** TRADE RESULT re-submission ok : thread = " <<
							pthread_self() << " #Retries = " <<
							iNumRetry << endl;
					pThrParam->pBrokerageHouse->LogErrorMessage(osErr.str(),
							false);
				}
				break;
			case TRADE_STATUS:
				iRet = pThrParam->pBrokerageHouse->RunTradeStatus(
						&(pMessage->TxnInput.TradeStatusTxnInput),
						tradeStatus);
				break;
			case TRADE_UPDATE:
				iRet = pThrParam->pBrokerageHouse->RunTradeUpdate(
						&(pMessage->TxnInput.TradeUpdateTxnInput),
						tradeUpdate);
				break;
			case DATA_MAINTENANCE:
				iRet = pThrParam->pBrokerageHouse->RunDataMaintenance(
						&(pMessage->TxnInput.DataMaintenanceTxnInput),
						dataMaintenance);
				break;
			case TRADE_CLEANUP:
				iRet = pThrParam->pBrokerageHouse->RunTradeCleanup(
						&(pMessage->TxnInput.TradeCleanupTxnInput),
						tradeCleanup);
				break;
			default:
				cout << "wrong txn type" << endl;
				iRet = ERR_TYPE_WRONGTXN;
			}
		} catch (const pqxx::broken_connection &e) {
			// exceptions thrown by pqxx
			ostringstream osErr;
			osErr << "pqxx broken connection: " << e.what() << endl <<
					" at " << "BrokerageHouse::WorkerThread" << endl;
			pThrParam->pBrokerageHouse->LogErrorMessage(osErr.str());
			iRet = ERR_TYPE_PQXX;
		} catch (const pqxx::sql_error &e) {
			ostringstream osErr;
			osErr << "pqxx SQL error: " << e.what() << endl <<
					"Query was: '" << e.query() << "'" << endl << " at " <<
					"BrokerageHouse::WorkerThread" << endl;
			pThrParam->pBrokerageHouse->LogErrorMessage(osErr.str());
			iRet = ERR_TYPE_PQXX;
			// Rollback any transaction in place.
			pDBConnection->RollbackTxn();
		}

		// send status to driver
		Reply.iStatus = iRet;
		try {
			sockDrv.Send(reinterpret_cast<void *>(&Reply), sizeof(Reply));
		} catch(CSocketErr *pErr) {
			sockDrv.CloseAccSocket();

			ostringstream osErr;
			osErr << "Error on Send: " << pErr->ErrorText() <<
					" at BrokerageHouse::WorkerThread" << endl;
			pThrParam->pBrokerageHouse->LogErrorMessage(osErr.str());
			delete pErr;

			// The socket has been closed, break and let this thread die.
			break;
		}
	} while (true);

	delete pDBConnection; // close connection with the database
	close(pThrParam->iSockfd); // close socket connection with the driver
	delete pThrParam;
	delete pMessage;

	return NULL;
}

// entry point for worker thread
void EntryWorkerThread(void *data)
{
	PThreadParameter pThrParam = reinterpret_cast<PThreadParameter>(data);

	pthread_t threadID; // thread ID
	pthread_attr_t threadAttribute; // thread attribute

	try {
		// initialize the attribute object
		int status = pthread_attr_init(&threadAttribute);
		if (status != 0) {
			throw new CThreadErr(CThreadErr::ERR_THREAD_ATTR_INIT);
		}

		// set the detachstate attribute to detached
		status = pthread_attr_setdetachstate(&threadAttribute,
				PTHREAD_CREATE_DETACHED);
		if (status != 0) {
			throw new CThreadErr(CThreadErr::ERR_THREAD_ATTR_DETACH);
		}

		// create the thread in the detached state
		status = pthread_create(&threadID, &threadAttribute, &WorkerThread,
				data);

		if (status != 0) {
			throw new CThreadErr(CThreadErr::ERR_THREAD_CREATE);
		}
	} catch(CThreadErr *pErr) {
		// close recently accepted connection, to release driver threads
		close(pThrParam->iSockfd);

		ostringstream osErr;
		osErr << endl << "Error: " << pErr->ErrorText() << " at " <<
				"BrokerageHouse::EntryWorkerThread" << endl <<
				"accepted socket connection closed" << endl;
		pThrParam->pBrokerageHouse->LogErrorMessage(osErr.str());
		delete pThrParam;
		delete pErr;
	}
}

// Constructor
CBrokerageHouse::CBrokerageHouse(const char *szHost, const char *szDBName,
		const char *szPostmasterPort, const int iListenPort,
		char *outputDirectory)
: m_iListenPort(iListenPort)
{
	strncpy(m_szHost, szHost, iMaxHostname);
	strncpy(m_szDBName, szDBName, iMaxDBName);
	strncpy(m_szPostmasterPort, szPostmasterPort, iMaxPort);

	char filename[iMaxPath + 1];
	snprintf(filename, iMaxPath, "%s/BrokerageHouse_Error.log",
			outputDirectory);
	m_fLog.open(filename, ios::out);
}

// Destructor
CBrokerageHouse::~CBrokerageHouse()
{
	m_fLog.close();
}

// Run Broker Volume transaction
INT32 CBrokerageHouse::RunBrokerVolume(PBrokerVolumeTxnInput pTxnInput,
		CBrokerVolume &brokerVolume)
{
	TBrokerVolumeTxnOutput	TxnOutput;
	brokerVolume.DoTxn(pTxnInput, &TxnOutput);

	return TxnOutput.status;
}

// Run Customer Position transaction
INT32 CBrokerageHouse::RunCustomerPosition(PCustomerPositionTxnInput pTxnInput,
		CCustomerPosition &customerPosition)
{
	TCustomerPositionTxnOutput	TxnOutput;
	customerPosition.DoTxn(pTxnInput, &TxnOutput);

	return TxnOutput.status;
}

// Run Data Maintenance transaction
INT32 CBrokerageHouse::RunDataMaintenance(PDataMaintenanceTxnInput pTxnInput,
		CDataMaintenance &dataMaintenance)
{
	TDataMaintenanceTxnOutput TxnOutput;
	dataMaintenance.DoTxn(pTxnInput, &TxnOutput);

	return TxnOutput.status;
}

// Run Trade Cleanup transaction
INT32 CBrokerageHouse::RunTradeCleanup(PTradeCleanupTxnInput pTxnInput,
		CTradeCleanup &tradeCleanup)
{
	TTradeCleanupTxnOutput TxnOutput;
	tradeCleanup.DoTxn(pTxnInput, &TxnOutput);

	return TxnOutput.status;
}

// Run Market Feed transaction
INT32 CBrokerageHouse::RunMarketFeed(PMarketFeedTxnInput pTxnInput,
		CMarketFeed &marketFeed)
{
	TMarketFeedTxnOutput TxnOutput;
	marketFeed.DoTxn(pTxnInput, &TxnOutput);

	return TxnOutput.status;
}

// Run Market Watch transaction
INT32 CBrokerageHouse::RunMarketWatch(PMarketWatchTxnInput pTxnInput,
		CMarketWatch &marketWatch)
{
	TMarketWatchTxnOutput TxnOutput;
	marketWatch.DoTxn(pTxnInput, &TxnOutput);

	return TxnOutput.status;
}

// Run Security Detail transaction
INT32 CBrokerageHouse::RunSecurityDetail(PSecurityDetailTxnInput pTxnInput,
		CSecurityDetail &securityDetail)
{
	TSecurityDetailTxnOutput TxnOutput;
	securityDetail.DoTxn(pTxnInput, &TxnOutput);

	return TxnOutput.status;
}

// Run Trade Lookup transaction
INT32 CBrokerageHouse::RunTradeLookup(PTradeLookupTxnInput pTxnInput,
		CTradeLookup &tradeLookup)
{
	TTradeLookupTxnOutput TxnOutput;
	tradeLookup.DoTxn(pTxnInput, &TxnOutput);

	return TxnOutput.status;
}

// Run Trade Order transaction
INT32 CBrokerageHouse::RunTradeOrder(PTradeOrderTxnInput pTxnInput,
		CTradeOrder &tradeOrder)
{
	TTradeOrderTxnOutput TxnOutput;
	tradeOrder.DoTxn(pTxnInput, &TxnOutput);

	return TxnOutput.status;
}

// Run Trade Result transaction
INT32 CBrokerageHouse::RunTradeResult(PTradeResultTxnInput pTxnInput,
		CTradeResult &tradeResult)
{
	TTradeResultTxnOutput TxnOutput;
	tradeResult.DoTxn(pTxnInput, &TxnOutput);

	return TxnOutput.status;
}

// Run Trade Status transaction
INT32 CBrokerageHouse::RunTradeStatus(PTradeStatusTxnInput pTxnInput,
		CTradeStatus &tradeStatus)
{
	TTradeStatusTxnOutput TxnOutput;
	tradeStatus.DoTxn(pTxnInput, &TxnOutput);

	return TxnOutput.status;
}

// Run Trade Update transaction
INT32 CBrokerageHouse::RunTradeUpdate(PTradeUpdateTxnInput pTxnInput,
		CTradeUpdate &tradeUpdate)
{
	TTradeUpdateTxnOutput TxnOutput;
	tradeUpdate.DoTxn(pTxnInput, &TxnOutput);

	return TxnOutput.status;
}

// Listener
void CBrokerageHouse::Listener(void)
{
	int acc_socket;
	PThreadParameter pThrParam = NULL;

	m_Socket.Listen(m_iListenPort);

	while (true) {
		acc_socket = 0;
		try {
			acc_socket = m_Socket.Accept();

			pThrParam = new TThreadParameter;
			// zero the structure
			memset(pThrParam, 0, sizeof(TThreadParameter));

			pThrParam->iSockfd = acc_socket;
			pThrParam->pBrokerageHouse = this;

			// call entry point
			EntryWorkerThread(reinterpret_cast<void *>(pThrParam));
		} catch (CSocketErr *pErr) {
			ostringstream osErr;
			osErr << "Problem accepting socket connection" << endl <<
					"Error: " << pErr->ErrorText() << " at " <<
					"BrokerageHouse::Listener" << endl;
			LogErrorMessage(osErr.str());
			delete pErr;
			delete pThrParam;
		}
	}
}

// LogErrorMessage
void CBrokerageHouse::LogErrorMessage(const string sErr, bool bScreen)
{
	m_LogLock.lock();
	if (bScreen) cout << sErr;
	m_fLog << sErr;
	m_fLog.flush();
	m_LogLock.unlock();
}
