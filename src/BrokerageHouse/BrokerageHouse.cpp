/*
 * BrokerageHouse.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 25 July 2006
 */

#include <transactions.h>

using namespace TPCE;

// worker thread
void* WorkerThread(void* data)
{
	PThreadParameter pThrParam = reinterpret_cast<PThreadParameter>(data);

	CSocket sockDrv;
	sockDrv.SetSocketfd( pThrParam->iSockfd );	// client socket

	PMsgDriverBrokerage pMessage = new TMsgDriverBrokerage;
	memset(pMessage, 0, sizeof(TMsgDriverBrokerage));   // zero the structure

	TMsgBrokerageDriver	Reply;		// return message
	INT32 iRet = 0;				// return error
	CDBConnection* pDBConnection = NULL;
	CTradeResult* pTradeResult = NULL;

	// new connection
	pDBConnection = new CDBConnection(
			pThrParam->pBrokerageHouse->m_szHost,
			pThrParam->pBrokerageHouse->m_szDBName,
			pThrParam->pBrokerageHouse->m_szPostmasterPort);
	CSendToMarket* pSendToMarket = new CSendToMarket(
			&(pThrParam->pBrokerageHouse->m_fLog));
	CMarketFeedDB m_MarketFeedDB(pDBConnection);
	CMarketFeed* pMarketFeed = new CMarketFeed(&m_MarketFeedDB, pSendToMarket);
	CTradeOrderDB TradeOrderDB(pDBConnection);
	CTradeOrder *pTradeOrder = new CTradeOrder(&TradeOrderDB, pSendToMarket);
	do
	{
		try
		{
			sockDrv.Receive(reinterpret_cast<void*>(pMessage),
					sizeof(TMsgDriverBrokerage));

			try
			{

				//  Parse Txn type
				switch ( pMessage->TxnType )
				{
				case BROKER_VOLUME:
				{
					CBrokerVolumeDB BrokerVolumeDB(pDBConnection);
					CBrokerVolume *pBrokerVolume = new CBrokerVolume(
							&BrokerVolumeDB);
					iRet = pThrParam->pBrokerageHouse->RunBrokerVolume(
							&(pMessage->TxnInput.BrokerVolumeTxnInput),
							*pBrokerVolume );
					delete pBrokerVolume;
					break;
				}
				case CUSTOMER_POSITION:
				{
					CCustomerPositionDB CustomerPositionDB(pDBConnection);
					CCustomerPosition *pCustomerPosition =
							new CCustomerPosition(&CustomerPositionDB);
					iRet = pThrParam->pBrokerageHouse->RunCustomerPosition(
							&(pMessage->TxnInput.CustomerPositionTxnInput),
							*pCustomerPosition );
					delete pCustomerPosition;
					break;
				}
		
				case MARKET_FEED:
				{
					iRet = pThrParam->pBrokerageHouse->RunMarketFeed(
							&(pMessage->TxnInput.MarketFeedTxnInput),
							*pMarketFeed );
					break;
				}
				case MARKET_WATCH:
				{
					CMarketWatchDB MarketWatchDB(pDBConnection);
					CMarketWatch *pMarketWatch =
							new CMarketWatch(&MarketWatchDB);
					iRet = pThrParam->pBrokerageHouse->RunMarketWatch(
							&(pMessage->TxnInput.MarketWatchTxnInput),
							*pMarketWatch );
					delete pMarketWatch;
					break;
				}
				case SECURITY_DETAIL:
				{
					CSecurityDetailDB SecurityDetailDB = CSecurityDetailDB(
							pDBConnection);
					CSecurityDetail *pSecurityDetail = new CSecurityDetail(
							&SecurityDetailDB);
					iRet = pThrParam->pBrokerageHouse->RunSecurityDetail(
							&(pMessage->TxnInput.SecurityDetailTxnInput),
							*pSecurityDetail );
					delete pSecurityDetail;
					break;
				}
				case TRADE_LOOKUP:
				{
					CTradeLookupDB TradeLookupDB(pDBConnection);
					CTradeLookup *pTradeLookup =
							new CTradeLookup(&TradeLookupDB);
					iRet = pThrParam->pBrokerageHouse->RunTradeLookup(
							&(pMessage->TxnInput.TradeLookupTxnInput),
							*pTradeLookup );
					delete pTradeLookup;
					break;
				}
				case TRADE_ORDER:
				{
					iRet = pThrParam->pBrokerageHouse->RunTradeOrder(
							&(pMessage->TxnInput.TradeOrderTxnInput),
							*pTradeOrder );
					break;
				}
				case TRADE_RESULT:
				{
					int iNumRetry = 1;
					while(true)
					{
						try
						{
							CTradeResultDB TradeResultDB(pDBConnection);
							pTradeResult = new CTradeResult(&TradeResultDB);
							iRet = pThrParam->pBrokerageHouse->RunTradeResult(
									&(pMessage->TxnInput.TradeResultTxnInput),
									*pTradeResult );
							delete pTradeResult;
							break;
						}
						// catch only serialization failure errors
						catch (const pqxx::sql_error &e)
						{
							if ( PGSQL_SERIALIZE_ERROR.compare(e.what()) )
							{
								ostringstream osErr;
								osErr << endl <<
										"**serialization failure : thread = " <<
										pthread_self() << " Retry #"
										<< iNumRetry << endl <<
										"** query = " << e.query() << endl;
								pThrParam->pBrokerageHouse->LogErrorMessage(
										osErr.str(), false);

								if (iNumRetry <= 3)
								{
									iNumRetry++;
									// rollback the current trade result
									pDBConnection->RollbackTxn();
									delete pTradeResult;
								}
								// it couldn't resubmit successfully,
								else throw;
								// error should be caught by the next pqxx
								// catch in this function
							}
							// other pqxx errors should be caught by the next
							// pqxx catch in this function
							else throw;
						}
					}

					if (iNumRetry > 1)
					{
						ostringstream osErr;
						osErr << "** txn re-submission ok : thread = " <<
								pthread_self() << " #Retries = " <<
								iNumRetry << endl;
						pThrParam->pBrokerageHouse->LogErrorMessage(osErr.str(),
								false);
					}
					break;
				}
				case TRADE_STATUS:
				{
					CTradeStatusDB TradeStatusDB(pDBConnection);
					CTradeStatus *pTradeStatus =
							new CTradeStatus(&TradeStatusDB);
					iRet = pThrParam->pBrokerageHouse->RunTradeStatus(
							&(pMessage->TxnInput.TradeStatusTxnInput),
							*pTradeStatus );
					delete pTradeStatus;
					break;
				}
				case TRADE_UPDATE:
				{
					CTradeUpdateDB TradeUpdateDB(pDBConnection);
					CTradeUpdate *pTradeUpdate =
							new CTradeUpdate(&TradeUpdateDB);
					iRet = pThrParam->pBrokerageHouse->RunTradeUpdate(
							&(pMessage->TxnInput.TradeUpdateTxnInput),
							*pTradeUpdate );
					delete pTradeUpdate;
					break;
				}
				case DATA_MAINTENANCE:
				{
					CDataMaintenanceDB DataMaintenanceDB(pDBConnection);
					CDataMaintenance *pDataMaintenance =
							new CDataMaintenance(&DataMaintenanceDB);
					iRet = pThrParam->pBrokerageHouse->RunDataMaintenance(
							&(pMessage->TxnInput.DataMaintenanceTxnInput),
							*pDataMaintenance );
					delete pDataMaintenance;
					break;
				}
				case TRADE_CLEANUP:
				{
					CTradeCleanupDB TradeCleanupDB(pDBConnection);
					CTradeCleanup *pTradeCleanup = new CTradeCleanup(
							&TradeCleanupDB);
					iRet = pThrParam->pBrokerageHouse->RunTradeCleanup(
							&(pMessage->TxnInput.TradeCleanupTxnInput),
							*pTradeCleanup );
					delete pTradeCleanup;
					break;
				}
				default:
				{
					cout << "wrong txn type" << endl;
					iRet = ERR_TYPE_WRONGTXN;
				}
					
				}
			}
			// exceptions thrown by pqxx
			catch (const pqxx::broken_connection &e)
			{
				ostringstream osErr;
				osErr << "pqxx broken connection: " << e.what() << endl <<
						" at " << "BrokerageHouse::WorkerThread" << endl;
				pThrParam->pBrokerageHouse->LogErrorMessage(osErr.str());
				iRet = ERR_TYPE_PQXX;
			}
			catch (const pqxx::sql_error &e)
			{
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
			sockDrv.Send( reinterpret_cast<void*>(&Reply), sizeof(Reply) );
		}
		catch(CSocketErr *pErr)
		{
			sockDrv.CloseAccSocket();

			ostringstream osErr;
			osErr << endl << "Error: " << pErr->ErrorText() << " at " <<
					"BrokerageHouse::WorkerThread" << endl;
			pThrParam->pBrokerageHouse->LogErrorMessage(osErr.str());
			delete pErr;

			// The socket has been closed, break and let this thread die.
			break;
		}
	} while (true);

	delete pMarketFeed;
	delete pTradeOrder;
	delete pSendToMarket;
	delete pDBConnection;		// close connection with the database
	close(pThrParam->iSockfd);	// close socket connection with the driver
	delete pThrParam;
	delete pMessage;

	return NULL;
}

// entry point for worker thread
void EntryWorkerThread(void* data)
{
	PThreadParameter pThrParam = reinterpret_cast<PThreadParameter>(data);

	pthread_t threadID; // thread ID
	pthread_attr_t threadAttribute; // thread attribute

	try
	{
		// initialize the attribute object
		int status = pthread_attr_init(&threadAttribute);
		if (status != 0)
		{
			throw new CThreadErr( CThreadErr::ERR_THREAD_ATTR_INIT );
		}
	
		// set the detachstate attribute to detached
		status = pthread_attr_setdetachstate(&threadAttribute,
				PTHREAD_CREATE_DETACHED);
		if (status != 0)
		{
			throw new CThreadErr( CThreadErr::ERR_THREAD_ATTR_DETACH );
		}
	
		// create the thread in the detached state
		status = pthread_create(&threadID, &threadAttribute, &WorkerThread,
				data);
	
		if (status != 0)
		{
			throw new CThreadErr( CThreadErr::ERR_THREAD_CREATE );
		}
	}
	catch(CThreadErr *pErr)
	{
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
	memset(m_szHost, 0, sizeof(m_szHost));
	strncpy(m_szHost, szHost, sizeof(m_szHost) - 1);

	memset(m_szDBName, 0, sizeof(m_szDBName));
	strncpy(m_szDBName, szDBName, sizeof(m_szDBName) - 1);

	memset(m_szPostmasterPort, 0, sizeof(m_szPostmasterPort));
	strncpy(m_szPostmasterPort, szPostmasterPort,
			sizeof(m_szPostmasterPort) - 1);

	char filename[1024];
	sprintf(filename, "%s/BrokerageHouse_Error.log", outputDirectory);
	m_fLog.open(filename, ios::out);
}

// Destructor
CBrokerageHouse::~CBrokerageHouse()
{
	m_fLog.close();
}

// Run Broker Volume transaction
INT32 CBrokerageHouse::RunBrokerVolume( PBrokerVolumeTxnInput pTxnInput,
		CBrokerVolume &BrokerVolume )
{
	TBrokerVolumeTxnOutput	TxnOutput;
	BrokerVolume.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Run Customer Position transaction
INT32 CBrokerageHouse::RunCustomerPosition( PCustomerPositionTxnInput pTxnInput,
		CCustomerPosition &CustomerPosition )
{
	TCustomerPositionTxnOutput	TxnOutput;
	CustomerPosition.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Run Data Maintenance transaction
INT32 CBrokerageHouse::RunDataMaintenance( PDataMaintenanceTxnInput pTxnInput,
		CDataMaintenance &DataMaintenance )
{
	TDataMaintenanceTxnOutput	TxnOutput;
	DataMaintenance.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Run Trade Cleanup transaction
INT32 CBrokerageHouse::RunTradeCleanup( PTradeCleanupTxnInput pTxnInput,
		CTradeCleanup &TradeCleanup )
{
	TTradeCleanupTxnOutput	TxnOutput;
	TradeCleanup.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Run Market Feed transaction
INT32 CBrokerageHouse::RunMarketFeed( PMarketFeedTxnInput pTxnInput,
		CMarketFeed &MarketFeed )
{
	TMarketFeedTxnOutput	TxnOutput;
	MarketFeed.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Run Market Watch transaction
INT32 CBrokerageHouse::RunMarketWatch( PMarketWatchTxnInput pTxnInput,
		CMarketWatch &MarketWatch )
{
	TMarketWatchTxnOutput	TxnOutput;
	MarketWatch.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Run Security Detail transaction
INT32 CBrokerageHouse::RunSecurityDetail( PSecurityDetailTxnInput pTxnInput,
		CSecurityDetail &SecurityDetail )
{
	TSecurityDetailTxnOutput	TxnOutput;
	SecurityDetail.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Run Trade Lookup transaction
INT32 CBrokerageHouse::RunTradeLookup( PTradeLookupTxnInput pTxnInput,
		CTradeLookup &TradeLookup )
{
	TTradeLookupTxnOutput	TxnOutput;
	TradeLookup.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Run Trade Order transaction
INT32 CBrokerageHouse::RunTradeOrder( PTradeOrderTxnInput pTxnInput,
		CTradeOrder &TradeOrder )
{
	TTradeOrderTxnOutput	TxnOutput;
	TradeOrder.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Run Trade Result transaction
INT32 CBrokerageHouse::RunTradeResult( PTradeResultTxnInput pTxnInput,
		CTradeResult &TradeResult )
{
	TTradeResultTxnOutput	TxnOutput;
	TradeResult.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Run Trade Status transaction
INT32 CBrokerageHouse::RunTradeStatus( PTradeStatusTxnInput pTxnInput,
		CTradeStatus &TradeStatus )
{
	TTradeStatusTxnOutput	TxnOutput;
	TradeStatus.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Run Trade Update transaction
INT32 CBrokerageHouse::RunTradeUpdate( PTradeUpdateTxnInput pTxnInput,
		CTradeUpdate &TradeUpdate )
{
	TTradeUpdateTxnOutput	TxnOutput;
	TradeUpdate.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Listener
void CBrokerageHouse::Listener( void )
{
	int acc_socket;
	PThreadParameter pThrParam = NULL;
	
	m_Socket.Listen( m_iListenPort );

	while(true)
	{
		acc_socket = 0;
		try
		{
			acc_socket = m_Socket.Accept();
	
			pThrParam = new TThreadParameter;
			// zero the structure
			memset(pThrParam, 0, sizeof(TThreadParameter));
	
			pThrParam->iSockfd = acc_socket;
			pThrParam->pBrokerageHouse = this;
	
			// call entry point
			EntryWorkerThread( reinterpret_cast<void*>(pThrParam) );
		}
		catch(CSocketErr *pErr)
		{
			ostringstream osErr;
			osErr<<endl<<"Problem to accept socket connection"
			     <<endl<<"Error: "<<pErr->ErrorText()
			     <<" at "<<"BrokerageHouse::Listener"<<endl;
			LogErrorMessage(osErr.str());
			delete pErr;
			delete pThrParam;
		}
	}
}

// LogErrorMessage
void CBrokerageHouse::LogErrorMessage( const string sErr, bool bScreen )
{
	m_LogLock.ClaimLock();
	if (bScreen) cout<<sErr;
	m_fLog<<sErr;
	m_fLog.flush();
	m_LogLock.ReleaseLock();
}
