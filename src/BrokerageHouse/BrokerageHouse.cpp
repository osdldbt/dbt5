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
void* TPCE::WorkerThread(void* data)
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

	try
	{
		sockDrv.Receive( reinterpret_cast<void*>(pMessage), sizeof(TMsgDriverBrokerage));

		try
		{
			// new connection
			pDBConnection = new CDBConnection(pThrParam->pBrokerageHouse->m_szHost,
								pThrParam->pBrokerageHouse->m_szDBName,
								pThrParam->pBrokerageHouse->m_szPostmasterPort);

			//  Parse Txn type
			switch ( pMessage->TxnType )
			{
				case BROKER_VOLUME:
				{
					CBrokerVolume* pBrokerVolume = new CBrokerVolume(pDBConnection);
					iRet = pThrParam->pBrokerageHouse->RunBrokerVolume(
							&(pMessage->TxnInput.BrokerVolumeTxnInput), *pBrokerVolume );
					delete pBrokerVolume;
					break;
				}
				case CUSTOMER_POSITION:
				{
					CCustomerPosition* pCustomerPosition = new CCustomerPosition(pDBConnection);
					iRet = pThrParam->pBrokerageHouse->RunCustomerPosition(
							&(pMessage->TxnInput.CustomerPositionTxnInput), *pCustomerPosition );
					delete pCustomerPosition;
					break;
				}
		
				case MARKET_FEED:
				{
					CSendToMarket* pSendToMarket = new CSendToMarket(&(pThrParam->pBrokerageHouse->m_fLog));
					CMarketFeed* pMarketFeed = new CMarketFeed(pDBConnection, pSendToMarket);
					iRet = pThrParam->pBrokerageHouse->RunMarketFeed(
							&(pMessage->TxnInput.MarketFeedTxnInput), *pMarketFeed );
					delete pMarketFeed;
					delete pSendToMarket;
					break;
				}
				case MARKET_WATCH:
				{
					CMarketWatch* pMarketWatch = new CMarketWatch(pDBConnection);
					iRet = pThrParam->pBrokerageHouse->RunMarketWatch(
							&(pMessage->TxnInput.MarketWatchTxnInput), *pMarketWatch );
					delete pMarketWatch;
					break;
				}
				case SECURITY_DETAIL:
				{
					CSecurityDetail* pSecurityDetail = new CSecurityDetail(pDBConnection);
					iRet = pThrParam->pBrokerageHouse->RunSecurityDetail(
							&(pMessage->TxnInput.SecurityDetailTxnInput), *pSecurityDetail );
					delete pSecurityDetail;
					break;
				}
				case TRADE_LOOKUP:
				{
					CTradeLookup* pTradeLookup = new CTradeLookup(pDBConnection);
					iRet = pThrParam->pBrokerageHouse->RunTradeLookup(
							&(pMessage->TxnInput.TradeLookupTxnInput), *pTradeLookup );
					delete pTradeLookup;
					break;
				}
				case TRADE_ORDER:
				{
					CSendToMarket* pSendToMarket = new CSendToMarket(&(pThrParam->pBrokerageHouse->m_fLog));
					CTradeOrder* pTradeOrder = new CTradeOrder(pDBConnection, pSendToMarket);
					iRet = pThrParam->pBrokerageHouse->RunTradeOrder(
							&(pMessage->TxnInput.TradeOrderTxnInput), *pTradeOrder );
					delete pTradeOrder;
					delete pSendToMarket;
					break;
				}
				case TRADE_RESULT:
				{
				   int iNumRetry = 1;
				   while(true)
				   {
					try
					{
					   pTradeResult = new CTradeResult(pDBConnection);
					   iRet = pThrParam->pBrokerageHouse->RunTradeResult(
							&(pMessage->TxnInput.TradeResultTxnInput), *pTradeResult );
					   delete pTradeResult;
					   break;
				   	}
				   	catch (const pqxx::sql_error &e)  // catch only serialization failure errors
				   	{
					   if ( PGSQL_SERIALIZE_ERROR.compare(e.what()) )
					   {
						ostringstream osErr;
						osErr<<endl<<"**serialization failure : thread = "<<pthread_self()<<
								" Retry #"<<iNumRetry<<endl<<"** query = "<<e.query()<<endl;
						pThrParam->pBrokerageHouse->LogErrorMessage(osErr.str(), false);

						if (iNumRetry <= 3)
						{
							iNumRetry++;
							pDBConnection->RollbackTxn();	// rollback the current trade result
							delete pTradeResult;
						}
						else throw;	// it couldn't resubmit successfully,
								// error should be caught by the next pqxx catch in this function
					   }
					   else throw;	// other pqxx errors should be caught by the next pqxx catch in this function
					}
				   }

				   if (iNumRetry > 1)
				   {
					ostringstream osErr;
					osErr<<"** txn re-submission ok : thread = "<<pthread_self()<<
						" #Retries = "<<iNumRetry<<endl;
					pThrParam->pBrokerageHouse->LogErrorMessage(osErr.str(), false);
				   }
				   break;
				}
				case TRADE_STATUS:
				{
					CTradeStatus* pTradeStatus = new CTradeStatus(pDBConnection);
					iRet = pThrParam->pBrokerageHouse->RunTradeStatus(
							&(pMessage->TxnInput.TradeStatusTxnInput), *pTradeStatus );
					delete pTradeStatus;
					break;
				}
				case TRADE_UPDATE:
				{
					CTradeUpdate* pTradeUpdate = new CTradeUpdate(pDBConnection);
					iRet = pThrParam->pBrokerageHouse->RunTradeUpdate(
							&(pMessage->TxnInput.TradeUpdateTxnInput), *pTradeUpdate );
					delete pTradeUpdate;
					break;
				}
				default:
				{
					cout<<"wrong txn type"<<endl;
					iRet = ERR_TYPE_WRONGTXN;
				}
					
			}
		}
		// exceptions thrown by pqxx
		catch (const pqxx::broken_connection &e)
		{
			ostringstream osErr;
			osErr<<"pqxx broken connection: "<<e.what()<<endl
		    	     <<" at "<<"BrokerageHouse::WorkerThread"<<endl;
			pThrParam->pBrokerageHouse->LogErrorMessage(osErr.str());
			iRet = ERR_TYPE_PQXX;
		}
		catch (const pqxx::sql_error &e)
		{
			ostringstream osErr;
			osErr<<"pqxx SQL error: "<<e.what()<<endl
			     <<"Query was: '"<<e.query()<<"'"<<endl
		             <<" at "<<"BrokerageHouse::WorkerThread"<<endl;
			pThrParam->pBrokerageHouse->LogErrorMessage(osErr.str());
			iRet = ERR_TYPE_PQXX;
		}
	
		// send status to driver
		Reply.iStatus = iRet;
		sockDrv.Send( reinterpret_cast<void*>(&Reply), sizeof(Reply) );
	}
	catch(CSocketErr *pErr)
	{
		ostringstream osErr;
		osErr<<endl<<"Error: "<<pErr->ErrorText()
		     <<" at "<<"BrokerageHouse::WorkerThread"<<endl;
		pThrParam->pBrokerageHouse->LogErrorMessage(osErr.str());
		delete pErr;
	}

	delete pDBConnection;		// close connection with the database
	close(pThrParam->iSockfd);	// close socket connection with the driver
	delete pThrParam;

	return NULL;
}

// entry point for worker thread
void TPCE::EntryWorkerThread(void* data)
{
	PThreadParameter pThrParam = reinterpret_cast<PThreadParameter>(data);

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
	
		// create the thread in the detached state
		status = pthread_create(&threadID, &threadAttribute, &WorkerThread, data);
	
		if (status != 0)
		{
			throw new CThreadErr( CThreadErr::ERR_THREAD_CREATE );
		}
	}
	catch(CThreadErr *pErr)
	{
		close(pThrParam->iSockfd); // close recently accepted connection, to release driver threads

		ostringstream osErr;
		osErr<<endl<<"Error: "<<pErr->ErrorText()
		     <<" at "<<"BrokerageHouse::EntryWorkerThread"<<endl
		     <<"accepted socket connection closed"<<endl;
		pThrParam->pBrokerageHouse->LogErrorMessage(osErr.str());
		delete pErr;
	}	
}

// Constructor
CBrokerageHouse::CBrokerageHouse(const char *szHost, const char *szDBName, 
					const char *szPostmasterPort, const int iListenPort)
: m_iListenPort(iListenPort)
{
	memset(m_szHost, 0, sizeof(m_szHost));
	strncpy(m_szHost, szHost, sizeof(m_szHost) - 1);

	memset(m_szDBName, 0, sizeof(m_szDBName));
	strncpy(m_szDBName, szDBName, sizeof(m_szDBName) - 1);

	memset(m_szPostmasterPort, 0, sizeof(m_szPostmasterPort));
	strncpy(m_szPostmasterPort, szPostmasterPort, sizeof(m_szPostmasterPort) - 1);

	m_fLog.open("BrokerageHouse_Error.log", ios::out);
}

// Destructor
CBrokerageHouse::~CBrokerageHouse()
{
	m_fLog.close();
}

// Run Broker Volume transaction
INT32 CBrokerageHouse::RunBrokerVolume( PBrokerVolumeTxnInput pTxnInput, CBrokerVolume &BrokerVolume )
{
	TBrokerVolumeTxnOutput	TxnOutput;
	BrokerVolume.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Run Customer Position transaction
INT32 CBrokerageHouse::RunCustomerPosition( PCustomerPositionTxnInput pTxnInput, CCustomerPosition &CustomerPosition )
{
	TCustomerPositionTxnOutput	TxnOutput;
	CustomerPosition.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Run Data Maintenance transaction
INT32 CBrokerageHouse::RunDataMaintenance( PDataMaintenanceTxnInput pTxnInput, CDataMaintenance &DataMaintenance )
{
	TDataMaintenanceTxnOutput	TxnOutput;
	DataMaintenance.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}


// Run Market Feed transaction
INT32 CBrokerageHouse::RunMarketFeed( PMarketFeedTxnInput pTxnInput, CMarketFeed &MarketFeed )
{
	TMarketFeedTxnOutput	TxnOutput;
	MarketFeed.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Run Market Watch transaction
INT32 CBrokerageHouse::RunMarketWatch( PMarketWatchTxnInput pTxnInput, CMarketWatch &MarketWatch )
{
	TMarketWatchTxnOutput	TxnOutput;
	MarketWatch.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Run Security Detail transaction
INT32 CBrokerageHouse::RunSecurityDetail( PSecurityDetailTxnInput pTxnInput, CSecurityDetail &SecurityDetail )
{
	TSecurityDetailTxnOutput	TxnOutput;
	SecurityDetail.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Run Trade Lookup transaction
INT32 CBrokerageHouse::RunTradeLookup( PTradeLookupTxnInput pTxnInput, CTradeLookup &TradeLookup )
{
	TTradeLookupTxnOutput	TxnOutput;
	TradeLookup.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Run Trade Order transaction
INT32 CBrokerageHouse::RunTradeOrder( PTradeOrderTxnInput pTxnInput, CTradeOrder &TradeOrder )
{
	TTradeOrderTxnOutput	TxnOutput;
	TradeOrder.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Run Trade Result transaction
INT32 CBrokerageHouse::RunTradeResult( PTradeResultTxnInput pTxnInput, CTradeResult &TradeResult )
{
	TTradeResultTxnOutput	TxnOutput;
	TradeResult.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Run Trade Status transaction
INT32 CBrokerageHouse::RunTradeStatus( PTradeStatusTxnInput pTxnInput, CTradeStatus &TradeStatus )
{
	TTradeStatusTxnOutput	TxnOutput;
	TradeStatus.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Run Trade Update transaction
INT32 CBrokerageHouse::RunTradeUpdate( PTradeUpdateTxnInput pTxnInput, CTradeUpdate &TradeUpdate )
{
	TTradeUpdateTxnOutput	TxnOutput;
	TradeUpdate.DoTxn( pTxnInput, &TxnOutput );

	return( TxnOutput.status );
}

// Listener
void CBrokerageHouse::Listener( void )
{
	int acc_socket;
	PThreadParameter pThrParam;
	
	m_Socket.Listen( m_iListenPort );

	while(true)
	{
		acc_socket = 0;
		try
		{
			acc_socket = m_Socket.Accept();
	
			pThrParam = new TThreadParameter;
			memset(pThrParam, 0, sizeof(TThreadParameter));   // zero the structure
	
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
