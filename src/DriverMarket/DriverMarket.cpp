/*
 * DriverMarket.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 30 July 2006
 */

#include <transactions.h>

using namespace TPCE;

// worker thread
void* TPCE::MarketWorkerThread(void* data)
{
	PMarketThreadParam pThrParam = reinterpret_cast<PMarketThreadParam>(data);

	CSocket sockDrv;
	sockDrv.SetSocketfd( pThrParam->iSockfd );	// client socket

	PTradeRequest pMessage = new TTradeRequest;
	memset(pMessage, 0, sizeof(TTradeRequest));   // zero the structure

	sockDrv.Receive( reinterpret_cast<void*>(pMessage), sizeof(TTradeRequest));

	//close connection
	close(pThrParam->iSockfd);
	
	// submit trade request
	pThrParam->pDriverMarket->m_pCMEE->SubmitTradeRequest( pMessage );

	return NULL;
}

// entry point for worker thread
void TPCE::EntryMarketWorkerThread(void* data)
{
	//thread
	pthread_t threadID; // thread ID
	int status; // error code
	pthread_attr_t threadAttribute; // thread attribute

	status = pthread_attr_init(&threadAttribute); // initialize the attribute object
	if (status != 0)
	{
		cout<<"pthread_attr_init failed, status = "<<status<<endl;
		//return false;
	}

	// set the detachstate attribute to detached
	status = pthread_attr_setdetachstate(&threadAttribute, PTHREAD_CREATE_DETACHED);
	if (status != 0)
	{
		cout<<"pthread_attr_setdetachstate failed, status = "<<status<<endl;
		//return false;
	}

	// create the thread in the detached state
	status = pthread_create(&threadID, &threadAttribute, &MarketWorkerThread, data);
						//reinterpret_cast<void*>( ));
	cout<<"thread id="<<threadID<<endl;
	if (status != 0)
	{
		cout<<"pthread_create failed, status = "<<status<<endl;
		//return false;
	}

}

// Constructor
CDriverMarket::CDriverMarket(char* szFileLoc, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount, int iListenPort)
: m_iListenPort(iListenPort)
{
	m_pLog = new CEGenLogger(eDriverEGenLoader, 0, "Market.log", &m_fmt);

	// Initialize MEESUT
	m_pCMEESUT = new CMEESUT();
	
	// Initialize SecurityFile
	m_pSecurities = new CSecurityFile(szFileLoc, iConfiguredCustomerCount, iActiveCustomerCount);

	// Initialize MEE
	m_pCMEE = new CMEE( 0, m_pCMEESUT, m_pLog, m_pSecurities, 1 );
	m_pCMEE->SetBaseTime();
}

// Destructor
CDriverMarket::~CDriverMarket()
{
	delete m_pCMEE;
	delete m_pSecurities;
	delete m_pCMEESUT;
	delete m_pLog;
}

// Listener
void CDriverMarket::Listener( void )
{
	int acc_socket;
	PMarketThreadParam pThrParam;
	
	m_Socket.Listen( m_iListenPort );

	while(true)
	{
		acc_socket = 0;
		acc_socket = m_Socket.Accept();

		// create new parameter structure
		pThrParam = new TMarketThreadParam;
		memset(pThrParam, 0, sizeof(TMarketThreadParam));   // zero the structure

		pThrParam->iSockfd = acc_socket;
		pThrParam->pDriverMarket = this;

		// call entry point
		EntryMarketWorkerThread( reinterpret_cast<void*>(pThrParam) );
	}

}

// ThrowError
void CDriverMarket::ThrowError()
{
	//throw new;
}
