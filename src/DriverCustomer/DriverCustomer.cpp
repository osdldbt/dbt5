/*
 * DriverCustomer.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 03 August 2006
 */

#include <transactions.h>

using namespace TPCE;

pthread_t* g_tid = NULL;

void* TPCE::CustomerWorkerThread(void* data)
{
	PCustomerThreadParam pThrParam = reinterpret_cast<PCustomerThreadParam>(data);

	while (true)
	{
		cout<<"thread = "<<pthread_self()<<"==> ";
		pThrParam->pDriverCustomer->m_pCCE->DoTxn();
		sleep(3); //pacing delay
	}

	return NULL;
}

// entry point for worker thread
void TPCE::EntryCustomerWorkerThread(void* data, int iThrNumber)
{
	pthread_attr_t threadAttribute; // thread attribute

	try
	{
		int status = pthread_attr_init(&threadAttribute); // initialize the attribute object
		if (status != 0)
		{
			throw new CThreadErr( CThreadErr::ERR_THREAD_ATTR_INIT );
		}
	
		// create the thread in the joinable state
		status = pthread_create(&g_tid[iThrNumber], &threadAttribute, &CustomerWorkerThread, data);
					
		if (status != 0)
		{
			throw new CThreadErr( CThreadErr::ERR_THREAD_CREATE );
		}
	}
	catch(CThreadErr *pErr)
	{
		cout<<"Thread "<<iThrNumber<<" didn't spawn correctly"<<endl
		    <<endl<<"Error: "<<pErr->ErrorText()
		    <<" at "<<"TPCE::EntryCustomerWorkerThread"<<endl;

		delete pErr;
	}
}

// Constructor
CDriverCustomer::CDriverCustomer(char* szInDir, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
				INT32 iScaleFactor, INT32 iDaysOfInitialTrades, UINT32 UniqueId, int iBHlistenPort)
{
	m_pLog = new CEGenLogger(eDriverEGenLoader, 0, "DriverCustomer.log", &m_fmt);

	m_InputFiles.Initialize(eDriverEGenLoader, iConfiguredCustomerCount, iActiveCustomerCount, szInDir);

	// initialize CESUT interface
	m_pCCESUT = new CCESUT(iBHlistenPort);

	m_pDriverCETxnSettings = new TDriverCETxnSettings;
	// initialize CE - Customer Emulator
	m_pCCE = new CCE( m_pCCESUT, m_pLog, m_InputFiles, iConfiguredCustomerCount, iActiveCustomerCount, 
					iScaleFactor, iDaysOfInitialTrades, UniqueId, m_pDriverCETxnSettings );
}

// Destructor
CDriverCustomer::~CDriverCustomer()
{
	delete m_pCCE;
	delete m_pDriverCETxnSettings;
	delete m_pCCESUT;
	delete m_pLog;
}

// RunTest
void CDriverCustomer::RunTest(int iSleep)
{
	struct timespec ts, rem;

	ts.tv_sec = (time_t) (iSleep / 1000);
	ts.tv_nsec = (long) (iSleep % 1000) * 1000000;

	PCustomerThreadParam pThrParam;
	int j = 2; // it would be the number of customers... I think

	g_tid = (pthread_t*) malloc(sizeof(pthread_t) * j);

	for (int i=0; i<j; i++)
	{
		pThrParam = new TCustomerThreadParam;
		memset(pThrParam, 0, sizeof(TCustomerThreadParam));   // zero the structure
		pThrParam->pDriverCustomer = this;

		EntryCustomerWorkerThread( reinterpret_cast<void*>(pThrParam), i );

		// Sleep for between starting terminals
		while (nanosleep(&ts, &rem) == -1) {
			if (errno == EINTR) {
				memcpy(&ts, &rem, sizeof(timespec));
			} else {
				cout<<"sleep time invalid "<<ts.tv_sec<<" s "<<ts.tv_nsec<<" ns"<<endl;
				break;
			}
		}
	}

	// wait until all threads quit
	for (int i = 0; i < j; i++) 
	{
		if (pthread_join(g_tid[i], NULL) != 0)
		{
			throw new CThreadErr( CThreadErr::ERR_THREAD_JOIN, "DriverCustomer::RunTest" );
		}
	}
}

