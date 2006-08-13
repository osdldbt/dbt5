/*
 * DriverCustomer.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 03 August 2006
 */

#include <transactions.h>

using namespace TPCE;

// global variables
pthread_t* g_tid = NULL;
int stop_time = 0;

void* TPCE::CustomerWorkerThread(void* data)
{
	PCustomerThreadParam pThrParam = reinterpret_cast<PCustomerThreadParam>(data);

	ostringstream osErr;
	struct timespec ts, rem;

	ts.tv_sec = (time_t) (pThrParam->pDriverCustomer->m_iPacingDelay / 1000);
	ts.tv_nsec = (long) (pThrParam->pDriverCustomer->m_iPacingDelay % 1000) * 1000000;

	do
	{
		//cout<<"thread = "<<pthread_self()<<"==> ";
		pThrParam->pDriverCustomer->m_pCCE->DoTxn();

		// wait for pacing delay -- this delays happens after the mix logging
		while (nanosleep(&ts, &rem) == -1) {
			if (errno == EINTR) {
				memcpy(&ts, &rem, sizeof(timespec));
			} else {
				osErr<<"pacing delay time invalid "<<ts.tv_sec<<" s "<<ts.tv_nsec<<" ns"<<endl;
				pThrParam->pDriverCustomer->LogErrorMessage(osErr.str());
				break;
			}
		}
	} while (time(NULL) < stop_time);

	osErr<<"User thread # "<<pthread_self()<<" terminated."<<endl;
	pThrParam->pDriverCustomer->LogErrorMessage(osErr.str());

	delete pThrParam;
	return NULL;
}

// entry point for worker thread
void TPCE::EntryCustomerWorkerThread(void* data, int iThrNumber)
{
	PCustomerThreadParam pThrParam = reinterpret_cast<PCustomerThreadParam>(data);
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
		ostringstream osErr;
		osErr<<"Thread "<<iThrNumber<<" didn't spawn correctly"<<endl
		     <<endl<<"Error: "<<pErr->ErrorText()
		     <<" at "<<"TPCE::EntryCustomerWorkerThread"<<endl;
		pThrParam->pDriverCustomer->LogErrorMessage(osErr.str());
		delete pErr;
	}
}

// Constructor
CDriverCustomer::CDriverCustomer(char* szInDir, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
				INT32 iScaleFactor, INT32 iDaysOfInitialTrades, UINT32 UniqueId, 
				int iBHlistenPort, int iUsers, int iPacingDelay)
: m_iUsers(iUsers),
  m_iPacingDelay(iPacingDelay)
{
	m_pLog = new CEGenLogger(eDriverEGenLoader, 0, "DriverCustomer.log", &m_fmt);
	m_pDriverCETxnSettings = new TDriverCETxnSettings;
	m_InputFiles.Initialize(eDriverEGenLoader, iConfiguredCustomerCount, iActiveCustomerCount, szInDir);

	m_fLog.open("DriverCustomer_Error.log", ios::out);
	m_fMix.open(CE_MIX_LOG_NAME, ios::out);

	// initialize CESUT interface
	m_pCCESUT = new CCESUT(iBHlistenPort, &m_fLog, &m_fMix, &m_LogLock, &m_MixLock);

	// initialize CE - Customer Emulator
	m_pCCE = new CCE( m_pCCESUT, m_pLog, m_InputFiles, iConfiguredCustomerCount, iActiveCustomerCount, 
					iScaleFactor, iDaysOfInitialTrades, UniqueId, m_pDriverCETxnSettings );

	// initialize DMSUT interface
	m_pCDMSUT = new CDMSUT(iBHlistenPort, &m_fLog, &m_fMix, &m_LogLock, &m_MixLock);

	// initialize DM - Data Maintenance
	m_pCDM = new CDM( m_pCDMSUT, m_pLog, m_InputFiles, iConfiguredCustomerCount, iActiveCustomerCount,
					iScaleFactor, iDaysOfInitialTrades, UniqueId );
}

// Destructor
CDriverCustomer::~CDriverCustomer()
{
	delete m_pCDM;
	delete m_pCDMSUT;
	delete m_pCCE;
	delete m_pCCESUT;

	m_fMix.close();
	m_fLog.close();
	
	delete m_pDriverCETxnSettings;
	delete m_pLog;
}

// RunTest
void CDriverCustomer::RunTest(int iSleep, int iTestDuration)
{
	g_tid = (pthread_t*) malloc(sizeof(pthread_t) * m_iUsers);

	// time to sleep between thread creation
	struct timespec ts, rem;
	ts.tv_sec = (time_t) (iSleep / 1000);
	ts.tv_nsec = (long) (iSleep % 1000) * 1000000;

	// Caulculate when the test should stop.
	int threads_start_time = (int) ((double) iSleep / 1000.0 * (double) m_iUsers);
	stop_time = time(NULL) + iTestDuration + threads_start_time;

	CDateTime dtAux;
	dtAux.SetToCurrent();

	cout<<"Test is starting at "<<dtAux.ToStr(02)<<endl
	    <<"Duration of ramp-up: "<<threads_start_time<<" seconds"<<endl;

	dtAux.AddMinutes((iTestDuration + threads_start_time)/60);
	cout<<"Test will stop at "<<dtAux.ToStr(02)<<endl;

	// before starting the test run Trade-Cleanup transaction
	cout<<endl<<"Running Trade-Cleanup transaction before starting the test..."<<endl;
	m_pCDM->DoCleanupTxn();
	cout<<"Trade-Cleanup transaction completed."<<endl<<endl;

	LogErrorMessage(">> Start of ramp-up.\n");

	//start thread that run the Data Maintenance transaction
	EntryDMWorkerThread(this);

	// parameter for the new thread
	PCustomerThreadParam pThrParam;

	for (int i = 1; i <= m_iUsers; i++)  // i=0 is Data-Maintenance
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
				ostringstream osErr;
				osErr<<"sleep time invalid "<<ts.tv_sec<<" s "<<ts.tv_nsec<<" ns"<<endl;
				LogErrorMessage(osErr.str());
				break;
			}
		}
	}

	// mark end of ramp-up
	m_MixLock.ClaimLock();
	m_fMix<<(int)time(NULL)<<",START"<<endl;
	m_MixLock.ReleaseLock();

	LogErrorMessage(">> End of ramp-up.\n\n");

	// wait until all threads quit
	for (int i = 0; i <= m_iUsers; i++)  // 0 represents the Data-Maintenance thread
	{
		if (pthread_join(g_tid[i], NULL) != 0)
		{
			throw new CThreadErr( CThreadErr::ERR_THREAD_JOIN, "DriverCustomer::RunTest" );
		}
	}
}


// DM worker thread
void* TPCE::DMWorkerThread(void* data)
{
	PCustomerThreadParam pThrParam = reinterpret_cast<PCustomerThreadParam>(data);

	do
	{
		pThrParam->pDriverCustomer->m_pCDM->DoTxn();
		sleep(60);	// Data-Maintenance runs once a minute
	} while (time(NULL) < stop_time);

	pThrParam->pDriverCustomer->LogErrorMessage("Data-Maintenance thread terminated.\n");
	delete pThrParam;
	
	return NULL;
}

// entry point for worker thread
void TPCE::EntryDMWorkerThread(CDriverCustomer* ptr)
{
	PCustomerThreadParam pThrParam = new TCustomerThreadParam;
	memset(pThrParam, 0, sizeof(TCustomerThreadParam));   // zero the structure
	pThrParam->pDriverCustomer = ptr;

	pthread_attr_t threadAttribute; // thread attribute

	try
	{
		int status = pthread_attr_init(&threadAttribute); // initialize the attribute object
		if (status != 0)
		{
			throw new CThreadErr( CThreadErr::ERR_THREAD_ATTR_INIT );
		}
	
		// create the thread in the joinable state
		status = pthread_create(&g_tid[0], &threadAttribute, &DMWorkerThread, reinterpret_cast<void*>(pThrParam));
					
		if (status != 0)
		{
			throw new CThreadErr( CThreadErr::ERR_THREAD_CREATE );
		}
		pThrParam->pDriverCustomer->LogErrorMessage(">> Data-Maintenance thread started.\n");
	}
	catch(CThreadErr *pErr)
	{
		pThrParam->pDriverCustomer->LogErrorMessage("Data-Maintenance Thread not created");
		delete pErr;
	}
}

// LogErrorMessage
void CDriverCustomer::LogErrorMessage( const string sErr )
{
	m_LogLock.ClaimLock();
	cout<<sErr;
	m_fLog<<sErr;
	m_fLog.flush();
	m_LogLock.ReleaseLock();
}
