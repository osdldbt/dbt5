/*
 * DriverCustomer.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 03 August 2006
 */

#include <transactions.h>
#include <Customer.h>

using namespace TPCE;

// global variables
pthread_t* g_tid = NULL;
int stop_time = 0;

// Constructor
CDriver::CDriver(char* szInDir,
		TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
		INT32 iScaleFactor, INT32 iDaysOfInitialTrades, UINT32 UniqueId,
		char* szBHaddr, int iBHlistenPort, int iUsers, int iPacingDelay,
		char* outputDirectory)
: m_iUsers(iUsers), m_iPacingDelay(iPacingDelay)
{
	char filename[1024];
	sprintf(filename, "%s/Driver.log", outputDirectory);
	m_pLog = new CEGenLogger(eDriverEGenLoader, 0, filename, &m_fmt);
	m_pDriverCETxnSettings = new TDriverCETxnSettings;
	m_InputFiles.Initialize(eDriverEGenLoader, iConfiguredCustomerCount,
			iActiveCustomerCount, szInDir);

	sprintf(filename, "%s/Driver_Error.log", outputDirectory);
	m_fLog.open(filename, ios::out);
	sprintf(filename, "%s/%s", outputDirectory, CE_MIX_LOG_NAME);
	m_fMix.open(filename, ios::out);

	strcpy(this->szInDir, szInDir);
	this->iConfiguredCustomerCount = iConfiguredCustomerCount;
	this->iActiveCustomerCount = iActiveCustomerCount;
	this->iScaleFactor = iScaleFactor;
	this->iDaysOfInitialTrades = iDaysOfInitialTrades;
	this->UniqueId = UniqueId;
	strcpy(this->szBHaddr, szBHaddr);
	this->iBHlistenPort = iBHlistenPort;
	this->iUsers = iUsers;
	this->iPacingDelay = iPacingDelay;
	strcpy(this->outputDirectory, outputDirectory);
	//
	// initialize DMSUT interface
	m_pCDMSUT = new CDMSUT(szBHaddr, iBHlistenPort, &m_fLog, &m_fMix,
			&m_LogLock, &m_MixLock);

	// initialize DM - Data Maintenance
	m_pCDM = new CDM(m_pCDMSUT, m_pLog, m_InputFiles,
			iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor,
			iDaysOfInitialTrades, UniqueId);
}

void* TPCE::CustomerWorkerThread(void* data)
{
	CCustomer *customer;
	PCustomerThreadParam pThrParam =
			reinterpret_cast<PCustomerThreadParam>(data);

	ostringstream osErr;
	struct timespec ts, rem;

	ts.tv_sec = (time_t) (pThrParam->pDriver->m_iPacingDelay / 1000);
	ts.tv_nsec = (long) (pThrParam->pDriver->m_iPacingDelay % 1000) *
			1000000;

	customer = new CCustomer(pThrParam->pDriver->szInDir,
			pThrParam->pDriver->iConfiguredCustomerCount,
			pThrParam->pDriver->iActiveCustomerCount,
			pThrParam->pDriver->iScaleFactor,
			pThrParam->pDriver->iDaysOfInitialTrades,
			pThrParam->pDriver->UniqueId,
			pThrParam->pDriver->szBHaddr,
			pThrParam->pDriver->iBHlistenPort,
			pThrParam->pDriver->iUsers,
			pThrParam->pDriver->iPacingDelay,
			pThrParam->pDriver->outputDirectory,
			&pThrParam->pDriver->m_fMix,
			&pThrParam->pDriver->m_MixLock);
	do
	{
		customer->DoTxn();

		// wait for pacing delay -- this delays happens after the mix logging
		while (nanosleep(&ts, &rem) == -1) {
			if (errno == EINTR) {
				memcpy(&ts, &rem, sizeof(timespec));
			} else {
				osErr << "pacing delay time invalid " << ts.tv_sec << " s "
						<< ts.tv_nsec << " ns" << endl;
				pThrParam->pDriver->LogErrorMessage(osErr.str());
				break;
			}
		}
	} while (time(NULL) < stop_time);

	osErr<<"User thread # "<<pthread_self()<<" terminated."<<endl;
	pThrParam->pDriver->LogErrorMessage(osErr.str());

	delete pThrParam;
	return NULL;
}

// entry point for worker thread
void TPCE::EntryCustomerWorkerThread(void* data, int iThrNumber)
{
	PCustomerThreadParam pThrParam =
			reinterpret_cast<PCustomerThreadParam>(data);
	pthread_attr_t threadAttribute; // thread attribute

	try
	{
		// initialize the attribute object
		int status = pthread_attr_init(&threadAttribute);
		if (status != 0)
		{
			throw new CThreadErr( CThreadErr::ERR_THREAD_ATTR_INIT );
		}

		// create the thread in the joinable state
		status = pthread_create(&g_tid[iThrNumber], &threadAttribute,
				&CustomerWorkerThread, data);

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
		pThrParam->pDriver->LogErrorMessage(osErr.str());
		delete pErr;
	}
}

// Destructor
CDriver::~CDriver()
{
	delete m_pCDM;
	delete m_pCDMSUT;

	m_fMix.close();
	m_fLog.close();

	delete m_pDriverCETxnSettings;
	delete m_pLog;
}

// RunTest
void CDriver::RunTest(int iSleep, int iTestDuration)
{
	g_tid = (pthread_t*) malloc(sizeof(pthread_t) * m_iUsers);

	// time to sleep between thread creation
	struct timespec ts, rem;
	ts.tv_sec = (time_t) (iSleep / 1000);
	ts.tv_nsec = (long) (iSleep % 1000) * 1000000;

	// Caulculate when the test should stop.
	int threads_start_time =
			(int) ((double) iSleep / 1000.0 * (double) m_iUsers);
	stop_time = time(NULL) + iTestDuration + threads_start_time;

	CDateTime dtAux;
	dtAux.SetToCurrent();

	cout<<"Test is starting at "<<dtAux.ToStr(02)<<endl
	    <<"Duration of ramp-up: "<<threads_start_time<<" seconds"<<endl;

	dtAux.AddMinutes((iTestDuration + threads_start_time)/60);
	cout<<"Test will stop at "<<dtAux.ToStr(02)<<endl;

	// before starting the test run Trade-Cleanup transaction
	cout << endl <<
			"Running Trade-Cleanup transaction before starting the test..." <<
			endl;
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
		// zero the structure
		memset(pThrParam, 0, sizeof(TCustomerThreadParam));
		pThrParam->pDriver = this;

		EntryCustomerWorkerThread( reinterpret_cast<void*>(pThrParam), i );

		// Sleep for between starting terminals
		while (nanosleep(&ts, &rem) == -1) {
			if (errno == EINTR) {
				memcpy(&ts, &rem, sizeof(timespec));
			} else {
				ostringstream osErr;
				osErr << "sleep time invalid " << ts.tv_sec << " s " <<
						ts.tv_nsec << " ns" << endl;
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
	// 0 represents the Data-Maintenance thread
	for (int i = 0; i <= m_iUsers; i++)
	{
		if (pthread_join(g_tid[i], NULL) != 0)
		{
			throw new CThreadErr( CThreadErr::ERR_THREAD_JOIN,
					"Driver::RunTest" );
		}
	}
}


// DM worker thread
void* TPCE::DMWorkerThread(void* data)
{
	PCustomerThreadParam pThrParam =
			reinterpret_cast<PCustomerThreadParam>(data);

	do
	{
		pThrParam->pDriver->m_pCDM->DoTxn();
		sleep(60);	// Data-Maintenance runs once a minute
	} while (time(NULL) < stop_time);

	pThrParam->pDriver->LogErrorMessage(
			"Data-Maintenance thread terminated.\n");
	delete pThrParam;

	return NULL;
}

// entry point for worker thread
void TPCE::EntryDMWorkerThread(CDriver* ptr)
{
	PCustomerThreadParam pThrParam = new TCustomerThreadParam;
	memset(pThrParam, 0, sizeof(TCustomerThreadParam));   // zero the structure
	pThrParam->pDriver = ptr;

	pthread_attr_t threadAttribute; // thread attribute

	try
	{
		// initialize the attribute object
		int status = pthread_attr_init(&threadAttribute);
		if (status != 0)
		{
			throw new CThreadErr( CThreadErr::ERR_THREAD_ATTR_INIT );
		}

		// create the thread in the joinable state
		status = pthread_create(&g_tid[0], &threadAttribute, &DMWorkerThread,
				reinterpret_cast<void*>(pThrParam));

		if (status != 0)
		{
			throw new CThreadErr( CThreadErr::ERR_THREAD_CREATE );
		}
		pThrParam->pDriver->LogErrorMessage(
				">> Data-Maintenance thread started.\n");
	}
	catch(CThreadErr *pErr)
	{
		pThrParam->pDriver->LogErrorMessage(
				"Data-Maintenance Thread not created");
		delete pErr;
	}
}

// LogErrorMessage
void CDriver::LogErrorMessage( const string sErr )
{
	m_LogLock.ClaimLock();
	cout<<sErr;
	m_fLog<<sErr;
	m_fLog.flush();
	m_LogLock.ReleaseLock();
}
