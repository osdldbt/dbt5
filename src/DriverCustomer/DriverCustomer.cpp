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

	struct timespec ts, rem;

	ts.tv_sec = (time_t) (pThrParam->pDriverCustomer->m_iPacingDelay / 1000);
	ts.tv_nsec = (long) (pThrParam->pDriverCustomer->m_iPacingDelay % 1000) * 1000000;

	while (true)
	{
		cout<<"thread = "<<pthread_self()<<"==> ";
		pThrParam->pDriverCustomer->m_pCCE->DoTxn();

		// wait for pacing delay -- this delays happens after the mix logging
		while (nanosleep(&ts, &rem) == -1) {
			if (errno == EINTR) {
				memcpy(&ts, &rem, sizeof(timespec));
			} else {
				ostringstream osErr;
				osErr<<"pacing delay time invalid "<<ts.tv_sec<<" s "<<ts.tv_nsec<<" ns"<<endl;
				pThrParam->pDriverCustomer->LogErrorMessage(osErr.str());
				break;
			}
		}
	}

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

	m_InputFiles.Initialize(eDriverEGenLoader, iConfiguredCustomerCount, iActiveCustomerCount, szInDir);

	m_fLog.open("DriverCustomer_Error.log", ios::out);
	m_fMix.open(CE_MIX_LOG_NAME, ios::out);
	// initialize CESUT interface
	m_pCCESUT = new CCESUT(iBHlistenPort, &m_fLog, &m_fMix);

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
	m_fLog.close();
	m_fMix.close();
}

// RunTest
void CDriverCustomer::RunTest(int iSleep)
{
	struct timespec ts, rem;

	ts.tv_sec = (time_t) (iSleep / 1000);
	ts.tv_nsec = (long) (iSleep % 1000) * 1000000;

	PCustomerThreadParam pThrParam;

	g_tid = (pthread_t*) malloc(sizeof(pthread_t) * m_iUsers);

	LogErrorMessage(">> Start of ramp-up.\n");
	for (int i = 1; i <= m_iUsers; i++)
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

	m_MixLock.ClaimLock();
	m_fMix<<(int)time(NULL)<<",START"<<endl;
	m_MixLock.ReleaseLock();

	LogErrorMessage(">> End of ramp-up.\n");

	// wait until all threads quit
	for (int i = 1; i <= m_iUsers; i++) 
	{
		if (pthread_join(g_tid[i], NULL) != 0)
		{
			throw new CThreadErr( CThreadErr::ERR_THREAD_JOIN, "DriverCustomer::RunTest" );
		}
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
