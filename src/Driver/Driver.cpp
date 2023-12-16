/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 03 August 2006
 */

#include <unistd.h>
#include <sys/syscall.h>

#include "DM.h"

#include "Driver.h"
#include "Customer.h"

// global variables
pthread_t *g_tid = NULL;
int stop_time = 0;

// Constructor
CDriver::CDriver(const DataFileManager &inputFiles, char *szInDir,
		TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
		INT32 iScaleFactor, INT32 iDaysOfInitialTrades, UINT32 iSeed,
		char *szBHaddr, int iBHlistenPort, int iUsers, int iPacingDelay,
		char *outputDirectory)
{
	strncpy(this->szInDir, szInDir, iMaxPath);
	this->szInDir[iMaxPath] = '\0';
	this->iConfiguredCustomerCount = iConfiguredCustomerCount;
	this->iActiveCustomerCount = iActiveCustomerCount;
	this->iScaleFactor = iScaleFactor;
	this->iDaysOfInitialTrades = iDaysOfInitialTrades;
	this->iSeed = iSeed;
	strncpy(this->szBHaddr, szBHaddr, iMaxHostname);
	this->szBHaddr[iMaxHostname] = '\0';
	this->iBHlistenPort = iBHlistenPort;
	this->iUsers = iUsers;
	this->iPacingDelay = iPacingDelay;
	strncpy(this->outputDirectory, outputDirectory, iMaxPath);
	this->outputDirectory[iMaxPath] = '\0';

	char filename[iMaxPath + 1];
	snprintf(filename, iMaxPath, "%s/Driver.log", outputDirectory);
	m_pLog = new CEGenLogger(eDriverEGenLoader, 0, filename, &m_fmt);
	m_pDriverCETxnSettings = new TDriverCETxnSettings;

	snprintf(filename, iMaxPath, "%s/Driver_Error.log", outputDirectory);
	m_fLog.open(filename, ios::out);
	snprintf(filename, iMaxPath, "%s/%s", outputDirectory, CE_MIX_LOG_NAME);
	m_fMix.open(filename, ios::out);

	cout << "initializing data maintenance..." << endl;

	// initialize DMSUT interface
	m_pCDMSUT = new CDMSUT(outputDirectory, szBHaddr, iBHlistenPort);

	// initialize DM - Data Maintenance
	pid_t pid = syscall(SYS_gettid);
	if (iSeed == 0) {
		m_pCDM = new CDM(m_pCDMSUT, m_pLog, inputFiles,
				iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor,
				iDaysOfInitialTrades, pid);
	} else {
		// Specifying the random number generator seed is considered an
		// invalid run.
		m_pCDM = new CDM(m_pCDMSUT, m_pLog, inputFiles,
				iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor,
				iDaysOfInitialTrades, pid, iSeed);
	}
}

void *
customerWorkerThread(void *data)
{
	CCustomer *customer;
	PCustomerThreadParam pThrParam
			= reinterpret_cast<PCustomerThreadParam>(data);

	ostringstream osErr;
	struct timespec ts, rem;

	ts.tv_sec = (time_t) (pThrParam->pDriver->iPacingDelay / 1000);
	ts.tv_nsec = (long) (pThrParam->pDriver->iPacingDelay % 1000) * 1000000;

	const DataFileManager inputFiles(pThrParam->pDriver->szInDir,
			pThrParam->pDriver->iConfiguredCustomerCount,
			pThrParam->pDriver->iActiveCustomerCount,
			TPCE::DataFileManager::IMMEDIATE_LOAD);
	customer = new CCustomer(inputFiles, pThrParam->pDriver->szInDir,
			pThrParam->pDriver->iConfiguredCustomerCount,
			pThrParam->pDriver->iActiveCustomerCount,
			pThrParam->pDriver->iScaleFactor,
			pThrParam->pDriver->iDaysOfInitialTrades,
			pThrParam->pDriver->iSeed, pThrParam->pDriver->szBHaddr,
			pThrParam->pDriver->iBHlistenPort, pThrParam->UniqueId,
			pThrParam->pDriver->iPacingDelay,
			pThrParam->pDriver->outputDirectory);
	do {
		customer->DoTxn();

		// wait for pacing delay -- this delays happens after the mix logging
		while (nanosleep(&ts, &rem) == -1) {
			if (errno == EINTR) {
				memcpy(&ts, &rem, sizeof(timespec));
			} else {
				ostringstream osErr;
				osErr << "pacing delay time invalid " << ts.tv_sec << " s "
					  << ts.tv_nsec << " ns" << endl;
				pThrParam->pDriver->logErrorMessage(osErr.str());
				break;
			}
		}
	} while (time(NULL) < stop_time);

	customer->LogStopTime();

	pid_t pid = syscall(SYS_gettid);
	cout << "User thread # " << pid << " terminated." << endl;

	delete pThrParam;
	return NULL;
}

// entry point for worker thread
void
entryCustomerWorkerThread(void *data)
{
	PCustomerThreadParam pThrParam
			= reinterpret_cast<PCustomerThreadParam>(data);
	pthread_attr_t threadAttribute; // thread attribute

	try {
		// initialize the attribute object
		int status = pthread_attr_init(&threadAttribute);
		if (status != 0) {
			throw new CThreadErr(CThreadErr::ERR_THREAD_ATTR_INIT);
		}

		// create the thread in the joinable state
		status = pthread_create(&g_tid[pThrParam->UniqueId], &threadAttribute,
				&customerWorkerThread, data);

		if (status != 0) {
			throw new CThreadErr(CThreadErr::ERR_THREAD_CREATE);
		}
	} catch (CThreadErr *pErr) {
		cerr << "Thread " << pThrParam->UniqueId << " didn't spawn correctly"
			 << endl
			 << endl
			 << "Error: " << pErr->ErrorText()
			 << " at EntryCustomerWorkerThread" << endl;
		exit(1);
	}
}

// Destructor
CDriver::~CDriver()
{
	delete m_pCDM;
	delete m_pCDMSUT;

	m_fLog.close();

	delete m_pDriverCETxnSettings;
	delete m_pLog;
}

// RunTest
void
CDriver::runTest(int iSleep, int iTestDuration)
{
	// Create enough space for all of the users plus an additional thread for
	// data maintenance.
	g_tid = (pthread_t *) malloc(sizeof(pthread_t) * (iUsers + 1));

	// before starting the test run Trade-Cleanup transaction
	cout << endl
		 << "Running Trade-Cleanup transaction before starting the test..."
		 << endl;
	m_pCDM->DoCleanupTxn();
	cout << "Trade-Cleanup transaction completed." << endl << endl;

	// time to sleep between thread creation, convert from millaseconds to
	// nanoseconds.
	struct timespec ts, rem;
	ts.tv_sec = (time_t) (iSleep / 1000);
	ts.tv_nsec = (long) (iSleep % 1000) * 1000000;

	// Caulculate when the test should stop.
	int threads_start_time
			= (int) ((double) iSleep / 1000.0 * (double) iUsers);
	stop_time = time(NULL) + iTestDuration + threads_start_time;

	CDateTime dtAux;

	cout << "Test is starting at " << dtAux.ToStr(02) << endl
		 << "Estimated duration of ramp-up: " << threads_start_time
		 << " seconds" << endl;

	dtAux.AddMinutes((iTestDuration + threads_start_time) / 60);
	cout << "Estimated end time " << dtAux.ToStr(02) << endl;

	cout << ">> Start of ramp-up." << endl;

	// start thread that runs the Data Maintenance transaction
	entryDMWorkerThread(this);

	for (int i = 1; i <= iUsers; i++) {
		// parameter for the new thread
		PCustomerThreadParam pThrParam = new TCustomerThreadParam;

		// zero the structure
		pThrParam->UniqueId = i;
		pThrParam->pDriver = this;

		entryCustomerWorkerThread(reinterpret_cast<void *>(pThrParam));

		// Sleep for between starting terminals
		while (nanosleep(&ts, &rem) == -1) {
			if (errno == EINTR) {
				memcpy(&ts, &rem, sizeof(timespec));
			} else {
				ostringstream osErr;
				osErr << "sleep time invalid " << ts.tv_sec << " s "
					  << ts.tv_nsec << " ns" << endl;
				logErrorMessage(osErr.str());
				break;
			}
		}
	}

	// mark end of ramp-up
	pid_t pid = syscall(SYS_gettid);
	m_fMix << (int) time(NULL) << ",START,,," << pid << endl;

	cout << ">> End of ramp-up." << endl;

	// wait until all threads quit
	// 0 represents the Data-Maintenance thread
	for (int i = 0; i <= iUsers; i++) {
		if (pthread_join(g_tid[i], NULL) != 0) {
			throw new CThreadErr(
					CThreadErr::ERR_THREAD_JOIN, "Driver::RunTest");
		}
	}
}

// DM worker thread
void *
dmWorkerThread(void *data)
{
	PCustomerThreadParam pThrParam
			= reinterpret_cast<PCustomerThreadParam>(data);

	// The Data-Maintenance transaction must run once per minute.
	// FIXME: What do we do if the transaction takes more than a minute
	// to run?
	time_t start_time;
	time_t end_time;
	unsigned int remaining;
	do {
		start_time = time(NULL);
		pThrParam->pDriver->m_pCDM->DoTxn();
		end_time = time(NULL);
		remaining = 60 - (end_time - start_time);
		if (end_time < stop_time && remaining > 0)
			sleep(remaining);
	} while (end_time < stop_time);

	pThrParam->pDriver->m_pCDMSUT->logStopTime();

	cout << "Data-Maintenance thread stopped." << endl;
	delete pThrParam;

	return NULL;
}

// entry point for worker thread
void
entryDMWorkerThread(CDriver *ptr)
{
	PCustomerThreadParam pThrParam = new TCustomerThreadParam;
	pThrParam->pDriver = ptr;

	pthread_attr_t threadAttribute; // thread attribute

	try {
		// initialize the attribute object
		pthread_attr_init(&threadAttribute);

		// create the thread in the joinable state
		pthread_create(&g_tid[0], &threadAttribute, &dmWorkerThread,
				reinterpret_cast<void *>(pThrParam));

		cout << ">> Data-Maintenance thread started." << endl;
	} catch (CThreadErr *pErr) {
		cerr << "Data-Maintenance thread not created successfully, exiting..."
			 << endl;
		exit(1);
	}
}

// logErrorMessage
void
CDriver::logErrorMessage(const string sErr)
{
	m_LogLock.lock();
	cout << sErr;
	m_fLog << sErr;
	m_fLog.flush();
	m_LogLock.unlock();
}
