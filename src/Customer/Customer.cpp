/*
 * Customer.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 03 August 2006
 */

#include <Customer.h>
#include <transactions.h>

using namespace TPCE;

// Constructor
CCustomer::CCustomer(char* szInDir,
		TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
		INT32 iScaleFactor, INT32 iDaysOfInitialTrades, UINT32 UniqueId,
		char* szBHaddr, int iBHlistenPort, int iUsers, int iPacingDelay,
		char* outputDirectory, ofstream *m_fMix, CSyncLock *m_MixLock)
: m_iUsers(iUsers), m_iPacingDelay(iPacingDelay)
{
	char filename[1024];
	sprintf(filename, "%s/Customer_%d.log", outputDirectory, pthread_self());
	m_pLog = new CEGenLogger(eDriverEGenLoader, 0, filename, &m_fmt);
	m_pDriverCETxnSettings = new TDriverCETxnSettings;
	m_InputFiles.Initialize(eDriverEGenLoader, iConfiguredCustomerCount,
			iActiveCustomerCount, szInDir);

	sprintf(filename, "%s/Customer_Error_%d.log", outputDirectory,
			pthread_self());
	m_fLog.open(filename, ios::out);

	// initialize CESUT interface
	m_pCCESUT = new CCESUT(szBHaddr, iBHlistenPort, &m_fLog, m_fMix,
			&m_LogLock, m_MixLock);

	// initialize CE - Customer Emulator
	m_pCCE = new CCE(m_pCCESUT, m_pLog, m_InputFiles,
			iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor,
			iDaysOfInitialTrades, UniqueId, m_pDriverCETxnSettings);
}

// Destructor
CCustomer::~CCustomer()
{
	delete m_pCCE;
	delete m_pCCESUT;

	m_fLog.close();

	delete m_pDriverCETxnSettings;
	delete m_pLog;
}

void CCustomer::DoTxn()
{
	m_pCCE->DoTxn();
}

// LogErrorMessage
void CCustomer::LogErrorMessage( const string sErr )
{
	m_LogLock.ClaimLock();
	cout<<sErr;
	m_fLog<<sErr;
	m_fLog.flush();
	m_LogLock.ReleaseLock();
}
