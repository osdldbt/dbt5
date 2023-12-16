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

#include "Customer.h"

#include "CE.h"

// Constructor
CCustomer::CCustomer(const DataFileManager &inputFiles, char *szInDir,
		TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
		INT32 iScaleFactor, INT32 iDaysOfInitialTrades, UINT32 iSeed,
		char *szBHaddr, int iBHlistenPort, UINT32 UniqueId, int iPacingDelay,
		char *outputDirectory)
: m_UniqueId(UniqueId), m_iPacingDelay(iPacingDelay)
{
	pid_t pid = syscall(SYS_gettid);
	char filename[iMaxPath + 1];
	snprintf(filename, iMaxPath, "%s/Customer_%d.log", outputDirectory, pid);
	m_pLog = new CEGenLogger(eDriverEGenLoader, 0, filename, &m_fmt);

	// initialize CESUT interface
	m_pCCESUT = new CCESUT(outputDirectory, szBHaddr, iBHlistenPort);

	// initialize CE - Customer Emulator
	if (iSeed == 0) {
		m_pCCE = new CCE(m_pCCESUT, m_pLog, inputFiles,
				iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor,
				iDaysOfInitialTrades, UniqueId);
	} else {
		// Specifying the random number generator seed is considered an
		// invalid run.
		// FIXME: Allow the TxnMixRNGSeed and TxnInputRGNSeed to be set.
		m_pCCE = new CCE(m_pCCESUT, m_pLog, inputFiles,
				iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor,
				iDaysOfInitialTrades, UniqueId, iSeed, iSeed);
	}
}

// Destructor
CCustomer::~CCustomer()
{
	delete m_pCCE;
	delete m_pCCESUT;
	delete m_pLog;
}

void
CCustomer::DoTxn()
{
	m_pCCE->DoTxn();
}

void
CCustomer::LogStopTime()
{
	m_pCCESUT->logStopTime();
}
