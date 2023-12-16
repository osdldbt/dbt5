/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * This class represents the workload driver
 * 03 August 2006
 */

#ifndef CUSTOMER_H
#define CUSTOMER_H

#include "EGenLogger.h"
#include "locking.h"

#include "CESUT.h"
using namespace TPCE;

class CCustomer
{
	UINT32 m_UniqueId;
	int m_iPacingDelay;
	CLogFormatTab m_fmt;
	CEGenLogger *m_pLog;
	CCESUT *m_pCCESUT;
	CCE *m_pCCE;

private:
	friend void *CustomerWorkerThread(void *);
	// entry point for driver worker thread
	friend void EntryCustomerWorkerThread(void *, int);

	friend void *DMWorkerThread(void *);
	friend void EntryDMWorkerThread(CCustomer *);

public:
	CCustomer(const DataFileManager &, char *szInDir,
			TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
			INT32 iScaleFactor, INT32 iDaysOfInitialTrades, UINT32 iSeed,
			char *szBHaddr, int iBHlistenPort, UINT32 UniqueId,
			int iPacingDelay, char *outputDirectory);
	~CCustomer();

	void DoTxn();
	void RunTest(int, int);
	void LogStopTime();
};

#endif // CUSTOMER_H
