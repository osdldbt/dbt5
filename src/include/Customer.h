/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2010 Rilson Nascimento
 *
 * This class represents the workload driver
 * 03 August 2006
 */

#ifndef CUSTOMER_H
#define CUSTOMER_H

#include "EGenLogger.h"
#include "InputFlatFilesStructure.h"
#include "locking.h"

#include "CESUT.h"
using namespace TPCE;

class CCustomer
{
	int m_iUsers;
	int m_iPacingDelay;
	CLogFormatTab m_fmt;
	CEGenLogger *m_pLog;
	CInputFiles m_InputFiles;
	CCESUT * m_pCCESUT;
	CCE * m_pCCE;
	PDriverCETxnSettings m_pDriverCETxnSettings;
	CMutex m_LogLock;
	ofstream m_fLog; // error log file

private:
	void LogErrorMessage(const string);

	friend void *CustomerWorkerThread(void *);
	// entry point for driver worker thread
	friend void EntryCustomerWorkerThread(void *, int);

	friend void *DMWorkerThread(void *);
	friend void EntryDMWorkerThread(CCustomer *);
public:
	CCustomer(char *szInDir, TIdent iConfiguredCustomerCount,
			TIdent iActiveCustomerCount, INT32 iScaleFactor,
			INT32 iDaysOfInitialTrades, UINT32 UniqueId, char *szBHaddr,
			int iBHlistenPort, int iUsers, int iPacingDelay,
			char *outputDirectory, ofstream *m_fMix, CMutex *m_MixLock);
	~CCustomer();

	void DoTxn();
	void RunTest(int, int);
};

#endif // CUSTOMER_H
