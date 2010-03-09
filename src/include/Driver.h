/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * 2006 Rilson Nascimento
 *
 * This class represents the workload driver
 * 03 August 2006
 */

#ifndef DRIVER_H
#define DRIVER_H

#include "locking.h"

using namespace TPCE;

class CDriver
{
private:
	int			m_iUsers;
	int			m_iPacingDelay;
	CLogFormatTab		m_fmt;
	CEGenLogger*		m_pLog;
	CInputFiles		m_InputFiles;
	PDriverCETxnSettings	m_pDriverCETxnSettings;
	CMutex		m_LogLock;
	ofstream		m_fLog;		// error log file
	ofstream		m_fMix;		// mix log file

	void LogErrorMessage(const string sErr);

	friend void *CustomerWorkerThread(void* data);
	// entry point for driver worker thread
	friend void EntryCustomerWorkerThread(void* data, int i);

	friend void *DMWorkerThread(void* data);
	friend void EntryDMWorkerThread(CDriver* ptr);
public:
	char szInDir[iMaxPath];
	TIdent iConfiguredCustomerCount;
	TIdent iActiveCustomerCount;
	INT32 iScaleFactor;
	INT32 iDaysOfInitialTrades;
	UINT32 UniqueId;
	char szBHaddr[1024];
	int iBHlistenPort;
	int iUsers;
	int iPacingDelay;
	char outputDirectory[iMaxPath];
	CMutex m_MixLock;
	CDMSUT *m_pCDMSUT;
	CDM *m_pCDM;

	CDriver(char* szInDir, TIdent iConfiguredCustomerCount,
			TIdent iActiveCustomerCount, INT32 iScaleFactor,
			INT32 iDaysOfInitialTrades, UINT32 UniqueId, char* szBHaddr,
			int iBHlistenPort, int iUsers, int iPacingDelay,
			char* outputDirectory);
	~CDriver();

	void RunTest(int iSleep, int iTestDuration);
};

//parameter structure for the threads
typedef struct TCustomerThreadParam
{
	CDriver*    pDriver;
} *PCustomerThreadParam;


#endif	// DRIVER_H
