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

class CDriver
{
private:
	int m_iUsers;
	int m_iPacingDelay;
	CLogFormatTab m_fmt;
	CEGenLogger *m_pLog;
	CInputFiles m_InputFiles;
	PDriverCETxnSettings m_pDriverCETxnSettings;
	CMutex m_LogLock;
	ofstream m_fLog; // error log file
	ofstream m_fMix; // mix log file

	void LogErrorMessage(const string);

	friend void *CustomerWorkerThread(void *);
	// entry point for driver worker thread
	friend void EntryCustomerWorkerThread(void *, int);

	friend void *DMWorkerThread(void *);
	friend void EntryDMWorkerThread(CDriver *);

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

	CDriver(char *, TIdent, TIdent, INT32, INT32, UINT32, char *, int, int,
			int, char *);
	~CDriver();

	void RunTest(int, int);
};

//parameter structure for the threads
typedef struct TCustomerThreadParam
{
	CDriver *pDriver;
} *PCustomerThreadParam;


#endif // DRIVER_H
