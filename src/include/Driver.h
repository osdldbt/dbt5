/*
 * Driver.h
 * This class represents the workload driver
 *
 * 2006 Rilson Nascimento
 *
 * 03 August 2006
 */

#ifndef DRIVER_H
#define DRIVER_H

namespace TPCE
{

class CDriver
{
	int			m_iUsers;
	int			m_iPacingDelay;
	CLogFormatTab		m_fmt;
	CEGenLogger*		m_pLog;
	CInputFiles		m_InputFiles;
	PDriverCETxnSettings	m_pDriverCETxnSettings;
	CSyncLock		m_LogLock;
	ofstream		m_fLog;		// error log file
	ofstream		m_fMix;		// mix log file

private:
	void LogErrorMessage(const string sErr);

	friend void* TPCE::CustomerWorkerThread(void* data);
	// entry point for driver worker thread
	friend void TPCE::EntryCustomerWorkerThread(void* data, int i);

	friend void* TPCE::DMWorkerThread(void* data);
	friend void TPCE::EntryDMWorkerThread(CDriver* ptr);
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
	CSyncLock m_MixLock;

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


}	// namespace TPCE

#endif	// DRIVER_H
