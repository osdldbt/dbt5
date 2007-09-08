/*
 * Customer.h
 * This class represents the workload driver
 *
 * 2006 Rilson Nascimento
 *
 * 03 August 2006
 */

#ifndef CUSTOMER_H
#define CUSTOMER_H

#include <transactions.h>

namespace TPCE
{

class CCustomer
{
	int			m_iUsers;
	int			m_iPacingDelay;
	CLogFormatTab		m_fmt;
	CEGenLogger*		m_pLog;
	CInputFiles		m_InputFiles;
	CCESUT*			m_pCCESUT;
	CCE*			m_pCCE;
	PDriverCETxnSettings	m_pDriverCETxnSettings;
	CSyncLock		m_LogLock;
	ofstream		m_fLog;		// error log file

private:
	void LogErrorMessage(const string sErr);

	friend void* TPCE::CustomerWorkerThread(void* data);
	// entry point for driver worker thread
	friend void TPCE::EntryCustomerWorkerThread(void* data, int i);

	friend void* TPCE::DMWorkerThread(void* data);
	friend void TPCE::EntryDMWorkerThread(CCustomer* ptr);
public:
	CCustomer(char* szInDir, TIdent iConfiguredCustomerCount,
			TIdent iActiveCustomerCount, INT32 iScaleFactor,
			INT32 iDaysOfInitialTrades, UINT32 UniqueId, char* szBHaddr,
			int iBHlistenPort, int iUsers, int iPacingDelay,
			char* outputDirectory, ofstream *m_fMix, CSyncLock *m_MixLock);
	~CCustomer();

	void DoTxn();
	void RunTest(int iSleep, int iTestDuration);
};

}	// namespace TPCE

#endif	// CUSTOMER_H
