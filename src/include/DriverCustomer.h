/*
 * DriverCustomer.h
 * This class represents the Customer Emulator driver
 *
 * 2006 Rilson Nascimento
 *
 * 03 August 2006
 */

#ifndef DRIVER_CUSTOMER_H
#define DRIVER_CUSTOMER_H

namespace TPCE
{

class CDriverCustomer
{
	int			m_iUsers;
	int			m_iPacingDelay;
	CLogFormatTab		m_fmt;
	CEGenLogger*		m_pLog;
	CInputFiles		m_InputFiles;
	CCESUT*			m_pCCESUT;
	CCE*			m_pCCE;
	CDMSUT*			m_pCDMSUT;
	CDM*			m_pCDM;
	PDriverCETxnSettings	m_pDriverCETxnSettings;
	CSyncLock		m_LogLock;
	CSyncLock		m_MixLock;
	ofstream		m_fLog;		// error log file
	ofstream		m_fMix;		// mix log file

private:
	void LogErrorMessage(const string sErr);

	friend void* TPCE::CustomerWorkerThread(void* data);
	friend void TPCE::EntryCustomerWorkerThread(void* data, int i);	// entry point for driver worker thread

	friend void* TPCE::DMWorkerThread(void* data);
	friend void TPCE::EntryDMWorkerThread(CDriverCustomer* ptr);
public:
	CDriverCustomer(char* szInDir, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
				INT32 iScaleFactor, INT32 iDaysOfInitialTrades, UINT32 UniqueId,
				char* szBHaddr, int iBHlistenPort, int iUsers, int iPacingDelay,
				char* outputDirectory);
	~CDriverCustomer();

	void RunTest(int iSleep, int iTestDuration);
};

//parameter structure for the threads
typedef struct TCustomerThreadParam
{
	CDriverCustomer*	pDriverCustomer;
} *PCustomerThreadParam;


}	// namespace TPCE

#endif	// DRIVER_CUSTOMER_H
