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
	CLogFormatTab		m_fmt;
	CEGenLogger*		m_pLog;
	CCESUT*			m_pCCESUT;
	CInputFiles		m_InputFiles;
	CCE*			m_pCCE;
	PDriverCETxnSettings	m_pDriverCETxnSettings;

private:
	void ThrowError(CThreadErr::Action eAction, char* szLocation);

	friend void* TPCE::CustomerWorkerThread(void* data);
	friend void TPCE::EntryCustomerWorkerThread(void* data, int i);	// entry point for driver worker thread

public:
	CDriverCustomer(char* szInDir, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount,
				INT32 iScaleFactor, INT32 iDaysOfInitialTrades, UINT32 UniqueId, int iBHlistenPort);
	~CDriverCustomer();

	void RunTest(int iSleep);
};

//parameter structure for the threads
typedef struct TCustomerThreadParam
{
	CDriverCustomer*	pDriverCustomer;
} *PCustomerThreadParam;


}	// namespace TPCE

#endif	// DRIVER_CUSTOMER_H
