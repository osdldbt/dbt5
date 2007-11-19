/*
 * BaseInterface.h
 * Base class for emulator-SUT interface
 *
 * 2006 Rilson Nascimento
 *
 * 13 August 2006
 */

#ifndef BASE_INTERFACE_H
#define BASE_INTERFACE_H

using namespace TPCE;

class CBaseInterface
{
protected:
	void TalkToSUT(PMsgDriverBrokerage pRequest);
	void LogErrorMessage(const string sErr);

	char*			m_szBHAddress;
	int			m_iBHlistenPort;
	CSyncLock*		m_pLogLock;
	CSyncLock*		m_pMixLock;
	ofstream*		m_pfLog;	// error log file
	ofstream*		m_pfMix;	// mix log file

private:
	CSocket	*sock;
	void LogResponseTime(int iStatus, int iTxnType, double dRT);
	
public:

	CBaseInterface(char* addr, const int iListenPort, ofstream* pflog,
			ofstream* pfmix, CSyncLock* pLogLock, CSyncLock* pMixLock);
	~CBaseInterface(void);
	void Connect();
	void Disconnect();
};

#endif	// BASE_INTERFACE_H
