/*
 * DriverMarket.h
 * This class represents the Market Exchange driver
 *
 * 2006 Rilson Nascimento
 *
 * 30 July 2006
 */

#ifndef DRIVER_MARKET_H
#define DRIVER_MARKET_H

namespace TPCE
{

class CDriverMarket
{
	int			m_iListenPort;
	CSocket			m_Socket;
	CLogFormatTab		m_fmt;
	CEGenLogger*		m_pLog;
	CMEESUT*		m_pCMEESUT;
	CSecurityFile*		m_pSecurities;
	CMEE*			m_pCMEE;

private:
	friend void* TPCE::MarketWorkerThread(void* data);
	friend void TPCE::EntryMarketWorkerThread(void* data);	// entry point for driver worker thread


public:
	CDriverMarket(char* szFileLoc, TIdent iConfiguredCustomerCount, TIdent iActiveCustomerCount, int iListenPort, int iBHlistenPort);
	~CDriverMarket();

	void Listener(void);

};

//parameter structure for the threads
typedef struct TMarketThreadParam
{
	CDriverMarket*		pDriverMarket;
	int			iSockfd;
} *PMarketThreadParam;


}	// namespace TPCE

#endif	// DRIVER_MARKET_H
