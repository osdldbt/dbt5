/*
 * MEESUT.h
 * MEE (Market Exchange Emulator) - SUT Interface class 
 *
 * 2006 Rilson Nascimento
 *
 * 30 July 2006
 */

#ifndef MEE_SUT_H
#define MEE_SUT_H

#include "MEESUTInterface.h"

namespace TPCE
{
class CMEESUT : public CMEESUTInterface
{
private:
	ofstream*		m_pfLog;
	int			m_iBHlistenPort;
	TTradeResultTxnInput	m_TradeResultTxnInput;
	TMarketFeedTxnInput	m_MarketFeedTxnInput;
	CSyncLock		m_LogLock;
	CSyncLock		m_MixLock;
	ofstream		m_fMix;		// mix log file
	
public:
	void LogErrorMessage(const string sErr);

	CMEESUT(const int iBHlistenPort, ofstream* pfile);
	~CMEESUT();

	virtual bool TradeResult( PTradeResultTxnInput pTxnInput );	// return whether it was successful
	virtual bool MarketFeed( PMarketFeedTxnInput pTxnInput );	// return whether it was successful
	
	friend void* TPCE::TradeResultAsync(void* data);
	friend bool TPCE::RunTradeResultAsync( CMEESUT* );

	friend void* TPCE::MarketFeedAsync(void* data);
	friend bool TPCE::RunMarketFeedAsync( CMEESUT* );

};

}	// namespace TPCE

#endif	// MEE_SUT_H
