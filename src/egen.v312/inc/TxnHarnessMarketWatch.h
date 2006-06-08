/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
#ifndef TXN_HARNESS_MARKET_WATCH_H
#define TXN_HARNESS_MARKET_WATCH_H

namespace TPCE
{

class CMarketWatch
{
	CMarketWatchDB	m_db;		

public:
	CMarketWatch(const char *szServer, const char *szDatabase)
		: m_db(szServer, szDatabase)
	{		
	};

	void DoTxn(PMarketWatchTxnInput pTxnInput, PMarketWatchTxnOutput pTxnOutput)
	{
		m_db.DoMarketWatchFrame1(pTxnInput, pTxnOutput);
	}

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException)
	{
		m_db.Cleanup(pException);
	}
};

}	// namespace TPCE

#endif //TXN_HARNESS_MARKET_WATCH_H
