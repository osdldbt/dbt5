/*
 * MarketWatchDB.h
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *
 * 15 July 2006
 */

#ifndef MARKET_WATCH_DB_H
#define MARKET_WATCH_DB_H

#include <TxnHarnessDBInterface.h> 
 
namespace TPCE
{

class CMarketWatchDB : public CTxnBaseDB, public CMarketWatchDBInterface
{
public:
	CMarketWatchDB(CDBConnection *pDBConn);
	~CMarketWatchDB();

	virtual void DoMarketWatchFrame1(const TMarketWatchFrame1Input *pIn,
			TMarketWatchFrame1Output *pOut);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException);
};

}	// namespace TPCE

#endif	// MARKET_WATCH_DB_H
