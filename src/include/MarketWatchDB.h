/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2010 Rilson Nascimento
 *
 * 15 July 2006
 */

#ifndef MARKET_WATCH_DB_H
#define MARKET_WATCH_DB_H

#include <TxnHarnessDBInterface.h> 

#include "TxnBaseDB.h"
 
class CMarketWatchDB : public CTxnBaseDB, public CMarketWatchDBInterface
{
public:
	CMarketWatchDB(CDBConnection *pDBConn) : CTxnBaseDB(pDBConn) {};
	~CMarketWatchDB() {};

	virtual void DoMarketWatchFrame1(const TMarketWatchFrame1Input *pIn,
			TMarketWatchFrame1Output *pOut);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException) {};
};

#endif	// MARKET_WATCH_DB_H
