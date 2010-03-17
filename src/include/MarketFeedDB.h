/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *
 * 21 July 2006
 */

#ifndef MARKET_FEED_DB_H
#define MARKET_FEED_DB_H

#include <TxnHarnessDBInterface.h> 

#include "TxnBaseDB.h"

class CMarketFeedDB : public CTxnBaseDB, public CMarketFeedDBInterface
{
public:
	CMarketFeedDB(CDBConnection *pDBConn) : CTxnBaseDB(pDBConn) {};
	~CMarketFeedDB() {};

	virtual void DoMarketFeedFrame1(const TMarketFeedFrame1Input *pIn,
			TMarketFeedFrame1Output *pOut,
			CSendToMarketInterface *pSendToMarket);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException) {};
};

#endif	// MARKET_FEED_DB_H
