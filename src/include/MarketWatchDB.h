/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 15 July 2006
 */

#ifndef MARKET_WATCH_DB_H
#define MARKET_WATCH_DB_H

#include <TxnHarnessDBInterface.h>

#include "TxnBaseDB.h"

class CMarketWatchDB: public CTxnBaseDB, public CMarketWatchDBInterface
{
private:
	bool m_Verbose;
	pid_t m_pid;

public:
	CMarketWatchDB(CDBConnection *, bool);
	~CMarketWatchDB(){};

	void DoMarketWatchFrame1(
			const TMarketWatchFrame1Input *, TMarketWatchFrame1Output *);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void *pException){};
};

#endif // MARKET_WATCH_DB_H
