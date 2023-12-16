/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 21 July 2006
 */

#ifndef MARKET_FEED_DB_H
#define MARKET_FEED_DB_H

#include <TxnHarnessDBInterface.h>

#include "TxnBaseDB.h"

class CMarketFeedDB: public CTxnBaseDB, public CMarketFeedDBInterface
{
private:
	bool m_Verbose;
	pid_t m_pid;

public:
	CMarketFeedDB(CDBConnection *, bool);
	~CMarketFeedDB(){};

	void DoMarketFeedFrame1(const TMarketFeedFrame1Input *,
			TMarketFeedFrame1Output *, CSendToMarketInterface *);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void *pException){};
};

#endif // MARKET_FEED_DB_H
