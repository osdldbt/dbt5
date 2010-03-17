/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *
 * 18 July 2006
 */

#ifndef TRADE_CLEANUP_DB_H
#define TRADE_CLEANUP_DB_H

#include <TxnHarnessDBInterface.h> 

#include "TxnBaseDB.h"
 
class CTradeCleanupDB : public CTxnBaseDB, public CTradeCleanupDBInterface
{
public:
	CTradeCleanupDB(CDBConnection *pDBConn) : CTxnBaseDB(pDBConn) {};
	~CTradeCleanupDB() {};

	virtual void DoTradeCleanupFrame1(const TTradeCleanupFrame1Input *pIn,
			TTradeCleanupFrame1Output *pOut);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException) {};
};

#endif	// TRADE_CLEANUP_DB_H
