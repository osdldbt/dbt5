/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2010 Rilson Nascimento
 *
 * 13 June 2006
 */

#ifndef TRADE_STATUS_DB_H
#define TRADE_STATUS_DB_H

#include "TxnHarnessDBInterface.h"

#include "TxnBaseDB.h"
#include "DBConnection.h"
using namespace TPCE;

class CTradeStatusDB : public CTxnBaseDB, public CTradeStatusDBInterface
{
public:
	CTradeStatusDB(CDBConnection *pDBConn) : CTxnBaseDB(pDBConn) {};

	~CTradeStatusDB() {};

	virtual void DoTradeStatusFrame1(const TTradeStatusFrame1Input *pIn,
			TTradeStatusFrame1Output *pOut);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException) {};
};

#endif	// TRADE_STATUS_DB_H
