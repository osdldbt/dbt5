/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2010 Rilson Nascimento
 *
 * 11 July 2006
 */

#ifndef TRADE_UPDATE_DB_H
#define TRADE_UPDATE_DB_H

#include "TxnHarnessDBInterface.h"

#include "TxnBaseDB.h"
#include "DBConnection.h"
using namespace TPCE;

class CTradeUpdateDB : public CTxnBaseDB, public CTradeUpdateDBInterface
{
public:
	CTradeUpdateDB(CDBConnection *pDBConn) : CTxnBaseDB(pDBConn) {};
	~CTradeUpdateDB() {};

	virtual void DoTradeUpdateFrame1(const TTradeUpdateFrame1Input *pIn,
			TTradeUpdateFrame1Output *pOut);
	virtual void DoTradeUpdateFrame2(const TTradeUpdateFrame2Input *pIn,
			TTradeUpdateFrame2Output *pOut);
	virtual void DoTradeUpdateFrame3(const TTradeUpdateFrame3Input *pIn,
			TTradeUpdateFrame3Output *pOut);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException) {};
};

#endif	// TRADE_UPDATE_DB_H
