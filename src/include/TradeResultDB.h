/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2010 Rilson Nascimento
 *
 * 07 July 2006
 */

#ifndef TRADE_RESULT_DB_H
#define TRADE_RESULT_DB_H

#include "TxnHarnessDBInterface.h"

#include "TxnBaseDB.h"
#include "DBConnection.h"
using namespace TPCE;

class CTradeResultDB : public CTxnBaseDB, public CTradeResultDBInterface
{
public:
	CTradeResultDB(CDBConnection *pDBConn) : CTxnBaseDB(pDBConn) {};
	~CTradeResultDB() {};

	virtual void DoTradeResultFrame1(const TTradeResultFrame1Input *pIn,
			TTradeResultFrame1Output *pOut);
	virtual void DoTradeResultFrame2(const TTradeResultFrame2Input *pIn,
			TTradeResultFrame2Output *pOut);
	virtual void DoTradeResultFrame3(const TTradeResultFrame3Input *pIn,
			TTradeResultFrame3Output *pOut);
	virtual void DoTradeResultFrame4(const TTradeResultFrame4Input *pIn,
			TTradeResultFrame4Output *pOut);
	virtual void DoTradeResultFrame5(const TTradeResultFrame5Input *pIn,
			TTradeResultFrame5Output *pOut);
	virtual void DoTradeResultFrame6(const TTradeResultFrame6Input *pIn,
			TTradeResultFrame6Output *pOut);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException) {};
};

#endif	// TRADE_RESULT_DB_H
