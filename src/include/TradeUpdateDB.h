/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 11 July 2006
 */

#ifndef TRADE_UPDATE_DB_H
#define TRADE_UPDATE_DB_H

#include "TxnHarnessDBInterface.h"

#include "TxnBaseDB.h"
#include "DBConnection.h"
using namespace TPCE;

class CTradeUpdateDB: public CTxnBaseDB, public CTradeUpdateDBInterface
{
private:
	bool m_Verbose;
	pid_t m_pid;

public:
	CTradeUpdateDB(CDBConnection *, bool);
	~CTradeUpdateDB(){};

	void DoTradeUpdateFrame1(
			const TTradeUpdateFrame1Input *, TTradeUpdateFrame1Output *);
	void DoTradeUpdateFrame2(
			const TTradeUpdateFrame2Input *, TTradeUpdateFrame2Output *);
	void DoTradeUpdateFrame3(
			const TTradeUpdateFrame3Input *, TTradeUpdateFrame3Output *);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void *pException){};
};

#endif // TRADE_UPDATE_DB_H
