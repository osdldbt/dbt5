/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 07 July 2006
 */

#ifndef TRADE_RESULT_DB_H
#define TRADE_RESULT_DB_H

#include "TxnHarnessDBInterface.h"

#include "TxnBaseDB.h"
#include "DBConnection.h"
using namespace TPCE;

class CTradeResultDB: public CTxnBaseDB, public CTradeResultDBInterface
{
private:
	bool m_Verbose;
	pid_t m_pid;

public:
	CTradeResultDB(CDBConnection *, bool);
	~CTradeResultDB(){};

	void DoTradeResultFrame1(
			const TTradeResultFrame1Input *, TTradeResultFrame1Output *);
	void DoTradeResultFrame2(
			const TTradeResultFrame2Input *, TTradeResultFrame2Output *);
	void DoTradeResultFrame3(
			const TTradeResultFrame3Input *, TTradeResultFrame3Output *);
	void DoTradeResultFrame4(
			const TTradeResultFrame4Input *, TTradeResultFrame4Output *);
	void DoTradeResultFrame5(const TTradeResultFrame5Input *);
	void DoTradeResultFrame6(
			const TTradeResultFrame6Input *, TTradeResultFrame6Output *);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void *pException){};
};

#endif // TRADE_RESULT_DB_H
