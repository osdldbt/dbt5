/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 03 July 2006
 */

#ifndef TRADE_ORDER_DB_H
#define TRADE_ORDER_DB_H

#include "TxnHarnessDBInterface.h"

#include "TxnBaseDB.h"
#include "DBConnection.h"
using namespace TPCE;

class CTradeOrderDB: public CTxnBaseDB, public CTradeOrderDBInterface
{
private:
	bool m_Verbose;
	pid_t m_pid;

public:
	CTradeOrderDB(CDBConnection *, bool);
	~CTradeOrderDB(){};

	void DoTradeOrderFrame1(
			const TTradeOrderFrame1Input *, TTradeOrderFrame1Output *);
	void DoTradeOrderFrame2(
			const TTradeOrderFrame2Input *, TTradeOrderFrame2Output *);
	void DoTradeOrderFrame3(
			const TTradeOrderFrame3Input *, TTradeOrderFrame3Output *);
	void DoTradeOrderFrame4(
			const TTradeOrderFrame4Input *, TTradeOrderFrame4Output *);
	void DoTradeOrderFrame5();
	void DoTradeOrderFrame6();

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void *pException){};
};

#endif // TRADE_ORDER_DB_H
