/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 13 June 2006
 */

#ifndef TRADE_STATUS_DB_H
#define TRADE_STATUS_DB_H

#include "TxnHarnessDBInterface.h"

#include "TxnBaseDB.h"
#include "DBConnection.h"
using namespace TPCE;

class CTradeStatusDB: public CTxnBaseDB, public CTradeStatusDBInterface
{
private:
	bool m_Verbose;
	pid_t m_pid;

public:
	CTradeStatusDB(CDBConnection *, bool);

	~CTradeStatusDB(){};

	void DoTradeStatusFrame1(
			const TTradeStatusFrame1Input *, TTradeStatusFrame1Output *);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void *pException){};
};

#endif // TRADE_STATUS_DB_H
