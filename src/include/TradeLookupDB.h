/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 08 July 2006
 */

#ifndef TRADE_LOOKUP_DB_H
#define TRADE_LOOKUP_DB_H

#include "TxnHarnessDBInterface.h"

#include "TxnBaseDB.h"
#include "DBConnection.h"
using namespace TPCE;

class CTradeLookupDB: public CTxnBaseDB, public CTradeLookupDBInterface
{
private:
	bool m_Verbose;
	pid_t m_pid;

public:
	CTradeLookupDB(CDBConnection *, bool);
	~CTradeLookupDB(){};

	void DoTradeLookupFrame1(
			const TTradeLookupFrame1Input *, TTradeLookupFrame1Output *);
	void DoTradeLookupFrame2(
			const TTradeLookupFrame2Input *, TTradeLookupFrame2Output *);
	void DoTradeLookupFrame3(
			const TTradeLookupFrame3Input *, TTradeLookupFrame3Output *);
	void DoTradeLookupFrame4(
			const TTradeLookupFrame4Input *, TTradeLookupFrame4Output *);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void *pException){};
};

#endif // TRADE_LOOKUP_DB_H
