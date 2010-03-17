/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2010 Rilson Nascimento
 *
 * 08 July 2006
 */

#ifndef TRADE_LOOKUP_DB_H
#define TRADE_LOOKUP_DB_H

#include "TxnHarnessDBInterface.h"

#include "TxnBaseDB.h"
#include "DBConnection.h"
using namespace TPCE;

class CTradeLookupDB : public CTxnBaseDB, public CTradeLookupDBInterface
{
public:
	CTradeLookupDB(CDBConnection *pDBConn) : CTxnBaseDB(pDBConn) {};
	~CTradeLookupDB() {};

	virtual void DoTradeLookupFrame1(const TTradeLookupFrame1Input *pIn,
			TTradeLookupFrame1Output *pOut);
	virtual void DoTradeLookupFrame2(const TTradeLookupFrame2Input *pIn,
			TTradeLookupFrame2Output *pOut);
	virtual void DoTradeLookupFrame3(const TTradeLookupFrame3Input *pIn,
			TTradeLookupFrame3Output *pOut);
	virtual void DoTradeLookupFrame4(const TTradeLookupFrame4Input *pIn,
			TTradeLookupFrame4Output *pOut);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException) {};
};

#endif	// TRADE_LOOKUP_DB_H
