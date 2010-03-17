/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *
 * 12 July 2006
 */

#ifndef CUSTOMER_POSITION_DB_H
#define CUSTOMER_POSITION_DB_H

#include "TxnHarnessDBInterface.h"

#include "TxnBaseDB.h"
#include "DBConnection.h"
using namespace TPCE;

class CCustomerPositionDB : public CTxnBaseDB,
		public CCustomerPositionDBInterface
{
public:
	CCustomerPositionDB(CDBConnection *pDBConn) : CTxnBaseDB(pDBConn) {};
	~CCustomerPositionDB() {};

	virtual void DoCustomerPositionFrame1(
			const TCustomerPositionFrame1Input *pIn,
			TCustomerPositionFrame1Output *pOut);
	virtual void DoCustomerPositionFrame2(
			const TCustomerPositionFrame2Input *pIn,
			TCustomerPositionFrame2Output *pOut);
	virtual void DoCustomerPositionFrame3(TCustomerPositionFrame3Output *pOut);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException) {};
};

#endif	// CUSTOMER_POSITION_DB_H
