/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 12 July 2006
 */

#ifndef CUSTOMER_POSITION_DB_H
#define CUSTOMER_POSITION_DB_H

#include "TxnHarnessDBInterface.h"

#include "TxnBaseDB.h"
#include "DBConnection.h"
using namespace TPCE;

class CCustomerPositionDB: public CTxnBaseDB,
						   public CCustomerPositionDBInterface
{
private:
	bool m_Verbose;
	pid_t m_pid;

public:
	CCustomerPositionDB(CDBConnection *, bool);
	~CCustomerPositionDB(){};

	void DoCustomerPositionFrame1(const TCustomerPositionFrame1Input *,
			TCustomerPositionFrame1Output *);
	void DoCustomerPositionFrame2(const TCustomerPositionFrame2Input *,
			TCustomerPositionFrame2Output *);
	void DoCustomerPositionFrame3();

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void *pException){};
};

#endif // CUSTOMER_POSITION_DB_H
