/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 17 July 2006
 */

#ifndef DATA_MAINTENANCE_DB_H
#define DATA_MAINTENANCE_DB_H

#include <TxnHarnessDBInterface.h>

#include "TxnBaseDB.h"

class CDataMaintenanceDB: public CTxnBaseDB, public CDataMaintenanceDBInterface
{
private:
	bool m_Verbose;
	pid_t m_pid;

public:
	CDataMaintenanceDB(CDBConnection *, bool);
	~CDataMaintenanceDB(){};

	void DoDataMaintenanceFrame1(const TDataMaintenanceFrame1Input *pIn);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void *pException){};
};

#endif // DATA_MAINTENANCE_DB_H
