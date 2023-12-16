/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 13 July 2006
 */

#ifndef SECURITY_DETAIL_DB_H
#define SECURITY_DETAIL_DB_H

#include <TxnHarnessDBInterface.h>

#include "TxnBaseDB.h"

class CSecurityDetailDB: public CTxnBaseDB, public CSecurityDetailDBInterface
{
private:
	bool m_Verbose;
	pid_t m_pid;

public:
	CSecurityDetailDB(CDBConnection *, bool);
	~CSecurityDetailDB(){};

	void DoSecurityDetailFrame1(
			const TSecurityDetailFrame1Input *, TSecurityDetailFrame1Output *);

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void *pException){};
};

#endif // SECURITY_DETAIL_DB_H
