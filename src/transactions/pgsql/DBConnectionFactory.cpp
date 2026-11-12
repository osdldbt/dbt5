/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * Creates PostgreSQL connections for the transaction harness.
 */

#include "DBConnection.h"
#include "pgsql/DBConnectionClientSide.h"
#include "pgsql/DBConnectionServerSide.h"

CDBConnection *
CreateDBConnection(bool bClientSide, const char *szHost, const char *szDBName,
		const char *szDBPort, bool bVerbose)
{
	if (bClientSide) {
		return new CDBConnectionClientSide(
				szHost, szDBName, szDBPort, bVerbose);
	}
	return new CDBConnectionServerSide(szHost, szDBName, szDBPort, bVerbose);
}
