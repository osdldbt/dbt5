/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * 13 June 2006
 */ 

#include <transactions.h>

// Constructor: Creates PgSQL connection
CDBConnection::CDBConnection(const char *szHost, const char *szDBName,
		const char *szPostmasterPort)
{
	char	szConnectStr[256] = "";

	// Just pad everything with spaces so we don't have to figure out if it's
	// needed or not.
	if (strlen(szHost) > 0) {
		strcat(szConnectStr, " host=");
		strcat(szConnectStr, szHost);
	}
	if (strlen(szDBName) > 0) {
		strcat(szConnectStr, " dbname=");
		strcat(szConnectStr, szDBName);
	}
	if (strlen(szPostmasterPort) > 0) {
		strcat(szConnectStr, " port=");
		strcat(szConnectStr, szPostmasterPort);
	}

	m_Conn = new connection( szConnectStr );
	m_Txn = new nontransaction( *m_Conn, "txn" );
}

// Destructor: Disconnect from server
CDBConnection::~CDBConnection()
{
	// does nothing since this is a dummy transaction, just from completeness	
	m_Txn->commit();
	m_Conn->disconnect();
	
	delete m_Txn;
	delete m_Conn;
}

void CDBConnection::BeginTxn()
{
	m_Txn->exec("BEGIN;");
}

void CDBConnection::CommitTxn()
{
	m_Txn->exec("COMMIT;");
}

void CDBConnection::RollbackTxn()
{
	m_Txn->exec("ROLLBACK;");
}
