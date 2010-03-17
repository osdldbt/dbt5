/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * The purpose of this class object is allow other classes to share the
 * same database connection.  The database connection is managed by this
 * class.
 *
 * 13 June 2006
 */ 

#include "transactions.h"

// Constructor: Creates PgSQL connection
CDBConnection::CDBConnection(const char *szHost, const char *szDBName,
		const char *szDBPort)
{
	char szConnectStr[256] = "";

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
	if (strlen(szDBPort) > 0) {
		strcat(szConnectStr, " port=");
		strcat(szConnectStr, szDBPort);
	}

	char name[16];
	sprintf(name, "%d", (int) pthread_self());
	m_Conn = new connection(szConnectStr);
	m_Txn = new nontransaction(*m_Conn, name);
}

// Destructor: Disconnect from server
CDBConnection::~CDBConnection()
{
	m_Conn->disconnect();
	
	delete m_Txn;
	delete m_Conn;
}

void CDBConnection::commit()
{
	m_Txn->exec("COMMIT;");
}

string CDBConnection::escape(string s)
{
	return m_Txn->esc(s);
}

result CDBConnection::execute(string sql)
{
	return m_Txn->exec(sql);
}

void CDBConnection::rollback()
{
	m_Txn->exec("ROLLBACK;");
}

void CDBConnection::startTransaction()
{
	m_Txn->exec("BEGIN;");
}
