/*
 * DBConnection.cpp
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *
 * 13 June 2006
 */ 

#include <transactions.h>

using namespace TPCE;

// Constructor: Creates PgSQL connection
CDBConnection::CDBConnection(const char *szHost, const char *szDBName,
		const char *szPostmasterPort)
{
	char	szConnectStr[256];
	sprintf( szConnectStr, "host=%s dbname=%s port=%s", 
 			szHost, szDBName, szPostmasterPort );

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

