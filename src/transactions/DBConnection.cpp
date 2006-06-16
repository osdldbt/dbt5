/*
 * DBConnection.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 13 June 2006
 */ 

#include "transactions.h"

using namespace TPCE;

// Constructor: Creates PgSQL connection
CDBConnection::CDBConnection(const char *szHost, const char *szDBName, const char *szPostmasterPort)
{
	char	szConnectStr[256];
	sprintf( szConnectStr, "host=%s dbname=%s port=%s", 
 			szHost, szDBName, szPostmasterPort );

	try
	{
		m_Conn = new connection( szConnectStr );
		m_Txn = new nontransaction( *m_Conn, "txn" );
	}
	catch(const exception &e)
	{
		cout<< "libpqxx: "<<e.what() << endl;
	}
}

// Destructor: Disconnect from server
CDBConnection::~CDBConnection()
{
	try
	{
		m_Txn->commit(); // does nothing since this is a dummy transaction, just from completeness	
		m_Conn->disconnect();
	}
	catch(const exception &e)
	{
		cout<< "libpqxx: "<<e.what() << endl;
	}
	
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

