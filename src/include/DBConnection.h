/*
 * DBConnection.h
 * PostgreSQL connection class
 *
 * 2006 Rilson Nascimento
 *
 * 13 June 2006
 */ 

#ifndef DB_CONNECTION_H
#define DB_CONNECTION_H

using namespace TPCE;

class CDBConnection
{

public:

	connection*			m_Conn;		// libpqxx Connection
	nontransaction*			m_Txn;		// libpqxx dummy Transaction

	CDBConnection(const char *szHost, const char *szDBName,
			const char *szPostmasterPort);
	~CDBConnection();

	void BeginTxn();
	void CommitTxn();
	void RollbackTxn();
	
};

#endif	//DB_CONNECTION_H
