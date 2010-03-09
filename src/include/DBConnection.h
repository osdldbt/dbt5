/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * 2006 Rilson Nascimento
 *
 * PostgreSQL connection class
 * 13 June 2006
 */ 

#ifndef DB_CONNECTION_H
#define DB_CONNECTION_H

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
