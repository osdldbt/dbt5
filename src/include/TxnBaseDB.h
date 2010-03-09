/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * 2006 Rilson Nascimento
 *
 * Base class for transacation classes
 * 13 June 2006
 */

#ifndef TXN_BASE_DB_H
#define TXN_BASE_DB_H

#include "locking.h"

class CTxnBaseDB
{

protected:
	CDBConnection*		m_pDBConnection;
	connection*		m_Conn;		// libpqxx Connection
	nontransaction*		m_Txn;		// libpqxx dummy Transaction
	CMutex		m_coutLock;

public:
	CTxnBaseDB(CDBConnection *pDBConn);
	~CTxnBaseDB() {};

	void BeginTxn();
	void CommitTxn();
	void RollbackTxn();
};

#endif	//TXN_BASE_DB_H
