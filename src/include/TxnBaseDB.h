/*
 * TxnBaseDB.h
 * Base class for transacation classes
 *
 * 2006 Rilson Nascimento
 *
 * 13 June 2006
 */

#ifndef TXN_BASE_DB_H
#define TXN_BASE_DB_H

#include "locking.h"

using namespace TPCE;

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
