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

CTxnBaseDB::CTxnBaseDB(CDBConnection *pDBConn)
: m_pDBConnection(pDBConn)
{
	m_Conn = m_pDBConnection->m_Conn;  //FIXME?
	m_Txn = m_pDBConnection->m_Txn;
}

void CTxnBaseDB::BeginTxn()
{
	m_pDBConnection->BeginTxn();
}

void CTxnBaseDB::CommitTxn()
{
	m_pDBConnection->CommitTxn();
}

void CTxnBaseDB::RollbackTxn()
{
	m_pDBConnection->RollbackTxn();
}
