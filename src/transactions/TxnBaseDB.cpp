/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * 13 June 2006
 */

#include "TxnBaseDB.h"

CTxnBaseDB::CTxnBaseDB(CDBConnection *pDB)
{
	this->pDB = pDB;
}

CTxnBaseDB::~CTxnBaseDB()
{
}

void CTxnBaseDB::begin()
{
	pDB->startTransaction();
}

void CTxnBaseDB::commit()
{
	pDB->commit();
}

string CTxnBaseDB::escape(string s)
{
	return pDB->escape(s);
}

result CTxnBaseDB::execute(string sql)
{
	return pDB->execute(sql);
}

void CTxnBaseDB::rollback()
{
	pDB->rollback();
}
