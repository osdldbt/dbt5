/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *               2010 Mark Wong
 *
 * Base class for transacation classes
 * 13 June 2006
 */

#ifndef TXN_BASE_DB_H
#define TXN_BASE_DB_H

#include <string>
using namespace std;

#include "DBT5Consts.h"
using namespace TPCE;

#include <pqxx/pqxx>
using namespace pqxx;

#include "DBConnection.h"
#include "locking.h"

class CTxnBaseDB
{
protected:
	CDBConnection *pDB;

	void begin();
	void commit();
	string escape(string);
	result execute(string);
	void reconect();
	void rollback();

public:
	CTxnBaseDB(CDBConnection *pDB);
	~CTxnBaseDB();
};

#endif // TXN_BASE_DB_H
