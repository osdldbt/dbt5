/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *               2010 Mark Wong
 *
 * PostgreSQL connection class
 * 13 June 2006
 */ 

#ifndef DB_CONNECTION_H
#define DB_CONNECTION_H

#include <pqxx/pqxx>
using namespace pqxx;

class CDBConnection
{
private:
	connection *m_Conn; // libpqxx Connection
	nontransaction *m_Txn; // libpqxx dummy Transaction

public:
	CDBConnection(const char *, const char *, const char *);
	~CDBConnection();

	void commit();
	string escape(string);
	result execute(string sql);
	void rollback();
	void startTransaction();
};

#endif //DB_CONNECTION_H
