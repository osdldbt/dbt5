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

	void commitTransaction();
	string escape(string);

	void execute(string, TBrokerVolumeFrame1Output *);

	void execute(string, TCustomerPositionFrame1Output *);
	void execute(string, TCustomerPositionFrame2Output *);

	void execute(string, TDataMaintenanceFrame1Output *);

	void execute(string sql, TMarketFeedFrame1Output *,
        CSendToMarketInterface *);

	void execute(string, TMarketWatchFrame1Output *);

	void execute(string, TSecurityDetailFrame1Output *);

	void execute(string, TTradeCleanupFrame1Output *);

	void execute(string, TTradeLookupFrame1Output *);
	void execute(string, TTradeLookupFrame2Output *);
	void execute(string, TTradeLookupFrame3Output *);
	void execute(string, TTradeLookupFrame4Output *);

	void execute(string, TTradeOrderFrame1Output *);
	void execute(string, TTradeOrderFrame2Output *);
	void execute(string, TTradeOrderFrame3Output *);
	void execute(string, TTradeOrderFrame4Output *);

	void execute(string, TTradeResultFrame1Output *);
	void execute(string, TTradeResultFrame2Output *);
	void execute(string, TTradeResultFrame3Output *);
	void execute(string, TTradeResultFrame4Output *);
	void execute(string, TTradeResultFrame5Output *);
	void execute(string, TTradeResultFrame6Output *);

	void execute(string, TTradeStatusFrame1Output *);

	void execute(string, TTradeUpdateFrame1Output *);
	void execute(string, TTradeUpdateFrame2Output *);
	void execute(string, TTradeUpdateFrame3Output *);

	void reconect();

	void rollbackTransaction();

	void setReadCommitted();
	void setReadUncommitted();
	void setRepeatableRead();
	void setSerializable();

	void startTransaction();

public:
	CTxnBaseDB(CDBConnection *pDB);
	~CTxnBaseDB();
};

#endif // TXN_BASE_DB_H
