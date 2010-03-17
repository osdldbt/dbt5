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

#include "TxnHarnessStructs.h"
#include "TxnHarnessSendToMarket.h"
using namespace TPCE;

class CDBConnection
{
private:
	connection *m_Conn; // libpqxx Connection
	nontransaction *m_Txn; // libpqxx dummy Transaction

	TTradeRequest m_TriggeredLimitOrders;

public:
	CDBConnection(const char *, const char *, const char *);
	~CDBConnection();

	void begin();

	void commit();
	string escape(string);

	void execute(string, TBrokerVolumeFrame1Output *);

	void execute(string, TCustomerPositionFrame1Output *);
	void execute(string, TCustomerPositionFrame2Output *);

	void execute(string, TDataMaintenanceFrame1Output *);

	void execute(string, TMarketFeedFrame1Output *, CSendToMarketInterface *);

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

	void rollback();

	void setReadCommitted();
	void setReadUncommitted();
	void setRepeatableRead();
	void setSerializable();
};

#endif //DB_CONNECTION_H
