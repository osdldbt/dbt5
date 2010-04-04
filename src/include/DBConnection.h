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

#include "BrokerageHouse.h"
#include "DBT5Consts.h"
using namespace TPCE;

class CDBConnection
{
private:
	connection *m_Conn; // libpqxx Connection
	nontransaction *m_Txn; // libpqxx dummy Transaction

	char szConnectStr[iMaxConnectString + 1];
	char name[16];

	CBrokerageHouse *bh;

	TTradeRequest m_TriggeredLimitOrders;

public:
	CDBConnection(const char *, const char *, const char *);
	~CDBConnection();

	void begin();

	void commit();
	void connect();
	string escape(string);
	void disconnect();

	void execute(const TBrokerVolumeFrame1Input *,
			TBrokerVolumeFrame1Output *);

	void execute(const TCustomerPositionFrame1Input *,
			TCustomerPositionFrame1Output *);
	void execute(const TCustomerPositionFrame2Input *,
			TCustomerPositionFrame2Output *);

	void execute(const TDataMaintenanceFrame1Input *,
			TDataMaintenanceFrame1Output *);

	void execute(const TMarketFeedFrame1Input *, TMarketFeedFrame1Output *,
			CSendToMarketInterface *);

	void execute(const TMarketWatchFrame1Input *, TMarketWatchFrame1Output *);

	void execute(const TSecurityDetailFrame1Input *,
			TSecurityDetailFrame1Output *);

	void execute(const TTradeCleanupFrame1Input *,
			TTradeCleanupFrame1Output *);

	void execute(const TTradeLookupFrame1Input *, TTradeLookupFrame1Output *);
	void execute(const TTradeLookupFrame2Input *, TTradeLookupFrame2Output *);
	void execute(const TTradeLookupFrame3Input *, TTradeLookupFrame3Output *);
	void execute(const TTradeLookupFrame4Input *, TTradeLookupFrame4Output *);

	void execute(const TTradeOrderFrame1Input *, TTradeOrderFrame1Output *);
	void execute(const TTradeOrderFrame2Input *, TTradeOrderFrame2Output *);
	void execute(const TTradeOrderFrame3Input *, TTradeOrderFrame3Output *);
	void execute(const TTradeOrderFrame4Input *, TTradeOrderFrame4Output *);

	void execute(const TTradeResultFrame1Input *, TTradeResultFrame1Output *);
	void execute(const TTradeResultFrame2Input *, TTradeResultFrame2Output *);
	void execute(const TTradeResultFrame3Input *, TTradeResultFrame3Output *);
	void execute(const TTradeResultFrame4Input *, TTradeResultFrame4Output *);
	void execute(const TTradeResultFrame5Input *, TTradeResultFrame5Output *);
	void execute(const TTradeResultFrame6Input *, TTradeResultFrame6Output *);

	void execute(const TTradeStatusFrame1Input *, TTradeStatusFrame1Output *);

	void execute(const TTradeUpdateFrame1Input *, TTradeUpdateFrame1Output *);
	void execute(const TTradeUpdateFrame2Input *, TTradeUpdateFrame2Output *);
	void execute(const TTradeUpdateFrame3Input *, TTradeUpdateFrame3Output *);

	void reconnect();

	void rollback();

	void setBrokerageHouse(CBrokerageHouse *);

	void setReadCommitted();
	void setReadUncommitted();
	void setRepeatableRead();
	void setSerializable();
};

#endif //DB_CONNECTION_H
