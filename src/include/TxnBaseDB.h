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
