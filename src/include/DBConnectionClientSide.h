/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * PostgreSQL connection class
 */

#ifndef DB_CONNECTION_CLIENT_SIDE_H
#define DB_CONNECTION_CLIENT_SIDE_H

#include <libpq-fe.h>
#include <unistd.h>
#include <sys/syscall.h>

#include "DBConnection.h"
#include "DBConnectionClientSide.h"

#include "TxnHarnessStructs.h"
#include "TxnHarnessSendToMarket.h"

#include "BrokerageHouse.h"
#include "DBT5Consts.h"
using namespace TPCE;

class CDBConnectionClientSide: public CDBConnection
{
public:
	CDBConnectionClientSide(const char *szHost, const char *szDBName,
			const char *szDBPort, bool bVerbose = false);
	~CDBConnectionClientSide();

	void execute(
			const TBrokerVolumeFrame1Input *, TBrokerVolumeFrame1Output *);

	void execute(const TCustomerPositionFrame1Input *,
			TCustomerPositionFrame1Output *);
	void execute(const TCustomerPositionFrame2Input *,
			TCustomerPositionFrame2Output *);

	void execute(const TDataMaintenanceFrame1Input *);

	void execute(const TMarketWatchFrame1Input *, TMarketWatchFrame1Output *);

	void execute(
			const TSecurityDetailFrame1Input *, TSecurityDetailFrame1Output *);

	void execute(const TTradeCleanupFrame1Input *);

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
	void execute(const TTradeResultFrame5Input *);
	void execute(const TTradeResultFrame6Input *, TTradeResultFrame6Output *);

	void execute(const TTradeStatusFrame1Input *, TTradeStatusFrame1Output *);

	void execute(const TTradeUpdateFrame1Input *, TTradeUpdateFrame1Output *);
	void execute(const TTradeUpdateFrame2Input *, TTradeUpdateFrame2Output *);
	void execute(const TTradeUpdateFrame3Input *, TTradeUpdateFrame3Output *);
};

#endif // DB_CONNECTION_CLIENT_SIDE_H
