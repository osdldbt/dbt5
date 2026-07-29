/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * PostgreSQL connection class
 * 13 June 2006
 */

#ifndef DB_CONNECTION_H
#define DB_CONNECTION_H

#include <libpq-fe.h>
#include <unistd.h>
#include <sys/syscall.h>

#include "TxnHarnessStructs.h"
#include "TxnHarnessSendToMarket.h"

#include "BrokerageHouse.h"
#include "DBT5Consts.h"
using namespace TPCE;

/*
 * PostgreSQL binary DATE values count days from 2000-01-01 and binary
 * TIMESTAMP values count microseconds from 2000-01-01 00:00:00.  These
 * convert Gregorian calendar values directly so the results do not
 * depend on the process time zone the way mktime() does.
 */
int64_t daysFromPgEpoch(int year, int month, int day);
uint64_t usecFromPgEpoch(const TIMESTAMP_STRUCT *ts);

/*
 * Clears a PGresult when leaving scope so a result held across other
 * exec() calls is not leaked when one of them throws.
 */
class PGresultHolder
{
	PGresult *m_res;

	// non-copyable
	PGresultHolder(const PGresultHolder &);
	PGresultHolder &operator=(const PGresultHolder &);

public:
	explicit PGresultHolder(PGresult *res): m_res(res) {}
	~PGresultHolder()
	{
		PQclear(m_res);
	}
};

class CDBConnection
{
	char szConnectStr[iMaxConnectString + 1];
	char name[16];

	CBrokerageHouse *bh;

	TTradeRequest m_TriggeredLimitOrders;

protected:
	PGconn *m_Conn;
	bool m_bVerbose;

public:
	CDBConnection(const char *szHost, const char *szDBName,
			const char *szDBPort, bool bVerbose = false);
	virtual ~CDBConnection();

	void begin();
	void commit();
	void connect();
	string escape(string);
	void disconnect();

	PGresult *exec(const char *);
	PGresult *exec(const char *, int, const Oid *, const char *const *,
			const int *, const int *, int);

	virtual void execute(
			const TBrokerVolumeFrame1Input *, TBrokerVolumeFrame1Output *)
			= 0;

	virtual void execute(const TCustomerPositionFrame1Input *,
			TCustomerPositionFrame1Output *)
			= 0;
	virtual void execute(const TCustomerPositionFrame2Input *,
			TCustomerPositionFrame2Output *)
			= 0;

	virtual void execute(const TDataMaintenanceFrame1Input *) = 0;

	void execute(const TMarketFeedFrame1Input *, TMarketFeedFrame1Output *,
			CSendToMarketInterface *);

	virtual void execute(
			const TMarketWatchFrame1Input *, TMarketWatchFrame1Output *)
			= 0;

	virtual void execute(
			const TSecurityDetailFrame1Input *, TSecurityDetailFrame1Output *)
			= 0;

	virtual void execute(const TTradeCleanupFrame1Input *);

	virtual void execute(
			const TTradeLookupFrame1Input *, TTradeLookupFrame1Output *)
			= 0;
	virtual void execute(
			const TTradeLookupFrame2Input *, TTradeLookupFrame2Output *)
			= 0;
	virtual void execute(
			const TTradeLookupFrame3Input *, TTradeLookupFrame3Output *)
			= 0;
	virtual void execute(
			const TTradeLookupFrame4Input *, TTradeLookupFrame4Output *)
			= 0;

	virtual void execute(
			const TTradeOrderFrame1Input *, TTradeOrderFrame1Output *)
			= 0;
	virtual void execute(
			const TTradeOrderFrame2Input *, TTradeOrderFrame2Output *)
			= 0;
	virtual void execute(
			const TTradeOrderFrame3Input *, TTradeOrderFrame3Output *)
			= 0;
	virtual void execute(
			const TTradeOrderFrame4Input *, TTradeOrderFrame4Output *)
			= 0;

	virtual void execute(
			const TTradeResultFrame1Input *, TTradeResultFrame1Output *)
			= 0;
	virtual void execute(
			const TTradeResultFrame2Input *, TTradeResultFrame2Output *)
			= 0;
	virtual void execute(
			const TTradeResultFrame3Input *, TTradeResultFrame3Output *)
			= 0;
	virtual void execute(
			const TTradeResultFrame4Input *, TTradeResultFrame4Output *)
			= 0;
	virtual void execute(const TTradeResultFrame5Input *) = 0;
	virtual void execute(
			const TTradeResultFrame6Input *, TTradeResultFrame6Output *)
			= 0;

	virtual void execute(
			const TTradeStatusFrame1Input *, TTradeStatusFrame1Output *)
			= 0;

	virtual void execute(
			const TTradeUpdateFrame1Input *, TTradeUpdateFrame1Output *)
			= 0;
	virtual void execute(
			const TTradeUpdateFrame2Input *, TTradeUpdateFrame2Output *)
			= 0;
	virtual void execute(
			const TTradeUpdateFrame3Input *, TTradeUpdateFrame3Output *)
			= 0;

	void reconnect();

	void rollback();

	void setBrokerageHouse(CBrokerageHouse *);

	void setReadCommitted();
	void setReadUncommitted();
	void setRepeatableRead();
	void setSerializable();
};

#endif // DB_CONNECTION_H
