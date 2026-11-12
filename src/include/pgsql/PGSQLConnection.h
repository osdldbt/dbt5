/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * PostgreSQL connection class
 * 13 June 2006
 */

#ifndef PGSQL_CONNECTION_H
#define PGSQL_CONNECTION_H

#include <libpq-fe.h>
#include <unistd.h>
#include <sys/syscall.h>

#include "TxnHarnessStructs.h"
#include "TxnHarnessSendToMarket.h"

#include "BrokerageHouse.h"
#include "DBConnection.h"
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

class CPGSQLConnection: public CDBConnection
{
	char szConnectStr[iMaxConnectString + 1];
	char name[16];

	CBrokerageHouse *bh;

	TTradeRequest m_TriggeredLimitOrders;

protected:
	PGconn *m_Conn;
	bool m_bVerbose;

public:
	CPGSQLConnection(const char *szHost, const char *szDBName,
			const char *szDBPort, bool bVerbose = false);
	virtual ~CPGSQLConnection();

	void begin();
	void commit();
	void connect();
	string escape(string);
	void disconnect();

	PGresult *exec(const char *);
	PGresult *exec(const char *, int, const Oid *, const char *const *,
			const int *, const int *, int);

	void execute(const TMarketFeedFrame1Input *, TMarketFeedFrame1Output *,
			CSendToMarketInterface *);

	void execute(const TTradeCleanupFrame1Input *);

	void reconnect();

	void rollback();

	void setBrokerageHouse(CBrokerageHouse *);

	void setReadCommitted();
	void setReadUncommitted();
	void setRepeatableRead();
	void setSerializable();
};

#endif // PGSQL_CONNECTION_H
