/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * Database connection interface
 * 13 June 2006
 *
 * This is what the transaction classes talk to.  Each supported DBMS provides
 * an implementation in its own directory (e.g. pgsql/) and
 * CreateDBConnection().
 */

#ifndef DB_CONNECTION_H
#define DB_CONNECTION_H

#include <unistd.h>
#include <sys/syscall.h>

#include "TxnHarnessStructs.h"
#include "TxnHarnessSendToMarket.h"

#include "BrokerageHouse.h"
#include "DBT5Consts.h"
using namespace TPCE;

/*
 * Thrown for errors that abort a transaction but are safe to retry,
 * such as serialization failures and deadlocks.  Derives from string
 * so handlers that only catch the generic error string still work.
 */
class CDBRetryableError: public string
{
public:
	explicit CDBRetryableError(const string &msg): string(msg) {}
};

class CDBConnection
{
public:
	virtual ~CDBConnection() {}

	virtual void begin() = 0;
	virtual void commit() = 0;
	virtual void connect() = 0;
	virtual string escape(string) = 0;
	virtual void disconnect() = 0;

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

	virtual void execute(const TMarketFeedFrame1Input *,
			TMarketFeedFrame1Output *, CSendToMarketInterface *)
			= 0;

	virtual void execute(
			const TMarketWatchFrame1Input *, TMarketWatchFrame1Output *)
			= 0;

	virtual void execute(
			const TSecurityDetailFrame1Input *, TSecurityDetailFrame1Output *)
			= 0;

	virtual void execute(const TTradeCleanupFrame1Input *) = 0;

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

	virtual void reconnect() = 0;

	virtual void rollback() = 0;

	virtual void setBrokerageHouse(CBrokerageHouse *) = 0;

	virtual void setReadCommitted() = 0;
	virtual void setReadUncommitted() = 0;
	virtual void setRepeatableRead() = 0;
	virtual void setSerializable() = 0;
};

/*
 * Creates a connection for the DBMS this kit was built for.  Transactions
 * are executed with client side SQL when bClientSide is true, otherwise with
 * the DBMS's server side functions.  Defined by the DBMS specific code.
 */
CDBConnection *CreateDBConnection(bool bClientSide, const char *szHost,
		const char *szDBName, const char *szDBPort, bool bVerbose = false);

#endif // DB_CONNECTION_H
