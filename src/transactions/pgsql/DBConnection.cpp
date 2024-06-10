/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * The purpose of this class object is allow other classes to share the
 * same database connection.  The database connection is managed by this
 * class.
 *
 * 13 June 2006
 */

#include <catalog/pg_type_d.h>

#include "DBConnection.h"

// Constructor: Creates PgSQL connection
CDBConnection::CDBConnection(const char *szHost, const char *szDBName,
		const char *szDBPort, bool bVerbose)
: m_bVerbose(bVerbose)
{
	szConnectStr[0] = '\0';

	// Just pad everything with spaces so we don't have to figure out if it's
	// needed or not.
	if (strlen(szHost) > 0) {
		strcat(szConnectStr, " host=");
		strcat(szConnectStr, szHost);
	}
	if (strlen(szDBName) > 0) {
		strcat(szConnectStr, " dbname=");
		strcat(szConnectStr, szDBName);
	}
	if (strlen(szDBPort) > 0) {
		strcat(szConnectStr, " port=");
		strcat(szConnectStr, szDBPort);
	}

	pid_t pid = syscall(SYS_gettid);
	snprintf(name, sizeof(name), "%d", pid);
	m_Conn = PQconnectdb(szConnectStr);
}

// Destructor: Disconnect from server
CDBConnection::~CDBConnection()
{
	PQfinish(m_Conn);
}

void
CDBConnection::begin()
{
	PGresult *res = PQexec(m_Conn, "BEGIN;");
	PQclear(res);
}

void
CDBConnection::connect()
{
	m_Conn = PQconnectdb(szConnectStr);
}

void
CDBConnection::commit()
{
	PGresult *res = PQexec(m_Conn, "COMMIT;");
	PQclear(res);
}

char *
CDBConnection::escape(string s)
{
	char *esc = PQescapeLiteral(m_Conn, s.c_str(), s.length());
	if (esc == NULL)
		cerr << "ERROR: could not escape '" << s << "'" << endl;
	return esc;
}

void
CDBConnection::disconnect()
{
	PQfinish(m_Conn);
}

PGresult *
CDBConnection::exec(const char *sql)
{
	return exec(sql, 0, NULL, NULL, NULL, NULL, 0);
}

PGresult *
CDBConnection::exec(const char *sql, int nParams, const Oid *paramTypes,
		const char *const *paramValues, const int *paramLengths,
		const int *paramFormats, int resultFormat)
{
	// FIXME: Handle serialization errors.
	// For PostgreSQL, see comment in the Concurrency Control chapter, under
	// the Transaction Isolation section for dealing with serialization
	// failures.  These serialization failures can occur with REPEATABLE READS
	// or SERIALIZABLE.

	PGresult *res = PQexecParams(m_Conn, sql, nParams, paramTypes, paramValues,
			paramLengths, paramFormats, resultFormat);
	ExecStatusType status = PQresultStatus(res);

	switch (status) {
	case PGRES_COMMAND_OK:
	case PGRES_TUPLES_OK:
		return res;
	default:
		break;
	}

	pid_t pid = syscall(SYS_gettid);
	ostringstream msg;
	switch (status) {
	case PGRES_FATAL_ERROR:
		msg << pid << " " << time(NULL) << " " << endl
			<< "SQL: " << sql << endl
			<< PQresultErrorMessage(res) << endl;
		rollback();
		throw msg.str();
		break;
	case PGRES_EMPTY_QUERY:
	case PGRES_COPY_OUT:
	case PGRES_COPY_IN:
	case PGRES_BAD_RESPONSE:
	case PGRES_NONFATAL_ERROR:
	default:
		cout << pid << " *** " << PQresStatus(PQresultStatus(res)) << endl;
		break;
	}

	return res;
}

void
CDBConnection::execute(const TMarketFeedFrame1Input *pIn,
		TMarketFeedFrame1Output *pOut, CSendToMarketInterface *pMarketExchange)
{
	PGresult *res;
	PGresult *res2;

	pOut->num_updated = 0;
	pOut->send_len = 0;

	for (int i = 0; i < 20; i++) {
		begin();
		setRepeatableRead();

#define MFF1Q1                                                                \
	"UPDATE last_trade\n"                                                     \
	"SET lt_price = $1\n"                                                     \
	"  , lt_vol = lt_vol + $2\n"                                              \
	"  , lt_dts = CURRENT_TIMESTAMP\n"                                        \
	"WHERE lt_s_symb = $3"

		char price_quote[14];
		snprintf(price_quote, 13, "%f", pIn->Entries[i].price_quote);
		uint32_t trade_qty = htobe32((uint32_t) pIn->Entries[i].trade_qty);

		if (m_bVerbose) {
			cout << MFF1Q1 << endl;
			cout << "$1 = " << price_quote << endl;
			cout << "$2 = " << be32toh(trade_qty) << endl;
			cout << "$3 = " << pIn->Entries[i].symbol << endl;
		}

		const Oid paramTypes1[3] = { NUMERICOID, INT4OID, TEXTOID };
		const char *paramValues1[3]
				= { price_quote, (char *) &trade_qty, pIn->Entries[i].symbol };
		const int paramLengths1[3] = { sizeof(char) * 14, sizeof(uint32_t),
			sizeof(char) * (cSYMBOL_len + 1) };
		const int paramFormats1[3] = { 0, 1, 0 };

		res = exec(MFF1Q1, 3, paramTypes1, paramValues1, paramLengths1,
				paramFormats1, 0);
		pOut->num_updated += atoi(PQcmdTuples(res));
		PQclear(res);

#define MFF1Q2                                                                \
	"SELECT tr_t_id\n"                                                        \
	"     , tr_bid_price\n"                                                   \
	"     , tr_tt_id\n"                                                       \
	"     , tr_qty\n"                                                         \
	"FROM trade_request\n"                                                    \
	"WHERE tr_s_symb = $1\n"                                                  \
	"  AND (\n"                                                               \
	"           (tr_tt_id = $2 AND tr_bid_price >= $3)\n"                     \
	"        OR (tr_tt_id = $4 AND tr_bid_price <= $3)\n"                     \
	"        OR (tr_tt_id = $5 AND tr_bid_price >= $3)\n"                     \
	"      )"

		const char *paramValues2[5] = { pIn->Entries[i].symbol,
			pIn->StatusAndTradeType.type_stop_loss, price_quote,
			pIn->StatusAndTradeType.type_limit_sell,
			pIn->StatusAndTradeType.type_limit_buy };
		const int paramLengths2[5] = { sizeof(char) * (cSYMBOL_len + 1),
			sizeof(char) * (cTT_ID_len + 1), sizeof(char) * 14,
			sizeof(char) * (cTT_ID_len + 1), sizeof(char) * (cTT_ID_len + 1) };
		const int paramFormats2[5] = { 0, 0, 0, 0, 0 };

		if (m_bVerbose) {
			cout << MFF1Q2 << endl;
			cout << "$1 = " << pIn->Entries[i].symbol << endl;
			cout << "$2 = " << pIn->StatusAndTradeType.type_stop_loss << endl;
			cout << "$3 = " << price_quote << endl;
			cout << "$4 = " << pIn->StatusAndTradeType.type_limit_sell << endl;
			cout << "$5 = " << pIn->StatusAndTradeType.type_limit_buy << endl;
		}

		res = exec(MFF1Q2, 5, NULL, paramValues2, paramLengths2, paramFormats2,
				0);

		int count = PQntuples(res);
		for (int j = 0; j < count; j++) {
			uint64_t trade_id
					= htobe64((uint64_t) atoll(PQgetvalue(res, j, 0)));

			if (m_bVerbose) {
				cout << "t_id[" << j << "] = " << be64toh(trade_id);
			}

#define MFF1Q3                                                                \
	"UPDATE trade\n"                                                          \
	"SET t_dts = CURRENT_TIMESTAMP\n"                                         \
	"  , t_st_id = $1\n"                                                      \
	"WHERE t_id = $2"

			if (m_bVerbose) {
				cout << MFF1Q3 << endl;
				cout << "$1 = " << pIn->StatusAndTradeType.status_submitted
					 << endl;
				cout << "$2 = " << be64toh(trade_id) << endl;
			}

			const char *paramValues3[2]
					= { pIn->StatusAndTradeType.status_submitted,
						  (char *) &trade_id };
			const int paramLengths3[2]
					= { sizeof(char) * (cST_ID_len + 1), sizeof(uint64_t) };
			const int paramFormats3[2] = { 0, 1 };

			res2 = exec(MFF1Q3, 2, NULL, paramValues3, paramLengths3,
					paramFormats3, 0);
			PQclear(res2);

#define MFF1Q4                                                                \
	"DELETE FROM trade_request\n"                                             \
	"WHERE tr_t_id = $1"

			if (m_bVerbose) {
				cout << MFF1Q4 << endl;
				cout << "$1 = " << be64toh(trade_id) << endl;
			}

			const char *paramValues4[2] = { (char *) &trade_id,
				pIn->StatusAndTradeType.status_submitted };
			const int paramLengths4[2]
					= { sizeof(uint64_t), sizeof(char) * (cST_ID_len + 1) };
			const int paramFormats4[2] = { 1, 0 };

			res2 = exec(MFF1Q4, 1, NULL, paramValues4, paramLengths4,
					paramFormats4, 0);
			PQclear(res2);

#define MFF1Q5                                                                \
	"INSERT INTO trade_history\n"                                             \
	"VALUES (\n"                                                              \
	"    $1\n"                                                                \
	"  , CURRENT_TIMESTAMP\n"                                                 \
	"  , $2\n"                                                                \
	")"

			if (m_bVerbose) {
				cout << MFF1Q5 << endl;
				cout << "$1 = " << be64toh(trade_id) << endl;
				cout << "$2 = " << pIn->StatusAndTradeType.status_submitted
					 << endl;
			}

			res2 = exec(MFF1Q5, 2, NULL, paramValues4, paramLengths4,
					paramFormats4, 0);
			PQclear(res2);
		}

		commit();

		for (int j = 0; j < count; j++) {
			strncpy(m_TriggeredLimitOrders.symbol, pIn->Entries[i].symbol,
					cSYMBOL_len);
			m_TriggeredLimitOrders.trade_id = atoll(PQgetvalue(res, j, 0));
			m_TriggeredLimitOrders.price_quote = atof(PQgetvalue(res, j, 1));
			strncpy(m_TriggeredLimitOrders.trade_type_id,
					PQgetvalue(res, j, 2), cTT_ID_len);
			m_TriggeredLimitOrders.trade_qty = atoi(PQgetvalue(res, j, 3));

			if (m_bVerbose) {
				cout << "symbol[" << j
					 << "] = " << m_TriggeredLimitOrders.symbol;
				cout << "trade_id[" << j
					 << "] = " << m_TriggeredLimitOrders.trade_id;
				cout << "price_quote[" << j
					 << "] = " << m_TriggeredLimitOrders.price_quote;
				cout << "trade_type_id[" << j
					 << "] = " << m_TriggeredLimitOrders.trade_type_id;
				cout << "trade_qty[" << j
					 << "] = " << m_TriggeredLimitOrders.trade_qty;
			}

			bool bSent = pMarketExchange->SendToMarketFromFrame(
					m_TriggeredLimitOrders);
			if (!bSent) {
				cout << "WARNING: SendToMarketFromFrame() returned failure "
						"but continuing..."
					 << endl;
			}
			++pOut->send_len;
		}

		PQclear(res);
	}
}

void
CDBConnection::execute(const TTradeCleanupFrame1Input *pIn)
{
	PGresult *res = NULL;

#define TCF1Q1                                                                \
	"SELECT tr_t_id\n"                                                        \
	"FROM trade_request\n"                                                    \
	"ORDER BY tr_t_id"

	if (m_bVerbose) {
		cout << TCF1Q1 << endl;
	}

	res = exec(TCF1Q1);

	int n = PQntuples(res);
	for (int i = 0; i < n; i++) {
		PGresult *res2 = NULL;

		uint64_t tr_t_id = htobe64((uint64_t) atoll(PQgetvalue(res, i, 0)));

#define TCF1Q2                                                                \
	"INSERT INTO trade_history(\n"                                            \
	"    th_t_id\n"                                                           \
	"  , th_dts\n"                                                            \
	"  , th_st_id\n"                                                          \
	")\n"                                                                     \
	"VALUES (\n"                                                              \
	"    $1\n"                                                                \
	"  , CURRENT_TIMESTAMP\n"                                                 \
	"  , $2\n"                                                                \
	")\n"                                                                     \
	"ON CONFLICT DO NOTHING"

		if (m_bVerbose) {
			cout << TCF1Q2 << endl;
			cout << "$1 = " << be64toh(tr_t_id) << endl;
			cout << "$2 = " << pIn->st_submitted_id << endl;
		}

		const char *paramValues1[2]
				= { (char *) &tr_t_id, pIn->st_submitted_id };
		const int paramLengths1[2]
				= { sizeof(uint64_t), sizeof(char) * (cST_ID_len + 1) };
		const int paramFormats1[2] = { 1, 0 };

		res2 = exec(TCF1Q2, 2, NULL, paramValues1, paramLengths1,
				paramFormats1, 0);
		PQclear(res2);

#define TCF1Q3                                                                \
	"UPDATE trade\n"                                                          \
	"SET t_st_id = $1\n"                                                      \
	"  , t_dts = CURRENT_TIMESTAMP\n"                                         \
	"WHERE t_id = $2"

		if (m_bVerbose) {
			cout << TCF1Q3 << endl;
			cout << "$1 = " << pIn->st_canceled_id << endl;
			cout << "$2 = " << be64toh(tr_t_id) << endl;
		}

		const char *paramValues2[2]
				= { pIn->st_canceled_id, (char *) &tr_t_id };
		const int paramLengths2[2]
				= { sizeof(char) * (cST_ID_len + 1), sizeof(uint64_t) };
		const int paramFormats2[2] = { 0, 1 };

		res2 = exec(TCF1Q3, 2, NULL, paramValues2, paramLengths2,
				paramFormats2, 0);
		PQclear(res2);

#define TCF1Q4                                                                \
	"INSERT INTO trade_history(\n"                                            \
	"    th_t_id\n"                                                           \
	"  , th_dts\n"                                                            \
	"  , th_st_id\n"                                                          \
	")\n"                                                                     \
	"VALUES (\n"                                                              \
	"    $1\n"                                                                \
	"  , now()\n"                                                             \
	"  , $2\n"                                                                \
	")\n"                                                                     \
	"ON CONFLICT DO NOTHING"

		if (m_bVerbose) {
			cout << TCF1Q4 << endl;
			cout << "$1 = " << pIn->st_canceled_id << endl;
			cout << "$2 = " << be64toh(tr_t_id) << endl;
		}

		paramValues1[1] = pIn->st_canceled_id;

		res2 = exec(TCF1Q4, 2, NULL, paramValues1, paramLengths1,
				paramFormats1, 0);
		PQclear(res2);
	}
	PQclear(res);

#define TCF1Q5 "DELETE FROM trade_request"

	if (m_bVerbose) {
		cout << TCF1Q5 << endl;
	}
	res = exec(TCF1Q5);
	PQclear(res);

#define TCF1Q6                                                                \
	"SELECT t_id\n"                                                           \
	"FROM trade\n"                                                            \
	"WHERE t_id >= $1\n"                                                      \
	"  AND t_st_id = $2"

	uint64_t start_trade_id = htobe64((uint64_t) pIn->start_trade_id);

	if (m_bVerbose) {
		cout << TCF1Q6 << endl;
		cout << "$1 = " << be64toh(start_trade_id) << endl;
		cout << "$2 = " << pIn->st_submitted_id << endl;
	}

	const char *paramValues[2]
			= { (char *) &start_trade_id, pIn->st_submitted_id };
	const int paramLengths[2]
			= { sizeof(uint64_t), sizeof(char) * (cST_ID_len + 1) };
	const int paramFormats[2] = { 1, 0 };

	res = exec(TCF1Q6, 2, NULL, paramValues, paramLengths, paramFormats, 0);

	n = PQntuples(res);
	for (int i = 0; i < n; i++) {
		PGresult *res2 = NULL;

		uint64_t tr_t_id = htobe64((uint64_t) atoll(PQgetvalue(res, i, 0)));

#define TCF1Q7                                                                \
	"UPDATE trade\n"                                                          \
	"SET t_st_id = $1\n"                                                      \
	"  , t_dts = CURRENT_TIMESTAMP\n"                                         \
	"WHERE t_id = $2"

		if (m_bVerbose) {
			cout << TCF1Q7 << endl;
			cout << "$1 = " << pIn->st_canceled_id << endl;
			cout << "$2 = " << be64toh(tr_t_id) << endl;
		}

		const char *paramValues2[2]
				= { pIn->st_canceled_id, (char *) &tr_t_id };
		const int paramLengths2[2]
				= { sizeof(char) * (cST_ID_len + 1), sizeof(uint64_t) };
		const int paramFormats2[2] = { 0, 1 };

		res2 = exec(TCF1Q3, 2, NULL, paramValues2, paramLengths2,
				paramFormats2, 0);
		PQclear(res2);

#define TCF1Q8                                                                \
	"INSERT INTO trade_history(\n"                                            \
	"    th_t_id\n"                                                           \
	"  , th_dts\n"                                                            \
	"  , th_st_id\n"                                                          \
	")\n"                                                                     \
	"VALUES (\n"                                                              \
	"    $1\n"                                                                \
	"  , CURRENT_TIMESTAMP\n"                                                 \
	"  , $2\n"                                                                \
	")\n"                                                                     \
	"ON CONFLICT DO NOTHING"

		if (m_bVerbose) {
			cout << TCF1Q8 << endl;
			cout << "$1 = " << be64toh(tr_t_id) << endl;
			cout << "$2 = " << pIn->st_submitted_id << endl;
		}

		const char *paramValues1[2]
				= { (char *) &tr_t_id, pIn->st_canceled_id };
		const int paramLengths1[2]
				= { sizeof(uint64_t), sizeof(char) * (cST_ID_len + 1) };
		const int paramFormats1[2] = { 1, 0 };

		res2 = exec(TCF1Q8, 2, NULL, paramValues1, paramLengths1,
				paramFormats1, 0);
		PQclear(res2);
	}
	PQclear(res);
}


void
CDBConnection::reconnect()
{
	disconnect();
	connect();
}

void
CDBConnection::rollback()
{
	PGresult *res = PQexec(m_Conn, "ROLLBACK;");
	PQclear(res);
}

void
CDBConnection::setBrokerageHouse(CBrokerageHouse *bh)
{
	this->bh = bh;
}

void
CDBConnection::setReadCommitted()
{
	PGresult *res = PQexec(
			m_Conn, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
	PQclear(res);
}

void
CDBConnection::setReadUncommitted()
{
	PGresult *res = PQexec(
			m_Conn, "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;");
	PQclear(res);
}

void
CDBConnection::setRepeatableRead()
{
	PGresult *res = PQexec(
			m_Conn, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
	PQclear(res);
}

void
CDBConnection::setSerializable()
{
	PGresult *res
			= PQexec(m_Conn, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE");
	PQclear(res);
}
