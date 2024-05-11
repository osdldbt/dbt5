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

#include "DBConnection.h"

// Constructor: Creates PgSQL connection
CDBConnection::CDBConnection(const char *szHost, const char *szDBName,
		const char *szDBPort)
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
	PGresult *res, *res2;
	ostringstream osSQL;

	char now_dts[30];
	now_dts[29] = '\0';

	res = exec("SELECT CURRENT_TIMESTAMP");
	strncpy(now_dts, PQgetvalue(res, 0, 0), 29);
	PQclear(res);

	pOut->num_updated = 0;
	pOut->send_len = 0;

	for (int i = 0; i < 20; i++) {

		begin();
		setRepeatableRead();

		osSQL.str("");
		osSQL << "UPDATE last_trade SET lt_price = "
			  << pIn->Entries[i].price_quote << ", lt_vol = lt_vol + "
			  << pIn->Entries[i].trade_qty << ", lt_dts = '" << now_dts
			  << "' WHERE lt_s_symb = '" << pIn->Entries[i].symbol << "'";
		res = exec(osSQL.str().c_str());
		pOut->num_updated += atoi(PQcmdTuples(res));
		PQclear(res);

		osSQL.str("");
		osSQL << "SELECT tr_t_id, tr_bid_price, tr_tt_id, tr_qty FROM "
				 "trade_request WHERE tr_s_symb = '"
			  << pIn->Entries[i].symbol << "' AND ((tr_tt_id = '"
			  << pIn->StatusAndTradeType.type_stop_loss
			  << "' AND tr_bid_price >= " << pIn->Entries[i].price_quote
			  << ") OR (tr_tt_id = '"
			  << pIn->StatusAndTradeType.type_limit_sell
			  << "' AND tr_bid_price <= " << pIn->Entries[i].price_quote
			  << ") OR (tr_tt_id = '" << pIn->StatusAndTradeType.type_limit_buy
			  << "' AND tr_bid_price >= " << pIn->Entries[i].price_quote
			  << "))";
		res = exec(osSQL.str().c_str());

		int i_tr_t_id = 0;
		int i_tr_bid_price = 1;
		int i_tr_qty = 2;
		int i_tr_tt_id = 3;

		for (int j = 0; j < PQntuples(res); j++) {
			osSQL.str("");
			osSQL << "UPDATE trade SET t_dts = '" << now_dts
				  << "', t_st_id = '"
				  << pIn->StatusAndTradeType.status_submitted
				  << "' WHERE t_id = " << PQgetvalue(res, j, i_tr_t_id);
			res2 = exec(osSQL.str().c_str());
			PQclear(res2);

			osSQL.str("");
			osSQL << "DELETE FROM trade_request WHERE tr_t_id = "
				  << PQgetvalue(res, j, i_tr_t_id);
			res2 = exec(osSQL.str().c_str());
			PQclear(res2);

			osSQL.str("");
			osSQL << "INSERT INTO trade_history VALUES ("
				  << PQgetvalue(res, j, i_tr_t_id) << ", '" << now_dts
				  << "', '" << pIn->StatusAndTradeType.status_submitted
				  << "')";
			res2 = exec(osSQL.str().c_str());
			PQclear(res2);
		}

		commit();

		m_TriggeredLimitOrders.symbol[cSYMBOL_len] = '\0';
		m_TriggeredLimitOrders.trade_type_id[cTT_ID_len] = '\0';
		for (int j = 0; j < PQntuples(res); j++) {
			strncpy(m_TriggeredLimitOrders.symbol, pIn->Entries[i].symbol,
					cSYMBOL_len);
			m_TriggeredLimitOrders.trade_id
					= atoll(PQgetvalue(res, j, i_tr_t_id));
			m_TriggeredLimitOrders.price_quote
					= atof(PQgetvalue(res, j, i_tr_bid_price));
			m_TriggeredLimitOrders.trade_qty
					= atoi(PQgetvalue(res, j, i_tr_qty));
			strncpy(m_TriggeredLimitOrders.trade_type_id,
					PQgetvalue(res, j, i_tr_tt_id), cTT_ID_len);

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
