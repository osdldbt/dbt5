/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *               2010 Mark Wong
 */

#ifndef DBT5_CONSTS_H
#define DBT5_CONSTS_H

#include <string>
using namespace std;

namespace TPCE
{
const int iMaxPort = 8;
const int iMaxRetries = 10;
const int iMaxConnectString = 128;

const int iBrokerageHousePort = 30000;
const int iMarketExchangePort = 30010;

// Transaction Names
static const char szTransactionName[12][18] = {
		"TRADE_ORDER",
		"TRADE_RESULT",
		"TRADE_LOOKUP",
		"TRADE_UPDATE",
		"TRADE_STATUS",
		"CUSTOMER_POSITION",
		"BROKER_VOLUME",
		"SECURITY_DETAIL",
		"MARKET_FEED",
		"MARKET_WATCH",
		"DATA_MAINTENANCE",
		"TRADE_CLEANUP"};

// PostgreSQL Messages
static std::string PGSQL_SERIALIZE_ERROR =
		"ERROR:  could not serialize access due to concurrent update";
static std::string PGSQL_RECOVERY_ERROR =
		"FATAL:  the database system is in recovery mode";
static std::string PGSQL_CONNECTION_FAILED = "Connection to database failed";
}

#endif	// DBT5_CONSTS_H
