/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2010 Mark Wong
 */

#ifndef DBT5_CONSTS_H
#define DBT5_CONSTS_H

namespace TPCE
{
const int iMaxPort = 8;

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
}

#endif	// DBT5_CONSTS_H
