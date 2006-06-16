/*
 *  Include file for transactions' include files
 *  2006 Rilson Nascimento
 */ 

#ifndef TRANSACTIONS_H
#define TRANSACTIONS_H

#include <iostream>
using namespace std;

// libpqxx's header and namespace
#include <pqxx/pqxx>
using namespace pqxx;

#include "../EGen_v3.14/inc/TxnHarnessStructs.h"

#include "DBConnection.h"
#include "TxnBaseDB.h"
#include "TradeStatusDB.h"
// #include "BrokerVolumeDB.h"
// #include "CustomerPositionDB.h"
// #include "DataMaintenanceDB.h"
// #include "MarketFeedDB.h"
// #include "MarketWatchDB.h"
// #include "SecurityDetailDB.h"
// #include "TradeLookupDB.h"
// #include "TradeOrderDB.h"
// #include "TradeResultDB.h"
// #include "TradeStatusDB.h"
// #include "TradeUpdateDB.h"

#endif	// #ifndef TRANSACTIONS_H
