/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2010 Rilson Nascimento
 *
 * DM - SUT Interface class
 * 17 July 2006
 */

#ifndef DM_SUT_TEST_H
#define DM_SUT_TEST_H

#include "DMSUTInterface.h"

#include "TxnHarnessBrokerVolume.h"
#include "TxnHarnessCustomerPosition.h"
#include "TxnHarnessDataMaintenance.h"
#include "TxnHarnessMarketFeed.h"
#include "TxnHarnessMarketWatch.h"
#include "TxnHarnessSecurityDetail.h"
#include "TxnHarnessTradeCleanup.h"
#include "TxnHarnessTradeLookup.h"
#include "TxnHarnessTradeOrder.h"
#include "TxnHarnessTradeResult.h"
#include "TxnHarnessTradeStatus.h"
#include "TxnHarnessTradeUpdate.h"

#include "DM.h"

#include "DBConnection.h"

#include "BrokerVolumeDB.h"
#include "CustomerPositionDB.h"
#include "DataMaintenanceDB.h"
#include "MarketFeedDB.h"
#include "MarketWatchDB.h"
#include "SecurityDetailDB.h"
#include "TradeCleanupDB.h"
#include "TradeLookupDB.h"
#include "TradeOrderDB.h"
#include "TradeResultDB.h"
#include "TradeStatusDB.h"
#include "TradeUpdateDB.h"
using namespace TPCE;

class CDMSUTtest : public CDMSUTInterface
{
protected:
	CDBConnection *m_pDBConnection;

public:
	CDMSUTtest(CDBConnection *);
	~CDMSUTtest();

	// return whether it was successful
	virtual bool DataMaintenance(PDataMaintenanceTxnInput);
	// return whether it was successful
	virtual bool TradeCleanup(PTradeCleanupTxnInput);
};

#endif	// DM_SUT_TEST_H
