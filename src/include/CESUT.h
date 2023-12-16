/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * CE (Customer Emulator) - SUT (Brokerage House) connection class
 * 28 July 2006
 */

#ifndef CE_SUT_H
#define CE_SUT_H

#include "CE.h"
#include "CESUTInterface.h"
#include "locking.h"

#include "BaseInterface.h"
using namespace TPCE;

class CCESUT: public CCESUTInterface, public CBaseInterface
{
public:
	CCESUT(char *, char *, const int);
	~CCESUT(void);

	bool BrokerVolume(PBrokerVolumeTxnInput);
	bool CustomerPosition(PCustomerPositionTxnInput);
	bool MarketWatch(PMarketWatchTxnInput);
	bool SecurityDetail(PSecurityDetailTxnInput);
	bool TradeLookup(PTradeLookupTxnInput);
	bool TradeOrder(PTradeOrderTxnInput, INT32, bool);
	bool TradeStatus(PTradeStatusTxnInput);
	bool TradeUpdate(PTradeUpdateTxnInput);

private:
	struct TMsgDriverBrokerage request;
};

#endif // CE_SUT_H
