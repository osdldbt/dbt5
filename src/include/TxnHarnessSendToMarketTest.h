/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * Test version
 * 22 July 2006
 */

#ifndef TXN_HARNESS_SENDTOMARKET_TEST_H
#define TXN_HARNESS_SENDTOMARKET_TEST_H

#include "MiscConsts.h"
#include "TxnHarnessSendToMarketInterface.h"
using namespace TPCE;

class CSendToMarketTest: public CSendToMarketInterface
{
public:
	CSendToMarketTest(TIdent, TIdent, char *);
	~CSendToMarketTest();

	bool SendToMarket(TTradeRequest &);

private:
	TIdent iConfiguredCustomerCount;
	TIdent iActiveCustomerCount;
	char szInDir[iMaxPath + 1];
};

#endif // TXN_HARNESS_SENDTOMARKET_TEST_H
