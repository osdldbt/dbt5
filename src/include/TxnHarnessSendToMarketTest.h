/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *
 * Test version
 * 22 July 2006
 */

#ifndef TXN_HARNESS_SENDTOMARKET_TEST_H
#define TXN_HARNESS_SENDTOMARKET_TEST_H

const int iInDirLen2 = 255;

class CSendToMarketTest : public CSendToMarketInterface
{
public:
	CSendToMarketTest(TIdent iConfiguredCustomerCountIn,
			TIdent iActiveCustomerCountIn, char *szInDirIn);
	~CSendToMarketTest();

	virtual bool SendToMarket(TTradeRequest &trade_mes);

private:
	TIdent iConfiguredCustomerCount;
	TIdent iActiveCustomerCount;
	char szInDir[iInDirLen2 + 1];
};

#endif	// TXN_HARNESS_SENDTOMARKET_TEST_H
