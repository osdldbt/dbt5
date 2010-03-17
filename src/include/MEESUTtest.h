/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2010 Rilson Nascimento
 *
 * MEE (Market Exchange Emulator) - SUT Interface test class
 * 23 July 2006
 */

#ifndef MEE_SUT_TEST_H
#define MEE_SUT_TEST_H

#include "MiscConsts.h"
#include "MEESUTInterface.h"

#include "DBConnection.h"
using namespace TPCE;

class CMEESUTtest : public CMEESUTInterface
{
private:
	CDBConnection *m_pDBConnection;
	TTradeResultTxnInput m_TradeResultTxnInput;
	TMarketFeedTxnInput m_MarketFeedTxnInput;

public:
	CMEESUTtest(CDBConnection *pDBConn, TIdent iConfiguredCustomerCountIn,
			TIdent iActiveCustomerCountIn,
			char *szInDirIn) : m_pDBConnection(pDBConn) {
		iConfiguredCustomerCount = iConfiguredCustomerCountIn;
		iActiveCustomerCount = iActiveCustomerCountIn;
		strncpy(szInDir, szInDirIn, 256);
	};
	~CMEESUTtest() { };

	// return whether it was successful
	virtual bool TradeResult(PTradeResultTxnInput);
	// return whether it was successful
	virtual bool MarketFeed( PMarketFeedTxnInput);
	
	friend void *TradeResultAsync(void *);
	friend bool RunTradeResultAsync(CMEESUTtest *);

	friend void *MarketFeedAsync(void *);
	friend bool RunMarketFeedAsync(CMEESUTtest *);

	TIdent iConfiguredCustomerCount;
	TIdent iActiveCustomerCount;
	char szInDir[iMaxPath + 1];
};

#endif	// MEE_SUT_TEST_H
