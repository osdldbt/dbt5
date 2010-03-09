/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *
 * MEE (Market Exchange Emulator) - SUT Interface test class
 * 23 July 2006
 */

#ifndef MEE_SUT_TEST_H
#define MEE_SUT_TEST_H

#include "MEESUTInterface.h"

using namespace TPCE;

class CMEESUTtest : public CMEESUTInterface
{
private:
	CDBConnection*		m_pDBConnection;
	TTradeResultTxnInput	m_TradeResultTxnInput;
	TMarketFeedTxnInput	m_MarketFeedTxnInput;

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
	virtual bool TradeResult( PTradeResultTxnInput pTxnInput );
	// return whether it was successful
	virtual bool MarketFeed( PMarketFeedTxnInput pTxnInput );
	
	friend void *TradeResultAsync(void* data);
	friend bool RunTradeResultAsync( CMEESUTtest* );

	friend void *MarketFeedAsync(void* data);
	friend bool RunMarketFeedAsync( CMEESUTtest* );

	TIdent iConfiguredCustomerCount;
	TIdent iActiveCustomerCount;
	char szInDir[256];
};

#endif	// MEE_SUT_TEST_H
