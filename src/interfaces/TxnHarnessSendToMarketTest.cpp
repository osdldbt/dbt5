/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *               2010 Mark Wong
 *
 * 22 July 2006
 */

#include "TxnHarnessSendToMarketTest.h"

#include "MEESUTtest.h"
#include "EGenLogFormatterTab.h"
#include "EGenLogger.h"
#include "MEE.h"

CSendToMarketTest::CSendToMarketTest(TIdent iConfiguredCustomerCountIn,
		TIdent iActiveCustomerCountIn, char *szInDirIn)
{
	iConfiguredCustomerCount = iConfiguredCustomerCountIn;
	iActiveCustomerCount = iActiveCustomerCountIn;
	strncpy(szInDir, szInDirIn, iMaxPath);
}

CSendToMarketTest::~CSendToMarketTest()
{
}

bool CSendToMarketTest::SendToMarket(TTradeRequest &trade_mes)
{
	CLogFormatTab fmt;
	CEGenLogger log(eDriverEGenLoader, 0, "temp.log", &fmt);

	// Initialize MEE - Market Exchange Emulator class
	CMEESUTtest m_CMEESUT(NULL, iConfiguredCustomerCount, iActiveCustomerCount,
			szInDir);
	CInputFiles inputFiles;
	inputFiles.Initialize(eDriverEGenLoader, iConfiguredCustomerCount,
			iActiveCustomerCount, szInDir);

	CMEE m_CMEE(0, &m_CMEESUT, &log, inputFiles, 1);
	m_CMEE.SetBaseTime();
	
	cout<<endl<<"Sending to Market a ";
	if (strcmp(trade_mes.trade_type_id, "TLS") == 0 ||
			strcmp(trade_mes.trade_type_id, "TSL") == 0 ||
			strcmp(trade_mes.trade_type_id, "TLB") == 0) {
		// limit-order
		cout << "Limit-Order" << endl;
		m_CMEE.SubmitTradeRequest(&trade_mes);
	} else {
		// market-order
		cout << "Market-Order: Trade-Result & Market-Feed should trigger"
				<< endl;
		// we have to fill MEE's buffer, so it can trigger Market-Feed
		for (int i = 0; i <= 15; i++) {
			// we're submitting the same trade 20 times...
			m_CMEE.SubmitTradeRequest(&trade_mes);
			sleep(1); // ...one second between submissions
		}
	}
	return true;
}
