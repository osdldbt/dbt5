/*
 * TxnHarnessSendToMarketTest.cpp
 *
 * 2006 Rilson Nascimento
 * 2010 Mark Wong
 *
 * 22 July 2006
 */

#include <transactions.h>
#include <MEESUTtest.h>
#include <TxnHarnessSendToMarketTest.h>

using namespace TPCE;

CSendToMarketTest::CSendToMarketTest(TIdent iConfiguredCustomerCountIn,
		TIdent iActiveCustomerCountIn, char *szInDirIn)
{
	iConfiguredCustomerCount = iConfiguredCustomerCountIn;
	iActiveCustomerCount = iActiveCustomerCountIn;
	strncpy(szInDir, szInDirIn, iInDirLen2);
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

	CMEE		m_CMEE( 0, &m_CMEESUT, &log, inputFiles, 1 );
	m_CMEE.SetBaseTime();
	
	cout<<endl<<"Sending to Market a ";
	if (strcmp(trade_mes.trade_type_id, "TLS") == 0
			|| strcmp(trade_mes.trade_type_id, "TSL") == 0
			|| strcmp(trade_mes.trade_type_id, "TLB") == 0) // limit-order
	{
		cout<<"Limit-Order"<<endl;
		m_CMEE.SubmitTradeRequest( &trade_mes );
	}
	else // market-order
	{
		cout<<"Market-Order: Trade-Result & Market-Feed should trigger"<<endl;
		// we have to fill MEE's buffer, so it can trigger Market-Feed
		for (int i=0; i<=15; i++)
		{
			// we're submitting the same trade 20 times...
			m_CMEE.SubmitTradeRequest( &trade_mes );
			sleep(1);				  // ...one second between submissions
		}
	}
	return true;
}
