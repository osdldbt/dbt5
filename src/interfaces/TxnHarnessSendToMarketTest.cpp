/*
 * TxnHarnessSendToMarketTest.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 22 July 2006
 */

#include <transactions.h>
#include <MEESUTtest.h>
#include <TxnHarnessSendToMarketTest.h>

using namespace TPCE;

CSendToMarketTest::CSendToMarketTest()
{
}

CSendToMarketTest::~CSendToMarketTest()
{
}

bool CSendToMarketTest::SendToMarket(TTradeRequest &trade_mes)
{
	const char *server = "localhost";
	const char *db = "dbt5";
	const char *port = "5432";
	//CDBConnection		m_Conn( server, db, port );

	CLogFormatTab fmt;
	CEGenLogger log(eDriverEGenLoader, 0, "temp.log", &fmt);

	// Initialize MEE - Market Exchange Emulator class
	CMEESUTtest		m_CMEESUT( NULL /*&m_Conn*/ ); // I am creating connections on the fly inside the thread
	char*	szFileName = "EGen_v3.14/flat_in/Security.txt";
	CSecurityFile	*Securities = new CSecurityFile(szFileName, iDefaultLoadUnitSize, iDefaultLoadUnitSize);

	CMEE		m_CMEE( 0, &m_CMEESUT, &log, Securities, 1 );
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
		for (int i=0; i<=15; i++)  // we have to fill MEE's buffer, so it can trigger Market-Feed
		{
			m_CMEE.SubmitTradeRequest( &trade_mes );  // we're submitting the same trade 20 times...
			sleep(1);				  // ...one second between submissions
		}
	}
	return true;
}
