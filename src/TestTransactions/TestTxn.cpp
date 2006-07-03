// 2006 Rilson Nascimento
// Test of TradeStatus transaction

#include "../include/transactions.h"
#include "../include/harness.h"
#include "../EGen_v3.14/inc/CETxnInputGenerator.h"

using namespace TPCE;

using namespace TPCE;

enum eTxnType {
	TRADE_ORDER = 0,
	TRADE_RESULT,
	TRADE_LOOKUP,
	TRADE_UPDATE,
	TRADE_STATUS,
	CUSTOMER_POSITION,
	BROKER_VOLUME,
	SECURITY_DETAIL,
	MARKET_FEED,
	MARKET_WATCH,
	DATA_MAINTENANCE,
	TRADE_CLEANUP
};
eTxnType TxnType = TRADE_STATUS;


// shows program usage
void Usage()
{
	cout<<"\nUsage: TestTxn [option]"<<endl<<endl;
	cout<<"  where"<<endl;
	cout<<"   Option		Description"<<endl;
	cout<<"   =========		========================"<<endl;
	cout<<"   -t number		Transaction type"<<endl;
	cout<<"\t\t\tA - TRADE_ORDER"<<endl;
	cout<<"\t\t\tB - TRADE_RESULT"<<endl;
	cout<<"\t\t\tC - TRADE_LOOKUP"<<endl;
	cout<<"\t\t\tD - TRADE_UPDATE"<<endl;
	cout<<"\t\t\tE - TRADE_STATUS (default)"<<endl;
	cout<<"\t\t\tF - CUSTOMER_POSITION"<<endl;
	cout<<"\t\t\tG - BROKER_VOLUME"<<endl;
	cout<<"\t\t\tH - SECURITY_DETAIL"<<endl;
	cout<<"\t\t\tI - MARKET_FEED"<<endl;
	cout<<"\t\t\tJ - MARKET_WATCH"<<endl;
	cout<<"\t\t\tK - DATA_MAINTENANCE"<<endl;
	cout<<"\t\t\tL - TRADE_CLEANUP"<<endl<<endl;
}


// parse command line
bool ParseCommandLine( int argc, char *argv[] )
{
	int   arg;
	char  *sp;
	char  *vp;

	if (argc < 2) 
	{
		// use default
		return(true);
	}

	/*
	*  Scan the command line arguments
	*/
	for ( arg = 1; arg < argc; ++arg ) {
		/*
		*  Look for a switch 
		*/
		sp = argv[arg];
		if ( *sp == '-' ) 
		{
			++sp;
		}
		*sp = (char)tolower( *sp );
		
		/*
		*  Find the switch's argument.  It is either immediately after the
		*  switch or in the next argv
		*/
		vp = sp + 1;
			// Allow for switched that don't have any parameters.
			// Need to check that the next argument is in fact a parameter
			// and not the next switch that starts with '-'.
			//
		if ( (*vp == 0) && ((arg + 1) < argc) && (argv[arg + 1][0] != '-') )
		{
			vp = argv[++arg];
		}
		
		/*
		*  Parse the switch
		*/
		switch ( *sp ) {
		case 't':
			switch ( *vp) {
			case 'A':
				TxnType = TRADE_ORDER;
				break;
			case 'B':
				TxnType = TRADE_RESULT;
				break;
			case 'C':
				TxnType = TRADE_LOOKUP;
				break;
			case 'D': 
				TxnType = TRADE_UPDATE;
				break;
			case 'E':
				TxnType = TRADE_STATUS;
				break;
			case 'F':
				TxnType = CUSTOMER_POSITION;
				break;
			case 'G':
				TxnType = BROKER_VOLUME;
				break;
			case 'H':
				TxnType = SECURITY_DETAIL;
				break;
			case 'I':
				TxnType = MARKET_FEED;
				break;
			case 'J':
				TxnType = MARKET_WATCH;
				break;
			case 'K':
				TxnType = DATA_MAINTENANCE;
				break;
			case 'L':
				TxnType = TRADE_CLEANUP;
				break;
			default:
				return(false);
			}
			break;
		default:
			return(false);
		}
	}
	return(true);
}


// Trade Status
void TradeStatus(CDBConnection* pConn, CCETxnInputGenerator* pTxnInputGenerator)
{
	// trade status harness code (TPC provided)
	// this class uses our implementation of CTradeStatusDB class
	CTradeStatus		m_TradeStatus(pConn);

	// trade status input/output parameters
	TTradeStatusTxnInput	m_TradeStatusTxnInput;
	TTradeStatusTxnOutput	m_TradeStatusTxnOutput;
	
	// using TPC-provided input generator class
	pTxnInputGenerator->GenerateTradeStatusInput( m_TradeStatusTxnInput );
	cout<<m_TradeStatusTxnInput.acct_id<<endl;

	m_TradeStatus.DoTxn(&m_TradeStatusTxnInput, &m_TradeStatusTxnOutput);	// Perform Trade Status
	
	// Print output
	for (unsigned int i = 0; i < (sizeof(m_TradeStatusTxnOutput.trade_id)/sizeof(m_TradeStatusTxnOutput.trade_id[0])); ++i)
	{
		if ( m_TradeStatusTxnOutput.trade_id[i] == 0 ) break;
		cout<<"status_name["<<i<<"]="<<m_TradeStatusTxnOutput.status_name[i];
		cout<<"trade_id["<<i<<"]="<<m_TradeStatusTxnOutput.trade_id[i]<<endl;
	}

}


// main
int main(int argc, char* argv[])
{
	const char *server = "localhost";
	const char *db = "dbt5";
	const char *port = "5432";

	if (!ParseCommandLine(argc, argv))
	{
		Usage();
		return(-1);
	}

	try
	{
		// database connection
		CDBConnection		m_Conn( server, db, port );
	
		// initialize Input Generator
		//
		CLogFormatTab fmt;
		CEGenLogger log(eDriverEGenLoader, 0, "TxnTest.log", &fmt);
	
		char*	szInDir = "EGen_v3.14/flat_in";
	
		CInputFiles	inputFiles;
		inputFiles.Initialize(eDriverEGenLoader, iDefaultLoadUnitSize, iDefaultLoadUnitSize, szInDir);
	
		TDriverCETxnSettings	m_DriverCETxnSettings;
	
		CCETxnInputGenerator	m_TxnInputGenerator(inputFiles, iDefaultLoadUnitSize, iDefaultLoadUnitSize, 
								500, 10*HoursPerWorkDay, &log, &m_DriverCETxnSettings);
	
		RNGSEED	Seed = 32756;
		m_TxnInputGenerator.SetRNGSeed( Seed );
	
		//  Parse Txn type
		switch ( TxnType ) 
		{
			case TRADE_ORDER:
				cout<<"=== Testing Trade Order ==="<<endl<<endl;
				break;
			case TRADE_RESULT:
				cout<<"=== Testing Trade Result ==="<<endl<<endl;
				break;
			case TRADE_LOOKUP:
				cout<<"=== Testing Trade Lookup ==="<<endl<<endl;
				break;
			case TRADE_UPDATE:
				cout<<"=== Testing Trade Update ==="<<endl<<endl;
				break;
			case TRADE_STATUS:
				cout<<"=== Testing Trade Status ==="<<endl<<endl;
				TradeStatus( &m_Conn, &m_TxnInputGenerator );
				break;
			case CUSTOMER_POSITION:
				cout<<"=== Testing Customer Position ==="<<endl<<endl;
				break;
			case BROKER_VOLUME:
				cout<<"=== Testing Broker Volume ==="<<endl<<endl;
				break;
			case SECURITY_DETAIL:
				cout<<"=== Testing Security Detail ==="<<endl<<endl;
				break;
			case MARKET_FEED:
				cout<<"=== Testing Market Feed ==="<<endl<<endl;
				break;
			case MARKET_WATCH:
				cout<<"=== Testing Market Watch ==="<<endl<<endl;
				break;
			case DATA_MAINTENANCE:
				cout<<"=== Testing Data Maintenance ==="<<endl<<endl;
				break;
			case TRADE_CLEANUP:
				cout<<"=== Testing Trade Cleanup ==="<<endl<<endl;
				break;
			default:
				cout<<"wrong txn type"<<endl;
				return(-1);
		}
	}
	catch (CBaseErr *pErr)
	{
		cout<<endl<<"Error "<<pErr->ErrorNum()<<": "<<pErr->ErrorText();
		if (pErr->ErrorLoc()) {
			cout<<" at "<<pErr->ErrorLoc()<<endl;
		} else {
			cout<<endl;
		}
		return(1);
	}
	catch (exception &e)
	{
		cout<<e.what()<<endl;
		return(1);
	}

	return(0);
}
