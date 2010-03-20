/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2010 Rilson Nascimento
 */

#include <cstdlib>
using namespace std;

#include "CETxnInputGenerator.h"
#include "TxnHarnessSendToMarketTest.h"
#include "DMSUTtest.h"
#include "CESUT.h"
#include "locking.h"

// BrokerageHouseMain variables;
CCESUT	*m_pCCESUT = NULL;
char szBHaddr[iMaxHostname + 1] = "";
int iBHlistenPort = iBrokerageHousePort;

const int iPortLen = 7;
char szPort[iPortLen + 1] = "";

const int iDBHostLen = 63;
char szDBHost[iDBHostLen + 1] = "";

const int iDBNameLen = 31;
char szDBName[iDBNameLen + 1] = "";

const int iInDirLen = 256;
char szInDir[iInDirLen + 1] = "";

TIdent iConfiguredCustomerCount = iDefaultLoadUnitSize;
// FIXME: iActiveCustomerCount needs to be configurable.
TIdent iActiveCustomerCount = iConfiguredCustomerCount;
int iScaleFactor = 500;
int iDaysOfInitialTrades = 300;

eTxnType TxnType = NULL_TXN;
RNGSEED	Seed = 0;

// shows program usage
void Usage()
{
	cout << "\nUsage: TestTxn [option]" << endl << endl;
	cout << "  where" << endl;
	cout << "   Option               Description" << endl;
	cout << "   =========            ========================" << endl;
	cout << "   -b address           Address of BrokerageHouseMain" << endl;
	cout << "   -c number            Customer count (default 5000)" << endl;
	cout << "   -f number            Number of customers for 1 TRTPS (default 500)" << endl;
	cout << "   -h host              Hostname of database server" << endl;
	cout << "   -i path              full path to EGen flat_in directory" <<
			endl;
	cout << "   -g dbname            Database name" << endl;
	cout << "                        Optional if testing BrokerageHouseMain" <<
			endl;
	cout << "   -p number            database listener port" << endl;
	cout << "   -r number            optional random number" << endl;
	cout << "   -t letter            Transaction type" << endl;
	cout << "                        A - TRADE_ORDER" << endl;
	cout << "                            TRADE_RESULT" << endl;
	cout << "                            MARKET_FEED" << endl;
	cout << "                        C - TRADE_LOOKUP" << endl;
	cout << "                        D - TRADE_UPDATE" << endl;
	cout << "                        E - TRADE_STATUS" << endl;
	cout << "                        F - CUSTOMER_POSITION" << endl;
	cout << "                        G - BROKER_VOLUME" << endl;
	cout << "                        H - SECURITY_DETAIL"<<endl;
	cout << "                        J - MARKET_WATCH" << endl;
	cout << "                        K - DATA_MAINTENANCE" << endl;
	cout << "                        L - TRADE_CLEANUP" << endl;
	cout << "   -w number            Days of initial trades (default 300)" <<
			endl;
	cout << endl;
	cout << "Note: Trade Order triggers Trade Result and Market Feed" << endl;
	cout << "      when the type of trade is Market (type_is_market=1)" << endl;
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
		case 'b':
			strncpy(szBHaddr, vp, iMaxHostname);
			cout << "Will connect to BrokerageHouseMain at '" << szBHaddr <<
					"'." << endl;
			break;
		case 'c':
			sscanf(vp, "%"PRId64, &iConfiguredCustomerCount);
			break;
		case 'f':
			iScaleFactor = atoi(vp);
			break;
		case 'h':
			strncpy(szDBHost, vp, iDBHostLen);
			break;
		case 'i':
			strncpy(szInDir, vp, iInDirLen);
			break;
		case 'g':
			strncpy(szDBName, vp, iDBNameLen);
			break;
		case 'p':
			strncpy(szPort, vp, iPortLen);
			break;
		case 't':
			switch ( *vp) {
			case 'A':
				TxnType = TRADE_ORDER;
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
		case 'r':
			Seed = atoi(vp);
			break;
		case 'w':
			iDaysOfInitialTrades = atoi(vp);
			break;
		default:
			return(false);
		}
	}
	return(true);
}

// Trade Order
void TradeOrder(CDBConnection* pConn, CCETxnInputGenerator* pTxnInputGenerator)
{
	// SendToMarket test class that can call Trade-Result and Market-Feed 
	// via the MEE - Market Exchange Emulator when type_is_market = 1. 
	// These two txns run async.
	CSendToMarketTest m_pSendToMarket(iConfiguredCustomerCount,
			iConfiguredCustomerCount, szInDir);

	// trade order harness code (TPC provided)
	// this class uses our implementation of CTradeOrderDB class
	CTradeOrderDB m_TradeOrderDB(pConn);
	CTradeOrder m_TradeOrder(&m_TradeOrderDB, &m_pSendToMarket);

	// trade order input/output parameters
	TTradeOrderTxnInput	m_TradeOrderTxnInput;
	TTradeOrderTxnOutput	m_TradeOrderTxnOutput;
	
	// using TPC-provided input generator class
	bool	bExecutorIsAccountOwner;
	INT32	iTradeType;
	pTxnInputGenerator->GenerateTradeOrderInput( m_TradeOrderTxnInput,
			iTradeType, bExecutorIsAccountOwner );

	// Perform Trade Order
	if (m_pCCESUT != NULL) {
		m_pCCESUT->TradeOrder(&m_TradeOrderTxnInput, iTradeType,
				bExecutorIsAccountOwner);
	} else {
		m_TradeOrder.DoTxn(&m_TradeOrderTxnInput, &m_TradeOrderTxnOutput);
	}
}


// Trade Status
void TradeStatus(CDBConnection* pConn, CCETxnInputGenerator* pTxnInputGenerator)
{
	// trade status harness code (TPC provided)
	// this class uses our implementation of CTradeStatusDB class
	CTradeStatusDB m_TradeStatusDB(pConn);
	CTradeStatus m_TradeStatus(&m_TradeStatusDB);

	// trade status input/output parameters
	TTradeStatusTxnInput	m_TradeStatusTxnInput;
	TTradeStatusTxnOutput	m_TradeStatusTxnOutput;
	
	// using TPC-provided input generator class
	pTxnInputGenerator->GenerateTradeStatusInput( m_TradeStatusTxnInput );

	// Perform Trade Status
	if (m_pCCESUT != NULL) {
		m_pCCESUT->TradeStatus(&m_TradeStatusTxnInput);
	} else {
		m_TradeStatus.DoTxn(&m_TradeStatusTxnInput, &m_TradeStatusTxnOutput);
	}
}


// Trade Lookup
void TradeLookup(CDBConnection* pConn, CCETxnInputGenerator* pTxnInputGenerator)
{
	// trade lookup harness code (TPC provided)
	// this class uses our implementation of CTradeLookupDB class
	CTradeLookupDB m_TradeLookupDB(pConn);
	CTradeLookup m_TradeLookup(&m_TradeLookupDB);

	// trade lookup input/output parameters
	TTradeLookupTxnInput	m_TradeLookupTxnInput;
	TTradeLookupTxnOutput	m_TradeLookupTxnOutput;
	
	// using TPC-provided input generator class
	pTxnInputGenerator->GenerateTradeLookupInput( m_TradeLookupTxnInput );

	// Perform Trade Lookup
	if (m_pCCESUT != NULL) {
		m_pCCESUT->TradeLookup(&m_TradeLookupTxnInput);
	} else {
		m_TradeLookup.DoTxn(&m_TradeLookupTxnInput, &m_TradeLookupTxnOutput);
	}
}


// Trade Update
void TradeUpdate(CDBConnection* pConn, CCETxnInputGenerator* pTxnInputGenerator)
{
	// trade update harness code (TPC provided)
	// this class uses our implementation of CTradeUpdateDB class
	CTradeUpdateDB m_TradeUpdateDB(pConn);
	CTradeUpdate m_TradeUpdate(&m_TradeUpdateDB);

	// trade update input/output parameters
	TTradeUpdateTxnInput	m_TradeUpdateTxnInput;
	TTradeUpdateTxnOutput	m_TradeUpdateTxnOutput;
	
	// using TPC-provided input generator class
	pTxnInputGenerator->GenerateTradeUpdateInput( m_TradeUpdateTxnInput );

	// Perform Trade Update
	if (m_pCCESUT != NULL) {
		m_pCCESUT->TradeUpdate(&m_TradeUpdateTxnInput);
	} else {
		m_TradeUpdate.DoTxn(&m_TradeUpdateTxnInput, &m_TradeUpdateTxnOutput);
	}
}


// Customer Position
void CustomerPosition(CDBConnection* pConn,
		CCETxnInputGenerator* pTxnInputGenerator)
{
	// customer position harness code (TPC provided)
	// this class uses our implementation of CCustomerPositionDB class
	CCustomerPositionDB m_CustomerPositionDB(pConn);
	CCustomerPosition m_CustomerPosition(&m_CustomerPositionDB);

	// customer position input/output parameters
	TCustomerPositionTxnInput	m_CustomerPositionTxnInput;
	TCustomerPositionTxnOutput	m_CustomerPositionTxnOutput;
	
	// using TPC-provided input generator class
	pTxnInputGenerator->GenerateCustomerPositionInput(
			m_CustomerPositionTxnInput);

	// Perform Customer Position
	if (m_pCCESUT != NULL) {
		m_pCCESUT->CustomerPosition(&m_CustomerPositionTxnInput);
	} else {
		m_CustomerPosition.DoTxn(&m_CustomerPositionTxnInput,
				&m_CustomerPositionTxnOutput);
	}
}


// Broker Volume
void BrokerVolume(CDBConnection* pConn,
		CCETxnInputGenerator* pTxnInputGenerator)
{
	// Broker Volume harness code (TPC provided)
	// this class uses our implementation of CBrokerVolumeDB class
	CBrokerVolumeDB m_BrokerVolumeDB(pConn);
	CBrokerVolume m_BrokerVolume(&m_BrokerVolumeDB);

	// broker volume input/output parameters
	TBrokerVolumeTxnInput	m_BrokerVolumeTxnInput;
	TBrokerVolumeTxnOutput	m_BrokerVolumeTxnOutput;
	
	// using TPC-provided input generator class
	pTxnInputGenerator->GenerateBrokerVolumeInput( m_BrokerVolumeTxnInput );

	// Perform Broker Volume
	if (m_pCCESUT != NULL) {
		m_pCCESUT->BrokerVolume(&m_BrokerVolumeTxnInput);
	} else {
		m_BrokerVolume.DoTxn(&m_BrokerVolumeTxnInput, &m_BrokerVolumeTxnOutput);
	}
}


// Security Detail
void SecurityDetail(CDBConnection* pConn,
		CCETxnInputGenerator* pTxnInputGenerator)
{
	// Security Detail harness code (TPC provided)
	// this class uses our implementation of CSecurityDetailDB class
	CSecurityDetailDB m_SecurityDetailDB(pConn);
	CSecurityDetail m_SecurityDetail(&m_SecurityDetailDB);

	// security detail input/output parameters
	TSecurityDetailTxnInput	m_SecurityDetailTxnInput;
	TSecurityDetailTxnOutput	m_SecurityDetailTxnOutput;
	
	// using TPC-provided input generator class
	pTxnInputGenerator->GenerateSecurityDetailInput( m_SecurityDetailTxnInput );

	// Perform Security Detail
	if (m_pCCESUT != NULL) {
		m_pCCESUT->SecurityDetail(&m_SecurityDetailTxnInput);
	} else {
		m_SecurityDetail.DoTxn(&m_SecurityDetailTxnInput,
				&m_SecurityDetailTxnOutput);
	}
}


// Market Watch
void MarketWatch(CDBConnection* pConn, CCETxnInputGenerator* pTxnInputGenerator)
{
	// Market Watch harness code (TPC provided)
	// this class uses our implementation of CMarketWatchDB class
	CMarketWatchDB m_MarketWatchDB(pConn);
	CMarketWatch m_MarketWatch(&m_MarketWatchDB);

	// Market Watch input/output parameters
	TMarketWatchTxnInput	m_MarketWatchTxnInput;
	TMarketWatchTxnOutput	m_MarketWatchTxnOutput;
	
	// using TPC-provided input generator class
	pTxnInputGenerator->GenerateMarketWatchInput( m_MarketWatchTxnInput );

	// Perform Market Watch
	if (m_pCCESUT != NULL) {
		m_pCCESUT->MarketWatch(&m_MarketWatchTxnInput);
	} else {
		m_MarketWatch.DoTxn(&m_MarketWatchTxnInput, &m_MarketWatchTxnOutput);
	}
}


// Data Maintenance
void DataMaintenance(CDM* pCDM)
{
	// using TPC-provided Data Maintenance class to perform the Data
	// Maintenance transaction.  Testing all tables
	for (int i = 0; i <= 11; i++)
	{
		pCDM->DoTxn();
	}
}


// Trade Cleanup
void TradeCleanup(CDM* pCDM)
{
	// using TPC-provided Data Maintenance class to perform the Trade Cleanup.
	pCDM->DoCleanupTxn();
}

// Auto Random number generator
unsigned int AutoRng()
{
	struct timeval tv;
	struct tm ltr;
	gettimeofday(&tv, NULL);
	struct tm* lt = localtime_r(&tv.tv_sec, &ltr);
	return (((lt->tm_hour * MinutesPerHour + lt->tm_min) * SecondsPerMinute +
			lt->tm_sec) * MsPerSecond + tv.tv_usec / 1000);
}

// main
int main(int argc, char* argv[])
{
	ofstream m_fLog;
	ofstream m_fMix;
	CMutex m_LogLock;
	CMutex m_MixLock;

	if (!ParseCommandLine(argc, argv))
	{
		Usage();
		return(-1);
	}

	if (strlen(szInDir) == 0) {
		cout << "Use -s to specify full path to EGen flat_in directory." <<
				endl;
		exit(1);
	}

	if (TxnType == NULL_TXN) {
		cout << "Use -t to specify which transaction to test." << endl;
		exit(1);
	}

	if (strlen(szBHaddr) != 0) {
		m_fLog.open("test.log", ios::out);
		m_fMix.open("test-mix.log", ios::out);
		m_pCCESUT = new CCESUT(szBHaddr, iBHlistenPort, &m_fLog, &m_fMix,
				&m_LogLock, &m_MixLock);
	}

	try
	{
		// database connection
		CDBConnection m_Conn(szDBHost, szDBName, szPort);
	
		// initialize Input Generator
		//
		CLogFormatTab fmt;
		CEGenLogger log(eDriverEGenLoader, 0, "TxnTest.log", &fmt);
	
		CInputFiles	inputFiles;
		inputFiles.Initialize(eDriverEGenLoader, iConfiguredCustomerCount,
				iConfiguredCustomerCount, szInDir);
	
		TDriverCETxnSettings	m_DriverCETxnSettings;
	
		CCETxnInputGenerator m_TxnInputGenerator(inputFiles,
				iConfiguredCustomerCount, iActiveCustomerCount,
				iScaleFactor, iDaysOfInitialTrades * HoursPerWorkDay, &log,
				&m_DriverCETxnSettings);
	
		if (Seed == 0) 
		{
			srand(AutoRng());
			Seed = rand();
		}
		cout<<"Seed: "<<Seed<<endl<<endl;
		m_TxnInputGenerator.SetRNGSeed( Seed );

		// Initialize DM - Data Maintenance class
		// DM is used by Data-Maintenance and Trade-Cleanup transactions
		// Data-Maintenance SUT interface (provided by us)
		CDMSUTtest	m_CDMSUT( &m_Conn );
		CDM	m_CDM( &m_CDMSUT, &log, inputFiles, iConfiguredCustomerCount,
				iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades,
				1 );

		CDateTime	StartTime, EndTime, TxnTime;	// to time the transaction
		StartTime.SetToCurrent();

		//  Parse Txn type
		switch ( TxnType ) 
		{
		case TRADE_ORDER:
			cout << "=== Testing Trade Order, Trade Result and Market Feed ==="
					<< endl << endl;
			TradeOrder( &m_Conn, &m_TxnInputGenerator );
			break;
		case TRADE_LOOKUP:
			cout<<"=== Testing Trade Lookup ==="<<endl<<endl;
			TradeLookup( &m_Conn, &m_TxnInputGenerator );
			break;
		case TRADE_UPDATE:
			cout<<"=== Testing Trade Update ==="<<endl<<endl;
			TradeUpdate( &m_Conn, &m_TxnInputGenerator );
			break;
		case TRADE_STATUS:
			cout<<"=== Testing Trade Status ==="<<endl<<endl;
			TradeStatus( &m_Conn, &m_TxnInputGenerator );
			break;
		case CUSTOMER_POSITION:
			cout<<"=== Testing Customer Position ==="<<endl<<endl;
			CustomerPosition( &m_Conn, &m_TxnInputGenerator );
			break;
		case BROKER_VOLUME:
			cout<<"=== Testing Broker Volume ==="<<endl<<endl;
			BrokerVolume( &m_Conn, &m_TxnInputGenerator );
			break;
		case SECURITY_DETAIL:
			cout<<"=== Testing Security Detail ==="<<endl<<endl;
			SecurityDetail( &m_Conn, &m_TxnInputGenerator );
			break;
		case MARKET_WATCH:
			cout<<"=== Testing Market Watch ==="<<endl<<endl;
			MarketWatch( &m_Conn, &m_TxnInputGenerator );
			break;
		case DATA_MAINTENANCE:
			cout<<"=== Testing Data Maintenance ==="<<endl<<endl;
			DataMaintenance( &m_CDM );
			break;
		case TRADE_CLEANUP:
			cout<<"=== Testing Trade Cleanup ==="<<endl<<endl;
			TradeCleanup( &m_CDM );
			break;
		default:
			cout<<"wrong txn type"<<endl;
			return(-1);
		}

		// record txn end time
		EndTime.SetToCurrent();

		// calculate txn response time
		TxnTime.Set(0);	// clear time
		TxnTime.Add(0, (int)((EndTime - StartTime) * MsPerSecond));	// add ms

 		cout<<"Txn Response Time = "<<(TxnTime.MSec()/1000.0)<<endl;
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
	// exceptions thrown by pqxx
	//
	catch (const pqxx::broken_connection &e) // broken connection
	{
		cout<<"libpxx: "<<e.what()<<endl;
		return 3;
	}
	catch (const pqxx::sql_error &e)
	{
		cout<<"SQL error: "<<e.what()<<endl
		    <<"Query was: '"<<e.query()<<"'"<<endl;
		return 3;
	}
	catch (const exception &e)
	{
		cout<<e.what()<<endl;
		return 3;
	}

	return(0);
}
