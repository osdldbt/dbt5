/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 */

#include <cstdlib>
using namespace std;

#include "CETxnInputGenerator.h"
#include "TxnHarnessSendToMarketTest.h"
#include "DMSUTtest.h"
#include "CESUT.h"
#include "locking.h"

// BrokerageHouseMain variables;
CCESUT *m_pCCESUT = NULL;
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
RNGSEED Seed = 0;

// shows program usage
void
Usage()
{
	cout << "\nUsage: TestTxn [option]" << endl << endl;
	cout << "  where" << endl;
	cout << "   Option               Description" << endl;
	cout << "   =========            ========================" << endl;
	cout << "   -b address           Address of Brokerage House." << endl;
	cout << "                        Connect to database directly if not used."
		 << endl;
	cout << "   -c number            Customer count (default 5000)" << endl;
	cout << "   -f number            Number of customers for 1 TRTPS (default "
			"500)"
		 << endl;
	cout << "   -h host              Hostname of database server" << endl;
	cout << "   -i path              full path to EGen flat_in directory"
		 << endl;
	cout << "   -g dbname            Database name" << endl;
	cout << "                        Optional if testing BrokerageHouseMain"
		 << endl;
	cout << "   -p number            database listener port" << endl;
	cout << "   -r number            seed random number generator" << endl;
	cout << "   -t letter            Transaction type" << endl;
	cout << "                        A - TRADE_ORDER" << endl;
	cout << "                            TRADE_RESULT" << endl;
	cout << "                            MARKET_FEED" << endl;
	cout << "                        C - TRADE_LOOKUP" << endl;
	cout << "                        D - TRADE_UPDATE" << endl;
	cout << "                        E - TRADE_STATUS" << endl;
	cout << "                        F - CUSTOMER_POSITION" << endl;
	cout << "                        G - BROKER_VOLUME" << endl;
	cout << "                        H - SECURITY_DETAIL" << endl;
	cout << "                        J - MARKET_WATCH" << endl;
	cout << "                        K - DATA_MAINTENANCE" << endl;
	cout << "                        L - TRADE_CLEANUP" << endl;
	cout << "   -w number            Days of initial trades (default 300)"
		 << endl;
	cout << endl;
	cout << "Note: Trade Order triggers Trade Result and Market Feed" << endl;
	cout << "      when the type of trade is Market (type_is_market=1)"
		 << endl;
}

// parse command line
bool
ParseCommandLine(int argc, char *argv[])
{
	int arg;
	char *sp;
	char *vp;

	if (argc < 2) {
		// use default
		return (true);
	}

	/*
	 *  Scan the command line arguments
	 */
	for (arg = 1; arg < argc; ++arg) {
		/*
		 *  Look for a switch
		 */
		sp = argv[arg];
		if (*sp == '-') {
			++sp;
		}
		*sp = (char) tolower(*sp);

		/*
		 *  Find the switch's argument.  It is either immediately after the
		 *  switch or in the next argv
		 */
		vp = sp + 1;
		// Allow for switched that don't have any parameters.
		// Need to check that the next argument is in fact a parameter
		// and not the next switch that starts with '-'.
		//
		if ((*vp == 0) && ((arg + 1) < argc) && (argv[arg + 1][0] != '-')) {
			vp = argv[++arg];
		}

		/*
		 *  Parse the switch
		 */
		switch (*sp) {
		case 'b':
			strncpy(szBHaddr, vp, iMaxHostname);
			szBHaddr[iMaxHostname] = '\0';
			cout << "Will connect to BrokerageHouseMain at '" << szBHaddr
				 << "'." << endl;
			break;
		case 'c':
			sscanf(vp, "%" PRId64, &iConfiguredCustomerCount);
			break;
		case 'f':
			iScaleFactor = atoi(vp);
			break;
		case 'h':
			strncpy(szDBHost, vp, iDBHostLen);
			szDBHost[iDBHostLen] = '\0';
			break;
		case 'i':
			strncpy(szInDir, vp, iInDirLen);
			szInDir[iInDirLen] = '\0';
			break;
		case 'g':
			strncpy(szDBName, vp, iDBNameLen);
			szDBName[iDBNameLen] = '\0';
			break;
		case 'p':
			strncpy(szPort, vp, iPortLen);
			szPort[iPortLen] = '\0';
			break;
		case 't':
			switch (*vp) {
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
				return (false);
			}
			break;
		case 'r':
			Seed = atoi(vp);
			break;
		case 'w':
			iDaysOfInitialTrades = atoi(vp);
			break;
		default:
			return (false);
		}
	}
	return (true);
}

// Trade Order
INT32
TradeOrder(CDBConnection *pConn, CCETxnInputGenerator *pTxnInputGenerator)
{
	// SendToMarket test class that can call Trade-Result and Market-Feed
	// via the MEE - Market Exchange Emulator when type_is_market = 1.
	// These two txns run async.
	CSendToMarketTest m_pSendToMarket(
			iConfiguredCustomerCount, iConfiguredCustomerCount, szInDir);

	// trade order harness code (TPC provided)
	// this class uses our implementation of CTradeOrderDB class
	CTradeOrderDB m_TradeOrderDB(pConn, true);
	CTradeOrder m_TradeOrder(&m_TradeOrderDB, &m_pSendToMarket);

	// trade order input/output parameters
	TTradeOrderTxnInput m_TradeOrderTxnInput;
	memset(&m_TradeOrderTxnInput, 0, sizeof(TTradeOrderTxnInput));
	TTradeOrderTxnOutput m_TradeOrderTxnOutput;
	memset(&m_TradeOrderTxnOutput, 0, sizeof(TTradeOrderTxnOutput));

	// using TPC-provided input generator class
	bool bExecutorIsAccountOwner;
	INT32 iTradeType;
	pTxnInputGenerator->GenerateTradeOrderInput(
			m_TradeOrderTxnInput, iTradeType, bExecutorIsAccountOwner);

	// Perform Trade Order
	if (m_pCCESUT != NULL) {
		m_pCCESUT->TradeOrder(
				&m_TradeOrderTxnInput, iTradeType, bExecutorIsAccountOwner);
	} else {
		m_TradeOrder.DoTxn(&m_TradeOrderTxnInput, &m_TradeOrderTxnOutput);
	}

	return m_TradeOrderTxnOutput.status;
}

// Trade Status
INT32
TradeStatus(CDBConnection *pConn, CCETxnInputGenerator *pTxnInputGenerator)
{
	// trade status harness code (TPC provided)
	// this class uses our implementation of CTradeStatusDB class
	CTradeStatusDB m_TradeStatusDB(pConn, true);
	CTradeStatus m_TradeStatus(&m_TradeStatusDB);

	// trade status input/output parameters
	TTradeStatusTxnInput m_TradeStatusTxnInput;
	memset(&m_TradeStatusTxnInput, 0, sizeof(TTradeStatusTxnInput));
	TTradeStatusTxnOutput m_TradeStatusTxnOutput;
	memset(&m_TradeStatusTxnOutput, 0, sizeof(TTradeStatusTxnOutput));

	// using TPC-provided input generator class
	pTxnInputGenerator->GenerateTradeStatusInput(m_TradeStatusTxnInput);

	// Perform Trade Status
	if (m_pCCESUT != NULL) {
		m_pCCESUT->TradeStatus(&m_TradeStatusTxnInput);
	} else {
		m_TradeStatus.DoTxn(&m_TradeStatusTxnInput, &m_TradeStatusTxnOutput);
	}

	return m_TradeStatusTxnOutput.status;
}

// Trade Lookup
INT32
TradeLookup(CDBConnection *pConn, CCETxnInputGenerator *pTxnInputGenerator)
{
	// trade lookup harness code (TPC provided)
	// this class uses our implementation of CTradeLookupDB class
	CTradeLookupDB m_TradeLookupDB(pConn, true);
	CTradeLookup m_TradeLookup(&m_TradeLookupDB);

	// trade lookup input/output parameters
	TTradeLookupTxnInput m_TradeLookupTxnInput;
	memset(&m_TradeLookupTxnInput, 0, sizeof(TTradeLookupTxnInput));
	TTradeLookupTxnOutput m_TradeLookupTxnOutput;
	memset(&m_TradeLookupTxnOutput, 0, sizeof(TTradeLookupTxnOutput));

	// using TPC-provided input generator class
	pTxnInputGenerator->GenerateTradeLookupInput(m_TradeLookupTxnInput);

	// Perform Trade Lookup
	if (m_pCCESUT != NULL) {
		m_pCCESUT->TradeLookup(&m_TradeLookupTxnInput);
	} else {
		m_TradeLookup.DoTxn(&m_TradeLookupTxnInput, &m_TradeLookupTxnOutput);
	}

	return m_TradeLookupTxnOutput.status;
}

// Trade Update
INT32
TradeUpdate(CDBConnection *pConn, CCETxnInputGenerator *pTxnInputGenerator)
{
	// trade update harness code (TPC provided)
	// this class uses our implementation of CTradeUpdateDB class
	CTradeUpdateDB m_TradeUpdateDB(pConn, true);
	CTradeUpdate m_TradeUpdate(&m_TradeUpdateDB);

	// trade update input/output parameters
	TTradeUpdateTxnInput m_TradeUpdateTxnInput;
	memset(&m_TradeUpdateTxnInput, 0, sizeof(TTradeUpdateTxnInput));
	TTradeUpdateTxnOutput m_TradeUpdateTxnOutput;
	memset(&m_TradeUpdateTxnOutput, 0, sizeof(TTradeUpdateTxnOutput));

	// using TPC-provided input generator class
	pTxnInputGenerator->GenerateTradeUpdateInput(m_TradeUpdateTxnInput);

	// Perform Trade Update
	if (m_pCCESUT != NULL) {
		m_pCCESUT->TradeUpdate(&m_TradeUpdateTxnInput);
	} else {
		m_TradeUpdate.DoTxn(&m_TradeUpdateTxnInput, &m_TradeUpdateTxnOutput);
	}

	return m_TradeUpdateTxnOutput.status;
}

// Customer Position
INT32
CustomerPosition(
		CDBConnection *pConn, CCETxnInputGenerator *pTxnInputGenerator)
{
	// customer position harness code (TPC provided)
	// this class uses our implementation of CCustomerPositionDB class
	CCustomerPositionDB m_CustomerPositionDB(pConn, true);
	CCustomerPosition m_CustomerPosition(&m_CustomerPositionDB);

	// customer position input/output parameters
	TCustomerPositionTxnInput m_CustomerPositionTxnInput;
	memset(&m_CustomerPositionTxnInput, 0, sizeof(TCustomerPositionTxnInput));
	TCustomerPositionTxnOutput m_CustomerPositionTxnOutput;
	memset(&m_CustomerPositionTxnOutput, 0,
			sizeof(TCustomerPositionTxnOutput));

	// using TPC-provided input generator class
	pTxnInputGenerator->GenerateCustomerPositionInput(
			m_CustomerPositionTxnInput);

	// Perform Customer Position
	if (m_pCCESUT != NULL) {
		m_pCCESUT->CustomerPosition(&m_CustomerPositionTxnInput);
	} else {
		m_CustomerPosition.DoTxn(
				&m_CustomerPositionTxnInput, &m_CustomerPositionTxnOutput);
	}

	return m_CustomerPositionTxnOutput.status;
}

// Broker Volume
INT32
BrokerVolume(CDBConnection *pConn, CCETxnInputGenerator *pTxnInputGenerator)
{
	// Broker Volume harness code (TPC provided)
	// this class uses our implementation of CBrokerVolumeDB class
	CBrokerVolumeDB m_BrokerVolumeDB(pConn, true);
	CBrokerVolume m_BrokerVolume(&m_BrokerVolumeDB);

	// broker volume input/output parameters
	TBrokerVolumeTxnInput m_BrokerVolumeTxnInput;
	memset(&m_BrokerVolumeTxnInput, 0, sizeof(TBrokerVolumeTxnInput));
	TBrokerVolumeTxnOutput m_BrokerVolumeTxnOutput;
	memset(&m_BrokerVolumeTxnOutput, 0, sizeof(TBrokerVolumeTxnOutput));

	// using TPC-provided input generator class
	pTxnInputGenerator->GenerateBrokerVolumeInput(m_BrokerVolumeTxnInput);

	// Perform Broker Volume
	if (m_pCCESUT != NULL) {
		m_pCCESUT->BrokerVolume(&m_BrokerVolumeTxnInput);
	} else {
		m_BrokerVolume.DoTxn(
				&m_BrokerVolumeTxnInput, &m_BrokerVolumeTxnOutput);
	}

	return m_BrokerVolumeTxnOutput.status;
}

// Security Detail
INT32
SecurityDetail(CDBConnection *pConn, CCETxnInputGenerator *pTxnInputGenerator)
{
	// Security Detail harness code (TPC provided)
	// this class uses our implementation of CSecurityDetailDB class
	CSecurityDetailDB m_SecurityDetailDB(pConn, true);
	CSecurityDetail m_SecurityDetail(&m_SecurityDetailDB);

	// security detail input/output parameters
	TSecurityDetailTxnInput m_SecurityDetailTxnInput;
	memset(&m_SecurityDetailTxnInput, 0, sizeof(TSecurityDetailTxnInput));
	TSecurityDetailTxnOutput m_SecurityDetailTxnOutput;
	memset(&m_SecurityDetailTxnOutput, 0, sizeof(TSecurityDetailTxnOutput));

	// using TPC-provided input generator class
	pTxnInputGenerator->GenerateSecurityDetailInput(m_SecurityDetailTxnInput);

	// Perform Security Detail
	if (m_pCCESUT != NULL) {
		m_pCCESUT->SecurityDetail(&m_SecurityDetailTxnInput);
	} else {
		m_SecurityDetail.DoTxn(
				&m_SecurityDetailTxnInput, &m_SecurityDetailTxnOutput);
	}

	return m_SecurityDetailTxnOutput.status;
}

// Market Watch
INT32
MarketWatch(CDBConnection *pConn, CCETxnInputGenerator *pTxnInputGenerator)
{
	// Market Watch harness code (TPC provided)
	// this class uses our implementation of CMarketWatchDB class
	CMarketWatchDB m_MarketWatchDB(pConn, true);
	CMarketWatch m_MarketWatch(&m_MarketWatchDB);

	// Market Watch input/output parameters
	TMarketWatchTxnInput m_MarketWatchTxnInput;
	memset(&m_MarketWatchTxnInput, 0, sizeof(TMarketWatchTxnInput));
	TMarketWatchTxnOutput m_MarketWatchTxnOutput;
	memset(&m_MarketWatchTxnOutput, 0, sizeof(TMarketWatchTxnOutput));

	// using TPC-provided input generator class
	pTxnInputGenerator->GenerateMarketWatchInput(m_MarketWatchTxnInput);

	// Perform Market Watch
	if (m_pCCESUT != NULL) {
		m_pCCESUT->MarketWatch(&m_MarketWatchTxnInput);
	} else {
		m_MarketWatch.DoTxn(&m_MarketWatchTxnInput, &m_MarketWatchTxnOutput);
	}

	return m_MarketWatchTxnOutput.status;
}

// Data Maintenance
void
DataMaintenance(CDM *pCDM)
{
	// using TPC-provided Data Maintenance class to perform the Data
	// Maintenance transaction.  Testing all tables
	for (int i = 0; i <= 11; i++) {
		pCDM->DoTxn();
	}
}

// Trade Cleanup
void
TradeCleanup(CDM *pCDM)
{
	// using TPC-provided Data Maintenance class to perform the Trade Cleanup.
	pCDM->DoCleanupTxn();
}

// main
int
main(int argc, char *argv[])
{
	ofstream m_fLog;
	ofstream m_fMix;
	CMutex m_LogLock;
	CMutex m_MixLock;
	char outputDirectory[2] = ".";

	if (!ParseCommandLine(argc, argv)) {
		Usage();
		return (-1);
	}

	if (strlen(szInDir) == 0) {
		cout << "Use -i to specify full path to EGen flat_in directory."
			 << endl;
		exit(1);
	}

	if (TxnType == NULL_TXN) {
		cout << "Use -t to specify which transaction to test." << endl;
		exit(1);
	}

	if (strlen(szBHaddr) != 0) {
		m_fLog.open("test.log", ios::out);
		m_fMix.open("test-mix.log", ios::out);
		m_pCCESUT = new CCESUT(outputDirectory, szBHaddr, iBHlistenPort);
	}

	try {
		// database connection
		CDBConnection m_Conn(szDBHost, szDBName, szPort);

		// initialize Input Generator
		//
		CLogFormatTab fmt;
		CEGenLogger log(eDriverEGenLoader, 0, "TestTxn.log", &fmt);

		const DataFileManager inputFiles(szInDir, iConfiguredCustomerCount,
				iActiveCustomerCount, TPCE::DataFileManager::IMMEDIATE_LOAD);

		TDriverCETxnSettings m_DriverCETxnSettings;

		CCETxnInputGenerator m_TxnInputGenerator(inputFiles,
				iConfiguredCustomerCount, iActiveCustomerCount, iScaleFactor,
				iDaysOfInitialTrades * HoursPerWorkDay, &log,
				&m_DriverCETxnSettings);

		if (Seed == 0)
			Seed = m_TxnInputGenerator.GetRNGSeed();
		else
			m_TxnInputGenerator.SetRNGSeed(Seed);
		cout << "Seed: " << Seed << endl << endl;

		// Initialize DM - Data Maintenance class
		// DM is used by Data-Maintenance and Trade-Cleanup transactions
		// Data-Maintenance SUT interface (provided by us)
		CDMSUTtest m_CDMSUT(&m_Conn);
		CDM m_CDM(&m_CDMSUT, &log, inputFiles, iConfiguredCustomerCount,
				iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades, 1);

		CDateTime StartTime;

		//  Parse Txn type
		INT32 status = 0;
		switch (TxnType) {
		case TRADE_ORDER:
			cout << "=== Testing Trade Order, Trade Result and Market Feed ==="
				 << endl
				 << endl;
			status = TradeOrder(&m_Conn, &m_TxnInputGenerator);
			break;
		case TRADE_LOOKUP:
			cout << "=== Testing Trade Lookup ===" << endl << endl;
			status = TradeLookup(&m_Conn, &m_TxnInputGenerator);
			break;
		case TRADE_UPDATE:
			cout << "=== Testing Trade Update ===" << endl << endl;
			status = TradeUpdate(&m_Conn, &m_TxnInputGenerator);
			break;
		case TRADE_STATUS:
			cout << "=== Testing Trade Status ===" << endl << endl;
			status = TradeStatus(&m_Conn, &m_TxnInputGenerator);
			break;
		case CUSTOMER_POSITION:
			cout << "=== Testing Customer Position ===" << endl << endl;
			status = CustomerPosition(&m_Conn, &m_TxnInputGenerator);
			break;
		case BROKER_VOLUME:
			cout << "=== Testing Broker Volume ===" << endl << endl;
			status = BrokerVolume(&m_Conn, &m_TxnInputGenerator);
			break;
		case SECURITY_DETAIL:
			cout << "=== Testing Security Detail ===" << endl << endl;
			status = SecurityDetail(&m_Conn, &m_TxnInputGenerator);
			break;
		case MARKET_WATCH:
			cout << "=== Testing Market Watch ===" << endl << endl;
			status = MarketWatch(&m_Conn, &m_TxnInputGenerator);
			break;
		case DATA_MAINTENANCE:
			cout << "=== Testing Data Maintenance ===" << endl << endl;
			DataMaintenance(&m_CDM);
			break;
		case TRADE_CLEANUP:
			cout << "=== Testing Trade Cleanup ===" << endl << endl;
			TradeCleanup(&m_CDM);
			break;
		default:
			cout << "wrong txn type" << endl;
			return (-1);
		}

		// record txn end time
		CDateTime EndTime;

		// calculate txn response time
		CDateTime TxnTime;
		TxnTime.Set(0); // clear time
		TxnTime.Add(0, (int) ((EndTime - StartTime) * MsPerSecond)); // add ms

		cout << "Txn Status = " << status << endl;
		cout << "Txn Response Time = " << (TxnTime.MSec() / 1000.0) << endl;
	} catch (CBaseErr *pErr) {
		cout << "Error " << pErr->ErrorNum() << ": " << pErr->ErrorText();
		if (pErr->ErrorLoc()) {
			cout << " at " << pErr->ErrorLoc() << endl;
		} else {
			cout << endl;
		}
		return 1;
	} catch (const char *str) {
		cerr << "### FUCK THERE HAS BEEN A PROBLEM" << endl;
		return 3;
	}

	delete m_pCCESUT;

	return 0;
}
