/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 25 July 2006
 */

#include "BrokerageHouse.h"
#include "CommonStructs.h"
#include "DBConnection.h"

#include "BrokerVolumeDB.h"
#include "CustomerPositionDB.h"
#include "DataMaintenanceDB.h"
#include "MarketFeedDB.h"
#include "MarketWatchDB.h"
#include "SecurityDetailDB.h"
#include "TradeCleanupDB.h"
#include "TradeLookupDB.h"
#include "TradeOrderDB.h"
#include "TradeResultDB.h"
#include "TradeStatusDB.h"
#include "TradeUpdateDB.h"

void *
workerThread(void *data)
{
	try {
		PThreadParameter pThrParam = reinterpret_cast<PThreadParameter>(data);

		CSocket sockDrv;
		sockDrv.setSocketFd(pThrParam->iSockfd); // client socket

		PMsgDriverBrokerage pMessage = new TMsgDriverBrokerage;
		memset(pMessage, 0, sizeof(TMsgDriverBrokerage)); // zero the structure

		TMsgBrokerageDriver Reply; // return message
		INT32 iRet = 0; // transaction return code
		CDBConnection *pDBConnection = NULL;

		// new database connection
		pDBConnection = new CDBConnection(pThrParam->pBrokerageHouse->m_szHost,
				pThrParam->pBrokerageHouse->m_szDBName,
				pThrParam->pBrokerageHouse->m_szDBPort);
		pDBConnection->setBrokerageHouse(pThrParam->pBrokerageHouse);
		CSendToMarket sendToMarket
				= CSendToMarket(&(pThrParam->pBrokerageHouse->m_fLog),
						pThrParam->m_szMEEHost, atoi(pThrParam->m_szMEEPort));
		CMarketFeedDB marketFeedDB(
				pDBConnection, pThrParam->pBrokerageHouse->verbose());
		CMarketFeed marketFeed = CMarketFeed(&marketFeedDB, &sendToMarket);
		CTradeOrderDB tradeOrderDB(
				pDBConnection, pThrParam->pBrokerageHouse->verbose());
		CTradeOrder tradeOrder = CTradeOrder(&tradeOrderDB, &sendToMarket);

		// Initialize all classes that will be used to execute transactions.
		CBrokerVolumeDB brokerVolumeDB(
				pDBConnection, pThrParam->pBrokerageHouse->verbose());
		CBrokerVolume brokerVolume = CBrokerVolume(&brokerVolumeDB);
		CCustomerPositionDB customerPositionDB(
				pDBConnection, pThrParam->pBrokerageHouse->verbose());
		CCustomerPosition customerPosition
				= CCustomerPosition(&customerPositionDB);
		CMarketWatchDB marketWatchDB(
				pDBConnection, pThrParam->pBrokerageHouse->verbose());
		CMarketWatch marketWatch = CMarketWatch(&marketWatchDB);
		CSecurityDetailDB securityDetailDB = CSecurityDetailDB(
				pDBConnection, pThrParam->pBrokerageHouse->verbose());
		CSecurityDetail securityDetail = CSecurityDetail(&securityDetailDB);
		CTradeLookupDB tradeLookupDB(
				pDBConnection, pThrParam->pBrokerageHouse->verbose());
		CTradeLookup tradeLookup = CTradeLookup(&tradeLookupDB);
		CTradeStatusDB tradeStatusDB(
				pDBConnection, pThrParam->pBrokerageHouse->verbose());
		CTradeStatus tradeStatus = CTradeStatus(&tradeStatusDB);
		CTradeUpdateDB tradeUpdateDB(
				pDBConnection, pThrParam->pBrokerageHouse->verbose());
		CTradeUpdate tradeUpdate = CTradeUpdate(&tradeUpdateDB);
		CDataMaintenanceDB dataMaintenanceDB(
				pDBConnection, pThrParam->pBrokerageHouse->verbose());
		CDataMaintenance dataMaintenance
				= CDataMaintenance(&dataMaintenanceDB);
		CTradeCleanupDB tradeCleanupDB(
				pDBConnection, pThrParam->pBrokerageHouse->verbose());
		CTradeCleanup tradeCleanup = CTradeCleanup(&tradeCleanupDB);
		CTradeResultDB tradeResultDB(
				pDBConnection, pThrParam->pBrokerageHouse->verbose());
		CTradeResult tradeResult = CTradeResult(&tradeResultDB);

		do {
			try {
				sockDrv.dbt5Receive(reinterpret_cast<void *>(pMessage),
						sizeof(TMsgDriverBrokerage));
			} catch (std::runtime_error &err) {
				sockDrv.dbt5Disconnect();

				ostringstream osErr;
				osErr << "Error on Receive: " << err.what()
					  << " at BrokerageHouse::workerThread" << endl;
				pThrParam->pBrokerageHouse->logErrorMessage(osErr.str());

				// The socket has been closed, break and let this thread die.
				break;
			} catch (CSocketErr *pErr) {
				sockDrv.dbt5Disconnect();

				if (pErr->getAction() == CSocketErr::ERR_SOCKET_CLOSED) {
					delete pErr;
					break;
				}

				ostringstream osErr;
				osErr << "Error on Receive: " << pErr->ErrorText()
					  << " at BrokerageHouse::workerThread" << endl;
				pThrParam->pBrokerageHouse->logErrorMessage(osErr.str());
				delete pErr;

				// The socket has been closed, break and let this thread die.
				break;
			}

			try {
				//  Parse Txn type
				switch (pMessage->TxnType) {
				case BROKER_VOLUME:
					iRet = pThrParam->pBrokerageHouse->RunBrokerVolume(
							&(pMessage->TxnInput.BrokerVolumeTxnInput),
							brokerVolume);
					break;
				case CUSTOMER_POSITION:
					iRet = pThrParam->pBrokerageHouse->RunCustomerPosition(
							&(pMessage->TxnInput.CustomerPositionTxnInput),
							customerPosition);
					if (iRet != 0)
						pDBConnection->rollback();
					break;
				case MARKET_FEED:
					iRet = pThrParam->pBrokerageHouse->RunMarketFeed(
							&(pMessage->TxnInput.MarketFeedTxnInput),
							marketFeed);
					break;
				case MARKET_WATCH:
					iRet = pThrParam->pBrokerageHouse->RunMarketWatch(
							&(pMessage->TxnInput.MarketWatchTxnInput),
							marketWatch);
					break;
				case SECURITY_DETAIL:
					iRet = pThrParam->pBrokerageHouse->RunSecurityDetail(
							&(pMessage->TxnInput.SecurityDetailTxnInput),
							securityDetail);
					break;
				case TRADE_LOOKUP:
					iRet = pThrParam->pBrokerageHouse->RunTradeLookup(
							&(pMessage->TxnInput.TradeLookupTxnInput),
							tradeLookup);
					break;
				case TRADE_ORDER:
					iRet = pThrParam->pBrokerageHouse->RunTradeOrder(
							&(pMessage->TxnInput.TradeOrderTxnInput),
							tradeOrder);
					if (iRet != 0)
						pDBConnection->rollback();
					break;
				case TRADE_RESULT:
					iRet = pThrParam->pBrokerageHouse->RunTradeResult(
							&(pMessage->TxnInput.TradeResultTxnInput),
							tradeResult);
					if (iRet != 0)
						pDBConnection->rollback();
					break;
				case TRADE_STATUS:
					iRet = pThrParam->pBrokerageHouse->RunTradeStatus(
							&(pMessage->TxnInput.TradeStatusTxnInput),
							tradeStatus);
					break;
				case TRADE_UPDATE:
					iRet = pThrParam->pBrokerageHouse->RunTradeUpdate(
							&(pMessage->TxnInput.TradeUpdateTxnInput),
							tradeUpdate);
					break;
				case DATA_MAINTENANCE:
					iRet = pThrParam->pBrokerageHouse->RunDataMaintenance(
							&(pMessage->TxnInput.DataMaintenanceTxnInput),
							dataMaintenance);
					break;
				case TRADE_CLEANUP:
					iRet = pThrParam->pBrokerageHouse->RunTradeCleanup(
							&(pMessage->TxnInput.TradeCleanupTxnInput),
							tradeCleanup);
					break;
				default:
					cout << "wrong txn type" << endl;
					iRet = ERR_TYPE_WRONGTXN;
				}
			} catch (const char *str) {
				pid_t pid = syscall(SYS_gettid);
				ostringstream msg;
				msg << time(NULL) << " " << pid << " "
					<< szTransactionName[pMessage->TxnType] << endl;
				pThrParam->pBrokerageHouse->logErrorMessage(msg.str());
				iRet = CBaseTxnErr::EXPECTED_ROLLBACK;
			}

			if (iRet < 0)
				cerr << "INVALID RUN : see "
					 << pThrParam->pBrokerageHouse->errorLogFilename()
					 << " for transaction details" << endl;

			// send status to driver
			Reply.iStatus = iRet;
			try {
				sockDrv.dbt5Send(
						reinterpret_cast<void *>(&Reply), sizeof(Reply));
			} catch (CSocketErr *pErr) {
				sockDrv.dbt5Disconnect();

				ostringstream osErr;
				osErr << "Error on Send: " << pErr->ErrorText()
					  << " at BrokerageHouse::workerThread" << endl;
				pThrParam->pBrokerageHouse->logErrorMessage(osErr.str());
				delete pErr;

				// The socket has been closed, break and let this thread die.
				break;
			}
		} while (true);

		delete pDBConnection; // close connection with the database
		close(pThrParam->iSockfd); // close socket connection with the driver
		delete pThrParam;
		delete pMessage;
	} catch (CSocketErr *err) {
	}
	return NULL;
}

// entry point for worker thread
void
entryWorkerThread(void *data)
{
	PThreadParameter pThrParam = reinterpret_cast<PThreadParameter>(data);

	pthread_t threadID; // thread ID
	pthread_attr_t threadAttribute; // thread attribute

	try {
		// initialize the attribute object
		int status = pthread_attr_init(&threadAttribute);
		if (status != 0) {
			throw new CThreadErr(CThreadErr::ERR_THREAD_ATTR_INIT);
		}

		// set the detachstate attribute to detached
		status = pthread_attr_setdetachstate(
				&threadAttribute, PTHREAD_CREATE_DETACHED);
		if (status != 0) {
			throw new CThreadErr(CThreadErr::ERR_THREAD_ATTR_DETACH);
		}

		// create the thread in the detached state
		status = pthread_create(
				&threadID, &threadAttribute, &workerThread, data);

		if (status != 0) {
			throw new CThreadErr(CThreadErr::ERR_THREAD_CREATE);
		}
	} catch (CThreadErr *pErr) {
		// close recently accepted connection, to release driver threads
		close(pThrParam->iSockfd);

		ostringstream osErr;
		osErr << "Error: " << pErr->ErrorText() << " at "
			  << "BrokerageHouse::entryWorkerThread" << endl
			  << "accepted socket connection closed" << endl;
		pThrParam->pBrokerageHouse->logErrorMessage(osErr.str());
		delete pThrParam;
		delete pErr;
	}
}

// Constructor
CBrokerageHouse::CBrokerageHouse(const char *szHost, const char *szDBName,
		const char *szDBPort, const char *szMEEHost, const char *szMEEPort,
		const int iListenPort, char *outputDirectory, bool verbose = false)
: m_iListenPort(iListenPort), m_Verbose(verbose)
{
	strncpy(m_szHost, szHost, iMaxHostname);
	m_szHost[iMaxHostname] = '\0';
	strncpy(m_szDBName, szDBName, iMaxDBName);
	m_szDBName[iMaxDBName] = '\0';
	strncpy(m_szDBPort, szDBPort, iMaxPort);
	m_szDBPort[iMaxPort] = '\0';

	strncpy(m_szMEEHost, szMEEHost, iMaxHostname);
	m_szMEEHost[iMaxHostname] = '\0';
	strncpy(m_szMEEPort, szMEEPort, iMaxPort);
	m_szMEEPort[iMaxPort] = '\0';

	snprintf(m_errorLogFilename, iMaxPath, "%s/BrokerageHouse_Error.log",
			outputDirectory);
	m_fLog.open(m_errorLogFilename, ios::out);
}

// Destructor
CBrokerageHouse::~CBrokerageHouse()
{
	m_Socket.closeListenerSocket();
	m_fLog.close();
}

void
CBrokerageHouse::dumpInputData(PBrokerVolumeTxnInput pTxnInput)
{
	pid_t pid = syscall(SYS_gettid);
	ostringstream msg;
	for (int i = 0; i < max_broker_list_len; i++) {
		msg << pid << " broker_list[" << i
			<< "] = " << pTxnInput->broker_list[i] << endl;
		msg << pid << " sector_name[" << i
			<< "] = " << pTxnInput->sector_name[i] << endl;
	}
	logErrorMessage(msg.str(), m_Verbose);
}

void
CBrokerageHouse::dumpInputData(PCustomerPositionTxnInput pTxnInput)
{
	pid_t pid = syscall(SYS_gettid);
	ostringstream msg;
	msg << pid << " acct_id_idx = " << pTxnInput->acct_id_idx << endl
		<< pid << " cust_id = " << pTxnInput->cust_id << endl
		<< pid << " get_history = " << pTxnInput->get_history << endl
		<< pid << " tax_id = " << pTxnInput->tax_id << endl;
	logErrorMessage(msg.str(), m_Verbose);
}

void
CBrokerageHouse::dumpInputData(PDataMaintenanceTxnInput pTxnInput)
{
	pid_t pid = syscall(SYS_gettid);
	ostringstream msg;
	msg << pid << " acct_id = " << pTxnInput->acct_id << endl
		<< pid << " c_id = " << pTxnInput->c_id << endl
		<< pid << " co_id = " << pTxnInput->co_id << endl
		<< pid << " day_of_month = " << pTxnInput->day_of_month << endl
		<< pid << " vol_incr = " << pTxnInput->vol_incr << endl
		<< pid << " symbol = " << pTxnInput->symbol << endl
		<< pid << " table_name = " << pTxnInput->table_name << endl
		<< pid << " tx_id = " << pTxnInput->tx_id << endl;
	logErrorMessage(msg.str(), m_Verbose);
}

void
CBrokerageHouse::dumpInputData(PTradeCleanupTxnInput pTxnInput)
{
	pid_t pid = syscall(SYS_gettid);
	ostringstream msg;
	msg << pid << " start_trade_id = " << pTxnInput->start_trade_id << endl
		<< pid << " st_canceled_id = " << pTxnInput->st_canceled_id << endl
		<< pid << " st_pending_id = " << pTxnInput->st_pending_id << endl
		<< pid << " st_submitted_id = " << pTxnInput->st_submitted_id << endl;
	logErrorMessage(msg.str(), m_Verbose);
}

void
CBrokerageHouse::dumpInputData(PMarketWatchTxnInput pTxnInput)
{
	pid_t pid = syscall(SYS_gettid);
	ostringstream msg;
	msg << pid << " acct_id = " << pTxnInput->acct_id << endl
		<< pid << " c_id = " << pTxnInput->c_id << endl
		<< pid << " ending_co_id = " << pTxnInput->ending_co_id << endl
		<< pid << " starting_co_id = " << pTxnInput->starting_co_id << endl
		<< pid << " start_day = " << pTxnInput->start_day.year << "-"
		<< pTxnInput->start_day.month << "-" << pTxnInput->start_day.day << " "
		<< pTxnInput->start_day.hour << ":" << pTxnInput->start_day.minute
		<< ":" << pTxnInput->start_day.second << "."
		<< pTxnInput->start_day.fraction << endl
		<< pid << " industry_name = " << pTxnInput->industry_name << endl;
	logErrorMessage(msg.str(), m_Verbose);
}

void
CBrokerageHouse::dumpInputData(PMarketFeedTxnInput pTxnInput)
{
	pid_t pid = syscall(SYS_gettid);
	ostringstream msg;
	msg << pid << " StatusAndTradeType.status_submitted = "
		<< pTxnInput->StatusAndTradeType.status_submitted << endl
		<< pid << " StatusAndTradeType.type_limit_buy = "
		<< pTxnInput->StatusAndTradeType.type_limit_buy << endl
		<< pid << " StatusAndTradeType.type_limit_sell = "
		<< pTxnInput->StatusAndTradeType.type_limit_sell << endl
		<< pid << " StatusAndTradeType.type_stop_loss = "
		<< pTxnInput->StatusAndTradeType.type_stop_loss << endl;
	for (int i = 0; i < max_feed_len; i++) {
		msg << pid << " Entries[" << i
			<< "].price_quote = " << pTxnInput->Entries[i].price_quote << endl
			<< pid << " Entries[" << i
			<< "].trade_qty = " << pTxnInput->Entries[i].trade_qty << endl
			<< pid << " Entries[" << i
			<< "].symbol = " << pTxnInput->Entries[i].symbol << endl;
	}
	logErrorMessage(msg.str(), m_Verbose);
}

void
CBrokerageHouse::dumpInputData(PSecurityDetailTxnInput pTxnInput)
{
	pid_t pid = syscall(SYS_gettid);
	ostringstream msg;
	msg << pid << " max_rows_to_return = " << pTxnInput->max_rows_to_return
		<< endl
		<< pid << " access_lob_flag = " << pTxnInput->access_lob_flag << endl
		<< pid << " start_day = " << pTxnInput->start_day.year << "-"
		<< pTxnInput->start_day.month << "-" << pTxnInput->start_day.day << " "
		<< pTxnInput->start_day.hour << ":" << pTxnInput->start_day.minute
		<< ":" << pTxnInput->start_day.second << "."
		<< pTxnInput->start_day.fraction << endl
		<< pid << " symbol = " << pTxnInput->symbol << endl;
	logErrorMessage(msg.str(), m_Verbose);
}

void
CBrokerageHouse::dumpInputData(PTradeStatusTxnInput pTxnInput)
{
	pid_t pid = syscall(SYS_gettid);
	ostringstream msg;
	msg << pid << " acct_id = " << pTxnInput->acct_id << endl;
	logErrorMessage(msg.str(), m_Verbose);
}

void
CBrokerageHouse::dumpInputData(PTradeLookupTxnInput pTxnInput)
{
	pid_t pid = syscall(SYS_gettid);
	ostringstream msg;
	for (int i = 0; i < TradeLookupFrame1MaxRows; i++) {
		msg << pid << " trade_id[" << i << "] = " << pTxnInput->trade_id[i]
			<< endl;
	}
	msg << pid << " acct_id = " << pTxnInput->acct_id << endl
		<< pid << " max_acct_id = " << pTxnInput->max_acct_id << endl
		<< pid << " frame_to_execute = " << pTxnInput->frame_to_execute << endl
		<< pid << " max_trades = " << pTxnInput->max_trades << endl
		<< pid << " end_trade_dts = " << pTxnInput->end_trade_dts.year << "-"
		<< pTxnInput->end_trade_dts.month << "-"
		<< pTxnInput->end_trade_dts.day << " " << pTxnInput->end_trade_dts.hour
		<< ":" << pTxnInput->end_trade_dts.minute << ":"
		<< pTxnInput->end_trade_dts.second << "."
		<< pTxnInput->end_trade_dts.fraction << endl
		<< pid << " start_trade_dts = " << pTxnInput->start_trade_dts.year
		<< "-" << pTxnInput->start_trade_dts.month << "-"
		<< pTxnInput->start_trade_dts.day << " "
		<< pTxnInput->start_trade_dts.hour << ":"
		<< pTxnInput->start_trade_dts.minute << ":"
		<< pTxnInput->start_trade_dts.second << "."
		<< pTxnInput->start_trade_dts.fraction << endl
		<< pid << " symbol = " << pTxnInput->symbol << endl;
	logErrorMessage(msg.str(), m_Verbose);
}

void
CBrokerageHouse::dumpInputData(PTradeOrderTxnInput pTxnInput)
{
	pid_t pid = syscall(SYS_gettid);
	ostringstream msg;
	msg << pid << " requested_price = " << pTxnInput->requested_price << endl
		<< pid << " acct_id = " << pTxnInput->acct_id << endl
		<< pid << " is_lifo = " << pTxnInput->is_lifo << endl
		<< pid << " roll_it_back = " << pTxnInput->roll_it_back << endl
		<< pid << " trade_qty = " << pTxnInput->trade_qty << endl
		<< pid << " type_is_margin = " << pTxnInput->type_is_margin << endl
		<< pid << " co_name = " << pTxnInput->co_name << endl
		<< pid << " exec_f_name = " << pTxnInput->exec_f_name << endl
		<< pid << " exec_l_name = " << pTxnInput->exec_l_name << endl
		<< pid << " exec_tax_id = " << pTxnInput->exec_tax_id << endl
		<< pid << " issue = " << pTxnInput->issue << endl
		<< pid << " st_pending_id = " << pTxnInput->st_pending_id << endl
		<< pid << " st_submitted_id = " << pTxnInput->st_submitted_id << endl
		<< pid << " symbol = " << pTxnInput->symbol << endl
		<< pid << " trade_type_id = " << pTxnInput->trade_type_id << endl;
	logErrorMessage(msg.str(), m_Verbose);
}

void
CBrokerageHouse::dumpInputData(PTradeResultTxnInput pTxnInput)
{
	pid_t pid = syscall(SYS_gettid);
	ostringstream msg;
	msg << pid << " trade_price = " << pTxnInput->trade_price << endl
		<< pid << " trade_id = " << pTxnInput->trade_id << endl;
	logErrorMessage(msg.str(), m_Verbose);
}

void
CBrokerageHouse::dumpInputData(PTradeUpdateTxnInput pTxnInput)
{
	pid_t pid = syscall(SYS_gettid);
	ostringstream msg;
	for (int i = 0; i < TradeUpdateFrame1MaxRows; i++) {
		msg << pid << " trade_id[" << i << "] = " << pTxnInput->trade_id[i]
			<< endl;
	}
	msg << pid << " acct_id = " << pTxnInput->acct_id << endl
		<< pid << " max_acct_id = " << pTxnInput->max_acct_id << endl
		<< pid << " frame_to_execute = " << pTxnInput->frame_to_execute << endl
		<< pid << " max_trades = " << pTxnInput->max_trades << endl
		<< pid << " max_updates = " << pTxnInput->max_updates << endl
		<< pid << " end_trade_dts = " << pTxnInput->end_trade_dts.year << "-"
		<< pTxnInput->end_trade_dts.month << "-"
		<< pTxnInput->end_trade_dts.day << " " << pTxnInput->end_trade_dts.hour
		<< ":" << pTxnInput->end_trade_dts.minute << ":"
		<< pTxnInput->end_trade_dts.second << "."
		<< pTxnInput->end_trade_dts.fraction << endl
		<< pid << " start_trade_dts = " << pTxnInput->start_trade_dts.year
		<< "-" << pTxnInput->start_trade_dts.month << "-"
		<< pTxnInput->start_trade_dts.day << " "
		<< pTxnInput->start_trade_dts.hour << ":"
		<< pTxnInput->start_trade_dts.minute << ":"
		<< pTxnInput->start_trade_dts.second << "."
		<< pTxnInput->start_trade_dts.fraction << endl
		<< pid << " symbol = " << pTxnInput->symbol << endl;
	logErrorMessage(msg.str(), m_Verbose);
}

// Run Broker Volume transaction
INT32
CBrokerageHouse::RunBrokerVolume(
		PBrokerVolumeTxnInput pTxnInput, CBrokerVolume &brokerVolume)
{
	TBrokerVolumeTxnOutput bvOutput;
	memset(&bvOutput, 0, sizeof(TBrokerVolumeTxnOutput));

	try {
		brokerVolume.DoTxn(pTxnInput, &bvOutput);
	} catch (const exception &e) {
		logErrorMessage("BV EXCEPTION\n", m_Verbose);
		bvOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (bvOutput.status != CBaseTxnErr::SUCCESS) {
		pid_t pid = syscall(SYS_gettid);
		ostringstream msg;
		msg << pid << " Broker Volume " << bvOutput.status << endl;
		switch (bvOutput.status) {
		case -111:
			msg << pid << " (list_len < 0) or (list_len > max_broker_list_len)"
				<< endl;
			break;
		}
		logErrorMessage(msg.str(), m_Verbose);
		dumpInputData(pTxnInput);
	}
	return bvOutput.status;
}

// Run Customer Position transaction
INT32
CBrokerageHouse::RunCustomerPosition(PCustomerPositionTxnInput pTxnInput,
		CCustomerPosition &customerPosition)
{
	TCustomerPositionTxnOutput cpOutput;
	memset(&cpOutput, 0, sizeof(TCustomerPositionTxnOutput));

	try {
		customerPosition.DoTxn(pTxnInput, &cpOutput);
	} catch (const exception &e) {
		logErrorMessage("CP EXCEPTION\n", m_Verbose);
		cpOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (cpOutput.status != CBaseTxnErr::SUCCESS) {
		pid_t pid = syscall(SYS_gettid);
		ostringstream msg;
		msg << pid << " Customer Position " << cpOutput.status << endl;
		switch (cpOutput.status) {
		case -211:
			msg << pid << " (acct_len < 1) or (acct_len > max_acct_len)"
				<< endl;
			break;
		case -221:
			msg << pid << " (hist_len < 10) or (hist_len > max_hist_len)"
				<< endl;
			break;
		}
		logErrorMessage(msg.str(), m_Verbose);
		dumpInputData(pTxnInput);
	}
	return cpOutput.status;
}

// Run Data Maintenance transaction
INT32
CBrokerageHouse::RunDataMaintenance(
		PDataMaintenanceTxnInput pTxnInput, CDataMaintenance &dataMaintenance)
{
	TDataMaintenanceTxnOutput dmOutput;
	memset(&dmOutput, 0, sizeof(TDataMaintenanceTxnOutput));

	try {
		dataMaintenance.DoTxn(pTxnInput, &dmOutput);
	} catch (const exception &e) {
		logErrorMessage("DM EXCEPTION\n", m_Verbose);
		dmOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (dmOutput.status != CBaseTxnErr::SUCCESS) {
		ostringstream msg;
		msg << __FILE__ << " " << __LINE__ << " " << dmOutput.status << endl;
		logErrorMessage(msg.str(), m_Verbose);
		dumpInputData(pTxnInput);
	}
	return dmOutput.status;
}

// Run Trade Cleanup transaction
INT32
CBrokerageHouse::RunTradeCleanup(
		PTradeCleanupTxnInput pTxnInput, CTradeCleanup &tradeCleanup)
{
	TTradeCleanupTxnOutput tcOutput;
	memset(&tcOutput, 0, sizeof(TTradeCleanupTxnOutput));

	try {
		tradeCleanup.DoTxn(pTxnInput, &tcOutput);
	} catch (const exception &e) {
		logErrorMessage("TC EXCEPTION\n", m_Verbose);
		tcOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (tcOutput.status != CBaseTxnErr::SUCCESS) {
		ostringstream msg;
		msg << __FILE__ << " " << __LINE__ << " " << tcOutput.status << endl;
		logErrorMessage(msg.str(), m_Verbose);
		dumpInputData(pTxnInput);
	}
	return tcOutput.status;
}

// Run Market Feed transaction
INT32
CBrokerageHouse::RunMarketFeed(
		PMarketFeedTxnInput pTxnInput, CMarketFeed &marketFeed)
{
	TMarketFeedTxnOutput mfOutput;
	memset(&mfOutput, 0, sizeof(TMarketFeedTxnOutput));

	try {
		marketFeed.DoTxn(pTxnInput, &mfOutput);
	} catch (const exception &e) {
		logErrorMessage("MF EXCEPTION\n", m_Verbose);
		mfOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (mfOutput.status != CBaseTxnErr::SUCCESS) {
		pid_t pid = syscall(SYS_gettid);
		ostringstream msg;
		msg << pid << " Market Feed " << mfOutput.status << endl;
		switch (mfOutput.status) {
		case -311:
			msg << pid << " (num_updated < unique_symbols)" << endl;
			break;
		}
		logErrorMessage(msg.str(), m_Verbose);
		dumpInputData(pTxnInput);
	}
	return mfOutput.status;
}

// Run Market Watch transaction
INT32
CBrokerageHouse::RunMarketWatch(
		PMarketWatchTxnInput pTxnInput, CMarketWatch &marketWatch)
{
	TMarketWatchTxnOutput mwOutput;
	memset(&mwOutput, 0, sizeof(TMarketWatchTxnOutput));

	try {
		marketWatch.DoTxn(pTxnInput, &mwOutput);
	} catch (const exception &e) {
		logErrorMessage("MW EXCEPTION\n", m_Verbose);
		mwOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (mwOutput.status != CBaseTxnErr::SUCCESS) {
		pid_t pid = syscall(SYS_gettid);
		ostringstream msg;
		msg << pid << " Market Watch " << mwOutput.status << endl;
		switch (mwOutput.status) {
		case -411:
			msg << pid
				<< " (acct_id != 0) and (cust_id != 0) and (industry_name != "
				   "'')"
				<< endl;
			break;
		}
		logErrorMessage(msg.str(), m_Verbose);
		dumpInputData(pTxnInput);
	}
	return mwOutput.status;
}

// Run Security Detail transaction
INT32
CBrokerageHouse::RunSecurityDetail(
		PSecurityDetailTxnInput pTxnInput, CSecurityDetail &securityDetail)
{
	TSecurityDetailTxnOutput sdOutput;
	memset(&sdOutput, 0, sizeof(TSecurityDetailTxnOutput));

	try {
		securityDetail.DoTxn(pTxnInput, &sdOutput);
	} catch (const exception &e) {
		logErrorMessage("SD EXCEPTION\n", m_Verbose);
		sdOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (sdOutput.status != CBaseTxnErr::SUCCESS) {
		pid_t pid = syscall(SYS_gettid);
		ostringstream msg;
		msg << pid << " Security Detail " << sdOutput.status << endl;
		switch (sdOutput.status) {
		case -511:
			msg << pid << " (day_len < min_day_len) or (day_len > max_day_len)"
				<< endl;
			break;
		case -512:
			msg << pid << " fin_len != max_fin_len" << endl;
			break;
		case -513:
			msg << pid << " news_len != max_news_len" << endl;
			break;
		}
		logErrorMessage(msg.str(), m_Verbose);
		dumpInputData(pTxnInput);
	}
	return sdOutput.status;
}

// Run Trade Lookup transaction
INT32
CBrokerageHouse::RunTradeLookup(
		PTradeLookupTxnInput pTxnInput, CTradeLookup &tradeLookup)
{
	TTradeLookupTxnOutput tlOutput;
	memset(&tlOutput, 0, sizeof(TTradeLookupTxnOutput));

	try {
		tradeLookup.DoTxn(pTxnInput, &tlOutput);
	} catch (const exception &e) {
		logErrorMessage("TL EXCEPTION\n", m_Verbose);
		tlOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (tlOutput.status != CBaseTxnErr::SUCCESS) {
		pid_t pid = syscall(SYS_gettid);
		ostringstream msg;
		msg << pid << " Trade Lookup " << tlOutput.status << endl;
		switch (tlOutput.status) {
		case -611:
			msg << pid << " num_found != max_trades" << endl;
			break;
		case -621:
			msg << pid << " (num_found < 0) or (num_found > max_trades)"
				<< endl;
			break;
		case 621:
			msg << pid << " num_found == 0" << endl;
			break;
		case -631:
			msg << pid << " (num_found < 0) or (num_found > max_trades)"
				<< endl;
			break;
		case 631:
			msg << pid << " num_found == 0" << endl;
			break;
		case -641:
			msg << pid << " num_trades_found <> 1" << endl;
			break;
		case 641:
			msg << pid << " num_trades_found == 0" << endl;
			break;
		case -642:
			msg << pid << " (num_found < 1) or (num_found > 20)" << endl;
			break;
		}
		logErrorMessage(msg.str(), m_Verbose);
		dumpInputData(pTxnInput);
	}
	return tlOutput.status;
}

// Run Trade Order transaction
INT32
CBrokerageHouse::RunTradeOrder(
		PTradeOrderTxnInput pTxnInput, CTradeOrder &tradeOrder)
{
	TTradeOrderTxnOutput toOutput;
	memset(&toOutput, 0, sizeof(TTradeOrderTxnOutput));

	try {
		tradeOrder.DoTxn(pTxnInput, &toOutput);
	} catch (const exception &e) {
		toOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (toOutput.status != CBaseTxnErr::SUCCESS
			&& !(toOutput.status == CBaseTxnErr::EXPECTED_ROLLBACK
					&& pTxnInput->roll_it_back)) {
		pid_t pid = syscall(SYS_gettid);
		ostringstream msg;
		msg << pid << " Trade Order " << toOutput.status << endl;
		switch (toOutput.status) {
		case -711:
			msg << pid << " num_found <> 1" << endl;
			break;
		case -721:
			msg << pid
				<< " exec_l_name != cust_l_name or exec_f_name != cust_f_name "
				   "or exec_tax_id != tax_id"
				<< endl;
			break;
		case -731:
			msg << pid
				<< " (sell_value > buy_value) and ((tax_status == 1) or "
				   "(tax_status == 2)) and (tax_amount <= 0.00)"
				<< endl;
			break;
		case -732:
			msg << pid << " comm_rate <= 0.0000" << endl;
			break;
		case -733:
			msg << pid << " charge_amount == 0.00" << endl;
			break;
		}
		logErrorMessage(msg.str(), m_Verbose);
		dumpInputData(pTxnInput);
	}
	return toOutput.status;
}

// Run Trade Result transaction
INT32
CBrokerageHouse::RunTradeResult(
		PTradeResultTxnInput pTxnInput, CTradeResult &tradeResult)
{
	TTradeResultTxnOutput trOutput;
	memset(&trOutput, 0, sizeof(TTradeResultTxnOutput));

	try {
		tradeResult.DoTxn(pTxnInput, &trOutput);
	} catch (const exception &e) {
		logErrorMessage("TR EXCEPTION\n", m_Verbose);
		trOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (trOutput.status != CBaseTxnErr::SUCCESS) {
		pid_t pid = syscall(SYS_gettid);
		ostringstream msg;
		msg << pid << " Trade Result " << trOutput.status << endl;
		switch (trOutput.status) {
		case -811:
			msg << pid << " num_found <> 1" << endl;
			break;
		case -831:
			msg << pid << " tax_amount <= 0.00" << endl;
			break;
		case -841:
			msg << pid << " comm_rate <= 0.00" << endl;
			break;
		}
		logErrorMessage(msg.str(), m_Verbose);
		dumpInputData(pTxnInput);
	}
	return trOutput.status;
}

// Run Trade Status transaction
INT32
CBrokerageHouse::RunTradeStatus(
		PTradeStatusTxnInput pTxnInput, CTradeStatus &tradeStatus)
{
	TTradeStatusTxnOutput tsOutput;
	memset(&tsOutput, 0, sizeof(TTradeStatusTxnOutput));

	try {
		tradeStatus.DoTxn(pTxnInput, &tsOutput);
	} catch (const exception &e) {
		logErrorMessage("TS EXCEPTION\n", m_Verbose);
		tsOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (tsOutput.status != CBaseTxnErr::SUCCESS) {
		pid_t pid = syscall(SYS_gettid);
		ostringstream msg;
		msg << pid << " Trade Status " << tsOutput.status << endl;
		switch (tsOutput.status) {
		case -911:
			msg << pid << " num_found <> max_trade_status_len" << endl;
			break;
		}
		logErrorMessage(msg.str(), m_Verbose);
		dumpInputData(pTxnInput);
	}
	return tsOutput.status;
}

// Run Trade Update transaction
INT32
CBrokerageHouse::RunTradeUpdate(
		PTradeUpdateTxnInput pTxnInput, CTradeUpdate &tradeUpdate)
{
	TTradeUpdateTxnOutput tuOutput;
	memset(&tuOutput, 0, sizeof(TTradeUpdateTxnOutput));

	try {
		tradeUpdate.DoTxn(pTxnInput, &tuOutput);
	} catch (const exception &e) {
		logErrorMessage("TU EXCEPTION\n", m_Verbose);
		tuOutput.status = CBaseTxnErr::EXPECTED_ROLLBACK;
	}

	if (tuOutput.status != CBaseTxnErr::SUCCESS) {
		pid_t pid = syscall(SYS_gettid);
		ostringstream msg;
		msg << pid << " Trade Update " << tuOutput.status << endl;
		switch (tuOutput.status) {
		case -1011:
			msg << pid << " num_found != max_trades" << endl;
			break;
		case -1012:
			msg << pid << " num_updated != max_updates" << endl;
			break;
		case -1021:
			msg << pid << " (num_found < 0) or (num_found > max_trades)"
				<< endl;
			break;
		case 1021:
			msg << pid << " num_updated == 0" << endl;
			break;
		case -1022:
			msg << pid << " num_updated <> num_found" << endl;
			break;
		case 1031:
			msg << pid << " num_updated == 0" << endl;
			break;
		case -1031:
			msg << pid << " (num_found < 0) or (num_found > max_trades)"
				<< endl;
			break;
		case -1032:
			msg << pid << " num_updated > num_found" << endl;
			break;
		case 1032:
			msg << pid << " num_updated == 0" << endl;
			break;
		}
		logErrorMessage(msg.str(), m_Verbose);
		dumpInputData(pTxnInput);
	}
	return tuOutput.status;
}

// Listener
void
CBrokerageHouse::startListener(void)
{
	int acc_socket;
	PThreadParameter pThrParam = NULL;

	m_Socket.dbt5Listen(m_iListenPort);

	while (true) {
		acc_socket = 0;
		try {
			acc_socket = m_Socket.dbt5Accept();

			pThrParam = new TThreadParameter;
			// zero the structure
			memset(pThrParam, 0, sizeof(TThreadParameter));

			pThrParam->iSockfd = acc_socket;
			pThrParam->pBrokerageHouse = this;
			strncpy(pThrParam->m_szMEEHost, m_szMEEHost, iMaxHostname);
			pThrParam->m_szMEEHost[iMaxHostname] = '\0';
			strncpy(pThrParam->m_szMEEPort, m_szMEEPort, iMaxPort);
			pThrParam->m_szMEEPort[iMaxPort] = '\0';

			// call entry point
			entryWorkerThread(reinterpret_cast<void *>(pThrParam));
		} catch (CSocketErr *pErr) {
			ostringstream osErr;
			osErr << "Problem accepting socket connection" << endl
				  << "Error: " << pErr->ErrorText() << " at "
				  << "BrokerageHouse::Listener" << endl;
			logErrorMessage(osErr.str());
			delete pErr;
			delete pThrParam;
		}
	}
}

// logErrorMessage
void
CBrokerageHouse::logErrorMessage(const string sErr, bool bScreen)
{
	m_LogLock.lock();
	if (bScreen)
		cout << sErr;
	m_fLog << sErr;
	m_fLog.flush();
	m_LogLock.unlock();
}

char *
CBrokerageHouse::errorLogFilename()
{
	return m_errorLogFilename;
}

bool
CBrokerageHouse::verbose()
{
	return m_Verbose;
}
