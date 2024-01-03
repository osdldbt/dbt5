/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 08 July 2006
 */

#include "TradeLookupDB.h"

CTradeLookupDB::CTradeLookupDB(CDBConnection *pDBConn, bool verbose = false)
: CTxnBaseDB(pDBConn), m_Verbose(verbose)
{
	m_pid = syscall(SYS_gettid);
}

// Call Trade Lookup Frame 1
void
CTradeLookupDB::DoTradeLookupFrame1(
		const TTradeLookupFrame1Input *pIn, TTradeLookupFrame1Output *pOut)
{
	if (m_Verbose) {
		cout << m_pid << " <<< TLF1" << endl;
		cout << m_pid << " - Trade Lookup Frame 1 (input)" << endl
			 << m_pid << " -- max_trades: " << pIn->max_trades << endl;
		for (int i = 0; i < pIn->max_trades; i++)
			cout << m_pid << " -- trade_id[" << i << "]: " << pIn->trade_id[i]
				 << endl;
	}

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);
	commitTransaction();

	if (m_Verbose) {
		cout << m_pid << " - Trade Lookup Frame 1 (output)" << endl
			 << m_pid << " -- num_found: " << pOut->num_found << endl;
		for (int i = 0; i < pOut->num_found; i++) {
			cout << m_pid << " -- bid_price[" << i
				 << "]: " << pOut->trade_info[i].bid_price << endl
				 << m_pid << " -- exec_name[" << i
				 << "]: " << pOut->trade_info[i].exec_name << endl
				 << m_pid << " -- is_cash[" << i
				 << "]: " << pOut->trade_info[i].is_cash << endl
				 << m_pid << " -- is_market[" << i
				 << "]: " << pOut->trade_info[i].is_market << endl
				 << m_pid << " -- trade_price[" << i
				 << "]: " << pOut->trade_info[i].trade_price << endl
				 << m_pid << " -- settlement_amount[" << i
				 << "]: " << pOut->trade_info[i].settlement_amount << endl
				 << m_pid << " -- settlement_cash_due_date[" << i
				 << "]: " << pOut->trade_info[i].settlement_cash_due_date.year
				 << "-" << pOut->trade_info[i].settlement_cash_due_date.month
				 << "-" << pOut->trade_info[i].settlement_cash_due_date.day
				 << " " << pOut->trade_info[i].settlement_cash_due_date.hour
				 << ":" << pOut->trade_info[i].settlement_cash_due_date.minute
				 << ":" << pOut->trade_info[i].settlement_cash_due_date.second
				 << endl
				 << m_pid << " -- settlement_cash_type[" << i
				 << "]: " << pOut->trade_info[i].settlement_cash_type << endl
				 << m_pid << " -- cash_transaction_amount[" << i
				 << "]: " << pOut->trade_info[i].cash_transaction_amount
				 << endl
				 << m_pid << " -- cash_transaction_dts[" << i
				 << "]: " << pOut->trade_info[i].cash_transaction_dts.year
				 << "-" << pOut->trade_info[i].cash_transaction_dts.month
				 << "-" << pOut->trade_info[i].cash_transaction_dts.day << " "
				 << pOut->trade_info[i].cash_transaction_dts.hour << ":"
				 << pOut->trade_info[i].cash_transaction_dts.minute << ":"
				 << pOut->trade_info[i].cash_transaction_dts.second << endl
				 << m_pid << " -- cash_transaction_name[" << i
				 << "]: " << pOut->trade_info[i].cash_transaction_name << endl
				 << m_pid << " -- trade_history_dts[" << i
				 << "][0]: " << pOut->trade_info[i].trade_history_dts[0].year
				 << "-" << pOut->trade_info[i].trade_history_dts[0].month
				 << "-" << pOut->trade_info[i].trade_history_dts[0].day << " "
				 << pOut->trade_info[i].trade_history_dts[0].hour << ":"
				 << pOut->trade_info[i].trade_history_dts[0].minute << ":"
				 << pOut->trade_info[i].trade_history_dts[0].second << endl
				 << m_pid << " -- trade_history_status_id[" << i
				 << "][0]: " << pOut->trade_info[i].trade_history_status_id[0]
				 << endl
				 << m_pid << " -- trade_history_dts[" << i
				 << "][1]: " << pOut->trade_info[i].trade_history_dts[1].year
				 << "-" << pOut->trade_info[i].trade_history_dts[1].month
				 << "-" << pOut->trade_info[i].trade_history_dts[1].day << " "
				 << pOut->trade_info[i].trade_history_dts[1].hour << ":"
				 << pOut->trade_info[i].trade_history_dts[1].minute << ":"
				 << pOut->trade_info[i].trade_history_dts[1].second << endl
				 << m_pid << " -- trade_history_status_id[" << i
				 << "][1]: " << pOut->trade_info[i].trade_history_status_id[1]
				 << endl
				 << m_pid << " -- trade_history_dts[" << i
				 << "][2]: " << pOut->trade_info[i].trade_history_dts[2].year
				 << "-" << pOut->trade_info[i].trade_history_dts[2].month
				 << "-" << pOut->trade_info[i].trade_history_dts[2].day << " "
				 << pOut->trade_info[i].trade_history_dts[2].hour << ":"
				 << pOut->trade_info[i].trade_history_dts[2].minute << ":"
				 << pOut->trade_info[i].trade_history_dts[2].second << endl
				 << m_pid << " -- trade_history_status_id[" << i
				 << "][2]: " << pOut->trade_info[i].trade_history_status_id[2]
				 << endl;
		}
		cout << m_pid << " >>> TLF1" << endl;
	}
}

// Call Trade Lookup Frame 2
void
CTradeLookupDB::DoTradeLookupFrame2(
		const TTradeLookupFrame2Input *pIn, TTradeLookupFrame2Output *pOut)
{
	if (m_Verbose) {
		cout << m_pid << " <<< TLF2" << endl
			 << m_pid << " - Trade Lookup Frame 2 (input)" << endl
			 << m_pid << " -- acct_id: " << pIn->acct_id << endl
			 << m_pid << " -- max_trades: " << pIn->max_trades << endl
			 << m_pid << " -- trade_dts: " << pIn->end_trade_dts.year << "-"
			 << pIn->end_trade_dts.month << "-" << pIn->end_trade_dts.day
			 << " " << pIn->end_trade_dts.hour << ":"
			 << pIn->end_trade_dts.minute << ":" << pIn->end_trade_dts.second
			 << endl;
	}

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);
	commitTransaction();

	if (m_Verbose) {
		cout << m_pid << " - Trade Lookup Frame 2 (output)" << endl
			 << m_pid << " -- num_found: " << pOut->num_found << endl;
		for (int i = 0; i < pOut->num_found; i++) {
			cout << m_pid << " -- bid_price[" << i
				 << "]: " << pOut->trade_info[i].bid_price << endl
				 << m_pid << " -- exec_name[" << i
				 << "]: " << pOut->trade_info[i].exec_name << endl
				 << m_pid << " -- is_cash[" << i
				 << "]: " << pOut->trade_info[i].is_cash << endl
				 << m_pid << " -- trade_price[" << i
				 << "]: " << pOut->trade_info[i].trade_price << endl
				 << m_pid << " -- trade_id[" << i
				 << "]: " << pOut->trade_info[i].trade_id << endl
				 << m_pid << " -- settlement_amount[" << i
				 << "]: " << pOut->trade_info[i].settlement_amount << endl
				 << m_pid << " -- settlement_cash_due_date[" << i
				 << "]: " << pOut->trade_info[i].settlement_cash_due_date.year
				 << "-" << pOut->trade_info[i].settlement_cash_due_date.month
				 << "-" << pOut->trade_info[i].settlement_cash_due_date.day
				 << " " << pOut->trade_info[i].settlement_cash_due_date.hour
				 << ":" << pOut->trade_info[i].settlement_cash_due_date.minute
				 << ":" << pOut->trade_info[i].settlement_cash_due_date.second
				 << endl
				 << m_pid << " -- settlement_cash_type[" << i
				 << "]: " << pOut->trade_info[i].settlement_cash_type << endl
				 << m_pid << " -- cash_transaction_amount[" << i
				 << "]: " << pOut->trade_info[i].cash_transaction_amount
				 << endl
				 << m_pid << " -- cash_transaction_dts[" << i
				 << "]: " << pOut->trade_info[i].cash_transaction_dts.year
				 << "-" << pOut->trade_info[i].cash_transaction_dts.month
				 << "-" << pOut->trade_info[i].cash_transaction_dts.day << " "
				 << pOut->trade_info[i].cash_transaction_dts.hour << ":"
				 << pOut->trade_info[i].cash_transaction_dts.minute << ":"
				 << pOut->trade_info[i].cash_transaction_dts.second << endl
				 << m_pid << " -- cash_transaction_name[" << i
				 << "]: " << pOut->trade_info[i].cash_transaction_name << endl
				 << m_pid << " -- trade_history_dts[" << i
				 << "][0]: " << pOut->trade_info[i].trade_history_dts[0].year
				 << "-" << pOut->trade_info[i].trade_history_dts[0].month
				 << "-" << pOut->trade_info[i].trade_history_dts[0].day << " "
				 << pOut->trade_info[i].trade_history_dts[0].hour << ":"
				 << pOut->trade_info[i].trade_history_dts[0].minute << ":"
				 << pOut->trade_info[i].trade_history_dts[0].second << endl
				 << m_pid << " -- trade_history_status_id[" << i
				 << "][0]: " << pOut->trade_info[i].trade_history_status_id[0]
				 << endl
				 << m_pid << " -- trade_history_dts[" << i
				 << "][1]: " << pOut->trade_info[i].trade_history_dts[1].year
				 << "-" << pOut->trade_info[i].trade_history_dts[1].month
				 << "-" << pOut->trade_info[i].trade_history_dts[1].day << " "
				 << pOut->trade_info[i].trade_history_dts[1].hour << ":"
				 << pOut->trade_info[i].trade_history_dts[1].minute << ":"
				 << pOut->trade_info[i].trade_history_dts[1].second << endl
				 << m_pid << " -- trade_history_status_id[" << i
				 << "][1]: " << pOut->trade_info[i].trade_history_status_id[1]
				 << endl
				 << m_pid << " -- trade_history_dts[" << i
				 << "][2]: " << pOut->trade_info[i].trade_history_dts[2].year
				 << "-" << pOut->trade_info[i].trade_history_dts[2].month
				 << "-" << pOut->trade_info[i].trade_history_dts[2].day << " "
				 << pOut->trade_info[i].trade_history_dts[2].hour << ":"
				 << pOut->trade_info[i].trade_history_dts[2].minute << ":"
				 << pOut->trade_info[i].trade_history_dts[2].second << endl
				 << m_pid << " -- trade_history_status_id[" << i
				 << "][2]: " << pOut->trade_info[i].trade_history_status_id[2]
				 << endl;
		}
		cout << ">>> TLF2" << endl;
	}
}

// Call Trade Lookup Frame 3
void
CTradeLookupDB::DoTradeLookupFrame3(
		const TTradeLookupFrame3Input *pIn, TTradeLookupFrame3Output *pOut)
{
	if (m_Verbose) {
		cout << m_pid << " <<< TLF3" << endl
			 << m_pid << " - Trade Lookup Frame 3 (input)" << endl
			 << m_pid << " -- trade_dts: " << pIn->end_trade_dts.year << "-"
			 << pIn->end_trade_dts.month << "-" << pIn->end_trade_dts.day
			 << " " << pIn->end_trade_dts.hour << ":"
			 << pIn->end_trade_dts.minute << ":" << pIn->end_trade_dts.second
			 << endl
			 << m_pid << " -- max_acct_id: " << pIn->max_acct_id << endl
			 << m_pid << " -- max_trades: " << pIn->max_trades << endl
			 << m_pid << " -- trade_dts: " << pIn->start_trade_dts.year << "-"
			 << pIn->start_trade_dts.month << "-" << pIn->start_trade_dts.day
			 << " " << pIn->start_trade_dts.hour << ":"
			 << pIn->start_trade_dts.minute << ":"
			 << pIn->start_trade_dts.second << endl
			 << m_pid << " -- symbol: " << pIn->symbol << endl;
	}

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);
	commitTransaction();

	if (m_Verbose) {
		cout << m_pid << " - Trade Lookup Frame 3 (output)" << endl
			 << m_pid << " -- num_found: " << pOut->num_found << endl;
		for (int i = 0; i < pOut->num_found; i++) {
			cout << m_pid << " -- acct_id[" << i << ": "
				 << pOut->trade_info[i].acct_id << endl
				 << m_pid << " -- cash_transaction_amount[" << i
				 << "]: " << pOut->trade_info[i].cash_transaction_amount
				 << endl
				 << m_pid << " -- cash_transaction_dts[" << i
				 << "]: " << pOut->trade_info[i].cash_transaction_dts.year
				 << "-" << pOut->trade_info[i].cash_transaction_dts.month
				 << "-" << pOut->trade_info[i].cash_transaction_dts.day << " "
				 << pOut->trade_info[i].cash_transaction_dts.hour << ":"
				 << pOut->trade_info[i].cash_transaction_dts.minute << ":"
				 << pOut->trade_info[i].cash_transaction_dts.second << endl
				 << m_pid << " -- cash_transaction_name[" << i
				 << "]: " << pOut->trade_info[i].cash_transaction_name << endl
				 << m_pid << " -- exec_name[" << i
				 << "]: " << pOut->trade_info[i].exec_name << endl
				 << m_pid << " -- is_cash[" << i
				 << "]: " << pOut->trade_info[i].is_cash << endl
				 << m_pid << " -- price[" << i
				 << "]: " << pOut->trade_info[i].price << endl
				 << m_pid << " -- quantity[" << i
				 << "]: " << pOut->trade_info[i].quantity << endl
				 << m_pid << " -- settlement_amount[" << i
				 << "]: " << pOut->trade_info[i].settlement_amount << endl
				 << m_pid << " -- settlement_cash_due_date[" << i
				 << "]: " << pOut->trade_info[i].settlement_cash_due_date.year
				 << "-" << pOut->trade_info[i].settlement_cash_due_date.month
				 << "-" << pOut->trade_info[i].settlement_cash_due_date.day
				 << " " << pOut->trade_info[i].settlement_cash_due_date.hour
				 << ":" << pOut->trade_info[i].settlement_cash_due_date.minute
				 << ":" << pOut->trade_info[i].settlement_cash_due_date.second
				 << endl
				 << m_pid << " -- settlement_cash_type[" << i
				 << "]: " << pOut->trade_info[i].settlement_cash_type << endl
				 << m_pid << " -- cash_transaction_dts[" << i
				 << "]: " << pOut->trade_info[i].trade_dts.year << "-"
				 << pOut->trade_info[i].trade_dts.month << "-"
				 << pOut->trade_info[i].trade_dts.day << " "
				 << pOut->trade_info[i].trade_dts.hour << ":"
				 << pOut->trade_info[i].trade_dts.minute << ":"
				 << pOut->trade_info[i].trade_dts.second << endl
				 << m_pid << " -- trade_history_dts[" << i
				 << "][i]: " << pOut->trade_info[i].trade_history_dts[i].year
				 << "-" << pOut->trade_info[i].trade_history_dts[i].month
				 << "-" << pOut->trade_info[i].trade_history_dts[i].day << " "
				 << pOut->trade_info[i].trade_history_dts[i].hour << ":"
				 << pOut->trade_info[i].trade_history_dts[i].minute << ":"
				 << pOut->trade_info[i].trade_history_dts[i].second << endl
				 << m_pid << " -- trade_history_status_id[" << i
				 << "][i]: " << pOut->trade_info[i].trade_history_status_id[i]
				 << endl
				 << m_pid << " -- trade_history_dts[" << i
				 << "][1]: " << pOut->trade_info[i].trade_history_dts[1].year
				 << "-" << pOut->trade_info[i].trade_history_dts[1].month
				 << "-" << pOut->trade_info[i].trade_history_dts[1].day << " "
				 << pOut->trade_info[i].trade_history_dts[1].hour << ":"
				 << pOut->trade_info[i].trade_history_dts[1].minute << ":"
				 << pOut->trade_info[i].trade_history_dts[1].second << endl
				 << m_pid << " -- trade_history_status_id[" << i
				 << "][1]: " << pOut->trade_info[i].trade_history_status_id[1]
				 << endl
				 << m_pid << " -- trade_history_dts[" << i
				 << "][2]: " << pOut->trade_info[i].trade_history_dts[2].year
				 << "-" << pOut->trade_info[i].trade_history_dts[2].month
				 << "-" << pOut->trade_info[i].trade_history_dts[2].day << " "
				 << pOut->trade_info[i].trade_history_dts[2].hour << ":"
				 << pOut->trade_info[i].trade_history_dts[2].minute << ":"
				 << pOut->trade_info[i].trade_history_dts[2].second << endl
				 << m_pid << " -- trade_history_status_id[" << i
				 << "][2]: " << pOut->trade_info[i].trade_history_status_id[2]
				 << endl
				 << m_pid << " -- trade_id[" << i
				 << "]: " << pOut->trade_info[i].trade_id << endl
				 << m_pid << " -- trade_list[" << i
				 << "]: " << pOut->trade_info[i].trade_id << endl
				 << m_pid << " -- trade_type[" << i
				 << "]: " << pOut->trade_info[i].trade_type << endl;
		}
		cout << ">>> TLF3" << endl;
	}
}

// Call Trade Lookup Frame 4
void
CTradeLookupDB::DoTradeLookupFrame4(
		const TTradeLookupFrame4Input *pIn, TTradeLookupFrame4Output *pOut)
{
	if (m_Verbose) {
		cout << m_pid << " <<< TLF4" << endl
			 << m_pid << " - Trade Lookup Frame 4 (input)" << endl
			 << m_pid << " -- acct_id: " << pIn->acct_id << endl
			 << m_pid << " -- start_trade_dts: " << pIn->trade_dts.year << "-"
			 << pIn->trade_dts.month << "-" << pIn->trade_dts.day << " "
			 << pIn->trade_dts.hour << ":" << pIn->trade_dts.minute << ":"
			 << pIn->trade_dts.second << endl;
	}

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);
	commitTransaction();

	if (m_Verbose) {
		cout << m_pid << " - Trade Lookup Frame 4 (output)" << endl
			 << m_pid << " -- num_found: " << pOut->num_found << endl
			 << m_pid << " -- num_trades_found: " << pOut->num_trades_found
			 << endl;
		for (int i = 0; i < pOut->num_found; i++) {
			cout << m_pid << " -- holding_history_id[" << i
				 << "]: " << pOut->trade_info[i].holding_history_id << endl
				 << m_pid << " -- holding_history_trade_id[" << i
				 << "]: " << pOut->trade_info[i].holding_history_trade_id
				 << endl
				 << m_pid << " -- quantity_before[" << i
				 << "]: " << pOut->trade_info[i].quantity_before << endl
				 << m_pid << " -- quantity_after[" << i
				 << "]: " << pOut->trade_info[i].quantity_after << endl;
		}
		cout << m_pid << " -- trade_id: " << pOut->trade_id << endl
			 << m_pid << " >>> TLF4" << endl;
	}
}
