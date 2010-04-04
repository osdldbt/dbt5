/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * 08 July 2006
 */

#include "TradeLookupDB.h"

// Call Trade Lookup Frame 1
void CTradeLookupDB::DoTradeLookupFrame1(const TTradeLookupFrame1Input *pIn,
		TTradeLookupFrame1Output *pOut)
{
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TLF1" << endl;
	cout << "- Trade Lookup Frame 1 (input)" << endl <<
			"-- max_trades: " << pIn->max_trades << endl <<
			"-- Trades: {" << osTrades.str() << "}" << endl;
	m_coutLock.unlock();
#endif // DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Lookup Frame 1 (output)"<< endl <<
			"-- num_found: " << pOut->num_found << endl;
	for (i = 0; i < pOut->num_found; i++) {
		cout << "-- bid_price[" << i << "]: " <<
						pOut->trade_info[i].bid_price << endl <<
				"-- exec_name[" << i << "]: " <<
						pOut->trade_info[i].exec_name << endl <<
				"-- is_cash[" << i << "]: " <<
						pOut->trade_info[i].is_cash << endl <<
				"-- is_market[" << i << "]: " <<
						pOut->trade_info[i].is_market << endl <<
				"-- trade_price[" << i << "]: " <<
						pOut->trade_info[i].trade_price << endl <<
				"-- settlement_amount[" << i << "]: " <<
						pOut->trade_info[i].settlement_amount << endl <<
				"-- settlement_cash_due_date[" << i << "]: " <<
						pOut->trade_info[i].settlement_cash_due_date.year <<
						"-" <<
						pOut->trade_info[i].settlement_cash_due_date.month <<
						"-" <<
						pOut->trade_info[i].settlement_cash_due_date.day <<
						" " <<
						pOut->trade_info[i].settlement_cash_due_date.hour <<
						":" <<
						pOut->trade_info[i].settlement_cash_due_date.minute <<
						":" <<
						pOut->trade_info[i].settlement_cash_due_date.second <<
						endl <<
				"-- settlement_cash_type[" << i << "]: " <<
						pOut->trade_info[i].settlement_cash_type << endl <<
				"-- cash_transaction_amount[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_amount << endl <<
				"-- cash_transaction_dts[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_dts.year << "-" <<
						pOut->trade_info[i].cash_transaction_dts.month << "-" <<
						pOut->trade_info[i].cash_transaction_dts.day << " "<<
						pOut->trade_info[i].cash_transaction_dts.hour << ":" <<
						pOut->trade_info[i].cash_transaction_dts.minute <<
						":" <<
						pOut->trade_info[i].cash_transaction_dts.second <<
						endl <<
				"-- cash_transaction_name[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_name << endl <<
				"-- trade_history_dts[" << i << "][0]: " <<
						pOut->trade_info[i].trade_history_dts[0].year << "-" <<
						pOut->trade_info[i].trade_history_dts[0].month << "-" <<
						pOut->trade_info[i].trade_history_dts[0].day << " " <<
						pOut->trade_info[i].trade_history_dts[0].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[0].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[0].second <<
						endl <<
				"-- trade_history_status_id[" << i << "][0]: " <<
						pOut->trade_info[i].trade_history_status_id[0] <<
						endl <<
				"-- trade_history_dts[" << i << "][1]: " <<
						pOut->trade_info[i].trade_history_dts[1].year << "-" <<
						pOut->trade_info[i].trade_history_dts[1].month << "-" <<
						pOut->trade_info[i].trade_history_dts[1].day << " " <<
						pOut->trade_info[i].trade_history_dts[1].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[1].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[1].second <<
						endl <<
				"-- trade_history_status_id[" << i << "][1]: " <<
						pOut->trade_info[i].trade_history_status_id[1] <<
						endl <<
				"-- trade_history_dts[" << i << "][2]: " <<
						pOut->trade_info[i].trade_history_dts[2].year << "-" <<
						pOut->trade_info[i].trade_history_dts[2].month << "-" <<
						pOut->trade_info[i].trade_history_dts[2].day << " " <<
						pOut->trade_info[i].trade_history_dts[2].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[2].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[2].second <<
						endl <<
				"-- trade_history_status_id[" << i << "][2]: " <<
						pOut->trade_info[i].trade_history_status_id[2] << endl;
	}
	cout << ">>> TLF1" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}

// Call Trade Lookup Frame 2
void CTradeLookupDB::DoTradeLookupFrame2(const TTradeLookupFrame2Input *pIn,
		TTradeLookupFrame2Output *pOut)
{
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TLF2" << endl;
	cout << "- Trade Lookup Frame 2 (input)" << endl <<
			"-- acct_id: " << pIn->acct_id << endl <<
			"-- max_trades: " << pIn->max_trades << endl <<
			"-- trade_dts: " << pIn->end_trade_dts.year << "-" <<
					pIn->end_trade_dts.month << "-" <<
					pIn->end_trade_dts.day << " " <<
					pIn->end_trade_dts.hour << ":" <<
					pIn->end_trade_dts.minute << ":" <<
					pIn->end_trade_dts.second << endl;
	m_coutLock.unlock();
#endif // DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Lookup Frame 2 (output)" << endl <<
			"-- num_found: " << pOut->num_found << endl;
	for (int i = 0; i < pOut->num_found; i++) {
		cout << "-- bid_price[" << i << "]: " <<
						pOut->trade_info[i].bid_price << endl <<
				"-- exec_name[" << i << "]: " <<
						pOut->trade_info[i].exec_name << endl <<
				"-- is_cash[" << i << "]: " <<
						pOut->trade_info[i].is_cash << endl <<
				"-- trade_price[" << i << "]: " <<
						pOut->trade_info[i].trade_price << endl <<
				"-- trade_id[" << i << "]: " <<
						pOut->trade_info[i].trade_id << endl <<
				"-- settlement_amount[" << i << "]: " <<
						pOut->trade_info[i].settlement_amount << endl <<
				"-- settlement_cash_due_date[" << i << "]: " <<
						pOut->trade_info[i].settlement_cash_due_date.year <<
						"-" <<
						pOut->trade_info[i].settlement_cash_due_date.month <<
						"-" <<
						pOut->trade_info[i].settlement_cash_due_date.day <<
						" " <<
						pOut->trade_info[i].settlement_cash_due_date.hour <<
						":" <<
						pOut->trade_info[i].settlement_cash_due_date.minute <<
						":" <<
						pOut->trade_info[i].settlement_cash_due_date.second <<
						endl <<
				"-- settlement_cash_type[" << i << "]: " <<
						pOut->trade_info[i].settlement_cash_type << endl <<
				"-- cash_transaction_amount[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_amount << endl <<
				"-- cash_transaction_dts[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_dts.year << "-" <<
						pOut->trade_info[i].cash_transaction_dts.month << "-" <<
						pOut->trade_info[i].cash_transaction_dts.day << " " <<
						pOut->trade_info[i].cash_transaction_dts.hour << ":" <<
						pOut->trade_info[i].cash_transaction_dts.minute <<
						":" <<
						pOut->trade_info[i].cash_transaction_dts.second <<
						endl <<
				"-- cash_transaction_name[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_name << endl <<
				"-- trade_history_dts[" << i << "][0]: " <<
						pOut->trade_info[i].trade_history_dts[0].year << "-" <<
						pOut->trade_info[i].trade_history_dts[0].month << "-" <<
						pOut->trade_info[i].trade_history_dts[0].day << " " <<
						pOut->trade_info[i].trade_history_dts[0].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[0].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[0].second <<
						endl <<
				"-- trade_history_status_id[" << i << "][0]: " <<
						pOut->trade_info[i].trade_history_status_id[0] <<
						endl <<
				"-- trade_history_dts[" << i << "][1]: " <<
						pOut->trade_info[i].trade_history_dts[1].year << "-" <<
						pOut->trade_info[i].trade_history_dts[1].month << "-" <<
						pOut->trade_info[i].trade_history_dts[1].day << " " <<
						pOut->trade_info[i].trade_history_dts[1].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[1].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[1].second <<
						endl <<
				"-- trade_history_status_id[" << i << "][1]: " <<
						pOut->trade_info[i].trade_history_status_id[1] <<
						endl <<
				"-- trade_history_dts[" << i << "][2]: " <<
						pOut->trade_info[i].trade_history_dts[2].year << "-" <<
						pOut->trade_info[i].trade_history_dts[2].month << "-" <<
						pOut->trade_info[i].trade_history_dts[2].day << " " <<
						pOut->trade_info[i].trade_history_dts[2].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[2].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[2].second <<
						endl <<
				"-- trade_history_status_id[" << i << "][2]: " <<
						pOut->trade_info[i].trade_history_status_id[2] << endl;
	}
	cout << ">>> TLF2" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}

// Call Trade Lookup Frame 3
void CTradeLookupDB::DoTradeLookupFrame3(const TTradeLookupFrame3Input *pIn,
		TTradeLookupFrame3Output *pOut)
{
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TLF3" << endl;
	cout << "- Trade Lookup Frame 3 (input)" << endl <<
			"-- max_acct_id: " << pIn->max_acct_id << endl <<
			"-- max_trades: " << pIn->max_trades << endl <<
			"-- trade_dts: " << pIn->end_trade_dts.year << "-" <<
					pIn->end_trade_dts.month << "-" <<
					pIn->end_trade_dts.day << " " <<
					pIn->end_trade_dts.hour << ":" <<
					pIn->end_trade_dts.minute << ":" <<
					pIn->end_trade_dts.second << endl <<
			"-- symbol: " << pIn->symbol << endl;
	m_coutLock.unlock();
#endif // DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Lookup Frame 3 (output)" << endl <<
			"-- num_found: " << pOut->num_found << endl;
	for (int i = 0; i < pOut->num_found; i++) {
		cout << "-- acct_id[" << i << ": " << pOut->trade_info[i].acct_id <<
						endl <<
				"-- cash_transaction_amount[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_amount << endl <<
				"-- cash_transaction_dts[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_dts.year << "-" <<
						pOut->trade_info[i].cash_transaction_dts.month << "-" <<
						pOut->trade_info[i].cash_transaction_dts.day << " " <<
						pOut->trade_info[i].cash_transaction_dts.hour << ":" <<
						pOut->trade_info[i].cash_transaction_dts.minute <<
						":" <<
						pOut->trade_info[i].cash_transaction_dts.second <<
						endl <<
				"-- cash_transaction_name[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_name << endl <<
				"-- exec_name[" << i << "]: " <<
						pOut->trade_info[i].exec_name << endl <<
				"-- is_cash[" << i << "]: " << pOut->trade_info[i].is_cash <<
						endl <<
				"-- price[" << i << "]: " << pOut->trade_info[i].price <<
						endl <<
				"-- quantity[" << i << "]: " << pOut->trade_info[i].quantity <<
						endl <<
				"-- settlement_amount[" << i << "]: " <<
						pOut->trade_info[i].settlement_amount << endl <<
				"-- settlement_cash_due_date[" << i << "]: " <<
						pOut->trade_info[i].settlement_cash_due_date.year <<
						"-" <<
						pOut->trade_info[i].settlement_cash_due_date.month <<
						"-" <<
						pOut->trade_info[i].settlement_cash_due_date.day <<
						" " <<
						pOut->trade_info[i].settlement_cash_due_date.hour <<
						":" <<
						pOut->trade_info[i].settlement_cash_due_date.minute <<
						":" <<
						pOut->trade_info[i].settlement_cash_due_date.second <<
						endl <<
				"-- settlement_cash_type[" << i << "]: " <<
						pOut->trade_info[i].settlement_cash_type << endl <<
				"-- cash_transaction_dts[" << i << "]: " <<
						pOut->trade_info[i].trade_dts.year << "-" <<
						pOut->trade_info[i].trade_dts.month << "-" <<
						pOut->trade_info[i].trade_dts.day << " "<<
						pOut->trade_info[i].trade_dts.hour << ":" <<
						pOut->trade_info[i].trade_dts.minute << ":" <<
						pOut->trade_info[i].trade_dts.second << endl <<
				"-- trade_history_dts[" << i << "][i]: " <<
						pOut->trade_info[i].trade_history_dts[i].year << "-" <<
						pOut->trade_info[i].trade_history_dts[i].month << "-" <<
						pOut->trade_info[i].trade_history_dts[i].day << " " <<
						pOut->trade_info[i].trade_history_dts[i].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[i].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[i].second <<
						endl <<
				"-- trade_history_status_id[" << i << "][i]: " <<
						pOut->trade_info[i].trade_history_status_id[i] <<
						endl <<
				"-- trade_history_dts[" << i << "][1]: " <<
						pOut->trade_info[i].trade_history_dts[1].year << "-" <<
						pOut->trade_info[i].trade_history_dts[1].month << "-" <<
						pOut->trade_info[i].trade_history_dts[1].day << " " <<
						pOut->trade_info[i].trade_history_dts[1].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[1].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[1].second <<
						endl <<
				"-- trade_history_status_id[" << i << "][1]: " <<
						pOut->trade_info[i].trade_history_status_id[1] <<
						endl <<
				"-- trade_history_dts[" << i << "][2]: " <<
						pOut->trade_info[i].trade_history_dts[2].year << "-" <<
						pOut->trade_info[i].trade_history_dts[2].month << "-" <<
						pOut->trade_info[i].trade_history_dts[2].day << " " <<
						pOut->trade_info[i].trade_history_dts[2].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[2].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[2].second <<
						endl <<
				"-- trade_history_status_id[" << i << "][2]: " <<
						pOut->trade_info[i].trade_history_status_id[2] <<
						endl <<
				"-- trade_id[" << i << "]: " << pOut->trade_info[i].trade_id <<
						endl <<
				"-- trade_list[" << i << "]: " <<
						pOut->trade_info[i].trade_id << endl <<
				"-- trade_type[" << i << "]: " <<
						pOut->trade_info[i].trade_type << endl;
	}
	cout << ">>> TLF3" << endl;
	m_coutLock.unlock();
#endif //DEBUG
}

// Call Trade Lookup Frame 4
void CTradeLookupDB::DoTradeLookupFrame4(const TTradeLookupFrame4Input *pIn,
		TTradeLookupFrame4Output *pOut)
{
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TLF4" << endl;
	cout << "- Trade Lookup Frame 4 (input)" << endl <<
			"-- acct_id: " << pIn->acct_id << endl <<
			"-- trade_dts: " << pIn->trade_dts.year << "-" <<
					pIn->trade_dts.month << "-" << pIn->trade_dts.day <<
					" " << pIn->trade_dts.hour << ":" <<
					pIn->trade_dts.minute << ":" <<
					pIn->trade_dts.second << endl;
	m_coutLock.unlock();
#endif //DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Lookup Frame 4 (output)" << endl <<
			"-- holding_history_id[0]: " <<
					pOut->trade_info[0].holding_history_id << endl <<
			"-- holding_history_trade_id[0]: " <<
					pOut->trade_info[0].holding_history_trade_id << endl <<
			"-- quantity_before[0]: " <<
					pOut->trade_info[0].quantity_before << endl <<
			"-- quantity_after[0]: " <<
					pOut->trade_info[0].quantity_after << endl <<
			"-- num_found: " << pOut->num_found << endl <<
			"-- trade_id: " << pOut->trade_id << endl;
	cout << ">>> TLF4" << endl;
	m_coutLock.unlock();
#endif //DEBUG
}
