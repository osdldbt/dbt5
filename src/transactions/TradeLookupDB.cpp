/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 08 July 2006
 */

#include "TradeLookupDB.h"

// Call Trade Lookup Frame 1
void CTradeLookupDB::DoTradeLookupFrame1(const TTradeLookupFrame1Input *pIn,
		TTradeLookupFrame1Output *pOut)
{
#ifdef DEBUG
	pthread_t pid = pthread_self();
	int i;
	cout << pid << " <<< TLF1" << endl;
	cout << pid << " - Trade Lookup Frame 1 (input)" << endl <<
			pid << " -- max_trades: " << pIn->max_trades << endl;
	for (i = 0; i < pIn->max_trades; i++)
		cout << pid << " -- trade_id[" << i << "]: " << pIn->trade_id[i] << endl;
#endif // DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	cout << pid << " - Trade Lookup Frame 1 (output)"<< endl <<
			pid << " -- num_found: " << pOut->num_found << endl;
	for (i = 0; i < pOut->num_found; i++) {
		cout << pid << " -- bid_price[" << i << "]: " <<
						pOut->trade_info[i].bid_price << endl <<
				pid << " -- exec_name[" << i << "]: " <<
						pOut->trade_info[i].exec_name << endl <<
				pid << " -- is_cash[" << i << "]: " <<
						pOut->trade_info[i].is_cash << endl <<
				pid << " -- is_market[" << i << "]: " <<
						pOut->trade_info[i].is_market << endl <<
				pid << " -- trade_price[" << i << "]: " <<
						pOut->trade_info[i].trade_price << endl <<
				pid << " -- settlement_amount[" << i << "]: " <<
						pOut->trade_info[i].settlement_amount << endl <<
				pid << " -- settlement_cash_due_date[" << i << "]: " <<
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
				pid << " -- settlement_cash_type[" << i << "]: " <<
						pOut->trade_info[i].settlement_cash_type << endl <<
				pid << " -- cash_transaction_amount[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_amount << endl <<
				pid << " -- cash_transaction_dts[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_dts.year << "-" <<
						pOut->trade_info[i].cash_transaction_dts.month << "-" <<
						pOut->trade_info[i].cash_transaction_dts.day << " "<<
						pOut->trade_info[i].cash_transaction_dts.hour << ":" <<
						pOut->trade_info[i].cash_transaction_dts.minute <<
						":" <<
						pOut->trade_info[i].cash_transaction_dts.second <<
						endl <<
				pid << " -- cash_transaction_name[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_name << endl <<
				pid << " -- trade_history_dts[" << i << "][0]: " <<
						pOut->trade_info[i].trade_history_dts[0].year << "-" <<
						pOut->trade_info[i].trade_history_dts[0].month << "-" <<
						pOut->trade_info[i].trade_history_dts[0].day << " " <<
						pOut->trade_info[i].trade_history_dts[0].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[0].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[0].second <<
						endl <<
				pid << " -- trade_history_status_id[" << i << "][0]: " <<
						pOut->trade_info[i].trade_history_status_id[0] <<
						endl <<
				pid << " -- trade_history_dts[" << i << "][1]: " <<
						pOut->trade_info[i].trade_history_dts[1].year << "-" <<
						pOut->trade_info[i].trade_history_dts[1].month << "-" <<
						pOut->trade_info[i].trade_history_dts[1].day << " " <<
						pOut->trade_info[i].trade_history_dts[1].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[1].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[1].second <<
						endl <<
				pid << " -- trade_history_status_id[" << i << "][1]: " <<
						pOut->trade_info[i].trade_history_status_id[1] <<
						endl <<
				pid << " -- trade_history_dts[" << i << "][2]: " <<
						pOut->trade_info[i].trade_history_dts[2].year << "-" <<
						pOut->trade_info[i].trade_history_dts[2].month << "-" <<
						pOut->trade_info[i].trade_history_dts[2].day << " " <<
						pOut->trade_info[i].trade_history_dts[2].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[2].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[2].second <<
						endl <<
				pid << " -- trade_history_status_id[" << i << "][2]: " <<
						pOut->trade_info[i].trade_history_status_id[2] << endl;
	}
	cout << pid << " >>> TLF1" << endl;
#endif // DEBUG
}

// Call Trade Lookup Frame 2
void CTradeLookupDB::DoTradeLookupFrame2(const TTradeLookupFrame2Input *pIn,
		TTradeLookupFrame2Output *pOut)
{
#ifdef DEBUG
	pthread_t pid = pthread_self();
	cout << pid << " <<< TLF2" << endl;
	cout << pid << " - Trade Lookup Frame 2 (input)" << endl <<
			pid << " -- acct_id: " << pIn->acct_id << endl <<
			pid << " -- max_trades: " << pIn->max_trades << endl <<
			pid << " -- trade_dts: " << pIn->end_trade_dts.year << "-" <<
					pIn->end_trade_dts.month << "-" <<
					pIn->end_trade_dts.day << " " <<
					pIn->end_trade_dts.hour << ":" <<
					pIn->end_trade_dts.minute << ":" <<
					pIn->end_trade_dts.second << endl;
#endif // DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	cout << pid << " - Trade Lookup Frame 2 (output)" << endl <<
			pid << " -- num_found: " << pOut->num_found << endl;
	for (int i = 0; i < pOut->num_found; i++) {
		cout << pid << " -- bid_price[" << i << "]: " <<
						pOut->trade_info[i].bid_price << endl <<
				pid << " -- exec_name[" << i << "]: " <<
						pOut->trade_info[i].exec_name << endl <<
				pid << " -- is_cash[" << i << "]: " <<
						pOut->trade_info[i].is_cash << endl <<
				pid << " -- trade_price[" << i << "]: " <<
						pOut->trade_info[i].trade_price << endl <<
				pid << " -- trade_id[" << i << "]: " <<
						pOut->trade_info[i].trade_id << endl <<
				pid << " -- settlement_amount[" << i << "]: " <<
						pOut->trade_info[i].settlement_amount << endl <<
				pid << " -- settlement_cash_due_date[" << i << "]: " <<
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
				pid << " -- settlement_cash_type[" << i << "]: " <<
						pOut->trade_info[i].settlement_cash_type << endl <<
				pid << " -- cash_transaction_amount[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_amount << endl <<
				pid << " -- cash_transaction_dts[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_dts.year << "-" <<
						pOut->trade_info[i].cash_transaction_dts.month << "-" <<
						pOut->trade_info[i].cash_transaction_dts.day << " " <<
						pOut->trade_info[i].cash_transaction_dts.hour << ":" <<
						pOut->trade_info[i].cash_transaction_dts.minute <<
						":" <<
						pOut->trade_info[i].cash_transaction_dts.second <<
						endl <<
				pid << " -- cash_transaction_name[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_name << endl <<
				pid << " -- trade_history_dts[" << i << "][0]: " <<
						pOut->trade_info[i].trade_history_dts[0].year << "-" <<
						pOut->trade_info[i].trade_history_dts[0].month << "-" <<
						pOut->trade_info[i].trade_history_dts[0].day << " " <<
						pOut->trade_info[i].trade_history_dts[0].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[0].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[0].second <<
						endl <<
				pid << " -- trade_history_status_id[" << i << "][0]: " <<
						pOut->trade_info[i].trade_history_status_id[0] <<
						endl <<
				pid << " -- trade_history_dts[" << i << "][1]: " <<
						pOut->trade_info[i].trade_history_dts[1].year << "-" <<
						pOut->trade_info[i].trade_history_dts[1].month << "-" <<
						pOut->trade_info[i].trade_history_dts[1].day << " " <<
						pOut->trade_info[i].trade_history_dts[1].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[1].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[1].second <<
						endl <<
				pid << " -- trade_history_status_id[" << i << "][1]: " <<
						pOut->trade_info[i].trade_history_status_id[1] <<
						endl <<
				pid << " -- trade_history_dts[" << i << "][2]: " <<
						pOut->trade_info[i].trade_history_dts[2].year << "-" <<
						pOut->trade_info[i].trade_history_dts[2].month << "-" <<
						pOut->trade_info[i].trade_history_dts[2].day << " " <<
						pOut->trade_info[i].trade_history_dts[2].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[2].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[2].second <<
						endl <<
				pid << " -- trade_history_status_id[" << i << "][2]: " <<
						pOut->trade_info[i].trade_history_status_id[2] << endl;
	}
	cout << ">>> TLF2" << endl;
#endif // DEBUG
}

// Call Trade Lookup Frame 3
void CTradeLookupDB::DoTradeLookupFrame3(const TTradeLookupFrame3Input *pIn,
		TTradeLookupFrame3Output *pOut)
{
#ifdef DEBUG
	pthread_t pid = pthread_self();
	cout << pid << " <<< TLF3" << endl;
	cout << pid << " - Trade Lookup Frame 3 (input)" << endl <<
			pid << " -- max_acct_id: " << pIn->max_acct_id << endl <<
			pid << " -- max_trades: " << pIn->max_trades << endl <<
			pid << " -- trade_dts: " << pIn->end_trade_dts.year << "-" <<
					pIn->end_trade_dts.month << "-" <<
					pIn->end_trade_dts.day << " " <<
					pIn->end_trade_dts.hour << ":" <<
					pIn->end_trade_dts.minute << ":" <<
					pIn->end_trade_dts.second << endl <<
			pid << " -- symbol: " << pIn->symbol << endl;
#endif // DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	cout << pid << " - Trade Lookup Frame 3 (output)" << endl <<
			pid << " -- num_found: " << pOut->num_found << endl;
	for (int i = 0; i < pOut->num_found; i++) {
		cout << pid << " -- acct_id[" << i << ": " <<
						pOut->trade_info[i].acct_id << endl <<
				pid << " -- cash_transaction_amount[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_amount << endl <<
				pid << " -- cash_transaction_dts[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_dts.year << "-" <<
						pOut->trade_info[i].cash_transaction_dts.month << "-" <<
						pOut->trade_info[i].cash_transaction_dts.day << " " <<
						pOut->trade_info[i].cash_transaction_dts.hour << ":" <<
						pOut->trade_info[i].cash_transaction_dts.minute <<
						":" <<
						pOut->trade_info[i].cash_transaction_dts.second <<
						endl <<
				pid << " -- cash_transaction_name[" << i << "]: " <<
						pOut->trade_info[i].cash_transaction_name << endl <<
				pid << " -- exec_name[" << i << "]: " <<
						pOut->trade_info[i].exec_name << endl <<
				pid << " -- is_cash[" << i << "]: " <<
						pOut->trade_info[i].is_cash << endl <<
				pid << " -- price[" << i << "]: " <<
						pOut->trade_info[i].price << endl <<
				pid << " -- quantity[" << i << "]: " <<
						pOut->trade_info[i].quantity << endl <<
				pid << " -- settlement_amount[" << i << "]: " <<
						pOut->trade_info[i].settlement_amount << endl <<
				pid << " -- settlement_cash_due_date[" << i << "]: " <<
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
				pid << " -- settlement_cash_type[" << i << "]: " <<
						pOut->trade_info[i].settlement_cash_type << endl <<
				pid << " -- cash_transaction_dts[" << i << "]: " <<
						pOut->trade_info[i].trade_dts.year << "-" <<
						pOut->trade_info[i].trade_dts.month << "-" <<
						pOut->trade_info[i].trade_dts.day << " "<<
						pOut->trade_info[i].trade_dts.hour << ":" <<
						pOut->trade_info[i].trade_dts.minute << ":" <<
						pOut->trade_info[i].trade_dts.second << endl <<
				pid << " -- trade_history_dts[" << i << "][i]: " <<
						pOut->trade_info[i].trade_history_dts[i].year << "-" <<
						pOut->trade_info[i].trade_history_dts[i].month << "-" <<
						pOut->trade_info[i].trade_history_dts[i].day << " " <<
						pOut->trade_info[i].trade_history_dts[i].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[i].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[i].second <<
						endl <<
				pid << " -- trade_history_status_id[" << i << "][i]: " <<
						pOut->trade_info[i].trade_history_status_id[i] <<
						endl <<
				pid << " -- trade_history_dts[" << i << "][1]: " <<
						pOut->trade_info[i].trade_history_dts[1].year << "-" <<
						pOut->trade_info[i].trade_history_dts[1].month << "-" <<
						pOut->trade_info[i].trade_history_dts[1].day << " " <<
						pOut->trade_info[i].trade_history_dts[1].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[1].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[1].second <<
						endl <<
				pid << " -- trade_history_status_id[" << i << "][1]: " <<
						pOut->trade_info[i].trade_history_status_id[1] <<
						endl <<
				pid << " -- trade_history_dts[" << i << "][2]: " <<
						pOut->trade_info[i].trade_history_dts[2].year << "-" <<
						pOut->trade_info[i].trade_history_dts[2].month << "-" <<
						pOut->trade_info[i].trade_history_dts[2].day << " " <<
						pOut->trade_info[i].trade_history_dts[2].hour << ":" <<
						pOut->trade_info[i].trade_history_dts[2].minute <<
						":" <<
						pOut->trade_info[i].trade_history_dts[2].second <<
						endl <<
				pid << " -- trade_history_status_id[" << i << "][2]: " <<
						pOut->trade_info[i].trade_history_status_id[2] <<
						endl <<
				pid << " -- trade_id[" << i << "]: " <<
						pOut->trade_info[i].trade_id << endl <<
				pid << " -- trade_list[" << i << "]: " <<
						pOut->trade_info[i].trade_id << endl <<
				pid << " -- trade_type[" << i << "]: " <<
						pOut->trade_info[i].trade_type << endl;
	}
	cout << ">>> TLF3" << endl;
#endif //DEBUG
}

// Call Trade Lookup Frame 4
void CTradeLookupDB::DoTradeLookupFrame4(const TTradeLookupFrame4Input *pIn,
		TTradeLookupFrame4Output *pOut)
{
#ifdef DEBUG
	pthread_t pid = pthread_self();
	cout << pid << " <<< TLF4" << endl;
	cout << pid << " - Trade Lookup Frame 4 (input)" << endl <<
			pid << " -- acct_id: " << pIn->acct_id << endl <<
			pid << " -- trade_dts: " << pIn->trade_dts.year << "-" <<
					pIn->trade_dts.month << "-" << pIn->trade_dts.day <<
					" " << pIn->trade_dts.hour << ":" <<
					pIn->trade_dts.minute << ":" <<
					pIn->trade_dts.second << endl;
#endif //DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	cout << pid << " - Trade Lookup Frame 4 (output)" << endl <<
			pid << " -- holding_history_id[0]: " <<
					pOut->trade_info[0].holding_history_id << endl <<
			pid << " -- holding_history_trade_id[0]: " <<
					pOut->trade_info[0].holding_history_trade_id << endl <<
			pid << " -- quantity_before[0]: " <<
					pOut->trade_info[0].quantity_before << endl <<
			pid << " -- quantity_after[0]: " <<
					pOut->trade_info[0].quantity_after << endl <<
			pid << " -- num_found: " << pOut->num_found << endl <<
			pid << " -- trade_id: " << pOut->trade_id << endl;
	cout << pid << " >>> TLF4" << endl;
#endif //DEBUG
}
