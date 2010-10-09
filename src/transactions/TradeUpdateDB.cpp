/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * 11 July 2006
 */

#include "TradeUpdateDB.h"

// Call Trade Lookup Frame 1
void CTradeUpdateDB::DoTradeUpdateFrame1(const TTradeUpdateFrame1Input *pIn,
		TTradeUpdateFrame1Output *pOut)
{
#ifdef DEBUG
	pthread_t pid = pthread_self();
	int i;
	cout << pid << " <<< TUF1" << endl;
	cout << pid << " - Trade Update Frame 1 (input)" << endl <<
			pid << " -- max_trades: " << pIn->max_trades << endl <<
			pid << " -- max_updates: " << pIn->max_updates << endl;
	for (int i = 0; i < pIn->max_updates; i++)
		cout << pid << " -- trade_id[" << i << "]: " << pIn->trade_id[i] <<
				endl;
#endif // DEBUG

	startTransaction();
	setRepeatableRead();
	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	cout << pid << " - Trade Update Frame 1 (output)" << endl <<
			pid << " -- num_found: " << pOut->num_found << endl <<
			pid << " -- num_updated: " << pOut->num_updated << endl;
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
				pid << " -- settlement_amount[" << i << "]: "<<
						pOut->trade_info[i].settlement_amount << endl <<
				pid << " -- settlement_cash_due_date[" << i << "]: " <<
						pOut->trade_info[i].settlement_cash_due_date.year <<
						"-" <<
						pOut->trade_info[i].settlement_cash_due_date.month <<
						"-" <<
						pOut->trade_info[i].settlement_cash_due_date.day <<
						" " <<
						pOut->trade_info[i].settlement_cash_due_date.hour <<
						":"<<
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
	cout << pid << " >>> TUF1" << endl;
#endif // DEBUG
}

// Call Trade Lookup Frame 2
void CTradeUpdateDB::DoTradeUpdateFrame2(const TTradeUpdateFrame2Input *pIn,
		TTradeUpdateFrame2Output *pOut)
{
#ifdef DEBUG
	pthread_t pid = pthread_self();
	cout << pid << " <<< TUF2" << endl;
	cout << pid << " - Trade Update Frame 2 (input)" << endl <<
			pid << " -- acct_id: " << pIn->acct_id << endl <<
			pid << " -- max_trades: " << pIn->max_trades << endl <<
			pid << " -- max_updates: " << pIn->max_updates << endl <<
			pid << " -- trade_dts: " << pIn->end_trade_dts.year << "-" <<
					pIn->end_trade_dts.month << "-" <<
					pIn->end_trade_dts.day << " " <<
					pIn->end_trade_dts.hour << ":" <<
					pIn->end_trade_dts.minute << ":" <<
					pIn->end_trade_dts.second << endl;
#endif // DEBUG

	startTransaction();
	setRepeatableRead();
	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	cout << pid << " -- Trade Update Frame 2 (output)" << endl <<
			pid << " -- num_found: " << pOut->num_found << endl <<
			pid << " -- num_updated: " << pOut->num_updated << endl;
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
						pOut->trade_info[i].cash_transaction_dts.minute <<":" <<
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
	cout << pid << " >>> TUF2" << endl;
#endif // DEBUG
}

// Call Trade Lookup Frame 3
void CTradeUpdateDB::DoTradeUpdateFrame3(const TTradeUpdateFrame3Input *pIn,
		TTradeUpdateFrame3Output *pOut)
{
#ifdef DEBUG
	pthread_t pid = pthread_self();
	cout << pid << " <<< TUF3" << endl;
	cout << pid << " - Trade Update Frame 3 (input)" << endl <<
			pid << " -- max_acct_id: " << pIn->max_acct_id << endl <<
			pid << " -- max_trades: " << pIn->max_trades << endl <<
			pid << " -- max_updates: " << pIn->max_updates << endl <<
			pid << " -- trade_dts: " << pIn->start_trade_dts.year << "-" <<
					pIn->start_trade_dts.month << "-" <<
					pIn->start_trade_dts.day << " " <<
					pIn->start_trade_dts.hour << ":" <<
					pIn->start_trade_dts.minute << ":" <<
					pIn->start_trade_dts.second << endl <<
			pid << " -- symbol: " << pIn->symbol << endl;
#endif //DEBUG

	startTransaction();
	setRepeatableRead();
	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	cout << pid << " - Trade Update Frame 3 (output)" << endl <<
			pid << " -- num_found: " << pOut->num_found << endl <<
			pid << " -- num_updated: " << pOut->num_updated << endl;
	for (int i = 0; i < pOut->num_found; i++) {
		cout << pid << " -- acct_id[" << i << "]: " <<
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
				pid << " -- trade_dts[" << i << "]: " <<
						pOut->trade_info[i].trade_dts.year << "-" <<
						pOut->trade_info[i].trade_dts.month << "-" <<
						pOut->trade_info[i].trade_dts.day << " " <<
						pOut->trade_info[i].trade_dts.hour << ":" <<
						pOut->trade_info[i].trade_dts.minute << ":" <<
						pOut->trade_info[i].trade_dts.second << endl <<
				pid << " -- cash_transaction_dts[" << i << "]: " <<
						pOut->trade_info[i].trade_dts.year << "-" <<
						pOut->trade_info[i].trade_dts.month << "-" <<
						pOut->trade_info[i].trade_dts.day << " " <<
						pOut->trade_info[i].trade_dts.hour << ":" <<
						pOut->trade_info[i].trade_dts.minute << ":" <<
						pOut->trade_info[i].trade_dts.second << endl <<
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
						pOut->trade_info[i].trade_history_status_id[2] <<
						endl <<
				pid << " -- trade_id[" << i << "]: " <<
						pOut->trade_info[i].trade_id << endl <<
				pid << " -- trade_type[" << i << "]: " <<
						pOut->trade_info[i].trade_type << endl;
	}
	cout << pid << " >>> TUF3" << endl;
#endif // DEBUG
}
