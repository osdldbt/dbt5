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
	m_coutLock.lock();
	cout << "<<< TUF1" << endl;
	cout << "- Trade Update Frame 1 (input)" << endl <<
			"-- max_trades: " << pIn->max_trades << endl <<
			"-- max_updates: " << pIn->max_updates << endl <<
			"-- Trades: {" << osTrades.str() << "}" << endl;
	m_coutLock.unlock();
#endif // DEBUG

	startTransaction();
	setRepeatableRead();
	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Update Frame 1 (output)" << endl <<
			"-- num_found: " << pOut->num_found << endl <<
			"-- num_updated: " << pOut->num_updated << endl;
	for (i = 0; i < pOut->num_found; i++) {
		cout << "-- bid_price[" << i << "]: " <<
						pOut->trade_info[i].bid_price << endl <<
				"-- exec_name[" << i << "]: " <<
						pOut->trade_info[i].exec_name << endl <<
				"-- is_cash[" << i << "]: " << pOut->trade_info[i].is_cash <<
						endl <<
				"-- is_market[" << i << "]: " <<
						pOut->trade_info[i].is_market << endl <<
				"-- trade_price[" << i << "]: " <<
						pOut->trade_info[i].trade_price << endl <<
				"-- settlement_amount[" << i << "]: "<<
						pOut->trade_info[i].settlement_amount << endl <<
				"-- settlement_cash_due_date[" << i << "]: " <<
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
	cout << ">>> TUF1" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}

// Call Trade Lookup Frame 2
void CTradeUpdateDB::DoTradeUpdateFrame2(const TTradeUpdateFrame2Input *pIn,
		TTradeUpdateFrame2Output *pOut)
{
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TUF2" << endl;
	cout << "- Trade Update Frame 2 (input)" << endl <<
			"-- acct_id: " << pIn->acct_id << endl <<
			"-- max_trades: " << pIn->max_trades << endl <<
			"-- max_updates: " << pIn->max_updates << endl <<
			"-- trade_dts: " << pIn->end_trade_dts.year << "-" <<
					pIn->end_trade_dts.month << "-" <<
					pIn->end_trade_dts.day << " " <<
					pIn->end_trade_dts.hour << ":" <<
					pIn->end_trade_dts.minute << ":" <<
					pIn->end_trade_dts.second << endl;
	m_coutLock.unlock();
#endif // DEBUG

	startTransaction();
	setRepeatableRead();
	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	m_coutLock.lock();
	cout << "-- Trade Update Frame 2 (output)" << endl <<
			"-- num_found: " << pOut->num_found << endl <<
			"-- num_updated: " << pOut->num_updated << endl;
	for (int i = 0; i < pOut->num_found; i++) {
		cout << "-- bid_price[" << i << "]: " <<
						pOut->trade_info[i].bid_price << endl <<
				"-- exec_name[" << i << "]: " <<
						pOut->trade_info[i].exec_name << endl <<
				"-- is_cash[" << i << "]: " << pOut->trade_info[i].is_cash <<
						endl <<
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
						pOut->trade_info[i].cash_transaction_dts.minute <<":" <<
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
	cout << ">>> TUF2" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}

// Call Trade Lookup Frame 3
void CTradeUpdateDB::DoTradeUpdateFrame3(const TTradeUpdateFrame3Input *pIn,
		TTradeUpdateFrame3Output *pOut)
{
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TUF3" << endl;
	cout << "- Trade Update Frame 3 (input)" << endl <<
			"-- max_acct_id: " << pIn->max_acct_id << endl <<
			"-- max_trades: " << pIn->max_trades << endl <<
			"-- max_updates: " << pIn->max_updates << endl <<
			"-- trade_dts: " << pIn->start_trade_dts.year << "-" <<
					pIn->start_trade_dts.month << "-" <<
					pIn->start_trade_dts.day << " " <<
					pIn->start_trade_dts.hour << ":" <<
					pIn->start_trade_dts.minute << ":" <<
					pIn->start_trade_dts.second << endl <<
			"-- symbol: " << pIn->symbol << endl;
	m_coutLock.unlock();
#endif //DEBUG

	startTransaction();
	setRepeatableRead();
	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Update Frame 3 (output)" << endl <<
			"-- num_found: " << pOut->num_found << endl <<
			"-- num_updated: " << pOut->num_updated << endl;
	for (int i = 0; i < pOut->num_found; i++) {
		cout << "-- acct_id[" << i << "]: " << pOut->trade_info[i].acct_id <<
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
				"-- trade_dts[" << i << "]: " <<
						pOut->trade_info[i].trade_dts.year << "-" <<
						pOut->trade_info[i].trade_dts.month << "-" <<
						pOut->trade_info[i].trade_dts.day << " " <<
						pOut->trade_info[i].trade_dts.hour << ":" <<
						pOut->trade_info[i].trade_dts.minute << ":" <<
						pOut->trade_info[i].trade_dts.second << endl <<
				"-- cash_transaction_dts[" << i << "]: " <<
						pOut->trade_info[i].trade_dts.year << "-" <<
						pOut->trade_info[i].trade_dts.month << "-" <<
						pOut->trade_info[i].trade_dts.day << " " <<
						pOut->trade_info[i].trade_dts.hour << ":" <<
						pOut->trade_info[i].trade_dts.minute << ":" <<
						pOut->trade_info[i].trade_dts.second << endl <<
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
						pOut->trade_info[i].trade_history_status_id[2] <<
						endl <<
				"-- trade_id[" << i << "]: " << pOut->trade_info[i].trade_id <<
						endl <<
				"-- trade_type[" << i << "]: " <<
						pOut->trade_info[i].trade_type << endl;
	}
	cout << ">>> TUF3" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}
