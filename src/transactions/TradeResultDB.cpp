/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 07 July 2006
 */

#include "TradeResultDB.h"

// Call Trade Result Frame 1
void CTradeResultDB::DoTradeResultFrame1(
		const TTradeResultFrame1Input *pIn,
		TTradeResultFrame1Output *pOut)
{
#ifdef DEBUG
	pid_t pid = syscall(SYS_gettid);
	cout << pid << " <<< TRF1" << endl;
	cout << pid << " - Trade Result Frame 1 (input)" << endl <<
			pid << " -- trade_id: " << pIn->trade_id << endl;
#endif // DEBUG

	// start transaction but not commit in this frame
	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setSerializable();
	execute(pIn, pOut);

#ifdef DEBUG
	cout << pid << " - Trade Result Frame 1 (output)" << endl <<
			pid << " -- acct_id: " << pOut->acct_id << endl <<
			pid << " -- charge: " << pOut->charge << endl <<
			pid << " -- hs_qty: " << pOut->hs_qty << endl <<
			pid << " -- is_lifo: " << pOut->is_lifo << endl <<
			pid << " -- num_found: " << pOut->num_found << endl <<
			pid << " -- symbol: " << pOut->symbol << endl <<
			pid << " -- trade_is_cash: " << pOut->trade_is_cash << endl <<
			pid << " -- trade_qty: " << pOut->trade_qty << endl <<
			pid << " -- type_id: " << pOut->type_id << endl <<
			pid << " -- type_is_market: " << pOut->type_is_market << endl <<
			pid << " -- type_is_sell: " << pOut->type_is_sell << endl <<
			pid << " -- type_name: " << pOut->type_name << endl;
	cout << pid << " >>> TRF1" << endl;
#endif // DEBUG
}

// Call Trade Result Frame 2
void CTradeResultDB::DoTradeResultFrame2(
		const TTradeResultFrame2Input *pIn,
		TTradeResultFrame2Output *pOut)
{
#ifdef DEBUG
	pid_t pid = syscall(SYS_gettid);
	cout << pid << " <<< TRF2" << endl;
	cout << pid << " - Trade Result Frame 2 (input)" << endl <<
			pid << " -- acct_id: " << pIn->acct_id << endl <<
			pid << " -- hs_qty: " << pIn->hs_qty << endl <<
			pid << " -- is_lifo: " << pIn->is_lifo << endl <<
			pid << " -- symbol: " << pIn->symbol << endl <<
			pid << " -- trade_id: " << pIn->trade_id << endl <<
			pid << " -- trade_price: " << pIn->trade_price << endl <<
			pid << " -- trade_qty: " << pIn->trade_qty << endl <<
			pid << " -- type_is_sell: " << pIn->type_is_sell << endl;
#endif // DEBUG

	execute(pIn, pOut);

#ifdef DEBUG
	cout << pid << " - Trade Result Frame 2 (output)" << endl <<
			pid << " -- broker_id: " << pOut->broker_id << endl <<
			pid << " -- buy_value: " << pOut->buy_value << endl <<
			pid << " -- cust_id: " << pOut->cust_id << endl <<
			pid << " -- sell_value: " << pOut->sell_value << endl <<
			pid << " -- tax_status: " << pOut->tax_status << endl <<
			pid << " -- trade_dts: " << pOut->trade_dts.year << "-" <<
					pOut->trade_dts.month << "-" <<
					pOut->trade_dts.day << " " <<
					pOut->trade_dts.hour << ":" <<
					pOut->trade_dts.minute << ":" <<
					pOut->trade_dts.second << endl;
	cout << pid << " >>> TRF2" << endl;
#endif // DEBUG
}

// Call Trade Result Frame 3
void CTradeResultDB::DoTradeResultFrame3(
		const TTradeResultFrame3Input *pIn,
		TTradeResultFrame3Output *pOut)
{
#ifdef DEBUG
	pid_t pid = syscall(SYS_gettid);
	cout << pid << " <<< TRF3" << endl;
	cout << pid << " --Trade Result Frame 3 (input)" << endl <<
			pid << " -- buy_value: " << pIn->buy_value << endl <<
			pid << " -- cust_id: " << pIn->cust_id << endl <<
			pid << " -- sell_value: " << pIn->sell_value << endl <<
			pid << " -- trade_id: " << pIn->trade_id << endl;
#endif //DEBUG

	execute(pIn, pOut);

#ifdef DEBUG
	cout << pid << " - Trade Result Frame 3 (output)" << endl <<
			pid << " -- tax_amount:" << pOut->tax_amount << endl;
	cout << pid << " >>> TRF3" << endl;
#endif //DEBUG
}

// Call Trade Result Frame 4
void CTradeResultDB::DoTradeResultFrame4(
		const TTradeResultFrame4Input *pIn,
		TTradeResultFrame4Output *pOut)
{
#ifdef DEBUG
	pid_t pid = syscall(SYS_gettid);
	cout << pid << " <<< TRF4" << endl;
	cout << pid << " - Trade Result Frame 4 (input)" << endl <<
			pid << " -- cust_id: " << pIn->cust_id << endl <<
			pid << " -- symbol: " << pIn->symbol << endl <<
			pid << " -- trade_qty: " << pIn->trade_qty << endl <<
			pid << " -- type_id: " << pIn->type_id << endl;
#endif //DEBUG

	execute(pIn, pOut);

#ifdef DEBUG
	cout << pid << " - Trade Result Frame 4 (output)" << endl <<
			pid << " -- comm_rate: " << pOut->comm_rate << endl <<
			pid << " -- s_name: " << pOut->s_name << endl;
	cout << pid << " >>> TRF4" << endl;
#endif //DEBUG
}

// Call Trade Result Frame 5
void CTradeResultDB::DoTradeResultFrame5(const TTradeResultFrame5Input *pIn)
{
#ifdef DEBUG
	pid_t pid = syscall(SYS_gettid);
	cout << pid << " <<< TRF5" << endl;
	cout << pid << " - Trade Result Frame 5 (input)" << endl <<
			pid << " -- broker_id: " << pIn->broker_id << endl <<
			pid << " -- comm_amount: " << pIn->comm_amount << endl <<
			pid << " -- st_completed_id: " << pIn->st_completed_id << endl <<
			pid << " -- trade_dts: " << pIn->trade_dts.year << "-" <<
					pIn->trade_dts.month << "-" <<
					pIn->trade_dts.day << " " <<
					pIn->trade_dts.hour << ":" <<
					pIn->trade_dts.minute << ":" <<
					pIn->trade_dts.second << endl <<
			pid << " -- trade_id: " << pIn->trade_id << endl <<
			pid << " -- trade_price: " << pIn->trade_price << endl;
#endif //DEBUG

	execute(pIn);

#ifdef DEBUG
	cout << pid << " - Trade Result Frame 5 (output)" << endl;
	cout << pid << " >>> TRF5" << endl;
#endif //DEBUG
}

// Call Trade Result Frame 6
void CTradeResultDB::DoTradeResultFrame6(
		const TTradeResultFrame6Input *pIn,
		TTradeResultFrame6Output *pOut)
{
#ifdef DEBUG
	pid_t pid = syscall(SYS_gettid);
	cout << pid << " <<< TRF6" << endl;
	cout << pid << " - Trade Result Frame 6 (input)" << endl <<
			pid << " -- acct_id: " << pIn->acct_id << endl <<
			pid << " -- due_date: " << pIn->due_date.year << "-" <<
					pIn->due_date.month << "-" << pIn->due_date.day <<
					" " << pIn->due_date.hour << ":" <<
					pIn->due_date.minute << ":" <<
					pIn->due_date.second << endl <<
			pid << " -- s_name: " << pIn->s_name << endl <<
			pid << " -- se_amount: " << pIn->se_amount << endl <<
			pid << " -- trade_dts: " << pIn->trade_dts.year << "-" <<
					pIn->trade_dts.month << "-" <<
					pIn->trade_dts.day << " " <<
					pIn->trade_dts.hour << ":" <<
					pIn->trade_dts.minute << ":" <<
					pIn->trade_dts.second << endl <<
			pid << " -- trade_id: " << pIn->trade_id << endl <<
			pid << " -- trade_is_cash: " << pIn->trade_is_cash << endl <<
			pid << " -- trade_qty: " << pIn->trade_qty << endl <<
			pid << " -- type_name: " << pIn->type_name << endl;
#endif //DEBUG

	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	cout << pid << " Trade Result Frame 6 (output)" << endl <<
			pid << " - acct_bal:" << pOut->acct_bal << endl;
	cout << pid << " >>> TRF6" << endl;
#endif //DEBUG
}
