/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 07 July 2006
 */

#include "TradeResultDB.h"

CTradeResultDB::CTradeResultDB(CDBConnection *pDBConn, bool verbose = false)
: CTxnBaseDB(pDBConn), m_Verbose(verbose)
{
	m_pid = syscall(SYS_gettid);
}

// Call Trade Result Frame 1
void
CTradeResultDB::DoTradeResultFrame1(
		const TTradeResultFrame1Input *pIn, TTradeResultFrame1Output *pOut)
{
	if (m_Verbose) {
		cout << m_pid << " <<< TRF1" << endl
			 << m_pid << " - Trade Result Frame 1 (input)" << endl
			 << m_pid << " -- trade_id: " << pIn->trade_id << endl;
	}

	// start transaction but not commit in this frame
	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setSerializable();
	execute(pIn, pOut);

	if (m_Verbose) {
		cout << m_pid << " - Trade Result Frame 1 (output)" << endl
			 << m_pid << " -- acct_id: " << pOut->acct_id << endl
			 << m_pid << " -- charge: " << pOut->charge << endl
			 << m_pid << " -- hs_qty: " << pOut->hs_qty << endl
			 << m_pid << " -- is_lifo: " << pOut->is_lifo << endl
			 << m_pid << " -- num_found: " << pOut->num_found << endl
			 << m_pid << " -- symbol: " << pOut->symbol << endl
			 << m_pid << " -- trade_is_cash: " << pOut->trade_is_cash << endl
			 << m_pid << " -- trade_qty: " << pOut->trade_qty << endl
			 << m_pid << " -- type_id: " << pOut->type_id << endl
			 << m_pid << " -- type_is_market: " << pOut->type_is_market << endl
			 << m_pid << " -- type_is_sell: " << pOut->type_is_sell << endl
			 << m_pid << " -- type_name: " << pOut->type_name << endl
			 << m_pid << " >>> TRF1" << endl;
	}
}

// Call Trade Result Frame 2
void
CTradeResultDB::DoTradeResultFrame2(
		const TTradeResultFrame2Input *pIn, TTradeResultFrame2Output *pOut)
{
	if (m_Verbose) {
		cout << m_pid << " <<< TRF2" << endl
			 << m_pid << " - Trade Result Frame 2 (input)" << endl
			 << m_pid << " -- acct_id: " << pIn->acct_id << endl
			 << m_pid << " -- hs_qty: " << pIn->hs_qty << endl
			 << m_pid << " -- is_lifo: " << pIn->is_lifo << endl
			 << m_pid << " -- symbol: " << pIn->symbol << endl
			 << m_pid << " -- trade_id: " << pIn->trade_id << endl
			 << m_pid << " -- trade_price: " << pIn->trade_price << endl
			 << m_pid << " -- trade_qty: " << pIn->trade_qty << endl
			 << m_pid << " -- type_is_sell: " << pIn->type_is_sell << endl;
	}

	execute(pIn, pOut);

	if (m_Verbose) {
		cout << m_pid << " - Trade Result Frame 2 (output)" << endl
			 << m_pid << " -- broker_id: " << pOut->broker_id << endl
			 << m_pid << " -- buy_value: " << pOut->buy_value << endl
			 << m_pid << " -- cust_id: " << pOut->cust_id << endl
			 << m_pid << " -- sell_value: " << pOut->sell_value << endl
			 << m_pid << " -- tax_status: " << pOut->tax_status << endl
			 << m_pid << " -- trade_dts: " << pOut->trade_dts.year << "-"
			 << pOut->trade_dts.month << "-" << pOut->trade_dts.day << " "
			 << pOut->trade_dts.hour << ":" << pOut->trade_dts.minute << ":"
			 << pOut->trade_dts.second << endl
			 << m_pid << " >>> TRF2" << endl;
	}
}

// Call Trade Result Frame 3
void
CTradeResultDB::DoTradeResultFrame3(
		const TTradeResultFrame3Input *pIn, TTradeResultFrame3Output *pOut)
{
	if (m_Verbose) {
		cout << m_pid << " <<< TRF3" << endl
			 << m_pid << " --Trade Result Frame 3 (input)" << endl
			 << m_pid << " -- buy_value: " << pIn->buy_value << endl
			 << m_pid << " -- cust_id: " << pIn->cust_id << endl
			 << m_pid << " -- sell_value: " << pIn->sell_value << endl
			 << m_pid << " -- trade_id: " << pIn->trade_id << endl;
	}

	execute(pIn, pOut);

	if (m_Verbose) {
		cout << m_pid << " - Trade Result Frame 3 (output)" << endl
			 << m_pid << " -- tax_amount:" << pOut->tax_amount << endl
			 << m_pid << " >>> TRF3" << endl;
	}
}

// Call Trade Result Frame 4
void
CTradeResultDB::DoTradeResultFrame4(
		const TTradeResultFrame4Input *pIn, TTradeResultFrame4Output *pOut)
{
	if (m_Verbose) {
		cout << m_pid << " <<< TRF4" << endl
			 << m_pid << " - Trade Result Frame 4 (input)" << endl
			 << m_pid << " -- cust_id: " << pIn->cust_id << endl
			 << m_pid << " -- symbol: " << pIn->symbol << endl
			 << m_pid << " -- trade_qty: " << pIn->trade_qty << endl
			 << m_pid << " -- type_id: " << pIn->type_id << endl;
	}

	execute(pIn, pOut);

	if (m_Verbose) {
		cout << m_pid << " - Trade Result Frame 4 (output)" << endl
			 << m_pid << " -- comm_rate: " << pOut->comm_rate << endl
			 << m_pid << " -- s_name: " << pOut->s_name << endl
			 << m_pid << " >>> TRF4" << endl;
	}
}

// Call Trade Result Frame 5
void
CTradeResultDB::DoTradeResultFrame5(const TTradeResultFrame5Input *pIn)
{
	if (m_Verbose) {
		cout << m_pid << " <<< TRF5" << endl
			 << m_pid << " - Trade Result Frame 5 (input)" << endl
			 << m_pid << " -- broker_id: " << pIn->broker_id << endl
			 << m_pid << " -- comm_amount: " << pIn->comm_amount << endl
			 << m_pid << " -- st_completed_id: " << pIn->st_completed_id
			 << endl
			 << m_pid << " -- trade_dts: " << pIn->trade_dts.year << "-"
			 << pIn->trade_dts.month << "-" << pIn->trade_dts.day << " "
			 << pIn->trade_dts.hour << ":" << pIn->trade_dts.minute << ":"
			 << pIn->trade_dts.second << endl
			 << m_pid << " -- trade_id: " << pIn->trade_id << endl
			 << m_pid << " -- trade_price: " << pIn->trade_price << endl;
	}

	execute(pIn);

	if (m_Verbose) {
		cout << m_pid << " - Trade Result Frame 5 (output)" << endl
			 << m_pid << " >>> TRF5" << endl;
	}
}

// Call Trade Result Frame 6
void
CTradeResultDB::DoTradeResultFrame6(
		const TTradeResultFrame6Input *pIn, TTradeResultFrame6Output *pOut)
{
	if (m_Verbose) {
		cout << m_pid << " <<< TRF6" << endl
			 << m_pid << " - Trade Result Frame 6 (input)" << endl
			 << m_pid << " -- acct_id: " << pIn->acct_id << endl
			 << m_pid << " -- due_date: " << pIn->due_date.year << "-"
			 << pIn->due_date.month << "-" << pIn->due_date.day << " "
			 << pIn->due_date.hour << ":" << pIn->due_date.minute << ":"
			 << pIn->due_date.second << endl
			 << m_pid << " -- s_name: " << pIn->s_name << endl
			 << m_pid << " -- se_amount: " << pIn->se_amount << endl
			 << m_pid << " -- trade_dts: " << pIn->trade_dts.year << "-"
			 << pIn->trade_dts.month << "-" << pIn->trade_dts.day << " "
			 << pIn->trade_dts.hour << ":" << pIn->trade_dts.minute << ":"
			 << pIn->trade_dts.second << endl
			 << m_pid << " -- trade_id: " << pIn->trade_id << endl
			 << m_pid << " -- trade_is_cash: " << pIn->trade_is_cash << endl
			 << m_pid << " -- trade_qty: " << pIn->trade_qty << endl
			 << m_pid << " -- type_name: " << pIn->type_name << endl;
	}

	execute(pIn, pOut);
	commitTransaction();

	if (m_Verbose) {
		cout << m_pid << " Trade Result Frame 6 (output)" << endl
			 << m_pid << " - acct_bal:" << pOut->acct_bal << endl
			 << m_pid << " >>> TRF6" << endl;
	}
}
