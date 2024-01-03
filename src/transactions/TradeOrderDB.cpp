/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 03 July 2006
 */

#include "TradeOrderDB.h"

CTradeOrderDB::CTradeOrderDB(CDBConnection *pDBConn, bool verbose)
: CTxnBaseDB(pDBConn), m_Verbose(verbose)
{
	m_pid = syscall(SYS_gettid);
}

// Call Trade Order Frame 1
void
CTradeOrderDB::DoTradeOrderFrame1(
		const TTradeOrderFrame1Input *pIn, TTradeOrderFrame1Output *pOut)
{
	if (m_Verbose) {
		cout << m_pid << " <<< TOF1" << endl
			 << m_pid << " - Trade Order Frame 1 (input)" << endl
			 << m_pid << " -- acct_id: " << pIn->acct_id << endl;
	}

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setRepeatableRead();
	execute(pIn, pOut);

	if (m_Verbose) {
		cout << m_pid << " - Trade Order Frame 1 (output)" << endl
			 << m_pid << " -- acct_name: " << pOut->acct_name << endl
			 << m_pid << " -- broker_id: " << pOut->broker_id << endl
			 << m_pid << " -- broker_name: " << pOut->broker_name << endl
			 << m_pid << " -- cust_f_name: " << pOut->cust_f_name << endl
			 << m_pid << " -- cust_id: " << pOut->cust_id << endl
			 << m_pid << " -- cust_l_name: " << pOut->cust_l_name << endl
			 << m_pid << " -- cust_tier: " << pOut->cust_tier << endl
			 << m_pid << " -- num_found: " << pOut->num_found << endl
			 << m_pid << " -- tax_id: " << pOut->tax_id << endl
			 << m_pid << " -- tax_status: " << pOut->tax_status << endl
			 << m_pid << " >>> TOF1" << endl;
	}
}

// Call Trade Order Frame 2
void
CTradeOrderDB::DoTradeOrderFrame2(
		const TTradeOrderFrame2Input *pIn, TTradeOrderFrame2Output *pOut)
{
	if (m_Verbose) {
		cout << m_pid << " <<< TOF2" << endl
			 << m_pid << " - Trade Order Frame 2 (input)" << endl
			 << m_pid << " -- acct_id: " << pIn->acct_id << endl
			 << m_pid << " -- exec_f_name: " << escape(pIn->exec_f_name)
			 << endl
			 << m_pid << " -- exec_l_name: " << escape(pIn->exec_l_name)
			 << endl
			 << m_pid << " -- exec_tax_id: " << pIn->exec_tax_id << endl;
	}

	execute(pIn, pOut);

	if (m_Verbose) {
		cout << m_pid << " - Trade Order Frame 2 (output)" << endl
			 << m_pid << " -- ap_acl: " << pOut->ap_acl << endl
			 << m_pid << " >>> TOF2" << endl;
	}
}

// Call Trade Order Frame 3
void
CTradeOrderDB::DoTradeOrderFrame3(
		const TTradeOrderFrame3Input *pIn, TTradeOrderFrame3Output *pOut)
{
	if (m_Verbose) {
		cout << m_pid << " <<< TOF3" << endl
			 << m_pid << " - Trade Order Frame 3 (input)" << endl
			 << m_pid << " -- acct_id: " << pIn->acct_id << endl
			 << m_pid << " -- cust_id: " << pIn->cust_id << endl
			 << m_pid << " -- cust_tier: " << pIn->cust_tier << endl
			 << m_pid << " -- is_lifo: " << pIn->is_lifo << endl
			 << m_pid << " -- issue: " << pIn->issue << endl
			 << m_pid << " -- st_pending_id: " << pIn->st_pending_id << endl
			 << m_pid << " -- st_submitted_id: " << pIn->st_submitted_id
			 << endl
			 << m_pid << " -- tax_status: " << pIn->tax_status << endl
			 << m_pid << " -- trade_qty: " << pIn->trade_qty << endl
			 << m_pid << " -- trade_type_id: " << pIn->trade_type_id << endl
			 << m_pid << " -- type_is_margin: " << pIn->type_is_margin << endl
			 << m_pid << " -- co_name: " << pIn->co_name << endl
			 << m_pid << " -- requested_price: " << pIn->requested_price
			 << endl
			 << m_pid << " -- symbol: " << pIn->symbol << endl;
	}

	execute(pIn, pOut);

	if (m_Verbose) {
		cout << m_pid << " - Trade Order Frame 3 (output)" << endl
			 << m_pid << " -- co_name: " << pOut->co_name << endl
			 << m_pid << " -- requested_price: " << pOut->requested_price
			 << endl
			 << m_pid << " -- symbol: " << pOut->symbol << endl
			 << m_pid << " -- buy_value: " << pOut->buy_value << endl
			 << m_pid << " -- charge_amount: " << pOut->charge_amount << endl
			 << m_pid << " -- comm_rate: " << pOut->comm_rate << endl
			 << m_pid << " -- acct_assets: " << pOut->acct_assets << endl
			 << m_pid << " -- market_price: " << pOut->market_price << endl
			 << m_pid << " -- s_name: " << pOut->s_name << endl
			 << m_pid << " -- sell_value: " << pOut->sell_value << endl
			 << m_pid << " -- status_id: " << pOut->status_id << endl
			 << m_pid << " -- tax_amount: " << pOut->tax_amount << endl
			 << m_pid << " -- type_is_market: " << pOut->type_is_market << endl
			 << m_pid << " -- type_is_sell: " << pOut->type_is_sell << endl
			 << m_pid << " >>> TOF3" << endl;
	}
}

// Call Trade Order Frame 4
void
CTradeOrderDB::DoTradeOrderFrame4(
		const TTradeOrderFrame4Input *pIn, TTradeOrderFrame4Output *pOut)
{
	if (m_Verbose) {
		cout << m_pid << " <<< TOF4" << endl
			 << m_pid << " -Trade Order Frame 4 (input)" << endl
			 << m_pid << " -- acct_id: " << pIn->acct_id << endl
			 << m_pid << " -- broker_id: " << pIn->broker_id << endl
			 << m_pid << " -- charge_amount: " << pIn->charge_amount << endl
			 << m_pid << " -- comm_amount: " << pIn->comm_amount << endl
			 << m_pid << " -- exec_name: " << pIn->exec_name << endl
			 << m_pid << " -- is_cash: " << pIn->is_cash << endl
			 << m_pid << " -- is_lifo: " << pIn->is_lifo << endl
			 << m_pid << " -- requested_price: " << pIn->requested_price
			 << endl
			 << m_pid << " -- status_id: " << pIn->status_id << endl
			 << m_pid << " -- symbol: " << pIn->symbol << endl
			 << m_pid << " -- trade_qty: " << pIn->trade_qty << endl
			 << m_pid << " -- trade_type_id: " << pIn->trade_type_id << endl
			 << m_pid << " -- type_is_market: " << pIn->type_is_market << endl;
	}

	// we are inside a transaction
	execute(pIn, pOut);

	if (m_Verbose) {
		cout << m_pid << " - Trade Order Frame 4 (output)" << endl
			 << m_pid << " -- trade_id: " << pOut->trade_id << endl
			 << m_pid << " >>> TOF4" << endl;
	}
}

// Call Trade Order Frame 5
void
CTradeOrderDB::DoTradeOrderFrame5()
{
	if (m_Verbose) {
		cout << m_pid << " <<< TOF5" << endl;
	}

	// rollback the transaction we are inside
	rollbackTransaction();

	if (m_Verbose) {
		cout << m_pid << " >>> TOF5" << endl;
	}
}

// Call Trade Order Frame 6
void
CTradeOrderDB::DoTradeOrderFrame6()
{
	if (m_Verbose) {
		cout << m_pid << " <<< TOF6" << endl;
	}

	// commit the transaction we are inside
	commitTransaction();

	if (m_Verbose) {
		cout << m_pid << " >>> TOF6" << endl;
	}
}
