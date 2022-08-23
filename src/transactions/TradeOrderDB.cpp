/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 03 July 2006
 */

#include "TradeOrderDB.h"

// Call Trade Order Frame 1
void CTradeOrderDB::DoTradeOrderFrame1(const TTradeOrderFrame1Input *pIn,
		TTradeOrderFrame1Output *pOut)
{
#ifdef DEBUG
	pthread_t pid = pthread_self();
	cout << pid << " <<< TOF1" << endl;
	cout << pid << " - Trade Order Frame 1 (input)" << endl <<
			pid << " -- acct_id: " << pIn->acct_id << endl;
#endif // DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setRepeatableRead();
	execute(pIn, pOut);

#ifdef DEBUG
	cout << pid << " - Trade Order Frame 1 (output)" << endl <<
			pid << " -- acct_name: " << pOut->acct_name << endl <<
			pid << " -- broker_name: " << pOut->broker_name << endl <<
			pid << " -- cust_f_name: " << pOut->cust_f_name << endl <<
			pid << " -- cust_id: " << pOut->cust_id << endl <<
			pid << " -- cust_l_name: " << pOut->cust_l_name << endl <<
			pid << " -- cust_tier: " << pOut->cust_tier << endl <<
			pid << " -- num_found: " << pOut->num_found << endl <<
			pid << " -- tax_id: " << pOut->tax_id << endl <<
			pid << " -- tax_status: " << pOut->tax_status << endl;
	cout << pid << " >>> TOF1" << endl;
#endif // DEBUG
}

// Call Trade Order Frame 2
void CTradeOrderDB::DoTradeOrderFrame2(const TTradeOrderFrame2Input *pIn,
		TTradeOrderFrame2Output *pOut)
{
#ifdef DEBUG
	pthread_t pid = pthread_self();
	cout << pid << " <<< TOF2" << endl;
	cout << pid << " - Trade Order Frame 2 (input)" << endl <<
			pid << " -- acct_id: " << pIn->acct_id << endl <<
			pid << " -- exec_f_name: " << escape(pIn->exec_f_name) << endl <<
			pid << " -- exec_l_name: " << escape(pIn->exec_l_name) << endl <<
			pid << " -- exec_tax_id: " << pIn->exec_tax_id << endl;
#endif // DEBUG

	execute(pIn, pOut);

#ifdef DEBUG
	cout << pid << " - Trade Order Frame 2 (output)" << endl <<
			pid << " -- ap_acl: " << pOut->ap_acl << endl <<
	cout << pid << " >>> TOF2" << endl;
#endif // DEBUG
}

// Call Trade Order Frame 3
void CTradeOrderDB::DoTradeOrderFrame3(const TTradeOrderFrame3Input *pIn,
		TTradeOrderFrame3Output *pOut)
{
#ifdef DEBUG
	pthread_t pid = pthread_self();
	cout << pid << " <<< TOF3" << endl;
	cout << pid << " - Trade Order Frame 3 (input)" << endl <<
			pid << " -- acct_id: " << pIn->acct_id << endl <<
			pid << " -- cust_id: " << pIn->cust_id << endl <<
			pid << " -- cust_tier: " << pIn->cust_tier << endl <<
			pid << " -- is_lifo: " << pIn->is_lifo << endl <<
			pid << " -- issue: " << pIn->issue << endl <<
			pid << " -- st_pending_id: " << pIn->st_pending_id << endl <<
			pid << " -- st_submitted_id: " << pIn->st_submitted_id << endl <<
			pid << " -- tax_status: " << pIn->tax_status << endl <<
			pid << " -- trade_qty: " << pIn->trade_qty << endl <<
			pid << " -- trade_type_id: " << pIn->trade_type_id << endl <<
			pid << " -- type_is_margin: " << pIn->type_is_margin << endl <<
			pid << " -- co_name: " << pIn->co_name << endl <<
			pid << " -- requested_price: " << pIn->requested_price << endl <<
			pid << " -- symbol: " << pIn->symbol << endl;
#endif //DEBUG

	execute(pIn, pOut);

#ifdef DEBUG
	cout << pid << " - Trade Order Frame 3 (output)" << endl <<
			pid << " -- co_name: " << pOut->co_name << endl <<
			pid << " -- requested_price: " << pOut->requested_price << endl <<
			pid << " -- symbol: " << pOut->symbol << endl <<
			pid << " -- buy_value: " << pOut->buy_value << endl <<
			pid << " -- charge_amount: " << pOut->charge_amount << endl <<
			pid << " -- comm_rate: " << pOut->comm_rate << endl <<
			pid << " -- acct_assets: " << pOut->acct_assets << endl <<
			pid << " -- market_price: " << pOut->market_price << endl <<
			pid << " -- s_name: " << pOut->s_name << endl <<
			pid << " -- sell_value: " << pOut->sell_value << endl <<
			pid << " -- status_id: " << pOut->status_id << endl <<
			pid << " -- tax_amount: " << pOut->tax_amount << endl <<
			pid << " -- type_is_market: " << pOut->type_is_market << endl <<
			pid << " -- type_is_sell: " << pOut->type_is_sell << endl;
	cout << pid << " >>> TOF3" << endl;
#endif //DEBUG
}

// Call Trade Order Frame 4
void CTradeOrderDB::DoTradeOrderFrame4(const TTradeOrderFrame4Input *pIn,
		TTradeOrderFrame4Output *pOut)
{
#ifdef DEBUG
	pthread_t pid = pthread_self();
	cout << pid << " <<< TOF4" << endl;
	cout << pid << " -Trade Order Frame 4 (input)" << endl <<
			pid << " -- acct_id: " << pIn->acct_id << endl <<
			pid << " -- broker_id: " << pIn->broker_id << endl <<
			pid << " -- charge_amount: " << pIn->charge_amount << endl <<
			pid << " -- comm_amount: " << pIn->comm_amount << endl <<
			pid << " -- exec_name: " << pIn->exec_name << endl <<
			pid << " -- is_cash: " << pIn->is_cash << endl <<
			pid << " -- is_lifo: " << pIn->is_lifo << endl <<
			pid << " -- requested_price: " << pIn->requested_price << endl <<
			pid << " -- status_id: " << pIn->status_id << endl <<
			pid << " -- symbol: " << pIn->symbol << endl <<
			pid << " -- trade_qty: " << pIn->trade_qty << endl <<
			pid << " -- trade_type_id: " << pIn->trade_type_id << endl <<
			pid << " -- type_is_market: " << pIn->type_is_market << endl;
#endif //DEBUG

	// we are inside a transaction
	execute(pIn, pOut);

#ifdef DEBUG
	cout << pid << " - Trade Order Frame 4 (output)" << endl <<
			pid << " -- trade_id: " << pOut->trade_id << endl;
	cout << pid << " >>> TOF4" << endl;
#endif //DEBUG
}

// Call Trade Order Frame 5
void CTradeOrderDB::DoTradeOrderFrame5()
{
#ifdef DEBUG
	pthread_t pid = pthread_self();
	cout << pid << " <<< TOF5" << endl;
#endif

	// rollback the transaction we are inside
	rollbackTransaction();

#ifdef DEBUG
	cout << pid << " >>> TOF5" << endl;
#endif
}

// Call Trade Order Frame 6
void CTradeOrderDB::DoTradeOrderFrame6()
{
#ifdef DEBUG
	pthread_t pid = pthread_self();
	cout << pid << " <<< TOF6" << endl;
#endif

	// commit the transaction we are inside
	commitTransaction();

#ifdef DEBUG
	cout << pid << " >>> TOF6" << endl;
#endif
}
