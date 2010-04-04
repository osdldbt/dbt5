/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * 03 July 2006
 */

#include "TradeOrderDB.h"

// Call Trade Order Frame 1
void CTradeOrderDB::DoTradeOrderFrame1(const TTradeOrderFrame1Input *pIn,
		TTradeOrderFrame1Output *pOut)
{
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TOF1" << endl;
	cout << "- Trade Order Frame 1 (input)" << endl <<
			"-- acct_id: " << pIn->acct_id << endl;
	m_coutLock.unlock();
#endif // DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setRepeatableRead();
	execute(pIn, pOut);

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Order Frame 1 (output)" << endl <<
			"-- acct_name: " << pOut->acct_name << endl <<
			"-- broker_name: " << pOut->broker_name << endl <<
			"-- cust_f_name: " << pOut->cust_f_name << endl <<
			"-- cust_id: " << pOut->cust_id << endl <<
			"-- cust_l_name: " << pOut->cust_l_name << endl <<
			"-- cust_tier: " << pOut->cust_tier << endl <<
			"-- tax_id: " << pOut->tax_id << endl <<
			"-- tax_status: " << pOut->tax_status << endl;
	cout << ">>> TOF1" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}

// Call Trade Order Frame 2
void CTradeOrderDB::DoTradeOrderFrame2(const TTradeOrderFrame2Input *pIn,
		TTradeOrderFrame2Output *pOut)
{
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TOF2" << endl;
	cout << "- Trade Order Frame 2 (input)" << endl <<
			"-- acct_id: " << pIn->acct_id << endl <<
			"-- exec_f_name: " << escape(pIn->exec_f_name) << endl <<
			"-- exec_l_name: " << escape(pIn->exec_l_name) << endl <<
			"-- exec_tax_id: " << pIn->exec_tax_id << endl;
	m_coutLock.unlock();
#endif // DEBUG

	execute(pIn, pOut);

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Order Frame 2 (output)" << endl <<
			"-- ap_acl: " << pOut->ap_acl << endl <<
			"-- status: " << pOut->status << endl;
	cout << ">>> TOF2" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}

// Call Trade Order Frame 3
void CTradeOrderDB::DoTradeOrderFrame3(const TTradeOrderFrame3Input *pIn,
		TTradeOrderFrame3Output *pOut)
{
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TOF3" << endl;
	cout << "- Trade Order Frame 3 (input)" << endl <<
			"-- acct_id: " << pIn->acct_id << endl <<
			"-- cust_id: " << pIn->cust_id << endl <<
			"-- cust_tier: " << pIn->cust_tier << endl <<
			"-- is_lifo: " << pIn->is_lifo << endl <<
			"-- issue: " << pIn->issue << endl <<
			"-- st_pending_id: " << pIn->st_pending_id << endl <<
			"-- st_submitted_id: " << pIn->st_submitted_id << endl <<
			"-- tax_status: " << pIn->tax_status << endl <<
			"-- trade_qty: " << pIn->trade_qty << endl <<
			"-- trade_type_id: " << pIn->trade_type_id << endl <<
			"-- type_is_margin: " << pIn->type_is_margin << endl <<
			"-- co_name: " << pIn->co_name << endl <<
			"-- requested_price: " << pIn->requested_price << endl <<
			"-- symbol: " << pIn->symbol << endl;
	m_coutLock.unlock();
#endif //DEBUG

	execute(pIn, pOut);

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Order Frame 3 (output)" << endl <<
			"-- co_name: " << pOut->co_name << endl <<
			"-- requested_price: " << pOut->requested_price << endl <<
			"-- symbol: " << pOut->symbol << endl <<
			"-- buy_value: " << pOut->buy_value << endl <<
			"-- charge_amount: " << pOut->charge_amount << endl <<
			"-- comm_rate: " << pOut->comm_rate << endl <<
			"-- cust_assets: " << pOut->cust_assets << endl <<
			"-- market_price: " << pOut->market_price << endl <<
			"-- s_name: " << pOut->s_name << endl <<
			"-- sell_value: " << pOut->sell_value << endl <<
			"-- status_id: " << pOut->status_id << endl <<
			"-- tax_amount: " << pOut->tax_amount << endl <<
			"-- type_is_market: " << pOut->type_is_market << endl <<
			"-- type_is_sell: " << pOut->type_is_sell << endl;
	cout << ">>> TOF3" << endl;
	m_coutLock.unlock();
#endif //DEBUG
}

// Call Trade Order Frame 4
void CTradeOrderDB::DoTradeOrderFrame4(const TTradeOrderFrame4Input *pIn,
		TTradeOrderFrame4Output *pOut)
{
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TOF4" << endl;
	cout << "-Trade Order Frame 4 (input)" << endl <<
			"-- acct_id: " << pIn->acct_id << endl <<
			"-- broker_id: " << pIn->broker_id << endl <<
			"-- charge_amount: " << pIn->charge_amount << endl <<
			"-- comm_amount: " << pIn->comm_amount << endl <<
			"-- exec_name: " << pIn->exec_name << endl <<
			"-- is_cash: " << pIn->is_cash << endl <<
			"-- is_lifo: " << pIn->is_lifo << endl <<
			"-- requested_price: " << pIn->requested_price << endl <<
			"-- status_id: " << pIn->status_id << endl <<
			"-- symbol: " << pIn->symbol << endl <<
			"-- trade_qty: " << pIn->trade_qty << endl <<
			"-- trade_type_id: " << pIn->trade_type_id << endl <<
			"-- type_is_market: " << pIn->type_is_market << endl;
	m_coutLock.unlock();
#endif //DEBUG

	// we are inside a transaction
	execute(pIn, pOut);

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Order Frame 4 (output)" << endl <<
			"-- status: " << pOut->status << endl <<
			"-- trade_id: " << pOut->trade_id << endl;
	cout << ">>> TOF4" << endl;
	m_coutLock.unlock();
#endif //DEBUG
}

// Call Trade Order Frame 5
void CTradeOrderDB::DoTradeOrderFrame5(TTradeOrderFrame5Output *pOut)
{
#ifdef DEBUG
	cout << "<<< TOF5" << endl;
#endif

	// rollback the transaction we are inside
	rollbackTransaction();
	pOut->status = CBaseTxnErr::ROLLBACK;

#ifdef DEBUG
	cout << "- Trade Order Frame 5 (output)" << endl <<
			"-- status: " << pOut->status << endl;
	cout << ">>> TOF5" << endl;
#endif
}

// Call Trade Order Frame 6
void CTradeOrderDB::DoTradeOrderFrame6(TTradeOrderFrame6Output *pOut)
{
#ifdef DEBUG
	cout << "<<< TOF6" << endl;
#endif

	// commit the transaction we are inside
	commitTransaction();
	pOut->status = CBaseTxnErr::SUCCESS;

#ifdef DEBUG
	cout << "- Trade Order Frame 6 (output)" << endl <<
			"-- status: " << pOut->status << endl;
	cout << ">>> TOF6" << endl;
#endif
}
