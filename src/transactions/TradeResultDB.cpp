/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
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
	m_coutLock.lock();
	cout << "<<< TRF1" << endl;
	cout << "- Trade Result Frame 1 (input)" << endl <<
			"-- trade_id: " << pIn->trade_id << endl;
	m_coutLock.unlock();
#endif // DEBUG

	// start transaction but not commit in this frame
	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setSerializable();
	execute(pIn, pOut);

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Result Frame 1 (output)" << endl <<
			"-- acct_id: " << pOut->acct_id << endl <<
			"-- charge: " << pOut->charge << endl <<
			"-- hs_qty: " << pOut->hs_qty << endl <<
			"-- is_lifo: " << pOut->is_lifo << endl <<
			"-- symbol: " << pOut->symbol << endl <<
			"-- trade_is_cash: " << pOut->trade_is_cash << endl <<
			"-- trade_qty: " << pOut->trade_qty << endl <<
			"-- type_id: " << pOut->type_id << endl <<
			"-- type_is_market: " << pOut->type_is_market << endl <<
			"-- type_is_sell: " << pOut->type_is_sell << endl <<
			"-- type_name: " << pOut->type_name << endl;
	cout << ">>> TRF1" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}

// Call Trade Result Frame 2
void CTradeResultDB::DoTradeResultFrame2(
		const TTradeResultFrame2Input *pIn,
		TTradeResultFrame2Output *pOut)
{
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TRF2" << endl;
	cout << "- Trade Result Frame 2 (input)" << endl <<
			"-- acct_id: " << pIn->acct_id << endl <<
			"-- hs_qty: " << pIn->hs_qty << endl <<
			"-- is_lifo: " << pIn->is_lifo << endl <<
			"-- symbol: " << pIn->symbol << endl <<
			"-- trade_id: " << pIn->trade_id << endl <<
			"-- trade_price: " << pIn->trade_price << endl <<
			"-- trade_qty: " << pIn->trade_qty << endl <<
			"-- type_is_sell: " << pIn->type_is_sell << endl;
	m_coutLock.unlock();
#endif // DEBUG

	execute(pIn, pOut);

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Result Frame 2 (output)" << endl <<
			"-- broker_id: " << pOut->broker_id << endl <<
			"-- buy_value: " << pOut->buy_value << endl <<
			"-- cust_id: " << pOut->cust_id << endl <<
			"-- sell_value: " << pOut->sell_value << endl <<
			"-- status: " << pOut->status << endl <<
			"-- tax_status: " << pOut->tax_status << endl <<
			"-- trade_dts: " << pOut->trade_dts.year << "-" <<
					pOut->trade_dts.month << "-" <<
					pOut->trade_dts.day << " " <<
					pOut->trade_dts.hour << ":" <<
					pOut->trade_dts.minute << ":" <<
					pOut->trade_dts.second << endl;
	cout << ">>> TRF2" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}

// Call Trade Result Frame 3
void CTradeResultDB::DoTradeResultFrame3(
		const TTradeResultFrame3Input *pIn,
		TTradeResultFrame3Output *pOut)
{
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TRF3" << endl;
	cout << "--Trade Result Frame 3 (input)" << endl <<
			"-- buy_value: " << pIn->buy_value << endl <<
			"-- cust_id: " << pIn->cust_id << endl <<
			"-- sell_value: " << pIn->sell_value << endl <<
			"-- trade_id: " << pIn->trade_id << endl;
	m_coutLock.unlock();
#endif //DEBUG

	execute(pIn, pOut);

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Result Frame 3 (output)" << endl <<
			"-- status:" << pOut->status << endl <<
			"-- tax_amount:" << pOut->tax_amount << endl;
	cout << ">>> TRF3" << endl;
	m_coutLock.unlock();
#endif //DEBUG
}

// Call Trade Result Frame 4
void CTradeResultDB::DoTradeResultFrame4(
		const TTradeResultFrame4Input *pIn,
		TTradeResultFrame4Output *pOut)
{
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TRF4" << endl;
	cout << "- Trade Result Frame 4 (input)" << endl <<
			"-- cust_id: " << pIn->cust_id << endl <<
			"-- symbol: " << pIn->symbol << endl <<
			"-- trade_qty: " << pIn->trade_qty << endl <<
			"-- type_id: " << pIn->type_id << endl;
	m_coutLock.unlock();
#endif //DEBUG

	execute(pIn, pOut);

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Result Frame 4 (output)" << endl <<
			"-- comm_rate:" << pOut->comm_rate << endl <<
			"-- s_name:" << pOut->s_name << endl <<
			"-- status:" << pOut->status << endl;
	cout << ">>> TRF4" << endl;
	m_coutLock.unlock();
#endif //DEBUG
}

// Call Trade Result Frame 5
void CTradeResultDB::DoTradeResultFrame5(
		const TTradeResultFrame5Input *pIn,
		TTradeResultFrame5Output *pOut)
{
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TRF5" << endl;
	cout << "- Trade Result Frame 5 (input)" << endl <<
			"-- broker_id: " << pIn->broker_id << endl <<
			"-- comm_amount: " << pIn->comm_amount << endl <<
			"-- st_completed_id: " << pIn->st_completed_id << endl <<
			"-- trade_dts: " << pIn->trade_dts.year << "-" <<
					pIn->trade_dts.month << "-" <<
					pIn->trade_dts.day << " " <<
					pIn->trade_dts.hour << ":" <<
					pIn->trade_dts.minute << ":" <<
					pIn->trade_dts.second << endl <<
			"-- trade_id: " << pIn->trade_id << endl <<
			"-- trade_price: " << pIn->trade_price << endl;
	m_coutLock.unlock();
#endif //DEBUG

	execute(pIn, pOut);

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Result Frame 5 (output)" << endl <<
			"-- stats:" << pOut->status << endl;
	cout << ">>> TRF5" << endl;
	m_coutLock.unlock();
#endif //DEBUG
}

// Call Trade Result Frame 6
void CTradeResultDB::DoTradeResultFrame6(
		const TTradeResultFrame6Input *pIn,
		TTradeResultFrame6Output *pOut)
{
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TRF6" << endl;
	cout << "- Trade Result Frame 6 (input)" << endl <<
			"-- acct_id: " << pIn->acct_id << endl <<
			"-- due_date: " << pIn->due_date.year << "-" <<
					pIn->due_date.month << "-" << pIn->due_date.day <<
					" " << pIn->due_date.hour << ":" <<
					pIn->due_date.minute << ":" <<
					pIn->due_date.second << endl <<
			"-- s_name: " << pIn->s_name << endl <<
			"-- se_amount: " << pIn->se_amount << endl <<
			"-- trade_dts: " << pIn->trade_dts.year << "-" <<
					pIn->trade_dts.month << "-" <<
					pIn->trade_dts.day << " " <<
					pIn->trade_dts.hour << ":" <<
					pIn->trade_dts.minute << ":" <<
					pIn->trade_dts.second << endl <<
			"-- trade_id: " << pIn->trade_id << endl <<
			"-- trade_is_cash: " << pIn->trade_is_cash << endl <<
			"-- trade_qty: " << pIn->trade_qty << endl <<
			"-- type_name: " << pIn->type_name << endl;
	m_coutLock.unlock();
#endif //DEBUG

	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	m_coutLock.lock();
	cout << "Trade Result Frame 6 (output)" << endl <<
			"- acct_bal:" << pOut->acct_bal << endl <<
			"- status:" << pOut->status << endl;
	cout << ">>> TRF6" << endl;
	m_coutLock.unlock();
#endif //DEBUG
}
