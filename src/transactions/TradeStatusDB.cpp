/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * 13 June 2006
 */

#include "TradeStatusDB.h"

// Call Trade Status Frame 1
void CTradeStatusDB::DoTradeStatusFrame1(const TTradeStatusFrame1Input *pIn,
		TTradeStatusFrame1Output *pOut)
{
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TSF1" << endl;
	cout << "- Trade Status Frame 1 (input)" << endl <<
			"-- acct_id: " << pIn->acct_id << endl;
	m_coutLock.unlock();
#endif // DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Status Frame 1 (output)" << endl <<
			"-- cust_l_name: " << pOut->cust_l_name << endl <<
			"-- cust_f_name: " << pOut->cust_f_name << endl <<
			"-- broker_name: " << pOut->broker_name << endl;
	for (int i = 0; i < max_trade_status_len; i++) {
		cout << "-- charge[" << i << "]: " << pOut->charge[i] << endl <<
				"-- exec_name[" << i << "]: " << pOut->exec_name[i] << endl <<
				"-- ex_name[" << i << "]: " << pOut->ex_name[i] << endl <<
				"-- s_name[" << i << "]: " << pOut->s_name[i] << endl <<
				"-- status_name[" << i << "]: " << pOut->status_name[i] <<
						endl <<
				"-- symbol[" << i << "]: " << pOut->symbol[i] << endl <<
				"-- trade_dts[" << i << "]: " << pOut->trade_dts[i].year <<
						"-" << pOut->trade_dts[i].month << "-" <<
						pOut->trade_dts[i].day << " " <<
						pOut->trade_dts[i].hour << ":" <<
						pOut->trade_dts[i].minute << ":" <<
						pOut->trade_dts[i].second << endl <<
				"-- trade_id[" << i << "]: " << pOut->trade_id[i] << endl <<
				"-- trade_qty[" << i << "]: " << pOut->trade_qty[i] << endl <<
				"-- type_name[" << i << "]: " << pOut->type_name[i] << endl;
	}
	cout << ">>> TSF1" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}
