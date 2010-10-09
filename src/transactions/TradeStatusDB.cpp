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
	pthread_t pid = pthread_self();
	cout << pid << " <<< TSF1" << endl;
	cout << pid << " - Trade Status Frame 1 (input)" << endl <<
			pid << " -- acct_id: " << pIn->acct_id << endl;
#endif // DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	cout << pid << " - Trade Status Frame 1 (output)" << endl <<
			pid << " -- cust_l_name: " << pOut->cust_l_name << endl <<
			pid << " -- cust_f_name: " << pOut->cust_f_name << endl <<
			pid << " -- broker_name: " << pOut->broker_name << endl;
	for (int i = 0; i < max_trade_status_len; i++) {
		cout << pid << " -- charge[" << i << "]: " << pOut->charge[i] << endl <<
				pid << " -- exec_name[" << i << "]: " << pOut->exec_name[i] <<
						endl <<
				pid << " -- ex_name[" << i << "]: " << pOut->ex_name[i] <<
						endl <<
				pid << " -- s_name[" << i << "]: " << pOut->s_name[i] << endl <<
				pid << " -- status_name[" << i << "]: " <<
						pOut->status_name[i] << endl <<
				pid << " -- symbol[" << i << "]: " << pOut->symbol[i] << endl <<
				pid << " -- trade_dts[" << i << "]: " <<
						pOut->trade_dts[i].year << "-" <<
						pOut->trade_dts[i].month << "-" <<
						pOut->trade_dts[i].day << " " <<
						pOut->trade_dts[i].hour << ":" <<
						pOut->trade_dts[i].minute << ":" <<
						pOut->trade_dts[i].second << endl <<
				pid << " -- trade_id[" << i << "]: " << pOut->trade_id[i] <<
						endl <<
				pid << " -- trade_qty[" << i << "]: " << pOut->trade_qty[i] <<
						endl <<
				pid << " -- type_name[" << i << "]: " << pOut->type_name[i] <<
						endl;
	}
	cout << pid << " >>> TSF1" << endl;
#endif // DEBUG
}
