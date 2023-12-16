/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 13 June 2006
 */

#include "TradeStatusDB.h"

CTradeStatusDB::CTradeStatusDB(CDBConnection *pDBConn, bool verbose = false)
: CTxnBaseDB(pDBConn), m_Verbose(verbose)
{
	m_pid = syscall(SYS_gettid);
}

// Call Trade Status Frame 1
void
CTradeStatusDB::DoTradeStatusFrame1(
		const TTradeStatusFrame1Input *pIn, TTradeStatusFrame1Output *pOut)
{
	if (m_Verbose) {
		cout << m_pid << " <<< TSF1" << endl
			 << m_pid << " - Trade Status Frame 1 (input)" << endl
			 << m_pid << " -- acct_id: " << pIn->acct_id << endl;
	}

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);
	commitTransaction();

	if (m_Verbose) {
		cout << m_pid << " - Trade Status Frame 1 (output)" << endl
			 << m_pid << " -- cust_l_name: " << pOut->cust_l_name << endl
			 << m_pid << " -- cust_f_name: " << pOut->cust_f_name << endl
			 << m_pid << " -- broker_name: " << pOut->broker_name << endl;
		for (int i = 0; i < max_trade_status_len; i++) {
			cout << m_pid << " -- charge[" << i << "]: " << pOut->charge[i]
				 << endl
				 << m_pid << " -- exec_name[" << i
				 << "]: " << pOut->exec_name[i] << endl
				 << m_pid << " -- ex_name[" << i << "]: " << pOut->ex_name[i]
				 << endl
				 << m_pid << " -- s_name[" << i << "]: " << pOut->s_name[i]
				 << endl
				 << m_pid << " -- status_name[" << i
				 << "]: " << pOut->status_name[i] << endl
				 << m_pid << " -- symbol[" << i << "]: " << pOut->symbol[i]
				 << endl
				 << m_pid << " -- trade_dts[" << i
				 << "]: " << pOut->trade_dts[i].year << "-"
				 << pOut->trade_dts[i].month << "-" << pOut->trade_dts[i].day
				 << " " << pOut->trade_dts[i].hour << ":"
				 << pOut->trade_dts[i].minute << ":"
				 << pOut->trade_dts[i].second << endl
				 << m_pid << " -- trade_id[" << i << "]: " << pOut->trade_id[i]
				 << endl
				 << m_pid << " -- trade_qty[" << i
				 << "]: " << pOut->trade_qty[i] << endl
				 << m_pid << " -- type_name[" << i
				 << "]: " << pOut->type_name[i] << endl;
		}
		cout << m_pid << " >>> TSF1" << endl;
	}
}
