/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 18 July 2006
 */

#include "TradeCleanupDB.h"

CTradeCleanupDB::CTradeCleanupDB(CDBConnection *pDBConn, bool verbose = false)
: CTxnBaseDB(pDBConn), m_Verbose(verbose)
{
	m_pid = syscall(SYS_gettid);
}

// Call Trade Cleanup Frame 1
void
CTradeCleanupDB::DoTradeCleanupFrame1(const TTradeCleanupFrame1Input *pIn)
{
	if (m_Verbose) {
		cout << m_pid << " - Trade Cleanup Frame 1 (input)" << endl
			 << m_pid << " -- st_canceled_id: " << pIn->st_canceled_id << endl
			 << m_pid << " -- st_pending_id: " << pIn->st_pending_id << endl
			 << m_pid << " -- st_submitted_id: " << pIn->st_submitted_id
			 << endl
			 << m_pid << " -- trade_id: " << pIn->start_trade_id << endl;
	}

	startTransaction();
	execute(pIn);
	commitTransaction();

	if (m_Verbose) {
		cout << m_pid << " - Trade Cleanup Frame 1 (output)" << endl
			 << m_pid << " >>> TCF1" << endl;
	}
}
