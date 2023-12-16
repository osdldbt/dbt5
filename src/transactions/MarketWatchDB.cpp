/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 13 July 2006
 */

#include "MarketWatchDB.h"

CMarketWatchDB::CMarketWatchDB(CDBConnection *pDBConn, bool verbose = false)
: CTxnBaseDB(pDBConn), m_Verbose(verbose)
{
	m_pid = syscall(SYS_gettid);
}

// Call Market Watch Frame 1
void
CMarketWatchDB::DoMarketWatchFrame1(
		const TMarketWatchFrame1Input *pIn, TMarketWatchFrame1Output *pOut)
{
	if (m_Verbose) {
		cout << m_pid << " <<< MWF1" << endl
			 << m_pid << " - Market Watch Frame 1 (input)" << endl
			 << m_pid << " -- acct_id: " << pIn->acct_id << endl
			 << m_pid << " -- cust_id: " << pIn->c_id << endl
			 << m_pid << " -- ending_co_id: " << pIn->ending_co_id << endl
			 << m_pid << " -- industry_name: " << pIn->industry_name
			 << " (5% used)" << endl
			 << m_pid << " -- starting_co_id: " << pIn->starting_co_id
			 << " (used only when industry_name is used)" << endl;
	}

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);

	commitTransaction();

	if (m_Verbose) {
		cout << m_pid << " - Market Watch Frame 1 (output)" << endl
			 << m_pid << " -- pct_change: " << pOut->pct_change << endl
			 << m_pid << " >>> MWF1" << endl;
	}
}
