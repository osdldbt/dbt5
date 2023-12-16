/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 21 July 2006
 */

#include "MarketFeedDB.h"

CMarketFeedDB::CMarketFeedDB(CDBConnection *pDBConn, bool verbose = false)
: CTxnBaseDB(pDBConn), m_Verbose(verbose)
{
	m_pid = syscall(SYS_gettid);
}

// Call Market Feed Frame 1
void
CMarketFeedDB::DoMarketFeedFrame1(const TMarketFeedFrame1Input *pIn,
		TMarketFeedFrame1Output *pOut, CSendToMarketInterface *pMarketExchange)
{
	if (m_Verbose) {
		cout << m_pid << " <<< MFF1" << endl
			 << m_pid << " - Market Feed Frame 1 (input)" << endl
			 << m_pid << " -- max_feed_len: " << max_feed_len << endl
			 << m_pid << " -- status_submitted: "
			 << pIn->StatusAndTradeType.status_submitted << endl
			 << m_pid << " -- type_limit_buy: "
			 << pIn->StatusAndTradeType.type_limit_buy << endl
			 << m_pid << " -- type_limit_sell: "
			 << pIn->StatusAndTradeType.type_limit_sell << endl
			 << m_pid << " -- type_stop_loss: "
			 << pIn->StatusAndTradeType.type_stop_loss << endl;
	}

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setRepeatableRead();
	execute(pIn, pOut, pMarketExchange);
	commitTransaction();

	if (m_Verbose) {
		cout << m_pid << " - Market Feed Frame 1 (output)" << endl
			 << m_pid << " -- send_len: " << pOut->send_len << endl
			 << m_pid << " >>> MFF1" << endl;
	}
}
