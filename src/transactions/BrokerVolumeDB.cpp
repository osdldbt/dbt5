/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 13 July 2006
 */

#include "BrokerVolumeDB.h"

CBrokerVolumeDB::CBrokerVolumeDB(CDBConnection *pDBConn, bool verbose = false)
: CTxnBaseDB(pDBConn), m_Verbose(verbose)
{
	m_pid = syscall(SYS_gettid);
}

// Call Broker Volume Frame 1
void
CBrokerVolumeDB::DoBrokerVolumeFrame1(
		const TBrokerVolumeFrame1Input *pIn, TBrokerVolumeFrame1Output *pOut)
{
	if (m_Verbose) {
		cout << m_pid << " <<< BVF1" << endl
			 << m_pid << " - Broker Volume Frame 1 (input)" << endl;
		for (int i = 0; i < max_broker_list_len; i++)
			cout << m_pid << " -- broker_list[" << i
				 << "]: " << pIn->broker_list[i] << endl;
		cout << m_pid << " -- sector name: " << pIn->sector_name << endl;
	}

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);
	commitTransaction();

	if (m_Verbose) {
		cout << m_pid << " - Broker Volume Frame 1 (output)" << endl
			 << m_pid << " -- list_len: " << pOut->list_len << endl;
		for (int i = 0; i < pOut->list_len; i++) {
			cout << m_pid << " -- broker_name[" << i
				 << "]: " << pOut->broker_name[i] << endl
				 << m_pid << " -- volume[" << i << "]: " << pOut->volume[i]
				 << endl;
		}
		cout << m_pid << " >>> BVF1" << endl;
	}
}
