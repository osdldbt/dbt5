/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 17 July 2006
 */

#include "DataMaintenanceDB.h"

CDataMaintenanceDB::CDataMaintenanceDB(
		CDBConnection *pDBConn, bool verbose = false)
: CTxnBaseDB(pDBConn), m_Verbose(verbose)
{
	m_pid = syscall(SYS_gettid);
}

// Call Data Maintenance Frame 1
void
CDataMaintenanceDB::DoDataMaintenanceFrame1(
		const TDataMaintenanceFrame1Input *pIn)
{
	if (m_Verbose) {
		cout << m_pid << " <<< DMF1" << endl
			 << m_pid << " - Data Maintenance Frame 1 (input)" << endl
			 << m_pid << " -- c_id: " << pIn->c_id << endl
			 << m_pid << " -- co_id: " << pIn->co_id << endl
			 << m_pid << " -- day_of_month: " << pIn->day_of_month << endl
			 << m_pid << " -- symbol: " << pIn->symbol << endl
			 << m_pid << " -- table_name: " << pIn->table_name << endl
			 << m_pid << " -- tx_id name: " << pIn->tx_id << endl
			 << m_pid << " -- vol_incr: " << pIn->vol_incr << endl;
	}

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn);
	commitTransaction();

	if (m_Verbose) {
		cout << m_pid << " - Data Maintenance Frame 1 (output)" << endl
			 << m_pid << " >>> DMF1" << endl;
	}
}
