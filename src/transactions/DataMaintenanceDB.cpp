/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * 17 July 2006
 */

#include "DataMaintenanceDB.h"

// Call Data Maintenance Frame 1
void CDataMaintenanceDB::DoDataMaintenanceFrame1(
		const TDataMaintenanceFrame1Input *pIn,
		TDataMaintenanceFrame1Output *pOut)
{
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< DMF1" << endl;
	cout << "*** " << osCall.str() << endl;
	cout << "- Data Maintenance Frame 1 (input)" << endl <<
			"-- c_id: " << pIn->c_id << endl <<
			"-- co_id: " << pIn->co_id << endl <<
			"-- day_of_month: " << pIn->day_of_month << endl <<
			"-- symbol: " << pIn->symbol << endl <<
			"-- table_name: " << pIn->table_name << endl <<
			"-- tx_id name: " << pIn->tx_id << endl <<
			"-- vol_incr: " << pIn->vol_incr << endl;
	m_coutLock.unlock();
#endif // DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Data Maintenance Frame 1 (output)" << endl <<
			"-- status: " << pOut->status << endl;
	cout << ">>> DMF1" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}
