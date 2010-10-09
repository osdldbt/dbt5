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
		const TDataMaintenanceFrame1Input *pIn)
{
#ifdef DEBUG
	pthread_t pid = pthread_self();
	cout << pid << " <<< DMF1" << endl;
	cout << pid << " - Data Maintenance Frame 1 (input)" << endl <<
			pid << " -- c_id: " << pIn->c_id << endl <<
			pid << " -- co_id: " << pIn->co_id << endl <<
			pid << " -- day_of_month: " << pIn->day_of_month << endl <<
			pid << " -- symbol: " << pIn->symbol << endl <<
			pid << " -- table_name: " << pIn->table_name << endl <<
			pid << " -- tx_id name: " << pIn->tx_id << endl <<
			pid << " -- vol_incr: " << pIn->vol_incr << endl;
#endif // DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn);
	commitTransaction();

#ifdef DEBUG
	cout << pid << " - Data Maintenance Frame 1 (output)" << endl;
	cout << pid << " >>> DMF1" << endl;
#endif // DEBUG
}
