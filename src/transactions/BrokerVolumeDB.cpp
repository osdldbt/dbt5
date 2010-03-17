/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * 13 July 2006
 */

#include "BrokerVolumeDB.h"

// Call Broker Volume Frame 1
void CBrokerVolumeDB::DoBrokerVolumeFrame1(const TBrokerVolumeFrame1Input *pIn,
		TBrokerVolumeFrame1Output *pOut)
{
	ostringstream osBrokers;
	int i = 0;

	osBrokers << pIn->broker_list[i];
	for (i = 1; pIn->broker_list[i][0] != '\0'; i++) {
		osBrokers << ", " << escape(pIn->broker_list[i]);
	}

	ostringstream osCall;
	osCall << "SELECT * FROM BrokerVolumeFrame1('{" <<
			osBrokers.str() << "}','" <<
			pIn->sector_name << "')";
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< BVF1" << endl;
	cout << "*** " << osCall.str() << endl;
	cout << "- Broker Volume Frame 1 (input)" << endl <<
		"-- broker_list: " << osBrokers.str() << endl <<
		"-- sector name: " << pIn->sector_name << endl;
	m_coutLock.unlock();
#endif // DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(osCall.str(), pOut);
	commitTransaction();

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Broker Volume Frame 1 (output)" << endl <<
			"-- status: " << pOut->status << endl <<
			"-- list_len: " << pOut->list_len << endl;
	for (i = 0; i < pOut->list_len; i ++) {
		cout << "-- broker_name[" << i << "]: " << pOut->broker_name[i] <<
				endl <<
				"-- volume[" << i << "]: " << pOut->volume[i] << endl;
	}
	cout << ">>> BVF1" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}
