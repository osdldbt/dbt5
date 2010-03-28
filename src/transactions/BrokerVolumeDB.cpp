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
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< BVF1" << endl;
	cout << "- Broker Volume Frame 1 (input)" << endl <<
		"-- broker_list: " << osBrokers.str() << endl <<
		"-- sector name: " << pIn->sector_name << endl;
	m_coutLock.unlock();
#endif // DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);
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
