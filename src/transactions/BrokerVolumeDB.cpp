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
	pthread_t pid = pthread_self();
	int i;
	cout << pid << " <<< BVF1" << endl;
	cout << pid << " - Broker Volume Frame 1 (input)" << endl;
	for (i = 0; i < max_broker_list_len; i++)
		cout << pid << " -- broker_list[" << i << "]: " <<
				pIn->broker_list[i] << endl;
	cout << pid << " -- sector name: " << pIn->sector_name << endl;
#endif // DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	cout << pid << " - Broker Volume Frame 1 (output)" << endl <<
			pid << " -- list_len: " << pOut->list_len << endl;
	for (i = 0; i < pOut->list_len; i ++) {
		cout << pid << " -- broker_name[" << i << "]: " <<
						pOut->broker_name[i] << endl <<
				pid << " -- volume[" << i << "]: " << pOut->volume[i] << endl;
	}
	cout << pid << " >>> BVF1" << endl;
#endif // DEBUG
}
