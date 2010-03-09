/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * 13 July 2006
 */

#include <transactions.h>

using namespace TPCE;

// Call Broker Volume Frame 1
void CBrokerVolumeDB::DoBrokerVolumeFrame1(const TBrokerVolumeFrame1Input *pIn,
		TBrokerVolumeFrame1Output *pOut)
{
	ostringstream osBrokers;
	int i = 0;

	enum bvf1 { i_broker_name=0, i_list_len, i_status, i_volume };

	osBrokers << pIn->broker_list[i];
	for (i = 1; pIn->broker_list[i][0] != '\0'; i++) {
		osBrokers << ", " << m_Txn->esc(pIn->broker_list[i]);
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

	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	// stored procedure can return an empty result set by design
	result::const_iterator c = R.begin();

	pOut->list_len = c[i_list_len].as(int());;

	vector<string> vAux;
	vector<string>::iterator p;

	TokenizeSmart(c[i_broker_name].c_str(), vAux);
	i = 0;	
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->broker_name[i], (*p).c_str(), cB_NAME_len);
		++i;
	}
	check_count(pOut->list_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_volume].c_str(), vAux);
	i = 0;	
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->volume[i] = atof((*p).c_str());
		++i;
	}
	check_count(pOut->list_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->status = c[i_status].as(int());

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
