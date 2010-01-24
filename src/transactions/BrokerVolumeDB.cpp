/*
 * BrokerVolumeDB.cpp
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *
 * 13 July 2006
 */

#include <transactions.h>

using namespace TPCE;

// Call Broker Volume Frame 1
void CBrokerVolumeDB::DoBrokerVolumeFrame1(const TBrokerVolumeFrame1Input *pIn,
		TBrokerVolumeFrame1Output *pOut)
{
#ifdef DEBUG
	cout << "BVF1" << endl;
#endif

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

	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	// stored procedure can return an empty result set by design
	result::const_iterator c = R.begin();

	vector<string> vAux;
	vector<string>::iterator p;

	Tokenize(c[i_broker_name].c_str(), vAux);
	i = 0;	
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strcpy(pOut->broker_name[i], (*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_volume].c_str(), vAux);
	i = 0;	
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->volume[i] = atof((*p).c_str());
		++i;
	}
	vAux.clear();

 	pOut->list_len = c[i_list_len].as(int());;
	pOut->status = c[i_status].as(int());

#ifdef DEBUG
	m_coutLock.lock();
	cout<<"Broker Volume Frame 1 (input)"<<endl
	    <<"- broker_list: "<<osBrokers.str()<<endl
	    <<"- sector name: "<<pIn->sector_name<<endl;
	cout<<"Broker Volume Frame 1 (output)"<<endl
	    <<"- list_len: "<<pOut->list_len<<endl
	    <<"- broker_name[0]: "<<pOut->broker_name[0]<<endl
	    <<"- volume[0]: "<<pOut->volume[0]<<endl;
	m_coutLock.unlock();
#endif // DEBUG
}
