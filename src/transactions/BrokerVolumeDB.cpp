/*
 * BrokerVolumeDB.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 13 July 2006
 */

#include <transactions.h>

using namespace TPCE;

// Constructor
CBrokerVolumeDB::CBrokerVolumeDB(CDBConnection *pDBConn)
: CTxnBaseDB(pDBConn)
{
}

// Destructor
CBrokerVolumeDB::~CBrokerVolumeDB()
{
}

// Call Broker Volume Frame 1
void CBrokerVolumeDB::DoBrokerVolumeFrame1(const TBrokerVolumeFrame1Input *pIn,
		TBrokerVolumeFrame1Output *pOut)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osBrokers;
	int i = 0;
	osBrokers << pIn->broker_list[i];

	for ( i = 1; 0 != strcmp( pIn->broker_list[i], "") ; i++)
	{
		osBrokers << ", " << m_Txn->esc(pIn->broker_list[i]);
	}

	ostringstream osCall;
	osCall << "select * from BrokerVolumeFrame1('{" << osBrokers.str() <<
			"}','" << pIn->sector_name <<
			"') as (b_name varchar, sum double precision)";

	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	// stored procedure can return an empty result set by design
	result::const_iterator c = R.begin();

	i = 0;	
	for ( c; c != R.end(); ++c )
	{
		strcpy(pOut->broker_name[i], c[0].c_str());
		pOut->volume[i] = c[1].as(double());
		
		i++;
	}
 	pOut->list_len = i;
	pOut->status = CBaseTxnErr::SUCCESS;

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Broker Volume Frame 1 (input)"<<endl
	    <<"- broker_list: "<<osBrokers.str()<<endl
	    <<"- sector name: "<<pIn->sector_name<<endl;
	cout<<"Broker Volume Frame 1 (output)"<<endl
	    <<"- list_len: "<<pOut->list_len<<endl
	    <<"- broker_name[0]: "<<pOut->broker_name[0]<<endl
	    <<"- volume[0]: "<<pOut->volume[0]<<endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG
}


void CBrokerVolumeDB::Cleanup(void* pException)
{
}
