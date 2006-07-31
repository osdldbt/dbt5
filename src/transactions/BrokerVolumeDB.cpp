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
void CBrokerVolumeDB::DoBrokerVolumeFrame1(PBrokerVolumeFrame1Input pFrame1Input, PBrokerVolumeFrame1Output pFrame1Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osBrokers;
	int i = 0;
	osBrokers << pFrame1Input->broker_list[i];

	for ( i = 1; 0 != strcmp( pFrame1Input->broker_list[i], "") ; i++)
	{
		osBrokers << ", " << m_Txn->esc(pFrame1Input->broker_list[i]);
	}

	ostringstream osCall;
	osCall << "select * from BrokerVolumeFrame1('{"<<osBrokers.str()<<"}','"<<pFrame1Input->sector_name<<"') as "
			"(b_name varchar, sum double precision)";

	BeginTxn();
	result R( m_Txn->exec( osCall.str() ) );

	// stored procedure can return an empty result set by design
	result::const_iterator c = R.begin();

	i = 0;	
	for ( c; c != R.end(); ++c )
	{
		sprintf(pFrame1Output->broker_name[i], "%s", c[0].c_str());
		pFrame1Output->volume[i] = c[1].as(double());
		
		i++;
	}
 	pFrame1Output->list_len = i;

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Broker Volume Frame 1 (input)"<<endl
	    <<"- broker_list: "<<osBrokers.str()<<endl
	    <<"- sector name: "<<pFrame1Input->sector_name<<endl;
	cout<<"Broker Volume Frame 1 (output)"<<endl
	    <<"- list_len: "<<pFrame1Output->list_len<<endl
	    <<"- broker_name[0]: "<<pFrame1Output->broker_name[0]<<endl
	    <<"- volume[0]: "<<pFrame1Output->volume[0]<<endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG
	
#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}


void CBrokerVolumeDB::Cleanup(void* pException)
{
}
