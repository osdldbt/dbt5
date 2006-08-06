/*
 * DataMaintenanceDB.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 17 July 2006
 */

#include <transactions.h>

using namespace TPCE;

// Constructor
CDataMaintenanceDB::CDataMaintenanceDB(CDBConnection *pDBConn)
: CTxnBaseDB(pDBConn)
{
}

// Destructor
CDataMaintenanceDB::~CDataMaintenanceDB()
{
}

// Call Data Maintenance Frame 1
void CDataMaintenanceDB::DoDataMaintenanceFrame1(PDataMaintenanceFrame1Input pFrame1Input, PDataMaintenanceFrame1Output pFrame1Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from DataMaintenanceFrame1("<<pFrame1Input->add_flag<<"::smallint,"<<pFrame1Input->c_id<<"::ident_t,"
			<<pFrame1Input->co_id<<"::ident_t,"<<pFrame1Input->day_of_month<<"::smallint,'"<<pFrame1Input->symbol
			<<"'::char(15),'"<<pFrame1Input->table_name<<"'::char(18),'"<<pFrame1Input->tx_id
			<<"'::char(4))"; //as (status smallint)";

	BeginTxn();
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;"); // Isolation level required by Clause 7.4.1.3
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	if (R.empty()) 
	{
		throw logic_error("TradeLookupFrame1: empty result set");
	}
	result::const_iterator c = R.begin();
 	pFrame1Output->status = c[0].as(int());

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Data Maintenance Frame 1 (input)"<<endl
	    <<"- add_flag: "<<pFrame1Input->add_flag<<endl
	    <<"- c_id: "<<pFrame1Input->c_id<<endl
	    <<"- co_id: "<<pFrame1Input->co_id<<endl
	    <<"- day_of_month: "<<pFrame1Input->day_of_month<<endl
	    <<"- symbol: "<<pFrame1Input->symbol<<endl
	    <<"- table_name: "<<pFrame1Input->table_name<<endl
	    <<"- tx_id name: "<<pFrame1Input->tx_id<<endl;
	cout<<"Data Maintenance Frame 1 (output)"<<endl
	    <<"- status: "<<pFrame1Output->status<<endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG
	
#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}


void CDataMaintenanceDB::Cleanup(void* pException)
{
}
