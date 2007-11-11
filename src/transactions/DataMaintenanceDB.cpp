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
void CDataMaintenanceDB::DoDataMaintenanceFrame1(
		const TDataMaintenanceFrame1Input *pIn,
		TDataMaintenanceFrame1Output *pOut)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "SELECT * FROM DataMaintenanceFrame1(" <<
			pIn->acct_id << ", " <<
			pIn->c_id << ", " <<
			pIn->co_id << ", " <<
			pIn->day_of_month << ", '" <<
			pIn->symbol << "', '" <<
			pIn->table_name << "', '" <<
			pIn->tx_id << "', " <<
			pIn->vol_incr << ")";

	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	if (R.empty()) 
	{
		throw logic_error("TradeLookupFrame1: empty result set");
	}
	result::const_iterator c = R.begin();
 	pOut->status = c[0].as(int());

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout << "Data Maintenance Frame 1 (input)" << endl <<
			"- c_id: " << pIn->c_id << endl <<
			"- co_id: " << pIn->co_id << endl <<
			"- day_of_month: " << pIn->day_of_month << endl <<
			"- symbol: " << pIn->symbol << endl <<
			"- table_name: " << pIn->table_name << endl <<
			"- tx_id name: " << pIn->tx_id << endl <<
			"- vol_incr: " << pIn->vol_incr << endl;
	cout << "Data Maintenance Frame 1 (output)" << endl <<
			"- status: " << pOut->status << endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG
}


void CDataMaintenanceDB::Cleanup(void* pException)
{
}
