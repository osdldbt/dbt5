/*
 * MarketWatchDB.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 13 July 2006
 */

#include <transactions.h>

using namespace TPCE;

// Constructor
CMarketWatchDB::CMarketWatchDB(CDBConnection *pDBConn)
: CTxnBaseDB(pDBConn)
{
}

// Destructor
CMarketWatchDB::~CMarketWatchDB()
{
}

// Call Market Watch Frame 1
void CMarketWatchDB::DoMarketWatchFrame1(PMarketWatchFrame1Input pFrame1Input, PMarketWatchFrame1Output pFrame1Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from MarketWatchFrame1("<<pFrame1Input->acct_id<<","<<pFrame1Input->c_id<<","<<
			pFrame1Input->ending_co_id<<",'"<<pFrame1Input->industry_name<<"', "<<
			pFrame1Input->starting_co_id<<") as (status smallint, pct_change double precision)";

	BeginTxn();
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		throw logic_error("TradeLookupFrame1: empty result set");
	}
	result::const_iterator c = R.begin();

	pFrame1Output->status = c[0].as(int());
	pFrame1Output->pct_change = c[1].as(double());

	if (pFrame1Output->status == 0)	// status ok
	{
		CommitTxn();
	}
	else
	{
		RollbackTxn();
	}

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Market Watch Frame 1 (input)"<<endl
	    <<"- acct_id: "<<pFrame1Input->acct_id<<endl
	    <<"- cust_id: "<<pFrame1Input->c_id<<endl
	    <<"- ending_co_id: "<<pFrame1Input->ending_co_id<<endl
	    <<"- industry_name: "<<pFrame1Input->industry_name<<endl
	    <<"- starting_co_id: "<<pFrame1Input->starting_co_id<<endl;
	cout<<"Market Watch Frame 1 (output)"<<endl
	    <<"- status: "<<pFrame1Output->status<<endl
	    <<"- pct_change: "<<pFrame1Output->pct_change<<endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG
	
#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}


void CMarketWatchDB::Cleanup(void* pException)
{
}
