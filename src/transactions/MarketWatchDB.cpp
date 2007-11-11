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
void CMarketWatchDB::DoMarketWatchFrame1(const TMarketWatchFrame1Input *pIn,
		TMarketWatchFrame1Output *pOut)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from MarketWatchFrame1("<<pIn->acct_id<<","<<pIn->c_id<<","<<
			pIn->ending_co_id<<",'"<<pIn->industry_name<<"', "<<
			pIn->starting_co_id<<") as (status smallint, pct_change double precision)";

	BeginTxn();
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;"); // Isolation level required by Clause 7.4.1.3
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		//throw logic_error("TradeLookupFrame1: empty result set");
		cout<<"warning: empty result set at DoMarketWatchFrame1"<<endl;
	}
	else
	{
		result::const_iterator c = R.begin();

		pOut->status = c[0].as(int());
		pOut->pct_change = c[1].as(double());
	}

	if (pOut->status == 0)	// status ok
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
	    <<"- acct_id: "<<pIn->acct_id<<endl
	    <<"- cust_id: "<<pIn->c_id<<endl
	    <<"- ending_co_id: "<<pIn->ending_co_id<<endl
	    <<"- industry_name: "<<pIn->industry_name<<endl
	    <<"- starting_co_id: "<<pIn->starting_co_id<<endl;
	cout<<"Market Watch Frame 1 (output)"<<endl
	    <<"- status: "<<pOut->status<<endl
	    <<"- pct_change: "<<pOut->pct_change<<endl;
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
