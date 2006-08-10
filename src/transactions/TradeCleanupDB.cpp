/*
 * TradeCleanupDB.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 18 July 2006
 */

#include <transactions.h>

using namespace TPCE;

// Constructor
CTradeCleanupDB::CTradeCleanupDB(CDBConnection *pDBConn)
: CTxnBaseDB(pDBConn)
{
}

// Destructor
CTradeCleanupDB::~CTradeCleanupDB()
{
}

// Call Trade Cleanup Frame 1
void CTradeCleanupDB::DoTradeCleanupFrame1(PTradeCleanupFrame1Input pFrame1Input, PTradeCleanupFrame1Output pFrame1Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from TradeCleanupFrame1('"<<pFrame1Input->st_canceled_id<<"'::char(4),'"
			<<pFrame1Input->st_pending_id<<"'::char(4),'"<<pFrame1Input->st_submitted_id<<
			"'::char(4),"<<pFrame1Input->start_trade_id<<")";

	BeginTxn();
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	if (R.empty()) 
	{
		//throw logic_error("TradeCleanupFrame1: empty result set");
		cerr<<"warning: empty result set at DoTradeCleanupFrame1"<<endl;
	}
	result::const_iterator c = R.begin();
 	pFrame1Output->status = c[0].as(int());

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Cleanup Frame 1 (input)"<<endl
	    <<"- st_canceled_id: "<<pFrame1Input->st_canceled_id<<endl
	    <<"- st_pending_id: "<<pFrame1Input->st_pending_id<<endl
	    <<"- st_submitted_id: "<<pFrame1Input->st_submitted_id<<endl
	    <<"- trade_id: "<<pFrame1Input->start_trade_id<<endl;
	cout<<"Trade Cleanup Frame 1 (output)"<<endl
	    <<"- status: "<<pFrame1Output->status<<endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG
	
#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}


void CTradeCleanupDB::Cleanup(void* pException)
{
}
