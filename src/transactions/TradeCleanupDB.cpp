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
void CTradeCleanupDB::DoTradeCleanupFrame1(
		const TTradeCleanupFrame1Input *pIn,
		TTradeCleanupFrame1Output *pOut)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from TradeCleanupFrame1('" <<
			pIn->st_canceled_id << "'::char(4),'" <<
			pIn->st_pending_id << "'::char(4),'" <<
			pIn->st_submitted_id << "'::char(4)," <<
			pIn->start_trade_id << ")";

	BeginTxn();
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	if (R.empty()) 
	{
		//throw logic_error("TradeCleanupFrame1: empty result set");
		cerr<<"warning: empty result set at DoTradeCleanupFrame1"<<endl;
		// Should this be set to success?
 		pOut->status = CBaseTxnErr::SUCCESS;
		return;
	}
	result::const_iterator c = R.begin();
 	pOut->status = c[0].as(int());

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Cleanup Frame 1 (input)"<<endl
	    <<"- st_canceled_id: "<<pIn->st_canceled_id<<endl
	    <<"- st_pending_id: "<<pIn->st_pending_id<<endl
	    <<"- st_submitted_id: "<<pIn->st_submitted_id<<endl
	    <<"- trade_id: "<<pIn->start_trade_id<<endl;
	cout<<"Trade Cleanup Frame 1 (output)"<<endl
	    <<"- status: "<<pOut->status<<endl;
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
