/*
 * TradeOrderDB.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 03 July 2006
 */

#include <transactions.h>

using namespace TPCE;

// Constructor
CTradeOrderDB::CTradeOrderDB(CDBConnection *pDBConn)
: CTxnBaseDB(pDBConn)
{
}

// Destructor
CTradeOrderDB::~CTradeOrderDB()
{
}

// Call Trade Order Frame 1
void CTradeOrderDB::DoTradeOrderFrame1(const TTradeOrderFrame1Input *pIn,
		TTradeOrderFrame1Output *pOut)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from TradeOrderFrame1(" << pIn->acct_id <<
			") as (acct_name varchar, broker_name varchar, cust_f_name "
			"varchar, cust_id IDENT_T, cust_l_name varchar, cust_tier "
			"smallint, tax_id varchar, tax_status smallint);";

	// start transaction but not commit in this frame
	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		//throw logic_error("TradeOrderFrame1: empty result set");
		cout<<"warning: empty result set at DoTradeOrderFrame1"<<endl;
		return;
	}
	result::const_iterator c = R.begin();
	
	strcpy(pOut->acct_name, c[0].c_str());
	strcpy(pOut->broker_name, c[1].c_str());
	strcpy(pOut->cust_f_name, c[2].c_str());
	pOut->cust_id = c[3].as(int());
	strcpy(pOut->cust_l_name, c[4].c_str());
	pOut->cust_tier = c[5].as(int());
	strcpy(pOut->tax_id, c[6].c_str());
	pOut->tax_status = c[7].as(int());

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Order Frame 1 (input)"<<endl
	    <<"- acct_id: "<<pIn->acct_id<<endl;
	cout<<"Trade Order Frame 1 (output)"<<endl
	    <<"- acct_name: "<<pOut->acct_name<<endl
	    <<"- broker_name: "<<pOut->broker_name<<endl
	    <<"- cust_f_name: "<<pOut->cust_f_name<<endl
	    <<"- cust_id: "<<pOut->cust_id<<endl
	    <<"- cust_l_name: "<<pOut->cust_l_name<<endl
	    <<"- cust_tier: "<<pOut->cust_tier<<endl
	    <<"- tax_id: "<<pOut->tax_id<<endl
	    <<"- tax_status: "<<pOut->tax_status<<endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG
}

// Call Trade Order Frame 2
void CTradeOrderDB::DoTradeOrderFrame2(const TTradeOrderFrame2Input *pIn,
		TTradeOrderFrame2Output *pOut)
{
#if defined(COMPILE_PLSQL_FUNCTION)
	
	ostringstream osCall;
	osCall << "select TradeOrderFrame2("<< pIn->acct_id << ",'" <<
			m_Txn->esc(pIn->exec_f_name) << "','"  <<
			m_Txn->esc(pIn->exec_l_name) << "','" <<
			pIn->exec_tax_id<<"');";

	// we are inside a transaction
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		//throw logic_error("TradeOrderFrame2: empty result set");
		cout<<"warning: empty result set at DoTradeOrderFrame2"<<endl;
		RollbackTxn();
		// Should this set to SUCCESS?
		pOut->status = CBaseTxnErr::SUCCESS;
		return;
	}
	result::const_iterator c = R.begin();
	
	pOut->status = CBaseTxnErr::SUCCESS;
}

// Call Trade Order Frame 3
void CTradeOrderDB::DoTradeOrderFrame3(const TTradeOrderFrame3Input *pIn,
		TTradeOrderFrame3Output *pOut)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from TradeOrderFrame3(" << pIn->acct_id <<
			"::IDENT_T," << pIn->cust_id << "::IDENT_T," <<
			pIn->cust_tier << "::smallint," << pIn->is_lifo <<
			"::smallint,'" << pIn->issue << "'::char(6),'" <<
			pIn->st_pending_id << "'::char(4),'" <<
			pIn->st_submitted_id << "'::char(4)," <<
			pIn->tax_status << "::smallint," <<
			pIn->trade_qty << "::S_QTY_T,'" <<
			pIn->trade_type_id << "'::char(3)," <<
			pIn->type_is_margin << "::smallint,'" <<
			m_Txn->esc(pIn->co_name) << "'::varchar," <<
			pIn->requested_price << "::S_PRICE_T,'" <<
			pIn->symbol << "'::varchar) as (comp_name varchar, "
			"requested_price S_PRICE_T, symb_name varchar, buy_value "
			"BALANCE_T, charge_amount VALUE_T, comm_rate S_PRICE_T, "
			"cust_assets BALANCE_T, market_price S_PRICE_T, sec_name varchar, "
			"sell_value BALANCE_T, status_id char(4), tax_amount VALUE_T,"
			"type_is_market smallint, type_is_sell smallint);";

	// we are inside a transaction
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		//throw logic_error("TradeOrderFrame3: empty result set");
		cout<<"warning: empty result set at DoTradeOrderFrame3"<<endl;
		return;
	}
	result::const_iterator c = R.begin();
	
	strcpy(pOut->co_name, c[0].c_str());
	pOut->requested_price = c[1].as(double());
	strcpy(pOut->symbol, c[2].c_str());
	pOut->buy_value = c[3].as(double());
	pOut->charge_amount = c[4].as(double());
	pOut->comm_rate = c[5].as(double());
	pOut->cust_assets = c[6].as(double());
	pOut->market_price = c[7].as(double());
	strcpy(pOut->s_name, c[8].c_str());
	pOut->sell_value = c[9].as(double());
	strcpy(pOut->status_id, c[10].c_str());
	pOut->tax_amount = c[11].as(double());
	pOut->type_is_market = c[12].as(int());
	pOut->type_is_sell = c[13].as(int());

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Order Frame 3 (input)"<<endl
	    <<"- acct_id: "<<pIn->acct_id<<endl
	    <<"- cust_id: "<<pIn->cust_id<<endl
	    <<"- cust_tier: "<<pIn->cust_tier<<endl
	    <<"- is_lifo: "<<pIn->is_lifo<<endl
	    <<"- issue: "<<pIn->issue<<endl
	    <<"- st_pending_id: "<<pIn->st_pending_id<<endl
	    <<"- st_submitted_id: "<<pIn->st_submitted_id<<endl
	    <<"- tax_status: "<<pIn->tax_status<<endl
	    <<"- trade_qty: "<<pIn->trade_qty<<endl
	    <<"- trade_type_id: "<<pIn->trade_type_id<<endl
	    <<"- type_is_margin: "<<pIn->type_is_margin<<endl
	    <<"- co_name: "<<pIn->co_name<<endl
	    <<"- requested_price: "<<pIn->requested_price<<endl
	    <<"- symbol: "<<pIn->symbol<<endl;
	cout<<"Trade Order Frame 3 (output)"<<endl
	    <<"- co_name: "<<pOut->co_name<<endl
	    <<"- requested_price: "<<pOut->requested_price<<endl
	    <<"- symbol: "<<pOut->symbol<<endl
	    <<"- buy_value: "<<pOut->buy_value<<endl
	    <<"- charge_amount: "<<pOut->charge_amount<<endl
	    <<"- comm_rate: "<<pOut->comm_rate<<endl
	    <<"- cust_assets: "<<pOut->cust_assets<<endl
	    <<"- market_price: "<<pOut->market_price<<endl
	    <<"- s_name: "<<pOut->s_name<<endl
	    <<"- sell_value: "<<pOut->sell_value<<endl
	    <<"- status_id: "<<pOut->status_id<<endl
	    <<"- tax_amount: "<<pOut->tax_amount<<endl
	    <<"- type_is_market: "<<pOut->type_is_market<<endl
	    <<"- type_is_sell: "<<pOut->type_is_sell<<endl;
	m_coutLock.ReleaseLock();
#endif //DEBUG
}

// Call Trade Order Frame 4
void CTradeOrderDB::DoTradeOrderFrame4(const TTradeOrderFrame4Input *pIn,
		TTradeOrderFrame4Output *pOut)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select TradeOrderFrame4(" << pIn->acct_id <<
			"::ident_t," << pIn->charge_amount << "::value_t," <<
			pIn->comm_amount << "::value_t,'" <<
			m_Txn->esc(pIn->exec_name) << "'::char(6)," <<
			pIn->is_cash << "::smallint," << pIn->is_lifo <<
			"::smallint," << pIn->requested_price << "::s_price_t,'" <<
			pIn->status_id << "'::char(4),'" << pIn->symbol <<
			"'::varchar," << pIn->trade_qty << "::s_qty_t,'" <<
			pIn->trade_type_id << "'::char(3)," <<
			pIn->type_is_market << "::smallint)";

	// we are inside a transaction
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		//throw logic_error("TradeOrderFrame4: empty result set");
		cout<<"warning: empty result set at DoTradeOrderFrame4"<<endl;
		// Should this be set to SUCCESS?
		pOut->status = CBaseTxnErr::SUCCESS;
		return;
	}
	result::const_iterator c = R.begin();

	pOut->trade_id = c[0].as(int());
	pOut->status = CBaseTxnErr::SUCCESS;

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Order Frame 4 (input)"<<endl
	    <<"- acct_id: "<<pIn->acct_id<<endl
	    <<"- charge_amount: "<<pIn->charge_amount<<endl
	    <<"- comm_amount: "<<pIn->comm_amount<<endl
	    <<"- exec_name: "<<pIn->exec_name<<endl
	    <<"- is_cash: "<<pIn->is_cash<<endl
	    <<"- is_lifo: "<<pIn->is_lifo<<endl
	    <<"- requested_price: "<<pIn->requested_price<<endl
	    <<"- status_id: "<<pIn->status_id<<endl
	    <<"- symbol: "<<pIn->symbol<<endl
	    <<"- trade_qty: "<<pIn->trade_qty<<endl
	    <<"- trade_type_id: "<<pIn->trade_type_id<<endl
	    <<"- type_is_market: "<<pIn->type_is_market<<endl;
	cout<<"Trade Order Frame 4 (output)"<<endl
	    <<"- trade_id: "<<pOut->trade_id<<endl;
	m_coutLock.ReleaseLock();
#endif //DEBUG
}

// Call Trade Order Frame 5
void CTradeOrderDB::DoTradeOrderFrame5(TTradeOrderFrame5Output *pOut)
{
	// rollback the transaction we are inside
	RollbackTxn();
	pOut->status = CBaseTxnErr::ROLLBACK;
}

// Call Trade Order Frame 6
void CTradeOrderDB::DoTradeOrderFrame6(TTradeOrderFrame6Output *pOut)
{
	// commit the transaction we are inside
	CommitTxn();
	pOut->status = CBaseTxnErr::SUCCESS;
}

// Clean-up
void CTradeOrderDB::Cleanup(void* pException)
{
}
