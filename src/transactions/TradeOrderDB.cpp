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
void CTradeOrderDB::DoTradeOrderFrame1(PTradeOrderFrame1Input pFrame1Input, PTradeOrderFrame1Output pFrame1Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from TradeOrderFrame1("<<pFrame1Input->acct_id<<") as (acct_name varchar,"
			"broker_name varchar, cust_f_name varchar, cust_id IDENT_T,"
			"cust_l_name varchar, cust_tier smallint, tax_id varchar, tax_status smallint);";

	// start transaction but not commit in this frame
	BeginTxn();
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		throw logic_error("TradeOrderFrame1: empty result set");
	}
	result::const_iterator c = R.begin();
	
	sprintf(pFrame1Output->acct_name, "%s", c[0].c_str());
	sprintf(pFrame1Output->broker_name, "%s", c[1].c_str());
	sprintf(pFrame1Output->cust_f_name, "%s", c[2].c_str());
	pFrame1Output->cust_id = c[3].as(int());
	sprintf(pFrame1Output->cust_l_name, "%s", c[4].c_str());
	pFrame1Output->cust_tier = c[5].as(int());
	sprintf(pFrame1Output->tax_id, "%s", c[6].c_str());
	pFrame1Output->tax_status = c[7].as(int());

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Order Frame 1 (input)"<<endl
	    <<"- acct_id: "<<pFrame1Input->acct_id<<endl;
	cout<<"Trade Order Frame 1 (output)"<<endl
	    <<"- acct_name: "<<pFrame1Output->acct_name<<endl
	    <<"- broker_name: "<<pFrame1Output->broker_name<<endl
	    <<"- cust_f_name: "<<pFrame1Output->cust_f_name<<endl
	    <<"- cust_id: "<<pFrame1Output->cust_id<<endl
	    <<"- cust_l_name: "<<pFrame1Output->cust_l_name<<endl
	    <<"- cust_tier: "<<pFrame1Output->cust_tier<<endl
	    <<"- tax_id: "<<pFrame1Output->tax_id<<endl
	    <<"- tax_status: "<<pFrame1Output->tax_status<<endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG

#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}

// Call Trade Order Frame 2
void CTradeOrderDB::DoTradeOrderFrame2(PTradeOrderFrame2Input pFrame2Input, PTradeOrderFrame2Output pFrame2Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)
	
	ostringstream osCall;
	osCall << "select TradeOrderFrame2("<<pFrame2Input->acct_id<<",'"<<pFrame2Input->exec_f_name<<"','"
			<<pFrame2Input->exec_l_name<<"','"<<pFrame2Input->exec_tax_id<<"');";

	// we are inside a transaction
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		throw logic_error("TradeOrderFrame2: empty result set");
	}
	result::const_iterator c = R.begin();
	
	pFrame2Output->bad_permission = c[0].as(int());

#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}

// Call Trade Order Frame 3
void CTradeOrderDB::DoTradeOrderFrame3(PTradeOrderFrame3Input pFrame3Input, PTradeOrderFrame3Output pFrame3Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from TradeOrderFrame3("<<pFrame3Input->acct_id<<"::IDENT_T,"<<pFrame3Input->cust_id<<"::IDENT_T,"
			<<pFrame3Input->cust_tier<<"::smallint,"<<pFrame3Input->is_lifo<<"::smallint,'"
			<<pFrame3Input->issue<<"'::char(6),'"<<pFrame3Input->st_pending_id<<"'::char(4),'"
			<<pFrame3Input->st_submitted_id<<"'::char(4),"<<pFrame3Input->tax_status<<"::smallint,"
			<<pFrame3Input->trade_qty<<"::S_QTY_T,'"<<pFrame3Input->trade_type_id<<"'::char(3),"
			<<pFrame3Input->type_is_margin<<"::smallint,'"<<pFrame3Input->co_name<<"'::varchar,"
			<<pFrame3Input->requested_price<<"::S_PRICE_T,'"<<pFrame3Input->symbol<<"'::varchar) as "
			"(comp_name varchar, requested_price S_PRICE_T, symb_name varchar, buy_value BALANCE_T,"
			"charge_amount VALUE_T, comm_rate S_PRICE_T, cust_assets BALANCE_T, market_price S_PRICE_T,"
			"sec_name varchar, sell_value BALANCE_T, status_id char(4), tax_amount VALUE_T,"
			"type_is_market smallint, type_is_sell smallint);";

	// we are inside a transaction
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		throw logic_error("TradeOrderFrame3: empty result set");
	}
	result::const_iterator c = R.begin();
	
	sprintf(pFrame3Output->co_name, "%s", c[0].c_str());
	pFrame3Output->requested_price = c[1].as(double());
	sprintf(pFrame3Output->symbol, "%s", c[2].c_str());
	pFrame3Output->buy_value = c[3].as(double());
	pFrame3Output->charge_amount = c[4].as(double());
	pFrame3Output->comm_rate = c[5].as(double());
	pFrame3Output->cust_assets = c[6].as(double());
	pFrame3Output->market_price = c[7].as(double());
	sprintf(pFrame3Output->s_name, "%s", c[8].c_str());
	pFrame3Output->sell_value = c[9].as(double());
	sprintf(pFrame3Output->status_id, "%s", c[10].c_str());
	pFrame3Output->tax_amount = c[11].as(double());
	pFrame3Output->type_is_market = c[12].as(int());
	pFrame3Output->type_is_sell = c[13].as(int());

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Order Frame 3 (input)"<<endl
	    <<"- acct_id: "<<pFrame3Input->acct_id<<endl
	    <<"- cust_id: "<<pFrame3Input->cust_id<<endl
	    <<"- cust_tier: "<<pFrame3Input->cust_tier<<endl
	    <<"- is_lifo: "<<pFrame3Input->is_lifo<<endl
	    <<"- issue: "<<pFrame3Input->issue<<endl
	    <<"- st_pending_id: "<<pFrame3Input->st_pending_id<<endl
	    <<"- st_submitted_id: "<<pFrame3Input->st_submitted_id<<endl
	    <<"- tax_status: "<<pFrame3Input->tax_status<<endl
	    <<"- trade_qty: "<<pFrame3Input->trade_qty<<endl
	    <<"- trade_type_id: "<<pFrame3Input->trade_type_id<<endl
	    <<"- type_is_margin: "<<pFrame3Input->type_is_margin<<endl
	    <<"- co_name: "<<pFrame3Input->co_name<<endl
	    <<"- requested_price: "<<pFrame3Input->requested_price<<endl
	    <<"- symbol: "<<pFrame3Input->symbol<<endl;
	cout<<"Trade Order Frame 3 (output)"<<endl
	    <<"- co_name: "<<pFrame3Output->co_name<<endl
	    <<"- requested_price: "<<pFrame3Output->requested_price<<endl
	    <<"- symbol: "<<pFrame3Output->symbol<<endl
	    <<"- buy_value: "<<pFrame3Output->buy_value<<endl
	    <<"- charge_amount: "<<pFrame3Output->charge_amount<<endl
	    <<"- comm_rate: "<<pFrame3Output->comm_rate<<endl
	    <<"- cust_assets: "<<pFrame3Output->cust_assets<<endl
	    <<"- market_price: "<<pFrame3Output->market_price<<endl
	    <<"- s_name: "<<pFrame3Output->s_name<<endl
	    <<"- sell_value: "<<pFrame3Output->sell_value<<endl
	    <<"- status_id: "<<pFrame3Output->status_id<<endl
	    <<"- tax_amount: "<<pFrame3Output->tax_amount<<endl
	    <<"- type_is_market: "<<pFrame3Output->type_is_market<<endl
	    <<"- type_is_sell: "<<pFrame3Output->type_is_sell<<endl;
	m_coutLock.ReleaseLock();
#endif //DEBUG

#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}

// Call Trade Order Frame 4
void CTradeOrderDB::DoTradeOrderFrame4(PTradeOrderFrame4Input pFrame4Input, PTradeOrderFrame4Output pFrame4Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select TradeOrderFrame4("<<pFrame4Input->acct_id<<"::ident_t,"<<pFrame4Input->charge_amount<<"::value_t,"
			<<pFrame4Input->comm_amount<<"::value_t,'"<<pFrame4Input->exec_name<<"'::char(6),"
			<<pFrame4Input->is_cash<<"::smallint,"<<pFrame4Input->is_lifo<<"::smallint,"<<pFrame4Input->requested_price
			<<"::s_price_t,'"<<pFrame4Input->status_id<<"'::char(4),'"
			<<pFrame4Input->symbol<<"'::varchar,"<<pFrame4Input->trade_qty<<"::s_qty_t,'"
			<<pFrame4Input->trade_type_id<<"'::char(3),"<<pFrame4Input->type_is_market<<"::smallint)";

	// we are inside a transaction
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		throw logic_error("TradeOrderFrame4: empty result set");
	}
	result::const_iterator c = R.begin();

	pFrame4Output->trade_id = c[0].as(int());

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Order Frame 4 (input)"<<endl
	    <<"- acct_id: "<<pFrame4Input->acct_id<<endl
	    <<"- charge_amount: "<<pFrame4Input->charge_amount<<endl
	    <<"- comm_amount: "<<pFrame4Input->comm_amount<<endl
	    <<"- exec_name: "<<pFrame4Input->exec_name<<endl
	    <<"- is_cash: "<<pFrame4Input->is_cash<<endl
	    <<"- is_lifo: "<<pFrame4Input->is_lifo<<endl
	    <<"- requested_price: "<<pFrame4Input->requested_price<<endl
	    <<"- status_id: "<<pFrame4Input->status_id<<endl
	    <<"- symbol: "<<pFrame4Input->symbol<<endl
	    <<"- trade_qty: "<<pFrame4Input->trade_qty<<endl
	    <<"- trade_type_id: "<<pFrame4Input->trade_type_id<<endl
	    <<"- type_is_market: "<<pFrame4Input->type_is_market<<endl;
	cout<<"Trade Order Frame 4 (output)"<<endl
	    <<"- trade_id: "<<pFrame4Output->trade_id<<endl;
	m_coutLock.ReleaseLock();
#endif //DEBUG

#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}

// Call Trade Order Frame 5
void CTradeOrderDB::DoTradeOrderFrame5(PTradeOrderFrame5Output pFrame5Output)
{
	// rollback the transaction we are inside
	RollbackTxn();
	//pFrame5Output->status = CBaseTxnErr::ROLLBACK;
}

// Call Trade Order Frame 6
void CTradeOrderDB::DoTradeOrderFrame6(PTradeOrderFrame6Output pFrame6Output)
{
	// commit the transaction we are inside
	CommitTxn();
	//pFrame6Output->status = CBaseTxnErr::SUCCESS;
}

// Clean-up
void CTradeOrderDB::Cleanup(void* pException)
{
}
