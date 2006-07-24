/*
 * TradeResultDB.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 07 July 2006
 */

#include <transactions.h>

using namespace TPCE;

// Constructor
CTradeResultDB::CTradeResultDB(CDBConnection *pDBConn)
: CTxnBaseDB(pDBConn)
{
}

// Destructor
CTradeResultDB::~CTradeResultDB()
{
}

// Call Trade Result Frame 1
void CTradeResultDB::DoTradeResultFrame1(PTradeResultFrame1Input pFrame1Input, PTradeResultFrame1Output pFrame1Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from TradeResultFrame1("<<pFrame1Input->trade_id<<") as (acct_id ident_t, charge value_t, "
		"hs_qty s_qty_t, is_lifo smallint, symbol char(15), trade_is_cash smallint, trade_qty s_qty_t, type_id char(3),"
		"type_is_market smallint, type_is_sell smallint, type_name char(12))";
//cout<<osCall.str()<<endl;

	// start transaction but not commit in this frame
	BeginTxn();
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		throw logic_error("TradeResultFrame1: empty result set");
	}
	result::const_iterator c = R.begin();
	
	pFrame1Output->acct_id = c[0].as(int());
	pFrame1Output->charge = c[1].as(double());
	pFrame1Output->hs_qty = c[2].as(int());
	pFrame1Output->is_lifo = c[3].as(int());
	sprintf(pFrame1Output->symbol, "%s", c[4].c_str());
	pFrame1Output->trade_is_cash = c[5].as(int());
	pFrame1Output->trade_qty = c[6].as(int());
	sprintf(pFrame1Output->type_id, "%s", c[7].c_str());
	pFrame1Output->type_is_market = c[8].as(int());
	pFrame1Output->type_is_sell = c[9].as(int());
	sprintf(pFrame1Output->type_name, "%s", c[10].c_str());

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Result Frame 1 (input)"<<endl
	    <<"- trade_id: "<<pFrame1Input->trade_id<<endl;
	cout<<"Trade Result Frame 1 (output)"<<endl
	    <<"- acct_name: "<<pFrame1Output->acct_id <<endl
	    <<"- charge: "<<pFrame1Output->charge<<endl
	    <<"- hs_qty: "<<pFrame1Output->hs_qty<<endl
	    <<"- is_lifo: "<<pFrame1Output->is_lifo<<endl
	    <<"- symbol: "<<pFrame1Output->symbol<<endl
	    <<"- trade_is_cash: "<<pFrame1Output->trade_is_cash<<endl
	    <<"- trade_qty: "<<pFrame1Output->trade_qty<<endl
	    <<"- type_id: "<<pFrame1Output->type_id<<endl
	    <<"- type_is_market: "<<pFrame1Output->type_is_market<<endl
	    <<"- type_is_sell: "<<pFrame1Output->type_is_sell<<endl
	    <<"- type_name: "<<pFrame1Output->type_name<<endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG

#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}

// Call Trade Result Frame 2
void CTradeResultDB::DoTradeResultFrame2(PTradeResultFrame2Input pFrame2Input, PTradeResultFrame2Output pFrame2Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)
	
	ostringstream osCall;
	osCall << "select * from TradeResultFrame2("<<pFrame2Input->acct_id<<"::IDENT_T,"<<pFrame2Input->hs_qty<<"::S_QTY_T,"
			<<pFrame2Input->is_lifo<<"::smallint,'"<<pFrame2Input->symbol<<"'::char(15),"
			<<pFrame2Input->trade_id<<"::TRADE_T,"<<pFrame2Input->trade_price<<"::S_PRICE_T,"
			<<pFrame2Input->trade_qty<<"::S_QTY_T,"<<pFrame2Input->type_is_sell<<"::smallint) as "
			"(broker_id IDENT_T, buy_value numeric(12,2), cust_id IDENT_T, sell_value numeric(12,2),"
			"tax_status smallint, trade_dts_year double precision, trade_dts_month double precision,"
			"trade_dts_day double precision, trade_dts_hour double precision,"
			"trade_dts_minute double precision, trade_dts_second double precision)";

	// we are inside a transaction
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		throw logic_error("TradeResultFrame2: empty result set");
	}
	result::const_iterator c = R.begin();

	pFrame2Output->broker_id = c[0].as(int());
	pFrame2Output->buy_value = c[1].as(double());
	pFrame2Output->cust_id = c[2].as(int());
	pFrame2Output->sell_value = c[3].as(double());
	pFrame2Output->tax_status = c[4].as(int());
	pFrame2Output->trade_dts.year = c[5].as(int());
	pFrame2Output->trade_dts.month = c[6].as(int());
	pFrame2Output->trade_dts.day = c[7].as(int());
	pFrame2Output->trade_dts.hour = c[8].as(int());
	pFrame2Output->trade_dts.minute = c[9].as(int());
	pFrame2Output->trade_dts.second = int(c[10].as(double()));

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Result Frame 2 (input)"<<endl
	    <<"- acct_id:"<<pFrame2Input->acct_id<<endl
	    <<"- hs_qty:"<<pFrame2Input->hs_qty<<endl
	    <<"- is_lifo:"<<pFrame2Input->is_lifo<<endl
	    <<"- symbol:"<<pFrame2Input->symbol<<endl
	    <<"- trade_id:"<<pFrame2Input->trade_id<<endl
	    <<"- trade_price:"<<pFrame2Input->trade_price<<endl
	    <<"- trade_qty:"<<pFrame2Input->trade_qty<<endl
	    <<"- type_is_sell:"<<pFrame2Input->type_is_sell<<endl;
	cout<<"Trade Result Frame 2 (output)"<<endl
	    <<"- broker_id: "<<pFrame2Output->broker_id<<endl
	    <<"- buy_value: "<<pFrame2Output->buy_value<<endl
	    <<"- cust_id: "<<pFrame2Output->cust_id<<endl
	    <<"- sell_value: "<<pFrame2Output->sell_value<<endl
	    <<"- tax_status: "<<pFrame2Output->tax_status<<endl
	    <<"- trade_dts: "<<pFrame2Output->trade_dts.year<<"-"<<pFrame2Output->trade_dts.month<<"-"
	    	<<pFrame2Output->trade_dts.day<<" "<<pFrame2Output->trade_dts.hour<<":"
	    	<<pFrame2Output->trade_dts.minute<<":"<<pFrame2Output->trade_dts.second<<endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG

#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}

// Call Trade Result Frame 3
void CTradeResultDB::DoTradeResultFrame3(PTradeResultFrame3Input pFrame3Input, PTradeResultFrame3Output pFrame3Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from TradeResultFrame3("<<pFrame3Input->buy_value<<"::numeric(12,2),"<<pFrame3Input->cust_id<<
			"::IDENT_T,"<<pFrame3Input->sell_value<<"::numeric(12,2),"<<pFrame3Input->trade_id<<
			"::TRADE_T,"<<pFrame3Input->tax_amount<<"::VALUE_T)";

	// we are inside a transaction
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		throw logic_error("TradeResultFrame3: empty result set");
	}
	result::const_iterator c = R.begin();

	pFrame3Output->tax_amount = c[0].as(double());

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Result Frame 3 (input)"<<endl
	    <<"- buy_value:"<<pFrame3Input->buy_value<<endl
	    <<"- cust_id:"<<pFrame3Input->cust_id<<endl
	    <<"- sell_value:"<<pFrame3Input->sell_value<<endl
	    <<"- trade_id:"<<pFrame3Input->trade_id<<endl
	    <<"- tax_amount:"<<pFrame3Input->tax_amount<<endl;
	cout<<"Trade Result Frame 3 (output)"<<endl
	    <<"- tax_amount:"<<pFrame3Output->tax_amount<<endl;
	m_coutLock.ReleaseLock();
#endif //DEBUG

#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}

// Call Trade Result Frame 4
void CTradeResultDB::DoTradeResultFrame4(PTradeResultFrame4Input pFrame4Input, PTradeResultFrame4Output pFrame4Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from TradeResultFrame4("<<pFrame4Input->cust_id<<"::IDENT_T,'"<<pFrame4Input->symbol<<
			"'::char(15),"<<pFrame4Input->trade_qty<<"::S_QTY_T,'"<<pFrame4Input->type_id<<"'::char(3)) "
			" as (comm_rate numeric(5,2), s_name varchar)";

	// we are inside a transaction
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		throw logic_error("TradeResultFrame4: empty result set");
	}
	result::const_iterator c = R.begin();

	pFrame4Output->comm_rate = c[0].as(double());
	sprintf(pFrame4Output->s_name, "%s", c[1].c_str());

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Result Frame 4 (input)"<<endl
	    <<"- cust_id:"<<pFrame4Input->cust_id<<endl
	    <<"- symbol:"<<pFrame4Input->symbol<<endl
	    <<"- trade_qty:"<<pFrame4Input->trade_qty<<endl
	    <<"- type_id:"<<pFrame4Input->type_id<<endl;
	cout<<"Trade Result Frame 4 (output)"<<endl
	    <<"- comm_rate:"<<pFrame4Output->comm_rate<<endl
	    <<"- s_name:"<<pFrame4Output->s_name<<endl;
	m_coutLock.ReleaseLock();
#endif //DEBUG

#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}

// Call Trade Result Frame 5
void CTradeResultDB::DoTradeResultFrame5(PTradeResultFrame5Input pFrame5Input, PTradeResultFrame5Output pFrame5Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Result Frame 5 (input)"<<endl
	    <<"- broker_id:"<<pFrame5Input->broker_id<<endl
	    <<"- comm_amount:"<<pFrame5Input->comm_amount<<endl
	    <<"- st_completed_id:"<<pFrame5Input->st_completed_id<<endl
	    <<"- trade_dts: "<<pFrame5Input->trade_dts.year<<"-"<<pFrame5Input->trade_dts.month<<"-"
	    	<<pFrame5Input->trade_dts.day<<" "<<pFrame5Input->trade_dts.hour<<":"
	    	<<pFrame5Input->trade_dts.minute<<":"<<pFrame5Input->trade_dts.second<<endl
	    <<"- trade_id:"<<pFrame5Input->trade_id<<endl
	    <<"- trade_price:"<<pFrame5Input->trade_price<<endl;
	m_coutLock.ReleaseLock();
#endif //DEBUG

	ostringstream osCall;
	osCall << "select * from TradeResultFrame5("<<pFrame5Input->broker_id<<"::IDENT_T,"<<pFrame5Input->comm_amount<<
			"::numeric(5,2),'"<<pFrame5Input->st_completed_id<<"'::char(4),'"<<pFrame5Input->trade_dts.year<<
			"-"<<pFrame5Input->trade_dts.month<<"-"<<pFrame5Input->trade_dts.day<<" "<<pFrame5Input->trade_dts.hour<<
			":"<<pFrame5Input->trade_dts.minute<<":"<<pFrame5Input->trade_dts.second<<"'::timestamp,"
			<<pFrame5Input->trade_id<<"::IDENT_T,"<<pFrame5Input->trade_price<<"::S_PRICE_T);";

	// we are inside a transaction
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		throw logic_error("TradeResultFrame5: empty result set");
	}

#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION	
}

// Call Trade Result Frame 6
void CTradeResultDB::DoTradeResultFrame6(PTradeResultFrame6Input pFrame6Input, PTradeResultFrame6Output pFrame6Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)
	ostringstream osCall;
	osCall << "select * from TradeResultFrame6("<<pFrame6Input->acct_id<<"::IDENT_T, '"<<pFrame6Input->due_date.year<<
			"-"<<pFrame6Input->due_date.month<<"-"<<pFrame6Input->due_date.day<<" "<<pFrame6Input->due_date.hour<<
			":"<<pFrame6Input->due_date.minute<<":"<<pFrame6Input->due_date.second<<"'::timestamp,'"<<
			pFrame6Input->s_name<<"'::varchar, "<<pFrame6Input->se_amount<<"::VALUE_T,'"<<pFrame6Input->trade_dts.year<<
			"-"<<pFrame6Input->trade_dts.month<<"-"<<pFrame6Input->trade_dts.day<<" "<<pFrame6Input->trade_dts.hour<<
			":"<<pFrame6Input->trade_dts.minute<<":"<<pFrame6Input->trade_dts.second<<"'::timestamp,"<<
			pFrame6Input->trade_id<<"::IDENT_T,"<<pFrame6Input->trade_is_cash<<"::smallint,"<<pFrame6Input->trade_qty<<
			"::S_QTY_T,'"<<pFrame6Input->type_name<<"'::char(12))";

	// we are inside a transaction
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	if (R.empty()) 
	{
		throw logic_error("TradeResultFrame6: empty result set");
	}
	result::const_iterator c = R.begin();

	pFrame6Output->acct_bal = c[0].as(double());

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Result Frame 6 (input)"<<endl
	    <<"- acct_id:"<<pFrame6Input->acct_id<<endl
	    <<"- due_date: "<<pFrame6Input->due_date.year<<"-"<<pFrame6Input->due_date.month<<"-"
	    	<<pFrame6Input->due_date.day<<" "<<pFrame6Input->due_date.hour<<":"
	    	<<pFrame6Input->due_date.minute<<":"<<pFrame6Input->due_date.second<<endl
	    <<"- s_name:"<<pFrame6Input->s_name<<endl
	    <<"- se_amount:"<<pFrame6Input->se_amount<<endl
	    <<"- trade_dts: "<<pFrame6Input->trade_dts.year<<"-"<<pFrame6Input->trade_dts.month<<"-"
	    	<<pFrame6Input->trade_dts.day<<" "<<pFrame6Input->trade_dts.hour<<":"
	    	<<pFrame6Input->trade_dts.minute<<":"<<pFrame6Input->trade_dts.second<<endl
	    <<"- trade_id:"<<pFrame6Input->trade_id<<endl
	    <<"- trade_is_cash:"<<pFrame6Input->trade_is_cash<<endl
	    <<"- trade_qty:"<<pFrame6Input->trade_qty<<endl
	    <<"- type_name:"<<pFrame6Input->type_name<<endl;
	cout<<"Trade Result Frame 6 (output)"<<endl
	    <<"- acct_bal:"<<pFrame6Output->acct_bal<<endl;
	m_coutLock.ReleaseLock();
#endif //DEBUG

#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION		
}

// Clean-up
void CTradeResultDB::Cleanup(void* pException)
{
}
