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
void CTradeResultDB::DoTradeResultFrame1(
		const TTradeResultFrame1Input *pIn,
		TTradeResultFrame1Output *pOut)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from TradeResultFrame1(" << pIn->trade_id <<
			") as (acct_id ident_t, charge value_t, hs_qty s_qty_t, is_lifo "
			"smallint, symbol char(15), trade_is_cash smallint, "
			"trade_qty s_qty_t, type_id char(3), type_is_market smallint, "
			"type_is_sell smallint, type_name char(12))";

	// start transaction but not commit in this frame
	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;");
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		//throw logic_error("TradeResultFrame1: empty result set");
		cerr<<"warning: empty result set at DoTradeResultFrame1"<<endl;
		pOut->status = CBaseTxnErr::SUCCESS;
		return;
	}
	result::const_iterator c = R.begin();
	
	pOut->acct_id = c[0].as(int());
	pOut->charge = c[1].as(double());
	pOut->hs_qty = c[2].as(int());
	pOut->is_lifo = c[3].as(int());
	strcpy(pOut->symbol, c[4].c_str());
	pOut->trade_is_cash = c[5].as(int());
	pOut->trade_qty = c[6].as(int());
	strcpy(pOut->type_id, c[7].c_str());
	pOut->type_is_market = c[8].as(int());
	pOut->type_is_sell = c[9].as(int());
	strcpy(pOut->type_name, c[10].c_str());

	pOut->status = CBaseTxnErr::SUCCESS;

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Result Frame 1 (input)"<<endl
	    <<"- trade_id: "<<pIn->trade_id<<endl;
	cout<<"Trade Result Frame 1 (output)"<<endl
	    <<"- acct_id: "<<pOut->acct_id <<endl
	    <<"- charge: "<<pOut->charge<<endl
	    <<"- hs_qty: "<<pOut->hs_qty<<endl
	    <<"- is_lifo: "<<pOut->is_lifo<<endl
	    <<"- symbol: "<<pOut->symbol<<endl
	    <<"- trade_is_cash: "<<pOut->trade_is_cash<<endl
	    <<"- trade_qty: "<<pOut->trade_qty<<endl
	    <<"- type_id: "<<pOut->type_id<<endl
	    <<"- type_is_market: "<<pOut->type_is_market<<endl
	    <<"- type_is_sell: "<<pOut->type_is_sell<<endl
	    <<"- type_name: "<<pOut->type_name<<endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG
}

// Call Trade Result Frame 2
void CTradeResultDB::DoTradeResultFrame2(
		const TTradeResultFrame2Input *pIn,
		TTradeResultFrame2Output *pOut)
{
#if defined(COMPILE_PLSQL_FUNCTION)
	
	ostringstream osCall;
	osCall << "select * from TradeResultFrame2(" << pIn->acct_id <<
			"::IDENT_T," << pIn->hs_qty << "::S_QTY_T," <<
			pIn->is_lifo << "::smallint,'" << pIn->symbol <<
			"'::char(15)," << pIn->trade_id << "::TRADE_T," <<
			pIn->trade_price << "::S_PRICE_T," <<
			pIn->trade_qty << "::S_QTY_T," <<
			pIn->type_is_sell << "::smallint) as (broker_id IDENT_T, "
			"buy_value numeric(12,2), cust_id IDENT_T, sell_value "
			"numeric(12,2), tax_status smallint, trade_dts_year double "
			"precision, trade_dts_month double precision, trade_dts_day double "
			"precision, trade_dts_hour double precision, trade_dts_minute "
			"double precision, trade_dts_second double precision)";

	// we are inside a transaction
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		//throw logic_error("TradeResultFrame2: empty result set");
		cerr<<"warning: empty result set at DoTradeResultFrame2"<<endl;
		return;
	}
	result::const_iterator c = R.begin();

	pOut->broker_id = c[0].as(int());
	pOut->buy_value = c[1].as(double());
	pOut->cust_id = c[2].as(int());
	pOut->sell_value = c[3].as(double());
	pOut->tax_status = c[4].as(int());
	pOut->trade_dts.year = c[5].as(int());
	pOut->trade_dts.month = c[6].as(int());
	pOut->trade_dts.day = c[7].as(int());
	pOut->trade_dts.hour = c[8].as(int());
	pOut->trade_dts.minute = c[9].as(int());
	pOut->trade_dts.second = int(c[10].as(double()));

	pOut->status = CBaseTxnErr::SUCCESS;

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Result Frame 2 (input)"<<endl
	    <<"- acct_id:"<<pIn->acct_id<<endl
	    <<"- hs_qty:"<<pIn->hs_qty<<endl
	    <<"- is_lifo:"<<pIn->is_lifo<<endl
	    <<"- symbol:"<<pIn->symbol<<endl
	    <<"- trade_id:"<<pIn->trade_id<<endl
	    <<"- trade_price:"<<pIn->trade_price<<endl
	    <<"- trade_qty:"<<pIn->trade_qty<<endl
	    <<"- type_is_sell:"<<pIn->type_is_sell<<endl;
	cout << "Trade Result Frame 2 (output)" << endl <<
		"- broker_id: " << pOut->broker_id << endl <<
		"- buy_value: " << pOut->buy_value << endl <<
		"- cust_id: " << pOut->cust_id << endl <<
		"- sell_value: " << pOut->sell_value << endl <<
		"- tax_status: " << pOut->tax_status << endl <<
		"- trade_dts: " << pOut->trade_dts.year << "-" <<
		pOut->trade_dts.month << "-" << pOut->trade_dts.day <<
		" " << pOut->trade_dts.hour << ":" <<
		pOut->trade_dts.minute << ":" <<
		pOut->trade_dts.second << endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG
}

// Call Trade Result Frame 3
void CTradeResultDB::DoTradeResultFrame3(
		const TTradeResultFrame3Input *pIn,
		TTradeResultFrame3Output *pOut)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from TradeResultFrame3(" << pIn->buy_value <<
			"::numeric(12,2)," << pIn->cust_id << "::IDENT_T," <<
			pIn->sell_value << "::numeric(12,2)," <<
			pIn->trade_id << "::TRADE_T)" << endl;

	// we are inside a transaction
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		//throw logic_error("TradeResultFrame3: empty result set");
		cerr<<"warning: empty result set at DoTradeResultFrame3"<<endl;
		return;
	}
	result::const_iterator c = R.begin();

	pOut->tax_amount = c[0].as(double());
	pOut->status = CBaseTxnErr::SUCCESS;

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Result Frame 3 (input)"<<endl
	    <<"- buy_value:"<<pIn->buy_value<<endl
	    <<"- cust_id:"<<pIn->cust_id<<endl
	    <<"- sell_value:"<<pIn->sell_value<<endl
	    <<"- trade_id:"<<pIn->trade_id<<endl;
	cout<<"Trade Result Frame 3 (output)"<<endl
	    <<"- tax_amount:"<<pOut->tax_amount<<endl;
	m_coutLock.ReleaseLock();
#endif //DEBUG
}

// Call Trade Result Frame 4
void CTradeResultDB::DoTradeResultFrame4(
		const TTradeResultFrame4Input *pIn,
		TTradeResultFrame4Output *pOut)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from TradeResultFrame4(" << pIn->cust_id <<
			"::IDENT_T,'" << pIn->symbol << "'::char(15)," <<
			pIn->trade_qty << "::S_QTY_T,'" << pIn->type_id <<
			"'::char(3)) as (comm_rate numeric(5,2), s_name varchar)";

	// we are inside a transaction
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		//throw logic_error("TradeResultFrame4: empty result set");
		cerr<<"warning: empty result set at DoTradeResultFrame4"<<endl;
		return;
	}
	result::const_iterator c = R.begin();

	pOut->comm_rate = c[0].as(double());
	strcpy(pOut->s_name, c[1].c_str());
	pOut->status = CBaseTxnErr::SUCCESS;

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Result Frame 4 (input)"<<endl
	    <<"- cust_id:"<<pIn->cust_id<<endl
	    <<"- symbol:"<<pIn->symbol<<endl
	    <<"- trade_qty:"<<pIn->trade_qty<<endl
	    <<"- type_id:"<<pIn->type_id<<endl;
	cout<<"Trade Result Frame 4 (output)"<<endl
	    <<"- comm_rate:"<<pOut->comm_rate<<endl
	    <<"- s_name:"<<pOut->s_name<<endl;
	m_coutLock.ReleaseLock();
#endif //DEBUG
}

// Call Trade Result Frame 5
void CTradeResultDB::DoTradeResultFrame5(
		const TTradeResultFrame5Input *pIn,
		TTradeResultFrame5Output *pOut)
{
#if defined(COMPILE_PLSQL_FUNCTION)

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout << "Trade Result Frame 5 (input)" << endl <<
			"- broker_id:" << pIn->broker_id << endl <<
			"- comm_amount:" << pIn->comm_amount << endl <<
			"- st_completed_id:" << pIn->st_completed_id << endl <<
			"- trade_dts: " << pIn->trade_dts.year << "-" <<
			pIn->trade_dts.month << "-" <<
			pIn->trade_dts.day << " " <<
			pIn->trade_dts.hour << ":" <<
			pIn->trade_dts.minute << ":" <<
			pIn->trade_dts.second << endl <<
			"- trade_id:" << pIn->trade_id << endl <<
			"- trade_price:" << pIn->trade_price << endl;
	m_coutLock.ReleaseLock();
#endif //DEBUG

	ostringstream osCall;
	osCall << "select * from TradeResultFrame5(" << pIn->broker_id <<
			"::IDENT_T," << pIn->comm_amount << "::numeric(5,2),'" <<
			pIn->st_completed_id << "'::char(4),'" <<
			pIn->trade_dts.year << "-" <<
			pIn->trade_dts.month << "-" <<
			pIn->trade_dts.day << " " <<
			pIn->trade_dts.hour << ":" <<
			pIn->trade_dts.minute << ":" <<
			pIn->trade_dts.second << "'::timestamp," <<
			pIn->trade_id << "::IDENT_T," <<
			pIn->trade_price << "::S_PRICE_T);";

	// we are inside a transaction
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty()) 
	{
		//throw logic_error("TradeResultFrame5: empty result set");
		cerr<<"warning: empty result set at DoTradeResultFrame5"<<endl;
	}
	pOut->status = CBaseTxnErr::SUCCESS;
}

// Call Trade Result Frame 6
void CTradeResultDB::DoTradeResultFrame6(
		const TTradeResultFrame6Input *pIn,
		TTradeResultFrame6Output *pOut)
{
#if defined(COMPILE_PLSQL_FUNCTION)
	ostringstream osCall;
	osCall << "select * from TradeResultFrame6(" << pIn->acct_id <<
			"::IDENT_T, '" << pIn->due_date.year << "-"<<
			pIn->due_date.month << "-" << pIn->due_date.day <<
			" " << pIn->due_date.hour << ":" <<
			pIn->due_date.minute << ":" <<
			pIn->due_date.second << "'::timestamp,'" <<
			m_Txn->esc(pIn->s_name) << "'::varchar, " <<
			pIn->se_amount << "::VALUE_T,'" <<
			pIn->trade_dts.year << "-" <<
			pIn->trade_dts.month << "-" <<
			pIn->trade_dts.day << " " <<
			pIn->trade_dts.hour << ":" <<
			pIn->trade_dts.minute << ":" <<
			pIn->trade_dts.second << "'::timestamp," <<
			pIn->trade_id << "::IDENT_T," <<
			pIn->trade_is_cash << "::smallint," <<
			pIn->trade_qty << "::S_QTY_T,'" <<
			pIn->type_name << "'::char(12))";

	// we are inside a transaction
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	if (R.empty()) 
	{
		//throw logic_error("TradeResultFrame6: empty result set");
		cerr<<"warning: empty result set at DoTradeResultFrame6"<<endl;
		return;
	}
	result::const_iterator c = R.begin();

	pOut->acct_bal = c[0].as(double());
	pOut->status = CBaseTxnErr::SUCCESS;

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout << "Trade Result Frame 6 (input)" << endl <<
			"- acct_id:" << pIn->acct_id << endl <<
			"- due_date: " << pIn->due_date.year << "-" <<
			pIn->due_date.month << "-" << pIn->due_date.day <<
			" " << pIn->due_date.hour << ":" <<
			pIn->due_date.minute << ":" <<
			pIn->due_date.second << endl <<
			"- s_name:" << pIn->s_name << endl <<
			"- se_amount:" << pIn->se_amount << endl <<
			"- trade_dts: " << pIn->trade_dts.year << "-" <<
			pIn->trade_dts.month << "-" <<
			pIn->trade_dts.day << " " <<
			pIn->trade_dts.hour << ":" <<
			pIn->trade_dts.minute << ":" <<
			pIn->trade_dts.second << endl <<
			"- trade_id:" << pIn->trade_id << endl <<
			"- trade_is_cash:" << pIn->trade_is_cash << endl <<
			"- trade_qty:" << pIn->trade_qty << endl <<
			"- type_name:" << pIn->type_name << endl;
	cout<<"Trade Result Frame 6 (output)"<<endl
	    <<"- acct_bal:"<<pOut->acct_bal<<endl;
	m_coutLock.ReleaseLock();
#endif //DEBUG
}

// Clean-up
void CTradeResultDB::Cleanup(void* pException)
{
}
