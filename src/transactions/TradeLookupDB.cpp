/*
 * TradeLookupDB.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 08 July 2006
 */

#include <transactions.h>

using namespace TPCE;

// Constructor
CTradeLookupDB::CTradeLookupDB(CDBConnection *pDBConn)
: CTxnBaseDB(pDBConn)
{
}

// Destructor
CTradeLookupDB::~CTradeLookupDB()
{
}

// Call Trade Lookup Frame 1
void CTradeLookupDB::DoTradeLookupFrame1(PTradeLookupFrame1Input pFrame1Input,
		PTradeLookupFrame1Output pFrame1Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osTrades;
	int i = 0;
	osTrades << pFrame1Input->trade_id[i];
	for ( i = 1; i < pFrame1Input->max_trades; i++)
	{
		osTrades << "," << pFrame1Input->trade_id[i];
	}

	ostringstream osCall;
	osCall << "select * from tradelookupframe1(" << pFrame1Input->max_trades <<
			",'{" << osTrades.str() << "}') as (BidPrice numeric(8,2), "
			"ExecName varchar, IsCash smallint, IsMarket smallint, TradePrice "
			"numeric(8,2), SettlementAmount numeric(10,2), "
			"SettleCashDueDateYear double precision, SettleCashDueDateMonth "
			"double precision, SettleCashDueDateDay double precision, "
			"SettleCashDueDateHour double precision, SettleCashDueDateMinute "
			"double precision, SettleCashDueDateSec double precision, "
			"SettlementCashType varchar, CashTransactionAmount numeric(10,2), "
			"CashTxnDtsYear double precision, CashTxnDtsMonth double "
			"precision, CashTxnDtsDay double precision, CashTxnDtsHour double "
			"precision, CashTxnDtsMinute double precision, CashTxnDtsSec "
			"double precision, CashTransactionName varchar, Dts1Year double "
			"precision, Dts1Month double precision, Dts1Day double precision, "
			"Dts1Hour double precision, Dts1Minute double precision, Dts1Sec "
			"double precision, TradeHistoryStatusID1 char(4), Dts2Year double "
			"precision, Dts2Month double precision, Dts2Day double precision, "
			"Dts2Hour double precision, Dts2Minute double precision, Dts2Sec "
			"double precision, TradeHistoryStatusID2 char(4), Dts3Year double "
			"precision, Dts3Month double precision, Dts3Day double precision, "
			"Dts3Hour double precision, Dts3Minute double precision, Dts3Sec "
			"double precision, TradeHistoryStatusID3 char(4))";

	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	if (R.empty()) 
	{
		//throw logic_error("TradeLookupFrame1: empty result set");
		cout<<"warning: empty result set at DoTradeLookupFrame1"<<endl;
	}
	result::const_iterator c = R.begin();

	i = 0;	
	for ( c; c != R.end(); ++c )
	{
		pFrame1Output->trade_info[i].bid_price = c[0].as(double());
		strcpy(pFrame1Output->trade_info[i].exec_name, c[1].c_str());
		pFrame1Output->trade_info[i].is_cash = c[2].as(int());
		pFrame1Output->trade_info[i].is_market = c[3].as(int());
		pFrame1Output->trade_info[i].trade_price = c[4].as(double());
		pFrame1Output->trade_info[i].settlement_amount = c[5].as(double());
		pFrame1Output->trade_info[i].settlement_cash_due_date.year =
				c[6].as(int());
		pFrame1Output->trade_info[i].settlement_cash_due_date.month =
				c[7].as(int());
		pFrame1Output->trade_info[i].settlement_cash_due_date.day =
				c[8].as(int());
		pFrame1Output->trade_info[i].settlement_cash_due_date.hour =
				c[9].as(int());
		pFrame1Output->trade_info[i].settlement_cash_due_date.minute =
				c[10].as(int());
		pFrame1Output->trade_info[i].settlement_cash_due_date.second =
				int(c[11].as(double()));
		strcpy(pFrame1Output->trade_info[i].settlement_cash_type, 
				c[12].c_str());
		pFrame1Output->trade_info[i].cash_transaction_amount =
				c[13].as(double());
		pFrame1Output->trade_info[i].cash_transaction_dts.year =
				c[14].as(int());
		pFrame1Output->trade_info[i].cash_transaction_dts.month =
				c[15].as(int());
		pFrame1Output->trade_info[i].cash_transaction_dts.day = c[16].as(int());
		pFrame1Output->trade_info[i].cash_transaction_dts.hour =
				c[17].as(int());
		pFrame1Output->trade_info[i].cash_transaction_dts.minute =
				c[18].as(int());
		pFrame1Output->trade_info[i].cash_transaction_dts.second =
				int(c[19].as(double()));
		strcpy(pFrame1Output->trade_info[i].cash_transaction_name,
				c[20].c_str());
		pFrame1Output->trade_info[i].trade_history_dts[0].year =
				c[21].as(int());
		pFrame1Output->trade_info[i].trade_history_dts[0].month =
				c[22].as(int());
		pFrame1Output->trade_info[i].trade_history_dts[0].day = c[23].as(int());
		pFrame1Output->trade_info[i].trade_history_dts[0].hour =
				c[24].as(int());
		pFrame1Output->trade_info[i].trade_history_dts[0].minute =
				c[25].as(int());
		pFrame1Output->trade_info[i].trade_history_dts[0].second =
				int(c[26].as(double()));
		strcpy(pFrame1Output->trade_info[i].trade_history_status_id[0],
				c[27].c_str());
		pFrame1Output->trade_info[i].trade_history_dts[1].year =
				c[28].as(int());
		pFrame1Output->trade_info[i].trade_history_dts[1].month =
				c[29].as(int());
		pFrame1Output->trade_info[i].trade_history_dts[1].day = c[30].as(int());
		pFrame1Output->trade_info[i].trade_history_dts[1].hour =
				c[31].as(int());
		pFrame1Output->trade_info[i].trade_history_dts[1].minute =
				c[32].as(int());
		pFrame1Output->trade_info[i].trade_history_dts[1].second =
				int(c[33].as(double()));
		strcpy(pFrame1Output->trade_info[i].trade_history_status_id[1],
				c[34].c_str());
		pFrame1Output->trade_info[i].trade_history_dts[2].year =
				c[35].as(int());
		pFrame1Output->trade_info[i].trade_history_dts[2].month =
				c[36].as(int());
		pFrame1Output->trade_info[i].trade_history_dts[2].day = c[37].as(int());
		pFrame1Output->trade_info[i].trade_history_dts[2].hour =
				c[38].as(int());
		pFrame1Output->trade_info[i].trade_history_dts[2].minute =
				c[39].as(int());
		pFrame1Output->trade_info[i].trade_history_dts[2].second =
				int(c[40].as(double()));
		strcpy(pFrame1Output->trade_info[i].trade_history_status_id[2],
				c[41].c_str());

		i++;
	}
 	pFrame1Output->num_found = i;
	pFrame1Output->status = CBaseTxnErr::SUCCESS;

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Lookup Frame 1 (input)"<<endl
	    <<"- max_trades: "<<pFrame1Input->max_trades<<endl
	    <<"- Trades: {"<<osTrades.str()<<"}"<<endl;
	cout << "Trade Lookup Frame 1 (output)"<< endl <<
			"- num_found: " << pFrame1Output->num_found << endl <<
			"- bid_price[0]: " << pFrame1Output->trade_info[0].bid_price <<
			endl <<
			"- exec_name[0]: " << pFrame1Output->trade_info[0].exec_name <<
			endl <<
			"- is_cash[0]: " << pFrame1Output->trade_info[0].is_cash << endl <<
			"- is_market[0]: " << pFrame1Output->trade_info[0].is_market <<
			endl <<
			"- trade_price[0]: " << pFrame1Output->trade_info[0].trade_price <<
			endl <<
			"- settlement_amount[0]: " <<
			pFrame1Output->trade_info[0].settlement_amount << endl <<
			"- settlement_cash_due_date[0]: " <<
			pFrame1Output->trade_info[0].settlement_cash_due_date.year << "-" <<
			pFrame1Output->trade_info[0].settlement_cash_due_date.month <<
			"-" << pFrame1Output->trade_info[0].settlement_cash_due_date.day <<
			" " << pFrame1Output->trade_info[0].settlement_cash_due_date.hour <<
			":" <<
			pFrame1Output->trade_info[0].settlement_cash_due_date.minute <<
			":" <<
			pFrame1Output->trade_info[0].settlement_cash_due_date.second <<
			endl <<
			"- settlement_cash_type[0]: " <<
			pFrame1Output->trade_info[0].settlement_cash_type << endl <<
			"- cash_transaction_amount[0]: " <<
			pFrame1Output->trade_info[0].cash_transaction_amount << endl <<
			"- cash_transaction_dts[0]: " <<
			pFrame1Output->trade_info[0].cash_transaction_dts.year << "-" <<
			pFrame1Output->trade_info[0].cash_transaction_dts.month << "-" <<
			pFrame1Output->trade_info[0].cash_transaction_dts.day << " "<<
			pFrame1Output->trade_info[0].cash_transaction_dts.hour << ":" <<
			pFrame1Output->trade_info[0].cash_transaction_dts.minute << ":" <<
			pFrame1Output->trade_info[0].cash_transaction_dts.second << endl <<
			"- cash_transaction_name[0]: " <<
			pFrame1Output->trade_info[0].cash_transaction_name << endl <<
			"- trade_history_dts[0][0]: " <<
			pFrame1Output->trade_info[0].trade_history_dts[0].year << "-" <<
			pFrame1Output->trade_info[0].trade_history_dts[0].month << "-" <<
			pFrame1Output->trade_info[0].trade_history_dts[0].day << " " <<
			pFrame1Output->trade_info[0].trade_history_dts[0].hour << ":" <<
			pFrame1Output->trade_info[0].trade_history_dts[0].minute << ":" <<
			pFrame1Output->trade_info[0].trade_history_dts[0].second << endl <<
			"- trade_history_status_id[0][0]: " <<
			pFrame1Output->trade_info[0].trade_history_status_id[0] << endl <<
			"- trade_history_dts[0][1]: " <<
			pFrame1Output->trade_info[0].trade_history_dts[1].year << "-" <<
			pFrame1Output->trade_info[0].trade_history_dts[1].month << "-" <<
			pFrame1Output->trade_info[0].trade_history_dts[1].day << " " <<
			pFrame1Output->trade_info[0].trade_history_dts[1].hour << ":" <<
			pFrame1Output->trade_info[0].trade_history_dts[1].minute << ":" <<
			pFrame1Output->trade_info[0].trade_history_dts[1].second << endl <<
			"- trade_history_status_id[0][1]: " <<
			pFrame1Output->trade_info[0].trade_history_status_id[1] << endl <<
			"- trade_history_dts[0][2]: " <<
			pFrame1Output->trade_info[0].trade_history_dts[2].year << "-" <<
			pFrame1Output->trade_info[0].trade_history_dts[2].month << "-" <<
			pFrame1Output->trade_info[0].trade_history_dts[2].day << " " <<
			pFrame1Output->trade_info[0].trade_history_dts[2].hour << ":" <<
			pFrame1Output->trade_info[0].trade_history_dts[2].minute << ":" <<
			pFrame1Output->trade_info[0].trade_history_dts[2].second << endl <<
			"- trade_history_status_id[0][2]: " <<
			pFrame1Output->trade_info[0].trade_history_status_id[2] << endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG

#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}

// Call Trade Lookup Frame 2
void CTradeLookupDB::DoTradeLookupFrame2(PTradeLookupFrame2Input pFrame2Input,
		PTradeLookupFrame2Output pFrame2Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from tradelookupframe2(" << pFrame2Input->acct_id <<
			"::IDENT_T," << pFrame2Input->max_trades << ",'" <<
			pFrame2Input->trade_dts.year << "-" <<
			pFrame2Input->trade_dts.month << "-" <<
			pFrame2Input->trade_dts.day << " " <<
			pFrame2Input->trade_dts.hour << ":" <<
			pFrame2Input->trade_dts.minute << ":" <<
			pFrame2Input->trade_dts.second <<
			"'::timestamp) as (BidPrice S_PRICE_T, ExecName varchar, "
			"IsCash smallint, TradePrice S_PRICE_T, TradeList TRADE_T, "
			"SettlementAmount VALUE_T, SettleCashDueDateYear double precision, "
			"SettleCashDueDateMonth double precision, SettleCashDueDateDay "
			"double precision, SettleCashDueDateHour double precision, "
			"SettleCashDueDateMinute double precision, SettleCashDueDateSec "
			"double precision, SettlementCashType varchar, "
			"CashTransactionAmount VALUE_T, CashTxnDtsYear double precision, "
			"CashTxnDtsMonth double precision, CashTxnDtsDay double precision, "
			"CashTxnDtsHour double precision, CashTxnDtsMinute double "
			"precision, CashTxnDtsSec double precision, CashTransactionName "
			"varchar, Dts1Year double precision, Dts1Month double precision, "
			"Dts1Day double precision, Dts1Hour double precision, Dts1Minute "
			"double precision, Dts1Sec double precision, TradeHistoryStatusID1 "
			"char(4), Dts2Year double precision, Dts2Month double precision, "
			"Dts2Day double precision, Dts2Hour double precision, Dts2Minute "
			"double precision, Dts2Sec double precision, TradeHistoryStatusID2 "
			"char(4), Dts3Year double precision, Dts3Month double precision, "
			"Dts3Day double precision, Dts3Hour double precision, Dts3Minute "
			"double precision, Dts3Sec double precision, TradeHistoryStatusID3 "
			"char(4))";

	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	if (R.empty()) 
	{
		//throw logic_error("TradeLookupFrame2: empty result set");
		cout<<"warning: empty result set at DoTradeLookupFrame2"<<endl;
	}
	result::const_iterator c = R.begin();

	int i = 0;	
	for ( c; c != R.end(); ++c )
	{
		pFrame2Output->trade_info[i].bid_price = c[0].as(double());
		strcpy(pFrame2Output->trade_info[i].exec_name, c[1].c_str());
		pFrame2Output->trade_info[i].is_cash = c[2].as(int());
		pFrame2Output->trade_info[i].trade_price = c[3].as(double());
		pFrame2Output->trade_info[i].trade_id = c[4].as(int());
		pFrame2Output->trade_info[i].settlement_amount = c[5].as(double());
		pFrame2Output->trade_info[i].settlement_cash_due_date.year =
				c[6].as(int());
		pFrame2Output->trade_info[i].settlement_cash_due_date.month =
				c[7].as(int());
		pFrame2Output->trade_info[i].settlement_cash_due_date.day =
				c[8].as(int());
		pFrame2Output->trade_info[i].settlement_cash_due_date.hour =
				c[9].as(int());
		pFrame2Output->trade_info[i].settlement_cash_due_date.minute =
				c[10].as(int());
		pFrame2Output->trade_info[i].settlement_cash_due_date.second =
				int(c[11].as(double()));
		strcpy(pFrame2Output->trade_info[i].settlement_cash_type,
				c[12].c_str());
		pFrame2Output->trade_info[i].cash_transaction_amount =
				c[13].as(double());
		pFrame2Output->trade_info[i].cash_transaction_dts.year =
				c[14].as(int());
		pFrame2Output->trade_info[i].cash_transaction_dts.month =
				c[15].as(int());
		pFrame2Output->trade_info[i].cash_transaction_dts.day =
				c[16].as(int());
		pFrame2Output->trade_info[i].cash_transaction_dts.hour =
				c[17].as(int());
		pFrame2Output->trade_info[i].cash_transaction_dts.minute =
				c[18].as(int());
		pFrame2Output->trade_info[i].cash_transaction_dts.second =
				int(c[19].as(double()));
		strcpy(pFrame2Output->trade_info[i].cash_transaction_name,
				c[20].c_str());
		pFrame2Output->trade_info[i].trade_history_dts[0].year =
				c[21].as(int());
		pFrame2Output->trade_info[i].trade_history_dts[0].month =
				c[22].as(int());
		pFrame2Output->trade_info[i].trade_history_dts[0].day = c[23].as(int());
		pFrame2Output->trade_info[i].trade_history_dts[0].hour =
				c[24].as(int());
		pFrame2Output->trade_info[i].trade_history_dts[0].minute =
				c[25].as(int());
		pFrame2Output->trade_info[i].trade_history_dts[0].second =
				int(c[26].as(double()));
		strcpy(pFrame2Output->trade_info[i].trade_history_status_id[0],
				c[27].c_str());
		pFrame2Output->trade_info[i].trade_history_dts[1].year =
				c[28].as(int());
		pFrame2Output->trade_info[i].trade_history_dts[1].month =
				c[29].as(int());
		pFrame2Output->trade_info[i].trade_history_dts[1].day = c[30].as(int());
		pFrame2Output->trade_info[i].trade_history_dts[1].hour =
				c[31].as(int());
		pFrame2Output->trade_info[i].trade_history_dts[1].minute =
				c[32].as(int());
		pFrame2Output->trade_info[i].trade_history_dts[1].second =
				int(c[33].as(double()));
		strcpy(pFrame2Output->trade_info[i].trade_history_status_id[1],
				c[34].c_str());
		pFrame2Output->trade_info[i].trade_history_dts[2].year =
				c[35].as(int());
		pFrame2Output->trade_info[i].trade_history_dts[2].month =
				c[36].as(int());
		pFrame2Output->trade_info[i].trade_history_dts[2].day =
				c[37].as(int());
		pFrame2Output->trade_info[i].trade_history_dts[2].hour =
				c[38].as(int());
		pFrame2Output->trade_info[i].trade_history_dts[2].minute =
				c[39].as(int());
		pFrame2Output->trade_info[i].trade_history_dts[2].second =
				int(c[40].as(double()));
		strcpy(pFrame2Output->trade_info[i].trade_history_status_id[2],
				c[41].c_str());

		i++;
	}
 	pFrame2Output->num_found = i;
	pFrame2Output->status = CBaseTxnErr::SUCCESS;

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Lookup Frame 2 (input)"<<endl
	    <<"- acct_id: "<<pFrame2Input->acct_id<<endl
	    <<"- max_trades: "<<pFrame2Input->max_trades<<endl
	    <<"- trade_dts: "<<pFrame2Input->trade_dts.year<<"-"<<
		pFrame2Input->trade_dts.month<<"-"<<
		pFrame2Input->trade_dts.day<<" "<<
		pFrame2Input->trade_dts.hour<<":"<<
		pFrame2Input->trade_dts.minute<<":"<<
		pFrame2Input->trade_dts.second<<endl;
	cout << "Trade Lookup Frame 2 (output)" << endl <<
			"- num_found: " << pFrame2Output->num_found << endl <<
			"- bid_price[0]: " << pFrame2Output->trade_info[0].bid_price <<
			endl <<
			"- exec_name[0]: " << pFrame2Output->trade_info[0].exec_name <<
			endl <<
			"- is_cash[0]: " << pFrame2Output->trade_info[0].is_cash << endl <<
			"- trade_price[0]: " << pFrame2Output->trade_info[0].trade_price <<
			endl <<
			"- trade_id[0]: " << pFrame2Output->trade_info[0].trade_id <<
			endl <<
			"- settlement_amount[0]: " <<
			pFrame2Output->trade_info[0].settlement_amount << endl <<
			"- settlement_cash_due_date[0]: " <<
			pFrame2Output->trade_info[0].settlement_cash_due_date.year << "-"<<
			pFrame2Output->trade_info[0].settlement_cash_due_date.month <<
			"-" << pFrame2Output->trade_info[0].settlement_cash_due_date.day <<
			" " << pFrame2Output->trade_info[0].settlement_cash_due_date.hour <<
			":" <<
			pFrame2Output->trade_info[0].settlement_cash_due_date.minute <<
			":" <<
			pFrame2Output->trade_info[0].settlement_cash_due_date.second <<
			endl << "- settlement_cash_type[0]: " <<
			pFrame2Output->trade_info[0].settlement_cash_type << endl <<
			"- cash_transaction_amount[0]: " <<
			pFrame2Output->trade_info[0].cash_transaction_amount << endl <<
			"- cash_transaction_dts[0]: " <<
			pFrame2Output->trade_info[0].cash_transaction_dts.year << "-" <<
			pFrame2Output->trade_info[0].cash_transaction_dts.month << "-" <<
			pFrame2Output->trade_info[0].cash_transaction_dts.day << " " <<
			pFrame2Output->trade_info[0].cash_transaction_dts.hour << ":" <<
			pFrame2Output->trade_info[0].cash_transaction_dts.minute << ":" <<
			pFrame2Output->trade_info[0].cash_transaction_dts.second << endl <<
			"- cash_transaction_name[0]: " <<
			pFrame2Output->trade_info[0].cash_transaction_name << endl <<
			"- trade_history_dts[0][0]: " <<
			pFrame2Output->trade_info[0].trade_history_dts[0].year << "-" <<
			pFrame2Output->trade_info[0].trade_history_dts[0].month << "-" <<
			pFrame2Output->trade_info[0].trade_history_dts[0].day << " " <<
			pFrame2Output->trade_info[0].trade_history_dts[0].hour << ":"<<
			pFrame2Output->trade_info[0].trade_history_dts[0].minute << ":" <<
			pFrame2Output->trade_info[0].trade_history_dts[0].second << endl <<
			"- trade_history_status_id[0][0]: " <<
			pFrame2Output->trade_info[0].trade_history_status_id[0] << endl <<
			"- trade_history_dts[0][1]: " <<
			pFrame2Output->trade_info[0].trade_history_dts[1].year << "-" <<
			pFrame2Output->trade_info[0].trade_history_dts[1].month << "-" <<
			pFrame2Output->trade_info[0].trade_history_dts[1].day << " " <<
			pFrame2Output->trade_info[0].trade_history_dts[1].hour << ":" <<
			pFrame2Output->trade_info[0].trade_history_dts[1].minute << ":" <<
			pFrame2Output->trade_info[0].trade_history_dts[1].second << endl <<
			"- trade_history_status_id[0][1]: " <<
			pFrame2Output->trade_info[0].trade_history_status_id[1] << endl <<
			"- trade_history_dts[0][2]: " <<
			pFrame2Output->trade_info[0].trade_history_dts[2].year << "-" <<
			pFrame2Output->trade_info[0].trade_history_dts[2].month << "-" <<
			pFrame2Output->trade_info[0].trade_history_dts[2].day << " " <<
			pFrame2Output->trade_info[0].trade_history_dts[2].hour << ":" <<
			pFrame2Output->trade_info[0].trade_history_dts[2].minute << ":" <<
			pFrame2Output->trade_info[0].trade_history_dts[2].second << endl <<
			"- trade_history_status_id[0][2]: " <<
			pFrame2Output->trade_info[0].trade_history_status_id[2] << endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG

#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}

// Call Trade Lookup Frame 3
void CTradeLookupDB::DoTradeLookupFrame3(PTradeLookupFrame3Input pFrame3Input,
		PTradeLookupFrame3Output pFrame3Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "SELECT * from TradeLookupFrame3(" << pFrame3Input->max_acct_id <<
			"::IDENT_T," << pFrame3Input->max_trades << "::integer,'" <<
			pFrame3Input->trade_dts.year << "-" <<
			pFrame3Input->trade_dts.month << "-" <<
			pFrame3Input->trade_dts.day << " " <<
			pFrame3Input->trade_dts.hour << ":" <<
			pFrame3Input->trade_dts.minute << ":" <<
			pFrame3Input->trade_dts.second << "'::timestamp,'" <<
			pFrame3Input->symbol <<
			"'::char(15)) as (acct_id IDENT_T, cash_transaction_amount "
			"VALUE_T, CashTxnDtsYear double precision, CashTxnDtsMonth double "
			"precision, CashTxnDtsDay double precision, CashTxnDtsHour double "
			"precision, CashTxnDtsMinute double precision, CashTxnDtsSec "
			"double precision, cash_transaction_name varchar, exec_name "
			"varchar, is_cash smallint, price S_PRICE_T, quantity S_QTY_T, "
			"settlement_amount VALUE_T, SettleCashDueDateYear double "
			"precision, SettleCashDueDateMonth double precision, "
			"SettleCashDueDateDay double precision, SettleCashDueDateHour "
			"double precision, SettleCashDueDateMinute double precision, "
			"SettleCashDueDateSec double precision, settlement_cash_type "
			"varchar, trade_dtsYear double precision, trade_dtsMonth double "
			"precision, trade_dtsDay double precision, trade_dtsHour double "
			"precision, trade_dtsMinute double precision, trade_dtsSec double "
			"precision, Dts1Year double precision, Dts1Month double precision, "
			"Dts1Day double precision, Dts1Hour double precision, Dts1Minute "
			"double precision, Dts1Sec double precision, TradeHistoryStatusID1 "
			"char(4), Dts2Year double precision, Dts2Month double precision, "
			"Dts2Day double precision, Dts2Hour double precision, "
			"Dts2Minute double precision, Dts2Sec double precision, "
			"TradeHistoryStatusID2 char(4), Dts3Year double precision, "
			"Dts3Month double precision, Dts3Day double precision, Dts3Hour "
			"double precision, Dts3Minute double precision, Dts3Sec double "
			"precision, TradeHistoryStatusID3 char(4), trade_list TRADE_T, "
			"trade_type char(3))";

	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	if (R.empty()) 
	{
		//throw logic_error("TradeLookupFrame3: empty result set");
		cout<<"warning: empty result set at DoTradeLookupFrame3"<<endl;
	}
	result::const_iterator c = R.begin();
	
	int i = 0;	
	for ( c; c != R.end(); ++c )
	{
		pFrame3Output->trade_info[i].acct_id = c[0].as(int());
		pFrame3Output->trade_info[i].cash_transaction_amount =
				c[1].as(double());
		pFrame3Output->trade_info[i].cash_transaction_dts.year = c[2].as(int());
		pFrame3Output->trade_info[i].cash_transaction_dts.month =
				c[3].as(int());
		pFrame3Output->trade_info[i].cash_transaction_dts.day = c[4].as(int());
		pFrame3Output->trade_info[i].cash_transaction_dts.hour = c[5].as(int());
		pFrame3Output->trade_info[i].cash_transaction_dts.minute =
				c[6].as(int());
		pFrame3Output->trade_info[i].cash_transaction_dts.second =
				int(c[7].as(double()));
		strcpy(pFrame3Output->trade_info[i].cash_transaction_name,
				c[8].c_str());
		strcpy(pFrame3Output->trade_info[i].exec_name, c[9].c_str());
		pFrame3Output->trade_info[i].is_cash = c[10].as(int());
		pFrame3Output->trade_info[i].price = c[11].as(double());
		pFrame3Output->trade_info[i].quantity = c[12].as(int());
		pFrame3Output->trade_info[i].settlement_amount = c[13].as(double());
		pFrame3Output->trade_info[i].settlement_cash_due_date.year =
				c[14].as(int());
		pFrame3Output->trade_info[i].settlement_cash_due_date.month =
				c[15].as(int());
		pFrame3Output->trade_info[i].settlement_cash_due_date.day =
				c[16].as(int());
		pFrame3Output->trade_info[i].settlement_cash_due_date.hour =
				c[17].as(int());
		pFrame3Output->trade_info[i].settlement_cash_due_date.minute =
				c[18].as(int());
		pFrame3Output->trade_info[i].settlement_cash_due_date.second =
				int(c[19].as(double()));
		strcpy(pFrame3Output->trade_info[i].settlement_cash_type,
				c[20].c_str());
		pFrame3Output->trade_info[i].trade_dts.year = c[21].as(int());
		pFrame3Output->trade_info[i].trade_dts.month = c[22].as(int());
		pFrame3Output->trade_info[i].trade_dts.day = c[23].as(int());
		pFrame3Output->trade_info[i].trade_dts.hour = c[24].as(int());
		pFrame3Output->trade_info[i].trade_dts.minute = c[25].as(int());
		pFrame3Output->trade_info[i].trade_dts.second = int(c[26].as(double()));
		pFrame3Output->trade_info[i].trade_history_dts[0].year =
				c[27].as(int());
		pFrame3Output->trade_info[i].trade_history_dts[0].month =
				c[28].as(int());
		pFrame3Output->trade_info[i].trade_history_dts[0].day = c[29].as(int());
		pFrame3Output->trade_info[i].trade_history_dts[0].hour =
				c[30].as(int());
		pFrame3Output->trade_info[i].trade_history_dts[0].minute =
				c[31].as(int());
		pFrame3Output->trade_info[i].trade_history_dts[0].second =
				int(c[32].as(double()));
		strcpy(pFrame3Output->trade_info[i].trade_history_status_id[0],
				c[33].c_str());
		pFrame3Output->trade_info[i].trade_history_dts[1].year =
				c[34].as(int());
		pFrame3Output->trade_info[i].trade_history_dts[1].month =
				c[35].as(int());
		pFrame3Output->trade_info[i].trade_history_dts[1].day = c[36].as(int());
		pFrame3Output->trade_info[i].trade_history_dts[1].hour =
				c[37].as(int());
		pFrame3Output->trade_info[i].trade_history_dts[1].minute =
				c[38].as(int());
		pFrame3Output->trade_info[i].trade_history_dts[1].second =
				int(c[39].as(double()));
		strcpy(pFrame3Output->trade_info[i].trade_history_status_id[1],
				c[40].c_str());
		pFrame3Output->trade_info[i].trade_history_dts[2].year =
				c[41].as(int());
		pFrame3Output->trade_info[i].trade_history_dts[2].month =
				c[42].as(int());
		pFrame3Output->trade_info[i].trade_history_dts[2].day = c[43].as(int());
		pFrame3Output->trade_info[i].trade_history_dts[2].hour =
				c[44].as(int());
		pFrame3Output->trade_info[i].trade_history_dts[2].minute =
				c[45].as(int());
		pFrame3Output->trade_info[i].trade_history_dts[2].second =
				int(c[46].as(double()));
		strcpy(pFrame3Output->trade_info[i].trade_history_status_id[2],
				c[47].c_str());
		pFrame3Output->trade_info[i].trade_id = c[48].as(int());
		strcpy(pFrame3Output->trade_info[i].trade_type, c[49].c_str());

		i++;
	}
 	pFrame3Output->num_found = i;
	pFrame3Output->status = CBaseTxnErr::SUCCESS;

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Lookup Frame 3 (input)"<<endl
	    <<"- max_acct_id: "<<pFrame3Input->max_acct_id<<endl
	    <<"- max_trades: "<<pFrame3Input->max_trades<<endl
	    <<"- trade_dts: "<<pFrame3Input->trade_dts.year<<"-"<<
		pFrame3Input->trade_dts.month<<"-"<<
		pFrame3Input->trade_dts.day<<" "<<
		pFrame3Input->trade_dts.hour<<":"<<
		pFrame3Input->trade_dts.minute<<":"<<
		pFrame3Input->trade_dts.second<<endl
	    <<"- symbol: "<<pFrame3Input->symbol<<endl;
	cout << "Trade Lookup Frame 3 (output)" << endl <<
			"- num_found: " << pFrame3Output->num_found << endl <<
			"- acct_id: " << pFrame3Output->trade_info[0].acct_id << endl <<
			"- cash_transaction_amount[0]: " <<
			pFrame3Output->trade_info[0].cash_transaction_amount << endl <<
			"- cash_transaction_dts[0]: " <<
			pFrame3Output->trade_info[0].cash_transaction_dts.year << "-" <<
			pFrame3Output->trade_info[0].cash_transaction_dts.month << "-" <<
			pFrame3Output->trade_info[0].cash_transaction_dts.day << " " <<
			pFrame3Output->trade_info[0].cash_transaction_dts.hour << ":" <<
			pFrame3Output->trade_info[0].cash_transaction_dts.minute << ":" <<
			pFrame3Output->trade_info[0].cash_transaction_dts.second << endl <<
			"- cash_transaction_name[0]: " <<
			pFrame3Output->trade_info[0].cash_transaction_name << endl <<
			"- exec_name[0]: " << pFrame3Output->trade_info[0].exec_name <<
			endl << "- is_cash[0]: " << pFrame3Output->trade_info[0].is_cash <<
			endl <<
			"- price[0]: " << pFrame3Output->trade_info[0].price << endl <<
			"- quantity[0]: " << pFrame3Output->trade_info[0].quantity <<
			endl <<
			"- settlement_amount[0]: " <<
			pFrame3Output->trade_info[0].settlement_amount << endl <<
			"- settlement_cash_due_date[0]: " <<
			pFrame3Output->trade_info[0].settlement_cash_due_date.year << "-" <<
			pFrame3Output->trade_info[0].settlement_cash_due_date.month <<
			"-" << pFrame3Output->trade_info[0].settlement_cash_due_date.day <<
			" " << pFrame3Output->trade_info[0].settlement_cash_due_date.hour <<
			":" <<
			pFrame3Output->trade_info[0].settlement_cash_due_date.minute <<
			":" <<
			pFrame3Output->trade_info[0].settlement_cash_due_date.second <<
			endl <<
			"- settlement_cash_type[0]: " <<
			pFrame3Output->trade_info[0].settlement_cash_type << endl <<
			"- cash_transaction_dts[0]: " <<
			pFrame3Output->trade_info[0].trade_dts.year << "-" <<
			pFrame3Output->trade_info[0].trade_dts.month << "-" <<
			pFrame3Output->trade_info[0].trade_dts.day << " "<<
			pFrame3Output->trade_info[0].trade_dts.hour << ":" <<
			pFrame3Output->trade_info[0].trade_dts.minute << ":" <<
			pFrame3Output->trade_info[0].trade_dts.second << endl <<
			"- trade_history_dts[0][0]: " <<
			pFrame3Output->trade_info[0].trade_history_dts[0].year << "-" <<
			pFrame3Output->trade_info[0].trade_history_dts[0].month << "-" <<
			pFrame3Output->trade_info[0].trade_history_dts[0].day << " " <<
			pFrame3Output->trade_info[0].trade_history_dts[0].hour << ":" <<
			pFrame3Output->trade_info[0].trade_history_dts[0].minute << ":" <<
			pFrame3Output->trade_info[0].trade_history_dts[0].second << endl <<
			"- trade_history_status_id[0][0]: " <<
			pFrame3Output->trade_info[0].trade_history_status_id[0] << endl <<
			"- trade_history_dts[0][1]: " <<
			pFrame3Output->trade_info[0].trade_history_dts[1].year << "-" <<
			pFrame3Output->trade_info[0].trade_history_dts[1].month << "-" <<
			pFrame3Output->trade_info[0].trade_history_dts[1].day << " " <<
			pFrame3Output->trade_info[0].trade_history_dts[1].hour << ":" <<
			pFrame3Output->trade_info[0].trade_history_dts[1].minute << ":" <<
			pFrame3Output->trade_info[0].trade_history_dts[1].second << endl <<
			"- trade_history_status_id[0][1]: " <<
			pFrame3Output->trade_info[0].trade_history_status_id[1] << endl <<
			"- trade_history_dts[0][2]: " <<
			pFrame3Output->trade_info[0].trade_history_dts[2].year << "-" <<
			pFrame3Output->trade_info[0].trade_history_dts[2].month << "-" <<
			pFrame3Output->trade_info[0].trade_history_dts[2].day << " " <<
			pFrame3Output->trade_info[0].trade_history_dts[2].hour << ":" <<
			pFrame3Output->trade_info[0].trade_history_dts[2].minute << ":" <<
			pFrame3Output->trade_info[0].trade_history_dts[2].second << endl <<
			"- trade_history_status_id[0][2]: " <<
			pFrame3Output->trade_info[0].trade_history_status_id[2] << endl <<
			"- trade_id[0]: " << pFrame3Output->trade_info[0].trade_id <<
			endl <<
			"- trade_type[0]: " << pFrame3Output->trade_info[0].trade_type <<
			endl;
	m_coutLock.ReleaseLock();
#endif //DEBUG

#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}

// Call Trade Lookup Frame 4
void CTradeLookupDB::DoTradeLookupFrame4(PTradeLookupFrame4Input pFrame4Input,
		PTradeLookupFrame4Output pFrame4Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from TradeLookupFrame4(" << pFrame4Input->acct_id <<
			"::IDENT_T, '" << pFrame4Input->trade_dts.year << "-" <<
			pFrame4Input->trade_dts.month << "-" <<
			pFrame4Input->trade_dts.day << " " <<
			pFrame4Input->trade_dts.hour << ":" <<
			pFrame4Input->trade_dts.minute << ":" <<
			pFrame4Input->trade_dts.second <<
			"'::timestamp) as (hh_h_t_id TRADE_T, hh_t_id TRADE_T, qty_before "
			"S_QTY_T, qty_after S_QTY_T, trade_id TRADE_T);";

	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	if (R.empty()) 
	{
		//throw logic_error("TradeLookupFrame4: empty result set");
		cout<<"warning: empty result set at DoTradeLookupFrame4"<<endl;
 		pFrame4Output->num_found = 0;
		pFrame4Output->status = CBaseTxnErr::SUCCESS;
		return;
	}
	result::const_iterator c = R.begin();

	pFrame4Output->trade_id = c[4].as(int());

	int i = 0;	
	for ( c; c != R.end(); ++c )
	{
		pFrame4Output->trade_info[i].holding_history_id = c[0].as(int());
		pFrame4Output->trade_info[i].holding_history_trade_id = c[1].as(int());
		pFrame4Output->trade_info[i].quantity_before = c[2].as(int());
		pFrame4Output->trade_info[i].quantity_after = c[3].as(int());

		i++;
	}
 	pFrame4Output->num_found = i;
	pFrame4Output->status = CBaseTxnErr::SUCCESS;

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Lookup Frame 4 (input)"<<endl
	    <<"- acct_id: "<<pFrame4Input->acct_id<<endl
	    <<"- trade_dts: "<<pFrame4Input->trade_dts.year<<"-"<<
		pFrame4Input->trade_dts.month<<"-"<<
		pFrame4Input->trade_dts.day<<" "<<
		pFrame4Input->trade_dts.hour<<":"<<
		pFrame4Input->trade_dts.minute<<":"<<
		pFrame4Input->trade_dts.second<<endl;
	cout << "Trade Lookup Frame 4 (output)" << endl <<
			"- holding_history_id[0]: " <<
			pFrame4Output->trade_info[0].holding_history_id << endl <<
			"- holding_history_trade_id[0]: " <<
			pFrame4Output->trade_info[0].holding_history_trade_id << endl <<
			"- quantity_before[0]: " <<
			pFrame4Output->trade_info[0].quantity_before << endl <<
			"- quantity_after[0]: " <<
			pFrame4Output->trade_info[0].quantity_after << endl <<
			"- num_found: " << pFrame4Output->num_found << endl <<
			"- trade_id: " << pFrame4Output->trade_id << endl;
	m_coutLock.ReleaseLock();
#endif //DEBUG

#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}


// Clean-up
void CTradeLookupDB::Cleanup(void* pException)
{
}
