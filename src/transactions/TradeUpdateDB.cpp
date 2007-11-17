/*
 * TradeUpdateDB.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 11 July 2006
 */

#include <transactions.h>

using namespace TPCE;

// Constructor
CTradeUpdateDB::CTradeUpdateDB(CDBConnection *pDBConn)
: CTxnBaseDB(pDBConn)
{
}

// Destructor
CTradeUpdateDB::~CTradeUpdateDB()
{
}

// Call Trade Lookup Frame 1
void CTradeUpdateDB::DoTradeUpdateFrame1(const TTradeUpdateFrame1Input *pIn,
		TTradeUpdateFrame1Output *pOut)
{
#ifdef DEBUG
	cout << "TUF1" << endl;
#endif

	enum tuf1 {
			i_bid_price=0, i_cash_transaction_amount,
			i_cash_transaction_dts, i_cash_transaction_name, i_exec_name,
			i_is_cash, i_is_market, i_num_found, i_num_updated,
			i_settlement_amount, i_settlement_cash_due_date,
			i_settlement_cash_type, i_status, i_trade_history_dts,
			i_trade_history_status_id, i_trade_price
	};

	ostringstream osTrades;
	int i = 0;
	osTrades << pIn->trade_id[i];
	for ( i = 1; i < pIn->max_trades; i++)
	{
		osTrades << "," << pIn->trade_id[i];
	}

	ostringstream osCall;
	osCall << "SELECT * FROM TradeUpdateFrame1(" <<
			pIn->max_trades << "," <<
			pIn->max_updates << ",'{" <<
			osTrades.str() << "}')";

	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	if (R.empty()) 
	{
		//throw logic_error("TradeUpdateFrame1: empty result set");
		cout<<"warning: empty result set at DoTradeUpdateFrame1"<<endl;
		// Should this still be set to SUCCESS?
		pOut->status = CBaseTxnErr::SUCCESS;
		return;
	}
	result::const_iterator c = R.begin();

	vector<string> vAux;
	vector<string>::iterator p;

	Tokenize(c[i_bid_price].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].bid_price = atof((*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_cash_transaction_amount].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].cash_transaction_amount = atof((*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_cash_transaction_dts].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%d-%d-%d %d:%d:%d",
				pOut->trade_info[i].cash_transaction_dts.year,
				pOut->trade_info[i].cash_transaction_dts.month,
				pOut->trade_info[i].cash_transaction_dts.day,
				pOut->trade_info[i].cash_transaction_dts.hour,
				pOut->trade_info[i].cash_transaction_dts.minute,
				pOut->trade_info[i].cash_transaction_dts.second);
		++i;
	}
	vAux.clear();

	Tokenize(c[i_cash_transaction_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strcpy(pOut->trade_info[i].cash_transaction_name, (*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_exec_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strcpy(pOut->trade_info[i].exec_name, (*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_is_cash].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].is_cash = atof((*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_is_market].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].is_market = atof((*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_settlement_amount].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].settlement_amount = atof((*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_settlement_cash_due_date].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%d-%d-%d %d:%d:%d",
				pOut->trade_info[i].settlement_cash_due_date.year,
				pOut->trade_info[i].settlement_cash_due_date.month,
				pOut->trade_info[i].settlement_cash_due_date.day,
				pOut->trade_info[i].settlement_cash_due_date.hour,
				pOut->trade_info[i].settlement_cash_due_date.minute,
				pOut->trade_info[i].settlement_cash_due_date.second);
		++i;
	}
	vAux.clear();

	Tokenize(c[i_settlement_cash_type].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strcpy(pOut->trade_info[i].settlement_cash_type, (*p).c_str());
		++i;
	}
	vAux.clear();

 	pOut->num_found = c[i_num_found].as(int());
	pOut->num_updated = c[i_num_updated].as(int());
	pOut->status = c[i_status].as(int());

	//FIXME: Need to insert code to handle the trade_history_dts and
	// trade_history_status_id multi-dimensional array columns.

	Tokenize(c[i_trade_price].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].trade_price = atof((*p).c_str());
		++i;
	}
	vAux.clear();

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Trade Update Frame 1 (input)"<<endl
	    <<"- max_trades: "<<pIn->max_trades<<endl
	    <<"- max_updates: "<<pIn->max_updates<<endl
	    <<"- Trades: {"<<osTrades.str()<<"}"<<endl;
	cout << "Trade Update Frame 1 (output)" << endl <<
			"- num_found: " << pOut->num_found << endl <<
			"- num_updated: " << pOut->num_updated << endl <<
			"- bid_price[0]: " << pOut->trade_info[0].bid_price <<
			endl <<
			"- exec_name[0]: " << pOut->trade_info[0].exec_name <<
			endl <<
			"- is_cash[0]: " << pOut->trade_info[0].is_cash << endl <<
			"- is_market[0]: " << pOut->trade_info[0].is_market <<
			endl <<
			"- trade_price[0]: " << pOut->trade_info[0].trade_price <<
			endl <<
			"- settlement_amount[0]: "<<
			pOut->trade_info[0].settlement_amount << endl <<
			"- settlement_cash_due_date[0]: " <<
			pOut->trade_info[0].settlement_cash_due_date.year << "-" <<
			pOut->trade_info[0].settlement_cash_due_date.month <<
			"-" << pOut->trade_info[0].settlement_cash_due_date.day <<
			" " << pOut->trade_info[0].settlement_cash_due_date.hour <<
			":"<<
			pOut->trade_info[0].settlement_cash_due_date.minute <<
			":" <<
			pOut->trade_info[0].settlement_cash_due_date.second <<
			endl <<
			"- settlement_cash_type[0]: " <<
			pOut->trade_info[0].settlement_cash_type << endl <<	
			"- cash_transaction_amount[0]: " <<
			pOut->trade_info[0].cash_transaction_amount << endl <<
			"- cash_transaction_dts[0]: " <<
			pOut->trade_info[0].cash_transaction_dts.year << "-" <<
			pOut->trade_info[0].cash_transaction_dts.month << "-" <<
			pOut->trade_info[0].cash_transaction_dts.day << " " <<
			pOut->trade_info[0].cash_transaction_dts.hour << ":" <<
			pOut->trade_info[0].cash_transaction_dts.minute << ":" <<
			pOut->trade_info[0].cash_transaction_dts.second << endl <<
			"- cash_transaction_name[0]: " <<
			pOut->trade_info[0].cash_transaction_name << endl <<
			"- trade_history_dts[0][0]: " <<
			pOut->trade_info[0].trade_history_dts[0].year << "-" <<
			pOut->trade_info[0].trade_history_dts[0].month << "-" <<
			pOut->trade_info[0].trade_history_dts[0].day << " " <<
			pOut->trade_info[0].trade_history_dts[0].hour << ":" <<
			pOut->trade_info[0].trade_history_dts[0].minute << ":" <<
			pOut->trade_info[0].trade_history_dts[0].second << endl <<
			"- trade_history_status_id[0][0]: " <<
			pOut->trade_info[0].trade_history_status_id[0] << endl <<
			"- trade_history_dts[0][1]: " <<
			pOut->trade_info[0].trade_history_dts[1].year << "-" <<
			pOut->trade_info[0].trade_history_dts[1].month << "-" <<
			pOut->trade_info[0].trade_history_dts[1].day << " " <<
			pOut->trade_info[0].trade_history_dts[1].hour << ":" <<
			pOut->trade_info[0].trade_history_dts[1].minute << ":" <<
			pOut->trade_info[0].trade_history_dts[1].second << endl <<
			"- trade_history_status_id[0][1]: " <<
			pOut->trade_info[0].trade_history_status_id[1] << endl <<
			"- trade_history_dts[0][2]: " <<
			pOut->trade_info[0].trade_history_dts[2].year << "-" <<
			pOut->trade_info[0].trade_history_dts[2].month << "-" <<
			pOut->trade_info[0].trade_history_dts[2].day << " " <<
			pOut->trade_info[0].trade_history_dts[2].hour << ":" <<
			pOut->trade_info[0].trade_history_dts[2].minute << ":" <<
			pOut->trade_info[0].trade_history_dts[2].second << endl <<
			"- trade_history_status_id[0][2]: " <<
			pOut->trade_info[0].trade_history_status_id[2] << endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG
}

// Call Trade Lookup Frame 2
void CTradeUpdateDB::DoTradeUpdateFrame2(const TTradeUpdateFrame2Input *pIn,
		TTradeUpdateFrame2Output *pOut)
{
#ifdef DEBUG
	cout << "TLF2" << endl;
#endif

	enum tuf2 {
			i_bid_price=0, i_cash_transaction_amount,
			i_cash_transaction_dts, i_cash_transaction_name, i_exec_name,
			i_is_cash, i_num_found, i_num_updated, i_settlement_amount,
			i_settlement_cash_due_date, i_settlement_cash_type, i_status,
			i_trade_history_dts, i_trade_history_status_id, i_trade_list,
			i_trade_price
	};

	ostringstream osCall;
	osCall << "SELECT * FROM TradeUpdateFrame2(" <<
			pIn->acct_id << ",'" <<
			pIn->end_trade_dts.year << "-" <<
			pIn->end_trade_dts.month << "-" <<
			pIn->end_trade_dts.day << " " <<
			pIn->end_trade_dts.hour << ":" <<
			pIn->end_trade_dts.minute << ":" <<
			pIn->end_trade_dts.second << "'," <<
			pIn->max_trades << "," <<
			pIn->max_updates << ",'" <<
			pIn->end_trade_dts.year << "-" <<
			pIn->end_trade_dts.month << "-" <<
			pIn->end_trade_dts.day << " " <<
			pIn->end_trade_dts.hour << ":" <<
			pIn->end_trade_dts.minute << ":" <<
			pIn->end_trade_dts.second << "')";

	BeginTxn();
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	if (R.empty()) 
	{
		//throw logic_error("TradeUpdateFrame2: empty result set");
		cout<<"warning: empty result set at DoTradeUpdateFrame2"<<endl;
 		pOut->num_found = 0;
		pOut->status = CBaseTxnErr::SUCCESS;
		return;
	}
	result::const_iterator c = R.begin();

	vector<string> vAux;
	vector<string>::iterator p;

	Tokenize(c[i_bid_price].c_str(), vAux);
	int i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].bid_price = atof((*p).c_str());
		++i;
	}
	vAux.clear();

	pOut->num_updated = c[0].as(int());

	Tokenize(c[i_cash_transaction_amount].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].cash_transaction_amount = atof((*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_cash_transaction_dts].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%d-%d-%d %d:%d:%d",
				pOut->trade_info[i].cash_transaction_dts.year,
				pOut->trade_info[i].cash_transaction_dts.month,
				pOut->trade_info[i].cash_transaction_dts.day,
				pOut->trade_info[i].cash_transaction_dts.hour,
				pOut->trade_info[i].cash_transaction_dts.minute,
				pOut->trade_info[i].cash_transaction_dts.second);
		++i;
	}
	vAux.clear();

	Tokenize(c[i_cash_transaction_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strcpy(pOut->trade_info[i].cash_transaction_name, (*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_exec_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strcpy(pOut->trade_info[i].exec_name, (*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_is_cash].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].is_cash = atof((*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_settlement_amount].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].settlement_amount = atof((*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_settlement_cash_due_date].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%d-%d-%d %d:%d:%d",
				pOut->trade_info[i].settlement_cash_due_date.year,
				pOut->trade_info[i].settlement_cash_due_date.month,
				pOut->trade_info[i].settlement_cash_due_date.day,
				pOut->trade_info[i].settlement_cash_due_date.hour,
				pOut->trade_info[i].settlement_cash_due_date.minute,
				pOut->trade_info[i].settlement_cash_due_date.second);
		++i;
	}
	vAux.clear();

	Tokenize(c[i_settlement_cash_type].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strcpy(pOut->trade_info[i].settlement_cash_type, (*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_trade_list].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].trade_id = atol((*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_trade_price].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].trade_price = atof((*p).c_str());
		++i;
	}
	vAux.clear();

	i = 0;	
	for ( c; c != R.end(); ++c )
	{
		pOut->trade_info[i].bid_price = c[1].as(double());
		strcpy(pOut->trade_info[i].exec_name, c[2].c_str());
		pOut->trade_info[i].is_cash = c[3].as(int());
		pOut->trade_info[i].trade_price = c[4].as(double());
		pOut->trade_info[i].trade_id = c[5].as(int());
		pOut->trade_info[i].settlement_amount = c[6].as(double());
		pOut->trade_info[i].settlement_cash_due_date.year =
				c[7].as(int());
		pOut->trade_info[i].settlement_cash_due_date.month =
				c[8].as(int());
		pOut->trade_info[i].settlement_cash_due_date.day =
				c[9].as(int());
		pOut->trade_info[i].settlement_cash_due_date.hour =
				c[10].as(int());
		pOut->trade_info[i].settlement_cash_due_date.minute =
				c[11].as(int());
		pOut->trade_info[i].settlement_cash_due_date.second =
				int(c[12].as(double()));
		strcpy(pOut->trade_info[i].settlement_cash_type,
				c[13].c_str());
		pOut->trade_info[i].cash_transaction_amount =
				c[14].as(double());
		pOut->trade_info[i].cash_transaction_dts.year =
				c[15].as(int());
		pOut->trade_info[i].cash_transaction_dts.month =
				c[16].as(int());
		pOut->trade_info[i].cash_transaction_dts.day =
				c[17].as(int());
		pOut->trade_info[i].cash_transaction_dts.hour =
				c[18].as(int());
		pOut->trade_info[i].cash_transaction_dts.minute =
				c[19].as(int());
		pOut->trade_info[i].cash_transaction_dts.second =
				int(c[20].as(double()));
		strcpy(pOut->trade_info[i].cash_transaction_name,
				c[21].c_str());
		pOut->trade_info[i].trade_history_dts[0].year =
				c[22].as(int());
		pOut->trade_info[i].trade_history_dts[0].month =
				 c[23].as(int());
		pOut->trade_info[i].trade_history_dts[0].day = c[24].as(int());
		pOut->trade_info[i].trade_history_dts[0].hour =
				 c[25].as(int());
		pOut->trade_info[i].trade_history_dts[0].minute =
				 c[26].as(int());
		pOut->trade_info[i].trade_history_dts[0].second =
				 int(c[27].as(double()));
		strcpy(pOut->trade_info[i].trade_history_status_id[0],
				 c[28].c_str());
		pOut->trade_info[i].trade_history_dts[1].year =
				 c[29].as(int());
		pOut->trade_info[i].trade_history_dts[1].month =
				 c[30].as(int());
		pOut->trade_info[i].trade_history_dts[1].day = c[31].as(int());
		pOut->trade_info[i].trade_history_dts[1].hour =
				 c[32].as(int());
		pOut->trade_info[i].trade_history_dts[1].minute =
				 c[33].as(int());
		pOut->trade_info[i].trade_history_dts[1].second =
				 int(c[34].as(double()));
		strcpy(pOut->trade_info[i].trade_history_status_id[1],
				 c[35].c_str());
		pOut->trade_info[i].trade_history_dts[2].year =
				 c[36].as(int());
		pOut->trade_info[i].trade_history_dts[2].month =
				 c[37].as(int());
		pOut->trade_info[i].trade_history_dts[2].day = c[38].as(int());
		pOut->trade_info[i].trade_history_dts[2].hour =
				 c[39].as(int());
		pOut->trade_info[i].trade_history_dts[2].minute =
				 c[40].as(int());
		pOut->trade_info[i].trade_history_dts[2].second =
				 int(c[41].as(double()));
		strcpy(pOut->trade_info[i].trade_history_status_id[2],
				 c[42].c_str());

		i++;
	}
 	pOut->num_found = i;
	pOut->status = CBaseTxnErr::SUCCESS;

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout <<"Trade Update Frame 2 (input)" << endl <<
			"- acct_id: " << pIn->acct_id << endl <<
			"- max_trades: " << pIn->max_trades << endl <<
			"- max_updates: " << pIn->max_updates << endl <<
			"- trade_dts: " << pIn->end_trade_dts.year << "-" <<
			pIn->end_trade_dts.month << "-" <<
			pIn->end_trade_dts.day << " " <<
			pIn->end_trade_dts.hour << ":" <<
			pIn->end_trade_dts.minute << ":" <<
			pIn->end_trade_dts.second << endl;
	cout << "Trade Update Frame 2 (output)" << endl <<
			"- num_found: " << pOut->num_found << endl <<
			"- num_updated: " << pOut->num_updated << endl <<
			"- bid_price[0]: " << pOut->trade_info[0].bid_price <<
			endl <<
			"- exec_name[0]: " << pOut->trade_info[0].exec_name <<
			endl <<
			"- is_cash[0]: " << pOut->trade_info[0].is_cash << endl <<
			"- trade_price[0]: " << pOut->trade_info[0].trade_price <<
			endl <<
			"- trade_id[0]: " << pOut->trade_info[0].trade_id <<
			endl <<
			"- settlement_amount[0]: " <<
			pOut->trade_info[0].settlement_amount << endl <<
			"- settlement_cash_due_date[0]: " <<
			pOut->trade_info[0].settlement_cash_due_date.year << "-" <<
			pOut->trade_info[0].settlement_cash_due_date.month <<
			"-" << pOut->trade_info[0].settlement_cash_due_date.day <<
			" " << pOut->trade_info[0].settlement_cash_due_date.hour <<
			":" <<
			pOut->trade_info[0].settlement_cash_due_date.minute <<
			":" <<
			pOut->trade_info[0].settlement_cash_due_date.second <<
			endl <<
			"- settlement_cash_type[0]: " <<
			pOut->trade_info[0].settlement_cash_type << endl <<
			"- cash_transaction_amount[0]: " <<
			pOut->trade_info[0].cash_transaction_amount << endl <<
			"- cash_transaction_dts[0]: " <<
			pOut->trade_info[0].cash_transaction_dts.year << "-" <<
			pOut->trade_info[0].cash_transaction_dts.month << "-" <<
			pOut->trade_info[0].cash_transaction_dts.day << " " <<
			pOut->trade_info[0].cash_transaction_dts.hour << ":" <<
			pOut->trade_info[0].cash_transaction_dts.minute <<":" <<
			pOut->trade_info[0].cash_transaction_dts.second << endl <<
			"- cash_transaction_name[0]: " <<
			pOut->trade_info[0].cash_transaction_name << endl <<
			"- trade_history_dts[0][0]: " <<
			pOut->trade_info[0].trade_history_dts[0].year << "-" <<
			pOut->trade_info[0].trade_history_dts[0].month << "-" <<
			pOut->trade_info[0].trade_history_dts[0].day << " " <<
			pOut->trade_info[0].trade_history_dts[0].hour << ":" <<
			pOut->trade_info[0].trade_history_dts[0].minute << ":" <<
			pOut->trade_info[0].trade_history_dts[0].second << endl <<
			"- trade_history_status_id[0][0]: " <<
			pOut->trade_info[0].trade_history_status_id[0] << endl <<
			"- trade_history_dts[0][1]: " <<
			pOut->trade_info[0].trade_history_dts[1].year << "-" <<
			pOut->trade_info[0].trade_history_dts[1].month << "-" <<
			pOut->trade_info[0].trade_history_dts[1].day << " " <<
			pOut->trade_info[0].trade_history_dts[1].hour << ":" <<
			pOut->trade_info[0].trade_history_dts[1].minute << ":" <<
			pOut->trade_info[0].trade_history_dts[1].second << endl <<
			"- trade_history_status_id[0][1]: " <<
			pOut->trade_info[0].trade_history_status_id[1] << endl <<
			"- trade_history_dts[0][2]: " <<
			pOut->trade_info[0].trade_history_dts[2].year << "-" <<
			pOut->trade_info[0].trade_history_dts[2].month << "-" <<
			pOut->trade_info[0].trade_history_dts[2].day << " " <<
			pOut->trade_info[0].trade_history_dts[2].hour << ":" <<
			pOut->trade_info[0].trade_history_dts[2].minute << ":" <<
			pOut->trade_info[0].trade_history_dts[2].second << endl <<
			"- trade_history_status_id[0][2]: " <<
			pOut->trade_info[0].trade_history_status_id[2] << endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG
}

// Call Trade Lookup Frame 3
void CTradeUpdateDB::DoTradeUpdateFrame3(const TTradeUpdateFrame3Input *pIn,
		TTradeUpdateFrame3Output *pOut)
{
#ifdef DEBUG
	cout << "TLF3" << endl;
#endif

	enum tuf3 {                                                             
			i_acct_id=0, i_cash_transaction_amount,
			i_cash_transaction_dts, i_cash_transaction_name, i_exec_name,
			i_is_cash, i_num_found, i_num_updated, i_price, i_quantity,
			i_s_name, i_settlement_amount, i_settlement_cash_due_date,
			i_settlement_cash_type, i_status, i_trade_dts,
			i_trade_history_dts, i_trade_history_status_id, i_trade_list,
			i_type_name, i_trade_type
	};

	ostringstream osCall;
	osCall << "SELECT * from TradeUpdateFrame3('" <<
			pIn->end_trade_dts.year << "-" <<
			pIn->end_trade_dts.month << "-" <<
			pIn->end_trade_dts.day << " " <<
			pIn->end_trade_dts.hour << ":" <<
			pIn->end_trade_dts.minute << ":" <<
			pIn->end_trade_dts.second << "'," <<
			pIn->max_acct_id << "," <<
			pIn->max_trades << "," <<
			pIn->max_updates << ",'" <<
			pIn->start_trade_dts.year << "-" <<
			pIn->start_trade_dts.month << "-" <<
			pIn->start_trade_dts.day << " " <<
			pIn->start_trade_dts.hour << ":" <<
			pIn->start_trade_dts.minute << ":" <<
			pIn->start_trade_dts.second << "','" <<
			pIn->symbol << "')";

	BeginTxn();
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	if (R.empty()) 
	{
		//throw logic_error("TradeUpdateFrame3: empty result set");
		cout<<"warning: empty result set at DoTradeUpdateFrame3"<<endl;
 		pOut->num_found = 0;
		// Should this be set to SUCCESS?
		pOut->status = CBaseTxnErr::SUCCESS;
		return;
	}
	result::const_iterator c = R.begin();
	result::const_iterator e = R.end();
	--e;

	vector<string> vAux;
	vector<string>::iterator p;

	Tokenize(c[i_acct_id].c_str(), vAux);
	int i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].acct_id = atol((*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_cash_transaction_amount].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].cash_transaction_amount = atof((*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_cash_transaction_dts].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%d-%d-%d %d:%d:%d",
				pOut->trade_info[i].cash_transaction_dts.year,
				pOut->trade_info[i].cash_transaction_dts.month,
				pOut->trade_info[i].cash_transaction_dts.day,
				pOut->trade_info[i].cash_transaction_dts.hour,
				pOut->trade_info[i].cash_transaction_dts.minute,
				pOut->trade_info[i].cash_transaction_dts.second);
		++i;
	}
	vAux.clear();

	// FIXME: The tokenizer isn't breaking up the array properly, because of
	// commas (,) in the string.
/*
cout << __FILE__ << ":" << __LINE__ << " = '" << c[i_cash_transaction_name].c_str() << "'" << endl;
	Tokenize(c[i_cash_transaction_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
cout << __FILE__ << ":" << __LINE__ << " = '" << (*p).c_str() << "'" << endl;
		strcpy(pOut->trade_info[i].cash_transaction_name, (*p).c_str());
		++i;
	}
	vAux.clear();
*/

	Tokenize(c[i_exec_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strcpy(pOut->trade_info[i].exec_name, (*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_is_cash].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].is_cash = atof((*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_price].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].price = atof((*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_quantity].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].quantity = atoi((*p).c_str());
		++i;
	}
	vAux.clear();

	// FIXME: The tokenizer isn't breaking up the array properly, because of
	// commas (,) in the string.
/*
	Tokenize(c[i_s_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
cout << __FILE__ << ":" << __LINE__ << " = '" << (*p).c_str() << "'" << endl;
		strcpy(pOut->trade_info[i].s_name, (*p).c_str());
		++i;
	}
	vAux.clear();
*/

	Tokenize(c[i_settlement_amount].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].settlement_amount = atof((*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_settlement_cash_due_date].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%d-%d-%d %d:%d:%d",
				pOut->trade_info[i].settlement_cash_due_date.year,
				pOut->trade_info[i].settlement_cash_due_date.month,
				pOut->trade_info[i].settlement_cash_due_date.day,
				pOut->trade_info[i].settlement_cash_due_date.hour,
				pOut->trade_info[i].settlement_cash_due_date.minute,
				pOut->trade_info[i].settlement_cash_due_date.second);
		++i;
	}
	vAux.clear();

	Tokenize(c[i_settlement_cash_type].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strcpy(pOut->trade_info[i].settlement_cash_type, (*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_trade_list].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_info[i].trade_id = atol((*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_type_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strcpy(pOut->trade_info[i].type_name, (*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_trade_type].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strcpy(pOut->trade_info[i].trade_type, (*p).c_str());
		++i;
	}
	vAux.clear();

 	pOut->num_found = c[i_num_found].as(int());
 	pOut->num_updated = c[i_num_updated].as(int());
	pOut->status = c[i_status].as(int());

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout << "Trade Update Frame 3 (input)" << endl <<
			"- max_acct_id: " << pIn->max_acct_id << endl <<
			"- max_trades: " << pIn->max_trades << endl <<
			"- max_updates: " << pIn->max_updates << endl <<
			"- trade_dts: " << pIn->start_trade_dts.year << "-" <<
			pIn->start_trade_dts.month << "-" <<
			pIn->start_trade_dts.day << " " <<
			pIn->start_trade_dts.hour << ":" <<
			pIn->start_trade_dts.minute << ":" <<
			pIn->start_trade_dts.second << endl <<
			"- symbol: " << pIn->symbol << endl;
	cout << "Trade Update Frame 3 (output)" << endl <<
			"- num_found: " << pOut->num_found << endl <<
			"- num_updated: " << pOut->num_updated << endl <<
			"- acct_id: " << pOut->trade_info[0].acct_id << endl <<
			"- cash_transaction_amount[0]: " <<
			pOut->trade_info[0].cash_transaction_amount << endl <<
			"- cash_transaction_dts[0]: " <<
			pOut->trade_info[0].cash_transaction_dts.year << "-" <<
			pOut->trade_info[0].cash_transaction_dts.month << "-" <<
			pOut->trade_info[0].cash_transaction_dts.day << " " <<
			pOut->trade_info[0].cash_transaction_dts.hour << ":" <<
			pOut->trade_info[0].cash_transaction_dts.minute << ":" <<
			pOut->trade_info[0].cash_transaction_dts.second << endl <<
			"- cash_transaction_name[0]: " <<
			pOut->trade_info[0].cash_transaction_name << endl <<
			"- exec_name[0]: " << pOut->trade_info[0].exec_name <<
			endl <<
			"- is_cash[0]: " << pOut->trade_info[0].is_cash << endl <<
			"- price[0]: " << pOut->trade_info[0].price << endl <<
			"- quantity[0]: " << pOut->trade_info[0].quantity <<
			endl <<
			"- settlement_amount[0]: " <<
			pOut->trade_info[0].settlement_amount << endl <<
			"- settlement_cash_due_date[0]: " <<
			pOut->trade_info[0].settlement_cash_due_date.year << "-" <<
			pOut->trade_info[0].settlement_cash_due_date.month <<
			"-" << pOut->trade_info[0].settlement_cash_due_date.day <<
			" " << pOut->trade_info[0].settlement_cash_due_date.hour <<
			":" <<
			pOut->trade_info[0].settlement_cash_due_date.minute <<
			":" <<
			pOut->trade_info[0].settlement_cash_due_date.second <<
			endl <<
			"- settlement_cash_type[0]: " <<
			pOut->trade_info[0].settlement_cash_type << endl <<
			"- cash_transaction_dts[0]: " <<
			pOut->trade_info[0].trade_dts.year << "-" <<
			pOut->trade_info[0].trade_dts.month << "-" <<
			pOut->trade_info[0].trade_dts.day << " " <<
			pOut->trade_info[0].trade_dts.hour << ":" <<
			pOut->trade_info[0].trade_dts.minute << ":" <<
			pOut->trade_info[0].trade_dts.second << endl <<
			"- trade_history_dts[0][0]: " <<
			pOut->trade_info[0].trade_history_dts[0].year << "-" <<
			pOut->trade_info[0].trade_history_dts[0].month << "-" <<
			pOut->trade_info[0].trade_history_dts[0].day << " " <<
			pOut->trade_info[0].trade_history_dts[0].hour << ":" <<
			pOut->trade_info[0].trade_history_dts[0].minute << ":" <<
			pOut->trade_info[0].trade_history_dts[0].second << endl <<
			"- trade_history_status_id[0][0]: " <<
			pOut->trade_info[0].trade_history_status_id[0] << endl <<
			"- trade_history_dts[0][1]: " <<
			pOut->trade_info[0].trade_history_dts[1].year << "-" <<
			pOut->trade_info[0].trade_history_dts[1].month << "-" <<
			pOut->trade_info[0].trade_history_dts[1].day << " " <<
			pOut->trade_info[0].trade_history_dts[1].hour << ":" <<
			pOut->trade_info[0].trade_history_dts[1].minute << ":" <<
			pOut->trade_info[0].trade_history_dts[1].second << endl <<
			"- trade_history_status_id[0][1]: " <<
			pOut->trade_info[0].trade_history_status_id[1] << endl <<
			"- trade_history_dts[0][2]: " <<
			pOut->trade_info[0].trade_history_dts[2].year << "-" <<
			pOut->trade_info[0].trade_history_dts[2].month << "-" <<
			pOut->trade_info[0].trade_history_dts[2].day << " " <<
			pOut->trade_info[0].trade_history_dts[2].hour << ":" <<
			pOut->trade_info[0].trade_history_dts[2].minute << ":" <<
			pOut->trade_info[0].trade_history_dts[2].second << endl <<
			"- trade_history_status_id[0][2]: " <<
			pOut->trade_info[0].trade_history_status_id[2] << endl <<
			"- trade_id[0]: " << pOut->trade_info[0].trade_id <<
			endl <<
			"- trade_type[0]: " << pOut->trade_info[0].trade_type <<
			endl;
	m_coutLock.ReleaseLock();
#endif //DEBUG
}

// Clean-up
void CTradeUpdateDB::Cleanup(void* pException)
{
}
