/*
 * TradeStatusDB.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 13 June 2006
 */

#include <transactions.h>

using namespace TPCE;

// Constructor
CTradeStatusDB::CTradeStatusDB(CDBConnection *pDBConn)
: CTxnBaseDB(pDBConn)
{
}

// Destructor
CTradeStatusDB::~CTradeStatusDB()
{
}

// Call Trade Status Frame 1
void CTradeStatusDB::DoTradeStatusFrame1(const TTradeStatusFrame1Input *pIn,
		TTradeStatusFrame1Output *pOut)
{
#ifdef DEBUG
	cout << "TSF1" << endl;
#endif

	enum tsf1 {                                                             
			i_broker_name=0, i_charge, i_cust_f_name, i_cust_l_name,        
			i_ex_name, i_exec_name, i_s_name, i_status, i_status_name,      
			i_symbol, i_trade_dts, i_trade_id, i_trade_qty, i_type_name     
	};

	ostringstream osCall;
	osCall << "SELECT * from TradeStatusFrame1(" << pIn->acct_id << ")";

	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();		

	if (R.empty()) 
	{
		//throw logic_error("empty result set!");
		cout << "warning: empty result set at DoTradeStatusFrame1" << endl
				<< "- acct_id: " << pIn->acct_id << endl;
		pOut->status = CBaseTxnErr::SUCCESS;
		return;
	}

	result::const_iterator c = R.begin();

	vector<string> vAux;
	vector<string>::iterator p;
	
	strcpy(pOut->broker_name, c[i_broker_name].c_str());

	Tokenize(c[i_broker_name].c_str(), vAux);
	int i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->charge[i] = atof((*p).c_str());
		++i;
	}
	vAux.clear();

	strcpy(pOut->cust_f_name, c[i_cust_f_name].c_str());
	strcpy(pOut->cust_l_name, c[i_cust_l_name].c_str());

	Tokenize(c[i_ex_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strcpy(pOut->ex_name[i], (*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_s_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strcpy(pOut->s_name[i], (*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_symbol].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strcpy(pOut->symbol[i], (*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_trade_dts].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%d-%d-%d %d:%d:%d",
				&pOut->trade_dts[i].year,
				&pOut->trade_dts[i].month,
				&pOut->trade_dts[i].day,
				&pOut->trade_dts[i].hour,
				&pOut->trade_dts[i].minute,
				&pOut->trade_dts[i].second);
		++i;
	}
	vAux.clear();

	Tokenize(c[i_trade_id].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_id[i] = atol((*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_trade_qty].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_qty[i] = atoi((*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_type_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strcpy(pOut->type_name[i], (*p).c_str());
		++i;
	}
	vAux.clear();

	pOut->status = c[i_status].as(int());

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout << "Trade Status Frame 1 (input)" << endl <<
			"- acct_id: " << pIn->acct_id << endl;
	cout << "Trade Status Frame 1 (output)" << endl <<
			"- cust_l_name: " << pOut->cust_l_name << endl <<
			"- cust_f_name: " << pOut->cust_f_name << endl <<
			"- broker_name: " << pOut->broker_name << endl <<
			"- charge[0]: " << pOut->charge[0] << endl <<
			"- exec_name[0]: " << pOut->exec_name[0] << endl <<
			"- ex_name[0]: " << pOut->ex_name[0] << endl <<
			"- s_name[0]: " << pOut->s_name[0] << endl <<
			"- status_name[0]: " << pOut->status_name[0] << endl <<
			"- symbol[0]: " << pOut->symbol[0] << endl <<
			"- trade_dts[0]: " << pOut->trade_dts[0].year << "-" <<
			pOut->trade_dts[0].month << "-" <<
			pOut->trade_dts[0].day << " " <<
			pOut->trade_dts[0].hour << ":" <<
			pOut->trade_dts[0].minute << ":" <<
			pOut->trade_dts[0].second << endl <<
			"- trade_id[0]: " << pOut->trade_id[0] << endl <<
			"- trade_qty[0]: " << pOut->trade_qty[0] << endl <<
			"- type_name[0]: " << pOut->type_name[0] << endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG
}

void CTradeStatusDB::Cleanup(void* pException)
{
}
