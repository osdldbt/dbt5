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
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "SELECT * from TradeStatusFrame1(" << pIn->acct_id <<
			") as (lname varchar, fname varchar, broker varchar, "
			"charge value_t, exec_name varchar, ex_name varchar, s_name "
			"varchar,status_name char(10), symbol char(15), trade_dts_year "
			"double precision, trade_dts_month double precision, trade_dts_day "
			"double precision, trade_dts_hour double precision, "
			"trade_dts_minute double precision, trade_dts_second double "
			"precision, trade_id trade_t, trade_qty s_qty_t, type_name "
			"char(12))";

	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();		

	if (R.empty()) 
	{
		//throw logic_error("empty result set!");
		cout<<"warning: empty result set at DoTradeStatusFrame1"<<endl
			<<"- acct_id: "<<pIn->acct_id<<endl;
		pOut->status = CBaseTxnErr::SUCCESS;
		return;
	}

	result::const_iterator c = R.begin();
	
	strcpy(pOut->cust_l_name, c[0].c_str());
	strcpy(pOut->cust_f_name, c[1].c_str());
	strcpy(pOut->broker_name, c[2].c_str());

	int i = 0;	
	for ( c; c != R.end(); ++c )
	{
		pOut->charge[i] = c[3].as(double());
		strcpy(pOut->exec_name[i], c[4].c_str());
		strcpy(pOut->ex_name[i], c[5].c_str());
		strcpy(pOut->s_name[i], c[6].c_str());
		strcpy(pOut->status_name[i], c[7].c_str());
		strcpy(pOut->symbol[i], c[8].c_str() );
		pOut->trade_dts[i].year = c[9].as(int());
		pOut->trade_dts[i].month = c[10].as(int());
		pOut->trade_dts[i].day = c[11].as(int());
		pOut->trade_dts[i].hour = c[12].as(int());
		pOut->trade_dts[i].minute = c[13].as(int());
		pOut->trade_dts[i].second = int(c[14].as(double()));
		pOut->trade_id[i] = c[15].as(int());
		pOut->trade_qty[i] = c[16].as(int());
		strcpy(pOut->type_name[i], c[17].c_str());
		
		i++;
	}

	pOut->status = CBaseTxnErr::SUCCESS;

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
	
#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}

void CTradeStatusDB::Cleanup(void* pException)
{
}
