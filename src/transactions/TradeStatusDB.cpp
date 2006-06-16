/*
 * TradeStatusDB.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 13 June 2006
 */

#include "transactions.h"

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
void CTradeStatusDB::DoTradeStatusFrame1(PTradeStatusFrame1Input pFrame1Input, PTradeStatusFrame1Output pFrame1Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)
	cout<<"acct_id:"<<pFrame1Input->acct_id<<endl; // input ok?
	
	ostringstream osCall;
	osCall <<"SELECT * from TradeStatusFrame1("<<pFrame1Input->acct_id<<") as (fname varchar, lname varchar,"
			"broker varchar, charge value_t, exec_name varchar, ex_name varchar, s_name varchar, "
			"status_name char(10), symbol char(15), trade_dts timestamp, trade_id trade_t, "
			"trade_qty s_qty_t, type_name char(12))";

	try
	{
		BeginTxn();
		result R( m_Txn->exec( osCall.str() ) );
		CommitTxn();		

 		if (R.empty()) throw logic_error("empty result set!");

		result::const_iterator c = R.begin();
		
		int i = 0;	
		for ( c; c != R.end(); ++c )
		{
			sprintf(pFrame1Output->cust_l_name, "%s", c[0].c_str() ); // should not be in the loop
			sprintf(pFrame1Output->cust_f_name, "%s", c[1].c_str() ); // should not be in the loop
			sprintf(pFrame1Output->broker_name, "%s", c[2].c_str() ); // should not be in the loop
			sprintf(pFrame1Output->ex_name[i], "%s", c[5].c_str() );
			sprintf(pFrame1Output->exec_name[i], "%s", c[4].c_str() );
			sprintf(pFrame1Output->s_name[i], "%s", c[6].c_str() );
			sprintf(pFrame1Output->status_name[i], "%s", c[7].c_str() );
			sprintf(pFrame1Output->symbol[i], "%s", c[8].c_str() );
			sprintf(pFrame1Output->type_name[i], "%s", c[12].c_str() );
			pFrame1Output->charge[i] = c[3].as(double());
			pFrame1Output->trade_id[i] = c[10].as(int());
			pFrame1Output->trade_qty[i]= c[11].as(int());
		//	pFrame1Output->trade_dts[i] 	= c[9];
			
			i++;
		}
	}
	catch(const exception &e)
	{
		cout<< "libpqxx: "<<e.what() << endl;
	}
#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}

void CTradeStatusDB::Cleanup(void* pException)
{
}
