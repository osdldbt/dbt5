// 2006 Rilson Nascimento
// Test of TradeStatus transaction

#include "../include/transactions.h"
#include "../include/harness.h"

using namespace TPCE;

int main(int argc, char* argv[])
{
	const char *server = "localhost";
	const char *db = "dbt5";
	const char *port = "5432";

	CDBConnection		m_Conn( server, db, port );
	CTradeStatus		m_TradeStatus(&m_Conn);	// trade status harness code (TPC provided)
							// this class uses our implementation of CTradeStatusDB class
	
	//PTradeStatusTxnInput	pInput;			
	TTradeStatusTxnInput	Input;			// trade status input parameters
	TTradeStatusTxnOutput	Output;			// 		output parameters
	
	// expect acct_id as first argument
	int arg = 1;
	
	if (argv[arg] == 0)
	{
		cout<<"\nUsage: testTS {acct_id}\n"<<endl;
		return(-1);
	}
	char* vp = argv[arg];

	if ( ((arg + 1) < argc) )
	{
		vp = argv[++arg];
	}
	
	sscanf(argv[1], "%"PRId64, &Input.acct_id);

	try 
	{
		m_TradeStatus.DoTxn(&Input, &Output);		// Perform Trade Status (harness + SP)
	}
	catch (exception &e)
	{
		cout<<e.what()<<endl;
		return(1);
	}
	
	// Print output
	for (unsigned int i = 0; i < (sizeof(Output.trade_id)/sizeof(Output.trade_id[0])); ++i)
	{
		if ( Output.trade_id[i] == 0 ) break;
		cout<<"status_name["<<i<<"]="<<Output.status_name[i];
		cout<<"trade_id["<<i<<"]="<<Output.trade_id[i]<<endl;
	}

	return(0);
}
