/*
 * MarketFeedDB.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 21 July 2006
 */

#include <transactions.h>

using namespace TPCE;

// Constructor
CMarketFeedDB::CMarketFeedDB(CDBConnection *pDBConn)
: CTxnBaseDB(pDBConn)
{
}

// Destructor
CMarketFeedDB::~CMarketFeedDB()
{
}

// Call Market Feed Frame 1
void CMarketFeedDB::DoMarketFeedFrame1(PMarketFeedFrame1Input pFrame1Input, 
					PMarketFeedFrame1Output pFrame1Output,
					CSendToMarketInterface*	pSendToMarket)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osSymbol, osPrice, osQty;

	for (int i = 0; i < (sizeof(pFrame1Input->Entries)/sizeof(pFrame1Input->Entries[0])); ++i)
	{
		if (i == 0)
		{
			osSymbol << "\"" << pFrame1Input->Entries[i].symbol;
			osPrice << pFrame1Input->Entries[i].price_quote;
			osQty << pFrame1Input->Entries[i].trade_qty;
		}
		else
		{
			osSymbol << "\",\"" << pFrame1Input->Entries[i].symbol;
			osPrice << "," << pFrame1Input->Entries[i].price_quote;
			osQty << "," << pFrame1Input->Entries[i].trade_qty;
		}
	}
	osSymbol << "\"";

	ostringstream osCall;
	osCall << "select * from MarketFeedFrame1(" << max_feed_len <<
			"::smallint,'{" << osPrice.str() << "}','" <<
			pFrame1Input->StatusAndTradeType.status_submitted <<
			"'::char(4),'{" << osSymbol.str() << "}', '{" << osQty.str() <<
			"}','" << pFrame1Input->StatusAndTradeType.type_limit_buy <<
			"'::char(3),'" <<
			pFrame1Input->StatusAndTradeType.type_limit_sell <<
			"'::char(3),'" <<
			pFrame1Input->StatusAndTradeType.type_stop_loss <<
			"'::char(3)) as (symbol char(15), trade_id TRADE_T, price "
			"numeric(8,2), trade_quant integer, trade_type char(3))";

	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;");
	result R( m_Txn->exec( osCall.str() ) );
	// Sponsors should consider committing the database changes
	// before calling the send_to_market interface (we're doing that)
	CommitTxn();

	if (!R.empty()) 
	{
		result::const_iterator c = R.begin();
	
		int i = 0;
		bool bSent;	
		for ( c; c != R.end(); ++c )
		{
			sprintf( m_TriggeredLimitOrders.symbol, "%s", c[0].c_str());
			m_TriggeredLimitOrders.trade_id = c[1].as(int());
			m_TriggeredLimitOrders.price_quote =  c[2].as(double());
			m_TriggeredLimitOrders.trade_qty = c[3].as(int());
			sprintf( m_TriggeredLimitOrders.trade_type_id, "%s", c[4].c_str());

			bSent = pSendToMarket->SendToMarketFromFrame(
					m_TriggeredLimitOrders);
			if (bSent)
			{
				++i;
			}
		}
		pFrame1Output->send_len = i;	
	}
	else
	{
		pFrame1Output->send_len = 0;
	}
	pFrame1Output->status = CBaseTxnErr::SUCCESS;
	
#ifdef DEBUG m_coutLock.ClaimLock(); cout << "Market Feed Frame 1 (input)" << endl << "- max_feed_len: " << max_feed_len << endl <<
			"- price_quote: " << "{" << osPrice.str() << "}"<< endl <<
			"- status_submitted: " <<
			pFrame1Input->StatusAndTradeType.status_submitted << endl <<
			"- symbol: " << "{" << osSymbol.str() << "}" << endl <<
			"- trade_qty: " << "{" << osQty.str() << "}" << endl <<
			"- type_limit_buy: " <<
			pFrame1Input->StatusAndTradeType.type_limit_buy << endl <<
			"- type_limit_sell: " <<
			pFrame1Input->StatusAndTradeType.type_limit_sell << endl <<
			"- type_stop_loss: " <<
			pFrame1Input->StatusAndTradeType.type_stop_loss << endl;
	cout<<"Market Feed Frame 1 (output)"<<endl
	    <<"- send_len: "<<pFrame1Output->send_len<<endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG
	
#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}


void CMarketFeedDB::Cleanup(void* pException)
{
}
