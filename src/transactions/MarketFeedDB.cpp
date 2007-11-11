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
void CMarketFeedDB::DoMarketFeedFrame1(
		const TMarketFeedFrame1Input *pIn, TMarketFeedFrame1Output *pOut,
		CSendToMarketInterface *pSendToMarket)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osSymbol, osPrice, osQty;

	for (int i = 0; i < (sizeof(pIn->Entries)/sizeof(pIn->Entries[0])); ++i)
	{
		if (i == 0)
		{
			osSymbol << "\"" << pIn->Entries[i].symbol;
			osPrice << pIn->Entries[i].price_quote;
			osQty << pIn->Entries[i].trade_qty;
		}
		else
		{
			osSymbol << "\",\"" << pIn->Entries[i].symbol;
			osPrice << "," << pIn->Entries[i].price_quote;
			osQty << "," << pIn->Entries[i].trade_qty;
		}
	}
	osSymbol << "\"";

	ostringstream osCall;
	osCall << "select * from MarketFeedFrame1(" << max_feed_len <<
			"::smallint,'{" << osPrice.str() << "}','" <<
			pIn->StatusAndTradeType.status_submitted <<
			"'::char(4),'{" << osSymbol.str() << "}', '{" << osQty.str() <<
			"}','" << pIn->StatusAndTradeType.type_limit_buy <<
			"'::char(3),'" <<
			pIn->StatusAndTradeType.type_limit_sell <<
			"'::char(3),'" <<
			pIn->StatusAndTradeType.type_stop_loss <<
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
			strcpy(m_TriggeredLimitOrders.symbol, c[0].c_str());
			m_TriggeredLimitOrders.trade_id = c[1].as(int());
			m_TriggeredLimitOrders.price_quote =  c[2].as(double());
			m_TriggeredLimitOrders.trade_qty = c[3].as(int());
			strcpy(m_TriggeredLimitOrders.trade_type_id, c[4].c_str());

			bSent = pSendToMarket->SendToMarketFromFrame(
					m_TriggeredLimitOrders);
			if (bSent)
			{
				++i;
			}
		}
		pOut->send_len = i;	
	}
	else
	{
		pOut->send_len = 0;
	}
	pOut->status = CBaseTxnErr::SUCCESS;
	
#ifdef DEBUG
	m_coutLock.ClaimLock(); cout << "Market Feed Frame 1 (input)" << endl << "- max_feed_len: " << max_feed_len << endl <<
			"- price_quote: " << "{" << osPrice.str() << "}"<< endl <<
			"- status_submitted: " <<
			pIn->StatusAndTradeType.status_submitted << endl <<
			"- symbol: " << "{" << osSymbol.str() << "}" << endl <<
			"- trade_qty: " << "{" << osQty.str() << "}" << endl <<
			"- type_limit_buy: " <<
			pIn->StatusAndTradeType.type_limit_buy << endl <<
			"- type_limit_sell: " <<
			pIn->StatusAndTradeType.type_limit_sell << endl <<
			"- type_stop_loss: " <<
			pIn->StatusAndTradeType.type_stop_loss << endl;
	cout<<"Market Feed Frame 1 (output)"<<endl
	    <<"- send_len: "<<pOut->send_len<<endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG
}


void CMarketFeedDB::Cleanup(void* pException)
{
}
