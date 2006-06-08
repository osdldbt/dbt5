/*
*
*
*
*/

#ifndef TXN_HARNESS_TRADE_LOOKUP_H
#define TXN_HARNESS_TRADE_LOOKUP_H

#include <assert.h>

namespace TPCE
{

class CTradeLookup
{
	CTradeLookupDB	m_db;		

public:
	CTradeLookup( const char *szServer, const char *szDatabase )
		: m_db( szServer, szDatabase )
	{		
	};

	void DoTxn( PTradeLookupTxnInput pTxnInput, PTradeLookupTxnOutput pTxnOutput )
	{
		int i;

		memset( pTxnOutput, 0, sizeof( *pTxnOutput ));
		switch( pTxnInput->frame_to_execute )
		{
		case 1:
			TTradeLookupFrame1Input		Frame1Input;
			TTradeLookupFrame1Output	Frame1Output;
			memset(&Frame1Input, 0, sizeof( Frame1Input ));
			memset(&Frame1Output, 0, sizeof( Frame1Output ));

			Frame1Input.max_trades = pTxnInput->max_trades;
			memcpy( Frame1Input.trade_id, pTxnInput->trade_id, sizeof( Frame1Input.trade_id ));

			m_db.DoTradeLookupFrame1( &Frame1Input, &Frame1Output );

			pTxnOutput->frame_executed = 1;
			for (i = 0; i < Frame1Output.num_found; ++i)
			{
				pTxnOutput->is_cash[i] = Frame1Output.trade_info[i].is_cash;
				pTxnOutput->is_market[i] = Frame1Output.trade_info[i].is_market;
			}
			pTxnOutput->num_found = Frame1Output.num_found;
			pTxnOutput->status = Frame1Output.status;
			break;

		case 2:
			TTradeLookupFrame2Input		Frame2Input;
			TTradeLookupFrame2Output	Frame2Output;
			memset(&Frame2Input, 0, sizeof( Frame2Input ));
			memset(&Frame2Output, 0, sizeof( Frame2Output ));

			Frame2Input.acct_id = pTxnInput->acct_id;
			Frame2Input.max_trades = pTxnInput->max_trades;
			Frame2Input.trade_dts = pTxnInput->trade_dts;

			m_db.DoTradeLookupFrame2( &Frame2Input, &Frame2Output );

			pTxnOutput->frame_executed = 2;
			for (i = 0; i < Frame2Output.num_found; ++i)
			{
				pTxnOutput->is_cash[i] = Frame2Output.trade_info[i].is_cash;
				pTxnOutput->trade_list[i] = Frame2Output.trade_info[i].trade_id;
			}
			pTxnOutput->num_found = Frame2Output.num_found;
			pTxnOutput->status = Frame2Output.status;			
			break;

		case 3:
			TTradeLookupFrame3Input		Frame3Input;
			TTradeLookupFrame3Output	Frame3Output;
			memset(&Frame3Input, 0, sizeof( Frame3Input ));
			memset(&Frame3Output, 0, sizeof( Frame3Output ));

			Frame3Input.acct_id = pTxnInput->acct_id;
			Frame3Input.trade_dts = pTxnInput->trade_dts;

			m_db.DoTradeLookupFrame3( &Frame3Input, &Frame3Output );

			pTxnOutput->frame_executed = 3;
			pTxnOutput->num_found = Frame3Output.num_found;
			pTxnOutput->status = Frame3Output.status;
			pTxnOutput->trade_list[0] = Frame3Output.trade_id;
			break;

		case 4:
			TTradeLookupFrame4Input		Frame4Input;
			TTradeLookupFrame4Output	Frame4Output;
			memset(&Frame4Input, 0, sizeof( Frame4Input ));
			memset(&Frame4Output, 0, sizeof( Frame4Output ));

			Frame4Input.max_trades = pTxnInput->max_trades;
			strncpy( Frame4Input.symbol, pTxnInput->symbol, sizeof( Frame4Input.symbol ) -1 );
			Frame4Input.trade_dts = pTxnInput->trade_dts;
			Frame4Input.max_acct_id = pTxnInput->max_acct_id;

			m_db.DoTradeLookupFrame4( &Frame4Input, &Frame4Output );

			pTxnOutput->frame_executed = 4;
			pTxnOutput->num_found = Frame4Output.num_found;
			pTxnOutput->status = Frame4Output.status;
			for (i = 0; i < Frame4Output.num_found; ++i)
			{
				pTxnOutput->trade_list[i] = Frame4Output.trade_info[i].trade_id;
			}
			break;

		default:
			// should never get here.
			assert( false );
			break;
		}
	}

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup( void* pException )
	{
		m_db.Cleanup( pException );
	}
};

}	// namespace TPCE

#endif //TXN_HARNESS_TRADE_LOOKUP_H
