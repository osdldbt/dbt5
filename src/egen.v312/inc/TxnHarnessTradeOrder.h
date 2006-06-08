/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Trade Order transaction class.
*
*/
#ifndef TXN_HARNESS_TRADE_ORDER_H
#define TXN_HARNESS_TRADE_ORDER_H

#include "error.h"
#include <stdio.h>

namespace TPCE
{

class CTradeOrder
{
	CTradeOrderDB		m_db;
	char				m_exec_name[cF_NAME_len+cL_NAME_len+1];
	char				m_b_name[cB_NAME_len+1];	
	int					m_h_qty;
	double				m_approx_cost;
	double				m_cs_balance;
	double				m_value_hold;
	double				m_trade_value;
	double				m_tax_amount;
	double				m_comm_amount;
	bool				m_margin;
	INT32				m_type_is_sell;
	INT32				m_type_is_market;
	char				m_trade_name[cTT_NAME_len+1];
	TTradeRequest		m_trade_req;
	CSendToMarketInterface *m_pSendToMarket;

public:
	CTradeOrder(const char *szDBServer, const char *szDatabase, CSendToMarketInterface *pSendToMarket)
		: m_db(szDBServer, szDatabase),
		m_pSendToMarket(pSendToMarket)
	{
	};

	void DoTxn( PTradeOrderTxnInput pTxnInput, PTradeOrderTxnOutput pTxnOutput )
	{
		TTradeOrderFrame1Input	Frame1Input;
		TTradeOrderFrame1Output	Frame1Output;
		memset(&Frame1Input, 0, sizeof( Frame1Input ));
		memset(&Frame1Output, 0, sizeof( Frame1Output ));

		TTradeOrderFrame2Input	Frame2Input;
		TTradeOrderFrame2Output	Frame2Output;
		memset(&Frame2Input, 0, sizeof( Frame2Input ));
		memset(&Frame2Output, 0, sizeof( Frame2Output ));

		TTradeOrderFrame3Input	Frame3Input;
		TTradeOrderFrame3Output	Frame3Output;
		memset(&Frame3Input, 0, sizeof( Frame3Input ));
		memset(&Frame3Output, 0, sizeof( Frame3Output ));

		TTradeOrderFrame4Input	Frame4Input;
		TTradeOrderFrame4Output	Frame4Output;
		memset(&Frame4Input, 0, sizeof( Frame4Input ));
		memset(&Frame4Output, 0, sizeof( Frame4Output ));

		TTradeOrderFrame5Output	Frame5Output;
		memset(&Frame5Output, 0, sizeof( Frame5Output ));

		TTradeOrderFrame6Output	Frame6Output;
		memset(&Frame6Output, 0, sizeof( Frame6Output ));

		TTradeRequest			TradeRequestForMEE;	//sent to MEE

		//Init Frame1 input params
		Frame1Input.acct_id = pTxnInput->acct_id;
		m_db.DoTradeOrderFrame1(&Frame1Input, &Frame1Output );

		Frame2Output.bad_permission = false;

		if (strcmp(pTxnInput->exec_l_name, Frame1Output.cust_l_name)
			|| strcmp(pTxnInput->exec_f_name, Frame1Output.cust_f_name)
			|| strcmp(pTxnInput->exec_tax_id, Frame1Output.tax_id))
		{
			Frame2Input.acct_id = pTxnInput->acct_id;
			strncpy(Frame2Input.exec_f_name, pTxnInput->exec_f_name, sizeof(Frame2Input.exec_f_name));
			strncpy(Frame2Input.exec_l_name, pTxnInput->exec_l_name, sizeof(Frame2Input.exec_l_name));
			strncpy(Frame2Input.exec_tax_id, pTxnInput->exec_tax_id, sizeof(Frame2Input.exec_tax_id));

			m_db.DoTradeOrderFrame2(&Frame2Input, &Frame2Output);

			if (Frame2Output.bad_permission)
			{	//Frame 2 found unauthorized executor
				m_db.DoTradeOrderFrame5(&Frame5Output);	//Rollback
				pTxnOutput->status = CBaseTxnErr::UNAUTHORIZED_EXECUTOR;	//return error code
				return;
			}

			pTxnOutput->status = Frame2Output.status;			
		}

		//Init Frame 3 input params
		Frame3Input.acct_id = pTxnInput->acct_id;
		Frame3Input.cust_id = Frame1Output.cust_id;
		Frame3Input.cust_tier = Frame1Output.cust_tier;
		Frame3Input.is_lifo = pTxnInput->is_lifo;
		strncpy(Frame3Input.issue, pTxnInput->issue, sizeof(Frame3Input.issue));
		strncpy(Frame3Input.st_pending_id, pTxnInput->st_pending_id, sizeof(Frame3Input.st_pending_id));
		strncpy(Frame3Input.st_submitted_id, pTxnInput->st_submitted_id, sizeof(Frame3Input.st_submitted_id));
		Frame3Input.tax_status = Frame1Output.tax_status;
		Frame3Input.trade_qty = pTxnInput->trade_qty;
		strncpy(Frame3Input.trade_type_id, pTxnInput->trade_type_id, sizeof(Frame3Input.trade_type_id));
		Frame3Input.type_is_margin = pTxnInput->type_is_margin;
		strncpy(Frame3Input.co_name, pTxnInput->co_name, sizeof(Frame3Input.co_name));
		Frame3Input.requested_price = pTxnInput->requested_price;
		strncpy(Frame3Input.symbol, pTxnInput->symbol, sizeof(Frame3Input.symbol));

		m_db.DoTradeOrderFrame3(&Frame3Input, &Frame3Output);

		//Frame 4 input params
		Frame4Input.acct_id = pTxnInput->acct_id;
		Frame4Input.charge_amount = Frame3Output.charge_amount;
		Frame4Input.comm_amount = Frame3Output.comm_rate / 100 
										  * pTxnInput->trade_qty 
										  * Frame3Output.requested_price;
		// round up for correct precision (cents only)
		Frame4Input.comm_amount = (double)((int)(100.00 * Frame4Input.comm_amount + 0.5)) / 100.00;

		sprintf(Frame4Input.exec_name, "%s %s", pTxnInput->exec_f_name, pTxnInput->exec_l_name);
		Frame4Input.is_cash = !Frame3Input.type_is_margin;
		Frame4Input.is_lifo = pTxnInput->is_lifo;
		Frame4Input.requested_price = Frame3Output.requested_price;
		strncpy(Frame4Input.status_id, Frame3Output.status_id, sizeof(Frame4Input.status_id));
		strncpy(Frame4Input.symbol, Frame3Output.symbol, sizeof(Frame4Input.symbol));
		Frame4Input.tax_amount = Frame3Output.tax_amount;
		Frame4Input.trade_qty = pTxnInput->trade_qty;
		strncpy(Frame4Input.trade_type_id, pTxnInput->trade_type_id, sizeof(Frame4Input.trade_type_id));
		Frame4Input.type_is_market = Frame3Output.type_is_market;

		m_db.DoTradeOrderFrame4(&Frame4Input, &Frame4Output);

		pTxnOutput->trade_id = Frame4Output.trade_id;	//output param
		pTxnOutput->status = Frame4Output.status;

		if (pTxnInput->roll_it_back)
		{
			m_db.DoTradeOrderFrame5(&Frame5Output);
			pTxnOutput->status = Frame5Output.status;
		}
		else
		{
			m_db.DoTradeOrderFrame6(&Frame6Output);
			pTxnOutput->status = Frame6Output.status;		

			//
			//Send to Market Emulator here.
			//
			TradeRequestForMEE.price_quote = Frame4Input.requested_price;
			strncpy(TradeRequestForMEE.symbol, Frame4Input.symbol, sizeof(TradeRequestForMEE.symbol));
			TradeRequestForMEE.trade_id = Frame4Output.trade_id;
			TradeRequestForMEE.trade_qty = Frame4Input.trade_qty;
			strncpy( TradeRequestForMEE.trade_type_id, pTxnInput->trade_type_id, sizeof( TradeRequestForMEE.trade_type_id ));
			//TradeRequestForMEE.eTradeType = pTxnInput->eSTMTradeType;
			if( Frame4Input.type_is_market )
			{
				TradeRequestForMEE.eAction = eMEEProcessOrder;
			}
			else
			{
				TradeRequestForMEE.eAction = eMEESetLimitOrderTrigger;
			}

			m_pSendToMarket->SendToMarket(TradeRequestForMEE); // maybe should check the return code here
		}

	}

	// Function to pass any exception thrown inside
	// database class frame implementation
	// back into the database class
	void Cleanup(void* pException)
	{
		m_db.Cleanup(pException);
	}
};

}	// namespace TPCE

#endif //TXN_HARNESS_TRADE_ORDER_H
