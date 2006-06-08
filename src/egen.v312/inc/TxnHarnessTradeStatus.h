/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
#ifndef TXN_HARNESS_TRADE_STATUS_H
#define TXN_HARNESS_TRADE_STATUS_H

namespace TPCE
{

class CTradeStatus
{
	CTradeStatusDB	m_db;		

public:
	CTradeStatus(const char *szServer, const char *szDatabase)
		: m_db(szServer, szDatabase)
	{		
	}

	void DoTxn( PTradeStatusTxnInput pTxnInput, PTradeStatusTxnOutput pTxnOutput)
	{
		TTradeStatusFrame1Output	Frame1Output;
		memset(&Frame1Output, 0, sizeof( Frame1Output ));

		m_db.DoTradeStatusFrame1(pTxnInput, &Frame1Output);

		pTxnOutput->status = Frame1Output.status;
		strncpy( pTxnOutput->status_name, Frame1Output.status_name, sizeof( pTxnOutput->status_name ) - 1 );
		pTxnOutput->trade_id = Frame1Output.trade_id;
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

#endif //TXN_HARNESS_TRADE_STATUS_H
