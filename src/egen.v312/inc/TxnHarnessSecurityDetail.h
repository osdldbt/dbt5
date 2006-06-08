/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
#ifndef TXN_HARNESS_SECURITY_DETAIL_H
#define TXN_HARNESS_SECURITY_DETAIL_H

namespace TPCE
{

class CSecurityDetail
{
	CSecurityDetailDB	m_db;		

public:
	CSecurityDetail(const char *szServer, const char *szDatabase)
		: m_db(szServer, szDatabase)
	{		
	};

	void DoTxn( PSecurityDetailTxnInput pTxnInput, PSecurityDetailTxnOutput pTxnOutput )
	{				
		TSecurityDetailFrame1Output	Frame1Output;
		// We purposely do not memset the whole structure to 0
		// because of the large LOB members. So instead we just
		// 0 out the specific members that get returned out.
		//memset(&Frame1Output, 0, sizeof( Frame1Output ));
		pTxnOutput->last_vol = 0;
		pTxnOutput->news_len = 0;
		pTxnOutput->status = 0;

		m_db.DoSecurityDetailFrame1(pTxnInput, &Frame1Output);

		pTxnOutput->last_vol = Frame1Output.last_vol;
		pTxnOutput->news_len = Frame1Output.news_len;
		pTxnOutput->status = Frame1Output.status;
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

#endif //TXN_HARNESS_SECURITY_DETAIL_H
