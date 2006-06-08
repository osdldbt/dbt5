/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Customer Position transaction class.
*
*/
#ifndef TXN_HARNESS_CUSTOMER_POSITION_H
#define TXN_HARNESS_CUSTOMER_POSITION_H

namespace TPCE
{

class CCustomerPosition
{
	CCustomerPositionDB	m_db;		
	
public:
	CCustomerPosition(const char *szServer, const char *szDatabase)
		: m_db(szServer, szDatabase)
	{		
	};

	void DoTxn( PCustomerPositionTxnInput pTxnInput, PCustomerPositionTxnOutput pTxnOutput)
	{
		TCustomerPositionFrame1Input	CustomerPositionFrame1Input;
		TCustomerPositionFrame1Output	CustomerPositionFrame1Output;

		TCustomerPositionFrame2Input	CustomerPositionFrame2Input;
		TCustomerPositionFrame2Output	CustomerPositionFrame2Output;

		TCustomerPositionFrame3Output	CustomerPositionFrame3Output;

		memset(&CustomerPositionFrame1Input, 0, sizeof(CustomerPositionFrame1Input));
		memset(&CustomerPositionFrame1Output, 0, sizeof(CustomerPositionFrame1Output));

		memset(&CustomerPositionFrame2Input, 0, sizeof(CustomerPositionFrame2Input));
		memset(&CustomerPositionFrame2Output, 0, sizeof(CustomerPositionFrame2Output));

		memset(&CustomerPositionFrame3Output, 0, sizeof(CustomerPositionFrame3Output));
		
		CustomerPositionFrame1Input.cust_id = pTxnInput->cust_id;
		strncpy(CustomerPositionFrame1Input.tax_id, pTxnInput->tax_id, sizeof(CustomerPositionFrame1Input.tax_id)-1);

		m_db.DoCustomerPositionFrame1(&CustomerPositionFrame1Input, &CustomerPositionFrame1Output);

		// Copy Frame 1 outputs to transaction level outputs.

		pTxnOutput->acct_len = CustomerPositionFrame1Output.acct_len;
		pTxnOutput->c_ad_id = CustomerPositionFrame1Output.c_ad_id;
		strncpy(pTxnOutput->c_area_1, CustomerPositionFrame1Output.c_area_1, sizeof(pTxnOutput->c_area_1) - 1);
		strncpy(pTxnOutput->c_area_2, CustomerPositionFrame1Output.c_area_2, sizeof(pTxnOutput->c_area_2) - 1);
		strncpy(pTxnOutput->c_area_3, CustomerPositionFrame1Output.c_area_3, sizeof(pTxnOutput->c_area_3) - 1);
		strncpy(pTxnOutput->c_ctry_1, CustomerPositionFrame1Output.c_ctry_1, sizeof(pTxnOutput->c_ctry_1) - 1);
		strncpy(pTxnOutput->c_ctry_2, CustomerPositionFrame1Output.c_ctry_2, sizeof(pTxnOutput->c_ctry_2) - 1);
		strncpy(pTxnOutput->c_ctry_3, CustomerPositionFrame1Output.c_ctry_3, sizeof(pTxnOutput->c_ctry_3) - 1);
		pTxnOutput->c_dob = CustomerPositionFrame1Output.c_dob;
		strncpy(pTxnOutput->c_email_1, CustomerPositionFrame1Output.c_email_1, sizeof(pTxnOutput->c_email_1) - 1);
		strncpy(pTxnOutput->c_email_2, CustomerPositionFrame1Output.c_email_2, sizeof(pTxnOutput->c_email_2) - 1);
		strncpy(pTxnOutput->c_ext_1, CustomerPositionFrame1Output.c_ext_1, sizeof(pTxnOutput->c_ext_1) - 1);
		strncpy(pTxnOutput->c_ext_2, CustomerPositionFrame1Output.c_ext_2, sizeof(pTxnOutput->c_ext_2) - 1);
		strncpy(pTxnOutput->c_ext_3, CustomerPositionFrame1Output.c_ext_3, sizeof(pTxnOutput->c_ext_3) - 1);
		strncpy(pTxnOutput->c_f_name, CustomerPositionFrame1Output.c_f_name, sizeof(pTxnOutput->c_f_name) - 1);
		strncpy(pTxnOutput->c_gndr, CustomerPositionFrame1Output.c_gndr, sizeof(pTxnOutput->c_gndr) - 1);
		strncpy(pTxnOutput->c_l_name, CustomerPositionFrame1Output.c_l_name, sizeof(pTxnOutput->c_l_name) - 1);
		strncpy(pTxnOutput->c_local_1, CustomerPositionFrame1Output.c_local_1, sizeof(pTxnOutput->c_local_1) - 1);
		strncpy(pTxnOutput->c_local_2, CustomerPositionFrame1Output.c_local_2, sizeof(pTxnOutput->c_local_2) - 1);
		strncpy(pTxnOutput->c_local_3, CustomerPositionFrame1Output.c_local_3, sizeof(pTxnOutput->c_local_3) - 1);
		strncpy(pTxnOutput->c_m_name, CustomerPositionFrame1Output.c_m_name, sizeof(pTxnOutput->c_m_name) - 1);
		strncpy(pTxnOutput->c_st_id, CustomerPositionFrame1Output.c_st_id, sizeof(pTxnOutput->c_st_id) - 1);
		pTxnOutput->c_tier = CustomerPositionFrame1Output.c_tier;
		//pTxnOutput->cust_id = CustomerPositionFrame1Output.cust_id;

		for (int i=0; i<pTxnOutput->acct_len; i++)
		{
			pTxnOutput->acct_id[i] = CustomerPositionFrame1Output.acct_id[i];
			pTxnOutput->asset_total[i] = CustomerPositionFrame1Output.asset_total[i];
			pTxnOutput->cash_bal[i] = CustomerPositionFrame1Output.cash_bal[i];
		}

		if (pTxnInput->get_history)
		{
			CustomerPositionFrame2Input.acct_id = CustomerPositionFrame1Output.acct_id[ pTxnInput->acct_id_idx ];
			m_db.DoCustomerPositionFrame2(&CustomerPositionFrame2Input, &CustomerPositionFrame2Output);

			pTxnOutput->status = CustomerPositionFrame2Output.status;
			// Copy Frame 2 outputs to transaction level outputs.

			pTxnOutput->hist_len = CustomerPositionFrame2Output.hist_len;
			for (int i=0; i<pTxnOutput->hist_len; i++)
			{
				pTxnOutput->hist_dts[i] = CustomerPositionFrame2Output.hist_dts[i];
				pTxnOutput->qty[i] = CustomerPositionFrame2Output.qty[i];
				strncpy(pTxnOutput->symbol[i], CustomerPositionFrame2Output.symbol[i], sizeof(pTxnOutput->symbol[i]) - 1);
				pTxnOutput->trade_id[i] = CustomerPositionFrame2Output.trade_id[i];
				strncpy(pTxnOutput->trade_status[i], CustomerPositionFrame2Output.trade_status[i], sizeof(pTxnOutput->trade_status[i]) - 1);
			}
		}
		else
		{
			//commit here
			m_db.DoCustomerPositionFrame3(&CustomerPositionFrame3Output);

			pTxnOutput->status = CustomerPositionFrame3Output.status;
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

#endif //TXN_HARNESS_CUSTOMER_POSITION_H
