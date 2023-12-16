/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 12 July 2006
 */

#include "CustomerPositionDB.h"

CCustomerPositionDB::CCustomerPositionDB(
		CDBConnection *pDBConn, bool verbose = false)
: CTxnBaseDB(pDBConn), m_Verbose(verbose)
{
	m_pid = syscall(SYS_gettid);
}

// Call Customer Position Frame 1
void
CCustomerPositionDB::DoCustomerPositionFrame1(
		const TCustomerPositionFrame1Input *pIn,
		TCustomerPositionFrame1Output *pOut)
{
	if (m_Verbose) {
		cout << m_pid << " <<< CPF1" << endl
			 << m_pid << " - Customer Position Frame 1 (input)" << endl
			 << m_pid << " -- cust_id: " << pIn->cust_id << endl
			 << m_pid << " -- tax_id: " << pIn->tax_id << endl;
	}

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);

	if (m_Verbose) {
		cout << m_pid << " - Customer Position Frame 1 (output)" << endl
			 << m_pid << " -- cust_id: " << pOut->cust_id << endl
			 << m_pid << " -- acct_len: " << pOut->acct_len << endl;
		for (int i = 0; i < pOut->acct_len; i++) {
			cout << m_pid << " -- acct_id[" << i << "]: " << pOut->acct_id[i]
				 << endl
				 << m_pid << " -- cash_bal[" << i << "]: " << pOut->cash_bal[i]
				 << endl
				 << m_pid << " -- asset_total[" << i
				 << "]: " << pOut->asset_total[i] << endl;
		}
		cout << m_pid << " -- c_st_id: " << pOut->c_st_id << endl
			 << m_pid << " -- c_l_name: " << pOut->c_l_name << endl
			 << m_pid << " -- c_f_name: " << pOut->c_f_name << endl
			 << m_pid << " -- c_m_name: " << pOut->c_m_name << endl
			 << m_pid << " -- c_gndr: " << pOut->c_gndr << endl
			 << m_pid << " -- c_tier: " << pOut->c_tier << endl
			 << m_pid << " -- c_dob: " << pOut->c_dob.year << "-"
			 << pOut->c_dob.month << "-" << pOut->c_dob.day << " "
			 << pOut->c_dob.hour << ":" << pOut->c_dob.minute << ":"
			 << pOut->c_dob.second << endl
			 << m_pid << " -- c_ad_id: " << pOut->c_ad_id << endl
			 << m_pid << " -- c_ctry_1: " << pOut->c_ctry_1 << endl
			 << m_pid << " -- c_area_1: " << pOut->c_area_1 << endl
			 << m_pid << " -- c_local_1: " << pOut->c_local_1 << endl
			 << m_pid << " -- c_ext_1: " << pOut->c_ext_1 << endl
			 << m_pid << " -- c_ctry_2: " << pOut->c_ctry_2 << endl
			 << m_pid << " -- c_area_2: " << pOut->c_area_2 << endl
			 << m_pid << " -- c_local_2: " << pOut->c_local_2 << endl
			 << m_pid << " -- c_ext_2: " << pOut->c_ext_2 << endl
			 << m_pid << " -- c_ctry_3: " << pOut->c_ctry_3 << endl
			 << m_pid << " -- c_area_3: " << pOut->c_area_3 << endl
			 << m_pid << " -- c_local_3: " << pOut->c_local_3 << endl
			 << m_pid << " -- c_ext_3: " << pOut->c_ext_3 << endl
			 << m_pid << " -- c_email_1: " << pOut->c_email_1 << endl
			 << m_pid << " -- c_email_2: " << pOut->c_email_2 << endl
			 << m_pid << " >>> CPF1" << endl;
	}
}

// Call Customer Position Frame 2
void
CCustomerPositionDB::DoCustomerPositionFrame2(
		const TCustomerPositionFrame2Input *pIn,
		TCustomerPositionFrame2Output *pOut)
{
	if (m_Verbose) {
		cout << m_pid << " <<< CPF2" << endl
			 << m_pid << " - Customer Position Frame 2 (input)" << endl
			 << m_pid << " -- cust_id: " << pIn->acct_id << endl;
	}

	execute(pIn, pOut);
	commitTransaction();

	if (m_Verbose) {
		cout << m_pid << " - Customer Position Frame 2 (output)" << endl
			 << m_pid << " -- hist_len: " << pOut->hist_len << endl;
		for (int i = 0; i < pOut->hist_len; i++) {
			cout << m_pid << " -- trade_id[" << i << "]: " << pOut->trade_id[i]
				 << endl
				 << m_pid << " -- symbol[" << i << "]: " << pOut->symbol[i]
				 << endl
				 << m_pid << " -- qty[" << i << "]: " << pOut->qty[i] << endl
				 << m_pid << " -- trade_status[" << i
				 << "]: " << pOut->trade_status[i] << endl
				 << m_pid << " -- hist_dts[" << i
				 << "]: " << pOut->hist_dts[i].year << "-"
				 << pOut->hist_dts[i].month << "-" << pOut->hist_dts[i].day
				 << " " << pOut->hist_dts[i].hour << ":"
				 << pOut->hist_dts[i].minute << ":" << pOut->hist_dts[i].second
				 << endl;
		}
		cout << m_pid << " >>> CPF2" << endl;
	}
}

// Call Customer Position Frame 3
void
CCustomerPositionDB::DoCustomerPositionFrame3()
{
	if (m_Verbose) {
		cout << m_pid << " <<< CPF3" << endl;
	}

	commitTransaction();

	if (m_Verbose) {
		cout << m_pid << " >>> CPF3" << endl;
	}
}
