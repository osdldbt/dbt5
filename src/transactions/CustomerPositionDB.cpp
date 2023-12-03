/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 *
 * 12 July 2006
 */

#include "CustomerPositionDB.h"

// Call Customer Position Frame 1
void CCustomerPositionDB::DoCustomerPositionFrame1(
		const TCustomerPositionFrame1Input *pIn,
		TCustomerPositionFrame1Output *pOut)
{
#ifdef DEBUG
	pid_t pid = syscall(SYS_gettid);
	cout << pid << " <<< CPF1" << endl;
	cout << pid << " - Customer Position Frame 1 (input)" << endl <<
			pid << " -- cust_id: " << pIn->cust_id << endl <<
			pid << " -- tax_id: " << pIn->tax_id << endl;
#endif // DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);

#ifdef DEBUG
	cout << pid << " - Customer Position Frame 1 (output)" << endl <<
			pid << " -- cust_id: " << pOut->cust_id << endl <<
			pid << " -- acct_len: " << pOut->acct_len << endl;
	for (int i = 0; i < pOut->acct_len; i++) {
		cout << pid << " -- acct_id[" << i << "]: " << pOut->acct_id[i] <<
						endl <<
				pid << " -- cash_bal[" << i << "]: " << pOut->cash_bal[i] <<
								endl <<
				pid << " -- asset_total[" << i << "]: " <<
								pOut->asset_total[i] << endl;
	}
	cout << pid << " -- c_st_id: " << pOut->c_st_id << endl <<
			pid << " -- c_l_name: " << pOut->c_l_name << endl <<
			pid << " -- c_f_name: " << pOut->c_f_name << endl <<
			pid << " -- c_m_name: " << pOut->c_m_name << endl <<
			pid << " -- c_gndr: " << pOut->c_gndr << endl <<
			pid << " -- c_tier: " << pOut->c_tier << endl <<
			pid << " -- c_dob: " << pOut->c_dob.year << "-" <<
					pOut->c_dob.month << "-" << pOut->c_dob.day << " " <<
					pOut->c_dob.hour << ":" << pOut->c_dob.minute << ":" <<
					pOut->c_dob.second << endl <<
			pid << " -- c_ad_id: " << pOut->c_ad_id << endl <<
			pid << " -- c_ctry_1: " << pOut->c_ctry_1 << endl <<
			pid << " -- c_area_1: " << pOut->c_area_1 << endl <<
			pid << " -- c_local_1: " << pOut->c_local_1 << endl <<
			pid << " -- c_ext_1: " << pOut->c_ext_1 << endl <<
			pid << " -- c_ctry_2: " << pOut->c_ctry_2 << endl <<
			pid << " -- c_area_2: " << pOut->c_area_2 << endl <<
			pid << " -- c_local_2: " << pOut->c_local_2 << endl <<
			pid << " -- c_ext_2: " << pOut->c_ext_2 << endl <<
			pid << " -- c_ctry_3: " << pOut->c_ctry_3 << endl <<
			pid << " -- c_area_3: " << pOut->c_area_3 << endl <<
			pid << " -- c_local_3: " << pOut->c_local_3 << endl <<
			pid << " -- c_ext_3: " << pOut->c_ext_3 << endl <<
			pid << " -- c_email_1: " << pOut->c_email_1 << endl <<
			pid << " -- c_email_2: " << pOut->c_email_2 << endl;
	cout << pid << " >>> CPF1" << endl;
#endif // DEBUG
}

// Call Customer Position Frame 2
void CCustomerPositionDB::DoCustomerPositionFrame2(
		const TCustomerPositionFrame2Input *pIn,
		TCustomerPositionFrame2Output *pOut)
{
#ifdef DEBUG
	pid_t pid = syscall(SYS_gettid);
	cout << pid << " <<< CPF2" << endl;
	cout << pid << " - Customer Position Frame 2 (input)" << endl <<
			pid << " -- cust_id: " << pIn->acct_id << endl;
#endif // DEBUG

	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	cout << pid << " - Customer Position Frame 2 (output)" << endl <<
			pid << " -- hist_len: " << pOut->hist_len << endl;
	for (int i = 0; i < pOut->hist_len; i++) {
		cout << pid << " -- trade_id[" << i << "]: " << pOut->trade_id[i] <<
						endl <<
				pid << " -- symbol[" << i << "]: " << pOut->symbol[i] << endl <<
				pid << " -- qty[" << i << "]: " << pOut->qty[i] << endl <<
				pid << " -- trade_status[" << i << "]: " <<
						pOut->trade_status[i] << endl <<
				pid << " -- hist_dts[" << i << "]: " <<
						pOut->hist_dts[i].year << "-" <<
						pOut->hist_dts[i].month << "-" <<
						pOut->hist_dts[i].day << " " <<
						pOut->hist_dts[i].hour << ":" <<
						pOut->hist_dts[i].minute << ":" <<
						pOut->hist_dts[i].second << endl;
	}
	cout << pid << " >>> CPF2" << endl;
#endif // DEBUG
}

// Call Customer Position Frame 3
void CCustomerPositionDB::DoCustomerPositionFrame3()
{
#ifdef DEBUG
	pid_t pid = syscall(SYS_gettid);
	cout << pid << " <<< CPF3" << endl;
#endif

	commitTransaction();

#ifdef DEBUG
	cout << pid << " >>> CPF3" << endl;
#endif
}
