/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
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
	m_coutLock.lock();
	cout << "<<< CPF1" << endl;
	cout << "*** " << osCall.str() << endl;
	cout << "- Customer Position Frame 1 (input)" << endl <<
			"-- cust_id: " << pIn->cust_id << endl <<
			"-- tax_id: " << pIn->tax_id << endl;
	m_coutLock.unlock();
#endif // DEBUG

	startTransaction();
	// Isolation level required by Clause 7.4.1.3
	setReadCommitted();
	execute(pIn, pOut);

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Customer Position Frame 1 (output)" << endl <<
			"-- cust_id: " << pOut->cust_id << endl <<
			"-- acct_len: " << pOut->acct_len << endl;
	for (int i = 0; i < pOut->acct_len; i++) {
		cout << "-- acct_id[" << i << "]: " << pOut->acct_id[i] << endl <<
				"-- cash_bal[" << i << "]: " << pOut->cash_bal[i] << endl <<
				"-- asset_total[" << i << "]: " << pOut->asset_total[i] << endl;
	}
	cout << "-- c_st_id: " << pOut->c_st_id << endl <<
			"-- c_l_name: " << pOut->c_l_name << endl <<
			"-- c_f_name: " << pOut->c_f_name << endl <<
			"-- c_m_name: " << pOut->c_m_name << endl <<
			"-- c_gndr: " << pOut->c_gndr << endl <<
			"-- c_tier: " << pOut->c_tier << endl <<
			"-- c_dob: " << pOut->c_dob.year << "-" << pOut->c_dob.month <<
					"-" << pOut->c_dob.day << " " << pOut->c_dob.hour <<
					":" << pOut->c_dob.minute << ":" << pOut->c_dob.second <<
					endl <<
			"-- c_ad_id: " << pOut->c_ad_id << endl <<
			"-- c_ctry_1: " << pOut->c_ctry_1 << endl <<
			"-- c_area_1: " << pOut->c_area_1 << endl <<
			"-- c_local_1: " << pOut->c_local_1 << endl <<
			"-- c_ext_1: " << pOut->c_ext_1 << endl <<
			"-- c_ctry_2: " << pOut->c_ctry_2 << endl <<
			"-- c_area_2: " << pOut->c_area_2 << endl <<
			"-- c_local_2: " << pOut->c_local_2 << endl <<
			"-- c_ext_2: " << pOut->c_ext_2 << endl <<
			"-- c_ctry_3: " << pOut->c_ctry_3 << endl <<
			"-- c_area_3: " << pOut->c_area_3 << endl <<
			"-- c_local_3: " << pOut->c_local_3 << endl <<
			"-- c_ext_3: " << pOut->c_ext_3 << endl <<
			"-- c_email_1: " << pOut->c_email_1 << endl <<
			"-- c_email_2: " << pOut->c_email_2 << endl;
	cout << ">>> CPF1" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}

// Call Customer Position Frame 2
void CCustomerPositionDB::DoCustomerPositionFrame2(
		const TCustomerPositionFrame2Input *pIn,
		TCustomerPositionFrame2Output *pOut)
{
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< CPF2" << endl;
	cout << "- Customer Position Frame 2 (input)" << endl <<
			"-- cust_id: " << pIn->acct_id << endl;
	m_coutLock.unlock();
#endif // DEBUG

	execute(pIn, pOut);
	commitTransaction();

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Customer Position Frame 2 (output)" << endl <<
			"-- hist_len: " << pOut->hist_len << endl;
	for (int i = 0; i < pOut->hist_len; i++) {
		cout << "-- trade_id[" << i << "]: " << pOut->trade_id[i] << endl <<
				"-- symbol[" << i << "]: " << pOut->symbol[i] << endl <<
				"-- qty[" << i << "]: " << pOut->qty[i] << endl <<
				"-- trade_status[" << i << "]: " << pOut->trade_status[i] <<
						endl <<
				"-- hist_dts[" << i << "]: " <<
						pOut->hist_dts[i].year << "-" <<
						pOut->hist_dts[i].month << "-" <<
						pOut->hist_dts[i].day << " " <<
						pOut->hist_dts[i].hour << ":" <<
						pOut->hist_dts[i].minute << ":" <<
						pOut->hist_dts[i].second << endl;
	}
	cout << ">>> CPF2" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}

// Call Customer Position Frame 3
void CCustomerPositionDB::DoCustomerPositionFrame3(
		TCustomerPositionFrame3Output *pOut)
{
#ifdef DEBUG
	cout << "<<< CPF3" << endl;
#endif

	commitTransaction();
	pOut->status = CBaseTxnErr::SUCCESS;

#ifdef DEBUG
	cout << ">>> CPF3" << endl;
#endif
}
