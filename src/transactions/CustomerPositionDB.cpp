/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * 12 July 2006
 */

#include "transactions.h"

// Call Customer Position Frame 1
void CCustomerPositionDB::DoCustomerPositionFrame1(
		const TCustomerPositionFrame1Input *pIn,
		TCustomerPositionFrame1Output *pOut)
{
	ostringstream osCall;
	osCall << "SELECT * FROM CustomerPositionFrame1(" <<
			pIn->cust_id << ",'" <<
			pIn->tax_id << "')";
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< CPF1" << endl;
	cout << "*** " << osCall.str() << endl;
	cout << "- Customer Position Frame 1 (input)" << endl <<
			"-- cust_id: " << pIn->cust_id << endl <<
			"-- tax_id: " << pIn->tax_id << endl;
	m_coutLock.unlock();
#endif // DEBUG

	begin();
	// Isolation level required by Clause 7.4.1.3
	execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
	result R(execute(osCall.str()));

	if (R.empty())
	{
		cout << "warning: empty result set at DoCustomerPositionFrame1" <<
				endl <<
				osCall.str() << endl;
		pOut->status = CBaseTxnErr::ROLLBACK;
		return;
	}
	result::const_iterator c = R.begin();

	enum cpf1 {
			i_cust_id=0, i_acct_id, i_acct_len, i_asset_total, i_c_ad_id,
			i_c_area_1, i_c_area_2, i_c_area_3, i_c_ctry_1, i_c_ctry_2,
			i_c_ctry_3, i_c_dob, i_c_email_1, i_c_email_2, i_c_ext_1,
			i_c_ext_2, i_c_ext_3, i_c_f_name, i_c_gndr, i_c_l_name,
			i_c_local_1, i_c_local_2, i_c_local_3, i_c_m_name, i_c_st_id,
			i_c_tier, i_cash_bal, i_status
	};

	pOut->acct_len = c[i_acct_len].as(int());

	vector<string> vAux;
	vector<string>::iterator p;

	pOut->cust_id = c[i_cust_id].as(long());

	// Tokenize acct_id
	TokenizeSmart(c[i_acct_id].c_str(), vAux);
	int i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->acct_id[i] = atol( (*p).c_str() );
		++i;
	}
	check_count(pOut->acct_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	// Tokenize asset_total
	TokenizeSmart(c[i_asset_total].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->asset_total[i] = atof( (*p).c_str() );
		++i;
	}
	check_count(pOut->acct_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->c_ad_id = c[i_c_ad_id].as(long());

	strncpy(pOut->c_area_1, c[i_c_area_1].c_str(), cAREA_len);
	strncpy(pOut->c_area_2, c[i_c_area_2].c_str(), cAREA_len);
	strncpy(pOut->c_area_3, c[i_c_area_3].c_str(), cAREA_len);

	strncpy(pOut->c_ctry_1, c[i_c_ctry_1].c_str(), cCTRY_len);
	strncpy(pOut->c_ctry_2, c[i_c_ctry_2].c_str(), cCTRY_len);
	strncpy(pOut->c_ctry_3, c[i_c_ctry_3].c_str(), cCTRY_len);

	sscanf(c[i_c_dob].c_str(), "%d-%d-%d", &pOut->c_dob.year,
			&pOut->c_dob.month, &pOut->c_dob.day);

	strncpy(pOut->c_email_1, c[i_c_email_1].c_str(), cEMAIL_len);
	strncpy(pOut->c_email_2, c[i_c_email_2].c_str(), cEMAIL_len);

	strncpy(pOut->c_ext_1, c[i_c_ext_1].c_str(), cEXT_len);
	strncpy(pOut->c_ext_2, c[i_c_ext_2].c_str(), cEXT_len);
	strncpy(pOut->c_ext_3, c[i_c_ext_3].c_str(), cEXT_len);

	strncpy(pOut->c_f_name, c[i_c_f_name].c_str(), cF_NAME_len);
	strncpy(pOut->c_gndr, c[i_c_gndr].c_str(), cGNDR_len);
	strncpy(pOut->c_l_name, c[i_c_l_name].c_str(), cL_NAME_len);

	strncpy(pOut->c_local_1, c[i_c_local_1].c_str(), cLOCAL_len);
	strncpy(pOut->c_local_2, c[i_c_local_2].c_str(), cLOCAL_len);
	strncpy(pOut->c_local_3, c[i_c_local_3].c_str(), cLOCAL_len);

	strncpy(pOut->c_m_name, c[i_c_m_name].c_str(), cM_NAME_len);
	strncpy(pOut->c_st_id, c[i_c_st_id].c_str(), cST_ID_len);
	strncpy(&pOut->c_tier, c[i_c_tier].c_str(), 1);

	// Tokenize cash_bal
	TokenizeSmart(c[i_cash_bal].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->cash_bal[i] = atof( (*p).c_str() );
		++i;
	}
	check_count(pOut->acct_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->status = c[i_status].as(int());

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Customer Position Frame 1 (output)" << endl <<
			"-- cust_id: " << pOut->cust_id << endl <<
			"-- acct_len: " << pOut->acct_len << endl;
	for (i = 0; i < pOut->acct_len; i++) {
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
	enum cpf2 {
			i_hist_dts=0, i_hist_len, i_qty, i_status, i_symbol,
			i_trade_id, i_trade_status
	};

	ostringstream osCall;
	osCall << "SELECT * FROM CustomerPositionFrame2(" << pIn->acct_id << ")";
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< CPF2" << endl;
	cout << "*** " << osCall.str() << endl;
	cout << "- Customer Position Frame 2 (input)" << endl <<
			"-- cust_id: " << pIn->acct_id << endl;
	m_coutLock.unlock();
#endif // DEBUG


	// we are inside a txn
	result R(execute(osCall.str()));
	commit();

	if (R.empty())
	{
		cout << "warning: empty result set at DoCustomerPositionFrame2" <<
				endl <<
				osCall.str() << endl;
		pOut->status = CBaseTxnErr::ROLLBACK;
		return;
	}
	result::const_iterator c = R.begin();

	pOut->hist_len = c[i_hist_len].as(int());;

	vector<string> vAux;
	vector<string>::iterator p;
	int i;

	TokenizeSmart(c[i_hist_dts].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%d-%d-%d %d:%d:%d",
				&pOut->hist_dts[i].year,
				&pOut->hist_dts[i].month,
				&pOut->hist_dts[i].day,
				&pOut->hist_dts[i].hour,
				&pOut->hist_dts[i].minute,
				&pOut->hist_dts[i].second);
		++i;
	}
	check_count(pOut->hist_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_qty].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->qty[i] = atoi((*p).c_str());
		++i;
	}
	check_count(pOut->hist_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->status = c[i_status].as(int());

	TokenizeSmart(c[i_symbol].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->symbol[i], (*p).c_str(), cSYMBOL_len);
		++i;
	}
	check_count(pOut->hist_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_trade_id].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_id[i] = atol((*p).c_str());
		++i;
	}
	check_count(pOut->hist_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_trade_status].c_str(), vAux);
	i = 0;
	for (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->trade_status[i], (*p).c_str(), cST_NAME_len);
		++i;
	}
	check_count(pOut->hist_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Customer Position Frame 2 (output)" << endl <<
			"-- hist_len: " << pOut->hist_len << endl;
	for (i = 0; i < pOut->hist_len; i++) {
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

	// commit the transaction we are inside
	commit();
	pOut->status = 0;

#ifdef DEBUG
	cout << ">>> CPF3" << endl;
#endif
}
