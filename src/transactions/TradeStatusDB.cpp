/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * 13 June 2006
 */

#include <transactions.h>

// Call Trade Status Frame 1
void CTradeStatusDB::DoTradeStatusFrame1(const TTradeStatusFrame1Input *pIn,
		TTradeStatusFrame1Output *pOut)
{
	enum tsf1 {                                                             
			i_broker_name=0, i_charge, i_cust_f_name, i_cust_l_name,        
			i_ex_name, i_exec_name, i_s_name, i_status, i_status_name,      
			i_symbol, i_trade_dts, i_trade_id, i_trade_qty, i_type_name     
	};

	ostringstream osCall;
	osCall << "SELECT * from TradeStatusFrame1(" << pIn->acct_id << ")";
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< TSF1" << endl;
	cout << "*** " << osCall.str() << endl;
	cout << "- Trade Status Frame 1 (input)" << endl <<
			"-- acct_id: " << pIn->acct_id << endl;
	m_coutLock.unlock();
#endif // DEBUG

	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();		

	if (R.empty()) 
	{
		cout << "warning: empty result set at DoTradeStatusFrame1" << endl <<
				osCall.str() << endl;
		pOut->status = CBaseTxnErr::ROLLBACK;
		return;
	}

	result::const_iterator c = R.begin();

	vector<string> vAux;
	vector<string>::iterator p;
	
	strncpy(pOut->broker_name, c[i_broker_name].c_str(), cB_NAME_len);

	TokenizeSmart(c[i_charge].c_str(), vAux);
	int i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->charge[i] = atof((*p).c_str());
		++i;
	}
	vAux.clear();

	strncpy(pOut->cust_f_name, c[i_cust_f_name].c_str(), cF_NAME_len);
	strncpy(pOut->cust_l_name, c[i_cust_l_name].c_str(), cL_NAME_len);

	int len = i;

	TokenizeSmart(c[i_ex_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->ex_name[i], (*p).c_str(), cEX_NAME_len);
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_exec_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->exec_name[i], (*p).c_str(), cEXEC_NAME_len);
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_s_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->s_name[i], (*p).c_str(), cS_NAME_len);
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->status = c[i_status].as(int());

	TokenizeSmart(c[i_status_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->status_name[i], (*p).c_str(), cST_NAME_len);
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_symbol].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->symbol[i], (*p).c_str(), cSYMBOL_len);
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_trade_dts].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		sscanf((*p).c_str(), "%d-%d-%d %d:%d:%d",
				&pOut->trade_dts[i].year,
				&pOut->trade_dts[i].month,
				&pOut->trade_dts[i].day,
				&pOut->trade_dts[i].hour,
				&pOut->trade_dts[i].minute,
				&pOut->trade_dts[i].second);
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_trade_id].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_id[i] = atol((*p).c_str());
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_trade_qty].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_qty[i] = atoi((*p).c_str());
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	TokenizeSmart(c[i_type_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strncpy(pOut->type_name[i], (*p).c_str(), cTT_NAME_len);
		++i;
	}
	check_count(len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Trade Status Frame 1 (output)" << endl <<
			"-- cust_l_name: " << pOut->cust_l_name << endl <<
			"-- cust_f_name: " << pOut->cust_f_name << endl <<
			"-- broker_name: " << pOut->broker_name << endl;
	for (i = 0; i < len; i++) {
		cout << "-- charge[" << i << "]: " << pOut->charge[i] << endl <<
				"-- exec_name[" << i << "]: " << pOut->exec_name[i] << endl <<
				"-- ex_name[" << i << "]: " << pOut->ex_name[i] << endl <<
				"-- s_name[" << i << "]: " << pOut->s_name[i] << endl <<
				"-- status_name[" << i << "]: " << pOut->status_name[i] <<
						endl <<
				"-- symbol[" << i << "]: " << pOut->symbol[i] << endl <<
				"-- trade_dts[" << i << "]: " << pOut->trade_dts[i].year <<
						"-" << pOut->trade_dts[i].month << "-" <<
						pOut->trade_dts[i].day << " " <<
						pOut->trade_dts[i].hour << ":" <<
						pOut->trade_dts[i].minute << ":" <<
						pOut->trade_dts[i].second << endl <<
				"-- trade_id[" << i << "]: " << pOut->trade_id[i] << endl <<
				"-- trade_qty[" << i << "]: " << pOut->trade_qty[i] << endl <<
				"-- type_name[" << i << "]: " << pOut->type_name[i] << endl;
	}
	cout << ">>> TSF1" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}
