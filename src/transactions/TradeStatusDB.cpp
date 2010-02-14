/*
 * TradeStatusDB.cpp
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *
 * 13 June 2006
 */

#include <transactions.h>

using namespace TPCE;

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
	
	strcpy(pOut->broker_name, c[i_broker_name].c_str());
	strcpy(pOut->cust_f_name, c[i_cust_f_name].c_str());
	strcpy(pOut->cust_l_name, c[i_cust_l_name].c_str());
	pOut->status = c[i_status].as(int());

	Tokenize(c[i_charge].c_str(), vAux);
	int i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->charge[i] = atof((*p).c_str());
		++i;
	}
	vAux.clear();

#ifdef DEBUG
	// This only matter for the DEBUG output, I think...
	// Hope that the transaction executed correctly and the number of items
	// found in the "charge" column array really is the same in the other
	// columns returning arrays, since this transation doesn't return the
	// number of items that will be in each column array.
	int len = i;
#endif

	TokenizeString(c[i_ex_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strcpy(pOut->ex_name[i], (*p).c_str());
		++i;
	}
	vAux.clear();

	TokenizeString(c[i_exec_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strcpy(pOut->exec_name[i], (*p).c_str());
		++i;
	}
	vAux.clear();

	TokenizeString(c[i_s_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strcpy(pOut->s_name[i], (*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_symbol].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strcpy(pOut->symbol[i], (*p).c_str());
		++i;
	}
	vAux.clear();

	TokenizeString(c[i_trade_dts].c_str(), vAux);
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
	vAux.clear();

	Tokenize(c[i_trade_id].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_id[i] = atol((*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_trade_qty].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		pOut->trade_qty[i] = atoi((*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_type_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strcpy(pOut->type_name[i], (*p).c_str());
		++i;
	}
	vAux.clear();

	Tokenize(c[i_status_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p) {
		strcpy(pOut->status_name[i], (*p).c_str());
		++i;
	}
	vAux.clear();

#ifdef DEBUG
	m_coutLock.lock();
	cout << ">>> TSF1" << endl;
	cout << "*** " << osCall.str() << endl;
	cout << "- Trade Status Frame 1 (input)" << endl <<
			"-- acct_id: " << pIn->acct_id << endl;
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
	m_coutLock.unlock();
#endif // DEBUG
}
