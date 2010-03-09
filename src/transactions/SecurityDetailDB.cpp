/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *               2010      Mark Wong
 *
 * 15 July 2006
 */

#include <transactions.h>

// Call Security Detail Frame 1
void CSecurityDetailDB::DoSecurityDetailFrame1(
		const TSecurityDetailFrame1Input *pIn,
		TSecurityDetailFrame1Output *pOut)
{
	enum sdf1 {
	    i_s52_wk_high=0, i_s52_wk_high_date, i_s52_wk_low,
	    i_s52_wk_low_date, i_ceo_name, i_co_ad_cty, i_co_ad_div,
	    i_co_ad_line1, i_co_ad_line2, i_co_ad_town, i_co_ad_zip,
	    i_co_desc, i_co_name, i_co_st_id, i_cp_co_name, i_cp_in_name,
	    i_day, i_day_len, i_divid, i_ex_ad_cty, i_ex_ad_div,
	    i_ex_ad_line1, i_ex_ad_line2, i_ex_ad_town, i_ex_ad_zip,
	    i_ex_close, i_ex_date, i_ex_desc, i_ex_name, i_ex_num_symb,
	    i_ex_open, i_fin, i_fin_len, i_last_open, i_last_price,
	    i_last_vol, i_news, i_news_len, i_num_out, i_open_date,
	    i_pe_ratio, i_s_name, i_sp_rate, i_start_date, i_status, i_yield
	};

	ostringstream osCall;
	osCall << "SELECT * FROM SecurityDetailFrame1(" <<
			(pIn->access_lob_flag == 0 ? "false" : "true") << "," <<
			pIn->max_rows_to_return << ",'" <<
			pIn->start_day.year << "-" <<
			pIn->start_day.month << "-" <<
			pIn->start_day.day << "','" <<
			pIn->symbol << "')";
#ifdef DEBUG
	m_coutLock.lock();
	cout << "<<< SDF1" << endl;
	cout << "*** " << osCall.str() << endl;
	cout << "- Security Detail Frame 1 (input)" << endl <<
			"-- access_lob_flag: " << pIn->access_lob_flag << endl <<
			"-- start_day: " << pIn->start_day.year << "-" <<
					pIn->start_day.month << "-" << pIn->start_day.day << " " <<
					pIn->start_day.hour << ":" << pIn->start_day.minute <<
					":" << pIn->start_day.second << endl <<
			"-- max_rows_to_return: " << pIn->max_rows_to_return << endl <<
			"-- symbol: "<<pIn->symbol<<endl;
	m_coutLock.unlock();
#endif // DEBUG

	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	// stored procedure can return an empty result set by design
	result::const_iterator c = R.begin();

	pOut->fin_len = c[i_fin_len].as(int());
	pOut->day_len = c[i_day_len].as(int());
	pOut->news_len = c[i_news_len].as(int());

	pOut->s52_wk_high = c[i_s52_wk_high].as(double());
	sscanf(c[i_s52_wk_high_date].c_str(), "%d-%d-%d",
			&pOut->s52_wk_high_date.year,
			&pOut->s52_wk_high_date.month,
			&pOut->s52_wk_high_date.day);
	pOut->s52_wk_low = c[i_s52_wk_low].as(double());
	sscanf(c[i_s52_wk_low_date].c_str(), "%d-%d-%d",
			&pOut->s52_wk_low_date.year,
			&pOut->s52_wk_low_date.month,
			&pOut->s52_wk_low_date.day);

	strncpy(pOut->ceo_name, c[i_ceo_name].c_str(), cCEO_NAME_len);
	strncpy(pOut->co_ad_cty, c[i_co_ad_cty].c_str(), cAD_CTRY_len);
	strncpy(pOut->co_ad_div, c[i_co_ad_div].c_str(), cAD_DIV_len);
	strncpy(pOut->co_ad_line1, c[i_co_ad_line1].c_str(), cAD_LINE_len);
	strncpy(pOut->co_ad_line2, c[i_co_ad_line2].c_str(), cAD_LINE_len);
	strncpy(pOut->co_ad_town, c[i_co_ad_town].c_str(), cAD_TOWN_len);
	strncpy(pOut->co_ad_zip, c[i_co_ad_zip].c_str(), cAD_ZIP_len);
	strncpy(pOut->co_desc, c[i_co_desc].c_str(), cCO_DESC_len);
	strncpy(pOut->co_name, c[i_co_name].c_str(), cCO_NAME_len);
	strncpy(pOut->co_st_id, c[i_co_st_id].c_str(), cST_ID_len);

	vector<string> vAux;
	vector<string>::iterator p;

	// Tokenize cp_co_name
	TokenizeSmart(c[i_cp_co_name].c_str(), vAux);
	int i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strncpy(pOut->cp_co_name[i], (*p).c_str(), cCO_NAME_len);
		++i;
	}
	// FIXME: The stored functions for PostgreSQL are designed to return 3
	// items in the array, even though it's not required.
	check_count(3, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	// Tokenize cp_in_name
	TokenizeSmart(c[i_cp_in_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strncpy(pOut->cp_in_name[i], (*p).c_str(), cIN_NAME_len);
		++i;
	}
	// FIXME: The stored functions for PostgreSQL are designed to return 3
	// items in the array, even though it's not required.
	check_count(3, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	// Tokenize day
	TokenizeArray(c[i_day].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		vector<string> v2;
		vector<string>::iterator p2;

		TokenizeSmart((*p).c_str(), v2);

		p2 = v2.begin();
		sscanf((*p2++).c_str(), "%d-%d-%d",
				&pOut->day[i].date.year,
				&pOut->day[i].date.month,
				&pOut->day[i].date.day);
		pOut->day[i].close = atof((*p2++).c_str());
		pOut->day[i].high = atof((*p2++).c_str());
		pOut->day[i].low = atof((*p2++).c_str());
		pOut->day[i].vol = atoi((*p2++).c_str());
		++i;
		v2.clear();
	}
	check_count(pOut->day_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->divid = c[i_divid].as(double());

	strncpy(pOut->ex_ad_cty, c[i_ex_ad_cty].c_str(), cAD_CTRY_len);
	strncpy(pOut->ex_ad_div, c[i_ex_ad_div].c_str(), cAD_DIV_len);
	strncpy(pOut->ex_ad_line1, c[i_ex_ad_line1].c_str(), cAD_LINE_len);
	strncpy(pOut->ex_ad_line2, c[i_ex_ad_line2].c_str(), cAD_LINE_len);
	strncpy(pOut->ex_ad_town, c[i_ex_ad_town].c_str(), cAD_TOWN_len);
	strncpy(pOut->ex_ad_zip, c[i_ex_ad_zip].c_str(), cAD_ZIP_len);
	pOut->ex_close = c[i_ex_close].as(int());
	sscanf(c[i_ex_date].c_str(), "%d-%d-%d",
			&pOut->ex_date.year,
			&pOut->ex_date.month,
			&pOut->ex_date.day);
	strncpy(pOut->ex_desc, c[i_ex_desc].c_str(), cEX_DESC_len);
	strncpy(pOut->ex_name, c[i_ex_name].c_str(), cEX_NAME_len);
	pOut->ex_num_symb = c[i_ex_num_symb].as(int());
	pOut->ex_open = c[i_ex_open].as(int());

	// Tokenize fin
	TokenizeArray(c[i_fin].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		vector<string> v2;
		vector<string>::iterator p2;

		TokenizeSmart((*p).c_str(), v2);

		p2 = v2.begin();
		pOut->fin[i].year = atoi((*p2++).c_str());
		pOut->fin[i].qtr = atoi((*p2++).c_str());
		sscanf((*p2++).c_str(), "%d-%d-%d",
				&pOut->fin[i].start_date.year,
				&pOut->fin[i].start_date.month,
				&pOut->fin[i].start_date.day);
		pOut->fin[i].rev = atof((*p2++).c_str());
		pOut->fin[i].net_earn = atof((*p2++).c_str());
		pOut->fin[i].basic_eps = atof((*p2++).c_str());
		pOut->fin[i].dilut_eps = atof((*p2++).c_str());
		pOut->fin[i].margin = atof((*p2++).c_str());
		pOut->fin[i].invent = atof((*p2++).c_str());
		pOut->fin[i].assets = atof((*p2++).c_str());
		pOut->fin[i].liab = atof((*p2++).c_str());
		pOut->fin[i].out_basic = atof((*p2++).c_str());
		pOut->fin[i].out_dilut = atof((*p2++).c_str());
		++i;
		v2.clear();
	}
	check_count(pOut->fin_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	pOut->last_open = c[i_last_open].as(double());
	pOut->last_price = c[i_last_price].as(double());
	pOut->last_vol = c[i_last_vol].as(int());

	// Tokenize nes
	TokenizeArray(c[i_news].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		vector<string> v2;
		vector<string>::iterator p2;

		TokenizeSmart((*p).c_str(), v2);

		p2 = v2.begin();
		strcpy(pOut->news[i].item, (*p2++).c_str());
		sscanf((*p2++).c_str(), "%d-%d-%d %d:%d:%d",
				&pOut->news[i].dts.year,
				&pOut->news[i].dts.month,
				&pOut->news[i].dts.day,
				&pOut->news[i].dts.hour,
				&pOut->news[i].dts.minute,
				&pOut->news[i].dts.second);
		strcpy(pOut->news[i].src, (*p2++).c_str());
		strcpy(pOut->news[i].auth, (*p2++).c_str());
		strcpy(pOut->news[i].headline, (*p2++).c_str());
		strcpy(pOut->news[i].summary, (*p2++).c_str());
		++i;
		v2.clear();
	}
	check_count(pOut->news_len, vAux.size(), __FILE__, __LINE__);
	vAux.clear();

	sscanf(c[i_open_date].c_str(), "%d-%d-%d",
			&pOut->open_date.year,
			&pOut->open_date.month,
			&pOut->open_date.day);
	pOut->pe_ratio = c[i_pe_ratio].as(double());
	strncpy(pOut->s_name, c[i_s_name].c_str(), cS_NAME_len);
	pOut->num_out = c[i_num_out].as(int());
	strncpy(pOut->sp_rate, c[i_sp_rate].c_str(), cSP_RATE_len);
	sscanf(c[i_start_date].c_str(), "%d-%d-%d",
			&pOut->start_date.year,
			&pOut->start_date.month,
			&pOut->start_date.day);
	pOut->status = c[i_status].as(int());
	pOut->yield = c[i_yield].as(double());

#ifdef DEBUG
	m_coutLock.lock();
	cout << "- Security Detail Frame 1 (output)" << endl <<
			"-- fin_len: " << pOut->fin_len << endl <<
			"-- day_len: " << pOut->day_len << endl <<
			"-- news_len: " << pOut->news_len << endl;
	for (i = 0; i < 3; i++) {
		cout << "-- cp_co_name[" << i << "]: " << pOut->cp_co_name[i] << endl <<
			"-- cp_in_name[" << i << "]: " << pOut->cp_in_name[i] << endl;
	}
	for (i = 0; i < pOut->fin_len; i++) {
		cout << "-- fin[" << i << "].year: " << pOut->fin[i].year << endl <<
				"-- fin[" << i << "].qtr: " << pOut->fin[i].qtr << endl <<
				"-- fin[" << i << "].start_date: " <<
						pOut->fin[i].start_date.year << "-" <<
						pOut->fin[i].start_date.month << "-" <<
						pOut->fin[i].start_date.day << endl <<
				"-- fin[" << i << "].rev: "<<pOut->fin[i].rev << endl <<
				"-- fin[" << i << "].net_earn: " << pOut->fin[i].net_earn <<
						endl <<
				"-- fin[" << i << "].basic_eps: " << pOut->fin[i].basic_eps <<
						endl <<
				"-- fin[" << i << "].dilut_eps: " << pOut->fin[i].dilut_eps <<
						endl <<
				"-- fin[" << i << "].margin: " << pOut->fin[i].margin << endl <<
				"-- fin[" << i << "].invent: " << pOut->fin[i].invent << endl <<
				"-- fin[" << i << "].assets: " << pOut->fin[i].assets << endl <<
				"-- fin[" << i << "].liab: " << pOut->fin[i].liab << endl <<
				"-- fin[" << i << "].out_basic: " << pOut->fin[i].out_basic <<
						endl <<
				"-- fin[" << i << "].out_dilut: " << pOut->fin[i].out_dilut <<
						endl;
	}
	for (i = 0; i < pOut->day_len; i++) {
		cout << "-- day[" << i << "].date: " <<
					pOut->day[i].date.year << "-" <<
					pOut->day[i].date.month << "-" <<
					pOut->day[i].date.day << endl <<
			"-- day[" << i << "].close: " << pOut->day[i].close << endl <<
			"-- day[" << i << "].high: " << pOut->day[i].high << endl <<
			"-- day[" << i << "].low: " << pOut->day[i].low << endl <<
			"-- day[" << i << "].vol: " << pOut->day[i].vol << endl;
	}
	for (i = 0; i < pOut->news_len; i++) {
		cout << "-- news[" << i << "].item: " << pOut->news[i].item << endl <<
				"-- news[" << i << "].dts: " <<
						pOut->news[i].dts.year << "-" <<
						pOut->news[i].dts.month << "-" <<
						pOut->news[i].dts.day << " " <<
						pOut->news[i].dts.hour << ":" <<
						pOut->news[i].dts.minute << ":" <<
						pOut->news[i].dts.second << endl <<
				"-- news[" << i << "].src: " << pOut->news[i].src << endl <<
				"-- news[" << i << "].auth: " << pOut->news[i].auth << endl <<
				"-- news[" << i << "].headline: " << pOut->news[i].headline <<
						endl <<
				"-- news[" << i << "].summary: " << pOut->news[i].summary <<
						endl;
	}
	cout << "-- last_price: " << pOut->last_price << endl <<
			"-- last_open: " << pOut->last_open << endl <<
			"-- last_vol: " << pOut->last_vol << endl <<
			"-- s_name: " << pOut->s_name << endl <<
			"-- co_name: " << pOut->co_name << endl <<
			"-- sp_rate: " << pOut->sp_rate << endl <<
			"-- ceo_name: " << pOut->ceo_name << endl <<
			"-- co_desc: " << pOut->co_desc << endl <<
			"-- open_date: " << pOut->open_date.year << "-" <<
					pOut->open_date.month << "-" <<
					pOut->open_date.day << " " <<
					pOut->open_date.hour << ":" <<
					pOut->open_date.minute << ":" <<
					pOut->open_date.second << endl <<
			"-- co_st_id: " << pOut->co_st_id << endl <<
			"-- co_ad_line1: " << pOut->co_ad_line1 << endl <<
			"-- co_ad_line2: " << pOut->co_ad_line2 << endl <<
			"-- co_ad_town: " << pOut->co_ad_town << endl <<
			"-- co_ad_div: " << pOut->co_ad_div << endl <<
			"-- co_ad_zip: " << pOut->co_ad_zip << endl <<
			"-- co_ad_cty: " << pOut->co_ad_cty << endl <<
			"-- num_out: " << pOut->num_out << endl <<
			"-- start_date: " << pOut->start_date.year << "-" <<
					pOut->start_date.month << "-" <<
					pOut->start_date.day << " " <<
					pOut->start_date.hour << ":" <<
					pOut->start_date.minute << ":" <<
					pOut->start_date.second << endl <<
			"-- ex_date: " << pOut->ex_date.year << "-" <<
					pOut->ex_date.month << "-" <<
					pOut->ex_date.day << " " << pOut->ex_date.hour <<
					":" << pOut->ex_date.minute << ":" <<
					pOut->ex_date.second << endl <<
			"-- pe_ratio: " << pOut->pe_ratio << endl <<
			"-- s52_wk_high: " << pOut->s52_wk_high << endl <<
			"-- s52_wk_high_date: " << pOut->s52_wk_high_date.year <<
					"-" << pOut->s52_wk_high_date.month << "-" <<
					pOut->s52_wk_high_date.day << " " <<
					pOut->s52_wk_high_date.hour << ":" <<
					pOut->s52_wk_high_date.minute << ":" <<
					pOut->s52_wk_high_date.second << endl <<
			"-- s52_wk_low: " << pOut->s52_wk_low << endl <<
			"-- s52_wk_low_date: " << pOut->s52_wk_low_date.year <<
					"-" << pOut->s52_wk_low_date.month << "-" <<
					pOut->s52_wk_low_date.day << " " <<
					pOut->s52_wk_low_date.hour << ":" <<
					pOut->s52_wk_low_date.minute << ":" <<
					pOut->s52_wk_low_date.second << endl <<
			"-- divid: " << pOut->divid << endl <<
			"-- yield: " << pOut->yield << endl <<
			"-- ex_ad_div: " << pOut->ex_ad_div << endl <<
			"-- ex_ad_cty: " << pOut->ex_ad_cty << endl <<
			"-- ex_ad_line1: " << pOut->ex_ad_line1 << endl <<
			"-- ex_ad_line2: " << pOut->ex_ad_line2 << endl <<
			"-- ex_ad_town: " << pOut->ex_ad_town << endl <<
			"-- ex_ad_zip: " << pOut->ex_ad_zip << endl <<
			"-- ex_close: " << pOut->ex_close << endl <<
			"-- ex_desc: " << pOut->ex_desc << endl <<
			"-- ex_name: " << pOut->ex_name << endl <<
			"-- ex_num_symb: " << pOut->ex_num_symb << endl <<
			"-- ex_open: " << pOut->ex_open << endl;
	cout << ">>> SDF1" << endl;
	m_coutLock.unlock();
#endif // DEBUG
}
