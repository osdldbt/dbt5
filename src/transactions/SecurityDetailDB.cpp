/*
 * SecurityDetailDB.cpp
 *
 * Copyright (C) 2006-2007 Rilson Nascimento
 *
 * 15 July 2006
 */

#include <transactions.h>

using namespace TPCE;

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
			pIn->start_day.day << " " <<
			pIn->start_day.hour << ":" <<
			pIn->start_day.minute << ":" <<
			pIn->start_day.second << "','" <<
			pIn->symbol << "')";

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

	vector<string> vAux;
	vector<string>::iterator p;

	// Tokenize cp_co_name
	TokenizeString(c[i_cp_co_name].c_str(), vAux);
	int i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strcpy(pOut->cp_co_name[i], (*p).c_str());	
		++i;
	}
	vAux.clear();

	// Tokenize cp_in_name
	TokenizeString(c[i_cp_in_name].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strcpy(pOut->cp_in_name[i], (*p).c_str());	
		++i;
	}
	vAux.clear();

	// FIXME: I don't know how to handle these 'fin' variables.
/*
	// Tokenize fin.year
	Tokenize(c[5].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].year = atoi( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize fin.qtr
	Tokenize( c[6].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].qtr = atoi( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize start_date.year
	Tokenize(c[i_start_date].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		sscanf((*p).c_str(), "%d-%d-%d %d:%d:%d",
				&pOut->fin[i].start_date.year,
				&pOut->fin[i].start_date.month,
				&pOut->fin[i].start_date.day,
				&pOut->fin[i].start_date.hour,
				&pOut->fin[i].start_date.minute,
				&pOut->fin[i].start_date.second);
		++i;
	}
	vAux.clear();

	// Tokenize fin.rev
	Tokenize( c[13].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].rev = atof( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize fin.net_earn
	Tokenize( c[14].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].net_earn = atof( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize fin.basic_eps
	Tokenize( c[15].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].basic_eps = atof( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize fin.dilut_eps
	Tokenize( c[16].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].dilut_eps = atof( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize fin.margin
	Tokenize( c[17].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].margin = atof( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize fin.invent
	Tokenize( c[18].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].invent = atof( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize fin.assets
	Tokenize( c[19].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].assets = atof( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize fin.liab
	Tokenize( c[20].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].liab = atof( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize fin.out_basic
	Tokenize( c[21].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].out_basic = atof( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize fin.out_dilut
	Tokenize( c[22].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].out_dilut = atof( (*p).c_str() );
		++i;
	}
	vAux.clear();
*/

	// FIXME: Figure uot what this 'date' variable is supposed to correlate to
	// from the stored function.
/*
	// Tokenize date.year
	Tokenize( c[23].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->day[i].date.year = atoi( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize date.month
	Tokenize( c[24].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->day[i].date.month = atoi( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize date.day
	Tokenize( c[25].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->day[i].date.day = atoi( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize date.hour
	Tokenize( c[26].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->day[i].date.hour = atoi( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize date.minute
	Tokenize( c[27].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->day[i].date.minute = atoi( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize date.second
	Tokenize( c[28].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->day[i].date.second = atoi( (*p).c_str() );
		++i;
	}
	vAux.clear();
*/

	// FIXME: I don't know how to handle the 'day' variables.
/*
	// Tokenize day.close
	Tokenize( c[29].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->day[i].close = atof( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize day.high
	Tokenize( c[30].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->day[i].high = atof( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize day.low
	Tokenize( c[31].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->day[i].low = atof( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize day.vol
	Tokenize( c[32].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->day[i].vol = atoi( (*p).c_str() );
		++i;
	}
	vAux.clear();
*/

	// FIXME: I don't know how to handle the 'news' variables.
/*
	// Tokenize news.item
	Tokenize( c[33].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strcpy(pOut->news[i].item, (*p).c_str());	
		++i;
	}
	vAux.clear();

	// Tokenize news.dts.year
	Tokenize( c[34].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->news[i].dts.year = atoi( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize news.dts.month
	Tokenize( c[35].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->news[i].dts.month = atoi( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize news.dts.day
	Tokenize( c[36].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->news[i].dts.day = atoi( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize news.dts.hour
	Tokenize( c[37].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->news[i].dts.hour = atoi( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize news.dts.minute
	Tokenize( c[38].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->news[i].dts.minute = atoi( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize news.dts.second
	Tokenize( c[39].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->news[i].dts.second = atoi( (*p).c_str() );
		++i;
	}
	vAux.clear();

	// Tokenize news.src
	Tokenize( c[40].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strcpy(pOut->news[i].src, (*p).c_str());	
		++i;
	}
	vAux.clear();

	// Tokenize news.auth
	Tokenize( c[41].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strcpy(pOut->news[i].auth, (*p).c_str());	
		++i;
	}
	vAux.clear();

	// Tokenize news.headline
	Tokenize( c[42].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strcpy(pOut->news[i].headline, (*p).c_str());	
		++i;
	}
	vAux.clear();

	// Tokenize news.summary
	Tokenize( c[43].c_str(), vAux);
	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strcpy(pOut->news[i].summary, (*p).c_str());	
		++i;
	}
	vAux.clear();
*/

	pOut->last_price = c[i_last_price].as(double());
	pOut->last_open = c[i_last_open].as(double());
	pOut->last_vol = c[i_last_vol].as(int());
	strcpy(pOut->s_name, c[i_s_name].c_str());	
	strcpy(pOut->co_name, c[i_co_name].c_str());	
	strcpy(pOut->sp_rate, c[i_sp_rate].c_str());	
	strcpy(pOut->ceo_name, c[i_ceo_name].c_str());	
	strcpy(pOut->co_desc, c[i_co_desc].c_str());	
	sscanf(c[i_open_date].c_str(), "%d-%d-%d",
			&pOut->open_date.year,
			&pOut->open_date.month,
			&pOut->open_date.day);
	strcpy(pOut->co_st_id, c[i_co_st_id].c_str());
	strcpy(pOut->co_ad_line1, c[i_co_ad_line1].c_str());
	strcpy(pOut->co_ad_line2, c[i_co_ad_line2].c_str());
	strcpy(pOut->co_ad_town, c[i_co_ad_town].c_str());
	strcpy(pOut->co_ad_div, c[i_co_ad_div].c_str());
	strcpy(pOut->co_ad_zip, c[i_co_ad_zip].c_str());
	strcpy(pOut->co_ad_cty, c[i_co_ad_cty].c_str());
	pOut->num_out = c[i_num_out].as(int());
	sscanf(c[i_start_date].c_str(), "%d-%d-%d",
			&pOut->start_date.year,
			&pOut->start_date.month,
			&pOut->start_date.day);
	sscanf(c[i_ex_date].c_str(), "%d-%d-%d",
			&pOut->ex_date.year,
			&pOut->ex_date.month,
			&pOut->ex_date.day);
	pOut->pe_ratio = c[i_pe_ratio].as(double());
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
	pOut->divid = c[i_divid].as(double());
	pOut->yield = c[i_yield].as(double());
	strcpy(pOut->ex_ad_div, c[i_ex_ad_div].c_str());
	strcpy(pOut->ex_ad_cty, c[i_ex_ad_cty].c_str());
	strcpy(pOut->ex_ad_line1, c[i_ex_ad_line1].c_str());
	strcpy(pOut->ex_ad_line2, c[i_ex_ad_line2].c_str());
	strcpy(pOut->ex_ad_town, c[i_ex_ad_town].c_str());
	strcpy(pOut->ex_ad_zip, c[i_ex_ad_zip].c_str());
	pOut->ex_close = c[i_ex_close].as(int());
	strcpy(pOut->ex_desc, c[i_ex_desc].c_str());
	strcpy(pOut->ex_name, c[i_ex_name].c_str());
	pOut->ex_num_symb = c[i_ex_num_symb].as(int());
	pOut->ex_open = c[i_ex_open].as(int());

	pOut->status = c[i_status].as(int());

#ifdef DEBUG
	m_coutLock.lock();
	cout << ">>> SDF1" << endl;
	cout << "*** " << osCall.str() << endl;
	cout << "- Security Detail Frame 1 (input)" << endl <<
			"-- access_lob_flag: " << pIn->access_lob_flag << endl <<
			"-- start_day: " << pIn->start_day.year << "-" <<
					pIn->start_day.month << "-" << pIn->start_day.day << " " <<
					pIn->start_day.hour << ":" << pIn->start_day.minute <<
					":" << pIn->start_day.second << endl <<
			"-- max_rows_to_return: " << pIn->max_rows_to_return << endl <<
			"-- symbol: "<<pIn->symbol<<endl;
	cout << "- Security Detail Frame 1 (output)" << endl <<
			"-- fin_len: " << pOut->fin_len << endl <<
			"-- day_len: " << pOut->day_len << endl <<
			"-- news_len: " << pOut->news_len << endl <<
			"-- cp_co_name[0]: " << pOut->cp_co_name[0] << endl <<
			"-- cp_in_name[0]: " << pOut->cp_in_name[0] << endl <<
			"-- fin[0].year: " << pOut->fin[0].year << endl <<
			"-- fin[0].qtr: " << pOut->fin[0].qtr << endl <<
			"-- fin[0].start_date: " << pOut->fin[0].start_date.year <<
					"-" << pOut->fin[0].start_date.month << "-" <<
					pOut->fin[0].start_date.day << " " <<
					pOut->fin[0].start_date.hour << ":" <<
					pOut->fin[0].start_date.minute << ":" <<
					pOut->fin[0].start_date.second << endl <<
			"-- fin[0].rev: "<<pOut->fin[0].rev << endl <<
			"-- fin[0].net_earn: " << pOut->fin[0].net_earn << endl <<
			"-- fin[0].basic_eps: " << pOut->fin[0].basic_eps << endl <<
			"-- fin[0].dilut_eps: " << pOut->fin[0].dilut_eps << endl <<
			"-- fin[0].margin: " << pOut->fin[0].margin << endl <<
			"-- fin[0].invent: " << pOut->fin[0].invent << endl <<
			"-- fin[0].assets: " << pOut->fin[0].assets << endl <<
			"-- fin[0].liab: " << pOut->fin[0].liab << endl <<
			"-- fin[0].out_basic: " << pOut->fin[0].out_basic << endl <<
			"-- fin[0].out_dilut: " << pOut->fin[0].out_dilut << endl <<
			"-- day[0].date: " << pOut->day[0].date.year << "-" <<
					pOut->day[0].date.month << "-" <<
					pOut->day[0].date.day << " " <<
					pOut->day[0].date.hour << ":" <<
					pOut->day[0].date.minute << ":" <<
					pOut->day[0].date.second << endl <<
			"-- day[0].close: " << pOut->day[0].close << endl <<
			"-- day[0].high: " << pOut->day[0].high << endl <<
			"-- day[0].low: " << pOut->day[0].low << endl <<
			"-- day[0].vol: " << pOut->day[0].vol << endl <<
			"-- news[0].item: " << pOut->news[0].item << endl <<
			"-- news[0].dts: " << pOut->news[0].dts.year << "-" <<
					pOut->news[0].dts.month << "-" <<
					pOut->news[0].dts.day << " " <<
					pOut->news[0].dts.hour << ":" <<
					pOut->news[0].dts.minute << ":" <<
					pOut->news[0].dts.second << endl <<
			"-- news[0].src: " << pOut->news[0].src << endl <<
			"-- news[0].auth: " << pOut->news[0].auth << endl <<
			"-- news[0].headline: " << pOut->news[0].headline << endl <<
			"-- news[0].summary: " << pOut->news[0].summary << endl <<
			"-- last_price: " << pOut->last_price << endl <<
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
	m_coutLock.unlock();
#endif // DEBUG
}
