/*
 * SecurityDetailDB.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 15 July 2006
 */

#include <transactions.h>

using namespace TPCE;

// Constructor
CSecurityDetailDB::CSecurityDetailDB(CDBConnection *pDBConn)
: CTxnBaseDB(pDBConn)
{
}

// Destructor
CSecurityDetailDB::~CSecurityDetailDB()
{
}

// Call Security Detail Frame 1
void CSecurityDetailDB::DoSecurityDetailFrame1(
		const TSecurityDetailFrame1Input *pIn,
		TSecurityDetailFrame1Output *pOut)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from SecurityDetailFrame1(" <<
			pIn->access_lob_flag << "::smallint, " <<
			pIn->max_rows_to_return << "::integer, '" <<
			pIn->start_day.year << "-" <<
			pIn->start_day.month << "-" <<
			pIn->start_day.day << " " <<
			pIn->start_day.hour << ":" <<
			pIn->start_day.minute << ":" <<
			pIn->start_day.second <<
			"'::timestamp, '" << pIn->symbol <<
			"'::char(15)) as (fin_len integer, day_len integer, news_len "
			"integer, cp_co_name text, cp_in_name text, fin_year text, fin_qtr "
			"text, fin_start_year text, fin_start_month text, fin_start_day "
			"text, fin_start_hour text, fin_start_minute text, "
			"fin_start_second text, fin_rev text, fin_net_earn text, "
			"fin_basic_eps text, fin_dilut_eps text, fin_margin text, "
			"fin_invent text, fin_assets text, fin_liab text, fin_out_basic "
			"text, fin_out_dilut text, day_date_year text, day_date_month "
			"text, day_date_day text, day_date_hour text, day_date_minute "
			"text, day_date_second text, day_close text, day_high text, "
			"day_low text, day_vol text, news_it text, news_year text, "
			"news_month text, news_day text, news_hour text, news_minute text, "
			"news_sec text, news_src text, news_auth text, news_headline text, "
			"news_summary text, last_price S_PRICE_T, last_open S_PRICE_T, "
			"last_vol S_COUNT_T, S_NAME varchar, CO_ID IDENT_T, CO_NAME "
			"varchar, CO_SP_RATE char(4), CO_CEO varchar, CO_DESC varchar, "
			"open_year double precision, open_month double precision, open_day "
			"double precision, open_hour double precision, open_minute double "
			"precision, open_second double precision, CO_ST_ID char(4), "
			"CA_AD_LINE1 varchar, CA_AD_LINE2 varchar, ZCA_ZC_TOWN varchar, "
			"ZCA_ZC_DIV varchar, CA_AD_ZC_CODE char(12), CA_AD_CTRY varchar, "
			"S_NUM_OUT S_COUNT_T, s_start_year double precision, s_start_month "
			"double precision, s_start_day double precision, s_start_hour "
			"double precision, s_start_minute double precision, s_start_second "
			"double precision, s_exch_year double precision, s_exch_month "
			"double precision, s_exch_day double precision, s_exch_hour double "
			"precision, s_exch_minute double precision, s_exch_second double "
			"precision, S_PE VALUE_T, S_52WK_HIGH S_PRICE_T, high_year double "
			"precision, high_month double precision, high_day double "
			"precision, high_hour double precision, high_minute double "
			"precision, high_second double precision, S_52WK_LOW S_PRICE_T, "
			"low_year double precision, low_month double precision, low_day "
			"double precision, low_hour double precision, low_minute double "
			"precision, low_second double precision, S_DIVIDEND VALUE_T, "
			"S_YIELD NUMERIC(5,2), ZEA_ZC_DIV varchar, EA_AD_CTRY varchar, "
			"EA_AD_LINE1 varchar, EA_AD_LINE2 varchar, ZEA_ZC_TOWN varchar, "
			"EA_AD_ZC_CODE char(12), EX_CLOSE smallint, EX_DESC varchar, "
			"EX_NAME varchar, EX_NUM_SYMB integer, EX_OPEN smallint);";

	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();

	// stored procedure can return an empty result set by design
	result::const_iterator c = R.begin();

	pOut->fin_len = c[0].as(int());
	pOut->day_len = c[1].as(int());
	pOut->news_len = c[2].as(int());

	vector<string> vAux;
	vector<string>::iterator p;

	// Tokenize cp_co_name
	Tokenize( c[3].c_str(), vAux);

	int i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strcpy(pOut->cp_co_name[i], (*p).c_str());	
		i++;
	}

	// Tokenize cp_in_name
	vAux.clear();
	Tokenize( c[4].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strcpy(pOut->cp_in_name[i], (*p).c_str());	
		i++;
	}

	// Tokenize fin.year
	vAux.clear();
	Tokenize( c[5].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].year = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize fin.qtr
	vAux.clear();
	Tokenize( c[6].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].qtr = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize start_date.year
	vAux.clear();
	Tokenize( c[7].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].start_date.year = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize start_date.month
	vAux.clear();
	Tokenize( c[8].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].start_date.month = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize start_date.day
	vAux.clear();
	Tokenize( c[9].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].start_date.day = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize start_date.hour
	vAux.clear();
	Tokenize( c[10].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].start_date.hour = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize start_date.minute
	vAux.clear();
	Tokenize( c[11].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].start_date.minute = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize start_date.second
	vAux.clear();
	Tokenize( c[12].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].start_date.second = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize fin.rev
	vAux.clear();
	Tokenize( c[13].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].rev = atof( (*p).c_str() );
		i++;
	}

	// Tokenize fin.net_earn
	vAux.clear();
	Tokenize( c[14].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].net_earn = atof( (*p).c_str() );
		i++;
	}

	// Tokenize fin.basic_eps
	vAux.clear();
	Tokenize( c[15].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].basic_eps = atof( (*p).c_str() );
		i++;
	}

	// Tokenize fin.dilut_eps
	vAux.clear();
	Tokenize( c[16].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].dilut_eps = atof( (*p).c_str() );
		i++;
	}

	// Tokenize fin.margin
	vAux.clear();
	Tokenize( c[17].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].margin = atof( (*p).c_str() );
		i++;
	}

	// Tokenize fin.invent
	vAux.clear();
	Tokenize( c[18].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].invent = atof( (*p).c_str() );
		i++;
	}

	// Tokenize fin.assets
	vAux.clear();
	Tokenize( c[19].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].assets = atof( (*p).c_str() );
		i++;
	}

	// Tokenize fin.liab
	vAux.clear();
	Tokenize( c[20].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].liab = atof( (*p).c_str() );
		i++;
	}

	// Tokenize fin.out_basic
	vAux.clear();
	Tokenize( c[21].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].out_basic = atof( (*p).c_str() );
		i++;
	}

	// Tokenize fin.out_dilut
	vAux.clear();
	Tokenize( c[22].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->fin[i].out_dilut = atof( (*p).c_str() );
		i++;
	}

	// Tokenize date.year
	vAux.clear();
	Tokenize( c[23].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->day[i].date.year = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize date.month
	vAux.clear();
	Tokenize( c[24].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->day[i].date.month = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize date.day
	vAux.clear();
	Tokenize( c[25].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->day[i].date.day = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize date.hour
	vAux.clear();
	Tokenize( c[26].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->day[i].date.hour = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize date.minute
	vAux.clear();
	Tokenize( c[27].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->day[i].date.minute = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize date.second
	vAux.clear();
	Tokenize( c[28].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->day[i].date.second = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize day.close
	vAux.clear();
	Tokenize( c[29].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->day[i].close = atof( (*p).c_str() );
		i++;
	}

	// Tokenize day.high
	vAux.clear();
	Tokenize( c[30].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->day[i].high = atof( (*p).c_str() );
		i++;
	}

	// Tokenize day.low
	vAux.clear();
	Tokenize( c[31].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->day[i].low = atof( (*p).c_str() );
		i++;
	}

	// Tokenize day.vol
	vAux.clear();
	Tokenize( c[32].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->day[i].vol = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize news.item
	vAux.clear();
	Tokenize( c[33].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strcpy(pOut->news[i].item, (*p).c_str());	
		i++;
	}

	// Tokenize news.dts.year
	vAux.clear();
	Tokenize( c[34].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->news[i].dts.year = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize news.dts.month
	vAux.clear();
	Tokenize( c[35].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->news[i].dts.month = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize news.dts.day
	vAux.clear();
	Tokenize( c[36].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->news[i].dts.day = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize news.dts.hour
	vAux.clear();
	Tokenize( c[37].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->news[i].dts.hour = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize news.dts.minute
	vAux.clear();
	Tokenize( c[38].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->news[i].dts.minute = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize news.dts.second
	vAux.clear();
	Tokenize( c[39].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->news[i].dts.second = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize news.src
	vAux.clear();
	Tokenize( c[40].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strcpy(pOut->news[i].src, (*p).c_str());	
		i++;
	}

	// Tokenize news.auth
	vAux.clear();
	Tokenize( c[41].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strcpy(pOut->news[i].auth, (*p).c_str());	
		i++;
	}

	// Tokenize news.headline
	vAux.clear();
	Tokenize( c[42].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strcpy(pOut->news[i].headline, (*p).c_str());	
		i++;
	}

	// Tokenize news.summary
	vAux.clear();
	Tokenize( c[43].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strcpy(pOut->news[i].summary, (*p).c_str());	
		i++;
	}

	pOut->last_price = c[44].as(double());
	pOut->last_open = c[45].as(double());
	pOut->last_vol = c[46].as(int());
	strcpy(pOut->s_name, c[47].c_str());	
	//co_id - 48
	strcpy(pOut->co_name, c[49].c_str());	
	strcpy(pOut->sp_rate, c[50].c_str());	
	strcpy(pOut->ceo_name, c[51].c_str());	
	strcpy(pOut->co_desc, c[52].c_str());	
	pOut->open_date.year = c[53].as(int());
	pOut->open_date.month = c[54].as(int());
	pOut->open_date.day = c[55].as(int());
	pOut->open_date.hour = c[56].as(int());
	pOut->open_date.minute = c[57].as(int());
	pOut->open_date.second = int(c[58].as(double()));
	strcpy(pOut->co_st_id, c[59].c_str());
	strcpy(pOut->co_ad_line1, c[60].c_str());
	strcpy(pOut->co_ad_line2, c[61].c_str());
	strcpy(pOut->co_ad_town, c[62].c_str());
	strcpy(pOut->co_ad_div, c[63].c_str());
	strcpy(pOut->co_ad_zip, c[64].c_str());
	strcpy(pOut->co_ad_cty, c[65].c_str());
	pOut->num_out = c[66].as(int());
	pOut->start_date.year = c[67].as(int());
	pOut->start_date.month = c[68].as(int());
	pOut->start_date.day = c[69].as(int());
	pOut->start_date.hour = c[70].as(int());
	pOut->start_date.minute = c[71].as(int());
	pOut->start_date.second = int(c[72].as(double()));
	pOut->ex_date.year = c[73].as(int());
	pOut->ex_date.month = c[74].as(int());
	pOut->ex_date.day = c[75].as(int());
	pOut->ex_date.hour = c[76].as(int());
	pOut->ex_date.minute = c[77].as(int());
	pOut->ex_date.second = int(c[78].as(double()));
	pOut->pe_ratio = c[79].as(double());
	pOut->s52_wk_high = c[80].as(double());
	pOut->s52_wk_high_date.year = c[81].as(int());
	pOut->s52_wk_high_date.month = c[82].as(int());
	pOut->s52_wk_high_date.day = c[83].as(int());
	pOut->s52_wk_high_date.hour = c[84].as(int());
	pOut->s52_wk_high_date.minute = c[85].as(int());
	pOut->s52_wk_high_date.second = int(c[86].as(double()));
	pOut->s52_wk_low = c[87].as(double());
	pOut->s52_wk_low_date.year = c[88].as(int());
	pOut->s52_wk_low_date.month = c[89].as(int());
	pOut->s52_wk_low_date.day = c[90].as(int());
	pOut->s52_wk_low_date.hour = c[91].as(int());
	pOut->s52_wk_low_date.minute = c[92].as(int());
	pOut->s52_wk_low_date.second = int(c[93].as(double()));
	pOut->divid = c[94].as(double());
	pOut->yield = c[95].as(double());
	strcpy(pOut->ex_ad_div, c[96].c_str());
	strcpy(pOut->ex_ad_cty, c[97].c_str());
	strcpy(pOut->ex_ad_line1, c[98].c_str());
	strcpy(pOut->ex_ad_line2, c[99].c_str());
	strcpy(pOut->ex_ad_town, c[100].c_str());
	strcpy(pOut->ex_ad_zip, c[101].c_str());
	pOut->ex_close = c[102].as(int());
	strcpy(pOut->ex_desc, c[103].c_str());
	strcpy(pOut->ex_name, c[104].c_str());
	pOut->ex_num_symb = c[105].as(int());
	pOut->ex_open = c[106].as(int());

	pOut->status = CBaseTxnErr::SUCCESS;

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Security Detail Frame 1 (input)"<<endl
	    <<"- access_lob_flag: "<<pIn->access_lob_flag<<endl
	    <<"- start_day: "<<pIn->start_day.year<<"-"<<
		pIn->start_day.month<<"-"<<
		pIn->start_day.day<<" "<<
		pIn->start_day.hour<<":"<<
		pIn->start_day.minute<<":"<<
		pIn->start_day.second<<endl
	    <<"- max_rows_to_return: "<<pIn->max_rows_to_return<<endl
	    <<"- symbol: "<<pIn->symbol<<endl;
	cout << "Security Detail Frame 1 (output)" << endl <<
			"- fin_len: " << pOut->fin_len << endl <<
			"- day_len: " << pOut->day_len << endl <<
			"- news_len: " << pOut->news_len << endl <<
			"- cp_co_name[0]: " << pOut->cp_co_name[0] << endl <<
			"- cp_in_name[0]: " << pOut->cp_in_name[0] << endl <<
			"- fin[0].year: " << pOut->fin[0].year << endl <<
			"- fin[0].qtr: " << pOut->fin[0].qtr << endl <<
			"- fin[0].start_date: " << pOut->fin[0].start_date.year <<
			"-" << pOut->fin[0].start_date.month << "-" <<
			pOut->fin[0].start_date.day << " " <<
			pOut->fin[0].start_date.hour << ":" <<
			pOut->fin[0].start_date.minute << ":" <<
			pOut->fin[0].start_date.second << endl <<
			"- fin[0].rev: "<<pOut->fin[0].rev << endl <<
			"- fin[0].net_earn: " << pOut->fin[0].net_earn << endl <<
			"- fin[0].basic_eps: " << pOut->fin[0].basic_eps << endl <<
			"- fin[0].dilut_eps: " << pOut->fin[0].dilut_eps << endl <<
			"- fin[0].margin: " << pOut->fin[0].margin << endl <<
			"- fin[0].invent: " << pOut->fin[0].invent << endl <<
			"- fin[0].assets: " << pOut->fin[0].assets << endl <<
			"- fin[0].liab: " << pOut->fin[0].liab << endl <<
			"- fin[0].out_basic: " << pOut->fin[0].out_basic << endl <<
			"- fin[0].out_dilut: " << pOut->fin[0].out_dilut << endl <<
			"- day[0].date: " << pOut->day[0].date.year << "-" <<
			pOut->day[0].date.month << "-" <<
			pOut->day[0].date.day << " " <<
			pOut->day[0].date.hour << ":" <<
			pOut->day[0].date.minute << ":" <<
			pOut->day[0].date.second << endl <<
			"- day[0].close: " << pOut->day[0].close << endl <<
			"- day[0].high: " << pOut->day[0].high << endl <<
			"- day[0].low: " << pOut->day[0].low << endl <<
			"- day[0].vol: " << pOut->day[0].vol << endl <<
			"- news[0].item: " << pOut->news[0].item << endl <<
			"- news[0].dts: " << pOut->news[0].dts.year << "-" <<
			pOut->news[0].dts.month << "-" <<
			pOut->news[0].dts.day << " " <<
			pOut->news[0].dts.hour << ":" <<
			pOut->news[0].dts.minute << ":" <<
			pOut->news[0].dts.second << endl <<
			"- news[0].src: " << pOut->news[0].src << endl <<
			"- news[0].auth: " << pOut->news[0].auth << endl <<
			"- news[0].headline: " << pOut->news[0].headline << endl <<
			"- news[0].summary: " << pOut->news[0].summary << endl <<
			"- last_price: " << pOut->last_price << endl <<
			"- last_open: " << pOut->last_open << endl <<
			"- last_vol: " << pOut->last_vol << endl <<
			"- s_name: " << pOut->s_name << endl <<
			"- co_name: " << pOut->co_name << endl <<
			"- sp_rate: " << pOut->sp_rate << endl <<
			"- ceo_name: " << pOut->ceo_name << endl <<
			"- co_desc: " << pOut->co_desc << endl <<
			"- open_date: " << pOut->open_date.year << "-" <<
			pOut->open_date.month << "-" <<
			pOut->open_date.day << " " <<
			pOut->open_date.hour << ":" <<
			pOut->open_date.minute << ":" <<
			pOut->open_date.second << endl << "- co_st_id: " <<
			pOut->co_st_id << endl << "- co_ad_line1: " <<
			pOut->co_ad_line1 << endl <<
			"- co_ad_line2: " << pOut->co_ad_line2 << endl <<
			"- co_ad_town: " << pOut->co_ad_town << endl <<
			"- co_ad_div: " << pOut->co_ad_div << endl <<
			"- co_ad_zip: " << pOut->co_ad_zip << endl <<
			"- co_ad_cty: " << pOut->co_ad_cty << endl <<
			"- num_out: " << pOut->num_out << endl <<
			"- start_date: " << pOut->start_date.year << "-" <<
			pOut->start_date.month << "-" <<
			pOut->start_date.day << " " <<
			pOut->start_date.hour << ":" <<
			pOut->start_date.minute << ":" <<
			pOut->start_date.second << endl <<
			"- ex_date: " << pOut->ex_date.year << "-" <<
			pOut->ex_date.month << "-" <<
			pOut->ex_date.day << " " << pOut->ex_date.hour <<
			":" << pOut->ex_date.minute << ":" <<
			pOut->ex_date.second << endl <<
			"- pe_ratio: " << pOut->pe_ratio << endl <<
			"- s52_wk_high: " << pOut->s52_wk_high << endl <<
			"- s52_wk_high_date: " << pOut->s52_wk_high_date.year <<
			"-" << pOut->s52_wk_high_date.month << "-" <<
			pOut->s52_wk_high_date.day << " " <<
			pOut->s52_wk_high_date.hour << ":" <<
			pOut->s52_wk_high_date.minute << ":" <<
			pOut->s52_wk_high_date.second << endl <<
			"- s52_wk_low: " << pOut->s52_wk_low << endl <<
			"- s52_wk_low_date: " << pOut->s52_wk_low_date.year <<
			"-" << pOut->s52_wk_low_date.month << "-" <<
			pOut->s52_wk_low_date.day << " " <<
			pOut->s52_wk_low_date.hour << ":" <<
			pOut->s52_wk_low_date.minute << ":" <<
			pOut->s52_wk_low_date.second << endl <<
			"- divid: " << pOut->divid << endl <<
			"- yield: " << pOut->yield << endl <<
			"- ex_ad_div: " << pOut->ex_ad_div << endl <<
			"- ex_ad_cty: " << pOut->ex_ad_cty << endl <<
			"- ex_ad_line1: " << pOut->ex_ad_line1 << endl <<
			"- ex_ad_line2: " << pOut->ex_ad_line2 << endl <<
			"- ex_ad_town: " << pOut->ex_ad_town << endl <<
			"- ex_ad_zip: " << pOut->ex_ad_zip << endl <<
			"- ex_close: " << pOut->ex_close << endl <<
			"- ex_desc: " << pOut->ex_desc << endl <<
			"- ex_name: " << pOut->ex_name << endl <<
			"- ex_num_symb: " << pOut->ex_num_symb << endl <<
			"- ex_open: " << pOut->ex_open << endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG
	
#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}


void CSecurityDetailDB::Cleanup(void* pException)
{
}
