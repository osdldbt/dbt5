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
		PSecurityDetailFrame1Input pFrame1Input,
		PSecurityDetailFrame1Output pFrame1Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from SecurityDetailFrame1(" <<
			pFrame1Input->access_lob_flag << "::smallint, " <<
			pFrame1Input->max_rows_to_return << "::integer, '" <<
			pFrame1Input->start_day.year << "-" <<
			pFrame1Input->start_day.month << "-" <<
			pFrame1Input->start_day.day << " " <<
			pFrame1Input->start_day.hour << ":" <<
			pFrame1Input->start_day.minute << ":" <<
			pFrame1Input->start_day.second <<
			"'::timestamp, '" << pFrame1Input->symbol <<
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

	pFrame1Output->fin_len = c[0].as(int());
	pFrame1Output->day_len = c[1].as(int());
	pFrame1Output->news_len = c[2].as(int());

	vector<string> vAux;
	vector<string>::iterator p;

	// Tokenize cp_co_name
	Tokenize( c[3].c_str(), vAux);

	int i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strcpy(pFrame1Output->cp_co_name[i], (*p).c_str());	
		i++;
	}

	// Tokenize cp_in_name
	vAux.clear();
	Tokenize( c[4].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strcpy(pFrame1Output->cp_in_name[i], (*p).c_str());	
		i++;
	}

	// Tokenize fin.year
	vAux.clear();
	Tokenize( c[5].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->fin[i].year = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize fin.qtr
	vAux.clear();
	Tokenize( c[6].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->fin[i].qtr = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize start_date.year
	vAux.clear();
	Tokenize( c[7].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->fin[i].start_date.year = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize start_date.month
	vAux.clear();
	Tokenize( c[8].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->fin[i].start_date.month = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize start_date.day
	vAux.clear();
	Tokenize( c[9].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->fin[i].start_date.day = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize start_date.hour
	vAux.clear();
	Tokenize( c[10].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->fin[i].start_date.hour = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize start_date.minute
	vAux.clear();
	Tokenize( c[11].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->fin[i].start_date.minute = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize start_date.second
	vAux.clear();
	Tokenize( c[12].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->fin[i].start_date.second = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize fin.rev
	vAux.clear();
	Tokenize( c[13].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->fin[i].rev = atof( (*p).c_str() );
		i++;
	}

	// Tokenize fin.net_earn
	vAux.clear();
	Tokenize( c[14].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->fin[i].net_earn = atof( (*p).c_str() );
		i++;
	}

	// Tokenize fin.basic_eps
	vAux.clear();
	Tokenize( c[15].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->fin[i].basic_eps = atof( (*p).c_str() );
		i++;
	}

	// Tokenize fin.dilut_eps
	vAux.clear();
	Tokenize( c[16].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->fin[i].dilut_eps = atof( (*p).c_str() );
		i++;
	}

	// Tokenize fin.margin
	vAux.clear();
	Tokenize( c[17].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->fin[i].margin = atof( (*p).c_str() );
		i++;
	}

	// Tokenize fin.invent
	vAux.clear();
	Tokenize( c[18].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->fin[i].invent = atof( (*p).c_str() );
		i++;
	}

	// Tokenize fin.assets
	vAux.clear();
	Tokenize( c[19].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->fin[i].assets = atof( (*p).c_str() );
		i++;
	}

	// Tokenize fin.liab
	vAux.clear();
	Tokenize( c[20].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->fin[i].liab = atof( (*p).c_str() );
		i++;
	}

	// Tokenize fin.out_basic
	vAux.clear();
	Tokenize( c[21].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->fin[i].out_basic = atof( (*p).c_str() );
		i++;
	}

	// Tokenize fin.out_dilut
	vAux.clear();
	Tokenize( c[22].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->fin[i].out_dilut = atof( (*p).c_str() );
		i++;
	}

	// Tokenize date.year
	vAux.clear();
	Tokenize( c[23].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->day[i].date.year = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize date.month
	vAux.clear();
	Tokenize( c[24].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->day[i].date.month = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize date.day
	vAux.clear();
	Tokenize( c[25].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->day[i].date.day = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize date.hour
	vAux.clear();
	Tokenize( c[26].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->day[i].date.hour = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize date.minute
	vAux.clear();
	Tokenize( c[27].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->day[i].date.minute = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize date.second
	vAux.clear();
	Tokenize( c[28].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->day[i].date.second = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize day.close
	vAux.clear();
	Tokenize( c[29].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->day[i].close = atof( (*p).c_str() );
		i++;
	}

	// Tokenize day.high
	vAux.clear();
	Tokenize( c[30].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->day[i].high = atof( (*p).c_str() );
		i++;
	}

	// Tokenize day.low
	vAux.clear();
	Tokenize( c[31].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->day[i].low = atof( (*p).c_str() );
		i++;
	}

	// Tokenize day.vol
	vAux.clear();
	Tokenize( c[32].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->day[i].vol = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize news.item
	vAux.clear();
	Tokenize( c[33].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strcpy(pFrame1Output->news[i].item, (*p).c_str());	
		i++;
	}

	// Tokenize news.dts.year
	vAux.clear();
	Tokenize( c[34].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->news[i].dts.year = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize news.dts.month
	vAux.clear();
	Tokenize( c[35].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->news[i].dts.month = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize news.dts.day
	vAux.clear();
	Tokenize( c[36].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->news[i].dts.day = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize news.dts.hour
	vAux.clear();
	Tokenize( c[37].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->news[i].dts.hour = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize news.dts.minute
	vAux.clear();
	Tokenize( c[38].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->news[i].dts.minute = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize news.dts.second
	vAux.clear();
	Tokenize( c[39].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->news[i].dts.second = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize news.src
	vAux.clear();
	Tokenize( c[40].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strcpy(pFrame1Output->news[i].src, (*p).c_str());	
		i++;
	}

	// Tokenize news.auth
	vAux.clear();
	Tokenize( c[41].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strcpy(pFrame1Output->news[i].auth, (*p).c_str());	
		i++;
	}

	// Tokenize news.headline
	vAux.clear();
	Tokenize( c[42].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strcpy(pFrame1Output->news[i].headline, (*p).c_str());	
		i++;
	}

	// Tokenize news.summary
	vAux.clear();
	Tokenize( c[43].c_str(), vAux);

	i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		strcpy(pFrame1Output->news[i].summary, (*p).c_str());	
		i++;
	}

	pFrame1Output->last_price = c[44].as(double());
	pFrame1Output->last_open = c[45].as(double());
	pFrame1Output->last_vol = c[46].as(int());
	strcpy(pFrame1Output->s_name, c[47].c_str());	
	//co_id - 48
	strcpy(pFrame1Output->co_name, c[49].c_str());	
	strcpy(pFrame1Output->sp_rate, c[50].c_str());	
	strcpy(pFrame1Output->ceo_name, c[51].c_str());	
	strcpy(pFrame1Output->co_desc, c[52].c_str());	
	pFrame1Output->open_date.year = c[53].as(int());
	pFrame1Output->open_date.month = c[54].as(int());
	pFrame1Output->open_date.day = c[55].as(int());
	pFrame1Output->open_date.hour = c[56].as(int());
	pFrame1Output->open_date.minute = c[57].as(int());
	pFrame1Output->open_date.second = int(c[58].as(double()));
	strcpy(pFrame1Output->co_st_id, c[59].c_str());
	strcpy(pFrame1Output->co_ad_line1, c[60].c_str());
	strcpy(pFrame1Output->co_ad_line2, c[61].c_str());
	strcpy(pFrame1Output->co_ad_town, c[62].c_str());
	strcpy(pFrame1Output->co_ad_div, c[63].c_str());
	strcpy(pFrame1Output->co_ad_zip, c[64].c_str());
	strcpy(pFrame1Output->co_ad_cty, c[65].c_str());
	pFrame1Output->num_out = c[66].as(int());
	pFrame1Output->start_date.year = c[67].as(int());
	pFrame1Output->start_date.month = c[68].as(int());
	pFrame1Output->start_date.day = c[69].as(int());
	pFrame1Output->start_date.hour = c[70].as(int());
	pFrame1Output->start_date.minute = c[71].as(int());
	pFrame1Output->start_date.second = int(c[72].as(double()));
	pFrame1Output->ex_date.year = c[73].as(int());
	pFrame1Output->ex_date.month = c[74].as(int());
	pFrame1Output->ex_date.day = c[75].as(int());
	pFrame1Output->ex_date.hour = c[76].as(int());
	pFrame1Output->ex_date.minute = c[77].as(int());
	pFrame1Output->ex_date.second = int(c[78].as(double()));
	pFrame1Output->pe_ratio = c[79].as(double());
	pFrame1Output->s52_wk_high = c[80].as(double());
	pFrame1Output->s52_wk_high_date.year = c[81].as(int());
	pFrame1Output->s52_wk_high_date.month = c[82].as(int());
	pFrame1Output->s52_wk_high_date.day = c[83].as(int());
	pFrame1Output->s52_wk_high_date.hour = c[84].as(int());
	pFrame1Output->s52_wk_high_date.minute = c[85].as(int());
	pFrame1Output->s52_wk_high_date.second = int(c[86].as(double()));
	pFrame1Output->s52_wk_low = c[87].as(double());
	pFrame1Output->s52_wk_low_date.year = c[88].as(int());
	pFrame1Output->s52_wk_low_date.month = c[89].as(int());
	pFrame1Output->s52_wk_low_date.day = c[90].as(int());
	pFrame1Output->s52_wk_low_date.hour = c[91].as(int());
	pFrame1Output->s52_wk_low_date.minute = c[92].as(int());
	pFrame1Output->s52_wk_low_date.second = int(c[93].as(double()));
	pFrame1Output->divid = c[94].as(double());
	pFrame1Output->yield = c[95].as(double());
	strcpy(pFrame1Output->ex_ad_div, c[96].c_str());
	strcpy(pFrame1Output->ex_ad_cty, c[97].c_str());
	strcpy(pFrame1Output->ex_ad_line1, c[98].c_str());
	strcpy(pFrame1Output->ex_ad_line2, c[99].c_str());
	strcpy(pFrame1Output->ex_ad_town, c[100].c_str());
	strcpy(pFrame1Output->ex_ad_zip, c[101].c_str());
	pFrame1Output->ex_close = c[102].as(int());
	strcpy(pFrame1Output->ex_desc, c[103].c_str());
	strcpy(pFrame1Output->ex_name, c[104].c_str());
	pFrame1Output->ex_num_symb = c[105].as(int());
	pFrame1Output->ex_open = c[106].as(int());

	pFrame1Output->status = CBaseTxnErr::SUCCESS;

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Security Detail Frame 1 (input)"<<endl
	    <<"- access_lob_flag: "<<pFrame1Input->access_lob_flag<<endl
	    <<"- start_day: "<<pFrame1Input->start_day.year<<"-"<<
		pFrame1Input->start_day.month<<"-"<<
		pFrame1Input->start_day.day<<" "<<
		pFrame1Input->start_day.hour<<":"<<
		pFrame1Input->start_day.minute<<":"<<
		pFrame1Input->start_day.second<<endl
	    <<"- max_rows_to_return: "<<pFrame1Input->max_rows_to_return<<endl
	    <<"- symbol: "<<pFrame1Input->symbol<<endl;
	cout << "Security Detail Frame 1 (output)" << endl <<
			"- fin_len: " << pFrame1Output->fin_len << endl <<
			"- day_len: " << pFrame1Output->day_len << endl <<
			"- news_len: " << pFrame1Output->news_len << endl <<
			"- cp_co_name[0]: " << pFrame1Output->cp_co_name[0] << endl <<
			"- cp_in_name[0]: " << pFrame1Output->cp_in_name[0] << endl <<
			"- fin[0].year: " << pFrame1Output->fin[0].year << endl <<
			"- fin[0].qtr: " << pFrame1Output->fin[0].qtr << endl <<
			"- fin[0].start_date: " << pFrame1Output->fin[0].start_date.year <<
			"-" << pFrame1Output->fin[0].start_date.month << "-" <<
			pFrame1Output->fin[0].start_date.day << " " <<
			pFrame1Output->fin[0].start_date.hour << ":" <<
			pFrame1Output->fin[0].start_date.minute << ":" <<
			pFrame1Output->fin[0].start_date.second << endl <<
			"- fin[0].rev: "<<pFrame1Output->fin[0].rev << endl <<
			"- fin[0].net_earn: " << pFrame1Output->fin[0].net_earn << endl <<
			"- fin[0].basic_eps: " << pFrame1Output->fin[0].basic_eps << endl <<
			"- fin[0].dilut_eps: " << pFrame1Output->fin[0].dilut_eps << endl <<
			"- fin[0].margin: " << pFrame1Output->fin[0].margin << endl <<
			"- fin[0].invent: " << pFrame1Output->fin[0].invent << endl <<
			"- fin[0].assets: " << pFrame1Output->fin[0].assets << endl <<
			"- fin[0].liab: " << pFrame1Output->fin[0].liab << endl <<
			"- fin[0].out_basic: " << pFrame1Output->fin[0].out_basic << endl <<
			"- fin[0].out_dilut: " << pFrame1Output->fin[0].out_dilut << endl <<
			"- day[0].date: " << pFrame1Output->day[0].date.year << "-" <<
			pFrame1Output->day[0].date.month << "-" <<
			pFrame1Output->day[0].date.day << " " <<
			pFrame1Output->day[0].date.hour << ":" <<
			pFrame1Output->day[0].date.minute << ":" <<
			pFrame1Output->day[0].date.second << endl <<
			"- day[0].close: " << pFrame1Output->day[0].close << endl <<
			"- day[0].high: " << pFrame1Output->day[0].high << endl <<
			"- day[0].low: " << pFrame1Output->day[0].low << endl <<
			"- day[0].vol: " << pFrame1Output->day[0].vol << endl <<
			"- news[0].item: " << pFrame1Output->news[0].item << endl <<
			"- news[0].dts: " << pFrame1Output->news[0].dts.year << "-" <<
			pFrame1Output->news[0].dts.month << "-" <<
			pFrame1Output->news[0].dts.day << " " <<
			pFrame1Output->news[0].dts.hour << ":" <<
			pFrame1Output->news[0].dts.minute << ":" <<
			pFrame1Output->news[0].dts.second << endl <<
			"- news[0].src: " << pFrame1Output->news[0].src << endl <<
			"- news[0].auth: " << pFrame1Output->news[0].auth << endl <<
			"- news[0].headline: " << pFrame1Output->news[0].headline << endl <<
			"- news[0].summary: " << pFrame1Output->news[0].summary << endl <<
			"- last_price: " << pFrame1Output->last_price << endl <<
			"- last_open: " << pFrame1Output->last_open << endl <<
			"- last_vol: " << pFrame1Output->last_vol << endl <<
			"- s_name: " << pFrame1Output->s_name << endl <<
			"- co_name: " << pFrame1Output->co_name << endl <<
			"- sp_rate: " << pFrame1Output->sp_rate << endl <<
			"- ceo_name: " << pFrame1Output->ceo_name << endl <<
			"- co_desc: " << pFrame1Output->co_desc << endl <<
			"- open_date: " << pFrame1Output->open_date.year << "-" <<
			pFrame1Output->open_date.month << "-" <<
			pFrame1Output->open_date.day << " " <<
			pFrame1Output->open_date.hour << ":" <<
			pFrame1Output->open_date.minute << ":" <<
			pFrame1Output->open_date.second << endl << "- co_st_id: " <<
			pFrame1Output->co_st_id << endl << "- co_ad_line1: " <<
			pFrame1Output->co_ad_line1 << endl <<
			"- co_ad_line2: " << pFrame1Output->co_ad_line2 << endl <<
			"- co_ad_town: " << pFrame1Output->co_ad_town << endl <<
			"- co_ad_div: " << pFrame1Output->co_ad_div << endl <<
			"- co_ad_zip: " << pFrame1Output->co_ad_zip << endl <<
			"- co_ad_cty: " << pFrame1Output->co_ad_cty << endl <<
			"- num_out: " << pFrame1Output->num_out << endl <<
			"- start_date: " << pFrame1Output->start_date.year << "-" <<
			pFrame1Output->start_date.month << "-" <<
			pFrame1Output->start_date.day << " " <<
			pFrame1Output->start_date.hour << ":" <<
			pFrame1Output->start_date.minute << ":" <<
			pFrame1Output->start_date.second << endl <<
			"- ex_date: " << pFrame1Output->ex_date.year << "-" <<
			pFrame1Output->ex_date.month << "-" <<
			pFrame1Output->ex_date.day << " " << pFrame1Output->ex_date.hour <<
			":" << pFrame1Output->ex_date.minute << ":" <<
			pFrame1Output->ex_date.second << endl <<
			"- pe_ratio: " << pFrame1Output->pe_ratio << endl <<
			"- s52_wk_high: " << pFrame1Output->s52_wk_high << endl <<
			"- s52_wk_high_date: " << pFrame1Output->s52_wk_high_date.year <<
			"-" << pFrame1Output->s52_wk_high_date.month << "-" <<
			pFrame1Output->s52_wk_high_date.day << " " <<
			pFrame1Output->s52_wk_high_date.hour << ":" <<
			pFrame1Output->s52_wk_high_date.minute << ":" <<
			pFrame1Output->s52_wk_high_date.second << endl <<
			"- s52_wk_low: " << pFrame1Output->s52_wk_low << endl <<
			"- s52_wk_low_date: " << pFrame1Output->s52_wk_low_date.year <<
			"-" << pFrame1Output->s52_wk_low_date.month << "-" <<
			pFrame1Output->s52_wk_low_date.day << " " <<
			pFrame1Output->s52_wk_low_date.hour << ":" <<
			pFrame1Output->s52_wk_low_date.minute << ":" <<
			pFrame1Output->s52_wk_low_date.second << endl <<
			"- divid: " << pFrame1Output->divid << endl <<
			"- yield: " << pFrame1Output->yield << endl <<
			"- ex_ad_div: " << pFrame1Output->ex_ad_div << endl <<
			"- ex_ad_cty: " << pFrame1Output->ex_ad_cty << endl <<
			"- ex_ad_line1: " << pFrame1Output->ex_ad_line1 << endl <<
			"- ex_ad_line2: " << pFrame1Output->ex_ad_line2 << endl <<
			"- ex_ad_town: " << pFrame1Output->ex_ad_town << endl <<
			"- ex_ad_zip: " << pFrame1Output->ex_ad_zip << endl <<
			"- ex_close: " << pFrame1Output->ex_close << endl <<
			"- ex_desc: " << pFrame1Output->ex_desc << endl <<
			"- ex_name: " << pFrame1Output->ex_name << endl <<
			"- ex_num_symb: " << pFrame1Output->ex_num_symb << endl <<
			"- ex_open: " << pFrame1Output->ex_open << endl;
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
