/*
 * CustomerPositionDB.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 12 July 2006
 */

#include <transactions.h>

using namespace TPCE;

// Constructor
CCustomerPositionDB::CCustomerPositionDB(CDBConnection *pDBConn)
: CTxnBaseDB(pDBConn)
{
}

// Destructor
CCustomerPositionDB::~CCustomerPositionDB()
{
}

// Call Customer Position Frame 1
void CCustomerPositionDB::DoCustomerPositionFrame1(PCustomerPositionFrame1Input pFrame1Input, PCustomerPositionFrame1Output pFrame1Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from CustomerPositionFrame1("<<pFrame1Input->cust_id<<",'"
			<<pFrame1Input->tax_id<<"',"<<max_acct_len<<") as (cust_id IDENT_T, acct_id text, acct_len integer,"
			"acct_bal text, asset_total text, C_ST_ID char(4), C_L_NAME varchar, C_F_NAME varchar, C_M_NAME char,"
			"C_GNDR char, C_TIER smallint, cdobyear double precision, cdobmonth double precision, cdobday double "
			"precision, cdobhour double precision, cdobminute double precision, cdobsec double precision, C_AD_ID "
			"IDENT_T, C_CTRY_1 char(3), C_AREA_1 char(3), C_LOCAL_1 char(10), C_EXT_1 char(5), C_CTRY_2 char(3), "
			"C_AREA_2 char(3), C_LOCAL_2 char(10), C_EXT_2 char(5), C_CTRY_3 char(3), C_AREA_3 char(3), C_LOCAL_3 "
			"char(10), C_EXT_3 char(5), C_EMAIL_1 varchar, C_EMAIL_2 varchar);";

	BeginTxn();
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty())
	{
		throw logic_error("empty result set!");
	}
	result::const_iterator c = R.begin();

	vector<string> vAux;
	vector<string>::iterator p;

	// Tokenize acct_id
	Tokenize( c[1].c_str(), vAux);

	int i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->acct_id[i] = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize cash_bal
	vAux.clear();
	Tokenize( c[3].c_str(), vAux);

	i = 0;
	for  ( p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->cash_bal[i] = atof( (*p).c_str() );
		i++;
	}

	// Tokenize asset_total
	vAux.clear();
	Tokenize( c[4].c_str(), vAux);

	i = 0;
	for  ( p = vAux.begin(); p != vAux.end(); ++p)
	{
		pFrame1Output->asset_total[i] = atof( (*p).c_str() );
		i++;
	}

	pFrame1Output->cust_id = c[0].as(int());
	pFrame1Output->acct_len = c[2].as(int());
	sprintf(pFrame1Output->c_st_id, "%s", c[5].c_str() );	
	sprintf(pFrame1Output->c_l_name, "%s", c[6].c_str() );
	sprintf(pFrame1Output->c_f_name, "%s", c[7].c_str() );
	sprintf(pFrame1Output->c_m_name, "%s", c[8].c_str() );
	sprintf(pFrame1Output->c_gndr, "%s", c[9].c_str() );
	sprintf(&pFrame1Output->c_tier, "%s", c[10].c_str() );
	pFrame1Output->c_dob.year = c[11].as(int());
	pFrame1Output->c_dob.month = c[12].as(int());
	pFrame1Output->c_dob.day = c[13].as(int());
	pFrame1Output->c_dob.hour = c[14].as(int());
	pFrame1Output->c_dob.minute = c[15].as(int());
	pFrame1Output->c_dob.second = int(c[16].as(double()));
	pFrame1Output->c_ad_id = c[17].as(int());
	sprintf(pFrame1Output->c_ctry_1, "%s", c[18].c_str() );
	sprintf(pFrame1Output->c_area_1, "%s", c[19].c_str() );
	sprintf(pFrame1Output->c_local_1, "%s", c[20].c_str() );
	sprintf(pFrame1Output->c_ext_1, "%s", c[21].c_str() );
	sprintf(pFrame1Output->c_ctry_2, "%s", c[22].c_str() );
	sprintf(pFrame1Output->c_area_2, "%s", c[23].c_str() );
	sprintf(pFrame1Output->c_local_2, "%s", c[24].c_str() );
	sprintf(pFrame1Output->c_ext_2, "%s", c[25].c_str() );
	sprintf(pFrame1Output->c_ctry_3, "%s", c[26].c_str() );
	sprintf(pFrame1Output->c_area_3, "%s", c[27].c_str() );
	sprintf(pFrame1Output->c_local_3, "%s", c[28].c_str() );
	sprintf(pFrame1Output->c_ext_3, "%s", c[29].c_str() );
	sprintf(pFrame1Output->c_email_1, "%s", c[30].c_str() );
	sprintf(pFrame1Output->c_email_2, "%s", c[31].c_str() );

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Customer Position Frame 1 (input)"<<endl
	    <<"- cust_id: "<<pFrame1Input->cust_id<<endl
	    <<"- tax_id: "<<pFrame1Input->tax_id<<endl;
	cout<<"Customer Position Frame 1 (output)"<<endl
	    <<"- cust_id: "<<pFrame1Output->cust_id<<endl
	    <<"- acct_len: "<<pFrame1Output->acct_len<<endl
	    <<"- acct_id[0]: "<<pFrame1Output->acct_id[0]<<endl
	    <<"- cash_bal[0]: "<<pFrame1Output->cash_bal[0]<<endl
	    <<"- asset_total[0]: "<<pFrame1Output->asset_total[0]<<endl
	    <<"- c_st_id: "<<pFrame1Output->c_st_id<<endl
	    <<"- c_l_name: "<<pFrame1Output->c_l_name<<endl
	    <<"- c_f_name: "<<pFrame1Output->c_f_name<<endl
	    <<"- c_m_name: "<<pFrame1Output->c_m_name<<endl
	    <<"- c_gndr: "<<pFrame1Output->c_gndr<<endl
	    <<"- c_tier: "<<pFrame1Output->c_tier<<endl
	    <<"- c_dob[0]: "<<pFrame1Output->c_dob.year<<"-"<<pFrame1Output->c_dob.month<<"-"
		<<pFrame1Output->c_dob.day<<" "<<pFrame1Output->c_dob.hour<<":"
		<<pFrame1Output->c_dob.minute<<":"<<pFrame1Output->c_dob.second<<endl
	    <<"- c_ad_id: "<<pFrame1Output->c_ad_id<<endl
	    <<"- c_ctry_1: "<<pFrame1Output->c_ctry_1<<endl
	    <<"- c_area_1: "<<pFrame1Output->c_area_1<<endl
	    <<"- c_local_1: "<<pFrame1Output->c_local_1<<endl
	    <<"- c_ext_1: "<<pFrame1Output->c_ext_1<<endl
	    <<"- c_ctry_2: "<<pFrame1Output->c_ctry_2<<endl
	    <<"- c_area_2: "<<pFrame1Output->c_area_2<<endl
	    <<"- c_local_2: "<<pFrame1Output->c_local_2<<endl
	    <<"- c_ext_2: "<<pFrame1Output->c_ext_2<<endl
	    <<"- c_ctry_3: "<<pFrame1Output->c_ctry_3<<endl
	    <<"- c_area_3: "<<pFrame1Output->c_area_3<<endl
	    <<"- c_local_3: "<<pFrame1Output->c_local_3<<endl
	    <<"- c_ext_3: "<<pFrame1Output->c_ext_3<<endl
	    <<"- c_email_1: "<<pFrame1Output->c_email_1<<endl
	    <<"- c_email_2: "<<pFrame1Output->c_email_2<<endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG
	
#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}

// Call Customer Position Frame 2
void CCustomerPositionDB::DoCustomerPositionFrame2(PCustomerPositionFrame2Input pFrame2Input, PCustomerPositionFrame2Output pFrame2Output)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from CustomerPositionFrame2("<<pFrame2Input->acct_id<<") as (t_id trade_t, t_s_symb char(15),"
			"t_qty S_QTY_T, st_name char(10), dtsyear double precision, dtsmonth double precision, dtsday "
			"double precision, dtshour double precision, dtsminute double precision, dtssec double precision);";

	// we are inside a txn
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();	

	if (R.empty())
	{
		throw logic_error("empty result set!");
	}
	result::const_iterator c = R.begin();

	int i = 0;	
	for ( c; c != R.end(); ++c )
	{
		pFrame2Output->trade_id[i] = c[0].as(int());
		sprintf(pFrame2Output->symbol[i], "%s", c[1].c_str());
		pFrame2Output->qty[i] = c[2].as(int());
		sprintf(pFrame2Output->trade_status[i], "%s", c[3].c_str());
		pFrame2Output->hist_dts[i].year = c[4].as(int());
		pFrame2Output->hist_dts[i].month = c[5].as(int());
		pFrame2Output->hist_dts[i].day = c[6].as(int());
		pFrame2Output->hist_dts[i].hour = c[7].as(int());
		pFrame2Output->hist_dts[i].minute = c[8].as(int());
		pFrame2Output->hist_dts[i].second = int(c[9].as(double()));

		i++;
	}
	pFrame2Output->hist_len = i;

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Customer Position Frame 2 (input)"<<endl
	    <<"- cust_id: "<<pFrame2Input->acct_id<<endl;
	cout<<"Customer Position Frame 2 (output)"<<endl
	    <<"- hist_len: "<<pFrame2Output->hist_len<<endl
	    <<"- trade_id[0]: "<<pFrame2Output->trade_id[0]<<endl
	    <<"- symbol[0]: "<<pFrame2Output->symbol[0]<<endl
	    <<"- qty[0]: "<<pFrame2Output->qty[0]<<endl
	    <<"- trade_status[0]: "<<pFrame2Output->trade_status[0]<<endl
	    <<"- hist_dts[0]: "<<pFrame2Output->hist_dts[0].year<<"-"<<
		pFrame2Output->hist_dts[0].month<<"-"<<
		pFrame2Output->hist_dts[0].day<<" "<<
		pFrame2Output->hist_dts[0].hour<<":"<<
		pFrame2Output->hist_dts[0].minute<<":"<<
		pFrame2Output->hist_dts[0].second<<endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG
	
#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}

// Call Customer Position Frame 3
void CCustomerPositionDB::DoCustomerPositionFrame3(PCustomerPositionFrame3Output pFrame3Output)
{
	// commit the transaction we are inside
	CommitTxn();
	//pFrame3Output->status = CBaseTxnErr::SUCCESS;
}

void CCustomerPositionDB::Cleanup(void* pException)
{
}
