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
void CCustomerPositionDB::DoCustomerPositionFrame1(
		const TCustomerPositionFrame1Input *pIn,
		TCustomerPositionFrame1Output *pOut)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from CustomerPositionFrame1(" <<
			pIn->cust_id << ",'" << pIn->tax_id << "'," <<
			max_acct_len <<
			") as (cust_id IDENT_T, acct_id text, acct_len integer, "
			"acct_bal text, asset_total text, C_ST_ID char(4), C_L_NAME "
			"varchar, C_F_NAME varchar, C_M_NAME char, C_GNDR char, C_TIER "
			"smallint, cdobyear double precision, cdobmonth double precision, "
			"cdobday double precision, cdobhour double precision, cdobminute "
			"double precision, cdobsec double precision, C_AD_ID IDENT_T, "
			"C_CTRY_1 char(3), C_AREA_1 char(3), C_LOCAL_1 char(10), C_EXT_1 "
			"char(5), C_CTRY_2 char(3), C_AREA_2 char(3), C_LOCAL_2 char(10), "
			"C_EXT_2 char(5), C_CTRY_3 char(3), C_AREA_3 char(3), C_LOCAL_3 "
			"char(10), C_EXT_3 char(5), C_EMAIL_1 varchar, C_EMAIL_2 varchar);";

	BeginTxn();
	// Isolation level required by Clause 7.4.1.3
	m_Txn->exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;");
	result R( m_Txn->exec( osCall.str() ) );

	if (R.empty())
	{
		//throw logic_error("empty result set!");
		cout<<"warning: empty result set at DoCustomerPositionFrame1"<<endl;
		return;
	}
	result::const_iterator c = R.begin();

	vector<string> vAux;
	vector<string>::iterator p;

	// Tokenize acct_id
	Tokenize( c[1].c_str(), vAux);

	int i = 0;
	for  (p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->acct_id[i] = atoi( (*p).c_str() );
		i++;
	}

	// Tokenize cash_bal
	vAux.clear();
	Tokenize( c[3].c_str(), vAux);

	i = 0;
	for  ( p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->cash_bal[i] = atof( (*p).c_str() );
		i++;
	}

	// Tokenize asset_total
	vAux.clear();
	Tokenize( c[4].c_str(), vAux);

	i = 0;
	for  ( p = vAux.begin(); p != vAux.end(); ++p)
	{
		pOut->asset_total[i] = atof( (*p).c_str() );
		i++;
	}

	pOut->cust_id = c[0].as(int());
	pOut->acct_len = c[2].as(int());
	strcpy(pOut->c_st_id, c[5].c_str());	
	strcpy(pOut->c_l_name, c[6].c_str());
	strcpy(pOut->c_f_name, c[7].c_str());
	strcpy(pOut->c_m_name, c[8].c_str());
	strcpy(pOut->c_gndr, c[9].c_str());
	strcpy(&pOut->c_tier, c[10].c_str());
	pOut->c_dob.year = c[11].as(int());
	pOut->c_dob.month = c[12].as(int());
	pOut->c_dob.day = c[13].as(int());
	pOut->c_dob.hour = c[14].as(int());
	pOut->c_dob.minute = c[15].as(int());
	pOut->c_dob.second = int(c[16].as(double()));
	pOut->c_ad_id = c[17].as(int());
	strcpy(pOut->c_ctry_1, c[18].c_str());
	strcpy(pOut->c_area_1, c[19].c_str());
	strcpy(pOut->c_local_1, c[20].c_str());
	strcpy(pOut->c_ext_1, c[21].c_str());
	strcpy(pOut->c_ctry_2, c[22].c_str());
	strcpy(pOut->c_area_2, c[23].c_str());
	strcpy(pOut->c_local_2, c[24].c_str());
	strcpy(pOut->c_ext_2, c[25].c_str());
	strcpy(pOut->c_ctry_3, c[26].c_str());
	strcpy(pOut->c_area_3, c[27].c_str());
	strcpy(pOut->c_local_3, c[28].c_str());
	strcpy(pOut->c_ext_3, c[29].c_str());
	strcpy(pOut->c_email_1, c[30].c_str());
	strcpy(pOut->c_email_2, c[31].c_str());

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Customer Position Frame 1 (input)"<<endl
	    <<"- cust_id: "<<pIn->cust_id<<endl
	    <<"- tax_id: "<<pIn->tax_id<<endl;
	cout << "Customer Position Frame 1 (output)" << endl <<
			"- cust_id: " << pOut->cust_id << endl <<
			"- acct_len: " << pOut->acct_len << endl <<
			"- acct_id[0]: " << pOut->acct_id[0] << endl <<
			"- cash_bal[0]: " << pOut->cash_bal[0] << endl <<
			"- asset_total[0]: " << pOut->asset_total[0] << endl <<
			"- c_st_id: " << pOut->c_st_id << endl <<
			"- c_l_name: " << pOut->c_l_name << endl <<
			"- c_f_name: " << pOut->c_f_name << endl <<
			"- c_m_name: " << pOut->c_m_name << endl <<
			"- c_gndr: " << pOut->c_gndr << endl <<
			"- c_tier: " << pOut->c_tier << endl <<
			"- c_dob[0]: " << pOut->c_dob.year << "-" <<
			pOut->c_dob.month << "-" << pOut->c_dob.day <<
			" " << pOut->c_dob.hour << ":" <<
			pOut->c_dob.minute << ":" << pOut->c_dob.second <<
			endl <<
			"- c_ad_id: " << pOut->c_ad_id << endl <<
			"- c_ctry_1: " << pOut->c_ctry_1 << endl <<
			"- c_area_1: " << pOut->c_area_1 << endl <<
			"- c_local_1: " << pOut->c_local_1 << endl <<
			"- c_ext_1: " << pOut->c_ext_1 << endl <<
			"- c_ctry_2: " << pOut->c_ctry_2 << endl <<
			"- c_area_2: " << pOut->c_area_2 << endl <<
			"- c_local_2: " << pOut->c_local_2 << endl <<
			"- c_ext_2: " << pOut->c_ext_2 << endl <<
			"- c_ctry_3: " << pOut->c_ctry_3 << endl <<
			"- c_area_3: " << pOut->c_area_3 << endl <<
			"- c_local_3: " << pOut->c_local_3 << endl <<
			"- c_ext_3: " << pOut->c_ext_3 << endl <<
			"- c_email_1: " << pOut->c_email_1 << endl <<
			"- c_email_2: " << pOut->c_email_2 << endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG
	
#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}

// Call Customer Position Frame 2
void CCustomerPositionDB::DoCustomerPositionFrame2(
		const TCustomerPositionFrame2Input *pIn,
		TCustomerPositionFrame2Output *pOut)
{
#if defined(COMPILE_PLSQL_FUNCTION)

	ostringstream osCall;
	osCall << "select * from CustomerPositionFrame2(" <<
			pIn->acct_id <<
			") as (t_id trade_t, t_s_symb char(15), t_qty S_QTY_T, st_name "
			"char(10), dtsyear double precision, dtsmonth double precision, "
			"dtsday double precision, dtshour double precision, dtsminute "
			"double precision, dtssec double precision);";

	// we are inside a txn
	result R( m_Txn->exec( osCall.str() ) );
	CommitTxn();	

	if (R.empty())
	{
		//throw logic_error("empty result set!");
		cout<<"warning: empty result set at DoCustomerPositionFrame2"<<endl;
	}
	result::const_iterator c = R.begin();

	int i = 0;	
	for ( c; c != R.end(); ++c )
	{
		pOut->trade_id[i] = c[0].as(int());
		strcpy(pOut->symbol[i], c[1].c_str());
		pOut->qty[i] = c[2].as(int());
		strcpy(pOut->trade_status[i], c[3].c_str());
		pOut->hist_dts[i].year = c[4].as(int());
		pOut->hist_dts[i].month = c[5].as(int());
		pOut->hist_dts[i].day = c[6].as(int());
		pOut->hist_dts[i].hour = c[7].as(int());
		pOut->hist_dts[i].minute = c[8].as(int());
		pOut->hist_dts[i].second = int(c[9].as(double()));

		i++;
	}
	pOut->hist_len = i;
	pOut->status = CBaseTxnErr::SUCCESS;

#ifdef DEBUG
	m_coutLock.ClaimLock();
	cout<<"Customer Position Frame 2 (input)"<<endl
	    <<"- cust_id: "<<pIn->acct_id<<endl;
	cout<<"Customer Position Frame 2 (output)"<<endl
	    <<"- hist_len: "<<pOut->hist_len<<endl
	    <<"- trade_id[0]: "<<pOut->trade_id[0]<<endl
	    <<"- symbol[0]: "<<pOut->symbol[0]<<endl
	    <<"- qty[0]: "<<pOut->qty[0]<<endl
	    <<"- trade_status[0]: "<<pOut->trade_status[0]<<endl
	    <<"- hist_dts[0]: "<<pOut->hist_dts[0].year<<"-"<<
		pOut->hist_dts[0].month<<"-"<<
		pOut->hist_dts[0].day<<" "<<
		pOut->hist_dts[0].hour<<":"<<
		pOut->hist_dts[0].minute<<":"<<
		pOut->hist_dts[0].second<<endl;
	m_coutLock.ReleaseLock();
#endif // DEBUG
	
#elif defined(COMPILE_C_FUNCTION)
//
// TODO C-language function
//
#endif //COMPILE_PLSQL_FUNCTION
}

// Call Customer Position Frame 3
void CCustomerPositionDB::DoCustomerPositionFrame3(
		TCustomerPositionFrame3Output *pOut)
{
	// commit the transaction we are inside
	CommitTxn();
	//pFrame3Output->status = CBaseTxnErr::SUCCESS;
}

void CCustomerPositionDB::Cleanup(void* pException)
{
}
