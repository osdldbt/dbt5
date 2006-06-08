/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/


/*
*	Database loader class for COMMISSION_RATE table.
*/
#ifndef ODBC_COMMISSION_RATE_LOAD_H
#define ODBC_COMMISSION_RATE_LOAD_H

namespace TPCE
{

class CODBCCommissionRateLoad : public CDBLoader <COMMISSION_RATE_ROW>
{	
public:
	CODBCCommissionRateLoad(char *szServer, char *szDatabase, char *szTable = "COMMISSION_RATE")
		: CDBLoader<COMMISSION_RATE_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.CR_C_TIER, 0, SQL_VARLEN_DATA, NULL, 0, SQLINT4, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.CR_TT_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.CR_EX_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.CR_FROM_QTY, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.CR_TO_QTY, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.CR_RATE, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			)
			ThrowError(CODBCERR::eBcpBind);

//		if ( bcp_control(m_hdbc, BCPHINTS, "TABLOCK" ) != SUCCEED )	
//			ThrowError(CODBCERR::eBcpControl);
	};

};

}	// namespace TPCE

#endif //ODBC_COMMISSION_RATE_LOAD_H