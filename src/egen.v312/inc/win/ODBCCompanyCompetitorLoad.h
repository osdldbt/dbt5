/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/


/*
*	Database loader class for COMPANY_COMPETITOR table.
*/
#ifndef ODBC_COMPANY_COMPETITOR_LOAD_H
#define ODBC_COMPANY_COMPETITOR_LOAD_H

namespace TPCE
{

class CODBCCompanyCompetitorLoad : public CDBLoader <COMPANY_COMPETITOR_ROW>
{	
public:
	CODBCCompanyCompetitorLoad(char *szServer, char *szDatabase, char *szTable = "COMPANY_COMPETITOR")
		: CDBLoader<COMPANY_COMPETITOR_ROW>(szServer, szDatabase, szTable)
	{
	}

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.CP_CO_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.CP_COMP_CO_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.CP_IN_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED			
			)
			ThrowError(CODBCERR::eBcpBind);

//		if ( bcp_control(m_hdbc, BCPHINTS, "ORDER (AD_ID)" ) != SUCCEED )	
//			ThrowError(CODBCERR::eBcpControl);
	};

};

}	// namespace TPCE

#endif //ODBC_COMPANY_COMPETITOR_LOAD_H