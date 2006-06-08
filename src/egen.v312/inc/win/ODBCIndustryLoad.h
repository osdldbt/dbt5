/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/


/*
*	Database loader class for INDUSTRY table.
*/
#ifndef ODBC_INDUSTRY_LOAD_H
#define ODBC_INDUSTRY_LOAD_H

namespace TPCE
{

class CODBCIndustryLoad : public CDBLoader <INDUSTRY_ROW>
{	
public:
	CODBCIndustryLoad(char *szServer, char *szDatabase, char *szTable = "INDUSTRY")
		: CDBLoader<INDUSTRY_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.IN_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.IN_NAME, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.IN_SC_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			)
			ThrowError(CODBCERR::eBcpBind);

//		if ( bcp_control(m_hdbc, BCPHINTS, "TABLOCK" ) != SUCCEED )	
//			ThrowError(CODBCERR::eBcpControl);
	};

};

}	// namespace TPCE

#endif //ODBC_INDUSTRY_LOAD_H
