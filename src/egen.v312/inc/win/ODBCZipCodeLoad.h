/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/


/*
*	Database loader class for ZIP_CODE table.
*/
#ifndef ODBC_ZIP_CODE_LOAD_H
#define ODBC_ZIP_CODE_LOAD_H

namespace TPCE
{

class CODBCZipCodeLoad : public CDBLoader <ZIP_CODE_ROW>
{	
public:
	CODBCZipCodeLoad(char *szServer, char *szDatabase, char *szTable = "ZIP_CODE")
		: CDBLoader<ZIP_CODE_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.ZC_CODE, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.ZC_TOWN, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.ZC_DIV, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED			
			)
			ThrowError(CODBCERR::eBcpBind);

//		if ( bcp_control(m_hdbc, BCPHINTS, "TABLOCK, ORDER (AD_ID)" ) != SUCCEED )	
//			ThrowError(CODBCERR::eBcpControl);
	};

};

}	// namespace TPCE

#endif //ODBC_ZIP_CODE_LOAD_H