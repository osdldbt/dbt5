/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/


/*
*	Database loader class for STATUS_TYPE table.
*/
#ifndef ODBC_STATUS_TYPE_LOAD_H
#define ODBC_STATUS_TYPE_LOAD_H

namespace TPCE
{

class CODBCStatusTypeLoad : public CDBLoader <STATUS_TYPE_ROW>
{	
public:
	CODBCStatusTypeLoad(char *szServer, char *szDatabase, char *szTable = "STATUS_TYPE")
		: CDBLoader<STATUS_TYPE_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.ST_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.ST_NAME, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			)
			ThrowError(CODBCERR::eBcpBind);

//		if ( bcp_control(m_hdbc, BCPHINTS, "TABLOCK" ) != SUCCEED )	
//			ThrowError(CODBCERR::eBcpControl);
	};

};

}	// namespace TPCE

#endif //ODBC_STATUS_TYPE_LOAD_H