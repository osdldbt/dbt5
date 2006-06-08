/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/


/*
*	Database loader class for SECTOR table.
*/
#ifndef ODBC_SECTOR_LOAD_H
#define ODBC_SECTOR_LOAD_H

namespace TPCE
{

class CODBCSectorLoad : public CDBLoader <SECTOR_ROW>
{	
public:
	CODBCSectorLoad(char *szServer, char *szDatabase, char *szTable = "SECTOR")
		: CDBLoader<SECTOR_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.SC_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.SC_NAME, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED			
			)
			ThrowError(CODBCERR::eBcpBind);

//		if ( bcp_control(m_hdbc, BCPHINTS, "TABLOCK" ) != SUCCEED )	
//			ThrowError(CODBCERR::eBcpControl);
	};

};

}	// namespace TPCE

#endif //ODBC_SECTOR_LOAD_H