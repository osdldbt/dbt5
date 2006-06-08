/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Database loader class for WATCH_LIST table.
*/
#ifndef ODBC_WATCH_LIST_LOAD_H
#define ODBC_WATCH_LIST_LOAD_H

namespace TPCE
{

class CODBCWatchListLoad : public CDBLoader <WATCH_LIST_ROW>
{	
public:
	CODBCWatchListLoad(char *szServer, char *szDatabase, char *szTable = "WATCH_LIST")
		: CDBLoader<WATCH_LIST_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.WL_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.WL_C_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED			
			)
			ThrowError(CODBCERR::eBcpBind);

		if ( bcp_control(m_hdbc, BCPHINTS, "ORDER (WL_ID)" ) != SUCCEED )	
			ThrowError(CODBCERR::eBcpControl);
	};

};

}	// namespace TPCE

#endif //ODBC_WATCH_LIST_LOAD_H