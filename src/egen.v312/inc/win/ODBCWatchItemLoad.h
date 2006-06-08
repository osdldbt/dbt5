/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Database loader class for WATCH_ITEM table.
*/
#ifndef ODBC_WATCH_ITEM_LOAD_H
#define ODBC_WATCH_ITEM_LOAD_H

namespace TPCE
{

class CODBCWatchItemLoad : public CDBLoader <WATCH_ITEM_ROW>
{	
public:
	CODBCWatchItemLoad(char *szServer, char *szDatabase, char *szTable = "WATCH_ITEM")
		: CDBLoader<WATCH_ITEM_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.WI_WL_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.WI_S_SYMB, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			)
			ThrowError(CODBCERR::eBcpBind);

		if ( bcp_control(m_hdbc, BCPHINTS, "ORDER (WI_WL_ID)" ) != SUCCEED )	
			ThrowError(CODBCERR::eBcpControl);
	};

};

}	// namespace TPCE

#endif //ODBC_WATCH_ITEM_LOAD_H