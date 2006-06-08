/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Database loader class for NEWS_ITEM table.
*/
#ifndef ODBC_NEWS_ITEM_LOAD_H
#define ODBC_NEWS_ITEM_LOAD_H

namespace TPCE
{

class CODBCNewsItemLoad : public CDBLoader <NEWS_ITEM_ROW>
{	
	DBDATETIME	ODBC_NI_DTS;

	virtual inline void CopyRow(PT row)
	{
		memcpy(&m_row, row, sizeof(m_row));
		m_row.NI_DTS.GetDBDATETIME(&ODBC_NI_DTS);
	};

public:
	CODBCNewsItemLoad(char *szServer, char *szDatabase, char *szTable = "NEWS_ITEM")
		: CDBLoader<NEWS_ITEM_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.NI_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.NI_HEADLINE, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.NI_SUMMARY, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.NI_ITEM, 0, cNI_ITEM_len, NULL, 0, SQLTEXT, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &ODBC_NI_DTS, 0, SQL_VARLEN_DATA, NULL, 0, SQLDATETIME, ++i) != SUCCEED	
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.NI_SOURCE, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.NI_AUTHOR, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			)
			ThrowError(CODBCERR::eBcpBind);

		if ( bcp_control(m_hdbc, BCPHINTS, "ORDER (NI_ID)" ) != SUCCEED )	
			ThrowError(CODBCERR::eBcpControl);
	};

};

}	// namespace TPCE

#endif //ODBC_NEWS_ITEM_LOAD_H