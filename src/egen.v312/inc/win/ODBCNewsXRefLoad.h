/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Database loader class for NEWS_XREF table.
*/
#ifndef ODBC_NEWS_XREF_LOAD_H
#define ODBC_NEWS_XREF_LOAD_H

namespace TPCE
{

class CODBCNewsXRefLoad : public CDBLoader <NEWS_XREF_ROW>
{	
public:
	CODBCNewsXRefLoad(char *szServer, char *szDatabase, char *szTable = "NEWS_XREF")
		: CDBLoader<NEWS_XREF_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.NX_NI_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.NX_CO_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED			
			)
			ThrowError(CODBCERR::eBcpBind);

		if ( bcp_control(m_hdbc, BCPHINTS, "ORDER (NX_NI_ID)" ) != SUCCEED )	
			ThrowError(CODBCERR::eBcpControl);
	};

};

}	// namespace TPCE

#endif //ODBC_NEWS_XREF_LOAD_H