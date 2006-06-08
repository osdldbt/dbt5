/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Database loader class for COMPANY table.
*/
#ifndef ODBC_COMPANY_LOAD_H
#define ODBC_COMPANY_LOAD_H

namespace TPCE
{

class CODBCCompanyLoad : public CDBLoader <COMPANY_ROW>
{	
private:
	DBDATETIME	ODBC_CO_OPEN_DATE;
	virtual inline void CopyRow(PT row)
	{
		memcpy(&m_row, row, sizeof(m_row));
		m_row.CO_OPEN_DATE.GetDBDATETIME(&ODBC_CO_OPEN_DATE);
	};

public:
	CODBCCompanyLoad(char *szServer, char *szDatabase, char *szTable = "COMPANY")
		: CDBLoader<COMPANY_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.CO_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.CO_ST_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.CO_NAME, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.CO_IN_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.CO_SP_RATE, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.CO_CEO, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.CO_AD_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.CO_DESC, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &ODBC_CO_OPEN_DATE, 0, SQL_VARLEN_DATA, NULL, 0, SQLDATETIME, ++i) != SUCCEED
			)
			ThrowError(CODBCERR::eBcpBind);

		if ( bcp_control(m_hdbc, BCPHINTS, "ORDER (CO_ID)" ) != SUCCEED )	
			ThrowError(CODBCERR::eBcpControl);
	};

};

}	// namespace TPCE

#endif //ODBC_COMPANY_LOAD_H