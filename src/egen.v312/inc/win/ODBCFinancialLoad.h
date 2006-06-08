/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Database loader class for FINANCIAL table.
*/
#ifndef ODBC_FINANCIAL_LOAD_H
#define ODBC_FINANCIAL_LOAD_H

namespace TPCE
{

class CODBCFinancialLoad : public CDBLoader <FINANCIAL_ROW>
{	
private:
	DBDATETIME	ODBC_FI_QTR_START_DATE;
	virtual inline void CopyRow(PT row)
	{
		memcpy(&m_row, row, sizeof(m_row));
		m_row.FI_QTR_START_DATE.GetDBDATETIME(&ODBC_FI_QTR_START_DATE);
	};

public:
	CODBCFinancialLoad(char *szServer, char *szDatabase, char *szTable = "FINANCIAL")
		: CDBLoader<FINANCIAL_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.FI_CO_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.FI_YEAR, 0, SQL_VARLEN_DATA, NULL, 0, SQLINT4, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.FI_QTR, 0, SQL_VARLEN_DATA, NULL, 0, SQLINT4, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &ODBC_FI_QTR_START_DATE, 0, SQL_VARLEN_DATA, NULL, 0, SQLDATETIME, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.FI_REVENUE, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.FI_NET_EARN, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.FI_BASIC_EPS, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.FI_DILUT_EPS, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.FI_MARGIN, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.FI_INVENTORY, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.FI_ASSETS, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.FI_LIABILITY, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED			
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.FI_OUT_BASIC, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.FI_OUT_DILUT, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			)
			ThrowError(CODBCERR::eBcpBind);

		if ( bcp_control(m_hdbc, BCPHINTS, "ORDER (FI_CO_ID)" ) != SUCCEED )	
			ThrowError(CODBCERR::eBcpControl);
	};

};

}	// namespace TPCE

#endif //ODBC_FINANCIAL_LOAD_H