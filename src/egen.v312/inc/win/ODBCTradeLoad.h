/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/


/*
*	Database loader class for TRADE table.
*/
#ifndef ODBC_TRADE_LOAD_H
#define ODBC_TRADE_LOAD_H

namespace TPCE
{

class CODBCTradeLoad : public CDBLoader <TRADE_ROW>
{	
private:
	DBDATETIME	ODBC_T_DTS;
	virtual inline void CopyRow(PT row)
	{
		memcpy(&m_row, row, sizeof(m_row));
		m_row.T_DTS.GetDBDATETIME(&ODBC_T_DTS);
	};

public:
	CODBCTradeLoad(char *szServer, char *szDatabase, char *szTable = "TRADE")
		: CDBLoader<TRADE_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		int		iKeepIdentityTrue = TRUE;		

		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.T_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &ODBC_T_DTS, 0, SQL_VARLEN_DATA, NULL, 0, SQLDATETIME, ++i) != SUCCEED			
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.T_ST_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.T_TT_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.T_IS_CASH, 0, SQL_VARLEN_DATA, NULL, 0, SQLINT4, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.T_S_SYMB, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.T_QTY, 0, SQL_VARLEN_DATA, NULL, 0, SQLINT4, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.T_BID_PRICE, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.T_CA_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.T_EXEC_NAME, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.T_TRADE_PRICE, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.T_CHRG, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED			
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.T_COMM, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.T_TAX, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.T_LIFO, 0, SQL_VARLEN_DATA, NULL, 0, SQLINT4, ++i) != SUCCEED
			)
			ThrowError(CODBCERR::eBcpBind);

		if ( bcp_control(m_hdbc, BCPHINTS, "ORDER (T_ID)" ) != SUCCEED )	
			ThrowError(CODBCERR::eBcpControl);

		// Load passed in T_ID data
		if ( bcp_control(m_hdbc, BCPKEEPIDENTITY, &iKeepIdentityTrue ) != SUCCEED )	
			ThrowError(CODBCERR::eBcpControl);

	};

};

}	// namespace TPCE

#endif //ODBC_TRADE_LOAD_H