/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/


/*
*	Database loader class for LAST TRADE table.
*/
#ifndef ODBC_LAST_TRADE_LOAD_H
#define ODBC_LAST_TRADE_LOAD_H

namespace TPCE
{

class CODBCLastTradeLoad : public CDBLoader <LAST_TRADE_ROW>
{	
private:
	DBDATETIME	ODBC_LT_DTS;

	virtual inline void CopyRow(PT row)
	{
		memcpy(&m_row, row, sizeof(m_row));

		m_row.LT_DTS.GetDBDATETIME(&ODBC_LT_DTS);
	};

public:
	CODBCLastTradeLoad(char *szServer, char *szDatabase, char *szTable = "LAST_TRADE")
		: CDBLoader<LAST_TRADE_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.LT_S_SYMB, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &ODBC_LT_DTS, 0, SQL_VARLEN_DATA, NULL, 0, SQLDATETIME, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.LT_PRICE, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.LT_OPEN_PRICE, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED			
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.LT_VOL, 0, SQL_VARLEN_DATA, NULL, 0, SQLINT4, ++i) != SUCCEED
			)
			ThrowError(CODBCERR::eBcpBind);

		//if ( bcp_control(m_hdbc, BCPHINTS, "TABLOCK" ) != SUCCEED )	
		//	ThrowError(CODBCERR::eBcpControl);
	};

};

}	// namespace TPCE

#endif //ODBC_LAST_TRADE_LOAD_H