/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	ODBC Database loader class for DAILY_MARKET table.
*/
#ifndef ODBC_DAILY_MARKET_LOAD_H
#define ODBC_DAILY_MARKET_LOAD_H

namespace TPCE
{

class CODBCDailyMarketLoad : public CDBLoader <DAILY_MARKET_ROW>
{	
private:
	DBDATETIME	ODBC_DM_DATE;
	virtual inline void CopyRow(PT row)
	{
		memcpy(&m_row, row, sizeof(m_row));
		m_row.DM_DATE.GetDBDATETIME(&ODBC_DM_DATE);
	};

public:
	CODBCDailyMarketLoad(char *szServer, char *szDatabase, char *szTable = "DAILY_MARKET")
		: CDBLoader<DAILY_MARKET_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (
			   bcp_bind(m_hdbc, (BYTE *) &ODBC_DM_DATE, 0, SQL_VARLEN_DATA, NULL, 0, SQLDATETIME, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.DM_S_SYMB, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED			
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.DM_CLOSE, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.DM_HIGH, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.DM_LOW, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.DM_VOL, 0, SQL_VARLEN_DATA, NULL, 0, SQLINT4, ++i) != SUCCEED			
			)
			ThrowError(CODBCERR::eBcpBind);

		//if ( bcp_control(m_hdbc, BCPHINTS, "TABLOCK" ) != SUCCEED )	
		//	ThrowError(CODBCERR::eBcpControl);
	};

};

}	// namespace TPCE

#endif //ODBC_DAILY_MARKET_LOAD_H