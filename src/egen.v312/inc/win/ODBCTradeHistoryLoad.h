/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Database loader class for TRADE_HISTORY table.
*/
#ifndef ODBC_TRADE_HISTORY_LOAD_H
#define ODBC_TRADE_HISTORY_LOAD_H

namespace TPCE
{

class CODBCTradeHistoryLoad : public CDBLoader <TRADE_HISTORY_ROW>
{	
private:
	DBDATETIME	ODBC_TH_DTS;
	virtual inline void CopyRow(PT row)
	{
		memcpy(&m_row, row, sizeof(m_row));
		m_row.TH_DTS.GetDBDATETIME(&ODBC_TH_DTS);
	};

public:
	CODBCTradeHistoryLoad(char *szServer, char *szDatabase, char *szTable = "TRADE_HISTORY")
		: CDBLoader<TRADE_HISTORY_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.TH_T_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED						
			|| bcp_bind(m_hdbc, (BYTE *) &ODBC_TH_DTS, 0, SQL_VARLEN_DATA, NULL, 0, SQLDATETIME, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.TH_ST_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			)
			ThrowError(CODBCERR::eBcpBind);
		
		if ( bcp_control(m_hdbc, BCPHINTS, "ORDER (TH_T_ID)" ) != SUCCEED )	
			ThrowError(CODBCERR::eBcpControl);
	};

};

}	// namespace TPCE

#endif //ODBC_TRADE_HISTORY_LOAD_H