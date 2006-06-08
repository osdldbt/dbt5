/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/


/*
*	Database loader class for SETTLEMENT table.
*/
#ifndef ODBC_SETTLEMENT_LOAD_H
#define ODBC_SETTLEMENT_LOAD_H

namespace TPCE
{

class CODBCSettlementLoad : public CDBLoader <SETTLEMENT_ROW>
{	
private:
	DBDATETIME	ODBC_SE_CASH_DUE_DATE;
	virtual inline void CopyRow(PT row)
	{
		memcpy(&m_row, row, sizeof(m_row));
		m_row.SE_CASH_DUE_DATE.GetDBDATETIME(&ODBC_SE_CASH_DUE_DATE);
	};

public:
	CODBCSettlementLoad(char *szServer, char *szDatabase, char *szTable = "SETTLEMENT")
		: CDBLoader<SETTLEMENT_ROW>(szServer, szDatabase, szTable)
	{		
	};

	void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.SE_T_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED			
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.SE_CASH_TYPE, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &ODBC_SE_CASH_DUE_DATE, 0, SQL_VARLEN_DATA, NULL, 0, SQLDATETIME, ++i) != SUCCEED			
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.SE_AMT, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED			
			)
			ThrowError(CODBCERR::eBcpBind);
		
		if ( bcp_control(m_hdbc, BCPHINTS, "ORDER (SE_T_ID)" ) != SUCCEED )	
			ThrowError(CODBCERR::eBcpControl);
	}	
};

}	// namespace TPCE

#endif //ODBC_SETTLEMENT_LOAD_H