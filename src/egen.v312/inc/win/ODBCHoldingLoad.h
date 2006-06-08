/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Database loader class for HOLDING table.
*/
#ifndef ODBC_HOLDING_LOAD_H
#define ODBC_HOLDING_LOAD_H

namespace TPCE
{

class CODBCHoldingLoad : public CDBLoader <HOLDING_ROW>
{	
private:
	DBDATETIME	ODBC_H_DTS;
	virtual inline void CopyRow(PT row)
	{
		memcpy(&m_row, row, sizeof(m_row));
		m_row.H_DTS.GetDBDATETIME(&ODBC_H_DTS);
	};

public:
	CODBCHoldingLoad(char *szServer, char *szDatabase, char *szTable = "HOLDING")
		: CDBLoader<HOLDING_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.H_T_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.H_CA_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.H_S_SYMB, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, 0, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &ODBC_H_DTS, 0, SQL_VARLEN_DATA, NULL, 0, SQLDATETIME, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.H_PRICE, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.H_QTY, 0, SQL_VARLEN_DATA, NULL, 0, SQLINT4, ++i) != SUCCEED
			)
			ThrowError(CODBCERR::eBcpBind);
		
		if ( bcp_control(m_hdbc, BCPHINTS, "ORDER (H_T_ID)" ) != SUCCEED )	
			ThrowError(CODBCERR::eBcpControl);
	};

};

}	// namespace TPCE

#endif //ODBC_HOLDING_LOAD_H