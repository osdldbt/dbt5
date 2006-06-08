/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/


/*
*	Database loader class for CASH_TRANSACTION table.
*/
#ifndef ODBC_CASH_TRANSACTION_LOAD_H
#define ODBC_CASH_TRANSACTION_LOAD_H

namespace TPCE
{

class CODBCCashTransactionLoad : public CDBLoader <CASH_TRANSACTION_ROW>
{	
private:
	DBDATETIME	ODBC_CT_DTS;
	virtual inline void CopyRow(PT row)
	{
		memcpy(&m_row, row, sizeof(m_row));
		m_row.CT_DTS.GetDBDATETIME( &ODBC_CT_DTS );
	};

public:
	CODBCCashTransactionLoad(char *szServer, char *szDatabase, char *szTable = "CASH_TRANSACTION")
		: CDBLoader<CASH_TRANSACTION_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.CT_T_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED   			
			|| bcp_bind(m_hdbc, (BYTE *) &ODBC_CT_DTS, 0, SQL_VARLEN_DATA, NULL, 0, SQLDATETIME, ++i) != SUCCEED			
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.CT_AMT, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.CT_NAME, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			)
			ThrowError(CODBCERR::eBcpBind);
		
		if ( bcp_control(m_hdbc, BCPHINTS, "ORDER (CT_T_ID)" ) != SUCCEED )	
			ThrowError(CODBCERR::eBcpControl);
	};

};

}	// namespace TPCE

#endif //ODBC_CASH_TRANSACTION_LOAD_H