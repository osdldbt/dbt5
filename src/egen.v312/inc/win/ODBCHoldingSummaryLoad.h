/******************************************************************************
*	(c) Copyright 2005, Hewlett Packard
*	Provided to the TPC under license.
******************************************************************************/

/******************************************************************************
*
*	Original Author:	Doug Johnson
*
*	Contributors:		{Add your name here if you make modifications}
*
*	Description:		MSSQL ODBC implementation of HOLDING_SUMMARY loader.
*
******************************************************************************/
#ifndef ODBC_HOLDING_SUMMARY_LOAD_H
#define ODBC_HOLDING_SUMMARY_LOAD_H

namespace TPCE
{

class CODBCHoldingSummaryLoad : public CDBLoader <HOLDING_SUMMARY_ROW>
{	
private:
public:
	CODBCHoldingSummaryLoad(char *szServer, char *szDatabase, char *szTable = "HOLDING_SUMMARY")
		: CDBLoader<HOLDING_SUMMARY_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.HS_CA_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.HS_S_SYMB, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, 0, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.HS_QTY, 0, SQL_VARLEN_DATA, NULL, 0, SQLINT4, ++i) != SUCCEED
			)
			ThrowError(CODBCERR::eBcpBind);
	};

};

}	// namespace TPCE

#endif //ODBC_HOLDING_SUMMARY_LOAD_H