/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/


/*
*	Database loader class for EXCHANGE table.
*/
#ifndef ODBC_EXCHANGE_LOAD_H
#define ODBC_EXCHANGE_LOAD_H

namespace TPCE
{

class CODBCExchangeLoad : public CDBLoader <EXCHANGE_ROW>
{	
public:
	CODBCExchangeLoad(char *szServer, char *szDatabase, char *szTable = "EXCHANGE")
		: CDBLoader<EXCHANGE_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.EX_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
		    || bcp_bind(m_hdbc, (BYTE *) &m_row.EX_NAME, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.EX_NUM_SYMB, 0, SQL_VARLEN_DATA, NULL, 0, SQLINT4, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.EX_OPEN, 0, SQL_VARLEN_DATA, NULL, 0, SQLINT4, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.EX_CLOSE, 0, SQL_VARLEN_DATA, NULL, 0, SQLINT4, ++i) != SUCCEED
//		    || bcp_bind(m_hdbc, (BYTE *) &m_row.EX_OPEN, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
//		    || bcp_bind(m_hdbc, (BYTE *) &m_row.EX_CLOSE, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
		    || bcp_bind(m_hdbc, (BYTE *) &m_row.EX_DESC, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.EX_AD_ID, 0, SQL_VARLEN_DATA, NULL, 0, SQLINT4, ++i) != SUCCEED
			)
			ThrowError(CODBCERR::eBcpBind);

//		if ( bcp_control(m_hdbc, BCPHINTS, "TABLOCK" ) != SUCCEED )	
//			ThrowError(CODBCERR::eBcpControl);
	};

};

}	// namespace TPCE

#endif //ODBC_EXCHANGE_LOAD_H