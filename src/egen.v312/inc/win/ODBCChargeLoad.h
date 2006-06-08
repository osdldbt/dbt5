/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/


/*
*	Database loader class for CHARGE table.
*/
#ifndef ODBC_CHARGE_LOAD_H
#define ODBC_CHARGE_LOAD_H

namespace TPCE
{

class CODBCChargeLoad : public CDBLoader <CHARGE_ROW>
{	
public:
	CODBCChargeLoad(char *szServer, char *szDatabase, char *szTable = "CHARGE")
		: CDBLoader<CHARGE_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.CH_TT_ID, 0, SQL_VARLEN_DATA, (BYTE *)"", 1, SQLCHARACTER, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.CH_C_TIER, 0, SQL_VARLEN_DATA, NULL, 0, SQLINT4, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.CH_CHRG, 0, SQL_VARLEN_DATA, NULL, 0, SQLFLT8, ++i) != SUCCEED

			)
			ThrowError(CODBCERR::eBcpBind);

//		if ( bcp_control(m_hdbc, BCPHINTS, "TABLOCK" ) != SUCCEED )	
//			ThrowError(CODBCERR::eBcpControl);
	};

};

}	// namespace TPCE

#endif //ODBC_CHARGE_LOAD_H