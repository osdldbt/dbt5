/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Database loader class for HOLDING_HISTORY table.
*/
#ifndef HOLDING_HISTORY_LOAD_H
#define HOLDING_HISTORY_LOAD_H

namespace TPCE
{

class CODBCHoldingHistoryLoad : public CDBLoader <HOLDING_HISTORY_ROW>
{
public:
	CODBCHoldingHistoryLoad(char *szServer, char *szDatabase, char *szTable = "HOLDING_HISTORY")
		: CDBLoader<HOLDING_HISTORY_ROW>(szServer, szDatabase, szTable)
	{
	};

	virtual void BindColumns()
	{
		//Binding function we have to implement.
		int i = 0;
		if (   bcp_bind(m_hdbc, (BYTE *) &m_row.HH_H_T_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.HH_T_ID, 0, SQL_VARLEN_DATA, NULL, 0, IDENT_BIND, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.HH_BEFORE_QTY, 0, SQL_VARLEN_DATA, NULL, 0, SQLINT4, ++i) != SUCCEED
			|| bcp_bind(m_hdbc, (BYTE *) &m_row.HH_AFTER_QTY, 0, SQL_VARLEN_DATA, NULL, 0, SQLINT4, ++i) != SUCCEED
			)
			ThrowError(CODBCERR::eBcpBind);

		if ( bcp_control(m_hdbc, BCPHINTS, "ORDER (HH_H_T_ID)" ) != SUCCEED )	
			ThrowError(CODBCERR::eBcpControl);
	};
};

}	// namespace TPCE

#endif //HOLDING_HISTORY_LOAD_H