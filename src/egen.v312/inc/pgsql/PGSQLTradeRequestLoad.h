/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento

/*
*	Database loader class for TRADE_REQUEST table.
*/
#ifndef PGSQL_TRADE_REQUEST_LOAD_H
#define PGSQL_TRADE_REQUEST_LOAD_H

namespace TPCE
{

class CPGSQLTradeRequestLoad : public CPGSQLLoader <TRADE_REQUEST_ROW>
{	

public:
	CPGSQLTradeRequestLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "TRADE_REQUEST")
		: CPGSQLLoader<TRADE_REQUEST_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.TR_T_ID));
			buf.push_back(m_row.TR_TT_ID);
			buf.push_back(m_row.TR_S_SYMB);
			buf.push_back(stringify(m_row.TR_QTY));
			buf.push_back(stringify(m_row.TR_BID_PRICE));
			buf.push_back(stringify(m_row.TR_CA_ID));
	
			m_TW->insert(buf);
			buf.clear();
		}
		catch (const sql_error &e)
		{
			cerr << "SQL error: " << e.what() << endl
			<< "Query was: '" << e.query() << "'" << endl;
		}
		catch (const exception &e)
		{
			cerr<<e.what() << endl;
		}
	}
};

}	// namespace TPCE

#endif //PGSQL_TRADE_REQUEST_LOAD_H
