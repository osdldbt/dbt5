/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/
// 2006 Rilson Nascimento

/*
*	Database loader class for TRADE_HISTORY table.
*/
#ifndef PGSQL_TRADE_HISTORY_LOAD_H
#define PGSQL_TRADE_HISTORY_LOAD_H

namespace TPCE
{

class CPGSQLTradeHistoryLoad : public CPGSQLLoader <TRADE_HISTORY_ROW>
{	

public:
	CPGSQLTradeHistoryLoad(char *szHost, char *szDBName, char *szPostmasterPort, char *szTable = "TRADE_HISTORY")
		: CPGSQLLoader<TRADE_HISTORY_ROW>(szHost, szDBName, szPostmasterPort, szTable)
	{
	};


	virtual void WriteNextRecord(PT next_record)
	{
		CopyRow(next_record);	//copy to the bound location inside this class first
	
		try {
			buf.push_back(stringify(m_row.TH_T_ID));			
			buf.push_back(m_row.TH_DTS.ToStr(iDateTimeFmt));
			buf.push_back(m_row.TH_ST_ID);
	
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

#endif //PGSQL_TRADE_HISTORY_LOAD_H
